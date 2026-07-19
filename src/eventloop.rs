//! The daemon's main event loop and graceful shutdown.
//!
//! Drains the RPC, IPFS-publish, and CRUD service inboxes each tick, delivers
//! plugin envelopes, and on Ctrl-C persists entity state and closes the iroh
//! endpoint.  Split out of `main.rs` so the entry point covers only startup.

use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use ma_core::config::Config;
use ma_core::{
    Did, Inbox, IpfsGatewayResolver, MaEndpoint, Message, SigningKey, INBOX_PROTOCOL_ID,
    IPFS_PROTOCOL_ID, MESSAGE_TYPE_CRUD, MESSAGE_TYPE_CRUD_REPLY,
    MESSAGE_TYPE_IDENTITY_PUBLISH_REQUEST, MESSAGE_TYPE_IPFS_REQUEST, MESSAGE_TYPE_RPC,
    MESSAGE_TYPE_RPC_REPLY,
};
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};
use tokio::sync::RwLock;
use tracing::{debug, error, info, warn};
use zeroize::Zeroize;

use crate::acl::{AclCache, GroupCache, SharedAcl};
use crate::entity::{KindRegistry, SendEnvelope};
use crate::ipfs::IpfsServiceState;
use crate::manifest::ManifestWriter;
use crate::plugin::EntityRegistry;
use crate::status::SharedStats;
use crate::{bootstrap, crud, i18n, inbox, ipfs, rpc, status};

/// Map a `message_type` string to the iroh delivery protocol.
///
/// Only RPC and its reply go to `/ma/rpc/0.0.1`; IPFS requests go to
/// `/ma/ipfs/0.0.1`; CRUD goes to `/ma/crud/0.0.1`.  Everything else
/// (message, broadcast, chat, emote, unknown) falls back to `/ma/inbox/0.0.1`.
fn protocol_for(msg_type: &str) -> &'static str {
    match msg_type {
        MESSAGE_TYPE_RPC | MESSAGE_TYPE_RPC_REPLY => rpc::RPC_PROTOCOL_ID,
        MESSAGE_TYPE_IPFS_REQUEST | MESSAGE_TYPE_IDENTITY_PUBLISH_REQUEST => IPFS_PROTOCOL_ID,
        MESSAGE_TYPE_CRUD | MESSAGE_TYPE_CRUD_REPLY => crud::CRUD_PROTOCOL_ID,
        _ => INBOX_PROTOCOL_ID,
    }
}

#[allow(clippy::too_many_arguments, clippy::too_many_lines)]
pub async fn run(
    mut endpoint: Arc<dyn MaEndpoint>,
    rpc_messages: Inbox<Message>,
    inbox_messages: Inbox<Message>,
    mut crud_messages: Option<Inbox<Message>>,
    mut ipfs_state: Option<IpfsServiceState>,
    envelope_tx: UnboundedSender<(String, SendEnvelope)>,
    mut envelope_rx: UnboundedReceiver<(String, SendEnvelope)>,
    shared_config: Arc<RwLock<Config>>,
    shared_resolver: Arc<IpfsGatewayResolver>,
    stats: SharedStats,
    acl: SharedAcl,
    acl_cache: AclCache,
    group_cache: GroupCache,
    entity_registry: EntityRegistry,
    kind_registry: KindRegistry,
    manifest_writer: ManifestWriter,
    our_did: String,
    signing_key: SigningKey,
    avatar_key: [u8; 32],
    runtime_ipns_key: [u8; 32],
    did_publish_timeout_secs: u64,
    did_publish_lifetime_hours: u64,
    poll_ms: u64,
) -> Result<()> {
    let mut ticker = tokio::time::interval(Duration::from_millis(poll_ms));
    let ctrl_c = tokio::signal::ctrl_c();
    tokio::pin!(ctrl_c);

    loop {
        tokio::select! {
            _ = ticker.tick() => {
                let now = status::now_unix_secs();
                let kubo_url = shared_config.read().await.kubo_rpc_url.clone();

                // Drain /ma/rpc/0.0.1
                while let Some(mut message) = rpc_messages.pop(now) {
                    debug!(
                        node = %message.from,
                        protocol = rpc::RPC_PROTOCOL_ID,
                        "{}", i18n::t("node-connected")
                    );
                    info!(
                        from = %message.from,
                        to = %message.to,
                        id = %message.id,
                        message_type = %message.message_type,
                        "{}", i18n::t("rpc-message-received")
                    );
                    {
                        let mut s = stats.write().await;
                        s.rpc_requests += 1;
                    }
                    let acl_snapshot = acl.read().await.clone();
                    let ctx = rpc::RpcHandlerCtx {
                        our_did: Arc::from(our_did.as_str()),
                        signing_key: Arc::new(signing_key.clone()),
                        endpoint: Arc::clone(&endpoint),
                        kubo_rpc_url: Arc::from(kubo_url.as_str()),
                        resolver: Arc::clone(&shared_resolver),
                        entity_registry: entity_registry.clone(),
                        kind_registry: kind_registry.clone(),
                        envelope_tx: envelope_tx.clone(),
                        stats: stats.clone(),
                        acl_cache: acl_cache.clone(),
                        group_cache: group_cache.clone(),
                        avatar_key,
                        manifest_writer: manifest_writer.clone(),
                        shared_config: Arc::clone(&shared_config),
                    };
                    tokio::spawn(async move {
                        if let Err(err) = tokio::time::timeout(
                            Duration::from_secs(30),
                            rpc::handle_rpc_message(&message, &acl_snapshot, &ctx),
                        )
                        .await
                        .unwrap_or_else(|_| Err(anyhow::anyhow!("rpc handler timed out")))
                        {
                            warn!(error = %err, from = %message.from, "{}", i18n::t("rpc-message-rejected"));
                        }
                        message.content.zeroize();
                        message.signature.zeroize();
                    });
                }

                // Drain /ma/ipfs/0.0.1
                if let Some(ref mut ipfs) = ipfs_state {
                    while let Some(mut message) = ipfs.messages.pop(now) {
                        debug!(
                            node = %message.from,
                            protocol = IPFS_PROTOCOL_ID,
                            "{}", i18n::t("node-connected")
                        );
                        debug!(
                            from = %message.from,
                            to = %message.to,
                            id = %message.id,
                            message_type = %message.message_type,
                            content_len = message.content.len(),
                            "{}", i18n::t("received-encrypted-ma-msg")
                        );
                        {
                            let mut s = stats.write().await;
                            s.ipfs_requests += 1;
                        }
                        let acl_snapshot = acl.read().await.clone();
                        if let Err(err) = tokio::time::timeout(
                            Duration::from_mins(1),
                            ipfs::handle_ipfs_message(
                            &message,
                            &acl_snapshot,
                            &ipfs::IpfsHandlerCtx {
                                our_did: &our_did,
                                signing_key: &signing_key,
                                endpoint: Arc::clone(&endpoint),
                                kubo_rpc_url: &kubo_url,
                                publisher: &ipfs.publisher,
                                resolver: Arc::clone(&shared_resolver),
                                doc_cache: Arc::clone(&ipfs.doc_cache),
                                group_cache: group_cache.clone(),
                            },
                            &mut ipfs.replay_guard,
                        ))
                        .await
                        .unwrap_or_else(|_| Err(anyhow::anyhow!("ipfs handler timed out")))
                        {
                            warn!(error = %err, from = %message.from, "{}", i18n::t("ipfs-message-rejected"));
                        }
                        message.content.zeroize();
                        message.signature.zeroize();
                    }
                }

                // Drain /ma/crud/0.0.1
                if let Some(ref mut crud_inbox) = crud_messages {
                    while let Some(mut message) = crud_inbox.pop(now) {
                        info!(
                            from = %message.from,
                            to = %message.to,
                            id = %message.id,
                            message_type = %message.message_type,
                            "{}", i18n::t("crud-message-received")
                        );
                        // Snapshot the ACL and drop the read guard *before* the
                        // await. handle_crud_message may acquire a write lock on
                        // the same SharedAcl (e.g. :acl: edit-save), and holding
                        // a read guard across that await would deadlock.
                        let acl_snapshot = acl.read().await.clone();
                        let ctx = crud::CrudHandlerCtx {
                            our_did: Arc::from(our_did.as_str()),
                            signing_key: Arc::new(signing_key.clone()),
                            endpoint: Arc::clone(&endpoint),
                            kubo_rpc_url: Arc::from(kubo_url.as_str()),
                            resolver: Arc::clone(&shared_resolver),
                            stats: stats.clone(),
                            entity_registry: entity_registry.clone(),
                            kind_registry: kind_registry.clone(),
                            shared_config: Arc::clone(&shared_config),
                            acl_cache: acl_cache.clone(),
                            group_cache: group_cache.clone(),
                            root_acl: acl.clone(),
                            envelope_tx: envelope_tx.clone(),
                            avatar_key,
                            manifest_writer: manifest_writer.clone(),
                        };
                        tokio::spawn(async move {
                            if let Err(err) = tokio::time::timeout(
                                Duration::from_secs(30),
                                crud::handle_crud_message(&message, &acl_snapshot, &ctx),
                            )
                            .await
                            .unwrap_or_else(|_| Err(anyhow::anyhow!("crud handler timed out")))
                            {
                                warn!(error = %err, from = %message.from, "CRUD message rejected");
                            }
                            message.content.zeroize();
                            message.signature.zeroize();
                        });
                    }
                }

                // Drain /ma/inbox/0.0.1
                while let Some(mut message) = inbox_messages.pop(now) {
                    debug!(
                        from = %message.from,
                        to = %message.to,
                        message_type = %message.message_type,
                        "{}", i18n::t("inbox-message-received")
                    );
                    let ctx = inbox::InboxHandlerCtx {
                        our_did: Arc::from(our_did.as_str()),
                        entity_registry: entity_registry.clone(),
                        kubo_rpc_url: Arc::from(kubo_url.as_str()),
                    };
                    tokio::spawn(async move {
                        if let Err(err) = tokio::time::timeout(
                            Duration::from_secs(30),
                            inbox::handle_inbox_message(&message, &ctx),
                        )
                        .await
                        .unwrap_or_else(|_| Err(anyhow::anyhow!("inbox handler timed out")))
                        {
                            warn!(error = %err, from = %message.from, "inbox message rejected");
                        }
                        message.content.zeroize();
                        message.signature.zeroize();
                    });
                }

                // Drain plugin outbox — envelopes sent fire-and-forget by ma_send/ma_reply.
                while let Ok((fragment, env)) = envelope_rx.try_recv() {
                    let msg_type = if env.reply_to.is_some() {
                        MESSAGE_TYPE_RPC_REPLY
                    } else {
                        env.message_type.as_deref().unwrap_or(MESSAGE_TYPE_RPC)
                    };
                    let sender_did_url = format!("{our_did}#{fragment}");
                    let recipient = match Did::try_from(env.to.as_str()) {
                        Ok(d) => d,
                        Err(e) => {
                            warn!(fragment = %fragment, to = %env.to, error = %e, "plugin envelope: invalid recipient DID; skipped");
                            continue;
                        }
                    };
                    let mut msg = match ma_core::Message::new(
                        &sender_did_url,
                        &env.to,
                        msg_type,
                        &env.content_type,
                        &env.content,
                        &signing_key,
                    ) {
                        Ok(m) => m,
                        Err(e) => {
                            warn!(fragment = %fragment, error = %e, "plugin envelope: failed to build message; skipped");
                            continue;
                        }
                    };
                    msg.reply_to = env.reply_to;
                    let protocol = protocol_for(msg_type);
                    // Spawn each delivery independently so one unreachable peer
                    // cannot block others. Cap the outbox-open at 5 seconds.
                    let ep   = Arc::clone(&endpoint);
                    let res  = Arc::clone(&shared_resolver);
                    let base = recipient.base_id().clone();
                    tokio::spawn(async move {
                        match tokio::time::timeout(
                            Duration::from_secs(5),
                            ep.outbox(res.as_ref(), &base, protocol),
                        )
                        .await
                        {
                            Ok(Ok(mut outbox)) => {
                                if let Err(e) = outbox.send(&msg).await {
                                    warn!(fragment = %fragment, to = %env.to, error = %e, "plugin envelope delivery failed");
                                }
                            }
                            Ok(Err(e)) => warn!(fragment = %fragment, to = %env.to, error = %e, "plugin envelope: outbox open failed"),
                            Err(_)     => warn!(fragment = %fragment, to = %env.to, "plugin envelope: outbox connect timed out (5 s)"),
                        }
                    });
                }
            }
            signal = &mut ctrl_c => {
                if let Err(err) = signal {
                    error!(error = %err, "{}", i18n::t("ctrlc-handler-failed"));
                }
                eprintln!();
                eprintln!("{}", i18n::t("shutdown-requested"));
                info!("{}", i18n::t("shutdown-requested"));
                let kubo_url = shared_config.read().await.kubo_rpc_url.clone();

                // ── Persist entity states before exit ─────────────────────────
                let active_root_cid = stats.read().await.root_cid.clone();
                if let Some(ref rc) = active_root_cid {
                    let count = entity_registry.read().await.len();
                    if count > 0 {
                        info!(count = %count, "{}", i18n::t("entity-states-saving"));
                        match bootstrap::save_all_entity_states(
                            rc,
                            &kubo_url,
                            &entity_registry,
                        )
                        .await
                        {
                            Ok(new_cid) => {
                                stats.write().await.root_cid = Some(new_cid.clone());
                                info!(cid = %new_cid, "{}", i18n::t("entity-states-saved"));
                            }
                            Err(e) => warn!(error = %e, "Failed to save entity states"),
                        }
                    }

                    let latest_root_cid = stats.read().await.root_cid.clone().unwrap_or_else(|| rc.clone());
                    match tokio::time::timeout(
                        Duration::from_secs(did_publish_timeout_secs),
                        ipfs::publish_runtime_root_cid(
                            &kubo_url,
                            &runtime_ipns_key,
                            &latest_root_cid,
                            did_publish_lifetime_hours,
                        ),
                    )
                    .await
                    {
                        Ok(Ok(_)) => info!(runtime_cid = %latest_root_cid, "shutdown runtime_ipns publish succeeded"),
                        Ok(Err(err)) => error!(runtime_cid = %latest_root_cid, error = %format!("{err:#}"), "shutdown runtime_ipns publish failed"),
                        Err(_) => error!(runtime_cid = %latest_root_cid, "shutdown runtime_ipns publish timed out"),
                    }
                }

                break;
            }
        }
    }

    info!("{}", i18n::t("closing-endpoint"));
    // Wait up to 10 s for in-flight delivery tasks that hold Arc clones to
    // release them, then unwrap to get &mut and call close() gracefully.
    let close_deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    while Arc::strong_count(&endpoint) > 1 && tokio::time::Instant::now() < close_deadline {
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    match Arc::get_mut(&mut endpoint) {
        Some(ep) => {
            if tokio::time::timeout(Duration::from_secs(5), ep.close())
                .await
                .is_err()
            {
                warn!("endpoint close timed out after 5 s; forcing exit");
            }
        }
        None => {
            warn!("endpoint still held by in-flight tasks after 10 s; dropping without graceful close");
        }
    }
    info!("{}", i18n::t("shutdown-complete"));
    Ok(())
}
