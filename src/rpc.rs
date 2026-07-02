//! `/ma/rpc/0.0.1` handler: entity plugin dispatch and `:ping`.

use std::sync::Arc;

use anyhow::{anyhow, Context, Result};
use ciborium::Value as CborValue;
use ma_core::{
    ipfs_add, Did, DidDocumentResolver, IpfsGatewayResolver, Ipld, SigningKey, CONTENT_TYPE_TERM,
    MESSAGE_TYPE_RPC, MESSAGE_TYPE_RPC_REPLY,
};
use tracing::{debug, info, warn};

use crate::acl::{check_full, AclCache, AclMap, CAP_RPC};
use crate::entity::{
    CastInput, IpldLink, Lifecycle, LocalMessage, PluginKind, RuntimeManifest, SendEnvelope,
};
use crate::plugin::EntityRegistry;
use crate::status::SharedStats;

pub const RPC_PROTOCOL_ID: &str = "/ma/rpc/0.0.1";

// ── Handler context ────────────────────────────────────────────────────────────

pub struct RpcHandlerCtx {
    pub our_did: Arc<str>,
    pub signing_key: Arc<SigningKey>,
    pub endpoint: Arc<dyn ma_core::MaEndpoint>,
    pub kubo_rpc_url: Arc<str>,
    pub resolver: Arc<IpfsGatewayResolver>,
    pub entity_registry: EntityRegistry,
    pub kind_registry: crate::entity::KindRegistry,
    pub envelope_tx: tokio::sync::mpsc::UnboundedSender<(String, SendEnvelope)>,
    pub stats: SharedStats,
    pub acl_cache: AclCache,
    pub avatar_key: [u8; 32],
}

// ── Entity creation helper ─────────────────────────────────────────────────────

async fn persist_new_entity(
    kubo_url: &str,
    old_root_cid: &str,
    fragment: &str,
    entity_node: &crate::entity::EntityNode,
    stats: &SharedStats,
) -> Result<()> {
    let mut manifest: RuntimeManifest = crate::kubo::dag_get(kubo_url, old_root_cid).await?;
    let entity_cid = crate::kubo::dag_put(kubo_url, entity_node).await?;
    manifest
        .entities
        .insert(fragment.to_string(), IpldLink::new(&entity_cid));
    let new_root_cid = crate::kubo::dag_put(kubo_url, &manifest).await?;
    if let Err(e) = crate::kubo::pin_update(kubo_url, old_root_cid, &new_root_cid).await {
        warn!(old = %old_root_cid, new = %new_root_cid, error = %e, "persist_new_entity: pin_update failed");
    }
    stats.write().await.root_cid = Some(new_root_cid);
    Ok(())
}

// ── Entry point ────────────────────────────────────────────────────────────────

pub async fn handle_rpc_message(
    message: &ma_core::Message,
    acl: &AclMap,
    ctx: &RpcHandlerCtx,
) -> Result<()> {
    // Intra-runtime messages (sender = `<our_did>#<entity>`) skip the root ACL
    // transport gate — they are trusted local dispatches between entities on
    // this runtime.
    let intra_runtime = message.from.starts_with(&format!("{}#", ctx.our_did));
    if !intra_runtime {
        let owners = ctx.stats.read().await.owners.clone();
        if !crate::acl::is_owner(&owners, &message.from) {
            check_full(acl, &message.from, &[CAP_RPC], |_| async { Ok(vec![]) }).await?;
        }
    }

    if message.message_type != MESSAGE_TYPE_RPC {
        return Err(anyhow!(
            "unsupported RPC message type '{}' on {}",
            message.message_type,
            RPC_PROTOCOL_ID,
        ));
    }

    let term: CborValue = ciborium::de::from_reader(message.payload().as_slice())
        .context("invalid CBOR in RPC message")?;

    // Fragment routing: entity plugin dispatch.
    if let Some(fragment) = extract_fragment(&message.to, &ctx.our_did) {
        let ep = ctx.entity_registry.read().await.get(fragment).cloned();
        return if let Some(entity) = ep {
            let fragment_for_log = entity.fragment.clone();
            match handle_entity_plugin_message(message, term, entity, ctx).await {
                Ok(()) => Ok(()),
                Err(err) => {
                    let reason = err.to_string();
                    warn!(
                        fragment = %fragment_for_log,
                        from = %message.from,
                        error = %reason,
                        "plugin dispatch rejected"
                    );
                    send_rpc_error_reply(message, ctx, &reason)
                }
            }
        } else {
            let reason = format!("unknown entity fragment: {fragment}");
            info!(fragment = %fragment, "{}", crate::i18n::t("entity-not-found"));
            send_rpc_error_reply(message, ctx, &reason)
        };
    }

    // Unfragmented: only :ping.
    let ping_text = match &term {
        CborValue::Text(s) => s.as_str(),
        _ => {
            return send_rpc_i18n_error(message, ctx, "rpc-not-text-atom").await;
        }
    };
    if ping_text == ":ping" {
        info!("{}", crate::i18n::t("ping-received"));
        let mut pong = Vec::new();
        ciborium::ser::into_writer(&CborValue::Text(":pong".to_string()), &mut pong)
            .context("encode :pong")?;
        return send_rpc_reply(message, ctx, &pong);
    }
    send_rpc_i18n_error(message, ctx, "rpc-unknown-verb").await
}

// ── Fragment extraction ────────────────────────────────────────────────────────

/// Strip `<our_did>#` from `to` and return the bare fragment, if present.
fn extract_fragment<'a>(to: &'a str, our_did: &str) -> Option<&'a str> {
    let prefix = format!("{our_did}#");
    to.strip_prefix(prefix.as_str())
}

// ── Entity plugin dispatch ────────────────────────────────────────────────────

#[allow(
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss,
    clippy::too_many_lines
)]
async fn handle_entity_plugin_message(
    message: &ma_core::Message,
    term: CborValue,
    entity: Arc<crate::plugin::EntityPlugin>,
    ctx: &RpcHandlerCtx,
) -> Result<()> {
    info!(fragment = %entity.fragment, from = %message.from, "{}", crate::i18n::t("entity-dispatched"));

    // Entity verb-ACL enforcement.
    // Extract the verb from the CBOR term (text atom or first array element).
    let verb_str: Option<String> = match &term {
        CborValue::Text(s) => Some(s.clone()),
        CborValue::Array(items) => {
            if let Some(CborValue::Text(s)) = items.first() {
                Some(s.clone())
            } else {
                None
            }
        }
        _ => None,
    };

    // Empty acl field → deny-all (fail-closed). Matches EntityNode.acl doc contract.
    if entity.acl.is_empty() {
        return Err(anyhow!(
            "entity '{}' has no ACL configured: access denied (fail-closed)",
            entity.fragment
        ));
    }
    let acl_name = &entity.acl;
    let acl_key = format!("acls.{acl_name}");
    let maybe_acl = ctx.acl_cache.read().await.get(&acl_key).cloned();
    if let Some(ref acl_map) = maybe_acl {
        let verb_ref = verb_str.as_deref().unwrap_or("*");
        let registry = ctx.entity_registry.clone();
        // Pre-normalize caller: `did:ma:<our_did>#fragment` → `#fragment` so
        // that the `"#"` local-entity wildcard in ACL maps matches intra-runtime
        // callers.  This is safe because message signatures are cryptographically
        // verified — a remote peer cannot forge our DID as the sender.
        let caller_did = message
            .from
            .strip_prefix(&format!("{}#", ctx.our_did))
            .map_or_else(|| message.from.clone(), |frag| format!("#{frag}"));
        let caller_str = caller_did.clone();
        crate::acl::check_full(acl_map, &caller_did, &[verb_ref, "*"], |g| {
            let registry = registry.clone();
            let g = g.to_string();
            let caller = caller_str.clone();
            async move { crate::acl::query_actor_group(&g, &caller, &registry).await }
        })
        .await
        .with_context(|| {
            format!(
                "entity '{}' ACL denied {} calling {:?}",
                entity.fragment, message.from, verb_str
            )
        })?;
    } else {
        // ACL name set but not in cache → deny (fail-closed).
        return Err(anyhow!(
            "entity '{}' ACL '{}' not found in cache: access denied",
            entity.fragment,
            acl_name
        ));
    }

    let mut content_bytes = Vec::new();
    ciborium::ser::into_writer(&term, &mut content_bytes)
        .context("re-encoding RPC content for plugin dispatch")?;

    let local_msg = LocalMessage {
        id: message.id.clone(),
        from: message.from.clone(),
        to: message.to.clone(),
        created_at: (message.created_at * 1_000_000_000.0) as u64,
        expires: message.exp,
        reply_to: message.reply_to.clone(),
        content_type: message.content_type.clone(),
        content: content_bytes,
    };

    let cast_input = CastInput { msg: local_msg };
    let result = match entity.kind {
        PluginKind::Stateless => entity.handle_cast(&cast_input).await?,
        PluginKind::Stateful => entity.handle_call(&cast_input).await?,
    };

    // If the plugin called `ma_set_state` during this dispatch, persist to IPFS.
    // Spawned so the main event loop is not blocked by the IPFS round-trip.
    if let Some(state_bytes) = result.pending_state {
        let entity_arc = Arc::clone(&entity);
        let kubo_url = ctx.kubo_rpc_url.to_string();
        tokio::spawn(async move {
            match ipfs_add(&kubo_url, state_bytes.clone()).await {
                Ok(cid) => {
                    debug!(fragment = %entity_arc.fragment, %cid, "plugin state saved via ma_set_state");
                    entity_arc.mark_saved(state_bytes);
                }
                Err(e) => {
                    warn!(fragment = %entity_arc.fragment, error = %e, "failed to persist plugin state");
                }
            }
        });
    }

    // Process entity creation requests queued by `ma_create_entity` host function.
    for req in result.create_requests {
        let maybe_kind = ctx
            .kind_registry
            .read()
            .await
            .get(&req.kind_protocol)
            .cloned();
        let Some(kind_node) = maybe_kind else {
            warn!(caller = %entity.fragment, kind = %req.kind_protocol,
                "ma_create_entity: kind not in registry; skipped");
            continue;
        };

        let entity_node = crate::entity::EntityNode {
            kind: req.kind_protocol.clone(),
            behaviour: Some(IpldLink::new(&req.behaviour_cid)),
            acl: entity.acl.clone(),
            state: None,
            parent: Some(entity.fragment.clone()),
            label: None,
            lifecycle: Lifecycle::New,
        };

        match crate::plugin::EntityPlugin::load(
            req.fragment.clone(),
            &entity_node,
            &kind_node,
            &ctx.our_did,
            &ctx.kubo_rpc_url,
            ctx.envelope_tx.clone(),
            ctx.entity_registry.clone(),
            ctx.avatar_key,
        )
        .await
        {
            Ok((ep, Lifecycle::Running)) => {
                ctx.entity_registry
                    .write()
                    .await
                    .insert(req.fragment.clone(), Arc::new(ep));
                info!(fragment = %req.fragment, kind = %req.kind_protocol,
                    parent = %req.parent, "entity created via ma_create_entity");
                // Persist the updated manifest in the background so the main
                // event loop is not blocked by the IPFS dag_get / dag_put.
                // The entity is already live in the in-memory registry above.
                //
                // KNOWN RACE: if a single dispatch queues multiple create_requests
                // AND the spawned persist tasks run concurrently, each reads the
                // same old_cid as base and the last writer wins for stats.root_cid,
                // causing all but the last entity to be absent from the IPFS
                // manifest after a crash-restart.  In-memory state is always
                // correct; only crash recovery is affected.
                //
                // If this manifests (missing entity after restart): run
                // `cargo run -- --bootstrap bootstrap.example.yaml` to rebuild
                // the manifest from scratch, or manually re-send the
                // `:entities.<fragment>: <cid>` CRUD command for each missing
                // entity.  A proper fix would serialise all manifest writes
                // through a dedicated background task with an mpsc channel.
                let root_cid = ctx.stats.read().await.root_cid.clone();
                if let Some(old_cid) = root_cid {
                    let mut running_node = entity_node.clone();
                    running_node.lifecycle = Lifecycle::Running;
                    let kubo_url = ctx.kubo_rpc_url.to_string();
                    let fragment = req.fragment.clone();
                    let stats = ctx.stats.clone();
                    tokio::spawn(async move {
                        if let Err(e) = persist_new_entity(
                            &kubo_url,
                            &old_cid,
                            &fragment,
                            &running_node,
                            &stats,
                        )
                        .await
                        {
                            warn!(fragment = %fragment, error = %e, "failed to persist new entity to manifest");
                        }
                    });
                }
            }
            Ok((_, Lifecycle::Error)) => {
                // New entity — init() failed: send error back to parent plugin,
                // do NOT persist to manifest.
                warn!(fragment = %req.fragment, kind = %req.kind_protocol,
                    "ma_create_entity: init() returned :error; entity discarded");
                // Queue error reply into parent's outbox via envelope_tx.
                let err_content = {
                    let mut buf = Vec::new();
                    let _ = ciborium::ser::into_writer(
                        &CborValue::Array(vec![
                            CborValue::Text(":error".into()),
                            CborValue::Text(format!("init() failed for #{}", req.fragment)),
                            CborValue::Text(req.fragment.clone()),
                        ]),
                        &mut buf,
                    );
                    buf
                };
                let _ = ctx.envelope_tx.send((
                    req.parent.clone(),
                    crate::entity::SendEnvelope {
                        to: format!("{}#{}", ctx.our_did, req.parent),
                        content_type: ma_core::CONTENT_TYPE_TERM.to_string(),
                        content: err_content,
                        reply_to: None,
                    },
                ));
            }
            Ok((_, lc)) => {
                warn!(fragment = %req.fragment, lifecycle = %lc,
                    "ma_create_entity: unexpected lifecycle after load");
            }
            Err(e) => {
                warn!(fragment = %req.fragment, kind = %req.kind_protocol,
                    error = %e, "ma_create_entity: EntityPlugin::load failed");
            }
        }
    }

    // Process entity deletion requests queued by `ma_delete_entity` host function.
    for target_fragment in result.delete_requests {
        let reg_read = ctx.entity_registry.read().await;
        let target = if let Some(e) = reg_read.get(&target_fragment) {
            e.clone()
        } else {
            warn!(caller = %entity.fragment, target = %target_fragment,
                "ma_delete_entity: target not found; skipped");
            continue;
        };

        // Authorization: caller must be the target's parent (or self-delete).
        let authorized = target.parent.as_deref() == Some(entity.fragment.as_str())
            || target_fragment == entity.fragment;
        if !authorized {
            warn!(caller = %entity.fragment, target = %target_fragment,
                "ma_delete_entity: caller is not parent; denied");
            continue;
        }

        // Refuse to delete if any entity in the registry has this as its parent.
        let has_children = reg_read
            .values()
            .any(|e| e.parent.as_deref() == Some(target_fragment.as_str()));
        if has_children {
            warn!(caller = %entity.fragment, target = %target_fragment,
                "ma_delete_entity: target has children; denied");
            continue;
        }
        drop(reg_read);

        ctx.entity_registry.write().await.remove(&target_fragment);
        info!(caller = %entity.fragment, target = %target_fragment, "entity deleted via ma_delete_entity");
    }

    Ok(())
}

// ── Generic reply helper ───────────────────────────────────────────────────────

fn send_rpc_reply(incoming: &ma_core::Message, ctx: &RpcHandlerCtx, content: &[u8]) -> Result<()> {
    send_rpc_reply_typed(incoming, ctx, CONTENT_TYPE_TERM, content)
}

fn send_rpc_reply_typed(
    incoming: &ma_core::Message,
    ctx: &RpcHandlerCtx,
    content_type: &str,
    content: &[u8],
) -> Result<()> {
    let sender = Did::try_from(incoming.from.as_str())
        .with_context(|| format!("invalid sender DID: {}", incoming.from))?;

    let mut reply = ma_core::Message::new(
        ctx.our_did.as_ref(),
        &incoming.from,
        MESSAGE_TYPE_RPC_REPLY,
        content_type,
        content,
        &ctx.signing_key,
    )
    .context("failed to build RPC reply")?;
    reply.reply_to = Some(incoming.id.clone());

    // Spawn the delivery so the event loop is never blocked by DID resolution
    // or QUIC connection setup. The reply is fire-and-forget from the handler's
    // perspective; failures are logged but do not affect the caller.
    let endpoint = Arc::clone(&ctx.endpoint);
    let resolver = Arc::clone(&ctx.resolver);
    let from = incoming.from.clone();
    let msg_id = incoming.id.clone();
    tokio::spawn(async move {
        match endpoint
            .outbox(resolver.as_ref(), &sender.base_id(), RPC_PROTOCOL_ID)
            .await
        {
            Ok(mut outbox) => {
                if let Err(err) = outbox.send(&reply).await {
                    warn!(error = %err, to = %from, "RPC reply send failed");
                } else {
                    info!(to = %from, reply_to = %msg_id, "{}", crate::i18n::t("rpc-reply-sent"));
                }
            }
            Err(err) => {
                warn!(error = %err, to = %from, "RPC reply delivery failed");
            }
        }
    });
    Ok(())
}

/// Resolve the caller's preferred language from their DID document.
/// Falls back to the runtime's own language on any error.
async fn rpc_caller_lang(from: &str, ctx: &RpcHandlerCtx) -> String {
    if let Ok(doc) = ctx.resolver.resolve(from).await {
        if let Some(Ipld::Map(ma)) = &doc.ma {
            if let Some(Ipld::String(lang)) = ma.get("lang") {
                if crate::i18n::has_lang(lang) {
                    return lang.clone();
                }
            }
        }
    }
    crate::i18n::runtime_lang()
}

/// Send an RPC error reply with the message localised to the caller's language.
async fn send_rpc_i18n_error(
    incoming: &ma_core::Message,
    ctx: &RpcHandlerCtx,
    key: &str,
) -> Result<()> {
    let lang = rpc_caller_lang(&incoming.from, ctx).await;
    send_rpc_error_reply(incoming, ctx, &crate::i18n::t_lang(&lang, key))
}

fn send_rpc_error_reply(
    incoming: &ma_core::Message,
    ctx: &RpcHandlerCtx,
    reason: &str,
) -> Result<()> {
    let mut payload = Vec::new();
    ciborium::ser::into_writer(
        &CborValue::Array(vec![
            CborValue::Text(":error".to_string()),
            CborValue::Text(reason.to_string()),
        ]),
        &mut payload,
    )
    .context("failed to encode RPC error reply")?;
    send_rpc_reply(incoming, ctx, &payload)
}
