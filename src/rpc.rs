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
use crate::entity::{CastInput, IpldLink, Lifecycle, LocalMessage, PluginKind, RuntimeManifest, SendEnvelope};
use crate::plugin::EntityRegistry;
use crate::schedule::{parse_duration, register_schedule, ScheduleRequest, SchedulerCtx};
use crate::status::SharedStats;

pub const RPC_PROTOCOL_ID: &str = "/ma/rpc/0.0.1";

// ── Handler context ────────────────────────────────────────────────────────────

pub struct RpcHandlerCtx<'a> {
    pub our_did: &'a str,
    pub signing_key: &'a SigningKey,
    pub endpoint: &'a dyn ma_core::MaEndpoint,
    pub kubo_rpc_url: &'a str,
    pub resolver: Arc<IpfsGatewayResolver>,
    pub entity_registry: EntityRegistry,
    pub kind_registry: crate::entity::KindRegistry,
    pub envelope_tx: tokio::sync::mpsc::UnboundedSender<(String, SendEnvelope)>,
    pub stats: SharedStats,
    pub acl_cache: AclCache,
    pub scheduler: Arc<tokio_cron_scheduler::JobScheduler>,
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
    manifest.entities.insert(fragment.to_string(), IpldLink::new(&entity_cid));
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
    ctx: &RpcHandlerCtx<'_>,
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
    if let Some(fragment) = extract_fragment(&message.to, ctx.our_did) {
        // System actor: #scheduler handles schedule-registration requests.
        if fragment == "scheduler" {
            return match handle_scheduler_message(term, message, ctx) {
                Ok(()) => Ok(()),
                Err(err) => {
                    let reason = err.to_string();
                    warn!(from = %message.from, error = %reason, "#scheduler: bad request");
                    send_rpc_error_reply(message, ctx, &reason).await
                }
            };
        }
        let ep = ctx.entity_registry.read().await.get(fragment).cloned();
        return if let Some(entity) = ep {
            match handle_entity_plugin_message(message, term, &entity, ctx).await {
                Ok(()) => Ok(()),
                Err(err) => {
                    let reason = err.to_string();
                    warn!(
                        fragment = %entity.fragment,
                        from = %message.from,
                        error = %reason,
                        "plugin dispatch rejected"
                    );
                    send_rpc_error_reply(message, ctx, &reason).await
                }
            }
        } else {
            let reason = format!("unknown entity fragment: {fragment}");
            info!(fragment = %fragment, "{}", crate::i18n::t("entity-not-found"));
            send_rpc_error_reply(message, ctx, &reason).await
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
        return send_rpc_reply(message, ctx, pong).await;
    }
    send_rpc_i18n_error(message, ctx, "rpc-unknown-verb").await
}

// ── Fragment extraction ────────────────────────────────────────────────────────

/// Strip `<our_did>#` from `to` and return the bare fragment, if present.
fn extract_fragment<'a>(to: &'a str, our_did: &str) -> Option<&'a str> {
    let prefix = format!("{our_did}#");
    to.strip_prefix(prefix.as_str())
}

// ── #scheduler system actor ───────────────────────────────────────────────────

/// Handle a message addressed to `#scheduler`.
///
/// Plugins send to `did:ma:<ipns>#scheduler` via `ma_send` to register
/// scheduled dispatches without needing schedule host functions.
///
/// Message content is a CBOR array:
/// ```text
/// [":cron",     spec_str,       target_frag, verb_or_array]
/// [":interval", duration_str,   target_frag, verb_or_array]
/// [":at",       timestamp_ms,   target_frag, verb_or_array]
/// [":random",   max_secs_int,   target_frag, verb_or_array]
/// ```
/// `verb_or_array` is either a text atom `":verb"` or an array `[":verb", arg1, …]`.
/// `target_frag` is the bare fragment name (e.g. `"fortune"`) or a full DID-URL.
fn handle_scheduler_message(
    term: CborValue,
    message: &ma_core::Message,
    ctx: &RpcHandlerCtx<'_>,
) -> Result<()> {
    let items = match term {
        CborValue::Array(a) => a,
        other => return Err(anyhow!("scheduler: expected CBOR array, got {other:?}")),
    };
    if items.len() < 4 {
        return Err(anyhow!(
            "scheduler: expected [type, spec, target, verb], got {} elements",
            items.len()
        ));
    }
    let type_verb = match &items[0] {
        CborValue::Text(s) => s.clone(),
        _ => return Err(anyhow!("scheduler: first element must be text atom")),
    };
    let target = match &items[2] {
        CborValue::Text(s) => s.clone(),
        _ => return Err(anyhow!("scheduler: third element must be target fragment")),
    };
    // Accept bare fragment or full DID-URL `did:ma:<ipns>#fragment`.
    let fragment = if let Some(frag) = target.strip_prefix(&format!("{}#", ctx.our_did)) {
        frag.to_string()
    } else if let Some(pos) = target.find('#') {
        target[pos + 1..].to_string()
    } else {
        target.clone()
    };

    // Encode verb (4th element) + optional inline args (5th+) as CBOR call bytes.
    let content = {
        let args = items.get(4..).unwrap_or(&[]);
        match &items[3] {
            CborValue::Text(v) if args.is_empty() => {
                let mut out = Vec::new();
                ciborium::ser::into_writer(&CborValue::Text(v.clone()), &mut out).ok();
                out
            }
            CborValue::Text(v) => {
                let mut arr = vec![CborValue::Text(v.clone())];
                arr.extend_from_slice(args);
                let mut out = Vec::new();
                ciborium::ser::into_writer(&CborValue::Array(arr), &mut out).ok();
                out
            }
            arr @ CborValue::Array(_) => {
                let mut out = Vec::new();
                ciborium::ser::into_writer(arr, &mut out).ok();
                out
            }
            _ => return Err(anyhow!("scheduler: verb (4th element) must be text atom or array")),
        }
    };

    let req = match type_verb.as_str() {
        ":cron" => {
            let spec = match &items[1] {
                CborValue::Text(s) => s.clone(),
                _ => return Err(anyhow!("scheduler :cron: spec must be text")),
            };
            ScheduleRequest::Cron { spec, content }
        }
        ":interval" => {
            let dur_str = match &items[1] {
                CborValue::Text(s) => s.clone(),
                _ => return Err(anyhow!("scheduler :interval: duration must be text")),
            };
            let secs = parse_duration(&dur_str)
                .map_err(|e| anyhow!("scheduler :interval: {e}"))?
                .as_secs();
            ScheduleRequest::Interval { secs, content }
        }
        ":at" => {
            let ts = match &items[1] {
                CborValue::Integer(n) => i64::try_from(i128::from(*n))
                    .map_err(|_| anyhow!("scheduler :at: timestamp out of i64 range"))?,
                _ => return Err(anyhow!("scheduler :at: timestamp must be integer")),
            };
            ScheduleRequest::At { timestamp_ms: ts, content }
        }
        ":random" => {
            let max_secs = match &items[1] {
                CborValue::Integer(n) => u64::try_from(i128::from(*n)).unwrap_or(60),
                _ => return Err(anyhow!("scheduler :random: max_secs must be integer")),
            };
            ScheduleRequest::Random { max_secs, content }
        }
        other => return Err(anyhow!("scheduler: unknown schedule type '{other}'")),
    };

    let sched_ctx = SchedulerCtx {
        entity_registry: ctx.entity_registry.clone(),
        kubo_rpc_url: ctx.kubo_rpc_url.to_string(),
        our_did: ctx.our_did.to_string(),
    };
    let sched = Arc::clone(&ctx.scheduler);
    let from_str = message.from.clone();
    tokio::spawn(async move {
        if let Err(e) = register_schedule(&sched, sched_ctx, fragment.clone(), None, req).await {
            warn!(target = %fragment, from = %from_str, error = %e, "scheduler: failed to register schedule");
        }
    });
    Ok(())
}

// ── Wasm entity dispatch ───────────────────────────────────────────────────────

#[allow(
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss,
    clippy::too_many_lines
)]
async fn handle_entity_plugin_message(
    message: &ma_core::Message,
    term: CborValue,
    entity: &crate::plugin::EntityPlugin,
    ctx: &RpcHandlerCtx<'_>,
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
        let caller_str = message.from.clone();
        crate::acl::check_full(acl_map, &message.from, &[verb_ref, "*"], |g| {
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

    let cast_input = CastInput {
        msg: local_msg,
    };
    let result = match entity.kind {
        PluginKind::Stateless => entity.handle_cast(&cast_input)?,
        PluginKind::Stateful => entity.handle_call(&cast_input)?,
    };

    // If the plugin called `ma_set_state` during this dispatch, persist to IPFS.
    if let Some(state_bytes) = result.pending_state {
        match ipfs_add(ctx.kubo_rpc_url, state_bytes.clone()).await {
            Ok(cid) => {
                debug!(fragment = %entity.fragment, %cid, "plugin state saved via ma_set_state");
                entity.mark_saved(state_bytes);
            }
            Err(e) => {
                warn!(fragment = %entity.fragment, error = %e, "failed to persist plugin state");
            }
        }
    }

    // Process entity creation requests queued by `ma_create_entity` host function.
    for req in result.create_requests {
        let maybe_kind = ctx.kind_registry.read().await.get(&req.kind_protocol).cloned();
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
            ctx.our_did,
            ctx.kubo_rpc_url,
            ctx.envelope_tx.clone(),
        )
        .await
        {
            Ok((ep, Lifecycle::Running)) => {
                ctx.entity_registry
                    .write()
                    .await
                    .insert(req.fragment.clone(), Arc::new(ep));
                if let Some(ref old_cid) = ctx.stats.read().await.root_cid.clone() {
                    let mut running_node = entity_node.clone();
                    running_node.lifecycle = Lifecycle::Running;
                    if let Err(e) = persist_new_entity(
                        ctx.kubo_rpc_url, old_cid, &req.fragment, &running_node, &ctx.stats,
                    ).await {
                        warn!(fragment = %req.fragment, error = %e, "failed to persist new entity to manifest");
                    }
                }
                info!(fragment = %req.fragment, kind = %req.kind_protocol,
                    parent = %req.parent, "entity created via ma_create_entity");
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
        let target = if let Some(e) = reg_read.get(&target_fragment) { e.clone() } else {
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

async fn send_rpc_reply(
    incoming: &ma_core::Message,
    ctx: &RpcHandlerCtx<'_>,
    content: Vec<u8>,
) -> Result<()> {
    send_rpc_reply_typed(incoming, ctx, CONTENT_TYPE_TERM, &content).await
}

async fn send_rpc_reply_typed(
    incoming: &ma_core::Message,
    ctx: &RpcHandlerCtx<'_>,
    content_type: &str,
    content: &[u8],
) -> Result<()> {
    let sender = Did::try_from(incoming.from.as_str())
        .with_context(|| format!("invalid sender DID: {}", incoming.from))?;

    let mut reply = ma_core::Message::new(
        ctx.our_did,
        &incoming.from,
        MESSAGE_TYPE_RPC_REPLY,
        content_type,
        content,
        ctx.signing_key,
    )
    .context("failed to build RPC reply")?;
    reply.reply_to = Some(incoming.id.clone());

    match ctx
        .endpoint
        .outbox(ctx.resolver.as_ref(), &sender.base_id(), RPC_PROTOCOL_ID)
        .await
    {
        Ok(mut outbox) => {
            outbox.send(&reply).await.context("RPC reply send failed")?;
            info!(
                to = %incoming.from,
                reply_to = %incoming.id,
                "{}",
                crate::i18n::t("rpc-reply-sent")
            );
        }
        Err(err) => {
            warn!(error = %err, to = %incoming.from, "RPC reply delivery failed");
        }
    }
    Ok(())
}

/// Resolve the caller's preferred language from their DID document.
/// Falls back to the runtime's own language on any error.
async fn rpc_caller_lang(from: &str, ctx: &RpcHandlerCtx<'_>) -> String {
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
    ctx: &RpcHandlerCtx<'_>,
    key: &str,
) -> Result<()> {
    let lang = rpc_caller_lang(&incoming.from, ctx).await;
    send_rpc_error_reply(incoming, ctx, &crate::i18n::t_lang(&lang, key)).await
}

async fn send_rpc_error_reply(
    incoming: &ma_core::Message,
    ctx: &RpcHandlerCtx<'_>,
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
    send_rpc_reply(incoming, ctx, payload).await
}
