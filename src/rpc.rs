//! `/ma/rpc/0.0.1` handler: entity plugin dispatch and `:ping`.

use std::sync::Arc;

use anyhow::{anyhow, Context, Result};
use ciborium::Value as CborValue;
use ma_core::{
    ipfs_add, Did, DidDocumentResolver, IpfsGatewayResolver, Ipld, SigningKey, CONTENT_TYPE_TERM,
    MESSAGE_TYPE_RPC, MESSAGE_TYPE_RPC_REPLY,
};
use tracing::{debug, info, warn};

use crate::acl::{check_full, AclCache, AclMap, GroupCache, CAP_RPC};
use crate::entity::{
    CastInput, IpldLink, Lifecycle, LocalMessage, PluginMsg, SendEnvelope, SetBehaviourRequest,
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
    pub group_cache: GroupCache,
    pub avatar_key: [u8; 32],
    pub manifest_writer: crate::manifest::ManifestWriter,
    pub shared_config: Arc<tokio::sync::RwLock<ma_core::Config>>,
}

// ── Entity creation helper ─────────────────────────────────────────────────────

async fn persist_new_entity(
    manifest_writer: &crate::manifest::ManifestWriter,
    kubo_url: &str,
    fragment: &str,
    entity_node: &crate::entity::EntityNode,
) -> Result<()> {
    let entity_cid = crate::kubo::dag_put(kubo_url, entity_node).await?;
    let fragment = fragment.to_string();
    manifest_writer
        .mutate(move |m| {
            m.entities.insert(fragment, IpldLink::new(&entity_cid));
            Ok(())
        })
        .await?;
    Ok(())
}

async fn public_plugin_config_for_rpc(
    ctx: &RpcHandlerCtx,
) -> Result<std::collections::BTreeMap<String, String>> {
    let root_cid = ctx
        .stats
        .read()
        .await
        .root_cid
        .clone()
        .ok_or_else(|| anyhow!("no manifest root CID available"))?;
    let manifest: crate::entity::RuntimeManifest =
        crate::kubo::dag_get(&ctx.kubo_rpc_url, &root_cid).await?;
    let cfg = ctx.shared_config.read().await;
    Ok(crate::crud::config::public_plugin_config(&manifest, &cfg))
}

fn normalize_behaviour_cid(value: &str) -> Result<String> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return Err(anyhow!("behaviour reference is empty"));
    }
    if let Some(cid) = trimmed.strip_prefix("/ipfs/") {
        if cid.is_empty() {
            return Err(anyhow!("/ipfs/ behaviour reference is missing a CID"));
        }
        return Ok(cid.to_string());
    }
    if trimmed.starts_with("/ipns/") {
        return Err(anyhow!("/ipns/ behaviour references are not supported here; publish the code to /ipfs/<cid> first"));
    }
    Ok(trimmed.to_string())
}

async fn load_entity_node_for_update(
    ctx: &RpcHandlerCtx,
    fragment: &str,
    behaviour_cid: Option<&str>,
) -> Result<crate::entity::EntityNode> {
    let root_cid = ctx
        .stats
        .read()
        .await
        .root_cid
        .clone()
        .ok_or_else(|| anyhow!("no manifest root CID available"))?;
    let manifest: crate::entity::RuntimeManifest =
        crate::kubo::dag_get(&ctx.kubo_rpc_url, &root_cid).await?;
    let link = manifest
        .entities
        .get(fragment)
        .ok_or_else(|| anyhow!("entity '{fragment}' is not in the manifest"))?;
    let mut node: crate::entity::EntityNode =
        crate::kubo::dag_get(&ctx.kubo_rpc_url, &link.cid).await?;
    node.behaviour = behaviour_cid.map(IpldLink::new);
    Ok(node)
}

async fn apply_behaviour_request(req: SetBehaviourRequest, ctx: &RpcHandlerCtx) -> Result<()> {
    let behaviour_cid = req
        .behaviour_cid
        .as_deref()
        .map(normalize_behaviour_cid)
        .transpose()?;
    let updated_node =
        load_entity_node_for_update(ctx, &req.fragment, behaviour_cid.as_deref()).await?;
    let kind_node = ctx
        .kind_registry
        .read()
        .await
        .get(&updated_node.kind)
        .cloned()
        .ok_or_else(|| anyhow!("kind '{}' is not in registry", updated_node.kind))?;
    let (iroh_node_id, started_at) = {
        let s = ctx.stats.read().await;
        (s.endpoint_id.clone(), s.started_at)
    };
    let runtime_config = public_plugin_config_for_rpc(ctx).await.unwrap_or_else(|e| {
        warn!(error = %e, "ma_set_behaviour: failed to build public plugin config; continuing with entity-local config only");
        std::collections::BTreeMap::new()
    });
    let (plugin, lifecycle) = crate::plugin::EntityPlugin::load(
        req.fragment.clone(),
        &updated_node,
        &kind_node,
        &ctx.our_did,
        &ctx.kubo_rpc_url,
        ctx.envelope_tx.clone(),
        ctx.entity_registry.clone(),
        ctx.avatar_key,
        &iroh_node_id,
        started_at,
        runtime_config,
        None,
    )
    .await?;
    let plugin = Arc::new(plugin);
    let state_bytes = plugin.trigger_save(&ctx.kubo_rpc_url).await.ok().flatten();

    let root_cid = ctx
        .manifest_writer
        .set_entity_behaviour(&req.fragment, behaviour_cid.as_deref())
        .await?;
    if let Some(state_cid) = state_bytes {
        if let Err(e) = ctx
            .manifest_writer
            .set_entity_state(&req.fragment, &state_cid)
            .await
        {
            warn!(fragment = %req.fragment, cid = %state_cid, error = %e, "ma_set_behaviour: failed to persist reloaded entity state");
        }
    }
    ctx.entity_registry
        .write()
        .await
        .insert(req.fragment.clone(), plugin);
    info!(fragment = %req.fragment, ?lifecycle, %root_cid, "entity behaviour updated via ma_set_behaviour");
    Ok(())
}

// ── Entry point ────────────────────────────────────────────────────────────────

pub async fn handle_rpc_message(
    message: &ma_core::Message,
    acl: &AclMap,
    ctx: &RpcHandlerCtx,
) -> Result<()> {
    if rpc_message_kind(&message.message_type) == RpcMessageKind::Reply {
        debug!(
            from = %message.from,
            to = %message.to,
            reply_to = ?message.reply_to,
            "RPC reply ignored: no runtime reply waiter"
        );
        return Ok(());
    }

    // Intra-runtime messages (sender = `<our_did>#<entity>`) skip the root ACL
    // transport gate — they are trusted local dispatches between entities on
    // this runtime.
    let intra_runtime = message.from.starts_with(&format!("{}#", ctx.our_did));
    if !intra_runtime {
        let owners = ctx.stats.read().await.owners.clone();
        if !crate::acl::is_owner(&owners, &message.from) {
            let group_cache = ctx.group_cache.clone();
            check_full(acl, &message.from, &[CAP_RPC], |key| {
                let group_cache = group_cache.clone();
                let name = key.strip_prefix('+').unwrap_or(key).to_string();
                async move {
                    Ok(group_cache
                        .read()
                        .await
                        .get(&name)
                        .cloned()
                        .unwrap_or_default())
                }
            })
            .await?;
        }
    }

    if rpc_message_kind(&message.message_type) != RpcMessageKind::Request {
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
            debug!(fragment = %fragment, "{}", crate::i18n::t("entity-not-found"));
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
        debug!("{}", crate::i18n::t("ping-received"));
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

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum RpcMessageKind {
    Request,
    Reply,
    Unsupported,
}

fn rpc_message_kind(message_type: &str) -> RpcMessageKind {
    match message_type {
        MESSAGE_TYPE_RPC => RpcMessageKind::Request,
        MESSAGE_TYPE_RPC_REPLY => RpcMessageKind::Reply,
        _ => RpcMessageKind::Unsupported,
    }
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
    debug!(fragment = %entity.fragment, from = %message.from, "{}", crate::i18n::t("entity-dispatched"));

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
        let group_cache = ctx.group_cache.clone();
        // Pre-normalize caller: `did:ma:<our_did>#fragment` → `#fragment` so
        // that the `"#"` local-entity wildcard in ACL maps matches intra-runtime
        // callers.  This is safe because message signatures are cryptographically
        // verified — a remote peer cannot forge our DID as the sender.
        let caller_did = message
            .from
            .strip_prefix(&format!("{}#", ctx.our_did))
            .map_or_else(|| message.from.clone(), |frag| format!("#{frag}"));
        crate::acl::check_full(acl_map, &caller_did, &[verb_ref, "*"], |key| {
            let group_cache = group_cache.clone();
            let name = key.strip_prefix('+').unwrap_or(key).to_string();
            async move {
                Ok(group_cache
                    .read()
                    .await
                    .get(&name)
                    .cloned()
                    .unwrap_or_default())
            }
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
        created_at: message.created_at,
        exp: message.exp,
        reply_to: message.reply_to.clone(),
        message_type: message.message_type.clone(),
        content_type: message.content_type.clone(),
        content: content_bytes,
    };

    let cast_input = CastInput {
        msg: PluginMsg::from(&local_msg),
    };
    // The underlying plugin error (`e`) can contain arbitrary internal detail —
    // for Wasm/Python plugins this includes a full interpreter traceback with
    // absolute build-machine file paths (venv layout, username, etc). That is
    // never safe to hand to a remote RPC caller. Log the full detail locally
    // and propagate only a sanitized, fragment-scoped reason on the wire.
    let result = entity.on_message(&cast_input).await.map_err(|e| {
        warn!(
            fragment = %entity.fragment,
            from = %message.from,
            error = %e,
            "plugin execution failed"
        );
        anyhow!("entity '{}' plugin execution failed", entity.fragment)
    })?;

    // If the plugin called `ma_set_state` during this dispatch, persist to IPFS.
    // Spawned so the main event loop is not blocked by the IPFS round-trip.
    if let Some(state_bytes) = result.pending_state {
        let entity_arc = Arc::clone(&entity);
        let kubo_url = ctx.kubo_rpc_url.to_string();
        let writer = ctx.manifest_writer.clone();
        tokio::spawn(async move {
            match ipfs_add(&kubo_url, state_bytes.clone()).await {
                Ok(cid) => match writer.set_entity_state(&entity_arc.fragment, &cid).await {
                    Ok(root_cid) => {
                        debug!(fragment = %entity_arc.fragment, %cid, %root_cid, "plugin state saved via ma_set_state");
                        entity_arc.mark_saved(state_bytes);
                    }
                    Err(e) => {
                        warn!(fragment = %entity_arc.fragment, cid = %cid, error = %e, "failed to update manifest with plugin state");
                    }
                },
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
            behaviour: req
                .behaviour_cid
                .as_deref()
                .map(normalize_behaviour_cid)
                .transpose()?
                .as_deref()
                .map(IpldLink::new),
            acl: entity.acl.clone(),
            state: None,
            parent: Some(entity.fragment.clone()),
            label: None,
            attributes: std::collections::BTreeMap::new(),
            init: None,
            initialized: false,
        };

        let (iroh_node_id, started_at) = {
            let s = ctx.stats.read().await;
            (s.endpoint_id.clone(), s.started_at)
        };
        let runtime_config = public_plugin_config_for_rpc(ctx).await.unwrap_or_else(|e| {
            warn!(error = %e, "ma_create_entity: failed to build public plugin config; continuing with entity-local config only");
            std::collections::BTreeMap::new()
        });

        match crate::plugin::EntityPlugin::load(
            req.fragment.clone(),
            &entity_node,
            &kind_node,
            &ctx.our_did,
            &ctx.kubo_rpc_url,
            ctx.envelope_tx.clone(),
            ctx.entity_registry.clone(),
            ctx.avatar_key,
            &iroh_node_id,
            started_at,
            runtime_config,
            req.init_payload.clone(),
        )
        .await
        {
            Ok((ep, Lifecycle::Running)) => {
                let mut running_node = entity_node.clone();
                running_node.initialized = true;
                if let Ok(Some(cid)) = ep.trigger_save(&ctx.kubo_rpc_url).await {
                    running_node.state = Some(IpldLink::new(cid));
                }
                ctx.entity_registry
                    .write()
                    .await
                    .insert(req.fragment.clone(), Arc::new(ep));
                info!(fragment = %req.fragment, kind = %req.kind_protocol,
                    parent = %req.parent, "entity created via ma_create_entity");
                // Persist the updated manifest in the background so the main
                // event loop is not blocked by the IPFS dag_put.  The entity is
                // already live in the in-memory registry above.  The manifest
                // writer serialises this against all other manifest mutations,
                // so concurrent creates can no longer clobber each other.
                let kubo_url = ctx.kubo_rpc_url.to_string();
                let fragment = req.fragment.clone();
                let writer = ctx.manifest_writer.clone();
                tokio::spawn(async move {
                    if let Err(e) =
                        persist_new_entity(&writer, &kubo_url, &fragment, &running_node).await
                    {
                        warn!(fragment = %fragment, error = %e, "failed to persist new entity to manifest");
                    }
                });
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
                        message_type: None,
                        content: err_content,
                        reply_to: None,
                    },
                ));
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

    for req in result.behaviour_requests {
        apply_behaviour_request(req, ctx).await?;
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
                    debug!(to = %from, reply_to = %msg_id, "{}", crate::i18n::t("rpc-reply-sent"));
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

#[cfg(test)]
mod tests {
    use ma_core::{MESSAGE_TYPE_RPC, MESSAGE_TYPE_RPC_REPLY};

    use super::{extract_fragment, rpc_message_kind, RpcMessageKind};

    #[test]
    fn strips_matching_did_prefix() {
        assert_eq!(
            extract_fragment("did:ma:abc#rms", "did:ma:abc"),
            Some("rms")
        );
    }

    #[test]
    fn none_for_different_did() {
        assert_eq!(extract_fragment("did:ma:xyz#rms", "did:ma:abc"), None);
    }

    #[test]
    fn none_without_fragment() {
        assert_eq!(extract_fragment("did:ma:abc", "did:ma:abc"), None);
    }

    #[test]
    fn rpc_reply_is_classified_separately_from_requests() {
        assert_eq!(rpc_message_kind(MESSAGE_TYPE_RPC), RpcMessageKind::Request);
        assert_eq!(
            rpc_message_kind(MESSAGE_TYPE_RPC_REPLY),
            RpcMessageKind::Reply
        );
        assert_eq!(rpc_message_kind("text/plain"), RpcMessageKind::Unsupported);
    }
}
