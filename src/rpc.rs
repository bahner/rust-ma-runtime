//! `/ma/rpc/0.0.1` handler: `#root` entity management, Wasm entity dispatch,
//! and ping dispatch.

use std::sync::Arc;

use anyhow::{anyhow, Context, Result};
use ciborium::Value as CborValue;
use ma_core::{ipfs_add, Acl, Did, IpfsGatewayResolver, SigningKey, MESSAGE_TYPE_RPC, MESSAGE_TYPE_RPC_REPLY};
use tracing::{debug, info, warn};

use crate::acl::acl_check;
use crate::entity::{
    CastInput, EntityNode, IpldLink, KindNode, LocalMessage, PluginCtx, PluginKind,
    RuntimeManifest, SendEnvelope,
};
use crate::plugin::{EntityRegistry, RootPlugin};
use crate::status::SharedStats;

pub const RPC_PROTOCOL_ID: &str = "/ma/rpc/0.0.1";
const PING_ATOM: &str = ":ping";

// ── Handler context ────────────────────────────────────────────────────────────

pub struct RpcHandlerCtx<'a> {
    pub our_did: &'a str,
    pub signing_key: &'a SigningKey,
    pub endpoint: &'a dyn ma_core::MaEndpoint,
    pub kubo_rpc_url: &'a str,
    pub entity_registry: EntityRegistry,
    pub stats: SharedStats,
    /// Loaded `/ma/root/0.0.1` plugin, if available.
    /// When `Some`, `#root` requests are dispatched through the plugin.
    /// When `None`, the runtime falls back to built-in hardcoded handlers.
    pub root_plugin: Option<Arc<RootPlugin>>,
}

// ── Entry point ────────────────────────────────────────────────────────────────

pub async fn handle_rpc_message(
    message: &ma_core::Message,
    acl: &Acl,
    ctx: &RpcHandlerCtx<'_>,
) -> Result<()> {
    acl_check(acl, &message.from)?;

    if message.message_type != MESSAGE_TYPE_RPC {
        return Err(anyhow!(
            "unsupported RPC message type '{}' on {}",
            message.message_type,
            RPC_PROTOCOL_ID,
        ));
    }

    let term: CborValue = ciborium::de::from_reader(message.content.as_slice())
        .context("invalid CBOR in RPC message")?;

    // Fragment routing: messages addressed to `did:ma:<ipns>#fragment`.
    if let Some(fragment) = extract_fragment(&message.to, ctx.our_did) {
        if fragment == "root" {
            return handle_root_entity(message, term, ctx).await;
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

    // Unfragmented: route to the `ping` entity plugin if loaded, else built-in.
    if !matches!(&term, CborValue::Text(s) if s == PING_ATOM) {
        debug!(from = %message.from, atom = ?term, "{}", crate::i18n::t("unknown-rpc-atom"));
        return Ok(());
    }

    {
        let mut s = ctx.stats.write().await;
        s.pings_received += 1;
    }

    let ping_ep = ctx.entity_registry.read().await.get("ping").cloned();
    if let Some(entity) = ping_ep {
        info!(from = %message.from, "{}", crate::i18n::t("ping-received"));
        return match handle_entity_plugin_message(message, term, &entity, ctx).await {
            Ok(()) => Ok(()),
            Err(err) => {
                let reason = err.to_string();
                warn!(from = %message.from, error = %reason, "ping plugin dispatch failed");
                send_rpc_error_reply(message, ctx, &reason).await
            }
        };
    }

    // Fallback: no ping entity registered — log and ignore.
    warn!(from = %message.from, "ping received but no ping entity is loaded — ignoring");
    Ok(())
}

// ── Fragment extraction ────────────────────────────────────────────────────────

/// Strip `<our_did>#` from `to` and return the bare fragment, if present.
fn extract_fragment<'a>(to: &'a str, our_did: &str) -> Option<&'a str> {
    let prefix = format!("{our_did}#");
    to.strip_prefix(prefix.as_str())
}

// ── CBOR verb parsing ──────────────────────────────────────────────────────────

/// Decode `CborValue::Text(":verb")` or `CborValue::Array([":verb", args…])`
/// into `(verb, args)`.
fn parse_cbor_verb(term: CborValue) -> Result<(String, Vec<String>)> {
    Ok(match term {
        CborValue::Text(s) => (s, vec![]),
        CborValue::Array(items) => {
            let mut it = items.into_iter();
            let Some(CborValue::Text(verb)) = it.next() else {
                return Err(anyhow!("RPC array must start with a text verb atom"));
            };
            let args: Vec<String> = it
                .filter_map(|v| if let CborValue::Text(s) = v { Some(s) } else { None })
                .collect();
            (verb, args)
        }
        _ => return Err(anyhow!("RPC payload must be a CBOR text atom or array")),
    })
}

// ── Wasm entity dispatch ───────────────────────────────────────────────────────

#[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
async fn handle_entity_plugin_message(
    message: &ma_core::Message,
    term: CborValue,
    entity: &crate::plugin::EntityPlugin,
    ctx: &RpcHandlerCtx<'_>,
) -> Result<()> {
    info!(fragment = %entity.fragment, from = %message.from, "{}", crate::i18n::t("entity-dispatched"));

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

    let plugin_ctx = PluginCtx {
        self_did: message.to.clone(),
        owner: entity.owner.clone(),
    };

    let cast_input = CastInput { msg: local_msg, ctx: plugin_ctx };
    let result = match entity.kind {
        PluginKind::Stateless => entity.handle_cast(&cast_input)?,
        PluginKind::Stateful => entity.handle_call(&cast_input)?,
    };

    for env in result.envelopes {
        if let Err(e) = send_envelope(env, ctx, &entity.fragment).await {
            warn!(fragment = %entity.fragment, error = %e, "plugin envelope delivery failed");
        }
    }

    // If the plugin called `ma_set_state` during this dispatch, persist to IPFS.
    if let Some(state_bytes) = result.pending_state {
        match ipfs_add(ctx.kubo_rpc_url, state_bytes.clone()).await {
            Ok(cid) => {
                debug!(fragment = %entity.fragment, %cid, "plugin state saved via ma_set_state");
                entity.mark_saved(state_bytes);
            }
            Err(e) => warn!(fragment = %entity.fragment, error = %e, "failed to persist plugin state"),
        }
    }
    Ok(())
}

/// Send an outbound message produced by a plugin via the `ma_send` host function.
async fn send_envelope(env: SendEnvelope, ctx: &RpcHandlerCtx<'_>, fragment: &str) -> Result<()> {
    let recipient = Did::try_from(env.to.as_str())
        .with_context(|| format!("invalid `to` DID in plugin envelope: {}", env.to))?;

    let msg_type = if env.reply_to.is_some() {
        MESSAGE_TYPE_RPC_REPLY
    } else {
        MESSAGE_TYPE_RPC
    };

    // Include entity fragment in sender DID-URL (e.g., did:ma:ipns#fortune)
    let sender_did_url = format!("{}#{}", ctx.our_did, fragment);

    let mut msg = ma_core::Message::new(
        &sender_did_url,
        &env.to,
        msg_type,
        &env.content_type,
        env.content,
        ctx.signing_key,
    )
    .context("failed to build plugin outbound message")?;
    msg.reply_to = env.reply_to;

    let resolver = IpfsGatewayResolver::new(ctx.kubo_rpc_url.to_string());
    match ctx
        .endpoint
        .outbox(&resolver, &recipient.base_id(), RPC_PROTOCOL_ID)
        .await
    {
        Ok(mut outbox) => {
            outbox.send(&msg).await.context("plugin message send failed")?;
            if msg_type == MESSAGE_TYPE_RPC_REPLY {
                info!(
                    to = %env.to,
                    reply_to = ?msg.reply_to,
                    content_type = %env.content_type,
                    "{}",
                    crate::i18n::t("entity-replied")
                );
            }
        }
        Err(err) => warn!(error = %err, to = %env.to, "plugin message delivery failed"),
    }
    Ok(())
}

// ── #root entity ───────────────────────────────────────────────────────────────

async fn handle_root_entity(
    message: &ma_core::Message,
    term: CborValue,
    ctx: &RpcHandlerCtx<'_>,
) -> Result<()> {
    // If a /ma/root/0.0.1 plugin is loaded, use it.
    if let Some(root_plugin) = ctx.root_plugin.as_ref() {
        return handle_root_via_plugin(message, term, root_plugin, ctx).await;
    }
    // Fallback: built-in hardcoded handlers (v0 compat).
    handle_root_builtin(message, term, ctx).await
}

// ── Root plugin dispatch ───────────────────────────────────────────────────────

async fn handle_root_via_plugin(
    message: &ma_core::Message,
    term: CborValue,
    root_plugin: &RootPlugin,
    ctx: &RpcHandlerCtx<'_>,
) -> Result<()> {
    use crate::plugin::root_abi::{CommitIntent, Op, RootRequest, RootResponse};

    // 1. Parse the incoming CBOR term into op + path + optional args.
    let req = build_root_request(message, term, ctx)?;

    // 2. Snapshot the manifest as JSON for `ma_root_read` and `subtree_snapshot`.
    let root_cid = current_root_cid(ctx).await?;
    let manifest: RuntimeManifest = crate::kubo::dag_get(ctx.kubo_rpc_url, &root_cid).await?;
    let manifest_json = serde_json::to_value(&manifest)
        .context("serialising manifest for root plugin")?;

    // 3. Build the CBOR-encoded RootRequest with the relevant subtree snapshot.
    let req_with_snapshot = RootRequest {
        subtree_snapshot: subtree_snapshot_for_path(&req.path, &manifest, ctx.kubo_rpc_url).await?,
        ..req
    };

    let mut input_cbor = Vec::new();
    crate::plugin::encode_cbor(&req_with_snapshot, &mut input_cbor)
        .context("encoding RootRequest")?;

    // 4. Call the plugin.
    let output_bytes = tokio::task::spawn_blocking({
        let plugin = Arc::clone(ctx.root_plugin.as_ref().unwrap());
        move || plugin.call(manifest_json, input_cbor)
    })
    .await
    .context("root plugin task join")??;

    // 5. Parse response.
    let resp: RootResponse = crate::plugin::decode_cbor(&output_bytes)
        .context("decoding RootResponse")?;

    if !resp.ok {
        let reason = resp.error.unwrap_or_else(|| "root plugin error".to_string());
        warn!(from = %message.from, %reason, "root plugin returned error");
        return send_rpc_error_reply(message, ctx, &reason).await;
    }

    // 6. Execute commit intents atomically.
    let new_root_cid = if resp.commit.is_empty() {
        root_cid.clone()
    } else {
        apply_commit_intents(resp.commit, &root_cid, ctx).await?
    };

    // 7. Reply to caller.
    let result_val = resp.result.unwrap_or(serde_json::Value::Null);
    let reply_bytes = serde_json::to_vec(&result_val)?;
    info!(from = %message.from, root_cid = %new_root_cid, "root plugin dispatch ok");
    send_rpc_reply(message, ctx, "application/json", reply_bytes).await
}

/// Build a `RootRequest` (without `subtree_snapshot`) from the incoming CBOR term.
/// The `:crud` tuple convention: `[":crud", "op", "path"]` or
/// `[":crud", "op", "path", "value_or_cid"]`.
/// Legacy verbs (`:list-entities`, etc.) are translated to `Op::Verb`.
fn build_root_request(
    message: &ma_core::Message,
    term: CborValue,
    ctx: &RpcHandlerCtx<'_>,
) -> Result<crate::plugin::root_abi::RootRequest> {
    use crate::plugin::root_abi::{Op, RootRequest};

    let owner_did = ctx.our_did.to_string();

    let (verb, args) = parse_cbor_verb(term)?;

    let (op, path, value, cid, plugin_verb) = match verb.as_str() {
        ":crud" => {
            // [":crud", "op", "path"] or [":crud", "op", "path", "value_or_cid"]
            if args.len() < 2 {
                return Err(anyhow!(":crud requires at least op and path args"));
            }
            let op = match args[0].as_str() {
                "get" => Op::Get,
                "set" => Op::Set,
                "delete" => Op::Delete,
                "apply_cid" => Op::ApplyCid,
                "verb" => Op::Verb,
                other => return Err(anyhow!("unknown crud op: {other}")),
            };
            let path = args[1].clone();
            let value = if op == Op::Set { args.get(2).cloned() } else { None };
            let cid = if op == Op::ApplyCid { args.get(2).cloned() } else { None };
            let v = if op == Op::Verb { args.get(2).cloned() } else { None };
            (op, path, value, cid, v)
        }
        // Legacy verbs — translate to Op::Verb on the entities subtree.
        ":list-entities" => (Op::Get, "entities".to_string(), None, None, None),
        ":create-entity" | ":delete-entity" => {
            return Err(anyhow!(
                "legacy verb '{verb}' not supported via root plugin; use :crud"
            ))
        }
        other => (Op::Verb, "entities".to_string(), None, None, Some(other.to_string())),
    };

    Ok(RootRequest {
        op,
        path,
        value,
        cid,
        verb: plugin_verb,
        caller_did: message.from.clone(),
        message_id: message.id.clone(),
        owner_did,
        subtree_snapshot: serde_json::Value::Null, // filled in by caller
    })
}

/// Build the relevant subtree snapshot JSON for the plugin.
async fn subtree_snapshot_for_path(
    path: &str,
    manifest: &RuntimeManifest,
    _kubo_url: &str,
) -> Result<serde_json::Value> {
    let root = path.split('.').next().unwrap_or("");
    Ok(match root {
        "entities" => serde_json::to_value(&manifest.entities)?,
        "kinds" => serde_json::to_value(&manifest.kinds)?,
        "config" => serde_json::to_value(&manifest.config)?,
        _ => serde_json::Value::Null,
    })
}

/// Execute a list of `CommitIntent`s by mutating the manifest in IPFS.
/// Returns the new root CID after all commits are applied.
async fn apply_commit_intents(
    intents: Vec<crate::plugin::root_abi::CommitIntent>,
    root_cid: &str,
    ctx: &RpcHandlerCtx<'_>,
) -> Result<String> {
    use crate::plugin::root_abi::CommitIntent;

    let mut manifest: RuntimeManifest =
        crate::kubo::dag_get(ctx.kubo_rpc_url, root_cid).await?;

    for intent in intents {
        match intent {
            CommitIntent::UpsertEntity { name, node } => {
                // If the node is an IPLD-link sentinel `{ "/": cid }`, the runtime
                // must fetch and validate the entity from that CID.
                if let Some(cid_str) = node.get("/").and_then(|v| v.as_str()) {
                    let entity_node: EntityNode =
                        crate::kubo::dag_get(ctx.kubo_rpc_url, cid_str)
                            .await
                            .with_context(|| format!("fetching entity node from {cid_str}"))?;
                    let stored_cid = crate::kubo::dag_put(ctx.kubo_rpc_url, &entity_node)
                        .await
                        .with_context(|| format!("re-storing entity {name}"))?;
                    // Load plugin async (best-effort; if it fails, entity is still registered)
                    match crate::plugin::EntityPlugin::load(name.clone(), &entity_node, ctx.kubo_rpc_url).await {
                        Ok(ep) => { ctx.entity_registry.write().await.insert(name.clone(), Arc::new(ep)); }
                        Err(e) => warn!(name = %name, error = %e, "entity plugin load after apply_cid failed"),
                    }
                    manifest.entities.insert(name, IpldLink::new(&stored_cid));
                } else {
                    // Inline entity node — deserialise, store, register.
                    let entity_node: EntityNode = serde_json::from_value(node.clone())
                        .with_context(|| format!("deserialising entity node for {name}"))?;
                    let stored_cid = crate::kubo::dag_put(ctx.kubo_rpc_url, &entity_node)
                        .await
                        .with_context(|| format!("storing entity {name}"))?;
                    match crate::plugin::EntityPlugin::load(name.clone(), &entity_node, ctx.kubo_rpc_url).await {
                        Ok(ep) => { ctx.entity_registry.write().await.insert(name.clone(), Arc::new(ep)); }
                        Err(e) => warn!(name = %name, error = %e, "entity plugin load after upsert failed"),
                    }
                    manifest.entities.insert(name, IpldLink::new(&stored_cid));
                }
            }

            CommitIntent::DeleteEntity { name } => {
                ctx.entity_registry.write().await.remove(&name);
                manifest.entities.remove(&name);
            }

            CommitIntent::UpsertKind { family, implementation, node } => {
                use crate::entity::{KindRef, IpldLink as Link};
                let kind_cid = if let Some(cid_str) = node.get("/").and_then(|v| v.as_str()) {
                    cid_str.to_string()
                } else {
                    let kind_node: crate::entity::KindNode = serde_json::from_value(node)
                        .with_context(|| format!("deserialising kind {family}/{implementation}"))?;
                    crate::kubo::dag_put(ctx.kubo_rpc_url, &kind_node).await?
                };
                manifest.kinds
                    .entry(family)
                    .or_default()
                    .insert(implementation, KindRef::Link(Link::new(&kind_cid)));
            }

            CommitIntent::DeleteKind { family, implementation } => {
                if let Some(fam) = manifest.kinds.get_mut(&family) {
                    fam.remove(&implementation);
                }
            }

            CommitIntent::SetConfig { key, value } => {
                manifest.config.insert(key, value);
            }

            CommitIntent::DeleteConfig { key } => {
                manifest.config.remove(&key);
            }
        }
    }

    let new_cid = crate::kubo::dag_put(ctx.kubo_rpc_url, &manifest).await?;
    update_stats_entities(ctx).await;
    ctx.stats.write().await.root_cid = Some(new_cid.clone());
    Ok(new_cid)
}

// ── Built-in #root handlers (legacy fallback) ─────────────────────────────────

async fn handle_root_builtin(
    message: &ma_core::Message,
    term: CborValue,
    ctx: &RpcHandlerCtx<'_>,
) -> Result<()> {
    let (verb, args) = parse_cbor_verb(term)?;

    match verb.as_str() {
        ":list-entities" => {
            info!("{}", crate::i18n::t("root-list-entities"));
            let names: Vec<String> =
                ctx.entity_registry.read().await.keys().cloned().collect();
            let reply = serde_json::to_vec(&names)?;
            send_rpc_reply(message, ctx, "application/json", reply).await
        }

        ":create-entity" => {
            // args: name, kind, behavior_cid, [acl_entry…]
            if args.len() < 3 {
                return Err(anyhow!(
                    ":create-entity requires at least 3 args: name kind behavior_cid [acl…]"
                ));
            }
            let (name, kind, behavior_cid) = (&args[0], &args[1], &args[2]);
            let acl: Vec<String> = if args.len() > 3 {
                args[3..].to_vec()
            } else {
                vec!["*".into()]
            };
            info!(name = %name, "{}", crate::i18n::t("root-create-entity"));

            let wasi = lookup_kind_wasi(ctx, kind).await?;

            let node = EntityNode {
                name: name.clone(),
                kind: kind.clone(),
                behavior: IpldLink::new(behavior_cid.as_str()),
                state: None,
                owner: message.from.clone(),
                acl,
                wasi,
            };

            let entity_cid = crate::kubo::dag_put(ctx.kubo_rpc_url, &node)
                .await
                .with_context(|| format!("dag_put entity {name}"))?;

            let new_root_cid = update_manifest_add(ctx, name, &entity_cid).await?;

            let reply_json = match crate::plugin::EntityPlugin::load(
                name.clone(),
                &node,
                ctx.kubo_rpc_url,
            )
            .await
            {
                Ok(ep) => {
                    ctx.entity_registry
                        .write()
                        .await
                        .insert(name.clone(), Arc::new(ep));
                    update_stats_entities(ctx).await;
                    ctx.stats.write().await.root_cid = Some(new_root_cid.clone());
                    info!(name = %name, cid = %entity_cid, "{}", crate::i18n::t("entity-created"));
                    serde_json::json!({"cid": new_root_cid, "entity_cid": entity_cid, "status": "ok"})
                }
                Err(e) => {
                    warn!(name = %name, error = %e, "{}", crate::i18n::t("entity-load-failed"));
                    serde_json::json!({
                        "cid": new_root_cid, "entity_cid": entity_cid,
                        "status": "plugin_load_failed", "error": e.to_string()
                    })
                }
            };
            send_rpc_reply(message, ctx, "application/json", serde_json::to_vec(&reply_json)?).await
        }

        ":delete-entity" => {
            if args.is_empty() {
                return Err(anyhow!(":delete-entity requires an entity name arg"));
            }
            let name = &args[0];
            info!(name = %name, "{}", crate::i18n::t("root-delete-entity"));

            ctx.entity_registry.write().await.remove(name);
            let new_root_cid = update_manifest_remove(ctx, name).await?;

            info!(name = %name, cid = %new_root_cid, "{}", crate::i18n::t("entity-deleted"));
            update_stats_entities(ctx).await;
            ctx.stats.write().await.root_cid = Some(new_root_cid.clone());

            let reply = serde_json::to_vec(
                &serde_json::json!({"cid": new_root_cid, "status": "ok"}),
            )?;
            send_rpc_reply(message, ctx, "application/json", reply).await
        }

        other => {
            debug!(verb = %other, "{}", crate::i18n::t("unknown-rpc-atom"));
            Ok(())
        }
    }
}

// ── Manifest CRUD helpers ──────────────────────────────────────────────────────

async fn current_root_cid(ctx: &RpcHandlerCtx<'_>) -> Result<String> {
    ctx.stats
        .read()
        .await
        .root_cid
        .clone()
    .ok_or_else(|| anyhow!("no root_cid; run --gen-root-cid first"))
}

async fn update_manifest_add(
    ctx: &RpcHandlerCtx<'_>,
    name: &str,
    entity_cid: &str,
) -> Result<String> {
    let root_cid = current_root_cid(ctx).await?;
    let mut manifest: RuntimeManifest =
        crate::kubo::dag_get(ctx.kubo_rpc_url, &root_cid).await?;
    manifest
        .entities
        .insert(name.to_string(), IpldLink::new(entity_cid));
    let new_cid = crate::kubo::dag_put(ctx.kubo_rpc_url, &manifest).await?;
    info!(name = %name, cid = %new_cid, "{}", crate::i18n::t("root-entity-updated"));
    Ok(new_cid)
}

async fn update_manifest_remove(ctx: &RpcHandlerCtx<'_>, name: &str) -> Result<String> {
    let root_cid = current_root_cid(ctx).await?;
    let mut manifest: RuntimeManifest =
        crate::kubo::dag_get(ctx.kubo_rpc_url, &root_cid).await?;
    manifest.entities.remove(name);
    let new_cid = crate::kubo::dag_put(ctx.kubo_rpc_url, &manifest).await?;
    info!(name = %name, cid = %new_cid, "{}", crate::i18n::t("root-entity-updated"));
    Ok(new_cid)
}

async fn lookup_kind_wasi(ctx: &RpcHandlerCtx<'_>, kind: &str) -> Result<bool> {
    let root_cid = current_root_cid(ctx).await?;
    let manifest: RuntimeManifest = crate::kubo::dag_get(ctx.kubo_rpc_url, &root_cid)
        .await
        .context("fetching runtime manifest for kind lookup")?;
    let kind_link = manifest
        .kind_link(kind)
        .ok_or_else(|| anyhow!("unknown kind in manifest: {kind}"))?;
    let kind_node: KindNode = crate::kubo::dag_get(ctx.kubo_rpc_url, &kind_link.cid)
        .await
        .with_context(|| format!("fetching kind node for {kind}"))?;
    Ok(kind_node.wasi)
}

async fn update_stats_entities(ctx: &RpcHandlerCtx<'_>) {
    let names: Vec<String> = ctx.entity_registry.read().await.keys().cloned().collect();
    ctx.stats.write().await.entity_names = names;
}

// ── Generic reply helper ───────────────────────────────────────────────────────

async fn send_rpc_reply(
    incoming: &ma_core::Message,
    ctx: &RpcHandlerCtx<'_>,
    content_type: &str,
    content: Vec<u8>,
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

    let resolver = IpfsGatewayResolver::new(ctx.kubo_rpc_url.to_string());
    match ctx
        .endpoint
        .outbox(&resolver, &sender.base_id(), RPC_PROTOCOL_ID)
        .await
    {
        Ok(mut outbox) => {
            outbox.send(&reply).await.context("RPC reply send failed")?;
            info!(
                to = %incoming.from,
                reply_to = %incoming.id,
                content_type = %content_type,
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
    send_rpc_reply(incoming, ctx, "application/cbor", payload).await
}

