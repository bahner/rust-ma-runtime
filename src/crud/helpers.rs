use std::sync::Arc;

use anyhow::{anyhow, Context, Result};
use ciborium::Value as CborValue;
use ma_core::{
    Did, DidDocumentResolver, IpfsGatewayResolver, Ipld, CONTENT_TYPE_TERM, CONTENT_TYPE_TERM_CBOR,
    CONTENT_TYPE_TERM_DAG_CBOR, CONTENT_TYPE_TERM_YAML,
};
use tracing::{info, warn};

use crate::entity::{EntityNode, KindNode, RuntimeManifest};

use super::CrudHandlerCtx;

// ── Path helpers ───────────────────────────────────────────────────────────────

/// Parse `.ns.seg1.seg2` → `("ns", ["seg1", "seg2"])`.
pub(super) fn parse_path(path: &str) -> Result<(&str, Vec<String>)> {
    let body = path
        .strip_prefix('.')
        .ok_or_else(|| anyhow!("CRUD path must start with '.' — got: {path}"))?;
    let (ns, rest_str) = body.split_once('.').unwrap_or((body, ""));
    if ns.is_empty() {
        return Err(anyhow!("CRUD path has no handler segment: {path}"));
    }
    let segs: Vec<String> = rest_str
        .split('.')
        .filter(|s| !s.is_empty())
        .map(String::from)
        .collect();
    Ok((ns, segs))
}

/// Decoded CRUD operation from a single incoming message payload.
pub(super) enum CrudOp {
    /// `[".path"]`
    Get(String),
    /// `[".path", value]` — value is a CBOR scalar or `<cid>` IPFS reference
    Set(String, CborValue),
    /// `[".path", ""]` — empty string value means delete
    Delete(String),
}

/// Decode a `application/x-ma-crud` payload.
///
/// - `[".path"]`          → GET
/// - `[".path", ""]`      → DELETE (empty string = delete)
/// - `[".path", value]`   → SET (value is a CBOR scalar or `<cid>` IPFS reference)
pub(super) fn decode_crud_payload(content: &[u8]) -> Result<CrudOp> {
    let val: CborValue =
        ciborium::de::from_reader(content).context("invalid CBOR in CRUD payload")?;
    let CborValue::Array(items) = val else {
        return Err(anyhow!("CRUD payload must be a CBOR array"));
    };
    match items.len() {
        1 => {
            let CborValue::Text(path) = items.into_iter().next().unwrap() else {
                return Err(anyhow!("CRUD get: path must be a text string"));
            };
            Ok(CrudOp::Get(path))
        }
        2 => {
            let mut it = items.into_iter();
            let first = it.next().unwrap();
            let second = it.next().unwrap();
            let CborValue::Text(path) = first else {
                return Err(anyhow!("CRUD payload: path must be a text string"));
            };
            match second {
                CborValue::Text(s) if s.is_empty() => Ok(CrudOp::Delete(path)),
                value => Ok(CrudOp::Set(path, value)),
            }
        }
        n => Err(anyhow!(
            "CRUD payload must be a 1 or 2-element CBOR array, got {n}"
        )),
    }
}

/// Return the inner CID string if `s` is bracketed as `<cid>`, else `None`.
///
/// Explicit `<brackets>` are required wherever a CID is expected — bare CID
/// strings are treated as plain text and never auto-detected.
pub(super) fn strip_brackets(s: &str) -> Option<&str> {
    s.strip_prefix('<')?.strip_suffix('>')
}

// ── Manifest helpers ───────────────────────────────────────────────────────────

pub(super) async fn with_manifest_crud<F>(ctx: &CrudHandlerCtx, f: F) -> Result<String>
where
    F: FnOnce(&mut RuntimeManifest) -> Result<()>,
{
    // All manifest mutations are serialised through the writer, which owns the
    // authoritative root CID — no read-modify-write race on a stale base.
    let new_cid = ctx.manifest_writer.mutate(f).await?;
    update_stats_entities(ctx).await;
    Ok(new_cid)
}

pub(super) async fn current_root_cid(ctx: &CrudHandlerCtx) -> Result<String> {
    ctx.stats
        .read()
        .await
        .root_cid
        .clone()
        .ok_or_else(|| anyhow!("no manifest root CID available"))
}

/// Fetch and deserialise the current `RuntimeManifest` from IPFS.
pub(super) async fn load_manifest(ctx: &CrudHandlerCtx) -> Result<RuntimeManifest> {
    let root_cid = current_root_cid(ctx).await?;
    crate::kubo::dag_get(&ctx.kubo_rpc_url, &root_cid).await
}

/// Load an ACL from `cid`, insert it into `acl_cache` under `cache_key`.
/// Logs success or failure; non-fatal either way.
pub(super) async fn acl_cache_update(ctx: &CrudHandlerCtx, cache_key: &str, cid: &str) {
    match crate::acl::load_acl_from_cid(&ctx.kubo_rpc_url, cid).await {
        Ok(acl_map) => {
            ctx.acl_cache
                .write()
                .await
                .insert(cache_key.to_string(), acl_map);
            info!(key = %cache_key, %cid, "ACL loaded into cache");
        }
        Err(e) => {
            warn!(key = %cache_key, %cid, error = %e, "failed to load ACL into cache");
        }
    }
}

/// Spawn an independent task that loads a plugin from `entity_node` and inserts
/// it into the entity registry (replacing any existing version).
///
/// Returns immediately — the reload happens asynchronously so the CRUD event
/// loop is never blocked by WASM fetching, instantiation, or `init()`.
#[allow(clippy::too_many_arguments)]
pub(super) fn spawn_entity_reload(
    name: String,
    entity_node: EntityNode,
    kind_registry: crate::entity::KindRegistry,
    stats: crate::status::SharedStats,
    kubo_rpc_url: Arc<str>,
    our_did: Arc<str>,
    envelope_tx: tokio::sync::mpsc::UnboundedSender<(String, crate::entity::SendEnvelope)>,
    entity_registry: crate::plugin::EntityRegistry,
    avatar_key: [u8; 32],
) {
    tokio::spawn(async move {
        // Look up the KindNode from the kind registry.
        let kind_node: Arc<KindNode> = {
            let registry = kind_registry.read().await;
            if let Some(k) = registry.get(&entity_node.kind).cloned() {
                k
            } else {
                // Fall back: fetch from IPFS via the manifest.
                drop(registry);
                let root_cid = stats.read().await.root_cid.clone();
                let Some(root_cid) = root_cid else {
                    warn!(name = %name, kind = %entity_node.kind, "no root CID available; cannot reload entity");
                    return;
                };
                let manifest: crate::entity::RuntimeManifest = match crate::kubo::dag_get(
                    &kubo_rpc_url,
                    &root_cid,
                )
                .await
                {
                    Ok(m) => m,
                    Err(e) => {
                        warn!(name = %name, kind = %entity_node.kind, error = %e, "failed to load manifest for kind lookup");
                        return;
                    }
                };
                let kind_link = if let Some(l) = manifest.kinds.get_protocol(&entity_node.kind) {
                    l.clone()
                } else {
                    warn!(name = %name, kind = %entity_node.kind, "kind not in manifest; cannot reload entity");
                    return;
                };
                match crate::kubo::dag_get::<KindNode>(&kubo_rpc_url, &kind_link.cid).await {
                    Ok(k) => Arc::new(k),
                    Err(e) => {
                        warn!(name = %name, kind = %entity_node.kind, error = %e, "failed to fetch kind node; cannot reload entity");
                        return;
                    }
                }
            }
        };

        let (iroh_node_id, started_at) = {
            let s = stats.read().await;
            (s.endpoint_id.clone(), s.started_at)
        };

        match crate::plugin::EntityPlugin::load(
            name.clone(),
            &entity_node,
            &kind_node,
            &our_did,
            &kubo_rpc_url,
            envelope_tx,
            entity_registry.clone(),
            avatar_key,
            &iroh_node_id,
            started_at,
        )
        .await
        {
            Ok((ep, _lifecycle)) => {
                entity_registry
                    .write()
                    .await
                    .insert(name.clone(), Arc::new(ep));
                info!(name = %name, "{}", crate::i18n::t("entity-reloaded"));
            }
            Err(e) => {
                warn!(
                    name = %name,
                    error = %e,
                    "{}",
                    crate::i18n::t("entity-load-failed")
                );
            }
        }
    });
}

async fn update_stats_entities(ctx: &CrudHandlerCtx) {
    let names: Vec<String> = ctx.entity_registry.read().await.keys().cloned().collect();
    ctx.stats.write().await.entity_names = names;
}

// ── i18n helpers ───────────────────────────────────────────────────────────────

/// Resolve caller's DID document and extract their preferred language.
/// Falls back to the runtime's own language on any error.
pub(super) async fn caller_lang(from: &str, resolver: &IpfsGatewayResolver) -> String {
    if let Ok(doc) = resolver.resolve(from).await {
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

/// Send a CRUD error reply with a message localised to the caller's language.
pub(super) async fn send_crud_i18n_error(
    incoming: &ma_core::Message,
    reply_type: &str,
    ctx: &CrudHandlerCtx,
    key: &str,
) -> Result<()> {
    let lang = caller_lang(&incoming.from, ctx.resolver.as_ref()).await;
    send_crud_error(incoming, reply_type, ctx, &crate::i18n::t_lang(&lang, key)).await
}

/// Like [`send_crud_i18n_error`] but substitutes `%name%` placeholders in the
/// translated message before sending.
pub(super) async fn send_crud_i18n_errorf(
    incoming: &ma_core::Message,
    reply_type: &str,
    ctx: &CrudHandlerCtx,
    key: &str,
    args: &[(&str, &str)],
) -> Result<()> {
    let lang = caller_lang(&incoming.from, ctx.resolver.as_ref()).await;
    send_crud_error(
        incoming,
        reply_type,
        ctx,
        &crate::i18n::tf_lang(&lang, key, args),
    )
    .await
}

// ── Reply helpers ──────────────────────────────────────────────────────────────

pub(super) async fn send_crud_ok(
    incoming: &ma_core::Message,
    reply_type: &str,
    ctx: &CrudHandlerCtx,
) -> Result<()> {
    let mut out = Vec::new();
    ciborium::ser::into_writer(&CborValue::Text(":ok".to_string()), &mut out)
        .context("encoding :ok")?;
    send_crud_reply(incoming, reply_type, ctx, &out).await
}

pub(super) async fn send_crud_ok_path(
    incoming: &ma_core::Message,
    reply_type: &str,
    ctx: &CrudHandlerCtx,
    path: &str,
) -> Result<()> {
    let mut out = Vec::new();
    ciborium::ser::into_writer(
        &CborValue::Array(vec![
            CborValue::Text(":ok".to_string()),
            CborValue::Text(path.to_string()),
        ]),
        &mut out,
    )
    .context("encoding [:ok, path]")?;
    send_crud_reply(incoming, reply_type, ctx, &out).await
}

pub(super) async fn send_crud_ok_cid(
    incoming: &ma_core::Message,
    reply_type: &str,
    ctx: &CrudHandlerCtx,
    cid: &str,
) -> Result<()> {
    let mut out = Vec::new();
    ciborium::ser::into_writer(
        &CborValue::Array(vec![
            CborValue::Text(":ok".to_string()),
            CborValue::Text(cid.to_string()),
        ]),
        &mut out,
    )
    .context("encoding [:ok, cid]")?;
    send_crud_reply(incoming, reply_type, ctx, &out).await
}

pub(super) async fn send_crud_error(
    incoming: &ma_core::Message,
    reply_type: &str,
    ctx: &CrudHandlerCtx,
    reason: &str,
) -> Result<()> {
    let mut out = Vec::new();
    ciborium::ser::into_writer(
        &CborValue::Array(vec![
            CborValue::Text(":error".to_string()),
            CborValue::Text(reason.to_string()),
        ]),
        &mut out,
    )
    .context("encoding error reply")?;
    send_crud_reply(incoming, reply_type, ctx, &out).await
}

pub(super) async fn send_crud_reply_cbor<T: serde::Serialize + Sync>(
    incoming: &ma_core::Message,
    reply_type: &str,
    ctx: &CrudHandlerCtx,
    value: &T,
) -> Result<()> {
    let mut out = Vec::new();
    ciborium::ser::into_writer(value, &mut out).context("encoding CBOR reply")?;
    send_crud_reply(incoming, reply_type, ctx, &out).await
}

/// Send a GET data reply whose payload is a raw CBOR-serialised struct
/// (e.g. `EntityNode`).  Uses `CONTENT_TYPE_TERM_CBOR` so the receiver
/// knows it must decode CBOR to display the value.
pub(super) async fn send_crud_data_cbor<T: serde::Serialize + Sync>(
    incoming: &ma_core::Message,
    reply_type: &str,
    ctx: &CrudHandlerCtx,
    value: &T,
) -> Result<()> {
    let mut out = Vec::new();
    ciborium::ser::into_writer(value, &mut out).context("encoding CBOR data reply")?;
    send_crud_reply_raw(incoming, reply_type, ctx, CONTENT_TYPE_TERM_CBOR, &out).await
}

/// Send a GET data reply whose payload is an inline YAML string (encoded
/// as a CBOR text value).  Uses `CONTENT_TYPE_TERM_YAML` so the receiver
/// can use it directly as editor content without further decoding.
pub(super) async fn send_crud_data_yaml<T: serde::Serialize + Sync>(
    incoming: &ma_core::Message,
    reply_type: &str,
    ctx: &CrudHandlerCtx,
    value: &T,
) -> Result<()> {
    let yaml_str = serde_yaml::to_string(value).context("encoding YAML reply")?;
    let mut out = Vec::new();
    ciborium::ser::into_writer(&CborValue::Text(yaml_str), &mut out)
        .context("encoding YAML string as CBOR text")?;
    send_crud_reply_raw(incoming, reply_type, ctx, CONTENT_TYPE_TERM_YAML, &out).await
}

/// Send a GET data reply whose payload is a `CIDv1` string (encoded as a
/// CBOR text value).  Uses `CONTENT_TYPE_TERM_DAG_CBOR` so the receiver
/// knows it must fetch the CID from IPFS to obtain the actual content.
pub(super) async fn send_crud_data_dag_cbor(
    incoming: &ma_core::Message,
    reply_type: &str,
    ctx: &CrudHandlerCtx,
    cid: &str,
) -> Result<()> {
    let mut out = Vec::new();
    ciborium::ser::into_writer(&CborValue::Text(cid.to_string()), &mut out)
        .context("encoding CID as CBOR text")?;
    send_crud_reply_raw(incoming, reply_type, ctx, CONTENT_TYPE_TERM_DAG_CBOR, &out).await
}

pub(super) async fn send_crud_reply(
    incoming: &ma_core::Message,
    reply_type: &str,
    ctx: &CrudHandlerCtx,
    content: &[u8],
) -> Result<()> {
    send_crud_reply_raw(incoming, reply_type, ctx, CONTENT_TYPE_TERM, content).await
}

async fn send_crud_reply_raw(
    incoming: &ma_core::Message,
    reply_type: &str,
    ctx: &CrudHandlerCtx,
    content_type: &str,
    content: &[u8],
) -> Result<()> {
    let sender = Did::try_from(incoming.from.as_str())
        .with_context(|| format!("invalid sender DID: {}", incoming.from))?;

    let mut reply = ma_core::Message::new(
        ctx.our_did.as_ref(),
        &incoming.from,
        reply_type,
        content_type,
        content,
        ctx.signing_key.as_ref(),
    )
    .context("failed to build CRUD reply")?;
    reply.reply_to = Some(incoming.id.clone());

    match ctx
        .endpoint
        .outbox(
            ctx.resolver.as_ref(),
            &sender.base_id(),
            ma_core::CRUD_PROTOCOL_ID,
        )
        .await
    {
        Ok(mut outbox) => {
            outbox
                .send(&reply)
                .await
                .context("CRUD reply send failed")?;
            info!(to = %incoming.from, reply_to = %incoming.id, "CRUD reply sent");
        }
        Err(err) => {
            warn!(error = %err, to = %incoming.from, "CRUD reply delivery failed");
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{decode_crud_payload, parse_path, strip_brackets, CrudOp};
    use ciborium::Value as CborValue;

    fn cbor(v: &CborValue) -> Vec<u8> {
        let mut buf = Vec::new();
        ciborium::ser::into_writer(v, &mut buf).unwrap();
        buf
    }

    #[test]
    fn parse_path_splits_namespace_and_segments() {
        let (ns, segs) = parse_path(".entities.rms.acl").unwrap();
        assert_eq!(ns, "entities");
        assert_eq!(segs, vec!["rms", "acl"]);
    }

    #[test]
    fn parse_path_bare_namespace_has_no_segments() {
        let (ns, segs) = parse_path(".entities").unwrap();
        assert_eq!(ns, "entities");
        assert!(segs.is_empty());
    }

    #[test]
    fn parse_path_requires_leading_dot() {
        assert!(parse_path("entities.rms").is_err());
    }

    #[test]
    fn parse_path_rejects_empty_namespace() {
        assert!(parse_path("..rms").is_err());
    }

    #[test]
    fn parse_path_ignores_double_and_trailing_dots() {
        let (ns, segs) = parse_path(".entities..rms.").unwrap();
        assert_eq!(ns, "entities");
        assert_eq!(segs, vec!["rms"]);
    }

    #[test]
    fn decode_crud_get_on_single_element() {
        let payload = cbor(&CborValue::Array(vec![CborValue::Text(".entities".into())]));
        assert!(
            matches!(decode_crud_payload(&payload).unwrap(), CrudOp::Get(p) if p == ".entities")
        );
    }

    #[test]
    fn decode_crud_delete_on_empty_string() {
        let payload = cbor(&CborValue::Array(vec![
            CborValue::Text(".entities.rms".into()),
            CborValue::Text(String::new()),
        ]));
        assert!(
            matches!(decode_crud_payload(&payload).unwrap(), CrudOp::Delete(p) if p == ".entities.rms")
        );
    }

    #[test]
    fn decode_crud_set_on_value() {
        let payload = cbor(&CborValue::Array(vec![
            CborValue::Text(".config.k".into()),
            CborValue::Text("v".into()),
        ]));
        assert!(
            matches!(decode_crud_payload(&payload).unwrap(), CrudOp::Set(p, _) if p == ".config.k")
        );
    }

    #[test]
    fn decode_crud_rejects_non_array() {
        let payload = cbor(&CborValue::Text("nope".into()));
        assert!(decode_crud_payload(&payload).is_err());
    }

    #[test]
    fn decode_crud_rejects_wrong_arity() {
        let payload = cbor(&CborValue::Array(vec![
            CborValue::Text("a".into()),
            CborValue::Text("b".into()),
            CborValue::Text("c".into()),
        ]));
        assert!(decode_crud_payload(&payload).is_err());
    }

    #[test]
    fn strip_brackets_extracts_inner() {
        assert_eq!(strip_brackets("<bafy123>"), Some("bafy123"));
    }

    #[test]
    fn strip_brackets_rejects_bare_and_partial() {
        assert_eq!(strip_brackets("bafy123"), None);
        assert_eq!(strip_brackets("<bafy123"), None);
        assert_eq!(strip_brackets("bafy123>"), None);
    }
}
