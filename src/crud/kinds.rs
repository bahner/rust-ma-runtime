use anyhow::{anyhow, Result};
use ciborium::Value as CborValue;

use crate::entity::{IpldLink, KindNode};

use super::helpers::{
    load_manifest, send_crud_i18n_error, send_crud_ok, send_crud_ok_cid, send_crud_ok_yaml,
    send_crud_reply_cbor, strip_brackets, with_manifest_crud,
};
use super::CrudHandlerCtx;

// ── Kinds handler ────────────────────────────────────────────────────────────

/// Handle `/kinds` CRUD operations.
///
/// | Operation | Path | Body |
/// |-----------|------|------|
/// | List all  | `GET /kinds` | — |
/// | Get kind  | `GET /kinds/ma/avatar/0.0.1` | — |
/// | Upsert    | `SET /kinds/ma/avatar/0.0.1` | `<bafyCID>` |
/// | Delete    | `DEL /kinds/ma/avatar/0.0.1` | — |
pub(super) async fn handle_kinds_ns(
    message: &ma_core::Message,
    rest: &[String],
    tail: Option<&str>,
    args: Vec<CborValue>,
    reply_type: &str,
    ctx: &CrudHandlerCtx,
) -> Result<()> {
    if rest.is_empty() {
        // Operations on /kinds root
        return match (tail, args.as_slice()) {
            // GET /kinds → list all protocol IDs
            (None, []) => {
                let manifest = load_manifest(ctx).await?;
                let ids: Vec<String> = manifest.kinds.protocol_ids();
                send_crud_reply_cbor(message, reply_type, ctx, &ids).await
            }
            // DELETE /kinds → refuse
            (Some(""), []) => {
                send_crud_i18n_error(message, reply_type, ctx, "refuse-delete-root").await
            }
            _ => Err(anyhow!("unknown kinds root operation")),
        };
    }

    // /kinds/ma/avatar/0.0.1 → protocol_id = "/ma/avatar/0.0.1"
    let protocol_id = format!("/{}", rest.join("/"));

    match (tail, args.as_slice()) {
        // GET /kinds/<protocol> → fetch and return KindNode as YAML
        (None, []) => {
            let manifest = load_manifest(ctx).await?;
            match manifest.kinds.get_protocol(&protocol_id) {
                Some(link) => {
                    let kind: KindNode = crate::kubo::dag_get(&ctx.kubo_rpc_url, &link.cid).await?;
                    send_crud_ok_yaml(message, reply_type, ctx, &kind).await
                }
                None => send_crud_i18n_error(message, reply_type, ctx, "kind-not-found").await,
            }
        }
        // SET /kinds/<protocol> <cid> → upsert kind
        (Some(""), [CborValue::Text(raw)]) => {
            let cid = strip_brackets(raw).unwrap_or(raw.as_str()).to_string();
            let new_root = with_manifest_crud(ctx, |m| {
                m.kinds.insert_protocol(&protocol_id, IpldLink::new(&cid));
                Ok(())
            })
            .await?;
            send_crud_ok_cid(message, reply_type, ctx, &new_root).await
        }
        // DELETE /kinds/<protocol> → remove kind
        (Some(""), []) => {
            with_manifest_crud(ctx, |m| {
                m.kinds.remove_protocol(&protocol_id);
                Ok(())
            })
            .await?;
            send_crud_ok(message, reply_type, ctx).await
        }
        _ => Err(anyhow!("unknown kinds operation")),
    }
}
