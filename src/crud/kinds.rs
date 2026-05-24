use anyhow::{anyhow, Context, Result};
use ciborium::Value as CborValue;

use crate::entity::IpldLink;

use super::helpers::{
    is_ipfs_path, load_manifest, send_crud_error, send_crud_i18n_error, send_crud_ok,
    send_crud_ok_cid, send_crud_reply_cbor, with_manifest_crud,
};
use super::CrudHandlerCtx;

// ── Kinds namespace ────────────────────────────────────────────────────────────

/// Handle `:kinds` CRUD operations.
///
/// Protocol IDs contain slashes and cannot appear as dot-path segments, so
/// individual kind mutations pass the protocol ID as part of the CBOR value:
///
/// | Operation | CRUD payload                                       |
/// |-----------|---------------------------------------------------|
/// | List all  | `[":get", ":kinds"]`                              |
/// | Upsert    | `[":kinds", [protocol_text, "/ipfs/…"]]`          |
/// | Delete    | `[":kinds", protocol_text]`                       |
/// | Refuse    | `[":delete", ":kinds"]`                           |
pub(super) async fn handle_kinds_ns(
    message: &ma_core::Message,
    rest: &[String],
    tail: Option<&str>,
    args: Vec<CborValue>,
    reply_type: &str,
    ctx: &CrudHandlerCtx<'_>,
) -> Result<()> {
    if !rest.is_empty() {
        return Err(anyhow!(
            "kinds path segments not supported (protocol IDs use CBOR value args)"
        ));
    }
    match (tail, args.as_slice()) {
        // GET :kinds → list all protocol IDs
        (None, []) => {
            let manifest = load_manifest(ctx).await?;
            let ids: Vec<String> = manifest.kinds.protocol_ids();
            send_crud_reply_cbor(message, reply_type, ctx, &ids).await
        }
        // SET :kinds with [protocol_text, "/ipfs/…"] → upsert kind
        (Some(""), [CborValue::Array(items)]) => {
            let items = items.clone();
            match items.as_slice() {
                [CborValue::Text(protocol), CborValue::Text(path)] => {
                    if !is_ipfs_path(path) {
                        return send_crud_error(
                            message,
                            reply_type,
                            ctx,
                            "kind value must be an IPFS path (/ipfs/, /ipns/, or /ipld/)",
                        )
                        .await;
                    }
                    let protocol = protocol.clone();
                    let cid = crate::kubo::dag_resolve(ctx.kubo_rpc_url, path)
                        .await
                        .with_context(|| format!("resolving kind path {path}"))?;
                    let new_root = with_manifest_crud(ctx, |m| {
                        m.kinds.insert_protocol(&protocol, IpldLink::new(&cid));
                        Ok(())
                    })
                    .await?;
                    send_crud_ok_cid(message, reply_type, ctx, &new_root).await
                }
                _ => Err(anyhow!(
                    "kinds upsert value must be [protocol_text, ipfs_path]"
                )),
            }
        }
        // SET :kinds with protocol_text → delete that kind
        (Some(""), [CborValue::Text(protocol)]) => {
            let protocol = protocol.clone();
            with_manifest_crud(ctx, |m| {
                m.kinds.remove_protocol(&protocol);
                Ok(())
            })
            .await?;
            send_crud_ok(message, reply_type, ctx).await
        }
        // DELETE :kinds (root) → refuse
        (Some(""), []) => {
            send_crud_i18n_error(message, reply_type, ctx, "refuse-delete-root").await
        }
        _ => Err(anyhow!("unknown kinds operation")),
    }
}
