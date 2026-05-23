use anyhow::{anyhow, Context, Result};
use ciborium::Value as CborValue;

use crate::entity::IpldLink;

use super::helpers::{
    load_manifest, send_crud_i18n_error, send_crud_ok, send_crud_ok_cid, send_crud_reply_cbor,
    send_crud_reply_yaml, with_manifest_crud,
};
use super::CrudHandlerCtx;

// ── Kinds namespace ────────────────────────────────────────────────────────────

/// Handle `:kinds` CRUD operations.
///
/// Protocol IDs contain slashes and cannot appear as dot-path segments, so
/// individual kind mutations pass the protocol ID as part of the CBOR value:
///
/// | Operation | Message type | Value                           |
/// |-----------|-------------|--------------------------------|
/// | List all  | GET         | —                              |
/// | Edit list | EDIT        | —                              |
/// | Upsert    | SET         | `[protocol_text, cbor_bytes]`  |
/// | Delete    | SET         | `protocol_text`                |
/// | Refuse    | DELETE      | —                              |
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
        // EDIT :kinds → list all protocol IDs as YAML
        (Some("edit"), []) => {
            let manifest = load_manifest(ctx).await?;
            let ids: Vec<String> = manifest.kinds.protocol_ids();
            let yaml = serde_yaml::to_string(&ids).context("serialising kinds list as YAML")?;
            send_crud_reply_yaml(message, reply_type, ctx, &yaml).await
        }
        // SET :kinds with [protocol_text, cbor_bytes] → upsert kind
        (Some(""), [CborValue::Array(items)]) => {
            let items = items.clone();
            match items.as_slice() {
                [CborValue::Text(protocol), CborValue::Bytes(cbor_bytes)] => {
                    let protocol = protocol.clone();
                    let cbor_bytes = cbor_bytes.clone();
                    let cid = crate::kubo::dag_put_raw(ctx.kubo_rpc_url, &cbor_bytes)
                        .await
                        .context("dag_put kind")?;
                    let new_root = with_manifest_crud(ctx, |m| {
                        m.kinds.insert_protocol(&protocol, IpldLink::new(&cid));
                        Ok(())
                    })
                    .await?;
                    send_crud_ok_cid(message, reply_type, ctx, &new_root).await
                }
                _ => Err(anyhow!(
                    "kinds SET value must be [protocol_text, cbor_bytes]"
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
