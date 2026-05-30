//! `:create` — namespace creation has been removed.
//!
//! Namespaces are superseded by the entity parent hierarchy. This handler
//! now returns a `not-supported` error for any `:create` operation.

use anyhow::Result;
use ciborium::Value as CborValue;

use super::helpers::send_crud_i18n_error;
use super::CrudHandlerCtx;

pub(super) async fn handle_create_ns(
    message: &ma_core::Message,
    _tail: Option<&str>,
    _args: Vec<CborValue>,
    reply_type: &str,
    ctx: &CrudHandlerCtx<'_>,
) -> Result<()> {
    send_crud_i18n_error(message, reply_type, ctx, "not-supported").await
}
