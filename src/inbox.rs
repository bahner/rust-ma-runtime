//! Handler for the `/ma/inbox/0.0.1` service.
//!
//! Inbox messages are fire-and-forget: fragment-addressed messages are routed
//! to the target entity's `handle_cast` (stateless) or `handle_call` (stateful,
//! to allow state persistence), and the result is discarded.  No reply is sent.
//! Unfragmented (broadcast / runtime-level) messages are logged and dropped.

use std::sync::Arc;

use anyhow::Result;
use tracing::{info, warn};

use crate::entity::{CastInput, LocalMessage, PluginKind, PluginMsg};
use crate::plugin::EntityRegistry;

pub struct InboxHandlerCtx {
    pub our_did: Arc<str>,
    pub entity_registry: EntityRegistry,
    pub kubo_rpc_url: Arc<str>,
}

pub async fn handle_inbox_message(message: &ma_core::Message, ctx: &InboxHandlerCtx) -> Result<()> {
    let fragment = extract_fragment(&message.to, &ctx.our_did);

    let Some(fragment) = fragment else {
        info!(
            from = %message.from,
            to = %message.to,
            message_type = %message.message_type,
            "inbox: unfragmented message dropped"
        );
        return Ok(());
    };

    let entity = ctx.entity_registry.read().await.get(fragment).cloned();

    let Some(entity) = entity else {
        warn!(fragment = %fragment, "inbox: unknown entity fragment; dropped");
        return Ok(());
    };

    info!(
        fragment = %fragment,
        from = %message.from,
        message_type = %message.message_type,
        "inbox: dispatching to entity"
    );

    let local_msg = LocalMessage {
        id: message.id.clone(),
        from: message.from.clone(),
        to: message.to.clone(),
        created_at: message.created_at,
        expires: message.exp,
        reply_to: message.reply_to.clone(),
        message_type: message.message_type.clone(),
        content_type: message.content_type.clone(),
        content: message.content.clone(),
    };
    let cast_input = CastInput {
        msg: PluginMsg::from(&local_msg),
    };

    let result = match entity.kind {
        PluginKind::Stateless => entity.handle_cast(&cast_input).await?,
        PluginKind::Stateful => entity.handle_call(&cast_input).await?,
    };

    // Persist state if the entity called ma_set_state during this dispatch.
    if let Some(state_bytes) = result.pending_state {
        let kubo_url = Arc::clone(&ctx.kubo_rpc_url);
        let fragment_str = entity.fragment.clone();
        let entity_arc = Arc::clone(&entity);
        tokio::spawn(async move {
            match crate::kubo::dag_put(&kubo_url, &state_bytes).await {
                Ok(cid) => {
                    entity_arc.mark_saved(state_bytes);
                    info!(fragment = %fragment_str, cid = %cid, "inbox: entity state persisted");
                }
                Err(e) => {
                    warn!(fragment = %fragment_str, error = %e, "inbox: failed to persist entity state");
                }
            }
        });
    }

    Ok(())
}

/// Strip `<our_did>#` from `to` and return the bare fragment, if present.
fn extract_fragment<'a>(to: &'a str, our_did: &str) -> Option<&'a str> {
    let prefix = format!("{our_did}#");
    to.strip_prefix(prefix.as_str())
}
