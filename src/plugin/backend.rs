//! Extism plugin backend: host functions, the private context types they
//! capture, and the per-entity worker threads.
//!
//! All Wasm interaction lives here.  Each entity's `Plugin` is built and driven
//! on its own dedicated worker thread (`run_wasm_thread`), so no Wasm call ever
//! parks a Tokio worker.  The public handle and message types live in the
//! parent module.

use anyhow::{anyhow, Result};
use extism::{host_fn, Function, Manifest, Plugin, UserData, Wasm, PTR};
use tokio::sync::{
    mpsc::{UnboundedReceiver, UnboundedSender},
    oneshot,
};
use tracing::warn;

use crate::entity::{
    CastInput, CreateEntityRequest, Lifecycle, ReplyRequest, SendEnvelope,
};

use super::{DispatchResult, EntityMsg, EntityRegistry, NativeDispatch, CALL_PATH, CAPTURE};

// ── Fragment generation ───────────────────────────────────────────────────────

/// Generate an 8-character URL-safe alphanumeric fragment (nanoid-style).
fn generate_fragment() -> String {
    use rand::Rng;
    const ALPHABET: &[u8] = b"0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ";
    let mut rng = rand::thread_rng();
    (0..8)
        .map(|_| ALPHABET[rng.gen_range(0..ALPHABET.len())] as char)
        .collect()
}

/// True if dispatching `target` from `caller` (with `call_path` already on the
/// stack) would form a cycle — i.e. a self-call or a re-entry into an entity
/// that is an ancestor still awaiting a reply.  Such a dispatch would deadlock
/// (the ancestor's thread is blocked and can never service the call), so
/// `ma_call` returns `:error` instead.
fn is_call_cycle(target: &str, caller: &str, call_path: &[String]) -> bool {
    target == caller || call_path.iter().any(|f| f == target)
}

// ── Host functions ────────────────────────────────────────────────────────────

// Context captured by `ma_send` and `ma_reply` host functions.
//
// Sending is fire-and-forget: the envelope is forwarded to the main event
// loop via an unbounded channel.  The scheduler (and any other dispatch
// path) has zero envelope-handling responsibility.
struct OutboxCtx {
    tx: UnboundedSender<(String, SendEnvelope)>,
    fragment: String,
}

// `ma_send` host function exposed to plugins (namespace `extism:host/user`).
//
// The plugin passes a CBOR-encoded `SendEnvelope`.  The host forwards it
// directly to the main event loop via the outbox channel.
host_fn!(ma_send_fn(user_data: OutboxCtx; input: Vec<u8>) -> Vec<u8> {
    let envelope: SendEnvelope = from_cbor_bytes(&input)?;
    let arc = user_data.get()?;
    let ctx = arc.lock().unwrap();
    let _ = ctx.tx.send((ctx.fragment.clone(), envelope));
    drop(ctx);
    Ok(Vec::new())
});

// `ma_reply` host function: convenience wrapper around `ma_send`.
//
// Plugin passes a CBOR-encoded `ReplyRequest { msg, content }`.  The runtime
// fills in `to` (= msg.from), `reply_to` (= msg.id), and `content_type`
// automatically — plugin only provides the reply body.
host_fn!(ma_reply_fn(user_data: OutboxCtx; input: Vec<u8>) -> Vec<u8> {
    let req: ReplyRequest = from_cbor_bytes(&input)?;
    // If inside a synchronous ma_call (capture mode), capture the first reply
    // for this dispatch instead of enqueuing to the outbox.
    let captured = CAPTURE.with(|c| {
        let mut slot = c.borrow_mut();
        if let Some(inner) = slot.as_mut() {
            if inner.is_none() {
                *inner = Some(req.content.clone());
                return true;
            }
        }
        false
    });
    if captured {
        return Ok(Vec::new());
    }
    let envelope = SendEnvelope {
        to: req.msg.from,
        content_type: req.content_type,
        message_type: None,
        content: req.content,
        reply_to: Some(req.msg.id),
    };
    let arc = user_data.get()?;
    let ctx = arc.lock().unwrap();
    let _ = ctx.tx.send((ctx.fragment.clone(), envelope));
    drop(ctx);
    Ok(Vec::new())
});

// Internal state context shared between the `ma_set_state` host function and
// `EntityPlugin`.  Lives inside a `UserData<StateCtx>`.
struct StateCtx {
    /// New state bytes queued for IPFS persistence by the current dispatch.
    pending: Option<Vec<u8>>,
    /// Last successfully persisted snapshot (loaded from IPFS at startup, then
    /// updated by `mark_saved`).  Used for change detection.
    persisted: Option<Vec<u8>>,
    /// `true` when `pending` differs from `persisted` and has not yet been
    /// written to IPFS.
    dirty: bool,
}

impl StateCtx {
    const fn new(persisted: Vec<u8>) -> Self {
        Self {
            pending: None,
            persisted: Some(persisted),
            dirty: false,
        }
    }
}

// `ma_set_state` host function: plugin calls this to queue a new state.
// Sets `dirty` **only** when the bytes actually differ from the last
// persisted snapshot — no-op saves do not pollute the dirty flag.
host_fn!(ma_set_state_fn(user_data: StateCtx; input: Vec<u8>) -> Vec<u8> {
    let arc = user_data.get()?;
    let mut ctx = arc.lock().unwrap();
    if ctx.persisted.as_deref() != Some(input.as_slice()) {
        ctx.pending = Some(input);
        ctx.dirty = true;
        drop(ctx);
    }
    Ok(Vec::new())
});

// ── ma_create_entity host function ────────────────────────────────────────────

// Context captured by `ma_create_entity` host function.
struct CreateEntityCtx {
    pending: Vec<CreateEntityRequest>,
    /// Fragment of the calling (parent) entity.
    caller_fragment: String,
}

// `ma_create_entity` host function: plugin requests creation of a new entity.
//
// Input is CBOR-encoded `{ "kind": "/ma/…/0.0.1", "behaviour": "bafyCID" }`.
// The runtime generates a nanoid fragment, queues the request, and returns the
// fragment string (CBOR-encoded) to the plugin immediately.
// Actual plugin loading and manifest persistence happen after dispatch returns.
#[derive(serde::Deserialize)]
struct CreateEntityInput {
    kind: String,
    behaviour: String,
}

host_fn!(ma_create_entity_fn(user_data: CreateEntityCtx; input: Vec<u8>) -> Vec<u8> {
    let req: CreateEntityInput = from_cbor_bytes(&input)?;
    let fragment = generate_fragment();
    let arc = user_data.get()?;
    let mut ctx = arc.lock().unwrap();
    let parent = ctx.caller_fragment.clone();
    ctx.pending.push(CreateEntityRequest {
        fragment: fragment.clone(),
        kind_protocol: req.kind,
        behaviour_cid: req.behaviour,
        parent,
    });
    drop(ctx);
    let mut out = Vec::new();
    ciborium::ser::into_writer(&fragment, &mut out)
        .map_err(|e| extism::Error::msg(format!("ma_create_entity: CBOR encode: {e}")))?;
    Ok(out)
});

// ── ma_avatar_id host function ────────────────────────────────────────────────

// Context captured by `ma_avatar_id` host function.
// The key is derived from the runtime's IPNS secret at startup and never changes.
struct AvatarIdCtx {
    key: [u8; 32],
}

// `ma_avatar_id` host function: compute a per-runtime pseudonymous avatar ID.
//
// Input:  DID string (UTF-8 bytes), e.g. "did:ma:k51alice…"
// Output: 24 hex chars (first 12 bytes of blake3 keyed hash).
//
// The key is derived from the runtime's IPNS secret, so:
//   - Same DID → same avatar_id within this runtime (deterministic across restarts).
//   - Different runtimes → different avatar_ids (privacy across worlds).
//   - The DID is never stored by house/avatar plugins; only the avatar_id is kept.
host_fn!(ma_avatar_id_fn(user_data: AvatarIdCtx; input: Vec<u8>) -> Vec<u8> {
    let did = String::from_utf8(input)
        .map_err(|e| extism::Error::msg(format!("ma_avatar_id: invalid UTF-8: {e}")))?;
    let arc = user_data.get()?;
    let key = arc.lock().unwrap().key;
    let hash = blake3::keyed_hash(&key, did.as_bytes());
    let mut hex = String::with_capacity(24);
    for b in &hash.as_bytes()[..12] {
        hex.push(b"0123456789abcdef"[(b >> 4) as usize] as char);
        hex.push(b"0123456789abcdef"[(b & 0x0f) as usize] as char);
    }
    Ok(hex.into_bytes())
});

// ── ma_call host function ─────────────────────────────────────────────────────────────────────────────────────────────────

/// Parse a duration in whole seconds from `var`, falling back to `default`.
fn env_secs(var: &str, default: u64) -> std::time::Duration {
    std::time::Duration::from_secs(
        std::env::var(var)
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(default),
    )
}

/// Hard cap on any single Wasm export invocation (init / `handle_cast` /
/// `handle_call`).  Enforced by extism via wasmtime epoch interruption — a
/// plugin stuck in an infinite loop gets aborted and the worker thread
/// survives, instead of being wedged forever.
///
/// Override with `MA_WASM_CALL_TIMEOUT_SECS` (used by tests; also an
/// operational escape hatch).
pub(super) fn wasm_call_timeout() -> std::time::Duration {
    env_secs("MA_WASM_CALL_TIMEOUT_SECS", 30)
}

/// Upper bound on how long `ma_call` waits for the target entity's reply.
/// Slightly above [`wasm_call_timeout`] so the callee's own execution cap
/// fires first; this bound only triggers when the callee's dispatch queue
/// is severely backed up.
fn ma_call_timeout() -> std::time::Duration {
    wasm_call_timeout() + std::time::Duration::from_secs(5)
}

// Context captured by the `ma_call` host function.
struct CallCtx {
    registry: EntityRegistry,
    /// DID of the calling entity — used as `from` in the synthetic message.
    caller_did: String,
    /// Fragment of the calling entity — prevents self-calls.
    caller_fragment: String,
    /// Tokio runtime handle — lets the (plain OS) worker thread run a
    /// timed wait for the reply without ever parking a Tokio worker.
    handle: tokio::runtime::Handle,
}

// Input sent by the plugin to `ma_call`.
#[derive(serde::Deserialize)]
struct CallRequest {
    /// Bare fragment name of the target entity (no `#` prefix).
    to: String,
    /// CBOR-encoded verb atom or array to send to the target.
    #[serde(with = "serde_bytes")]
    content: Vec<u8>,
}

// `ma_call` host function: synchronous call to another entity in this runtime.
//
// The plugin passes a CBOR-encoded `CallRequest { to, content }`.
// The runtime looks up the target entity handle by fragment, sends a capturing
// `Dispatch` message to the target's worker thread, and blocks *this* entity's
// thread until the reply arrives.  Because each entity owns its own thread,
// this never parks a Tokio worker.
//
// Cycle detection: the current CALL_PATH plus this entity's own fragment is
// checked against the target; A→B→A (or longer) returns :error instead of
// deadlocking (the target's thread could never make progress if it is an
// ancestor waiting on us).
host_fn!(ma_call_fn(user_data: CallCtx; input: Vec<u8>) -> Vec<u8> {
    let req: CallRequest = from_cbor_bytes(&input)?;

    let arc = user_data.get()?;
    let ctx = arc.lock().unwrap();
    let registry = ctx.registry.clone();
    let caller_did = ctx.caller_did.clone();
    let caller_fragment = ctx.caller_fragment.clone();
    let handle = ctx.handle.clone();
    drop(ctx);

    // Build the call path leading to the target: everything already on the
    // stack, plus this entity.  Reject self-calls and cycles.
    let mut call_path = CALL_PATH.with(|p| p.borrow().clone());
    let cycle_detected = is_call_cycle(&req.to, &caller_fragment, &call_path);
    if cycle_detected {
        let mut out = Vec::new();
        ciborium::ser::into_writer(
            &ciborium::Value::Array(vec![
                ciborium::Value::Text(":error".into()),
                ciborium::Value::Text(format!(
                    "ma_call: cycle detected — #{} is already in the call stack",
                    req.to
                )),
            ]),
            &mut out,
        )
        .map_err(|e| extism::Error::msg(format!("ma_call encode: {e}")))?;
        return Ok(out);
    }
    call_path.push(caller_fragment.clone());

    // Look up the target handle.  `blocking_read` is safe here: an entity
    // worker thread is a plain OS thread, never a Tokio worker, so it is not
    // in an async execution context.
    let ep = registry.blocking_read().get(&req.to).cloned();
    let Some(ep) = ep else {
        let mut out = Vec::new();
        ciborium::ser::into_writer(
            &ciborium::Value::Array(vec![
                ciborium::Value::Text(":error".into()),
                ciborium::Value::Text(format!("ma_call: entity #{} not found", req.to)),
            ]),
            &mut out,
        )
        .map_err(|e| extism::Error::msg(format!("ma_call encode: {e}")))?;
        return Ok(out);
    };

    let now_secs = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();

    let msg = crate::entity::LocalMessage {
        id: format!("ma-call-{}-{}", caller_fragment, req.to),
        from: caller_did,
        to: format!("#{}", req.to),
        created_at: now_secs,
        expires: now_secs + 5,
        reply_to: None,
        message_type: ma_core::MESSAGE_TYPE_RPC.to_string(),
        content_type: ma_core::CONTENT_TYPE_TERM.to_string(),
        content: req.content,
    };
    let cast_input = CastInput { msg: crate::entity::PluginMsg::from(&msg) };

    // Send a capturing dispatch to the target's worker thread and block this
    // thread until it replies.
    let (reply_tx, reply_rx) = oneshot::channel();
    if ep
        .tx
        .send(EntityMsg::Dispatch {
            stateful: true,
            capture: true,
            call_path,
            input: cast_input,
            reply: reply_tx,
        })
        .is_err()
    {
        let mut out = Vec::new();
        ciborium::ser::into_writer(
            &ciborium::Value::Array(vec![
                ciborium::Value::Text(":error".into()),
                ciborium::Value::Text(format!("ma_call: entity #{} thread is gone", req.to)),
            ]),
            &mut out,
        )
        .map_err(|e| extism::Error::msg(format!("ma_call encode: {e}")))?;
        return Ok(out);
    }

    let dispatch_result = match handle.block_on(async {
        tokio::time::timeout(ma_call_timeout(), reply_rx).await
    }) {
        Ok(Ok(Ok(r))) => r,
        Ok(Ok(Err(e))) => {
            let mut out = Vec::new();
            ciborium::ser::into_writer(
                &ciborium::Value::Array(vec![
                    ciborium::Value::Text(":error".into()),
                    ciborium::Value::Text(format!("ma_call: dispatch failed: {e}")),
                ]),
                &mut out,
            )
            .map_err(|err| extism::Error::msg(format!("ma_call encode: {err}")))?;
            return Ok(out);
        }
        Ok(Err(_)) => {
            let mut out = Vec::new();
            ciborium::ser::into_writer(
                &ciborium::Value::Array(vec![
                    ciborium::Value::Text(":error".into()),
                    ciborium::Value::Text(format!("ma_call: no reply from #{}", req.to)),
                ]),
                &mut out,
            )
            .map_err(|err| extism::Error::msg(format!("ma_call encode: {err}")))?;
            return Ok(out);
        }
        Err(_elapsed) => {
            let mut out = Vec::new();
            ciborium::ser::into_writer(
                &ciborium::Value::Array(vec![
                    ciborium::Value::Text(":error".into()),
                    ciborium::Value::Text(format!(
                        "ma_call: timed out waiting for #{} after {}s",
                        req.to,
                        ma_call_timeout().as_secs()
                    )),
                ]),
                &mut out,
            )
            .map_err(|err| extism::Error::msg(format!("ma_call encode: {err}")))?;
            return Ok(out);
        }
    };

    // Return the captured ma_reply content, or :ok if plugin never replied.
    if let Some(bytes) = dispatch_result.captured_reply {
        Ok(bytes)
    } else {
        let mut out = Vec::new();
        ciborium::ser::into_writer(&ciborium::Value::Text(":ok".into()), &mut out)
            .map_err(|e| extism::Error::msg(format!("ma_call encode: {e}")))?;
        Ok(out)
    }
});

// ── ma_delete_entity host function ───────────────────────────────────────────

/// Context captured by `ma_delete_entity` and `ma_end` host functions.
struct DeleteEntityCtx {
    pending: Vec<String>,
    /// Set by `ma_end` — entity requests its own removal after this dispatch.
    self_terminate: bool,
    /// Fragment of the owning entity; used when `self_terminate` is true.
    self_fragment: String,
}

// `ma_delete_entity` host function: plugin requests removal of another entity.
//
// Input is a CBOR-encoded fragment string.
// The request is queued; the runtime validates and removes after dispatch.
host_fn!(ma_delete_entity_fn(user_data: DeleteEntityCtx; input: Vec<u8>) -> Vec<u8> {
    let target: String = from_cbor_bytes(&input)?;
    let arc = user_data.get()?;
    arc.lock().unwrap().pending.push(target);
    let mut out = Vec::new();
    ciborium::ser::into_writer(&":queued", &mut out)
        .map_err(|e| extism::Error::msg(format!("ma_delete_entity: CBOR encode: {e}")))?;
    Ok(out)
});

// `ma_end` host function: entity requests its own removal (self-termination).
//
// Takes no meaningful input.  After the current dispatch completes the runtime
// removes this entity from the registry and manifest.
host_fn!(ma_end_fn(user_data: DeleteEntityCtx; _input: Vec<u8>) -> Vec<u8> {
    let arc = user_data.get()?;
    arc.lock().unwrap().self_terminate = true;
    let mut out = Vec::new();
    ciborium::ser::into_writer(&ciborium::Value::Text(":ok".to_string()), &mut out)
        .map_err(|e| extism::Error::msg(format!("ma_end: CBOR encode: {e}")))?;
    Ok(out)
});

// ── init() payload ────────────────────────────────────────────────────────────


// ── Worker threads ────────────────────────────────────────────────────────────

/// Everything a Wasm entity's worker thread needs to build and run its plugin.
/// All fields are `Send`; the non-`Send` `Plugin` is constructed on the thread.
pub(super) struct WasmThreadCfg {
    pub(super) fragment: String,
    pub(super) our_did: String,
    pub(super) wasm_bytes: Vec<u8>,
    pub(super) init_state: Vec<u8>,
    pub(super) wasi: bool,
    pub(super) host_functions: Vec<String>,
    pub(super) has_init: bool,
    pub(super) node_kind: String,
    pub(super) envelope_tx: UnboundedSender<(String, SendEnvelope)>,
    pub(super) entity_registry: EntityRegistry,
    pub(super) avatar_key: [u8; 32],
    /// Tokio runtime handle, captured in `EntityPlugin::load` (async context).
    /// Passed to `CallCtx` so `ma_call` can run timed waits from the worker thread.
    pub(super) handle: tokio::runtime::Handle,
    /// IPFS CID of the Wasm behaviour bytes for this entity.
    pub(super) behaviour_cid: String,
    /// iroh QUIC node ID of this runtime.
    pub(super) iroh_node_id: String,
    /// Unix epoch seconds when the runtime process started.
    pub(super) started_at: u64,
    /// DID-URL of the parent entity, if any.
    pub(super) parent: Option<String>,
}

/// Handles retained by the worker thread to drain plugin side-effects after
/// each dispatch and to service state messages.
struct WasmThreadState {
    plugin: Plugin,
    state: UserData<StateCtx>,
    create_queue: UserData<CreateEntityCtx>,
    delete_queue: UserData<DeleteEntityCtx>,
}

/// Build the flat config map injected into the extism `Manifest` for this
/// entity.  Available to the plugin at any time via `extism_config_get`.
///
/// This replaces `EntityCtx` — plugins no longer receive ctx in `init()`.
///
/// Keys:
///   `self`         full DID-URL of this entity (`did:ma:<runtime>#<id>`)  [always]
///   `id`           bare fragment without `#`                               [always]
///   `kind`         kind protocol ID e.g. `/ma/root/0.0.1`                 [always]
///   `cid`          IPFS CID of the entity's Wasm behaviour                [always]
///   `runtime`      runtime's own DID                                       [always]
///   `iroh_node_id` iroh QUIC node ID of this runtime                      [always]
///   `started_at`   Unix epoch seconds when the runtime started            [always]
///   `parent`       DID-URL of the parent entity                           [if set]
fn build_plugin_config(cfg: &WasmThreadCfg) -> std::collections::BTreeMap<String, String> {
    let mut config = std::collections::BTreeMap::new();
    config.insert("self".to_string(), format!("{}#{}", cfg.our_did, cfg.fragment));
    config.insert("id".to_string(), cfg.fragment.clone());
    config.insert("kind".to_string(), cfg.node_kind.clone());
    config.insert("cid".to_string(), cfg.behaviour_cid.clone());
    config.insert("runtime".to_string(), cfg.our_did.clone());
    config.insert("iroh_node_id".to_string(), cfg.iroh_node_id.clone());
    config.insert("started_at".to_string(), cfg.started_at.to_string());
    if let Some(parent) = &cfg.parent {
        config.insert("parent".to_string(), parent.clone());
    }
    config
}

/// Build the Wasm plugin and its filtered host-function set on the worker thread.
fn build_wasm_plugin(cfg: &WasmThreadCfg) -> Result<WasmThreadState> {
    let outbox_ctx_send = UserData::new(OutboxCtx {
        tx: cfg.envelope_tx.clone(),
        fragment: cfg.fragment.clone(),
    });
    let outbox_ctx_reply = UserData::new(OutboxCtx {
        tx: cfg.envelope_tx.clone(),
        fragment: cfg.fragment.clone(),
    });
    let state: UserData<StateCtx> = UserData::new(StateCtx::new(cfg.init_state.clone()));
    let create_queue: UserData<CreateEntityCtx> = UserData::new(CreateEntityCtx {
        pending: Vec::new(),
        caller_fragment: cfg.fragment.clone(),
    });
    let delete_queue: UserData<DeleteEntityCtx> = UserData::new(DeleteEntityCtx {
        pending: Vec::new(),
        self_terminate: false,
        self_fragment: cfg.fragment.clone(),
    });
    let call_ctx: UserData<CallCtx> = UserData::new(CallCtx {
        registry: cfg.entity_registry.clone(),
        caller_did: format!("{}#{}", cfg.our_did, &cfg.fragment),
        caller_fragment: cfg.fragment.clone(),
        handle: cfg.handle.clone(),
    });
    let avatar_id_ctx: UserData<AvatarIdCtx> = UserData::new(AvatarIdCtx {
        key: cfg.avatar_key,
    });

    // IMPORTANT: This order MUST match the @extism.import_fn declaration order
    // in the Python actor library chain (actor.py → avatar.py / root.py).
    // extism-py assigns IMPORT_INDEX sequentially across all @extism.import_fn
    // declarations, and ffi.__invoke_host_func(idx) indexes into the FILTERED
    // host_fns array by position.  The filter preserves all_fns order, so
    // IMPORT_INDEX must equal the position in this list after filtering.
    //
    // Python IMPORT_INDEX assignments:
    //   actor.py:  ma_reply=0, ma_set_state=1, ma_send=2, ma_call=3, ma_end=4
    //   avatar.py: ma_avatar_id=5
    //   root.py:   ma_create_entity=5, ma_delete_entity=6
    //
    // Plugins that do NOT declare ma_end skip it via the host_functions filter,
    // so their existing indices (ma_avatar_id=4, ma_create_entity=4) are unchanged.
    let all_fns: Vec<(&str, Function)> = vec![
        (
            "ma_reply",
            Function::new("ma_reply", [PTR], [PTR], outbox_ctx_reply, ma_reply_fn),
        ),
        (
            "ma_set_state",
            Function::new("ma_set_state", [PTR], [PTR], state.clone(), ma_set_state_fn),
        ),
        (
            "ma_send",
            Function::new("ma_send", [PTR], [PTR], outbox_ctx_send, ma_send_fn),
        ),
        (
            "ma_call",
            Function::new("ma_call", [PTR], [PTR], call_ctx, ma_call_fn),
        ),
        (
            "ma_end",
            Function::new("ma_end", [PTR], [PTR], delete_queue.clone(), ma_end_fn),
        ),
        (
            "ma_avatar_id",
            Function::new("ma_avatar_id", [PTR], [PTR], avatar_id_ctx, ma_avatar_id_fn),
        ),
        (
            "ma_create_entity",
            Function::new(
                "ma_create_entity",
                [PTR],
                [PTR],
                create_queue.clone(),
                ma_create_entity_fn,
            ),
        ),
        (
            "ma_delete_entity",
            Function::new(
                "ma_delete_entity",
                [PTR],
                [PTR],
                delete_queue.clone(),
                ma_delete_entity_fn,
            ),
        ),
    ];
    let allowed: std::collections::HashSet<&str> =
        cfg.host_functions.iter().map(String::as_str).collect();
    let host_fns: Vec<Function> = all_fns
        .into_iter()
        .filter(|(name, _)| allowed.contains(*name))
        .map(|(_, f)| f)
        .collect();

    let manifest = Manifest::new([Wasm::data(cfg.wasm_bytes.clone())])
        .with_timeout(wasm_call_timeout())
        .with_config(build_plugin_config(cfg).into_iter());
    let plugin = Plugin::new(&manifest, host_fns, cfg.wasi)
        .map_err(|e| anyhow!("failed to create extism plugin for '{}': {e}", cfg.fragment))?;

    Ok(WasmThreadState {
        plugin,
        state,
        create_queue,
        delete_queue,
    })
}

/// Run `init()` on the freshly built plugin, returning the resulting lifecycle.
///
/// Entity context is available via `extism_config_get` — `init()` receives
/// only the raw persisted state bytes (empty slice for a brand-new entity).
fn run_init(ts: &mut WasmThreadState, cfg: &WasmThreadCfg) -> Result<Lifecycle> {
    if !cfg.has_init {
        return Ok(Lifecycle::Running);
    }
    let init_result_bytes = ts
        .plugin
        .call::<&[u8], Vec<u8>>("init", cfg.init_state.as_slice())
        .map_err(|e| anyhow!("init() failed for '{}': {e}", cfg.fragment))?;

    let lifecycle = match ciborium::de::from_reader::<ciborium::Value, _>(
        init_result_bytes.as_slice(),
    ) {
        Ok(ciborium::Value::Text(s)) if s == ":ok" => Lifecycle::Running,
        Ok(ciborium::Value::Array(ref v))
            if v.first() == Some(&ciborium::Value::Text(":ok".into())) =>
        {
            Lifecycle::Running
        }
        Ok(ciborium::Value::Array(ref v))
            if v.first() == Some(&ciborium::Value::Text(":error".into())) =>
        {
            let reason = v
                .get(1)
                .and_then(|r| {
                    if let ciborium::Value::Text(s) = r {
                        Some(s.as_str())
                    } else {
                        None
                    }
                })
                .unwrap_or("unknown");
            warn!(fragment = %cfg.fragment, reason = %reason, "init() returned :error");
            Lifecycle::Error
        }
        Ok(other) => {
            warn!(fragment = %cfg.fragment, value = ?other, "init() returned unexpected value; treating as :ok");
            Lifecycle::Running
        }
        Err(_) => Lifecycle::Running,
    };
    Ok(lifecycle)
}

/// Execute one dispatch to the plugin, draining side-effects into a
/// [`DispatchResult`].  Runs on the entity's own worker thread.
fn execute_dispatch(
    ts: &mut WasmThreadState,
    fragment: &str,
    stateful: bool,
    capture: bool,
    call_path: Vec<String>,
    input: &CastInput,
) -> Result<DispatchResult> {
    let export = if stateful {
        "handle_call"
    } else {
        "handle_cast"
    };
    let mut input_bytes = Vec::new();
    ciborium::ser::into_writer(input, &mut input_bytes)
        .map_err(|e| anyhow!("failed to CBOR-encode CastInput: {e}"))?;

    // Publish per-dispatch context for the ma_call / ma_reply host functions,
    // which run on this same thread during the call.
    CALL_PATH.with(|p| *p.borrow_mut() = call_path);
    CAPTURE.with(|c| *c.borrow_mut() = if capture { Some(None) } else { None });

    let output = ts
        .plugin
        .call::<&[u8], Vec<u8>>(export, input_bytes.as_slice())
        .map_err(|e| anyhow!("{export}() failed for '{fragment}': {e}"));

    let captured_reply = CAPTURE.with(|c| c.borrow_mut().take().flatten());
    CALL_PATH.with(|p| p.borrow_mut().clear());

    let output = output?;

    let pending_state = ts
        .state
        .get()
        .map_err(|e| anyhow!("state error: {e}"))?
        .lock()
        .map_err(|e| anyhow!("state poisoned: {e}"))?
        .pending
        .take();

    let create_requests = ts
        .create_queue
        .get()
        .map_err(|e| anyhow!("create_queue error: {e}"))?
        .lock()
        .map_err(|e| anyhow!("create_queue poisoned: {e}"))?
        .pending
        .drain(..)
        .collect();

    let delete_requests = {
        let arc = ts
            .delete_queue
            .get()
            .map_err(|e| anyhow!("delete_queue error: {e}"))?;
        let mut dq = arc.lock().map_err(|e| anyhow!("delete_queue poisoned: {e}"))?;
        let mut reqs: Vec<String> = dq.pending.drain(..).collect();
        if dq.self_terminate {
            reqs.push(dq.self_fragment.clone());
            dq.self_terminate = false;
        }
        reqs
    };

    Ok(DispatchResult {
        output,
        pending_state,
        create_requests,
        delete_requests,
        captured_reply,
    })
}

/// Entry point for a Wasm entity's dedicated worker thread.
///
/// Builds the plugin, runs `init()`, reports the lifecycle back to
/// [`EntityPlugin::load`], then serves dispatch / state messages until the
/// channel closes.  The thread is a plain OS thread (never a Tokio worker),
/// so blocking here — including `ma_call`'s `blocking_recv` — is safe.
#[allow(clippy::needless_pass_by_value)] // cfg is moved into and owned by the thread
pub(super) fn run_wasm_thread(
    cfg: WasmThreadCfg,
    mut rx: UnboundedReceiver<EntityMsg>,
    life_tx: oneshot::Sender<Result<Lifecycle>>,
) {
    let mut ts = match build_wasm_plugin(&cfg) {
        Ok(ts) => ts,
        Err(e) => {
            let _ = life_tx.send(Err(e));
            return;
        }
    };
    let lifecycle = match run_init(&mut ts, &cfg) {
        Ok(lc) => lc,
        Err(e) => {
            let _ = life_tx.send(Err(e));
            return;
        }
    };
    if life_tx.send(Ok(lifecycle)).is_err() {
        // Loader gave up (dropped the receiver); nothing to serve.
        return;
    }

    while let Some(msg) = rx.blocking_recv() {
        match msg {
            EntityMsg::Dispatch {
                stateful,
                capture,
                call_path,
                input,
                reply,
            } => {
                let res =
                    execute_dispatch(&mut ts, &cfg.fragment, stateful, capture, call_path, &input);
                let _ = reply.send(res);
            }
            EntityMsg::TakePending { reply } => {
                let pending = ts
                    .state
                    .get()
                    .ok()
                    .and_then(|arc| arc.lock().ok().and_then(|mut c| c.pending.take()));
                let _ = reply.send(pending);
            }
            EntityMsg::MarkSaved(bytes) => {
                if let Ok(arc) = ts.state.get() {
                    if let Ok(mut c) = arc.lock() {
                        c.persisted = Some(bytes);
                        c.dirty = false;
                    }
                }
            }
            EntityMsg::Shutdown => break,
        }
    }
}

/// Entry point for a native (compiled-in) entity's worker thread.
///
/// The closure may call `tokio::spawn` (e.g. `#scheduler`), so the runtime
/// context is entered *only* around the closure — never around `blocking_recv`,
/// which would panic inside an async context.
#[allow(clippy::needless_pass_by_value)] // handler + handle are owned by the thread
pub(super) fn run_native_thread(
    handler: NativeDispatch,
    handle: tokio::runtime::Handle,
    mut rx: UnboundedReceiver<EntityMsg>,
) {
    while let Some(msg) = rx.blocking_recv() {
        match msg {
            EntityMsg::Dispatch { input, reply, .. } => {
                let res = {
                    let _guard = handle.enter();
                    handler(&input)
                };
                let _ = reply.send(res);
            }
            // Native entities have no Wasm state; state messages are no-ops.
            EntityMsg::TakePending { reply } => {
                let _ = reply.send(None);
            }
            EntityMsg::MarkSaved(_) => {}
            EntityMsg::Shutdown => break,
        }
    }
}
// ── CBOR helpers ──────────────────────────────────────────────────────────────

fn from_cbor_bytes<T: serde::de::DeserializeOwned>(bytes: &[u8]) -> Result<T> {
    ciborium::de::from_reader(bytes).map_err(|e| anyhow!("CBOR decode error: {e}"))
}

#[cfg(test)]
mod tests {
    use super::{generate_fragment, is_call_cycle};

    #[test]
    fn generate_fragment_is_8_alphanumeric() {
        let f = generate_fragment();
        assert_eq!(f.len(), 8);
        assert!(f.chars().all(|c| c.is_ascii_alphanumeric()));
    }

    #[test]
    fn generate_fragment_varies() {
        let set: std::collections::HashSet<_> = (0..5).map(|_| generate_fragment()).collect();
        assert!(set.len() > 1, "fragments should not all be identical");
    }

    #[test]
    fn cycle_detects_self_call() {
        assert!(is_call_cycle("a", "a", &[]));
    }

    #[test]
    fn cycle_detects_ancestor_reentry() {
        let path = vec!["a".to_string(), "b".to_string()];
        assert!(is_call_cycle("a", "c", &path)); // a -> b -> c -> a
    }

    #[test]
    fn cycle_allows_fresh_target() {
        let path = vec!["a".to_string(), "b".to_string()];
        assert!(!is_call_cycle("d", "c", &path)); // a -> b -> c -> d
    }
}
