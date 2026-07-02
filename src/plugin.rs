//! Extism-based Wasm plugin wrapper and native entity dispatch.
//!
//! [`EntityPlugin`] supports two backends:
//!
//! - **Extism** — Wasm loaded from IPFS, used for all user-defined entities.
//! - **Native** — a compiled-in Rust closure, used for built-in system entities
//!   such as `#scheduler`.  Native entities are registered via
//!   [`EntityPlugin::new_native`]; [`EntityPlugin::load`] **must not** be
//!   called for [`Evaluator::Native`] kinds.
//!
//! Both backends implement the same dispatch surface: [`EntityPlugin::handle_cast`]
//! / [`EntityPlugin::handle_call`].  The [`PluginKind`] field determines which
//! export is called (for Extism) or which path the closure takes (for Native).

use std::{
    cell::RefCell,
    collections::HashMap,
    sync::Arc,
};

use anyhow::{anyhow, Context, Result};
use extism::{host_fn, Function, Manifest, Plugin, UserData, Wasm, PTR};
use ma_core::{cat_bytes, ipfs_add};
use tokio::sync::{
    mpsc::{UnboundedReceiver, UnboundedSender},
    oneshot, RwLock,
};
use tracing::{debug, warn};

use crate::entity::{
    CastInput, CreateEntityRequest, EntityCtx, EntityNode, Evaluator, KindNode, Lifecycle,
    PluginKind, ReplyRequest, SendEnvelope,
};

// ── Actor thread model ────────────────────────────────────────────────────────
//
// Every entity runs on its own dedicated OS thread that exclusively owns its
// Wasm `Plugin` (or native closure).  Dispatch is message-passing: callers send
// an [`EntityMsg`] on the entity's channel and receive the result on a oneshot.
//
// This eliminates `block_in_place` entirely: no Wasm call ever parks a Tokio
// worker thread, and nested `ma_call` chains (room → house → avatar) never
// deadlock — each entity blocks only its own thread while awaiting a sub-call.

/// Messages sent to an entity's dedicated worker thread.
enum EntityMsg {
    /// Dispatch a message to `handle_cast` / `handle_call`.
    Dispatch {
        /// `true` → `handle_call` (stateful); `false` → `handle_cast` (stateless).
        stateful: bool,
        /// `true` → capture the first `ma_reply` and return it in
        /// [`DispatchResult::captured_reply`] instead of enqueuing to the outbox.
        /// Set only for synchronous `ma_call` dispatches.
        capture: bool,
        /// Chain of entity fragments already in the current call stack — used by
        /// `ma_call` for cycle detection.
        call_path: Vec<String>,
        input: CastInput,
        reply: oneshot::Sender<Result<DispatchResult>>,
    },
    /// Take the pending (unsaved) state bytes out of the entity's `StateCtx`.
    /// Used by [`EntityPlugin::trigger_save`].
    TakePending {
        reply: oneshot::Sender<Option<Vec<u8>>>,
    },
    /// Record that `bytes` were successfully persisted to IPFS: update the
    /// `persisted` snapshot and clear the dirty flag.
    MarkSaved(Vec<u8>),
    /// Stop the worker thread.
    #[allow(dead_code)]
    Shutdown,
}

// ── Per-thread dispatch state ─────────────────────────────────────────────────
//
// Set on the entity's own thread immediately before each `plugin.call(...)`,
// and read by the `ma_call` / `ma_reply` host functions (which run on the same
// thread during the call).  Cleared after the call returns.

thread_local! {
    /// The chain of caller fragments leading to the current dispatch.
    /// `ma_call` appends the current entity and checks for cycles before
    /// sending to a target.
    static CALL_PATH: RefCell<Vec<String>> = const { RefCell::new(Vec::new()) };

    /// Reply-capture slot for synchronous `ma_call` dispatches.
    ///   `None`          — not capturing; `ma_reply` goes to the outbox.
    ///   `Some(None)`    — capturing, nothing captured yet.
    ///   `Some(Some(b))` — first `ma_reply` captured.
    static CAPTURE: RefCell<Option<Option<Vec<u8>>>> = const { RefCell::new(None) };
}

// ── Native dispatch type ─────────────────────────────────────────────────────

/// Type of the compiled-in Rust closure used by native entity plugins.
///
/// The closure receives a [`CastInput`] and returns a [`DispatchResult`],
/// exactly like a Wasm `handle_cast` / `handle_call` export.
/// Both [`EntityPlugin::handle_cast`] and [`EntityPlugin::handle_call`] route
/// through the same closure for native entities — native entities do not
/// distinguish stateful vs stateless internally (the closure owns its own
/// state via `Arc<Mutex<…>>` or similar).
pub type NativeDispatch =
    std::sync::Arc<dyn Fn(&CastInput) -> anyhow::Result<DispatchResult> + Send + Sync>;

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
    let hex: String = hash.as_bytes()[..12]
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect();
    Ok(hex.into_bytes())
});

// ── ma_call host function ─────────────────────────────────────────────────────

// Context captured by the `ma_call` host function.
struct CallCtx {
    registry: EntityRegistry,
    /// DID of the calling entity — used as `from` in the synthetic message.
    caller_did: String,
    /// Fragment of the calling entity — prevents self-calls.
    caller_fragment: String,
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
    drop(ctx);

    // Build the call path leading to the target: everything already on the
    // stack, plus this entity.  Reject self-calls and cycles.
    let mut call_path = CALL_PATH.with(|p| p.borrow().clone());
    let cycle_detected = req.to == caller_fragment || call_path.contains(&req.to);
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
    let ep = match ep {
        Some(ep) => ep,
        None => {
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
        }
    };

    let now_ns = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
        .saturating_mul(1_000_000_000);

    let msg = crate::entity::LocalMessage {
        id: format!("ma-call-{}-{}", caller_fragment, req.to),
        from: caller_did,
        to: format!("#{}", req.to),
        created_at: now_ns,
        expires: now_ns + 5_000_000_000,
        reply_to: None,
        content_type: ma_core::CONTENT_TYPE_TERM.to_string(),
        content: req.content,
    };
    let cast_input = CastInput { msg };

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

    let dispatch_result = match reply_rx.blocking_recv() {
        Ok(Ok(r)) => r,
        Ok(Err(e)) => {
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
        Err(_) => {
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

/// Context captured by `ma_delete_entity` host function.
struct DeleteEntityCtx {
    pending: Vec<String>,
}

// `ma_delete_entity` host function: plugin requests removal of an entity.
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

// ── Dispatch result ─────────────────────────────────────────────────────────

/// Return value from `handle_cast` and `handle_call`.
pub struct DispatchResult {
    /// Raw CBOR bytes returned by the plugin export.
    pub output: Vec<u8>,
    /// State bytes queued by the plugin via `ma_set_state` host function.
    /// `None` if the plugin did not call `ma_set_state` during this invocation.
    pub pending_state: Option<Vec<u8>>,
    /// Entity creation requests enqueued via `ma_create_entity` host function.
    pub create_requests: Vec<CreateEntityRequest>,
    /// Entity deletion requests enqueued via `ma_delete_entity` host function.
    /// Each entry is the fragment of the entity to delete.
    pub delete_requests: Vec<String>,
    /// First `ma_reply` captured during a synchronous `ma_call` dispatch
    /// (`capture = true`).  `None` for normal (outbox) dispatches.
    pub captured_reply: Option<Vec<u8>>,
}

// ── Registry type alias ───────────────────────────────────────────────────────

/// Thread-safe map from fragment name (e.g. `"fortune"`) to its entity handle.
pub type EntityRegistry = Arc<RwLock<HashMap<String, Arc<EntityPlugin>>>>;

pub fn new_entity_registry() -> EntityRegistry {
    Arc::new(RwLock::new(HashMap::new()))
}

// ── init() payload ────────────────────────────────────────────────────────────

#[derive(serde::Serialize)]
struct InitPayload<'a> {
    ctx: &'a EntityCtx,
    #[serde(with = "serde_bytes", skip_serializing_if = "Vec::is_empty")]
    state: Vec<u8>,
}

// ── EntityPlugin (handle) ─────────────────────────────────────────────────────

/// Handle to an entity running on its own dedicated worker thread.
///
/// The actual Wasm `Plugin` (or native closure) lives on that thread and is
/// never shared; all interaction goes through the [`EntityMsg`] channel.  This
/// struct is a cheap, cloneable handle carrying the entity's metadata plus the
/// channel sender.
pub struct EntityPlugin {
    /// Globally unique entity name (bare, no `#` prefix, no dots).
    /// Matches the key in `RuntimeManifest.entities` and equals the DID fragment:
    /// `did:ma:<ipns>#<fragment>` → lookup `entities[fragment]`.
    pub fragment: String,
    pub kind: PluginKind,
    /// ACL name string — resolved via `acls.<acl>` in the root manifest.
    /// Empty string means deny-all (fail-closed).
    /// For native entities the ACL is applied normally; the runtime does not
    /// bypass entity-level ACLs for compiled-in entities.
    pub acl: String,
    /// Parent fragment — the entity that owns/created this one, if any.
    /// Immutable after creation. Only the parent may delete this entity.
    pub parent: Option<String>,
    /// `true` for compiled-in native entities (e.g. `#scheduler`).
    native: bool,
    /// Channel to the entity's dedicated worker thread.
    tx: UnboundedSender<EntityMsg>,
}

impl EntityPlugin {
    /// Create a native entity plugin backed by a compiled-in Rust closure.
    ///
    /// Use this for built-in system entities (e.g. `#scheduler`) whose
    /// implementation is compiled into the runtime binary.  The closure
    /// receives a [`CastInput`] and returns a [`DispatchResult`].
    ///
    /// The closure runs on a dedicated worker thread that has entered the
    /// current Tokio runtime context, so `tokio::spawn` inside the closure
    /// works.  Must be called from within a Tokio runtime.
    ///
    /// Returns `(plugin, Lifecycle::Running)` — native entities are always
    /// immediately running; they never go through `init()`.
    pub fn new_native(
        fragment: impl Into<String>,
        node: &EntityNode,
        handler: NativeDispatch,
    ) -> (Self, Lifecycle) {
        let fragment = fragment.into();
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel::<EntityMsg>();
        let handle = tokio::runtime::Handle::current();
        let thread_name = format!("entity-{fragment}");
        std::thread::Builder::new()
            .name(thread_name)
            .spawn(move || run_native_thread(handler, handle, rx))
            .expect("failed to spawn native entity thread");

        let ep = Self {
            fragment,
            kind: PluginKind::Stateless, // native entities dispatch via a single closure
            acl: node.acl.clone(),
            parent: node.parent.clone(),
            native: true,
            tx,
        };
        (ep, Lifecycle::Running)
    }

    /// Returns `true` if this plugin is backed by a compiled-in Rust closure
    /// (i.e. not an Extism/Wasm entity).  Native entities are not stored in
    /// the IPFS manifest and must be skipped during manifest-bound operations.
    pub fn is_native(&self) -> bool {
        self.native
    }

    /// Load a Wasm plugin from IPFS, spawn its worker thread, and initialise it.
    ///
    /// The Wasm `Plugin` is created and driven entirely on the dedicated worker
    /// thread — it never crosses a thread boundary — so no `unsafe impl Send`
    /// is required and no Tokio worker is ever blocked by a Wasm call.
    ///
    /// Returns `(handle, Lifecycle::Running)` on success, or
    /// `(handle, Lifecycle::Error)` if `init()` returned `[:error, …]` (the
    /// entity is still dispatchable for debugging).
    /// Returns `Err` for fatal errors (Wasm fetch / plugin instantiation), or
    /// when `kind_node.evaluator == Evaluator::Native` (use
    /// [`EntityPlugin::new_native`] instead).
    #[allow(clippy::too_many_lines, clippy::too_many_arguments)]
    pub async fn load(
        fragment: impl Into<String>,
        node: &EntityNode,
        kind_node: &KindNode,
        our_did: &str,
        kubo_url: &str,
        envelope_tx: UnboundedSender<(String, SendEnvelope)>,
        entity_registry: EntityRegistry,
        avatar_key: [u8; 32],
    ) -> Result<(Self, Lifecycle)> {
        let fragment = fragment.into();
        let behaviour_cid = node.behaviour.as_ref().map(|l| l.cid.as_str());
        let kind = kind_node.plugin_kind();
        let wasi = kind_node.wasi();

        // Native entities must be registered via new_native(), not load().
        if kind_node.evaluator == Evaluator::Native {
            return Err(anyhow!(
                "entity '{fragment}' has evaluator 'native': use EntityPlugin::new_native() instead of load()"
            ));
        }

        // Only Extism is supported beyond this point.
        if kind_node.evaluator != Evaluator::Extism {
            return Err(anyhow!(
                "unsupported evaluator {:?} for '{fragment}'",
                kind_node.evaluator
            ));
        }
        let behaviour_cid = behaviour_cid.ok_or_else(|| {
            anyhow!("entity '{fragment}' has evaluator 'extism' but no behaviour CID")
        })?;

        debug!(fragment = %fragment, cid = %behaviour_cid, kind = ?kind, wasi = wasi, "loading entity plugin");

        // 1. Fetch Wasm bytes from IPFS (async, before the thread is spawned).
        let wasm_bytes = cat_bytes(kubo_url, behaviour_cid)
            .await
            .with_context(|| format!("fetching wasm for '{fragment}' from {behaviour_cid}"))?;

        // 2. For stateful plugins: fetch persisted state so StateCtx has the
        //    correct baseline and init() can restore it.  Stateless plugins
        //    have no state; module-level code handles any one-time setup.
        let init_state: Vec<u8> = if kind == PluginKind::Stateful {
            match &node.state {
                Some(link) => cat_bytes(kubo_url, &link.cid).await.unwrap_or_default(),
                None => Vec::new(),
            }
        } else {
            Vec::new()
        };

        // 3. Assemble all Send-able data the worker thread needs.  The Wasm
        //    Plugin and its host Functions are built *on* the thread.
        let cfg = WasmThreadCfg {
            fragment: fragment.clone(),
            our_did: our_did.to_string(),
            wasm_bytes,
            init_state,
            wasi,
            host_functions: kind_node.host_functions.clone(),
            has_init: kind_node.api.iter().any(|s| s == "init"),
            node_kind: node.kind.clone(),
            parent: node.parent.clone(),
            lifecycle: node.lifecycle.clone(),
            envelope_tx,
            entity_registry,
            avatar_key,
        };

        let (tx, rx) = tokio::sync::mpsc::unbounded_channel::<EntityMsg>();
        let (life_tx, life_rx) = oneshot::channel::<Result<Lifecycle>>();
        let thread_name = format!("entity-{fragment}");
        std::thread::Builder::new()
            .name(thread_name)
            .spawn(move || run_wasm_thread(cfg, rx, life_tx))
            .with_context(|| format!("spawning worker thread for '{fragment}'"))?;

        // Wait for the thread to build the plugin and run init().
        let lifecycle = match life_rx.await {
            Ok(Ok(lc)) => lc,
            Ok(Err(e)) => return Err(e),
            Err(_) => {
                return Err(anyhow!(
                    "entity '{fragment}' worker thread exited before init completed"
                ))
            }
        };

        let ep = Self {
            fragment,
            kind,
            acl: node.acl.clone(),
            parent: node.parent.clone(),
            native: false,
            tx,
        };
        Ok((ep, lifecycle))
    }

    /// Dispatch to the `handle_cast` export (stateless — no state threading).
    pub async fn handle_cast(&self, input: &CastInput) -> Result<DispatchResult> {
        self.dispatch(false, input).await
    }

    /// Dispatch to the `handle_call` export (stateful).
    pub async fn handle_call(&self, input: &CastInput) -> Result<DispatchResult> {
        self.dispatch(true, input).await
    }

    /// Send a non-capturing dispatch to the worker thread and await the result.
    async fn dispatch(&self, stateful: bool, input: &CastInput) -> Result<DispatchResult> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.tx
            .send(EntityMsg::Dispatch {
                stateful,
                capture: false,
                call_path: Vec::new(),
                input: input.clone(),
                reply: reply_tx,
            })
            .map_err(|_| anyhow!("entity '{}' worker thread is gone", self.fragment))?;
        match reply_rx.await {
            Ok(res) => res,
            Err(_) => Err(anyhow!("entity '{}' dropped dispatch reply", self.fragment)),
        }
    }

    /// Record a successful IPFS persist: update the persisted snapshot and
    /// clear the dirty flag.  No-op (ignored by the thread) for native entities.
    pub fn mark_saved(&self, saved_bytes: Vec<u8>) {
        let _ = self.tx.send(EntityMsg::MarkSaved(saved_bytes));
    }

    /// Persist any state queued by `ma_set_state` during the last dispatch.
    ///
    /// Plugins call `ma_set_state` reactively inside `handle_call`; this method
    /// flushes whatever is still queued to IPFS and returns the resulting CID.
    /// Returns `Ok(None)` when there is no pending state (nothing to save).
    /// Always returns `Ok(None)` for native entities (they manage their own state).
    pub async fn trigger_save(&self, kubo_url: &str) -> Result<Option<String>> {
        if self.native {
            return Ok(None);
        }
        let (reply_tx, reply_rx) = oneshot::channel();
        self.tx
            .send(EntityMsg::TakePending { reply: reply_tx })
            .map_err(|_| anyhow!("entity '{}' worker thread is gone", self.fragment))?;
        let pending = reply_rx
            .await
            .map_err(|_| anyhow!("entity '{}' dropped TakePending reply", self.fragment))?;

        if let Some(bytes) = pending {
            let cid = ipfs_add(kubo_url, bytes.clone())
                .await
                .map_err(|e| anyhow!("ipfs_add for '{}' state: {e}", self.fragment))?;
            self.mark_saved(bytes);
            Ok(Some(cid))
        } else {
            Ok(None)
        }
    }
}

// ── Worker threads ────────────────────────────────────────────────────────────

/// Everything a Wasm entity's worker thread needs to build and run its plugin.
/// All fields are `Send`; the non-`Send` `Plugin` is constructed on the thread.
struct WasmThreadCfg {
    fragment: String,
    our_did: String,
    wasm_bytes: Vec<u8>,
    init_state: Vec<u8>,
    wasi: bool,
    host_functions: Vec<String>,
    has_init: bool,
    node_kind: String,
    parent: Option<String>,
    lifecycle: Lifecycle,
    envelope_tx: UnboundedSender<(String, SendEnvelope)>,
    entity_registry: EntityRegistry,
    avatar_key: [u8; 32],
}

/// Handles retained by the worker thread to drain plugin side-effects after
/// each dispatch and to service state messages.
struct WasmThreadState {
    plugin: Plugin,
    state: UserData<StateCtx>,
    create_queue: UserData<CreateEntityCtx>,
    delete_queue: UserData<DeleteEntityCtx>,
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
    });
    let call_ctx: UserData<CallCtx> = UserData::new(CallCtx {
        registry: cfg.entity_registry.clone(),
        caller_did: format!("{}#{}", cfg.our_did, &cfg.fragment),
        caller_fragment: cfg.fragment.clone(),
    });
    let avatar_id_ctx: UserData<AvatarIdCtx> = UserData::new(AvatarIdCtx { key: cfg.avatar_key });

    // IMPORTANT: This order MUST match the @extism.import_fn declaration order
    // in the Python actor library chain (actor.py → avatar.py / root.py).
    // extism-py assigns IMPORT_INDEX sequentially across all @extism.import_fn
    // declarations, and ffi.__invoke_host_func(idx) indexes into the FILTERED
    // host_fns array by position.  The filter preserves all_fns order, so
    // IMPORT_INDEX must equal the position in this list after filtering.
    //
    // Python IMPORT_INDEX assignments:
    //   actor.py:  ma_reply=0, ma_set_state=1, ma_send=2, ma_call=3
    //   avatar.py: ma_avatar_id=4
    //   root.py:   ma_create_entity=4, ma_delete_entity=5
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
    let allowed: std::collections::HashSet<&str> = cfg
        .host_functions
        .iter()
        .map(String::as_str)
        .collect();
    let host_fns: Vec<Function> = all_fns
        .into_iter()
        .filter(|(name, _)| allowed.contains(*name))
        .map(|(_, f)| f)
        .collect();

    let manifest = Manifest::new([Wasm::data(cfg.wasm_bytes.clone())]);
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
fn run_init(ts: &mut WasmThreadState, cfg: &WasmThreadCfg) -> Result<Lifecycle> {
    if !cfg.has_init {
        return Ok(Lifecycle::Running);
    }
    let ctx_for_init = EntityCtx {
        self_did: format!("{}#{}", cfg.our_did, &cfg.fragment),
        fragment: cfg.fragment.clone(),
        kind: cfg.node_kind.clone(),
        parent: cfg.parent.clone(),
        lifecycle: cfg.lifecycle.clone(),
    };
    let init_payload = InitPayload {
        ctx: &ctx_for_init,
        state: cfg.init_state.clone(),
    };
    let mut init_bytes = Vec::new();
    ciborium::ser::into_writer(&init_payload, &mut init_bytes).context("encoding init payload")?;
    let init_result_bytes = ts
        .plugin
        .call::<&[u8], Vec<u8>>("init", init_bytes.as_slice())
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
    let export = if stateful { "handle_call" } else { "handle_cast" };
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

    let delete_requests = ts
        .delete_queue
        .get()
        .map_err(|e| anyhow!("delete_queue error: {e}"))?
        .lock()
        .map_err(|e| anyhow!("delete_queue poisoned: {e}"))?
        .pending
        .drain(..)
        .collect();

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
fn run_wasm_thread(
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
                let res = execute_dispatch(
                    &mut ts,
                    &cfg.fragment,
                    stateful,
                    capture,
                    call_path,
                    &input,
                );
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
fn run_native_thread(
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
