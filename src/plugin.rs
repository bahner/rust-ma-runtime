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
    collections::HashMap,
    sync::{Arc, Mutex},
};

use anyhow::{anyhow, Context, Result};
use extism::{host_fn, Function, Manifest, Plugin, UserData, Wasm, PTR};
use ma_core::{cat_bytes, ipfs_add};
use tokio::sync::{mpsc::UnboundedSender, RwLock};
use tracing::{debug, warn};

use crate::entity::{
    CastInput, CreateEntityRequest, EntityCtx, EntityNode, Evaluator, KindNode, Lifecycle,
    PluginKind, ReplyRequest, SendEnvelope,
};

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

// ── Private entity backend ────────────────────────────────────────────────────

/// Backing implementation for an [`EntityPlugin`].
enum EntityBackend {
    /// Wasm module loaded from IPFS via Extism.
    Extism {
        plugin: Mutex<Plugin>,
        state: UserData<StateCtx>,
        create_queue: UserData<CreateEntityCtx>,
        delete_queue: UserData<DeleteEntityCtx>,
    },
    /// Compiled-in Rust closure — no Wasm involved.
    Native(NativeDispatch),
}

// Safety: Plugin does not implement Send upstream, but our usage is exclusively
// through &mut-guarded Mutex calls.  NativeDispatch is Send+Sync by its bound.
unsafe impl Send for EntityBackend {}
unsafe impl Sync for EntityBackend {}

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
    Ok(Vec::new())
});

// `ma_reply` host function: convenience wrapper around `ma_send`.
//
// Plugin passes a CBOR-encoded `ReplyRequest { msg, content }`.  The runtime
// fills in `to` (= msg.from), `reply_to` (= msg.id), and `content_type`
// automatically — plugin only provides the reply body.
host_fn!(ma_reply_fn(user_data: OutboxCtx; input: Vec<u8>) -> Vec<u8> {
    let req: ReplyRequest = from_cbor_bytes(&input)?;
    let envelope = SendEnvelope {
        to: req.msg.from,
        content_type: req.content_type,
        content: req.content,
        reply_to: Some(req.msg.id),
    };
    let arc = user_data.get()?;
    let ctx = arc.lock().unwrap();
    let _ = ctx.tx.send((ctx.fragment.clone(), envelope));
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
    let mut out = Vec::new();
    ciborium::ser::into_writer(&fragment, &mut out)
        .map_err(|e| extism::Error::msg(format!("ma_create_entity: CBOR encode: {e}")))?;
    Ok(out)
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
    let mut ctx = arc.lock().unwrap();
    ctx.pending.push(target);
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
}

// ── Registry type alias ───────────────────────────────────────────────────────

/// Thread-safe map from fragment name (e.g. `"fortune"`) to loaded plugin.
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

// ── EntityPlugin ──────────────────────────────────────────────────────────────

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
    backend: EntityBackend,
}

// Safety: EntityBackend carries its own Send+Sync bounds.
unsafe impl Send for EntityPlugin {}
unsafe impl Sync for EntityPlugin {}

impl EntityPlugin {
    /// Create a native entity plugin backed by a compiled-in Rust closure.
    ///
    /// Use this for built-in system entities (e.g. `#scheduler`) whose
    /// implementation is compiled into the runtime binary.  The closure
    /// receives a [`CastInput`] and returns a [`DispatchResult`].
    ///
    /// Returns `(plugin, Lifecycle::Running)` — native entities are always
    /// immediately running; they never go through `init()`.
    ///
    /// **Do not** call [`EntityPlugin::load`] for [`Evaluator::Native`] kinds;
    /// that function returns `Err` when the evaluator is `Native`.
    pub fn new_native(
        fragment: impl Into<String>,
        node: &EntityNode,
        handler: NativeDispatch,
    ) -> (Self, Lifecycle) {
        let ep = Self {
            fragment: fragment.into(),
            kind: PluginKind::Stateless, // native entities dispatch via a single closure
            acl: node.acl.clone(),
            parent: node.parent.clone(),
            backend: EntityBackend::Native(handler),
        };
        (ep, Lifecycle::Running)
    }

    /// Returns `true` if this plugin is backed by a compiled-in Rust closure
    /// rather than a Wasm module.
    pub fn is_native(&self) -> bool {
        matches!(self.backend, EntityBackend::Native(_))
    }

    /// Load a Wasm plugin from IPFS, initialise it with persisted state (or empty).
    ///
    /// Returns `(plugin, Lifecycle::Running)` on success.
    /// Returns `(plugin, Lifecycle::Error)` if `init()` fails (plugin still usable
    /// for `:debug`/`:dump` calls).
    /// Returns `Err` only for fatal errors (Wasm fetch, plugin instantiation),
    /// or when `kind_node.evaluator == Evaluator::Native` (callers must use
    /// [`EntityPlugin::new_native`] instead).
    #[allow(clippy::too_many_lines)]
    pub async fn load(
        fragment: impl Into<String>,
        node: &EntityNode,
        kind_node: &KindNode,
        our_did: &str,
        kubo_url: &str,
        envelope_tx: UnboundedSender<(String, SendEnvelope)>,
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

        // 1. Fetch Wasm bytes from IPFS.
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

        // 3. Build all possible host functions, then filter to the set declared
        //    in kind_node.host_functions (principle of least privilege).
        let outbox_ctx_send = UserData::new(OutboxCtx {
            tx: envelope_tx.clone(),
            fragment: fragment.clone(),
        });
        let outbox_ctx_reply = UserData::new(OutboxCtx {
            tx: envelope_tx,
            fragment: fragment.clone(),
        });
        let state: UserData<StateCtx> = UserData::new(StateCtx::new(init_state.clone()));
        let create_queue: UserData<CreateEntityCtx> = UserData::new(CreateEntityCtx {
            pending: Vec::new(),
            caller_fragment: fragment.clone(),
        });
        let delete_queue: UserData<DeleteEntityCtx> = UserData::new(DeleteEntityCtx {
            pending: Vec::new(),
        });
        let ctx_for_init = EntityCtx {
            self_did: format!("{}#{}", our_did, &fragment),
            fragment: fragment.clone(),
            kind: node.kind.clone(),
            parent: node.parent.clone(),
            lifecycle: node.lifecycle.clone(),
        };

        let all_fns: Vec<(&str, Function)> = vec![
            (
                "ma_send",
                Function::new("ma_send", [PTR], [PTR], outbox_ctx_send, ma_send_fn),
            ),
            (
                "ma_reply",
                Function::new("ma_reply", [PTR], [PTR], outbox_ctx_reply, ma_reply_fn),
            ),
            (
                "ma_set_state",
                Function::new("ma_set_state", [PTR], [PTR], state.clone(), ma_set_state_fn),
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
        let allowed: std::collections::HashSet<&str> = kind_node
            .host_functions
            .iter()
            .map(String::as_str)
            .collect();
        let host_fns: Vec<Function> = all_fns
            .into_iter()
            .filter(|(name, _)| allowed.contains(*name))
            .map(|(_, f)| f)
            .collect();

        let manifest = Manifest::new([Wasm::data(wasm_bytes)]);
        let mut plugin = tokio::task::block_in_place(|| Plugin::new(&manifest, host_fns, wasi))
            .map_err(|e| anyhow!("failed to create extism plugin for '{fragment}': {e}"))?;

        // 4. Call init() only if the kind declares it in its API.
        //    Kinds without `init` (e.g. stateless plugins) skip this step
        //    and start in the Running state directly.
        //    Return value: :ok → Running, [:error, reason] → Error.
        let lifecycle = if kind_node.api.iter().any(|s| s == "init") {
            let init_payload = InitPayload {
                ctx: &ctx_for_init,
                state: init_state,
            };
            let mut init_bytes = Vec::new();
            ciborium::ser::into_writer(&init_payload, &mut init_bytes)
                .context("encoding init payload")?;
            let init_result_bytes = tokio::task::block_in_place(|| {
                plugin
                    .call::<&[u8], Vec<u8>>("init", init_bytes.as_slice())
                    .map_err(|e| anyhow!("init() failed for '{fragment}': {e}"))
            })?;
            match ciborium::de::from_reader::<ciborium::Value, _>(init_result_bytes.as_slice()) {
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
                    warn!(fragment = %fragment, reason = %reason, "init() returned :error");
                    Lifecycle::Error
                }
                Ok(other) => {
                    warn!(fragment = %fragment, value = ?other, "init() returned unexpected value; treating as :ok");
                    Lifecycle::Running
                }
                Err(_) => Lifecycle::Running,
            }
        } else {
            Lifecycle::Running
        };

        let ep = Self {
            fragment,
            kind,
            acl: node.acl.clone(),
            parent: node.parent.clone(),
            backend: EntityBackend::Extism {
                plugin: Mutex::new(plugin),
                state,
                create_queue,
                delete_queue,
            },
        };
        Ok((ep, lifecycle))
    }

    /// Dispatch to the `handle_cast` export (stateless — no state threading).
    pub fn handle_cast(&self, input: &CastInput) -> Result<DispatchResult> {
        self.dispatch_to("handle_cast", input)
    }

    /// Dispatch to the `handle_call` export (stateful).
    ///
    /// `dirty` is only set if the plugin calls `ma_set_state` with bytes that
    /// actually differ from the last persisted snapshot.
    pub fn handle_call(&self, input: &CastInput) -> Result<DispatchResult> {
        self.dispatch_to("handle_call", input)
    }

    /// Dispatch to the named export (Extism) or the native closure.
    fn dispatch_to(&self, export: &str, input: &CastInput) -> Result<DispatchResult> {
        match &self.backend {
            EntityBackend::Native(f) => f(input),
            EntityBackend::Extism {
                plugin,
                state,
                create_queue,
                delete_queue,
            } => {
                let mut input_bytes = Vec::new();
                ciborium::ser::into_writer(input, &mut input_bytes)
                    .map_err(|e| anyhow!("failed to CBOR-encode CastInput: {e}"))?;

                let output = tokio::task::block_in_place(|| {
                    let mut plugin = plugin
                        .lock()
                        .map_err(|e| anyhow!("plugin mutex poisoned: {e}"))?;
                    plugin
                        .call::<&[u8], Vec<u8>>(export, input_bytes.as_slice())
                        .map_err(|e| anyhow!("{export}() failed for '{}': {e}", self.fragment))
                })?;

                let pending_state = state
                    .get()
                    .map_err(|e| anyhow!("state error: {e}"))?
                    .lock()
                    .map_err(|e| anyhow!("state poisoned: {e}"))?
                    .pending
                    .take();

                let create_requests = create_queue
                    .get()
                    .map_err(|e| anyhow!("create_queue error: {e}"))?
                    .lock()
                    .map_err(|e| anyhow!("create_queue poisoned: {e}"))?
                    .pending
                    .drain(..)
                    .collect();

                let delete_requests = delete_queue
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
                })
            }
        }
    }

    /// Record a successful IPFS persist: update the persisted snapshot and
    /// clear the dirty flag.  No-op for native entities.
    pub fn mark_saved(&self, saved_bytes: Vec<u8>) {
        if let EntityBackend::Extism { state, .. } = &self.backend {
            if let Ok(arc) = state.get() {
                if let Ok(mut ctx) = arc.lock() {
                    ctx.persisted = Some(saved_bytes);
                    ctx.dirty = false;
                }
            }
        }
    }

    /// Tell the plugin to save its state by calling the `save_state` export.
    ///
    /// A well-behaved plugin responds by calling `ma_set_state(bytes)`.  If it
    /// doesn't, `Ok(None)` is returned and `dirty` is unchanged (tough luck).
    /// On IPFS success the `dirty` flag is cleared and the CID is returned.
    /// Always returns `Ok(None)` for native entities (they manage their own state).
    pub async fn trigger_save(&self, kubo_url: &str) -> Result<Option<String>> {
        let EntityBackend::Extism { plugin, state, .. } = &self.backend else {
            return Ok(None);
        };

        tokio::task::block_in_place(|| {
            let mut plugin = plugin
                .lock()
                .map_err(|e| anyhow!("plugin mutex poisoned: {e}"))?;
            plugin
                .call::<&[u8], Vec<u8>>("save_state", b"")
                .map_err(|e| anyhow!("save_state() failed for '{}': {e}", self.fragment))
        })?;

        let pending = state
            .get()
            .map_err(|e| anyhow!("state error: {e}"))?
            .lock()
            .map_err(|e| anyhow!("state poisoned: {e}"))?
            .pending
            .take();

        if let Some(bytes) = pending {
            let cid = ipfs_add(kubo_url, bytes.clone())
                .await
                .map_err(|e| anyhow!("ipfs_add for '{}' state: {e}", self.fragment))?;
            self.mark_saved(bytes);
            Ok(Some(cid))
        } else {
            warn!(fragment = %self.fragment, "save_state export did not call ma_set_state");
            Ok(None)
        }
    }
}

// ── CBOR helpers ──────────────────────────────────────────────────────────────

fn from_cbor_bytes<T: serde::de::DeserializeOwned>(bytes: &[u8]) -> Result<T> {
    ciborium::de::from_reader(bytes).map_err(|e| anyhow!("CBOR decode error: {e}"))
}
