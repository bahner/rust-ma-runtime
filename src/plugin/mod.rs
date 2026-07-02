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

use std::{cell::RefCell, collections::HashMap, sync::Arc};

use anyhow::{anyhow, Context, Result};
use ma_core::{cat_bytes, ipfs_add};
use tokio::sync::{mpsc::UnboundedSender, oneshot, RwLock};
use tracing::debug;

use crate::entity::{
    CastInput, CreateEntityRequest, EntityNode, Evaluator, KindNode, Lifecycle, PluginKind,
    SendEnvelope,
};

mod backend;
use backend::{run_native_thread, run_wasm_thread, WasmThreadCfg};

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
    #[allow(clippy::option_option)] // the three states are meaningful, see above
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
    pub const fn is_native(&self) -> bool {
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
            handle: tokio::runtime::Handle::current(),
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
        reply_rx
            .await
            .unwrap_or_else(|_| Err(anyhow!("entity '{}' dropped dispatch reply", self.fragment)))
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

#[cfg(test)]
mod wasm_repro {
    //! Local reproduction harness for plugin WASM crashes.
    //! Requires a local Kubo node with the plugin CID pinned.
    //! Run: cargo test wasm_repro -- --ignored --nocapture

    use std::collections::BTreeMap;

    use crate::entity::{
        CastInput, EntityNode, Evaluator, IpldLink, KindNode, Lifecycle, PluginMsg, SendEnvelope,
    };

    use super::{new_entity_registry, EntityPlugin};

    #[tokio::test(flavor = "multi_thread")]
    #[ignore = "requires local Kubo with the restaurant.wasm CID"]
    async fn dispatch_restaurant_wasm() {
        let cid = std::env::var("WASM_CID").unwrap_or_else(|_| {
            "bafybeihz77pxaep345puckusx2h6lrkh4e3ecta42bfgoaa7oopcogf32e".into()
        });
        let kubo = "http://127.0.0.1:5001";

        let mut attributes = BTreeMap::new();
        attributes.insert("stateful".to_string(), serde_json::Value::Bool(true));
        attributes.insert("wasi".to_string(), serde_json::Value::Bool(true));

        let kind_node = KindNode {
            protocol: "/ma/room/0.0.1".to_string(),
            api: vec!["init".to_string(), "handle_call".to_string()],
            host_functions: vec![
                "ma_reply".to_string(),
                "ma_set_state".to_string(),
                "ma_send".to_string(),
                "ma_call".to_string(),
            ],
            evaluator: Evaluator::Extism,
            attributes,
            allow: vec![],
        };

        let entity_node = EntityNode {
            kind: "/ma/room/0.0.1".to_string(),
            behaviour: Some(IpldLink::new(&cid)),
            acl: "open".to_string(),
            state: None,
            parent: None,
            label: Some("Test Room".to_string()),
            lifecycle: Lifecycle::New,
        };

        let (envelope_tx, mut envelope_rx) =
            tokio::sync::mpsc::unbounded_channel::<(String, SendEnvelope)>();
        let registry = new_entity_registry();

        println!("Loading wasm from {cid} ...");
        let (ep, lifecycle) = EntityPlugin::load(
            "room",
            &entity_node,
            &kind_node,
            "did:ma:testrunner",
            kubo,
            envelope_tx,
            registry.clone(),
            [7u8; 32],
        )
        .await
        .expect("plugin load");
        println!("Loaded. lifecycle = {lifecycle}");

        for verb in [":menu", ":ping", ":look"] {
            let mut content = Vec::new();
            ciborium::ser::into_writer(&ciborium::Value::Text(verb.to_string()), &mut content)
                .unwrap();

            let msg = PluginMsg {
                id: format!("test-{}", verb.trim_start_matches(':')),
                from: "did:ma:k51qzi5uqu5dlgh2drt9od7f7fmfe1u6rf5j2s2acfp9olltfx51oqhnl048xm"
                    .to_string(),
                to: "did:ma:testrunner#room".to_string(),
                reply_to: None,
                content_type: "application/x-ma-term".to_string(),
                content,
            };

            println!("\n=== dispatch {verb} ===");
            match ep.handle_call(&CastInput { msg }).await {
                Ok(result) => {
                    println!("OK, output {} bytes", result.output.len());
                    while let Ok((frag, env)) = envelope_rx.try_recv() {
                        let val: ciborium::Value =
                            ciborium::de::from_reader(env.content.as_slice()).unwrap();
                        println!("  reply from #{frag} -> {val:?}");
                    }
                }
                Err(e) => panic!("CRASH on {verb}: {e}"),
            }
        }
    }
}
