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
//! Both backends implement the same dispatch surface: [`EntityPlugin::on_message`].
//! The [`PluginKind`] field determines whether state is threaded in/out (for
//! Extism) or which path the closure takes (for Native) — the Wasm export
//! called is always `on_message`, regardless of statefulness.

use std::{collections::HashMap, sync::Arc};

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
// worker thread.  Entities communicate exclusively via `ma_send` (fire-and-
// forget).  There is no synchronous inter-entity call primitive.

/// Messages sent to an entity's dedicated worker thread.
enum EntityMsg {
    /// Dispatch a message to the `on_message` export.
    Dispatch {
        /// `true` → stateful (state threaded in/out around the call);
        /// `false` → stateless (no state threading). The export called is
        /// always `on_message` either way.
        stateful: bool,
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

// ── Native dispatch type ─────────────────────────────────────────────────────

/// Type of the compiled-in Rust closure used by native entity plugins.
///
/// The closure receives a [`CastInput`] and returns a [`DispatchResult`],
/// exactly like a Wasm `on_message` export.
/// [`EntityPlugin::on_message`] routes through this closure for native
/// entities — native entities do not distinguish stateful vs stateless
/// internally (the closure owns its own state via `Arc<Mutex<…>>` or similar).
pub type NativeDispatch =
    std::sync::Arc<dyn Fn(&CastInput) -> anyhow::Result<DispatchResult> + Send + Sync>;

// ── Dispatch result ─────────────────────────────────────────────────────────

/// Return value from `on_message`.
pub struct DispatchResult {
    /// Raw CBOR bytes returned by the plugin export.
    pub output: Vec<u8>,
    /// State bytes queued by the plugin via `ma_set_state` host function.
    /// `None` if the plugin did not call `ma_set_state` during this invocation.
    pub pending_state: Option<Vec<u8>>,
    /// New behaviour reference (single CID) queued by the plugin via
    /// `ma_set_behaviour_cid` during this invocation, if any.
    pub pending_behaviour_cid: Option<String>,
    /// Entity creation requests enqueued via `ma_create_entity` host function.
    pub create_requests: Vec<CreateEntityRequest>,
    /// Entity deletion requests enqueued via `ma_delete_entity` host function.
    /// Each entry is the fragment of the entity to delete.
    pub delete_requests: Vec<String>,
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

    /// Load a Wasm plugin from IPFS, spawn its worker thread, and drive it
    /// through the applicable lifecycle stages (`set_state`/`set_behaviour`/
    /// `do_init`/`do_start`) in order.
    ///
    /// The Wasm `Plugin` is created and driven entirely on the dedicated worker
    /// thread — it never crosses a thread boundary — so no `unsafe impl Send`
    /// is required and no Tokio worker is ever blocked by a Wasm call.
    ///
    /// `init_payload` is the opaque creation payload for `do_init` (§14.2.1);
    /// pass `None` for an ordinary reload (bootstrap/restart) of an
    /// already-existing entity. It is only meaningful when `node.lifecycle`
    /// is [`Lifecycle::New`] (this entity's very first load) — a kind
    /// exporting `do_init` receives it exactly once, ever, for a given entity.
    ///
    /// Returns `(handle, Lifecycle::Running)` on success, or
    /// `(handle, Lifecycle::Error)` if `do_init` returned `[:error, …]` (the
    /// entity is still dispatchable for debugging).
    /// Returns `Err` for fatal errors (Wasm fetch / plugin instantiation), or
    /// when `kind_node.kind_type == Evaluator::Native` (use
    /// [`EntityPlugin::new_native`] instead).
    #[allow(clippy::too_many_lines, clippy::too_many_arguments)]
    pub async fn load(
        fragment: impl Into<String>,
        node: &EntityNode,
        kind_node: &KindNode,
        our_did: &str,
        kubo_url: &str,
        envelope_tx: UnboundedSender<(String, SendEnvelope)>,
        _entity_registry: EntityRegistry,
        avatar_key: [u8; 32],
        iroh_node_id: &str,
        started_at: u64,
        init_payload: Option<Vec<u8>>,
    ) -> Result<(Self, Lifecycle)> {
        let fragment = fragment.into();
        let kind = kind_node.plugin_kind();
        let wasi = kind_node.wasi();

        // Native entities must be registered via new_native(), not load().
        if kind_node.kind_type == Evaluator::Native {
            return Err(anyhow!(
                "entity '{fragment}' has kind type 'native': use EntityPlugin::new_native() instead of load()"
            ));
        }

        // Only Extism is supported beyond this point.
        if kind_node.kind_type != Evaluator::Extism {
            return Err(anyhow!(
                "unsupported kind type {:?} for '{fragment}'",
                kind_node.kind_type
            ));
        }

        // Wasm bytes source depends on whether this kind shares one binary
        // across all its entities (`cid` present) or lets each entity supply
        // its own (`cid` absent — the entity's own Wasm bytes live on
        // `EntityNode.behaviour` instead, instantiated as-is, never resolved
        // as interpreted text).
        let (wasm_cid, wasm_bytes, entity_behaviour_cid, behaviour_text) = if let Some(shared_cid) =
            &kind_node.cid
        {
            let wasm_cid = shared_cid.cid.clone();
            debug!(fragment = %fragment, cid = %wasm_cid, kind = ?kind, wasi = wasi, "loading entity plugin (shared binary)");
            let wasm_bytes = cat_bytes(kubo_url, &wasm_cid)
                .await
                .with_context(|| format!("fetching wasm for '{fragment}' from {wasm_cid}"))?;

            // If this kind declares a behaviour dialect and the entity
            // carries its own behaviour source reference, resolve it to
            // plain text for `set_behaviour`.
            let entity_behaviour_cid = node.behaviour.as_ref().map(|l| l.cid.clone());
            let behaviour_text: Option<Vec<u8>> = if kind_node.behaviour.is_some() {
                match &entity_behaviour_cid {
                    Some(cid) => Some(cat_bytes(kubo_url, cid).await.with_context(|| {
                        format!("fetching behaviour for '{fragment}' from {cid}")
                    })?),
                    None => None,
                }
            } else {
                None
            };
            (wasm_cid, wasm_bytes, entity_behaviour_cid, behaviour_text)
        } else {
            // No shared binary for this kind — the entity must supply its
            // own Wasm bytes via `behaviour`.
            let entity_cid = node.behaviour.as_ref().map(|l| l.cid.clone()).ok_or_else(|| {
                    anyhow!(
                        "entity '{fragment}' has kind '{}' with no shared cid: entity must supply its own Wasm via behaviour",
                        kind_node.protocol
                    )
                })?;
            debug!(fragment = %fragment, cid = %entity_cid, kind = ?kind, wasi = wasi, "loading entity plugin (own binary via behaviour)");
            let wasm_bytes = cat_bytes(kubo_url, &entity_cid)
                .await
                .with_context(|| format!("fetching wasm for '{fragment}' from {entity_cid}"))?;
            (entity_cid, wasm_bytes, None, None)
        };

        // For stateful plugins: fetch persisted state so StateCtx has the
        // correct baseline and set_state can restore it.  Stateless plugins
        // have no state; module-level code handles any one-time setup.
        let init_state: Vec<u8> = if kind == PluginKind::Stateful {
            match &node.state {
                Some(link) => cat_bytes(kubo_url, &link.cid).await.unwrap_or_default(),
                None => Vec::new(),
            }
        } else {
            Vec::new()
        };

        let is_genesis = node.lifecycle == Lifecycle::New;

        // Assemble all Send-able data the worker thread needs.  The Wasm
        // Plugin and its host Functions are built *on* the thread.
        let cfg = WasmThreadCfg {
            fragment: fragment.clone(),
            our_did: our_did.to_string(),
            wasm_bytes,
            init_state,
            wasi,
            host_functions: kind_node.host_functions.clone(),
            has_set_state: kind_node.lifecycle.iter().any(|s| s == "set_state"),
            has_set_behaviour: kind_node.lifecycle.iter().any(|s| s == "set_behaviour"),
            has_do_init: kind_node.lifecycle.iter().any(|s| s == "do_init"),
            has_do_start: kind_node.lifecycle.iter().any(|s| s == "do_start"),
            has_do_shutdown: kind_node.api.iter().any(|s| s == "do_shutdown"),
            is_genesis,
            init_payload,
            behaviour_text,
            node_kind: node.kind.clone(),
            envelope_tx,
            avatar_key,
            wasm_cid,
            entity_behaviour_cid,
            kubo_url: kubo_url.to_string(),
            tokio_handle: tokio::runtime::Handle::current(),
            iroh_node_id: iroh_node_id.to_string(),
            started_at,
            parent: node.parent.clone(),
        };

        let (tx, rx) = tokio::sync::mpsc::unbounded_channel::<EntityMsg>();
        let (life_tx, life_rx) = oneshot::channel::<Result<Lifecycle>>();
        let thread_name = format!("entity-{fragment}");
        std::thread::Builder::new()
            .name(thread_name)
            .spawn(move || run_wasm_thread(cfg, rx, life_tx))
            .with_context(|| format!("spawning worker thread for '{fragment}'"))?;

        // Wait for the thread to build the plugin and run the genesis/start
        // lifecycle stages.  Bounded: Wasm *execution* is capped by the
        // extism epoch timeout, but plugin *instantiation* (compilation) is
        // not — a pathological module could otherwise hang bootstrap or a
        // reload task forever.
        let load_timeout = backend::wasm_call_timeout() * 2;
        let lifecycle = match tokio::time::timeout(load_timeout, life_rx).await {
            Ok(Ok(Ok(lc))) => lc,
            Ok(Ok(Err(e))) => return Err(e),
            Ok(Err(_)) => {
                return Err(anyhow!(
                    "entity '{fragment}' worker thread exited before genesis/start completed"
                ))
            }
            Err(_) => {
                // The worker thread is left to its fate: if it ever finishes
                // building, it exits on its own because `life_tx.send` fails.
                return Err(anyhow!(
                    "entity '{fragment}' plugin build/genesis timed out after {}s",
                    load_timeout.as_secs()
                ));
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

    /// Dispatch to the `on_message` export. Threads state in/out around the
    /// call automatically based on `self.kind` (stateful vs stateless) —
    /// callers never need to branch on kind themselves.
    pub async fn on_message(&self, input: &CastInput) -> Result<DispatchResult> {
        self.dispatch(self.kind == PluginKind::Stateful, input)
            .await
    }

    /// Send a dispatch to the worker thread and await the result.
    ///
    /// Bounded by a backstop timeout slightly above the Wasm execution cap:
    /// no caller can ever wait forever on a wedged worker, even if the epoch
    /// interrupt itself were to fail.
    async fn dispatch(&self, stateful: bool, input: &CastInput) -> Result<DispatchResult> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.tx
            .send(EntityMsg::Dispatch {
                stateful,
                input: input.clone(),
                reply: reply_tx,
            })
            .map_err(|_| anyhow!("entity '{}' worker thread is gone", self.fragment))?;
        // 2× the Wasm cap: the dispatch may sit behind one already-running
        // call (≤ 1× cap) before its own execution starts (≤ 1× cap).
        let backstop = backend::wasm_call_timeout() * 2;
        tokio::time::timeout(backstop, reply_rx).await.map_or_else(
            |_| {
                Err(anyhow!(
                    "entity '{}' dispatch timed out after {}s (worker wedged?)",
                    self.fragment,
                    backstop.as_secs()
                ))
            },
            |reply| {
                reply.unwrap_or_else(|_| {
                    Err(anyhow!("entity '{}' dropped dispatch reply", self.fragment))
                })
            },
        )
    }

    /// Record a successful IPFS persist: update the persisted snapshot and
    /// clear the dirty flag.  No-op (ignored by the thread) for native entities.
    pub fn mark_saved(&self, saved_bytes: Vec<u8>) {
        let _ = self.tx.send(EntityMsg::MarkSaved(saved_bytes));
    }

    /// Persist any state queued by `ma_set_state` during the last dispatch.
    ///
    /// Plugins call `ma_set_state` reactively inside `on_message`; this method
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
        // Bounded: the TakePending may sit behind one wedged dispatch
        // (≤ 1× Wasm cap) — never wait forever (shutdown path uses this).
        let backstop = backend::wasm_call_timeout() * 2;
        let pending = match tokio::time::timeout(backstop, reply_rx).await {
            Ok(reply) => reply
                .map_err(|_| anyhow!("entity '{}' dropped TakePending reply", self.fragment))?,
            Err(_) => {
                return Err(anyhow!(
                    "entity '{}' TakePending timed out after {}s (worker wedged?)",
                    self.fragment,
                    backstop.as_secs()
                ))
            }
        };

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
mod hostile {
    //! Hostile-plugin isolation tests: no matter how spectacularly a Wasm
    //! module misbehaves at load or runtime, it must never block anything
    //! beyond itself, and every failure must surface as a bounded `Err`.
    //!
    //! Uses [`crate::testkubo::MockKubo`] to serve the Wasm bytes — no real
    //! Kubo or network needed.

    use std::collections::BTreeMap;
    use std::time::{Duration, Instant};

    use crate::entity::{
        CastInput, EntityNode, Evaluator, IpldLink, KindNode, Lifecycle, PluginMsg, SendEnvelope,
    };
    use crate::testkubo::MockKubo;

    use super::{new_entity_registry, EntityPlugin};

    /// A module whose every export spins forever.
    const EVIL_WAT: &str = r#"
        (module
          (func $spin (result i32) (loop $l (br $l)) (i32.const 0))
          (export "do_start" (func $spin))
          (export "on_message" (func $spin)))
    "#;

    /// A module whose exports return immediately.
    const GOOD_WAT: &str = r#"
        (module
          (func $ok (result i32) (i32.const 0))
          (export "on_message" (func $ok)))
    "#;

    fn kind_node(api: &[&str], wasm_cid: &str) -> KindNode {
        let mut attributes = BTreeMap::new();
        attributes.insert("stateful".to_string(), serde_json::Value::Bool(true));
        attributes.insert("wasi".to_string(), serde_json::Value::Bool(false));
        const STAGES: &[&str] = &["set_state", "set_behaviour", "do_init", "do_start"];
        let lifecycle = api
            .iter()
            .filter(|s| STAGES.contains(s))
            .map(ToString::to_string)
            .collect();
        KindNode {
            protocol: "/ma/test/0.0.1".to_string(),
            cid: Some(IpldLink::new(wasm_cid)),
            kind_type: Evaluator::Extism,
            behaviour: None,
            api: api.iter().map(ToString::to_string).collect(),
            lifecycle,
            host_functions: vec![],
            attributes,
            allow: vec![],
        }
    }

    fn entity_node() -> EntityNode {
        EntityNode {
            kind: "/ma/test/0.0.1".to_string(),
            behaviour: None,
            acl: "open".to_string(),
            state: None,
            parent: None,
            label: None,
            lifecycle: Lifecycle::New,
        }
    }

    fn cast_input(id: &str) -> CastInput {
        let mut content = Vec::new();
        ciborium::ser::into_writer(&ciborium::Value::Text(":poke".into()), &mut content).unwrap();
        CastInput {
            msg: PluginMsg {
                id: id.to_string(),
                from: "did:ma:tester".to_string(),
                to: "did:ma:testrunner#x".to_string(),
                reply_to: None,
                message_type: ma_core::MESSAGE_TYPE_RPC.to_string(),
                content_type: "application/x-ma-term".to_string(),
                content,
            },
        }
    }

    async fn load(
        kubo_url: &str,
        fragment: &str,
        cid: &str,
        api: &[&str],
        envelope_tx: tokio::sync::mpsc::UnboundedSender<(String, SendEnvelope)>,
        registry: super::EntityRegistry,
    ) -> anyhow::Result<(EntityPlugin, Lifecycle)> {
        EntityPlugin::load(
            fragment,
            &entity_node(),
            &kind_node(api, cid),
            "did:ma:testrunner",
            kubo_url,
            envelope_tx,
            registry,
            [7u8; 32],
            "",
            0,
            None,
        )
        .await
    }

    /// One combined test (not parallel-safe pieces): the Wasm timeout env var
    /// is process-global, so all hostile scenarios run under one setting.
    #[tokio::test(flavor = "multi_thread")]
    #[allow(clippy::too_many_lines)]
    async fn hostile_wasm_never_blocks_anything_else() {
        std::env::set_var("MA_WASM_CALL_TIMEOUT_SECS", "2");
        let kubo = MockKubo::start().await;
        let (envelope_tx, _envelope_rx) =
            tokio::sync::mpsc::unbounded_channel::<(String, SendEnvelope)>();
        let registry = new_entity_registry();

        let evil_cid = kubo.add_bytes(wat::parse_str(EVIL_WAT).unwrap()).await;
        let good_cid = kubo.add_bytes(wat::parse_str(GOOD_WAT).unwrap()).await;
        let garbage_cid = kubo.add_bytes(b"this is not wasm at all".to_vec()).await;

        // ── 1. Garbage bytes: load fails cleanly and quickly. ────────────────
        let t = Instant::now();
        let res = load(
            kubo.url(),
            "garbage",
            &garbage_cid,
            &["on_message"],
            envelope_tx.clone(),
            registry.clone(),
        )
        .await;
        assert!(res.is_err(), "garbage wasm must fail to load");
        assert!(
            t.elapsed() < Duration::from_secs(5),
            "garbage load not bounded: {:?}",
            t.elapsed()
        );

        // ── 2. Infinite loop in do_start(): load fails within the bound. ────
        let t = Instant::now();
        let res = load(
            kubo.url(),
            "evil-init",
            &evil_cid,
            &["do_start", "on_message"],
            envelope_tx.clone(),
            registry.clone(),
        )
        .await;
        assert!(res.is_err(), "infinite do_start must fail, not hang");
        assert!(
            t.elapsed() < Duration::from_secs(10),
            "infinite do_start not bounded: {:?}",
            t.elapsed()
        );

        // ── 3. Infinite loop in on_message: dispatch errors within the
        //       bound, and a healthy entity is fully responsive meanwhile. ────
        let (evil, _) = load(
            kubo.url(),
            "evil",
            &evil_cid,
            &["on_message"], // no do_start → load succeeds
            envelope_tx.clone(),
            registry.clone(),
        )
        .await
        .expect("evil load (no do_start) should succeed");
        let (good, _) = load(
            kubo.url(),
            "good",
            &good_cid,
            &["on_message"],
            envelope_tx.clone(),
            registry.clone(),
        )
        .await
        .expect("good load should succeed");
        let evil = std::sync::Arc::new(evil);
        registry
            .write()
            .await
            .insert("evil".to_string(), evil.clone());

        // Kick off the wedging dispatch.
        let evil_clone = evil.clone();
        let wedged =
            tokio::spawn(async move { evil_clone.on_message(&cast_input("wedge-1")).await });

        // While evil spins, the good entity must answer immediately.
        tokio::time::sleep(Duration::from_millis(200)).await;
        let t = Instant::now();
        good.on_message(&cast_input("good-1"))
            .await
            .expect("good entity must not be affected by evil one");
        assert!(
            t.elapsed() < Duration::from_secs(1),
            "good entity was starved: {:?}",
            t.elapsed()
        );

        // The wedged dispatch must come back as a bounded error.
        let t = Instant::now();
        let res = wedged.await.expect("join");
        assert!(res.is_err(), "infinite on_message must error, got Ok");
        assert!(
            t.elapsed() < Duration::from_secs(10),
            "wedged dispatch not bounded: {:?}",
            t.elapsed()
        );

        // ── 4. The evil worker survives the epoch abort and stays bounded. ───
        let t = Instant::now();
        let res2 = evil.on_message(&cast_input("wedge-2")).await;
        assert!(res2.is_err(), "second dispatch must also error");
        assert!(
            t.elapsed() < Duration::from_secs(10),
            "second wedged dispatch not bounded: {:?}",
            t.elapsed()
        );

        // ── 5. Reload over a wedged entity works: replace registry entry. ───
        let (evil2, _) = load(
            kubo.url(),
            "evil",
            &good_cid, // "fixed" version
            &["on_message"],
            envelope_tx.clone(),
            registry.clone(),
        )
        .await
        .expect("reload over wedged entity should succeed");
        registry
            .write()
            .await
            .insert("evil".to_string(), std::sync::Arc::new(evil2));
        let reloaded = registry.read().await.get("evil").cloned().unwrap();
        reloaded
            .on_message(&cast_input("post-reload"))
            .await
            .expect("reloaded (fixed) entity must dispatch fine");

        std::env::remove_var("MA_WASM_CALL_TIMEOUT_SECS");
    }
}

#[cfg(test)]
mod wasm_repro {
    //! Local reproduction harness for plugin WASM crashes.
    //! Requires a local Kubo node with the plugin CID pinned.
    //! Run: cargo test `wasm_repro` -- --ignored --nocapture

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
            cid: Some(IpldLink::new(&cid)),
            kind_type: Evaluator::Extism,
            behaviour: None,
            api: vec!["do_start".to_string(), "on_message".to_string()],
            lifecycle: vec!["do_start".to_string()],
            host_functions: vec![
                "ma_reply".to_string(),
                "ma_set_state".to_string(),
                "ma_send".to_string(),
            ],
            attributes,
            allow: vec![],
        };

        let entity_node = EntityNode {
            kind: "/ma/room/0.0.1".to_string(),
            behaviour: None,
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
            "",
            0,
            None,
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
                message_type: ma_core::MESSAGE_TYPE_RPC.to_string(),
                content_type: "application/x-ma-term".to_string(),
                content,
            };

            println!("\n=== dispatch {verb} ===");
            match ep.on_message(&CastInput { msg }).await {
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

    /// Reproduction of the "runtime hangs after entity reload" bug:
    /// load an entity, replace it in the registry (as `spawn_entity_reload`
    /// does), then dispatch to the reloaded instance.  The dispatch must
    /// complete within the WASM timeout — a hang here reproduces the bug.
    #[tokio::test(flavor = "multi_thread")]
    #[ignore = "requires local Kubo with the restaurant.wasm CID"]
    #[allow(clippy::too_many_lines)]
    async fn reload_then_dispatch() {
        let cid = std::env::var("WASM_CID").unwrap_or_else(|_| {
            "bafybeihz77pxaep345puckusx2h6lrkh4e3ecta42bfgoaa7oopcogf32e".into()
        });
        let kubo = "http://127.0.0.1:5001";

        let mut attributes = BTreeMap::new();
        attributes.insert("stateful".to_string(), serde_json::Value::Bool(true));
        attributes.insert("wasi".to_string(), serde_json::Value::Bool(true));

        let kind_node = KindNode {
            protocol: "/ma/room/0.0.1".to_string(),
            cid: Some(IpldLink::new(&cid)),
            kind_type: Evaluator::Extism,
            behaviour: None,
            api: vec!["do_start".to_string(), "on_message".to_string()],
            lifecycle: vec!["do_start".to_string()],
            host_functions: vec![
                "ma_reply".to_string(),
                "ma_set_state".to_string(),
                "ma_send".to_string(),
            ],
            attributes,
            allow: vec![],
        };

        let entity_node = EntityNode {
            kind: "/ma/room/0.0.1".to_string(),
            behaviour: None,
            acl: "open".to_string(),
            state: None,
            parent: None,
            label: Some("Test Room".to_string()),
            lifecycle: Lifecycle::New,
        };

        let (envelope_tx, mut envelope_rx) =
            tokio::sync::mpsc::unbounded_channel::<(String, SendEnvelope)>();
        let registry = new_entity_registry();

        // Initial load, as bootstrap does.
        let (ep, _) = EntityPlugin::load(
            "room",
            &entity_node,
            &kind_node,
            "did:ma:testrunner",
            kubo,
            envelope_tx.clone(),
            registry.clone(),
            [7u8; 32],
            "",
            0,
            None,
        )
        .await
        .expect("initial plugin load");
        registry
            .write()
            .await
            .insert("room".to_string(), std::sync::Arc::new(ep));

        // Dispatch once to the original instance (sanity).
        let dispatch = |registry: super::EntityRegistry, verb: &'static str, id: &'static str| async move {
            let mut content = Vec::new();
            ciborium::ser::into_writer(&ciborium::Value::Text(verb.to_string()), &mut content)
                .unwrap();
            let msg = PluginMsg {
                id: id.to_string(),
                from: "did:ma:k51qzi5uqu5dlgh2drt9od7f7fmfe1u6rf5j2s2acfp9olltfx51oqhnl048xm"
                    .to_string(),
                to: "did:ma:testrunner#room".to_string(),
                reply_to: None,
                message_type: ma_core::MESSAGE_TYPE_RPC.to_string(),
                content_type: "application/x-ma-term".to_string(),
                content,
            };
            let ep = registry.read().await.get("room").cloned().unwrap();
            ep.on_message(&CastInput { msg }).await
        };
        tokio::time::timeout(
            std::time::Duration::from_secs(40),
            dispatch(registry.clone(), ":menu", "pre-reload"),
        )
        .await
        .expect("pre-reload dispatch timed out")
        .expect("pre-reload dispatch failed");
        while envelope_rx.try_recv().is_ok() {}

        // Reload, exactly as spawn_entity_reload does: load a second instance
        // with the same registry, then replace the map entry (dropping the old
        // Arc → old worker thread exits).
        println!("=== reloading entity ===");
        let (ep2, _) = EntityPlugin::load(
            "room",
            &entity_node,
            &kind_node,
            "did:ma:testrunner",
            kubo,
            envelope_tx.clone(),
            registry.clone(),
            [7u8; 32],
            "",
            0,
            None,
        )
        .await
        .expect("reload plugin load");
        registry
            .write()
            .await
            .insert("room".to_string(), std::sync::Arc::new(ep2));
        println!("=== entity reloaded; dispatching ===");

        // Dispatch to the reloaded instance. This is where the runtime hangs.
        let result = tokio::time::timeout(
            std::time::Duration::from_secs(40),
            dispatch(registry.clone(), ":menu", "post-reload"),
        )
        .await
        .expect("BUG REPRODUCED: dispatch to reloaded entity hung")
        .expect("post-reload dispatch failed");
        println!("post-reload dispatch OK, {} bytes", result.output.len());

        // And once more, to be sure the worker is still serviceable.
        tokio::time::timeout(
            std::time::Duration::from_secs(40),
            dispatch(registry.clone(), ":look", "post-reload-2"),
        )
        .await
        .expect("BUG REPRODUCED: second dispatch to reloaded entity hung")
        .expect("second post-reload dispatch failed");
    }
}
