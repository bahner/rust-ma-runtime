//! Extism-based Wasm plugin wrapper for entity dispatch.
//!
//! See module-level docs in each sub-section for details.

pub mod root_abi {
    //! Re-exported ABI types for the /ma/root/0.0.1 plugin.
    //!
    //! Defined inline here so the runtime binary can depend on them without
    //! a separate crate dependency (the root/ crate is the plugin, compiled
    //! to wasm32-unknown-unknown only).

    use serde::{Deserialize, Serialize};

    #[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
    #[serde(rename_all = "snake_case")]
    pub enum Op {
        Get,
        Set,
        Delete,
        ApplyCid,
        Verb,
    }

    #[derive(Debug, Clone, Serialize, Deserialize)]
    pub struct RootRequest {
        pub op: Op,
        pub path: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        pub value: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        pub cid: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        pub verb: Option<String>,
        pub caller_did: String,
        pub message_id: String,
        pub owner_did: String,
        pub subtree_snapshot: serde_json::Value,
    }

    #[derive(Debug, Clone, Serialize, Deserialize)]
    #[serde(tag = "type", rename_all = "snake_case")]
    pub enum CommitIntent {
        UpsertEntity { name: String, node: serde_json::Value },
        DeleteEntity { name: String },
        UpsertKind { family: String, implementation: String, node: serde_json::Value },
        DeleteKind { family: String, implementation: String },
        SetConfig { key: String, value: serde_json::Value },
        DeleteConfig { key: String },
    }

    #[derive(Debug, Clone, Serialize, Deserialize)]
    pub struct RootResponse {
        pub ok: bool,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        pub result: Option<serde_json::Value>,
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        pub commit: Vec<CommitIntent>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        pub error: Option<String>,
    }
}

use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use anyhow::{anyhow, Context, Result};
use extism::{host_fn, Function, Manifest, Plugin, UserData, Wasm, PTR};
use ma_core::{cat_bytes, ipfs_add};
use tokio::sync::RwLock;
use tracing::{debug, warn};

use crate::entity::{CastInput, EntityNode, PluginKind, ReplyRequest, SendEnvelope};

// ── Host functions ────────────────────────────────────────────────────────────

// `ma_send` host function exposed to plugins (namespace `extism:host/user`).
//
// The plugin passes a CBOR-encoded `SendEnvelope`.  The host deserialises it
// and pushes it onto the per-call queue; the runtime drains the queue after
// each dispatch.
host_fn!(ma_send_fn(user_data: Vec<SendEnvelope>; input: Vec<u8>) -> Vec<u8> {
    let envelope: SendEnvelope = from_cbor_bytes(&input)?;
    user_data.get()?.lock().unwrap().push(envelope);
    Ok(Vec::new())
});

// `ma_reply` host function: convenience wrapper around `ma_send`.
//
// Plugin passes a CBOR-encoded `ReplyRequest { msg, content }`.  The runtime
// fills in `to` (= msg.from), `reply_to` (= msg.id), and `content_type`
// automatically — plugin only provides the reply body.
host_fn!(ma_reply_fn(user_data: Vec<SendEnvelope>; input: Vec<u8>) -> Vec<u8> {
    let req: ReplyRequest = from_cbor_bytes(&input)?;
    let envelope = SendEnvelope {
        to: req.msg.from,
        content_type: req.content_type,
        content: req.content,
        reply_to: Some(req.msg.id),
    };
    let arc = user_data.get()?;
    arc.lock().unwrap().push(envelope);
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
    fn new(persisted: Vec<u8>) -> Self {
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
    }
    Ok(Vec::new())
});

// ── Dispatch result ─────────────────────────────────────────────────────────

/// Return value from `handle_cast` and `handle_call`.
pub struct DispatchResult {
    /// Outbound messages enqueued via the `ma_send` host function.
    pub envelopes: Vec<SendEnvelope>,
    /// State bytes queued by the plugin via `ma_set_state` host function.
    /// `None` if the plugin did not call `ma_set_state` during this invocation.
    pub pending_state: Option<Vec<u8>>,
}

// ── Registry type alias ───────────────────────────────────────────────────────

/// Thread-safe map from fragment name (e.g. `"fortune"`) to loaded plugin.
pub type EntityRegistry = Arc<RwLock<HashMap<String, Arc<EntityPlugin>>>>;

pub fn new_entity_registry() -> EntityRegistry {
    Arc::new(RwLock::new(HashMap::new()))
}

// ── EntityPlugin ──────────────────────────────────────────────────────────────

pub struct EntityPlugin {
    pub fragment: String,
    pub owner: String,
    pub kind: PluginKind,
    plugin: Mutex<Plugin>,
    /// Queue populated by the `ma_send` host function during a plugin call.
    send_queue: UserData<Vec<SendEnvelope>>,
    /// Shared state context: pending bytes, last-persisted snapshot, dirty flag.
    state: UserData<StateCtx>,
}

// Safety: `extism::Plugin` calls into C (libextism) but the Mutex ensures
// exclusive access and no shared mutable state leaks across thread boundaries.
// extism::Plugin does not implement Send upstream, but our usage is exclusively
// through &mut-guarded Mutex calls, making this sound.
unsafe impl Send for EntityPlugin {}
unsafe impl Sync for EntityPlugin {}

impl EntityPlugin {
    /// Load a plugin from IPFS, initialise it with persisted state (or empty).
    pub async fn load(
        fragment: impl Into<String>,
        node: &EntityNode,
        kubo_url: &str,
    ) -> Result<Self> {
        let fragment = fragment.into();
        let behavior_cid = &node.behavior.cid;
        let kind = PluginKind::from_kind_str(&node.kind);

        debug!(fragment = %fragment, cid = %behavior_cid, kind = ?kind, "loading entity plugin");

        // 1. Fetch Wasm bytes from IPFS.
        let wasm_bytes = cat_bytes(kubo_url, behavior_cid)
            .await
            .with_context(|| format!("fetching wasm for {fragment} from {behavior_cid}"))?;

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

        // 3. Build extism manifest; register host functions.
        //    All plugins get ma_send and ma_reply (outbound messaging).
        //    Stateful plugins additionally get ma_set_state (persistence).
        let send_queue: UserData<Vec<SendEnvelope>> = UserData::new(Vec::new());
        let state: UserData<StateCtx> = UserData::new(StateCtx::new(init_state.clone()));
        let ma_send = Function::new("ma_send", [PTR], [PTR], send_queue.clone(), ma_send_fn);
        let ma_reply = Function::new("ma_reply", [PTR], [PTR], send_queue.clone(), ma_reply_fn);
        let mut host_fns = vec![ma_send, ma_reply];
        if kind == PluginKind::Stateful {
            host_fns.push(Function::new("ma_set_state", [PTR], [PTR], state.clone(), ma_set_state_fn));
        }
        let manifest = Manifest::new([Wasm::data(wasm_bytes)]);
        let mut plugin = tokio::task::block_in_place(|| Plugin::new(&manifest, host_fns, node.wasi))
            .map_err(|e| anyhow!("failed to create extism plugin for {fragment}: {e}"))?;

        // 4. Stateful only: call init() with the persisted state snapshot.
        if kind == PluginKind::Stateful {
            tokio::task::block_in_place(|| {
                plugin
                    .call::<&[u8], Vec<u8>>("init", init_state.as_slice())
                    .map_err(|e| anyhow!("init() failed for {fragment}: {e}"))
            })?;
        }

        Ok(Self {
            fragment,
            owner: node.owner.clone(),
            kind,
            plugin: Mutex::new(plugin),
            send_queue,
            state,
        })
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

    /// Encode `input`, invoke the named WASM export, drain both host-function
    /// queues, and return a `DispatchResult`.
    fn dispatch_to(&self, export: &str, input: &CastInput) -> Result<DispatchResult> {
        let mut input_bytes = Vec::new();
        ciborium::ser::into_writer(
            &to_cbor_value(input).map_err(|e| anyhow!("failed to encode CastInput: {e}"))?,
            &mut input_bytes,
        )
        .map_err(|e| anyhow!("failed to CBOR-encode CastInput: {e}"))?;

        tokio::task::block_in_place(|| {
            let mut plugin = self
                .plugin
                .lock()
                .map_err(|e| anyhow!("plugin mutex poisoned: {e}"))?;
            plugin
                .call::<&[u8], Vec<u8>>(export, input_bytes.as_slice())
                .map_err(|e| anyhow!("{export}() failed for {}: {e}", self.fragment))
        })?;

        let envelopes = self
            .send_queue
            .get()
            .map_err(|e| anyhow!("send_queue error: {e}"))?
            .lock()
            .map_err(|e| anyhow!("send_queue poisoned: {e}"))?
            .drain(..)
            .collect();

        let pending_state = self
            .state
            .get()
            .map_err(|e| anyhow!("state error: {e}"))?
            .lock()
            .map_err(|e| anyhow!("state poisoned: {e}"))?
            .pending
            .take();

        Ok(DispatchResult { envelopes, pending_state })
    }

    /// Returns `true` if the plugin has state not yet persisted to IPFS.
    #[allow(dead_code)]
    pub fn is_dirty(&self) -> bool {
        self.state
            .get()
            .ok()
            .and_then(|arc| arc.lock().ok().map(|s| s.dirty))
            .unwrap_or(false)
    }

    /// Record a successful IPFS persist: update the persisted snapshot and
    /// clear the dirty flag.  Call this after a successful `ipfs_add`.
    pub fn mark_saved(&self, saved_bytes: Vec<u8>) {
        if let Ok(arc) = self.state.get() {
            if let Ok(mut ctx) = arc.lock() {
                ctx.persisted = Some(saved_bytes);
                ctx.dirty = false;
            }
        }
    }

    /// Tell the plugin to save its state by calling the `save_state` export.
    ///
    /// A well-behaved plugin responds by calling `ma_set_state(bytes)`.  If it
    /// doesn't, `Ok(None)` is returned and `dirty` is unchanged (tough luck).
    /// On IPFS success the `dirty` flag is cleared and the CID is returned.
    #[allow(dead_code)]
    pub async fn trigger_save(&self, kubo_url: &str) -> Result<Option<String>> {
        tokio::task::block_in_place(|| {
            let mut plugin = self
                .plugin
                .lock()
                .map_err(|e| anyhow!("plugin mutex poisoned: {e}"))?;
            plugin
                .call::<&[u8], Vec<u8>>("save_state", b"")
                .map_err(|e| anyhow!("save_state() failed for {}: {e}", self.fragment))
        })?;

        let pending = self
            .state
            .get()
            .map_err(|e| anyhow!("state error: {e}"))?
            .lock()
            .map_err(|e| anyhow!("state poisoned: {e}"))?
            .pending
            .take();

        match pending {
            Some(bytes) => {
                let cid = ipfs_add(kubo_url, bytes.clone())
                    .await
                    .map_err(|e| anyhow!("ipfs_add for {} state: {e}", self.fragment))?;
                self.mark_saved(bytes);
                Ok(Some(cid))
            }
            None => {
                warn!(fragment = %self.fragment, "save_state export did not call ma_set_state");
                Ok(None)
            }
        }
    }

}

// ── CBOR helpers ──────────────────────────────────────────────────────────────

/// Serialise a `serde::Serialize` value to `ciborium::Value` via JSON round-trip.
pub(crate) fn to_cbor_value<T: serde::Serialize>(value: &T) -> Result<ciborium::Value> {
    let json = serde_json::to_value(value)?;
    json_to_cbor(json).map_err(|e| anyhow!("{e}"))
}

/// Encode a `serde::Serialize` value to CBOR bytes.
pub fn encode_cbor<T: serde::Serialize>(value: &T, buf: &mut Vec<u8>) -> Result<()> {
    let cbor = to_cbor_value(value)?;
    ciborium::ser::into_writer(&cbor, buf)
        .map_err(|e| anyhow!("CBOR encode: {e}"))
}

/// Decode CBOR bytes to a `serde::DeserializeOwned` value.
pub fn decode_cbor<T: serde::de::DeserializeOwned>(bytes: &[u8]) -> Result<T> {
    from_cbor_bytes(bytes)
}

pub(crate) fn json_to_cbor(v: serde_json::Value) -> std::result::Result<ciborium::Value, String> {
    Ok(match v {
        serde_json::Value::Null => ciborium::Value::Null,
        serde_json::Value::Bool(b) => ciborium::Value::Bool(b),
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                ciborium::Value::Integer(ciborium::value::Integer::from(i))
            } else if let Some(f) = n.as_f64() {
                ciborium::Value::Float(f)
            } else {
                return Err(format!("unconvertible number {n}"));
            }
        }
        serde_json::Value::String(s) => ciborium::Value::Text(s),
        serde_json::Value::Array(arr) => {
            ciborium::Value::Array(arr.into_iter().map(json_to_cbor).collect::<Result<Vec<_>, _>>()?)
        }
        serde_json::Value::Object(map) => ciborium::Value::Map(
            map.into_iter()
                .map(|(k, v)| Ok((ciborium::Value::Text(k), json_to_cbor(v)?)))
                .collect::<std::result::Result<Vec<_>, String>>()?,
        ),
    })
}

/// Deserialise `ciborium::Value` bytes into a `serde::Deserialize` type via
/// JSON round-trip.
fn from_cbor_bytes<T: serde::de::DeserializeOwned>(bytes: &[u8]) -> Result<T> {
    let cbor: ciborium::Value =
        ciborium::de::from_reader(bytes).map_err(|e| anyhow!("CBOR decode error: {e}"))?;
    let json = cbor_to_json(cbor)?;
    let result = serde_json::from_value(json)?;
    Ok(result)
}

fn cbor_to_json(v: ciborium::Value) -> Result<serde_json::Value> {
    Ok(match v {
        ciborium::Value::Bool(b) => serde_json::Value::Bool(b),
        ciborium::Value::Integer(i) => {
            let n: i64 = i.try_into().map_err(|_| anyhow!("CBOR integer overflow"))?;
            serde_json::Value::Number(n.into())
        }
        ciborium::Value::Float(f) => {
            serde_json::Value::Number(
                serde_json::Number::from_f64(f)
                    .ok_or_else(|| anyhow!("non-finite CBOR float"))?,
            )
        }
        ciborium::Value::Text(s) => serde_json::Value::String(s),
        ciborium::Value::Bytes(b) => {
            // Encode as a JSON array of integers so that serde can deserialise
            // Vec<u8> fields correctly via the CBOR→JSON round-trip.
            serde_json::Value::Array(
                b.into_iter()
                    .map(|byte| serde_json::Value::Number(byte.into()))
                    .collect(),
            )
        }
        ciborium::Value::Array(arr) => {
            serde_json::Value::Array(arr.into_iter().map(cbor_to_json).collect::<Result<Vec<_>>>()?)
        }
        ciborium::Value::Map(map) => {
            let mut obj = serde_json::Map::new();
            for (k, v) in map {
                let key = match k {
                    ciborium::Value::Text(s) => s,
                    other => format!("{other:?}"),
                };
                obj.insert(key, cbor_to_json(v)?);
            }
            serde_json::Value::Object(obj)
        }
        _ => serde_json::Value::Null,
    })
}

// ── RootPlugin ────────────────────────────────────────────────────────────────
//
// The root plugin implements `/ma/root/0.0.1`.  Unlike normal `EntityPlugin`s,
// which send replies via the `ma_reply` host function, the root plugin RETURNS
// its structured `RootResponse` as the `handle_cast` return value.  The runtime
// parses the response, executes `CommitIntent`s, and sends the caller's reply.
//
// Host functions exposed to the root plugin:
//   `ma_root_read(path_utf8) -> cbor_value`  — read any leaf from the manifest
//                                              snapshot supplied at call time.

/// Shared manifest snapshot for the `ma_root_read` host function.
/// Wrapped in `Arc<Mutex>` so that the `UserData` can be updated before each
/// call and the host function can read it consistently within a single dispatch.
type RootManifestData = serde_json::Value;

host_fn!(ma_root_read_fn(user_data: RootManifestData; path_bytes: Vec<u8>) -> Vec<u8> {
    let path = String::from_utf8(path_bytes)
        .map_err(|e| anyhow::anyhow!("ma_root_read: invalid UTF-8 path: {e}"))?;

    let manifest = user_data.get()?;
    let snapshot = manifest.lock().map_err(|e| anyhow::anyhow!("manifest lock: {e}"))?;

    // Walk the dot-separated path into the snapshot.
    let result = {
        let mut cur: &serde_json::Value = &snapshot;
        let mut found = true;
        for seg in path.split('.') {
            match cur.get(seg) {
                Some(next) => cur = next,
                None => { found = false; break; }
            }
        }
        if found { cur.clone() } else { serde_json::Value::Null }
    };

    let cbor = to_cbor_value(&result)?;
    let mut buf = Vec::new();
    ciborium::ser::into_writer(&cbor, &mut buf)
        .map_err(|e| anyhow::anyhow!("ma_root_read: CBOR encode: {e}"))?;
    Ok(buf)
});

/// A loaded instance of the `/ma/root/0.0.1` Extism plugin.
///
/// Thread-safe: the inner `Plugin` is guarded by a `Mutex`.  All calls are
/// made via `tokio::task::block_in_place`.
pub struct RootPlugin {
    pub owner: String,
    plugin: Mutex<Plugin>,
    /// Manifest snapshot shared with `ma_root_read`.  Updated before each call.
    manifest_data: UserData<RootManifestData>,
}

// Same soundness justification as for `EntityPlugin`.
unsafe impl Send for RootPlugin {}
unsafe impl Sync for RootPlugin {}

impl RootPlugin {
    /// Load the root plugin from IPFS by its `behavior` CID.
    pub async fn load(node: &crate::entity::EntityNode, kubo_url: &str) -> Result<Self> {
        let cid = &node.behavior.cid;
        debug!(cid = %cid, "loading root plugin");

        let wasm_bytes = ma_core::cat_bytes(kubo_url, cid)
            .await
            .with_context(|| format!("fetching root plugin wasm from {cid}"))?;

        let manifest_data: UserData<RootManifestData> =
            UserData::new(serde_json::Value::Null);

        let ma_root_read = Function::new(
            "ma_root_read",
            [PTR],
            [PTR],
            manifest_data.clone(),
            ma_root_read_fn,
        );

        let manifest = Manifest::new([Wasm::data(wasm_bytes)]);
        let plugin = tokio::task::block_in_place(|| {
            Plugin::new(&manifest, [ma_root_read], node.wasi)
                .map_err(|e| anyhow!("failed to create root plugin: {e}"))
        })?;

        Ok(Self {
            owner: node.owner.clone(),
            plugin: Mutex::new(plugin),
            manifest_data,
        })
    }

    /// Call `handle_cast` with a pre-built CBOR-encoded `RootRequest`.
    ///
    /// `manifest` is a JSON representation of the full current `RuntimeManifest`
    /// (used by `ma_root_read`).  The plugin returns CBOR-encoded `RootResponse`
    /// bytes which the caller is responsible for parsing.
    pub fn call(&self, manifest_json: serde_json::Value, input_cbor: Vec<u8>) -> Result<Vec<u8>> {
        // Update the manifest snapshot that `ma_root_read` will see.
        {
            let arc = self.manifest_data.get()
                .map_err(|e| anyhow!("manifest_data error: {e}"))?;
            *arc.lock().map_err(|e| anyhow!("manifest_data poisoned: {e}"))? = manifest_json;
        }

        tokio::task::block_in_place(|| {
            let mut plugin = self.plugin.lock()
                .map_err(|e| anyhow!("root plugin mutex poisoned: {e}"))?;
            plugin
                .call::<&[u8], Vec<u8>>("handle_cast", input_cbor.as_slice())
                .map_err(|e| anyhow!("root plugin handle_cast failed: {e}"))
        })
    }
}
