//! Extism-based Wasm plugin wrapper for entity dispatch.

use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use anyhow::{anyhow, Context, Result};
use extism::{host_fn, Function, Manifest, Plugin, UserData, Wasm, PTR};
use ma_core::{cat_bytes, ipfs_add};
use tokio::sync::{mpsc::UnboundedSender, RwLock};
use tracing::{debug, error, info, warn};

use crate::entity::{CastInput, CreateEntityRequest, EntityCtx, EntityNode, KindNode, PluginKind, ReplyRequest, SendEnvelope};
use crate::schedule::{encode_cbor_call_cbor, parse_duration, ScheduleRequest};

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

// ── ma_log host function ──────────────────────────────────────────────────────

#[derive(serde::Deserialize)]
struct LogInput {
    level: String,
    msg: String,
}

// `ma_log` host function: plugin logs a message at the given level.
//
// Input is CBOR-encoded `{ "level": "debug"|"info"|"warn"|"error", "msg": "…" }`.
// The fragment name is passed via UserData for tracing context.
host_fn!(ma_log_fn(user_data: String; input: Vec<u8>) -> Vec<u8> {
    let req: LogInput = from_cbor_bytes(&input)?;
    let fragment = user_data.get()?.lock().unwrap().clone();
    match req.level.as_str() {
        "debug" => debug!("[{}] {}", fragment, req.msg),
        "warn"  => warn!( "[{}] {}", fragment, req.msg),
        "error" => error!("[{}] {}", fragment, req.msg),
        _       => info!( "[{}] {}", fragment, req.msg),
    }
    Ok(Vec::new())
});

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

// `ma_schedule_cron` host function: plugin queues a recurring cron schedule.
// Input is CBOR-encoded `{ "spec": "…", "verb": "…", "args": […] }`.
#[derive(serde::Deserialize)]
struct CronScheduleInput {
    spec: String,
    verb: String,
    #[serde(default)]
    args: Vec<ciborium::Value>,
}
host_fn!(ma_schedule_cron_fn(user_data: Vec<ScheduleRequest>; input: Vec<u8>) -> Vec<u8> {
    let req: CronScheduleInput = from_cbor_bytes(&input)?;
    let content = encode_cbor_call_cbor(&req.verb, &req.args);
    user_data.get()?.lock().unwrap().push(ScheduleRequest::Cron {
        spec: req.spec,
        content,
    });
    Ok(Vec::new())
});

// `ma_schedule_at` host function: plugin queues a one-shot schedule.
// Input is CBOR-encoded `{ "timestamp_ms": N, "verb": "…", "args": […] }`.
#[derive(serde::Deserialize)]
struct AtScheduleInput {
    timestamp_ms: i64,
    verb: String,
    #[serde(default)]
    args: Vec<ciborium::Value>,
}
host_fn!(ma_schedule_at_fn(user_data: Vec<ScheduleRequest>; input: Vec<u8>) -> Vec<u8> {
    let req: AtScheduleInput = from_cbor_bytes(&input)?;
    let content = encode_cbor_call_cbor(&req.verb, &req.args);
    user_data.get()?.lock().unwrap().push(ScheduleRequest::At {
        timestamp_ms: req.timestamp_ms,
        content,
    });
    Ok(Vec::new())
});

// `ma_schedule_random` host function: plugin queues a self-rescheduling random job.
// Input is CBOR-encoded `{ "max_secs": N, "verb": "…", "args": […] }`.
#[derive(serde::Deserialize)]
struct RandomScheduleInput {
    max_secs: u64,
    verb: String,
    #[serde(default)]
    args: Vec<ciborium::Value>,
}
host_fn!(ma_schedule_random_fn(user_data: Vec<ScheduleRequest>; input: Vec<u8>) -> Vec<u8> {
    let req: RandomScheduleInput = from_cbor_bytes(&input)?;
    let content = encode_cbor_call_cbor(&req.verb, &req.args);
    user_data.get()?.lock().unwrap().push(ScheduleRequest::Random {
        max_secs: req.max_secs,
        content,
    });
    Ok(Vec::new())
});

// `ma_schedule_interval` host function: plugin queues a fixed-interval recurring job.
// Input is CBOR-encoded `{ "interval": "1h", "verb": "…", "args": […] }`.
// Duration string supports `s`, `m`, `h`, `d` units, combinable: `"1h30m"`.
#[derive(serde::Deserialize)]
struct IntervalScheduleInput {
    interval: String,
    verb: String,
    #[serde(default)]
    args: Vec<ciborium::Value>,
}
host_fn!(ma_schedule_interval_fn(user_data: Vec<ScheduleRequest>; input: Vec<u8>) -> Vec<u8> {
    let req: IntervalScheduleInput = from_cbor_bytes(&input)?;
    let secs = parse_duration(&req.interval)
        .map_err(|e| extism::Error::msg(format!("ma_schedule_interval: {e}")))?
        .as_secs();
    let content = encode_cbor_call_cbor(&req.verb, &req.args);
    user_data.get()?.lock().unwrap().push(ScheduleRequest::Interval { secs, content });
    Ok(Vec::new())
});

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
// ── ma_ctx host function ─────────────────────────────────────────────────────────────

// `ma_ctx` host function: returns entity metadata to the plugin.
//
// Input is ignored.  Output is CBOR-encoded `EntityCtx`.
host_fn!(ma_ctx_fn(user_data: EntityCtx; _input: Vec<u8>) -> Vec<u8> {
    let arc = user_data.get()?;
    let ctx = arc.lock().unwrap();
    let mut out = Vec::new();
    ciborium::ser::into_writer(&*ctx, &mut out)
        .map_err(|e| extism::Error::msg(format!("ma_ctx: CBOR encode: {e}")))?;
    Ok(out)
});
// Context captured by `ma_create_entity` host function.
struct CreateEntityCtx {
    pending: Vec<CreateEntityRequest>,
    /// Fragment of the calling (parent) entity.
    caller_fragment: String,
}

// `ma_create_entity` host function: plugin requests creation of a new entity.
//
// Input is CBOR-encoded `{ "kind": "/ma/…/0.0.1", "behavior": "bafyCID" }`.
// The runtime generates a nanoid fragment, queues the request, and returns the
// fragment string (CBOR-encoded) to the plugin immediately.
// Actual plugin loading and manifest persistence happen after dispatch returns.
#[derive(serde::Deserialize)]
struct CreateEntityInput {
    kind: String,
    behavior: String,
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
        behavior_cid: req.behavior,
        parent,
    });
    let mut out = Vec::new();
    ciborium::ser::into_writer(&fragment, &mut out)
        .map_err(|e| extism::Error::msg(format!("ma_create_entity: CBOR encode: {e}")))?;
    Ok(out)
});

// ── Dispatch result ─────────────────────────────────────────────────────────

/// Return value from `handle_cast` and `handle_call`.
pub struct DispatchResult {
    /// State bytes queued by the plugin via `ma_set_state` host function.
    /// `None` if the plugin did not call `ma_set_state` during this invocation.
    pub pending_state: Option<Vec<u8>>,
    /// Schedule requests enqueued via `ma_schedule_*` host functions.
    pub schedule_requests: Vec<ScheduleRequest>,
    /// Entity creation requests enqueued via `ma_create_entity` host function.
    pub create_requests: Vec<CreateEntityRequest>,
}

// ── Registry type alias ───────────────────────────────────────────────────────

/// Thread-safe map from fragment name (e.g. `"fortune"`) to loaded plugin.
pub type EntityRegistry = Arc<RwLock<HashMap<String, Arc<EntityPlugin>>>>;

pub fn new_entity_registry() -> EntityRegistry {
    Arc::new(RwLock::new(HashMap::new()))
}

// ── EntityPlugin ──────────────────────────────────────────────────────────────

pub struct EntityPlugin {
    /// Globally unique entity name (bare, no `#` prefix, no dots).
    /// Matches the key in `RuntimeManifest.entities` and equals the DID fragment:
    /// `did:ma:<ipns>#<fragment>` → lookup `entities[fragment]`.
    pub fragment: String,
    pub kind: PluginKind,
    /// Protocol ID of the kind this entity was loaded with (e.g. `/ma/stateless/python/0.0.1`).
    pub kind_protocol: String,
    /// ACL name string — resolved via `acls.<acl>` in the root manifest.
    /// Empty string means deny-all (fail-closed).
    pub acl: String,
    /// Static schedules declared in the entity definition.
    /// Stored here so bootstrap can register them without re-fetching IPFS data.
    pub schedules: HashMap<String, crate::schedule::StaticSchedule>,
    plugin: Mutex<Plugin>,
    /// Queue populated by `ma_schedule_*` host functions during a plugin call.
    schedule_queue: UserData<Vec<ScheduleRequest>>,
    /// Shared state context: pending bytes, last-persisted snapshot, dirty flag.
    state: UserData<StateCtx>,
    /// Queue populated by `ma_create_entity` host function during a plugin call.
    create_queue: UserData<CreateEntityCtx>,
    /// Entity metadata returned by `ma_ctx` host function.
    ctx_data: UserData<EntityCtx>,
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
        kind_node: &KindNode,
        our_did: &str,
        kubo_url: &str,
        envelope_tx: UnboundedSender<(String, SendEnvelope)>,
    ) -> Result<Self> {
        let fragment = fragment.into();
        let behavior_cid = &node.behavior.cid;
        let kind = kind_node.plugin_kind();
        let wasi = kind_node.wasi();

        debug!(fragment = %fragment, cid = %behavior_cid, kind = ?kind, wasi = wasi, "loading entity plugin");

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
        let schedule_queue: UserData<Vec<ScheduleRequest>> = UserData::new(Vec::new());
        let state: UserData<StateCtx> = UserData::new(StateCtx::new(init_state.clone()));
        let log_ctx: UserData<String> = UserData::new(fragment.clone());
        let create_queue: UserData<CreateEntityCtx> = UserData::new(CreateEntityCtx {
            pending: Vec::new(),
            caller_fragment: fragment.clone(),
        });
        let ctx_data: UserData<EntityCtx> = UserData::new(EntityCtx {
            self_did: format!("{}#{}", our_did, &fragment),
            fragment: fragment.clone(),
            kind: node.kind.clone(),
            parent: node.parent.clone(),
        });

        let all_fns: Vec<(&str, Function)> = vec![
            ("ma_send",    Function::new("ma_send",    [PTR], [PTR], outbox_ctx_send,        ma_send_fn)),
            ("ma_reply",   Function::new("ma_reply",   [PTR], [PTR], outbox_ctx_reply,       ma_reply_fn)),
            ("ma_log",     Function::new("ma_log",     [PTR], [PTR], log_ctx,                ma_log_fn)),
            ("ma_schedule_cron",     Function::new("ma_schedule_cron",     [PTR], [PTR], schedule_queue.clone(), ma_schedule_cron_fn)),
            ("ma_schedule_at",       Function::new("ma_schedule_at",       [PTR], [PTR], schedule_queue.clone(), ma_schedule_at_fn)),
            ("ma_schedule_random",   Function::new("ma_schedule_random",   [PTR], [PTR], schedule_queue.clone(), ma_schedule_random_fn)),
            ("ma_schedule_interval", Function::new("ma_schedule_interval", [PTR], [PTR], schedule_queue.clone(), ma_schedule_interval_fn)),
            ("ma_set_state", Function::new("ma_set_state", [PTR], [PTR], state.clone(), ma_set_state_fn)),
            ("ma_create_entity", Function::new("ma_create_entity", [PTR], [PTR], create_queue.clone(), ma_create_entity_fn)),
            ("ma_ctx", Function::new("ma_ctx", [PTR], [PTR], ctx_data.clone(), ma_ctx_fn)),
        ];
        let allowed: std::collections::HashSet<&str> =
            kind_node.host_functions.iter().map(String::as_str).collect();
        let host_fns: Vec<Function> = all_fns
            .into_iter()
            .filter(|(name, _)| allowed.contains(*name))
            .map(|(_, f)| f)
            .collect();

        let manifest = Manifest::new([Wasm::data(wasm_bytes)]);
        let mut plugin = tokio::task::block_in_place(|| Plugin::new(&manifest, host_fns, wasi))
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
            kind,
            kind_protocol: node.kind.clone(),
            acl: node.acl.clone(),
            schedules: node.schedules.clone(),
            plugin: Mutex::new(plugin),
            schedule_queue,
            state,
            create_queue,
            ctx_data,
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
        ciborium::ser::into_writer(input, &mut input_bytes)
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

        let schedule_requests = self
            .schedule_queue
            .get()
            .map_err(|e| anyhow!("schedule_queue error: {e}"))?
            .lock()
            .map_err(|e| anyhow!("schedule_queue poisoned: {e}"))?
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

        let create_requests = self
            .create_queue
            .get()
            .map_err(|e| anyhow!("create_queue error: {e}"))?
            .lock()
            .map_err(|e| anyhow!("create_queue poisoned: {e}"))?
            .pending
            .drain(..)
            .collect();

        Ok(DispatchResult {
            pending_state,
            schedule_requests,
            create_requests,
        })
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

        if let Some(bytes) = pending {
            let cid = ipfs_add(kubo_url, bytes.clone())
                .await
                .map_err(|e| anyhow!("ipfs_add for {} state: {e}", self.fragment))?;
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
