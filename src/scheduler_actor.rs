//! Native `#scheduler` entity — compiled-in schedule-registration actor.
//!
//! `#scheduler` is a system entity whose implementation lives here in Rust
//! rather than in a Wasm module.  It is registered in the entity registry at
//! startup via [`make_native_dispatch`] + [`crate::plugin::EntityPlugin::new_native`].
//!
//! ## Protocol
//!
//! Any entity on this runtime can send a CBOR array to
//! `did:ma:<our_did>#scheduler` to register a scheduled dispatch:
//!
//! ```text
//! [":cron",     spec_str,     target_frag, verb_or_array, extra_args…]
//! [":interval", duration_str, target_frag, verb_or_array, extra_args…]
//! [":at",       timestamp_ms, target_frag, verb_or_array, extra_args…]
//! [":random",   max_secs_int, target_frag, verb_or_array, extra_args…]
//! ```
//!
//! `target_frag` is a bare fragment name (`"fortune"`) or a full DID-URL
//! (`did:ma:<ipns>#fortune`).  The scheduler normalises it to the bare name.
//!
//! ## ACL
//!
//! `#scheduler` uses the `"scheduler"` ACL entry from the root manifest.
//! The default ACL (`default.acl`) restricts registration to local entities
//! only (`"#": [handle_cast]`).  Remote peers cannot register schedules
//! unless the operator explicitly opens the ACL.
//!
//! ## Kind
//!
//! Kind protocol: [`SCHEDULER_KIND`] (`/ma/scheduler/0.0.1`)
//! Evaluator: `native`
//! API: `["handle_cast"]` (stateless — schedule state lives in the `JobScheduler`)

use std::sync::Arc;

use anyhow::{anyhow, Result};
use ciborium::Value as CborValue;
use tracing::warn;

use crate::entity::{CastInput, EntityNode, Lifecycle};
use crate::plugin::{DispatchResult, NativeDispatch};
use crate::schedule::{parse_duration, register_schedule, ScheduleRequest, SchedulerCtx};

/// Kind protocol ID for the native scheduler entity.
pub const SCHEDULER_KIND: &str = "/ma/scheduler/0.0.1";

/// Fragment name for the scheduler entity (no `#` prefix).
pub const SCHEDULER_FRAGMENT: &str = "scheduler";

/// ACL name used to gate access to `#scheduler`.
///
/// Must be present in the root manifest's `acls` map.
/// The recommended ACL allows only local entities: `"#": [handle_cast]`.
pub const SCHEDULER_ACL: &str = "scheduler";

/// Build the [`EntityNode`] for `#scheduler`.
///
/// `behaviour` is `None` — native entities have no Wasm.
/// The lifecycle starts as `Running`; `new_native()` sets this immediately.
pub fn entity_node() -> EntityNode {
    EntityNode {
        kind: SCHEDULER_KIND.to_string(),
        behaviour: None,
        acl: SCHEDULER_ACL.to_string(),
        state: None,
        parent: None,
        label: Some("Scheduler".to_string()),
        lifecycle: Lifecycle::Running,
    }
}

/// Build the [`NativeDispatch`] closure for `#scheduler`.
///
/// The closure captures `sched` (the running [`tokio_cron_scheduler::JobScheduler`])
/// and `ctx` (the [`SchedulerCtx`] needed by [`register_schedule`]).
///
/// On each call it parses the incoming CBOR array, spawns an async task to
/// register the schedule, and returns `:ok` immediately (fire-and-forget).
/// Parse errors are returned as `Err` so the caller can send an error reply.
pub fn make_native_dispatch(
    sched: Arc<tokio_cron_scheduler::JobScheduler>,
    ctx: SchedulerCtx,
) -> NativeDispatch {
    Arc::new(move |input: &CastInput| -> Result<DispatchResult> {
        let term: CborValue = ciborium::de::from_reader(input.msg.content.as_slice())
            .map_err(|e| anyhow!("scheduler: invalid CBOR in message: {e}"))?;

        let req = parse_schedule_request(term, &input.msg.from)?;

        let sched = Arc::clone(&sched);
        let ctx = ctx.clone();
        let from = input.msg.from.clone();
        tokio::spawn(async move {
            if let Err(e) =
                register_schedule(&sched, ctx, req.fragment.clone(), None, req.request).await
            {
                warn!(target = %req.fragment, from = %from, error = %e, "scheduler: failed to register schedule");
            }
        });

        // Return :ok immediately — registration is async / fire-and-forget.
        let mut out = Vec::new();
        ciborium::ser::into_writer(&CborValue::Text(":ok".to_string()), &mut out)
            .map_err(|e| anyhow!("scheduler: CBOR encode :ok: {e}"))?;
        Ok(DispatchResult {
            output: out,
            pending_state: None,
            create_requests: vec![],
            delete_requests: vec![],
        })
    })
}

// ── Internal types ────────────────────────────────────────────────────────────

struct ParsedRequest {
    fragment: String,
    request: ScheduleRequest,
}

// ── CBOR schedule-request parser ─────────────────────────────────────────────

/// Parse a CBOR schedule-registration array into a [`ScheduleRequest`].
///
/// Expected wire format:
/// ```text
/// [type_atom, spec, target_frag, verb_or_array, extra_args…]
/// ```
/// where `type_atom` is one of `:cron`, `:interval`, `:at`, `:random`.
fn parse_schedule_request(term: CborValue, _from: &str) -> Result<ParsedRequest> {
    let items = match term {
        CborValue::Array(a) => a,
        other => return Err(anyhow!("scheduler: expected CBOR array, got {other:?}")),
    };
    if items.len() < 4 {
        return Err(anyhow!(
            "scheduler: expected [type, spec, target, verb], got {} elements",
            items.len()
        ));
    }

    let type_verb = match &items[0] {
        CborValue::Text(s) => s.clone(),
        _ => return Err(anyhow!("scheduler: first element must be text atom")),
    };

    let target = match &items[2] {
        CborValue::Text(s) => s.clone(),
        _ => return Err(anyhow!("scheduler: third element must be target fragment")),
    };

    // Normalise target to bare fragment: strip any leading DID-URL.
    let fragment = if let Some(pos) = target.find('#') {
        target[pos + 1..].to_string()
    } else {
        target
    };

    // Encode verb (4th element) + optional inline args (5th+) as CBOR content bytes.
    let content = encode_verb_content(&items)?;

    let request = match type_verb.as_str() {
        ":cron" => {
            let spec = match &items[1] {
                CborValue::Text(s) => s.clone(),
                _ => return Err(anyhow!("scheduler :cron: spec must be text")),
            };
            ScheduleRequest::Cron { spec, content }
        }
        ":interval" => {
            let dur_str = match &items[1] {
                CborValue::Text(s) => s.clone(),
                _ => return Err(anyhow!("scheduler :interval: duration must be text")),
            };
            let secs = parse_duration(&dur_str)
                .map_err(|e| anyhow!("scheduler :interval: {e}"))?
                .as_secs();
            ScheduleRequest::Interval { secs, content }
        }
        ":at" => {
            let ts = match &items[1] {
                CborValue::Integer(n) => i64::try_from(i128::from(*n))
                    .map_err(|_| anyhow!("scheduler :at: timestamp out of i64 range"))?,
                _ => return Err(anyhow!("scheduler :at: timestamp must be integer")),
            };
            ScheduleRequest::At {
                timestamp_ms: ts,
                content,
            }
        }
        ":random" => {
            let max_secs = match &items[1] {
                CborValue::Integer(n) => u64::try_from(i128::from(*n)).unwrap_or(60),
                _ => return Err(anyhow!("scheduler :random: max_secs must be integer")),
            };
            ScheduleRequest::Random { max_secs, content }
        }
        other => return Err(anyhow!("scheduler: unknown schedule type '{other}'")),
    };

    Ok(ParsedRequest { fragment, request })
}

/// Encode the verb (4th element) and any extra args (5th+) as pre-encoded CBOR bytes.
fn encode_verb_content(items: &[CborValue]) -> Result<Vec<u8>> {
    let args = items.get(4..).unwrap_or(&[]);
    let value = match &items[3] {
        CborValue::Text(v) if args.is_empty() => CborValue::Text(v.clone()),
        CborValue::Text(v) => {
            let mut arr = vec![CborValue::Text(v.clone())];
            arr.extend_from_slice(args);
            CborValue::Array(arr)
        }
        arr @ CborValue::Array(_) => arr.clone(),
        _ => {
            return Err(anyhow!(
                "scheduler: verb (4th element) must be text atom or array"
            ))
        }
    };
    let mut out = Vec::new();
    ciborium::ser::into_writer(&value, &mut out)
        .map_err(|e| anyhow!("scheduler: CBOR encode verb: {e}"))?;
    Ok(out)
}
