//! Native `#scheduler` entity — compiled-in schedule-registration actor.
//!
//! `#scheduler` is a system entity whose implementation lives here in Rust
//! rather than in a Wasm module. It is loaded from the manifest through the
//! native factory registry.
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
//! API: `["handle_cast"]`

use std::sync::Arc;

use anyhow::{anyhow, Result};
use ciborium::Value as CborValue;
use tracing::warn;

use crate::entity::CastInput;
use crate::plugin::{DispatchResult, NativeActor, NativeFactory, NativeSignal};
use crate::schedule::{parse_duration, register_schedule, ScheduleRequest, SchedulerCtx};

/// Kind protocol ID for the native scheduler entity.
pub const SCHEDULER_KIND: &str = "/ma/scheduler/0.0.1";

/// Build the native actor for `#scheduler`.
///
/// The closure captures `sched` (the running [`tokio_cron_scheduler::JobScheduler`])
/// and `ctx` (the [`SchedulerCtx`] needed by [`register_schedule`]).
///
/// On each call it parses the incoming CBOR array, spawns an async task to
/// register the schedule, and returns `:ok` immediately (fire-and-forget).
/// Parse errors are returned as `Err` so the caller can send an error reply.
pub fn native_actor(
    sched: Arc<tokio_cron_scheduler::JobScheduler>,
    ctx: SchedulerCtx,
) -> NativeActor {
    NativeActor::new(move |input: &CastInput| -> Result<DispatchResult> {
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

        let mut out = Vec::new();
        ciborium::ser::into_writer(&CborValue::Text(":ok".to_string()), &mut out)
            .map_err(|e| anyhow!("scheduler: CBOR encode :ok: {e}"))?;
        Ok(DispatchResult {
            output: out,
            pending_state: None,
            create_requests: vec![],
            delete_requests: vec![],
            behaviour_requests: vec![],
        })
    })
    .with_state_hooks(|| None, |_| {})
    .with_signal(|signal| {
        match signal {
            NativeSignal::SetState(_bytes) => {}
            NativeSignal::Init(_payload) => {}
            NativeSignal::Start | NativeSignal::Shutdown => {}
        }
        Ok(())
    })
}

/// Build the native factory used by manifest loading for `/ma/scheduler/0.0.1`.
pub fn native_factory(
    sched: Arc<tokio_cron_scheduler::JobScheduler>,
    ctx: SchedulerCtx,
) -> NativeFactory {
    Arc::new(move || native_actor(Arc::clone(&sched), ctx.clone()))
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

#[cfg(test)]
mod tests {
    use super::{encode_verb_content, parse_schedule_request};
    use crate::schedule::ScheduleRequest;
    use ciborium::Value as CborValue;

    fn text(s: &str) -> CborValue {
        CborValue::Text(s.to_string())
    }

    fn int(n: i64) -> CborValue {
        CborValue::Integer(n.into())
    }

    #[test]
    fn parses_cron() {
        let term = CborValue::Array(vec![
            text(":cron"),
            text("0 * * * * *"),
            text("myentity"),
            text(":tick"),
        ]);
        let p = parse_schedule_request(term, "from").unwrap();
        assert_eq!(p.fragment, "myentity");
        assert!(matches!(p.request, ScheduleRequest::Cron { spec, .. } if spec == "0 * * * * *"));
    }

    #[test]
    fn parses_interval_duration_to_seconds() {
        let term = CborValue::Array(vec![
            text(":interval"),
            text("30m"),
            text("garden"),
            text(":grow"),
        ]);
        let p = parse_schedule_request(term, "from").unwrap();
        assert!(matches!(p.request, ScheduleRequest::Interval { secs, .. } if secs == 1_800));
    }

    #[test]
    fn parses_at_timestamp() {
        let term = CborValue::Array(vec![
            text(":at"),
            int(1_700_000_000_000),
            text("e"),
            text(":wake"),
        ]);
        let p = parse_schedule_request(term, "from").unwrap();
        assert!(
            matches!(p.request, ScheduleRequest::At { timestamp_ms, .. } if timestamp_ms == 1_700_000_000_000)
        );
    }

    #[test]
    fn parses_random_max_secs() {
        let term = CborValue::Array(vec![
            text(":random"),
            int(300),
            text("dog"),
            text(":scratch"),
        ]);
        let p = parse_schedule_request(term, "from").unwrap();
        assert!(matches!(p.request, ScheduleRequest::Random { max_secs, .. } if max_secs == 300));
    }

    #[test]
    fn normalises_did_url_target_to_bare_fragment() {
        let term = CborValue::Array(vec![
            text(":cron"),
            text("* * * * * *"),
            text("did:ma:abc#myentity"),
            text(":tick"),
        ]);
        let p = parse_schedule_request(term, "from").unwrap();
        assert_eq!(p.fragment, "myentity");
    }

    #[test]
    fn rejects_too_few_elements() {
        let term = CborValue::Array(vec![text(":cron"), text("spec"), text("target")]);
        assert!(parse_schedule_request(term, "from").is_err());
    }

    #[test]
    fn rejects_unknown_type() {
        let term = CborValue::Array(vec![text(":weekly"), text("x"), text("t"), text(":v")]);
        assert!(parse_schedule_request(term, "from").is_err());
    }

    #[test]
    fn encode_verb_content_bare_verb_stays_atom() {
        let items = vec![text(":x"), text("y"), text("t"), text(":grow")];
        let content = encode_verb_content(&items).unwrap();
        let decoded: CborValue = ciborium::de::from_reader(content.as_slice()).unwrap();
        assert!(matches!(decoded, CborValue::Text(s) if s == ":grow"));
    }

    #[test]
    fn encode_verb_content_wraps_extra_args_in_array() {
        let items = vec![
            text(":x"),
            text("y"),
            text("t"),
            text(":grow"),
            text("a"),
            text("b"),
        ];
        let content = encode_verb_content(&items).unwrap();
        let decoded: CborValue = ciborium::de::from_reader(content.as_slice()).unwrap();
        assert!(matches!(decoded, CborValue::Array(a) if a.len() == 3));
    }
}
