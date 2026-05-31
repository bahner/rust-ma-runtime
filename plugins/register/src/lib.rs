use std::cell::RefCell;
use std::collections::BTreeMap;
use extism_pdk::*;

include!("../../shared/types.rs");

// ── State ─────────────────────────────────────────────────────────────────────

#[derive(Serialize, Deserialize, Default)]
struct RegisterState {
    fwd: BTreeMap<String, String>,
    rev: BTreeMap<String, String>,
}

thread_local! {
    static STATE: RefCell<RegisterState> = RefCell::new(RegisterState::default());
    static SELF_DID: RefCell<String> = RefCell::new(String::new());
    static PARENT: RefCell<Option<String>> = const { RefCell::new(None) };
}

fn is_writer(from: &str) -> bool {
    let self_matches = SELF_DID.with(|c| c.borrow().as_str() == from);
    let parent_matches = PARENT.with(|c| {
        c.borrow()
            .as_deref()
            .map_or(false, |p| p == from)
    });
    self_matches || parent_matches
}

fn persist() {
    STATE.with(|cell| {
        let state = cell.borrow();
        let mut bytes = Vec::new();
        if ciborium::ser::into_writer(&*state, &mut bytes).is_ok() {
            persist_state_bytes(&bytes);
        }
    });
}

// ── Exports ───────────────────────────────────────────────────────────────────

#[plugin_fn]
pub fn init(input: Vec<u8>) -> FnResult<Vec<u8>> {
    let payload: InitPayload = match ciborium::de::from_reader(input.as_slice()) {
        Ok(v) => v,
        Err(e) => return Ok(cbor_error(&format!("init decode: {e}"))),
    };

    SELF_DID.with(|c| *c.borrow_mut() = payload.ctx.self_did.clone());
    PARENT.with(|c| *c.borrow_mut() = payload.ctx.parent.clone());

    if !payload.state.is_empty() {
        if let Ok(s) = ciborium::de::from_reader::<RegisterState, _>(payload.state.as_slice()) {
            STATE.with(|cell| *cell.borrow_mut() = s);
        }
    }

    Ok(cbor_ok_unit())
}

#[plugin_fn]
pub fn handle_call(input: Vec<u8>) -> FnResult<Vec<u8>> {
    let cast: CastInput = match ciborium::de::from_reader(input.as_slice()) {
        Ok(v) => v,
        Err(_) => return Ok(vec![]),
    };
    let msg = &cast.msg;

    let (verb, args) = match parse_verb(&msg.content) {
        Some(v) => v,
        None => {
            send_reply(msg, cbor_error("invalid verb term"));
            return Ok(vec![]);
        }
    };

    let result = STATE.with(|cell| {
        let mut state = cell.borrow_mut();
        match verb.as_str() {
            // ── Read verbs (anyone) ───────────────────────────────────────────
            ":count" => cbor_ok(ciborium::Value::Integer(
                (state.fwd.len() as i64).into(),
            )),

            ":keys" => {
                let keys: Vec<ciborium::Value> = state
                    .fwd
                    .keys()
                    .map(|k| ciborium::Value::Text(k.clone()))
                    .collect();
                cbor_ok(ciborium::Value::Array(keys))
            }

            ":values" => {
                let vals: Vec<ciborium::Value> = state
                    .fwd
                    .values()
                    .map(|v| ciborium::Value::Text(v.clone()))
                    .collect();
                cbor_ok(ciborium::Value::Array(vals))
            }

            ":get" => match arg_text(&args, 0) {
                Some(key) => match state.fwd.get(&key) {
                    Some(v) => cbor_ok(ciborium::Value::Text(v.clone())),
                    None => cbor_error("not found"),
                },
                None => cbor_error(":get requires a key"),
            },

            ":reverse" => match arg_text(&args, 0) {
                Some(val) => match state.rev.get(&val) {
                    Some(k) => cbor_ok(ciborium::Value::Text(k.clone())),
                    None => cbor_error("not found"),
                },
                None => cbor_error(":reverse requires a value"),
            },

            ":has" => match arg_text(&args, 0) {
                Some(key) => cbor_ok(ciborium::Value::Bool(state.fwd.contains_key(&key))),
                None => cbor_error(":has requires a key"),
            },

            ":has_value" => match arg_text(&args, 0) {
                Some(val) => cbor_ok(ciborium::Value::Bool(state.rev.contains_key(&val))),
                None => cbor_error(":has_value requires a value"),
            },

            // ── Write verbs (parent / self only) ─────────────────────────────
            ":set" => {
                if !is_writer(&msg.from) {
                    return cbor_error("permission denied");
                }
                let key = match arg_text(&args, 0) {
                    Some(k) => k,
                    None => return cbor_error(":set requires key and value"),
                };
                let val = match arg_text(&args, 1) {
                    Some(v) => v,
                    None => return cbor_error(":set requires key and value"),
                };
                // Remove old reverse mapping if key existed
                if let Some(old_val) = state.fwd.get(&key).cloned() {
                    state.rev.remove(&old_val);
                }
                // Remove old forward mapping if value already mapped elsewhere
                if let Some(old_key) = state.rev.get(&val).cloned() {
                    state.fwd.remove(&old_key);
                }
                state.fwd.insert(key.clone(), val.clone());
                state.rev.insert(val, key);
                persist();
                cbor_ok(ciborium::Value::Null)
            }

            ":delete" => {
                if !is_writer(&msg.from) {
                    return cbor_error("permission denied");
                }
                match arg_text(&args, 0) {
                    Some(key) => {
                        if let Some(val) = state.fwd.remove(&key) {
                            state.rev.remove(&val);
                        }
                        persist();
                        cbor_ok(ciborium::Value::Null)
                    }
                    None => cbor_error(":delete requires a key"),
                }
            }

            ":delete_value" => {
                if !is_writer(&msg.from) {
                    return cbor_error("permission denied");
                }
                match arg_text(&args, 0) {
                    Some(val) => {
                        if let Some(key) = state.rev.remove(&val) {
                            state.fwd.remove(&key);
                        }
                        persist();
                        cbor_ok(ciborium::Value::Null)
                    }
                    None => cbor_error(":delete_value requires a value"),
                }
            }

            ":clear" => {
                if !is_writer(&msg.from) {
                    return cbor_error("permission denied");
                }
                state.fwd.clear();
                state.rev.clear();
                persist();
                cbor_ok(ciborium::Value::Null)
            }

            other => cbor_error(&format!("unknown verb: {other}")),
        }
    });

    send_reply(msg, result);
    Ok(vec![])
}
