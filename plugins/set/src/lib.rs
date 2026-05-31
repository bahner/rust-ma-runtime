use std::cell::RefCell;
use extism_pdk::*;

include!("../../shared/types.rs");

// ── State ─────────────────────────────────────────────────────────────────────

thread_local! {
    static MEMBERS: RefCell<Vec<String>> = RefCell::new(Vec::new());
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
    MEMBERS.with(|cell| {
        let members = cell.borrow();
        let val = ciborium::Value::Array(
            members
                .iter()
                .map(|s| ciborium::Value::Text(s.clone()))
                .collect(),
        );
        let bytes = cbor_encode(&val);
        persist_state_bytes(&bytes);
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
        if let Ok(ciborium::Value::Array(items)) =
            ciborium::de::from_reader::<ciborium::Value, _>(payload.state.as_slice())
        {
            let members: Vec<String> = items
                .into_iter()
                .filter_map(|v| {
                    if let ciborium::Value::Text(s) = v {
                        Some(s)
                    } else {
                        None
                    }
                })
                .collect();
            MEMBERS.with(|cell| *cell.borrow_mut() = members);
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

    let result = MEMBERS.with(|cell| {
        let mut members = cell.borrow_mut();
        match verb.as_str() {
            // ── Read verbs (anyone) ───────────────────────────────────────────
            ":count" => cbor_ok(ciborium::Value::Integer(
                (members.len() as i64).into(),
            )),

            ":members" => {
                let arr: Vec<ciborium::Value> = members
                    .iter()
                    .map(|s| ciborium::Value::Text(s.clone()))
                    .collect();
                cbor_ok(ciborium::Value::Array(arr))
            }

            ":contains" => match arg_text(&args, 0) {
                Some(val) => cbor_ok(ciborium::Value::Bool(members.contains(&val))),
                None => cbor_error(":contains requires a value"),
            },

            // ── Write verbs (parent / self only) ─────────────────────────────
            ":add" => {
                if !is_writer(&msg.from) {
                    return cbor_error("permission denied");
                }
                match arg_text(&args, 0) {
                    Some(val) => {
                        if !members.contains(&val) {
                            members.push(val);
                            persist();
                        }
                        cbor_ok(ciborium::Value::Null)
                    }
                    None => cbor_error(":add requires a value"),
                }
            }

            ":remove" => {
                if !is_writer(&msg.from) {
                    return cbor_error("permission denied");
                }
                match arg_text(&args, 0) {
                    Some(val) => {
                        members.retain(|m| m != &val);
                        persist();
                        cbor_ok(ciborium::Value::Null)
                    }
                    None => cbor_error(":remove requires a value"),
                }
            }

            ":clear" => {
                if !is_writer(&msg.from) {
                    return cbor_error("permission denied");
                }
                members.clear();
                persist();
                cbor_ok(ciborium::Value::Null)
            }

            other => cbor_error(&format!("unknown verb: {other}")),
        }
    });

    send_reply(msg, result);
    Ok(vec![])
}
