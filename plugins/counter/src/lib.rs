use std::cell::RefCell;
use extism_pdk::*;

include!("../../shared/types.rs");

// ── State ─────────────────────────────────────────────────────────────────────

thread_local! {
    static VALUE: RefCell<i64> = const { RefCell::new(0) };
}

fn persist_counter(v: i64) {
    let bytes = cbor_encode(&ciborium::Value::Map(vec![(
        ciborium::Value::Text("value".into()),
        ciborium::Value::Integer(v.into()),
    )]));
    persist_state_bytes(&bytes);
}

// ── Exports ───────────────────────────────────────────────────────────────────

#[plugin_fn]
pub fn init(input: Vec<u8>) -> FnResult<Vec<u8>> {
    let payload: InitPayload = match ciborium::de::from_reader(input.as_slice()) {
        Ok(v) => v,
        Err(e) => return Ok(cbor_error(&format!("init decode: {e}"))),
    };

    if !payload.state.is_empty() {
        if let Ok(ciborium::Value::Map(entries)) =
            ciborium::de::from_reader::<ciborium::Value, _>(payload.state.as_slice())
        {
            for (k, v) in entries {
                if k == ciborium::Value::Text("value".into()) {
                    if let ciborium::Value::Integer(n) = v {
                        if let Ok(n64) = i64::try_from(n) {
                            VALUE.with(|cell| *cell.borrow_mut() = n64);
                        }
                    }
                }
            }
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

    let result = VALUE.with(|cell| {
        let mut val = cell.borrow_mut();
        match verb.as_str() {
            ":get" => cbor_ok(ciborium::Value::Integer((*val).into())),

            ":reset" => {
                *val = 0;
                persist_counter(*val);
                cbor_ok(ciborium::Value::Integer(0i64.into()))
            }

            ":inc" => {
                let n = arg_i64(&args, 0).unwrap_or(1);
                *val = val.saturating_add(n);
                persist_counter(*val);
                cbor_ok(ciborium::Value::Integer((*val).into()))
            }

            ":dec" => {
                let n = arg_i64(&args, 0).unwrap_or(1);
                *val = val.saturating_sub(n);
                persist_counter(*val);
                cbor_ok(ciborium::Value::Integer((*val).into()))
            }

            ":set" => match arg_i64(&args, 0) {
                Some(n) => {
                    *val = n;
                    persist_counter(*val);
                    cbor_ok(ciborium::Value::Integer((*val).into()))
                }
                None => cbor_error(":set requires an integer argument"),
            },

            other => cbor_error(&format!("unknown verb: {other}")),
        }
    });

    send_reply(msg, result);
    Ok(vec![])
}
