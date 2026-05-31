use std::cell::RefCell;
use extism_pdk::*;

include!("../../shared/types.rs");

// ── Context (no persistent state — root never calls ma_set_state) ─────────────

thread_local! {
    static SELF_DID: RefCell<String> = RefCell::new(String::new());
    static PARENT: RefCell<Option<String>> = const { RefCell::new(None) };
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

    Ok(cbor_ok_unit())
}

#[plugin_fn]
pub fn handle_cast(input: Vec<u8>) -> FnResult<Vec<u8>> {
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

    match verb.as_str() {
        ":ping" => {
            send_reply(msg, cbor_encode(&ciborium::Value::Text(":pong".into())));
        }

        ":create" => {
            // [:create, kind_protocol, cid]
            // [:create, kind_protocol, cid, label]
            let kind = match arg_text(&args, 0) {
                Some(k) => k,
                None => {
                    send_reply(msg, cbor_error(":create requires kind and cid"));
                    return Ok(vec![]);
                }
            };
            let cid = match arg_text(&args, 1) {
                Some(c) => c,
                None => {
                    send_reply(msg, cbor_error(":create requires kind and cid"));
                    return Ok(vec![]);
                }
            };
            let label = arg_text(&args, 2);

            // Build the create request: {"kind": kind, "behaviour": cid}
            let req = ciborium::Value::Map(vec![
                (
                    ciborium::Value::Text("kind".into()),
                    ciborium::Value::Text(kind),
                ),
                (
                    ciborium::Value::Text("behaviour".into()),
                    ciborium::Value::Text(cid),
                ),
            ]);
            let req_bytes = cbor_encode(&req);
            let result_offset = unsafe { ma_create_entity(mem_write(&req_bytes)) };
            let result_bytes = mem_read(result_offset);

            if result_bytes.is_empty() {
                send_reply(msg, cbor_error("create failed"));
                return Ok(vec![]);
            }

            let fragment: String = match ciborium::de::from_reader(result_bytes.as_slice()) {
                Ok(v) => v,
                Err(e) => {
                    send_reply(msg, cbor_error(&format!("create result decode: {e}")));
                    return Ok(vec![]);
                }
            };

            let reply_val = match label {
                Some(lbl) => cbor_ok(ciborium::Value::Array(vec![
                    ciborium::Value::Text(fragment),
                    ciborium::Value::Text(lbl),
                ])),
                None => cbor_ok(ciborium::Value::Text(fragment)),
            };
            send_reply(msg, reply_val);
        }

        ":delete" => {
            let fragment = match arg_text(&args, 0) {
                Some(f) => f,
                None => {
                    send_reply(msg, cbor_error(":delete requires a fragment"));
                    return Ok(vec![]);
                }
            };
            let frag_bytes = cbor_encode(&ciborium::Value::Text(fragment));
            unsafe { ma_delete_entity(mem_write(&frag_bytes)); }
            send_reply(msg, cbor_ok_unit());
        }

        other => {
            send_reply(msg, cbor_error(&format!("unknown verb: {other}")));
        }
    }

    Ok(vec![])
}
