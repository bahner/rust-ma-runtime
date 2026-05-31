// Shared wire types and CBOR helpers for ma plugins.
// This file is included verbatim by each plugin with `include!("../../shared/types.rs")`.

use serde::{Deserialize, Serialize};

// ── Wire types (must match ma-runtime/src/entity.rs) ─────────────────────────

#[derive(Debug, Deserialize)]
pub struct EntityCtx {
    #[serde(rename = "self")]
    pub self_did: String,
    pub fragment: String,
    #[serde(default)]
    pub parent: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct LocalMessage {
    pub id: String,
    pub from: String,
    pub to: String,
    pub created_at: u64,
    pub expires: u64,
    #[serde(default)]
    pub reply_to: Option<String>,
    pub content_type: String,
    /// Inner CBOR-encoded verb: ":verb" or [":verb", args…]
    #[serde(with = "serde_bytes")]
    pub content: Vec<u8>,
}

#[derive(Debug, Deserialize)]
pub struct CastInput {
    pub msg: LocalMessage,
}

#[derive(Debug, Deserialize)]
pub struct InitPayload {
    pub ctx: EntityCtx,
    /// Last persisted state bytes (CBOR-encoded); absent / empty when new.
    #[serde(with = "serde_bytes", default)]
    pub state: Vec<u8>,
}

/// Passed to the `ma_reply` host function.
#[derive(Serialize)]
pub struct ReplyRequest<'a> {
    pub msg: &'a LocalMessage,
    pub content_type: &'static str,
    #[serde(with = "serde_bytes")]
    pub content: Vec<u8>,
}

// ── Host function declarations ────────────────────────────────────────────────

#[link(wasm_import_module = "extism:host/user")]
unsafe extern "C" {
    pub fn ma_reply(ptr: u64) -> u64;
    pub fn ma_set_state(ptr: u64) -> u64;
    pub fn ma_send(ptr: u64) -> u64;
    pub fn ma_create_entity(ptr: u64) -> u64;
    pub fn ma_delete_entity(ptr: u64) -> u64;
}

// ── Memory helpers ────────────────────────────────────────────────────────────

/// Allocate extism memory, write `data`, and return the offset.
pub fn mem_write(data: &[u8]) -> u64 {
    extism_pdk::Memory::from_bytes(data)
        .expect("extism memory alloc")
        .offset()
}

/// Read extism memory at `offset` as bytes. Returns empty vec when offset == 0.
pub fn mem_read(offset: u64) -> Vec<u8> {
    if offset == 0 {
        return Vec::new();
    }
    extism_pdk::Memory::find(offset)
        .map(|m| m.to_vec())
        .unwrap_or_default()
}

// ── CBOR helpers ──────────────────────────────────────────────────────────────

pub fn cbor_encode(val: &ciborium::Value) -> Vec<u8> {
    let mut out = Vec::new();
    ciborium::ser::into_writer(val, &mut out).expect("cbor encode");
    out
}

/// Encode CBOR `:ok` atom (used as init() return value).
pub fn cbor_ok_unit() -> Vec<u8> {
    cbor_encode(&ciborium::Value::Text(":ok".into()))
}

/// Encode `[":ok", value]`.
pub fn cbor_ok(val: ciborium::Value) -> Vec<u8> {
    cbor_encode(&ciborium::Value::Array(vec![
        ciborium::Value::Text(":ok".into()),
        val,
    ]))
}

/// Encode `[":error", reason]`.
pub fn cbor_error(reason: &str) -> Vec<u8> {
    cbor_encode(&ciborium::Value::Array(vec![
        ciborium::Value::Text(":error".into()),
        ciborium::Value::Text(reason.into()),
    ]))
}

/// Send a reply via the `ma_reply` host function.
pub fn send_reply(msg: &LocalMessage, content: Vec<u8>) {
    let req = ReplyRequest {
        msg,
        content_type: "application/cbor",
        content,
    };
    let mut bytes = Vec::new();
    if ciborium::ser::into_writer(&req, &mut bytes).is_err() {
        return;
    }
    unsafe { ma_reply(mem_write(&bytes)); }
}

/// Persist state bytes via the `ma_set_state` host function.
pub fn persist_state_bytes(bytes: &[u8]) {
    unsafe { ma_set_state(mem_write(bytes)); }
}

/// Parse the verb term from `msg.content`.
/// Returns `(verb_str, args_vec)` or sends an error reply and returns None.
pub fn parse_verb<'a>(
    content: &[u8],
) -> Option<(String, Vec<ciborium::Value>)> {
    let term: ciborium::Value = ciborium::de::from_reader(content).ok()?;
    match term {
        ciborium::Value::Text(s) => Some((s, vec![])),
        ciborium::Value::Array(items) if !items.is_empty() => {
            if let ciborium::Value::Text(v) = &items[0] {
                Some((v.clone(), items[1..].to_vec()))
            } else {
                None
            }
        }
        _ => None,
    }
}

/// Extract a text string argument from the args slice at `idx`.
pub fn arg_text(args: &[ciborium::Value], idx: usize) -> Option<String> {
    match args.get(idx) {
        Some(ciborium::Value::Text(s)) => Some(s.clone()),
        _ => None,
    }
}

/// Extract an i64 from args at `idx`.
pub fn arg_i64(args: &[ciborium::Value], idx: usize) -> Option<i64> {
    match args.get(idx) {
        Some(ciborium::Value::Integer(n)) => i64::try_from(*n).ok(),
        _ => None,
    }
}
