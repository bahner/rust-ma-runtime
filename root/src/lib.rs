//! ma-root-plugin — Extism plugin implementing /ma/root/0.0.1
//!
//! This plugin handles CRUD operations for the runtime manifest subtrees:
//! `entities.*`, `kinds.*`, `config.*`.
//!
//! The plugin never writes to IPFS directly.  It validates input, determines
//! what should change, and returns `CommitIntent` values that the host runtime
//! commits atomically.
//!
//! # ABI
//!
//! Input:  CBOR-encoded `RootRequest`
//! Output: CBOR-encoded `RootResponse`
//! Export: `handle_cast`  (one entry-point for all CRUD ops, kind = /ma/root/0.0.1)

use extism_pdk::*;

mod abi;
mod handlers;

use abi::{from_cbor, to_cbor, RootRequest, RootResponse};

#[plugin_fn]
pub fn handle_cast(input: Vec<u8>) -> FnResult<Vec<u8>> {
    let req: RootRequest = match from_cbor(&input) {
        Ok(r) => r,
        Err(e) => {
            let resp = RootResponse::err(format!("request decode failed: {e}"));
            return Ok(to_cbor(&resp).unwrap_or_default());
        }
    };

    let resp = handlers::dispatch(req);
    Ok(to_cbor(&resp).unwrap_or_else(|e| {
        // Last-resort: encode a plain error if serialisation fails
        let fallback = RootResponse::err(format!("response encode failed: {e}"));
        to_cbor(&fallback).unwrap_or_default()
    }))
}
