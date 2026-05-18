//! ABI types shared between the root plugin and the ma-runtime host.
//!
//! The plugin receives a `RootRequest` CBOR-encoded as input and returns a
//! `RootResponse` CBOR-encoded as output.  The runtime is the only party that
//! may mutate the manifest; the plugin returns a `commit` intent and the
//! runtime carries out the actual IPFS persist + registry reload.

use serde::{Deserialize, Serialize};

// ── Operations ────────────────────────────────────────────────────────────────

/// The operation the caller requested.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Op {
    /// Read a subtree or leaf and return the current value.
    Get,
    /// Set a leaf to an inline value (small payloads only).
    Set,
    /// Delete a leaf or entire subtree.
    Delete,
    /// Apply a YAML/JSON document fetched from `cid` to the given path.
    /// Used for large payloads — the runtime fetches and validates the CID.
    ApplyCid,
    /// Invoke a named verb on a path.  Verb semantics are path-defined.
    Verb,
}

// ── Subtree roots (v1 scope) ──────────────────────────────────────────────────

/// The top-level subtree the path addresses.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Subtree {
    Entities,
    Kinds,
    Config,
    /// Any other/unknown subtree — plugin returns an error for v1.
    Unknown(String),
}

impl Subtree {
    pub fn from_path(path: &str) -> Self {
        let root = path.split('.').next().unwrap_or("");
        match root {
            "entities" => Subtree::Entities,
            "kinds" => Subtree::Kinds,
            "config" => Subtree::Config,
            other => Subtree::Unknown(other.to_string()),
        }
    }
}

// ── Request ───────────────────────────────────────────────────────────────────

/// Input from the runtime to the root plugin.
///
/// Encoded as a CBOR map.  The runtime populates `caller_did` after verifying
/// the incoming RPC message signature; the plugin MUST NOT trust the caller
/// to set its own identity.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RootRequest {
    /// Requested operation.
    pub op: Op,

    /// Dot-separated path, e.g. `entities.fortune`, `entities.fortune.owner`,
    /// `kinds.stateless.python`, `config.poll_interval_ms`.
    pub path: String,

    /// Inline value for `Op::Set`.  Absent for other ops.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub value: Option<String>,

    /// IPFS CID for `Op::ApplyCid`.  Absent for other ops.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cid: Option<String>,

    /// Verb name for `Op::Verb`.  Absent for other ops.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub verb: Option<String>,

    /// Verified sender DID, injected by the runtime from the message `from` field.
    pub caller_did: String,

    /// Originating RPC message ID (for correlation).
    pub message_id: String,

    /// Runtime owner DID (authoritative, set by runtime, never from caller).
    pub owner_did: String,

    /// Current snapshot of the relevant subtree, serialised as a JSON value.
    /// For `entities.*` this is the RuntimeManifest entities map.
    /// For `kinds.*` this is the kinds tree.
    /// For `config.*` this is the config map.
    pub subtree_snapshot: serde_json::Value,
}

// ── Response ──────────────────────────────────────────────────────────────────

/// A single mutation the plugin wants the runtime to commit.
///
/// The runtime validates this intent before any IPFS write.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum CommitIntent {
    /// Insert or update an entity node (serialised as JSON).
    UpsertEntity { name: String, node: serde_json::Value },
    /// Remove an entity from the manifest.
    DeleteEntity { name: String },
    /// Insert or update a kind node.
    UpsertKind { family: String, implementation: String, node: serde_json::Value },
    /// Remove a kind from the manifest.
    DeleteKind { family: String, implementation: String },
    /// Set a key in the runtime config map.
    SetConfig { key: String, value: serde_json::Value },
    /// Delete a key from the runtime config map.
    DeleteConfig { key: String },
}

/// Output from the root plugin back to the runtime.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RootResponse {
    /// Whether the plugin handled the request successfully.
    pub ok: bool,

    /// Human-readable result for the caller (becomes the RPC reply body).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub result: Option<serde_json::Value>,

    /// Mutations the runtime should commit atomically (only present when ok).
    /// Empty for read-only ops.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub commit: Vec<CommitIntent>,

    /// Error reason when ok == false.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

impl RootResponse {
    pub fn ok(result: impl Into<serde_json::Value>) -> Self {
        Self { ok: true, result: Some(result.into()), commit: vec![], error: None }
    }

    pub fn ok_with_commit(result: impl Into<serde_json::Value>, commit: Vec<CommitIntent>) -> Self {
        Self { ok: true, result: Some(result.into()), commit, error: None }
    }

    pub fn err(reason: impl Into<String>) -> Self {
        Self { ok: false, result: None, commit: vec![], error: Some(reason.into()) }
    }
}

// ── Snapshot helpers ──────────────────────────────────────────────────────────

/// Drill into a JSON subtree snapshot following dot-separated path segments.
/// Returns `None` if the path does not exist.
pub fn get_nested<'a>(value: &'a serde_json::Value, path: &str) -> Option<&'a serde_json::Value> {
    let mut cur = value;
    for seg in path.split('.') {
        cur = cur.get(seg)?;
    }
    Some(cur)
}

/// Walk a dot-path into a mutable JSON snapshot and set the leaf.
/// Creates intermediate objects as needed.
pub fn set_nested(value: &mut serde_json::Value, path: &str, leaf: serde_json::Value) {
    let segs: Vec<&str> = path.split('.').collect();
    let mut cur = value;
    for seg in &segs[..segs.len() - 1] {
        if cur.get(seg).is_none() {
            cur[*seg] = serde_json::Value::Object(serde_json::Map::new());
        }
        cur = cur.get_mut(*seg).unwrap();
    }
    cur[segs[segs.len() - 1]] = leaf;
}

/// Remove a leaf from a dot-path in a mutable JSON snapshot.
pub fn delete_nested(value: &mut serde_json::Value, path: &str) {
    let segs: Vec<&str> = path.split('.').collect();
    if segs.len() == 1 {
        if let serde_json::Value::Object(map) = value {
            map.remove(segs[0]);
        }
        return;
    }
    let mut cur = value;
    for seg in &segs[..segs.len() - 1] {
        match cur.get_mut(*seg) {
            Some(v) => cur = v,
            None => return,
        }
    }
    if let serde_json::Value::Object(map) = cur {
        map.remove(segs[segs.len() - 1]);
    }
}

// ── CBOR codec ────────────────────────────────────────────────────────────────

/// Decode CBOR bytes into a typed value via serde_json round-trip.
pub fn from_cbor<T: for<'de> Deserialize<'de>>(bytes: &[u8]) -> Result<T, String> {
    let cbor: ciborium::Value =
        ciborium::de::from_reader(bytes).map_err(|e| format!("CBOR decode: {e}"))?;
    let json = cbor_to_json(cbor).map_err(|e| format!("CBOR->JSON: {e}"))?;
    serde_json::from_value(json).map_err(|e| format!("JSON deserialize: {e}"))
}

/// Encode a typed value to CBOR bytes via serde_json round-trip.
pub fn to_cbor<T: Serialize>(value: &T) -> Result<Vec<u8>, String> {
    let json = serde_json::to_value(value).map_err(|e| format!("JSON serialize: {e}"))?;
    let cbor = json_to_cbor(json).map_err(|e| format!("JSON->CBOR: {e}"))?;
    let mut buf = Vec::new();
    ciborium::ser::into_writer(&cbor, &mut buf)
        .map_err(|e| format!("CBOR encode: {e}"))?;
    Ok(buf)
}

fn cbor_to_json(v: ciborium::Value) -> Result<serde_json::Value, String> {
    Ok(match v {
        ciborium::Value::Null => serde_json::Value::Null,
        ciborium::Value::Bool(b) => serde_json::Value::Bool(b),
        ciborium::Value::Integer(i) => {
            let n: i64 = i.try_into().map_err(|_| "CBOR integer overflow".to_string())?;
            serde_json::Value::Number(n.into())
        }
        ciborium::Value::Float(f) => serde_json::Value::Number(
            serde_json::Number::from_f64(f)
                .ok_or_else(|| "non-finite float".to_string())?,
        ),
        ciborium::Value::Text(s) => serde_json::Value::String(s),
        ciborium::Value::Bytes(b) => serde_json::Value::Array(
            b.into_iter().map(|byte| serde_json::Value::Number(byte.into())).collect(),
        ),
        ciborium::Value::Array(arr) => serde_json::Value::Array(
            arr.into_iter().map(cbor_to_json).collect::<Result<Vec<_>, _>>()?,
        ),
        ciborium::Value::Map(map) => {
            let mut obj = serde_json::Map::new();
            for (k, v) in map {
                let key = if let ciborium::Value::Text(s) = k { s } else { format!("{k:?}") };
                obj.insert(key, cbor_to_json(v)?);
            }
            serde_json::Value::Object(obj)
        }
        _ => serde_json::Value::Null,
    })
}

fn json_to_cbor(v: serde_json::Value) -> Result<ciborium::Value, String> {
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
        serde_json::Value::Array(arr) => ciborium::Value::Array(
            arr.into_iter().map(json_to_cbor).collect::<Result<Vec<_>, _>>()?,
        ),
        serde_json::Value::Object(map) => ciborium::Value::Map(
            map.into_iter()
                .map(|(k, v)| Ok((ciborium::Value::Text(k), json_to_cbor(v)?)))
                .collect::<Result<Vec<_>, String>>()?,
        ),
    })
}
