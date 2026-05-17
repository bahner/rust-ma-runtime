//! Intra-runtime message types, plugin I/O types, and IPLD schema types for
//! the entity dispatch system.

use std::collections::{BTreeMap, HashMap};

use serde::{Deserialize, Serialize};

pub type KindTree = BTreeMap<String, BTreeMap<String, KindRef>>;

// ── IPLD link ─────────────────────────────────────────────────────────────────

/// An IPLD DAG link serialised as `{"/": "bafy..."}`.
///
/// Kubo's `dag/put` endpoint with `input-codec=dag-json` recognises this as a
/// proper IPLD link, enabling `ipfs dag get` traversal.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IpldLink {
    #[serde(rename = "/")]
    pub cid: String,
}

impl IpldLink {
    pub fn new(cid: impl Into<String>) -> Self {
        Self { cid: cid.into() }
    }
}

// ── Local (intra-runtime) message ─────────────────────────────────────────────

#[allow(dead_code)]
pub const LOCAL_RPC_CONTENT_TYPE: &str = "application/x-ma-local-rpc";

/// An intra-runtime message.  Follows the same schema as `ma_core::Message`
/// but `from` and `to` may be bare fragments (`#fragment`) or full DIDs, and
/// no signature is required — the runtime is the trusted authority for local
/// dispatch.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LocalMessage {
    pub id: String,
    pub from: String,
    pub to: String,
    /// Unix nanoepoch.
    pub created_at: u64,
    /// Expiry as Unix nanoepoch.
    pub expires: u64,
    pub reply_to: Option<String>,
    pub content_type: String,
    /// CBOR-encoded payload (verb atom or `[":verb", args…]` array).
    pub content: Vec<u8>,
}

// ── Plugin context and I/O ────────────────────────────────────────────────────

/// Which ABI a plugin kind implements.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PluginKind {
    /// Stateless — exports `handle_cast`.  No state is passed in or out.
    /// Replies are sent via the `ma_send` host function.
    Stateless,
    /// Stateful — exports `handle_call`.  Runtime passes current state in;
    /// plugin returns new state bytes.  Replies via `ma_send` host function.
    Stateful,
}

impl PluginKind {
    /// Derive kind from the `EntityNode.kind` protocol string
    /// (e.g. `/ma/stateful/python/0.0.1`).
    pub fn from_kind_str(s: &str) -> Self {
        if s.contains("stateful") {
            PluginKind::Stateful
        } else {
            PluginKind::Stateless
        }
    }
}

/// Context injected into every plugin call.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PluginCtx {
    /// Full DID-URL of this entity, e.g. `did:ma:<runtime>#fortune`.
    #[serde(rename = "self")]
    pub self_did: String,
    /// DID of the entity's owner.
    pub owner: String,
}

/// Input passed (CBOR-encoded) to both `handle_cast` (stateless) and
/// `handle_call` (stateful) exports.
///
/// State is **not** included here — stateful plugins own their state
/// internally and receive it once via `init(state_bytes)`.  They persist it
/// by calling the `ma_set_state` host function.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CastInput {
    pub msg: LocalMessage,
    pub ctx: PluginCtx,
}

/// Input for the `ma_reply` host function.
///
/// Plugin passes the original `LocalMessage` back alongside reply content;
/// the runtime fills in `to` and `reply_to` automatically from `msg`.
/// `content_type` is the MIME type of the reply body (e.g. `text/plain`,
/// `application/cbor`).  The wire `message_type` is set automatically by the
/// runtime based on `reply_to` being present.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplyRequest {
    /// The original incoming message — used to determine routing.
    pub msg: LocalMessage,
    /// MIME type of the reply body.
    pub content_type: String,
    /// Serialised reply body bytes.
    pub content: Vec<u8>,
}

// ── IPLD schema types ─────────────────────────────────────────────────────────

/// IPLD node representing a kind (Wasm ABI contract).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KindNode {
    pub protocol: String,
    /// Wasm exports this kind must provide.
    pub api: Vec<String>,
    /// Host functions the runtime makes available to plugins of this kind.
    /// Principle of least privilege: only register what the kind actually needs.
    pub host_functions: Vec<String>,
    /// Whether plugins of this kind require WASI system-call support.
    /// Set `true` for Python/WASI-compiled plugins; `false` for native Rust extism plugins.
    #[serde(default)]
    pub wasi: bool,
}

/// Kind reference stored in nested dict layout:
/// kinds.<family>.<implementation>.
///
/// New manifests store a plain IPLD link. Older manifests stored an object
/// with duplicated metadata and a nested `link` field.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum KindRef {
    Link(IpldLink),
    Legacy { link: IpldLink },
}

impl KindRef {
    pub fn link(&self) -> &IpldLink {
        match self {
            Self::Link(link) | Self::Legacy { link } => link,
        }
    }
}

/// IPLD node representing a single entity.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EntityNode {
    pub name: String,
    pub kind: String,
    /// IPLD link to the Wasm plugin bytes stored on IPFS.
    pub behavior: IpldLink,
    /// IPLD link to persisted state (optional, encrypted with the runtime key).
    /// Omitted when absent, which is the expected shape for stateless entities.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub state: Option<IpldLink>,
    pub owner: String,
    pub acl: Vec<String>,
    /// Whether this plugin requires WASI system-call support.
    /// Inherited from the kind definition at bootstrap time.
    #[serde(default)]
    pub wasi: bool,
}

/// Root IPLD node for this runtime.
/// Stored av CID i `config.yaml` som `bootstrap_cid` (tidligere `root_cid`)
/// one-time fallback) and published into the DID document under `ma.runtime`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RuntimeManifest {
    pub owner: String,
    #[serde(default)]
    pub kinds: KindTree,
    pub entities: HashMap<String, IpldLink>,
    #[serde(default)]
    pub locales: HashMap<String, IpldLink>,
    #[serde(default)]
    pub config: BTreeMap<String, serde_json::Value>,
}

impl RuntimeManifest {
    pub fn kind_link(&self, protocol: &str) -> Option<&IpldLink> {
        let (_, family, implementation, _) = split_kind_protocol(protocol)?;
        Some(self.kinds.get(family)?.get(implementation)?.link())
    }
}

fn split_kind_protocol(protocol: &str) -> Option<(&str, &str, &str, &str)> {
    // Expected: /ma/<family>/<implementation>/<version>
    let parts: Vec<&str> = protocol.trim_matches('/').split('/').collect();
    if parts.len() == 4 && parts[0] == "ma" {
        Some((parts[0], parts[1], parts[2], parts[3]))
    } else {
        None
    }
}

// ── Plugin host-function I/O ──────────────────────────────────────────────────

/// Outbound message queued by a plugin via the `ma_send` host function.
///
/// `to`       — recipient DID (or DID-URL).
/// `reply_to` — if set, marks this as a reply to the given message ID; the
///              runtime will use `MESSAGE_TYPE_RPC_REPLY` for the wire message.
/// `content`  — raw payload bytes (e.g. CBOR-encoded RPC atom or JSON).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SendEnvelope {
    pub to: String,
    pub content_type: String,
    pub content: Vec<u8>,
    pub reply_to: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::{EntityNode, IpldLink};

    #[test]
    fn serializing_entity_without_state_omits_state_field() {
        let node = EntityNode {
            name: "fortune".to_string(),
            kind: "/ma/stateless/python/0.0.1".to_string(),
            behavior: IpldLink::new("bafybehavior"),
            state: None,
            owner: "did:ma:k51qzi5uqu5example".to_string(),
            acl: vec!["*".to_string()],
            wasi: true,
        };

        let value = serde_json::to_value(&node).expect("serialize entity node");
        assert!(value.get("state").is_none(), "state must be omitted when None");
    }

    #[test]
    fn deserializing_entity_accepts_missing_state_field() {
        let raw = r#"{
            "name": "fortune",
            "kind": "/ma/stateless/python/0.0.1",
            "behavior": {"/": "bafybehavior"},
            "owner": "did:ma:k51qzi5uqu5example",
            "acl": ["*"],
            "wasi": true
        }"#;

        let node: EntityNode = serde_json::from_str(raw).expect("deserialize entity node");
        assert!(node.state.is_none(), "missing state should deserialize as None");
    }

    #[test]
    fn deserializing_entity_accepts_null_state_field() {
        let raw = r#"{
            "name": "fortune",
            "kind": "/ma/stateless/python/0.0.1",
            "behavior": {"/": "bafybehavior"},
            "state": null,
            "owner": "did:ma:k51qzi5uqu5example",
            "acl": ["*"],
            "wasi": true
        }"#;

        let node: EntityNode = serde_json::from_str(raw).expect("deserialize entity node");
        assert!(node.state.is_none(), "null state should deserialize as None");
    }
}
