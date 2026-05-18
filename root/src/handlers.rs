//! CRUD dispatch for the three v1 subtrees: entities, kinds, config.

use serde_json::Value;

use crate::abi::{
    delete_nested, get_nested, set_nested, CommitIntent, Op, RootRequest, RootResponse, Subtree,
};

// ── Authz ──────────────────────────────────────────────────────────────────────

fn is_write(op: &Op) -> bool {
    matches!(op, Op::Set | Op::Delete | Op::ApplyCid)
}

fn require_owner(req: &RootRequest) -> Option<RootResponse> {
    if is_write(&req.op) && req.caller_did != req.owner_did {
        Some(RootResponse::err(format!(
            "permission denied: caller {} is not owner {}",
            req.caller_did, req.owner_did
        )))
    } else {
        None
    }
}

// ── Main dispatcher ───────────────────────────────────────────────────────────

pub fn dispatch(req: RootRequest) -> RootResponse {
    // Phase 1: authz check (owner-only writes).
    if let Some(denied) = require_owner(&req) {
        return denied;
    }

    let subtree = Subtree::from_path(&req.path);
    match subtree {
        Subtree::Entities => handle_entities(req),
        Subtree::Kinds => handle_kinds(req),
        Subtree::Config => handle_config(req),
        Subtree::Unknown(root) => RootResponse::err(format!(
            "unknown path root '{root}'; supported: entities, kinds, config"
        )),
    }
}

// ── Entities subtree ───────────────────────────────────────────────────────────

fn handle_entities(req: RootRequest) -> RootResponse {
    // Strip the "entities" root prefix for relative navigation.
    let rel_path: String = req
        .path
        .strip_prefix("entities")
        .unwrap_or(&req.path)
        .trim_start_matches('.')
        .to_string();
    let rel_path = rel_path.as_str();

    match req.op {
        // GET ── list all entities if path == "entities", else return leaf
        Op::Get => {
            if rel_path.is_empty() {
                return RootResponse::ok(req.subtree_snapshot.clone());
            }
            match get_nested(&req.subtree_snapshot, rel_path) {
                Some(v) => RootResponse::ok(v.clone()),
                None => RootResponse::err(format!("not found: {}", req.path)),
            }
        }

        // DELETE ── remove entity or a leaf within an entity
        Op::Delete => {
            if rel_path.is_empty() {
                return RootResponse::err("cannot delete the entire entities subtree".to_string());
            }

            let entity_name = rel_path.split('.').next().unwrap_or(rel_path);
            if !field_exists(&req.subtree_snapshot, entity_name) {
                return RootResponse::err(format!("entity not found: {entity_name}"));
            }

            // Full entity delete
            if rel_path == entity_name {
                return RootResponse::ok_with_commit(
                    Value::String(format!("deleted entity {entity_name}")),
                    vec![CommitIntent::DeleteEntity { name: entity_name.to_string() }],
                );
            }

            // Leaf delete within an entity (e.g. entities.fortune.owner)
            // Get current entity node from snapshot, mutate, return upsert
            match req.subtree_snapshot.get(entity_name).cloned() {
                None => RootResponse::err(format!("entity not found: {entity_name}")),
                Some(mut node) => {
                    let leaf_path = rel_path
                        .strip_prefix(entity_name)
                        .unwrap_or("")
                        .trim_start_matches('.');
                    delete_nested(&mut node, leaf_path);
                    RootResponse::ok_with_commit(
                        Value::String(format!("deleted {}", req.path)),
                        vec![CommitIntent::UpsertEntity {
                            name: entity_name.to_string(),
                            node,
                        }],
                    )
                }
            }
        }

        // SET ── set a leaf value within an entity
        Op::Set => {
            let value_str = match req.value.as_deref() {
                Some(v) => v,
                None => return RootResponse::err("Op::Set requires a value".to_string()),
            };
            let entity_name = rel_path.split('.').next().unwrap_or(rel_path);
            let leaf_path = rel_path
                .strip_prefix(entity_name)
                .unwrap_or("")
                .trim_start_matches('.');

            if leaf_path.is_empty() {
                return RootResponse::err(
                    "cannot set an entity node directly; set a leaf field".to_string(),
                );
            }

            let mut node = req
                .subtree_snapshot
                .get(entity_name)
                .cloned()
                .unwrap_or(Value::Object(serde_json::Map::new()));

            set_nested(&mut node, leaf_path, Value::String(value_str.to_string()));
            RootResponse::ok_with_commit(
                Value::String(format!("set {}", req.path)),
                vec![CommitIntent::UpsertEntity {
                    name: entity_name.to_string(),
                    node,
                }],
            )
        }

        // APPLY_CID ── replace entire entity with validated document from IPFS CID
        Op::ApplyCid => {
            let cid = match req.cid.as_deref() {
                Some(c) => c,
                None => return RootResponse::err("Op::ApplyCid requires a cid".to_string()),
            };
            let entity_name = if rel_path.is_empty() {
                return RootResponse::err(
                    "Op::ApplyCid requires an entity name in the path".to_string(),
                );
            } else {
                rel_path.split('.').next().unwrap_or(rel_path)
            };

            // The runtime will fetch the CID, validate schema/kind, and commit.
            // We return the intent with just enough info for the runtime to proceed.
            RootResponse::ok_with_commit(
                Value::String(format!("apply cid {cid} to entity {entity_name}")),
                vec![CommitIntent::UpsertEntity {
                    name: entity_name.to_string(),
                    // Sentinel: an IPLD-link map signals to runtime to fetch + validate.
                    node: serde_json::json!({ "/": cid }),
                }],
            )
        }

        Op::Verb => handle_entity_verb(req, rel_path),
    }
}

fn handle_entity_verb(req: RootRequest, rel_path: &str) -> RootResponse {
    let verb = req.verb.as_deref().unwrap_or("");
    let entity_name = rel_path.split('.').next().unwrap_or(rel_path);

    match verb {
        // :list — same as GET with no path
        "list" => RootResponse::ok(req.subtree_snapshot.clone()),

        // :reload — signals runtime to reload plugin from current CID
        "reload" => RootResponse::ok_with_commit(
            Value::String(format!("reload entity {entity_name}")),
            vec![CommitIntent::UpsertEntity {
                name: entity_name.to_string(),
                node: req.subtree_snapshot.get(entity_name).cloned().unwrap_or(Value::Null),
            }],
        ),

        other => RootResponse::err(format!("unknown entity verb: {other}")),
    }
}

// ── Kinds subtree ──────────────────────────────────────────────────────────────

fn handle_kinds(req: RootRequest) -> RootResponse {
    let rel_path: String = req
        .path
        .strip_prefix("kinds")
        .unwrap_or(&req.path)
        .trim_start_matches('.')
        .to_string();
    let rel_path = rel_path.as_str();

    match req.op {
        Op::Get => {
            if rel_path.is_empty() {
                return RootResponse::ok(req.subtree_snapshot.clone());
            }
            match get_nested(&req.subtree_snapshot, rel_path) {
                Some(v) => RootResponse::ok(v.clone()),
                None => RootResponse::err(format!("not found: {}", req.path)),
            }
        }

        Op::Delete => {
            // Path must be at least family.implementation
            let parts: Vec<&str> = rel_path.split('.').collect();
            if parts.len() < 2 {
                return RootResponse::err(
                    "kind path must be family.implementation (e.g. kinds.stateless.python)"
                        .to_string(),
                );
            }
            let (family, implementation) = (parts[0], parts[1]);
            RootResponse::ok_with_commit(
                Value::String(format!("deleted kind {family}/{implementation}")),
                vec![CommitIntent::DeleteKind {
                    family: family.to_string(),
                    implementation: implementation.to_string(),
                }],
            )
        }

        Op::ApplyCid => {
            let cid = match req.cid.as_deref() {
                Some(c) => c,
                None => return RootResponse::err("Op::ApplyCid requires a cid".to_string()),
            };
            let parts: Vec<&str> = rel_path.split('.').collect();
            if parts.len() < 2 {
                return RootResponse::err(
                    "kind path must be family.implementation".to_string(),
                );
            }
            let (family, implementation) = (parts[0], parts[1]);
            RootResponse::ok_with_commit(
                Value::String(format!("apply cid {cid} to kind {family}/{implementation}")),
                vec![CommitIntent::UpsertKind {
                    family: family.to_string(),
                    implementation: implementation.to_string(),
                    node: serde_json::json!({ "/": cid }),
                }],
            )
        }

        Op::Set => RootResponse::err(
            "use apply_cid to create/update a kind node".to_string(),
        ),

        Op::Verb => {
            let verb = req.verb.as_deref().unwrap_or("");
            match verb {
                "list" => RootResponse::ok(req.subtree_snapshot.clone()),
                other => RootResponse::err(format!("unknown kind verb: {other}")),
            }
        }
    }
}

// ── Config subtree ─────────────────────────────────────────────────────────────

fn handle_config(req: RootRequest) -> RootResponse {
    let rel_path: String = req
        .path
        .strip_prefix("config")
        .unwrap_or(&req.path)
        .trim_start_matches('.')
        .to_string();
    let rel_path = rel_path.as_str();

    match req.op {
        Op::Get => {
            if rel_path.is_empty() {
                return RootResponse::ok(req.subtree_snapshot.clone());
            }
            match get_nested(&req.subtree_snapshot, rel_path) {
                Some(v) => RootResponse::ok(v.clone()),
                None => RootResponse::err(format!("not found: {}", req.path)),
            }
        }

        Op::Set => {
            let value_str = match req.value.as_deref() {
                Some(v) => v,
                None => return RootResponse::err("Op::Set requires a value".to_string()),
            };
            if rel_path.is_empty() {
                return RootResponse::err("cannot set entire config; specify a key".to_string());
            }
            // Try to parse value as JSON first, fall back to plain string
            let json_value: Value = serde_json::from_str(value_str)
                .unwrap_or_else(|_| Value::String(value_str.to_string()));
            RootResponse::ok_with_commit(
                Value::String(format!("set config.{rel_path}")),
                vec![CommitIntent::SetConfig {
                    key: rel_path.to_string(),
                    value: json_value,
                }],
            )
        }

        Op::Delete => {
            if rel_path.is_empty() {
                return RootResponse::err("cannot delete entire config subtree".to_string());
            }
            RootResponse::ok_with_commit(
                Value::String(format!("deleted config.{rel_path}")),
                vec![CommitIntent::DeleteConfig { key: rel_path.to_string() }],
            )
        }

        Op::ApplyCid => RootResponse::err(
            "apply_cid not supported for config; use set with inline value".to_string(),
        ),

        Op::Verb => {
            let verb = req.verb.as_deref().unwrap_or("");
            match verb {
                "list" => RootResponse::ok(req.subtree_snapshot.clone()),
                other => RootResponse::err(format!("unknown config verb: {other}")),
            }
        }
    }
}

// ── Helpers ────────────────────────────────────────────────────────────────────

fn field_exists(snapshot: &Value, key: &str) -> bool {
    snapshot.get(key).is_some()
}

// ── Tests ──────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::abi::{Op, RootRequest};
    use serde_json::json;

    fn make_req(op: Op, path: &str, snapshot: Value) -> RootRequest {
        RootRequest {
            op,
            path: path.to_string(),
            value: None,
            cid: None,
            verb: None,
            caller_did: "did:ma:owner".to_string(),
            message_id: "test-msg".to_string(),
            owner_did: "did:ma:owner".to_string(),
            subtree_snapshot: snapshot,
        }
    }

    #[test]
    fn get_entities_list() {
        let snapshot = json!({ "fortune": { "name": "fortune", "kind": "/ma/stateless/python/0.0.1" } });
        let req = make_req(Op::Get, "entities", snapshot.clone());
        let resp = dispatch(req);
        assert!(resp.ok);
        assert_eq!(resp.result.unwrap(), snapshot);
    }

    #[test]
    fn get_entity_field() {
        let snapshot = json!({ "fortune": { "owner": "did:ma:owner" } });
        let req = make_req(Op::Get, "entities.fortune.owner", snapshot);
        let resp = dispatch(req);
        assert!(resp.ok);
        assert_eq!(resp.result.unwrap(), json!("did:ma:owner"));
    }

    #[test]
    fn set_entity_field_produces_upsert_intent() {
        let snapshot = json!({ "fortune": { "owner": "did:ma:old" } });
        let mut req = make_req(Op::Set, "entities.fortune.owner", snapshot);
        req.value = Some("did:ma:new".to_string());
        let resp = dispatch(req);
        assert!(resp.ok);
        assert_eq!(resp.commit.len(), 1);
        match &resp.commit[0] {
            CommitIntent::UpsertEntity { name, node } => {
                assert_eq!(name, "fortune");
                assert_eq!(node["owner"], json!("did:ma:new"));
            }
            other => panic!("unexpected intent: {other:?}"),
        }
    }

    #[test]
    fn delete_entity_produces_delete_intent() {
        let snapshot = json!({ "fortune": { "name": "fortune" } });
        let req = make_req(Op::Delete, "entities.fortune", snapshot);
        let resp = dispatch(req);
        assert!(resp.ok);
        assert_eq!(resp.commit.len(), 1);
        assert!(matches!(&resp.commit[0], CommitIntent::DeleteEntity { name } if name == "fortune"));
    }

    #[test]
    fn non_owner_write_denied() {
        let snapshot = json!({});
        let mut req = make_req(Op::Delete, "entities.fortune", snapshot);
        req.caller_did = "did:ma:someone-else".to_string();
        let resp = dispatch(req);
        assert!(!resp.ok);
        assert!(resp.error.as_deref().unwrap_or("").contains("permission denied"));
    }

    #[test]
    fn unknown_subtree_returns_error() {
        let snapshot = json!({});
        let req = make_req(Op::Get, "locales.en", snapshot);
        let resp = dispatch(req);
        assert!(!resp.ok);
    }

    #[test]
    fn set_config_key() {
        let snapshot = json!({ "poll_interval_ms": 500 });
        let mut req = make_req(Op::Set, "config.poll_interval_ms", snapshot);
        req.value = Some("1000".to_string());
        let resp = dispatch(req);
        assert!(resp.ok);
        match &resp.commit[0] {
            CommitIntent::SetConfig { key, value } => {
                assert_eq!(key, "poll_interval_ms");
                assert_eq!(*value, json!(1000));
            }
            other => panic!("unexpected intent: {other:?}"),
        }
    }

    #[test]
    fn delete_config_key() {
        let snapshot = json!({ "debug": true });
        let req = make_req(Op::Delete, "config.debug", snapshot);
        let resp = dispatch(req);
        assert!(resp.ok);
        assert!(matches!(&resp.commit[0], CommitIntent::DeleteConfig { key } if key == "debug"));
    }

    #[test]
    fn apply_cid_to_entity() {
        let snapshot = json!({});
        let mut req = make_req(Op::ApplyCid, "entities.fortune", snapshot);
        req.cid = Some("bafybeic...".to_string());
        let resp = dispatch(req);
        assert!(resp.ok);
        match &resp.commit[0] {
            CommitIntent::UpsertEntity { name, node } => {
                assert_eq!(name, "fortune");
                assert_eq!(node["/"], json!("bafybeic..."));
            }
            other => panic!("unexpected intent: {other:?}"),
        }
    }
}
