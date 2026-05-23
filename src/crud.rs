//! `/ma/crud/0.0.1` — structured data management service.
//!
//! Handles four content types (get/edit/set/delete) addressed to named paths
//! in the runtime manifest tree.  See `ma-spec/ma-crud-service-v1.md`.

use std::sync::Arc;

use anyhow::{anyhow, Context, Result};
use ciborium::Value as CborValue;
use ma_core::{
    Did, DidDocumentResolver, IpfsGatewayResolver, Ipld, SigningKey,
    MESSAGE_TYPE_CRUD_DELETE, MESSAGE_TYPE_CRUD_DELETE_REPLY,
    MESSAGE_TYPE_CRUD_EDIT, MESSAGE_TYPE_CRUD_EDIT_REPLY,
    MESSAGE_TYPE_CRUD_GET, MESSAGE_TYPE_CRUD_GET_REPLY,
    MESSAGE_TYPE_CRUD_SET, MESSAGE_TYPE_CRUD_SET_REPLY,
};
use tokio::sync::RwLock;
use tracing::{debug, info, warn};

use crate::acl::{check_full, AclCache, AclMap, SharedAcl, CAP_CRUD};
use crate::entity::{EntityNode, IpldLink, NamespaceNode, RuntimeManifest};
use crate::plugin::EntityRegistry;
use crate::status::SharedStats;

pub use ma_core::CRUD_PROTOCOL_ID;

// ── Config key tables ──────────────────────────────────────────────────────────

/// Daemon config fields that may be read/written via CRUD and are saved to
/// `config.yaml` on change.
pub const DAEMON_CONFIG_KEYS_PUB: &[&str] = &[
    "kubo_rpc_url",
    "kubo_key_alias",
    "log_level",
    "log_level_stdout",
    "did_resolver_positive_ttl_secs",
    "did_resolver_negative_ttl_secs",
    "log_file",
];

const DAEMON_CONFIG_KEYS: &[&str] = DAEMON_CONFIG_KEYS_PUB;

/// Manifest config keys that may be written via CRUD (stored in IPFS DAG).
const MANIFEST_CONFIG_KEYS: &[&str] = &[
    "i18n",
    "did_document_publishing_interval_secs",
    "did_document_publishing_timeout_secs",
    "did_document_publishing_lifetime_hours",
    "ipns_publish_lifetime_hours",
    "ipns_publish_resolve",
    "ipns_publish_allow_offline",
    "status_cors_allowed_origins",
];

/// Keys that are never exposed or writable via CRUD.
/// Any key beginning with `secret` is also blocked dynamically.
const PROTECTED_CONFIG_KEYS: &[&str] = &[
    "slug",
    "secret_bundle",
    "secret_bundle_passphrase",
    "config_path",
];

pub fn is_protected_config_key_pub(key: &str) -> bool {
    PROTECTED_CONFIG_KEYS.contains(&key) || key.starts_with("secret")
}

fn is_protected_config_key(key: &str) -> bool {
    is_protected_config_key_pub(key)
}

/// Read a daemon config field as a `serde_json::Value` for CRUD responses.
/// Returns `Value::Null` for unknown or platform-guarded keys.
pub fn daemon_config_key_value_pub(cfg: &ma_core::Config, key: &str) -> serde_json::Value {
    match key {
        "kubo_rpc_url" => serde_json::Value::String(cfg.kubo_rpc_url.clone()),
        "kubo_key_alias" => serde_json::Value::String(cfg.kubo_key_alias.clone()),
        "log_level" => serde_json::Value::String(cfg.log_level.clone()),
        "log_level_stdout" => serde_json::Value::String(cfg.log_level_stdout.clone()),
        "did_resolver_positive_ttl_secs" => {
            serde_json::Value::Number(cfg.did_resolver_positive_ttl_secs.into())
        }
        "did_resolver_negative_ttl_secs" => {
            serde_json::Value::Number(cfg.did_resolver_negative_ttl_secs.into())
        }
        "log_file" => cfg
            .log_file
            .as_ref()
            .map_or(serde_json::Value::Null, |p| serde_json::Value::String(p.to_string_lossy().into_owned())),
        _ => serde_json::Value::Null,
    }
}

/// Apply a JSON value from CRUD to the corresponding `Config` field in memory.
pub fn set_daemon_config_key_pub(
    cfg: &mut ma_core::Config,
    key: &str,
    val: &serde_json::Value,
) {
    match key {
        "kubo_rpc_url" => {
            if let Some(s) = val.as_str() {
                cfg.kubo_rpc_url = s.to_string();
            }
        }
        "kubo_key_alias" => {
            if let Some(s) = val.as_str() {
                cfg.kubo_key_alias = s.to_string();
            }
        }
        "log_level" => {
            if let Some(s) = val.as_str() {
                cfg.log_level = s.to_string();
            }
        }
        "log_level_stdout" => {
            if let Some(s) = val.as_str() {
                cfg.log_level_stdout = s.to_string();
            }
        }
        "did_resolver_positive_ttl_secs" => {
            if let Some(n) = val.as_u64() {
                cfg.did_resolver_positive_ttl_secs = n;
            }
        }
        "did_resolver_negative_ttl_secs" => {
            if let Some(n) = val.as_u64() {
                cfg.did_resolver_negative_ttl_secs = n;
            }
        }
        "log_file" => {
            cfg.log_file = val.as_str().map(std::path::PathBuf::from);
        }
        _ => {}
    }
}

fn set_daemon_config_key(cfg: &mut ma_core::Config, key: &str, val: &serde_json::Value) {
    set_daemon_config_key_pub(cfg, key, val);
}

// ── Handler context ────────────────────────────────────────────────────────────

pub struct CrudHandlerCtx<'a> {
    pub our_did: &'a str,
    pub signing_key: &'a SigningKey,
    pub endpoint: &'a dyn ma_core::MaEndpoint,
    pub kubo_rpc_url: &'a str,
    pub resolver: Arc<IpfsGatewayResolver>,
    pub stats: SharedStats,
    pub entity_registry: EntityRegistry,
    pub shared_config: Arc<RwLock<ma_core::Config>>,
    /// Namespace ACL cache — maps `"<ns>.acl"` and `"<ns>.acls.<name>"` to their
    /// `AclMap`s for zero-overhead lookup at call time.
    pub acl_cache: AclCache,
    /// Shared root transport ACL — owner may update at runtime via `:acl: <cid>`.
    pub root_acl: SharedAcl,
}

// ── Entry point ────────────────────────────────────────────────────────────────

pub async fn handle_crud_message(
    message: &ma_core::Message,
    acl: &AclMap,
    ctx: &CrudHandlerCtx<'_>,
) -> Result<()> {
    // ACL: require "crud" capability.
    check_full(acl, &message.from, &[CAP_CRUD], |_| async { Ok(vec![]) }).await?;
    dispatch_management(message, ctx).await
}

async fn dispatch_management(
    message: &ma_core::Message,
    ctx: &CrudHandlerCtx<'_>,
) -> Result<()> {
    let path_owned: String;
    let tail_owned: Option<String>;
    let args: Vec<CborValue>;
    let reply_type: &str;

    match message.message_type.as_str() {
        MESSAGE_TYPE_CRUD_GET => {
            path_owned = decode_path_atom(&message.content)?;
            tail_owned = None;
            args = vec![];
            reply_type = MESSAGE_TYPE_CRUD_GET_REPLY;
        }
        MESSAGE_TYPE_CRUD_EDIT => {
            path_owned = decode_path_atom(&message.content)?;
            tail_owned = Some("edit".to_string());
            args = vec![];
            reply_type = MESSAGE_TYPE_CRUD_EDIT_REPLY;
        }
        MESSAGE_TYPE_CRUD_SET => {
            let (p, v) = decode_set_payload(&message.content)?;
            path_owned = p;
            tail_owned = Some(String::new());
            args = vec![v];
            reply_type = MESSAGE_TYPE_CRUD_SET_REPLY;
        }
        MESSAGE_TYPE_CRUD_DELETE => {
            path_owned = decode_path_atom(&message.content)?;
            tail_owned = Some(String::new());
            args = vec![];
            reply_type = MESSAGE_TYPE_CRUD_DELETE_REPLY;
        }
        other => {
            return send_crud_error(
                message,
                MESSAGE_TYPE_CRUD_GET_REPLY,
                ctx,
                &format!("wrong-protocol: {other}"),
            )
            .await;
        }
    }

    let tail: Option<&str> = tail_owned.as_deref();
    let (ns, rest) = parse_path(&path_owned)?;

    match ns {
        "entities" => handle_entities_ns(message, &rest, tail, args, reply_type, ctx).await,
        "kinds" => handle_kinds_ns(message, &rest, tail, args, reply_type, ctx).await,
        "config" => handle_config_ns(message, &rest, tail, args, reply_type, ctx).await,
        "acl" => handle_root_acl(message, tail, args, reply_type, ctx).await,
        other => handle_namespace_op(message, other, &rest, tail, args, reply_type, ctx).await,
    }
}

// ── Management capability helpers ─────────────────────────────────────────────

async fn check_entity_management_cap(
    message: &ma_core::Message,
    ctx: &CrudHandlerCtx<'_>,
    caps: &[&str],
) -> Result<()> {
    let acl = ctx.root_acl.read().await;
    check_full(&acl, &message.from, caps, |_| async { Ok(vec![]) })
        .await
        .with_context(|| {
            format!(
                "entity management denied for {}: requires {:?}",
                message.from, caps
            )
        })
}

async fn update_stats_entities(ctx: &CrudHandlerCtx<'_>) {
    let names: Vec<String> = ctx.entity_registry.read().await.keys().cloned().collect();
    ctx.stats.write().await.entity_names = names;
}

// ── Entities namespace ─────────────────────────────────────────────────────────

async fn handle_entities_ns(
    message: &ma_core::Message,
    rest: &[String],
    tail: Option<&str>,
    args: Vec<CborValue>,
    reply_type: &str,
    ctx: &CrudHandlerCtx<'_>,
) -> Result<()> {
    match rest.len() {
        0 => match (tail, args.as_slice()) {
            (None, []) => {
                info!("{}", crate::i18n::t("root-list-entities"));
                let names: Vec<String> =
                    ctx.entity_registry.read().await.keys().cloned().collect();
                send_crud_reply_cbor(message, reply_type, ctx, &names).await
            }
            (Some(""), _) => {
                send_crud_i18n_error(message, reply_type, ctx, "refuse-delete-root").await
            }
            _ => Err(anyhow!("unknown entities operation")),
        },
        1 => handle_single_entity(message, &rest[0], tail, args, reply_type, ctx).await,
        2.. => {
            handle_entity_field(message, &rest[0], &rest[1..], tail, args, reply_type, ctx).await
        }
    }
}

#[allow(clippy::too_many_lines)]
async fn handle_single_entity(
    message: &ma_core::Message,
    name: &String,
    tail: Option<&str>,
    args: Vec<CborValue>,
    reply_type: &str,
    ctx: &CrudHandlerCtx<'_>,
) -> Result<()> {
    match (tail, args.as_slice()) {
        (None, []) => {
            let manifest = load_manifest(ctx).await?;
            let link = manifest
                .entities
                .get(name.as_str())
                .ok_or_else(|| anyhow!("entity not found: {name}"))?;
            let entity: EntityNode = crate::kubo::dag_get(ctx.kubo_rpc_url, &link.cid).await?;
            let mut out = Vec::new();
            ciborium::ser::into_writer(&entity, &mut out)
                .context("serialising entity node as CBOR")?;
            send_crud_reply(message, reply_type, ctx, &out).await
        }
        (Some(""), []) => {
            // Delete entity — requires `delete` + `entities` in root ACL.
            check_entity_management_cap(message, ctx, &["delete", "entities"]).await?;
            let name = name.as_str();
            ctx.entity_registry.write().await.remove(name);
            let new_root = with_manifest_crud(ctx, |m| {
                m.entities.remove(name);
                Ok(())
            })
            .await?;
            info!(name = %name, cid = %new_root, "{}", crate::i18n::t("entity-deleted"));
            send_crud_ok(message, reply_type, ctx).await
        }
        (Some(""), [CborValue::Text(path)]) => {
            // Upsert entity — requires `create` + `entities` in root ACL.
            check_entity_management_cap(message, ctx, &["create", "entities"]).await?;
            let name = name.as_str();
            let cid = crate::kubo::dag_resolve(ctx.kubo_rpc_url, path)
                .await
                .with_context(|| format!("resolving path {path}"))?;
            let cid = cid.as_str();
            let entity_node: EntityNode = crate::kubo::dag_get(ctx.kubo_rpc_url, cid)
                .await
                .with_context(|| format!("fetching entity node from {cid}"))?;
            let new_root = with_manifest_crud(ctx, |m| {
                m.entities.insert(name.to_string(), IpldLink::new(cid));
                Ok(())
            })
            .await?;
            register_entity_plugin(ctx, name, &entity_node).await;
            info!(name = %name, cid = %cid, "{}", crate::i18n::t("entity-created"));
            send_crud_ok_cid(message, reply_type, ctx, &new_root).await
        }
        (Some("edit"), []) => {
            let manifest = load_manifest(ctx).await?;
            let entity: EntityNode = match manifest.entities.get(name.as_str()) {
                Some(link) => crate::kubo::dag_get(ctx.kubo_rpc_url, &link.cid).await?,
                None => EntityNode {
                    kind: String::new(),
                    behavior: String::new(),
                    acl: String::new(),
                    state: None,
                },
            };
            let yaml =
                serde_yaml::to_string(&entity).context("serialising entity node as YAML")?;
            send_crud_reply_yaml(message, reply_type, ctx, &yaml).await
        }
        (Some("edit"), [CborValue::Bytes(dag_cbor)]) => {
            let name = name.as_str();
            let cid = crate::kubo::dag_put_raw(ctx.kubo_rpc_url, dag_cbor)
                .await
                .with_context(|| format!("dag_put_raw for entity {name}"))?;
            let entity_node: EntityNode = crate::kubo::dag_get(ctx.kubo_rpc_url, &cid)
                .await
                .with_context(|| format!("validating entity node at {cid}"))?;
            with_manifest_crud(ctx, |m| {
                m.entities.insert(name.to_string(), IpldLink::new(&cid));
                Ok(())
            })
            .await?;
            register_entity_plugin(ctx, name, &entity_node).await;
            info!(name = %name, cid = %cid, "{}", crate::i18n::t("entity-created"));
            send_crud_ok_cid(message, reply_type, ctx, &cid).await
        }
        _ => Err(anyhow!("unknown entities.{name} operation")),
    }
}

// ── Entity field helpers ───────────────────────────────────────────────────────

async fn fetch_entity_node(ctx: &CrudHandlerCtx<'_>, name: &str) -> Result<EntityNode> {
    let manifest = load_manifest(ctx).await?;
    let link = manifest
        .entities
        .get(name)
        .ok_or_else(|| anyhow!("entity not found: {name}"))?;
    crate::kubo::dag_get(ctx.kubo_rpc_url, &link.cid)
        .await
        .with_context(|| format!("fetching entity {name} from {}", link.cid))
}

async fn update_entity_node(
    ctx: &CrudHandlerCtx<'_>,
    name: &str,
    entity: &EntityNode,
) -> Result<String> {
    let entity_cid = crate::kubo::dag_put(ctx.kubo_rpc_url, entity)
        .await
        .with_context(|| format!("publishing updated entity {name}"))?;
    with_manifest_crud(ctx, |m| {
        m.entities
            .insert(name.to_string(), IpldLink::new(&entity_cid));
        Ok(())
    })
    .await?;
    Ok(entity_cid)
}

async fn handle_entity_field(
    message: &ma_core::Message,
    name: &String,
    field_path: &[String],
    tail: Option<&str>,
    args: Vec<CborValue>,
    reply_type: &str,
    ctx: &CrudHandlerCtx<'_>,
) -> Result<()> {
    let Some((field, sub_path)) = field_path.split_first() else {
        return Err(anyhow!("empty field path in entity.{name}"));
    };

    // Generic GET / :edit — works for any leaf field without field-specific code.
    if matches!(tail, None | Some("edit")) && args.is_empty() && sub_path.is_empty() {
        let entity = fetch_entity_node(ctx, name).await?;
        let mut entity_cbor = Vec::new();
        ciborium::ser::into_writer(&entity, &mut entity_cbor)
            .context("serializing entity for field GET")?;
        let cbor_map: CborValue = ciborium::de::from_reader(entity_cbor.as_slice())
            .context("re-parsing entity CBOR map")?;
        if let CborValue::Map(entries) = cbor_map {
            if let Some((_, value)) = entries
                .into_iter()
                .find(|(k, _)| matches!(k, CborValue::Text(s) if s == field))
            {
                let mut out = Vec::new();
                ciborium::ser::into_writer(&value, &mut out)
                    .context("encoding field value as CBOR")?;
                return send_crud_reply(message, reply_type, ctx, &out).await;
            }
        }
        return Err(anyhow!("field '{field}' not found in entity '{name}'"));
    }

    match field.as_str() {
        "acl" => {
            handle_entity_acl_field(message, name, sub_path, tail, args, reply_type, ctx).await
        }
        _ => Err(anyhow!("unknown entity field '{field}' in entity.{name}")),
    }
}

async fn handle_entity_acl_field(
    message: &ma_core::Message,
    name: &String,
    sub_path: &[String],
    tail: Option<&str>,
    args: Vec<CborValue>,
    reply_type: &str,
    ctx: &CrudHandlerCtx<'_>,
) -> Result<()> {
    if !sub_path.is_empty() {
        return Err(anyhow!(
            "entity field 'acl' sub-path '{}' not yet implemented",
            sub_path.join(".")
        ));
    }
    match (tail, args.as_slice()) {
        (None, []) => {
            let entity = fetch_entity_node(ctx, name).await?;
            send_crud_reply_cbor(
                message,
                reply_type,
                ctx,
                &CborValue::Text(entity.acl.clone()),
            )
            .await
        }
        (Some(""), [CborValue::Text(acl_name)]) => {
            let mut entity = fetch_entity_node(ctx, name).await?;
            entity.acl = acl_name.clone();
            let entity_cid = update_entity_node(ctx, name, &entity).await?;
            info!(name = %name, acl_name = %acl_name, entity_cid = %entity_cid, "entity ACL name set");
            send_crud_ok_cid(message, reply_type, ctx, &entity_cid).await
        }
        (Some(""), []) => {
            let mut entity = fetch_entity_node(ctx, name).await?;
            entity.acl = String::new();
            let entity_cid = update_entity_node(ctx, name, &entity).await?;
            info!(name = %name, entity_cid = %entity_cid, "entity ACL cleared");
            send_crud_ok_cid(message, reply_type, ctx, &entity_cid).await
        }
        _ => Err(anyhow!("unknown entities.{name}.acl operation")),
    }
}

// ── Kinds namespace ────────────────────────────────────────────────────────────

/// Handle `:kinds` CRUD operations.
///
/// Protocol IDs contain slashes and cannot appear as dot-path segments, so
/// individual kind mutations pass the protocol ID as part of the CBOR value:
///
/// | Operation | Message type | Value                           |
/// |-----------|-------------|--------------------------------|
/// | List all  | GET         | —                              |
/// | Edit list | EDIT        | —                              |
/// | Upsert    | SET         | `[protocol_text, cbor_bytes]`  |
/// | Delete    | SET         | `protocol_text`                |
/// | Refuse    | DELETE      | —                              |
async fn handle_kinds_ns(
    message: &ma_core::Message,
    rest: &[String],
    tail: Option<&str>,
    args: Vec<CborValue>,
    reply_type: &str,
    ctx: &CrudHandlerCtx<'_>,
) -> Result<()> {
    if !rest.is_empty() {
        return Err(anyhow!(
            "kinds path segments not supported (protocol IDs use CBOR value args)"
        ));
    }
    match (tail, args.as_slice()) {
        // GET :kinds → list all protocol IDs
        (None, []) => {
            let manifest = load_manifest(ctx).await?;
            let ids: Vec<String> = manifest.kinds.protocol_ids();
            send_crud_reply_cbor(message, reply_type, ctx, &ids).await
        }
        // EDIT :kinds → list all protocol IDs as YAML
        (Some("edit"), []) => {
            let manifest = load_manifest(ctx).await?;
            let ids: Vec<String> = manifest.kinds.protocol_ids();
            let yaml = serde_yaml::to_string(&ids).context("serialising kinds list as YAML")?;
            send_crud_reply_yaml(message, reply_type, ctx, &yaml).await
        }
        // SET :kinds with [protocol_text, cbor_bytes] → upsert kind
        (Some(""), [CborValue::Array(items)]) => {
            let items = items.clone();
            match items.as_slice() {
                [CborValue::Text(protocol), CborValue::Bytes(cbor_bytes)] => {
                    let protocol = protocol.clone();
                    let cbor_bytes = cbor_bytes.clone();
                    let cid = crate::kubo::dag_put_raw(ctx.kubo_rpc_url, &cbor_bytes)
                        .await
                        .context("dag_put kind")?;
                    let new_root = with_manifest_crud(ctx, |m| {
                        m.kinds.insert_protocol(&protocol, IpldLink::new(&cid));
                        Ok(())
                    })
                    .await?;
                    send_crud_ok_cid(message, reply_type, ctx, &new_root).await
                }
                _ => Err(anyhow!("kinds SET value must be [protocol_text, cbor_bytes]")),
            }
        }
        // SET :kinds with protocol_text → delete that kind
        (Some(""), [CborValue::Text(protocol)]) => {
            let protocol = protocol.clone();
            with_manifest_crud(ctx, |m| {
                m.kinds.remove_protocol(&protocol);
                Ok(())
            })
            .await?;
            send_crud_ok(message, reply_type, ctx).await
        }
        // DELETE :kinds (root) → refuse
        (Some(""), []) => {
            send_crud_i18n_error(message, reply_type, ctx, "refuse-delete-root").await
        }
        _ => Err(anyhow!("unknown kinds operation")),
    }
}

// ── Config namespace ───────────────────────────────────────────────────────────

#[allow(clippy::too_many_lines)]
async fn handle_config_ns(
    message: &ma_core::Message,
    rest: &[String],
    tail: Option<&str>,
    args: Vec<CborValue>,
    reply_type: &str,
    ctx: &CrudHandlerCtx<'_>,
) -> Result<()> {
    match rest {
        [] => match (tail, args.as_slice()) {
            (None | Some("edit"), []) => {
                let manifest = load_manifest(ctx).await?;
                let mut combined = manifest.config.clone();
                {
                    let cfg = ctx.shared_config.read().await;
                    for key in DAEMON_CONFIG_KEYS {
                        let val = daemon_config_key_value_pub(&cfg, key);
                        if !val.is_null() {
                            combined.insert(key.to_string(), val);
                        }
                    }
                    drop(cfg);
                }
                if matches!(tail, Some("edit")) {
                    let yaml = serde_yaml::to_string(&combined)
                        .context("serialising config as YAML")?;
                    send_crud_reply_yaml(message, reply_type, ctx, &yaml).await
                } else {
                    send_crud_reply_cbor(message, reply_type, ctx, &combined).await
                }
            }
            (Some(""), _) => {
                send_crud_i18n_error(message, reply_type, ctx, "refuse-delete-root").await
            }
            _ => Err(anyhow!("unknown config operation")),
        },
        [key] => {
            if is_protected_config_key(key.as_str()) {
                return send_crud_error(
                    message,
                    reply_type,
                    ctx,
                    &format!("config key '{key}' is protected"),
                )
                .await;
            }
            let is_daemon_key = DAEMON_CONFIG_KEYS.contains(&key.as_str());
            match (tail, args.as_slice()) {
                (None, []) => {
                    let val = if is_daemon_key {
                        let cfg = ctx.shared_config.read().await;
                        daemon_config_key_value_pub(&cfg, key.as_str())
                    } else {
                        let manifest = load_manifest(ctx).await?;
                        manifest
                            .config
                            .get(key.as_str())
                            .ok_or_else(|| anyhow!("config key not found: {key}"))?
                            .clone()
                    };
                    send_crud_reply_cbor(message, reply_type, ctx, &val).await
                }
                (Some("edit"), []) => {
                    let val = if is_daemon_key {
                        let cfg = ctx.shared_config.read().await;
                        daemon_config_key_value_pub(&cfg, key.as_str())
                    } else {
                        let manifest = load_manifest(ctx).await?;
                        manifest.config.get(key.as_str()).cloned().unwrap_or_else(|| {
                            if key == "i18n" {
                                serde_json::Value::String(crate::i18n::runtime_lang())
                            } else {
                                serde_json::Value::Null
                            }
                        })
                    };
                    let yaml =
                        serde_yaml::to_string(&val).context("serialising config value as YAML")?;
                    send_crud_reply_yaml(message, reply_type, ctx, &yaml).await
                }
                (Some(""), []) => {
                    if is_daemon_key {
                        return send_crud_error(
                            message,
                            reply_type,
                            ctx,
                            &format!("daemon config key '{key}' cannot be deleted"),
                        )
                        .await;
                    }
                    let key = key.as_str().to_string();
                    with_manifest_crud(ctx, |m| {
                        m.config.remove(&key);
                        Ok(())
                    })
                    .await?;
                    send_crud_ok(message, reply_type, ctx).await
                }
                (Some(""), [CborValue::Text(value)]) => {
                    let key = key.as_str().to_string();
                    let json_val: serde_json::Value = serde_json::from_str(value.as_str())
                        .unwrap_or_else(|_| serde_json::Value::String(value.clone()));
                    if is_daemon_key {
                        set_daemon_config_key(
                            &mut *ctx.shared_config.write().await,
                            &key,
                            &json_val,
                        );
                        let save_result = ctx.shared_config.read().await.save();
                        if let Err(e) = save_result {
                            warn!(key = %key, error = %e, "failed to save config.yaml after CRUD update");
                        }
                        return send_crud_ok(message, reply_type, ctx).await;
                    }
                    // Manifest config key — only known keys may be written.
                    if !MANIFEST_CONFIG_KEYS.contains(&key.as_str()) {
                        return send_crud_error(
                            message,
                            reply_type,
                            ctx,
                            &format!("config key '{key}' is not a known manifest config key"),
                        )
                        .await;
                    }
                    let new_root = with_manifest_crud(ctx, |m| {
                        m.config.insert(key.clone(), json_val.clone());
                        Ok(())
                    })
                    .await?;
                    // Language hot-swap: reload FTL messages immediately.
                    if key == "i18n" {
                        if let serde_json::Value::String(ref lang) = json_val {
                            crate::i18n::switch_lang(lang, ctx.kubo_rpc_url).await;
                        }
                    }
                    send_crud_ok_cid(message, reply_type, ctx, &new_root).await
                }
                _ => Err(anyhow!("unknown config.{key} operation")),
            }
        }
        _ => Err(anyhow!("unknown config operation")),
    }
}

// ── Root transport-gate ACL ────────────────────────────────────────────────────

async fn handle_root_acl(
    message: &ma_core::Message,
    tail: Option<&str>,
    args: Vec<CborValue>,
    reply_type: &str,
    ctx: &CrudHandlerCtx<'_>,
) -> Result<()> {
    match (tail, args.as_slice()) {
        (None | Some("edit"), []) => {
            let manifest = load_manifest(ctx).await?;
            match &manifest.acl {
                Some(link) => {
                    let acl_map: AclMap =
                        crate::acl::load_acl_from_cid(ctx.kubo_rpc_url, &link.cid).await?;
                    let yaml = serde_yaml::to_string(&acl_map)
                        .context("serialising root ACL as YAML")?;
                    let cbor = CborValue::Array(vec![
                        CborValue::Text(":ok".to_string()),
                        CborValue::Text(yaml),
                    ]);
                    send_crud_reply_cbor(message, reply_type, ctx, &cbor).await
                }
                None => send_crud_i18n_error(message, reply_type, ctx, "no-root-acl").await,
            }
        }
        (Some(""), [CborValue::Text(cid)]) => {
            let cid = cid.clone();
            let acl_map = crate::acl::load_acl_from_cid(ctx.kubo_rpc_url, &cid)
                .await
                .with_context(|| format!("loading root ACL from {cid}"))?;
            let new_root = with_manifest_crud(ctx, |m| {
                m.acl = Some(IpldLink::new(&cid));
                Ok(())
            })
            .await?;
            *ctx.root_acl.write().await = acl_map;
            info!(cid = %cid, "Root transport ACL updated");
            send_crud_ok_cid(message, reply_type, ctx, &new_root).await
        }
        (Some("edit"), [CborValue::Bytes(dag_cbor)]) => {
            let dag_cbor = dag_cbor.clone();
            let cid = crate::kubo::dag_put_raw(ctx.kubo_rpc_url, &dag_cbor)
                .await
                .context("dag_put_raw for root ACL")?;
            let acl_map: AclMap = crate::kubo::dag_get(ctx.kubo_rpc_url, &cid)
                .await
                .with_context(|| format!("loading updated root ACL from {cid}"))?;
            let new_root = with_manifest_crud(ctx, |m| {
                m.acl = Some(IpldLink::new(&cid));
                Ok(())
            })
            .await?;
            *ctx.root_acl.write().await = acl_map;
            info!(cid = %cid, "Root transport ACL updated via edit-save");
            send_crud_ok_cid(message, reply_type, ctx, &new_root).await
        }
        // DELETE is refused — the root transport ACL must never be cleared.
        (Some(""), []) => {
            send_crud_i18n_error(message, reply_type, ctx, "refuse-delete-root").await
        }
        _ => Err(anyhow!("unknown :acl operation")),
    }
}

// ── Namespace dispatching ─────────────────────────────────────────────────────

/// Namespace names that are reserved and may not be used as user namespaces.
const RESERVED_NS: &[&str] = &[
    "acl", "acls", "protocol", "kinds", "entities", "i18n", "config",
];

/// Check the namespace gate ACL for `caller` against `caps`.
async fn ns_acl_check(
    ns: &str,
    caller: &str,
    caps: &[&str],
    acl_cache: &AclCache,
    kubo_rpc_url: &str,
    root_cid: &str,
) -> Result<()> {
    let gate_key = format!("{ns}.acl");
    let maybe_acl = acl_cache.read().await.get(&gate_key).cloned();
    let acl = maybe_acl.ok_or_else(|| anyhow!("no gate ACL for namespace {ns}: access denied"))?;
    let url = kubo_rpc_url.to_string();
    let rc = root_cid.to_string();
    crate::acl::check_full(&acl, caller, caps, |g| {
        let url = url.clone();
        let rc = rc.clone();
        let g = g.to_string();
        async move { crate::acl::fetch_group_members(&url, &g, &rc).await }
    })
    .await
    .with_context(|| format!("namespace gate check failed for {caller} in {ns}"))
}

#[allow(clippy::too_many_arguments)]
async fn handle_namespace_op(
    message: &ma_core::Message,
    ns: &str,
    rest: &[String],
    tail: Option<&str>,
    args: Vec<CborValue>,
    reply_type: &str,
    ctx: &CrudHandlerCtx<'_>,
) -> Result<()> {
    if ns.is_empty() {
        debug!("{}", crate::i18n::t("unknown-rpc-atom"));
        return Ok(());
    }
    let category = rest.first().map_or("", String::as_str);
    let sub_rest: &[String] = if rest.len() > 1 { &rest[1..] } else { &[] };

    // Gate check applies to blob operations only (not acl/acls management or ns root).
    if !matches!(category, "" | "acl" | "acls") {
        if let Ok(root_cid) = current_root_cid(ctx).await {
            let is_read = tail.is_none();
            let caps: &[&str] = if is_read {
                &[category, "read", "*"]
            } else {
                &[category, "update", "*"]
            };
            if let Err(e) = ns_acl_check(
                ns,
                &message.from,
                caps,
                &ctx.acl_cache,
                ctx.kubo_rpc_url,
                &root_cid,
            )
            .await
            {
                return send_crud_error(message, reply_type, ctx, &e.to_string()).await;
            }
        }
    }

    match category {
        "acl" => handle_ns_acl_gate(message, ns, tail, args, reply_type, ctx).await,
        "acls" => handle_ns_acls(message, ns, sub_rest, tail, args, reply_type, ctx).await,
        "" => handle_ns_root(message, ns, tail, args, reply_type, ctx).await,
        key => handle_ns_blob(message, ns, key, sub_rest, tail, args, reply_type, ctx).await,
    }
}

async fn handle_ns_root(
    message: &ma_core::Message,
    ns: &str,
    tail: Option<&str>,
    args: Vec<CborValue>,
    reply_type: &str,
    ctx: &CrudHandlerCtx<'_>,
) -> Result<()> {
    match (tail, args.as_slice()) {
        (None, []) => {
            let manifest = load_manifest(ctx).await?;
            match manifest.namespaces.get(ns) {
                Some(ns_node) => send_crud_reply_cbor(message, reply_type, ctx, ns_node).await,
                None => {
                    send_crud_i18n_error(message, reply_type, ctx, "namespace-not-found").await
                }
            }
        }
        // Create / upsert namespace
        (Some(""), _) => {
            if RESERVED_NS.contains(&ns) {
                return send_crud_i18n_error(
                    message,
                    reply_type,
                    ctx,
                    "refuse-delete-root",
                )
                .await;
            }
            let new_root = with_manifest_crud(ctx, |m| {
                m.namespaces
                    .entry(ns.to_string())
                    .or_insert_with(NamespaceNode::default);
                Ok(())
            })
            .await?;
            send_crud_ok_cid(message, reply_type, ctx, &new_root).await
        }
        _ => Err(anyhow!("unknown namespace '{ns}' operation")),
    }
}

async fn handle_ns_acl_gate(
    message: &ma_core::Message,
    ns: &str,
    tail: Option<&str>,
    args: Vec<CborValue>,
    reply_type: &str,
    ctx: &CrudHandlerCtx<'_>,
) -> Result<()> {
    match (tail, args.as_slice()) {
        (None, []) => {
            let manifest = load_manifest(ctx).await?;
            let ns_node = manifest
                .namespaces
                .get(ns)
                .ok_or_else(|| anyhow!("namespace not found: {ns}"))?;
            match &ns_node.acl {
                Some(link) => {
                    send_crud_reply_cbor(
                        message,
                        reply_type,
                        ctx,
                        &CborValue::Text(link.cid.clone()),
                    )
                    .await
                }
                None => send_crud_i18n_error(message, reply_type, ctx, "no-ns-gate-acl").await,
            }
        }
        (Some(""), [CborValue::Text(cid)]) => {
            let cid = cid.clone();
            let new_root = with_manifest_crud(ctx, |m| {
                let ns_node = m
                    .namespaces
                    .get_mut(ns)
                    .ok_or_else(|| anyhow!("namespace not found: {ns}"))?;
                ns_node.acl = Some(IpldLink::new(&cid));
                Ok(())
            })
            .await?;
            let cache_key = format!("{ns}.acl");
            acl_cache_update(ctx, &cache_key, &cid).await;
            send_crud_ok_cid(message, reply_type, ctx, &new_root).await
        }
        (Some(""), []) => {
            let new_root = with_manifest_crud(ctx, |m| {
                let ns_node = m
                    .namespaces
                    .get_mut(ns)
                    .ok_or_else(|| anyhow!("namespace not found: {ns}"))?;
                ns_node.acl = None;
                Ok(())
            })
            .await?;
            let cache_key = format!("{ns}.acl");
            ctx.acl_cache.write().await.remove(&cache_key);
            send_crud_ok_cid(message, reply_type, ctx, &new_root).await
        }
        _ => Err(anyhow!("unknown {ns}.acl operation")),
    }
}

#[allow(clippy::too_many_lines)]
async fn handle_ns_acls(
    message: &ma_core::Message,
    ns: &str,
    rest: &[String],
    tail: Option<&str>,
    args: Vec<CborValue>,
    reply_type: &str,
    ctx: &CrudHandlerCtx<'_>,
) -> Result<()> {
    match rest {
        [] => match (tail, args.as_slice()) {
            (None, []) => {
                let manifest = load_manifest(ctx).await?;
                let ns_node = manifest
                    .namespaces
                    .get(ns)
                    .ok_or_else(|| anyhow!("namespace not found: {ns}"))?;
                let names: Vec<CborValue> = ns_node
                    .acls
                    .keys()
                    .map(|k| CborValue::Text(k.clone()))
                    .collect();
                send_crud_reply_cbor(
                    message,
                    reply_type,
                    ctx,
                    &CborValue::Array(vec![
                        CborValue::Text(":ok".to_string()),
                        CborValue::Array(names),
                    ]),
                )
                .await
            }
            _ => Err(anyhow!("unknown {ns}.acls operation")),
        },
        [acl_name] => {
            let acl_name = acl_name.clone();
            match (tail, args.as_slice()) {
                (None, []) => {
                    let manifest = load_manifest(ctx).await?;
                    let ns_node = manifest
                        .namespaces
                        .get(ns)
                        .ok_or_else(|| anyhow!("namespace not found: {ns}"))?;
                    let link = ns_node
                        .acls
                        .get(&acl_name)
                        .ok_or_else(|| anyhow!("ACL not found: {ns}.acls.{acl_name}"))?;
                    send_crud_reply_cbor(
                        message,
                        reply_type,
                        ctx,
                        &CborValue::Text(link.cid.clone()),
                    )
                    .await
                }
                (Some(""), [CborValue::Text(cid)]) => {
                    let cid = cid.clone();
                    let new_root = with_manifest_crud(ctx, |m| {
                        let ns_node = m
                            .namespaces
                            .get_mut(ns)
                            .ok_or_else(|| anyhow!("namespace not found: {ns}"))?;
                        ns_node.acls.insert(acl_name.clone(), IpldLink::new(&cid));
                        Ok(())
                    })
                    .await?;
                    let cache_key = format!("{ns}.acls.{acl_name}");
                    acl_cache_update(ctx, &cache_key, &cid).await;
                    send_crud_ok_cid(message, reply_type, ctx, &new_root).await
                }
                (Some(""), []) => {
                    let new_root = with_manifest_crud(ctx, |m| {
                        let ns_node = m
                            .namespaces
                            .get_mut(ns)
                            .ok_or_else(|| anyhow!("namespace not found: {ns}"))?;
                        ns_node.acls.remove(&acl_name);
                        Ok(())
                    })
                    .await?;
                    let cache_key = format!("{ns}.acls.{acl_name}");
                    ctx.acl_cache.write().await.remove(&cache_key);
                    send_crud_ok_cid(message, reply_type, ctx, &new_root).await
                }
                _ => Err(anyhow!("unknown {ns}.acls.{acl_name} operation")),
            }
        }
        _ => Err(anyhow!("unknown {ns}.acls operation")),
    }
}

#[allow(clippy::too_many_arguments)]
async fn handle_ns_blob(
    message: &ma_core::Message,
    ns: &str,
    key: &str,
    sub_path: &[String],
    tail: Option<&str>,
    args: Vec<CborValue>,
    reply_type: &str,
    ctx: &CrudHandlerCtx<'_>,
) -> Result<()> {
    match (tail, args.as_slice()) {
        (None, []) => {
            let manifest = load_manifest(ctx).await?;
            let ns_node = manifest
                .namespaces
                .get(ns)
                .ok_or_else(|| anyhow!("namespace not found: {ns}"))?;
            let link_cid = ns_node
                .extra
                .get(key)
                .and_then(|v| v.get("/"))
                .and_then(|v| v.as_str())
                .ok_or_else(|| anyhow!("key not found: {ns}.{key}"))?;
            let resolved_cid = if sub_path.is_empty() {
                link_cid.to_string()
            } else {
                let ipfs_path = format!("/ipfs/{}/{}", link_cid, sub_path.join("/"));
                crate::kubo::dag_resolve(ctx.kubo_rpc_url, &ipfs_path)
                    .await
                    .with_context(|| format!("traversing {ns}.{key}.{}", sub_path.join(".")))?
            };
            send_crud_reply_cbor(message, reply_type, ctx, &CborValue::Text(resolved_cid)).await
        }
        (Some(""), [CborValue::Text(cid)]) if sub_path.is_empty() => {
            let cid = cid.clone();
            let new_root = with_manifest_crud(ctx, |m| {
                let ns_node = m
                    .namespaces
                    .get_mut(ns)
                    .ok_or_else(|| anyhow!("namespace not found: {ns}"))?;
                ns_node
                    .extra
                    .insert(key.to_string(), serde_json::json!({ "/": cid }));
                Ok(())
            })
            .await?;
            info!(ns = %ns, key = %key, cid = %cid, "blob registered");
            send_crud_ok_cid(message, reply_type, ctx, &new_root).await
        }
        (Some(""), []) if sub_path.is_empty() => {
            let new_root = with_manifest_crud(ctx, |m| {
                let ns_node = m
                    .namespaces
                    .get_mut(ns)
                    .ok_or_else(|| anyhow!("namespace not found: {ns}"))?;
                ns_node.extra.remove(key);
                Ok(())
            })
            .await?;
            info!(ns = %ns, key = %key, "blob deleted");
            send_crud_ok_cid(message, reply_type, ctx, &new_root).await
        }
        _ => Err(anyhow!("unknown {ns}.{key} operation")),
    }
}

// ── Path helpers ───────────────────────────────────────────────────────────────

/// Parse `:ns.seg1.seg2` → `("ns", ["seg1", "seg2"])`.
fn parse_path(path: &str) -> Result<(&str, Vec<String>)> {
    let body = path
        .strip_prefix(':')
        .ok_or_else(|| anyhow!("CRUD path must start with ':' — got: {path}"))?;
    let (ns, rest_str) = body.split_once('.').unwrap_or((body, ""));
    if ns.is_empty() {
        return Err(anyhow!("CRUD path has no namespace: {path}"));
    }
    let segs: Vec<String> = rest_str
        .split('.')
        .filter(|s| !s.is_empty())
        .map(String::from)
        .collect();
    Ok((ns, segs))
}

/// Decode the CBOR path atom (`:ns.key`) from raw message content.
fn decode_path_atom(content: &[u8]) -> Result<String> {
    let val: CborValue =
        ciborium::de::from_reader(content).context("invalid CBOR in CRUD path atom")?;
    match val {
        CborValue::Text(s) => Ok(s),
        _ => Err(anyhow!("CRUD path atom must be a CBOR text string")),
    }
}

/// Decode `[path_atom, value]` from a crud-set payload.
fn decode_set_payload(content: &[u8]) -> Result<(String, CborValue)> {
    let val: CborValue =
        ciborium::de::from_reader(content).context("invalid CBOR in CRUD set payload")?;
    match val {
        CborValue::Array(mut items) if items.len() == 2 => {
            let value = items.pop().expect("len==2");
            let path_cbor = items.pop().expect("len==2");
            let CborValue::Text(path) = path_cbor else {
                return Err(anyhow!("CRUD set path must be a CBOR text string"));
            };
            Ok((path, value))
        }
        _ => Err(anyhow!("CRUD set payload must be a two-element CBOR array")),
    }
}

// ── Manifest mutation helper ───────────────────────────────────────────────────

async fn with_manifest_crud<F>(ctx: &CrudHandlerCtx<'_>, f: F) -> Result<String>
where
    F: FnOnce(&mut RuntimeManifest) -> Result<()>,
{
    let old_cid = current_root_cid(ctx).await?;
    let mut manifest: RuntimeManifest =
        crate::kubo::dag_get(ctx.kubo_rpc_url, &old_cid).await?;
    f(&mut manifest)?;
    let new_cid = crate::kubo::dag_put(ctx.kubo_rpc_url, &manifest).await?;
    if let Err(e) = crate::kubo::pin_update(ctx.kubo_rpc_url, &old_cid, &new_cid).await {
        warn!(old = %old_cid, new = %new_cid, error = %e, "CRUD pin_update failed");
    }
    update_stats_entities(ctx).await;
    ctx.stats.write().await.root_cid = Some(new_cid.clone());
    Ok(new_cid)
}

async fn current_root_cid(ctx: &CrudHandlerCtx<'_>) -> Result<String> {
    ctx.stats
        .read()
        .await
        .root_cid
        .clone()
        .ok_or_else(|| anyhow!("no root_cid; run --gen-root-cid first"))
}

/// Fetch and deserialise the current `RuntimeManifest` from IPFS.
async fn load_manifest(ctx: &CrudHandlerCtx<'_>) -> Result<RuntimeManifest> {
    let root_cid = current_root_cid(ctx).await?;
    crate::kubo::dag_get(ctx.kubo_rpc_url, &root_cid).await
}

/// Load an ACL from `cid`, insert it into `acl_cache` under `cache_key`.
/// Logs success or failure; non-fatal either way.
async fn acl_cache_update(ctx: &CrudHandlerCtx<'_>, cache_key: &str, cid: &str) {
    match crate::acl::load_acl_from_cid(ctx.kubo_rpc_url, cid).await {
        Ok(acl_map) => {
            ctx.acl_cache
                .write()
                .await
                .insert(cache_key.to_string(), acl_map);
            info!(key = %cache_key, %cid, "ACL loaded into cache");
        }
        Err(e) => {
            warn!(key = %cache_key, %cid, error = %e, "failed to load ACL into cache");
        }
    }
}

/// Load a plugin from `entity_node` and register it in the entity registry.
/// Logs a warning if loading fails; non-fatal.
async fn register_entity_plugin(
    ctx: &CrudHandlerCtx<'_>,
    name: &str,
    entity_node: &EntityNode,
) {
    match crate::plugin::EntityPlugin::load(name.to_string(), entity_node, ctx.kubo_rpc_url)
        .await
    {
        Ok(ep) => {
            ctx.entity_registry
                .write()
                .await
                .insert(name.to_string(), Arc::new(ep));
        }
        Err(e) => warn!(
            name = %name,
            error = %e,
            "{}",
            crate::i18n::t("entity-load-failed")
        ),
    }
}

// ── i18n helpers ───────────────────────────────────────────────────────────────

/// Resolve caller's DID document and extract their preferred language.
/// Falls back to the runtime's own language on any error.
async fn caller_lang(from: &str, resolver: &IpfsGatewayResolver) -> String {
    if let Ok(doc) = resolver.resolve(from).await {
        if let Some(Ipld::Map(ma)) = &doc.ma {
            if let Some(Ipld::String(lang)) = ma.get("lang") {
                if crate::i18n::has_lang(lang) {
                    return lang.clone();
                }
            }
        }
    }
    crate::i18n::runtime_lang()
}

/// Send a CRUD error reply with a message localised to the caller's language.
async fn send_crud_i18n_error(
    incoming: &ma_core::Message,
    reply_type: &str,
    ctx: &CrudHandlerCtx<'_>,
    key: &str,
) -> Result<()> {
    let lang = caller_lang(&incoming.from, ctx.resolver.as_ref()).await;
    send_crud_error(incoming, reply_type, ctx, &crate::i18n::t_lang(&lang, key)).await
}

// ── Reply helpers ──────────────────────────────────────────────────────────────

async fn send_crud_ok(
    incoming: &ma_core::Message,
    reply_type: &str,
    ctx: &CrudHandlerCtx<'_>,
) -> Result<()> {
    let mut out = Vec::new();
    ciborium::ser::into_writer(&CborValue::Text(":ok".to_string()), &mut out)
        .context("encoding :ok")?;
    send_crud_reply(incoming, reply_type, ctx, &out).await
}

async fn send_crud_ok_cid(
    incoming: &ma_core::Message,
    reply_type: &str,
    ctx: &CrudHandlerCtx<'_>,
    cid: &str,
) -> Result<()> {
    let mut out = Vec::new();
    ciborium::ser::into_writer(
        &CborValue::Array(vec![
            CborValue::Text(":ok".to_string()),
            CborValue::Text(cid.to_string()),
        ]),
        &mut out,
    )
    .context("encoding [:ok, cid]")?;
    send_crud_reply(incoming, reply_type, ctx, &out).await
}

async fn send_crud_error(
    incoming: &ma_core::Message,
    reply_type: &str,
    ctx: &CrudHandlerCtx<'_>,
    reason: &str,
) -> Result<()> {
    let mut out = Vec::new();
    ciborium::ser::into_writer(
        &CborValue::Array(vec![
            CborValue::Text(":error".to_string()),
            CborValue::Text(reason.to_string()),
        ]),
        &mut out,
    )
    .context("encoding error reply")?;
    send_crud_reply(incoming, reply_type, ctx, &out).await
}

async fn send_crud_reply_cbor<T: serde::Serialize + Sync>(
    incoming: &ma_core::Message,
    reply_type: &str,
    ctx: &CrudHandlerCtx<'_>,
    value: &T,
) -> Result<()> {
    let mut out = Vec::new();
    ciborium::ser::into_writer(value, &mut out).context("encoding CBOR reply")?;
    send_crud_reply(incoming, reply_type, ctx, &out).await
}

async fn send_crud_reply_yaml(
    incoming: &ma_core::Message,
    reply_type: &str,
    ctx: &CrudHandlerCtx<'_>,
    yaml: &str,
) -> Result<()> {
    send_crud_reply_raw(incoming, reply_type, ctx, "text/yaml", yaml.as_bytes()).await
}

async fn send_crud_reply(
    incoming: &ma_core::Message,
    reply_type: &str,
    ctx: &CrudHandlerCtx<'_>,
    content: &[u8],
) -> Result<()> {
    send_crud_reply_raw(incoming, reply_type, ctx, "application/cbor", content).await
}

async fn send_crud_reply_raw(
    incoming: &ma_core::Message,
    reply_type: &str,
    ctx: &CrudHandlerCtx<'_>,
    content_type: &str,
    content: &[u8],
) -> Result<()> {
    let sender = Did::try_from(incoming.from.as_str())
        .with_context(|| format!("invalid sender DID: {}", incoming.from))?;

    let mut reply = ma_core::Message::new(
        ctx.our_did,
        &incoming.from,
        reply_type,
        content_type,
        content,
        ctx.signing_key,
    )
    .context("failed to build CRUD reply")?;
    reply.reply_to = Some(incoming.id.clone());

    match ctx
        .endpoint
        .outbox(ctx.resolver.as_ref(), &sender.base_id(), CRUD_PROTOCOL_ID)
        .await
    {
        Ok(mut outbox) => {
            outbox.send(&reply).await.context("CRUD reply send failed")?;
            info!(to = %incoming.from, reply_to = %incoming.id, "CRUD reply sent");
        }
        Err(err) => {
            warn!(error = %err, to = %incoming.from, "CRUD reply delivery failed");
        }
    }
    Ok(())
}

