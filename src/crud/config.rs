use anyhow::{anyhow, Context, Result};
use ciborium::Value as CborValue;
use tracing::warn;

use super::helpers::{
    load_manifest, send_crud_error, send_crud_i18n_error, send_crud_ok, send_crud_ok_cid,
    send_crud_reply_cbor, send_crud_reply_yaml, with_manifest_crud,
};
use super::CrudHandlerCtx;

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
        "log_file" => cfg.log_file.as_ref().map_or(serde_json::Value::Null, |p| {
            serde_json::Value::String(p.to_string_lossy().into_owned())
        }),
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

/// Convert a CBOR value to a `serde_json::Value` for storage in
/// `RuntimeManifest.config` (which uses `serde_json::Value` as its
/// value type). This lets clients send native CBOR types — text,
/// integer, boolean, float, null, arrays, maps — rather than
/// JSON-encoded strings.
fn cbor_to_json(val: &CborValue) -> serde_json::Value {
    match val {
        CborValue::Null => serde_json::Value::Null,
        CborValue::Bool(b) => serde_json::Value::Bool(*b),
        CborValue::Integer(i) => {
            if let Ok(n) = u64::try_from(*i) {
                serde_json::Value::Number(n.into())
            } else if let Ok(n) = i64::try_from(*i) {
                serde_json::Value::Number(n.into())
            } else {
                serde_json::Value::Null
            }
        }
        CborValue::Float(f) => serde_json::Number::from_f64(*f)
            .map(serde_json::Value::Number)
            .unwrap_or(serde_json::Value::Null),
        CborValue::Text(s) => serde_json::Value::String(s.clone()),
        CborValue::Bytes(b) => serde_json::Value::String(
            b.iter().map(|byte| format!("{byte:02x}")).collect(),
        ),
        CborValue::Array(arr) => {
            serde_json::Value::Array(arr.iter().map(cbor_to_json).collect())
        }
        CborValue::Map(pairs) => {
            let mut map = serde_json::Map::new();
            for (k, v) in pairs {
                if let CborValue::Text(key) = k {
                    map.insert(key.clone(), cbor_to_json(v));
                }
            }
            serde_json::Value::Object(map)
        }
        CborValue::Tag(_, inner) => cbor_to_json(inner),
        _ => serde_json::Value::Null,
    }
}

// ── Config namespace ───────────────────────────────────────────────────────────

#[allow(clippy::too_many_lines)]
pub(super) async fn handle_config_ns(
    message: &ma_core::Message,
    rest: &[String],
    tail: Option<&str>,
    args: Vec<CborValue>,
    reply_type: &str,
    ctx: &CrudHandlerCtx<'_>,
) -> Result<()> {
    // No key segment — operate on config root.
    if rest.is_empty() {
        return match (tail, args.as_slice()) {
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
        };
    }

    let [key] = rest else {
        return Err(anyhow!("unknown config operation"));
    };

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
        (Some(""), [value]) => {
            let key = key.as_str().to_string();
            let json_val = cbor_to_json(value);
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
