//! Boot-time helpers: secret-bundle loading and configuration defaults.
//!
//! These are pure functions over [`Config`] used during daemon startup, split
//! out of `main.rs` to keep the entry point focused on orchestration.

use anyhow::{anyhow, Context, Result};
use ma_core::config::{Config, SecretBundle};

pub fn load_secret_bundle(config: &Config) -> Result<SecretBundle> {
    let passphrase = config
        .secret_bundle_passphrase
        .as_deref()
        .ok_or_else(|| anyhow!("secret_bundle_passphrase is required (env or config)"))?;
    let bundle_path = config.effective_secret_bundle()?;
    SecretBundle::load(&bundle_path, passphrase).with_context(|| {
        format!(
            "failed to load secret bundle from {}",
            bundle_path.display()
        )
    })
}

pub fn get_u64_setting(config: &Config, key: &str, default: u64) -> u64 {
    config
        .extra
        .get(key)
        .and_then(serde_yaml::Value::as_u64)
        .unwrap_or(default)
}

pub fn runtime_manifest_config(
    config: &Config,
) -> std::collections::BTreeMap<String, serde_yaml::Value> {
    let mut out = std::collections::BTreeMap::new();

    out.insert(
        "did_resolver_positive_ttl_secs".to_string(),
        serde_yaml::Value::from(get_u64_setting(
            config,
            "did_resolver_positive_ttl_secs",
            60,
        )),
    );
    out.insert(
        "did_resolver_negative_ttl_secs".to_string(),
        serde_yaml::Value::from(get_u64_setting(
            config,
            "did_resolver_negative_ttl_secs",
            10,
        )),
    );
    out.insert(
        "did_document_publishing_interval_secs".to_string(),
        serde_yaml::Value::from(get_u64_setting(
            config,
            "did_document_publishing_interval_secs",
            300,
        )),
    );
    out.insert(
        "did_document_publishing_timeout_secs".to_string(),
        serde_yaml::Value::from(get_u64_setting(
            config,
            "did_document_publishing_timeout_secs",
            120,
        )),
    );
    out.insert(
        "did_document_publishing_lifetime_hours".to_string(),
        serde_yaml::Value::from(get_u64_setting(
            config,
            "did_document_publishing_lifetime_hours",
            8760,
        )),
    );
    out.insert(
        "ipns_publish_lifetime_hours".to_string(),
        serde_yaml::Value::from(get_u64_setting(config, "ipns_publish_lifetime_hours", 8760)),
    );
    out.insert(
        "ipns_publish_resolve".to_string(),
        serde_yaml::Value::from(
            config
                .extra
                .get("ipns_publish_resolve")
                .and_then(serde_yaml::Value::as_bool)
                .unwrap_or(false),
        ),
    );
    out.insert(
        "ipns_publish_allow_offline".to_string(),
        serde_yaml::Value::from(
            config
                .extra
                .get("ipns_publish_allow_offline")
                .and_then(serde_yaml::Value::as_bool)
                .unwrap_or(true),
        ),
    );
    out
}
