//! Fluent-based i18n for log messages.
//!
//! Call [`init`] once at startup with the desired language tag ("nb" or "en").
//! Then use [`t`] anywhere to get a localised message string.
//! Unknown IDs are returned as-is so nothing silently disappears.

use fluent::{FluentBundle, FluentResource};
use std::collections::HashMap;
use std::sync::OnceLock;
use unic_langid::LanguageIdentifier;

static MESSAGES: OnceLock<HashMap<String, String>> = OnceLock::new();

const FTL_NB: &str = include_str!("../locales/nb.ftl");
const FTL_EN: &str = include_str!("../locales/en.ftl");

/// All known message IDs – must match the keys in both FTL files.
const MESSAGE_IDS: &[&str] = &[
    "own-did-published",
    "own-did-publish-failed",
    "own-did-publish-timeout",
    "started",
    "shutdown-requested",
    "closing-endpoint",
    "shutdown-complete",
    "status-listening",
    "rpc-message-received",
    "rpc-message-rejected",
    "ipfs-message-rejected",
    "ctrlc-handler-failed",
    "node-connected",
    "received-encrypted-ma-msg",
    "unknown-rpc-atom",
    "ping-received",
    "pong-sent",
    "pong-resolve-failed",
    "did-publish-request-received",
    "document-published",
    "did-publish-cid-reply-sent",
    "did-publish-resolve-failed",
    "ipfs-store-request-received",
    "ipfs-stored",
    "ipfs-store-cid-reply-sent",
    "ipfs-store-resolve-failed",
];

/// Initialise the global message table for `lang`.
/// Accepted values: `"nb"` (default), `"en"`.
/// Must be called before the first [`t`] call.
pub fn init(lang: &str) {
    MESSAGES.get_or_init(|| {
        let ftl = match lang {
            "en" => FTL_EN,
            _ => FTL_NB,
        };
        parse_ftl(ftl, lang)
    });
}

/// Return the localised string for `id`.
/// Falls back to `id` itself if the message is missing or [`init`] was not called.
#[must_use]
pub fn t(id: &str) -> String {
    MESSAGES
        .get()
        .and_then(|m| m.get(id))
        .cloned()
        .unwrap_or_else(|| id.to_string())
}

fn parse_ftl(ftl: &str, lang: &str) -> HashMap<String, String> {
    let resource = FluentResource::try_new(ftl.to_string())
        .expect("FTL parse error");
    let langid: LanguageIdentifier = lang
        .parse()
        .unwrap_or_else(|_| "nb".parse().expect("invalid fallback lang"));
    let mut bundle: FluentBundle<FluentResource> = FluentBundle::new(vec![langid]);
    bundle
        .add_resource(resource)
        .expect("failed to add FTL resource");

    let mut map = HashMap::new();
    for &id in MESSAGE_IDS {
        if let Some(msg) = bundle.get_message(id) {
            if let Some(pattern) = msg.value() {
                let mut errors = vec![];
                let value = bundle.format_pattern(pattern, None, &mut errors);
                map.insert(id.to_string(), value.into_owned());
            }
        } else {
            // Keep the raw ID as fallback so callers always get something.
            map.insert(id.to_string(), id.to_string());
        }
    }
    map
}
