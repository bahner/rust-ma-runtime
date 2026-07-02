//! Periodic DID-document republishing task.
//!
//! Republishes the runtime's own DID document (and `runtime_ipns` root) from the
//! in-memory runtime head: immediately when the root CID changes, otherwise at
//! most once per cache-warm interval.  Split out of `main.rs` to keep the entry
//! point focused on orchestration.

use std::path::PathBuf;
use std::time::Duration;

use cid::Cid;
use ma_core::config::SecretBundle;
use ma_core::{Ipld, MaExtension};
use tracing::{error, info, warn};

use crate::ipfs;
use crate::status::SharedStats;

#[allow(clippy::too_many_arguments)]
pub fn spawn_periodic_did_publish(
    refresh_stats: SharedStats,
    refresh_kubo_url: String,
    refresh_ma_base: MaExtension,
    refresh_runtime_ipns_key: [u8; 32],
    refresh_bundle_path: PathBuf,
    refresh_passphrase: String,
    did_publish_interval_secs: u64,
    did_publish_cache_warm_secs: u64,
    did_publish_timeout_secs: u64,
    did_publish_lifetime_hours: u64,
) {
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(Duration::from_secs(did_publish_interval_secs));
        let mut last_published_cid: Option<String> = None;
        let mut last_published_at = std::time::Instant::now()
            .checked_sub(Duration::from_secs(did_publish_cache_warm_secs))
            .unwrap_or_else(std::time::Instant::now);
        loop {
            ticker.tick().await;
            let Some(latest_root_cid) = refresh_stats.read().await.root_cid.clone() else {
                continue;
            };
            let cid_changed = last_published_cid.as_deref() != Some(latest_root_cid.as_str());
            let cache_warm_elapsed =
                last_published_at.elapsed() >= Duration::from_secs(did_publish_cache_warm_secs);
            if !cid_changed && !cache_warm_elapsed {
                continue;
            }
            let runtime_cid = match Cid::try_from(latest_root_cid.as_str()) {
                Ok(c) => c,
                Err(err) => {
                    warn!(cid = %latest_root_cid, error = %err, "invalid root_cid for periodic DID publish");
                    continue;
                }
            };
            let bundle = match SecretBundle::load(&refresh_bundle_path, &refresh_passphrase) {
                Ok(b) => b,
                Err(err) => {
                    error!(error = %format!("{err:#}"), "periodic DID load secret bundle failed");
                    continue;
                }
            };
            let ma = refresh_ma_base
                .clone()
                .extra("runtime", Ipld::Link(runtime_cid));
            let document = match bundle.build_document(ma) {
                Ok(doc) => doc,
                Err(err) => {
                    error!(error = %format!("{err:#}"), "periodic DID build failed");
                    continue;
                }
            };
            let doc_cbor = match document.encode() {
                Ok(bytes) => bytes,
                Err(err) => {
                    error!(error = %format!("{err:#}"), "periodic DID encode failed");
                    continue;
                }
            };
            let ipns_key = bundle.ipns_secret_key.to_vec();
            let publish = tokio::time::timeout(
                Duration::from_secs(did_publish_timeout_secs),
                ipfs::do_publish_own_document(
                    refresh_kubo_url.clone(),
                    doc_cbor,
                    ipns_key,
                    did_publish_lifetime_hours,
                ),
            )
            .await;
            let mut did_ok = false;
            match publish {
                Ok(Ok(())) => {
                    info!(runtime_cid = %latest_root_cid, cid_changed, "periodic DID publish succeeded");
                    did_ok = true;
                }
                Ok(Err(err)) => {
                    error!(runtime_cid = %latest_root_cid, error = %format!("{err:#}"), "periodic DID publish failed");
                }
                Err(_) => error!(runtime_cid = %latest_root_cid, "periodic DID publish timed out"),
            }

            let ipns_ok = match tokio::time::timeout(
                Duration::from_secs(did_publish_timeout_secs),
                ipfs::publish_runtime_root_cid(
                    &refresh_kubo_url,
                    &refresh_runtime_ipns_key,
                    &latest_root_cid,
                    did_publish_lifetime_hours,
                ),
            )
            .await
            {
                Ok(Ok(_)) => {
                    info!(runtime_cid = %latest_root_cid, cid_changed, "periodic runtime_ipns publish succeeded");
                    true
                }
                Ok(Err(err)) => {
                    error!(runtime_cid = %latest_root_cid, error = %format!("{err:#}"), "periodic runtime_ipns publish failed");
                    false
                }
                Err(_) => {
                    error!(runtime_cid = %latest_root_cid, "periodic runtime_ipns publish timed out");
                    false
                }
            };

            if did_ok && ipns_ok {
                last_published_cid = Some(latest_root_cid);
                last_published_at = std::time::Instant::now();
            }
        }
    });
}
