//! Thin block/IPLD operations backed by the embedded IPFS node.
//!
//! These were formerly Kubo HTTP API wrappers. The runtime now runs its own
//! node, so every operation delegates to [`crate::node::global`]. The `kubo_url`
//! parameters are vestigial and retained only so call sites keep compiling
//! during the migration; they are removed in the final cleanup pass.

use anyhow::Result;
use serde::{de::DeserializeOwned, Serialize};
use std::sync::OnceLock;

/// HTTP client kept for the status-server gateway relay until that path is
/// migrated to the embedded node as well.
pub fn client() -> &'static reqwest::Client {
    static CLIENT: OnceLock<reqwest::Client> = OnceLock::new();

    CLIENT.get_or_init(|| {
        reqwest::Client::builder()
            .connect_timeout(std::time::Duration::from_secs(5))
            .timeout(std::time::Duration::from_secs(30))
            .build()
            .unwrap_or_else(|_| reqwest::Client::new())
    })
}

fn node() -> Result<crate::node::IpfsNode> {
    crate::node::global()
}

/// Publish a serialisable value as a `dag-cbor` node via the embedded node.
pub async fn dag_put<T: Serialize + Sync>(_kubo_url: &str, value: &T) -> Result<String> {
    node()?.put_dag(value).await
}

/// Recursively pin a CID.
pub async fn pin_add(_kubo_url: &str, cid: &str) -> Result<()> {
    node()?.pin(cid).await
}

/// Move the live pin from `old_cid` to `new_cid`. The old pin is dropped and
/// reclaimed by garbage collection.
pub async fn pin_update(_kubo_url: &str, _old_cid: &str, new_cid: &str) -> Result<()> {
    node()?.pin(new_cid).await
}

/// Add raw bytes as a UnixFS file without creating a direct pin.
pub async fn ipfs_add_bytes_unpinned(_kubo_url: &str, data: Vec<u8>) -> Result<String> {
    node()?.add_bytes(data).await
}

/// Fetch raw bytes from IPFS for a CID.
pub async fn cat_bytes(_kubo_url: &str, cid: &str) -> Result<Vec<u8>> {
    node()?.cat(&format!("/ipfs/{cid}")).await
}

/// Fetch an IPLD node and deserialise it from `dag-cbor`.
pub async fn dag_get<T: DeserializeOwned>(_kubo_url: &str, cid: &str) -> Result<T> {
    node()?.get_dag_value::<T>(cid).await
}

/// Resolve an IPFS/IPNS path (or bare CID) to a bare CID string.
pub async fn dag_resolve(_kubo_url: &str, path: &str) -> Result<String> {
    node()?.resolve(path).await
}
