//! Embedded IPFS node backing the runtime's publish and gateway operations.
//!
//! Replaces the external Kubo daemon: the runtime now runs its own libp2p
//! IPFS node (`rust-ipfs`) so it can store dag-cbor/raw blocks, resolve and
//! publish IPNS names, and fetch remote content without depending on a Kubo
//! RPC endpoint.

use std::path::PathBuf;
use std::str::FromStr;
use std::sync::OnceLock;

use anyhow::{anyhow, Context, Result};
use futures::StreamExt;
use ma_core::Ipld;
use rust_ipfs::builder::DefaultIpfsBuilder as IpfsBuilder;
use rust_ipfs::ipns::IpnsOption;
use rust_ipfs::unixfs::UnixfsStatus;
use rust_ipfs::{Ipfs, IpfsPath, Keypair};

/// Keychain label under which the runtime's dedicated IPNS key is imported.
pub const RUNTIME_IPNS_LABEL: &str = "runtime_ipns";

/// Process-wide node handle, initialised once at startup and shared by the
/// publish, gateway and CRUD paths (mirrors the existing `crate::kubo::client`
/// singleton pattern).
static NODE: OnceLock<IpfsNode> = OnceLock::new();

/// Store the process-wide node. Fails if a node is already initialised.
pub fn set_global(node: IpfsNode) -> Result<()> {
    NODE.set(node)
        .map_err(|_| anyhow!("IPFS node already initialised"))
}

/// Clone the process-wide node handle.
pub fn global() -> Result<IpfsNode> {
    NODE.get()
        .cloned()
        .ok_or_else(|| anyhow!("IPFS node not initialised"))
}

/// A running embedded IPFS node.
///
/// The node's libp2p identity is the DID identity key, so the default IPNS
/// name (`publish_ipns`) publishes under the `did:ma` IPNS root. The separate
/// runtime root key is imported into the keychain under [`RUNTIME_IPNS_LABEL`].
#[derive(Clone)]
pub struct IpfsNode {
    ipfs: Ipfs,
}

impl IpfsNode {
    /// Build and start the node from the DID identity key and runtime IPNS key.
    pub async fn start(
        repo_path: PathBuf,
        did_ipns_key: [u8; 32],
        runtime_ipns_key: [u8; 32],
        extra_bootstrap: &[String],
    ) -> Result<Self> {
        let did_keypair = Keypair::ed25519_from_bytes(did_ipns_key)
            .map_err(|e| anyhow!("invalid DID IPNS key: {e}"))?;
        let runtime_keypair = Keypair::ed25519_from_bytes(runtime_ipns_key)
            .map_err(|e| anyhow!("invalid runtime IPNS key: {e}"))?;

        let mut builder = IpfsBuilder::with_keypair(&did_keypair)?
            .with_default()
            .enable_tcp()
            .enable_dns()
            .default_record_key_validator()
            .set_path(&repo_path)
            .add_listening_addr("/ip4/0.0.0.0/tcp/0".parse()?);

        for addr in extra_bootstrap {
            let addr = addr
                .parse()
                .with_context(|| format!("invalid bootstrap multiaddr '{addr}'"))?;
            builder = builder.add_bootstrap(addr);
        }

        let ipfs = builder.start().await?;

        // The runtime IPNS key never touches disk; it is derived from the
        // SecretBundle and held in the in-memory keychain only.
        ipfs.keychain()
            .insert(RUNTIME_IPNS_LABEL, &runtime_keypair)
            .await
            .map_err(|e| anyhow!("failed to import runtime IPNS key: {e}"))?;

        let node = Self { ipfs };

        // Join the public DHT. Failure is non-fatal: the node still serves and
        // resolves local content, and publishing can be retried once peers are
        // reachable.
        node.ipfs.default_bootstrap().await?;
        let _ = node.ipfs.bootstrap().await;

        Ok(node)
    }

    /// Store a serialisable value as a dag-cbor node, pinning it, and return its CID string.
    pub async fn put_dag<T: serde::Serialize + Sync>(&self, value: &T) -> Result<String> {
        let cid = self.ipfs.put_dag(value).pin(true).await?;
        Ok(cid.to_string())
    }

    /// Store raw bytes as a UnixFS file, returning its CID string.
    pub async fn add_bytes(&self, bytes: Vec<u8>) -> Result<String> {
        let mut stream = self.ipfs.add_unixfs(bytes);
        while let Some(status) = stream.next().await {
            match status {
                UnixfsStatus::CompletedStatus { path, .. } => {
                    return path_to_cid(&path.to_string())
                }
                UnixfsStatus::FailedStatus { error, .. } => {
                    return Err(anyhow!("unixfs add failed: {error}"))
                }
                UnixfsStatus::ProgressStatus { .. } => {}
            }
        }
        Err(anyhow!("unixfs add did not complete"))
    }

    /// Fetch a dag-cbor node by `/ipfs/<cid>` path or bare CID.
    pub async fn get_dag(&self, path: &str) -> Result<Ipld> {
        let path =
            IpfsPath::from_str(path).with_context(|| format!("invalid IPFS path '{path}'"))?;
        self.ipfs.get_dag(path).await.map_err(|e| anyhow!("{e}"))
    }

    /// Fetch and deserialise a dag-cbor node as `T` (dag-json-compatible).
    pub async fn get_dag_value<T: serde::de::DeserializeOwned>(&self, path: &str) -> Result<T> {
        let ipld = self.get_dag(path).await?;
        let value = serde_json::to_value(&ipld).map_err(|e| anyhow!("serialise IPLD: {e}"))?;
        serde_json::from_value(value).map_err(|e| anyhow!("deserialise IPLD value: {e}"))
    }

    /// Fetch the raw bytes of a block by CID.
    pub async fn get_block_bytes(&self, cid: &str) -> Result<Vec<u8>> {
        let cid = cid::Cid::try_from(cid).with_context(|| format!("invalid CID '{cid}'"))?;
        let block = self.ipfs.get_block(cid).await.map_err(|e| anyhow!("{e}"))?;
        Ok(block.data().to_vec())
    }

    /// Fetch content bytes by `/ipfs/<cid>` or `/ipns/<name>` path.
    pub async fn cat(&self, path: &str) -> Result<Vec<u8>> {
        let path =
            IpfsPath::from_str(path).with_context(|| format!("invalid IPFS path '{path}'"))?;
        let bytes = self
            .ipfs
            .cat_unixfs(path)
            .await
            .map_err(|e| anyhow!("{e}"))?;
        Ok(bytes.to_vec())
    }

    /// Resolve an IPFS/IPNS path (or bare CID) to a bare CID string.
    pub async fn resolve(&self, path: &str) -> Result<String> {
        if !path.starts_with('/') {
            return Ok(path.to_string());
        }
        let resolved = self.resolve_ipns(path).await?;
        path_to_cid(&resolved)
    }

    /// Resolve an IPNS name (or path) to its current IPFS path.
    pub async fn resolve_ipns(&self, name: &str) -> Result<String> {
        let path =
            IpfsPath::from_str(name).with_context(|| format!("invalid IPNS name '{name}'"))?;
        let resolved = self.ipfs.resolve_ipns(&path, false).await?;
        Ok(resolved.to_string())
    }

    /// Publish a path under the DID identity IPNS name (the node keypair).
    pub async fn publish_did_ipns(&self, path: &str) -> Result<String> {
        let path =
            IpfsPath::from_str(path).with_context(|| format!("invalid IPFS path '{path}'"))?;
        let published = self.ipfs.publish_ipns(&path).await?;
        Ok(published.to_string())
    }

    /// Publish a path under the runtime's dedicated IPNS key.
    pub async fn publish_runtime_ipns(&self, path: &str) -> Result<String> {
        let path =
            IpfsPath::from_str(path).with_context(|| format!("invalid IPFS path '{path}'"))?;
        let published = self
            .ipfs
            .ipns()
            .publish(Some(RUNTIME_IPNS_LABEL), &path, IpnsOption::DHT)
            .await
            .map_err(|e| anyhow!("{e}"))?;
        Ok(published.to_string())
    }

    /// Recursively pin a CID so it survives garbage collection.
    pub async fn pin(&self, cid: &str) -> Result<()> {
        let cid = cid::Cid::try_from(cid).with_context(|| format!("invalid CID '{cid}'"))?;
        self.ipfs
            .insert_pin(cid)
            .recursive()
            .await
            .map_err(|e| anyhow!("{e}"))
    }

    /// Announce this node as a provider of `cid` so remote peers can fetch it.
    pub async fn provide(&self, cid: &str) -> Result<()> {
        let cid = cid::Cid::try_from(cid).with_context(|| format!("invalid CID '{cid}'"))?;
        self.ipfs.provide(cid).await.map_err(|e| anyhow!("{e}"))
    }

    /// Run garbage collection, retaining only pinned blocks.
    pub async fn gc(&self) -> Result<()> {
        self.ipfs.gc().await.map_err(|e| anyhow!("{e}"))?;
        Ok(())
    }
}

/// Strip an `/ipfs/<cid>` path (or bare CID) down to the bare CID string.
fn path_to_cid(path: &str) -> Result<String> {
    let cid = path
        .trim()
        .trim_start_matches('/')
        .trim_start_matches("ipfs/");
    if cid.is_empty() {
        Err(anyhow!("empty CID in path '{path}'"))
    } else {
        Ok(cid.to_string())
    }
}
