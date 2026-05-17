//! Minimal Kubo HTTP API wrappers for DAG operations not re-exported by
//! `ma_core`.  Only `dag_put` and `dag_get` are needed here; other Kubo
//! operations (`ipfs_add`, `cat_bytes`) are used directly from `ma_core`.

use anyhow::{anyhow, Result};
use reqwest::multipart;
use serde::{de::DeserializeOwned, Deserialize, Serialize};

// ── Response types ────────────────────────────────────────────────────────────

#[derive(Deserialize)]
struct DagPutCid {
    #[serde(rename = "/")]
    slash: String,
}

#[derive(Deserialize)]
struct DagPutResponse {
    #[serde(default, rename = "Cid")]
    cid_upper: Option<DagPutCid>,
    #[serde(default)]
    cid: Option<DagPutCid>,
}

// ── Public API ────────────────────────────────────────────────────────────────

/// Publish a serialisable value as a `dag-cbor` IPLD node via Kubo.
/// Input is serialised as `dag-json`; Kubo converts and stores as `dag-cbor`.
/// Returns the resulting CID string.
pub async fn dag_put<T: Serialize + Sync>(kubo_url: &str, value: &T) -> Result<String> {
    let base = kubo_url.trim_end_matches('/');
    let url = format!("{base}/api/v0/dag/put");
    let payload = serde_json::to_vec(value)?;

    let part = multipart::Part::bytes(payload)
        .file_name("node.json")
        .mime_str("application/json")?;
    let form = multipart::Form::new().part("file", part);

    let body = reqwest::Client::new()
        .post(url)
        .query(&[
            ("store-codec", "dag-cbor"),
            ("input-codec", "dag-json"),
            ("pin", "true"),
        ])
        .multipart(form)
        .send()
        .await?
        .error_for_status()?
        .text()
        .await?;

    let parsed: DagPutResponse = serde_json::from_str(&body)
        .map_err(|e| anyhow!("failed parsing dag/put response: {e} body={body}"))?;
    parsed
        .cid_upper
        .or(parsed.cid)
        .map(|c| c.slash)
        .ok_or_else(|| anyhow!("missing CID in dag/put response: {body}"))
}

/// Fetch an IPLD node from Kubo and deserialise it from `dag-json`.
pub async fn dag_get<T: DeserializeOwned>(kubo_url: &str, cid: &str) -> Result<T> {
    let base = kubo_url.trim_end_matches('/');
    let url = format!("{base}/api/v0/dag/get");

    let body = reqwest::Client::new()
        .post(&url)
        .query(&[("arg", cid), ("output-codec", "dag-json")])
        .send()
        .await?
        .error_for_status()?
        .text()
        .await?;

    serde_json::from_str(&body)
        .map_err(|e| anyhow!("failed to deserialise dag/get response for {cid}: {e} body={body}"))
}
