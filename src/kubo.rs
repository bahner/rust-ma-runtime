//! Thin Kubo RPC block/IPLD operations.
//!
//! Kubo RPC (`http://127.0.0.1:5001` by default) is the runtime's single IPFS
//! backend — publishing, DID/IPNS resolution and content reads all go through
//! the Kubo RPC API, and the runtime always requires a running Kubo daemon.
//! `ma-core` owns the higher-level publish/DID logic; this module is the
//! runtime-local glue for the raw DAG block operations that `ma-core`
//! deliberately does not re-export (`dag_put`, `dag_get`, `dag_resolve`, pin
//! management). Clients that cannot reach Kubo (e.g. browser wasm) supply their
//! own IPFS instead.
//!
//! Every request is bounded by a client timeout and passes through a small
//! semaphore gate: `dag/get` on a CID that is not in the local store makes Kubo
//! search the network, and without a client-side bound that request (and
//! whatever task awaits it) would hang indefinitely.

use anyhow::{anyhow, Result};
use reqwest::multipart;
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use std::sync::OnceLock;
use std::time::Duration;
use tokio::sync::{Semaphore, SemaphorePermit};

const MAX_CONCURRENT_KUBO_REQUESTS: usize = 16;
const KUBO_PERMIT_TIMEOUT: Duration = Duration::from_secs(5);

/// Shared HTTP client with hard timeouts, also used by the runtime's other
/// Kubo RPC call sites (`ipfs.rs`, `status.rs`).
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

fn request_gate() -> &'static Semaphore {
    static GATE: OnceLock<Semaphore> = OnceLock::new();
    GATE.get_or_init(|| Semaphore::new(MAX_CONCURRENT_KUBO_REQUESTS))
}

async fn acquire_request_permit(op: &str) -> Result<SemaphorePermit<'static>> {
    tokio::time::timeout(KUBO_PERMIT_TIMEOUT, request_gate().acquire())
        .await
        .map_err(|_| anyhow!("kubo request queue timed out for {op}"))?
        .map_err(|_| anyhow!("kubo request gate closed for {op}"))
}

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

#[derive(Deserialize)]
struct DagResolveResponse {
    #[serde(rename = "Cid")]
    cid: DagPutCid,
}

#[derive(Deserialize)]
struct AddResponse {
    #[serde(rename = "Hash")]
    hash: String,
}

// ── Public API ────────────────────────────────────────────────────────────────

/// Store a serialisable value as a `dag-cbor` IPLD node via Kubo.
///
/// Input is serialised as `dag-json`; Kubo converts and stores it as
/// `dag-cbor`. The node is deliberately **not** directly pinned — runtime state
/// is kept alive by the recursive pin on the root manifest (see [`pin_add`] /
/// [`pin_update`]). Returns the resulting CID string.
pub async fn dag_put<T: Serialize + Sync>(kubo_url: &str, value: &T) -> Result<String> {
    let _permit = acquire_request_permit("dag/put").await?;
    let base = kubo_url.trim_end_matches('/');
    let url = format!("{base}/api/v0/dag/put");
    let payload = serde_json::to_vec(value)?;

    let part = multipart::Part::bytes(payload)
        .file_name("node.json")
        .mime_str("application/json")?;
    let form = multipart::Form::new().part("file", part);

    let body = client()
        .post(url)
        .query(&[
            ("store-codec", "dag-cbor"),
            ("input-codec", "dag-json"),
            ("pin", "false"),
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

/// Recursively pin a CID. Used for first-time bootstrap when there is no prior
/// root to update from.
pub async fn pin_add(kubo_url: &str, cid: &str) -> Result<()> {
    let _permit = acquire_request_permit("pin/add").await?;
    let base = kubo_url.trim_end_matches('/');
    let url = format!("{base}/api/v0/pin/add");
    client()
        .post(&url)
        .query(&[("arg", cid), ("recursive", "true")])
        .send()
        .await?
        .error_for_status()?;
    Ok(())
}

/// Kubo defaults remote-pin listing and removal to `status=pinned`. Include the
/// transient states so queued and failed requests are visible and removable.
const REMOTE_PIN_STATUSES: [(&str, &str); 4] = [
    ("status", "queued"),
    ("status", "pinning"),
    ("status", "pinned"),
    ("status", "failed"),
];

#[derive(Deserialize)]
struct RemotePinListEntry {
    #[serde(default, rename = "Name")]
    name_upper: String,
    #[serde(default)]
    name_lower: String,
    #[serde(default, rename = "Cid")]
    cid_upper: String,
    #[serde(default)]
    cid_lower: String,
}

/// One remote-pin record as reported by the pinning service.
struct RemotePin {
    name: String,
    cid: String,
}

/// List every remote pin on `service`.
///
/// Kubo's `name` filter is not consistently an exact match across pinning
/// services, and the runtime needs the whole picture anyway (managed name,
/// in-flight name, undated names from earlier builds), so the caller filters
/// locally.
async fn list_remote_pins(kubo_url: &str, service: &str) -> Result<Vec<RemotePin>> {
    let _permit = acquire_request_permit("pin/remote/ls").await?;
    let base = kubo_url.trim_end_matches('/');
    let url = format!("{base}/api/v0/pin/remote/ls");
    let mut query: Vec<(&str, &str)> = vec![("service", service)];
    query.extend_from_slice(&REMOTE_PIN_STATUSES);
    let body = client()
        .post(url)
        .query(&query)
        .send()
        .await?
        .error_for_status()?
        .text()
        .await?;

    // pin/remote/ls streams NDJSON: one JSON record per pin, per line.
    let mut pins = Vec::new();
    for line in body.lines().map(str::trim).filter(|line| !line.is_empty()) {
        let entry: RemotePinListEntry = serde_json::from_str(line).map_err(|error| {
            anyhow!("failed parsing pin/remote/ls response: {error} line={line}")
        })?;
        let pin_name = if entry.name_upper.is_empty() {
            entry.name_lower
        } else {
            entry.name_upper
        };
        let cid = if entry.cid_upper.is_empty() {
            entry.cid_lower
        } else {
            entry.cid_upper
        };
        if !cid.is_empty() {
            pins.push(RemotePin {
                name: pin_name,
                cid,
            });
        }
    }
    Ok(pins)
}

/// Remove exactly the remote-pin record `pin` on `service`.
///
/// Both `cid` and `name` are sent: Kubo removes every record matching the
/// supplied filters, so omitting `name` would also delete a sibling pin (root,
/// DID, owner document) that happens to hold the same CID.
async fn remove_remote_pin(kubo_url: &str, service: &str, pin: &RemotePin) -> Result<()> {
    let _permit = acquire_request_permit("pin/remote/rm").await?;
    let base = kubo_url.trim_end_matches('/');
    let url = format!("{base}/api/v0/pin/remote/rm");
    let mut query: Vec<(&str, &str)> =
        vec![("service", service), ("cid", &pin.cid), ("name", &pin.name)];
    query.extend_from_slice(&REMOTE_PIN_STATUSES);
    query.push(("force", "true"));
    let resp = client().post(url).query(&query).send().await?;
    if resp.status().is_success() {
        return Ok(());
    }
    let status = resp.status();
    let body = resp.text().await.unwrap_or_default();
    if status.as_u16() == 404 || body.contains("not found") || body.contains("not pinned") {
        return Ok(());
    }
    Err(anyhow!(
        "pin/remote/rm {} as {} from {service} failed: {body}",
        pin.cid,
        pin.name
    ))
}

/// The undated base of a dated managed name, e.g. `ma-runtime-sky-root` for
/// `ma-runtime-sky-root-2026-11-03`. Also strips ma-core's `~new` suffix.
fn dated_base(name: &str) -> Option<&str> {
    let name = name.strip_suffix("~new").unwrap_or(name);
    let (base, day) = name.rsplit_once('-')?;
    let (base, month) = base.rsplit_once('-')?;
    let (base, year) = base.rsplit_once('-')?;
    [year, month, day]
        .iter()
        .zip([4, 2, 2])
        .all(|(part, len)| part.len() == len && part.chars().all(|c| c.is_ascii_digit()))
        .then_some(base)
}

/// Pin `cid` under `name` on `service`, waiting for the service to accept the
/// request. An already-existing pin is treated as success.
///
/// This deliberately does not use `background=true`: the reconcile removes the
/// previous record afterwards, so the fresh record must exist before that
/// happens.
async fn add_remote_pin(kubo_url: &str, service: &str, name: &str, cid: &str) -> Result<()> {
    let _permit = acquire_request_permit("pin/remote/add").await?;
    let base = kubo_url.trim_end_matches('/');
    let url = format!("{base}/api/v0/pin/remote/add");
    let cid_path = cid
        .trim()
        .trim_start_matches("/ipfs/")
        .trim_start_matches('/');
    let arg = format!("/ipfs/{cid_path}");
    let resp = client()
        .post(url)
        .query(&[("arg", arg.as_str()), ("service", service), ("name", name)])
        .send()
        .await?;
    if resp.status().is_success() {
        return Ok(());
    }
    let status = resp.status();
    let body = resp.text().await.unwrap_or_default();
    if status.as_u16() == 409
        || body.contains("DUPLICATE_OBJECT")
        || body.contains("already pinned")
        || body.contains("already exists")
    {
        return Ok(());
    }
    Err(anyhow!(
        "pin/remote/add {cid} to {service} as {name} failed: {body}"
    ))
}

/// How many times to re-list after an add before concluding the service has
/// not surfaced the fresh pin yet.
const REMOTE_PIN_CONFIRM_ATTEMPTS: usize = 3;
const REMOTE_PIN_CONFIRM_DELAY: Duration = Duration::from_secs(1);

/// Whether `pin` belongs to the managed pin named `name`.
///
/// `undated` is the same pin's base name: builds before the daily naming scheme
/// was restored wrote undated records, and those are cleaned up here. Other
/// dates are deliberately not included — they are previous days' backups.
fn belongs_to_managed_pin(
    name: &str,
    in_flight: &str,
    undated: Option<&str>,
    pin: &RemotePin,
) -> bool {
    if pin.name == name || pin.name == in_flight {
        return true;
    }
    undated.is_some_and(|base| pin.name == base || pin.name.strip_suffix("~new") == Some(base))
}

/// Re-list after an add until the fresh record is visible.
///
/// Even a synchronous `pin/remote/add` can be reported by the service slightly
/// after the request returns, so the reconcile confirms the record before it
/// removes anything.
async fn confirm_remote_pin(
    kubo_url: &str,
    service: &str,
    name: &str,
    cid: &str,
) -> Result<Vec<RemotePin>> {
    let mut pins = list_remote_pins(kubo_url, service).await?;
    for _ in 1..REMOTE_PIN_CONFIRM_ATTEMPTS {
        if pins.iter().any(|pin| pin.name == name && pin.cid == cid) {
            break;
        }
        tokio::time::sleep(REMOTE_PIN_CONFIRM_DELAY).await;
        pins = list_remote_pins(kubo_url, service).await?;
    }
    Ok(pins)
}

/// Ensure `cid` is pinned under the managed `name` and, when `overwrite` is
/// set, remove every other record belonging to that managed pin.
///
/// Managed names carry today's UTC date. Within that name, `name` and ma-core's
/// in-flight `<name>~new` are reconciled to the fresh CID; records under other
/// dates are left alone because they are earlier daily backups. Undated records
/// written by builds before the daily scheme was restored are removed too.
/// Removal is always scoped to the record's own name, so a sibling managed pin
/// (root, DID, owner document) that happens to hold the same CID is preserved.
///
/// Nothing is removed until the fresh CID is confirmed on the service. A
/// background pin request can be accepted long before it is listed, and
/// deleting the previous record first would leave the managed pin missing. If
/// the fresh CID never surfaces, any existing record that already holds it is
/// kept and stale cleanup is skipped for this pass.
///
/// The reconcile runs inline rather than as a detached ma-core job: a runtime
/// that restarts before the background task finishes would otherwise add one
/// pin per start and never remove the previous one. The pin is only added when
/// the managed name does not already hold `cid`, which avoids the
/// duplicate-object churn on every restart and republication.
pub async fn replace_named_remote_pin(
    kubo_url: &str,
    service: &str,
    name: &str,
    cid: &str,
    overwrite: bool,
) -> Result<usize> {
    let pins = list_remote_pins(kubo_url, service).await?;
    let pins = if pins.iter().any(|pin| pin.name == name && pin.cid == cid) {
        pins
    } else {
        add_remote_pin(kubo_url, service, name, cid).await?;
        confirm_remote_pin(kubo_url, service, name, cid).await?
    };
    if !overwrite {
        return Ok(0);
    }

    let in_flight = ma_core::in_flight_pin_name(name);
    let undated = dated_base(name);
    let keep = pins
        .iter()
        .find(|pin| pin.name == name && pin.cid == cid)
        // The service may not have surfaced the fresh record yet; an existing
        // record that already holds this CID is still safe to keep.
        .or_else(|| {
            pins.iter().find(|pin| {
                pin.cid == cid && belongs_to_managed_pin(name, &in_flight, undated, pin)
            })
        });
    let Some(keep) = keep else {
        tracing::warn!(
            name = %name,
            service,
            cid = %cid,
            "fresh remote pin is not visible yet; keeping existing pins"
        );
        return Ok(0);
    };

    let mut removed = 0;
    for pin in &pins {
        if !belongs_to_managed_pin(name, &in_flight, undated, pin) {
            continue;
        }
        if pin.name == keep.name && pin.cid == keep.cid {
            continue;
        }
        remove_remote_pin(kubo_url, service, pin).await?;
        removed += 1;
    }
    Ok(removed)
}

/// Atomically move the recursive pin from `old_cid` to `new_cid` via Kubo's
/// `pin/update` endpoint (`unpin=true`). If `old_cid` was not pinned, falls back
/// to [`pin_add`] of `new_cid` so first-time callers work too.
pub async fn pin_update(kubo_url: &str, old_cid: &str, new_cid: &str) -> Result<()> {
    let permit = acquire_request_permit("pin/update").await?;
    let base = kubo_url.trim_end_matches('/');
    let url = format!("{base}/api/v0/pin/update");
    let resp = client()
        .post(&url)
        .query(&[("arg", old_cid), ("arg", new_cid), ("unpin", "true")])
        .send()
        .await?;
    if resp.status().is_success() {
        return Ok(());
    }
    let body = resp.text().await.unwrap_or_default();
    // Kubo returns "not recursively pinned already" (or similar) when the old
    // CID was never pinned. Fall back to a plain recursive pin of the new CID.
    if body.contains("not recursively pinned") || body.contains("not pinned") {
        drop(permit);
        return pin_add(kubo_url, new_cid).await;
    }
    Err(anyhow!("pin/update {old_cid} → {new_cid} failed: {body}"))
}

async fn ipfs_add_bytes_with_pin(kubo_url: &str, data: Vec<u8>, pin: bool) -> Result<String> {
    let _permit = acquire_request_permit("add").await?;
    let base = kubo_url.trim_end_matches('/');
    let url = format!("{base}/api/v0/add");

    let part = multipart::Part::bytes(data).file_name("data");
    let form = multipart::Form::new().part("file", part);

    let body = client()
        .post(url)
        .query(&[("pin", if pin { "true" } else { "false" })])
        .multipart(form)
        .send()
        .await?
        .error_for_status()?
        .text()
        .await?;

    let parsed: AddResponse = serde_json::from_str(&body)
        .map_err(|e| anyhow!("failed parsing add response: {e} body={body}"))?;
    Ok(parsed.hash)
}

/// Add raw bytes to IPFS via Kubo without creating a direct local pin.
///
/// Use this for runtime-owned state that is linked from the root manifest and
/// therefore kept alive by the root recursive pin lifecycle.
pub async fn ipfs_add_bytes_unpinned(kubo_url: &str, data: Vec<u8>) -> Result<String> {
    ipfs_add_bytes_with_pin(kubo_url, data, false).await
}

/// Fetch raw bytes from IPFS through Kubo's `/cat` endpoint.
pub async fn cat_bytes(kubo_url: &str, cid: &str) -> Result<Vec<u8>> {
    let _permit = acquire_request_permit("cat").await?;
    let base = kubo_url.trim_end_matches('/');
    let url = format!("{base}/api/v0/cat");

    let bytes = client()
        .post(url)
        .query(&[("arg", cid)])
        .send()
        .await?
        .error_for_status()?
        .bytes()
        .await?;

    Ok(bytes.to_vec())
}

/// Fetch an IPLD node from Kubo and deserialise it from `dag-json`.
pub async fn dag_get<T: DeserializeOwned>(kubo_url: &str, cid: &str) -> Result<T> {
    let _permit = acquire_request_permit("dag/get").await?;
    let base = kubo_url.trim_end_matches('/');
    let url = format!("{base}/api/v0/dag/get");

    let body = client()
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

/// Resolve an IPFS/IPNS path to a bare CID string.
///
/// Accepts bare CIDs (`bafy…`), `/ipfs/<cid>`, and `/ipns/<key>` paths.
/// `/ipns/` paths are resolved through Kubo's name resolution.
pub async fn dag_resolve(kubo_url: &str, path: &str) -> Result<String> {
    // Bare CID — nothing to resolve.
    if !path.starts_with('/') {
        return Ok(path.to_string());
    }

    // A plain `/ipfs/<cid>` path resolves to itself. Answering locally avoids a
    // needless Kubo round-trip that, for a block the node has never seen, would
    // make Kubo walk the network just to hand back the same CID.
    if let Some(cid) = path.strip_prefix("/ipfs/") {
        if !cid.is_empty() && !cid.contains('/') {
            return Ok(cid.to_string());
        }
    }

    let _permit = acquire_request_permit("dag/resolve").await?;

    let base = kubo_url.trim_end_matches('/');
    let url = format!("{base}/api/v0/dag/resolve");

    let body = client()
        .post(&url)
        .query(&[("arg", path)])
        .send()
        .await?
        .error_for_status()?
        .text()
        .await?;

    let parsed: DagResolveResponse = serde_json::from_str(&body)
        .map_err(|e| anyhow!("failed parsing dag/resolve response for {path}: {e} body={body}"))?;
    Ok(parsed.cid.slash)
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{
        body::Bytes,
        extract::{RawQuery, State},
        routing::post,
        Router,
    };
    use std::sync::Arc;
    use tokio::sync::Mutex;

    #[derive(Clone, Default)]
    struct AddState(Arc<Mutex<Vec<String>>>);

    async fn add_handler(State(state): State<AddState>, RawQuery(q): RawQuery, _: Bytes) -> String {
        let pin = q
            .unwrap_or_default()
            .split('&')
            .find_map(|kv| kv.strip_prefix("pin="))
            .unwrap_or("")
            .to_string();
        state.0.lock().await.push(pin);
        "{\"Hash\":\"bafyreibkhaddtestcid\"}".to_string()
    }

    async fn start_add_server(state: AddState) -> String {
        let app = Router::new()
            .route("/api/v0/add", post(add_handler))
            .with_state(state);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        format!("http://{addr}")
    }

    #[tokio::test]
    async fn dag_resolve_answers_bare_and_ipfs_paths_locally() {
        // An unreachable Kubo URL proves the answer never leaves the process.
        let unreachable = "http://127.0.0.1:1";
        let cid = "bafyreiazobsn6czyrmgqvblmodhiesxt6th32fsddvq6y7lek3kqkcirty";

        assert_eq!(dag_resolve(unreachable, cid).await.unwrap(), cid);
        assert_eq!(
            dag_resolve(unreachable, &format!("/ipfs/{cid}"))
                .await
                .unwrap(),
            cid
        );
    }

    #[tokio::test]
    async fn state_add_uses_unpinned_kubo_add() {
        let state = AddState::default();
        let url = start_add_server(state.clone()).await;

        let cid = ipfs_add_bytes_unpinned(&url, b"state".to_vec())
            .await
            .unwrap();

        assert_eq!(cid, "bafyreibkhaddtestcid");
        assert_eq!(state.0.lock().await.as_slice(), &["false"]);
    }

    #[tokio::test]
    async fn replace_named_remote_pin_removes_stale_and_in_flight_pins() {
        let kubo = crate::testkubo::MockKubo::start().await;
        let name = "ma-runtime-sky-root-2026-11-03";
        let in_flight = ma_core::in_flight_pin_name(name);
        kubo.add_remote_pin("pinata", name, "bafyold").await;
        kubo.add_remote_pin("pinata", &in_flight, "bafyinflight")
            .await;

        let removed = replace_named_remote_pin(kubo.url(), "pinata", name, "bafynew", true)
            .await
            .unwrap();

        assert_eq!(removed, 2);
        assert_eq!(kubo.remote_pins_named("pinata", name).await, ["bafynew"]);
        assert_eq!(
            kubo.remote_pins_named("pinata", &in_flight).await,
            Vec::<String>::new()
        );
    }

    #[tokio::test]
    async fn replace_named_remote_pin_without_overwrite_keeps_stale_pins() {
        let kubo = crate::testkubo::MockKubo::start().await;
        let name = "ma-runtime-sky-root-2026-11-03";
        kubo.add_remote_pin("pinata", name, "bafyold").await;

        let removed = replace_named_remote_pin(kubo.url(), "pinata", name, "bafynew", false)
            .await
            .unwrap();

        assert_eq!(removed, 0);
        let mut pins = kubo.remote_pins_named("pinata", name).await;
        pins.sort();
        assert_eq!(pins, ["bafynew", "bafyold"]);
    }

    #[tokio::test]
    async fn replace_named_remote_pin_keeps_sibling_names() {
        let kubo = crate::testkubo::MockKubo::start().await;
        let service = "pinata-sibling";
        let root = "ma-runtime-sky-root-2026-11-03";
        let did = "ma-runtime-sky-did-2026-11-03";
        // Both managed names legitimately hold the same document-ish CID. A
        // CID-only removal would delete the DID pin along with the root pin.
        kubo.add_remote_pin(service, root, "bafyshared").await;
        kubo.add_remote_pin(service, did, "bafyshared").await;

        let removed = replace_named_remote_pin(kubo.url(), service, root, "bafynew", true)
            .await
            .unwrap();

        assert_eq!(removed, 1);
        assert_eq!(kubo.remote_pins_named(service, root).await, ["bafynew"]);
        assert_eq!(kubo.remote_pins_named(service, did).await, ["bafyshared"]);
    }

    #[tokio::test]
    async fn replace_named_remote_pin_keeps_previous_days() {
        let kubo = crate::testkubo::MockKubo::start().await;
        let service = "pinata-days";
        let current = "ma-runtime-sky-root-2026-11-03";
        let previous = "ma-runtime-sky-root-2026-11-02";
        kubo.add_remote_pin(service, previous, "bafyday1").await;
        kubo.add_remote_pin(service, current, "bafyold").await;

        let removed = replace_named_remote_pin(kubo.url(), service, current, "bafynew", true)
            .await
            .unwrap();

        assert_eq!(removed, 1);
        assert_eq!(kubo.remote_pins_named(service, current).await, ["bafynew"]);
        assert_eq!(
            kubo.remote_pins_named(service, previous).await,
            ["bafyday1"],
            "earlier days are backups and must survive"
        );
    }

    #[tokio::test]
    async fn replace_named_remote_pin_removes_undated_artifacts() {
        let kubo = crate::testkubo::MockKubo::start().await;
        let service = "pinata-undated";
        let undated = "ma-runtime-joanna-root";
        let undated_in_flight = ma_core::in_flight_pin_name(undated);
        let current = "ma-runtime-joanna-root-2026-11-03";
        kubo.add_remote_pin(service, undated, "bafyundated").await;
        kubo.add_remote_pin(service, &undated_in_flight, "bafyundated2")
            .await;

        let removed = replace_named_remote_pin(kubo.url(), service, current, "bafynew", true)
            .await
            .unwrap();

        assert_eq!(removed, 2);
        assert_eq!(
            kubo.remote_pins_named(service, undated).await,
            Vec::<String>::new()
        );
        assert_eq!(
            kubo.remote_pins_named(service, &undated_in_flight).await,
            Vec::<String>::new()
        );
        assert_eq!(kubo.remote_pins_named(service, current).await, ["bafynew"]);
    }

    #[tokio::test]
    async fn replace_named_remote_pin_keeps_existing_pin_when_fresh_is_not_visible() {
        let kubo = crate::testkubo::MockKubo::start().await;
        let service = "pinata-lag";
        let name = "ma-runtime-joanna-root-2026-11-03";
        kubo.add_remote_pin(service, name, "bafyold").await;
        kubo.lag_remote_adds();

        let removed = replace_named_remote_pin(kubo.url(), service, name, "bafynew", true)
            .await
            .unwrap();

        // The fresh pin never surfaced, so the previous managed pin must stay.
        assert_eq!(removed, 0);
        assert_eq!(kubo.remote_pins_named(service, name).await, ["bafyold"]);
    }

    #[tokio::test]
    async fn replace_named_remote_pin_keeps_undated_record_holding_the_fresh_cid() {
        let kubo = crate::testkubo::MockKubo::start().await;
        let service = "pinata-lag-undated";
        let undated = "ma-runtime-joanna-root";
        let undated_in_flight = ma_core::in_flight_pin_name(undated);
        let name = "ma-runtime-joanna-root-2026-11-03";
        kubo.add_remote_pin(service, undated, "bafynew").await;
        kubo.add_remote_pin(service, &undated_in_flight, "bafyold")
            .await;
        kubo.lag_remote_adds();

        let removed = replace_named_remote_pin(kubo.url(), service, name, "bafynew", true)
            .await
            .unwrap();

        // The undated record holding the fresh CID is the fallback; the stale
        // in-flight record goes.
        assert_eq!(removed, 1);
        assert_eq!(kubo.remote_pins_named(service, undated).await, ["bafynew"]);
        assert_eq!(
            kubo.remote_pins_named(service, &undated_in_flight).await,
            Vec::<String>::new()
        );
    }

    #[tokio::test]
    async fn replace_named_remote_pin_does_not_readd_live_cid() {
        let kubo = crate::testkubo::MockKubo::start().await;
        let service = "pinata-count";
        let name = "ma-runtime-sky-did-2026-11-03";
        kubo.add_remote_pin(service, name, "bafylive").await;

        let removed = replace_named_remote_pin(kubo.url(), service, name, "bafylive", true)
            .await
            .unwrap();

        assert_eq!(removed, 0);
        assert_eq!(kubo.remote_add_count(), 0);
        assert_eq!(kubo.remote_pins_named(service, name).await, ["bafylive"]);
    }
}
