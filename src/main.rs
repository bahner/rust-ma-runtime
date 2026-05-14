use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::{anyhow, Context, Result};
use axum::extract::State;
use axum::response::{Html, IntoResponse};
use axum::routing::get;
use axum::Router;
use ciborium::Value as CborValue;
use cid::Cid;
use clap::Parser;
use libp2p_identity::ed25519::SecretKey as LibP2pEd25519Secret;
use libp2p_identity::{Keypair, PeerId};
use ma_core::config::{Config, MaArgs, SecretBundle};
use ma_core::ipfs::{validate_ipfs_publish_request, IpfsDidPublisher};
use ma_core::{
    Acl, Did, Document, EncryptionKey, ReplayGuard, SigningKey, VerificationMethod,
    IPFS_PROTOCOL,
};
use serde_json::json;
use tokio::sync::RwLock;
use tracing::{debug, error, info, warn};
use zeroize::Zeroize;

const MA_DEFAULT_SLUG: &str = "ma-ipfs-publisher";
const DEFAULT_ACL_YAML: &str = "acl:\n  - \"*\"\n";
const RPC_PROTOCOL_ID: &str = "/ma/rpc/0.0.1";
const CONTENT_TYPE_RPC: &str = "application/x-ma-rpc";
const CONTENT_TYPE_RPC_REPLY: &str = "application/x-ma-rpc-reply";
const PING_ATOM: &str = ":ping";
const PONG_ATOM: &str = ":pong";

#[derive(Debug, Parser)]
#[command(name = "ma-ipfs-publisher")]
#[command(about = "Lean /ma/ipfs/0.0.1 + /ma/rpc/0.0.1 daemon powered by ma-core")]
struct Cli {
    #[command(flatten)]
    ma: MaArgs,

    /// Optional ACL YAML file. Format: `acl: ["*", "did:ma:...", "!did:ma:..."]`
    #[arg(long)]
    acl_file: Option<PathBuf>,

    /// Poll interval in milliseconds.
    #[arg(long, default_value_t = 100)]
    poll_ms: u64,

    /// Status web server bind address.
    #[arg(long, default_value = "127.0.0.1:5003")]
    status_bind: SocketAddr,
}

/// Shared mutable state for the status endpoint.
#[derive(Default)]
struct Stats {
    our_did: String,
    endpoint_id: String,
    ipfs_requests: u64,
    rpc_requests: u64,
    pings_received: u64,
    started_at: u64,
}

type SharedStats = Arc<RwLock<Stats>>;

#[tokio::main]
#[allow(clippy::too_many_lines)]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    if cli.ma.gen_headless_config {
        Config::gen_headless(&cli.ma, MA_DEFAULT_SLUG)?;
        return Ok(());
    }

    let config = Config::from_args(&cli.ma, MA_DEFAULT_SLUG)?;
    config.init_logging()?;

    let acl = load_acl(cli.acl_file.as_deref())?;
    info!("ACL loaded");

    let secrets = load_secret_bundle(&config)?;

    // ── Derive our IPNS identity from ipns_secret_key via libp2p-identity ──
    let ipns_key_bytes = secrets.ipns_secret_key;
    let ipns = derive_ipns_from_secret(ipns_key_bytes)?;
    let derived_did = format!("did:ma:{ipns}");
    info!(did = %derived_did, "daemon identity derived from bundle key");

    // ── Build and sign our own DID document ──
    let our_document =
        build_and_publish_own_document(&derived_did, &secrets, &config.kubo_rpc_url, ipns_key_bytes)
            .await
            .context("failed to publish own DID document")?;
    let our_did = our_document.id.clone();
    info!(did = %our_document.id, "own DID document published");

    // ── iroh endpoint (uses iroh_secret_key, separate from IPNS) ──
    let mut endpoint = ma_core::new_ma_endpoint(secrets.iroh_secret_key).await?;

    let ipfs_protocol =
        std::str::from_utf8(IPFS_PROTOCOL).context("invalid IPFS protocol bytes")?;
    let ipfs_messages = endpoint.service(ipfs_protocol);
    let rpc_messages = endpoint.service(RPC_PROTOCOL_ID);

    info!(
        endpoint_id = %endpoint.id(),
        did = %our_did,
        "ma-ipfs-publisher online"
    );

    let publisher = IpfsDidPublisher::new(&config.kubo_rpc_url)
        .with_context(|| format!("invalid kubo_rpc_url: {}", config.kubo_rpc_url))?;
    publisher
        .wait_until_ready(10)
        .await
        .context("kubo RPC is not reachable")?;
    info!(kubo_rpc_url = %config.kubo_rpc_url, "kubo RPC ready");

    // ── Reconstruct signing key for sending pong replies ──
    let signing_key = build_signing_key(&ipns, secrets.did_signing_key)?;

    // ── Shared status state ──
    let stats = Arc::new(RwLock::new(Stats {
        our_did: our_did.clone(),
        endpoint_id: endpoint.id(),
        started_at: now_unix_secs(),
        ..Default::default()
    }));

    // ── Status web server ──
    spawn_status_server(stats.clone(), cli.status_bind);

    // ── Main event loop ──
    let mut replay_guard = ReplayGuard::default();
    let mut ticker = tokio::time::interval(Duration::from_millis(cli.poll_ms));

    loop {
        tokio::select! {
            _ = ticker.tick() => {
                let now = now_unix_secs();

                // Drain /ma/rpc/0.0.1
                while let Some(mut message) = rpc_messages.pop(now) {
                    debug!(
                        node = %message.from,
                        protocol = RPC_PROTOCOL_ID,
                        "node connected to protocol"
                    );
                    debug!(
                        from = %message.from,
                        to = %message.to,
                        id = %message.id,
                        content_type = %message.content_type,
                        content = %preview_ma_message_content(&message.content),
                        "received encrypted ma-msg on /ma/rpc/0.0.1"
                    );
                    {
                        let mut s = stats.write().await;
                        s.rpc_requests += 1;
                    }
                    if let Err(err) = handle_rpc_message(
                        &message,
                        &acl,
                        &our_did,
                        &signing_key,
                        &*endpoint,
                        &config.kubo_rpc_url,
                        stats.clone(),
                    ).await {
                        warn!(error = %err, from = %message.from, "rpc message rejected");
                    }
                    message.content.zeroize();
                    message.signature.zeroize();
                }

                // Drain /ma/ipfs/0.0.1
                while let Some(mut message) = ipfs_messages.pop(now) {
                    debug!(
                        node = %message.from,
                        protocol = ipfs_protocol,
                        "node connected to protocol"
                    );
                    debug!(
                        from = %message.from,
                        to = %message.to,
                        id = %message.id,
                        content_type = %message.content_type,
                        content = %preview_ma_message_content(&message.content),
                        "received encrypted ma-msg on /ma/ipfs/0.0.1"
                    );
                    {
                        let mut s = stats.write().await;
                        s.ipfs_requests += 1;
                    }
                    if let Err(err) = handle_ipfs_message(
                        &message,
                        &acl,
                        &publisher,
                        &mut replay_guard,
                    ).await {
                        warn!(error = %err, from = %message.from, "ipfs message rejected");
                    }
                    message.content.zeroize();
                    message.signature.zeroize();
                }
            }
            signal = tokio::signal::ctrl_c() => {
                if let Err(err) = signal {
                    error!(error = %err, "ctrl-c handler failed");
                }
                info!("shutdown requested");
                break;
            }
        }
    }

    Ok(())
}

// ── Build and publish own DID document ──────────────────────────────────────

async fn build_and_publish_own_document(
    our_did: &str,
    secrets: &SecretBundle,
    kubo_rpc_url: &str,
    mut ipns_key_bytes: [u8; 32],
) -> Result<Document> {
    fn build_document_for_did(our_did: &str, secrets: &SecretBundle) -> Result<(Document, Vec<u8>)> {
        let did = Did::try_from(our_did).context("invalid own DID")?;

        let sign_did = Did::new_url(&did.ipns, None::<&str>).context("sign did")?;
        let enc_did = Did::new_url(&did.ipns, None::<&str>).context("enc did")?;

        let signing_key = SigningKey::from_private_key_bytes(sign_did, secrets.did_signing_key)
            .context("invalid did_signing_key")?;
        let encryption_key =
            EncryptionKey::from_private_key_bytes(enc_did, secrets.did_encryption_key)
                .context("invalid did_encryption_key")?;

        let mut document = Document::new(&did, &did);

        let assertion_vm = VerificationMethod::new(
            did.base_id(),
            did.base_id(),
            signing_key.key_type.clone(),
            signing_key.did.fragment.as_deref().unwrap_or("signing"),
            signing_key.public_key_multibase.clone(),
        )
        .context("assertion verification method")?;

        let key_agreement_vm = VerificationMethod::new(
            did.base_id(),
            did.base_id(),
            encryption_key.key_type.clone(),
            encryption_key
                .did
                .fragment
                .as_deref()
                .unwrap_or("encryption"),
            encryption_key.public_key_multibase.clone(),
        )
        .context("key agreement verification method")?;

        let assertion_vm_id = assertion_vm.id.clone();
        let key_agreement_vm_id = key_agreement_vm.id.clone();
        document
            .add_verification_method(assertion_vm.clone())
            .context("add assertion vm")?;
        document
            .add_verification_method(key_agreement_vm)
            .context("add key agreement vm")?;
        document.assertion_method = vec![assertion_vm_id];
        document.key_agreement = vec![key_agreement_vm_id];
        document
            .sign(&signing_key, &assertion_vm)
            .context("sign document")?;

        let doc_cbor = document.to_cbor().context("marshal own document as dag-cbor")?;
        Ok((document, doc_cbor))
    }

    let (document, doc_cbor) = build_document_for_did(our_did, secrets)?;

    // Kubo key/import expects a libp2p protobuf-encoded private key, not raw 32-byte seed.
    let lp2p_secret = LibP2pEd25519Secret::try_from_bytes(&mut ipns_key_bytes)
        .context("invalid ipns_secret_key bytes for Kubo import")?;
    let keypair = Keypair::from(libp2p_identity::ed25519::Keypair::from(lp2p_secret));
    let mut ipns_private_key_protobuf = keypair
        .to_protobuf_encoding()
        .context("failed to protobuf-encode ipns private key")?;

    let publisher = IpfsDidPublisher::new(kubo_rpc_url)?;
    publisher.wait_until_ready(10).await?;
    publisher
        .publish_document(&doc_cbor, &ipns_private_key_protobuf)
        .await
        .context("kubo publish failed for own DID document")?;

    ipns_private_key_protobuf.zeroize();
    ipns_key_bytes.zeroize();

    Ok(document)
}

fn load_secret_bundle(config: &Config) -> Result<SecretBundle> {
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

fn derive_ipns_from_secret(ipns_key_bytes: [u8; 32]) -> Result<String> {
    let mut ipns_key_for_identity = ipns_key_bytes;
    let lp2p_secret = LibP2pEd25519Secret::try_from_bytes(&mut ipns_key_for_identity)
        .context("invalid ipns_secret_key bytes")?;
    let keypair = Keypair::from(libp2p_identity::ed25519::Keypair::from(lp2p_secret));
    let peer_id: PeerId = keypair.public().to_peer_id();

    peer_id_to_ipns_base36(peer_id).context("failed to derive canonical IPNS id")
}

fn build_signing_key(ipns: &str, did_signing_key: [u8; 32]) -> Result<SigningKey> {
    let sign_did = Did::new_url(ipns, None::<&str>).context("invalid ipns for signing Did")?;
    SigningKey::from_private_key_bytes(sign_did, did_signing_key)
        .context("invalid did_signing_key")
}

fn spawn_status_server(stats: SharedStats, status_bind: SocketAddr) {
    let status_router = Router::new()
        .route("/", get(handle_index))
        .route("/status.json", get(handle_status_json))
        .with_state(stats);

    tokio::spawn(async move {
        let listener = tokio::net::TcpListener::bind(status_bind)
            .await
            .expect("status server bind failed");
        info!(bind = %status_bind, "status server listening");
        axum::serve(listener, status_router)
            .await
            .expect("status server failed");
    });
}

// ── RPC handler: respond to :ping with :pong ────────────────────────────────

async fn handle_rpc_message(
    message: &ma_core::Message,
    acl: &Acl,
    our_did: &str,
    signing_key: &SigningKey,
    endpoint: &dyn ma_core::MaEndpoint,
    kubo_rpc_url: &str,
    stats: SharedStats,
) -> Result<()> {
    acl_check(acl, &message.from)?;

    if message.content_type != CONTENT_TYPE_RPC {
        return Err(anyhow!(
            "unsupported RPC content type '{}' on {}",
            message.content_type,
            RPC_PROTOCOL_ID,
        ));
    }

    let term: CborValue = ciborium::de::from_reader(message.content.as_slice())
        .context("invalid CBOR in RPC message")?;

    if !matches!(&term, CborValue::Text(s) if s == PING_ATOM) {
        return Ok(());
    }

    {
        let mut s = stats.write().await;
        s.pings_received += 1;
    }
    info!(from = %message.from, "received :ping");

    let mut pong_bytes = Vec::new();
    ciborium::ser::into_writer(&CborValue::Text(PONG_ATOM.to_string()), &mut pong_bytes)
        .context("failed to encode :pong")?;

    let sender = Did::try_from(message.from.as_str())
        .with_context(|| format!("invalid sender DID: {}", message.from))?;
    let ping_did_url = format!("did:ma:{}#ping", sender.ipns);

    let mut reply = ma_core::Message::new(
        our_did,
        &ping_did_url,
        CONTENT_TYPE_RPC_REPLY,
        pong_bytes,
        signing_key,
    )
    .context("failed to build pong message")?;
    reply.reply_to = Some(message.id.clone());

    let resolver = ma_core::IpfsGatewayResolver::new(kubo_rpc_url.to_string());
    match endpoint
        .outbox(&resolver, &sender.base_id(), RPC_PROTOCOL_ID)
        .await
    {
        Ok(mut outbox) => {
            outbox.send(&reply).await.context("pong send failed")?;
            info!(to = %ping_did_url, "sent :pong");
        }
        Err(err) => {
            warn!(error = %err, to = %ping_did_url, "could not resolve sender to deliver :pong");
        }
    }

    Ok(())
}

// ── IPFS handler ─────────────────────────────────────────────────────────────

async fn handle_ipfs_message(
    message: &ma_core::Message,
    acl: &Acl,
    publisher: &IpfsDidPublisher,
    replay_guard: &mut ReplayGuard,
) -> Result<()> {
    let headers = message.headers();
    replay_guard
        .check_and_insert(&headers)
        .context("replay or invalid headers")?;

    acl_check(acl, &message.from)?;

    let mut message_cbor = message
        .to_cbor()
        .context("failed to encode message to CBOR")?;
    let mut validated =
        validate_ipfs_publish_request(&message_cbor).context("invalid /ma/ipfs request")?;

    publisher
        .publish_document(
            &validated.request.did_document,
            &validated.request.ipns_private_key,
        )
        .await
        .context("kubo publish failed")?;

    info!(
        from = %message.from,
        did = %validated.document_did.id(),
        "published DID document via /ma/ipfs/0.0.1"
    );

    validated.request.ipns_private_key.zeroize();
    validated.request.did_document.zeroize();
    message_cbor.zeroize();

    Ok(())
}

// ── ACL check ────────────────────────────────────────────────────────────────

fn acl_check(acl: &Acl, from: &str) -> Result<()> {
    let sender = Did::try_from(from).with_context(|| format!("invalid sender DID '{from}'"))?;
    if !acl.is_allowed(&sender.id()) || !acl.is_allowed(&sender.base_id()) {
        return Err(anyhow!("sender denied by ACL: {from}"));
    }
    Ok(())
}

// ── Status web server handlers ────────────────────────────────────────────────

async fn handle_index(State(stats): State<SharedStats>) -> impl IntoResponse {
    let (our_did, endpoint_id, ipfs_requests, rpc_requests, pings_received, uptime) = {
        let s = stats.read().await;
        (
            s.our_did.clone(),
            s.endpoint_id.clone(),
            s.ipfs_requests,
            s.rpc_requests,
            s.pings_received,
            now_unix_secs().saturating_sub(s.started_at),
        )
    };
    let html = format!(r#"<!DOCTYPE html>
<html lang="en">
<head><meta charset="utf-8"><title>ma-ipfs-publisher</title>
<style>body{{font-family:monospace;max-width:700px;margin:2em auto;background:#111;color:#eee}}
h1{{color:#7cf}}table{{border-collapse:collapse;width:100%}}
td,th{{padding:6px 12px;border:1px solid #333;text-align:left}}
th{{background:#222}}a{{color:#7cf}}</style></head>
<body>
<h1>間 IPFS Publisher</h1>
<table>
<tr><th>Field</th><th>Value</th></tr>
<tr><td>DID</td><td>{our_did}</td></tr>
<tr><td>Endpoint ID (iroh)</td><td>{endpoint_id}</td></tr>
<tr><td>Uptime (seconds)</td><td>{uptime}</td></tr>
<tr><td>IPFS publish requests</td><td>{ipfs_requests}</td></tr>
<tr><td>RPC requests</td><td>{rpc_requests}</td></tr>
<tr><td>Pings received</td><td>{pings_received}</td></tr>
</table>
<p><a href="/status.json">status.json</a></p>
</body></html>"#);
    Html(html)
}

async fn handle_status_json(State(stats): State<SharedStats>) -> impl IntoResponse {
    let (our_did, endpoint_id, ipfs_requests, rpc_requests, pings_received, started_at, uptime) = {
        let s = stats.read().await;
        (
            s.our_did.clone(),
            s.endpoint_id.clone(),
            s.ipfs_requests,
            s.rpc_requests,
            s.pings_received,
            s.started_at,
            now_unix_secs().saturating_sub(s.started_at),
        )
    };
    let body = json!({
        "did": our_did,
        "endpoint_id": endpoint_id,
        "uptime_secs": uptime,
        "ipfs_requests": ipfs_requests,
        "rpc_requests": rpc_requests,
        "pings_received": pings_received,
        "started_at": started_at,
    });
    (
        [(axum::http::header::CONTENT_TYPE, "application/json")],
        body.to_string(),
    )
}

// ── Helpers ───────────────────────────────────────────────────────────────────

fn load_acl(path: Option<&std::path::Path>) -> Result<Acl> {
    let yaml = if let Some(path) = path {
        std::fs::read_to_string(path)
            .with_context(|| format!("failed to read ACL file {}", path.display()))?
    } else {
        DEFAULT_ACL_YAML.to_string()
    };
    Acl::new_from_yaml(&yaml).context("invalid ACL YAML")
}

fn now_unix_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
    .map_or(0, |d| d.as_secs())
}

fn preview_ma_message_content(content: &[u8]) -> String {
    const MAX_PREVIEW_CHARS: usize = 256;

    let mut preview = String::from_utf8_lossy(content).into_owned();
    if preview.chars().count() > MAX_PREVIEW_CHARS {
        preview = preview.chars().take(MAX_PREVIEW_CHARS).collect::<String>() + "...";
    }

    // Keep logs one-line and readable even for binary-ish payloads.
    preview.replace('\n', "\\n").replace('\r', "\\r")
}

fn peer_id_to_ipns_base36(peer_id: PeerId) -> Result<String> {
    // IPNS peer names are CIDv1 with codec=libp2p-key (0x72), encoded base36 (k...).
    let mh = cid::multihash::Multihash::<64>::from_bytes(peer_id.as_ref().to_bytes().as_slice())
        .context("invalid peer multihash")?;
    let cid = Cid::new_v1(0x72, mh);
    cid.to_string_of_base(cid::multibase::Base::Base36Lower)
        .map_err(|e| anyhow!("failed to encode IPNS cid in base36: {e}"))
}
