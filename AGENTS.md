# 間 IPFS Publisher

A lean daemon that exposes `/ma/ipfs/0.0.1` and `/ma/rpc/0.0.1` on behalf of
clients that cannot reach the Kubo RPC API directly (e.g. browser-based 間
actors). It runs on a host with a Kubo daemon, derives its own `did:ma` identity
at startup, publishes its own DID document, then handles two services over iroh
QUIC transport:

- **`/ma/ipfs/0.0.1`** — receives signed IPFS-publish requests and publishes
  `did:ma` DID documents to IPFS/IPNS via Kubo on behalf of the caller.
- **`/ma/rpc/0.0.1`** — receives RPC messages; responds to `:ping` atoms with
  `:pong` replies using the `/ma/rpc/0.0.1` transport.

A minimal status HTTP server runs on `127.0.0.1:5003` (configurable).

## Design principles

- **Two services, nothing more.** Only `/ma/ipfs/0.0.1` and `/ma/rpc/0.0.1`
  are registered. No gossip, no additional RPC.
- **No local protocol code.** All publish logic, validation, secret-bundle
  handling, config, ACL, and transport are provided by the `ma-core` crate.
  Local code is nothing but glue.
- **Keys in memory only.** IPNS private key material arriving in a request is
  used once and immediately zeroized (`zeroize`). The daemon's own keys live in
  an encrypted `SecretBundle` on disk, decrypted into memory at startup and
  never written out again. The ipns key bytes are also zeroized after the own
  DID document is published.
- **Own identity published at startup.** The daemon derives its `did:ma` from
  `ipns_secret_key` via `libp2p-identity`, builds a signed `Document` with
  `did_signing_key` and `did_encryption_key`, and publishes it to Kubo before
  accepting any connections.
- **Strict input validation.** Every incoming CBOR envelope on `/ma/ipfs/0.0.1`
  is validated by `validate_ipfs_publish_request`, which parses the signed
  message, checks content-type, validates and verifies the DID document
  (including its proof signature), and asserts that the sender's IPNS identity
  matches the document's DID.
- **Replay protection.** A `ReplayGuard` (sliding 120-second window) is applied
  to `/ma/ipfs/0.0.1` messages before any processing.
- **ACL with deny-wins semantics.** Deny rules override any allow, including
  `*`. Both the full DID-URL and bare identity are checked. An identity-level
  deny (`!did:ma:…`) blocks all DID-URLs under that identity automatically.

## Dependencies

Only published crates — **never local paths**:

```toml
anyhow = "1"
axum = { version = "0.7", default-features = false, features = ["http1", "tokio"] }
ciborium = "0.2"
clap = { version = "4", features = ["derive"] }
libp2p-identity = { version = "0.2", features = ["ed25519", "peerid"] }
ma-core = { version = "0.9.1", default-features = false, features = ["config", "kubo", "iroh", "acl"] }
serde_json = "1"
tokio = { version = "1", features = ["macros", "rt-multi-thread", "signal", "time", "sync"] }
tracing = "0.1"
zeroize = "1"
```

`ma-core 0.9.1` exposes everything this daemon uses for DID handling, so no
direct `ma-did` dependency is required.

> **Development note:** A `[patch.crates-io] ma-core = { path = "../rust-ma-core" }`
> is active in `Cargo.toml` while new `ma-core` features (`ValidatedIpfsRequest`,
> `ValidatedIpfsStore`, `ipfs_add`) are not yet in a published release.
> Remove the patch when a new version is published.

## Configuration

The default slug is `ma-ipfs-publisher`. Config, secret bundle, and log file
follow XDG paths via ma-core:

| File | Default path |
|------|--------------|
| Config | `$XDG_CONFIG_HOME/ma/ma-ipfs-publisher.yaml` |
| Secret bundle | `$XDG_CONFIG_HOME/ma/ma-ipfs-publisher.bin` |
| Log | `$XDG_DATA_HOME/ma/ma-ipfs-publisher.log` |

`secret_bundle_passphrase` must be set (env `MA_MA_IPFS_PUBLISHER_SECRET_BUNDLE_PASSPHRASE`,
or `MA_SECRET_BUNDLE_PASSPHRASE`, or in the YAML config).

`kubo_rpc_url` defaults to `http://127.0.0.1:5001`.

### First-time setup

```sh
ma-ipfs-publisher --gen-headless-config
```

Generates a fresh `SecretBundle` with four random 32-byte keys (`iroh_secret_key`,
`ipns_secret_key`, `did_signing_key`, `did_encryption_key`), encrypts it with a
random passphrase, and writes both config and bundle to the XDG paths with mode
`0600`.

### Runtime

```sh
ma-ipfs-publisher
# or with an explicit ACL file:
ma-ipfs-publisher --acl-file /etc/ma/acl.yaml
# or with a custom status bind address:
ma-ipfs-publisher --status-bind 0.0.0.0:5003
```

## CLI flags

| Flag | Default | Description |
|------|---------|-------------|
| `--acl-file <PATH>` | — | ACL YAML file; defaults to open (`*`) if omitted |
| `--poll-ms <MS>` | `100` | Service poll interval |
| `--status-bind <ADDR>` | `127.0.0.1:5003` | Status web server bind address |
| `--gen-headless-config` | — | Generate config + secret bundle and exit |

## ACL format

The ACL YAML must contain an `acl:` sequence. The default when no file is
supplied is open (`*`).

```yaml
acl:
  - "*"                  # public access
  - "!did:ma:<bad>"      # deny this identity and all its DID-URLs
  - "!did:ma:<worse>"
```

Rules:

- **Deny always wins** over allow, including over `*`.
- An identity-level deny (`!did:ma:<ipns>` with no fragment) blocks every
  DID-URL under that identity (any fragment, any path).
- Entries are validated by `Did::try_from` (via `ma-core`) at load time —
  invalid entries cause a hard error.
- ACL is checked on `/ma/ipfs/0.0.1` and `/ma/rpc/0.0.1` messages.

## Status web server

Runs on `127.0.0.1:5003` (or `--status-bind`). Two endpoints:

| Endpoint | Content | Description |
|----------|---------|-------------|
| `GET /` | `text/html` | Human-friendly status page |
| `GET /status.json` | `application/json` | Compact JSON status object |

The JSON object contains:

```json
{
  "did": "did:ma:<ipns>",
  "endpoint_id": "<iroh-id>",
  "uptime_secs": 42,
  "ipfs_requests": 0,
  "rpc_requests": 0,
  "pings_received": 0,
  "started_at": 1234567890
}
```

## RPC protocol

Content types are defined in ma-spec, not ma-core — they are string literals:

| Direction | Content-Type |
|-----------|--------------|
| Request | `application/x-ma-rpc` |
| Reply | `application/x-ma-rpc-reply` |

RPC atoms are CBOR-encoded text strings beginning with `:`.

The daemon handles exactly one RPC verb:

- **`:ping`** — accepted only on `/ma/rpc/0.0.1`; replies with `:pong` to
  `did:ma:<sender_ipns>#ping`. The reply message sets `reply_to` to the
  originating message's ID. The reply is delivered via `endpoint.outbox()`
  using the sender's resolved RPC endpoint.

## ma-core API used

| Purpose | Call |
|---------|------|
| Config + CLI | `Config::from_args(&args, MA_DEFAULT_SLUG)` |
| First-time config | `Config::gen_headless(&args, MA_DEFAULT_SLUG)` |
| Key material | `SecretBundle::load(path, passphrase)` |
| IPNS derivation | `libp2p_identity::ed25519::SecretKey::try_from_bytes` → `Keypair` → `PeerId::to_base58()` |
| Own DID document | `Document::new`, `SigningKey::from_private_key_bytes`, `EncryptionKey::from_private_key_bytes`, `VerificationMethod::new`, `document.sign`, `document.marshal` |
| iroh endpoint | `ma_core::new_ma_endpoint(iroh_secret_key)` |
| Register service | `endpoint.service("/ma/ipfs/0.0.1")` + `endpoint.service("/ma/rpc/0.0.1")` |
| Kubo publisher | `IpfsDidPublisher::new(kubo_rpc_url)` |
| Kubo readiness | `publisher.wait_until_ready(attempts)` |
| Request validation | `validate_ipfs_publish_request(message_cbor)` |
| Publish | `publisher.publish_document(did_doc_json, ipns_key_b64)` |
| Replay guard | `ReplayGuard::default()` + `check_and_insert(&headers)` |
| ACL | `Acl::new_from_yaml(yaml)` + `acl.is_allowed(did_url)` |
| Outbox (pong) | `endpoint.outbox(&resolver, &sender_did, "/ma/rpc/0.0.1").await` → `outbox.send(&msg)` |
| Resolver | `IpfsGatewayResolver::new(kubo_rpc_url)` |

## Security notes

- `application/x-ma-ipfs-request` payloads **must** be encrypted envelopes per
  the ma-spec (messaging-format.md §2.2.1). The iroh transport provides the
  encrypted channel; `validate_ipfs_publish_request` enforces content-type.
- The IPNS private key embedded in each `/ma/ipfs/0.0.1` request is the
  sender's full publishing authority over their DID. It is used once and
  zeroized immediately after the Kubo call.
- The daemon's own `ipns_secret_key` bytes are zeroized immediately after the
  own DID document is published at startup.
- The daemon carries no signing or encryption keys of its own beyond those
  needed for transport and its own DID identity — it cannot impersonate any
  other `did:ma` identity.
- All files written by ma-core (config, bundle) use mode `0600`.
- The `iroh_secret_key` is only for the iroh QUIC transport layer; it is
  distinct from `ipns_secret_key` which roots the `did:ma` identity.
