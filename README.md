# 間 Runtime (`ma`)

A lean Tokio daemon that bridges browser-based `did:ma:` identities to Kubo
(IPFS) and manages a Wasm entity/plugin system over iroh QUIC transport.

---

## Design philosophy

間 is built around one idea: **secure messaging and actor interaction should
be fast and lightweight**, not just correct.

The cryptographic primitive choices follow the same school of thought as
[WireGuard](https://www.wireguard.com/) — pick the fastest algorithms that are still
thoroughly secure, keep the code small, and avoid complexity:

| Concern | 間 choice | Common alternative |
|---------|-----------|-------------------|
| Serialisation | CBOR | JSON |
| Hashing | BLAKE3 | SHA-2 |
| Key agreement | X25519 | RSA / DH |
| Signatures | Ed25519 | RSA / ECDSA |
| Transport | iroh QUIC | HTTP(S) |

Once DID documents are published to IPFS and resolved by both sides, 間
message exchange is significantly leaner than a comparable
[DIDComm](https://identity.foundation/didcomm-messaging/spec/)
implementation: no JSON parsing overhead, no unnecessary round trips.

間 follows **DIDComm principles** — sender-authenticated, end-to-end
encrypted messages rooted in [W3C DID](https://www.w3.org/TR/did-core/)
identities — without implementing the DIDComm standard itself.  The
security model is equivalent; only the wire format (CBOR + iroh QUIC
instead of JSON + HTTPS) and performance profile differ.

**Why Rust?**  The transport row is the deciding factor.  [iroh](https://iroh.computer/)
is an IPFS spinoff — a leaner, faster successor to the original libp2p QUIC
stack — and time has shown it to be the right substrate for this kind of
peer-to-peer work.  At the moment iroh is only available as a Rust crate, so
the language choice is not ideological: it is simply the price of admission
for the best transport available.

`ma` has not been independently audited.  The cryptographic design is
sound, but treat it as a well-engineered toy until an audit says otherwise.

---

## Overview

`ma` exposes three iroh QUIC services on behalf of clients that cannot reach the
Kubo RPC API directly (e.g. the `zion` browser actor):

| Service | Protocol ID | Purpose |
|---------|-------------|---------|
| IPFS publisher | `/ma/ipfs/0.0.1` | Receives signed DID-document publish requests and forwards them to Kubo |
| RPC | `/ma/rpc/0.0.1` | Entity/plugin dispatch, ping/pong |
| CRUD | `/ma/crud/0.0.1` | Structured data management (entities, kinds, config, namespaces) |

The daemon derives its own `did:ma:` identity at startup, publishes its own DID
document to IPFS/IPNS, and then enters the main event loop.

---

## Architecture

```
zion (browser WASM) ──iroh QUIC──► /ma/rpc/0.0.1
                                       │
                                       ├─ (unfragmented) → ping
                                       └─ #<name>        → Wasm plugin dispatch
                   ──iroh QUIC──► /ma/crud/0.0.1
                                       │
                                       ├─ :entities.*    → entity management
                                       ├─ :kinds.*       → kind registry
                                       ├─ :config.*      → runtime config
                                       └─ :namespaces.*  → namespace management
                   ──iroh QUIC──► /ma/ipfs/0.0.1
                                       │
                                       └─ publish DID document to Kubo/IPNS
```

Entity Wasm plugins are loaded from IPFS at startup.  State is persisted back
to IPFS on graceful shutdown.

---

## Prerequisites

- Rust (latest stable)
- [Kubo](https://docs.ipfs.tech/install/command-line/) running on
  `http://127.0.0.1:5001` (or set `kubo_rpc_url` in config)

---

## Building

```sh
cargo build --release
```

Binary: `target/release/ma`.

---

## First-time setup

### 1. Generate config and secret bundle

On the very first start `ma` detects a missing secret bundle and generates a
full headless config automatically — no manual step required. You can also
trigger generation explicitly:

```sh
ma --gen-headless-config
```

Writes four random 32-byte keys (plus a `runtime_ipns` key) encrypted with a
random passphrase to:

| File | Default path |
|------|-------------|
| Config | `$XDG_CONFIG_HOME/ma/ma.yaml` |
| Secret bundle | `$XDG_CONFIG_HOME/ma/ma.bin` |

The passphrase is printed once and also written into `ma.yaml` automatically,
so `ma` can restart without any manual input.  Store it in a password manager
as a backup.  If you prefer not to keep it in the config file, remove the
`secret_bundle_passphrase:` line and supply it via `MA_SECRET_BUNDLE_PASSPHRASE`.

### 2. Minimal runtime (no bootstrap needed)

On first start, if no `root_cid` is found in IPNS and none is given via
`--root-cid`, the daemon automatically publishes a minimal `RuntimeManifest`
to Kubo and uses it as the runtime head. This manifest contains:

- `config.owners` — empty (no owner yet)
- `acl` — absent (deny-all for all incoming transport until claimed)
- `entities`, `kinds`, `i18n` — all empty

The daemon is immediately functional for CRUD operations. To establish
ownership, run `.my.ma:claim` from zion (or `POST /claim` with your DID).
This updates the live transport ACL so you can issue RPC/CRUD requests.

If claiming remotely is not practical, set owners directly in `config.yaml`
or via `--owner` (repeatable) before starting the daemon:

```yaml
# ~/.config/ma/ma.yaml
owners:
  - did:ma:<your-ipns>
  - did:ma:<another-ipns>
```

```sh
ma --owner did:ma:<your-ipns> --owner did:ma:<another-ipns>
```

Owners listed this way are granted `["*"]` in the live transport ACL at
startup, before any manifest ACL is loaded.

To add a permanent ACL to the manifest:

```
@ma:acl: <cid>   # set an AclMap CID as the root transport-gate ACL
```

### 3. Bootstrap entities and locales (optional)

Run once after Kubo is ready to set up the entity system:

```sh
ma --gen-root-cid bootstrap.example.yaml
```

This publishes kind nodes, entity nodes, and the root `RuntimeManifest` as
IPLD DAG-CBOR objects to Kubo, then prints the generated `root_cid` and exits.

Locale files from `i18n/` are embedded in the binary at build time via
`src/i18n.yaml` (generated by `make src/i18n.yaml`). To rebuild the locale
map after editing any `.ftl` file:

```sh
make src/i18n.yaml
make release
```

Recommended runtime model:
- Runtime head CID is read from IPNS at startup.
- `--root-cid <cid>` overrides IPNS for the current process.

Warning for `--root-cid`:
- It immediately resets runtime head for this run.
- If you pass the wrong CID, retrieve the previous CID from runtime logs and restart with that value.

On subsequent starts, the daemon restores runtime head from IPNS automatically.

#### `bootstrap.yaml` format

```yaml
runtime:
  owner: did:ma:<your-ipns>   # DID of the operator; shown by `ma --status`

  # Kinds: protocol families → implementations → metadata
  kinds:
    stateless:
      ping:
        protocol: /ma/stateless/ping/0.0.1
        wasi: false             # Rust/native Extism plugin
        api:
          - handle_cast         # fn handle_cast() — process incoming message
        host_functions:
          - ma_send             # full-control outbound envelope
      python:
        protocol: /ma/stateless/python/0.0.1
        wasi: true              # Python/WASI plugin
        api:
          - handle_cast
        host_functions:
          - ma_reply            # reply to sender
          - ma_send
    stateful:
      python:
        protocol: /ma/stateful/python/0.0.1
        wasi: true
        api:
          - init                # called on load with persisted state bytes
          - handle_call
          - save_state          # janitor snapshot trigger
        host_functions:
          - ma_reply
          - ma_send
          - ma_set_state        # persist state to IPFS via runtime

  # Entities: named plugin instances
  entities:
    ping:
      kind: /ma/stateless/ping/0.0.1
      behavior_cid: bafkrei…   # CID of compiled .wasm — `ipfs add plugin.wasm`
      acl: ""                  # "" = deny-all; use a named ACL ref for open access
    fortune:
      kind: /ma/stateless/python/0.0.1
      behavior_cid: Qm…
      acl: ""
      # owner: did:ma:<other>  # optional; falls back to runtime.owner
```

---

## Running

```sh
ma
# or
ma --acl-file /etc/ma/acl.yaml --status-bind 0.0.0.0:5003
```

---

## CLI flags

| Flag | Default | Description |
|------|---------|-------------|
| `--gen-root-cid <YAML>` | — | Publish bootstrap manifest to IPFS, print `root_cid`, then exit |
| `--root-cid <CID>` | — | WARNING: resets runtime head for this process; recover prior CID from logs if mistaken |
| `--owner <DID>` | — | DID(s) of the runtime owner(s); repeat for multiple; falls back to `owners:` in config |
| `--acl-file <PATH>` | — | ACL YAML; open (`*`) if omitted |
| `--poll-ms <MS>` | `100` | Service poll interval |
| `--i18n <LANG>` | — | Log language (e.g. `nb`, `en`). Also `MA_I18N` env var. Falls back to `i18n:` in config or `nb`. |
| `--status-bind <ADDR>` | `127.0.0.1:5003` | Status HTTP server address |
| `--gen-headless-config` | — | Generate config + bundle, then exit |

---

## Startup sequence

1. Parse CLI, load config.
2. If `--gen-headless-config`: write config + bundle (including `runtime_ipns` key), exit.
3. If secret bundle is missing: auto-generate headless config and continue.
4. If `--gen-root-cid <yaml>`: wait for Kubo, publish IPLD manifest, print `root_cid`, exit.
5. Load ACL (deny-all until manifest is loaded later).
6. Load `SecretBundle`.
7. Create iroh QUIC endpoint, register services (RPC, IPFS publisher if enabled, CRUD if enabled).
8. Build own DID document, spawn background publish to IPNS.
9. Wait for Kubo readiness (10 attempts).
10. Resolve `root_cid` from `--root-cid` CLI or runtime IPNS. If none found, publish a
    minimal empty `RuntimeManifest` to Kubo and use that CID as the runtime head.
11. Fetch locale from `RuntimeManifest.config.i18n`; fall back to embedded FTL if unavailable.
12. Load Wasm entity plugins from IPFS manifest.
13. Start `#scheduler` native actor.
14. Load named ACLs into cache from manifest; replace transport-gate ACL from manifest ACL.
15. Resolve owners from CLI `--owner` and `config.extra.owners`; seed live ACL.
16. Start status HTTP server.
17. Main event loop: drain RPC, IPFS, and CRUD inboxes every `poll_ms`.
18. On `Ctrl-C`: save entity states to IPFS, update manifest CID in config, close endpoint.

---

## Entity / plugin system

### Concept

All entities are created and managed via CRUD messages to the runtime.
Entity behaviour lives in Wasm blobs stored on IPFS.

### IPLD schema (DAG-CBOR on Kubo)

```
RuntimeManifest { owner, kinds: {family→implementation→CID}, entities: {name→CID→EntityNode}, acl: CID?, acls: {name→CID} }
KindNode        { protocol, api: [String] }
EntityNode      { name, kind, behavior: {"/":CID}, state: {"/":CID}?, owner, acl: {"/":CID}, wasi: bool }
```

### Management CRUD — dot-path protocol

Send CBOR to the **bare** runtime DID (`did:ma:<runtime>`, no fragment) on
`/ma/crud/0.0.1`. Operations are encoded in the CBOR payload:

```
GET:    [":get",    ".ns.key"]
SET:    [".ns.key", value]
DELETE: [":delete", ".ns.key"]
```

#### Entities

| Path | GET | SET | DELETE |
|------|-----|-----|--------|
| `:entities` | List all entity names | — | — |
| `:entities.<name>` | Get EntityNode | — | — |
| `:entities.<name>` (SET) | — | Create/update entity by CID | — |
| `:entities.<name>` (DELETE) | — | — | Delete entity |

#### Kinds

| Path | GET | SET | DELETE |
|------|-----|-----|--------|
| `:kinds` | List all kinds | — | — |
| `:kinds.<family>` | List implementations | — | — |
| `:kinds.<family>.<impl>` | Get kind ref | Set kind ref CID | Delete kind ref |

#### Config

| Path | GET | SET | DELETE |
|------|-----|-----|--------|
| `:config` | Get entire config map | — | — |
| `:config.<key>` | Get value | Set value | Delete key |

#### Ping (RPC)

Send `:ping` as a CBOR atom on `/ma/rpc/0.0.1` to any entity fragment
(`did:ma:<runtime>#ping`) — the entity plugin handles it.

#### From zion terminal

```
# list entities
@sky:entities

# get entity
@sky:entities.ping

# create / update entity (CID is the EntityNode dag-cbor CID)
@sky:entities.ping: bafyreiXXXXXXXX

# delete entity
@sky:entities.ping:

# set a kind
@sky:kinds.ma-entity.wasi: bafyreiYYYYYYYY

# set a config key
@sky:config.poll_interval_ms: 250
```

### Plugin ABI

Each Wasm plugin must export:

| Export | Description |
|--------|-------------|
| `init(state: Bytes)` | Called at load with persisted state (empty bytes on first load) |
| `handle_message(PluginInput JSON) → PluginOutput JSON` | Handle an incoming RPC message |
| `get_state() → Bytes` | Return current state for IPFS persistence |

**`PluginInput`:**
```json
{
  "msg": {
    "id": "…", "from": "did:ma:…", "to": "did:ma:…#name",
    "created_at": 1234567890123456789,
    "expires": 1234567890,
    "reply_to": null,
    "content_type": "application/cbor",
    "content": [/* bytes */]
  },
  "ctx": { "self": "did:ma:…#name", "owner": "did:ma:…" }
}
```

**`PluginOutput`:**
```json
{ "reply": { "content_type": "application/json", "content": [/* bytes */] } }
```
`"reply": null` for no reply.

---

## ACL

Capabilities are plain strings in YAML sequences. The default when no
`--acl-file` is given allows everyone to use both services.

```yaml
acl:
  "*": [rpc, ipfs]            # everyone: RPC + IPFS publish (default)
  "did:ma:alice": [owner]     # alice: full access
  "did:ma:bob": [rpc]         # bob: RPC only, no IPFS publish
  "did:ma:eve":               # null = explicit deny
```

Built-in capabilities:

| Capability | Required by |
|------------|-------------|
| `rpc` | `/ma/rpc/0.0.1` |
| `ipfs` | `/ma/ipfs/0.0.1` |
| `"*"` | Wildcard — grants all capabilities when used in an Allow set |
| `create` / `update` / `delete` | Namespace / entity management |

Rules: deny always wins; direct match wins over wildcard; fragment stripped
from DID-URLs before lookup.

---

## Status web server

| Endpoint | Content-Type | Description |
|----------|-------------|-------------|
| `GET /` | `text/html` | Human-friendly page |
| `GET /status.json` | `application/json` | Machine-readable status |
| `GET /bootstrap.yaml` | `text/yaml` | Bootstrap template for this runtime |
| `POST /claim` | — | Claim ownership: body `{"did":"did:ma:…"}` |

### CORS policy (default-deny except allowlist)

Status endpoints are served with a CORS allowlist (not `*`).

Default allowed origins:

- `http://localhost:8000`
- `http://127.0.0.1:8000`
- `https://xn--nf5a.bahner.com`
- `https://間.bahner.com`

Override in config extra as either a YAML list or comma-separated string:

```yaml
status_cors_allowed_origins:
  - http://localhost:8000
  - http://127.0.0.1:8000
  - https://xn--nf5a.bahner.com
  - https://間.bahner.com
```

### `status.json`

```json
{
  "did": "did:ma:<ipns>",
  "ipns": "/ipns/<ipns>",
  "endpoint_id": "<iroh-id>",
  "uptime_secs": 42,
  "ipfs_publisher": true,
  "ipfs_requests": 0,
  "rpc_requests": 0,
  "pings_received": 0,
  "started_at": 1234567890,
  "entity_names": ["greeter", "counter"],
  "runtime": {"/": "bafyreiXXXX"}
}
```

---

## Runtime tuning keys (config extra)

These keys are safe/anonymous operational settings and are also mirrored into
the runtime manifest `config` node.

```yaml
did_resolver_positive_ttl_secs: 60
did_resolver_negative_ttl_secs: 10

did_document_publishing_interval_secs: 300
did_document_publishing_timeout_secs: 120
did_document_publishing_lifetime_hours: 8760

ipns_publish_lifetime_hours: 8760
ipns_publish_allow_offline: true
ipns_publish_resolve: false

status_cors_allowed_origins:
  - http://localhost:8000
  - http://127.0.0.1:8000
  - https://xn--nf5a.bahner.com
  - https://間.bahner.com
```

### IPNS safety notes

- Keep `ipns_secret_key` private and encrypted in `SecretBundle`.
- Prefer `ipns_publish_resolve: false` and explicit republish intervals to
  reduce publish-time dependency on network resolution.
- Use bounded timeouts for publish operations
  (`did_document_publishing_timeout_secs`).

---

## Configuration keys (`ma.yaml`)

| Key | Description |
|-----|-------------|
| `kubo_rpc_url` | Kubo RPC URL (default `http://127.0.0.1:5001`) |
| `secret_bundle_passphrase` | Bundle passphrase. Also `MA_SECRET_BUNDLE_PASSPHRASE` env. Written automatically by `--gen-headless-config`. |
| `ipfs_publisher` | `true`/`false` — enable `/ma/ipfs/0.0.1` (default `true`) |
| `crud_service` | `true`/`false` — enable `/ma/crud/0.0.1` (default `true`) |
| `owners` | List of owner DIDs granted `["*"]` in the live ACL at startup |
| `i18n` | BCP-47 language tag for log messages (e.g. `nb`, `en`) |
| `i18n_cid` | IPFS CID of the compiled locale DAG-CBOR node (set by `make src/i18n.yaml`) |

Runtime root CID publishing uses a deterministic Kubo key alias derived from the
`runtime_ipns` key in `SecretBundle`.

---

## Security notes

- **IPNS keys are zero-use.** The sender's IPNS private key carried in each
  `/ma/ipfs/0.0.1` request is used once for the Kubo publish call and
  immediately zeroized.
- **Replay protection** via a 120-second sliding window on `/ma/ipfs/0.0.1`.
- **Input validation** via `validate_ipfs_publish_request` (ma-core): checks
  content-type, DID document proof, sender identity match.
- **Wasm sandboxing** via extism/Wasmtime.
- **No auto-execution** of arbitrary CIDs — plugins load only from
  `behavior_cid` values in the signed IPLD manifest.
- Config and bundle files are created with mode `0600`.

---

## Makefile

```sh
make build          # cargo build
make release        # cargo build --release
make check          # cargo check
make run            # run the daemon
make src/i18n.yaml  # publish i18n/*.ftl to IPFS and embed CIDs into the binary
make install        # install binary to $PREFIX/bin (default ~/.local/bin)
```
