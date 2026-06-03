# How to run your own 間 runtime

This guide walks you through everything from a blank machine to a running
personal `ma` runtime with your own entities, reachable by others over the
peer-to-peer network.

---

## What you are setting up

`ma` is a personal daemon that gives you a permanent
[`did:ma:` identity](https://github.com/bahner/ma-spec) on the decentralised
web.  It acts as a bridge between browser-based actors (zion) and IPFS, and
hosts your own Wasm plugins — entities — that anyone with your DID can call.

The system has three moving parts:

| Component | What it does |
|-----------|-------------|
| **IPFS Desktop** (Kubo) | Content-addressed storage; your DID document and all entity state live here |
| **`ma`** | The runtime daemon; derives your DID, hosts plugins, handles messages |
| **zion** | Browser-based actor terminal; how you talk to `ma` and to others |

---

## Step 1 — Install IPFS Desktop

IPFS Desktop bundles Kubo and a system-tray GUI. Download it from:

**<https://docs.ipfs.tech/install/ipfs-desktop/>**

Install and launch it.  The tray icon will show a spinning animation while
it connects to the network, then settle to a steady icon once ready.

### Windows firewall prompt

On Windows, the first time Kubo starts you will get a Windows Defender
Firewall dialog asking whether to allow network access.  **Allow it** —
both private and public networks if you want full connectivity.

The daemon only binds to `127.0.0.1:5001` for its RPC API, so the only
practical network exposure is the libp2p swarm port (default 4001) which
is outbound-initiated and encrypted.  Running this on your own laptop or
desktop carries no meaningful risk.

---

## Step 2 — Install `ma`

Download the latest release binary for your platform from:

**<https://github.com/bahner/ma-runtime/releases>**

**Windows:** download `ma.exe`, place it anywhere (e.g. your Desktop or
`C:\Users\<you>\bin\`), and double-click it.  A terminal window opens and
`ma` starts.  No further setup is required.

**Linux (Ubuntu / Debian):** download the `ma` binary, make it executable,
and move it onto your `PATH`:

```sh
chmod +x ma
mv ma ~/.local/bin/
```

**macOS:** same as Linux.  You may need to right-click → Open the first time
to bypass the Gatekeeper quarantine warning.

Or build from source (requires Rust stable):

```sh
cargo install --git https://github.com/bahner/ma-runtime ma
```

---

## Step 3 — First start

**Windows:** double-click `ma.exe`.  A terminal window opens and the daemon
starts immediately.

**Linux / macOS:** run it from a terminal:

```sh
ma
```

On first start `ma` detects that no identity exists yet and automatically
generates a complete configuration — a fresh `did:ma:` identity, four
32-byte cryptographic keys, and a random passphrase to encrypt them.  The
passphrase is printed to stdout **once**:

```
Generated headless config.
Passphrase: correct-horse-battery-staple-...
```

**Save that passphrase.**  Store it in a password manager.

The passphrase is also written into `ma.yaml` automatically, so `ma` can
restart without prompting you.  If you prefer not to keep it in the config
file you can remove the `secret_bundle_passphrase:` line and supply it via
the environment variable `MA_SECRET_BUNDLE_PASSPHRASE` instead.

Config files land under your XDG config directory:

| OS | Path |
|----|------|
| Linux | `~/.config/ma/ma.yaml` |
| macOS | `~/Library/Application Support/ma/ma.yaml` |
| Windows | `%APPDATA%\ma\ma.yaml` |

### What happens at startup

`ma` will:

1. Derive its `did:ma:` identity from the IPNS key in the bundle.
2. Publish its own DID document to IPFS/IPNS (requires Kubo to be running).
3. Bootstrap a minimal empty manifest if no prior state is found in IPNS.
4. Start listening for iroh QUIC connections.
5. Start a local status HTTP server on `http://127.0.0.1:5003`.

You can visit **<http://127.0.0.1:5003>** in a browser at any time to see
your DID, endpoint ID, uptime, and loaded entities.

Machine-readable status:

```sh
curl http://127.0.0.1:5003/status.json
```

---

## Step 4 — Open zion and claim your runtime

zion is the browser-based actor terminal.  Open it at:

**<https://間.bahner.com>** (Punycode: `https://xn--nf5a.bahner.com`)

1. Create an identity (pick a username, set a passphrase).
2. Claim ownership of your local runtime — this discovers it, connects,
   registers your zion identity as the owner, and persists the claim to
   `ma.yaml` in one step:

```
.my.ma:claim
```

That's it.  Your DID document is published to IPFS automatically as part
of the process — no separate publish step is needed.

Until claimed, `ma` runs with an **open ACL** — all principals may use
RPC and IPFS publish.  This is intentional: it means DID documents can be
published immediately on a fresh runtime without any manual configuration.
Once you claim the runtime the ACL tightens.

### Why open before claiming?

Your DID document needs to reach IPFS before anyone can verify your
identity.  Without an open ACL on a fresh unclaimed runtime, zion could
not publish its DID document to `ma`, which means no one could call you —
a chicken-and-egg problem.  The open window is short and local-only (the
runtime is only reachable via iroh QUIC using your endpoint ID, which you
have not shared yet).

---

## Step 5 — Back up your identity

Your zion identity (keys, config, aliases, inbox) lives in your browser's
IndexedDB.  If that browser profile is lost, your identity is gone — there
is no server-side recovery.

**Export your bundle immediately after claiming:**

```
.my.identity:export
```

This downloads an encrypted JSON file named `<username>.zion.json`.  The
bundle is protected by your zion passphrase — without it the file is
useless, so the passphrase is the only thing you must keep secret.

**Store the file somewhere safe** — a USB drive, a cloud folder, a
password manager's attachment, or another browser.  You can also export
from the landing page (click **Export** next to your username) and import
on another browser via the **Import** button.

A few important things to understand about identity portability:

- **One active session per browser tab** — each tab is one identity, but
  you can have multiple browsers or profiles each running a different identity.
- **Only one place should be actively publishing at a time.**  The bundle
  can exist in multiple browsers, but whichever session most recently
  published to IPNS is the one that will receive messages — IPNS is
  last-write-wins.
- **Re-export after significant changes** — aliases, inbox state, and
  config only live in the browser.  The IPFS-published DID document is
  durable, but your local annotation data is not.

---

## Step 6 — Aliases: how to stay sane

Every actor in the network has a `did:ma:` identifier that looks like:

```
did:ma:k51qzi5uqu5dgqn5qgzrx81y9x2e5sesg5lqhiz6uvn8lep1k4l10k03ndubmt
```

Nobody memorises these.  Aliases are how you work with them:

```
# Store an alias
.my.aliases.alice: did:ma:k51qzi5uqu5…

# Use it anywhere
@alice:ping
@alice:fortune
.my.inbox:filter @alice
```

Aliases are stored in your local `EgoConfig` (IndexedDB in your browser)
and are personal — they do not propagate to others.  Think of them as your
own contact list.

The `@ma` alias is created automatically by `.my.ma:claim`.  Add aliases
for everyone you interact with regularly.  Without aliases, long strings of
`did:ma:…` will fill your terminal and you will lose track of who is who.

---

## Step 7 — Share your address

Your DID is your permanent address.  Give it to others so they can call
your entities or send you messages.

```
.my.identity
```

prints your full DID.  You can also link people directly to your DID
document on the IPFS gateway:

```
https://ipfs.io/ipns/<your-ipns-id>
```

where `<your-ipns-id>` is the part after `did:ma:`.  For example:

```
did:ma:k51qzi5uqu5dgqn5…
           ↓
https://ipfs.io/ipns/k51qzi5uqu5dgqn5…
```

You will get back a `bafy…` CID as confirmation when things are written to
IPFS — this is normal.  The `bafy…` prefix means DAG-CBOR (v1 CID); `Qm…`
means an older raw block.  You do not need to remember CIDs — they are
stored automatically.

---

## Step 8 — Bootstrap with kinds

Kinds define the protocol a plugin speaks — its API, host functions, and
whether it is stateful.  They live in the `RuntimeManifest` on IPFS and
are the contract that lets `ma` load and dispatch any plugin that conforms.

Before you can load entities you need kinds in your manifest.  The easiest
way is to bootstrap from `bootstrap.example.yaml` (already in the repo):

```sh
# Publish the manifest tree to IPFS, get back the root CID
ma --gen-root-cid bootstrap.example.yaml

# Output:
# bafyreiabc123…

# Set it as your runtime head
echo "root_cid: bafyreiabc123…" >> ~/.config/ma/ma.yaml
```

Then restart `ma` and it will load the kinds and entities from the new manifest.

The example bootstrap ships with these standard kinds out of the box:

| Kind | Protocol | Description |
|------|----------|-------------|
| `root` | `/ma/root/0.0.1` | Entity lifecycle orchestrator |
| `counter` | `/ma/counter/0.0.1` | Atomic integer with `:get/:inc/:dec/:set` |
| `register` | `/ma/register/0.0.1` | Bijective key↔value map |
| `set` | `/ma/set/0.0.1` | Unordered unique-value collection |
| `logger` | `/ma/logger/0.0.1` | Structured log store |
| `fortune` | `/ma/stateless/python/0.0.1` | Stateless Python handler |

Kinds are the most important architectural decision in your runtime.  A
kind defines the whole contract — changing a kind after entities use it is
a breaking change.  Design kinds to be stable and reusable; entities are
the cheap, disposable part.

---

## Step 9 — Load entities and talk to them

With a bootstrapped manifest, your runtime will have live entities.  Try
them from zion:

```
# Ping the scheduler (built-in native entity)
@ma:ping

# Get the current counter value
@ma#counter:get

# Increment it
@ma#counter:inc

# Ask for a fortune
@ma#fortune:handle_cast
```

Responses come back as CBOR and are displayed in the terminal.  `:ok`
replies render as green; `:error` replies as red.

---

## Step 10 — Write your own plugin

This is where it gets fun.

Plugins are Wasm modules.  The simplest are written in Python using
`extism-py`.  Here is a minimal stateless plugin:

```python
import extism
import cbor2

@extism.import_fn("extism:host/user", "ma_reply")
def ma_reply(data: bytes) -> None: ...

@extism.plugin_fn
def handle_cast():
    raw = extism.input_bytes()
    msg = cbor2.loads(raw).get("msg", {})
    ma_reply(cbor2.dumps({
        "msg": msg,
        "content_type": "application/cbor",
        "content": cbor2.dumps([":ok", "hello from my plugin"]),
    }))
```

Build it:

```sh
# Install extism-py (once)
pip install extism

# Compile to Wasm
extism-py hello.py -o hello.wasm

# Publish to IPFS
ipfs add --quieter hello.wasm
# → bafkreixyz…
```

Create an `EntityNode` descriptor (`hello.json`):

```json
{
  "name": "hello",
  "kind": "/ma/stateless/python/0.0.1",
  "behaviour": { "/": "bafkreixyz…" },
  "owner": "did:ma:<your-did>",
  "acl": ""
}
```

Publish it and register it with your runtime:

```sh
# Publish the entity node
ipfs dag put --store-codec dag-cbor hello.json
# → bafyreiXXX…
```

Then from zion:

```
@ma:entities.hello: bafyreiXXX…
```

Your runtime fetches the node from IPFS, loads the Wasm, and the entity is
live.  Call it:

```
@ma#hello:handle_cast
```

### Adding a new kind

If your plugin needs host functions or a different API, define a kind first
in your bootstrap YAML:

```yaml
kinds:
  /my/greeter/0.0.1:
    api:
      - handle_cast
    host_functions:
      - ma_reply
      - ma_send
```

Re-bootstrap to publish the new kind:

```sh
ma --gen-root-cid my-bootstrap.yaml
```

Update `root_cid` in `ma.yaml` and restart.  Now you can create entities
with `kind: /my/greeter/0.0.1`.

The [ma-python](https://github.com/bahner/ma-python) repository has working
examples for counter, fortune, register, set, and logger — all production
plugins used in the standard bootstrap.

---

## Calling someone else's runtime

Ask them for their DID (`.my.identity` in their zion prints it), then add
an alias manually:

```
# Store their DID as an alias
.my.aliases.sky: did:ma:k51…their-ipns…

# Call their fortune entity
@sky#fortune:handle_cast

# Send them a message
@sky hello there
```

They receive your message in their inbox, identified by your DID.  If they
have an alias for you, they see `@you`; otherwise they see the full
`did:ma:…`.  This is why aliases matter on both sides.

> **Note:** Port 5003 is a local-only status interface — it must never be
> exposed to the internet.  There is no network-based discovery of other
> people's runtimes; DID exchange happens out-of-band (share your DID the
> same way you would share an email address).

---

## Useful zion commands reference

```
# Identity
.my.identity                    print your full DID
.my.identity:publish @ma        publish DID document to IPFS via your runtime

# Runtime
.my.ma:discover                 find local runtime at http://127.0.0.1:5003
.my.ma:connect                  connect iroh QUIC transport
.my.ma:claim                    register yourself as owner

# Aliases
.my.aliases                     list all aliases
.my.aliases.name: did:ma:…      add alias
.my.aliases.name:               delete alias

# Inbox
.my.inbox                       list received messages
.my.inbox.0                     read message 0
.my.inbox.0:reply hello         reply inline
.my.inbox.0:                    delete message 0

# Sending
@target body                    send a text message
@target#entity:verb [args]      call an entity verb

# Status
http://127.0.0.1:5003           runtime status page (browser)
```

---

## Troubleshooting

**`ma` exits immediately with "kubo RPC is not reachable"**
: IPFS Desktop is not running, or Kubo is still starting up.  Wait for
  the tray icon to stabilise and retry.

**DID document never appears on the gateway**
: Check that IPFS Desktop is connected to peers (tray menu → "Connected
  Peers" should be > 0).  IPNS propagation can take a few minutes on a
  fresh node.

**zion shows `claim-conflict`**
: The runtime is already claimed (owners list is non-empty).  Check
  `~/.config/ma/ma.yaml` for the `owners:` key.

**Entities not loading after bootstrap**
: Make sure `root_cid` in `ma.yaml` matches the CID printed by
  `--gen-root-cid`, and that Kubo is pinning it
  (`ipfs pin ls | grep <cid>`).

**Wasm plugin fails to load**
: Verify the `behaviour` CID is reachable: `ipfs cat <cid> | wc -c`.
  Ensure the kind referenced in the entity node exists in your manifest.
