# ma-runtime – Krio
lang-name = Krio

own-did-published = Yon DID dokument don publish to IPNS
own-did-publish-failed = Dem nor fit publish yon DID dokument
own-did-publish-timeout = DID dokument publish don taim out afta 2 minit
started = ma runtime don start
shutdown-requested = Dem don ask for shutdown
closing-endpoint = De klos iroh endpoint...
shutdown-complete = Shutdown don complete
status-listening = Status server de listen
rpc-message-received = RPC mesej don reach
rpc-message-rejected = RPC mesej reject
ipfs-message-rejected = IPFS mesej reject
ctrlc-handler-failed = Ctrl-C handler fail
node-connected = Nod don konect to protocol
received-encrypted-ma-msg = Don risiv encryptid ma-msg pan /ma/ipfs/0.0.1
unknown-rpc-atom = Unkown RPC atom, lef am
rpc-not-text-atom = RPC tok no wan text atom
rpc-unknown-verb = RPC verb we wi nor sabi
rpc-reply-sent = RPC reply don sen
ping-received = Don risiv :ping, de sen :pong
did-publish-request-received = Don risiv rikwest for publish DID dokument
document-published = Dokument don publish
did-publish-cid-reply-sent = CID reply for DID publish don sen
did-publish-resolve-failed = Nor fit find senda for deliva ipfs-publish reply
ipfs-store-request-received = Don risiv IPFS store rikwest
ipfs-stored = Konten don store pan IPFS
ipfs-store-cid-reply-sent = CID reply don sen
ipfs-store-resolve-failed = Nor fit find senda for deliva ipfs-store reply

# Entity dispatch
bootstrap-complete = Bootstrap don complete
entity-loaded = Entity plugin don lod
entity-load-failed = Dem nor fit lod entity plugin
entity-not-found = Nor fin entity, lef RPC
entity-dispatched = RPC don dispatch to entity
entity-replied = Entity don sen RPC reply
root-create-entity = #root: kreate entity
root-list-entities = #root: list entity dem
root-delete-entity = #root: pul entity kommot
root-entity-updated = Runtime manifest don update
entity-created = Entity don kreate
entity-reloaded = Entity plugin don lod bak
entity-deleted = Entity don delete
entity-states-saving = De save entity states to IPFS
entity-state-saving = De save entity state
entity-state-saved = Entity state don save
entity-state-empty = Plugin give empty state, skip am
entity-states-saved = Entity states don save
link-set = Link set
ftl-loaded = Lang mesej don lod from IPFS

# First-run auto-init
no-config-found = Nor fine no config.
initialising-new-identity = De initialize new runtime identity.
generated-headless-config = Headless config don generate.

# Ownership / claim
runtime-claimed = Runtime don claim.

# Protected root elements
refuse-delete-root = Steadfastly nor go delete required root element
no-root-acl = Nor get root ACL — runtime de work without access control
acl-owners-access = Di kɔla get akses as memba ɔf +owners
runtime-claim-persisted = Owner don write to config.
runtime-already-claimed = Runtime don already claim.


# Namespace creation (:create)
crud-message-received = CRUD mesej don rish
crud-acl-updated = Root transport ACL don chenj

# CRUD validation errors
blob-value-ipfs-path = di blob valu must bi wan IPFS pat (/ipfs/, /ipns/, or /ipld/)
acl-value-ipfs-path = di ACL valu must bi wan IPFS pat (/ipfs/, /ipns/, or /ipld/)
kind-value-ipfs-path = di kind valu must bi wan IPFS pat (/ipfs/, /ipns/, or /ipld/)
kind-not-found = Di kind nor de
cidv1-required = di value mus bi bare CIDv1 (e stat wid 'b'; CIDv0 'Qm…' no dei)
config-key-protected = config ki '%key%' na pɔtɛkt
config-key-no-delete = daemon config ki '%key%' kɛn nɔt bi dilit
config-key-not-manifest = config ki '%key%' na nɔ nɔ manifest config ki
owners-value-not-list = di owners value mus bi list of DIDs, nɔ wan sinɔgul value
wrong-crud-protocol = rɔng CRUD protokɔl: %type%
entity-name-invalid = di entity nem fɔ bi printable UTF-8
reserved-entity-name = di entity nem '%name%' reserved
genesis-kind-owner-only = Na oni di runtime oner kin mek wan entity we na genesis kind

# IPv6 config
ipv6-enabled = IPv6 don on — e de bind IPv4 an IPv6 togeda
ipv6-disabled = IPv6 don turn aff — na only IPv4 e bin (restart nid fo put am bak on)
ipv6-enable-restart-required = Sev don. Restart nid fo dis chench fo wok.
ipv6-enable-unchanged = ipv6_enable don set to dat valu olredi — no chench.
