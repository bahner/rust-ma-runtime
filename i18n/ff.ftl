# ma-runtime – Fulfulde
lang-name = Fulfulde

own-did-published = Takkaare DID am jannginaa IPNS
own-did-publish-failed = Janngugo takkaare DID am waylaaki
own-did-publish-timeout = Janngugo takkaare DID am dariima caggal miinutuuje 2
started = ma runtime fuɗɗiima
shutdown-requested = Haaɗtugol heɓii
closing-endpoint = Ɗannugo iroh endpoint...
shutdown-complete = Haaɗtugol dariima
status-listening = Sarwiroo haala dengoo
rpc-message-received = Ƈiiɗol RPC heɓii
rpc-message-rejected = Ƈiiɗol RPC rewindii
ipfs-message-rejected = Ƈiiɗol IPFS rewindii
ctrlc-handler-failed = Janngiiɗo Ctrl-C waylaaki
node-connected = Nod jokkondiraa e laawol
received-encrypted-ma-msg = Ƈiiɗol ma wuurninaa heɓii to /ma/ipfs/0.0.1
unknown-rpc-atom = Atoom RPC faamaaka, waɗaaka
rpc-not-text-atom = Baylo RPC wonaa atoom ngo
rpc-unknown-verb = Nafoore RPC faamaaka
rpc-reply-sent = Jaabawol RPC neldaa
ping-received = :ping heɓii, neldoo :pong
did-publish-request-received = Sarɗol janngugo takkaare DID heɓii
document-published = Takkaare jannginaa
did-publish-cid-reply-sent = Jaabawol CID janngugo DID neldaa
did-publish-resolve-failed = Waylaaki hoolanaago neldoowo faa jaabodoo ipfs-publish
ipfs-store-request-received = Sarɗol haaɗtugol IPFS heɓii
ipfs-stored = Kuutorɗe njaaɗii IPFS
ipfs-store-cid-reply-sent = Jaabawol CID neldaa
ipfs-store-resolve-failed = Waylaaki hoolanaago neldoowo faa jaabodoo ipfs-store

# Neldugo huunde
bootstrap-complete = Bootstrap dariima
entity-loaded = Seɗɗa huunde waɗii
entity-load-failed = Waɗugo seɗɗa huunde waylaaki
entity-not-found = Huunde yiytaaki, RPC waɗaaka
entity-dispatched = RPC neldaa huunde
entity-replied = Huunde neldii jaabawol RPC
root-create-entity = #root: fuu huunde
root-list-entities = #root: liste huunde
root-delete-entity = #root: momtu huunde
root-entity-updated = Jibnol runtime heɓii yarlitaare
entity-created = Huunde fuɗɗii
entity-reloaded = Seɗɗa huunde waɗitaama
entity-deleted = Huunde momtaa
entity-states-saving = Momtugol ɗeɗɗe huunde IPFS
entity-state-saving = Momtugol ɗeɗɗe huunde
entity-state-saved = Ɗeɗɗe huunde momtaa
entity-state-empty = Seɗɗa yotti ɗeɗɗe jaaje, momtugo yejjitaa
entity-states-saved = Ɗeɗɗe huunde momtaa
link-set = Jokkol waɗaa
ftl-loaded = Koode hol'ita nelda IPFS

# Fuɗɗugo adannde / auto-init
no-config-found = Maantol heɓaaki.
initialising-new-identity = Fuɗɗugol gooto runtime haa.
generated-headless-config = Maantol headless fuɗɗaa.

# Jom
runtime-claimed = Runtime faaɓinaa.

# Huunde jalte geɗe eɓɓooje
refuse-delete-root = Rewindoo tiiɗnde momtugol geɗe jalte bardinooje
no-root-acl = ACL jalte nalitaake — runtime heɓatako darannaaji hiiwtori
acl-owners-access = Jooɗtotooɗo heɓii yamiroore ko gollotooɗo e +owners
runtime-claim-persisted = Jom winndirii maantol.
runtime-already-claimed = Runtime faaɓinaa ɗoon hannde.


# Namespace creation (:create)
crud-message-received = Tinnde CRUD heɓii
crud-acl-updated = Root transport ACL humpitaama

# CRUD validation errors
blob-value-ipfs-path = kerol blob waawi woodde laawol IPFS (/ipfs/, /ipns/, walaa /ipld/)
acl-value-ipfs-path = kerol ACL waawi woodde laawol IPFS (/ipfs/, /ipns/, walaa /ipld/)
kind-value-ipfs-path = kerol kind waawi woodde laawol IPFS (/ipfs/, /ipns/, walaa /ipld/)
kind-not-found = Juuɗe ngal e jogindiral weeɓaani
cidv1-required = haddi na faa CIDv1 cokoyel (fuuta 'b'; CIDv0 'Qm…' acceppaaka)
config-key-protected = sorol config '%key%' nder keerol
config-key-no-delete = sorol config daemon '%key%' waawaa wanaa
config-key-not-manifest = sorol config '%key%' alaa e sorol manifest config gannduɗi
owners-value-not-list = keerol owners foti wonde doggol DIDs, wanaa hono gootel
wrong-crud-protocol = protokol CRUD moƴƴi alaa: %type%
entity-name-invalid = innde entity ngoodha UTF-8 e bindateji
reserved-entity-name = innde entity '%name%' ndi dokkaa
genesis-kind-owner-only = Ko jom runtime tan waawi tagude huunde e juuɗe genesis

# IPv6 config
ipv6-enabled = IPv6 heɓii — jokku IPv4 e IPv6 ɗiɗi
ipv6-disabled = IPv6 haɓɓitaama — IPv4 tan woni seŋtiɗo (restart haɓeteeɗo ngam yuɓɓinde)
ipv6-enable-restart-required = Abbitaama. Restart haɓeteeɗo ngam tabitinde huɓɓinaango ngoo.
ipv6-enable-unchanged = ipv6_enable heɓii tan ko ɗuum boorti — alaa huunde feewi.
