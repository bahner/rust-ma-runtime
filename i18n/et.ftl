# ma-runtime – Eesti
lang-name = Eesti

own-did-published = Oma DID dokument avaldatud IPNS-is
own-did-publish-failed = Oma DID dokumendi avaldamine ebaõnnestus
own-did-publish-timeout = Oma DID dokumendi avaldamine aegus 2 minuti pärast
started = ma runtime käivitatud
shutdown-requested = Seiskamine nõutud
closing-endpoint = iroh lõpp-punkti sulgemine...
shutdown-complete = Seiskamine lõpetatud
status-listening = Olekuserver kuulab
rpc-message-received = RPC sõnum saadud
rpc-message-rejected = RPC sõnum tagasi lükatud
ipfs-message-rejected = IPFS sõnum tagasi lükatud
ctrlc-handler-failed = Ctrl-C käsitleja ebaõnnestus
node-connected = Sõlm ühendatud protokolliga
received-encrypted-ma-msg = Krüptitud ma sõnum saadud /ma/ipfs/0.0.1 kaudu
unknown-rpc-atom = Tundmatu RPC aatom, ignoreerimine
rpc-not-text-atom = RPC andmed ei ole tekstiatoom
rpc-unknown-verb = Tundmatu RPC verb
rpc-reply-sent = RPC vastus saadetud
ping-received = :ping saadud, saadan :pong
did-publish-request-received = DID dokumendi avaldamise päring saadud
document-published = Dokument avaldatud
did-publish-cid-reply-sent = CID vastus saadetud DID avaldamise jaoks
did-publish-resolve-failed = Saatjat ei õnnestunud lahendada ipfs-publish vastuse edastamiseks
ipfs-store-request-received = IPFS salvestamise päring saadud
ipfs-stored = Sisu salvestatud IPFS-i
ipfs-store-cid-reply-sent = CID vastus saadetud
ipfs-store-resolve-failed = Saatjat ei õnnestunud lahendada ipfs-store vastuse edastamiseks

# Olemite saatmine
bootstrap-complete = Bootstrap lõpetatud
entity-loaded = Olemite plugin laaditud
entity-load-failed = Olemite plugini laadimine ebaõnnestus
entity-not-found = Olemit ei leitud, RPC ignoreerimine
entity-dispatched = RPC edastatud olemile
entity-replied = Olem saatis RPC vastuse
root-create-entity = #root: loo olem
root-list-entities = #root: olemite loend
root-delete-entity = #root: kustuta olem
root-entity-updated = Runtime manifest uuendatud
default-config-root-populated = Vaikimisi /config/root täideti käivitamisel
default-config-root-no-root-entity = Vaikimisi /config/root ei saa käivitamisel täita: #root olem pole laaditud
default-config-root-no-root-cid = Vaikimisi /config/root ei saa käivitamisel täita: manifesti juur-CID pole saadaval
default-config-root-inspect-failed = Manifesti kontrollimine enne vaikimisi /config/root täitmist nurjus
default-config-root-populate-failed = Vaikimisi /config/root täitmine käivitamisel nurjus
entity-created = Olem loodud
entity-reloaded = Olemite plugin uuesti laaditud
entity-deleted = Olem kustutatud
entity-states-saving = Olemite olekute salvestamine IPFS-i
entity-state-saving = Olemi oleku salvestamine
entity-state-saved = Olemi olek salvestatud
entity-state-empty = Plugin tagastas tühja oleku, salvestamine vahele jäetud
entity-states-saved = Olemite olekud salvestatud
link-set = Link seatud
ftl-loaded = Keeleteated laaditud IPFS-ist

# Esimene käivitus / auto-init
no-config-found = Konfiguratsiooni ei leitud.
initialising-new-identity = Uue runtime identiteedi initsialiseerimine.
generated-headless-config = Peavaba konfiguratsioon genereeritud.

# Omandiõigus
runtime-claimed = Runtime registreeritud.

# Kaitstud juureelemendid
refuse-delete-root = Keeldun kategooriliselt nõutava juureelemendi kustutamisest
no-root-acl = Juur-ACL pole konfigureeritud — runtime töötab ilma juurdepääsukontrollita
acl-owners-access = Helistajale anti juurdepääs +owners rühma liikmena
runtime-claim-persisted = Omanik kirjutatud konfiguratsiooni.
runtime-already-claimed = Runtime on juba registreeritud.


# Namespace creation (:create)
crud-message-received = CRUD-sõnum vastu võetud
crud-acl-updated = Juurtranspordi ACL uuendati

# CRUD validation errors
blob-value-ipfs-path = blobi väärtus peab olema IPFS-tee (/ipfs/, /ipns/ või /ipld/)
acl-value-ipfs-path = ACL-i väärtus peab olema IPFS-tee (/ipfs/, /ipns/ või /ipld/)
kind-value-ipfs-path = kind-i väärtus peab olema IPFS-tee (/ipfs/, /ipns/ või /ipld/)
kind-not-found = Tüüpi ei leitud
cidv1-required = väärtus peab olema puhas CIDv1 (algab 'b'-ga; CIDv0 'Qm…' ei aktsepteerita)
config-key-protected = konfiguratsioonivõti '%key%' on kaitstud
config-key-no-delete = deemoni konfiguratsioonivõtit '%key%' ei saa kustutada
config-key-not-manifest = konfiguratsioonivõti '%key%' ei ole teadaolev manifest config võti
owners-value-not-list = owners väärtus peab olema DID-ide loend, mitte üksik väärtus
wrong-crud-protocol = vale CRUD-protokoll: %type%
entity-name-invalid = entity nimi peab olema prinditav UTF-8
reserved-entity-name = entity nimi '%name%' on reserveeritud
genesis-kind-owner-only = Ainult runtime omanik tohib luua genesis-tüüpi olemi

# IPv6 config
ipv6-enabled = IPv6 on lubatud — seob nii IPv4 kui ka IPv6
ipv6-disabled = IPv6 on keelatud — seotakse ainult IPv4 (uuesti lubamiseks on vajalik restart)
ipv6-enable-restart-required = Salvestatud. Muudatuse jõustumiseks on vajalik restart.
ipv6-enable-unchanged = ipv6_enable on juba sellele väärtusele seatud — muudatusi pole.
