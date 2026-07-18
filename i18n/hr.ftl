# ma-runtime – Hrvatski
lang-name = Hrvatski

own-did-published = Vlastiti DID dokument objavljen na IPNS
own-did-publish-failed = Objavljivanje vlastitog DID dokumenta nije uspjelo
own-did-publish-timeout = Objavljivanje vlastitog DID dokumenta isteklo nakon 2 minute
started = ma runtime pokrenut
shutdown-requested = Zatvaranje zatraženo
closing-endpoint = Zatvaranje iroh krajnje točke...
shutdown-complete = Zatvaranje dovršeno
status-listening = Statusni poslužitelj sluša
rpc-message-received = Primljena RPC poruka
rpc-message-rejected = RPC poruka odbijena
ipfs-message-rejected = IPFS poruka odbijena
ctrlc-handler-failed = Upravljač Ctrl-C nije uspio
node-connected = Čvor spojen na protokol
received-encrypted-ma-msg = Primljena šifrirana ma poruka na /ma/ipfs/0.0.1
unknown-rpc-atom = Nepoznati RPC atom, ignoriranje
rpc-not-text-atom = RPC sadržaj nije tekstni atom
rpc-unknown-verb = Nepoznata RPC naredba
rpc-reply-sent = RPC odgovor poslan
ping-received = Primljen :ping, šaljem :pong
did-publish-request-received = Primljen zahtjev za objavu DID dokumenta
document-published = Dokument objavljen
did-publish-cid-reply-sent = Poslan CID odgovor za objavu DID
did-publish-resolve-failed = Nije moguće razriješiti pošiljatelja za dostavu odgovora ipfs-publish
ipfs-store-request-received = Primljen zahtjev za pohranu IPFS
ipfs-stored = Sadržaj pohranjen na IPFS
ipfs-store-cid-reply-sent = CID odgovor poslan
ipfs-store-resolve-failed = Nije moguće razriješiti pošiljatelja za dostavu odgovora ipfs-store

# Slanje entiteta
bootstrap-complete = Bootstrap dovršen
entity-loaded = Dodatak entiteta učitan
entity-load-failed = Učitavanje dodatka entiteta nije uspjelo
entity-not-found = Entitet nije pronađen, RPC se ignorira
entity-dispatched = RPC proslijeđen entitetu
entity-replied = Entitet je poslao RPC odgovor
root-create-entity = #root: stvori entitet
root-list-entities = #root: popis entiteta
root-delete-entity = #root: obriši entitet
root-entity-updated = Runtime manifest ažuriran
default-config-root-populated = Zadani /config/root popunjen pri pokretanju
default-config-root-no-root-entity = Nije moguće popuniti zadani /config/root pri pokretanju: entitet #root nije učitan
default-config-root-no-root-cid = Nije moguće popuniti zadani /config/root pri pokretanju: korijenski CID manifesta nije dostupan
default-config-root-inspect-failed = Nije uspjela provjera manifesta prije popunjavanja zadanog /config/root
default-config-root-populate-failed = Nije uspjelo popunjavanje zadanog /config/root pri pokretanju
entity-created = Entitet stvoren
entity-reloaded = Dodatak entiteta ponovno učitan
entity-deleted = Entitet obrisan
entity-states-saving = Spremanje stanja entiteta u IPFS
entity-state-saving = Spremanje stanja entiteta
entity-state-saved = Stanje entiteta spremljeno
entity-state-empty = Dodatak vratio prazno stanje, spremanje preskočeno
entity-states-saved = Stanja entiteta spremljena
link-set = Veza postavljena
ftl-loaded = Jezične poruke učitane iz IPFS

# Prvo pokretanje / auto-init
no-config-found = Konfiguracija nije pronađena.
initialising-new-identity = Inicijalizacija novog runtime identiteta.
generated-headless-config = Headless konfiguracija generirana.

# Vlasništvo
runtime-claimed = Runtime registriran.

# Zaštićeni korijenski elementi
refuse-delete-root = Odlučno odbijam brisanje obveznog korijenskog elementa
no-root-acl = Nije konfiguriran root ACL — runtime radi bez kontrole pristupa
acl-owners-access = Pozivaču je odobren pristup kao članu grupe +owners
runtime-claim-persisted = Vlasnik zapisan u konfiguraciju.
runtime-already-claimed = Runtime je već registriran.


# Namespace creation (:create)
crud-message-received = Primljena CRUD poruka
crud-acl-updated = Korijenski transportni ACL ažuriran

# CRUD validation errors
blob-value-ipfs-path = vrijednost blob mora biti IPFS putanja (/ipfs/, /ipns/ ili /ipld/)
acl-value-ipfs-path = vrijednost ACL mora biti IPFS putanja (/ipfs/, /ipns/ ili /ipld/)
kind-value-ipfs-path = vrijednost kind mora biti IPFS putanja (/ipfs/, /ipns/ ili /ipld/)
kind-not-found = Vrsta nije pronađena
cidv1-required = vrijednost mora biti goli CIDv1 (počinje s 'b'; CIDv0 'Qm…' nije prihvaćen)
config-key-protected = config ključ '%key%' je zaštićen
config-key-no-delete = daemon config ključ '%key%' ne može se brisati
config-key-not-manifest = config ključ '%key%' nije poznati manifest config ključ
owners-value-not-list = vrijednost owners mora biti popis DID-ova, a ne jedna vrijednost
wrong-crud-protocol = pogrešan CRUD protokol: %type%
entity-name-invalid = naziv entity mora biti ispisivi UTF-8
reserved-entity-name = naziv entity '%name%' je rezerviran
genesis-kind-owner-only = Samo vlasnik runtimea može stvoriti entity vrste genesis

# IPv6 config
ipv6-enabled = IPv6 omogućen — vezuje IPv4 i IPv6 istovremeno
ipv6-disabled = IPv6 je onemogućen — veže se samo IPv4 (restart je potreban za ponovnu aktivaciju)
ipv6-enable-restart-required = Spremljeno. Potreban je restart kako bi ova promjena stupila na snagu.
ipv6-enable-unchanged = ipv6_enable je već postavljeno na tu vrijednost — nema promjene.
