# ma-runtime – Norsk Bokmål
lang-name = Norsk bokmål

own-did-published = Eget DID-dokument publisert til IPNS
own-did-publish-failed = Kunne ikke publisere eget DID-dokument
own-did-publish-timeout = Timeout ved publisering av eget DID-dokument (2 min)
started = ma runtime startet
shutdown-requested = Avslutning forespurt
closing-endpoint = Lukker iroh-endepunkt ...
shutdown-complete = Avslutning fullført
status-listening = Statusserver lytter
rpc-message-received = Mottok RPC-melding
crud-message-received = Mottok CRUD-melding
crud-acl-updated = Rot-transport-ACL oppdatert
rpc-message-rejected = RPC-melding avvist
ipfs-message-rejected = IPFS-melding avvist
ctrlc-handler-failed = Ctrl-C-behandler feilet
node-connected = Node koblet til protokoll
received-encrypted-ma-msg = Mottok kryptert ma-melding på /ma/ipfs/0.0.1
unknown-rpc-atom = Ukjent RPC-atom, ignorerer
rpc-not-text-atom = RPC-innhold er ikke et tekstatom
rpc-unknown-verb = Ukjent RPC-verb
rpc-reply-sent = RPC-svar sendt
ping-received = Mottok :ping, sender :pong
did-publish-request-received = Mottok forespørsel om publisering av dokument
document-published = Dokument publisert
did-publish-cid-reply-sent = Sendt CID-svar for DID-publisering
did-publish-resolve-failed = Kunne ikke løse opp avsender for ipfs-publish-svar
ipfs-store-request-received = Mottok IPFS store-forespørsel
ipfs-stored = Lagret innhold på IPFS
ipfs-store-cid-reply-sent = Sendt CID-svar
ipfs-store-resolve-failed = Kunne ikke løse opp avsender for ipfs-store-svar

# Enhetsutsending
bootstrap-complete = Bootstrap fullført
entity-loaded = Enhetsplugin lastet
entity-load-failed = Feil ved lasting av enhetsplugin
entity-not-found = Enhet ikke funnet, ignorerer RPC
entity-dispatched = RPC sendt til enhet
entity-replied = Enhet sendte RPC-svar
root-create-entity = #root: opprett enhet
root-list-entities = #root: list enheter
root-delete-entity = #root: slett enhet
root-entity-updated = Runtime-manifest oppdatert
default-config-root-populated = Standard /config/root satt ved oppstart
default-config-root-no-root-entity = Kan ikke sette standard /config/root ved oppstart: #root-enheten er ikke lastet
default-config-root-no-root-cid = Kan ikke sette standard /config/root ved oppstart: ingen manifest-root-CID er tilgjengelig
default-config-root-inspect-failed = Klarte ikke å lese manifestet før standard /config/root ble satt
default-config-root-populate-failed = Klarte ikke å sette standard /config/root ved oppstart
entity-created = Enhet opprettet
entity-reloaded = Enhetsplugin lastet på nytt
entity-deleted = Enhet slettet
entity-states-saving = Lagrer enhetstilstander til IPFS
entity-state-saving = Lagrer enhetstilstand
entity-state-saved = Enhetstilstand lagret
entity-state-empty = Plugin returnerte tom tilstand, hopper over lagring
entity-states-saved = Enhetstilstander lagret
link-set = Lenke satt
ftl-loaded = Språkmeldinger lastet fra IPFS

# Første-gangs auto-oppsett
no-config-found = Ingen konfigurasjon funnet.
initialising-new-identity = Oppretter ny runtime-identitet.
generated-headless-config = Genererte hodeløs konfigurasjon.

# Eierskap / krav
runtime-claimed = Runtime registrert.

# Beskyttede rotelementer
refuse-delete-root = Nekter bestemt å slette påkrevd rotelement
no-root-acl = Ingen rot-ACL er konfigurert — kjøretiden opererer uten tilgangskontroll
acl-owners-access = Innringer fikk tilgang som medlem av +owners
runtime-claim-persisted = Eier skrevet til konfigurasjon.
runtime-already-claimed = Runtime er allerede registrert.


# Namespace creation (:create)

# CRUD validation errors
blob-value-ipfs-path = blob-verdien må være en IPFS-sti (/ipfs/, /ipns/ eller /ipld/)
acl-value-ipfs-path = ACL-verdien må være en IPFS-sti (/ipfs/, /ipns/ eller /ipld/)
kind-value-ipfs-path = kind-verdien må være en IPFS-sti (/ipfs/, /ipns/ eller /ipld/)
kind-not-found = Typen ble ikke funnet
cidv1-required = verdien må være en ren CIDv1 (starter med 'b'; CIDv0 'Qm…' godtas ikke)
config-key-protected = config-nøkkelen '%key%' er beskyttet
config-key-no-delete = daemon-config-nøkkelen '%key%' kan ikke slettes
config-key-not-manifest = config-nøkkelen '%key%' er ikke en kjent manifest config-nøkkel
owners-value-not-list = owners-verdien må være en liste av DID-er, ikke en enkelt verdi
wrong-crud-protocol = feil CRUD-protokoll: %type%
entity-name-invalid = entity-navn må være skrivbart UTF-8
reserved-entity-name = entity-navn '%name%' er reservert
genesis-kind-owner-only = Bare en runtime-eier kan opprette en entity av typen genesis

# IPv6 config
ipv6-enabled = IPv6 aktivert — lytter på både IPv4 og IPv6
ipv6-disabled = IPv6 er deaktivert — binder kun IPv4 (restart kreves for å aktivere på nytt)
ipv6-enable-restart-required = Lagret. Restart kreves for at denne endringen skal tre i kraft.
ipv6-enable-unchanged = ipv6_enable er allerede satt til den verdien — ingen endring.
