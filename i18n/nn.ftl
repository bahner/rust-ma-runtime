# ma-runtime – Nynorsk
lang-name = Nynorsk

own-did-published = Eige DID-dokument publisert til IPNS
own-did-publish-failed = Kunne ikkje publisere eige DID-dokument
own-did-publish-timeout = Tidsavbrot ved publisering av eige DID-dokument (2 min)
started = ma runtime starta
shutdown-requested = Avslutning beden
closing-endpoint = Lukkar iroh-endepunkt...
shutdown-complete = Avslutning fullført
status-listening = Statustenar lyttar
rpc-message-received = Mottok RPC-melding
crud-message-received = Mottok CRUD-melding
crud-acl-updated = Rot-transport-ACL oppdatert
rpc-message-rejected = RPC-melding avvist
ipfs-message-rejected = IPFS-melding avvist
ctrlc-handler-failed = Ctrl-C-handsamar feila
node-connected = Node kopla til protokoll
received-encrypted-ma-msg = Mottok kryptert ma-melding på /ma/ipfs/0.0.1
unknown-rpc-atom = Ukjend RPC-atom, ignorerer
rpc-not-text-atom = RPC-innhald er ikkje eit tekstatom
rpc-unknown-verb = Ukjend RPC-verb
rpc-reply-sent = RPC-svar sendt
ping-received = Mottok :ping, sender :pong
did-publish-request-received = Mottok førespurnad om publisering av DID-dokument
document-published = Dokument publisert
did-publish-cid-reply-sent = Sendt CID-svar for DID-publisering
did-publish-resolve-failed = Klarte ikkje løyse opp avsendaren for ipfs-publish-svar
ipfs-store-request-received = Mottok IPFS store-førespurnad
ipfs-stored = Lagra innhald på IPFS
ipfs-store-cid-reply-sent = Sendt CID-svar
ipfs-store-resolve-failed = Klarte ikkje løyse opp avsendaren for ipfs-store-svar

# Einingsutsending
bootstrap-complete = Bootstrap fullført
entity-loaded = Einingsplugin lasta
entity-load-failed = Feil ved lasting av einingsplugin
entity-not-found = Eining ikkje funnen, ignorerer RPC
entity-dispatched = RPC send til eining
entity-replied = Eining sende RPC-svar
root-create-entity = #root: opprett eining
root-list-entities = #root: list einingar
root-delete-entity = #root: slett eining
root-entity-updated = Runtime-manifest oppdatert
default-config-root-populated = Standard /config/root sett ved oppstart
default-config-root-no-root-entity = Kan ikkje setje standard /config/root ved oppstart: #root-eininga er ikkje lasta
default-config-root-no-root-cid = Kan ikkje setje standard /config/root ved oppstart: ingen manifest-root-CID er tilgjengeleg
default-config-root-inspect-failed = Klarte ikkje å lese manifestet før standard /config/root vart sett
default-config-root-populate-failed = Klarte ikkje å setje standard /config/root ved oppstart
entity-created = Eining oppretta
entity-reloaded = Einingsplugin lasta på nytt
entity-deleted = Eining sletta
entity-states-saving = Lagrar einingstilstandar til IPFS
entity-state-saving = Lagrar einingstilstand
entity-state-saved = Einingstilstand lagra
entity-state-empty = Plugin returnerte tom tilstand, hoppar over lagring
entity-states-saved = Einingstilstandar lagra
link-set = Lenkje sett
ftl-loaded = Språkmeldingar lasta frå IPFS

# Første gongs auto-oppsett
no-config-found = Ingen konfigurasjon funnen.
initialising-new-identity = Oppretter ny runtime-identitet.
generated-headless-config = Hovudlaus konfigurasjon generert.

# Eigarskap
runtime-claimed = Runtime registrert.

# Verna rotelément
refuse-delete-root = Nektar bestemt å slette påkravd rotelement
no-root-acl = Ingen rot-ACL er konfigurert — køyretida opererer utan tilgangskontroll
acl-owners-access = Innringar fekk tilgang som medlem av +owners
runtime-claim-persisted = Eigar skrive til konfigurasjon.
runtime-already-claimed = Runtime er allereie registrert.


# Namespace creation (:create)

# CRUD validation errors
blob-value-ipfs-path = blob-verdien må vera ein IPFS-sti (/ipfs/, /ipns/ eller /ipld/)
acl-value-ipfs-path = ACL-verdien må vera ein IPFS-sti (/ipfs/, /ipns/ eller /ipld/)
kind-value-ipfs-path = kind-verdien må vera ein IPFS-sti (/ipfs/, /ipns/ eller /ipld/)
kind-not-found = Typen vart ikkje funnen
cidv1-required = verdien må vere ein rein CIDv1 (startar med 'b'; CIDv0 'Qm…' vert ikkje godtatt)
config-key-protected = config-nøkkelen '%key%' er verna
config-key-no-delete = daemon-config-nøkkelen '%key%' kan ikkje slettast
config-key-not-manifest = config-nøkkelen '%key%' er ikkje ein kjend manifest config-nøkkel
owners-value-not-list = owners-verdien må vere ei liste av DID-ar, ikkje ein enkelt verdi
wrong-crud-protocol = feil CRUD-protokoll: %type%
entity-name-invalid = entity-namn må vere skrivbart UTF-8
reserved-entity-name = entity-namn '%name%' er reservert
genesis-kind-owner-only = Berre ein runtime-eigar kan opprette ein entity av typen genesis

# IPv6 config
ipv6-enabled = IPv6 aktivert — bind til både IPv4 og IPv6
ipv6-disabled = IPv6 er deaktivert — bind berre IPv4 (restart krevst for å aktivere på nytt)
ipv6-enable-restart-required = Lagra. Restart krevst for at denne endringa skal tre i kraft.
ipv6-enable-unchanged = ipv6_enable er allereie sett til den verdien — ingen endring.
