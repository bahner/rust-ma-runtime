# ma-runtime – Français
lang-name = Français

own-did-published = Document DID propre publié sur IPNS
own-did-publish-failed = Échec de la publication du document DID propre
own-did-publish-timeout = Publication du document DID propre expirée après 2 minutes
started = ma runtime démarré
shutdown-requested = Arrêt demandé
closing-endpoint = Fermeture du point de terminaison iroh...
shutdown-complete = Arrêt terminé
status-listening = Serveur de statut en écoute
rpc-message-received = Message RPC reçu
rpc-message-rejected = Message RPC rejeté
ipfs-message-rejected = Message IPFS rejeté
ctrlc-handler-failed = Échec du gestionnaire Ctrl-C
node-connected = Nœud connecté au protocole
received-encrypted-ma-msg = Message ma chiffré reçu sur /ma/ipfs/0.0.1
unknown-rpc-atom = Atome RPC inconnu, ignoré
rpc-not-text-atom = La charge RPC n'est pas un atome texte
rpc-unknown-verb = Verbe RPC inconnu
rpc-reply-sent = Réponse RPC envoyée
ping-received = :ping reçu, envoi de :pong
did-publish-request-received = Demande de publication de document DID reçue
document-published = Document publié
did-publish-cid-reply-sent = Réponse CID envoyée pour la publication DID
did-publish-resolve-failed = Impossible de résoudre l'expéditeur pour livrer la réponse ipfs-publish
ipfs-store-request-received = Demande de stockage IPFS reçue
ipfs-stored = Contenu stocké sur IPFS
ipfs-store-cid-reply-sent = Réponse CID envoyée
ipfs-store-resolve-failed = Impossible de résoudre l'expéditeur pour livrer la réponse ipfs-store

# Distribution des entités
bootstrap-complete = Bootstrap terminé
entity-loaded = Plugin d'entité chargé
entity-load-failed = Échec du chargement du plugin d'entité
entity-not-found = Entité introuvable, RPC ignoré
entity-dispatched = RPC transmis à l'entité
entity-replied = L'entité a envoyé une réponse RPC
root-create-entity = #root : créer une entité
root-list-entities = #root : lister les entités
root-delete-entity = #root : supprimer une entité
root-entity-updated = Manifeste runtime mis à jour
default-config-root-populated = Valeur par défaut de /config/root renseignée au démarrage
default-config-root-no-root-entity = Impossible de renseigner /config/root par défaut au démarrage : l'entité #root n'est pas chargée
default-config-root-no-root-cid = Impossible de renseigner /config/root par défaut au démarrage : aucun CID racine du manifeste n'est disponible
default-config-root-inspect-failed = Impossible d'inspecter le manifeste avant de renseigner /config/root par défaut
default-config-root-populate-failed = Impossible de renseigner /config/root par défaut au démarrage
entity-created = Entité créée
entity-reloaded = Plugin d'entité rechargé
entity-deleted = Entité supprimée
entity-states-saving = Sauvegarde des états des entités sur IPFS
entity-state-saving = Sauvegarde de l'état de l'entité
entity-state-saved = État de l'entité sauvegardé
entity-state-empty = Le plugin a retourné un état vide, sauvegarde ignorée
entity-states-saved = États des entités sauvegardés
link-set = Lien défini
ftl-loaded = Messages de langue chargés depuis IPFS

# Premier démarrage / auto-init
no-config-found = Aucune configuration trouvée.
initialising-new-identity = Initialisation d'une nouvelle identité runtime.
generated-headless-config = Configuration headless générée.

# Propriété
runtime-claimed = Runtime enregistré.

# Éléments racine protégés
refuse-delete-root = Refuse catégoriquement de supprimer un élément racine requis
no-root-acl = Aucune ACL racine configurée — le runtime fonctionne sans contrôle d'accès
acl-owners-access = Accès accordé à l'appelant en tant que membre de +owners
runtime-claim-persisted = Propriétaire écrit dans la configuration.
runtime-already-claimed = Runtime déjà enregistré.


# Namespace creation (:create)
crud-message-received = Message CRUD reçu
crud-acl-updated = ACL de transport racine mise à jour

# CRUD validation errors
blob-value-ipfs-path = la valeur blob doit être un chemin IPFS (/ipfs/, /ipns/ ou /ipld/)
acl-value-ipfs-path = la valeur ACL doit être un chemin IPFS (/ipfs/, /ipns/ ou /ipld/)
kind-value-ipfs-path = la valeur kind doit être un chemin IPFS (/ipfs/, /ipns/ ou /ipld/)
kind-not-found = Type introuvable
cidv1-required = la valeur doit être un CIDv1 brut (commence par 'b' ; CIDv0 'Qm…' non accepté)
config-key-protected = la clé de config '%key%' est protégée
config-key-no-delete = la clé de config '%key%' du démon ne peut pas être supprimée
config-key-not-manifest = la clé de config '%key%' n'est pas une clé de manifest config connue
owners-value-not-list = la valeur owners doit être une liste de DIDs, pas une valeur unique
wrong-crud-protocol = mauvais protocole CRUD : %type%
entity-name-invalid = le nom d'entity doit être en UTF-8 imprimable
reserved-entity-name = le nom d'entity '%name%' est réservé
genesis-kind-owner-only = Seul un propriétaire du runtime peut créer un entity de type genesis

# IPv6 config
ipv6-enabled = IPv6 activé — liaison IPv4 et IPv6 simultanée
ipv6-disabled = IPv6 désactivé — liaison IPv4 uniquement (redémarrage requis pour réactiver)
ipv6-enable-restart-required = Enregistré. Un redémarrage est requis pour que ce changement prenne effet.
ipv6-enable-unchanged = ipv6_enable est déjà défini sur cette valeur — aucune modification.
