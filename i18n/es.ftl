# ma-runtime – Español
lang-name = Español

own-did-published = Documento DID propio publicado en IPNS
own-did-publish-failed = Error al publicar el documento DID propio
own-did-publish-timeout = La publicación del documento DID propio expiró después de 2 minutos
started = ma runtime iniciado
shutdown-requested = Apagado solicitado
closing-endpoint = Cerrando punto de conexión iroh...
shutdown-complete = Apagado completado
status-listening = Servidor de estado escuchando
rpc-message-received = Mensaje RPC recibido
rpc-message-rejected = Mensaje RPC rechazado
ipfs-message-rejected = Mensaje IPFS rechazado
ctrlc-handler-failed = Error en el manejador Ctrl-C
node-connected = Nodo conectado al protocolo
received-encrypted-ma-msg = Mensaje ma cifrado recibido en /ma/ipfs/0.0.1
unknown-rpc-atom = Átomo RPC desconocido, ignorando
rpc-not-text-atom = La carga RPC no es un átomo de texto
rpc-unknown-verb = Verbo RPC desconocido
rpc-reply-sent = Respuesta RPC enviada
ping-received = :ping recibido, enviando :pong
did-publish-request-received = Solicitud de publicación de documento DID recibida
document-published = Documento publicado
did-publish-cid-reply-sent = Respuesta CID enviada para publicación DID
did-publish-resolve-failed = No se pudo resolver el remitente para entregar la respuesta ipfs-publish
ipfs-store-request-received = Solicitud de almacenamiento IPFS recibida
ipfs-stored = Contenido almacenado en IPFS
ipfs-store-cid-reply-sent = Respuesta CID enviada
ipfs-store-resolve-failed = No se pudo resolver el remitente para entregar la respuesta ipfs-store

# Despacho de entidades
bootstrap-complete = Bootstrap completado
entity-loaded = Plugin de entidad cargado
entity-load-failed = Error al cargar el plugin de entidad
entity-not-found = Entidad no encontrada, ignorando RPC
entity-dispatched = RPC despachado a la entidad
entity-replied = La entidad envió respuesta RPC
root-create-entity = #root: crear entidad
root-list-entities = #root: listar entidades
root-delete-entity = #root: eliminar entidad
root-entity-updated = Manifiesto runtime actualizado
default-config-root-populated = /config/root predeterminado poblado al iniciar
default-config-root-no-root-entity = No se puede poblar /config/root predeterminado al iniciar: la entidad #root no está cargada
default-config-root-no-root-cid = No se puede poblar /config/root predeterminado al iniciar: no hay CID raíz del manifiesto disponible
default-config-root-inspect-failed = No se pudo inspeccionar el manifiesto antes de poblar /config/root predeterminado
default-config-root-populate-failed = No se pudo poblar /config/root predeterminado al iniciar
entity-created = Entidad creada
entity-reloaded = Plugin de entidad recargado
entity-deleted = Entidad eliminada
entity-states-saving = Guardando estados de entidades en IPFS
entity-state-saving = Guardando estado de entidad
entity-state-saved = Estado de entidad guardado
entity-state-empty = El plugin devolvió estado vacío, omitiendo persistencia
entity-states-saved = Estados de entidades guardados
link-set = Enlace establecido
ftl-loaded = Mensajes de idioma cargados desde IPFS

# Primer inicio / auto-init
no-config-found = No se encontró configuración.
initialising-new-identity = Inicializando nueva identidad runtime.
generated-headless-config = Configuración headless generada.

# Propiedad
runtime-claimed = Runtime registrado.

# Elementos raíz protegidos
refuse-delete-root = Me niego firmemente a eliminar un elemento raíz requerido
no-root-acl = No hay ACL raíz configurada — el runtime opera sin control de acceso
acl-owners-access = Llamante autorizado como miembro de +owners
runtime-claim-persisted = Propietario escrito en la configuración.
runtime-already-claimed = Runtime ya registrado.


# Namespace creation (:create)
crud-message-received = Mensaje CRUD recibido
crud-acl-updated = ACL de transporte raíz actualizada

# CRUD validation errors
blob-value-ipfs-path = el valor blob debe ser una ruta IPFS (/ipfs/, /ipns/ o /ipld/)
acl-value-ipfs-path = el valor ACL debe ser una ruta IPFS (/ipfs/, /ipns/ o /ipld/)
kind-value-ipfs-path = el valor kind debe ser una ruta IPFS (/ipfs/, /ipns/ o /ipld/)
kind-not-found = Tipo no encontrado
cidv1-required = el valor debe ser un CIDv1 puro (comienza con 'b'; CIDv0 'Qm…' no aceptado)
config-key-protected = la clave de config '%key%' está protegida
config-key-no-delete = la clave de config '%key%' del daemon no puede eliminarse
config-key-not-manifest = la clave de config '%key%' no es una clave de manifest config conocida
owners-value-not-list = el valor de owners debe ser una lista de DIDs, no un valor único
wrong-crud-protocol = protocolo CRUD incorrecto: %type%
entity-name-invalid = el nombre de entity debe ser UTF-8 imprimible
reserved-entity-name = el nombre de entity '%name%' está reservado
genesis-kind-owner-only = Solo un propietario del runtime puede crear un entity de tipo genesis

# IPv6 config
ipv6-enabled = IPv6 habilitado — vinculando tanto IPv4 como IPv6
ipv6-disabled = IPv6 deshabilitado — vinculando solo IPv4 (se requiere reinicio para volver a habilitar)
ipv6-enable-restart-required = Guardado. Se requiere reinicio para que este cambio surta efecto.
ipv6-enable-unchanged = ipv6_enable ya está establecido en ese valor — sin cambios.
