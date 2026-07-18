# ma-runtime – मराठी
lang-name = मराठी

own-did-published = स्वतःचा DID दस्तऐवज IPNS वर प्रकाशित झाला
own-did-publish-failed = स्वतःचा DID दस्तऐवज प्रकाशित करण्यात अयशस्वी
own-did-publish-timeout = स्वतःच्या DID दस्तऐवज प्रकाशनाचा वेळ 2 मिनिटांनंतर संपला
started = ma runtime सुरू झाले
shutdown-requested = बंद करण्याची विनंती केली
closing-endpoint = iroh एंडपॉइंट बंद होत आहे...
shutdown-complete = बंद करणे पूर्ण झाले
status-listening = स्टेटस सर्व्हर ऐकत आहे
rpc-message-received = RPC संदेश प्राप्त झाला
rpc-message-rejected = RPC संदेश नाकारला गेला
ipfs-message-rejected = IPFS संदेश नाकारला गेला
ctrlc-handler-failed = Ctrl-C हँडलर अयशस्वी झाला
node-connected = नोड प्रोटोकॉलशी जोडले
received-encrypted-ma-msg = /ma/ipfs/0.0.1 वर एन्क्रिप्टेड ma संदेश प्राप्त झाला
unknown-rpc-atom = अज्ञात RPC अणू, दुर्लक्ष करा
rpc-not-text-atom = RPC माहिती मजकूर अणू नाही
rpc-unknown-verb = अज्ञात RPC क्रिया
rpc-reply-sent = RPC उत्तर पाठवले
ping-received = :ping प्राप्त, :pong पाठवत आहे
did-publish-request-received = DID दस्तऐवज प्रकाशन विनंती प्राप्त झाली
document-published = दस्तऐवज प्रकाशित झाला
did-publish-cid-reply-sent = DID प्रकाशनासाठी CID उत्तर पाठवले
did-publish-resolve-failed = ipfs-publish उत्तर देण्यासाठी प्रेषक सोडवण्यात अयशस्वी
ipfs-store-request-received = IPFS स्टोरेज विनंती प्राप्त झाली
ipfs-stored = सामग्री IPFS वर संग्रहित केली
ipfs-store-cid-reply-sent = CID उत्तर पाठवले
ipfs-store-resolve-failed = ipfs-store उत्तर देण्यासाठी प्रेषक सोडवण्यात अयशस्वी

# एंटिटी डिस्पॅच
bootstrap-complete = Bootstrap पूर्ण झाले
entity-loaded = एंटिटी प्लगइन लोड झाला
entity-load-failed = एंटिटी प्लगइन लोड करण्यात अयशस्वी
entity-not-found = एंटिटी सापडली नाही, RPC दुर्लक्षित
entity-dispatched = RPC एंटिटीला पाठवले
entity-replied = एंटिटीने RPC उत्तर पाठवले
root-create-entity = #root: एंटिटी तयार करा
root-list-entities = #root: एंटिटी यादी
root-delete-entity = #root: एंटिटी हटवा
root-entity-updated = Runtime मेनिफेस्ट अपडेट झाले
default-config-root-populated = स्टार्टअपवेळी डीफॉल्ट /config/root भरले गेले
default-config-root-no-root-entity = स्टार्टअपवेळी डीफॉल्ट /config/root भरता येत नाही: #root entity लोड झालेली नाही
default-config-root-no-root-cid = स्टार्टअपवेळी डीफॉल्ट /config/root भरता येत नाही: manifest root CID उपलब्ध नाही
default-config-root-inspect-failed = डीफॉल्ट /config/root भरण्यापूर्वी manifest तपासता आला नाही
default-config-root-populate-failed = स्टार्टअपवेळी डीफॉल्ट /config/root भरणे अयशस्वी झाले
entity-created = एंटिटी तयार झाली
entity-reloaded = एंटिटी प्लगइन पुन्हा लोड झाला
entity-deleted = एंटिटी हटवली
entity-states-saving = IPFS वर एंटिटी स्थिती जतन होत आहेत
entity-state-saving = एंटिटी स्थिती जतन होत आहे
entity-state-saved = एंटिटी स्थिती जतन झाली
entity-state-empty = प्लगइनने रिकामी स्थिती परत केली, जतन वगळले
entity-states-saved = एंटिटी स्थिती जतन झाल्या
link-set = लिंक सेट केला
ftl-loaded = IPFS वरून भाषा संदेश लोड झाले

# पहली सुरुवात / स्वयं-प्रारंभ
no-config-found = कॉन्फिगरेशन सापडले नाही.
initialising-new-identity = नवीन runtime ओळख सुरू करत आहे.
generated-headless-config = हेडलेस कॉन्फिगरेशन तयार झाले.

# मालकी
runtime-claimed = Runtime नोंदवले.

# संरक्षित मूळ घटक
refuse-delete-root = आवश्यक मूळ घटक हटवण्यास ठामपणे नकार
no-root-acl = मूळ ACL कॉन्फिगर केलेले नाही — runtime प्रवेश नियंत्रणाशिवाय चालत आहे
acl-owners-access = कॉलरला +owners चा सदस्य म्हणून प्रवेश दिला गेला
runtime-claim-persisted = मालक कॉन्फिगरेशनमध्ये लिहिला.
runtime-already-claimed = Runtime आधीच नोंदवले आहे.


# Namespace creation (:create)
crud-message-received = CRUD संदेश प्राप्त झाला
crud-acl-updated = रूट ट्रान्सपोर्ट ACL अपडेट केले

# CRUD validation errors
blob-value-ipfs-path = blob मूल्य IPFS मार्ग (/ipfs/, /ipns/, किंवा /ipld/) असणे आवश्यक आहे
acl-value-ipfs-path = ACL मूल्य IPFS मार्ग (/ipfs/, /ipns/, किंवा /ipld/) असणे आवश्यक आहे
kind-value-ipfs-path = kind मूल्य IPFS मार्ग (/ipfs/, /ipns/, किंवा /ipld/) असणे आवश्यक आहे
kind-not-found = प्रकार आढळला नाही
cidv1-required = मूल्य एक साधे CIDv1 असणे आवश्यक आहे ('b' पासून सुरू; CIDv0 'Qm…' स्वीकार्य नाही)
config-key-protected = config की '%key%' संरक्षित आहे
config-key-no-delete = daemon config की '%key%' हटविता येत नाही
config-key-not-manifest = config की '%key%' हे ज्ञात manifest config की नाही
owners-value-not-list = owners मूल्य DIDs ची यादी असणे आवश्यक आहे, एकच मूल्य नव्हे
wrong-crud-protocol = चुकीचा CRUD प्रोटोकॉल: %type%
entity-name-invalid = entity चे नाव मुद्रण करण्यायोग्य UTF-8 असणे आवश्यक आहे
reserved-entity-name = entity चे नाव '%name%' राखीव आहे
genesis-kind-owner-only = फक्त runtime चा मालकच genesis प्रकारची entity तयार करू शकतो

# IPv6 config
ipv6-enabled = IPv6 सक्षम — IPv4 आणि IPv6 दोन्हींशी बांधणी होत आहे
ipv6-disabled = IPv6 अक्षम झाले — फक्त IPv4 बाइंड होत आहे (पुन्हा सक्षम करण्यासाठी restart आवश्यक आहे)
ipv6-enable-restart-required = जतन केले. हा बदल लागू होण्यासाठी restart आवश्यक आहे.
ipv6-enable-unchanged = ipv6_enable आधीच त्या मूल्यावर सेट आहे — कोणताही बदल नाही.
