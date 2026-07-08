# ma-runtime – 日本語
lang-name = 日本語

own-did-published = 自身の DID ドキュメントを IPNS に公開しました
own-did-publish-failed = 自身の DID ドキュメントの公開に失敗しました
own-did-publish-timeout = 自身の DID ドキュメントの公開が 2 分後にタイムアウトしました
started = ma runtime が起動しました
shutdown-requested = シャットダウンが要求されました
closing-endpoint = iroh エンドポイントを閉じています...
shutdown-complete = シャットダウンが完了しました
status-listening = ステータスサーバーがリッスン中です
rpc-message-received = RPC メッセージを受信しました
rpc-message-rejected = RPC メッセージが拒否されました
ipfs-message-rejected = IPFS メッセージが拒否されました
ctrlc-handler-failed = Ctrl-C ハンドラーが失敗しました
node-connected = ノードがプロトコルに接続しました
received-encrypted-ma-msg = /ma/ipfs/0.0.1 で暗号化された ma メッセージを受信しました
unknown-rpc-atom = 不明な RPC アトム、無視します
rpc-not-text-atom = RPC データはテキストアトムではない
rpc-unknown-verb = 未知の RPC 動詞
rpc-reply-sent = RPC 返信を送信しました
ping-received = :ping を受信しました、:pong を送信します
did-publish-request-received = DID ドキュメントの公開リクエストを受信しました
document-published = ドキュメントを公開しました
did-publish-cid-reply-sent = DID 公開の CID 返信を送信しました
did-publish-resolve-failed = ipfs-publish 返信の配信のために送信者を解決できませんでした
ipfs-store-request-received = IPFS ストレージリクエストを受信しました
ipfs-stored = コンテンツを IPFS に保存しました
ipfs-store-cid-reply-sent = CID 返信を送信しました
ipfs-store-resolve-failed = ipfs-store 返信の配信のために送信者を解決できませんでした

# エンティティのディスパッチ
bootstrap-complete = Bootstrap が完了しました
entity-loaded = エンティティプラグインを読み込みました
entity-load-failed = エンティティプラグインの読み込みに失敗しました
entity-not-found = エンティティが見つかりません、RPC を無視します
entity-dispatched = RPC をエンティティにディスパッチしました
entity-replied = エンティティが RPC 返信を送信しました
root-create-entity = #root: エンティティを作成
root-list-entities = #root: エンティティ一覧
root-delete-entity = #root: エンティティを削除
root-entity-updated = Runtime マニフェストが更新されました
entity-created = エンティティが作成されました
entity-reloaded = エンティティプラグインを再読み込みしました
entity-deleted = エンティティが削除されました
entity-states-saving = エンティティの状態を IPFS に保存しています
entity-state-saving = エンティティの状態を保存しています
entity-state-saved = エンティティの状態が保存されました
entity-state-empty = プラグインが空の状態を返しました、保存をスキップします
entity-states-saved = エンティティの状態が保存されました
link-set = リンクが設定されました
ftl-loaded = IPFS から言語メッセージを読み込みました

# 初回起動 / 自動初期化
no-config-found = 設定が見つかりません。
initialising-new-identity = 新しい runtime アイデンティティを初期化しています。
generated-headless-config = ヘッドレス設定が生成されました。

# 所有権
runtime-claimed = Runtime が登録されました。

# 保護されたルート要素
refuse-delete-root = 必要なルート要素の削除を断固として拒否します
no-root-acl = ルート ACL が設定されていません — runtime はアクセス制御なしで動作しています
acl-owners-access = 呼び出し元は +owners のメンバーとしてアクセスが許可されました
runtime-claim-persisted = 所有者が設定に書き込まれました。
runtime-already-claimed = Runtime はすでに登録されています。


# Namespace creation (:create)
crud-message-received = CRUDメッセージを受信
crud-acl-updated = ルートトランスポートACLを更新

# CRUD validation errors
blob-value-ipfs-path = blobの値はIPFSパス（/ipfs/、/ipns/、または/ipld/）である必要があります
acl-value-ipfs-path = ACLの値はIPFSパス（/ipfs/、/ipns/、または/ipld/）である必要があります
kind-value-ipfs-path = kindの値はIPFSパス（/ipfs/、/ipns/、または/ipld/）である必要があります
kind-not-found = 種別が見つかりません
cidv1-required = 値はベアCIDv1でなければなりません（'b'で始まる; CIDv0 'Qm…'は受け付けません）
config-key-protected = configキー '%key%' は保護されています
config-key-no-delete = daemonのconfigキー '%key%' は削除できません
config-key-not-manifest = configキー '%key%' は既知のmanifest configキーではありません
owners-value-not-list = owners の値は DID のリストでなければならず、単一の値ではいけません
wrong-crud-protocol = 不正なCRUDプロトコル: %type%
entity-name-invalid = entity名は印刷可能なUTF-8でなければなりません
reserved-entity-name = entity名 '%name%' は予約済みです
genesis-kind-owner-only = genesis 種別の entity を作成できるのは runtime の所有者のみです

# IPv6 config
ipv6-enabled = IPv6 有効 — IPv4 と IPv6 の両方にバインド中
ipv6-disabled = IPv6 が無効になりました — IPv4 のみをバインドしています（再有効化には restart が必要です）
ipv6-enable-restart-required = 保存しました。この変更を反映するには restart が必要です。
ipv6-enable-unchanged = ipv6_enable はすでにその値に設定されています — 変更はありません。
