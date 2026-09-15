---
title: 設定
description: guru-master、guru-worker、manage-tool、ダッシュボードのすべてのフラグと環境変数。
---

各バイナリは同じ値を CLI フラグまたは環境変数から受け取ります。両方が与えられた場合はフラグが優先されます。

## `guru-master`

| フラグ | 環境変数 | デフォルト |
|---|---|---|
| `--mode` | `GURU_WORKER_MODE` | `dashboard_grpc` |
| `--dashboard-addr` | `GURU_DASHBOARD_GRPC_ADDR` | `0.0.0.0:50051` |
| `--workers-addr` | `GURU_WORKERS_GRPC_ADDR` | `0.0.0.0:50052` |
| `--address` | `SURREALDB_HOST` | `ws://127.0.0.1:8000` |
| `--username` | `SURREALDB_USER` | `root` |
| `--password` | `SURREALDB_PASSWORD` | `root` |
| `--namespace` | `SURREALDB_NAMESPACE` | *必須* |
| `--database` | `SURREALDB_NAME` | *必須* |
| `--amqp-uri` | `AMQP_URI` | *すべてのモードで必須* |
| `--watch-poll-ms` | `GURU_WATCH_POLL_MS` | `1000`（1 以上である必要があります） |
| `--log-level` | `GURU_LOG_LEVEL` | `info` |
| — | `GURU_MASTER_KEY` | *`dashboard_grpc`、`workers_grpc`、`consumer` では必須*（環境変数のみ。32 バイトのランダム値を base64 エンコードしたもの — `manage-tool generate-master-key`） |

オペレーターが調整できるそれ以外の設定 — ヘルスのしきい値と保持期間、デフォルトの ACME ディレクトリ、
更新ウィンドウ、各定期ジョブの実行頻度 — は、環境変数ではなくデータベースに保存されます。
[モジュール設定](#モジュール設定)を参照してください。

`--mode` は `dashboard_grpc`、`workers_grpc`、`consumer`、`cron` を受け付けます。ブローカー URI は
`amqp://guru:guru@127.0.0.1:5672/` のような形式で、末尾の `/` がデフォルトの vhost を選択します。ブローカーは
`cron` を含む**すべて**のモードで必須です。定期処理はメッセージとして publish されるため、ブローカーが停止すると、
復旧するまで導出・liveness・証明書更新が止まります。

`GURU_MASTER_KEY` は保存時のすべてのシークレット — DNS プロバイダーの API トークン、ACME アカウントキー、
証明書と CA の秘密鍵 — を暗号化するもので、シークレットを読み取る 3 つのモード
`dashboard_grpc`、`workers_grpc`、`consumer` では必須です。`cron` はシークレットに触れず、このキーも読みません。
これをフラグにしていないのは意図的です。argv はプロセス一覧から見えてしまいます。キーを失うと、すべての
DNS プロバイダートークンを再入力し、すべての証明書を再発行することになります。また、キーをその場で変更する
ことはサポートされていません。

データベース関連の引数が必要なのは、接続を開く 3 つのモードだけです。`cron` はこれらを無視し、
ネームスペースなしで起動します。ネームスペースがなければ起動を拒む時計は、使われないだけの
データベース依存を抱えることになるからです。

### スケジューリングと実行

`cron` は時計であり、それ以外の何物でもありません。5 秒ごとにスキャンし、期限を迎えたジョブごとに実行シグナルを
1 件 publish するだけで、データベース接続は開きません。`consumer` はシグナルごとに永続キューを 1 つバインドして
パスを実行します。`CanvasDirty` の導出フックの隣で動くため、定期処理は編集とまったく同じようにスケールし、
リトライされ、フェイルオーバーします。そして、ハングしたジョブが時計を止めることはありません。

| ルーティングキー | キュー | publish 間隔 | 実行の最短間隔 | パスの内容 |
|---|---|---|---|---|
| `derive_stale_canvases` | `guru_orchestration_derive_stale_canvases` | 30 秒 | `sweep_interval_secs`（30） | `generation` が `derived_generation` を追い越したすべてのキャンバスを再導出します |
| `sweep_liveness` | `guru_orchestration_sweep_liveness` | 30 秒 | `liveness_interval_secs`（30） | `health_report_interval_secs × health_offline_after_intervals` の間レポートのないサーバーを `Offline` にします |
| `trim_health_history` | `guru_orchestration_trim_health_history` | 300 秒 | `health_retention_interval_secs`（300） | `server_health_ttl_secs` / `node_health_ttl_secs` より古い `server_health_record` / `node_health_record` 行を削除します |
| `renew_certificates` | `guru_orchestration_renew_certificates` | 60 秒 | `acme_interval_secs`（60） | ACME の発行と更新: 有効期限の `acme_renew_before_secs` 前に更新し、失敗した試行は `acme_retry_after_secs` 後に再試行します |
| `rotate_relay_certificates` | `guru_orchestration_rotate_relay_certificates` | 3600 秒 | `relay_rotation_interval_secs`（3600） | 有効期限まで `relay_cert_renew_before_secs` 以内になったリレーのリーフ証明書を再発行し、そのキャンバスを再導出します |

層は 2 つあり、しかも同じ数字ではありません。スケジューラーは設定をまったく読まないため、真ん中の列の固定周期で
publish します。一方 consumer は、何かを始める前に各実行を claim します — ジョブごとに `orchestration_job_run` 行を
1 つ使い、compare-and-set で、設定された間隔につきフリート全体で最大 1 回だけです。間隔はシグナルが運ぶ
*スケジューリングのティック*の間で測られ、consumer が処理に取りかかれた時刻の間ではありません。そのため、
混み合った consumer が知らぬ間に周期を伸ばすことはありません。したがって、シグナルの周期以下の間隔は
「毎シグナル」を意味し、それより大きい間隔はフリート全体でジョブを遅くします。そして重複配信や再配信された
メッセージは、実行されるのではなく拒否されます。すでに実行されたティックは二度と claim できないからです。
これらの間隔は保存された `orchestration` キーの値なので、変更はそのキーの他の設定と同じく consumer の
再起動時に反映されます。

サーバーは、desired リビジョンに `degraded_grace_secs` を超えて遅れている間、または最後に ack された
リビジョンがいずれかの pod で失敗している間、`Degraded` になります。ここで挙げた値は保存された
`orchestration` 設定のキーであり、フラグではありません。

## `guru-worker`

| フラグ | 環境変数 | デフォルト |
|---|---|---|
| `-c`, `--config` | `GURU_WORKER_CONFIG` | —（スタンドアロンモード。`SIGHUP` で再読み込み） |
| `--master` | `GURU_MASTER` | —（エージェントモード。`--server` が必要。`http://host:50052` は平文の h2c、`https://host` はシステムのルート証明書で検証する TLS） |
| `--server` | `GURU_SERVER_ID` | —（`orchestration_server` のレコードキー） |
| `--api-key-file` | `GURU_API_KEY_FILE` | —（`GURU_API_KEY` の代替） |
| `--state-dir` | `GURU_STATE_DIR` | `/var/lib/guru-worker` |
| `--health-interval` | `GURU_HEALTH_INTERVAL_SECS` | `15`（ヘルスレポートの送信間隔（秒）。エージェントモード。1 以上である必要があります） |
| `--public-ipv4-urls` | `GURU_PUBLIC_IPV4_URLS` | `https://checkip.amazonaws.com,https://api.ipify.org,https://ipv4.icanhazip.com`（エージェントモード。呼び出し元の IPv4 をテキストで返すプロバイダーをカンマ区切りで指定します。開始位置をローテーションしながら順に試し、それぞれ 3 秒。60 秒ごとに再確認し、変化したら報告します。空にするとルックアップを無効化しますが、インターフェイスのアドレスは引き続き報告されます） |
| `--public-ipv6-urls` | `GURU_PUBLIC_IPV6_URLS` | `https://ipv6.icanhazip.com,https://api6.ipify.org,https://v6.ipinfo.io/ip`（IPv6 についての同じ設定） |
| `--geo-url` | `GURU_GEO_URL` | `https://ipinfo.io/country`（エージェントモード。パブリックアドレスの 2 文字の国コードを返し、サーバーの横に表示されます。空にすると無効化） |
| `--log-level` | `GURU_LOG_LEVEL` | `info` |

`--config` と `--master` は相互排他で、どちらも指定しない場合、ワーカーはデフォルトパス
`/etc/guru-worker/config.toml` に対してスタンドアロンで動作します。`--log-level`/`GURU_LOG_LEVEL` が設定するのは
エージェントモードだけです。スタンドアロンでは代わりに設定ファイルの `log.level` を読み（フラグは一切参照しません）。
エージェントモードはオペレーター API キーを `GURU_API_KEY` から、または `--api-key-file` で指定したファイルから
読み込みます（末尾の空白は取り除かれます）。このキーは、マスターへの登録時にセッションごとに 1 回だけ使われます。

## ワーカー設定ファイル

上のフラグはプロセスの話で、こちらはトラフィックの話です。ファイルは TOML で、モデルは
`lib/guru_worker_config` が定義しており、両モードで*同一*のモデルです。`guru-master` はキャンバスから導出して
ストリーミングし、スタンドアロンワーカーはディスクから読み込みます。つまり、スタンドアロンワーカーが受け付ける
ファイルは、コントロールプレーンが送るものとまったく同じです。

トップレベル:

| キー | デフォルト | 値 |
|---|---|---|
| `ipv6_resolve` | `"tolerated"` | `required`、`preferred`、`tolerated`、`forbidden` — 宛先がドメイン名のときのアドレスファミリーポリシー |
| `log.level` | `"info"` | `tracing` の `EnvFilter` ディレクティブ — `info`、`debug`、あるいは `guru_worker=debug,warn` のように対象を絞った指定。**起動時に一度だけ**読まれるため、再読み込みでは変わりません |
| `[[forwarding]]` | `[]` | リスナー 1 つにつき 1 エントリ。エントリが 1 つもないファイルも有効で、その場合は何もしません |

スタンドアロンモードでプロセスのログを設定するのは `log.level` です。`--log-level`/`GURU_LOG_LEVEL` が
適用されるのはエージェントモードだけです。

`ipv6_resolve` はグローバルで、コンパイルされるすべてのターゲットに引き継がれます。`required`/`forbidden` は
もう一方のファミリーを名前解決の失敗として扱い、`preferred`/`tolerated` は両方が解決できたときに優先する側を
選び、そうでなければもう一方にフォールバックします。

### `[[forwarding]]`

| キー | 必須 | 値 |
|---|---|---|
| `tag` | はい | 自由形式の名前。ログ、lint の出力、適用エラーに現れるのはこの名前です |
| `listen` | はい | `ip:port` — リテラルのアドレスで、ホスト名は不可（`0.0.0.0:443`、`[::]:443`） |
| `receive_proxy_protocol` | いいえ | `"v1"` または `"v2"` — クライアントのペイロードの前に PROXY ヘッダーを期待します |
| `listen_as` | はい | 受け付け方: `"raw"`、または `tls` / `relay` テーブル（後述） |
| `to` | はい | 転送先: `[forwarding.to]` テーブル（後述） |

リスナーは `(listen, transport)` でキー付けされ、transport が QUIC になるのは `quic` リレーリスナーの場合だけです。
したがって、一方が QUIC（UDP）で他方が TCP であれば、2 つのエントリが同じ `ip:port` を共有できます。それ以外は
`duplicate listener` エラーになります。

`receive_proxy_protocol` はスイッチであり、厳密なバージョンチェックではありません。ヘッダーは自動検出されるため、
`"v1"` のエントリでも v2 ヘッダーを受け付けます。これが変えるのは、ヘッダーを*そもそも期待するかどうか*であり、
したがってワーカーがその接続に紐づけるクライアントアドレス（ログ、`ip_hash`、そして次段へ書き出すヘッダーで
使われるもの）が本当のクライアントなのか、手前のプロキシなのかが決まります。意味を持つのは `raw` と `tls` の
リスナーだけで、`relay` リスナーは常にヘッダーを読みます（後述）。

### `listen_as`

```toml
listen_as = "raw"                     # プレーン TCP、ペイロードはそのまま
```

```toml
[forwarding.listen_as.tls]            # ここで TLS を終端し、平文で転送する
key = "/etc/guru-worker/tls/key.pem"
full_chain = "/etc/guru-worker/tls/fullchain.pem"
```

```toml
[forwarding.listen_as.relay]          # 別の guru ワーカーからの受信
relay_type = "tcp"                    # "tcp" | "tls" | "quic"
# relay_type = "tls" と "quic" では追加で次が必要:
# key = "/etc/guru-worker/tls/key.pem"
# full_chain = "/etc/guru-worker/tls/fullchain.pem"
```

`key` と `full_chain` は PEM ファイルのパスで、設定を適用するときにパースされます。これが、証明書の更新を
再起動ではなく再読み込みで済ませられる理由です。スタンドアロンモードでは、これらを用意する仕組みはありません。
エージェントモードでは、マスターがすべてのリビジョンにこれらを同梱し（`ConfigRevision.files`）、`--state-dir` からの
相対パスとして渡します — TLS 付きの Entry には `certs/acme/<certificate>/{full_chain,key}.pem`、`tls`/`quic` の
リレーリスナーには `certs/relay/<pod>/{full_chain,key}.pem`、内部 CA には `certs/ca.pem` — ワーカーは適用前に
これらを書き出します（鍵ファイルは `0600`、各ディレクトリはアトミックに差し替え）。ファイル内の相対パスは
`--state-dir` を基準に解決されます。

トップレベルの `relay_ca = "certs/ca.pem"` は、`tls`/`quic` のリレー*ダイアラー*が対向を検証するための CA 証明書を
指定します。未設定ならシステムのルート証明書を使います。マスターは内部 CA が存在するとき
（`manage-tool orchestration init-ca`）に必ずこれを設定し、リレーのリーフ証明書は両端で SNI
`<pod-key>.relay.guru.internal` を使います。

`relay` リスナーは `to.type = "relay"` ホップの受信側であり、公開エントリポイントではありません。デコードした
ストリームから**常に** PROXY ヘッダーを読みます（上流のホップが本当のクライアントアドレスを伝える手段が
これです）。そのため、そこでは `receive_proxy_protocol` は無関係です。

### `[forwarding.to]`

```toml
[forwarding.to]
type = "exit"                         # 最終ホップ: 実際のバックエンドへ接続する
destination = "10.0.0.5:8080"         # ip:port または domain:port
send_proxy_protocol = "v2"            # 任意: "v1" | "v2"
```

```toml
[forwarding.to]
type = "relay"                        # 次の guru ホップ
protocol = "tcp"                      # "tcp" | "tls" | "quic"
destination = "hop.example.com:9443"
sni = "hop.example.com"               # "tls" と "quic" では必須
```

```toml
[forwarding.to]
type = "load_balance"
strategy = "round_robin"              # "round_robin" | "random" | "ip_hash" | "fallback"

[[forwarding.to.members]]             # メンバー自体が `to` ノードなので …
type = "exit"
destination = "10.0.0.6:8080"

[[forwarding.to.members]]             # … グループはネストできる: さらに `.members` を追加する
type = "load_balance"
strategy = "fallback"

[[forwarding.to.members.members]]
type = "exit"
destination = "backend.internal:8080"
```

`destination` はどちらの形式でも `host:port` です。可能な場合はリテラルのソケットアドレスとしてパースされ
（IPv6 は角括弧: `[2001:db8::1]:8080`）、そうでなければドメイン名として扱われ、その場合は接続ごとに
`ipv6_resolve` に従って解決されます。`fallback` はメンバーを順に試して最初に接続できたものを使い、他の
ストラテジーは 1 つを選びます。リレーホップは常に次のワーカーへ PROXY v2 ヘッダーを書き込みます。
`send_proxy_protocol` が任意指定として存在するのは `exit` だけで、対向が他人のバックエンドになるのは
そこだけだからです。

### 拒否されるもの、警告だけで済むもの

以下のいずれかに当てはまると読み込みは失敗し、ファイルが部分的に適用されることはありません:

| エラー | 原因 |
|---|---|
| `parse toml: TOML parse error at line N …` | どのテーブルも未知のキーを拒否します。タイポは黙ってデフォルトになるのではなく、エラーです |
| `duplicate listener <addr> (<tag>)` | 2 つのエントリが同じトランスポートで同じ `ip:port` を要求しています |
| `forwarding <tag> relay to tls/quic requires sni` | `sni` のない `tls`/`quic` リレーホップ（ネストの深さは問いません） |
| `forwarding <tag> has an empty load-balance group` | `members = []`（ネストの深さは問いません） |
| `invalid remote '…'` / `invalid port in remote '…'` | `destination` が `host:port` になっていません |

次のものは警告としてログに記録され、動作は継続します:

- メンバーがちょうど 1 つのロードバランスグループ（そのグループには意味がありません）
- `receive_proxy_protocol` を設定していないリスナーの配下のどこかにある `ip_hash`。この警告を文字どおり
  受け取るべきなのは `raw` と `tls` のリスナーの場合だけです。そこではすべての接続がワーカーに見える
  アドレス — つまり手前のプロキシのアドレス — をハッシュするため、「バランス」は 1 つのメンバーに
  集約されてしまいます。`relay` リスナーは必須のリレーヘッダーを常に読むので、本当のクライアントを
  ハッシュしており、そこではこの警告は誤検知です。lint はこの 2 つのケースを区別しません。

パース済みの設定でも、適用はエントリ単位で失敗することがあります — 証明書ファイルがない、別のプロセスが
すでにアドレスをバインドしている、など — その場合は `<tag>: <reason>` として報告されます。これは設定エラーでは
なく適用エラーで、しかも*pod 単位*です。他のすべての `[[forwarding]]` はコミットされ、失敗したものは以前の
リスナー（あるいは何もない状態）をそのまま維持します。スタンドアロンモードでは起動時の失敗は中断となり、
`SIGHUP` による再読み込みの失敗は失敗したエントリをログに出して以前のリスナーを維持します。エージェント
モードでは各 pod の結果がマスターに ack され、マスターは失敗した pod をサーバーと該当ノードに記録します。

## `manage-tool`

グローバルフラグは `guru-master` のデータベースオプションと同じです: `--address`（`SURREALDB_HOST`）、
`--username`（`SURREALDB_USER`）、`--password`（`SURREALDB_PASSWORD`）、`--namespace`（`SURREALDB_NAMESPACE`）、
`--database`（`SURREALDB_NAME`）。

| サブコマンド | 用途 |
|---|---|
| `create-admin --email <email> --password <password>` | 最初の管理者アカウントをブートストラップします |
| `generate-master-key` | 新しい `GURU_MASTER_KEY` を出力します（データベースは不要） |
| `config seed` | 値が保存されていないすべてのキーにデフォルト値を書き込みます。編集済みのキーには触れません |
| `config list` | すべてのキーと保存されているドキュメント（未保存ならデフォルト値）を出力します |
| `config get <key>` | 1 つのキーの保存されたドキュメントをデコードせずに出力します — 壊れていても読めます |
| `config set <key> <json>` | 1 つのキーの保存 JSON を置き換えます（書き込む前に検証されます） |
| `orchestration export-config --server <key>` | 1 つのサーバー向けに導出された `guru-worker` の TOML を出力します |
| `orchestration init-ca` | リレーの TLS/QUIC リンク用の内部 CA を作成し、その証明書を出力します。既存のものを置き換えることは拒否します（`GURU_MASTER_KEY` が必要） |

## ダッシュボード

| 環境変数 | デフォルト | 用途 |
|---|---|---|
| `GURU_GRPC_URL` | `127.0.0.1:50051` | `guru-master --mode dashboard_grpc` のダッシュボード gRPC エンドポイント |
| `PROTOCOL_HEADER` | — | 公開スキームを伝えるヘッダー（例: `x-forwarded-proto`）。**未設定の場合、アプリは `https` を前提とします** |
| `HOST_HEADER` | — | 公開ホストを伝えるヘッダー（例: `x-forwarded-host`。デフォルト以外のポートを含める必要があります） |
| `ADDRESS_HEADER` | — | クライアント IP を伝えるヘッダー（例: `x-forwarded-for`） |
| `BODY_SIZE_LIMIT` | `512K` | リクエストボディの最大サイズ |

サーバーは `:3000` で待ち受けます。各リクエストのオリジンはこれらのヘッダーから再構築され、ブラウザの
`Origin` がそれと一致しない POST は `403 Cross-site remote requests are forbidden` で拒否されます。そのため、
平文 HTTP やポートを付け替えるプロキシの背後では `PROTOCOL_HEADER` と `HOST_HEADER` の両方が必須です。
`ORIGIN` は実行時には**読まれません**。Node アダプターがビルド時に `kit.paths.origin` を埋め込むためです。
セッションクッキーは `Secure` 付きで発行されるため、ダッシュボードは（`localhost` を除き）HTTPS で配信する
必要があります。

## モジュール設定

オペレーターが調整できる設定はデータベースに置かれ、キーごとに `app_config` の 1 行が設定全体を JSON
ドキュメントとして保持します。データベースが唯一の真実の源で、キャッシュも二重のコピーもありません。そのため、
フリート内のすべての `guru-master` は環境変数を揃えなくても同一の設定で動作し、変更には再デプロイではなく
再起動だけが必要です。現在のキーは 2 つです:

| キー | 構造体 | 内容 |
|---|---|---|
| `auth` | `auth::config::AuthConfig` | `session_idle_ttl_secs` |
| `orchestration` | `orchestration::config::OrchestrationConfig` | `health_report_interval_secs`、`health_offline_after_intervals`、`degraded_grace_secs`、`server_health_ttl_secs`、`node_health_ttl_secs`、`default_acme_directory`、`acme_renew_before_secs`、`acme_retry_after_secs`、`relay_cert_valid_secs`、`relay_cert_renew_before_secs`、`sweep_interval_secs`、`liveness_interval_secs`、`health_retention_interval_secs`、`acme_interval_secs`、`relay_rotation_interval_secs`、`trust_proxy_address_headers`（デフォルトは `true`: ワーカー API は登録元のアドレスとして `x-real-ip` または `x-forwarded-for` の最初のホップを記録します。ドキュメント化されたプロキシを経由せずに `:50052` へ到達できる場合は無効にしてください。そうでなければワーカーが偽装できてしまいます） |

`surrealkit sync` の後に `manage-tool config seed` を実行するとデフォルト値が書き込まれ、
`manage-tool config list` で保存されている内容を確認できます。`list` と `get` は行をそのまま出力し、
デコードしません。そのため、マスターの起動時の読み込みを失敗させるドキュメントでも中身を確認できます。
`set` はドキュメント全体を置き換えますが、書き込む前に設定の型へデコードされるため、部分的なペイロードは
デフォルト値で補われ、形の違うペイロードは行に届く前に拒否されます:

```sh
manage-tool config set orchestration '{"acme_renew_before_secs":1209600}'
```

5 つの `*_interval_secs` フィールドは、定期ジョブが実際に実行され得る頻度です
（[スケジューリングと実行](#スケジューリングと実行)）。これらが環境変数ではなくここにあるのは、フリート
全体で値が一致していなければならないからです。間隔を強制する claim は、すべての consumer が共有する
1 つのデータベース行です。

```sh
manage-tool config set orchestration '{"acme_interval_secs":300,"relay_rotation_interval_secs":7200}'
```

同じ 2 つのドキュメントは、ダッシュボードからも参照・置き換えできます。**Admin**（そして Admin だけです —
他のロールはこの権限を持たず、API キーが持つこともありません）は、保存されているとおりの行、seed が
書き込むはずのペイロード、そしてドキュメント全体を置き換えるフォームを見られます。検証は
`manage-tool config set` とまったく同じなので、形の違うペイロードは拒否され、行は以前の内容を保ちます。
ダッシュボードからの保存もライブリロードではありません。新しい値を反映するには**`guru-master` を再起動**
してください。

データベースを使う 3 つのモード — `dashboard_grpc`、`workers_grpc`、`consumer` — は起動時に両方のキーを
一度だけ読み、その値を各サービスへ渡します。ライブリロードはありません。`cron` はデータベース接続を開かず、
どちらのキーも読みません。実行シグナルを publish するだけで、それを受け取った consumer が保存された
間隔を適用します。seed されていない環境ではデフォルト値で動作します。デシリアライズできない行はキー名を
示して起動を失敗させます。これは意図的です。壊れたドキュメントをデフォルト値で代用すれば、オペレーターの
設定全体を黙って入れ替えてしまい、たとえば ACME をステージングから本番のディレクトリへ移してしまう
かもしれません。
後のリリースで追加されたフィールドはデフォルト値で読まれるため、古い行もそのまま動作し続けます。
