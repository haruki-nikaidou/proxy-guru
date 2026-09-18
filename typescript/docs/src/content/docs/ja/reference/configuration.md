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
| `--database-url` | `GURU_DATABASE_URL` | *`cron` 以外のすべてのモードで必須* |
| `--db-pool-size` | `GURU_DB_POOL_SIZE` | `10`（1 以上である必要があります） |
| `--db-statement-timeout-ms` | `GURU_DB_STATEMENT_TIMEOUT_MS` | `5000`（1 以上である必要があります） |
| `--amqp-uri` | `AMQP_URI` | *すべてのモードで必須* |
| `--redis-url` | `REDIS_URL` | *`dashboard_grpc`、`workers_grpc`、`consumer` で必須* |
| `--watch-poll-ms` | `GURU_WATCH_POLL_MS` | `1000`（1 以上である必要があります） |
| `--log-level` | `GURU_LOG_LEVEL` | `info` |
| — | `GURU_MASTER_KEY` | *`dashboard_grpc`、`workers_grpc`、`consumer` では必須*（環境変数のみ。32 バイトのランダム値を base64 エンコードしたもの — `manage-tool generate-master-key`） |
| — | `GURU_SMTP_PASSWORD` | *任意、`notifier` のみ*（環境変数のみ。未設定の場合は認証なしで送信します） |
| — | `GURU_TELEGRAM_BOT_TOKEN` | *任意、`notifier` のみ*（環境変数のみ。未設定の場合は Telegram を無効にします） |

オペレーターが調整できるそれ以外の設定 — ヘルスのしきい値と保持期間、デフォルトの ACME ディレクトリ、
更新ウィンドウ、各定期ジョブの実行頻度 — は、環境変数ではなくデータベースに保存されます。
[モジュール設定](#モジュール設定)を参照してください。

`--mode` は `dashboard_grpc`、`workers_grpc`、`consumer`、`notifier`、`cron` を受け付けます。ブローカー URI は
`amqp://guru:guru@127.0.0.1:5672/` のような形式で、末尾の `/` がデフォルトの vhost を選択します。ブローカーは
`cron` を含む**すべて**のモードで必須です。定期処理はメッセージとして publish されるため、ブローカーが停止すると、
復旧するまで導出・liveness・証明書更新が止まります。

`notifier` は[通知](/ja/features/notifications/)の配信側で、インスタンスはちょうど 1 つだけです。これは
PostgreSQL のアドバイザリロックで強制されます。起動時に `notify` 設定を読み、必要とするのはデータベースと
ブローカーだけです — `GURU_MASTER_KEY`（何も復号しません）も `REDIS_URL`（ライブイベントを publish しません）も
受け取りません。このモードが使う 2 つのチャンネルのシークレットは、上の表にある環境変数です。

Redis は、提供と導出を行う 3 つのモードで必須であり、その理由も同じ種類のものです。これは
オペレーター API の `Watch*` ストリームを支えるライブバスです。URL は `redis://127.0.0.1:6379/` のような
形式です。すべての変更はチャンネル `guru:orchestration:live` にイベントを 1 件 publish し、各
`dashboard_grpc` レプリカはこれを一度だけ subscribe します。そのため、あるレプリカに対して行われた編集は、
他のレプリカが提供しているストリームにも届きます。何も保存されません。フリートはこのサーバーに
永続化を求めず、AOF も RDB も必要としません。Redis が停止すると、開いている `Watch*` ストリームは配信を
失います — subscriber はバックオフしながら再接続し、すべての watcher にデータベースの再読み込みを求めるため、
復旧すれば古い状態が残ることはありません — が、導出やワーカーが実行する内容に影響することは決してありません。

ダッシュボードはこれらのストリームを追従しています。キャンバスエディター、ヘルスページとその Pod イベントは、
ページごとに 1 本の server-sent-events 接続を通じてその場で更新され、ダッシュボードがそれを gRPC ストリームに
橋渡しします。それらのページの *ライブ* バッジが示すのはこのブラウザー接続であり、バス自体の状態は
ログ行 `live bus connected` で確認します。

`GURU_MASTER_KEY` は保存時のすべてのシークレット — DNS プロバイダーの API トークン、ACME アカウントキー、
証明書と CA の秘密鍵 — を暗号化するもので、シークレットを読み取る 3 つのモード
`dashboard_grpc`、`workers_grpc`、`consumer` では必須です。`cron` はシークレットに触れず、このキーも読みません。
`notifier` も同様です。これをフラグにしていないのは意図的です。argv はプロセス一覧から見えてしまいます。キーを
失うと、すべての DNS プロバイダートークンを再入力し、すべての証明書を再発行することになります。また、キーを
その場で変更することはサポートされていません。

`GURU_DATABASE_URL` が必要なのは、接続を開く 4 つのモードだけです。`cron` はこれを無視し、データベース URL を
まったく持たずに起動します。URL がなければ起動を拒む時計は、使われないだけのデータベース依存を抱えることに
なるからです。接続を開くモードでは、プールが最大 `GURU_DB_POOL_SIZE` 本の接続を保持し、すべての
ステートメントはサーバー側で `GURU_DB_STATEMENT_TIMEOUT_MS` に制限されます。これを超えたステートメントは
PostgreSQL によってキャンセルされ、エッジはそれを障害ではなく `UNAVAILABLE` として報告します。また、
サービスを提供する master はそれぞれ起動時に、アドバイザリロックの下で未適用のマイグレーションをすべて
適用します。そのため、フリートが一斉に起動しても各マイグレーションはちょうど一度だけ適用されます。

### スケジューリングと実行

`cron` は時計であり、それ以外の何物でもありません。5 秒ごとにスキャンし、期限を迎えたジョブごとに実行シグナルを
1 件 publish するだけで、データベース接続は開きません。`consumer` はシグナルごとに永続キューを 1 つバインドして
パスを実行します。`CanvasDirty` の導出フックの隣で動くため、定期処理は編集とまったく同じようにスケールし、
リトライされ、フェイルオーバーします。そして、ハングしたジョブが時計を止めることはありません。

| ルーティングキー | キュー | publish 間隔 | 実行の最短間隔 | パスの内容 |
|---|---|---|---|---|
| `derive_stale_canvases` | `guru_orchestration_derive_stale_canvases` | 30 秒 | `sweep_interval_secs`（30） | `generation` が `derived_generation` を追い越したすべてのキャンバスを再導出します |
| `sweep_liveness` | `guru_orchestration_sweep_liveness` | 30 秒 | `liveness_interval_secs`（30） | `health_report_interval_secs × health_offline_after_intervals` の間レポートのないサーバーを `Offline` にします |
| `trim_health_history` | `guru_orchestration_trim_health_history` | 300 秒 | `health_retention_interval_secs`（300） | `server_health_ttl_secs` / `pod_health_ttl_secs` より古い `server_health_record` / `pod_health_record` 行を削除します |
| `renew_certificates` | `guru_orchestration_renew_certificates` | 60 秒 | `acme_interval_secs`（60） | ACME の発行と更新: 有効期限の `acme_renew_before_secs` 前に更新し、失敗した試行は `acme_retry_after_secs` 後に再試行します |
| `rotate_relay_certificates` | `guru_orchestration_rotate_relay_certificates` | 3600 秒 | `relay_rotation_interval_secs`（3600） | 有効期限まで `relay_cert_renew_before_secs` 以内になったリレーのリーフ証明書を再発行し、そのキャンバスを再導出します |
| `resolve_server_countries` | `guru_orchestration_resolve_server_countries` | 60 秒 | `country_lookup_interval_secs`（60） | 国がまだ分からないサーバーの IPv4 アドレスについて、`country_lookup_url` で国を調べます。新しいアドレスや変わったアドレスはすぐに、失敗した照会は `country_lookup_retry_after_secs` 後に再度調べます |

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
| `--log-level` | `GURU_LOG_LEVEL` | `info` |

`--config` と `--master` は相互排他で、どちらも指定しない場合、ワーカーはデフォルトパス
`/etc/guru-worker/config.toml` に対してスタンドアロンで動作します。`--log-level`/`GURU_LOG_LEVEL` が設定するのは
エージェントモードだけで、それも最初の設定（起動時は保存済みの last-good、以後は master が送るもの）が適用されるまでです。
それ以降はダッシュボードで設定したサーバーのログレベルが、再起動なしで切り替わって有効になります。スタンドアロンでは代わりに設定ファイルの `log.level` を読み（フラグは一切参照しません）。
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
| `log.level` | `"info"` | `tracing` の `EnvFilter` ディレクティブ — `info`、`debug`、あるいは `guru_worker=debug,warn` のように対象を絞った指定。起動時に適用され、再読み込みのたびに再起動なしで適用し直されます。master が導出する設定では `trace`、`debug`、`info`、`warn`、`error` のいずれかです |
| `[keepalive]` | 下記参照 | データプレーンの全接続に対する生存確認 |
| `[quic]` | 下記参照 | このワーカーが参加するすべての QUIC 中継リンクでの自分側: 輻輳制御、レート、ウィンドウ |
| `[[forwarding]]` | `[]` | リスナー 1 つにつき 1 エントリ。エントリが 1 つもないファイルも有効で、その場合は何もしません |

スタンドアロンモードでプロセスのログを設定するのは `log.level` です。`--log-level`/`GURU_LOG_LEVEL` が
適用されるのはエージェントモードだけで、最初に適用された設定の `log.level` に置き換わります。

`ipv6_resolve` はグローバルで、コンパイルされるすべてのターゲットに引き継がれます。`required`/`forbidden` は
もう一方のファミリーを名前解決の失敗として扱い、`preferred`/`tolerated` は両方が解決できたときに優先する側を
選び、そうでなければもう一方にフォールバックします。

### `[keepalive]`

TCP は「静かな相手」と「消えた相手」を区別できません。FIN も RST も送らずにネットワークから消えたクライアント
（電波を失った携帯、期限切れの NAT エントリ、電源の落ちたホスト）は、その接続を、背後のパイプ全体ごと、
ワーカー上に永遠に残します。そのためワーカーは accept した、あるいは接続した TCP ソケットすべてで
`SO_KEEPALIVE` を有効にし、カーネルがアイドルな接続をプローブして、応答のないプローブが続いたら接続を
失敗させます。QUIC の中継ホップは逆の要求を持ちます。ping がなければ、静かなストリームの下の QUIC 接続は
アイドルタイムアウトし、ただ話すことがなかっただけの長い接続が切られてしまいます。値はすべて整数の秒
（または回数）で、1 以上でなければなりません。

| キー | デフォルト | 値 |
|---|---|---|
| `tcp_idle_secs` | `60` | 最初のプローブまでのアイドル時間（`TCP_KEEPIDLE`） |
| `tcp_interval_secs` | `10` | プローブ開始後のプローブ間隔（`TCP_KEEPINTVL`） |
| `tcp_retries` | `3` | 接続を失敗とみなすまでの無応答プローブ回数（`TCP_KEEPCNT`） |
| `quic_ping_secs` | `15` | アイドルな QUIC 中継接続での ping 周期。両端が送ります |
| `quic_idle_secs` | `60` | パケットが何も届かなくなってから QUIC 中継接続を失われたとみなすまでの時間。`quic_ping_secs` より大きい必要があります |

これはアイドル制限ではありません。プローブに応答する相手は、好きなだけ接続を保てます。プローブは接続が
アイドル時間のあいだ無言だったときにだけ始まり、1 回ごとに空のセグメントを 1 つ送って相手のカーネルが
自動で応答します。どのクライアントも対応しており、その通信は途中の NAT エントリも更新します。

このセクションはデフォルトと異なるときにだけ書き出され、`guru-master` が送るのはデフォルトです。
セクションが存在する前にビルドされたワーカーは未知のキーを拒否するため、スタンドアロンのファイルでこれを
調整する運用者は、このセクションを知っているワーカーを動かす必要があります。

### `[quic]`

このワーカーが待ち受ける、あるいはダイヤルするすべての QUIC 中継リンクでの自分側の設定です。quinn の
デフォルトはウェブクライアント向けであって中継向けではありません。Cubic は損失のたびに減速し、固定
1.25 MB のストリーム受信ウィンドウは、送信側が何をしようとストリームを `ウィンドウ / RTT` に抑えて
しまいます。レートを教えられたリンクは hysteria の *brutal* 送信器と、それに合わせたウィンドウを得ます。

| キー | デフォルト | 値 |
|---|---|---|
| `congestion` | `"cubic"` | `cubic` は帯域を探ります。`brutal` は経路の状態にかかわらず `send_mbps` ちょうどで送信し、観測した損失分（最大 4 分の 1）を上乗せします |
| `send_mbps` | `0` | 相手へ向けたレート（Mbit/s）。brutal の固定レートであり、送信ウィンドウの基準です。`0` は quinn のデフォルトのまま |
| `receive_mbps` | `0` | 相手がこちらへ送るレート（Mbit/s）。受信ウィンドウはその 0.5 秒分になります。`0` は quinn のストリームあたり 1.25 MB のまま |
| `max_streams` | `0` | リスナーが 1 接続で相手に許すストリーム数。`0` は 4096 |
| `stream_receive_window` | `0` | `receive_mbps` から導出する代わりに使う、ストリームあたりの受信ウィンドウ（バイト） |
| `receive_window` | `0` | 接続全体の受信ウィンドウ（バイト）。`0` は無制限 |
| `send_window` | `0` | `send_mbps` から導出する代わりに使う、接続あたりの未確認バイト数の上限 |

1 つのリンクで代理されるすべての接続は 1 本の QUIC 接続（接続ごとに 1 ストリーム）を共有するため、
レートはリンク全体の合計であって接続ごとの割り当てではありません。その方向で経路が実際に運べる値を
設定してください。大きすぎる値は損失を生むだけです。`quic` リレーリスナーの `[forwarding.quic]` テーブル、
または `quic` ホップの `[forwarding.to.quic]` テーブルは、そのリンクに限ってこのセクションを置き換えます。
`guru-master` はそうやって両端を組み合わせます。各サーバーは自分の数値を `[quic]` に持ち、相手の*下り*が
こちらの*上り*より低い（またはその逆の）場合、forwarding には低い方が書かれるので、相手が受け取れると
言った以上の速度で送る側はありません。`send_mbps` のない `brutal` は拒否され、QUIC リレー以外に置いた
どちらのテーブルも拒否されます。`[keepalive]` と同様に、このセクションはデフォルトのときは書き出されません。

### `[[forwarding]]`

| キー | 必須 | 値 |
|---|---|---|
| `tag` | はい | 自由形式の名前。ログ、lint の出力、適用エラーに現れるのはこの名前です |
| `listen` | はい | `ip:port` — リテラルのアドレスで、ホスト名は不可（`0.0.0.0:443`、`[::]:443`） |
| `receive_proxy_protocol` | いいえ | `"v1"` または `"v2"` — クライアントのペイロードの前に PROXY ヘッダーを期待します |
| `listen_as` | はい | 受け付け方: `"raw"`、または `tls` / `relay` テーブル（後述） |
| `quic` | いいえ | このリスナーだけに効く `[quic]` テーブル。`quic` リレーリスナーのみ |
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
相対パスとして渡します — TLS クライアントポッドには `certs/acme/<certificate>/{full_chain,key}.pem`、`tls`/`quic` の
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

### ルートテーブル

`to` にはインラインのツリーの代わりに、その forwarding 自身の **グループ** または **アップストリーム** の
id を書くこともできます。次のホップはすべてアップストリーム、次のホップの間の選択はすべてグループで、グループは
メンバーを id で並べます。つまりロードバランスを束ねたフェイルオーバーも、フェイルオーバーを束ねたロード
バランスも、メンバーがグループであるグループにすぎません。マスターが `route_table` 機能を報告するワーカーに
送るのはこの形式で、インラインのツリーはすべてのワーカーが読める形式として残っています。

```toml
[[forwarding]]
tag = "web"
listen = "[::]:443"
listen_as = "raw"
to = "g"                                # トラフィックが最初に入るグループまたはアップストリーム

[[forwarding.group]]
id = "g"
failover = ["g.0", "u:backup"]          # 生きている最初のメンバーを順に使う

[[forwarding.group]]
id = "g.0"
balance = [{ to = "u:hk-1", weight = 2 }, { to = "u:hk-2" }]   # weight の既定値は 1
sticky = "client_ip"                    # 任意、balance のみ

[[forwarding.upstream]]
id = "u:hk-1"
relay = { protocol = "quic", destination = "203.0.113.1:40000", sni = "hk-1.relay.guru.internal", confirm = true }

[[forwarding.upstream]]
id = "u:hk-2"
relay = { protocol = "tcp", destination = "203.0.113.2:40000" }

[[forwarding.upstream]]
id = "u:backup"
exit = { destination = "backend.internal:8080", send_proxy_protocol = "v2" }
```

balance は生きているメンバーに重みに比例して接続を振り分けます — スムーズラウンドロビン、あるいは
`sticky = "client_ip"` ならクライアントアドレスの重み付きランデブーハッシュで、メンバーが生きている間は
クライアントが同じメンバーにとどまります。failover は生きている最初のメンバーを使います。どちらも
すべてのホップが死んでいるメンバーを飛ばし、失敗した試行は同じクライアント接続のまま次の選択へ進みます。
3 回続けて失敗したアップストリームは死んだものとみなされ、10 秒後から一度に 1 接続だけが再び試せます。

リレーアップストリームの `confirm = true` は、リレーに *自分の* 次のホップへ接続できた時点で応答するよう
求めます。これにより、生きているリレーの先にある死んだエグジットはダイヤル側でホップの失敗となり、選択が
次へ進みます。`relay_confirm` を報告するワーカーに向けてだけ設定してください。マスターは自動でそうします。
リレーアップストリームの `quic` テーブルは、ツリーの `relay` と同じく、そのホップ側のリンク設定です。

### 拒否されるもの、警告だけで済むもの

以下のいずれかに当てはまると読み込みは失敗し、ファイルが部分的に適用されることはありません:

| エラー | 原因 |
|---|---|
| `parse toml: TOML parse error at line N …` | どのテーブルも未知のキーを拒否します。タイポは黙ってデフォルトになるのではなく、エラーです |
| `duplicate listener <addr> (<tag>)` | 2 つのエントリが同じトランスポートで同じ `ip:port` を要求しています |
| `forwarding <tag> relay to tls/quic requires sni` | `sni` のない `tls`/`quic` リレーホップ（ネストの深さは問いません） |
| `forwarding <tag> has an empty load-balance group` | `members = []`（ネストの深さは問いません） |
| `invalid remote '…'` / `invalid port in remote '…'` | `destination` が `host:port` になっていません |
| `forwarding <tag> refers to route id <id>, which it does not define` | `to` やグループのメンバーが、その forwarding のどのグループにもアップストリームにもない id を指しています |
| `forwarding <tag> defines route id <id> more than once` | 2 つのグループまたはアップストリームが同じ id を持っています |
| `forwarding <tag> has an empty group <id>` / `gives <id> a weight of zero` | メンバーのないグループ、重み 0 |
| `forwarding <tag> has a group cycle through <id>` | 互いを含み合うグループ |
| `forwarding <tag> has groups or upstreams but an inline to tree` | 1 つの forwarding で 2 つの形式が混在しています |

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
モードでは各 pod の結果がマスターに ack され、マスターは失敗した pod をサーバーに記録し、それぞれに `Failed` の
ヘルスイベントを残します。

## `manage-tool`

グローバルフラグは 1 つだけです: `--database-url`（`GURU_DATABASE_URL`）。`guru-master` が読むのと同じ URL です。

| サブコマンド | 用途 |
|---|---|
| `create-admin --email <email> --password <password>` | 最初の管理者アカウントをブートストラップします |
| `generate-master-key` | 新しい `GURU_MASTER_KEY` を出力します（データベースは不要） |
| `db migrate` | 未適用のスキーママイグレーションをすべて適用します。`guru-master` も起動時に同じことを行います |
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
再起動だけが必要です。現在のキーは 3 つです:

| キー | 構造体 | 内容 |
|---|---|---|
| `auth` | `auth::config::AuthConfig` | `session_idle_ttl_secs` |
| `notify` | `notify::config::NotifyConfig` | `smtp_host`（デフォルトは空で、その場合メールは完全に無効になります）、`smtp_port`（587）、`smtp_starttls`（`true`。`false` はプレーンな SMTP で話すため、localhost のリレーやテスト用のシンクにだけ使います）、`smtp_username`（空の場合は認証なしで送信します）、`smtp_from`（`guru <noreply@example.com>`）、`telegram_api_base`（`https://api.telegram.org`）、`delivery_attempts`（3）、`delivery_retry_delay_secs`（5）、`default_language`（`en`。`en`、`ja`、`zh_cn` のいずれかで、言語を指定していない設定行がどの言語でレンダリングされるかを決めます）。SMTP のパスワードとボットトークンはここには**ありません**。これらは `notifier` の `GURU_SMTP_PASSWORD` と `GURU_TELEGRAM_BOT_TOKEN` です。このドキュメントはすべての Admin が読めるからです |
| `orchestration` | `orchestration::config::OrchestrationConfig` | `health_report_interval_secs`、`health_offline_after_intervals`、`degraded_grace_secs`、`server_health_ttl_secs`、`pod_health_ttl_secs`、`default_acme_directory`、`acme_renew_before_secs`、`acme_retry_after_secs`、`relay_cert_valid_secs`、`relay_cert_renew_before_secs`、`sweep_interval_secs`、`liveness_interval_secs`、`health_retention_interval_secs`、`acme_interval_secs`、`relay_rotation_interval_secs`、`stream_keepalive_secs`（デフォルトは `15`: アイドル状態の `Watch*` ストリームが空のキープアライブを送り、そのストリームを開いたセッションを再確認する間隔で、ワーカーの `WatchConfig` がキープアライブを運ぶ間隔でもあります。`:50051` や `:50052` の手前にプロキシがある場合は、そのアイドルタイムアウトより短くしてください — Cloudflare は無音のストリームを約 100 秒で切断します）、`trust_proxy_address_headers`（デフォルトは `true`: ワーカー API は登録元のアドレスとして `x-real-ip` または `x-forwarded-for` の最初のホップを記録します。ドキュメント化されたプロキシを経由せずに `:50052` へ到達できる場合は無効にしてください。そうでなければワーカーが偽装できてしまいます）、`country_lookup_url`（デフォルトは `https://api.country.is/{ip}`: サーバーの国旗のために IPv4 アドレスの国を調べる先で、`{ip}` がアドレスに置き換わります。応答は 2 文字の `country` フィールドを持つ JSON オブジェクトでも 2 文字だけでもよいので、`https://get.geojs.io/v1/ip/country/{ip}` も使えます。空にすると照会しません）、`country_lookup_interval_secs`（デフォルトは 60）、`country_lookup_retry_after_secs`（デフォルトは 3600: 失敗した照会の後、同じアドレスを再び調べるまでの時間） |

`manage-tool db migrate` の後に `manage-tool config seed` を実行するとデフォルト値が書き込まれ、
`manage-tool config list` で保存されている内容を確認できます。`list` と `get` は行をそのまま出力し、
デコードしません。そのため、マスターの起動時の読み込みを失敗させるドキュメントでも中身を確認できます。
`set` はドキュメント全体を置き換えますが、書き込む前に設定の型へデコードされるため、部分的なペイロードは
デフォルト値で補われ、形の違うペイロードは行に届く前に拒否されます:

```sh
manage-tool config set orchestration '{"acme_renew_before_secs":1209600}'
```

6 つの `*_interval_secs` フィールドは、定期ジョブが実際に実行され得る頻度です
（[スケジューリングと実行](#スケジューリングと実行)）。これらが環境変数ではなくここにあるのは、フリート
全体で値が一致していなければならないからです。間隔を強制する claim は、すべての consumer が共有する
1 つのデータベース行です。

```sh
manage-tool config set orchestration '{"acme_interval_secs":300,"relay_rotation_interval_secs":7200}'
```

3 つのドキュメントはいずれも、ダッシュボードからも参照・置き換えできます。**Admin**（そして Admin だけです —
他のロールはこの権限を持たず、API キーが持つこともありません）は、保存されているとおりの行、seed が
書き込むはずのペイロード、そしてドキュメント全体を置き換えるフォームを見られます。検証は
`manage-tool config set` とまったく同じなので、形の違うペイロードは拒否され、行は以前の内容を保ちます。
ダッシュボードからの保存もライブリロードではありません。新しい値を反映するには**`guru-master` を再起動**
してください。

データベースを使う 4 つのモード — `dashboard_grpc`、`workers_grpc`、`consumer`、`notifier` — は起動時に、
自分が必要とするキーを一度だけ読み、その値を各サービスへ渡します。ライブリロードはありません。`cron` は
データベース接続を開かず、どのキーも読みません。実行シグナルを publish するだけで、それを受け取った
consumer が保存された間隔を適用します。seed されていない環境ではデフォルト値で動作します。
デシリアライズできない行はキー名を
示して起動を失敗させます。これは意図的です。壊れたドキュメントをデフォルト値で代用すれば、オペレーターの
設定全体を黙って入れ替えてしまい、たとえば ACME をステージングから本番のディレクトリへ移してしまう
かもしれません。
後のリリースで追加されたフィールドはデフォルト値で読まれるため、古い行もそのまま動作し続けます。
