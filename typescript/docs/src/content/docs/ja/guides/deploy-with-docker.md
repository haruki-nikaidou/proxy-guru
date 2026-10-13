---
title: Docker でデプロイ
description: GHCR のイメージからコントロールプレーンを動かし、スキーマを適用し、PostgreSQL、Iggy、Redis を用意して、GitHub リリースから master と manage-tool、そしてワーカーのバイナリを取得する手順。
---

このガイドは、何も入っていないマシンから動作するダッシュボードまで、シングルホストの本番デプロイを通しで
説明します。Linux、Docker、リバースプロキシの扱いに慣れていること、そしてこのプロジェクト — あるいは
これに似たアーキテクチャ — を初めて見ることを前提にしています。

意図的に対象外としているもの: データプレーンのノードにワーカーをインストールし、コントロールプレーンに
登録する作業。このガイドは、配布元にできるマシン上にバイナリを用意するところまでです。

## 1. 何をデプロイするのか

**1 つ**のイメージから動く 5 つのプロセスと、ダッシュボードです:

| コンポーネント | 実行モード | 通信相手 |
|---|---|---|
| オペレーター API | `dashboard_grpc` | PostgreSQL、Iggy、Redis |
| ワーカー API | `workers_grpc` | PostgreSQL、Iggy、Redis |
| イベントハンドラー（導出 + 通知のファンアウト） | `consumer` | PostgreSQL、Iggy、Redis |
| 定期ジョブ | `cron` | PostgreSQL、Iggy、Redis |
| 通知の配信 | `notifier` | PostgreSQL、Iggy（**インスタンスはちょうど 1 つ**） |
| ダッシュボード | — | オペレーター API（gRPC） |

:::note
TCP リバースプロキシサーバーの性質上、ワーカーを Docker コンテナ内にデプロイするのは適切ではありません。
そのため、ワーカーノード向けの Docker イメージは提供していません。
:::

永続的な状態が存在する場所はちょうど 2 か所です: **PostgreSQL**（キャンバス、サーバー、ポッドグラフ、
アカウント、設定ビュー）と **Iggy**（「このキャンバスが変更された」というヒント、ヘルスの事実、通知のログと、
その retry トピック）。**Redis** は 3 つ目のデータストアであり、唯一何も保持しないものです。単一の pub/sub
チャンネルで、オペレーター API のライブイベントを master のレプリカ間に運ぶだけで、永続化は一切設定しません。
コンテナのファイルシステム上には何も保持しないため、すべてのコンテナは使い捨てできます。

サイジングの前に理解しておくべき分割が、2 つのバックグラウンドモードです。`consumer` は Iggy のイベント
ハンドラー — `CanvasDirty` による導出と `HealthChanged` による通知のファンアウト — を実行します。`cron` は
6 つの定期ジョブすべてをプロセス内で実行し、各実行を作業の前にデータベースで claim するため、複数動かしても
各パスは設定された間隔につき 1 回だけ実行されます。

`notifier` は 3 つ目のバックグラウンドモードで、「自由にスケールさせてよい」という原則の唯一の例外です。`consumer` の
ファンアウトが publish したメールと Telegram メッセージを送るモードであり、2 つ目のインスタンスがあると、
再起動が重なった分の通知をすべて二重に送ってしまいます。そのため PostgreSQL のアドバイザリロックを取得し、
それを保持しているピアの隣では起動を拒否します。また、`GURU_MASTER_KEY` も `REDIS_URL` も必要としない
唯一のモードです（[通知](/ja/features/notifications/)）。

ポートと、そこに到達してよい相手:

| ポート | プロセス | 公開範囲 |
|---|---|---|
| `50051` | `dashboard_grpc` | **プライベート。** ダッシュボード専用。平文 h2c で、TLS なし、トランスポート層の認証なし。 |
| `50052` | `workers_grpc` | データプレーンのノードから到達可能にする（VPN、プライベートネットワーク、または TLS 終端する gRPC プロキシ）。 |
| `3000` | ダッシュボード | HTTPS リバースプロキシの背後に置く。直接公開してはいけません。 |
| `5432` | PostgreSQL | **プライベート。** ロール 1 つ、データベース 1 つ、パスワード 1 つ。 |
| `8090` | Iggy | **プライベート。** |
| `6379` | Redis | **プライベート。** 認証情報は一切ありません。アクセス制御はリスナーそのものです。 |

:::caution[gRPC ポートは平文です]
master の両モードは平文の HTTP/2 で待ち受け、ダッシュボードは `ChannelCredentials.createInsecure()` で
チャネルを開きます。`50051` はプライベートネットワーク（Docker ネットワーク、ループバックへのバインド、
または VPN）に置き、`50052` は公衆インターネットを越えるならそれ自体にトランスポートセキュリティが必要な
リンクとして扱ってください。
:::

## 2. 前提条件

このガイドの前に **[前提条件](/ja/guides/prerequisites/)** を済ませてください。イメージデプロイの場合、
そのページから必要なのは: Docker Engine と Compose プラグイン、オペレーターマシン上のこのリポジトリの
チェックアウト（Compose ファイルのため）、
`openssl`、TLS 証明書を持つ DNS 名 — そして PostgreSQL、Iggy、Redis 自体で、これらは同ページが
`/srv/guru/docker-compose.yml` から `/srv/guru/.env` の認証情報とともに起動します。

`manage-tool` CLI もイメージには含まれていませんが、**ビルドは必須ではありません**: `master-v*` タグは
GHCR のイメージに加えて、`guru-master` と `manage-tool` のバイナリを含む GitHub リリースも公開します
（セクション 10）。ダウンロードしたバイナリを使うなら、Rust ツールチェーン、`protobuf-compiler`、
C ツールチェーン、`cmake` はどれも不要です。これらが要るのは自分でビルドする場合だけで、`manage-tool` が
証明書関連のスタックを取り込み、そのクレートがベンダリングされた C ソースをコンパイルするためです。
スキーマはバイナリの中に入って配布されるため、適用のためにチェックアウトから何かを取り出す必要はありません。
どちらにしても Bun は不要で（ダッシュボードはイメージとして配布されます）、`perl` が必要なのは
`guru-worker` をビルドする場所だけで、ここではありません。

## 3. バージョンを選ぶ

公開はタグの push 時のみ行われ、イメージタグは git タグからコンポーネント接頭辞を除いたものになります。

| git タグ | 公開されるもの |
|---|---|
| `master-v0.1.0[-alpha]` | `ghcr.io/haruki-nikaidou/guru-master:v0.1.0[-alpha]` と、`guru-master` / `manage-tool` のバイナリ（`x86_64-unknown-linux-gnu`）を含む GitHub リリース |
| `frontend-v0.1.0[-alpha]` | `ghcr.io/haruki-nikaidou/guru-frontend:v0.1.0[-alpha]` |
| `worker-v0.1.0[-alpha]` | `linux/x86_64` 向けの生の `guru-worker` バイナリ 2 種（glibc 版と静的 musl 版）を含む GitHub リリース |

`latest` が動くのは最終版の `vX.Y.Z` のときだけで、プレリリースでは動きません。それでも Compose ファイルでは
**明示的なタグを固定してください**: `latest` ではどのリビジョンが動いているかを示す手段がなく、master と
スキーマは一緒に動きます。

どちらのイメージも公開されているため、pull に `docker login ghcr.io` は必要ありません。

## 4. シークレットを配置する

[前提条件](/ja/guides/prerequisites/)で、Compose ファイルの隣に `/srv/guru/.env` をデータストアの認証情報
（`POSTGRES_USER`、`POSTGRES_PASSWORD`、`POSTGRES_DB`、`IGGY_USER`、`IGGY_PASSWORD`）
とともに作成済みです。セクション 3 で固定したイメージタグを追記します:

```sh
# /srv/guru/.env  (append)
# The master and the frontend are tagged and released independently; pin each one.
MASTER_VERSION=v1.0.0
FRONTEND_VERSION=v1.0.0
```

`GURU_MASTER_KEY` は、`manage-tool` で生成できるようになるセクション 7 で同じファイルに追加します。

:::caution[リポジトリの `.env` は別のファイルです]
リポジトリのルートには*別の*環境の認証情報を含む `.env` が置かれている場合があり
（`GURU_DATABASE_URL`、`IGGY_URL`）、そのディレクトリから起動したすべてのプロセスがそれを引き継ぎます。
チェックアウトから `manage-tool` を実行するときは、`--database-url` を明示的に渡してください。
フラグを 1 つ忘れただけで「ローカル向け」のコマンドが本番環境を書き換えてしまいます。
:::

## 5. PostgreSQL、Iggy と Redis

3 つのデータストア、それらの Compose サービス、および背後にある要件（PostgreSQL ≥ 16 で永続ストレージ、
Iggy はサーバー 0.9.x で `SYS_NICE`・`seccomp=unconfined`・無制限の `memlock`、Redis は 7.x で
認証情報も永続化もなし）は
**[前提条件 → PostgreSQL、Iggy と Redis](/ja/guides/prerequisites/#4-postgresqliggy-と-redis)** に
まとめられています。master を起動する前に、これらが立ち上がっている必要があります:

```sh
cd /srv/guru
docker compose ps          # postgres healthy, iggy healthy, redis up
```

手動で確認するなら `docker exec <iggy container> iggy -u <user> -p <password> stream list` です。

このデプロイの形を決める点なので、ここで繰り返しておく価値のある帰結が 3 つあります: Iggy は
**5 つすべて**の master モードで必須であり — `IGGY_URL` がなければどのモードも起動しません — master は `/srv/guru/.env` にあるロールでデータベースへ到達し、
セクション 7 の `x-master` アンカーがそれを URL に組み立てます。そして Redis は `notifier` 以外の
すべてのモードで必須ですが、失われたときの代償はずっと小さく、停止しても止まるのは開いている `Watch*`
ストリームへの配信だけで、それ以外は何も止まりません。編集は適用され、キャンバスは導出され、ワーカーは
設定を受け取り続けます。subscriber は自力で再接続し、すべての watcher にデータベースの再読み込みを求めます。
ダッシュボードはこれらのストリームを追従しているため、Redis が停止している間は開いているキャンバスページや
ヘルスページの更新が止まり、Redis が復旧すれば自動的に追いつきます。それらのページの *ライブ* バッジは
ブラウザー自身の接続を示すもので、その間も緑のままです。

## 6. スキーマを適用する

各 master は起動時に未適用のものを適用するので、初回インストールではこのセクションは任意です — ただし先に
実行しておけば、スキーマの問題が 5 つの再起動するコンテナの中ではなくここで表面化します。
**[データベーススキーマのセットアップ](/ja/guides/setup-database-schema/)** に従ってください:

```sh
read -rs GURU_DATABASE_URL           # paste the URL, it is not echoed
export GURU_DATABASE_URL
./manage-tool db migrate
```

データベースがサーバー上のループバックでしか待ち受けていない場合は、トンネルを張ってください:
`ssh -N -L 5432:127.0.0.1:5432 guru-host`。

## 7. コントロールプレーンを動かす

`guru-master` の*デプロイ*設定は環境変数から来ます: `GURU_WORKER_MODE` がモードを選び、
`GURU_DATABASE_URL`、`IGGY_URL`、`REDIS_URL`、`GURU_MASTER_KEY` には
**デフォルト値がありません**。
オペレーターがインストールごとに調整するもの — ヘルスのしきい値と保持期間、デフォルトの ACME ディレクトリ、
更新ウィンドウ、各定期ジョブの実行間隔 — は代わりにデータベースに置かれるため（手順 8）、レプリカ側で
環境変数を揃える必要はありません。

master キーは一度生成し、データベースの認証情報と一緒に保管してください — これはすべての DNS プロバイダー
トークンと証明書の鍵を保存時に暗号化するもので、これがなければ復元する方法はありません。シークレットを読む
4 つのモード（`notifier` 以外）ではこれが必要です。`notifier` は決して読みませんが、以下のアンカーは単純に同じ環境変数を
5 つすべてに渡します。`manage-tool` は `master-v*` リリースからダウンロードするか、チェックアウトからビルドします
（手順 8 と手順 10 を参照）。このサブコマンドはデータベースを必要としません:

```sh
./target/release/manage-tool generate-master-key
```

`REDIS_URL` のモードの範囲も似たようなものです。`notifier` 以外のすべてのモードで必須で、`notifier` は使いません。

同じ `docker-compose.yml` を拡張します: `x-master` アンカーは `services:` の上に、5 つのサービスは
その中の `postgres`、`iggy`、`redis` の隣に置きます:

```yaml
x-master: &master
  image: ghcr.io/haruki-nikaidou/guru-master:${MASTER_VERSION}
  restart: unless-stopped
  environment: &master-env
    GURU_DATABASE_URL: postgres://${POSTGRES_USER}:${POSTGRES_PASSWORD}@postgres:5432/${POSTGRES_DB}
    IGGY_URL: iggy://${IGGY_USER}:${IGGY_PASSWORD}@iggy:8090
    REDIS_URL: redis://redis:6379/
    GURU_MASTER_KEY: ${GURU_MASTER_KEY}
    GURU_LOG_LEVEL: info
  depends_on:
    postgres:
      condition: service_healthy
    iggy:
      condition: service_healthy
    redis:
      condition: service_started

services:
  # ... postgres, iggy and redis from section 5 ...

  master-dashboard:
    <<: *master
    environment:
      <<: *master-env
      GURU_WORKER_MODE: dashboard_grpc
    # No `ports`: only the dashboard container reaches :50051, over this network.

  master-workers:
    <<: *master
    environment:
      <<: *master-env
      GURU_WORKER_MODE: workers_grpc
    # Loopback only: nginx terminates TLS for the worker API on :443 (section 9).
    ports:
      - "127.0.0.1:50052:50052"

  master-consumer:
    <<: *master
    environment:
      <<: *master-env
      GURU_WORKER_MODE: consumer

  master-cron:
    <<: *master
    environment:
      <<: *master-env
      GURU_WORKER_MODE: cron

  # Exactly one: the mode takes an advisory lock and a second replica exits.
  master-notifier:
    <<: *master
    environment:
      <<: *master-env
      GURU_WORKER_MODE: notifier
      # Both optional; each unset value disables its channel.
      GURU_SMTP_PASSWORD: "${GURU_SMTP_PASSWORD}"
      GURU_TELEGRAM_BOT_TOKEN: "${GURU_TELEGRAM_BOT_TOKEN}"
```

各モードの役割と、そのスケール方法:

- **`dashboard_grpc`** — `GURU_DASHBOARD_GRPC_ADDR`（`0.0.0.0:50051`）で提供するオペレーター API
  （`Auth` + `Orchestration`）。ステートレスなので、gRPC を理解するロードバランサーの背後で自由に
  レプリケートできます。
- **`workers_grpc`** — `GURU_WORKERS_GRPC_ADDR`（`0.0.0.0:50052`）で提供するワーカー API
  （`WorkerAgent`）と、ワーカーのストリームを起こす設定ビューのポーラー
  （`GURU_WATCH_POLL_MS`、デフォルト `1000`）。レプリケート可能ですが、各ワーカーセッションはその
  ストリームを保持しているインスタンスに固定されるため、前段には素の TCP/gRPC ロードバランサーを置き、
  HTTP/1 プロキシは決して置かないでください。
- **`consumer`** — Iggy のイベントハンドラー: `CanvasDirty` が届いたらキャンバスを再導出し、`HealthChanged` が
  届いたら通知をファンアウトします。ACME の発行と DNS-01 のチャレンジは `cron` 側なので、外向きの通信は
  主にデータベースと Iggy です。スループットとフェイルオーバーのためにレプリケートできますが、レプリカは
  同じ consumer 名を共有するため、`GURU_IGGY_PARTITIONS` で互いに重ならないパーティション集合（0 始まり、
  既定は `0`。トピックは分割するまで 1 パーティションなので、2 台目のレプリカにはまずトピックの分割が必要です）を明示的に与えてください。導出はキャンバスの世代カウンターで保護されます。失敗した
  ハンドラーは、データベースが応答しなければその場で、その他のデータベースエラーと衝突は retry トピック
  経由で（2、4、8、16 秒後）再試行され、それ以外はログに残して破棄されます。
- **`cron`** — 6 つの定期パスすべてをプロセス内で実行します: `derive_stale_canvases` と `sweep_liveness` は
  30 秒ごと、`renew_certificates` と `resolve_server_countries` は 60 秒ごと、`trim_health_history` は 5 分ごと、
  `rotate_relay_certificates` は 1 時間ごと。最初の実行は起動時です。ACME ディレクトリ、DNS プロバイダー API、
  `country_lookup_url` への外向き HTTPS、および公開リゾルバーへの DNS が必要になるのはこのモードです。
  各実行は作業の前に `orchestration_job_run` 行 1 件で実行権を主張する（compare-and-set）ので、2 つ目の
  レプリカは重複ではなくスタンバイになります。2 つの ACME パスが同じ DNS-01 チャレンジを競合させることも
  ありません — 証明書の取得試行は行ごとに主張されます。停止時は実行中のジョブを最大 20 秒待ってから中断します。
  ジョブが実際に実行されうる間隔はフラグではなく保存された設定です — 手順 8 を参照してください。
- **`notifier`** — 通知の配信であり、**ちょうど 1 つだけ**動かすべき唯一のモードです: `consumer` の
  ファンアウトが publish する通知のトピックを consume し、各通知を SMTP と Telegram Bot API で送ります。
  その前にセッションスコープの PostgreSQL アドバイザリロックを取得するため、2 つ目のレプリカは
  `another notifier already holds the advisory lock; run exactly one` を出力して非ゼロで終了します —
  これにより `deploy: replicas: 1` は慣習ではなく確実な保証になります。`GURU_MASTER_KEY` も `REDIS_URL` も
  必要とせず、必要なのは外向きの SMTP と `telegram_api_base` への HTTPS だけです。停止している間、通知は
  Iggy のトピックで待ちます。

TLS または QUIC 上のリレーリンクは、その Pod が導出される前に内部 CA を必要とします。master が使うものと
同じ `GURU_MASTER_KEY` を使い、オペレーターマシンから一度だけ実行してください:

```sh
GURU_MASTER_KEY='<the key>' ./target/release/manage-tool \
  --database-url "$GURU_DATABASE_URL" orchestration init-ca
```

CA 証明書を出力し、TLS/QUIC リレーを持つすべてのキャンバスに再導出のマークを付けます。2 回目の実行は
拒否されます。

コードから導かれる運用上の注意が 3 つあります:

- どのモードも起動時に Iggy に接続し、ストリームとトピックを冪等に作成します。Iggy に到達できなければ起動は
  失敗し、`restart: unless-stopped` が再試行します — 取り除かないでください。
- イメージは distroless です: シェルも `curl` もありません。シェルを呼び出す Compose の `healthcheck` は
  動きません。代わりに外部から監視してください（`50051`/`50052` への TCP 接続、あるいはログの収集）。
- Redis の subscriber は自力で再接続し（500 ms から倍々で 10 s まで）、そのたびに
  `live bus connected` をログし、再接続のあとは開いているすべての `Watch*` ストリームにデータベースを
  読み直させるため、古い状態が残りません。その間にチャンネルが運んでいたものは失われますが、それで
  構いません — このチャンネルが運ぶのは進行中のイベントであって、状態ではありません。このデプロイで
  バックアップに値するのは、依然として PostgreSQL だけです（セクション 13）。

起動します:

```sh
docker compose up -d
docker compose logs master-dashboard master-workers master-consumer master-cron master-notifier
```

正常な起動はこのように見えます。consumer と notifier は consume を始めると 1 行、`cron` は 6 つの周期を
列挙した 1 行を出力します:

```text
master-dashboard-1  | INFO guru_master: serving operator API addr=0.0.0.0:50051
master-workers-1    | INFO guru_master: serving worker API addr=0.0.0.0:50052
master-consumer-1   | INFO guru_master: consuming
master-cron-1       | INFO guru_master: running periodic jobs derive_stale_canvases=AtLeast(30s) rotate_relay_certificates=AtLeast(3600s) sweep_liveness=AtLeast(30s) trim_health_history=AtLeast(300s) renew_certificates=AtLeast(60s) resolve_server_countries=AtLeast(60s)
master-notifier-1   | INFO guru_master: notification channel channel="email: configured"
master-notifier-1   | INFO guru_master: consuming
```

`cron` の各ジョブは `cron_job{job=…}` スパンの中で動き、失敗した実行は `cron job failed` としてログされますが
プロセスは終了しません。

## 8. 最初の管理者を作る

セルフサービスのサインアップはありません: 最初のアカウントは `manage-tool` でデータベースに対して直接作成し、
これはまだ管理者が存在しないため意図的に RBAC をバイパスします。この CLI はイメージとして公開されていないので、
`master-v*` リリースからダウンロードする（セクション 10）か、チェックアウトからビルドしてください:

```sh
cd ~/proxy-guru
cargo build --release -p manage-tool

./target/release/manage-tool --database-url "$GURU_DATABASE_URL" \
  create-admin --email admin@example.com --password '<strong password>'
# Created admin account uz0ih3b30nrekqzs1h1y
```

`--database-url` は明示的に渡してください — `manage-tool` も環境変数から `GURU_DATABASE_URL` を読むため、
紛れ込んだ `.env` がコマンドの向き先を黙って変えてしまいます。

ダウンロードしたバイナリを使う場合は、このガイドに出てくる `./target/release/manage-tool` を自分が置いた
パス（例: `./manage-tool`）に読み替えてください。フラグもサブコマンドも同じです。

### モジュール設定のシードを投入する

オペレーターが調整する設定値はキーごとに 1 行の `app_config` テーブルに置かれ、シード投入の手順は
[データベーススキーマのセットアップ → モジュール設定のシード](/ja/guides/setup-database-schema/#3-モジュール設定のシードを投入する)
の一部です — そこで省略した場合は、今ここで実行してください:

```sh
./manage-tool --database-url "$GURU_DATABASE_URL" config seed
# seeded auth
# seeded orchestration
```

`config list` は保存されているすべてのドキュメントを出力し、`config set <key> <json>` はキーを 1 つ
置き換えます — 例えば ACME の更新ウィンドウを 2 週間にする場合:

```sh
./target/release/manage-tool ... config set orchestration '{"acme_renew_before_secs":1209600}'
```

`set` は書き込み前に設定の型に対して検証され、ドキュメント全体を置き換えます。指定しなかったフィールドは
デフォルト値になります。master はこれらのキーを起動時に一度だけ読むので、変更を反映するには再起動して
ください。[設定 → モジュール設定](/ja/reference/configuration/#モジュール設定)を参照してください。

定期ジョブの周期は同じキーに含まれます: `sweep_interval_secs`（30）、
`liveness_interval_secs`（30）、`health_retention_interval_secs`（300）、`acme_interval_secs`（60）、
`country_lookup_interval_secs`（60）、`relay_rotation_interval_secs`（3600）。`cron` の各ジョブは
固定周期で起動し、*設定された*間隔につき最大 1 回だけ実行権を主張するので、ジョブの周期以下の値は
「起動ごとに実行」を意味し、それより大きい値はフリート全体でそのジョブを遅くします:

```sh
./target/release/manage-tool ... config set orchestration '{"acme_interval_secs":300}'
```

同じバイナリには `orchestration export-config --server <key>` があり、あるサーバーについてキャンバスが現在
導出しているワーカーの TOML を出力します。ノードの挙動とキャンバスが食い違って見えるときに手に取るべき
ツールがこれです。

## 9. ダッシュボードを動かす

ダッシュボードは Node アダプター上の SvelteKit アプリです。`:3000` で待ち受け、`GURU_GRPC_URL` を通して
コントロールプレーンに到達します。同じファイルにサービスを 1 つ追加します:

```yaml
  frontend:
    image: ghcr.io/haruki-nikaidou/guru-frontend:${FRONTEND_VERSION}
    restart: unless-stopped
    environment:
      GURU_GRPC_URL: master-dashboard:50051
      PROTOCOL_HEADER: x-forwarded-proto
      HOST_HEADER: x-forwarded-host
    ports:
      - "127.0.0.1:3000:3000"
    depends_on:
      - master-dashboard
```

:::danger[HTTPS で配信し、プロトコルを転送すること]
「ダッシュボードは表示されるのにログインできない」という状態に陥る、単独で最も多い原因がこれです。

このアプリは自分が待ち受けているソケットを信頼しません。リクエストごとにヘッダーから自身のオリジンを再構成し、
`PROTOCOL_HEADER` が未設定のときは**スキームを `https` とみなします**。ログイン（SvelteKit の
*remote function*、つまり POST）は、ブラウザーの `Origin` ヘッダーが再構成されたオリジンと一致しない場合、
`403 {"message":"Cross-site remote requests are forbidden"}` で拒否されます。したがって:

- **`Host` を保持する HTTPS プロキシの背後:** 追加設定なしで動きます — スキームはデフォルトで `https`、
  ホストは `Host` から取られます。
- **それ以外（平文 HTTP、公開ホストやポートが異なる場合）:**
  `PROTOCOL_HEADER=x-forwarded-proto` と `HOST_HEADER=x-forwarded-host` を設定し、プロキシが両方を
  送るようにしてください。公開 URL が非デフォルトのポートを持つ場合、`X-Forwarded-Host` には
  **ポートを含める必要があります** — nginx の `$host` はポートを落とすので、`$http_host` を使ってください。
- `ORIGIN` は何もしません。このビルドの Node アダプターは、その値を `kit.paths.origin` からビルド時に
  埋め込むため、実行時の変数は無視されます。

いずれの場合でも HTTPS は必須です: セッション Cookie（`guru_session`、コントロールプレーンに対する完全な
ベアラー資格情報）は `Secure` 付きで発行されるため、`localhost` 以外では平文 HTTP だとブラウザーが
破棄します。
:::

以下のサーバーブロックは、ダッシュボード、ワーカー API、エージェントのダウンロードディレクトリを 1 つの
ホスト名に載せます。gRPC には固有のパスプレフィックスがありません: ワーカーの呼び出しはすべて
`POST /guru.orchestration.agent.WorkerAgent/<Method>` なので、そのプレフィックスに対する `location` が
それらのリクエストを `:50052` へ渡し、それ以外はすべてダッシュボードへ渡します。`/agent/` の location は
`manage-tool agent publish` が書き出したものを配信します（[エージェントのインストールと更新](/ja/guides/agent-install/)）。
最初の publish の前にディレクトリを作成しておいてください（`sudo mkdir -p /srv/guru/agent`）。
ダッシュボードとワーカー API を 2 つのホスト名に分けることもできます — server ブロックを 2 つ、それぞれの
名前の証明書を 1 つずつ — ただし `agent_public_base_url` がワーカー用の location を載せたほうを
指していることが条件です。

```nginx
map $http_upgrade $connection_upgrade {
    default upgrade;
    ''      close;
}

server {
    listen 443 ssl so_keepalive=60s:15s:4;
    listen [::]:443 ssl so_keepalive=60s:15s:4;
    http2 on;                 # nginx ≥ 1.25.1; older builds: `listen 443 ssl http2;`
    server_name guru.example.com;

    ssl_certificate     /etc/letsencrypt/live/guru.example.com/fullchain.pem;
    ssl_certificate_key /etc/letsencrypt/live/guru.example.com/privkey.pem;

    # ---- worker agent API: TLS terminated here, plaintext h2c to master-workers.
    location ^~ /guru.orchestration.agent.WorkerAgent/ {
        grpc_pass grpc://127.0.0.1:50052;
        grpc_connect_timeout 5s;
        grpc_read_timeout 7d;     # WatchConfig of a worker built before keep-alives is silent for hours
        grpc_send_timeout 7d;
        grpc_socket_keepalive on;
        client_max_body_size 0;   # ReportHealth is one body that grows for the session's life
        client_body_timeout 60s;  # four missed health reports
        grpc_set_header x-api-key     $http_x_api_key;
        grpc_set_header x-refresh-key $http_x_refresh_key;
        grpc_set_header X-Real-IP     $remote_addr;
    }

    # ---- agent binaries, installer, unit and start guard.
    location ^~ /agent/ {
        alias /srv/guru/agent/;
        autoindex off;
        default_type application/octet-stream;
        add_header Cache-Control "public, max-age=300";
    }

    # ---- dashboard, with the two headers the app needs.
    location ^~ / {
        proxy_pass http://127.0.0.1:3000;
        proxy_http_version 1.1;
        proxy_set_header Host              $host;
        proxy_set_header X-Forwarded-Host  $http_host;   # $host drops the port
        proxy_set_header X-Forwarded-Proto $scheme;
        proxy_set_header X-Forwarded-For   $proxy_add_x_forwarded_for;
        proxy_set_header Upgrade           $http_upgrade;
        proxy_set_header Connection        $connection_upgrade;
        proxy_buffering off;      # SvelteKit streams responses
        proxy_read_timeout 3600s;
    }
}
```

ワーカー用 location の各設定については
[ネイティブにデプロイ → nginx](/ja/guides/deploy-natively/#7-nginx-ダッシュボードとワーカー-api-を-1-つのホスト名に載せる)
で説明しています。nginx のデフォルトである 60 秒のままだと、キープアライブ以前にビルドされたワーカーが 1 分ごとに
再登録することになります。Cloudflare の後ろに置く場合は
[ネイティブにデプロイ → Cloudflare の後ろで](/ja/guides/deploy-natively/#cloudflare-の後ろで)を参照してください。
その他に有用なもの: 実際のクライアント IP が欲しい場合は `ADDRESS_HEADER=x-forwarded-for`、
非常に大きなキャンバスをインポートすることがある場合は `BODY_SIZE_LIMIT`（デフォルト `512K`）。

あとは `https://guru.example.com/` を開くと `/auth` にリダイレクトされるので、セクション 8 のアカウントで
サインインしてください。サイドバーにメールアドレスが表示された状態で、キャンバス一覧に到達するはずです。

## 10. GitHub リリースから master と `manage-tool` を取得する

`master-v*` タグは、GHCR のイメージ*に加えて*、`guru-master <version>` という名前の GitHub リリースも
公開します。`<version>` はタグから `master-` 接頭辞を除いたもので（`master-v0.3.0` なら `v0.3.0`）、
`vX.Y.Z` でないタグはプレリリースとして公開されます。アセットは 2 つで、どちらも
`x86_64-unknown-linux-gnu` だけです — 両方とも glibc にリンクしており、これは `distroless/cc` イメージと
同じ ABI です:

| アセット | 中身 |
|---|---|
| `guru-master-v0.3.0-x86_64-unknown-linux-gnu` | コントロールプレーン本体。Docker を使わずに動かす場合に使います |
| `manage-tool-v0.3.0-x86_64-unknown-linux-gnu` | 管理 CLI。イメージデプロイで必要になるのはこちらだけです |

:::caution[ワークフローより前のタグにはアセットがありません]
*Release Master* ワークフローは最初の master タグより後にできました。それより前の `master-v*` タグ
（執筆時点では `master-v0.0.1-alpha`、`master-v0.1.0-alpha`、`master-v0.2.0-beta`）にはイメージはあっても
リリースアセットはありません。固定したタグにアセットがない場合は、より新しいタグを使うか、`manage-tool` を
チェックアウトからビルドしてください（手順 8）。アセットの有無は次のセクションの `jq` スニペット
（`startswith("master-")` に変えるだけ）で確認できます。
:::

アセットが実際に付いているリリース（できればセクション 3 で固定した `MASTER_VERSION` と同じタグ）から
`manage-tool` を取ってくれば、Rust のツールチェーンを一切入れずに手順 7 と手順 8 を進められます:

```sh
VERSION=v0.3.0                       # アセットが列挙されているリリースを選ぶこと
gh release download "master-${VERSION}" \
  --repo haruki-nikaidou/proxy-guru \
  --pattern "manage-tool-${VERSION}-x86_64-unknown-linux-gnu" \
  --output manage-tool
chmod +x manage-tool
./manage-tool --help
```

`gh` を使わない場合は、次のセクションの `curl` + `jq` の手順がそのまま使えます。タグを
`master-${VERSION}` に、アセット名を `manage-tool-${VERSION}-x86_64-unknown-linux-gnu` に変えるだけです。

ダウンロードした `manage-tool` は、チェックアウトでビルドした `./target/release/manage-tool` と完全に
交換可能です。このガイドのコマンドはすべて後者の形で書いていますが、自分が置いたバイナリのパスに
読み替えて構いません。フラグ、サブコマンド、挙動はまったく同じです。

スキーマに別途の成果物は要りません。マイグレーションは両方のバイナリにコンパイル済みで組み込まれているため、
ダウンロードしたバイナリで `manage-tool db migrate` を実行すれば、対応する master が適用するのとまったく
同じものが適用されます。

## 11. GitHub リリースからワーカーバイナリを取得する

データプレーンはイメージではなく生のバイナリとして配布され、公開されているのは `linux/x86_64` のみです。
各 `worker-<version>` リリースはアセットを 2 つ持ち、`<version>` はタグから `worker-` 接頭辞を除いた
ものです — つまりタグ `worker-v0.1.0` は次の 2 つを公開します:

| アセット | リンク方法 | 配る先 |
|---|---|---|
| `guru-worker-v0.1.0-x86_64-unknown-linux-gnu` | glibc への動的リンク | 現行の Debian/Ubuntu/RHEL など glibc のディストリビューション |
| `guru-worker-v0.1.0-x86_64-unknown-linux-musl` | musl libc を静的リンク（static-pie） | Alpine やその他の musl ディストリビューション。libc がインストールされていないホストでもそのまま動きます |

選ぶ基準はノードの libc だけです。glibc のホストには `gnu` を、Alpine のような musl のホストには `musl` を
配ってください。判断がつかないときは `musl` が安全側です — musl libc をバイナリに取り込んだ静的バイナリなので
実行時に libc を必要とせず、glibc のホストでも動きます。その代わり musl 版は glibc の NSS モジュールを使わない
ため、`nsswitch.conf` で独自の名前解決を組んでいる glibc ホストでは `gnu` を選んでください。

そのアセットが実際に列挙されているリリースを選び、スクリプトを組む前に確認してください:

```sh
curl -fsSL https://api.github.com/repos/haruki-nikaidou/proxy-guru/releases \
  | jq -r '.[] | select(.tag_name | startswith("worker-"))
           | .tag_name + " -> " + ((.assets | map(.name)) | join(", "))'
```

:::caution[すべてのタグにバイナリがあるわけではありません]
リリースにアセットが列挙されていない `worker-v*` タグは、リリースワークフローが存在する前に付けられた
ものです（執筆時点では `worker-v0.0.1-alpha` がまさにその状態で、`assets: []` です）。そのようなタグから
ダウンロードできるものはありません — より新しいリリースを使うか、新しい `worker-v*` タグを push して
*Release Worker* ワークフローにバイナリをビルド・添付させてください。musl ビルドが追加される前のリリースには
`x86_64-unknown-linux-gnu` しか無いので、上のコマンドはアセット名も一緒に確認しています。
:::

GitHub CLI を使う場合 — どちらのアセットが欲しいかは `TARGET` で明示します:

```sh
VERSION=v0.1.0
TARGET=x86_64-unknown-linux-gnu      # Alpine/musl のノードでは x86_64-unknown-linux-musl
gh release download "worker-${VERSION}" \
  --repo haruki-nikaidou/proxy-guru \
  --pattern "guru-worker-${VERSION}-${TARGET}" \
  --output guru-worker
```

あるいは素の `curl` で — URL をハードコードしないよう、API 経由でアセットを解決します。ここでも名前を
完全一致で選び、2 つのうちどちらが降ってくるかが曖昧にならないようにします:

```sh
VERSION=v0.1.0
TARGET=x86_64-unknown-linux-gnu      # Alpine/musl のノードでは x86_64-unknown-linux-musl
url=$(curl -fsSL \
  "https://api.github.com/repos/haruki-nikaidou/proxy-guru/releases/tags/worker-${VERSION}" \
  | jq -r --arg name "guru-worker-${VERSION}-${TARGET}" \
      '.assets[] | select(.name == $name) | .browser_download_url')
curl -fsSL "$url" -o guru-worker
```

続いて実行権限を付け、起動することを確認します — バイナリは自身のバージョンを出力し、これは
`manage-tool agent publish` が記録するものでもあります:

```sh
chmod +x guru-worker
./guru-worker --version
# guru-worker 0.4.0-beta
```

バイナリは自分の成果物ストア（社内 HTTP サーバー、apt/OCI レジストリ、構成管理システム）にバージョンと
ターゲットをキーとして保管してください。`latest` エイリアスも公開されたチェックサムファイルもないので、
配布するコピーと一緒にバージョンとターゲット — できれば自分で取った `sha256sum` も — を記録しておきましょう。

取り違えていないかは、配る前にファイル自身に聞けます。`file guru-worker` は `gnu` 版を
`dynamically linked, interpreter /lib64/ld-linux-x86-64.so.2` と報告し、`musl` 版は
`static-pie linked` — インタープリターを持たず、ホストにインストールされた libc を必要としません。

このコントロールプレーンに対してワーカーノードをインストールし登録する手順は別途扱います。ここまでの内容は
「バイナリが利用可能で配布できる」状態で終わりです。コントロールプレーンをまったく使わずに動かすべきノードに
ついては別のガイドを参照してください:
[独立ワーカーのデプロイ](/ja/guides/independent-worker/)。

## 12. デプロイを検証する

以下を順に実施してください — どれも個別に、はっきりと失敗します:

```sh
# 1. データストア
docker compose ps                     # postgres + iggy が healthy、redis は up

# 2. スキーマ
./manage-tool db migrate              # セクション 6 のもの
#   → schema is up to date

# 3. コントロールプレーン: モードごとにバナーが 1 行、再起動ループがないこと
docker compose logs --tail=20 master-dashboard master-workers master-consumer master-cron master-notifier

# 4. Worker API through nginx: HTTP/2 200 with a grpc-status header means the
#    master answered (13 = "Missing request message", which an empty probe earns)
curl --http2 -sS -D - -o /dev/null -X POST -H 'content-type: application/grpc' \
  --data-binary '' https://guru.example.com/guru.orchestration.agent.WorkerAgent/Register

# 5. プロキシ経由のダッシュボード（/auth へ 303）
curl -s -o /dev/null -w '%{http_code}\n' https://guru.example.com/

# 6. ライブバス: ダッシュボードレプリカごとに 1 行、起動時と Redis への再接続ごとに
#    出力されます。オペレーター API の `Watch*` ストリーム — ひいてはダッシュボードで
#    ライブ更新されるキャンバスページとヘルスページ — はここから配信されています。
docker compose logs master-dashboard | grep 'live bus connected'

# 7. 管理者アカウントでログインする — ダッシュボード → オペレーター API → データベースを
#    端から端まで動かすチェックはこれだけです。
```

手順 1〜5 が通るのに手順 7 が `Forbidden` で失敗する場合は、セクション 9 のプロキシに関する警告を読み直して
ください。

## 13. アップグレード、バックアップ、ロールバック

**アップグレード。** `.env` の `MASTER_VERSION`（ダッシュボードにも新しいタグがあれば `FRONTEND_VERSION` も）
を上げ、`docker compose pull && docker compose up -d` を実行します。新しい master は起動しながら、未適用の
マイグレーションを自分で適用します。

ロールバックとは、バージョン変数を前のタグに固定し直すことです — ただしそれでマイグレーションが取り消される
わけではないため、古いバイナリが読んでいるものを削除・リネームするマイグレーションを含むリリースは、この方法では
ロールバックできません。該当する場合はリリースノートに記載されます。

**バックアップ。** 代替不能な状態を持つのは PostgreSQL だけです:

```sh
docker compose exec -T postgres pg_dump -U "$POSTGRES_USER" "$POSTGRES_DB" \
  > guru-$(date +%F).sql
```

素早いリストア経路が欲しければ `postgres-data` ボリュームのスナップショットも取ってください。Iggy のログ
（`iggy-data` ボリューム）が保持しているのは編集ヒント、ヘルスの事実、未配信の通知です。編集ヒントを失っても
`cron` の `derive_stale_canvases` がデータベースから拾い直しますが、未配信の通知は失われます。Iggy は 5 つの
モードすべての起動に必要なので、*稼働している*必要があります。Redis にはバックアップするものが何もありません: ボリュームも、
AOF も RDB もなく、運んでいるのは進行中のライブイベントだけです。新しい空の Redis で起動し直しても、
subscriber が再接続した時点で元どおりに動きます。

**ログ。** すべて標準出力への構造化された `tracing` 出力で、`GURU_LOG_LEVEL` は完全な `EnvFilter` 文字列
（`info`、`warn`、`guru_master=debug,orchestration=debug` など）を受け取ります。いつも使っている Docker の
ログドライバーで収集してください。

## 14. トラブルシューティング

| 症状 | 原因 |
|---|---|
| ダッシュボードのログインが `Forbidden` / `Cross-site remote requests are forbidden` を返す | 再構成されたオリジンがブラウザーの `Origin` と一致していません。HTTPS で配信するか、`PROTOCOL_HEADER`/`HOST_HEADER` を設定して `X-Forwarded-Proto` と `X-Forwarded-Host`（ポート付き）を転送してください。`ORIGIN` は効きません。 |
| ログインは成功するが、次のリクエストで `/auth` に戻される | `Secure` なセッション Cookie が破棄されました — ブラウザーが平文 HTTP でダッシュボードに到達しています。 |
| master が `every mode opens the database: set GURU_DATABASE_URL (or pass --database-url)` で終了する | URL が未設定か空です。デフォルト値はなく、`cron` を含むすべてのモードが必要とします。 |
| master が `master key: GURU_MASTER_KEY is not set`（あるいは `must be 32 bytes`）で終了する | `dashboard_grpc`、`workers_grpc`、`consumer`、`cron` はこのキーを必要とします（`notifier` は読みません）。`manage-tool generate-master-key` で生成してください。環境変数からのみ読み込まれます。 |
| master が `stored config for key ... does not match its type` で終了する | 保存されているドキュメントが壊れているか、フィールド名の変更より古いものです。`manage-tool config get <key>` で確認し、`config set` で書き直してください。 |
| TLS クライアントポッドが `certificate for … is pending` / `failed: …` のまま `invalid_pods` に留まる | ACME パスがまだ発行していないか、直前の試行が失敗しています（`ListCertificates` に `last_error` が出ます）。これは `cron` 内の `renew_certificates` ジョブで動きます: `cron` が起動していること、DNS プロバイダーのトークンと `domain_id`（Cloudflare の zone id / Vercel の domain）が正しいこと、`cron` が ACME ディレクトリに到達できることを確認してください。`RetryCertificate` で再試行を強制できます。 |
| リレーの Pod が `internal CA not initialised` のまま `invalid_pods` に留まる | `manage-tool orchestration init-ca` を一度実行してください。 |
| master が `Iggy is required: set IGGY_URL (or pass --iggy-url), …` で即座に終了する | `IGGY_URL` が未設定です。5 つのモードすべてが Iggy を必要とします。設定されていて起動に失敗する場合は、Iggy に到達できるか、URL の認証情報が `IGGY_USER`/`IGGY_PASSWORD` と一致しているかを確認してください。 |
| master が Redis エラーで即座に終了する | `REDIS_URL` が未設定か、サーバーに到達できません。`notifier` 以外のすべてのモードがこれを必要とします。 |
| 2 つ目の `notifier` が `another notifier already holds the advisory lock; run exactly one` で終了する | 想定される挙動です: 配信は単一インスタンスです。レプリカ数を 1 にするか、ロックを保持しているピアを停止してください。 |
| クリーンインストール直後に `relation "…" does not exist` が出る | マイグレーションが実行されていません。`GURU_DATABASE_URL` のロールにそのデータベースへの `CREATE` 権限がない可能性があります。`manage-tool db migrate` を実行し、そのエラーを読んでください。 |
| `manage-tool` が間違ったデータベースに書き込んだ | 作業ディレクトリの `.env` が `GURU_DATABASE_URL` を与えていました。常に `--database-url` を渡してください。 |
| キャンバスの編集がワーカーに届かない | `consumer` が停止しています: `CanvasDirty` の導出ハンドラーを実行するため、これなしでは編集はすぐには導出されません。`cron` の `derive_stale_canvases` が最大 30 秒ほどで拾うはずなので、それも起きない場合は `cron` を確認してください。 |
| 定期ジョブが動かなくなる（`Offline` にならない、更新も走らない） | `cron` が停止しています。ログの `cron job failed` も確認してください。 |
| 開いているキャンバスページやヘルスページの更新が止まる（再読み込みすると変更が見える）、または `Watch*` ストリームがスナップショットを配信しなくなる | Redis が停止しているか、そのストリームを提供している `dashboard_grpc` レプリカから到達できません。そのレプリカのログで `live bus connected` を探してください。編集自体は適用され、導出も走ります。止まっているのはライブ配信だけで、再接続すれば再開します。 |

すべてのフラグと変数については[設定](/ja/reference/configuration/)を、「導出」が実際に何をするのかについては
[ロールアウト](/ja/reference/rollout/)を参照してください。
