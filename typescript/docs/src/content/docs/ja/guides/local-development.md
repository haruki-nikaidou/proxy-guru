---
title: ローカル開発
description: PostgreSQL、Iggy、Redis、コントロールプレーン、ダッシュボードを 1 台のマシンで立ち上げます。
---

コントロールプレーンには PostgreSQL データベース、Apache Iggy サーバー、そして Redis サーバーが必要です。
それ以外はすべてワークスペースから実行できます。

:::caution[リポジトリの `.env` は開発用プロファイルではありません]
ルートの `.env` には**本番**の認証情報（`GURU_DATABASE_URL`、`IGGY_URL`、
`REDIS_URL`）が入っていることがあり、起動した
すべてのプロセスがそれを継承します。`--database-url` を明示的に渡す（あるいは変数を上書きする）ことで、
ローカル実行がうっかりリモートのデータベースに接続しないようにしてください。
:::

## 1. 依存サービス

```sh
docker run -d --name guru-postgres -p 15432:5432 \
  -e POSTGRES_USER=guru -e POSTGRES_PASSWORD=guru -e POSTGRES_DB=guru \
  postgres:18.6-alpine

# Iggy 0.9.x needs SYS_NICE, seccomp=unconfined and unlimited memlock (io_uring);
# `docker compose up iggy` from the repository root starts the same thing.
docker run -d --name guru-iggy -p 8090:8090 \
  --cap-add SYS_NICE --security-opt seccomp=unconfined --ulimit memlock=-1:-1 \
  -e IGGY_ROOT_USERNAME=iggy -e IGGY_ROOT_PASSWORD=iggy \
  -e IGGY_TCP_ADDRESS=0.0.0.0:8090 -e IGGY_NODE_ADVERTISED_ADDRESS=localhost \
  -e IGGY_QUIC_ENABLED=false -e IGGY_WEBSOCKET_ENABLED=false \
  apache/iggy:0.9.0

# Pub/sub only, so nothing is persisted; `docker compose up redis` from the
# repository root starts the same thing.
docker run -d --name guru-redis -p 6379:6379 redis:7-alpine \
  redis-server --save '' --appendonly no
```

PostgreSQL は **16 以降**を使用してください。

Iggy の URL は `iggy://<user>:<password>@<host>:8090` の形式で、ここでは `iggy://iggy:iggy@127.0.0.1:8090` です。
ストリームとトピックは各モードが起動時に作成するため、事前に作るものはありません。
Redis は `redis://127.0.0.1:6379/` を受け取り、認証情報はありません。ホスト側の `6379` を別のプロセスが
使っている場合は、`docker compose up redis` の前に `.env` で `REDIS_PORT`（例: `16379`）を設定して
ホスト側ポートをずらし、`REDIS_URL` にも同じポートを書いてください。

## 2. スキーマ

スキーマは `migrations/` に置かれた sqlx のマイグレーション群で、バイナリに埋め込まれています。
`guru-master` は起動時に未適用のものを適用します。手動で行う場合は次のようにします:

```sh
export GURU_DATABASE_URL=postgres://guru:guru@127.0.0.1:15432/guru
cargo run -p manage-tool -- db migrate
```

続いて、モジュールのデフォルト設定を `app_config` テーブルへ書き込みます。この処理は冪等で、編集済みの値を
上書きすることはないため、マイグレーションのたびに再実行してください:

```sh
cargo run -p manage-tool -- config seed
```

`config list`、`config get <key>`、`config set <key> <json>` でこれらの値を確認・変更できます。マスターは
再起動時に値を読み込みます。
[設定 → モジュール設定](/ja/reference/configuration/#モジュール設定)を参照してください。

## 3. 管理者アカウントの初期作成

```sh
cargo run -p manage-tool -- --database-url "$GURU_DATABASE_URL" \
  create-admin --email admin@example.com --password 'change-me'
```

## 4. コントロールプレーン

`notifier` 以外のすべてのモードは `GURU_MASTER_KEY` でシークレットを復号し、この変数にデフォルト値はありません。
指定しない場合、プロセスは起動時に
`Error: "master key: GURU_MASTER_KEY is not set"` で停止します。キーは一度だけ生成し（このサブコマンドは
データベースを必要としません）、マスターを起動するシェルに設定しておきます:

```sh
cargo run -p manage-tool -- generate-master-key
# j7ILadgGjBy+jYMIJuiPBl5eai65t7G8G4XimNcyLpU=

export GURU_MASTER_KEY='<the printed value>'
```

各モードは別々のプロセスです。ダッシュボードが通信するオペレーター API は `dashboard_grpc` です:

```sh
cargo run -p guru-master -- \
  --mode dashboard_grpc \
  --database-url "$GURU_DATABASE_URL" \
  --iggy-url 'iggy://iggy:iggy@127.0.0.1:8090' \
  --redis-url 'redis://127.0.0.1:6379/'
```

`consumer` が動いていないと編集したキャンバスは導出されないため、2 つ目のシェルで起動してください。これは
Iggy のイベントハンドラー — `CanvasDirty` による導出と、`HealthChanged` による通知のファンアウト — を実行します:

```sh
cargo run -p guru-master -- \
  --mode consumer \
  --database-url "$GURU_DATABASE_URL" \
  --iggy-url 'iggy://iggy:iggy@127.0.0.1:8090' \
  --redis-url 'redis://127.0.0.1:6379/'
```

3 つ目のシェルでは `cron` を動かします。6 つの定期ジョブ（古いキャンバスのスイープ、死活監視のスイープ、
ヘルス履歴の保持、ACME とリレーリーフ証明書のローテーション、サーバーの国の解決）をすべてプロセス内で実行し、
最初の実行は起動時です。ジョブはキャンバスを導出し、証明書を更新し、イベントを publish するため、引数は
`consumer` と同じです:

```sh
cargo run -p guru-master -- \
  --mode cron \
  --database-url "$GURU_DATABASE_URL" \
  --iggy-url 'iggy://iggy:iggy@127.0.0.1:8090' \
  --redis-url 'redis://127.0.0.1:6379/'
```

通知の配信は専用のモードが担い、そのインスタンスはちょうど 1 つだけです。マスターキーも Redis も必要とせず、
キャンバスやアカウントに通知設定ができて初めて何かをします（[通知](/ja/features/notifications/)）:

```sh
env -u GURU_MASTER_KEY GURU_TELEGRAM_BOT_TOKEN=... cargo run -p guru-master -- \
  --mode notifier \
  --database-url "$GURU_DATABASE_URL" \
  --iggy-url 'iggy://iggy:iggy@127.0.0.1:8090'
```

2 つ目の `notifier` は起動を拒否します — `another notifier already holds the advisory lock; run
exactly one` — これはバグではなく、ロックが効いている証拠です。メールについては、`notify` の `smtp_host` を
ローカルのシンクに向けてください（`docker run -d -p 1025:1025 -p 8025:8025 axllent/mailpit` の後に
`manage-tool config set notify '{"smtp_host":"127.0.0.1","smtp_port":1025,"smtp_starttls":false}'`）。
届いたメールは `http://127.0.0.1:8025` で読めます。

5 つ目のモードは `workers_grpc` で、ワーカー API と設定ビューのポーラーを兼ねており、引数は `dashboard_grpc` と
まったく同じです。すべてのモードは PostgreSQL と Iggy を必要とし、`IGGY_URL` がなければどのモードも
起動時に失敗します。`cron` が停止していれば定期ジョブは一切実行されません。`--redis-url`/`REDIS_URL` は
`notifier` 以外のすべてのモードで必須で、これがなければそれらのモードは起動しません。
これはオペレーター API の `Watch*` ストリームを支えるライブバスで、
ダッシュボードのキャンバスエディターとヘルスページはこのストリームを追従しています。Redis が停止すると、
開いているページは更新を停止しますが（*ライブ* バッジはブラウザー自身の接続を示すもので、緑のままです）、
編集と導出はいつもどおり続き、Redis が復旧すればページは自動的に追いつきます。

定期ジョブを間隔の到来を待たずに実行させたい場合は、そのクレーム行を削除します。テーブルは
`orchestration_job_run` で、ジョブ名をキーにジョブごとに 1 行あるため、
`DELETE orchestration_job_run:sweep_liveness` を実行すれば次の `sweep_liveness` の起動で実際に処理が走ります。

## 5. ダッシュボード

```sh
bun install
GURU_GRPC_URL=127.0.0.1:50051 bun run dev
```

ワークスペースルートでの `bun run dev` は `guru-frontend` パッケージへ委譲されます。

## 6. ドキュメントサイト

このドキュメント自体もワークスペースのパッケージです:

```sh
bun run --filter guru-docs dev     # or: bun run docs:dev
bun run --filter guru-docs build   # static output in typescript/docs/dist
```

## API クライアントの再生成

`proto/` が唯一の正となる定義です。変更後は TypeScript クライアントを再生成してください（Rust 側は
`rpguru_sdk` の `build.rs` が生成します）:

```sh
bun run generate:proto
```

## クエリを変更したとき

すべてのステートメントは sqlx の `query!` ファミリーを通るため、SQL はクレートのコンパイル時に実際の
スキーマに対して検査されます。通常のビルドにデータベースは不要で、`.sqlx/` にコミットされているオフライン
データを読みます。ステートメント（あるいはマイグレーション）を追加・変更したら、マイグレーション済みの
データベースに対してそのデータを再生成してコミットしてください。さもないと `DATABASE_URL` のないビルドは
古いクエリ集合を見続けます:

```sh
export DATABASE_URL=postgres://guru:guru@127.0.0.1:15432/guru_dev
# manage-tool が読むのは GURU_DATABASE_URL で、DATABASE_URL ではありません。明示的に渡します
cargo run -p manage-tool -- --database-url "$DATABASE_URL" db migrate
cargo sqlx prepare --workspace -- --all-targets   # .sqlx/ を書き換えます
```

`cargo sqlx prepare` には `sqlx-cli` が必要です（`cargo install sqlx-cli --no-default-features
--features postgres,rustls`）。`-- --all-targets` は省略できません。これがないとテスト自身の
ステートメントが `.sqlx/` に入らず、オフラインの `cargo test` がそこで失敗します。`DATABASE_URL` が
export されているとマクロはそのデータベースに直接接続し、`.sqlx/` は無視されます。だからこそ再生成した
データは別途コミットする必要があります。

## テスト

モジュールの結合テストは実際の PostgreSQL サーバーに対して実行されます。`#[sqlx::test]` が `DATABASE_URL`
から使い捨てのデータベースをテストごとに 1 つ作成し、そこへマイグレーションを適用します。これは必ず
**テスト用**のデータベースに向けてください。master が使うデータベースに向けてはいけません — テストランナーは
その隣にデータベースを作成しては削除します:

```sh
export DATABASE_URL=postgres://guru:guru@127.0.0.1:15432/guru_test
SQLX_OFFLINE=true cargo test
```

このデータベースは `createdb`（または `CREATE DATABASE guru_test;`）で一度だけ作成します。ロールには
`CREATEDB` 権限が必要です。テストランナーはテストごとに隣のデータベースを作成してマイグレーションを当てる
ため、`DATABASE_URL` が指すデータベース自体にスキーマは要りません。しかし `query!` マクロはそのデータ
ベースに対してすべてのステートメントを検査しようとし、テーブルがないため失敗します。`SQLX_OFFLINE=true`
はマクロを代わりに `.sqlx/` へ向け、`DATABASE_URL` の 2 つの役割を切り分けます（そのデータベースに一度
マイグレーションを当てても動きます）。
