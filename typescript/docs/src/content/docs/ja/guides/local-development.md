---
title: ローカル開発
description: SurrealDB、RabbitMQ、コントロールプレーン、ダッシュボードを 1 台のマシンで立ち上げます。
---

コントロールプレーンには SurrealDB インスタンスと AMQP ブローカーが必要です。それ以外はすべてワークスペースから
実行できます。

:::caution[リポジトリの `.env` は開発用プロファイルではありません]
ルートの `.env` には**本番**の認証情報（`SURREALDB_HOST`、`SURREALDB_USER`、
`SURREALDB_PASSWORD`、`SURREALDB_NAMESPACE`、`SURREALDB_NAME`、`AMQP_URI`）が入っていることがあり、起動した
すべてのプロセスがそれを継承します。データベース関連のフラグを明示的に渡す（あるいは変数を上書きする）ことで、
ローカル実行がうっかりリモートのデータベースに接続しないようにしてください。
:::

## 1. 依存サービス

```sh
docker run -d --name guru-surreal -p 8000:8000 \
  surrealdb/surrealdb:latest start --user root --pass root

docker run -d --name guru-rabbit -p 5672:5672 -p 15672:15672 rabbitmq:4-alpine
```

SurrealDB は **3.2 以降**のサーバーを使用してください。3.0 系の古いバイナリはワークスペースがリンクしている
クライアントと互換性がなく、同一トランザクション内で先に書き込まれた行を読む ASSERT を正しく扱えません。

ブローカーの URI の書式には注意が必要です。デフォルトの vhost には `amqp://guest:guest@127.0.0.1:5672/` を使います。

## 2. スキーマ

スキーマは `database/schema/*.surql`（モジュールごとに 1 ファイル）に置かれ、
[surrealkit](https://surrealdb.com/) で管理します:

```sh
surrealkit sync --host ws://127.0.0.1:8000 --ns guru --db guru
```

続いて、モジュールのデフォルト設定を `app_config` テーブルへ書き込みます。この処理は冪等で、編集済みの値を
上書きすることはないため、sync のたびに再実行してください:

```sh
cargo run -p manage-tool -- \
  --address ws://127.0.0.1:8000 --username root --password root \
  --namespace guru --database guru \
  config seed
```

`config list`、`config get <key>`、`config set <key> <json>` でこれらの値を確認・変更できます。マスターは
再起動時に値を読み込みます。
[設定 → モジュール設定](/ja/reference/configuration/#モジュール設定)を参照してください。

## 3. 管理者アカウントの初期作成

```sh
cargo run -p manage-tool -- \
  --address ws://127.0.0.1:8000 --username root --password root \
  --namespace guru --database guru \
  create-admin --email admin@example.com --password 'change-me'
```

## 4. コントロールプレーン

データベースに触れるすべてのモードは `GURU_MASTER_KEY` でシークレットを復号し、この変数にデフォルト値はありません。
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
  --address ws://127.0.0.1:8000 --username root --password root \
  --namespace guru --database guru \
  --amqp-uri 'amqp://guest:guest@127.0.0.1:5672/'
```

`consumer` が動いていないとキャンバスは一切導出されないため、2 つ目のシェルで起動してください。これは編集の
フックであると同時に、すべての定期ジョブ（古いキャンバスのスイープ、死活監視のスイープ、ヘルス履歴の保持、
ACME とリレーリーフ証明書のローテーション）の実行主体でもあります:

```sh
cargo run -p guru-master -- \
  --mode consumer \
  --address ws://127.0.0.1:8000 --username root --password root \
  --namespace guru --database guru \
  --amqp-uri 'amqp://guest:guest@127.0.0.1:5672/'
```

3 つ目のシェルではクロックを動かします。`cron` は実行時刻になったジョブごとに実行シグナルを 1 件発行するだけで、
それ以外は何もしません。データベース接続を開かず、`GURU_MASTER_KEY` を読まず、データベース引数も取らないため、
設定はブローカーの URI だけです:

```sh
env -u GURU_MASTER_KEY cargo run -p guru-master -- \
  --mode cron \
  --amqp-uri 'amqp://guest:guest@127.0.0.1:5672/'
```

4 つ目のモードは `workers_grpc` で、ワーカー API と設定ビューのポーラーを兼ねており、引数は `dashboard_grpc` と
まったく同じです。すべてのモードはブローカーを必要とします。RabbitMQ が停止していれば何も起動せず、`cron` か
`consumer` が停止していれば定期ジョブは一切実行されません。

定期ジョブを間隔の到来を待たずに実行させたい場合は、そのクレーム行を削除します。テーブルは
`orchestration_job_run` で、ジョブ名をキーにジョブごとに 1 行あるため、
`DELETE orchestration_job_run:sweep_liveness` を実行すれば次の `sweep_liveness` シグナルで実際に処理が走ります。

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

## テスト

モジュールの結合テストはインメモリの SurrealDB（`mem://`）に対して実行され、モジュール自身のスキーマファイルを
適用するため、サーバーを起動しておく必要はありません:

```sh
cargo test
```
