---
title: ワークスペース構成
description: Rust クレート、protobuf 定義、スキーマファイル、TypeScript パッケージの配置場所。
---

```text
bin/              # Rust バイナリ — 配線のみ、ビジネスロジックは持たない
  guru-master/        # コントロールプレーン。1 バイナリ、4 つのワーカーモード
  guru-worker/        # データプレーン
  manage-tool/        # 管理 CLI
lib/
  rpguru_sdk/         # 生成された gRPC/protobuf 型（Rust）
  guru_topology/      # ポッドグラフの検査とコンパイル（純粋、I/O なし）
  guru_worker_config/ # ワーカー設定モデル、両プレーンで共有
  db_types/           # 型付き行 ID とテキスト列挙のための table_record! / text_enum! マクロ
modules/          # ビジネスロジック、機能ごとに 1 クレート
  auth/  orchestration/  notify/  base/
proto/            # protobuf 定義（モジュール単位でグループ化）— API の唯一の定義元
database/         # PostgreSQL のスキーマ: sqlx マイグレーション（database/migrations）
typescript/       # Bun ワークスペース: すべてのフロントエンド / TypeScript パッケージ
  app-protobuf/       # 生成された gRPC/protobuf の TypeScript コード（共有）
  guru-frontend/      # SvelteKit ダッシュボード
  guru-graph/         # キャンバスの描画と、編集をひとつのバッチにする処理（純粋な TS）
  docs/               # このドキュメントサイト（Astro Starlight）
package.json      # Bun ワークスペースのルート（workspaces: ["typescript/*"]）
```

Rust バイナリは `bin/` 配下に置きます — リポジトリのルートには置きません。TypeScript/JavaScript のパッケージはすべて `typescript/` 配下に置き、単一の Bun ワークスペースの一部とします。ワークスペースにリンクされていない独立した npm/pnpm プロジェクトは許可されません。

## 生成される API コード

`proto/` は API の唯一の情報源（single source of truth）です。

- **Rust:** `.proto` ファイルを追加し、`rpguru_sdk` の `build.rs` に登録します。
- **TypeScript:** `bun run generate:proto` で再生成します（実体は `typescript/app-protobuf/generate-proto.sh`）。出力先は `typescript/app-protobuf/src/generated/` で、実行ごとに空にされて書き直されます — 手で編集しないでください。

フロントエンドのパッケージは `app-protobuf` に依存し（`"app-protobuf": "workspace:*"`）、proto ツリーの構造をそのまま反映したサブパスで生成モジュールを import します:

```ts
import { GreeterDefinition } from 'app-protobuf/sample/hello';
```

## ドキュメントサイト

このサイトは `typescript/docs` で、[`starlight-theme-black`](https://starlight-theme-black.vercel.app/) をテーマに適用した Astro Starlight プロジェクトです。

```text
typescript/docs/
├── astro.config.mjs           # Starlight の設定 + starlight-theme-black プラグイン
├── src/
│   ├── content.config.ts      # docs コレクション。スキーマはテーマが拡張
│   └── content/docs/
│       ├── index.mdx           # スプラッシュ形式のホームページ
│       ├── guides/             # タスク指向のページ
│       └── reference/          # リファレンスページ
```

ページを追加するには、`title` と `description` のフロントマターを持つ Markdown/MDX ファイルを `guides/` または `reference/` に置き、`astro.config.mjs` の `sidebar` 配列に登録します。

## 新しいモジュールを追加する

1. `modules/base` のディレクトリ構成を `modules/<name>` にコピーします。
2. ルートの `Cargo.toml` のワークスペース `members` にクレートを追加します。
3. スキーマを `database/schema/<name>.surql` に、API を `proto/` に定義します（`rpguru_sdk` への登録も行います）。
4. 内側から外側へ実装します: `entities` → `services` → `rpc`/`hooks`。
5. 新しいサービスとフックを `bin/guru-master` のワーカーに組み込みます。
6. `config` の値は `bin/manage-tool` からシードできる状態に保ちます。
