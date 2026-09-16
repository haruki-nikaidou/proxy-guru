---
title: 工作区布局
description: Rust crate、protobuf 定义、Schema 文件与 TypeScript 包分别放在什么位置。
---

```text
bin/              # Rust 二进制 —— 只做装配，不含业务逻辑
  guru-master/        # 控制平面；单个二进制，四种 worker 模式
  guru-worker/        # 数据平面
  manage-tool/        # 管理 CLI
lib/
  rpguru_sdk/         # 生成的 gRPC/protobuf 类型（Rust）
  guru_worker_config/ # worker 配置模型，由两个平面共享
  db_types/           # table_record! 与 text_enum!：带类型的行 id 和文本枚举
modules/          # 业务逻辑，每个功能一个 crate
  auth/  orchestration/  notify/  base/
proto/            # protobuf 定义（按模块分组）—— 唯一的 API 来源
database/         # PostgreSQL Schema：sqlx migration（database/migrations）
typescript/       # Bun 工作区：所有前端 / TypeScript 包
  app-protobuf/       # 生成的 gRPC/protobuf TypeScript 代码（共享）
  guru-frontend/      # SvelteKit 控制台
  docs/               # 本文档站点（Astro Starlight）
package.json      # Bun 工作区根（workspaces: ["typescript/*"]）
```

Rust 二进制一律位于 `bin/` 之下 —— 绝不放在仓库根目录。所有 TypeScript/JavaScript 包都位于
`typescript/` 之下，并属于那唯一的 Bun 工作区；不允许出现独立的、未纳入工作区的 npm/pnpm 项目。

## 生成的 API 代码

`proto/` 是 API 的唯一事实来源。

- **Rust：** 添加 `.proto` 文件，并在 `rpguru_sdk` 的 `build.rs` 中注册它。
- **TypeScript：** 用 `bun run generate:proto` 重新生成（由
  `typescript/app-protobuf/generate-proto.sh` 驱动）。产物输出到
  `typescript/app-protobuf/src/generated/`，该目录每次运行都会被清空并重写 —— 绝不要手工编辑。

前端包依赖 `app-protobuf`（`"app-protobuf": "workspace:*"`），并按子路径导入生成的模块，路径与
proto 目录树一一对应：

```ts
import { GreeterDefinition } from 'app-protobuf/sample/hello';
```

## 文档站点

本站点即 `typescript/docs`，一个使用
[`starlight-theme-black`](https://starlight-theme-black.vercel.app/) 主题的 Astro Starlight 项目。

```text
typescript/docs/
├── astro.config.mjs           # Starlight 配置 + starlight-theme-black 插件
├── src/
│   ├── content.config.ts      # docs 集合，其 schema 由主题扩展
│   └── content/docs/
│       ├── index.mdx           # splash 首页
│       ├── guides/             # 任务导向页面
│       └── reference/          # 查阅型页面
```

新增页面的方式：在 `guides/` 或 `reference/` 中放入一个带 `title` 和 `description` frontmatter 的
Markdown/MDX 文件，然后把它列入 `astro.config.mjs` 里的 `sidebar` 数组。

## 新增一个模块

1. 把 `modules/base` 的目录布局复制到 `modules/<name>`。
2. 在根 `Cargo.toml` 的工作区 `members` 中加入该 crate。
3. 在 `database/schema/<name>.surql` 中定义 Schema，在 `proto/` 中定义 API（并在 `rpguru_sdk` 中
   注册）。
4. 由内向外实现：`entities` → `services` → `rpc`/`hooks`。
5. 把新的 services 与 hooks 接入 `bin/guru-master` 的各个 worker。
6. 保证 `config` 的取值可以通过 `bin/manage-tool` 播种。
