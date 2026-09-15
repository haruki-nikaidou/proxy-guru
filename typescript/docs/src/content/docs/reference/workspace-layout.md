---
title: Workspace Layout
description: Where Rust crates, protobuf definitions, schema files and TypeScript packages live.
---

```text
bin/              # Rust binaries — wiring only, no business logic
  guru-master/        # control plane; one binary, four worker modes
  guru-worker/        # data plane
  manage-tool/        # admin CLI
lib/
  rpguru_sdk/         # generated gRPC/protobuf types (Rust)
  guru_worker_config/ # worker config model, shared by both planes
  newtype_record_id/  # table_record! macro for typed record ids
modules/          # business logic, one crate per feature
  auth/  orchestration/  notify/  base/
proto/            # protobuf definitions (grouped by module) — the single API source
database/         # SurrealDB schema + seed + tests (managed by surrealkit)
typescript/       # Bun workspace: all frontend / TypeScript packages
  app-protobuf/       # generated gRPC/protobuf TypeScript code (shared)
  guru-frontend/      # SvelteKit dashboard
  docs/               # this documentation site (Astro Starlight)
package.json      # root of the Bun workspace (workspaces: ["typescript/*"])
```

Rust binaries live under `bin/` — never at the repository root. Every TypeScript/JavaScript package
lives under `typescript/` and is part of the single Bun workspace; standalone, unlinked npm/pnpm
projects are not allowed.

## Generated API code

`proto/` is the single source of truth for the API.

- **Rust:** add the `.proto` file and register it in `rpguru_sdk`'s `build.rs`.
- **TypeScript:** regenerate with `bun run generate:proto` (driven by
  `typescript/app-protobuf/generate-proto.sh`). Output lands in
  `typescript/app-protobuf/src/generated/`, which is emptied and rewritten on every run — never
  edit it by hand.

Frontend packages depend on `app-protobuf` (`"app-protobuf": "workspace:*"`) and import generated
modules by subpath, mirroring the proto tree:

```ts
import { GreeterDefinition } from 'app-protobuf/sample/hello';
```

## Documentation site

This site is `typescript/docs`, an Astro Starlight project themed with
[`starlight-theme-black`](https://starlight-theme-black.vercel.app/).

```text
typescript/docs/
├── astro.config.mjs           # Starlight config + starlight-theme-black plugin
├── src/
│   ├── content.config.ts      # docs collection, schema extended by the theme
│   └── content/docs/
│       ├── index.mdx           # splash homepage
│       ├── guides/             # task-oriented pages
│       └── reference/          # lookup pages
```

Add a page by dropping a Markdown/MDX file with `title` and `description` frontmatter into
`guides/` or `reference/`, then list it in the `sidebar` array in `astro.config.mjs`.

## Adding a new module

1. Copy the `modules/base` directory layout into `modules/<name>`.
2. Add the crate to the workspace `members` in the root `Cargo.toml`.
3. Define the schema in `database/schema/<name>.surql` and the API in `proto/` (register it in
   `rpguru_sdk`).
4. Implement from the inside out: `entities` → `services` → `rpc`/`hooks`.
5. Wire the new services and hooks into `bin/guru-master`'s workers.
6. Keep `config` values seedable from `bin/manage-tool`.
