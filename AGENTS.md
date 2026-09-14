# AGENTS.md

How to organise code in this workspace. Read this before adding anything. The
rules exist so that every module looks the same and both humans and agents can
navigate the codebase mechanically.

## The golden rule

**Everything is a `Processor`.** `kanau::processor::Processor` is *state plus an
async function*:

```text
Processor = State + async fn(Input) -> Result<Output, Error>
```

A `Processor` is a `Clone`-able struct that owns its dependencies and implements
`Processor<Input>` once per operation. Model each database query, each business
operation, and each queue consumer as an input struct plus a `Processor` impl.
Do not invent parallel abstractions.

## Workspace layout

```
bin/          # Rust binaries — wiring only, no business logic
  app-server/     # runs modules behind pluggable workers (gRPC, consumer, cron, ...)
  manage-tool/    # CLI: admin bootstrap (create-admin), config seeding, maintenance
lib/
  app_protobuf/   # generated gRPC/protobuf types + shared conversions (Rust)
modules/          # business logic, one crate per feature
  base/           # foundational + template module
proto/            # protobuf definitions (grouped by module) — the single API source
database/         # SurrealDB schema + seed + tests (managed by surrealkit)
typescript/       # Bun workspace: all frontend / TypeScript packages
  app-protobuf/   # generated gRPC/protobuf TypeScript code (shared)
package.json      # root of the Bun workspace (workspaces: ["typescript/*"])
```

Rust binaries live under `bin/` — do **not** place binary crates at the
repository root. All TypeScript/JavaScript packages live under `typescript/`
and are managed as a single Bun workspace — do **not** create standalone,
unlinked npm/pnpm projects.

## Anatomy of a module

Every module crate mirrors `modules/base`:

```
src/
├── lib.rs        # declares the modules below; sets crate-wide lints
├── config.rs     # typed configuration (one `app_config` row, JSON document)
├── utils/        # small, dependency-light helpers
├── entities/     # persistence layer
│   ├── surreal/  # SurrealDB row types + SurrealProcessor queries
│   └── redis/    # Redis key/value types (rkyv-encoded)
├── services/     # business logic (stateful Processors)
├── events/       # AMQP payloads + routing
├── hooks/        # background reactors (consumers, cron, loggers)
└── rpc/          # gRPC service implementations (transport edge)
```

### Where does my code go?

| You are writing…                                    | Put it in…      |
| --------------------------------------------------- | --------------- |
| A SurrealDB query or a table row type               | `entities/surreal`|
| A Redis-cached value or ephemeral token             | `entities/redis`|
| A use case that combines queries and rules          | `services`      |
| A message other modules react to                    | `events`        |
| A reaction to an event / a cron job / an audit log  | `hooks`         |
| A gRPC endpoint implementation                       | `rpc`           |
| A typed setting an operator can change               | `config`        |
| A pure helper with no runtime deps                   | `utils`         |

## Layer rules

### `entities/surreal`

- One submodule per table or aggregate.
- Define a row struct deriving `surrealdb_types::SurrealValue`, and wrap the
  table's record id in a newtype with `table_record!(NameId, "table")` from
  `lib/newtype_record_id` (this generates the `RecordId` newtype plus its
  `SurrealValue` impl).
- Implement `Processor<Input>` for `wakuwaku::surreal::SurrealProcessor`, one
  impl per query/command, with `Error = surrealdb::Error`. Run statements with
  `self.db().query(SQL).bind(("k", v)).await?` then `resp.take::<T>(0)?`; use
  `.check()?` on write-only commands to surface per-statement errors.
- Queries are validated at runtime, not compile time, so cover them with the
  module's integration tests against an in-memory (`mem://`) database.
- SurrealDB 3.x gotchas: use `type::record(tb, id)` (the old `type::thing` was
  removed), and never bind a variable named `token` (it is reserved).
- Annotate every impl with the named tracing span described under *Tracing*.

### `entities/redis`

- One submodule per key kind.
- Define a value type and a key type; derive the `rkyv` traits.
- Implement `KeyValue` + `KeyValueRead` + `KeyValueWrite` from
  `wakuwaku::redis`.

### `services`

- A service is a `Clone` struct owning its dependencies (database, Redis, AMQP,
  loaded config, other services).
- One `Processor` impl per operation; return domain types, not protobuf types.
- Receive the loaded config at construction time (`guru-master` reads it once
  from the store during startup); never re-read it per call and never cache it
  elsewhere — the database is the only source of truth.
- Services orchestrate entities and publish events. **No transport types here.**

### `events`

- Define the payload, derive the `rkyv` traits, and implement `AmqpRouting`
  (`EXCHANGE`, `EXCHANGE_TYPE`, `ROUTING_KEY`) + `AmqpMessageSend`.
- Document each event's contract in a doc comment: **who publishes, who
  consumes, and the routing key.**
- Events are the *only* sanctioned way for modules to communicate
  asynchronously.

### `hooks`

- AMQP consumers implement `AmqpMessageProcessor<E>` (with a durable `QUEUE`
  name) plus `Processor<E>`.
- Also the home for cron jobs and event loggers.
- Like services, hooks own their dependencies and carry no transport logic.

### `rpc`

- Implement the protobuf service trait from `rpguru_sdk`.
- Handlers are thin adapters: decode request → call a service → encode reply.
- **No business logic** — if a handler grows rules, move them into a service.
- Re-export each concrete service type from `rpc/mod.rs` so `guru-master` can
  mount it.

### `config`

- A `serde`-(de)serializable struct implementing `Default`, bound to a stable
  string key with `base::entities::surreal::app_config::ConfigJson`.
- Stored as JSON in one `app_config` row (no cache, no second copy), seeded by
  `manage-tool config seed` and loaded once at startup with
  `base::services::config::LoadConfig`; services hold the value.
- Carry `#[serde(default)]` on the struct so a row written before a field was
  added still loads. Register the key as a `ConfigKey` variant in
  `manage-tool`, whose match arms then force every operation to handle it.
  A key an Admin should also be able to read and replace from the dashboard
  needs a typed `Get<Module>Config` / `Set<Module>Config` pair on that module's
  own gRPC service, answering with `guru.base.ConfigDocument` — the module that
  names the config type is the one that validates a payload for it, so nothing
  has to be type-erased. The `ConfigKey` enum stays the CLI's registry.

## Cross-cutting conventions

- **Errors:** use `wakuwaku::Error` at the service/hook boundary. Inside
  `entities/surreal` return `surrealdb::Error`; it converts into
  `wakuwaku::Error` via `?` at the service layer (needs `wakuwaku` ≥ 0.2.3).
  Define module-specific error enums with `thiserror` when a layer needs richer
  variants.
- **Lints:** keep the crate-level `#![deny(clippy::unwrap_used)]`,
  `expect_used`, and `panic` lints. No panics on the request path.
- **Tracing:** instrument entities and services with
  `#[tracing::instrument(skip_all, err)]`, and **always give the span an
  explicit `name`** — a bare attribute on `Processor::process` produces an
  indistinguishable `process` span for every impl. Naming convention:
  - `entities/surreal` queries → `name = "Query:<Input>"` (e.g.
    `"Query:FindAccountByEmail"`); if the impl drives a SurrealDB transaction,
    use `name = "Query-Transaction:<Input>"` instead.
  - `services` operations → `name = "Service:<Input>"` (e.g.
    `"Service:RegisterAccount"`).
  - gRPC handlers need no `name`: the trait-method name already labels the span.
- **Dependency direction:** `rpc → services → entities/events/config`. A feature
  module may depend on `base` (and on shared modules), but `base` must not depend
  on a feature module, and modules must not depend on each other's internals —
  communicate via gRPC or AMQP events.
- **Protobuf:** `proto/` is the single source of truth for the API. Add `.proto`
  files there, register them in `rpguru_sdk`'s `build.rs` (Rust side), and
  regenerate the TypeScript side with `bun run generate:proto`. Never hand-edit
  or duplicate generated code.
- **Schema:** SurrealDB schema lives in `database/schema/*.surql`, managed with
  **surrealkit** (`surrealkit sync` in development; `surrealkit rollout` for
  shared/production databases). Keep each module's tables in its own
  `<module>.surql` file; the module's integration tests apply that same file.

## Adding a new module (checklist)

1. Copy the `modules/base` directory layout into `modules/<name>`.
2. Add the crate to the workspace `members` in the root `Cargo.toml`.
3. Define the schema in `database/schema/<name>.surql` (surrealkit) and the API
   in `proto/` (register it in `rpguru_sdk`).
4. Implement, from the inside out: `entities` → `services` → `rpc`/`hooks`.
5. Wire the new services/hooks into `bin/guru-master`'s workers.
6. Keep `config` values seedable from `bin/manage-tool`.

## Frontend / TypeScript

All TypeScript lives under `typescript/` as one **Bun workspace** (the root
`package.json` declares `workspaces: ["typescript/*"]`). Use **Bun** for
everything — install, scripts, running — not npm or pnpm.

### `app-protobuf` — generated API code, shared once

`typescript/app-protobuf` is the TypeScript counterpart of the `rpguru_sdk`
Rust crate: it holds the gRPC/protobuf code generated from `proto/`, and
**nothing else**. Every frontend package depends on `app-protobuf` instead of
generating (and duplicating) its own client — this is the whole point of the
workspace.

- Codegen is driven by `typescript/app-protobuf/generate-proto.sh`, exposed as
  the `generate:proto` script. Run it from the repo root:

  ```sh
  bun install            # once, to fetch the toolchain (grpc-tools, ts-proto)
  bun run generate:proto # regenerate after any change to proto/
  ```

- Output lands in `typescript/app-protobuf/src/generated/` (emptied and
  rewritten on every run — never edit it by hand or commit changes into it
  manually). The template ships this directory empty.
- The package exposes generated modules by subpath, mirroring the proto tree:

  ```ts
  import { GreeterDefinition } from "app-protobuf/sample/hello";
  ```

### `docs` — the project manual

`typescript/docs` is an Astro Starlight site themed with the
[`lucode-starlight`](https://lucas-labs.github.io/lucode-starlight-theme/)
plugin. Pages are Markdown/MDX under `src/content/docs/`, split into `guides/`
(task-oriented) and `reference/` (lookup); the sidebar is declared in
`astro.config.mjs`. Run it with `bun run docs:dev` and build it with
`bun run docs:build`.

`site` and `base` are deliberately unset: there is no docs deployment yet, so the
site is portable and served from the root. When a target is chosen, set `site`
(this also enables the sitemap Starlight currently skips) and, if it is hosted
under a subpath, `base` — then prefix the root-relative links in
`src/content/docs/` with it, since Starlight does not rewrite Markdown links.

Document behaviour here, not in new top-level Markdown files — `README.md` stays
a short overview and this file stays the code-organisation contract.

### Adding a frontend package

1. Create it under `typescript/<name>/` with its own `package.json`; the Bun
   workspace picks it up automatically.
2. Add `"app-protobuf": "workspace:*"` to its dependencies and import the
   generated types from there — do **not** re-run protoc inside the package.
3. Keep generated code, gRPC clients, and other shared TypeScript in dedicated
   workspace packages so each concern has exactly one home, just like the Rust
   side.
