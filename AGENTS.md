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
migrations/       # PostgreSQL schema: sqlx migrations
vendor/           # patched third-party crates, wired in by [patch.crates-io] in the
                  # root Cargo.toml; each carries a PATCH.md with the diff and why
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
│   ├── db/       # PostgreSQL row types + sqlx queries
│   └── redis/    # Redis key/value types (rkyv-encoded)
├── services/     # business logic (stateful Processors)
├── events/       # AMQP payloads + routing
├── hooks/        # background reactors (consumers, cron, loggers)
└── rpc/          # gRPC service implementations (transport edge)
```

### Where does my code go?

| You are writing…                                    | Put it in…      |
| --------------------------------------------------- | --------------- |
| A PostgreSQL query or a table row type              | `entities/db`   |
| A Redis-cached value or ephemeral token             | `entities/redis`|
| A use case that combines queries and rules          | `services`      |
| A message other modules react to                    | `events`        |
| A reaction to an event / a cron job / an audit log  | `hooks`         |
| A gRPC endpoint implementation                       | `rpc`           |
| A typed setting an operator can change               | `config`        |
| A pure helper with no runtime deps                   | `utils`         |

## Layer rules

### `entities/db`

- One submodule per table or aggregate.
- Define a row struct, and wrap the table's id in a newtype with
  `table_record!(NameId, "table")` from `lib/db_types`: a `String` newtype
  transparent to sqlx and serde, so foreign-key columns and ids inside `jsonb`
  documents are typed too (`canvas: CanvasId`, never `String`). An enum stored
  as text gets its one spelling from `text_enum!`; a document whose shape varies
  per row is `jsonb` (serde on the type).
- Implement `Processor<Input>` for `base::db::Db`, one impl per query/command,
  with `Error = base::db::Error`. A multi-statement write is one
  `self.db().begin()` transaction with the statements in Rust order; a fence
  that loses returns `Error::Conflict(<token>)` and rolls the transaction back,
  while a refused conditional write (`UPDATE … WHERE <fence> RETURNING …` that
  matches nothing) is an empty result, never an error. Transactions that write
  the same rows lock them in one order, or two of them deadlock: in
  `orchestration`, a write to a canvas tree takes the tree's root row first
  (`entities::db::fence`).
- **Every statement goes through the `query!` family**, so the SQL is checked
  against the real schema while the crate compiles:
  `sqlx::query_as!(Row, SQL, arg, …)`, `sqlx::query_scalar!`, `sqlx::query!`.
  Consequences to live with:
  - No `SELECT *`: the macro requires the columns and the struct's fields to
    match exactly, so columns are spelled out.
  - A column whose Rust type is not the built-in mapping is aliased with its
    type — `id AS "id: CanvasId"`, `status AS "status: ServerHealthStatus"`,
    `spec AS "spec: Json<NodeSpec>"` — which needs a raw string literal for the
    quotes. Nullability comes from the schema; `AS "x!: T"` / `AS "x?: T"`
    override it where the query itself guarantees otherwise.
  - A parameter of such a type is passed with a wildcard cast, `input.id as _`,
    which is what tells the macro the value encodes itself.
  - `query_as!` does not use `FromRow`, so a `jsonb` column read into a domain
    type goes through a private row struct with `Json<T>` fields plus one
    conversion into the entity (`entities::db::group`, `view`, `pod`); entity
    structs themselves stay free of sqlx wrappers.
  - SQL longer than five lines lives in `<crate>/sql/<operation>.sql` and is
    called with `query_file!` / `query_file_as!` / `query_file_scalar!` (the
    path is relative to the crate root). The file is named after the operation
    it performs: the input struct in snake case where there is one
    (`commit_canvas_derivation.sql`), the free function for a helper
    (`ancestors_of.sql`), and one file per branch where a `Processor` runs
    several (`ack_server_config_error.sql`, `ack_server_config_applied.sql`).
    Nothing formats SQL at runtime.
  - The offline query data is committed in `.sqlx/`; regenerate it with
    `cargo sqlx prepare --workspace -- --all-targets` (needs `DATABASE_URL`
    pointing at a migrated database) whenever a statement or the schema
    changes, and commit the result — the Docker build compiles with
    `SQLX_OFFLINE=true`. `-- --all-targets` is what includes the tests' own
    statements; without it an offline `cargo test` fails on them. Each crate
    that invokes the macros carries a `sqlx.toml` pinning `chrono` as the
    date/time crate, because `wakuwaku` also enables sqlx's `time` feature.
- Cover queries with the module's integration tests all the same — the macros
  check shapes and types, not behaviour:
  `#[sqlx::test(migrator = "base::db::MIGRATOR")]` creates and migrates one
  database per test next to the one `DATABASE_URL` names (the `guru_test`
  database, never the production one). Run the suites with
  `SQLX_OFFLINE=true`, or the macros try to check every statement against that
  bare database and fail.
- PostgreSQL gotchas: compare a nullable column with `IS DISTINCT FROM`, not
  `<>`; a JSON `null` inside `jsonb` is not SQL `NULL` (`jsonb_typeof`).
- Annotate every impl with the named tracing span described under *Tracing*.

### `entities/redis`

- One submodule per key kind.
- Define a value type and a key type; derive the `rkyv` traits.
- Implement `KeyValue` + `KeyValueRead` + `KeyValueWrite` from
  `wakuwaku::redis`.

### `services`

- The database dependency is `base::db::Db`, the pool handle every entity query
  is implemented on. Statements are bounded by the server (`statement_timeout`,
  set by `base::db::connect`), so nothing here needs a timer of its own.
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
  string key with `base::entities::db::app_config::ConfigJson`.
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
  `entities/db` return `base::db::Error`; it converts into `wakuwaku::Error`
  via `?` at the service layer. `base::db::is_unavailable` tells "the database
  did not answer" (`UNAVAILABLE`) from a refusal, and `Error::Conflict` is a
  transaction that definitely did not commit.
  Define module-specific error enums with `thiserror` when a layer needs richer
  variants.
- **Lints:** keep the crate-level `#![deny(clippy::unwrap_used)]`,
  `expect_used`, and `panic` lints. No panics on the request path.
- **Tracing:** instrument entities and services with
  `#[tracing::instrument(skip_all, err)]`, and **always give the span an
  explicit `name`** — a bare attribute on `Processor::process` produces an
  indistinguishable `process` span for every impl. Naming convention:
  - `entities/db` queries → `name = "Query:<Input>"` (e.g.
    `"Query:FindAccountByEmail"`); if the impl drives a transaction, use
    `name = "Query-Transaction:<Input>"` instead.
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
- **Schema:** the PostgreSQL schema lives in `migrations/*.sql`,
  embedded by `sqlx::migrate!` as `base::db::MIGRATOR` and applied by
  `guru-master` at startup (advisory-locked, so replicas may start together) and
  by `manage-tool db migrate`. Every change is a new migration file; an applied
  one is never edited. The integration tests run the same migrator.

## Adding a new module (checklist)

1. Copy the `modules/base` directory layout into `modules/<name>`.
2. Add the crate to the workspace `members` in the root `Cargo.toml`.
3. Add the tables in a new `migrations/<n>_<name>.sql` and the API
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
[`starlight-theme-black`](https://starlight-theme-black.vercel.app/)
plugin. Pages are Markdown/MDX under `src/content/docs/`, split into `guides/`
(task-oriented) and `reference/` (lookup); the sidebar is declared in
`astro.config.mjs`. Run it with `bun run docs:dev` and build it with
`bun run docs:build`.

`site` and `base` are deliberately unset: there is no docs deployment yet, so the
site is portable and served from the root. When a target is chosen, set `site`
(this also enables the sitemap Starlight currently skips) and, if it is hosted
under a subpath, `base` — then prefix the root-relative links in
`src/content/docs/` with it, since Starlight does not rewrite Markdown links.

Translations use Starlight's own i18n. English is the default locale and is
served from the root (`defaultLocale: 'root'`), so English pages stay at
`src/content/docs/<path>`; every other locale mirrors that tree under its own
directory — `src/content/docs/ja/<path>` and `src/content/docs/zh-cn/<path>` —
with the same file names. Adding a locale means: register it in `locales` in
`astro.config.mjs`, add its label to every sidebar `translations` map (keyed by
the locale's `lang`, e.g. `zh-CN`), add it to the `navLinkTranslations` helper
used by the theme's `navLinks` (those items must be declared by `slug`, since
`starlight-theme-black` ignores `translations` when an item carries a literal
`label`), and translate the pages. Starlight ships the UI strings for `ja` and
`zh-CN`, so `src/content/i18n/` only needs a file when a UI string is
overridden — and those files are named after the locale's `lang`
(`zh-CN.json`), not after the content directory (`zh-cn/`).
Inside a translated page, prefix every root-relative link with the locale
segment (`/ja/guides/...`) and point in-page anchors at the *translated*
heading's slug — Starlight's slugger keeps CJK, so `## モジュール設定` is
reachable at `#モジュール設定`.

Document behaviour here, not in new top-level Markdown files — `README.md` stays
a short overview and this file stays the code-organisation contract.

### i18n in `guru-frontend`

Paraglide (inlang) generates `src/lib/paraglide/` at build time from
`messages/<locale>.json`; the locale comes from the `guru_locale` cookie, then
`Accept-Language`, then the base locale (no URL prefixes).

- Every user-visible string goes through `m.<key>()` from
  `#lib/paraglide/messages.js`. Never hard-code UI text in `.svelte` files.
- Remote functions return stable codes, never sentences; the client maps them
  to messages in `src/lib/i18n/codes.ts`.
- Adding a locale touches four places: `project.inlang/settings.json`
  (`locales`), a full `messages/<locale>.json`, its label in
  `src/lib/i18n/locales.ts` (the switchers iterate `locales` from the runtime),
  and optionally a colour/animal dictionary in `src/lib/i18n/naming.ts`.
- `bun run check:messages` (first step of `bun run check`) fails when any
  locale is missing a key or has different `{placeholders}` than `en`.

### Adding a frontend package

1. Create it under `typescript/<name>/` with its own `package.json`; the Bun
   workspace picks it up automatically.
2. Add `"app-protobuf": "workspace:*"` to its dependencies and import the
   generated types from there — do **not** re-run protoc inside the package.
3. Keep generated code, gRPC clients, and other shared TypeScript in dedicated
   workspace packages so each concern has exactly one home, just like the Rust
   side.
