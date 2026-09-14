---
title: Architecture
description: Crate roles, the Processor abstraction, and the layer rules every module follows.
---

## Crates

| Crate | Role |
|---|---|
| `bin/guru-master` | Control plane. One binary, four modes (`--mode`): `dashboard_grpc` (operator API), `workers_grpc` (worker API + config-view poller), `consumer` (AMQP derivation hook), `cron` (stale-canvas sweep). |
| `bin/guru-worker` | Data plane. Terminates listeners and forwards traffic. Runs standalone from a TOML file (reloaded on `SIGHUP`) or in agent mode, streaming configs from the master. |
| `bin/manage-tool` | Admin CLI: `create-admin` bootstrap, `orchestration export-config`. |
| `lib/guru_worker_config` | The worker config model, shared by both planes. |
| `lib/rpguru_sdk` | Generated gRPC/protobuf types (Rust) from `proto/`. |
| `lib/newtype_record_id` | `table_record!` macro for typed SurrealDB record ids. |
| `modules/auth` | Accounts, sessions, API keys, RBAC. |
| `modules/orchestration` | Canvases, servers, nodes, edges; topology validation, config derivation, worker rollout. |
| `modules/notify` | Notification module — scaffolded from `base`, not implemented yet. |
| `modules/base` | Shared foundations and the layout every module mirrors. |

## Everything is a Processor

`kanau::processor::Processor` is **state plus an async function**:

```text
Processor = State + async fn(Input) -> Result<Output, Error>
```

A `Processor` is a `Clone`-able struct that owns its dependencies and implements `Processor<Input>`
once per operation. Every database query, business operation and queue consumer is modelled as an
input struct plus one `Processor` impl — there is no parallel abstraction to learn.

## Module anatomy

Every module crate mirrors `modules/base`:

```text
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

| You are writing… | Put it in… |
|---|---|
| A SurrealDB query or a table row type | `entities/surreal` |
| A Redis-cached value or ephemeral token | `entities/redis` |
| A use case that combines queries and rules | `services` |
| A message other modules react to | `events` |
| A reaction to an event / a cron job / an audit log | `hooks` |
| A gRPC endpoint implementation | `rpc` |
| A typed setting an operator can change | `config` |
| A pure helper with no runtime deps | `utils` |

## Layer rules

- **Dependency direction:** `rpc → services → entities/events/config`. A feature module may depend
  on `base`, but `base` must not depend on a feature module, and modules must not reach into each
  other's internals — they communicate via gRPC or AMQP events.
- **`entities/surreal`:** one submodule per table or aggregate; a row struct deriving
  `SurrealValue`; the record id wrapped by `table_record!(NameId, "table")`. One `Processor` impl
  per query on `wakuwaku::surreal::SurrealProcessor`, with `Error = surrealdb::Error`. Queries are
  validated at runtime, so they are covered by integration tests against `mem://`.
- **`services`:** `Clone` structs owning their dependencies, one `Processor` impl per operation,
  returning domain types — never protobuf types.
- **`events`:** the payload plus `AmqpRouting` (`EXCHANGE`, `EXCHANGE_TYPE`, `ROUTING_KEY`) and
  `AmqpMessageSend`. The only sanctioned asynchronous channel between modules.
- **`hooks`:** AMQP consumers implement `AmqpMessageProcessor<E>` with a durable `QUEUE` name; also
  the home of cron jobs and event loggers.
- **`rpc`:** thin adapters — decode request, call a service, encode reply. No business logic.
- **Errors:** `wakuwaku::Error` at the service/hook boundary; `surrealdb::Error` inside
  `entities/surreal`, which converts with `?` at the service layer.
- **Lints:** crate-level `deny(clippy::unwrap_used)`, `expect_used` and `panic`. No panics on the
  request path.
- **Tracing:** `#[tracing::instrument(skip_all, err)]` with an explicit span `name` —
  `Query:<Input>` (or `Query-Transaction:<Input>`) for entities, `Service:<Input>` for services.
  gRPC handlers need none; the trait-method name already labels the span.

## SurrealDB notes

- Use `type::record(tb, id)`; the old `type::thing` was removed in 3.x.
- Never bind a variable named `token` — it is reserved.
- Write any row that a later field assertion reads *after* the `CREATE` that triggers it; some
  server versions do not see an in-transaction `UPDATE` from `record::exists()`.
