---
title: Local Development
description: Bring up SurrealDB, RabbitMQ, Redis, the control plane and the dashboard on one machine.
---

The control plane needs a SurrealDB instance, an AMQP broker and a Redis server. Everything else
runs from the workspace.

:::caution[The repository `.env` is not a dev profile]
The root `.env` can hold **production** credentials (`SURREALDB_HOST`, `SURREALDB_USER`,
`SURREALDB_PASSWORD`, `SURREALDB_NAMESPACE`, `SURREALDB_NAME`, `AMQP_URI`, `REDIS_URL`), and every
process you start inherits it. Pass the database flags explicitly — or override the variables — so
a local run cannot talk to a remote database by accident.
:::

## 1. Dependencies

```sh
docker run -d --name guru-surreal -p 8000:8000 \
  surrealdb/surrealdb:latest start --user root --pass root

docker run -d --name guru-rabbit -p 5672:5672 -p 15672:15672 rabbitmq:4-alpine

# Pub/sub only, so nothing is persisted; `docker compose up redis` from the
# repository root starts the same thing.
docker run -d --name guru-redis -p 6379:6379 redis:7-alpine \
  redis-server --save '' --appendonly no
```

Use a SurrealDB **3.2 or newer** server. Older 3.0 binaries disagree with the client the workspace
links against and mis-handle assertions that read a row written earlier in the same transaction.

The broker URI form matters: use `amqp://guest:guest@127.0.0.1:5672/` for the default vhost. Redis
takes `redis://127.0.0.1:6379/` and no credentials. If something else already listens on `6379`, set
`REDIS_PORT` in `.env` before `docker compose up redis` to move the host side (say to `16379`) and
name that port in `REDIS_URL`.

## 2. Schema

Schema lives in `database/schema/*.surql` (one file per module) and is managed with
[surrealkit](https://surrealdb.com/):

```sh
surrealkit sync --host ws://127.0.0.1:8000 --ns guru --db guru
```

Then write the default module configuration into the `app_config` table. It is idempotent and never
overwrites a value you have edited, so re-run it after every sync:

```sh
cargo run -p manage-tool -- \
  --address ws://127.0.0.1:8000 --username root --password root \
  --namespace guru --database guru \
  config seed
```

`config list`, `config get <key>` and `config set <key> <json>` inspect and change those values; the
masters pick them up on restart. See
[Configuration → Module configuration](/reference/configuration#module-configuration).

## 3. Bootstrap an administrator

```sh
cargo run -p manage-tool -- \
  --address ws://127.0.0.1:8000 --username root --password root \
  --namespace guru --database guru \
  create-admin --email admin@example.com --password 'change-me'
```

## 4. Control plane

Every mode that touches the database decrypts secrets with `GURU_MASTER_KEY`, and it has no
default: without it the process aborts at startup with
`Error: "master key: GURU_MASTER_KEY is not set"`. Generate one once — the subcommand needs no
database — and keep it in the shell you start the masters from:

```sh
cargo run -p manage-tool -- generate-master-key
# j7ILadgGjBy+jYMIJuiPBl5eai65t7G8G4XimNcyLpU=

export GURU_MASTER_KEY='<the printed value>'
```

Each mode is a separate process. The operator API the dashboard talks to is `dashboard_grpc`:

```sh
cargo run -p guru-master -- \
  --mode dashboard_grpc \
  --address ws://127.0.0.1:8000 --username root --password root \
  --namespace guru --database guru \
  --amqp-uri 'amqp://guest:guest@127.0.0.1:5672/' \
  --redis-url 'redis://127.0.0.1:6379/'
```

Nothing derives a canvas until a `consumer` runs, so start one in a second shell — it is both the
edit hook and the executor of every periodic job (the stale-canvas sweep, the liveness sweep,
health retention, ACME and relay-leaf rotation):

```sh
cargo run -p guru-master -- \
  --mode consumer \
  --address ws://127.0.0.1:8000 --username root --password root \
  --namespace guru --database guru \
  --amqp-uri 'amqp://guest:guest@127.0.0.1:5672/' \
  --redis-url 'redis://127.0.0.1:6379/'
```

In a third shell, the clock. `cron` publishes one execution signal per due job and nothing else:
it opens no database connection, never reads `GURU_MASTER_KEY`, and takes no database arguments,
so the broker URI is the whole configuration:

```sh
env -u GURU_MASTER_KEY cargo run -p guru-master -- \
  --mode cron \
  --amqp-uri 'amqp://guest:guest@127.0.0.1:5672/'
```

`workers_grpc` is the fourth mode — the worker API plus the config-view poller — and takes exactly
the same arguments as `dashboard_grpc`. Every mode needs the broker: with RabbitMQ down, nothing
starts, and with `cron` or the `consumer` down, no periodic job happens. `--redis-url` (or
`REDIS_URL`) is required by the three modes above that open a database connection, and unused by
`cron`; without it they abort with
`Redis is required: set REDIS_URL (or pass --redis-url), for example redis://127.0.0.1:6379/`. It
is the live bus behind the operator API's `Watch*` streams: with Redis down, an open stream stops
receiving, while edits and derivation carry on as usual. The dashboard does not consume those
streams yet, so a browser notices nothing either way.

To make a periodic job run without waiting for its interval, delete its claim row — the table is
`orchestration_job_run`, one row per job keyed by the job name, so
`DELETE orchestration_job_run:sweep_liveness` makes the next `sweep_liveness` signal the one that
runs.

## 5. Dashboard

```sh
bun install
GURU_GRPC_URL=127.0.0.1:50051 bun run dev
```

`bun run dev` at the workspace root proxies to the `guru-frontend` package.

## 6. Documentation site

These docs are their own workspace package:

```sh
bun run --filter guru-docs dev     # or: bun run docs:dev
bun run --filter guru-docs build   # static output in typescript/docs/dist
```

## Regenerating the API client

`proto/` is the single source of truth. After changing it, regenerate the TypeScript client (the
Rust side is generated by `rpguru_sdk`'s `build.rs`):

```sh
bun run generate:proto
```

## Tests

Module integration tests run against an in-memory SurrealDB (`mem://`) and apply the module's own
schema file, so they need no running server:

```sh
cargo test
```
