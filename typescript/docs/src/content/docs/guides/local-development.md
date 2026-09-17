---
title: Local Development
description: Bring up PostgreSQL, RabbitMQ, Redis, the control plane and the dashboard on one machine.
---

The control plane needs a PostgreSQL database, an AMQP broker and a Redis server. Everything else
runs from the workspace.

:::caution[The repository `.env` is not a dev profile]
The root `.env` can hold **production** credentials (`GURU_DATABASE_URL`, `AMQP_URI`,
`REDIS_URL`), and every process you start inherits it. Pass `--database-url` explicitly — or
override the variable — so a local run cannot talk to a remote database by accident.
:::

## 1. Dependencies

```sh
docker run -d --name guru-postgres -p 15432:5432 \
  -e POSTGRES_USER=guru -e POSTGRES_PASSWORD=guru -e POSTGRES_DB=guru \
  postgres:18.6-alpine

docker run -d --name guru-rabbit -p 5672:5672 -p 15672:15672 rabbitmq:4-alpine

# Pub/sub only, so nothing is persisted; `docker compose up redis` from the
# repository root starts the same thing.
docker run -d --name guru-redis -p 6379:6379 redis:7-alpine \
  redis-server --save '' --appendonly no
```

Use PostgreSQL **16 or newer**.

The broker URI form matters: use `amqp://guest:guest@127.0.0.1:5672/` for the default vhost. Redis
takes `redis://127.0.0.1:6379/` and no credentials. If something else already listens on `6379`, set
`REDIS_PORT` in `.env` before `docker compose up redis` to move the host side (say to `16379`) and
name that port in `REDIS_URL`.

## 2. Schema

The schema is a set of sqlx migrations in `migrations/`, embedded into the binaries.
`guru-master` applies what is pending when it starts; to do it by hand:

```sh
export GURU_DATABASE_URL=postgres://guru:guru@127.0.0.1:15432/guru
cargo run -p manage-tool -- db migrate
```

Then write the default module configuration into the `app_config` table. It is idempotent and never
overwrites a value you have edited, so re-run it after every migration:

```sh
cargo run -p manage-tool -- config seed
```

`config list`, `config get <key>` and `config set <key> <json>` inspect and change those values; the
masters pick them up on restart. See
[Configuration → Module configuration](/reference/configuration#module-configuration).

## 3. Bootstrap an administrator

```sh
cargo run -p manage-tool -- --database-url "$GURU_DATABASE_URL" \
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
  --database-url "$GURU_DATABASE_URL" \
  --amqp-uri 'amqp://guest:guest@127.0.0.1:5672/' \
  --redis-url 'redis://127.0.0.1:6379/'
```

Nothing derives a canvas until a `consumer` runs, so start one in a second shell — it is both the
edit hook and the executor of every periodic job (the stale-canvas sweep, the liveness sweep,
health retention, ACME and relay-leaf rotation):

```sh
cargo run -p guru-master -- \
  --mode consumer \
  --database-url "$GURU_DATABASE_URL" \
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
is the live bus behind the operator API's `Watch*` streams, which the dashboard's canvas editor and
health page follow: with Redis down, an open page stops updating (its *Live* badge stays green — that
badge is the browser's own connection) while edits and derivation carry on as usual, and it catches up
by itself once Redis is back.

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

Module integration tests run against a real PostgreSQL server: `#[sqlx::test]` creates one throwaway
database per test from `DATABASE_URL` and applies the migrations to it. Point that at a **test**
database, never the one a master runs against — the test runner creates and drops databases next to
it:

```sh
export DATABASE_URL=postgres://guru:guru@127.0.0.1:15432/guru_test
cargo test
```

Create it once with `createdb` (or `CREATE DATABASE guru_test;`); the role needs `CREATEDB`.
