---
title: Introduction
description: What Proxy Guru is, which pieces exist, and how traffic flows through the fabric.
---

Proxy Guru is a managed TCP/TLS proxy fabric. Operators design a topology on a **canvas**, the
control plane derives one config per server from that topology, and data-plane workers pick up
every new revision automatically.

## The two planes

**Control plane** — `bin/guru-master`. One binary with four modes selected by `--mode`
(`GURU_WORKER_MODE`):

| Mode | Responsibility |
|---|---|
| `dashboard_grpc` | Operator API consumed by the frontend |
| `workers_grpc` | Worker API plus the config-view poller |
| `consumer` | Every AMQP hook: the derivation hook, and all five periodic jobs |
| `cron` | The clock: publishes one execution signal per due periodic job |

The last two are one job split in half on purpose. `cron` reads no configuration and opens no
database connection; the `consumer` that receives a signal claims the run and does the work, so a
sweep, a liveness check or a certificate renewal scales and fails over exactly like a canvas edit.
All four modes need the broker.

**Data plane** — `bin/guru-worker`. Terminates listeners and forwards traffic. It runs either
standalone from a TOML file (reloaded on `SIGHUP`) or in agent mode, streaming configs from the
master.

The worker config model itself lives in `lib/guru_worker_config` and is shared by both planes: the
master derives it, the worker consumes it. A worker driven from a file needs no control plane at
all — see [Independent Worker Deployment](/guides/independent-worker/).

## Topology vocabulary

Each forwarding has a **listener** and a **destination**.

- Listener: `raw`, `tls`, or an inbound relay.
- Destination: a direct **exit**, a **relay** to another node over TLS-over-TCP or QUIC, or a
  **load-balance** group.

PROXY protocol v1 and v2 are supported on both ends.

## Modules

Business logic lives in `modules/`, one crate per feature:

| Module | Scope |
|---|---|
| `auth` | Accounts, sessions, API keys, RBAC |
| `orchestration` | Canvases, servers, nodes, edges; topology validation, config derivation, worker rollout |
| `notify` | Notification module — scaffolded from `base`, not implemented yet |
| `base` | Shared foundations and the layout every module mirrors |

## Stack

Rust 2024 on Tokio, [`wakuwaku`](https://crates.io/crates/wakuwaku) +
[`kanau`](https://crates.io/crates/kanau) (everything is a `Processor`), gRPC via Tonic, SurrealDB
for storage (schema in `database/`, managed with surrealkit), Redis for caching, AMQP for
inter-module events, OpenTelemetry for tracing, and a Bun workspace under `typescript/` sharing one
generated API client.

## Next

- [Local Development](/guides/local-development/) — bring up the whole stack on one
  machine.
- [Architecture](/reference/architecture/) — crate roles and layer rules.
- [Rollout Model](/reference/rollout/) — how a canvas edit reaches a worker.
- [Independent Worker Deployment](/guides/independent-worker/) — run a worker from a
  TOML file, without a master.
