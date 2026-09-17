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
| `consumer` | Every AMQP hook: the derivation hook, and all six periodic jobs |
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

A canvas tree holds one **pod graph**:

- A **server** is a machine running `guru-worker`, and the set of pods that run on it.
- A **pod** is one listener on one server. A *client* pod takes connections from clients directly
  (raw TCP, or TLS terminated with an ACME certificate, optionally receiving PROXY); a *relay* pod
  takes traffic other pods relay to it over TCP, TLS or QUIC.
- An **exit** is a `host:port` outside the fabric where traffic leaves.
- An **edge** is one way a pod's traffic goes on — to a relay pod, dialed in the protocol that pod
  listens with, or to an exit.
- A pod's **route** balances by weight or fails over in tiers between its own edges, nested freely.

Say ten rules enter on one box and should spread over four transit servers: that is ten client pods,
each with four edges to a relay pod of its own on every transit server, whose edges lead to the
exits. The canvas draws the four balances that choose alike as one splitter and the forty edges as
a handful of buses; see [Canvas](/reference/canvas/) for how it is drawn and edited.

A pod binds every address of the host by default; it can be restricted to IPv4 or pinned to one
interface, and a pod saved without a port gets a free one between 40000 and 59999.

Nobody types a server's IP. The worker reports its public IPv4/IPv6 and interface addresses when
it registers (and every minute after, if they change), the master remembers where the registration
came from, and other servers dial whatever that yields — IPv4 first. Pin an address on the server
only when the learned one is wrong for your network (NAT, an overlay), or on a single pod when
that pod should be reached differently.

## Modules

Business logic lives in `modules/`, one crate per feature:

| Module | Scope |
|---|---|
| `auth` | Accounts, sessions, API keys, RBAC |
| `orchestration` | Canvases, servers and the pod graph; graph checks, config derivation, worker rollout |
| `notify` | Notification module — scaffolded from `base`, not implemented yet |
| `base` | Shared foundations and the layout every module mirrors |

## Stack

Rust 2024 on Tokio, [`wakuwaku`](https://crates.io/crates/wakuwaku) +
[`kanau`](https://crates.io/crates/kanau) (everything is a `Processor`), gRPC via Tonic, PostgreSQL
for storage (sqlx; migrations in `migrations/`), Redis pub/sub for the operator API's
live `Watch*` streams, AMQP for inter-module events, OpenTelemetry for tracing, and a Bun workspace
under `typescript/` sharing one generated API client.

## Next

- [Local Development](/guides/local-development/) — bring up the whole stack on one
  machine.
- [Canvas](/reference/canvas/) — the pod graph, how the canvas draws it, and every edit gesture.
- [Architecture](/reference/architecture/) — crate roles and layer rules.
- [Rollout Model](/reference/rollout/) — how an edit reaches a worker.
- [Independent Worker Deployment](/guides/independent-worker/) — run a worker from a
  TOML file, without a master.
