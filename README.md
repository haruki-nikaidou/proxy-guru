<p align="center">
  <img src="typescript/docs/public/favicon.svg" alt="Proxy Guru" width="120" height="120">
</p>

# Proxy Guru

A managed TCP/TLS proxy fabric: operators design a topology on a **canvas**, the
control plane derives one config per server from it, and data-plane workers pick
up every new revision automatically.

## Pieces

| Crate | Role |
|---|---|
| `bin/guru-master` | Control plane. One binary, four modes (`--mode`): `dashboard_grpc` (operator API), `workers_grpc` (worker API + config-view poller), `consumer` (AMQP hooks — derivation and every periodic job), `cron` (clock: publishes one execution signal per due job). |
| `bin/guru-worker` | Data plane. Terminates listeners and forwards traffic. Runs standalone from a TOML file (reloaded on SIGHUP) or in agent mode, streaming configs from the master; in agent mode it is installed and updated from the dashboard (`bin/guru-worker/deploy/` holds the installer, unit and start guard). |
| `bin/manage-tool` | Admin CLI: `create-admin` bootstrap, `orchestration export-config`, `agent publish` (serve a worker build for the dashboard's install command and updates). |
| `lib/guru_topology` | The forwarding topology as a graph of pods: checks a graph and compiles it into each server's forwardings. Pure, no I/O. |
| `lib/guru_worker_config` | The worker config model, shared by both planes: the master derives it, the worker consumes it. |
| `lib/rpguru_sdk` | Generated gRPC/protobuf types (Rust) from `proto/`. |
| `modules/auth` | Accounts, sessions, API keys, RBAC. |
| `modules/orchestration` | Canvases, servers and the pod graph; graph checks, config derivation and worker rollout. |
| `typescript/guru-graph` | The dashboard's view of the pod graph: what a canvas draws, and every edit gesture as one change batch. Pure TypeScript. |
| `modules/notify` | Notification module — scaffolded from `base`, not implemented yet. |
| `modules/base` | Shared foundations and the layout every module mirrors. |

## Topology

A canvas tree holds a directed acyclic graph of **pods**. A pod is one listener
on one server; clients connect to it directly (raw TCP, or TLS terminated with
an ACME certificate), or other pods relay to it over TCP, TLS or QUIC. Every
**edge** is one way a pod's traffic goes on — to another pod, dialed in the
protocol that pod listens with, or to an **exit** outside the fabric — and each
pod's **route** balances by weight or fails over in tiers between its own edges,
nested freely. PROXY protocol v1/v2 is supported on both ends.

## Data plane

Each forwarding is one pod: its listener and its route, compiled into groups and
upstreams. Workers choose between members by what is alive, and a relay asked to
confirm answers only once its own next hop connected, so a dead exit behind a
live relay moves the choice on at the dialer.

## Rollout model

An edit — one checked batch of graph changes — bumps the root canvas's generation
and publishes `CanvasDirty`; the derivation hook re-derives the whole canvas tree. A lost message is caught by the periodic
`derive_stale_canvases` signal, which the same hook consumes. Every server has
one config view holding three snapshots — `desired`, `in_flight`, `applied`. A
worker stream promotes `desired` → `in_flight`, and its `AckConfig` promotes
`in_flight` → `applied`. Derivation is convergent: a server only switches
destination once the target actually serves it, so no revision drops traffic
mid-rollout.

Periodic work is scheduled and executed by different processes. `cron` is a
clock: it opens no database connection and only publishes `derive_stale_canvases`,
`rotate_relay_certificates`, `sweep_liveness`, `trim_health_history` and
`renew_certificates` when they come due. `consumer` runs them, claiming each run
at most once fleet-wide, so periodic work scales and fails over like an edit.

## Releases

Only tag pushes publish. `<version>` is the tag minus its prefix; `latest` moves
only for a final `vX.Y.Z`.

| Tag | Publishes |
|---|---|
| `master-v0.1.0[-alpha]` | `ghcr.io/haruki-nikaidou/guru-master:<version>` (`distroless/cc-debian13:nonroot`) — plus a GitHub release with the raw `linux/x86_64` glibc `guru-master` and `manage-tool` binaries |
| `frontend-v0.1.0[-alpha]` | `ghcr.io/haruki-nikaidou/guru-frontend:<version>` (`distroless/nodejs24-debian13:nonroot`) |
| `worker-v0.1.0[-alpha]` | GitHub release with the raw `linux/x86_64` `guru-worker` binary, glibc (`-gnu`) and static musl (`-musl`) |

Both build from the repository root — the frontend needs the whole Bun workspace
for the `app-protobuf` package:

```sh
docker build -f master.Dockerfile   -t guru-master   .
docker build -f frontend.Dockerfile -t guru-frontend .
```

`guru-master` is configured entirely through the environment (`GURU_WORKER_MODE`
selects the mode; `GURU_DATABASE_URL`, `AMQP_URI` and `REDIS_URL` have no
defaults). The broker is required in every mode — periodic
work is a message, so a broker outage stalls derivation, liveness and renewal
until it returns. `REDIS_URL` (`redis://127.0.0.1:6379/`) is required in the
three modes that open a database connection — `dashboard_grpc`, `workers_grpc`
and `consumer`, never `cron` — and carries change events between master
replicas, so the operator API's `Watch*` streams see edits made against any of
them. The frontend listens on `:3000` and reaches the control plane through
`GURU_GRPC_URL`.

## Documentation

The full manual lives in `typescript/docs` — an Astro Starlight site themed with
[`starlight-theme-black`](https://starlight-theme-black.vercel.app/):

```sh
bun install
bun run docs:dev     # http://localhost:4321/
bun run docs:build   # static output in typescript/docs/dist
```

## Stack

Rust 2024 on Tokio, [`wakuwaku`](https://crates.io/crates/wakuwaku) +
[`kanau`](https://crates.io/crates/kanau) (everything is a `Processor`), gRPC via
Tonic, PostgreSQL for storage (sqlx; migrations in `database/migrations`),
Redis pub/sub for the operator API's live `Watch*` streams, AMQP for
inter-module events, OpenTelemetry for tracing, and a Bun workspace under
`typescript/` sharing one generated API client.

Read [`AGENTS.md`](AGENTS.md) before adding code — it describes exactly how each
layer is organised. [`TEMPLATE_README.md`](TEMPLATE_README.md) documents the
upstream template this workspace started from.
