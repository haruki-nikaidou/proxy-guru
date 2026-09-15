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
| `lib/guru_worker_config` | The worker config model, shared by both planes: the master derives it, the worker consumes it. |
| `lib/rpguru_sdk` | Generated gRPC/protobuf types (Rust) from `proto/`. |
| `modules/auth` | Accounts, sessions, API keys, RBAC. |
| `modules/orchestration` | Canvases, servers, nodes, edges; topology validation, config derivation and worker rollout. |
| `modules/notify` | Notification module — scaffolded from `base`, not implemented yet. |
| `modules/base` | Shared foundations and the layout every module mirrors. |

## Data plane

Each forwarding has a listener (`raw`, `tls`, or an inbound relay) and a
destination: a direct **exit**, a **relay** to another node over TLS-over-TCP or
QUIC, or a **load-balance** group. PROXY protocol v1/v2 is supported on both
ends.

## Rollout model

Mutations bump the canvas generation and publish `CanvasDirty`; the derivation
hook re-derives the whole canvas. A lost message is caught by the periodic
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
selects the mode; `SURREALDB_NAMESPACE`, `SURREALDB_NAME` and `AMQP_URI` have no
defaults). The broker is required in every mode — periodic work is a message, so
a broker outage stalls derivation, liveness and renewal until it returns. The
frontend listens on `:3000` and reaches the control plane through `GURU_GRPC_URL`.

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
Tonic, SurrealDB for storage (schema in `database/`, managed with surrealkit),
Redis for caching, AMQP for inter-module events, OpenTelemetry for tracing, and a
Bun workspace under `typescript/` sharing one generated API client.

Read [`AGENTS.md`](AGENTS.md) before adding code — it describes exactly how each
layer is organised. [`TEMPLATE_README.md`](TEMPLATE_README.md) documents the
upstream template this workspace started from.
