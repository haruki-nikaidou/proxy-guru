---
title: Deploy with Docker
description: Run the control plane from the GHCR images, apply the schema with surrealkit, set up SurrealDB and RabbitMQ, and ship the worker binary from a GitHub release.
---

This guide walks a single-host production deployment from an empty machine to a working dashboard.
It assumes you are comfortable with Linux, Docker and a reverse proxy, and that you have never seen
this project — or an architecture like it — before.

Out of scope on purpose: installing a worker on a data-plane node and registering it with the
control plane. This guide only gets the binary onto a machine you can distribute it from.

## 1. What you are deploying

Four processes, all from **one** image, plus the dashboard:

| Component | Image / artifact | Run mode | Talks to |
|---|---|---|---|
| Operator API | `ghcr.io/haruki-nikaidou/guru-master` | `dashboard_grpc` | SurrealDB, RabbitMQ |
| Worker API | `ghcr.io/haruki-nikaidou/guru-master` | `workers_grpc` | SurrealDB, RabbitMQ |
| Periodic + derivation hooks | `ghcr.io/haruki-nikaidou/guru-master` | `consumer` | SurrealDB, RabbitMQ |
| Scheduler | `ghcr.io/haruki-nikaidou/guru-master` | `cron` | RabbitMQ |
| Dashboard | `ghcr.io/haruki-nikaidou/guru-frontend` | — | Operator API (gRPC) |
| Worker | `guru-worker` binary from a GitHub release | — | Worker API (gRPC) |

State lives in exactly two places: **SurrealDB** (canvases, servers, nodes, edges, accounts,
config views) and **RabbitMQ** (one durable queue for "this canvas changed" hints, plus one per
periodic job). Nothing is kept on a container filesystem, so every container is disposable. Redis
appears in the module scaffolding but the control plane does not connect to it today — you do not
need a Redis server.

The two hook modes are the split to understand before you size anything. `cron` is a clock: it
publishes one execution signal per due job and opens no database connection at all. `consumer`
runs the work — the derivation hook *and* every periodic job — so sweeps, liveness and certificate
renewal scale and fail over exactly like a canvas edit.

Ports, and who is allowed to reach them:

| Port | Process | Exposure |
|---|---|---|
| `50051` | `dashboard_grpc` | **Private.** The dashboard only; plaintext h2c, no TLS, no auth at the transport level. |
| `50052` | `workers_grpc` | Reachable by data-plane nodes (VPN, private network, or a TLS-terminating gRPC proxy). |
| `3000` | dashboard | Behind your HTTPS reverse proxy; never publish directly. |
| `8000` | SurrealDB | **Private.** Root credentials are all it has. |
| `5672` | RabbitMQ | **Private.** |

:::caution[The gRPC ports are plaintext]
Both master modes serve cleartext HTTP/2, and the dashboard opens its channel with
`ChannelCredentials.createInsecure()`. Keep `50051` on a private network (a Docker network, a
loopback bind, or a VPN) and treat `50052` as a link that needs its own transport security if it
crosses the public internet.
:::

## 2. Prerequisites

Work through **[Prerequisites](/guides/prerequisites/)** before this guide. For an image deployment
you need, from that page: Docker Engine and the Compose plugin, a checkout of this repository on an
operator machine (the schema files under `database/` and the `manage-tool` CLI are not shipped as
images), a Rust toolchain plus `protobuf-compiler` to build `manage-tool`, `surrealkit`, `openssl`,
a DNS name with a TLS certificate — and SurrealDB and RabbitMQ themselves, which that page brings
up from `/srv/guru/docker-compose.yml` with the credentials in `/srv/guru/.env`.

Bun is the one thing you can skip here — the dashboard ships as an image. The C toolchain and
`cmake` you do need: `manage-tool` pulls in the certificate stack, whose crates compile vendored C
sources. `perl` is only needed where `guru-worker` is built, which is not here.

## 3. Pick versions

Publishing happens on tag pushes only; the image tag is the git tag minus its component prefix.

| Git tag | Publishes |
|---|---|
| `master-v0.1.0[-alpha]` | `ghcr.io/haruki-nikaidou/guru-master:v0.1.0[-alpha]` |
| `frontend-v0.1.0[-alpha]` | `ghcr.io/haruki-nikaidou/guru-frontend:v0.1.0[-alpha]` |
| `worker-v0.1.0[-alpha]` | GitHub release carrying the raw `linux/x86_64` `guru-worker` binary |

`latest` moves only for a final `vX.Y.Z`, never for a pre-release. **Pin an explicit tag** in your
Compose file anyway: `latest` gives you no way to say which revision is running, and the master and
the schema move together.

Both images are public, so no `docker login ghcr.io` is needed to pull.

## 4. Lay out the secrets

[Prerequisites](/guides/prerequisites/) already created `/srv/guru/.env` next to the Compose file,
with the datastore credentials (`SURREAL_ROOT_USER`, `SURREAL_ROOT_PASSWORD`, `RABBIT_USER`,
`RABBIT_PASSWORD`, `GURU_NS`, `GURU_DB`). Append the image tags you pinned in section 3:

```sh
# /srv/guru/.env  (append)
# The master and the frontend are tagged and released independently; pin each one.
MASTER_VERSION=v0.2.0-beta
FRONTEND_VERSION=v0.1.0-beta
```

`GURU_MASTER_KEY` joins the same file in section 7, once `manage-tool` can print one.

:::caution[The repository `.env` is a different file]
The repository root may contain a `.env` with credentials of *another* environment, and both
`surrealkit` and every process started from that directory inherit it (`SURREALDB_HOST`,
`SURREALDB_USER`, `SURREALDB_PASSWORD`, `SURREALDB_NAMESPACE`, `SURREALDB_NAME`, `AMQP_URI`).
`surrealkit` resolves CLI flags > environment > `.env`, so always pass `--host/--ns/--db/--user/--pass`
explicitly when you run schema commands. A forgotten flag is how a "local" command ends up
rewriting production.
:::

## 5. SurrealDB and RabbitMQ

Both datastores, their Compose services and the requirements behind them (SurrealDB ≥ 3.2, root
credentials, durable RocksDB storage; RabbitMQ on the default vhost with a trailing-slash URI) live
in **[Prerequisites → SurrealDB and RabbitMQ](/guides/prerequisites/#4-surrealdb-and-rabbitmq)**.
They must be up before any master starts:

```sh
cd /srv/guru
docker compose ps          # surrealdb up, rabbitmq healthy
```

Two consequences worth repeating here, because they shape this deployment: the broker is mandatory
in **all four** master modes — periodic work is a message, so a broker outage stalls derivation,
liveness and certificate renewal — and the masters sign in to SurrealDB as **root**, so the
credentials in `/srv/guru/.env` are the ones the `x-master` anchor in section 7 passes on.

## 6. Apply the schema with `surrealkit`

Follow **[Setup Database Schema](/guides/setup-database-schema/)** — pick its *Image deployment*
tab, which uses the `/srv/guru/.env` names and runs from your checkout:

```sh
cd ~/proxy-guru                      # your checkout
read -rs SURREAL_ROOT_PASSWORD       # paste the root password, it is not echoed
export SURREAL_ROOT_PASSWORD

sk() {
  surrealkit --host ws://127.0.0.1:8000 --ns guru --db guru \
    --user root --pass "$SURREAL_ROOT_PASSWORD" "$@"
}
sk setup                             # then rollout plan / lint / start / complete
```

If the database only listens on loopback on the server, tunnel to it:
`ssh -N -L 8000:127.0.0.1:8000 guru-host`.

One thing that article settles differently for this deployment: `sk rollout complete` (the
destructive half) belongs **after** section 7 has rolled out the master version that matches the
schema. On a first install the two halves run back to back, since there is no old version to keep
alive. The rollout manifests and snapshots it writes under `database/` stay on this operator
machine — they are gitignored per-environment state, so back them up with your credentials rather
than committing them.

## 7. Run the control plane

`guru-master`'s *deployment* settings come from the environment: `GURU_WORKER_MODE` picks the mode,
and `SURREALDB_NAMESPACE`, `SURREALDB_NAME`, `AMQP_URI` and `GURU_MASTER_KEY` have **no defaults**.
Everything an operator tunes per installation — health thresholds and retention, the default ACME
directory, the renewal window, how often each periodic job runs — lives in the database instead
(step 8), so replicas need no matching environment.

Generate the master key once and keep it with the database credentials — it encrypts every DNS
provider token and certificate key at rest, and there is no way to recover them without it. The
three modes that read a secret need it; `cron` never does, and the anchor below simply hands the
same environment to all four. `manage-tool` is built from your checkout (see step 8); this
subcommand needs no database:

```sh
./target/release/manage-tool generate-master-key
```

Extend the same `docker-compose.yml`: the `x-master` anchor goes above `services:`, the four
services inside it, next to `surrealdb` and `rabbitmq`:

```yaml
x-master: &master
  image: ghcr.io/haruki-nikaidou/guru-master:${MASTER_VERSION}
  restart: unless-stopped
  environment: &master-env
    SURREALDB_HOST: ws://surrealdb:8000
    SURREALDB_USER: ${SURREAL_ROOT_USER}
    SURREALDB_PASSWORD: ${SURREAL_ROOT_PASSWORD}
    SURREALDB_NAMESPACE: ${GURU_NS}
    SURREALDB_NAME: ${GURU_DB}
    AMQP_URI: amqp://${RABBIT_USER}:${RABBIT_PASSWORD}@rabbitmq:5672/
    GURU_MASTER_KEY: ${GURU_MASTER_KEY}
    GURU_LOG_LEVEL: info
  depends_on:
    surrealdb:
      condition: service_started
    rabbitmq:
      condition: service_healthy

services:
  # ... surrealdb and rabbitmq from section 5 ...

  master-dashboard:
    <<: *master
    environment:
      <<: *master-env
      GURU_WORKER_MODE: dashboard_grpc
    # No `ports`: only the dashboard container reaches :50051, over this network.

  master-workers:
    <<: *master
    environment:
      <<: *master-env
      GURU_WORKER_MODE: workers_grpc
    ports:
      - "50052:50052"

  master-consumer:
    <<: *master
    environment:
      <<: *master-env
      GURU_WORKER_MODE: consumer

  master-cron:
    <<: *master
    environment:
      <<: *master-env
      GURU_WORKER_MODE: cron
```

What each mode is for, and how it scales:

- **`dashboard_grpc`** — the operator API (`Auth` + `Orchestration`) on `GURU_DASHBOARD_GRPC_ADDR`
  (`0.0.0.0:50051`). Stateless; replicate freely behind a gRPC-aware load balancer.
- **`workers_grpc`** — the worker API (`WorkerAgent`) on `GURU_WORKERS_GRPC_ADDR`
  (`0.0.0.0:50052`), plus the config-view poller that wakes worker streams
  (`GURU_WATCH_POLL_MS`, default `1000`). Replicable, but each worker session is pinned to the
  instance holding its stream, so put a plain TCP/gRPC load balancer in front, never an HTTP/1 proxy.
- **`consumer`** — every hook: it re-derives a canvas when a `CanvasDirty` message arrives (prefetch
  8) and it runs all five periodic passes when their execution signals arrive — the stale-canvas
  sweep, the health liveness sweep, health retention, ACME issuance/renewal and relay-leaf
  rotation. This is the mode that needs outbound HTTPS to the ACME directory and the DNS provider
  APIs, and DNS to public resolvers. Replicate it for throughput and for failover: derivation is
  guarded by the canvas generation counter, and every periodic pass claims its run in one
  `orchestration_job_run` row before working, so a signal delivered twice or to two replicas runs
  once. Two ACME passes cannot race the same DNS-01 challenge either — each certificate attempt is
  claimed per row.
- **`cron`** — the clock, and only the clock. It scans every 5 s and publishes one execution signal
  per due job: `derive_stale_canvases` and `sweep_liveness` every 30 s,
  `renew_certificates` every 60 s, `trim_health_history` every 5 min,
  `rotate_relay_certificates` hourly. It opens no database connection, never reads
  `GURU_MASTER_KEY` and keeps no local state, so the only secret it holds is the broker credential
  in `AMQP_URI` — which is also the one thing it cannot run without. There is nothing to scale:
  one replica is enough, and a second is harmless because the consumer's run claim discards the
  duplicate. How often a job may actually run is a stored setting, not a flag — see step 8.

Relay links over TLS or QUIC need the internal CA before their pods derive. Run this once from the
operator machine, with the same `GURU_MASTER_KEY` the master uses:

```sh
GURU_MASTER_KEY='<the key>' ./target/release/manage-tool \
  --address ws://127.0.0.1:8000 --username root --password '<root password>' \
  --namespace guru --database guru \
  orchestration init-ca
```

It prints the CA certificate and marks every canvas holding a TLS/QUIC relay for re-derivation. It
refuses to run twice.

Two operational notes that follow from the code:

- The `consumer` and `cron` modes **exit non-zero when the AMQP connection drops** (the client does
  not reconnect, and a silently dead consumer or a clock that publishes nowhere is worse than a
  restart): `the AMQP connection was lost: restart once the broker at AMQP_URI is reachable again`.
  `restart: unless-stopped` is what makes that self-healing — do not remove it.
- The images are distroless: no shell, no `curl`. A Compose `healthcheck` that shells out cannot
  work. Monitor from outside instead (a TCP connect to `50051`/`50052`, or scrape the logs).

Start them:

```sh
docker compose up -d
docker compose logs master-dashboard master-workers master-consumer master-cron
```

A healthy start looks like this. The consumer prints one line per queue it bound, the scheduler one
line naming every cadence it will publish on:

```text
master-dashboard-1  | INFO guru_master: serving operator API addr=0.0.0.0:50051
master-workers-1    | INFO guru_master: serving worker API addr=0.0.0.0:50052
master-consumer-1   | INFO guru_master: consuming queue="guru_orchestration_canvas_dirty" key="canvas_dirty"
master-consumer-1   | INFO guru_master: consuming queue="guru_orchestration_derive_stale_canvases" key="derive_stale_canvases"
master-consumer-1   | INFO guru_master: consuming queue="guru_orchestration_rotate_relay_certificates" key="rotate_relay_certificates"
master-consumer-1   | INFO guru_master: consuming queue="guru_orchestration_sweep_liveness" key="sweep_liveness"
master-consumer-1   | INFO guru_master: consuming queue="guru_orchestration_trim_health_history" key="trim_health_history"
master-consumer-1   | INFO guru_master: consuming queue="guru_orchestration_renew_certificates" key="renew_certificates"
master-cron-1       | INFO guru_master: scheduling periodic execution signals scan_interval_secs=5 derive_stale_canvases_secs=30 rotate_relay_certificates_secs=3600 sweep_liveness_secs=30 trim_health_history_secs=300 renew_certificates_secs=60
```

The scheduler is quiet after that: each publication is logged at `DEBUG`
(`published an execution signal job="sweep_liveness"`), while a publish that fails is an `ERROR`
and does not end the process. So `GURU_LOG_LEVEL=debug` on the cron container is how you confirm
the clock is ticking, and an `ERROR ... publishing an execution signal failed` is how a broker
problem shows up before the connection is declared lost.

## 8. Create the first administrator

There is no self-service signup: the first account is created directly against the database with
`manage-tool`, which deliberately bypasses RBAC because no admin exists yet. It is not published as
an image, so build it from your checkout:

```sh
cd ~/proxy-guru
cargo build --release -p manage-tool

./target/release/manage-tool \
  --address ws://127.0.0.1:8000 --username root --password '<root password>' \
  --namespace guru --database guru \
  create-admin --email admin@example.com --password '<strong password>'
# Created admin account auth_account:uz0ih3b30nrekqzs1h1y
```

Pass all five database flags explicitly — they also read `SURREALDB_*` from the environment, so a
stray `.env` silently redirects the command.

### Seed the module configuration

The operator-tunable settings live in the `app_config` table, one row per key, and the seed step is
part of [Setup Database Schema → Seed the module
configuration](/guides/setup-database-schema/#3-seed-the-module-configuration) — run it now if you
skipped it there:

```sh
./target/release/manage-tool \
  --address ws://127.0.0.1:8000 --username root --password "$SURREAL_ROOT_PASSWORD" \
  --namespace guru --database guru \
  config seed
# seeded auth
# seeded orchestration
```

`config list` prints every stored document, and `config set <key> <json>` replaces one key — for
example a two-week ACME renewal window:

```sh
./target/release/manage-tool ... config set orchestration '{"acme_renew_before_secs":1209600}'
```

A `set` is validated against the config's type before it is written and replaces the whole
document, with unspecified fields taking their default. The masters read these keys once at startup,
so restart them to pick a change up. See
[Configuration → Module configuration](/reference/configuration#module-configuration).

The cadence of the periodic jobs lives on the same key: `sweep_interval_secs` (30),
`liveness_interval_secs` (30), `health_retention_interval_secs` (300), `acme_interval_secs` (60)
and `relay_rotation_interval_secs` (3600). The scheduler publishes on a fixed cadence because it
reads no configuration; each consumer claims a run at most once per *configured* interval, so a
value at or below the signal's cadence means "run on every signal" and a larger one slows the job
down fleet-wide:

```sh
./target/release/manage-tool ... config set orchestration '{"acme_interval_secs":300}'
```

The same binary has `orchestration export-config --server <key>`, which prints the worker TOML the
canvas currently derives for one server. That is the tool to reach for when a node's behaviour and
the canvas seem to disagree.

## 9. Run the dashboard

The dashboard is a SvelteKit app on the Node adapter. It listens on `:3000` and reaches the control
plane through `GURU_GRPC_URL`. One more service in the same file:

```yaml
  frontend:
    image: ghcr.io/haruki-nikaidou/guru-frontend:${FRONTEND_VERSION}
    restart: unless-stopped
    environment:
      GURU_GRPC_URL: master-dashboard:50051
      PROTOCOL_HEADER: x-forwarded-proto
      HOST_HEADER: x-forwarded-host
    ports:
      - "127.0.0.1:3000:3000"
    depends_on:
      - master-dashboard
```

:::danger[Serve it over HTTPS, and forward the protocol]
This is the single most common way to get a dashboard that loads but cannot log in.

The app never trusts the socket it is listening on. For every request it reconstructs its own
origin from headers and **defaults the scheme to `https`** when `PROTOCOL_HEADER` is unset. Its
login (a SvelteKit *remote function*, i.e. a POST) is rejected with
`403 {"message":"Cross-site remote requests are forbidden"}` whenever the browser's `Origin` header
does not match that reconstructed origin. So:

- **Behind an HTTPS proxy that preserves `Host`:** it works with no extra configuration — scheme
  defaults to `https`, host comes from `Host`.
- **Anything else (plain HTTP, a different public host or port):** set
  `PROTOCOL_HEADER=x-forwarded-proto` and `HOST_HEADER=x-forwarded-host`, and make the proxy send
  both. `X-Forwarded-Host` **must include the port** if the public URL has a non-default one —
  nginx's `$host` drops it, use `$http_host`.
- `ORIGIN` does nothing. The Node adapter in this build bakes that value in at build time from
  `kit.paths.origin`; the runtime variable is ignored.

HTTPS is not optional in any case: the session cookie (`guru_session`, a full bearer credential for
the control plane) is issued with `Secure`, so browsers drop it over plain HTTP on anything but
`localhost`.
:::

A minimal nginx server block, with the two headers the app needs:

```nginx
server {
    listen 443 ssl;
    server_name guru.example.com;

    ssl_certificate     /etc/letsencrypt/live/guru.example.com/fullchain.pem;
    ssl_certificate_key /etc/letsencrypt/live/guru.example.com/privkey.pem;

    location / {
        proxy_pass http://127.0.0.1:3000;
        proxy_http_version 1.1;
        proxy_set_header Host              $host;
        proxy_set_header X-Forwarded-Host  $http_host;   # $host drops the port
        proxy_set_header X-Forwarded-Proto $scheme;
        proxy_set_header X-Forwarded-For   $proxy_add_x_forwarded_for;
        proxy_set_header Upgrade           $http_upgrade;
        proxy_set_header Connection        "upgrade";
    }
}
```

Also useful: `ADDRESS_HEADER=x-forwarded-for` if you want real client IPs, and `BODY_SIZE_LIMIT`
(default `512K`) if you ever import very large canvases.

Now open `https://guru.example.com/`, which redirects to `/auth`, and sign in with the account from
section 8. You should land on the canvas list with your email in the sidebar.

## 10. Get the worker binary from a GitHub release

The data plane ships as a raw binary, not an image, and only `linux/x86_64` is published. Each
`worker-<version>` release carries one asset, `guru-worker-<version>-x86_64-unknown-linux-gnu`,
where `<version>` is the tag minus its `worker-` prefix — so tag `worker-v0.1.0` publishes
`guru-worker-v0.1.0-x86_64-unknown-linux-gnu`.

Pick a release that actually lists that asset, and check before you script anything around it:

```sh
curl -fsSL https://api.github.com/repos/haruki-nikaidou/proxy-guru/releases \
  | jq -r '.[] | select(.tag_name | startswith("worker-"))
           | .tag_name + " -> " + ((.assets | map(.name)) | join(", "))'
```

:::caution[Not every tag has a binary]
A `worker-v*` tag whose release lists no assets was tagged before the release workflow existed (as
of writing, `worker-v0.0.1-alpha` is in exactly that state: `assets: []`). There is nothing to
download from such a tag — use a newer release, or push a fresh `worker-v*` tag so the *Release
Worker* workflow builds and attaches the binary.
:::

With the GitHub CLI:

```sh
VERSION=v0.1.0
gh release download "worker-${VERSION}" \
  --repo haruki-nikaidou/proxy-guru \
  --pattern 'guru-worker-*-x86_64-unknown-linux-gnu' \
  --output guru-worker
```

Or with plain `curl` — resolve the asset through the API so you never hard-code a URL:

```sh
VERSION=v0.1.0
url=$(curl -fsSL \
  "https://api.github.com/repos/haruki-nikaidou/proxy-guru/releases/tags/worker-${VERSION}" \
  | jq -r '.assets[] | select(.name | endswith("x86_64-unknown-linux-gnu")) | .browser_download_url')
curl -fsSL "$url" -o guru-worker
```

Then make it executable and confirm it runs (there is no `--version` flag; `--help` is the smoke
test):

```sh
chmod +x guru-worker
./guru-worker --help
```

Keep the binaries in your own artifact store (an internal HTTP server, an apt/OCI registry, your
config-management system) keyed by version. There is no `latest` alias and no published checksum
file, so record the version — and ideally your own `sha256sum` — alongside the copy you distribute.

The binary is glibc-linked (`x86_64-unknown-linux-gnu`), built on the GitHub runner's Debian base.
It runs on a current Debian/Ubuntu/RHEL; it will not run on Alpine or any other musl distribution.

Installing and registering a worker node against this control plane is covered separately;
everything above stops at "the binary is available and distributable". A node that should run
without a control plane at all is a different guide:
[Independent Worker Deployment](/guides/independent-worker/).

## 11. Verify the deployment

Work through these in order — each one fails loudly and independently:

```sh
# 1. Datastores
docker compose ps                     # surrealdb + rabbitmq healthy

# 2. Schema
sk status                             # from section 6
#   → the rollout you applied, [completed]

# 3. Control plane: one banner per mode, and no restart loop
docker compose logs --tail=20 master-dashboard master-workers master-consumer master-cron

# 4. Worker API reachable from a data-plane node's network
nc -z <host> 50052 && echo "workers_grpc reachable"

# 5. Dashboard through the proxy (303 to /auth)
curl -s -o /dev/null -w '%{http_code}\n' https://guru.example.com/

# 6. Log in with the admin account — this is the only check that exercises
#    dashboard → operator API → SurrealDB end to end.
```

If step 6 fails with `Forbidden` while steps 1–5 pass, re-read the proxy warning in section 9.

## 12. Upgrades, backups, rollback

**Upgrading.** Schema first, code second, contraction last:

1. `surrealkit rollout plan --name <change>` and review the manifest.
2. `surrealkit rollout start <target>` — expansion only; the running version keeps working.
3. Bump `MASTER_VERSION` (and `FRONTEND_VERSION`, if the dashboard also has a new tag) in
   `.env`, then `docker compose pull && docker compose up -d`.
4. Verify, then `surrealkit rollout complete <target>`.

If step 3 or 4 goes wrong: `surrealkit rollout rollback <target>`, and pin the version variables
back to the previous tags. A rollout killed mid-flight leaves `__rollout.status` on `running_*` — heal the
metadata with `surrealkit rollout repair <target>` before planning anything else.

**Backups.** SurrealDB is the only irreplaceable state:

```sh
docker compose exec -T surrealdb /surreal export \
  --endpoint http://127.0.0.1:8000 --user root --pass '<pw>' \
  --ns guru --db guru - > guru-$(date +%F).surql
```

Snapshot the `surreal-data` volume too if you want a fast restore path. RabbitMQ needs no backup:
its queues hold edit hints and execution signals, both of which the scheduler re-publishes and the
generation counters make idempotent — but the broker has to be *running*, because no periodic job
happens while it is not.

**Logs.** Everything is structured `tracing` output on stdout, with `GURU_LOG_LEVEL` taking a full
`EnvFilter` string (`info`, `warn`, `guru_master=debug,orchestration=debug`, …). Ship it with your
usual Docker log driver.

## 13. Troubleshooting

| Symptom | Cause |
|---|---|
| Dashboard login returns `Forbidden` / `Cross-site remote requests are forbidden` | Reconstructed origin ≠ browser `Origin`. Serve over HTTPS, or set `PROTOCOL_HEADER`/`HOST_HEADER` and forward `X-Forwarded-Proto` and `X-Forwarded-Host` (with the port). `ORIGIN` has no effect. |
| Login succeeds, next request bounces back to `/auth` | The `Secure` session cookie was dropped — the browser reached the dashboard over plain HTTP. |
| `error: the following required arguments were not provided: --namespace` | `SURREALDB_NAMESPACE` / `SURREALDB_NAME` are unset; they have no defaults. |
| Master exits with `master key: GURU_MASTER_KEY is not set` (or `must be 32 bytes`) | `dashboard_grpc`, `workers_grpc` and `consumer` need the key (`cron` does not read it). Generate one with `manage-tool generate-master-key`; it is read from the environment only. |
| Master exits with `stored config for key ... does not match its type` | The stored document is corrupt or predates a renamed field. Inspect it with `manage-tool config get <key>` and rewrite it with `config set`. |
| A TLS Entry's pod stays in `invalid_pods` with `certificate for … is pending` / `failed: …` | The ACME pass has not issued it yet, or the last attempt failed (`ListCertificates` shows `last_error`). It runs in `consumer`, on the `renew_certificates` signal: check that a `consumer` is up, that the DNS provider token and `domain_id` (Cloudflare zone id / Vercel domain) are right, and that the consumer reaches the ACME directory. `RetryCertificate` forces a retry. |
| A relay pod stays in `invalid_pods` with `internal CA not initialised` | Run `manage-tool orchestration init-ca` once. |
| Master exits immediately with an AMQP error | `AMQP_URI` unset or unreachable. All four modes require the broker. Check the trailing `/` on the URI. |
| `consumer` or `cron` restarts periodically | Expected on broker loss: the client does not reconnect, so the process exits and the restart policy brings it back. Investigate the broker, not the master. |
| `table does not exist` / cancelled transactions right after a clean install | SurrealDB older than 3.2, or the schema was never applied. Check `surrealkit status`. |
| `surrealkit` wrote to the wrong database | A `.env` in the working directory supplied the connection. Always pass `--host/--ns/--db/--user/--pass`. |
| Canvas edits never reach a worker | `consumer` is down: it runs both the edit hook and the stale-canvas sweep, so nothing derives without it. If `consumer` is up, check `cron` — without the clock the sweep never fires and only edits with a live `CanvasDirty` derive. |
| Periodic jobs stop happening (nothing goes `Offline`, no renewals) | RabbitMQ is down, or `cron` is. Both are required: the clock publishes the signals, the consumer runs them. |

See [Configuration](/reference/configuration/) for every flag and variable, and
[Rollout Model](/reference/rollout/) for what "derivation" actually does.
