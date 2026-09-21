---
title: Deploy with Docker
description: Run the control plane from the GHCR images, apply the schema, set up PostgreSQL, RabbitMQ and Redis, and ship the worker binary from a GitHub release.
---

This guide walks a single-host production deployment from an empty machine to a working dashboard.
It assumes you are comfortable with Linux, Docker and a reverse proxy, and that you have never seen
this project — or an architecture like it — before.

Out of scope on purpose: installing a worker on a data-plane node and registering it with the
control plane. This guide only gets the binary onto a machine you can distribute it from.

## 1. What you are deploying

Five processes, all from **one** image, plus the dashboard:

| Component | Run mode | Talks to |
|---|---|---|
| Operator API | `dashboard_grpc` | PostgreSQL, RabbitMQ, Redis |
| Worker API | `workers_grpc` | PostgreSQL, RabbitMQ, Redis |
| Periodic + derivation hooks | `consumer` | PostgreSQL, RabbitMQ, Redis |
| Scheduler | `cron` | RabbitMQ |
| Notification delivery | `notifier` | PostgreSQL, RabbitMQ (**exactly one instance**) |
| Dashboard | — | Operator API (gRPC) |

:::note
Because of the nature of a TCP reverse proxy server, deploying a worker inside a Docker container
is bad practice. Therefore, we do not provide a Docker image for worker nodes.
:::

Durable state lives in exactly two places: **PostgreSQL** (canvases, servers, the pod graph, accounts,
config views) and **RabbitMQ** (one durable queue for "this canvas changed" hints, plus one per
periodic job). **Redis** is the third datastore and the only one that keeps nothing: it carries the
operator API's live events between master replicas on a single pub/sub channel, with no persistence
configured. Nothing is kept on a container filesystem, so every container is disposable.

The two hook modes are the split to understand before you size anything. `cron` is a clock: it
publishes one execution signal per due job and opens no database connection at all. `consumer`
runs the work — the derivation hook *and* every periodic job — so sweeps, liveness and certificate
renewal scale and fail over exactly like a canvas edit.

`notifier` is the third hook mode and the one exception to "scale it freely": it sends the emails
and Telegram messages the `consumer`'s fan-out publishes, and a second instance would double every
notice a restart overlapped, so it takes a PostgreSQL advisory lock and refuses to start beside a
peer that holds it. It is also the only mode that needs neither `GURU_MASTER_KEY` nor `REDIS_URL`
([Notifications](/features/notifications/)).

Ports, and who is allowed to reach them:

| Port | Process | Exposure |
|---|---|---|
| `50051` | `dashboard_grpc` | **Private.** The dashboard only; plaintext h2c, no TLS, no auth at the transport level. |
| `50052` | `workers_grpc` | Reachable by data-plane nodes (VPN, private network, or a TLS-terminating gRPC proxy). |
| `3000` | dashboard | Behind your HTTPS reverse proxy; never publish directly. |
| `5432` | PostgreSQL | **Private.** One role, one database, one password. |
| `5672` | RabbitMQ | **Private.** |
| `6379` | Redis | **Private.** No credentials at all; the listener is the access control. |

:::caution[The gRPC ports are plaintext]
Both master modes serve cleartext HTTP/2, and the dashboard opens its channel with
`ChannelCredentials.createInsecure()`. Keep `50051` on a private network (a Docker network, a
loopback bind, or a VPN) and treat `50052` as a link that needs its own transport security if it
crosses the public internet.
:::

## 2. Prerequisites

Work through **[Prerequisites](/guides/prerequisites/)** before this guide. For an image deployment
you need, from that page: Docker Engine and the Compose plugin, a checkout of this repository on an
operator machine (for the Compose file), `openssl`, a DNS name with a TLS certificate — and
PostgreSQL, RabbitMQ and Redis themselves, which
that page brings up from `/srv/guru/docker-compose.yml` with the credentials in `/srv/guru/.env`.

`guru-master` and `manage-tool` are published as plain binaries too: every `master-v*` tag attaches
them to a GitHub release next to the image (section 10), so a Rust toolchain, `protobuf-compiler`,
a C toolchain and `cmake` are only needed **if you choose to build them** — `manage-tool` pulls in
the certificate stack, whose crates compile vendored C sources. The schema travels inside the
binaries, so nothing has to be copied out of the checkout to apply it.

Bun is the one thing you can skip here — the dashboard ships as an image. `perl` is only needed
where `guru-worker` is built, which is not here.

## 3. Pick versions

Publishing happens on tag pushes only; the image tag is the git tag minus its component prefix.

| Git tag | Publishes |
|---|---|
| `master-v0.1.0[-alpha]` | `ghcr.io/haruki-nikaidou/guru-master:v0.1.0[-alpha]`, **and** a GitHub release carrying the `guru-master` and `manage-tool` binaries (`x86_64-unknown-linux-gnu`) |
| `frontend-v0.1.0[-alpha]` | `ghcr.io/haruki-nikaidou/guru-frontend:v0.1.0[-alpha]` |
| `worker-v0.1.0[-alpha]` | GitHub release carrying two raw `linux/x86_64` `guru-worker` binaries: one glibc-linked (`x86_64-unknown-linux-gnu`) and one static musl (`x86_64-unknown-linux-musl`) |

`latest` moves only for a final `vX.Y.Z`, never for a pre-release. **Pin an explicit tag** in your
Compose file anyway: `latest` gives you no way to say which revision is running, and the master and
the schema move together.

Release assets repeat that same `<version>` in their file names. The `master-v*` release is
published *in addition to* the image and holds the master binary plus the `manage-tool` CLI, glibc
and `x86_64` only (section 10); the `worker-v*` release holds both a glibc and a static musl
`x86_64` worker binary (section 11).

Both images are public, so no `docker login ghcr.io` is needed to pull.

## 4. Lay out the secrets

[Prerequisites](/guides/prerequisites/) already created `/srv/guru/.env` next to the Compose file,
with the datastore credentials (`POSTGRES_USER`, `POSTGRES_PASSWORD`, `POSTGRES_DB`, `RABBIT_USER`,
`RABBIT_PASSWORD`). Append the image tags you pinned in section 3:

```sh
# /srv/guru/.env  (append)
# The master and the frontend are tagged and released independently; pin each one.
MASTER_VERSION=v1.0.0
FRONTEND_VERSION=v1.0.0
```

`GURU_MASTER_KEY` joins the same file in section 7, once `manage-tool` can print one.

:::caution[The repository `.env` is a different file]
The repository root may contain a `.env` with credentials of *another* environment
(`GURU_DATABASE_URL`, `AMQP_URI`), and every process started from that directory inherits it. Pass
`--database-url` explicitly when you run `manage-tool` from a checkout. A forgotten flag is how a
"local" command ends up rewriting production.
:::

## 5. PostgreSQL, RabbitMQ and Redis

All three datastores, their Compose services and the requirements behind them (PostgreSQL ≥ 16 with
durable storage; RabbitMQ on the default vhost with a trailing-slash URI; Redis 7.x with no
credentials and no persistence) live in
**[Prerequisites → PostgreSQL, RabbitMQ and Redis](/guides/prerequisites/#4-postgresql-rabbitmq-and-redis)**.
They must be up before any master starts:

```sh
cd /srv/guru
docker compose ps          # postgres healthy, rabbitmq healthy, redis up
```

Three consequences worth repeating here, because they shape this deployment: the broker is mandatory
in **all five** master modes — periodic work is a message, so a broker outage stalls derivation,
liveness and certificate renewal — and the masters reach the database with the role in
`/srv/guru/.env`, which the `x-master` anchor in section 7 assembles into a URL. Redis is
the third: required by the three modes that serve or derive, and far cheaper to lose.
An outage stops delivery on open `Watch*` streams and nothing else — edits still apply, canvases
still derive, workers still get their config — and the subscriber reconnects on its own, then has
every watcher re-read the database. The dashboard follows those streams, so while Redis is down an
open canvas or health page stops updating and catches up on its own once it is back; the *Live*
badge on those pages shows the browser's own connection and stays green meanwhile.

## 6. Apply the schema

Each master applies what is pending when it starts, so this section is optional on a first install —
but running it first means a schema problem surfaces here rather than in five restarting containers.
Follow **[Setup Database Schema](/guides/setup-database-schema/)**:

```sh
read -rs GURU_DATABASE_URL           # paste the URL, it is not echoed
export GURU_DATABASE_URL
./manage-tool db migrate
```

If the database only listens on loopback on the server, tunnel to it:
`ssh -N -L 5432:127.0.0.1:5432 guru-host`.

## 7. Run the control plane

`guru-master`'s *deployment* settings come from the environment: `GURU_WORKER_MODE` picks the mode,
and `GURU_DATABASE_URL`, `AMQP_URI`, `REDIS_URL` and `GURU_MASTER_KEY` have
**no defaults**. Everything an operator tunes per installation — health thresholds and retention,
the default ACME directory, the renewal window, how often each periodic job runs — lives in the
database instead (step 8), so replicas need no matching environment.

Generate the master key once and keep it with the database credentials — it encrypts every DNS
provider token and certificate key at rest, and there is no way to recover them without it. The
three modes that read a secret need it; `cron` and `notifier` never do, and the anchor below simply
hands the same environment to all five. `REDIS_URL` has a similar scope — the three modes that
serve or derive refuse to start without it, `cron` and `notifier` ignore it. `manage-tool` is either built
from your checkout or downloaded from a `master-v*` release (steps 8 and 10); this subcommand needs
no database:

```sh
./target/release/manage-tool generate-master-key
```

Extend the same `docker-compose.yml`: the `x-master` anchor goes above `services:`, the five
services inside it, next to `postgres`, `rabbitmq` and `redis`:

```yaml
x-master: &master
  image: ghcr.io/haruki-nikaidou/guru-master:${MASTER_VERSION}
  restart: unless-stopped
  environment: &master-env
    GURU_DATABASE_URL: postgres://${POSTGRES_USER}:${POSTGRES_PASSWORD}@postgres:5432/${POSTGRES_DB}
    AMQP_URI: amqp://${RABBIT_USER}:${RABBIT_PASSWORD}@rabbitmq:5672/
    REDIS_URL: redis://redis:6379/
    GURU_MASTER_KEY: ${GURU_MASTER_KEY}
    GURU_LOG_LEVEL: info
  depends_on:
    postgres:
      condition: service_healthy
    rabbitmq:
      condition: service_healthy
    redis:
      condition: service_started

services:
  # ... postgres, rabbitmq and redis from section 5 ...

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
    # Loopback only: nginx terminates TLS for the worker API on :443 (section 9).
    ports:
      - "127.0.0.1:50052:50052"

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

  # Exactly one: the mode takes an advisory lock and a second replica exits.
  master-notifier:
    <<: *master
    environment:
      <<: *master-env
      GURU_WORKER_MODE: notifier
      # Both optional; each unset value disables its channel.
      GURU_SMTP_PASSWORD: "${GURU_SMTP_PASSWORD}"
      GURU_TELEGRAM_BOT_TOKEN: "${GURU_TELEGRAM_BOT_TOKEN}"
```

What each mode is for, and how it scales:

- **`dashboard_grpc`** — the operator API (`Auth` + `Orchestration`) on `GURU_DASHBOARD_GRPC_ADDR`
  (`0.0.0.0:50051`). Stateless; replicate freely behind a gRPC-aware load balancer.
- **`workers_grpc`** — the worker API (`WorkerAgent`) on `GURU_WORKERS_GRPC_ADDR`
  (`0.0.0.0:50052`), plus the config-view poller that wakes worker streams
  (`GURU_WATCH_POLL_MS`, default `1000`). Replicable, but each worker session is pinned to the
  instance holding its stream, so put a plain TCP/gRPC load balancer in front, never an HTTP/1 proxy.
- **`consumer`** — every hook: it re-derives a canvas when a `CanvasDirty` message arrives (prefetch
  8) and it runs all six periodic passes when their execution signals arrive — the stale-canvas
  sweep, the health liveness sweep, health retention, ACME issuance/renewal, relay-leaf rotation
  and the country lookup of new server addresses. This is the mode that needs outbound HTTPS to the
  ACME directory, the DNS provider APIs and `country_lookup_url`, and DNS to public resolvers.
  Replicate it for throughput and for failover: derivation is guarded by the canvas generation
  counter, and every periodic pass claims its run in one `orchestration_job_run` row before working,
  so a signal delivered twice or to two replicas runs once. Two ACME passes cannot race the same
  DNS-01 challenge either — each certificate attempt is claimed per row.
- **`cron`** — the clock, and only the clock. It scans every 5 s and publishes one execution signal
  per due job: `derive_stale_canvases` and `sweep_liveness` every 30 s, `renew_certificates` and
  `resolve_server_countries` every 60 s, `trim_health_history` every 5 min,
  `rotate_relay_certificates` hourly. It opens no database connection, never reads
  `GURU_MASTER_KEY` and keeps no local state, so the only secret it holds is the broker credential
  in `AMQP_URI` — which is also the one thing it cannot run without. There is nothing to scale:
  one replica is enough, and a second is harmless because the consumer's run claim discards the
  duplicate. How often a job may actually run is a stored setting, not a flag — see step 8.
- **`notifier`** — notification delivery, and the one mode to run **exactly once**: it consumes the
  two notice queues the `consumer`'s fan-out publishes to and sends each notice over SMTP and the
  Telegram Bot API. It takes a session-scoped PostgreSQL advisory lock first, so a second replica
  exits non-zero with `another notifier already holds the advisory lock; run exactly one` — which
  makes `deploy: replicas: 1` a hard guarantee rather than a convention. It needs neither
  `GURU_MASTER_KEY` nor `REDIS_URL`, only outbound SMTP and HTTPS to `telegram_api_base`. While it
  is down, notices wait in their queues.

Relay links over TLS or QUIC need the internal CA before their pods derive. Run this once from the
operator machine, with the same `GURU_MASTER_KEY` the master uses:

```sh
GURU_MASTER_KEY='<the key>' ./target/release/manage-tool \
  --database-url "$GURU_DATABASE_URL" orchestration init-ca
```

It prints the CA certificate and marks every canvas holding a TLS/QUIC relay for re-derivation. It
refuses to run twice.

Three operational notes that follow from the code:

- The `consumer`, `notifier` and `cron` modes **exit non-zero when the AMQP connection drops** (the client does
  not reconnect, and a silently dead consumer or a clock that publishes nowhere is worse than a
  restart): `the AMQP connection was lost: restart once the broker at AMQP_URI is reachable again`.
  `restart: unless-stopped` is what makes that self-healing — do not remove it.
- The images are distroless: no shell, no `curl`. A Compose `healthcheck` that shells out cannot
  work. Monitor from outside instead (a TCP connect to `50051`/`50052`, or scrape the logs).
- Redis behaves the other way round: the subscriber reconnects by itself (500 ms doubling to 10 s)
  and logs `live bus connected` each time, and after every reconnect it has every open `Watch*`
  stream re-read the database, so nothing stays stale. What it held in the meantime is lost and that
  is fine — the channel carries in-flight events, never state. PostgreSQL is still the only thing in
  this deployment worth backing up (section 13).

Start them:

```sh
docker compose up -d
docker compose logs master-dashboard master-workers master-consumer master-cron master-notifier
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
master-consumer-1   | INFO guru_master: consuming queue="guru_orchestration_resolve_server_countries" key="resolve_server_countries"
master-consumer-1   | INFO guru_master: consuming queue="guru_notify_health_changed" key="health_changed"
master-cron-1       | INFO guru_master: scheduling periodic execution signals scan_interval_secs=5 derive_stale_canvases_secs=30 rotate_relay_certificates_secs=3600 sweep_liveness_secs=30 trim_health_history_secs=300 renew_certificates_secs=60 resolve_server_countries_secs=60
master-notifier-1   | INFO guru_master: notification channel channel="email: configured"
master-notifier-1   | INFO guru_master: consuming queue="guru_notify_health_notify_group" key="health_notify_group"
master-notifier-1   | INFO guru_master: consuming queue="guru_notify_health_notify_personal" key="health_notify_personal"
```

The scheduler is quiet after that: each publication is logged at `DEBUG`
(`published an execution signal job="sweep_liveness"`), while a publish that fails is an `ERROR`
and does not end the process. So `GURU_LOG_LEVEL=debug` on the cron container is how you confirm
the clock is ticking, and an `ERROR ... publishing an execution signal failed` is how a broker
problem shows up before the connection is declared lost.

## 8. Create the first administrator

There is no self-service signup: the first account is created directly against the database with
`manage-tool`, which deliberately bypasses RBAC because no admin exists yet. It is not part of any
image, so either build it from your checkout or download it from a `master-v*` release (section 10):

```sh
cd ~/proxy-guru
cargo build --release -p manage-tool

./target/release/manage-tool --database-url "$GURU_DATABASE_URL" \
  create-admin --email admin@example.com --password '<strong password>'
# Created admin account uz0ih3b30nrekqzs1h1y
```

Pass `--database-url` explicitly — `manage-tool` also reads `GURU_DATABASE_URL` from the
environment, so a stray `.env` silently redirects the command.

### Seed the module configuration

The operator-tunable settings live in the `app_config` table, one row per key, and the seed step is
part of [Setup Database Schema → Seed the module
configuration](/guides/setup-database-schema/#3-seed-the-module-configuration) — run it now if you
skipped it there:

```sh
./manage-tool --database-url "$GURU_DATABASE_URL" config seed
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
`liveness_interval_secs` (30), `health_retention_interval_secs` (300), `acme_interval_secs` (60),
`country_lookup_interval_secs` (60) and `relay_rotation_interval_secs` (3600). The scheduler
publishes on a fixed cadence because it reads no configuration; each consumer claims a run at most
once per *configured* interval, so a value at or below the signal's cadence means "run on every
signal" and a larger one slows the job down fleet-wide:

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

The server block below carries the dashboard, the worker API and the agent download directory on
one hostname. gRPC has no path prefix of its own: every worker call is
`POST /guru.orchestration.agent.WorkerAgent/<Method>`, so a `location` on that prefix hands those
requests to `:50052` and everything else to the dashboard. The `/agent/` location serves what
`manage-tool agent publish` writes ([Install and Update Agents](/guides/agent-install/)); create the
directory (`sudo mkdir -p /srv/guru/agent`) before the first publish. The dashboard and the worker
API may also live on two hostnames — two server blocks, each with a certificate for its name — as
long as `agent_public_base_url` names the one the worker locations are on.

```nginx
map $http_upgrade $connection_upgrade {
    default upgrade;
    ''      close;
}

server {
    listen 443 ssl so_keepalive=60s:15s:4;
    listen [::]:443 ssl so_keepalive=60s:15s:4;
    http2 on;                 # nginx ≥ 1.25.1; older builds: `listen 443 ssl http2;`
    server_name guru.example.com;

    ssl_certificate     /etc/letsencrypt/live/guru.example.com/fullchain.pem;
    ssl_certificate_key /etc/letsencrypt/live/guru.example.com/privkey.pem;

    # ---- worker agent API: TLS terminated here, plaintext h2c to master-workers.
    location ^~ /guru.orchestration.agent.WorkerAgent/ {
        grpc_pass grpc://127.0.0.1:50052;
        grpc_connect_timeout 5s;
        grpc_read_timeout 7d;     # WatchConfig of a worker built before keep-alives is silent for hours
        grpc_send_timeout 7d;
        grpc_socket_keepalive on;
        client_max_body_size 0;   # ReportHealth is one body that grows for the session's life
        client_body_timeout 60s;  # four missed health reports
        grpc_set_header x-api-key     $http_x_api_key;
        grpc_set_header x-refresh-key $http_x_refresh_key;
        grpc_set_header X-Real-IP     $remote_addr;
    }

    # ---- agent binaries, installer, unit and start guard.
    location ^~ /agent/ {
        alias /srv/guru/agent/;
        autoindex off;
        default_type application/octet-stream;
        add_header Cache-Control "public, max-age=300";
    }

    # ---- dashboard, with the two headers the app needs.
    location ^~ / {
        proxy_pass http://127.0.0.1:3000;
        proxy_http_version 1.1;
        proxy_set_header Host              $host;
        proxy_set_header X-Forwarded-Host  $http_host;   # $host drops the port
        proxy_set_header X-Forwarded-Proto $scheme;
        proxy_set_header X-Forwarded-For   $proxy_add_x_forwarded_for;
        proxy_set_header Upgrade           $http_upgrade;
        proxy_set_header Connection        $connection_upgrade;
        proxy_buffering off;      # SvelteKit streams responses
        proxy_read_timeout 3600s;
    }
}
```

Each of the worker-location settings is explained in
[Deploy Natively → nginx](/guides/deploy-natively/#7-nginx-dashboard-and-worker-api-on-one-hostname);
with the 60 s nginx defaults a worker built before keep-alives would re-register once a minute. Behind
Cloudflare, see [Deploy Natively → Behind Cloudflare](/guides/deploy-natively/#behind-cloudflare). Also useful:
`ADDRESS_HEADER=x-forwarded-for` if you want real client IPs, and `BODY_SIZE_LIMIT` (default
`512K`) if you ever import very large canvases.

Now open `https://guru.example.com/`, which redirects to `/auth`, and sign in with the account from
section 8. You should land on the canvas list with your email in the sidebar.

## 10. Get the master binaries from a GitHub release

The control plane runs from the image, but the master side is published as raw binaries as well.
Alongside the GHCR image, a `master-v*` tag publishes a GitHub release named `guru-master <version>`
with two assets, where `<version>` is the tag minus its `master-` prefix — so tag `master-v0.3.0`
publishes:

| Asset | What it is |
|---|---|
| `guru-master-v0.3.0-x86_64-unknown-linux-gnu` | the master binary the image runs, for a native deployment |
| `manage-tool-v0.3.0-x86_64-unknown-linux-gnu` | the operator CLI used in sections 7 and 8 |

Both are `x86_64-unknown-linux-gnu` only — glibc, the same ABI as the `distroless/cc` image the
master ships in — so a current Debian/Ubuntu/RHEL runs them as they are. There is no musl build on
the master side.

:::caution[Only tags pushed after the workflow existed]
The *Release Master* workflow is newer than the first master tags: every `master-v*` tag before it
(`master-v0.0.1-alpha`, `master-v0.1.0-alpha`, `master-v0.2.0-beta` at the time of writing) has an
image but no release assets. Check the release before you script around it — the `jq` snippet in
section 11 lists assets per tag, and `startswith("master-")` there works just as well — and use a
newer tag, or build `manage-tool` from your checkout (section 8), if the one you pinned has none.
:::

```sh
VERSION=v0.3.0                       # a release that actually lists the assets
gh release download "master-${VERSION}" \
  --repo haruki-nikaidou/proxy-guru \
  --pattern 'manage-tool-*-x86_64-unknown-linux-gnu' \
  --output manage-tool
chmod +x manage-tool
./manage-tool --help
```

A downloaded `manage-tool` is interchangeable with `./target/release/manage-tool` in every command
on this page: same flags, same subcommands, only the path differs.

The schema needs no separate artefact: the migrations are compiled into both binaries, so
`manage-tool db migrate` from a downloaded binary applies exactly what the matching master would.

## 11. Get the worker binary from a GitHub release

The data plane ships as a raw binary, not an image, and only `linux/x86_64` is published. Each
`worker-<version>` release carries **two** assets, where `<version>` is the tag minus its `worker-`
prefix — so tag `worker-v0.1.0` publishes:

| Asset | Links against | Use it on |
|---|---|---|
| `guru-worker-v0.1.0-x86_64-unknown-linux-gnu` | glibc, dynamically | a glibc host: current Debian/Ubuntu/RHEL |
| `guru-worker-v0.1.0-x86_64-unknown-linux-musl` | musl, statically — static-pie | Alpine and any other musl distribution |

The musl asset is a static position-independent executable: musl libc is linked into the binary, so
it has no runtime libc dependency and needs none installed on the host. That is why it runs on a
bare Alpine — or in an image with nothing else in it — where the gnu build would die on a missing
`ld-linux-x86-64.so`. Pick `-gnu` on a glibc host and `-musl` on a musl host; if you distribute one
binary to a mixed fleet, the musl build is the safe default.

Pick a release that actually lists those assets, and check before you script anything around it:

```sh
curl -fsSL https://api.github.com/repos/haruki-nikaidou/proxy-guru/releases \
  | jq -r '.[] | select(.tag_name | startswith("worker-"))
           | .tag_name + " -> " + ((.assets | map(.name)) | join(", "))'
```

:::caution[Not every tag has a binary]
A `worker-v*` tag whose release lists no assets was tagged before the release workflow existed (as
of writing, `worker-v0.0.1-alpha` is in exactly that state: `assets: []`). There is nothing to
download from such a tag — use a newer release, or push a fresh `worker-v*` tag so the *Release
Worker* workflow builds and attaches both binaries.
:::

With the GitHub CLI. Set `TARGET` once — each pattern below ends with the full triple, so it selects
exactly one asset:

```sh
VERSION=v0.1.0
TARGET=x86_64-unknown-linux-musl     # or x86_64-unknown-linux-gnu on a glibc host
gh release download "worker-${VERSION}" \
  --repo haruki-nikaidou/proxy-guru \
  --pattern "guru-worker-*-${TARGET}" \
  --output guru-worker
```

Or with plain `curl` — resolve the asset through the API so you never hard-code a URL, and match on
the same `TARGET`:

```sh
VERSION=v0.1.0
TARGET=x86_64-unknown-linux-musl     # or x86_64-unknown-linux-gnu on a glibc host
url=$(curl -fsSL \
  "https://api.github.com/repos/haruki-nikaidou/proxy-guru/releases/tags/worker-${VERSION}" \
  | jq -r --arg t "$TARGET" '.assets[] | select(.name | endswith($t)) | .browser_download_url')
curl -fsSL "$url" -o guru-worker
```

Then make it executable and confirm it runs — the binary prints its own version, which is also what
`manage-tool agent publish` records:

```sh
chmod +x guru-worker
./guru-worker --version
# guru-worker 0.4.0-beta
```

Keep the binaries in your own artifact store (an internal HTTP server, an apt/OCI registry, your
config-management system) keyed by version. There is no `latest` alias and no published checksum
file, so record the version — and ideally your own `sha256sum` — alongside the copy you distribute.

The gnu asset is dynamically linked against the glibc of the GitHub runner (`ubuntu-latest`), so a
long-lived distribution can be too old to load it — which is the one case where the musl asset is
the better answer even on a glibc host.

Installing and registering a worker node against this control plane is the next guide,
[Install and Update Agents](/guides/agent-install/): publish the binary you fetched here with
`manage-tool agent publish`, and the dashboard renders a one-line install command per server and
pushes updates from the same panel. A node that should run without a control plane at all is a
different guide: [Independent Worker Deployment](/guides/independent-worker/).

## 12. Verify the deployment

Work through these in order — each one fails loudly and independently:

```sh
# 1. Datastores
docker compose ps                     # postgres + rabbitmq healthy, redis up

# 2. Schema
./manage-tool db migrate              # from section 6
#   → schema is up to date

# 3. Control plane: one banner per mode, and no restart loop
docker compose logs --tail=20 master-dashboard master-workers master-consumer master-cron

# 4. Worker API through nginx: HTTP/2 200 with a grpc-status header means the
#    master answered (13 = "Missing request message", which an empty probe earns)
curl --http2 -sS -D - -o /dev/null -X POST -H 'content-type: application/grpc' \
  --data-binary '' https://guru.example.com/guru.orchestration.agent.WorkerAgent/Register

# 5. Dashboard through the proxy (303 to /auth)
curl -s -o /dev/null -w '%{http_code}\n' https://guru.example.com/

# 6. Live bus: one line per dashboard replica, printed at startup and after every
#    Redis reconnect. It is what the `Watch*` streams of the operator API — and
#    so the dashboard's live canvas and health pages — are served from.
docker compose logs master-dashboard | grep 'live bus connected'

# 7. Log in with the admin account — this is the only check that exercises
#    dashboard → operator API → database end to end.
```

If step 7 fails with `Forbidden` while steps 1–5 pass, re-read the proxy warning in section 9.

## 13. Upgrades, backups, rollback

**Upgrading.** Bump `MASTER_VERSION` (and `FRONTEND_VERSION`, if the dashboard also has a new tag)
in `.env`, then `docker compose pull && docker compose up -d`. The new masters apply any pending
migrations themselves as they start.

Rolling back means pinning the version variables to the previous tags — but a migration is not
undone by that, so a release whose migration drops or renames something the old binary reads cannot
be rolled back this way. The release notes say when that is the case.

**Backups.** PostgreSQL is the only irreplaceable state:

```sh
docker compose exec -T postgres pg_dump -U "$POSTGRES_USER" "$POSTGRES_DB" \
  > guru-$(date +%F).sql
```

Snapshot the `postgres-data` volume too if you want a fast restore path. RabbitMQ needs no backup:
its queues hold edit hints and execution signals, both of which the scheduler re-publishes and the
generation counters make idempotent — but the broker has to be *running*, because no periodic job
happens while it is not. Redis needs no backup either, and for a stronger reason: it is configured
without AOF and without RDB, so there is nothing in it to save. Replace the container and the
masters resubscribe.

**Logs.** Everything is structured `tracing` output on stdout, with `GURU_LOG_LEVEL` taking a full
`EnvFilter` string (`info`, `warn`, `guru_master=debug,orchestration=debug`, …). Ship it with your
usual Docker log driver.

## 14. Troubleshooting

| Symptom | Cause |
|---|---|
| Dashboard login returns `Forbidden` / `Cross-site remote requests are forbidden` | Reconstructed origin ≠ browser `Origin`. Serve over HTTPS, or set `PROTOCOL_HEADER`/`HOST_HEADER` and forward `X-Forwarded-Proto` and `X-Forwarded-Host` (with the port). `ORIGIN` has no effect. |
| Login succeeds, next request bounces back to `/auth` | The `Secure` session cookie was dropped — the browser reached the dashboard over plain HTTP. |
| Master exits with `this mode opens the database: set GURU_DATABASE_URL` | The URL is unset or empty; it has no default. `cron` is the one mode that does not need it. |
| Master exits with `master key: GURU_MASTER_KEY is not set` (or `must be 32 bytes`) | `dashboard_grpc`, `workers_grpc` and `consumer` need the key (`cron` does not read it). Generate one with `manage-tool generate-master-key`; it is read from the environment only. |
| Master exits with `stored config for key ... does not match its type` | The stored document is corrupt or predates a renamed field. Inspect it with `manage-tool config get <key>` and rewrite it with `config set`. |
| A TLS client pod stays in `invalid_pods` with `certificate for … is pending` / `failed: …` | The ACME pass has not issued it yet, or the last attempt failed (`ListCertificates` shows `last_error`). It runs in `consumer`, on the `renew_certificates` signal: check that a `consumer` is up, that the DNS provider token and `domain_id` (Cloudflare zone id / Vercel domain) are right, and that the consumer reaches the ACME directory. `RetryCertificate` forces a retry. |
| A relay pod stays in `invalid_pods` with `internal CA not initialised` | Run `manage-tool orchestration init-ca` once. |
| Master exits immediately with an AMQP error | `AMQP_URI` unset or unreachable. All five modes require the broker. Check the trailing `/` on the URI. |
| Master exits immediately with `Redis is required: set REDIS_URL (or pass --redis-url), for example redis://127.0.0.1:6379/` | `REDIS_URL` is unset, or the server is unreachable. `dashboard_grpc`, `workers_grpc` and `consumer` all require it; `cron` and `notifier` do not. |
| An open canvas or health page stops updating (a reload shows the change), or a `Watch*` stream stops delivering snapshots | Redis is down, or unreachable from the `dashboard_grpc` replica serving that stream — look for `live bus connected` in its log. Edits still apply and still derive; only the live delivery stops, and it resumes on reconnect. |
| `consumer`, `notifier` or `cron` restarts periodically | Expected on broker loss: the client does not reconnect, so the process exits and the restart policy brings it back. Investigate the broker, not the master. |
| A second `notifier` exits with `another notifier already holds the advisory lock; run exactly one` | Expected: delivery is single-instance. Scale it to one replica, or stop the peer that holds the lock. |
| `relation "…" does not exist` right after a clean install | The migrations never ran: the role in `GURU_DATABASE_URL` may lack `CREATE` on the database. Run `manage-tool db migrate` and read its error. |
| `manage-tool` wrote to the wrong database | A `.env` in the working directory supplied `GURU_DATABASE_URL`. Always pass `--database-url`. |
| Canvas edits never reach a worker | `consumer` is down: it runs both the edit hook and the stale-canvas sweep, so nothing derives without it. If `consumer` is up, check `cron` — without the clock the sweep never fires and only edits with a live `CanvasDirty` derive. |
| Periodic jobs stop happening (nothing goes `Offline`, no renewals) | RabbitMQ is down, or `cron` is. Both are required: the clock publishes the signals, the consumer runs them. |

See [Configuration](/reference/configuration/) for every flag and variable, and
[Rollout](/reference/rollout/) for what "derivation" actually does.
