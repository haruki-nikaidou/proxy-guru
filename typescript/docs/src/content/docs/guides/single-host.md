---
title: Single-Host Native Deployment
description: Run the control plane and the dashboard straight from a checkout under systemd, keep only SurrealDB and RabbitMQ in Docker, and terminate TLS for workers with nginx on the same host.
---

The [Deployment](/guides/deployment/) guide runs everything from images. This guide is the other
common shape: **one machine that is both the build box and the control plane**. The Rust binaries
run from `target/release/` under systemd, the dashboard runs from its build output, and only the two
datastores live in Docker. A code change is `cargo build` plus a unit restart, with no image in
between.

It also covers the one thing the image guide leaves out: putting the worker API behind **TLS on the
same public hostname as the dashboard**, so workers anywhere on the internet can reach it with
`--master https://guru.example.com` and no VPN.

Replace `guru.example.com` throughout with your hostname. Nothing else is host-specific.

## 1. Layout

| Piece | Runs as | Listens on |
|---|---|---|
| SurrealDB 3.2 | Docker Compose (`docker-compose.yml` in the checkout) | `127.0.0.1:8000` |
| RabbitMQ 4 | Docker Compose | `127.0.0.1:5672` |
| `guru-master` ×4 | systemd template `guru-master@<mode>` | `127.0.0.1:50051`, `127.0.0.1:50052` |
| dashboard | systemd `guru-frontend` | `127.0.0.1:3100` |
| nginx | your existing nginx, one server block | `:443` |

Everything but nginx binds loopback. The only public surface is the TLS server block, which serves
the dashboard on `/` and forwards gRPC on `/guru.orchestration.agent.WorkerAgent/` to `:50052`.

All secrets live in one file, the repository root `.env` (it is gitignored). Compose interpolates
`${…}` from it because it sits next to `docker-compose.yml`; systemd reads it verbatim through
`EnvironmentFile=`. systemd does **not** expand `${…}`, so `AMQP_URI` has to be written out, and hex
secrets avoid every quoting and URL-encoding question:

```sh
# /path/to/proxy-guru/.env  (chmod 600)
SURREALDB_HOST=ws://127.0.0.1:8000
SURREALDB_USER=root
SURREALDB_PASSWORD=<openssl rand -hex 32>
SURREALDB_NAMESPACE=guru
SURREALDB_NAME=guru
RABBIT_USER=guru
RABBIT_PASSWORD=<openssl rand -hex 32>
AMQP_URI=amqp://guru:<RABBIT_PASSWORD>@127.0.0.1:5672/
GURU_MASTER_KEY=<manage-tool generate-master-key>
GURU_DASHBOARD_GRPC_ADDR=127.0.0.1:50051
GURU_WORKERS_GRPC_ADDR=127.0.0.1:50052
GURU_LOG_LEVEL=info
```

`GURU_WORKERS_GRPC_ADDR` is loopback on purpose: nginx is the only thing that should reach the
plaintext worker API.

## 2. Datastores

The repository ships a `docker-compose.yml` for exactly this layout: SurrealDB on RocksDB under
`./data/db`, RabbitMQ under `./data/mq` (both gitignored), both published on loopback only.

```sh
cd /path/to/proxy-guru
docker compose up -d
docker compose ps          # wait for rabbitmq to report healthy
```

Two details in that file are load-bearing. The SurrealDB image runs as `nonroot` and cannot write a
root-owned bind mount, so the service sets `user: root`. RabbitMQ names its data directory after the
node name, `rabbit@<hostname>`, so the service pins `hostname:`; without it every container
recreation would start an empty broker.

## 3. Build and schema

```sh
cargo build --release --locked -p guru-master -p manage-tool -p guru-worker
```

Apply the schema with `surrealkit` exactly as in [Deployment §6](/guides/deployment/#6-apply-the-schema-with-surrealkit),
then seed the module configuration:

```sh
(set -a; . ./.env; set +a; ./target/release/manage-tool config seed)
```

The rollout manifests and snapshots `surrealkit` writes under `database/rollouts/` and
`database/snapshots/` are per-environment and gitignored here; with a single machine there is nothing
to share them with.

## 4. The masters under systemd

One template unit, one instance per mode. It runs the binary straight out of the checkout and reads
the repository `.env`. Because binary, `.env` and data all live inside the checkout, the unit runs as
the checkout's owner: `DynamicUser=`/`ProtectHome=` would hide them.

```ini
# /etc/systemd/system/guru-master@.service
[Unit]
Description=guru-master (%i)
After=network-online.target docker.service
Wants=network-online.target
# Never rate-limit Restart=always: the datastores may come up later than we do.
StartLimitIntervalSec=0

[Service]
Type=simple
WorkingDirectory=/path/to/proxy-guru
EnvironmentFile=/path/to/proxy-guru/.env
Environment=GURU_WORKER_MODE=%i
ExecStart=/path/to/proxy-guru/target/release/guru-master
Restart=always
RestartSec=3s
TimeoutStopSec=30s
NoNewPrivileges=true
PrivateTmp=true
LimitNOFILE=65536

[Install]
WantedBy=multi-user.target
```

```sh
systemctl daemon-reload
systemctl enable --now guru-master@dashboard_grpc guru-master@workers_grpc \
                       guru-master@consumer guru-master@cron
journalctl -u 'guru-master@*' -n 20
```

`consumer` and `cron` exit non-zero when the broker connection drops; `Restart=always` is what heals
them, so keep it. Then bootstrap, with the same environment the masters see:

```sh
(set -a; . ./.env; set +a; ./target/release/manage-tool orchestration init-ca)
(set -a; . ./.env; set +a; ./target/release/manage-tool create-admin \
    --email admin@example.com --password '<strong password>')
```

Redeploying after a code change is one line:

```sh
cargo build --release --locked -p guru-master -p manage-tool -p guru-worker && \
  systemctl restart 'guru-master@*'
```

## 5. The dashboard under systemd

Build it with Bun, forcing Bun for every subprocess — SvelteKit's post-build step needs
`Promise.withResolvers`, which Node 20 lacks — and run the adapter-node output with Bun as well:

```sh
bun install --frozen-lockfile --filter guru-frontend
cd typescript/guru-frontend && NODE_ENV=production bun --bun run build
```

```ini
# /etc/systemd/system/guru-frontend.service
[Unit]
Description=guru dashboard (SvelteKit adapter-node)
After=network-online.target guru-master@dashboard_grpc.service
Wants=network-online.target
StartLimitIntervalSec=0

[Service]
Type=simple
WorkingDirectory=/path/to/proxy-guru/typescript/guru-frontend
# No EnvironmentFile: the dashboard must not inherit the database credentials.
Environment=NODE_ENV=production
Environment=HOST=127.0.0.1
Environment=PORT=3100
Environment=GURU_GRPC_URL=127.0.0.1:50051
Environment=PROTOCOL_HEADER=x-forwarded-proto
Environment=HOST_HEADER=x-forwarded-host
Environment=ADDRESS_HEADER=x-forwarded-for
ExecStart=/root/.bun/bin/bun build/index.js
Restart=always
RestartSec=3s
NoNewPrivileges=true
PrivateTmp=true

[Install]
WantedBy=multi-user.target
```

```sh
systemctl enable --now guru-frontend
curl -s -o /dev/null -w '%{http_code}\n' http://127.0.0.1:3100/   # 303 → /auth
```

Rebuild and `systemctl restart guru-frontend` after a frontend change.

## 6. nginx: dashboard and worker API on one hostname

gRPC has no notion of a path prefix: every call is `POST /<package>.<Service>/<Method>`, and the
worker's service is `guru.orchestration.agent.WorkerAgent`. So nginx can share one server block
between the dashboard and the worker API by routing on that prefix. Longest prefix wins, so the gRPC
location never falls into the dashboard one.

nginx needs `http_v2_module` and `http_ssl_module` (any current build has both) and an `http`-level
`map` for WebSocket upgrades:

```nginx
map $http_upgrade $connection_upgrade {
    default upgrade;
    ''      close;
}
```

The server block, with the certificate you already have for the hostname:

```nginx
server {
    listen 443 ssl http2;
    listen [::]:443 ssl http2;
    server_name guru.example.com;

    ssl_certificate     /etc/letsencrypt/live/guru.example.com/fullchain.pem;
    ssl_certificate_key /etc/letsencrypt/live/guru.example.com/privkey.pem;

    # ---- worker agent API: TLS terminated here, plaintext h2c to the master.
    location ^~ /guru.orchestration.agent.WorkerAgent/ {
        grpc_pass grpc://127.0.0.1:50052;
        grpc_connect_timeout 5s;
        # WatchConfig is a server stream that may be silent for hours; ReportHealth a
        # client stream carrying one message per 15 s. The grpc_* defaults are 60 s.
        grpc_read_timeout 7d;
        grpc_send_timeout 7d;
        grpc_socket_keepalive on;
        # ReportHealth is one request body that grows for the life of the session;
        # nginx counts it cumulatively, so any limit would eventually 413 the stream.
        client_max_body_size 0;
        # Gap allowed between two ReportHealth frames (4 missed reports). With nginx
        # in the path this is what bounds dead-worker detection.
        client_body_timeout 60s;
        grpc_set_header x-api-key     $http_x_api_key;
        grpc_set_header x-refresh-key $http_x_refresh_key;
        grpc_set_header X-Real-IP     $remote_addr;
    }

    # ---- dashboard. `^~` keeps any regex locations in the same block (static
    # asset caching rules, PHP handlers) from capturing /_app/immutable/*.
    location ^~ / {
        proxy_pass http://127.0.0.1:3100;
        proxy_http_version 1.1;
        proxy_set_header Host              $host;
        proxy_set_header X-Forwarded-Host  $http_host;   # $host drops the port
        proxy_set_header X-Forwarded-Proto $scheme;
        proxy_set_header X-Forwarded-For   $proxy_add_x_forwarded_for;
        proxy_set_header Upgrade           $http_upgrade;
        proxy_set_header Connection        $connection_upgrade;
        proxy_cache off;          # in case a global proxy_cache is configured
        proxy_buffering off;      # SvelteKit streams responses
        proxy_redirect off;
        proxy_connect_timeout 5s;
        proxy_read_timeout 3600s;
        proxy_send_timeout 3600s;
    }
}
```

```sh
nginx -t && nginx -s reload
```

Three of those settings deserve a sentence each:

- **`grpc_read_timeout 7d`.** With the 60 s default, nginx ends `WatchConfig` every minute, the
  worker re-registers, and its refresh key rotates every minute — a self-inflicted
  re-registration storm.
- **`client_max_body_size 0`.** `ReportHealth` is a client-streaming RPC: one HTTP/2 request whose
  body grows by one message per report for as long as the session lives. nginx enforces the body
  limit cumulatively, so a limit would reset the health stream after a few weeks.
- **`client_body_timeout 60s`.** The master detects a dead worker through HTTP/2 PINGs on its own
  connection. With nginx in between those PINGs are answered by nginx, so a worker that vanished
  without a FIN is noticed when nginx times out the health body instead — about a minute rather than
  twenty seconds. Acceptable; this constant is where to tune it.

Only the `Origin`/forwarded headers on the dashboard location are mandatory for correctness (see the
[dashboard warning](/guides/deployment/#9-run-the-dashboard)); the rest is what keeps long-lived
streams alive.

:::caution[Keep the hostname off Cloudflare's proxy]
Cloudflare's proxy closes a streaming response after 100 s without data. `WatchConfig` is silent
between revisions, so behind the orange cloud every worker would reconnect every couple of minutes.
Leave the record DNS-only; TLS from the worker to nginx already protects the API key in transit.
:::

## 7. Verify

```sh
# gRPC reaches the master through TLS: expect HTTP/2 200 with a grpc-status header
curl --http2 -sS -D - -o /dev/null -X POST -H 'content-type: application/grpc' \
  --data-binary '' https://guru.example.com/guru.orchestration.agent.WorkerAgent/Register

# a worker with a bogus key must be rejected by the master, not by TLS or nginx
GURU_API_KEY=bogus ./target/release/guru-worker --master https://guru.example.com \
  --server probe --state-dir /tmp/gw
#   ERROR guru_worker::agent: agent session ended error=... "Missing identity"
```

Then create a server and an API key in the dashboard and start a real worker with
`--master https://guru.example.com`. Watch it for longer than three minutes — that is what proves the
60 s nginx defaults are overridden — and push a canvas change to see `applied config revision` in
its log.

The `https://` form needs a worker built from a checkout that includes TLS support in the agent
(`bin/guru-worker` with tonic's `tls-ring` + `tls-native-roots`); it verifies the certificate against
the host's `ca-certificates` and sends its own HTTP/2 keepalive pings. `http://host:50052` on a
private network works as before.
