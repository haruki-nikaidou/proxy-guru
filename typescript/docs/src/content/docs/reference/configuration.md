---
title: Configuration
description: Every flag and environment variable of guru-master, guru-worker, manage-tool and the dashboard.
---

Each binary takes the same value from a CLI flag or an environment variable; the flag wins.

## `guru-master`

| Flag | Environment | Default |
|---|---|---|
| `--mode` | `GURU_WORKER_MODE` | `dashboard_grpc` |
| `--dashboard-addr` | `GURU_DASHBOARD_GRPC_ADDR` | `0.0.0.0:50051` |
| `--workers-addr` | `GURU_WORKERS_GRPC_ADDR` | `0.0.0.0:50052` |
| `--database-url` | `GURU_DATABASE_URL` | *required in every mode but `cron`* |
| `--db-pool-size` | `GURU_DB_POOL_SIZE` | `10` (must be ≥ 1) |
| `--db-statement-timeout-ms` | `GURU_DB_STATEMENT_TIMEOUT_MS` | `5000` (must be ≥ 1) |
| `--amqp-uri` | `AMQP_URI` | *required in every mode* |
| `--redis-url` | `REDIS_URL` | *required in `dashboard_grpc`, `workers_grpc` and `consumer`* |
| `--watch-poll-ms` | `GURU_WATCH_POLL_MS` | `1000` (must be ≥ 1) |
| `--log-level` | `GURU_LOG_LEVEL` | `info` |
| — | `GURU_MASTER_KEY` | *required in `dashboard_grpc`, `workers_grpc` and `consumer`* (environment only; 32 random bytes, base64 — `manage-tool generate-master-key`) |
| — | `GURU_SMTP_PASSWORD` | *optional, `notifier` only* (environment only; unset sends unauthenticated) |
| — | `GURU_TELEGRAM_BOT_TOKEN` | *optional, `notifier` only* (environment only; unset disables Telegram) |

Everything else an operator can tune — health thresholds and retention, the default ACME directory,
the renewal window, how often each periodic job runs — lives in the database, not in the
environment. See [Module configuration](#module-configuration).

`--mode` accepts `dashboard_grpc`, `workers_grpc`, `consumer`, `notifier` and `cron`. A broker URI
looks like `amqp://guru:guru@127.0.0.1:5672/`, where the trailing `/` selects the default vhost. The
broker is required in **every** mode, `cron` included: periodic work is published as a message, so a
broker outage stalls derivation, liveness and certificate renewal until the broker returns.

`notifier` is the delivery side of [Notifications](/features/notifications/): exactly one instance,
enforced with a PostgreSQL advisory lock. It reads the `notify` config at startup and takes only the
database and the broker — no `GURU_MASTER_KEY` (it decrypts nothing) and no `REDIS_URL` (it
publishes no live events). Its two channel secrets are the environment variables above.

Redis is required in the three modes that serve or derive, and for the same kind of
reason: it is the live bus behind the operator API's `Watch*` streams. A URL looks like
`redis://127.0.0.1:6379/`. Every mutation publishes one event on the channel
`guru:orchestration:live`, and every `dashboard_grpc` replica subscribes to it once, so an edit made
on one replica reaches the streams served by the others. Nothing is stored: the fleet needs no
persistence, no AOF and no RDB from this server. A Redis outage costs open `Watch*` streams their
deliveries — the subscriber reconnects with backoff and asks every watcher to re-read the database,
so nothing stays stale once it returns — but it never affects derivation or what a worker runs.

The dashboard follows these streams: the canvas editor, the health page and its pod events update in
place, over one server-sent-events connection per page that the dashboard bridges to the gRPC
streams. The *Live* badge on those pages shows that browser connection; the log line
`live bus connected` is how you check the bus itself.

`GURU_MASTER_KEY` encrypts every secret at rest — DNS provider API tokens, ACME account keys,
certificate and CA private keys — and is required in the three modes that read one:
`dashboard_grpc`, `workers_grpc` and `consumer`. `cron` never touches a secret and never reads the
key, and neither does `notifier`. It is deliberately not a flag: argv is visible in process
listings. Losing the key means re-entering every DNS provider token and re-issuing every
certificate; changing it is not supported in place.

`GURU_DATABASE_URL` is required only in the four modes that open a connection. `cron` ignores it
and starts without a database URL at all: a clock that refused to start without one would carry a
database dependency, just an unused one. In the modes that do open it, the pool holds up to
`GURU_DB_POOL_SIZE` connections and every statement is bounded by the server at
`GURU_DB_STATEMENT_TIMEOUT_MS` — a statement past it is cancelled by PostgreSQL, which the edge
reports as `UNAVAILABLE` rather than as a fault. Each serving master also applies whatever
migrations are pending when it starts, under an advisory lock, so a fleet starting together applies
each one exactly once.

### Scheduling versus executing

`cron` is a clock and nothing else. It scans every 5 s, publishes one execution signal per due job,
and opens no database connection. `consumer` binds one durable queue per signal and runs the pass,
next to the `CanvasDirty` derivation hook — so periodic work scales, retries and fails over exactly
like an edit does, and a job that hangs cannot stop the clock.

| Routing key | Queue | Published every | Executed at most every | The pass |
|---|---|---|---|---|
| `derive_stale_canvases` | `guru_orchestration_derive_stale_canvases` | 30 s | `sweep_interval_secs` (30) | Re-derives every canvas whose `generation` ran ahead of its `derived_generation` |
| `sweep_liveness` | `guru_orchestration_sweep_liveness` | 30 s | `liveness_interval_secs` (30) | Marks a server `Offline` when it has not reported for `health_report_interval_secs × health_offline_after_intervals` |
| `trim_health_history` | `guru_orchestration_trim_health_history` | 300 s | `health_retention_interval_secs` (300) | Deletes `server_health_record` / `pod_health_record` rows older than `server_health_ttl_secs` / `pod_health_ttl_secs` |
| `renew_certificates` | `guru_orchestration_renew_certificates` | 60 s | `acme_interval_secs` (60) | ACME issuance and renewal: renews `acme_renew_before_secs` before expiry, retries a failed attempt after `acme_retry_after_secs` |
| `rotate_relay_certificates` | `guru_orchestration_rotate_relay_certificates` | 3600 s | `relay_rotation_interval_secs` (3600) | Re-issues relay leaves within `relay_cert_renew_before_secs` of expiry and re-derives their canvases |
| `resolve_server_countries` | `guru_orchestration_resolve_server_countries` | 60 s | `country_lookup_interval_secs` (60) | Looks up the country of every server IPv4 address that has none through `country_lookup_url`: a new or changed address at once, a failed lookup again after `country_lookup_retry_after_secs` |

Two layers, and they are not the same number. The scheduler publishes on the fixed cadence in the
middle column because it reads no configuration; the consumer claims each run — one
`orchestration_job_run` row per job, compare-and-set, at most once per configured interval
fleet-wide — before doing anything. The interval is measured between the *scheduling ticks* the
signals carry, not between the moments a consumer got round to them, so a busy consumer does not
silently stretch a cadence. An interval at or below the signal's cadence therefore means "every
signal", a larger one slows the job down across the whole fleet, and a duplicate or replayed
delivery is refused rather than run twice, because a tick that already ran can never be claimed
again. The intervals are values on the stored `orchestration` key, so a change takes effect when
the consumers restart, like everything else on that key.

A server is `Degraded` while it lags its desired revision for longer than `degraded_grace_secs` or
while the last acknowledged revision failed for any pod. The named values are keys of the stored
`orchestration` config, not flags.

## `guru-worker`

| Flag | Environment | Default |
|---|---|---|
| `-c`, `--config` | `GURU_WORKER_CONFIG` | — (standalone mode; reloaded on `SIGHUP`) |
| `--master` | `GURU_MASTER` | — (agent mode; requires `--server`; `http://host:50052` is plaintext h2c, `https://host` is TLS verified against the system roots) |
| `--server` | `GURU_SERVER_ID` | — (`orchestration_server` record key) |
| `--api-key-file` | `GURU_API_KEY_FILE` | — (alternative to `GURU_API_KEY`) |
| `--state-dir` | `GURU_STATE_DIR` | `/var/lib/guru-worker` |
| `--health-interval` | `GURU_HEALTH_INTERVAL_SECS` | `15` (seconds between health reports; agent mode; must be ≥ 1) |
| `--public-ipv4-urls` | `GURU_PUBLIC_IPV4_URLS` | `https://checkip.amazonaws.com,https://api.ipify.org,https://ipv4.icanhazip.com` (agent mode; comma-separated providers answering with the caller's IPv4 as text, walked from a rotating start, 3 s each; re-checked every 60 s and reported on change; empty disables the lookup, interface addresses are still reported) |
| `--public-ipv6-urls` | `GURU_PUBLIC_IPV6_URLS` | `https://ipv6.icanhazip.com,https://api6.ipify.org,https://v6.ipinfo.io/ip` (the same for IPv6) |
| `--log-level` | `GURU_LOG_LEVEL` | `info` |
| `--no-self-update` | `GURU_NO_SELF_UPDATE` | off (agent mode; when set, an update the dashboard requests is refused and reported back with that reason instead of installed) |

`--config` and `--master` are mutually exclusive, and with neither the worker runs standalone
against the default path `/etc/guru-worker/config.toml`. `--log-level`/`GURU_LOG_LEVEL` configures
agent mode only, and only until the first config is applied (the saved last-good one at startup,
then each one the master sends): from then on the server's log level in the dashboard is in force,
switched without a restart. Standalone reads `log.level` from the config file instead (it never
looks at the flag). Agent mode reads the operator API key from `GURU_API_KEY`, or from the file given by
`--api-key-file` (trailing whitespace is trimmed); the key is used once per session to register with
the master. It is either an operator API key (`gk_…`, Maintainer or Admin — a machine credential
that can register any server and nothing else) or the server's own agent key (`gs_…`), which the
dashboard issues together with the install command and which registers that one server only. In
agent mode the worker also reports its version and architecture at registration and asks for
updates every `agent_update_poll_secs` — see [Install and Update Agents](/guides/agent-install/).

## Worker config file

The flags above are the process; this is the traffic. The file is TOML, modelled by
`lib/guru_worker_config`, and it is the *same* model in both modes: `guru-master` derives it from a
canvas and streams it, and standalone workers load it from disk. So a file that a standalone worker
accepts is also exactly what the control plane would have sent.

Top level:

| Key | Default | Value |
|---|---|---|
| `ipv6_resolve` | `"tolerated"` | `required`, `preferred`, `tolerated`, `forbidden` — family policy when a destination is a domain name |
| `log.level` | `"info"` | A `tracing` `EnvFilter` directive — `info`, `debug`, or something targeted like `guru_worker=debug,warn`. Applied at startup and again by every reload, without a restart; a config the master derives carries one of `trace`, `debug`, `info`, `warn`, `error` |
| `[keepalive]` | see below | Liveness probing on every data-plane connection |
| `[quic]` | see below | This worker's side of every QUIC relay link: congestion control, rates, windows |
| `[[forwarding]]` | `[]` | One listener each; a file with none is valid and does nothing |

In standalone mode `log.level` is what configures the process log: `--log-level`/`GURU_LOG_LEVEL`
applies to agent mode only, where it is replaced by the `log.level` of the first config applied.

`ipv6_resolve` is global and captured into every compiled target: `required`/`forbidden` make the
other family a resolution failure, `preferred`/`tolerated` pick a winner when both resolve and fall
back otherwise.

### `[keepalive]`

TCP cannot tell a quiet peer from one that vanished: a client that drops off the network without a
FIN or RST (a phone losing signal, a NAT entry expiring, a host powering off) leaves its connection
open on the worker forever, together with the whole pipe behind it. The worker therefore enables
`SO_KEEPALIVE` on every TCP socket it accepts or dials, so the kernel probes an idle connection and
fails it after a run of unanswered probes. A QUIC relay hop has the opposite need: without pings the
QUIC connection under a quiet stream times out, cutting a long connection that merely had nothing to
say. All values are whole seconds (or a count) and must be at least 1.

| Key | Default | Value |
|---|---|---|
| `tcp_idle_secs` | `60` | Idle time before the first probe (`TCP_KEEPIDLE`) |
| `tcp_interval_secs` | `10` | Time between probes once probing has started (`TCP_KEEPINTVL`) |
| `tcp_retries` | `3` | Unanswered probes after which the connection is failed (`TCP_KEEPCNT`) |
| `quic_ping_secs` | `15` | Ping cadence on an idle QUIC relay connection, both ends |
| `quic_idle_secs` | `60` | Time without any packet after which a QUIC relay connection is lost; must exceed `quic_ping_secs` |

None of this is an idle limit: a peer that answers the probes keeps its connection for as long as
it likes. Probes only start once a connection has been silent for the idle time, and each is a
single empty segment answered by the peer's kernel — every client speaks it, and the traffic also
refreshes NAT entries on the way.

The section is written out only when it differs from the defaults, and `guru-master` sends the
defaults: workers built before the section existed reject unknown keys, so an operator who tunes a
standalone file must run a worker that knows it.

### `[quic]`

This worker's side of every QUIC relay link it listens on or dials. quinn's defaults suit a web
client, not a relay: Cubic backs off on every loss, and a fixed 1.25 MB per-stream receive window
caps a stream at `window / RTT` whatever the sender does. A link that is told its rate gets
hysteria's *brutal* sender and windows sized for it.

| Key | Default | Value |
|---|---|---|
| `congestion` | `"cubic"` | `cubic` probes for bandwidth; `brutal` sends at exactly `send_mbps` whatever the path does, over-sending by the observed loss (a quarter at most) |
| `send_mbps` | `0` | Rate toward the peer, Mbit/s: brutal's fixed rate and the basis of the send window. `0` keeps quinn's defaults |
| `receive_mbps` | `0` | The rate the peer sends at, Mbit/s; the receive windows hold half a second of it. `0` keeps quinn's 1.25 MB per stream |
| `max_streams` | `0` | Streams a listener lets the peer keep open on one connection; `0` means 4096 |
| `stream_receive_window` | `0` | Per-stream receive window in bytes, instead of the one derived from `receive_mbps` |
| `receive_window` | `0` | Whole-connection receive window in bytes; `0` leaves it unlimited |
| `send_window` | `0` | Bytes this side may have unacknowledged per connection, instead of the one derived from `send_mbps` |

One QUIC connection carries every proxied connection of a link (a stream each), so a rate is the
link's total, not a per-connection budget: set it to what the path carries in that direction, more
only makes loss. A `[forwarding.quic]` table on a `quic` relay listener, or a `[forwarding.to.quic]`
table on a `quic` hop, replaces the section for that one link. That is how `guru-master` pairs the
two ends: every server carries its own numbers in `[quic]`, and where the peer's *down* rate is lower
than this side's *up* rate (or the other way round) the forwarding gets the lower one, so nobody sends
faster than the other end said it can receive. `brutal` without `send_mbps` is rejected, as is either
table on anything but a QUIC relay. Like `[keepalive]`, the section is omitted at its defaults.

### `[[forwarding]]`

| Key | Required | Value |
|---|---|---|
| `tag` | yes | Free-form name; it is what appears in logs, lint output and apply errors |
| `listen` | yes | `ip:port` — a literal address, never a hostname (`0.0.0.0:443`, `[::]:443`) |
| `receive_proxy_protocol` | no | `"v1"` or `"v2"` — expect a PROXY header in front of the client payload |
| `listen_as` | yes | How to accept: `"raw"`, or a `tls` / `relay` table (below) |
| `quic` | no | A `[quic]` table for this listener alone; only on a `quic` relay listener |
| `to` | yes | Where it goes: a `[forwarding.to]` table (below) |

Listeners are keyed by `(listen, transport)`, and transport is QUIC only for a `quic` relay
listener. Two entries may therefore share one `ip:port` if one is QUIC (UDP) and the other is
TCP — anything else is a `duplicate listener` error.

`receive_proxy_protocol` is a switch, not a strict version check: the header is auto-detected, so a
`"v1"` entry still accepts a v2 header. What it changes is whether a header is *expected at all*,
and therefore whether the client address the worker attributes to the connection (for logs, for
`ip_hash`, and for the header it writes onward) is the real client or your upstream proxy. It is
meaningful for `raw` and `tls` listeners only — a `relay` listener always reads one (see below).

### `listen_as`

```toml
listen_as = "raw"                     # plain TCP, payload untouched
```

```toml
[forwarding.listen_as.tls]            # terminate TLS here, forward plaintext
key = "/etc/guru-worker/tls/key.pem"
full_chain = "/etc/guru-worker/tls/fullchain.pem"
```

```toml
[forwarding.listen_as.relay]          # ingress from another guru worker
relay_type = "tcp"                    # "tcp" | "tls" | "quic"
# relay_type = "tls" and "quic" additionally need:
# key = "/etc/guru-worker/tls/key.pem"
# full_chain = "/etc/guru-worker/tls/fullchain.pem"
```

`key` and `full_chain` are PEM paths parsed when the config is applied, which is what makes
certificate renewal a reload rather than a restart. In standalone mode nothing provisions them. In
agent mode the master ships them with every revision (`ConfigRevision.files`) as paths relative to
`--state-dir` — `certs/acme/<certificate>/{full_chain,key}.pem` for a TLS client pod,
`certs/relay/<pod>/{full_chain,key}.pem` for a `tls`/`quic` relay listener and `certs/ca.pem` for
the internal CA — and the worker writes them (key files `0600`, each directory swapped atomically)
before applying. A relative path in the file is resolved against `--state-dir`.

The top-level `relay_ca = "certs/ca.pem"` names the CA certificate that `tls`/`quic` relay *dialers*
verify their peer against; unset means the system roots. The master sets it whenever the internal
CA exists (`manage-tool orchestration init-ca`), and relay leaves carry the SNI
`<pod-key>.relay.guru.internal` on both ends.

A `relay` listener is the receiving end of a `to.type = "relay"` hop, not a public entrypoint: it
**always** reads a PROXY header off the decoded stream (that is how the upstream hop passes the true
client address along), so `receive_proxy_protocol` is irrelevant there.

### `[forwarding.to]`

```toml
[forwarding.to]
type = "exit"                         # last hop: connect to a real backend
destination = "10.0.0.5:8080"         # ip:port or domain:port
send_proxy_protocol = "v2"            # optional: "v1" | "v2"
```

```toml
[forwarding.to]
type = "relay"                        # next guru hop
protocol = "tcp"                      # "tcp" | "tls" | "quic"
destination = "hop.example.com:9443"
sni = "hop.example.com"               # required for "tls" and "quic"

# [forwarding.to.quic]                # optional, "quic" only: this hop's side of
# congestion = "brutal"               # the link instead of the top-level [quic]
# send_mbps = 1000
```

```toml
[forwarding.to]
type = "load_balance"
strategy = "round_robin"              # "round_robin" | "random" | "ip_hash" | "fallback"

[[forwarding.to.members]]             # members are themselves `to` nodes …
type = "exit"
destination = "10.0.0.6:8080"

[[forwarding.to.members]]             # … so groups nest: add another `.members`
type = "load_balance"
strategy = "fallback"

[[forwarding.to.members.members]]
type = "exit"
destination = "backend.internal:8080"
```

`destination` is `host:port` in both forms; it parses as a literal socket address when it can
(bracket IPv6: `[2001:db8::1]:8080`) and as a domain name otherwise, in which case it is resolved
per connection under `ipv6_resolve`. `fallback` tries members in order until one connects; the other
strategies pick one. A relay hop always writes a PROXY v2 header to the next worker — only `exit`
has an optional `send_proxy_protocol`, because only there is the peer someone else's backend.

### Route tables

Instead of an inline tree, `to` may name one of the forwarding's own **groups** or **upstreams**.
Every next hop is an upstream, every choice between next hops a group, and a group lists its
members by id — so a failover over balances, or a balance over failovers, is a group whose members
are groups. This is the form the master sends to a worker that reports the `route_table`
capability; the inline tree remains the form every worker reads.

```toml
[[forwarding]]
tag = "web"
listen = "[::]:443"
listen_as = "raw"
to = "g"                                # the group or upstream traffic starts at

[[forwarding.group]]
id = "g"
failover = ["g.0", "u:backup"]          # the first member that is alive, in order

[[forwarding.group]]
id = "g.0"
balance = [{ to = "u:hk-1", weight = 2 }, { to = "u:hk-2" }]   # weight defaults to 1
sticky = "client_ip"                    # optional, balance only

[[forwarding.upstream]]
id = "u:hk-1"
relay = { protocol = "quic", destination = "203.0.113.1:40000", sni = "hk-1.relay.guru.internal", confirm = true }

[[forwarding.upstream]]
id = "u:hk-2"
relay = { protocol = "tcp", destination = "203.0.113.2:40000" }

[[forwarding.upstream]]
id = "u:backup"
exit = { destination = "backend.internal:8080", send_proxy_protocol = "v2" }
```

A balance spreads connections over the members that are alive in proportion to their weights —
smooth round robin, or, with `sticky = "client_ip"`, a weighted rendezvous hash of the client
address, so a client stays on its member while that member is alive. A failover uses the first
member that is alive. Both pass over members whose every hop is dead, and a failed attempt moves on
to the next choice within the same client connection. An upstream that fails three times in a row
is dead; after ten seconds one connection at a time may try it again.

`confirm = true` on a relay upstream asks the relay to answer once its *own* next hop connected, so
a dead exit behind a live relay fails the hop at the dialer and the choice moves on. Only set it
toward a worker that reports `relay_confirm`; the master does so on its own. A `quic` table on a
relay upstream is that hop's side of the link, as on a tree `relay`.

### What is rejected, and what is only warned about

Loading fails — the file is never partially applied — on any of:

| Error | Cause |
|---|---|
| `parse toml: TOML parse error at line N …` | Every table rejects unknown keys; a typo is an error, not a silent default |
| `duplicate listener <addr> (<tag>)` | Two entries claim the same `ip:port` on the same transport |
| `forwarding <tag> relay to tls/quic requires sni` | A `tls`/`quic` relay hop without `sni`, at any depth |
| `forwarding <tag> has an empty load-balance group` | `members = []`, at any depth |
| `invalid remote '…'` / `invalid port in remote '…'` | A `destination` that is not `host:port` |
| `forwarding <tag> refers to route id <id>, which it does not define` | `to` or a group member naming an id no group or upstream of the forwarding has |
| `forwarding <tag> defines route id <id> more than once` | Two groups or upstreams sharing an id |
| `forwarding <tag> has an empty group <id>` / `gives <id> a weight of zero` | A group without members, a zero weight |
| `forwarding <tag> has a group cycle through <id>` | Groups that contain each other |
| `forwarding <tag> has groups or upstreams but an inline to tree` | The two forms mixed in one forwarding |

These are logged as warnings and keep running:

- a load-balance group with exactly one member (the group is pointless);
- `ip_hash` anywhere under a listener that does not set `receive_proxy_protocol`. Take this warning
  literally only for `raw` and `tls` listeners, where every connection then hashes the address the
  worker sees — your upstream proxy's — and the "balance" collapses onto one member. A `relay`
  listener reads its mandatory relay header regardless, so it hashes the true client and the
  warning is a false positive there; the lint does not distinguish the two cases.

Applying a parsed config can still fail per entry — a missing cert file, an address already bound by
another process — reported as `<tag>: <reason>`. That is an apply error, not a config error, and it
is *per pod*: every other `[[forwarding]]` is committed, the failed one keeps whatever listener it
had before (or none). In standalone mode a failed startup aborts, and a failed `SIGHUP` reload logs
the failed entries and keeps their previous listeners; in agent mode the outcome of every pod is
acknowledged to the master, which records the failed pods on the server and a `Failed` health
event for each of them.

## `manage-tool`

One global flag: `--database-url` (`GURU_DATABASE_URL`), the same URL `guru-master` reads.

| Subcommand | Purpose |
|---|---|
| `create-admin --email <email> --password <password>` | Bootstrap the first administrator account |
| `generate-master-key` | Print a fresh `GURU_MASTER_KEY` (needs no database) |
| `db migrate` | Apply every pending schema migration; `guru-master` does the same at startup |
| `config seed` | Write the default values for every key that has none; leaves edited keys untouched |
| `config list` | Print every key with its stored document (or the defaults when it has none) |
| `config get <key>` | Print one key's stored document, undecoded — readable even when it is corrupt |
| `config set <key> <json>` | Replace one key's stored JSON (validated before it is written) |
| `orchestration export-config --server <key>` | Print the derived `guru-worker` TOML for one server |
| `orchestration init-ca` | Create the internal CA for relay TLS/QUIC links and print its certificate; refuses to replace an existing one (needs `GURU_MASTER_KEY`) |

## Dashboard

| Environment | Default | Purpose |
|---|---|---|
| `GURU_GRPC_URL` | `127.0.0.1:50051` | Dashboard gRPC endpoint of `guru-master --mode dashboard_grpc` |
| `PROTOCOL_HEADER` | — | Header carrying the public scheme, e.g. `x-forwarded-proto`. **Unset means the app assumes `https`** |
| `HOST_HEADER` | — | Header carrying the public host, e.g. `x-forwarded-host` (must include a non-default port) |
| `ADDRESS_HEADER` | — | Header carrying the client IP, e.g. `x-forwarded-for` |
| `BODY_SIZE_LIMIT` | `512K` | Maximum request body |

The server listens on `:3000`. Every request's own origin is reconstructed from those headers, and
a POST whose browser `Origin` does not match it is rejected with
`403 Cross-site remote requests are forbidden` — so behind a plain-HTTP or port-shifted proxy both
`PROTOCOL_HEADER` and `HOST_HEADER` are mandatory. `ORIGIN` is **not** read at run time: the Node
adapter bakes `kit.paths.origin` in at build time. The session cookie is issued with `Secure`, so
the dashboard must be served over HTTPS (except on `localhost`).

## Module configuration

Operator-tunable settings live in the database, one `app_config` row per key holding the whole
config as a JSON document. The database is the only source of truth — no cache, no second copy — so
every `guru-master` in a fleet runs identical settings with no matching environment, and a change
needs no redeploy, only a restart. Three keys exist today:

| Key | Struct | Contents |
|---|---|---|
| `auth` | `auth::config::AuthConfig` | `session_idle_ttl_secs` |
| `notify` | `notify::config::NotifyConfig` | `smtp_host` (default empty, which disables email), `smtp_port` (587), `smtp_starttls` (`true`; `false` speaks plain SMTP and is for a relay on localhost or a test sink only), `smtp_username` (empty sends unauthenticated), `smtp_from` (`guru <noreply@example.com>`), `telegram_api_base` (`https://api.telegram.org`), `delivery_attempts` (3), `delivery_retry_delay_secs` (5), `default_language` (`en`; one of `en`, `ja`, `zh_cn` — the language a settings row that never named one is rendered in). The SMTP password and the bot token are **not** here: they are `GURU_SMTP_PASSWORD` and `GURU_TELEGRAM_BOT_TOKEN` on the `notifier`, because this document is readable by every Admin |
| `orchestration` | `orchestration::config::OrchestrationConfig` | `health_report_interval_secs`, `health_offline_after_intervals`, `degraded_grace_secs`, `server_health_ttl_secs`, `pod_health_ttl_secs` (the stored `node_health_ttl_secs` of older documents is still read), `default_acme_directory`, `acme_renew_before_secs`, `acme_retry_after_secs`, `relay_cert_valid_secs`, `relay_cert_renew_before_secs`, `sweep_interval_secs`, `liveness_interval_secs`, `health_retention_interval_secs`, `acme_interval_secs`, `relay_rotation_interval_secs`, `stream_keepalive_secs` (default `15`: how often an idle `Watch*` stream sends an empty keep-alive and re-checks the session that opened it; keep it under the idle timeout of any proxy in front of `:50051`), `trust_proxy_address_headers` (default `true`: the worker API records `x-real-ip` / the first `x-forwarded-for` hop as the address a registration came from; turn off when `:50052` is reachable without the documented proxy, or a worker could spoof it), `agent_public_base_url` (default empty: the origin workers dial and the dashboard's install command downloads from, e.g. `https://guru.example.com`; until it is set the dashboard cannot render an install command), `agent_download_path` (default `/agent`: the path under that origin nginx serves `manage-tool agent publish`'s output from), `agent_update_poll_secs` (default 60: how often a live worker asks whether an update was requested for it), `country_lookup_url` (default `https://api.country.is/{ip}`: where the country of a server's IPv4 address is looked up for its flag, with `{ip}` replaced by the address; the answer may be a JSON object with a two-letter `country` field or just the two letters, so `https://get.geojs.io/v1/ip/country/{ip}` works as well; empty turns the lookup off), `country_lookup_interval_secs` (default 60), `country_lookup_retry_after_secs` (default 3600: how long a failed lookup waits before the same address is asked about again) |

Run `manage-tool config seed` after `manage-tool db migrate` to write the defaults, and
`manage-tool config list` to see what is stored. `list` and `get` print the row verbatim — they do
not decode it, so a document that fails a master's startup read is still inspectable. A `set`
replaces the whole document, but it is decoded into the config's type first, so a partial payload
is filled in from the defaults and a payload of the wrong shape is rejected before it reaches the
row:

```sh
manage-tool config set orchestration '{"acme_renew_before_secs":1209600}'
```

The six `*_interval_secs` fields are how often a periodic job may actually run
([Scheduling versus executing](#scheduling-versus-executing)). They live here rather than in the
environment because the whole fleet has to agree on them: the claim that enforces an interval is
one database row shared by every consumer.

```sh
manage-tool config set orchestration '{"acme_interval_secs":300,"relay_rotation_interval_secs":7200}'
```

All three documents are readable and replaceable from the dashboard: an **Admin** (and only an
Admin — no other role holds the permission, and an API key never does) gets the row as stored, the
payload seeding would write, and a form that replaces the whole document. It validates exactly as
`manage-tool config set` does, so a payload of the wrong shape is rejected and the row keeps its
previous contents. Saving from the dashboard is not a live reload either: **restart `guru-master`**
for the new values to take effect.

The four database-backed modes — `dashboard_grpc`, `workers_grpc`, `consumer` and `notifier` — read
the keys they need once, during startup, and hand the values to their services; there is no live
reload. `cron` opens no database connection and so reads none of them: it only publishes execution
signals, and the consumer that receives one applies the stored interval. An unseeded installation
runs the defaults.
A row that does not deserialize fails startup naming the key — substituting defaults for a corrupt
document would silently swap an operator's whole config, for instance moving ACME from staging to
the production directory.
Fields added in a later release are read with their default value, so an older row keeps working.
