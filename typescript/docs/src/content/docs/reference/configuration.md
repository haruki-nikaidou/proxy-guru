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
| `--address` | `SURREALDB_HOST` | `ws://127.0.0.1:8000` |
| `--username` | `SURREALDB_USER` | `root` |
| `--password` | `SURREALDB_PASSWORD` | `root` |
| `--namespace` | `SURREALDB_NAMESPACE` | *required* |
| `--database` | `SURREALDB_NAME` | *required* |
| `--amqp-uri` | `AMQP_URI` | *required in `dashboard_grpc`, `workers_grpc`, `consumer`* |
| `--sweep-interval-secs` | `GURU_SWEEP_INTERVAL_SECS` | `30` (must be ≥ 1) |
| `--watch-poll-ms` | `GURU_WATCH_POLL_MS` | `1000` (must be ≥ 1) |
| `--log-level` | `GURU_LOG_LEVEL` | `info` |
| — | `GURU_MASTER_KEY` | *required* (environment only; 32 random bytes, base64 — `manage-tool generate-master-key`) |
| `--acme-interval-secs` | `GURU_ACME_INTERVAL_SECS` | `60` (must be ≥ 1) |

Everything else an operator can tune — health thresholds and retention, the default ACME directory,
the renewal window — lives in the database, not in the environment. See
[Module configuration](#module-configuration).

`--mode` accepts `dashboard_grpc`, `workers_grpc`, `consumer` and `cron`. A broker URI looks like
`amqp://guru:guru@127.0.0.1:5672/`, where the trailing `/` selects the default vhost.

`GURU_MASTER_KEY` encrypts every secret at rest — DNS provider API tokens, ACME account keys,
certificate and CA private keys — and is required in every mode. It is deliberately not a flag:
argv is visible in process listings. Losing the key means re-entering every DNS provider token and
re-issuing every certificate; changing it is not supported in place.

The `cron` mode runs, besides the derivation sweep: the server liveness sweep (every 30 s — a server
that has not reported for `health_report_interval_secs × health_offline_after_intervals` is
`Offline`), health retention (every 5 min, deletes `server_health_record` / `node_health_record`
rows older than `server_health_ttl_secs` / `node_health_ttl_secs`), ACME issuance and renewal (every
`--acme-interval-secs`; renews `acme_renew_before_secs` before expiry, retries a failed attempt
after `acme_retry_after_secs`), and relay-leaf rotation (hourly). A server is `Degraded` while it
lags its desired revision for longer than `degraded_grace_secs` or while the last acknowledged
revision failed for any pod. The named values are keys of the stored `orchestration` config, not
flags.

## `guru-worker`

| Flag | Environment | Default |
|---|---|---|
| `-c`, `--config` | `GURU_WORKER_CONFIG` | — (standalone mode; reloaded on `SIGHUP`) |
| `--master` | `GURU_MASTER` | — (agent mode; requires `--server`) |
| `--server` | `GURU_SERVER_ID` | — (`orchestration_server` record key) |
| `--api-key-file` | `GURU_API_KEY_FILE` | — (alternative to `GURU_API_KEY`) |
| `--state-dir` | `GURU_STATE_DIR` | `/var/lib/guru-worker` |
| `--health-interval` | `GURU_HEALTH_INTERVAL_SECS` | `15` (seconds between health reports; agent mode; must be ≥ 1) |
| `--log-level` | `GURU_LOG_LEVEL` | `info` |

`--config` and `--master` are mutually exclusive, and with neither the worker runs standalone
against the default path `/etc/guru-worker/config.toml`. `--log-level`/`GURU_LOG_LEVEL` configures
agent mode only: standalone reads `log.level` from the config file instead (it never looks at the
flag). Agent mode reads the operator API key from `GURU_API_KEY`, or from the file given by
`--api-key-file` (trailing whitespace is trimmed); the key is used once per session to register with
the master.

## Worker config file

The flags above are the process; this is the traffic. The file is TOML, modelled by
`lib/guru_worker_config`, and it is the *same* model in both modes: `guru-master` derives it from a
canvas and streams it, and standalone workers load it from disk. So a file that a standalone worker
accepts is also exactly what the control plane would have sent.

Top level:

| Key | Default | Value |
|---|---|---|
| `ipv6_resolve` | `"tolerated"` | `required`, `preferred`, `tolerated`, `forbidden` — family policy when a destination is a domain name |
| `log.level` | `"info"` | A `tracing` `EnvFilter` directive — `info`, `debug`, or something targeted like `guru_worker=debug,warn`. Read **once at startup**, so a reload does not change it |
| `[[forwarding]]` | `[]` | One listener each; a file with none is valid and does nothing |

In standalone mode `log.level` is what configures the process log: `--log-level`/`GURU_LOG_LEVEL`
applies to agent mode only.

`ipv6_resolve` is global and captured into every compiled target: `required`/`forbidden` make the
other family a resolution failure, `preferred`/`tolerated` pick a winner when both resolve and fall
back otherwise.

### `[[forwarding]]`

| Key | Required | Value |
|---|---|---|
| `tag` | yes | Free-form name; it is what appears in logs, lint output and apply errors |
| `listen` | yes | `ip:port` — a literal address, never a hostname (`0.0.0.0:443`, `[::]:443`) |
| `receive_proxy_protocol` | no | `"v1"` or `"v2"` — expect a PROXY header in front of the client payload |
| `listen_as` | yes | How to accept: `"raw"`, or a `tls` / `relay` table (below) |
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
`--state-dir` — `certs/acme/<certificate>/{full_chain,key}.pem` for an Entry with TLS,
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

### What is rejected, and what is only warned about

Loading fails — the file is never partially applied — on any of:

| Error | Cause |
|---|---|
| `parse toml: TOML parse error at line N …` | Every table rejects unknown keys; a typo is an error, not a silent default |
| `duplicate listener <addr> (<tag>)` | Two entries claim the same `ip:port` on the same transport |
| `forwarding <tag> relay to tls/quic requires sni` | A `tls`/`quic` relay hop without `sni`, at any depth |
| `forwarding <tag> has an empty load-balance group` | `members = []`, at any depth |
| `invalid remote '…'` / `invalid port in remote '…'` | A `destination` that is not `host:port` |

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
acknowledged to the master, which records the failed pods on the server and the affected nodes.

## `manage-tool`

Global flags mirror `guru-master`'s database options: `--address` (`SURREALDB_HOST`), `--username`
(`SURREALDB_USER`), `--password` (`SURREALDB_PASSWORD`), `--namespace` (`SURREALDB_NAMESPACE`),
`--database` (`SURREALDB_NAME`).

| Subcommand | Purpose |
|---|---|
| `create-admin --email <email> --password <password>` | Bootstrap the first administrator account |
| `generate-master-key` | Print a fresh `GURU_MASTER_KEY` (needs no database) |
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
needs no redeploy, only a restart. Two keys exist today:

| Key | Struct | Contents |
|---|---|---|
| `auth` | `auth::config::AuthConfig` | `session_idle_ttl_secs` |
| `orchestration` | `orchestration::config::OrchestrationConfig` | `health_report_interval_secs`, `health_offline_after_intervals`, `degraded_grace_secs`, `server_health_ttl_secs`, `node_health_ttl_secs`, `default_acme_directory`, `acme_renew_before_secs`, `acme_retry_after_secs`, `relay_cert_valid_secs`, `relay_cert_renew_before_secs` |

Run `manage-tool config seed` after `surrealkit sync` to write the defaults, and
`manage-tool config list` to see what is stored. `list` and `get` print the row verbatim — they do
not decode it, so a document that fails a master's startup read is still inspectable. A `set`
replaces the whole document, but it is decoded into the config's type first, so a partial payload
is filled in from the defaults and a payload of the wrong shape is rejected before it reaches the
row:

```sh
manage-tool config set orchestration '{"acme_renew_before_secs":1209600}'
```

`guru-master` reads both keys once, during startup, and hands the values to its services; there is
no live reload. An unseeded installation runs the defaults. A row that does not deserialize fails
startup naming the key — substituting defaults for a corrupt document would silently swap an
operator's whole config, for instance moving ACME from staging to the production directory.
Fields added in a later release are read with their default value, so an older row keeps working.
