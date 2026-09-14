---
title: Independent Worker Deployment
description: Run guru-worker standalone from a TOML file — no master, no database, no API key — and reload it with SIGHUP.
---

`guru-worker` does not need a control plane. Pointed at a TOML file it is a self-contained TCP/TLS
proxy: it binds the listeners the file describes, forwards traffic, and re-reads the file on
`SIGHUP`. This guide deploys exactly that — one binary, one config file, one systemd unit.

This is a supported mode, not a degraded one. The branch is a single `match` on `--master`: without
it the worker runs [`run_standalone`](https://github.com/haruki-nikaidou/proxy-guru/blob/main/bin/guru-worker/src/lib.rs)
and never constructs a gRPC client, never reads `GURU_API_KEY`, and never touches `--state-dir`
(the last-known-good file is an agent-mode concept). The CLI enforces the split: `--config` and
`--master` are mutually exclusive, and `--master` requires `--server`.

## 1. Decide whether you want this

|                              | Standalone (`--config`) | Agent (`--master`) |
|---|---|---|
| Source of truth | The file on the node | The canvas in SurrealDB |
| Needs SurrealDB / RabbitMQ / master | No | Yes |
| Needs an operator API key | No | Yes |
| How a change lands | You edit the file, then `SIGHUP` | Master streams a revision |
| Rollout ordering across nodes | Yours to arrange | Convergent, dependency-ordered |
| Survives a restart with the last config | The file *is* the config | From `--state-dir` |
| TLS key + chain on disk | Required | Required |

Standalone is the right choice for a single node, an air-gapped or one-off relay, a bastion you
manage with Ansible/Nix/Puppet, and for reproducing a node's behaviour on a laptop. Reach for agent
mode when several nodes form one topology and you want ordered rollouts instead of coordinating
`SIGHUP`s by hand.

:::note[The two modes read the same model]
The config format is `lib/guru_worker_config`, shared by both planes — see
[Worker config file](/reference/configuration/#worker-config-file). Nothing in the file is
"standalone-only", so a node can be migrated into a canvas later without rewriting its topology.
:::

## 2. Prerequisites

- A Linux `x86_64` host with glibc (current Debian/Ubuntu/RHEL). The published binary is
  `x86_64-unknown-linux-gnu` and will not run on Alpine or any other musl distribution.
- The `guru-worker` binary, from a `worker-v*` GitHub release — see
  [Deployment §10](/guides/deployment/#10-get-the-worker-binary-from-a-github-release) for how to
  resolve the asset and what to check. Nothing on that page's control plane is needed here.
- For any TLS or QUIC listener: a PEM private key and full-chain certificate **already on the
  host**. The worker reads them from the paths in the config; it never fetches or generates
  certificates in either mode.

You do **not** need Docker, SurrealDB, RabbitMQ, the dashboard, an API key, or network reachability
to anything except your own upstreams.

## 3. Install the binary

```sh
sudo install -m 0755 guru-worker /usr/local/bin/guru-worker
/usr/local/bin/guru-worker --help        # there is no --version; --help is the smoke test
```

Create a system group and a matching system user with no home and no shell, then a config directory
the user can read. Create the group explicitly rather than relying on `useradd` to derive one —
whether it does depends on the distribution's `useradd` defaults (`USERGROUPS_ENAB`) — because every
`chown` below, and `Group=` in the unit, needs it to exist:

```sh
sudo groupadd --system guru-worker
sudo useradd --system --gid guru-worker \
  --no-create-home --home-dir /nonexistent \
  --shell /usr/sbin/nologin guru-worker      # RHEL: /sbin/nologin
sudo mkdir -p /etc/guru-worker
sudo chown root:guru-worker /etc/guru-worker
sudo chmod 0750 /etc/guru-worker
```

Guard both if config management re-runs them:
`getent group guru-worker || sudo groupadd --system guru-worker` and
`getent passwd guru-worker || sudo useradd --system --gid guru-worker …`.

## 4. Write the config

`/etc/guru-worker/config.toml` is the default path, so a unit that passes no `--config` still works.
Start from the smallest thing that proves the data path — one raw TCP listener to one backend:

```toml
# /etc/guru-worker/config.toml
ipv6_resolve = "tolerated"

[log]
level = "info"

[[forwarding]]
tag = "edge"
listen = "0.0.0.0:8443"
listen_as = "raw"

[forwarding.to]
type = "exit"
destination = "10.0.0.5:8080"
```

A TLS-terminating entrypoint that fans out over a load-balance group looks like this — note that the
certificate paths are plain files the worker must be able to open:

```toml
[[forwarding]]
tag = "public-https"
listen = "0.0.0.0:443"

[forwarding.listen_as.tls]
key = "/etc/guru-worker/tls/key.pem"
full_chain = "/etc/guru-worker/tls/fullchain.pem"

[forwarding.to]
type = "load_balance"
strategy = "round_robin"

[[forwarding.to.members]]
type = "exit"
destination = "10.0.0.5:8080"

[[forwarding.to.members]]
type = "exit"
destination = "backend.internal:8080"
```

Every key, every listener shape and every validation rule is in
[Worker config file](/reference/configuration/#worker-config-file). Two things bite first-time
authors: unknown keys are a hard error (there are no silent defaults), and in standalone mode the
process log level comes from `log.level` in the file — `--log-level`/`GURU_LOG_LEVEL` is an
agent-mode flag and is ignored here.

Certificates and keys must be readable by the service user, and the key must not be world-readable:

```sh
sudo chown root:guru-worker /etc/guru-worker/tls/key.pem /etc/guru-worker/tls/fullchain.pem
sudo chmod 0640 /etc/guru-worker/tls/key.pem
```

Try the file in the foreground before you install a unit around it. There is no check-only flag:
the worker parses and validates the config *before* it binds anything, so a bad file dies
immediately, and a good one binds its listeners and keeps serving until you interrupt it with
`Ctrl-C`. Use a config whose ports are all above 1024 for this, or run it as root — an
unprivileged foreground run cannot bind `:443`.

```sh
sudo -u guru-worker /usr/local/bin/guru-worker -c /etc/guru-worker/config.toml
# fatal: parse toml: TOML parse error at line 4, column 1 … unknown field `listenas`  ← exits
# INFO guru_worker: loaded config path=/etc/guru-worker/config.toml                   ← serving; Ctrl-C to stop
```

Add `send_proxy_protocol = "v2"` to an `exit` only once the backend is PROXY-aware (nginx
`proxy_protocol`, HAProxy `accept-proxy`, Envoy's proxy-protocol listener filter). Sending it to a
plain HTTP server makes every request fail on a malformed request line, which is a confusing way to
discover that the data path works.

## 5. Run it under systemd

```ini
# /etc/systemd/system/guru-worker.service
[Unit]
Description=guru data-plane worker (standalone)
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=guru-worker
Group=guru-worker
ExecStart=/usr/local/bin/guru-worker --config /etc/guru-worker/config.toml
ExecReload=/bin/kill -HUP $MAINPID
Restart=on-failure
RestartSec=2

# Ports below 1024 without running as root
AmbientCapabilities=CAP_NET_BIND_SERVICE
CapabilityBoundingSet=CAP_NET_BIND_SERVICE

# The worker keeps no state of its own in this mode
NoNewPrivileges=yes
ProtectSystem=strict
ProtectHome=yes
PrivateTmp=yes
ReadOnlyPaths=/etc/guru-worker
LimitNOFILE=65535

[Install]
WantedBy=multi-user.target
```

```sh
sudo systemctl daemon-reload
sudo systemctl enable --now guru-worker
```

`LimitNOFILE` matters: every proxied connection costs two descriptors, and the default 1024 is a
ceiling you will hit under load rather than a safety net. There is no `StateDirectory=` on purpose —
standalone mode writes nothing.

## 6. Verify

```sh
# 1. Started, and one "listener started" line per [[forwarding]]
journalctl -u guru-worker -n 20 --no-pager
#   INFO guru_worker: loaded config path=/etc/guru-worker/config.toml
#   INFO guru_worker::supervisor: listener started addr=0.0.0.0:8443 transport=Tcp tag=edge

# 2. The socket is actually bound. Match the port, not the process name:
#    unprivileged `ss -p` hides process info for other users' sockets.
sudo ss -ltnp 'sport = :8443'          # QUIC/UDP listener: sudo ss -lunp 'sport = :8443'

# 3. Traffic reaches the backend through the listener
curl -sv telnet://127.0.0.1:8443 </dev/null    # or exercise the real protocol
```

If a listener is missing from `ss` but the unit is running, read the log: an apply error names the
offending entry as `<tag>: <reason>` — `Address already in use`, `Permission denied` on a privileged
port (missing `CAP_NET_BIND_SERVICE`), or a cert file it cannot open.

## 7. Change the config: edit, then reload

```sh
sudoedit /etc/guru-worker/config.toml
sudo systemctl reload guru-worker          # sends SIGHUP
journalctl -u guru-worker -n 5 --no-pager  # "config reloaded"
```

A reload is all-or-nothing and does not interrupt traffic it does not have to:

- The file is parsed, validated, and every new socket is bound **before** anything changes. If any
  step fails, nothing is touched and the log says `reload failed` or `reload apply failed; keeping
  running config` — the worker keeps serving the old topology.
- Listeners that survive the change hot-swap their compiled config; the next connection uses the new
  destinations. Existing connections keep their old path until they close.
- Listeners that disappeared stop accepting, but their in-flight connections are not killed.
- A listener that is replaced by a different one on the same `ip:port` is closed first, and the
  reload waits for the socket to actually be released, so the replacement binds within the same
  reload. A QUIC endpoint drains its live connections first and is forced down if that takes longer
  than three seconds — an address being taken over cannot be held indefinitely. If the replacement
  still fails to bind, every listener the reload had stopped is brought back up.

Because certificates are parsed on apply, renewal is a reload too — no restart, no dropped
connections. Wire it into your ACME client, e.g. for certbot:

```sh
# /etc/letsencrypt/renewal-hooks/deploy/guru-worker.sh
#!/bin/sh
install -o root -g guru-worker -m 0640 \
  /etc/letsencrypt/live/example.com/privkey.pem   /etc/guru-worker/tls/key.pem
install -o root -g guru-worker -m 0644 \
  /etc/letsencrypt/live/example.com/fullchain.pem /etc/guru-worker/tls/fullchain.pem
systemctl reload guru-worker
```

`SIGTERM`/`SIGINT` (so `systemctl stop`, `systemctl restart`) stop every listener and shut down.
Note that the log level is read once at startup: changing `log.level` needs a restart, not a reload.

## 8. Troubleshooting

| Symptom | Cause |
|---|---|
| `fatal: read /etc/guru-worker/config.toml: No such file or directory` | No `--config` and nothing at the default path, or the service user cannot read it |
| `fatal: parse toml: TOML parse error at line N …  unknown field …` | A typo'd key; every table rejects unknown fields, and the message points at the line |
| `fatal: duplicate listener 0.0.0.0:443 (edge)` | Two entries on the same `ip:port` and transport. Only a TCP and a QUIC entry may share a port |
| `fatal: forwarding edge relay to tls/quic requires sni` | A `tls`/`quic` relay hop with no `sni`, at any nesting depth |
| `fatal: edge: Permission denied (os error 13)` | A privileged port without `AmbientCapabilities=CAP_NET_BIND_SERVICE` (apply errors are prefixed with the entry's `tag`) |
| `fatal: tls-term: error:80000002:… calling fopen(/etc/guru-worker/tls/key.pem, r)` | A cert or key path the worker cannot open — OpenSSL reports it, so the message is noisy but names the file |
| `WARN config lint … used ip_hash for load balancing` | `ip_hash` under a listener with no `receive_proxy_protocol`. Real for a `raw`/`tls` listener behind a proxy — every connection hashes the proxy's address onto one member. Harmless for a `relay` listener, which always decodes its own PROXY header and so hashes the true client; the lint does not distinguish them |
| Reload appears to do nothing | Check for `reload failed` in the log — the old config is still serving. Also confirm `ExecReload` sends `SIGHUP`, not `SIGUSR1` |
| Backend sees the proxy's IP, not the client's | Add `send_proxy_protocol` on the `exit` (and teach the backend to read it), and set `receive_proxy_protocol` if the worker itself sits behind a proxy |

## 9. Adopting the node later

Standalone and agent mode are the same binary reading the same model, so migration is a unit-file
change rather than a rewrite:

1. Model the node as a server in a canvas and let the master derive its config.
2. Compare the two files — `manage-tool orchestration export-config --server <key>` prints what the
   canvas currently derives, which is the file the master would stream. (That command talks to
   SurrealDB, so it runs on an operator machine, not on the worker node.)
3. Swap `--config <file>` for `--master <url> --server <key>`, provide the API key through
   `GURU_API_KEY` or `--api-key-file`, and add a writable `--state-dir` so the node can restore its
   last-known-good config after a restart.
