# `orchestration` — canvases, the pod graph and worker rollout

The control plane of the proxy fabric. Operators edit a **pod graph** on a tree of
canvases; this module checks it, derives one `guru-worker` config per server, and
streams every new revision to the workers that registered for it.

## Layout

```
src/
├── lib.rs          # crate root: declares the modules below
├── config.rs       # `OrchestrationConfig` (key `orchestration`): health, ACME, relay
│                   # and periodic-interval knobs
├── utils/          # ids (wire string → typed id), secret (master-key encryption)
├── entities/
│   └── db/         # canvas, tree, fence, server, pod, exit, edge, group, graph
│                   # (load / batch write), view, health, dns, certificate (ACME),
│                   # ca (internal CA, relay leaves), job_run, agent_release
├── services/       # graph (read, check, apply), derive, converge, rollout, agent,
│                   # canvas, server, health, ca, acme, dns, live, watch
├── events/         # `CanvasDirty`, the periodic execution signals, live messages
├── hooks/          # schedule.rs (the `cron` clock and the run claim), derive.rs
│                   # (derivation, stale-canvas sweep, relay leaf rotation),
│                   # health.rs (liveness sweep, retention), acme.rs (issuance/renewal)
└── rpc/            # the operator API and the worker API, plus refresh-key middleware
```

## The pod graph

A canvas tree's forwarding topology is a directed acyclic graph, checked and
compiled by `lib/guru_topology`:

- A **pod** (`orchestration_pod`) is one listener on one server: a port (0 on a
  write picks a free one in 40000–59999), an optional bind address (unset binds
  every address of the host, `[::]` dual-stack), an optional advertise address,
  and its **ingress** — `client_raw` or `client_tls` (clients connect directly;
  PROXY may be received, TLS is terminated with an ACME certificate) or
  `relay_tcp` / `relay_tls` / `relay_quic` (other pods relay to it in that
  protocol). A server is only the set of pods that run on it.
- An **exit** (`orchestration_exit`) is a `host:port` outside the fabric,
  optionally sent PROXY.
- An **edge** (`orchestration_edge`) is one way a pod's traffic goes on: to a
  relay pod (dialed in the protocol that pod listens with, at the edge's override
  address and port if set, else the pod's advertise address or its server's
  effective address, and the pod's port) or to an exit. Parallel edges are
  allowed; a client pod cannot be led into.
- Each pod's **route** (`route jsonb`) is a tree over exactly its own out-edges:
  `{"edge": id}`, `{"balance": [{"weight": 2, "to": …}], "sticky": "client_ip"}`
  or `{"failover": [ … ]}`, nested freely.
- **Groups** (`orchestration_group` / `_member`) are what the dashboard keeps
  about its drawing (the layout of splitters and aggregators, say). Nothing is
  derived from them.

`services::graph` is the edit path. `GetGraph` loads the whole tree with the
diagnostics it checks with. `ApplyGraph` takes one batch of puts and deletes,
applies it to the tree in memory, allocates ports, runs `guru_topology::check`
and the switch-safety check, and writes the batch in one transaction fenced on
the root's generation (`entities::db::fence::touch_checked`) — or refuses all of
it when any diagnostic is an error. `dry_run` answers the diagnostics without
writing; `expected_generation` refuses a batch computed against a tree that has
moved on. A batch of groups alone takes no fence and dirties nothing.
`MoveItems` places servers, exits and subcanvases.

A server's addresses are learned, not typed: the worker reports its public
IPv4/IPv6 (looked up through `--public-ipv4-urls` / `--public-ipv6-urls`) and interface addresses on
`Register` and whenever they change; the master records the peer address the
registration came from (`x-real-ip` behind the documented proxy, see
`trust_proxy_address_headers`). Operators may pin either family
(`override_v4`/`override_v6`) or add `extra_addresses`. What another server
dials is `ServerEntity::effective_address`: v4 pin → reported v4 → observed v4
→ the same chain for v6; a pod may name one of the known addresses as
`advertise_ip` instead, and an edge's `override_ip` still wins. A pod dialing a
pod whose server has no address at all is invalid until one is known.

The dashboard's flag is the country of the first IPv4 of that chain
(`ServerEntity::v4_address`). No worker reports it: the `resolve_server_countries`
pass looks each address up through `country_lookup_url` once, again when it
changes and after a failure's `country_lookup_retry_after_secs`, and stores the
answer with the address it belongs to (`country_address`), so a server whose
address moved never shows the old address's flag.

Listener identity — the `ListenerCap` convergence matches on — is
`(server, port, protocol)`, never an address, so an address change re-derives
destinations without breaking the seamless-switch protocol.

`Register` also records the worker's **capabilities**: a worker reporting
`route_table` gets each pod's route as a table of groups and upstreams, anything
older the tree form (weights as repetitions, failover as fallback, sticky as
`ip_hash`); relays are asked to confirm only on workers reporting
`relay_confirm`.

## Subcanvases

A canvas may have a `parent`, and is drawn at `position` on it. Nesting only
organises the drawing: edges cross canvas boundaries freely, and a pod may be
drawn on any canvas of its server's tree.

- **The tree is the unit of everything.** Checks, switch safety and derivation
  load the whole tree (`entities::db::graph::LoadCanvasGraph`). Only the root's
  `generation` counts: every edit bumps it (`fence::touch_checked`), and the
  derivation hook resolves the root of whatever canvas it is told about. Workers
  never touch it (see *Rollout*).
- **Deleting** a canvas deletes its subtree in one transaction, and is refused
  while a pod outside still leads into it or runs on a server inside it.

## The config view

Rows are edited in place. What a worker runs lives in one
`orchestration_server_config_view` row per server, holding three immutable
snapshots — `desired` (latest derivation), `in_flight` (handed to the worker, not
yet acknowledged) and `applied` (what it runs). A snapshot is self-contained: the
rendered TOML plus, per `[[forwarding]]`, the listener it serves and the listeners
it points at.

## Rollout

```
ApplyGraph ─► apply batch in memory ─► check ─► write rows + bump root generation
                                                            │
                                                    publish CanvasDirty
                                                            │
       hooks::derive ─► compile + converge every server ────┴─► desired snapshot
                       (derive_stale_canvases re-derives what the message missed)
                                                                        │
worker: Register ─► WatchConfig (stream) ─► apply ─► AckConfig ─────────┘
```

Derivation is fenced by two counters. Operator edits bump
`orchestration_canvas.generation`; a pass derives at the generation it read and
commits only while the canvas is still there, so a concurrent edit is never
overwritten — the pass just loses and is redone. Workers never write the canvas
row: an ack, a registration or a changed address report bumps `seq` on that
server's own `orchestration_server_config_view` row, and a pass reads the tree's
views anyway, so it stamps their `seq` sum as `derived_view_seq` when it commits.
A canvas is stale while `generation > derived_generation` or the views' `seq`
sum exceeds `derived_view_seq`. The sum only shrinks when a view row is deleted,
and everything that deletes one is an edit that bumps `generation`, so a
shrinking sum never hides what a worker wrote. Keeping workers off the canvas
row is what lets a fleet acknowledge one revision in the same instant without
contending on it (a writer that meets a locked row waits for it, so a hot row
is a queue rather than an error, but the queue is still the fleet's latency).

The `CanvasDirty` message is only latency: the two counters are what actually
decide, and the periodic `derive_stale_canvases` pass acts on them, so a dropped
message costs delay and never correctness. That backstop is itself a message,
so a broker outage stalls derivation until the broker returns; the counters make
the catch-up automatic once it does.

## Periodic jobs

Nothing periodic runs in the process that schedules it. `--mode cron` holds one
`hooks::schedule::IntervalJob` per signal type — its own last-fire timestamp, so
a 30 s job and an hourly one do not flatten each other — and publishes the due
ones. It reads no configuration and opens no database connection. `--mode
consumer` binds one durable queue per signal and runs the pass:

| Signal / routing key | Queue | Hook | Interval |
|---|---|---|---|
| `derive_stale_canvases` | `guru_orchestration_derive_stale_canvases` | `hooks::derive::CanvasDeriver` | `sweep_interval()` |
| `rotate_relay_certificates` | `guru_orchestration_rotate_relay_certificates` | `hooks::derive::CanvasDeriver` | `relay_rotation_interval()` |
| `sweep_liveness` | `guru_orchestration_sweep_liveness` | `hooks::health::HealthCronHook` | `liveness_interval()` |
| `trim_health_history` | `guru_orchestration_trim_health_history` | `hooks::health::HealthCronHook` | `health_retention_interval()` |
| `renew_certificates` | `guru_orchestration_renew_certificates` | `hooks::acme::AcmeCronHook` | `acme_interval()` |

Delivery is at-least-once and consumers are replicated, so every hook gates on
`entities::db::job_run::ClaimJobRun::for_tick` first: one compare-and-set
on the job's `orchestration_job_run` row, fenced on both the tick the signal was
published for and the configured interval — which is measured between ticks, not
between runs, so a pass that takes a minute does not push the next one out.
Whoever wins runs the pass, everybody else returns having touched nothing, and a
job cannot run twice for the same tick nor more often than its configured
interval. That is why the publication cadence is a constant and the interval is
configuration: the constant is a floor the scheduler can honour without reading
the database, the interval is what an operator actually sees.

A pass that iterates rows claims them individually as well, and against the
value it read rather than against a clock: ACME compare-and-sets the
`last_attempt_at` its listing observed (`ClaimCertificateAttempt`) and relay
rotation writes each leaf against the version it read (`RotateRelayCertificate`,
`None` when another consumer got there first). Two consumers working the same
backlog therefore cannot collide, and neither can a pass whose listing went
stale while it worked — an ACME order takes minutes. The claim stamps
`last_attempt_at` before ordering, which is what holds the row out of
`ListCertificatesDue` for `acme_retry_after` while the attempt runs or after it
crashed.

## Seamless switching

Derivation says what a server *should* serve; `services::converge` says what it
may serve *now*, given what every other server is running:

- a forwarding is only pointed at a listener some server's `applied` snapshot
  already serves — otherwise the previous shape is held and the server is
  recorded as `waiting_for` the target;
- a listener is kept alive for as long as any snapshot still points at it, even
  after the graph stopped asking for it. A pod whose listener moved therefore
  runs two for a while; the held one is tagged after its socket
  (`<pod id> (9443/relay_tcp)`), since a worker keys its listeners by tag and a
  pod's tag is its id.

A multi-hop change therefore converges in as many passes as it has hops, with no
coordinator and no ordering. An edit that would put a *different protocol* on an
server/port some server still dials has no seamless path at all and is refused by
`ApplyGraph` with a diagnostic. A server that is gone for good is cleared with `ForgetServerApplied`
(Admin only, like the other operations that bypass a safety invariant), so its
dependants stop waiting for it.

A worker registers with an **operator API key** and receives a dynamic refresh key
that it keeps in memory only; the master stores its SHA-256 digest. Re-registering
rotates the key, which kills the previous session's stream — that is how a master
learns a worker restarted. Only servers whose derived TOML actually changed get a
new revision, so unrelated servers never restart their listeners.

The derived config carries the worker's `[keepalive]` defaults, and the TOML omits
the section when it equals them: every struct of `guru_worker_config` rejects
unknown keys, so a master must not send a section to workers built before it
existed. Tuning those values is a standalone-file affair until the dashboard
grows a setting for them. `[quic]` is the server's own setting (`ServerEntity::quic`,
edited in the server inspector) and is omitted the same way; on each QUIC relay
link `derive` pairs the two servers' numbers and writes the lower rate on the
forwarding, so a server never sends faster than its peer's down rate.

A revision is acknowledged **per pod**: the worker commits every forwarding it
could prepare and bind, keeps the previous listener of the ones it could not,
and lists each pod's outcome in `AckConfig`. `services::agent` then stores
`applied` as a snapshot of that mix — the acknowledged revision's shape for the
pods that applied, the previous `applied` shape for the failed ones — so
convergence keeps reasoning about what the worker really serves, and records the
failed pods on the view (`failed_pods`, `failed_revision`).

## Health

Workers stream `HealthReport`s over `ReportHealth` (refresh-key authenticated,
one per interval, each answered once recorded); `services::health` turns each into a `server_health_record`
row (byte and connection deltas, status `Online`/`Degraded`) and one
`pod_health_record` per pod the report names — a forwarding's tag is its pod's
id, and a pod held under two listeners keeps the worst status.
`Deploying` rows are also written the moment a derivation publishes a new
revision, and `Failed`/`Ready` rows the moment an ack lands, so status never
waits for the next report. Every write is one transaction fenced on the
server's `refresh_key_generation`, like the rest of the agent path. The stream
closing marks the server `Offline`; `hooks::health::HealthCronHook` catches a
worker that vanished without closing on the `sweep_liveness` signal (no report
for `health_report_interval × health_offline_after_intervals`) and trims both
tables to their TTLs on `trim_health_history`. Going offline hands the watch
session back (lease dropped, `watch_epoch` bumped), so a stream a proxy keeps
open for a dead worker cannot hold the server. The same sweep revokes the
session of a server that is already `Offline` once its registration
(`registered_at`) is older than that threshold without a report: a worker that
registered again and lost its connection before reporting changes no status, and
nothing else would end its stream. The current status is
denormalised on `orchestration_server.health_status` for listings.

## Certificates

Derivation takes a `DerivationCertificates` input next to the graph: the
ACME rows of every SNI a TLS client pod asks for, the relay leaf of every
`relay_tls` / `relay_quic` pod, and whether the internal CA exists. A pod whose material is
missing is an `invalid_pods` entry naming the state (`certificate for <sni> is
pending`, `internal CA not initialised`, `relay certificate not issued yet`),
never a server-level failure. Edit-time switch safety derives with
`DerivationCertificates::assumed()` so a missing certificate cannot hide an
unsafe protocol switch.

Material never lives in a snapshot: the TOML references fixed worker paths
(`certs/acme/<certificate>/…`, `certs/relay/<pod>/…`, `certs/ca.pem`) and each
`ForwardingDeps` records the `CertificateRef`s (row key + version) its entry was
derived against; `ConfigSnapshot.certificates` is their union. A renewal or leaf
rotation bumps the row's version, so the same TOML becomes a new revision, and
`try_send` assembles the files (`services::ca::BundleCertificates`, keys
decrypted with the master key) when the revision is handed to a worker. The
version is a *minimum*, not an exact pin: only the current material is stored,
so a revision handed out after a renewal carries the newer material under the
same paths — never older than what it was derived against — and the revision
the renewal produced follows right behind it.

The internal CA is created once by `manage-tool orchestration init-ca`
(`CaService` / `InitInternalCa`, which also touches every tree with a TLS/QUIC
relay). The derivation hook issues relay leaves before deriving
(`EnsureRelayCertificates`, SAN `<pod-key>.relay.guru.internal`), and on the
`rotate_relay_certificates` signal `hooks::derive::CanvasDeriver` re-issues
leaves within `relay_cert_renew_before` of expiry and re-derives their canvases.
Both re-issue paths replace an existing leaf, and both fence the write on the
`version` they read: a pass whose compare-and-set is refused adopts the winning
row instead of storing the leaf it signed, so overlapping derivations — or a
derivation and a duplicate rotation signal — end with one new leaf, not two
versions of clobbered material. Only a pod's first leaf is written
unconditionally, and that write is an upsert on the `relay_certificate_pod`
unique constraint: two passes issuing a pod's first leaf at once both succeed,
the later one replacing the material of the earlier and bumping the version, so
a first deployment never reports the pod invalid for a pass.

## Dependency direction

- `rpc` depends on `services` (and `rpguru_sdk`); the watch hub lives in
  `services::watch` so nothing below the edge depends on the edge.
- `services` depend on `entities`; every query is a `Processor` in
  `entities/db`.
- The schema lives in `migrations/`; the integration tests run the same
  migrator against a real PostgreSQL database, one per test.

See `AGENTS.md` at the workspace root for the full authoring guide.
