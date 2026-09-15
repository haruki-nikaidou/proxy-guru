# `orchestration` — canvases, servers, nodes and worker rollout

The control plane of the proxy fabric. Operators build a **canvas** of servers
and nodes; this module validates the topology, derives one `guru-worker` config
per server, and streams every new revision to the workers that registered for it.

## Layout

```
src/
├── lib.rs          # crate root: declares the modules below
├── config.rs       # `OrchestrationConfig` (key `orchestration`): health, ACME, relay
│                   # and periodic-interval knobs
├── utils/          # ids (record id ↔ wire string), secret (master-key encryption)
├── entities/
│   └── surreal/    # canvas, server, node, port, connection, view, topology,
│                   # health, dns, certificate (ACME), ca (internal CA, relay leaves)
├── services/       # CRUD, topology rules, derivation, convergence, rollout, agent, watch, ca
├── events/         # `CanvasDirty` plus the five periodic execution signals
├── hooks/          # schedule.rs (the `cron` clock and the run claim), derive.rs
│                   # (derivation, stale-canvas sweep, relay leaf rotation),
│                   # health.rs (liveness sweep, retention), acme.rs (issuance/renewal)
└── rpc/            # the operator API and the worker API, plus refresh-key middleware
```

## Node kinds

A canvas is a bipartite dataflow over two independent port kinds:

- **DeriveListen** flows `Pod(out) → Entry|Relay(in)`: what a pod's listener looks
  like on the wire.
- **DeriveDestination** flows `Exit(out) → … → Pod(in)`: where a pod's traffic goes,
  possibly through load balancers and relays.

Every port carries at most one edge.

### Servers, pods and addresses

A **pod** is one listener on one server: `{ server, port, bind_ip?, advertise_ip? }`.
The `server` link is what attributes the pod to a server (the schema asserts it
resolves within the canvas tree). `bind_ip` unset binds every address of the
host (`[::]` dual-stack; the worker falls back to `0.0.0.0` without IPv6),
`0.0.0.0` restricts it to IPv4, a literal pins one interface. An unwired pod
derives nothing and is not a problem.

Every new server also gets its **universal pod** (`universal_pod`), the node
bundles land on. The load-balance nodes carry the operator's rule as their
**members** (`members: [{slot, name}]` in the spec, 1–256, one `bundle` port
`member_<slot>` each, in the order listed): a **distribute** node
(`load_balance_distribute`: one mode, one relay `protocol`) takes entry pods as
*channels* on `chan:<pod>` ports and bundles all of them out through each
member to a universal pod (or to another distribute node); each universal pod
lands every channel it receives on a generated pod of its server (random port
in 40000–59999, editable) and bundles on through its fixed `bundle_out`, to
another universal pod, to a distribute node, or onto a member of an
**aggregate** node (`load_balance_aggregate`), which grows one `chan:<pod>`
input per channel for an exit. Bundles are collected automatically where they
arrive at a universal pod or a distribute node (`bundle_in:<source>`, created
by the connect); a member's slot is stable, so renaming or reordering members
keeps the bundle drawn on the port, and dropping a wired member is refused.
None of this is a new traffic model: `services::universal` expands the bundles
into ordinary pod / relay / load-balance **lanes** (rows tagged with `lane`,
laid out thin from a count rather than from members) and ordinary edges in the
same transaction as the edit that changed them (`ApplyTopologyBatch`), and
derivation, convergence and certificates only ever see the flat graph. The
`chan:` ports are paired with hidden `lane:` ports that `Index::peer` looks
through, exactly like an import/export boundary. Lanes are keyed
(`group:channel:role:source`), so an edit keeps every lane whose identity
survives it, with its port ids and its listening port; only a protocol change
on the distribute node re-rolls the landing ports, since a listener cannot
change protocol in place.

A server's addresses are learned, not typed: the worker reports its public
IPv4/IPv6 (looked up through `--public-ipv4-urls` / `--public-ipv6-urls`), its country (`--geo-url`) and interface addresses on
`Register` and whenever they change; the master records the peer address the
registration came from (`x-real-ip` behind the documented proxy, see
`trust_proxy_address_headers`). Operators may pin either family
(`override_v4`/`override_v6`) or add `extra_addresses`. What another server
dials is `ServerEntity::effective_address`: v4 pin → reported v4 → observed v4
→ the same chain for v6; a pod may name one of the known addresses as
`advertise_ip` instead, and a relay's `override_ip_address` still wins. A server
with no address at all is a warning, and every pod on *other* servers that dials
it stays in `invalid_pods` until one is known.

Listener identity — the `ListenerCap` convergence matches on — is
`(server, port, protocol)`, never an address, so an address change re-derives
destinations without breaking the seamless-switch protocol.

## Subcanvases

A `CanvasImport` node embeds another canvas as one node; a `CanvasExport` node
inside that canvas is one boundary port. Nesting is arbitrarily deep and stored
only on the import node (`spec.config.canvas`); root, ancestors and tree are
computed by `fn::orchestration_root` / `_ancestors` / `_tree` in the schema.

- **Import ports are derived.** An import node has one port per export node of
  its target: key = the export node's record id, kind copied, direction
  mirrored (an `InputIntoCanvas` export emits inside, so the import port is an
  input), ordered by the export's `position.y`. Creating, retiring, re-kinding or
  moving an export reshapes the importer's ports in the same transaction
  (`fn::orchestration_reshape_ports`); a port whose key survives keeps its edges,
  a retired export silently drops the parent edge on its mirrored port.
- **The tree is the unit of everything.** Topology checks, switch safety and
  derivation load the whole tree and look *through* boundaries
  (`topology::Index::peer`), so a nested graph derives byte-identical TOML to
  its flattened equivalent. Only the root's `generation` counts: every mutating
  transaction calls `fn::orchestration_touch`, and the derivation hook resolves
  the root of whatever canvas it is told about.
- **Rules.** A canvas cannot import itself or an ancestor, is imported at most
  once (unique index on `spec.config.canvas`), and an import node's target is
  immutable (retire and import again). An imported canvas cannot be deleted
  until its import is retired; deleting a root deletes its whole tree.
- **Servers stay put.** A parent sees a child canvas as a black box, but the
  tree is one graph: a pod anywhere in a tree may listen on any server of that
  tree (`spec.config.server` is asserted against the tree, not the canvas).

## The config view

Rows are edited in place. What a worker runs lives in one
`orchestration_server_config_view` row per server, holding three immutable
snapshots — `desired` (latest derivation), `in_flight` (handed to the worker, not
yet acknowledged) and `applied` (what it runs). A snapshot is self-contained: the
rendered TOML plus, per `[[forwarding]]`, the listener it serves and the listeners
it points at.

## Rollout

```
mutation ─► validate projected topology ─► write rows + bump canvas generation
                                                            │
                                                    publish CanvasDirty
                                                            │
       hooks::derive ─► derive + converge every server ─────┴─► desired snapshot
                       (derive_stale_canvases re-derives what the message missed)
                                                                        │
worker: Register ─► WatchConfig (stream) ─► apply ─► AckConfig ─────────┘
```

Derivation is fenced by `orchestration_canvas.generation`: a pass derives at the
generation it read and commits only while the canvas is still there, so a
concurrent edit is never overwritten — the pass just loses and is redone. The
`CanvasDirty` message is only latency: `generation > derived_generation` is what
actually decides, and the periodic `derive_stale_canvases` pass acts on it, so a
dropped message costs delay and never correctness. That backstop is itself a
message, so a broker outage stalls derivation until the broker returns; the
generation counters make the catch-up automatic once it does.

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
`entities::surreal::job_run::ClaimJobRun::for_tick` first: one compare-and-set
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
  after the canvas stopped asking for it. A pod whose listener moved therefore
  runs two for a while; the held one is tagged after its socket
  (`osaka-hop (9443/relay_tcp)`), since a worker keys its listeners by tag.

A multi-hop change therefore converges in as many passes as it has hops, with no
coordinator and no ordering. An edit that would put a *different protocol* on an
server/port some server still dials has no seamless path at all and is rejected at
edit time. A server that is gone for good is cleared with `ForgetServerApplied`
(Admin only, like the other operations that bypass a safety invariant), so its
dependants stop waiting for it.

A worker registers with an **operator API key** and receives a dynamic refresh key
that it keeps in memory only; the master stores its SHA-256 digest. Re-registering
rotates the key, which kills the previous session's stream — that is how a master
learns a worker restarted. Only servers whose derived TOML actually changed get a
new revision, so unrelated servers never restart their listeners.

A revision is acknowledged **per pod**: the worker commits every forwarding it
could prepare and bind, keeps the previous listener of the ones it could not,
and lists each pod's outcome in `AckConfig`. `services::agent` then stores
`applied` as a snapshot of that mix — the acknowledged revision's shape for the
pods that applied, the previous `applied` shape for the failed ones — so
convergence keeps reasoning about what the worker really serves, and records the
failed pods on the view (`failed_pods`, `failed_revision`).

## Health

Workers stream `HealthReport`s over `ReportHealth` (refresh-key authenticated,
one per interval); `services::health` turns each into a `server_health_record`
row (byte and connection deltas, status `Online`/`Degraded`) and one
`node_health_record` per node the report touches — the pod and every node in its
`ForwardingDeps.nodes`, worst status wins for a node shared by several pods.
`Deploying` rows are also written the moment a derivation publishes a new
revision, and `Failed`/`Ready` rows the moment an ack lands, so status never
waits for the next report. Every write is one transaction fenced on the
server's `refresh_key_generation`, like the rest of the agent path. The stream
closing marks the server `Offline`; `hooks::health::HealthCronHook` catches a
worker that vanished without closing on the `sweep_liveness` signal (no report
for `health_report_interval × health_offline_after_intervals`) and trims both
tables to their TTLs on `trim_health_history`. The current status is
denormalised on `orchestration_server.health_status` for listings.

## Certificates

Derivation takes a `DerivationCertificates` input next to the topology: the
ACME rows of every SNI a TLS Entry asks for, the relay leaf of every pod behind
a TLS/QUIC relay, and whether the internal CA exists. A pod whose material is
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
unconditionally; two of those collide on the `relay_certificate_pod` unique
index and one transaction fails, which the caller retries.

## Dependency direction

- `rpc` depends on `services` (and `rpguru_sdk`); the watch hub lives in
  `services::watch` so nothing below the edge depends on the edge.
- `services` depend on `entities`; every query is a `Processor` in
  `entities/surreal`.
- The schema lives in `database/schema/orchestration.surql`; the integration tests
  apply that exact file to a `mem://` database.

See `AGENTS.md` at the workspace root for the full authoring guide.
