# `orchestration` — canvases, servers, nodes and worker rollout

The control plane of the proxy fabric. Operators build a **canvas** of servers
and nodes; this module validates the topology, derives one `guru-worker` config
per server, and streams every new revision to the workers that registered for it.

## Layout

```
src/
├── lib.rs          # crate root: declares the modules below
├── config.rs       # `OrchestrationConfig`: health, ACME and relay-certificate knobs
├── utils/          # ids (record id ↔ wire string), secret (master-key encryption)
├── entities/
│   └── surreal/    # canvas, server, node, port, connection, view, topology,
│                   # health, dns, certificate (ACME), ca (internal CA, relay leaves)
├── services/       # CRUD, topology rules, derivation, convergence, rollout, agent, watch, ca
├── events/         # `CanvasDirty`, the derivation trigger
├── hooks/          # derive.rs (consumer, sweep, relay leaf rotation), health.rs (liveness
│                   # sweep, retention), acme.rs (issuance/renewal)
└── rpc/            # the operator API and the worker API, plus refresh-key middleware
```

## Node kinds

A canvas is a bipartite dataflow over two independent port kinds:

- **DeriveListen** flows `Pod(out) → Entry|Relay(in)`: what a pod's listener looks
  like on the wire.
- **DeriveDestination** flows `Exit(out) → … → Pod(in)`: where a pod's traffic goes,
  possibly through load balancers and relays.

Every port carries at most one edge.

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
  tree (`spec.config.ip` is asserted against the tree, not the canvas).

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
                             (cron sweep re-derives anything the message missed)
                                                                        │
worker: Register ─► WatchConfig (stream) ─► apply ─► AckConfig ─────────┘
```

Derivation is fenced by `orchestration_canvas.generation`: a pass derives at the
generation it read and commits only while the canvas is still there, so a
concurrent edit is never overwritten — the pass just loses and is redone. The
`CanvasDirty` message is only latency: `generation > derived_generation` is what
actually decides, and the cron sweep acts on it, so a broker outage costs delay
and never correctness.

## Seamless switching

Derivation says what a server *should* serve; `services::converge` says what it
may serve *now*, given what every other server is running:

- a forwarding is only pointed at a listener some server's `applied` snapshot
  already serves — otherwise the previous shape is held and the server is
  recorded as `waiting_for` the target;
- a listener is kept alive for as long as any snapshot still points at it, even
  after the canvas stopped asking for it.

A multi-hop change therefore converges in as many passes as it has hops, with no
coordinator and no ordering. An edit that would put a *different protocol* on an
ip:port some server still dials has no seamless path at all and is rejected at
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
closing marks the server `Offline`; `hooks::health::run_liveness_sweep` catches a
worker that vanished without closing (no report for
`health_report_interval × health_offline_after_intervals`), and
`run_health_retention` trims both tables to their TTLs. The current status is
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
(`EnsureRelayCertificates`, SAN `<pod-key>.relay.guru.internal`), and
`run_relay_cert_rotation` re-issues leaves within `relay_cert_renew_before` of
expiry and re-derives their canvases.

## Dependency direction

- `rpc` depends on `services` (and `rpguru_sdk`); the watch hub lives in
  `services::watch` so nothing below the edge depends on the edge.
- `services` depend on `entities`; every query is a `Processor` in
  `entities/surreal`.
- The schema lives in `database/schema/orchestration.surql`; the integration tests
  apply that exact file to a `mem://` database.

See `AGENTS.md` at the workspace root for the full authoring guide.
