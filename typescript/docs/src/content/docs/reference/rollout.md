---
title: Rollout
description: How an edit of the pod graph becomes a config revision applied by a worker.
---

## From edit to applied config

An edit is one checked batch of graph changes (`ApplyGraph`, see [Canvas](/reference/canvas/)); it
bumps the root canvas's generation and publishes `CanvasDirty`. A worker's ack, registration or
changed address report bumps a counter on its own server's config view instead, so a fleet
acknowledging at once never contends on the canvas row. The derivation hook re-derives the whole
canvas tree; the periodic `derive_stale_canvases` signal, consumed by the same hook, catches anything a
lost message missed by comparing both counters with what the last pass stamped. Both triggers are broker messages, so the backstop is not
broker-independent: a canvas whose `CanvasDirty` was lost waits for delivery to resume, and a
master with no broker reachable derives nothing at all.

Every server has **one config view** holding three snapshots — `desired`, `in_flight` and
`applied`. A worker stream promotes `desired` → `in_flight`, and its `AckConfig` promotes
`in_flight` → `applied`. Registration reconciles the view with what the worker actually runs: a
worker reporting the desired or in-flight revision has it recorded as `applied`, and a worker
reporting revision `0` — a fresh install, a wiped state directory — has `applied` forgotten, so the
stream hands the desired revision out again rather than treating the server as converged.

```text
graph edit ───────▶ CanvasDirty ──┐
                                  ├──▶ derivation hook (--mode consumer)
cron: derive_stale_canvases ──────┘
                                       │
                                       ▼
                             config view: desired
                                       │ worker stream
                                       ▼
                                   in_flight
                                       │ AckConfig
                                       ▼
                                    applied
```

## Convergence

Derivation is convergent: a server only switches destination once the target actually serves it, so
no revision drops traffic mid-rollout. That property is why the three-snapshot view exists instead
of a single "current config" field — the master always knows what a worker has really applied.

A revision is applied **per pod**: the worker commits every `[[forwarding]]` that prepares and
binds, keeps the previous listener of any that fails, and acknowledges the outcome of each. The
master then stores `applied` as the snapshot of that mix (the new shape for the pods that applied,
the previous one for those that failed), records the failed pods on the server's view and marks
the server `Degraded` and each failed pod `Failed` with the worker's message. A revision that
cannot be applied at all (unparsable TOML, unwritable certificate files) changes nothing on the
worker and is recorded as an `apply_error`.

## Health

Every worker streams one `HealthReport` per `--health-interval` (default 15 s) over `ReportHealth`:
the running revision, upload/download bytes and connection counts since the previous report (max
is the high-water mark), and one `PodStatus` per running forwarding. Each report becomes one
`server_health_record` row and one `pod_health_record` row per pod (a forwarding's tag is its pod's
id; a pod held under two listeners while its dependants switch keeps the worst status). Pod
statuses: `Ready` (the pod runs what `desired` asks), `Deploying` (a newer revision involving the
pod is derived but not applied yet — written the moment a derivation publishes it), `Failed` (the
pod failed to apply or to run). Server statuses: `Online`, `Degraded` (lagging `desired` past the
grace period, or the last acknowledged revision failed for some pod), `Offline` (the health stream
closed, or no report for three intervals — the `sweep_liveness` pass). Both histories are raw and
trimmed by the `trim_health_history` pass;
`ListServerHealthHistory` / `ListPodHealthHistory` read a time range.

Both passes run in `--mode consumer`, on a signal the `cron` scheduler publishes when the job comes
due. The scheduler holds no state beyond its own clock; the consumer claims each run in one
`orchestration_job_run` row, so the pass runs once per configured interval however many consumers
are up.

In the dashboard, a canvas's **Health** page reads both histories over a selected window (1 h / 6 h
/ 24 h / 7 d): one card per server showing its status, the connection count *at its last report* in
that window and the peak, plus two charts — throughput from the per-report upload/download deltas,
and connections against the high-water mark. The page follows the control plane's streams and
updates in place, without reloading. A server's status badge and the summary counts follow status
changes at once. New report points reach the charts every 2 s on the 1 h window, every 10 s on 6 h,
30 s on 24 h and 2 min on 7 d (longer windows repaint less often). Points that fall out of the
selected window drop off even when a server has gone silent. Every number is still a stored report,
so an `Offline` server keeps showing what it last sent until that ages out of the window. Each
card's *Pod events* tab lists the events of every pod on that server, including the `message` a
`Failed` row carries. It is streamed only while the tab is open, shows at most the newest 500
events per pod, and new events appear within about 2 s.

## Certificates

A TLS client pod resolves to one `certificate` row per `(sni, acme_directory)`, and relay listeners
over TLS or QUIC use the internal CA instead. Both matter here for the same reason: a snapshot pins
the version of every certificate its TOML references, so renewing one produces a new revision for
each server serving it even though the TOML bytes do not change, and a pod whose certificate is not
issued yet is left out of its server's config as an `invalid_pods` entry without failing the
server. See [ACME with DNS](/features/acme-dns/) for the issuance pass, the DNS providers and the
relay CA.

## Inspecting a derived config

`manage-tool` prints the exact TOML the master derived for one server:

```sh
cargo run -p manage-tool -- --database-url "$GURU_DATABASE_URL" \
  orchestration export-config --server <orchestration_server id>
```

The same model backs standalone workers: the printed file can be handed to
`guru-worker --config`.

The dashboard shows the same TOML — server panel, *Worker config* — next to the rollout reading it
belongs to: the desired, in-flight and applied revisions with their timestamps, `derive_error` and
`apply_error`, the servers this one is waiting for (resolved to names), and `invalid_pods` as a
table of pod, listen address and error. `ForgetServerApplied` sits there too, Admin-only and behind
a confirmation, since it declares the server dead while it may still be serving.

## Listener identity

Convergence matches listeners by `(server, port, protocol)`, never by address. A server's
addresses are learned from its worker (or pinned by an operator) and only decide what a relay
*dials*; changing one re-derives every destination that points at the server without touching
the seamless-switch protocol, because the listener the dependants reference did not change. A
per-pod derivation failure names the pod's listener as `bind:port` (`[::]:port` for a wildcard
bind), and a relay whose target server has no known address yet is reported there as
`server … has no address yet`.

A pod whose listener moves — a new port — serves both listeners until every dependant has
switched. The worker keys listeners by tag, and a pod's tag is its id, so the held one appears in
the TOML under its socket, `<pod id> (9443/relay_tcp)`, and leaves with that tag once nothing
points at it. A protocol change on a port some server still dials has no seamless path: the edit
is refused, and the pod needs a new port instead.

## Forwarding shapes

Each pod becomes one `[[forwarding]]`: its listener (`raw`, `tls`, or an inbound relay) and its
route. A worker that reports the `route_table` capability receives the route as a table of groups
and upstreams; an older worker receives an inline tree, with weights as repeated members, a failover
as `fallback` and a sticky balance as `ip_hash`. PROXY protocol v1 and v2 are supported on both
ends. See the [configuration reference](/reference/configuration/#route-tables).
