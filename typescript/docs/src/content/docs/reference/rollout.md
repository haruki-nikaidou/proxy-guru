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

A rollout is only half the story: whether a worker actually runs what was published is answered by
the health stream. Every worker pushes a `HealthReport` per interval, each report becomes one server
record plus one record per pod, and the verdict it produces — `Online`, `Degraded`, `Offline` for a
server, `Ready`, `Deploying`, `Failed` for a pod — is what the dashboard shows. Two of those
verdicts are written by this pipeline rather than by a report: `AckConfig` marks a server `Degraded`
when an apply failed, and a derivation marks every changed pod `Deploying` the moment it publishes.
See [Health Monitor](/features/health-monitor/) for the fields, the thresholds and the passes.

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
