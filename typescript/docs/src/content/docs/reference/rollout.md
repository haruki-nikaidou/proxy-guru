---
title: Rollout Model
description: How a canvas edit becomes a config revision applied by a worker.
---

## From edit to applied config

Mutations bump the canvas generation and publish `CanvasDirty`; a worker's ack, registration or
changed address report bumps a counter on its own server's config view instead, so a fleet
acknowledging at once never contends on the canvas row. The derivation hook re-derives the whole
canvas; the periodic `derive_stale_canvases` signal, consumed by the same hook, catches anything a
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
canvas mutation ──▶ CanvasDirty ──┐
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
the server `Degraded` and the affected nodes `Failed` with the worker's message. A revision that
cannot be applied at all (unparsable TOML, unwritable certificate files) changes nothing on the
worker and is recorded as an `apply_error`.

## Health

Every worker streams one `HealthReport` per `--health-interval` (default 15 s) over `ReportHealth`:
the running revision, upload/download bytes and connection counts since the previous report (max
is the high-water mark), and one `PodStatus` per running forwarding. Each report becomes one
`server_health_record` row and one `node_health_record` row per node the report touches (the pod
and every node it was derived through — Entry/Relay on the listen side, exits, relays and load
balancers on the destination side; a node shared by several pods gets one row with the worst
status). Node statuses: `Ready` (the pod runs what `desired` asks), `Deploying` (a newer revision
involving the node is derived but not applied yet — written the moment a derivation publishes it),
`Failed` (the pod failed to apply or to run). Server statuses: `Online`, `Degraded` (lagging
`desired` past the grace period, or the last acknowledged revision failed for some pod), `Offline`
(the health stream closed, or no report for three intervals — the `sweep_liveness` pass). Both
histories are raw and trimmed by the `trim_health_history` pass;
`ListServerHealthHistory` / `ListNodeHealthHistory` read a time range.

Both passes run in `--mode consumer`, on a signal the `cron` scheduler publishes when the job comes
due. The scheduler holds no state beyond its own clock; the consumer claims each run in one
`orchestration_job_run` row, so the pass runs once per configured interval however many consumers
are up.

In the dashboard, a canvas's **Health** page reads both histories over a selected window (1 h / 6 h
/ 24 h / 7 d): one card per server showing its status, the connection count *at its last report* in
that window and the peak, plus two charts — throughput from the per-report upload/download deltas,
and connections against the high-water mark. Nothing on the page is live: every number is a stored
report, so an `Offline` server still shows whatever it last sent. Each card's *Pod events* tab
lists that server's pods' node-health rows, including the `message` a `Failed` row carries, and a
canvas-wide *Node events* card covers the other side of the recording rule — the entries, relays,
exits and load balancers a report was derived through. Both are fetched only once opened.

## Certificates

An Entry with a `tls` block (`sni`, DNS provider, `domain_id`, optional ACME directory — empty means
the stored `orchestration` config's `default_acme_directory`, Let's Encrypt) resolves to one
`certificate` row per
`(sni, acme_directory)`. The ACME pass creates the row, runs a DNS-01 challenge through the DNS
provider (Cloudflare: `domain_id` is the zone id; Vercel: `domain_id` is the domain, the provider's
`account_id` is the team id), waits for the TXT record to be visible on public resolvers, and stores
the chain and the encrypted key. Until then the pod behind the Entry is an `invalid_pods` entry
("certificate for <sni> is pending"), never a server failure. Renewal happens 30 days before expiry;
a renewal bumps the row's version, and because every snapshot pins the versions its TOML references,
every server serving that certificate gets a new revision with the same file paths and new
material.

Relay listeners over TLS or QUIC use the **internal CA** instead: `manage-tool orchestration init-ca`
creates it once, the derivation hook issues a 30-day leaf per relay pod (SAN
`<pod-key>.relay.guru.internal`), the `rotate_relay_certificates` pass rotates leaves ten days
before expiry, and workers verify relay peers only against `certs/ca.pem` (`relay_ca` in the TOML).
DNS providers, certificates and the CA are Admin-managed through the operator API
(`CreateDnsProvider`, `ListCertificates`, `RetryCertificate`, …); secrets are encrypted with
`GURU_MASTER_KEY` and never returned.

The dashboard's **TLS** page is the Admin-facing side of this: DNS providers are created, edited
(an empty API token keeps the stored one) and deleted there, and the certificate table shows each
row's status, resolved provider, validity window with an expiry hint, and the `last_error` of a
failure, with *Retry* (clear a failure or force a renewal) and *Delete* per row. It never issues a
certificate: a row appears once the derivation pass reads an Entry's `tls` block, which is edited
on the Entry node in the canvas editor.

## Inspecting a derived config

`manage-tool` prints the exact TOML the master derived for one server:

```sh
cargo run -p manage-tool -- \
  --address ws://127.0.0.1:8000 --username root --password root \
  --namespace guru --database guru \
  orchestration export-config --server <orchestration_server key>
```

The same model backs standalone workers: the printed file can be handed to
`guru-worker --config`.

The dashboard shows the same TOML — server panel, *Worker config* — next to the rollout reading it
belongs to: the desired, in-flight and applied revisions with their timestamps, `derive_error` and
`apply_error`, the servers this one is waiting for (resolved to names), and `invalid_pods` as a
table of pod, listen address and error. `ForgetServerApplied` sits there too, Admin-only and behind
a confirmation, since it declares the server dead while it may still be serving.

## Forwarding shapes

## Listener identity

Convergence matches listeners by `(server, port, protocol)`, never by address. A server's
addresses are learned from its worker (or pinned by an operator) and only decide what a relay
*dials*; changing one re-derives every destination that points at the server without touching
the seamless-switch protocol, because the listener the dependants reference did not change. A
per-pod derivation failure names the pod's listener as `bind:port` (`[::]:port` for a wildcard
bind), and a relay whose target server has no known address yet is reported there as
`server … has no address yet`.

A pod whose listener moves — a new port, or a relay protocol change re-rolling a landing port —
serves both listeners until every dependant has switched. The worker keys listeners by tag, so the
held one appears in the TOML under its socket, `osaka-hop (9443/relay_tcp)`, and leaves with that
tag once nothing points at it.

Each forwarding pairs a listener with a destination:

- **Listener** — `raw`, `tls`, or an inbound relay.
- **Destination** — a direct **exit**, a **relay** to another node over TLS-over-TCP or QUIC, or a
  **load-balance** group.

PROXY protocol v1 and v2 are supported on both ends.
