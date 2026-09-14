---
title: Rollout Model
description: How a canvas edit becomes a config revision applied by a worker.
---

## From edit to applied config

Mutations bump the canvas generation and publish `CanvasDirty`. The derivation hook re-derives the
whole canvas; the periodic `derive_stale_canvases` signal, consumed by the same hook, catches
anything a lost message missed. Both triggers are broker messages, so the backstop is not
broker-independent: a canvas whose `CanvasDirty` was lost waits for delivery to resume, and a
master with no broker reachable derives nothing at all.

Every server has **one config view** holding three snapshots — `desired`, `in_flight` and
`applied`. A worker stream promotes `desired` → `in_flight`, and its `AckConfig` promotes
`in_flight` → `applied`.

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

## Forwarding shapes

Each forwarding pairs a listener with a destination:

- **Listener** — `raw`, `tls`, or an inbound relay.
- **Destination** — a direct **exit**, a **relay** to another node over TLS-over-TCP or QUIC, or a
  **load-balance** group.

PROXY protocol v1 and v2 are supported on both ends.
