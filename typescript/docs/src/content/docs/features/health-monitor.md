---
title: Health Monitor
description: What a worker reports, how the master turns it into server and pod status, and what the dashboard's Health page shows.
---

Every worker keeps one stream open to the control plane and pushes a report into it on a fixed
interval. That stream is the whole health monitor: it decides whether a server is `Online`, it
carries the traffic counters the dashboard charts, and its silence is what marks a server `Offline`.

## What a worker reports

One `HealthReport` per interval over `ReportHealth`, a bidirectional stream on the `WorkerAgent`
service authenticated with the worker's current refresh key; the master answers every report it
recorded:

| Field | Meaning |
|---|---|
| `running_revision` | The config revision the worker believes it is running. `0` means a fresh install or a wiped state directory. |
| `upload_bytes`, `download_bytes` | Bytes **since the previous report**, summed over every forwarding. |
| `current_connections` | The open connections **at the moment of the report** — a gauge, not a per-interval quantity. |
| `max_connections` | The high-water mark since the previous report. It restarts at the connections still open, not at zero. |
| `pods` | One entry per running forwarding: its tag (the pod's id) and an optional error. |
| `reported_addresses` | Only present on a report whose address discovery changed. |

The interval is the master's: `Register` answers with `health_report_interval_secs` from the stored
`orchestration` config (15 s by default), and the worker's own `--health-interval` applies only if
the master sends `0`. The stream opens as soon as the worker has registered — it does not wait for
the config stream — and the first report goes out immediately.

The report task lives exactly as long as the session. If it ends for any reason the worker tears the
session down and reconnects with capped exponential backoff, which means a fresh `Register`, a new
refresh key and a re-negotiated interval. Counters do not survive that: the deltas of the lost
interval are dropped and the connection gauge starts again from whatever is open.

The master's replies are also how the worker knows the whole path still works. Its HTTP/2 pings only
reach the first hop, which a proxy such as Cloudflare answers for a path whose far end is gone, so
the worker counts on data the master itself sent: a session whose reports go unanswered for three
intervals, or whose config stream carries nothing — not even the keep-alive it gets every
`stream_keepalive_secs` — for three of those, is given up and started over. A master built before the
replies announces `stream_keepalive_secs` as `0`, and then neither watchdog runs. After a session
that lived five minutes, the reconnect backoff starts over at one second.

## What the master stores

Each accepted report is one transaction. It is fenced on the worker's refresh-key generation — a
report from a superseded session is rejected, not recorded — and it writes:

- `last_health_report_at`, `last_seen_at` and the current `health_status` on the server row;
- one `server_health_record` row: status, report time and the four counters;
- one `pod_health_record` row per pod the report resolves.

A reported tag is resolved against the server's `applied` snapshot first and its `desired` snapshot
second; a tag in neither is skipped and writes nothing. When one pod is held under two listeners —
which happens while its dependants switch to a new port — the two entries collapse into a single
row carrying the **worst** of the two statuses.

Once that write has committed, the service publishes the record on the live bus — a separate step,
not part of the transaction — which is what the dashboard's streams and a canvas's `WatchGraph`
subscribers read.

## Server status

`Online`, `Degraded` and `Offline`. A new server starts `Offline`: nobody has heard from it yet.
Each accepted report re-evaluates the verdict, in this order:

1. the server's config view carries an `apply_error`, or any pod is recorded as failed → `Degraded`;
2. `running_revision` differs from the applied revision → `Degraded` (this is what catches a worker
   reporting `0`);
3. a newer revision is desired and was published longer ago than `degraded_grace_secs` (60 s) →
   `Degraded`;
4. otherwise `Online`.

`AckConfig` writes `Online`/`Degraded` directly as well, so a failed apply shows up without waiting
for the next report.

`Offline` has two enforcement points, both driven by `health_report_interval_secs ×
health_offline_after_intervals` — **45 s** at the defaults:

- the master reads the stream with that value as a per-report timeout and ends a silent stream with
  `no health report within 45s`;
- the `sweep_liveness` pass flips every non-offline server whose last accepted report is older than
  the threshold.

A stream that merely ends is neither: a proxy cuts streams of its own accord, and a worker that
reconnects within the threshold never was offline. The stream's end lets the worker's next session in;
the sweep judges a worker that does not come back.

Going `Offline` also clears the server's session lease and bumps its watch epoch, so the config
stream ends and the worker's next `Register` is accepted immediately instead of being refused as a
duplicate session. A status flip writes a `server_health_record` row with zero counters and does not
move `last_health_report_at`.

## Pod status

`Ready`, `Deploying` or `Failed`, evaluated per report from the snapshots rather than from wording:

- **Failed** — the report carried an error for the pod, or the last ack recorded it as failed. The
  error text becomes the row's message.
- **Deploying** — the pod's entry differs between `desired` and `applied`, or exists only in
  `desired`.
- **Ready** — resolved, no failure, nothing pending.

`Deploying` is also written the moment a derivation publishes a revision, with the message
`revision <n> published`, for every pod whose forwarding changed — including a certificate renewal
where the TOML is byte-identical but the material on disk is new.

## Periodic passes

Both run in `--mode consumer`; `--mode cron` only publishes their signals on a fixed cadence and
opens no database.

| Pass | Signal cadence | Config key (default) | What it does |
|---|---|---|---|
| `sweep_liveness` | 30 s | `liveness_interval_secs` (30) | Marks silent servers `Offline`, and revokes the session of an already-offline server whose registration never reported. |
| `trim_health_history` | 300 s | `health_retention_interval_secs` (300) | Deletes records older than `server_health_ttl_secs` / `pod_health_ttl_secs` (7 days each). |

Each consumer claims the tick in one `orchestration_job_run` row before working, so the pass runs
once per configured interval however many consumers are up. The claim compares scheduling ticks, not
wall clock: an interval at or below the signal's cadence means "run on every signal", a larger one
slows the pass down fleet-wide.

## Reading it back

Two unary methods read stored history — `ListServerHealthHistory` (inclusive time range, oldest
first, unlimited) and `ListPodHealthHistory` (newest first, `limit` 0 meaning 500) — and two
server-streaming methods follow it live: `WatchServerHealth` and `WatchPodHealth` open with a
snapshot from `since` (an hour ago when empty), then push one event per committed record, with a
keep-alive every `stream_keepalive_secs`. Both streams subscribe to the bus before reading history,
so nothing falls between the snapshot and the first event, and de-duplicate on the newest report
time they have seen, which is also how a reconnect avoids re-delivering.

Everything served is a stored row. A live stream makes the numbers arrive without a reload; it never
measures anything itself, so an `Offline` server keeps showing whatever it last sent.

## The Health page

![The Health page of a canvas: a Live indicator and the window selector on Last hour, four summary cards counting online, degraded and offline servers and how many have reports, then a server card with its Online badge, connections at its last report, the peak, the time of that report and the report count, above the throughput and connections charts](/img/features/health-page.avif)

`/canvas/<canvas-id>/health` is live: it opens a `WatchGraph` stream for canvas membership plus one
`WatchServerHealth` stream per server, and reopens a dropped stream from the last record it received
so only the gap is refetched. The window selector offers 1 h / 6 h / 24 h / 7 d.

The summary strip counts servers by status and how many produced at least one record in the window.
Each server card shows its status badge, the connections **at its last report** in the window, the
peak of the high-water marks, the time of that report and how many reports it covers, then two
charts over the same x axis: throughput stacked from the per-report byte deltas, and connections
with the high-water mark behind the gauge.

The **Pod events** tab lists every pod running on that server, newest first, with status, time and
message — the `message` of a `Failed` row is the worker's own error text. It is mounted only when
opened, because it costs one stream per pod.

## Configuration

Stored under the `orchestration` config key and read once at master startup, so a change takes
effect on restart:

| Key | Default | Effect |
|---|---|---|
| `health_report_interval_secs` | 15 | Interval handed to workers; sizes the offline threshold. |
| `health_offline_after_intervals` | 3 | Multiplier for the offline threshold (45 s at the defaults). |
| `degraded_grace_secs` | 60 | How long a server may lag the desired revision before `Degraded`. |
| `server_health_ttl_secs` | 604800 (7 d) | Retention of server records. |
| `pod_health_ttl_secs` | 604800 (7 d) | Retention of pod records. |
| `liveness_interval_secs` | 30 | Execution cadence of `sweep_liveness`. |
| `health_retention_interval_secs` | 300 | Execution cadence of `trim_health_history`. |
| `stream_keepalive_secs` | 15 | Keep-alive on the dashboard's `Watch*` streams and on a worker's `WatchConfig`. |

On the worker, `--health-interval` / `GURU_HEALTH_INTERVAL_SECS` (default 15, minimum 1) is a
fallback for a master that sends no interval. See the
[configuration reference](/reference/configuration/) for the full document and
[Rollout](/reference/rollout/) for how a revision reaches the worker in the first place.
