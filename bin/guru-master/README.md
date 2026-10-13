# `guru-master`

The main application server binary. It composes the business modules under
[`modules/`](../../modules) and runs them behind one or more **workers**.

## Workers

A worker is a single run mode chosen at startup (typically from environment
variables). One binary, several modes — pick the mode per deployment so each
responsibility scales independently:

| Mode             | Responsibility                                              |
| ---------------- | ----------------------------------------------------------- |
| `dashboard_grpc` | Serve the operator-facing `rpc` services over HTTP/2.       |
| `workers_grpc`   | Serve the worker-facing agent API over HTTP/2.              |
| `consumer`       | Run the Iggy event handlers: canvas derivation and the notification fan-out. |
| `notifier`       | Deliver notifications; exactly one instance (PostgreSQL advisory lock). |
| `cron`           | Run every periodic job in-process as a `wakuwaku::CronJob`. |

Running the same image in different modes keeps build and deployment uniform.

Every `cron` run claims its tick (`ClaimJobRun` on the tick's `fired_at`) in the
database before working — at most once per configured interval, fleet-wide — so
several `cron` processes are a standby, not a duplicate.

## Responsibilities

- Load configuration and construct shared dependencies (the PostgreSQL pool and
  `base::db::Db` handle, the Iggy client and publisher, Redis). Iggy is
  mandatory in every mode: startup fails when `IGGY_URL` is unset or
  unreachable, and creates the stream `guru` and every module's topics.
- Construct each module's services, handlers and jobs with one
  `wakuwaku::ServiceBuilder` per mode (`compose_subservice`,
  `compose_grpc_service`, `iggy_consumer`, `cron_job`).
- Mount the selected mode and run until a shutdown signal is received.
- Set up observability (tracing).

## What does *not* belong here

Business logic. The binary is wiring only: it selects a worker, builds
dependencies, and hands control to the modules. Keep domain behaviour in the
modules' `services`, `entities`, and `hooks`.
