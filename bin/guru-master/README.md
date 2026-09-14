# `guru-master`

The main application server binary. It composes the business modules under
[`modules/`](../../modules) and runs them behind one or more **workers**.

## Workers

A worker is a single run mode chosen at startup (typically from environment
variables). One binary, several modes — pick the mode per deployment so each
responsibility scales independently:

| Worker         | Responsibility                                              |
| -------------- | ----------------------------------------------------------- |
| gRPC           | Serve each module's `rpc` services over HTTP/2.             |
| AMQP consumer  | Drive each module's `hooks` from the message queue — both event reactions and every periodic job. |
| Cron scheduler | Publish one execution signal per due periodic job.          |
| REST / webhook | Expose HTTP endpoints for third-party callbacks.            |

Running the same image in different modes keeps build and deployment uniform.

The scheduler and the executor are deliberately different processes. `cron` is a
clock and nothing else: it reads no configuration, opens no database connection,
and publishes a signal when a job comes due. The consumer that receives the
signal claims the run — at most once per configured interval, fleet-wide — and
then does the work, so periodic jobs scale and fail over exactly like
event-driven ones, and a stuck job cannot take the clock down with it.

## Responsibilities

- Load configuration and construct shared dependencies (SurrealDB connection,
  AMQP pool). The broker is mandatory in every mode: periodic work is a message,
  so startup fails when `AMQP_URI` is unset or unreachable, and a broker outage
  stalls derivation, liveness and renewal until it returns. `cron` is the one
  mode that skips the database entirely — it publishes signals and reads nothing.
- Construct each module's services and hooks, injecting those dependencies.
- Mount the selected worker and run until a shutdown signal is received.
- Set up observability (tracing / OpenTelemetry) and health checks.

## What does *not* belong here

Business logic. The binary is wiring only: it selects a worker, builds
dependencies, and hands control to the modules. Keep domain behaviour in the
modules' `services`, `entities`, and `hooks`.
