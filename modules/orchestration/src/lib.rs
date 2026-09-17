//! # `orchestration` — canvases, the pod graph and worker rollout
//!
//! This module owns the control plane of the proxy fabric: operators edit the
//! **pod graph** of a tree of canvases, the module checks it, derives one
//! `guru-worker` config per server from it, and streams every new revision to the
//! workers that registered for it.
//!
//! ## Module layout
//!
//! - [`entities`] — persistence layer. PostgreSQL row types plus one `Processor`
//!   per query in [`entities::db`].
//! - [`services`] — business logic: the graph edit path (read, check, apply a
//!   batch), canvas and server management, derivation through `guru_topology`,
//!   convergence and the worker agent.
//! - [`rpc`] — the transport edge: the operator `Orchestration` service and the
//!   `WorkerAgent` service workers talk to, plus their middleware.
//! - [`events`] — AMQP payloads this module publishes or consumes, and the live
//!   messages it fans out over Redis.
//! - [`hooks`] — background reactors: the derivation hook and its sweep, relay
//!   leaf rotation, the health liveness sweep and retention, ACME renewal.
//! - [`config`] — [`config::OrchestrationConfig`], loaded from the database
//!   under the `"orchestration"` key: health intervals and retention, ACME and
//!   relay-certificate knobs.
//! - [`utils`] — record-id conversion and master-key encryption of stored secrets.
//!
//! ## How a change reaches a worker
//!
//! Rows are edited in place. What a worker runs lives in one
//! `orchestration_server_config_view` row per server, holding three immutable
//! snapshots: `desired` (latest derivation), `in_flight` (handed to the worker,
//! not yet acked) and `applied` (what it runs).
//!
//! 1. An edit is one `ApplyGraph` batch: applied to the tree in memory, checked
//!    as a whole, and written in one transaction that bumps the *root* canvas's
//!    `generation` against the generation it read, then `CanvasDirty` is
//!    published. Canvases nest by `parent`; the tree is the unit of checking and
//!    derivation, and edges cross canvases freely.
//! 2. [`hooks::derive`] re-derives the whole tree and commits only while the
//!    root's generation still matches; a cron sweep catches anything the message
//!    missed.
//! 3. Derivation is convergent, not sequenced: a server switches a destination
//!    only once the target's `applied` snapshot serves it, and keeps serving a
//!    listener for as long as any snapshot still points at it.
//! 4. A worker stream promotes `desired` to `in_flight` with one conditional
//!    update, and `AckConfig` promotes `in_flight` to `applied` — per pod: a pod
//!    the worker could not apply keeps its previous shape in the stored mix.
//! 5. Workers stream health reports; certificates (ACME for TLS client pods, the
//!    internal CA for TLS and QUIC relay pods) are pinned by version in every
//!    snapshot and delivered with the revision.

#![deny(clippy::unwrap_used)]
#![deny(clippy::expect_used)]
#![deny(clippy::panic)]
#![warn(clippy::arithmetic_side_effects)]

pub mod config;
pub mod entities;
pub mod events;
pub mod hooks;
pub mod rpc;
pub mod services;
pub mod utils;
