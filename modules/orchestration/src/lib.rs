//! # `orchestration` — canvases, servers, nodes and worker rollout
//!
//! This module owns the control plane of the proxy fabric: operators build a
//! **canvas** of servers and nodes, the module validates that topology, derives one
//! `guru-worker` config per server from it, and streams every new revision to the
//! workers that registered for it.
//!
//! ## Module layout
//!
//! - [`entities`] — persistence layer. SurrealDB row types plus one `Processor` per
//!   query in [`entities::surreal`].
//! - [`services`] — business logic: canvas/server/node/edge CRUD, the topology
//!   checker, the config deriver, convergence and the worker agent.
//! - [`rpc`] — the transport edge: the operator `Orchestration` service and the
//!   `WorkerAgent` service workers talk to, plus their middleware.
//! - [`events`] — AMQP payloads this module publishes or consumes.
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
//! 1. A mutation bumps the *root* canvas's `orchestration_canvas.generation` in
//!    its own transaction (`fn::orchestration_touch`) and publishes `CanvasDirty`.
//!    Canvases nest through `CanvasImport`/`CanvasExport` nodes; the tree is the
//!    unit of validation and derivation, and an import node's ports mirror its
//!    target's export nodes.
//! 2. [`hooks::derive`] re-derives the whole tree and commits only while the
//!    root's generation still matches; a cron sweep catches anything the message
//!    missed.
//! 3. Derivation is convergent, not sequenced: a server switches a destination
//!    only once the target's `applied` snapshot serves it, and keeps serving a
//!    listener for as long as any snapshot still points at it.
//! 4. A worker stream promotes `desired` to `in_flight` with one conditional
//!    update, and `AckConfig` promotes `in_flight` to `applied` — per pod: a pod
//!    the worker could not apply keeps its previous shape in the stored mix.
//! 5. Workers stream health reports; certificates (ACME for Entries, the
//!    internal CA for relay TLS/QUIC) are pinned by version in every snapshot
//!    and delivered with the revision.

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
