//! # `base` — the foundational module
//!
//! `base` is the reference/shared module of this workspace. Every business
//! feature lives in its own `modules/<name>` crate that mirrors the layout of
//! this crate and depends on `base` for the types and helpers shared across the
//! whole application (common entities, error types, config primitives,
//! utilities, and so on).
//!
//! ## Module layout (the convention every module follows)
//!
//! - [`entities`] — persistence layer. Plain data types plus the
//!   `Processor` implementations that read and write them. In this crate that
//!   is [`entities::surreal`], which owns the shared `app_config` table.
//! - [`services`] — business logic. Stateful `Processor`s that own their
//!   dependencies (database, Redis, message queue, other services) and
//!   orchestrate entities to fulfil a use case.
//! - [`rpc`] — the transport edge. gRPC service implementations that translate
//!   protobuf requests into service/entity calls and back.
//! - [`events`] — AMQP message payloads this module publishes or consumes,
//!   together with their routing.
//! - [`hooks`] — background reactors: AMQP consumers, cron jobs, and event
//!   loggers that run outside the request path.
//! - [`config`] — strongly typed configuration for the module, stored in the
//!   database under a stable key and loaded through
//!   [`services::config::ConfigStore`].
//! - [`utils`] — small, dependency-light helpers local to the module.
//!
//! [`db`] is the exception to the mirror: it lives here alone, because the handle it wraps
//! is shared by every module.
//!
//! See `AGENTS.md` at the workspace root for the full authoring guide.

#![deny(clippy::unwrap_used)]
#![deny(clippy::expect_used)]
#![deny(clippy::panic)]
#![warn(clippy::arithmetic_side_effects)]

pub mod config;
/// Base-only: the bounded database handle every module's services hold. Feature modules
/// mirror the layout below, but none of them declares a `db` module of its own.
pub mod db;
pub mod entities;
pub mod events;
pub mod hooks;
pub mod rpc;
pub mod services;
pub mod utils;
