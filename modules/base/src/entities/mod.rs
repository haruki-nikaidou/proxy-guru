//! Persistence layer: data types and the processors that read/write them.
//!
//! Entities are split by backing store; this module owns the one shared table,
//! so only [`surreal`] exists here. A feature module that caches or holds
//! ephemeral state adds its own `redis` submodule alongside it (see
//! `AGENTS.md`).
//!
//! Each query or command is a small input struct with a `Processor`
//! implementation, so persistence logic stays testable and composable.

pub mod surreal;
