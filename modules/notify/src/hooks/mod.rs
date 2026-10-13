//! Background reactors: the two Iggy event handlers this module runs.
//!
//! - [`fanout`] runs in `--mode consumer`, alongside the orchestration hooks:
//!   it consumes `orchestration`'s health facts and decides the audience.
//! - [`delivery`] runs in `--mode notifier`, of which there is exactly one
//!   instance: it consumes this module's own notices and sends them.
//!
//! Neither is gated on [`orchestration::entities::db::job_run::ClaimJobRun`]:
//! these are event hooks, not periodic ones, and the consumer's stored offsets
//! are what decide that an event is handled (at least) once.

pub mod delivery;
pub mod fanout;
