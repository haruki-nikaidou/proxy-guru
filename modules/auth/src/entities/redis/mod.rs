//! Redis entities: cached and ephemeral key/value state.
//!
//! Put your Redis-backed types here — one submodule per key kind (sessions,
//! one-time tokens, rate-limit counters, and so on). There are none yet.
//!
//! wakuwaku 1.0 runs Redis as a wakuwaku-entity data source (the `redis`
//! feature of `wakuwaku`, `wakuwaku::entity::redis`): a key kind is an `Entity`
//! marker, and each read or write is a `Query` + `Execute` pair, the same shape
//! as the PostgreSQL queries in [`db`](super::db). Enable the feature when the
//! first one lands; until then the workspace uses the raw `redis` crate for
//! pub/sub only.
