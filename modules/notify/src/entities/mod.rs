//! Persistence layer: the settings rows, and the fan-out's own memory.
//!
//! - [`db`] — PostgreSQL rows and the compile-time-checked queries on them:
//!   [`db::setting`] holds what operators asked for, [`db::state`] holds what
//!   was last announced about a subject.
//!
//! Nothing here is cached in Redis: a notification is rare, and a setting read
//! per notice is one point read.

pub mod db;
