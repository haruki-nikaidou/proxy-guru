//! PostgreSQL entities and queries.
//!
//! - [`setting`] — the three settings tables and the recipient resolution the
//!   fan-out reads.
//! - [`state`] — `notify_server_state` / `notify_pod_state`: what was last
//!   announced about one subject, which is what makes a repeated report of the
//!   same status notify nobody.

pub mod setting;
pub mod state;
