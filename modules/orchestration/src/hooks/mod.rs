//! Background reactors that run outside the request path.
//!
//! [`derive::CanvasDirtyHandler`] is the Iggy event handler `--mode consumer`
//! runs. Every periodic job is a `CronJob` run by `--mode cron`
//! ([`derive`], [`health`], [`acme`], [`country`]); each claims its run with
//! `ClaimJobRun::for_tick` first, so several cron replicas still run a pass at
//! most once per configured interval fleet-wide. [`live`] and [`shell`] are the
//! Redis pub/sub subscribers: the live dashboard bus and the remote-shell relay.

pub mod acme;
pub mod country;
pub mod derive;
pub mod health;
pub mod live;
pub mod shell;
