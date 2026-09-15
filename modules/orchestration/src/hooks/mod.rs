//! Background reactors that run outside the request path.
//!
//! Every periodic job is an AMQP consumer here, driven by an execution signal
//! the `cron` scheduler publishes; [`schedule`] holds the scheduler's clock and
//! the claim that keeps a pass at-most-once per interval.

pub mod acme;
pub mod derive;
pub mod health;
pub mod live;
pub mod schedule;
