//! Background reactors that run outside the request path.
//!
//! Every periodic job is an AMQP consumer here, driven by an execution signal
//! the `cron` scheduler publishes; [`schedule`] holds the scheduler's clock and
//! the claim that keeps a pass at-most-once per interval. [`live`] and
//! [`shell`] are the Redis pub/sub subscribers: the live dashboard bus and the
//! remote-shell relay.

pub mod acme;
pub mod country;
pub mod derive;
pub mod health;
pub mod live;
pub mod schedule;
pub mod shell;
