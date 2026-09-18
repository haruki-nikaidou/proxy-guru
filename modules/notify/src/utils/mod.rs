//! Small, self-contained helpers shared across this module.
//!
//! - [`message`] — rendering one notice into a subject and a body, in each
//!   supported language. Pure: a notice plus a language in, two strings out.
//! - [`secret`] — the two channel secrets, read from the environment.
//! - [`lock`] — the advisory lock that keeps the `notifier` mode single.

pub mod lock;
pub mod message;
pub mod secret;
