//! The process log filter follows the applied config. A test binary of its own:
//! the logger is global to the process, and no other test may install one.

#![allow(clippy::unwrap_used, clippy::panic, clippy::expect_used)]

use tracing::Level;

#[test]
fn an_applied_log_level_takes_effect_at_once() {
    guru_worker::init_tracing("info");
    assert!(tracing::enabled!(Level::INFO));
    assert!(!tracing::enabled!(Level::DEBUG));

    guru_worker::apply_log_level("debug");
    assert!(tracing::enabled!(Level::DEBUG));

    guru_worker::apply_log_level("warn");
    assert!(tracing::enabled!(Level::WARN));
    assert!(!tracing::enabled!(Level::INFO));

    // The same directive again changes nothing.
    guru_worker::apply_log_level("warn");
    assert!(!tracing::enabled!(Level::INFO));
}
