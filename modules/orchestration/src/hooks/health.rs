//! Health crons: liveness by silence, and retention of raw records.
//!
//! A worker's report stream closing marks its server `Offline` at once; the
//! sweep is the backstop for a master that never saw the close (it restarted,
//! or the connection died without a FIN) and for servers that never reported.

use crate::services::health::{HealthService, SweepLiveness, TrimHealthHistory};
use chrono::Utc;
use kanau::processor::Processor;
use std::time::Duration;
use tokio_util::sync::CancellationToken;

/// Flips silent servers to `Offline` every `interval`, until `shutdown`.
pub async fn run_liveness_sweep(
    health: HealthService,
    interval: Duration,
    shutdown: CancellationToken,
) {
    let mut ticker = tokio::time::interval(interval);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        tokio::select! {
            _ = shutdown.cancelled() => return,
            _ = ticker.tick() => {}
        }
        match health.process(SweepLiveness { now: Utc::now() }).await {
            Ok(flipped) if !flipped.is_empty() => {
                tracing::info!(
                    servers = flipped.len(),
                    "servers went offline for lack of reports"
                );
            }
            Ok(_) => {}
            Err(e) => tracing::error!(error = %e, "liveness sweep failed"),
        }
    }
}

/// Trims health history past its TTLs every `interval`, until `shutdown`.
pub async fn run_health_retention(
    health: HealthService,
    interval: Duration,
    shutdown: CancellationToken,
) {
    let mut ticker = tokio::time::interval(interval);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        tokio::select! {
            _ = shutdown.cancelled() => return,
            _ = ticker.tick() => {}
        }
        if let Err(e) = health.process(TrimHealthHistory { now: Utc::now() }).await {
            tracing::error!(error = %e, "health retention failed");
        }
    }
}
