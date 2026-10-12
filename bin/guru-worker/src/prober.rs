//! Retrying dead hops in the background.
//!
//! A dead hop used to be retried by the first client connection let through once
//! [`RETRY_AFTER`](crate::liveness::RETRY_AFTER) had passed, and that client waited
//! out the retry, however long a hop that is still dead takes to fail. The prober
//! takes those retries instead: every [`PROBE_TICK`] it claims the due retry of each
//! dead hop (see [`Liveness::due`]) and probes it on its own task; a hop that answers
//! is alive again for the next connection, one that does not waits out another
//! [`RETRY_AFTER`](crate::liveness::RETRY_AFTER).

use crate::liveness::Liveness;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::{Instant, MissedTickBehavior};
use tokio_util::sync::CancellationToken;

/// How often the prober looks for dead hops whose retry is due.
pub const PROBE_TICK: Duration = Duration::from_secs(1);

/// Probes every dead hop of `liveness` once its retry is due, until `cancel`.
pub async fn run(liveness: Arc<Liveness>, cancel: CancellationToken) {
    let mut tick = tokio::time::interval(PROBE_TICK);
    tick.set_missed_tick_behavior(MissedTickBehavior::Skip);
    loop {
        tokio::select! {
            () = cancel.cancelled() => return,
            _ = tick.tick() => {}
        }
        for (attempt, probe) in liveness.due(Instant::now()) {
            let cancel = cancel.clone();
            tokio::spawn(async move {
                let label = probe.label().to_string();
                tokio::select! {
                    // Dropping the attempt unsettled gives the retry back.
                    () = cancel.cancelled() => {}
                    probed = probe.probe() => match probed {
                        Ok(()) => {
                            tracing::info!(hop = %label, "next hop answers again (probe)");
                            attempt.succeeded();
                        }
                        Err(error) => {
                            tracing::debug!(hop = %label, %error, "dead next hop still does not answer");
                            attempt.failed(Instant::now());
                        }
                    }
                }
            });
        }
    }
}
