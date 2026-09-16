//! Health crons: liveness by silence, and retention of raw records.
//!
//! A worker's report stream closing marks its server `Offline` at once; the
//! sweep is the backstop for a master that never saw the close (it restarted,
//! or the connection died without a FIN) and for servers that never reported.
//! It also revokes the watch session of a server that is already `Offline` when
//! the registration holding it never reported: nothing else would ever end it.
//!
//! Neither pass is run by the process that schedules it: `--mode cron` publishes
//! [`SweepLivenessSignal`] / [`TrimHealthHistorySignal`] and this consumer does
//! the work, so the crons fail over and scale like any other consumer. Because
//! AMQP is at-least-once and consumers are horizontally scaled, every pass gates
//! on [`ClaimJobRun::for_tick`] first: whoever wins the compare-and-set runs it
//! once per configured interval fleet-wide, everybody else returns immediately.
//!
//! One layer is enough here, unlike [`crate::hooks::acme`], which claims each
//! certificate row on top of the job: both passes are a single idempotent write
//! over whatever the sweep finds, so a pass that runs twice, or late, reaches the
//! same state. A failure is returned rather than swallowed, so the consumer logs
//! it; the retry is the next scheduled tick, not the redelivery, because the
//! claim has already recorded this one.

use crate::entities::db::job_run::ClaimJobRun;
use crate::events::{SweepLivenessSignal, TrimHealthHistorySignal};
use crate::services::health::{HealthService, SweepLiveness, TrimHealthHistory};
use chrono::Utc;
use kanau::processor::Processor;
use wakuwaku::amqp::AmqpMessageProcessor;

/// Consumes the two health execution signals.
#[derive(Clone)]
pub struct HealthCronHook {
    pub health: HealthService,
}

impl AmqpMessageProcessor<SweepLivenessSignal> for HealthCronHook {
    const QUEUE: &'static str = "guru_orchestration_sweep_liveness";
}

impl Processor<SweepLivenessSignal> for HealthCronHook {
    type Output = ();
    type Error = wakuwaku::Error;
    #[tracing::instrument(name = "Hook:SweepLivenessSignal", skip_all, err)]
    async fn process(&self, input: SweepLivenessSignal) -> Result<Self::Output, Self::Error> {
        if !self
            .health
            .db
            .process(ClaimJobRun::for_tick(
                "sweep_liveness",
                self.health.config.liveness_interval(),
                input.tick_time(),
            ))
            .await?
        {
            return Ok(());
        }
        let swept = self
            .health
            .process(SweepLiveness { now: Utc::now() })
            .await?;
        if !swept.flipped.is_empty() {
            tracing::info!(
                servers = swept.flipped.len(),
                "servers went offline for lack of reports"
            );
        }
        for server in &swept.revoked {
            tracing::warn!(
                server = %server,
                "revoked a watch session whose registration never reported"
            );
        }
        Ok(())
    }
}

impl AmqpMessageProcessor<TrimHealthHistorySignal> for HealthCronHook {
    const QUEUE: &'static str = "guru_orchestration_trim_health_history";
}

impl Processor<TrimHealthHistorySignal> for HealthCronHook {
    type Output = ();
    type Error = wakuwaku::Error;
    #[tracing::instrument(name = "Hook:TrimHealthHistorySignal", skip_all, err)]
    async fn process(&self, input: TrimHealthHistorySignal) -> Result<Self::Output, Self::Error> {
        if !self
            .health
            .db
            .process(ClaimJobRun::for_tick(
                "trim_health_history",
                self.health.config.health_retention_interval(),
                input.tick_time(),
            ))
            .await?
        {
            return Ok(());
        }
        self.health
            .process(TrimHealthHistory { now: Utc::now() })
            .await?;
        Ok(())
    }
}
