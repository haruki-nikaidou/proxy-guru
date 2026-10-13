//! Health crons: liveness by silence, and retention of raw records.
//!
//! A worker's report stream closing marks its server `Offline` at once; the
//! sweep is the backstop for a master that never saw the close (it restarted,
//! or the connection died without a FIN) and for servers that never reported.
//! It also revokes the watch session of a server that is already `Offline` when
//! the registration holding it never reported: nothing else would ever end it.
//!
//! Both passes are `CronJob`s run by `--mode cron` ([`SweepLivenessJob`],
//! [`TrimHealthHistoryJob`]). Several cron replicas may run at once, so every
//! pass gates on [`ClaimJobRun::for_tick`] first: whoever wins the
//! compare-and-set runs it once per configured interval fleet-wide, everybody
//! else returns immediately.
//!
//! One layer is enough here, unlike [`crate::hooks::acme`], which claims each
//! certificate row on top of the job: both passes are a single idempotent write
//! over whatever the sweep finds, so a pass that runs twice, or late, reaches the
//! same state. A failure is returned rather than swallowed, so the cron runtime
//! logs it; the retry is the next run, because the claim has already recorded
//! this one.

use crate::entities::db::job_run::ClaimJobRun;
use crate::services::OrchestrationError;
use crate::services::health::{HealthService, SweepLiveness, TrimHealthHistory};
use kanau::processor::Processor;
use std::time::Duration;
use time::OffsetDateTime;
use wakuwaku::{CronJob, CronJobInterval, CronTick, ServiceCreation};

/// Flips servers that stopped reporting to `Offline`, at most once per
/// `liveness_interval` fleet-wide.
pub struct SweepLivenessJob {
    pub health: HealthService,
}

impl ServiceCreation for SweepLivenessJob {
    type Dep = HealthService;
    fn create_service(health: HealthService) -> Self {
        Self { health }
    }
}

impl CronJob for SweepLivenessJob {
    const INTERVAL: CronJobInterval = CronJobInterval::AtLeast(Duration::from_secs(30));
}

impl Processor<CronTick> for SweepLivenessJob {
    type Output = ();
    type Error = OrchestrationError;
    #[tracing::instrument(name = "Hook:SweepLivenessJob", skip_all, err)]
    async fn process(&self, tick: CronTick) -> Result<Self::Output, Self::Error> {
        if !self
            .health
            .db
            .process(ClaimJobRun::for_tick(
                "sweep_liveness",
                self.health.config.liveness_interval(),
                OffsetDateTime::from(tick.fired_at),
            ))
            .await?
        {
            return Ok(());
        }
        let swept = self
            .health
            .process(SweepLiveness {
                now: OffsetDateTime::now_utc(),
            })
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

/// Deletes health records past their retention TTLs, at most once per
/// `health_retention_interval` fleet-wide.
pub struct TrimHealthHistoryJob {
    pub health: HealthService,
}

impl ServiceCreation for TrimHealthHistoryJob {
    type Dep = HealthService;
    fn create_service(health: HealthService) -> Self {
        Self { health }
    }
}

impl CronJob for TrimHealthHistoryJob {
    const INTERVAL: CronJobInterval = CronJobInterval::AtLeast(Duration::from_secs(300));
}

impl Processor<CronTick> for TrimHealthHistoryJob {
    type Output = ();
    type Error = OrchestrationError;
    #[tracing::instrument(name = "Hook:TrimHealthHistoryJob", skip_all, err)]
    async fn process(&self, tick: CronTick) -> Result<Self::Output, Self::Error> {
        if !self
            .health
            .db
            .process(ClaimJobRun::for_tick(
                "trim_health_history",
                self.health.config.health_retention_interval(),
                OffsetDateTime::from(tick.fired_at),
            ))
            .await?
        {
            return Ok(());
        }
        self.health
            .process(TrimHealthHistory {
                now: OffsetDateTime::now_utc(),
            })
            .await?;
        Ok(())
    }
}
