//! The country cron: looks up the country of servers' IPv4 addresses.
//!
//! [`ResolveServerCountriesJob`] is a `CronJob` run by `--mode cron`, gated like
//! every periodic job on [`ClaimJobRun::for_tick`] so it runs once per
//! `country_lookup_interval_secs` fleet-wide. One layer is enough: a pass that
//! runs twice asks the service again about the same addresses at worst, and
//! stores the same answers. A failure is returned for the cron runtime to log;
//! the retry is the next run.

use crate::entities::db::job_run::ClaimJobRun;
use crate::services::OrchestrationError;
use crate::services::country::{CountryService, ResolveServerCountries};
use kanau::processor::Processor;
use std::time::Duration;
use time::OffsetDateTime;
use wakuwaku::{CronJob, CronJobInterval, CronTick, ServiceCreation};

/// Looks up the country of every server IPv4 address that has none, at most
/// once per `country_lookup_interval_secs` fleet-wide.
pub struct ResolveServerCountriesJob {
    pub country: CountryService,
}

impl ServiceCreation for ResolveServerCountriesJob {
    type Dep = CountryService;
    fn create_service(country: CountryService) -> Self {
        Self { country }
    }
}

impl CronJob for ResolveServerCountriesJob {
    const INTERVAL: CronJobInterval = CronJobInterval::AtLeast(Duration::from_secs(60));
}

impl Processor<CronTick> for ResolveServerCountriesJob {
    type Output = ();
    type Error = OrchestrationError;
    #[tracing::instrument(name = "Hook:ResolveServerCountriesJob", skip_all, err)]
    async fn process(&self, tick: CronTick) -> Result<Self::Output, Self::Error> {
        if !self
            .country
            .db
            .process(ClaimJobRun::for_tick(
                "resolve_server_countries",
                self.country.config.country_lookup_interval(),
                OffsetDateTime::from(tick.fired_at),
            ))
            .await?
        {
            return Ok(());
        }
        let pass = self
            .country
            .process(ResolveServerCountries {
                now: OffsetDateTime::now_utc(),
            })
            .await?;
        if pass.looked_up > 0 || pass.cleared > 0 {
            tracing::info!(
                looked_up = pass.looked_up,
                resolved = pass.resolved,
                cleared = pass.cleared,
                "server countries looked up"
            );
        }
        Ok(())
    }
}
