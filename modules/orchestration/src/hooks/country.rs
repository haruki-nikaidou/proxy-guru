//! The country cron: looks up the country of servers' IPv4 addresses.
//!
//! `--mode cron` publishes [`ResolveServerCountriesSignal`] and this consumer runs
//! the pass, gated like every periodic job on [`ClaimJobRun::for_tick`] so it runs
//! once per `country_lookup_interval_secs` fleet-wide. One layer is enough: a pass
//! that runs twice asks the service again about the same addresses at worst, and
//! stores the same answers. A failure is returned for the consumer to log; the
//! retry is the next tick.

use crate::entities::db::job_run::ClaimJobRun;
use crate::events::ResolveServerCountriesSignal;
use crate::services::country::{CountryService, ResolveServerCountries};
use chrono::Utc;
use kanau::processor::Processor;
use wakuwaku::amqp::AmqpMessageProcessor;

/// Consumes [`ResolveServerCountriesSignal`].
#[derive(Clone)]
pub struct CountryCronHook {
    pub country: CountryService,
}

impl AmqpMessageProcessor<ResolveServerCountriesSignal> for CountryCronHook {
    const QUEUE: &'static str = "guru_orchestration_resolve_server_countries";
}

impl Processor<ResolveServerCountriesSignal> for CountryCronHook {
    type Output = ();
    type Error = wakuwaku::Error;
    #[tracing::instrument(name = "Hook:ResolveServerCountriesSignal", skip_all, err)]
    async fn process(
        &self,
        input: ResolveServerCountriesSignal,
    ) -> Result<Self::Output, Self::Error> {
        if !self
            .country
            .db
            .process(ClaimJobRun::for_tick(
                "resolve_server_countries",
                self.country.config.country_lookup_interval(),
                input.tick_time(),
            ))
            .await?
        {
            return Ok(());
        }
        let pass = self
            .country
            .process(ResolveServerCountries { now: Utc::now() })
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
