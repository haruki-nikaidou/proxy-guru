//! The last run of each periodic job: what makes an execution signal
//! at-most-once per interval across every consumer.
//!
//! One row per job, the record key being the job name. A consumer that receives
//! a periodic signal first tries to *claim* the run, and the claim fences on two
//! things at once:
//!
//! - `last_signal_tick < tick` — the scheduling tick must be newer than the last
//!   one that ran. A redelivered or backlogged message carries a tick that
//!   already ran, so it is a no-op whatever the clock says.
//! - `last_signal_tick <= tick_not_before` — the configured interval must have
//!   elapsed *between the two ticks*. This is where the cadence an operator sets
//!   takes effect: the publication cadence of a signal is a constant (the
//!   scheduler reads no configuration), while the interval comes from
//!   [`crate::config::OrchestrationConfig`].
//!
//! Both are needed. Without the tick fence a duplicate delivered after the
//! interval — or any duplicate at all, once an operator sets the interval to
//! zero — would run the pass twice; without the interval fence the configured
//! cadence would have no effect.
//!
//! Both fences compare *ticks*, never the wall clock of the last run, and that
//! is not cosmetic: `last_run_at` is stamped when a consumer gets round to the
//! message, so measuring the interval from it would subtract the processing
//! delay from every period and drop every other signal whenever the interval
//! equals the publication cadence. `last_run_at` is recorded for operators, not
//! for the fence.

use base::db::{Db, Error};
use chrono::{DateTime, Utc};
use db_types::table_record;
use kanau::processor::Processor;
use std::time::Duration;

table_record!(JobRunId, "orchestration_job_run");

#[derive(Debug, Clone)]
pub struct JobRunEntity {
    pub id: JobRunId,
    pub last_run_at: DateTime<Utc>,
    /// The scheduling tick of the signal that last ran this job.
    pub last_signal_tick: DateTime<Utc>,
}

/// Claims the run of `job` for the signal published at `tick`: succeeds when the
/// job has never run, or last ran for a tick that is both older than this one and
/// at least the configured interval behind it. Stamps the tick and the wall clock
/// so nobody else claims the same run.
#[derive(Debug)]
pub struct ClaimJobRun {
    /// The job name; it is the row id.
    pub job: &'static str,
    /// When the claim is made; recorded for operators, never compared.
    pub now: DateTime<Utc>,
    /// The scheduling tick of the signal being handled.
    pub tick: DateTime<Utc>,
    /// `tick` minus the configured interval: the newest `last_signal_tick` that
    /// may still be claimed over.
    pub tick_not_before: DateTime<Utc>,
}

impl ClaimJobRun {
    /// The claim a hook makes when it receives the signal published at `tick` for
    /// a job that may run once per `every`.
    ///
    /// `every` is applied to the *ticks*, not to `now`: a consumer stamps the row
    /// when it gets round to the message, so measuring from that would subtract
    /// the processing delay from every period and refuse every other signal
    /// whenever the interval equals the publication cadence.
    pub fn for_tick(job: &'static str, every: Duration, tick: DateTime<Utc>) -> Self {
        let tick_not_before = chrono::Duration::from_std(every)
            .ok()
            .and_then(|every| tick.checked_sub_signed(every))
            .unwrap_or(DateTime::<Utc>::MIN_UTC);
        Self {
            job,
            now: Utc::now(),
            tick,
            tick_not_before,
        }
    }
}

impl Processor<ClaimJobRun> for Db {
    /// `true` when this caller owns the run.
    type Output = bool;
    type Error = Error;
    #[tracing::instrument(name = "Query:ClaimJobRun", skip_all, err, fields(job = %input.job))]
    async fn process(&self, input: ClaimJobRun) -> Result<Self::Output, Self::Error> {
        // One statement: the row is created on the first claim, and afterwards
        // updated only through the fence in the `WHERE`. A duplicate delivery
        // blocks on the row lock, re-evaluates the fence and gets nothing back.
        let claimed: Option<JobRunId> = sqlx::query_file_scalar!(
            "sql/claim_job_run.sql",
            JobRunId::from_key(input.job) as _,
            input.now,
            input.tick,
            input.tick_not_before
        )
        .fetch_optional(self.db())
        .await?;
        let claimed = claimed.is_some();
        if !claimed {
            // The one place a skipped pass becomes visible, and the age is what
            // tells an operator whether it was a duplicate or a backlog.
            tracing::debug!(
                job = input.job,
                tick_age_secs = Utc::now().signed_duration_since(input.tick).num_seconds(),
                "skipping a periodic signal: the job already ran for this tick or interval"
            );
        }
        Ok(claimed)
    }
}
