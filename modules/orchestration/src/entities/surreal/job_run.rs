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

use chrono::{DateTime, Utc};
use kanau::processor::Processor;
use newtype_record_id::table_record;
use std::time::Duration;
use surrealdb_types::SurrealValue;
use wakuwaku::surreal::SurrealProcessor;

table_record!(JobRunId, "orchestration_job_run");

#[derive(Debug, Clone, SurrealValue)]
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
    /// The job name; it is the record key.
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

impl Processor<ClaimJobRun> for SurrealProcessor {
    /// `true` when this caller owns the run.
    type Output = bool;
    type Error = surrealdb::Error;
    #[tracing::instrument(name = "Query-Transaction:ClaimJobRun", skip_all, err, fields(job = %input.job))]
    async fn process(&self, input: ClaimJobRun) -> Result<Self::Output, Self::Error> {
        // Statement 0 is BEGIN, 1-2 the LETs; the RETURN is statement 3.
        let mut resp = self
            .db()
            .query(include_str!("../../../sql/job_run/claim_job_run.surql"))
            .bind(("job", input.job.to_string()))
            .bind(("now", input.now))
            .bind(("tick", input.tick))
            .bind(("tick_not_before", input.tick_not_before))
            .await?;
        let claimed = resp.take::<Option<bool>>(3)?.unwrap_or(false);
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
