//! The two halves of periodic execution: the scheduler's clock and the
//! consumer's run claim.
//!
//! `--mode cron` holds one [`IntervalJob`] per signal type and publishes the ones
//! that are due. Each job keeps its *own* last-fire timestamp, so a scan that
//! visits every job does not flatten their cadences: a 30 s job fires on most
//! scans while an hourly one fires on one in 120.
//!
//! `--mode consumer` receives the signal and calls [`claim_run`] before doing any
//! work. The claim is what makes the split safe: AMQP is at-least-once and the
//! consumer is horizontally scaled, so the same pass can be delivered twice, to
//! two consumers, or late after a backlog. Whoever wins the compare-and-set runs
//! it; everybody else returns without touching anything.

use crate::entities::surreal::job_run::ClaimJobRun;
use chrono::{DateTime, Utc};
use kanau::processor::Processor;
use std::marker::PhantomData;
use std::task::Poll;
use std::time::Duration;
use time::OffsetDateTime;
use wakuwaku::amqp::AmqpPool;
use wakuwaku::interval_job::IntervalJobExecutionSignal;
use wakuwaku::surreal::SurrealProcessor;

/// One periodic signal's clock: the last tick it was published for.
pub struct IntervalJob<S> {
    last: Option<OffsetDateTime>,
    signal: PhantomData<S>,
}

impl<S> Default for IntervalJob<S> {
    fn default() -> Self {
        Self {
            last: None,
            signal: PhantomData,
        }
    }
}

impl<S: IntervalJobExecutionSignal> IntervalJob<S> {
    /// The signal to publish at `now`, or `None` when this job's cadence has not
    /// elapsed yet. A job that has never fired is due at once, so a freshly
    /// started scheduler kicks every pass off instead of idling for an hour.
    pub fn due(&mut self, now: OffsetDateTime) -> Option<S> {
        let signal = match self.last {
            None => S::tick(now),
            Some(last) => match S::time_pool(now, last) {
                Poll::Ready(signal) => signal,
                Poll::Pending => return None,
            },
        };
        self.last = Some(now);
        Some(signal)
    }

    /// Publishes the signal when it is due; `false` when it is not.
    ///
    /// A publish failure leaves the job marked as fired: retrying inside the scan
    /// would double-publish once the broker recovers, and the next scan is at
    /// most one cadence away.
    pub async fn publish(
        &mut self,
        pool: &AmqpPool,
        now: OffsetDateTime,
    ) -> Result<bool, wakuwaku::Error> {
        match self.due(now) {
            Some(signal) => {
                signal.send(pool).await?;
                Ok(true)
            }
            None => Ok(false),
        }
    }
}

/// Claims the run of `job` for the signal published at `tick`: at most once per
/// `every`, and never twice for the same tick.
///
/// `every` is measured between ticks, not between runs: a consumer stamps the row
/// when it gets to the message, so measuring from that would subtract the
/// processing delay from every period and refuse every other signal whenever the
/// interval equals the publication cadence.
///
/// The tick's age is logged when a claim is refused, which is what makes a
/// consumer backlog visible.
pub async fn claim_run(
    db: &SurrealProcessor,
    job: &'static str,
    every: Duration,
    tick: DateTime<Utc>,
) -> Result<bool, wakuwaku::Error> {
    let now = Utc::now();
    let tick_not_before = chrono::Duration::from_std(every)
        .ok()
        .and_then(|every| tick.checked_sub_signed(every))
        .unwrap_or(DateTime::<Utc>::MIN_UTC);
    let claimed = db
        .process(ClaimJobRun {
            job,
            now,
            tick,
            tick_not_before,
        })
        .await?;
    if !claimed {
        tracing::debug!(
            job,
            tick_age_secs = now.signed_duration_since(tick).num_seconds(),
            "skipping a periodic signal: the job already ran within its interval"
        );
    }
    Ok(claimed)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::events::{
        DeriveStaleCanvasesSignal, RotateRelayCertificatesSignal, TrimHealthHistorySignal,
    };

    fn at(secs: i64) -> OffsetDateTime {
        OffsetDateTime::from_unix_timestamp(1_700_000_000_i64.saturating_add(secs))
            .unwrap_or(OffsetDateTime::UNIX_EPOCH)
    }

    #[test]
    fn a_fresh_job_is_due_at_once_and_then_follows_its_cadence() {
        let mut job = IntervalJob::<DeriveStaleCanvasesSignal>::default();
        let first = job.due(at(0)).map(|s| s.tick_unix_secs);
        assert_eq!(
            first,
            Some(at(0).unix_timestamp()),
            "a new job fires at once"
        );
        assert!(job.due(at(29)).is_none(), "29s is short of the 30s cadence");
        assert!(job.due(at(30)).is_some(), "the cadence elapsed exactly");
        assert!(
            job.due(at(59)).is_none(),
            "the clock restarts from the last fire"
        );
    }

    #[test]
    fn each_job_keeps_its_own_cadence_across_a_shared_scan() {
        // The scheduler scans every job on one timer; the point of per-job state is
        // that the frequent job is not slowed to the rare one, nor the rare one
        // sped up to the scan.
        let mut fast = IntervalJob::<DeriveStaleCanvasesSignal>::default();
        let mut slow = IntervalJob::<RotateRelayCertificatesSignal>::default();
        let mut medium = IntervalJob::<TrimHealthHistorySignal>::default();
        let (mut fast_fires, mut slow_fires, mut medium_fires) = (0, 0, 0);
        // One hour of five-second scans.
        for scan in 0..=720 {
            let now = at(scan * 5);
            fast_fires += usize::from(fast.due(now).is_some());
            medium_fires += usize::from(medium.due(now).is_some());
            slow_fires += usize::from(slow.due(now).is_some());
        }
        assert_eq!(fast_fires, 121, "30s over an hour, plus the immediate fire");
        assert_eq!(
            medium_fires, 13,
            "300s over an hour, plus the immediate fire"
        );
        assert_eq!(slow_fires, 2, "3600s over an hour, plus the immediate fire");
    }

    #[test]
    fn a_scan_that_is_late_fires_once_not_once_per_missed_cadence() {
        // `MissedTickBehavior::Delay` used to guarantee this; with messages it is
        // the scheduler's job, otherwise a stalled process floods the queue.
        let mut job = IntervalJob::<DeriveStaleCanvasesSignal>::default();
        assert!(job.due(at(0)).is_some());
        assert!(
            job.due(at(3600)).is_some(),
            "an hour late, still one signal"
        );
        assert!(
            job.due(at(3601)).is_none(),
            "and the cadence restarts there"
        );
    }
}
