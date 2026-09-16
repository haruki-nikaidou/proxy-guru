//! The scheduler's clock.
//!
//! `--mode cron` holds one [`IntervalJob`] per signal type and publishes the ones
//! that are due. Each job keeps its *own* last-fire timestamp, so a scan that
//! visits every job does not flatten their cadences: a 30 s job fires on most
//! scans while an hourly one fires on one in 720.
//!
//! The other half of periodic execution lives in
//! [`crate::entities::db::job_run::ClaimJobRun`]: every hook claims its run
//! before doing any work, because AMQP is at-least-once and the consumer is
//! horizontally scaled, so the same pass can be delivered twice, to two
//! consumers, or late after a backlog.
//!
//! Deciding *whether* a signal is due is pure, and publishing is the signal's own
//! `send`: there is no state here beyond the five timestamps.

use std::marker::PhantomData;
use std::task::Poll;
use time::OffsetDateTime;
use wakuwaku::interval_job::IntervalJobExecutionSignal;

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
