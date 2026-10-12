//! What a worker remembers about its next hops between connections.
//!
//! Every exit and relay a forwarding dials is a hop, known by what is dialed
//! (protocol, address and name) rather than by which forwarding dials it:
//! forwardings that share a hop share what is known about it, and a reload that
//! keeps a hop keeps its record.
//!
//! A hop is alive until [`FAIL_AFTER`] connection attempts in a row have failed.
//! A dead hop is passed over while anything else can be tried. Once
//! [`RETRY_AFTER`] has passed since it last failed, the worker's prober (see
//! [`crate::prober`]) tries it, one probe at a time, and the first success makes
//! it alive again; no client connection waits on a hop that may well still be
//! dead. Only a connection with nothing else left tries a dead hop itself.

use crate::BoxError;
use std::collections::HashMap;
use std::future::Future;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::{Arc, Weak};
use std::time::Duration;
use tokio::time::Instant;

/// Consecutive failed attempts after which a hop counts as dead.
pub const FAIL_AFTER: u32 = 3;

/// How long a dead hop is passed over before the prober tries it again.
pub const RETRY_AFTER: Duration = Duration::from_secs(10);

/// Every hop the worker's forwardings dial. A record lives for as long as some
/// compiled forwarding holds it.
#[derive(Default)]
pub struct Liveness {
    hops: parking_lot::Mutex<HashMap<String, Weak<Hop>>>,
}

impl Liveness {
    /// The record of the hop `key` names, shared with every other holder.
    pub fn hop(&self, key: String) -> Arc<Hop> {
        let mut hops = self.hops.lock();
        if let Some(hop) = hops.get(&key).and_then(Weak::upgrade) {
            return hop;
        }
        hops.retain(|_, hop| hop.strong_count() > 0);
        let hop = Arc::new(Hop::default());
        hops.insert(key, Arc::downgrade(&hop));
        hop
    }

    /// Claims the due retry of every dead hop that something can probe, for the
    /// prober to make. A hop being probed is not due again until that probe is
    /// settled.
    pub fn due(&self, now: Instant) -> Vec<(Attempt, Arc<dyn Probe>)> {
        let hops: Vec<Arc<Hop>> = self
            .hops
            .lock()
            .values()
            .filter_map(Weak::upgrade)
            .collect();
        hops.into_iter()
            .filter_map(|hop| {
                let probe = hop.probe()?;
                let attempt = hop.begin(now, Claim::Probe)?;
                Some((attempt, probe))
            })
            .collect()
    }

    /// The hops currently dead, by key, for logs and reports.
    pub fn dead(&self) -> Vec<String> {
        let mut dead: Vec<String> = self
            .hops
            .lock()
            .iter()
            .filter(|(_, hop)| hop.upgrade().is_some_and(|hop| hop.is_dead()))
            .map(|(key, _)| key.clone())
            .collect();
        dead.sort();
        dead
    }
}

/// What a background retry of a dead hop runs: a dial of the hop that is closed
/// again at once. A record holds the probes of everything compiled that dials it.
pub trait Probe: Send + Sync {
    /// What is dialed, for logs.
    fn label(&self) -> &str;
    /// Dials the hop and lets go of it, `Ok` when it answered.
    fn probe(self: Arc<Self>) -> ProbeFuture;
}

/// What [`Probe::probe`] returns.
pub type ProbeFuture = Pin<Box<dyn Future<Output = Result<(), BoxError>> + Send>>;

/// One hop's record.
#[derive(Default)]
pub struct Hop {
    state: parking_lot::Mutex<State>,
    /// How to reach the hop without a client: one per compiled hop that dials
    /// it, held weakly so a record does not keep a superseded route alive.
    probes: parking_lot::Mutex<Vec<Weak<dyn Probe>>>,
}

#[derive(Default)]
struct State {
    failures: u32,
    /// When the hop died, or last failed while dead.
    dead_since: Option<Instant>,
    /// A connection is trying the dead hop right now.
    trying: bool,
    /// The address the hop last answered on, tried first next time when the
    /// hop has several.
    preferred: Option<SocketAddr>,
    /// How long the hop usually takes to answer (see [`Hop::pace`]).
    pace: Option<Duration>,
}

/// Who is asking to try a hop, which decides whether a dead one may be tried.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Claim {
    /// A connection choosing among what it can try: a dead hop only when its
    /// retry is due and nobody is making it yet.
    Normal,
    /// A connection with nothing else left: any hop, dead or not.
    Forced,
    /// The prober: only a dead hop whose retry is due and that nobody is
    /// probing yet.
    Probe,
}

/// Whether a connection should try a hop.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Availability {
    Alive,
    /// Dead, but due for a try that nobody is making yet.
    Due,
    Dead,
}

impl Hop {
    pub fn availability(&self, now: Instant) -> Availability {
        let state = self.state.lock();
        match state.dead_since {
            None => Availability::Alive,
            Some(since) if !state.trying && now.saturating_duration_since(since) >= RETRY_AFTER => {
                Availability::Due
            }
            Some(_) => Availability::Dead,
        }
    }

    pub fn is_dead(&self) -> bool {
        self.state.lock().dead_since.is_some()
    }

    /// The address the hop last answered on, if it has answered since it was
    /// first dialed.
    pub fn preferred(&self) -> Option<SocketAddr> {
        self.state.lock().preferred
    }

    /// Remembers the address the hop answered on.
    pub fn prefer(&self, addr: SocketAddr) {
        self.state.lock().preferred = Some(addr);
    }

    /// How long the hop usually takes to answer a connection: a minimum of the
    /// successful attempts that only creeps up, a sixteenth of the way to each
    /// slower one, so the odd slow answer of a lossy path barely moves it while a
    /// hop that became slower for good is followed. `None` before any success.
    pub fn pace(&self) -> Option<Duration> {
        self.state.lock().pace
    }

    /// Makes `probe` one of the ways to reach the hop without a client.
    pub fn attach(&self, probe: Weak<dyn Probe>) {
        let mut probes = self.probes.lock();
        probes.retain(|held| held.strong_count() > 0);
        if !probes.iter().any(|held| held.ptr_eq(&probe)) {
            probes.push(probe);
        }
    }

    fn probe(&self) -> Option<Arc<dyn Probe>> {
        self.probes.lock().iter().find_map(Weak::upgrade)
    }

    /// Claims an attempt on the hop. An alive hop may be tried by any
    /// connection; a dead one only by a [`Claim::Forced`] attempt made because
    /// nothing else is left, or by the one [`Claim::Probe`] that takes its due
    /// retry. `None` means the attempt should not be made.
    pub fn begin(self: &Arc<Self>, now: Instant, claim: Claim) -> Option<Attempt> {
        let mut state = self.state.lock();
        let trial = match (state.dead_since, claim) {
            (None, Claim::Probe) => return None,
            (None, Claim::Normal | Claim::Forced) => false,
            (Some(since), Claim::Probe)
                if !state.trying && now.saturating_duration_since(since) >= RETRY_AFTER =>
            {
                state.trying = true;
                true
            }
            (Some(_), Claim::Forced) => false,
            (Some(_), Claim::Normal | Claim::Probe) => return None,
        };
        Some(Attempt {
            hop: self.clone(),
            trial,
            settled: false,
        })
    }
}

/// An attempt in progress. Settle it with [`Attempt::succeeded`] or
/// [`Attempt::failed`]; dropping it unsettled (the client went away mid-dial,
/// the prober was stopped) only gives back a claimed retry, and records nothing.
pub struct Attempt {
    hop: Arc<Hop>,
    trial: bool,
    settled: bool,
}

impl Attempt {
    /// [`Attempt::succeeded`] for an attempt that took `took`, which the hop's
    /// [`Hop::pace`] learns from.
    pub fn succeeded_in(self, took: Duration) {
        {
            let mut state = self.hop.state.lock();
            state.pace = Some(match state.pace {
                Some(pace) if took > pace => pace.saturating_add(
                    took.saturating_sub(pace)
                        .checked_div(16)
                        .unwrap_or_default(),
                ),
                _ => took,
            });
        }
        self.succeeded();
    }

    pub fn succeeded(mut self) {
        let mut state = self.hop.state.lock();
        state.failures = 0;
        state.dead_since = None;
        state.trying = false;
        self.settled = true;
    }

    pub fn failed(mut self, now: Instant) {
        let mut state = self.hop.state.lock();
        state.failures = state.failures.saturating_add(1);
        if state.failures >= FAIL_AFTER || state.dead_since.is_some() {
            state.dead_since = Some(now);
        }
        if self.trial {
            state.trying = false;
        }
        self.settled = true;
    }
}

impl Drop for Attempt {
    fn drop(&mut self) {
        if !self.settled && self.trial {
            self.hop.state.lock().trying = false;
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::arithmetic_side_effects)]
mod tests {
    use super::*;

    #[test]
    fn a_hop_dies_after_consecutive_failures_only() {
        let hop = Arc::new(Hop::default());
        let now = Instant::now();
        for _ in 0..FAIL_AFTER - 1 {
            hop.begin(now, Claim::Normal).unwrap().failed(now);
        }
        hop.begin(now, Claim::Normal).unwrap().succeeded();
        for _ in 0..FAIL_AFTER - 1 {
            hop.begin(now, Claim::Normal).unwrap().failed(now);
        }
        assert_eq!(
            hop.availability(now),
            Availability::Alive,
            "a success resets the count"
        );
        hop.begin(now, Claim::Normal).unwrap().failed(now);
        assert_eq!(hop.availability(now), Availability::Dead);
        assert!(hop.begin(now, Claim::Normal).is_none());
        assert!(
            hop.begin(now, Claim::Forced).is_some(),
            "a forced attempt is always allowed"
        );
    }

    #[test]
    fn a_dead_hop_gets_one_try_at_a_time_once_due() {
        let hop = Arc::new(Hop::default());
        let start = Instant::now();
        for _ in 0..FAIL_AFTER {
            hop.begin(start, Claim::Normal).unwrap().failed(start);
        }
        let later = start + RETRY_AFTER;
        assert_eq!(hop.availability(later), Availability::Due);
        assert!(
            hop.begin(later, Claim::Normal).is_none(),
            "a connection leaves a due retry to the prober"
        );
        let trial = hop.begin(later, Claim::Probe).unwrap();
        assert_eq!(
            hop.availability(later),
            Availability::Dead,
            "one try at a time"
        );
        assert!(hop.begin(later, Claim::Probe).is_none());
        assert!(
            hop.begin(later, Claim::Forced).is_some(),
            "a connection with nothing else left still may"
        );

        trial.failed(later);
        assert_eq!(
            hop.availability(later + RETRY_AFTER / 2),
            Availability::Dead,
            "a failed try restarts the wait"
        );
        let trial = hop.begin(later + RETRY_AFTER, Claim::Probe).unwrap();
        trial.succeeded();
        assert_eq!(hop.availability(later + RETRY_AFTER), Availability::Alive);
    }

    #[test]
    fn an_abandoned_try_gives_the_turn_back() {
        let hop = Arc::new(Hop::default());
        let start = Instant::now();
        for _ in 0..FAIL_AFTER {
            hop.begin(start, Claim::Normal).unwrap().failed(start);
        }
        let later = start + RETRY_AFTER;
        drop(hop.begin(later, Claim::Probe).unwrap());
        assert_eq!(hop.availability(later), Availability::Due);
    }

    struct Answers;

    impl Probe for Answers {
        fn label(&self) -> &str {
            "answers"
        }

        fn probe(self: Arc<Self>) -> ProbeFuture {
            Box::pin(async { Ok(()) })
        }
    }

    #[test]
    fn only_dead_hops_something_can_probe_are_due_one_probe_at_a_time() {
        let liveness = Liveness::default();
        let hop = liveness.hop("exit|10.0.0.1:80".to_string());
        let start = Instant::now();
        let later = start + RETRY_AFTER;
        let probe: Arc<dyn Probe> = Arc::new(Answers);
        hop.attach(Arc::downgrade(&probe));
        hop.attach(Arc::downgrade(&probe));
        assert!(liveness.due(later).is_empty(), "an alive hop is not probed");

        for _ in 0..FAIL_AFTER {
            hop.begin(start, Claim::Normal).unwrap().failed(start);
        }
        assert!(liveness.due(start).is_empty(), "not due before RETRY_AFTER");
        let mut due = liveness.due(later);
        assert_eq!(due.len(), 1, "a probe attached twice is one probe");
        assert!(liveness.due(later).is_empty(), "one probe at a time");
        let (attempt, _) = due.pop().unwrap();
        attempt.failed(later);
        assert!(
            liveness.due(later).is_empty(),
            "a failed probe restarts the wait"
        );
        let (attempt, _) = liveness.due(later + RETRY_AFTER).pop().unwrap();
        attempt.succeeded();
        assert!(!hop.is_dead());

        for _ in 0..FAIL_AFTER {
            hop.begin(start, Claim::Normal).unwrap().failed(start);
        }
        drop(probe);
        assert!(
            liveness.due(later).is_empty(),
            "a hop whose routes are gone has nothing to probe it with"
        );
    }

    #[test]
    fn pace_follows_the_fast_answers_and_creeps_up_to_slow_ones() {
        let hop = Arc::new(Hop::default());
        let now = Instant::now();
        assert_eq!(hop.pace(), None);
        let ms = Duration::from_millis;
        hop.begin(now, Claim::Normal).unwrap().succeeded_in(ms(100));
        assert_eq!(hop.pace(), Some(ms(100)));
        hop.begin(now, Claim::Normal).unwrap().succeeded_in(ms(80));
        assert_eq!(hop.pace(), Some(ms(80)));
        hop.begin(now, Claim::Normal)
            .unwrap()
            .succeeded_in(ms(1680));
        assert_eq!(
            hop.pace(),
            Some(ms(180)),
            "a slow answer moves it a sixteenth"
        );
        hop.begin(now, Claim::Normal).unwrap().succeeded();
        assert_eq!(hop.pace(), Some(ms(180)), "an untimed success leaves it");
    }

    #[test]
    fn records_are_shared_by_key_and_forgotten_when_unused() {
        let liveness = Liveness::default();
        let a = liveness.hop("exit|10.0.0.1:80".to_string());
        let b = liveness.hop("exit|10.0.0.1:80".to_string());
        assert!(Arc::ptr_eq(&a, &b));
        let now = Instant::now();
        for _ in 0..FAIL_AFTER {
            a.begin(now, Claim::Normal).unwrap().failed(now);
        }
        assert_eq!(liveness.dead(), vec!["exit|10.0.0.1:80".to_string()]);
        drop((a, b));
        let fresh = liveness.hop("exit|10.0.0.1:80".to_string());
        assert!(!fresh.is_dead());
    }
}
