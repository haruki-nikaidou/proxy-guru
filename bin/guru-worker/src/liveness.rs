//! What a worker remembers about its next hops between connections.
//!
//! Every exit and relay a forwarding dials is a hop, known by what is dialed
//! (protocol, address and name) rather than by which forwarding dials it:
//! forwardings that share a hop share what is known about it, and a reload that
//! keeps a hop keeps its record.
//!
//! A hop is alive until [`FAIL_AFTER`] connection attempts in a row have failed.
//! A dead hop is passed over while anything else can be tried. Once
//! [`RETRY_AFTER`] has passed since it last failed, one connection at a time is
//! let through to try it, and the first success makes it alive again.

use std::collections::HashMap;
use std::sync::{Arc, Weak};
use std::time::{Duration, Instant};

/// Consecutive failed attempts after which a hop counts as dead.
pub const FAIL_AFTER: u32 = 3;

/// How long a dead hop is passed over before a connection may try it again.
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

/// One hop's record.
#[derive(Default)]
pub struct Hop {
    state: parking_lot::Mutex<State>,
}

#[derive(Default)]
struct State {
    failures: u32,
    /// When the hop died, or last failed while dead.
    dead_since: Option<Instant>,
    /// A connection is trying the dead hop right now.
    trying: bool,
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

    /// Claims an attempt on the hop. An alive hop may be tried by anyone; a
    /// dead one only by the one connection that claims its due try, or by a
    /// `forced` attempt made because nothing else is left. `None` means the
    /// attempt should not be made.
    pub fn begin(self: &Arc<Self>, now: Instant, forced: bool) -> Option<Attempt> {
        let mut state = self.state.lock();
        let trial = match state.dead_since {
            None => false,
            Some(since) if !state.trying && now.saturating_duration_since(since) >= RETRY_AFTER => {
                state.trying = true;
                true
            }
            Some(_) if forced => false,
            Some(_) => return None,
        };
        Some(Attempt {
            hop: self.clone(),
            trial,
            settled: false,
        })
    }
}

/// An attempt in progress. Settle it with [`Attempt::succeeded`] or
/// [`Attempt::failed`]; dropping it unsettled (the client went away mid-dial)
/// only gives back a claimed try, and records nothing.
pub struct Attempt {
    hop: Arc<Hop>,
    trial: bool,
    settled: bool,
}

impl Attempt {
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
            hop.begin(now, false).unwrap().failed(now);
        }
        hop.begin(now, false).unwrap().succeeded();
        for _ in 0..FAIL_AFTER - 1 {
            hop.begin(now, false).unwrap().failed(now);
        }
        assert_eq!(
            hop.availability(now),
            Availability::Alive,
            "a success resets the count"
        );
        hop.begin(now, false).unwrap().failed(now);
        assert_eq!(hop.availability(now), Availability::Dead);
        assert!(hop.begin(now, false).is_none());
        assert!(
            hop.begin(now, true).is_some(),
            "a forced attempt is always allowed"
        );
    }

    #[test]
    fn a_dead_hop_gets_one_try_at_a_time_once_due() {
        let hop = Arc::new(Hop::default());
        let start = Instant::now();
        for _ in 0..FAIL_AFTER {
            hop.begin(start, false).unwrap().failed(start);
        }
        let later = start + RETRY_AFTER;
        assert_eq!(hop.availability(later), Availability::Due);
        let trial = hop.begin(later, false).unwrap();
        assert_eq!(
            hop.availability(later),
            Availability::Dead,
            "one try at a time"
        );
        assert!(hop.begin(later, false).is_none());

        trial.failed(later);
        assert_eq!(
            hop.availability(later + RETRY_AFTER / 2),
            Availability::Dead,
            "a failed try restarts the wait"
        );
        let trial = hop.begin(later + RETRY_AFTER, false).unwrap();
        trial.succeeded();
        assert_eq!(hop.availability(later + RETRY_AFTER), Availability::Alive);
    }

    #[test]
    fn an_abandoned_try_gives_the_turn_back() {
        let hop = Arc::new(Hop::default());
        let start = Instant::now();
        for _ in 0..FAIL_AFTER {
            hop.begin(start, false).unwrap().failed(start);
        }
        let later = start + RETRY_AFTER;
        drop(hop.begin(later, false).unwrap());
        assert_eq!(hop.availability(later), Availability::Due);
    }

    #[test]
    fn records_are_shared_by_key_and_forgotten_when_unused() {
        let liveness = Liveness::default();
        let a = liveness.hop("exit|10.0.0.1:80".to_string());
        let b = liveness.hop("exit|10.0.0.1:80".to_string());
        assert!(Arc::ptr_eq(&a, &b));
        let now = Instant::now();
        for _ in 0..FAIL_AFTER {
            a.begin(now, false).unwrap().failed(now);
        }
        assert_eq!(liveness.dead(), vec!["exit|10.0.0.1:80".to_string()]);
        drop((a, b));
        let fresh = liveness.hop("exit|10.0.0.1:80".to_string());
        assert!(!fresh.is_dead());
    }
}
