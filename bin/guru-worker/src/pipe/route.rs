//! Choosing a next hop for one connection, and connecting to it.
//!
//! A connection walks its forwarding's [`Route`]. A failover takes its members
//! in order; a balance picks by weight (smooth round robin, weighted random, or
//! by client address when sticky). Either one passes over members whose every
//! hop is dead (the prober in [`crate::prober`] finds out when they are back).
//!
//! Attempts are raced. The first starts at once; when it has neither connected
//! nor failed after its hop's hedge delay (about one and a half times what the
//! hop usually takes, [`HEDGE_DELAY`] to [`HEDGE_DELAY_MAX`]), the next choice
//! starts beside it, and a failure starts the next choice at once. The first to
//! connect is the connection's; the others run to their end on their own, so
//! that a hop which never answers is still recorded as failing, and whatever
//! they connect is closed. The next choice is the one the route would have moved
//! on to: within a failover, the rest of the current member first, then the next
//! member; within a balance, the next pick among the members not tried yet. A
//! failover member never takes so many attempts that the members after it get
//! none.
//!
//! Every connection has a budget of [`MAX_ATTEMPTS`] hop attempts and a
//! deadline; each attempt has [`ATTEMPT_TIMEOUT`] to connect and, for a relay
//! that confirms (see [`crate::pipe::relay`]), [`CONFIRM_WAIT`] more for the
//! relay to say its own next hop answered. Only once nothing alive is left to
//! try and nothing is in flight are the dead hops tried, in route order: the
//! worker's record may be out of date.

use crate::BoxError;
use crate::liveness::{self, Availability, Claim};
use crate::pipe::TargetStream;
use crate::pipe::dial::{ADDRESS_STAGGER, Bounds, dial_hop};
use crate::prepared::{Balance, Hop, HopTarget, Pick, Route};
use std::future::Future;
use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::time::Instant;

/// How long one attempt may take to connect: DNS, TCP, and a TLS or QUIC
/// handshake.
pub const ATTEMPT_TIMEOUT: Duration = Duration::from_secs(5);

/// How long a confirming relay may take, once connected, to say that its own
/// next hop answered: enough for one attempt of its own.
pub const CONFIRM_WAIT: Duration = Duration::from_secs(5);

/// How long a client connection may take to reach a next hop, all attempts
/// included.
pub const CONNECT_BUDGET: Duration = Duration::from_secs(15);

/// How many hops one client connection may try.
pub const MAX_ATTEMPTS: u32 = 6;

/// The least an attempt is given before the next choice starts beside it.
pub const HEDGE_DELAY: Duration = Duration::from_millis(300);

/// The most an attempt is given before the next choice starts beside it; also
/// what a sticky balance gives, so a client strays from its member only when
/// that member is in real trouble.
pub const HEDGE_DELAY_MAX: Duration = Duration::from_secs(2);

/// The time limits of a background probe of a dead hop (see [`crate::prober`]):
/// one attempt's worth to connect, and a relay's [`CONFIRM_WAIT`] to confirm.
pub fn probe_bounds(now: Instant) -> Bounds {
    let connect_by = now.checked_add(ATTEMPT_TIMEOUT).unwrap_or(now);
    Bounds {
        connect_by,
        confirm_wait: CONFIRM_WAIT,
        deadline: connect_by.checked_add(CONFIRM_WAIT).unwrap_or(connect_by),
    }
}

/// Connects `client` to the first hop of `route` that answers, within
/// [`CONNECT_BUDGET`].
pub async fn connect(route: &Route, client: SocketAddr) -> Result<TargetStream, BoxError> {
    let now = Instant::now();
    let deadline = now.checked_add(CONNECT_BUDGET).unwrap_or(now);
    connect_until(route, client, deadline).await
}

/// [`connect`] with an explicit deadline, for a relay answering a dialer that
/// told it how long it will wait.
pub async fn connect_until(
    route: &Route,
    client: SocketAddr,
    deadline: Instant,
) -> Result<TargetStream, BoxError> {
    race(route, client, deadline, &Network).await
}

/// Whether anything under `route` is alive: what is dead, due for a retry or
/// not, is left to the prober and to a connection with nothing else left.
pub fn available(route: &Route, now: Instant) -> bool {
    match route {
        Route::Hop(hop) => hop.record.availability(now) == Availability::Alive,
        Route::Balance(balance) => balance
            .members
            .iter()
            .any(|(member, _)| available(member, now)),
        Route::Failover(members) => members.iter().any(|member| available(member, now)),
    }
}

/// How an attempt reaches a hop: over the network, or, in tests, by script.
pub(crate) trait Dialer: Clone + Send + Sync + 'static {
    type Stream: Send + 'static;

    fn dial(
        &self,
        hop: Arc<Hop>,
        client: SocketAddr,
        bounds: Bounds,
    ) -> impl Future<Output = Result<Self::Stream, BoxError>> + Send + 'static;
}

#[derive(Clone)]
struct Network;

impl Dialer for Network {
    type Stream = TargetStream;

    // Not an `async fn`: its future would borrow `self`, and an attempt's future
    // must own everything to run as a task of its own.
    #[allow(clippy::manual_async_fn)]
    fn dial(
        &self,
        hop: Arc<Hop>,
        client: SocketAddr,
        bounds: Bounds,
    ) -> impl Future<Output = Result<TargetStream, BoxError>> + Send + 'static {
        async move { dial_hop(&hop, client, bounds).await }
    }
}

/// How long an attempt on `hop` is given before the next choice starts beside
/// it: one and a half times the hop's pace, within [`HEDGE_DELAY`] and
/// [`HEDGE_DELAY_MAX`]; the most under a sticky balance.
pub fn hedge_delay(pace: Option<Duration>, sticky: bool) -> Duration {
    if sticky {
        return HEDGE_DELAY_MAX;
    }
    pace.map_or(HEDGE_DELAY, |pace| {
        pace.saturating_add(pace.checked_div(2).unwrap_or_default())
    })
    .clamp(HEDGE_DELAY, HEDGE_DELAY_MAX)
}

/// [`hedge_delay`] for `hop`, plus one [`ADDRESS_STAGGER`] while a hop with
/// alternates has not shown which of its addresses answers: its own race
/// between them comes first.
fn hop_hedge_delay(hop: &Hop, sticky: bool) -> Duration {
    let delay = hedge_delay(hop.record.pace(), sticky);
    let unsettled = matches!(&hop.target, HopTarget::Relay { alternates, .. } if !alternates.is_empty())
        && hop.record.preferred().is_none();
    if unsettled {
        delay.saturating_add(ADDRESS_STAGGER)
    } else {
        delay
    }
}

/// Connects `client` along `route` by `deadline`, racing attempts as the
/// module describes.
pub(crate) async fn race<D: Dialer>(
    route: &Route,
    client: SocketAddr,
    deadline: Instant,
    dialer: &D,
) -> Result<D::Stream, BoxError> {
    let mut tried: Vec<usize> = Vec::new();
    let mut live = Cursor::new(route, MAX_ATTEMPTS, false);
    let mut forced: Option<Cursor<'_>> = None;
    let (outcomes, mut results) = mpsc::channel::<(String, Result<D::Stream, BoxError>)>(
        usize::try_from(MAX_ATTEMPTS).unwrap_or(usize::MAX),
    );
    let mut left = MAX_ATTEMPTS;
    let mut in_flight = 0u32;
    let mut next_at: Option<Instant> = None;
    let mut last_error: Option<String> = None;
    loop {
        let now = Instant::now();
        if now >= deadline {
            break;
        }
        let may_start = left > 0 && (in_flight == 0 || next_at.is_some_and(|at| at <= now));
        if may_start {
            let mut pull = Pull {
                now,
                client: client.ip(),
                claim: Claim::Normal,
                tried: &mut tried,
            };
            let mut candidate = live.next(&mut pull);
            if candidate.is_none() && in_flight == 0 {
                // Nothing alive is left to try and nothing is on its way: the
                // dead hops, in route order.
                pull.claim = Claim::Forced;
                candidate = forced
                    .get_or_insert_with(|| Cursor::new(route, left, false))
                    .next(&mut pull);
            }
            match candidate {
                Some(Candidate {
                    hop,
                    attempt,
                    sticky,
                }) => {
                    left = left.saturating_sub(1);
                    in_flight = in_flight.saturating_add(1);
                    let delay = hop_hedge_delay(&hop, sticky);
                    if in_flight > 1 {
                        tracing::debug!(hop = %hop.label, "another next hop started beside a slow one");
                    }
                    next_at = now.checked_add(delay);
                    let bounds = Bounds {
                        connect_by: deadline.min(now.checked_add(ATTEMPT_TIMEOUT).unwrap_or(now)),
                        confirm_wait: CONFIRM_WAIT,
                        deadline,
                    };
                    tokio::spawn(run_attempt(
                        dialer.clone(),
                        hop,
                        attempt,
                        client,
                        bounds,
                        outcomes.clone(),
                    ));
                    continue;
                }
                None => next_at = None,
            }
        }
        if in_flight == 0 {
            break;
        }
        let hedge = async move {
            match next_at {
                Some(at) if left > 0 => tokio::time::sleep_until(at).await,
                _ => std::future::pending().await,
            }
        };
        tokio::select! {
            outcome = results.recv() => {
                let Some((label, result)) = outcome else { break };
                in_flight = in_flight.saturating_sub(1);
                match result {
                    Ok(stream) => return Ok(stream),
                    Err(error) => {
                        last_error = Some(format!("{label}: {error}"));
                        // A failure starts the next choice at once.
                        next_at = Some(Instant::now());
                    }
                }
            }
            () = hedge => {}
            () = tokio::time::sleep_until(deadline) => break,
        }
    }
    Err(match last_error {
        Some(error) => error.into(),
        None if left == 0 => "every connection attempt failed".into(),
        None if in_flight > 0 => "no next hop answered in time".into(),
        None => "no next hop could be tried".into(),
    })
}

/// One attempt, on a task of its own so it can outlive the connection's interest
/// in it: its outcome is recorded against the hop either way, and sent back to
/// the connection, which drops it (closing what it connected) once it has won
/// with another or given up.
async fn run_attempt<D: Dialer>(
    dialer: D,
    hop: Arc<Hop>,
    attempt: liveness::Attempt,
    client: SocketAddr,
    bounds: Bounds,
    outcomes: mpsc::Sender<(String, Result<D::Stream, BoxError>)>,
) {
    let started = Instant::now();
    let result = dialer.dial(hop.clone(), client, bounds).await;
    match &result {
        Ok(_) => {
            if hop.record.is_dead() {
                tracing::info!(hop = %hop.label, "next hop answers again");
            }
            attempt.succeeded_in(started.elapsed());
        }
        Err(error) => {
            let was_dead = hop.record.is_dead();
            attempt.failed(Instant::now());
            if !was_dead && hop.record.is_dead() {
                tracing::warn!(hop = %hop.label, %error, "next hop marked dead");
            } else {
                tracing::debug!(hop = %hop.label, %error, "next hop attempt failed");
            }
        }
    }
    // Nobody waiting any more drops what was connected.
    let _ = outcomes.send((hop.label.clone(), result)).await;
}

/// A hop to try, its attempt claimed.
struct Candidate {
    hop: Arc<Hop>,
    attempt: liveness::Attempt,
    /// Chosen under a sticky balance.
    sticky: bool,
}

/// What a cursor needs to hand out the next candidate.
struct Pull<'a> {
    now: Instant,
    client: IpAddr,
    claim: Claim,
    /// The records this connection has tried, by address: a hop two members
    /// share is tried once.
    tried: &'a mut Vec<usize>,
}

/// A connection's way through its route, one candidate at a time: a balance
/// picks only when another candidate is asked for, so a connection whose first
/// attempt wins moves a round-robin balance on exactly once.
enum Cursor<'r> {
    Hop {
        hop: &'r Arc<Hop>,
        sticky: bool,
        done: bool,
    },
    Failover {
        members: &'r [Arc<Route>],
        next: usize,
        current: Option<Box<Cursor<'r>>>,
        /// How many more candidates this group may hand out.
        quota: u32,
        sticky: bool,
    },
    Balance {
        balance: &'r Balance,
        untried: Vec<usize>,
        current: Option<Box<Cursor<'r>>>,
        quota: u32,
        sticky: bool,
    },
}

impl<'r> Cursor<'r> {
    fn new(route: &'r Route, quota: u32, sticky: bool) -> Self {
        match route {
            Route::Hop(hop) => Cursor::Hop {
                hop,
                sticky,
                done: false,
            },
            Route::Failover(members) => Cursor::Failover {
                members,
                next: 0,
                current: None,
                quota,
                sticky,
            },
            Route::Balance(balance) => Cursor::Balance {
                balance,
                untried: (0..balance.members.len()).collect(),
                current: None,
                quota,
                sticky: sticky || matches!(balance.pick, Pick::Sticky),
            },
        }
    }

    /// The next hop to try under this node, or `None` once it has nothing more
    /// it may hand out.
    fn next(&mut self, pull: &mut Pull<'_>) -> Option<Candidate> {
        match self {
            Cursor::Hop { hop, sticky, done } => {
                if *done {
                    return None;
                }
                let record = Arc::as_ptr(&hop.record) as usize;
                if pull.tried.contains(&record) {
                    *done = true;
                    return None;
                }
                // A dead hop is left for later rather than given up on: once
                // nothing alive is left, a forced pass takes it.
                let attempt = hop.record.begin(pull.now, pull.claim)?;
                *done = true;
                pull.tried.push(record);
                Some(Candidate {
                    hop: Arc::clone(hop),
                    attempt,
                    sticky: *sticky,
                })
            }
            Cursor::Failover {
                members,
                next,
                current,
                quota,
                sticky,
            } => loop {
                if *quota == 0 {
                    return None;
                }
                if let Some(cursor) = current {
                    if let Some(candidate) = cursor.next(pull) {
                        *quota = quota.saturating_sub(1);
                        return Some(candidate);
                    }
                    *current = None;
                }
                let member = members.get(*next)?;
                *next = next.saturating_add(1);
                if !eligible(member, pull) {
                    continue;
                }
                // Leave one attempt for every member after this one that could
                // take it.
                let later = members
                    .iter()
                    .skip(*next)
                    .filter(|later| eligible(later, pull))
                    .count();
                let later = u32::try_from(later).unwrap_or(u32::MAX);
                let share = quota.saturating_sub(later).max(1);
                *current = Some(Box::new(Cursor::new(member, share, *sticky)));
            },
            Cursor::Balance {
                balance,
                untried,
                current,
                quota,
                sticky,
            } => loop {
                if *quota == 0 {
                    return None;
                }
                if let Some(cursor) = current {
                    if let Some(candidate) = cursor.next(pull) {
                        *quota = quota.saturating_sub(1);
                        return Some(candidate);
                    }
                    *current = None;
                }
                let candidates: Vec<usize> = untried
                    .iter()
                    .copied()
                    .filter(|i| {
                        balance
                            .members
                            .get(*i)
                            .is_some_and(|(member, _)| eligible(member, pull))
                    })
                    .collect();
                let chosen = balance.pick(&candidates, pull.client)?;
                untried.retain(|i| *i != chosen);
                let (member, _) = balance.members.get(chosen)?;
                *current = Some(Box::new(Cursor::new(member, *quota, *sticky)));
            },
        }
    }
}

/// Whether a member is worth descending into: anything alive under it, or, on
/// the forced pass, anything at all.
fn eligible(member: &Route, pull: &Pull<'_>) -> bool {
    pull.claim == Claim::Forced || available(member, pull.now)
}

impl Balance {
    fn weight(&self, i: usize) -> u64 {
        self.members
            .get(i)
            .map_or(1, |(_, weight)| u64::from((*weight).max(1)))
    }

    /// The member to try next among `candidates` (indices into `members`).
    pub fn pick(&self, candidates: &[usize], client: IpAddr) -> Option<usize> {
        if candidates.is_empty() {
            return None;
        }
        match &self.pick {
            Pick::RoundRobin => self.smooth(candidates),
            Pick::Random(state) => self.random(candidates, state),
            Pick::Sticky => self.sticky(candidates, client),
        }
    }

    /// Smooth weighted round robin: every candidate gains its weight, the
    /// highest goes and gives back the candidates' total. Over the candidates'
    /// total picks each goes exactly its weight's worth of times, spread out.
    fn smooth(&self, candidates: &[usize]) -> Option<usize> {
        let mut current = self.current.lock();
        let total = candidates
            .iter()
            .map(|i| i64::try_from(self.weight(*i)).unwrap_or(i64::MAX))
            .fold(0i64, i64::saturating_add);
        let mut best: Option<(usize, i64)> = None;
        for &i in candidates {
            let weight = i64::try_from(self.weight(i)).unwrap_or(i64::MAX);
            let Some(value) = current.get_mut(i) else {
                continue;
            };
            *value = value.saturating_add(weight);
            if best.is_none_or(|(_, top)| *value > top) {
                best = Some((i, *value));
            }
        }
        let (chosen, _) = best?;
        if let Some(value) = current.get_mut(chosen) {
            *value = value.saturating_sub(total);
        }
        Some(chosen)
    }

    fn random(&self, candidates: &[usize], state: &AtomicU64) -> Option<usize> {
        let total = candidates
            .iter()
            .map(|i| self.weight(*i))
            .fold(0u64, u64::saturating_add);
        let mut roll = draw(state).checked_rem(total)?;
        for &i in candidates {
            let weight = self.weight(i);
            if roll < weight {
                return Some(i);
            }
            roll = roll.saturating_sub(weight);
        }
        candidates.last().copied()
    }

    /// Weighted rendezvous hashing: every candidate scores the client address,
    /// and the best score wins. Taking a candidate away moves only the clients
    /// it had; the rest stay where they are.
    fn sticky(&self, candidates: &[usize], client: IpAddr) -> Option<usize> {
        let address = match client.to_canonical() {
            IpAddr::V4(v4) => u64::from(u32::from(v4)),
            IpAddr::V6(v6) => {
                let bits = u128::from(v6);
                (bits as u64) ^ ((bits >> 64) as u64)
            }
        };
        let score = |i: usize| -> f64 {
            let index = u64::try_from(i).unwrap_or(u64::MAX);
            let hash = mix(self.salt ^ mix(address) ^ mix(index.wrapping_add(0x9E37_79B9)));
            // A uniform draw in (0, 1), turned into an exponential race weighted
            // by the member's weight.
            let draw = ((hash >> 11) as f64 + 0.5) / 9_007_199_254_740_992.0;
            -(self.weight(i) as f64) / draw.ln()
        };
        candidates
            .iter()
            .copied()
            .map(|i| (i, score(i)))
            .fold(None, |best: Option<(usize, f64)>, (i, s)| match best {
                Some((_, top)) if top >= s => best,
                _ => Some((i, s)),
            })
            .map(|(i, _)| i)
    }
}

/// SplitMix64's finaliser.
fn mix(mut z: u64) -> u64 {
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^ (z >> 31)
}

/// SplitMix64's increment, the golden ratio's fractional part.
const GAMMA: u64 = 0x9E37_79B9_7F4A_7C15;

/// The next number of the SplitMix64 generator whose state is `state`.
///
/// The state is a counter every draw advances in one atomic add, and the draw
/// is the finalised value it advanced to. Connections picking at the same
/// moment each get a counter value of their own — a load followed by a store
/// hands both the same number and advances once — and the finaliser is a
/// bijection, so no two draws repeat until the counter wraps.
fn draw(state: &AtomicU64) -> u64 {
    mix(state
        .fetch_add(GAMMA, Ordering::Relaxed)
        .wrapping_add(GAMMA))
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::arithmetic_side_effects)]
mod tests {
    use super::*;
    use std::sync::Arc;

    fn balance(weights: &[u32], pick: Pick) -> Balance {
        Balance {
            members: weights
                .iter()
                .map(|w| (Arc::new(Route::Failover(Vec::new())), *w))
                .collect(),
            pick,
            salt: 42,
            current: parking_lot::Mutex::new(vec![0; weights.len()]),
        }
    }

    fn client(i: u32) -> IpAddr {
        IpAddr::V4(std::net::Ipv4Addr::from(0x0A00_0000 + i))
    }

    #[test]
    fn round_robin_follows_the_weights_evenly_spread() {
        let b = balance(&[3, 1], Pick::RoundRobin);
        let picks: Vec<usize> = (0..8)
            .map(|_| b.pick(&[0, 1], client(0)).unwrap())
            .collect();
        assert_eq!(picks.iter().filter(|p| **p == 0).count(), 6);
        assert_eq!(
            picks[..4].iter().filter(|p| **p == 1).count(),
            1,
            "{picks:?}"
        );
        let b = balance(&[1, 1, 1], Pick::RoundRobin);
        let picks: Vec<usize> = (0..6)
            .map(|_| b.pick(&[0, 1, 2], client(0)).unwrap())
            .collect();
        assert_eq!(picks, [0, 1, 2, 0, 1, 2]);
    }

    #[test]
    fn round_robin_over_a_subset_ignores_the_rest() {
        let b = balance(&[1, 5, 1], Pick::RoundRobin);
        for _ in 0..20 {
            assert_ne!(b.pick(&[0, 2], client(0)), Some(1));
        }
    }

    #[test]
    fn random_respects_weights() {
        let b = balance(&[9, 1], Pick::Random(AtomicU64::new(0x1234_5678)));
        let picks: Vec<usize> = (0..10_000)
            .map(|_| b.pick(&[0, 1], client(0)).unwrap())
            .collect();
        let heavy = picks.iter().filter(|p| **p == 0).count();
        assert!((8_700..9_300).contains(&heavy), "{heavy}");
    }

    #[test]
    fn concurrent_random_draws_never_repeat() {
        let state = AtomicU64::new(0x1234_5678);
        let draws: Vec<u64> = std::thread::scope(|s| {
            let threads: Vec<_> = (0..8)
                .map(|_| s.spawn(|| (0..10_000).map(|_| draw(&state)).collect::<Vec<u64>>()))
                .collect();
            threads
                .into_iter()
                .flat_map(|thread| thread.join().unwrap())
                .collect()
        });
        let distinct: std::collections::HashSet<u64> = draws.iter().copied().collect();
        assert_eq!(
            distinct.len(),
            draws.len(),
            "two racing draws got the same number"
        );
    }

    #[test]
    fn sticky_keeps_clients_and_only_moves_those_of_a_lost_member() {
        let b = balance(&[1, 1, 1, 1], Pick::Sticky);
        let all = [0, 1, 2, 3];
        let before: Vec<usize> = (0..4000)
            .map(|i| b.pick(&all, client(i)).unwrap())
            .collect();
        let again: Vec<usize> = (0..4000)
            .map(|i| b.pick(&all, client(i)).unwrap())
            .collect();
        assert_eq!(before, again, "the same client picks the same member");
        for member in all {
            let share = before.iter().filter(|p| **p == member).count();
            assert!((800..1200).contains(&share), "member {member}: {share}");
        }
        let without_two = [0, 1, 3];
        for (i, was) in before.iter().enumerate() {
            let now = b.pick(&without_two, client(i as u32)).unwrap();
            if *was != 2 {
                assert_eq!(
                    now, *was,
                    "client {i} moved though its member is still there"
                );
            }
        }
    }

    #[test]
    fn sticky_follows_weights() {
        let b = balance(&[3, 1], Pick::Sticky);
        let heavy = (0..8000)
            .filter(|i| b.pick(&[0, 1], client(*i)) == Some(0))
            .count();
        assert!((5600..6400).contains(&heavy), "{heavy}");
    }

    #[test]
    fn nested_sticky_balances_do_not_choose_alike() {
        let outer = balance(&[1, 1], Pick::Sticky);
        let mut inner = balance(&[1, 1], Pick::Sticky);
        inner.salt = 7;
        let mut combos = std::collections::HashSet::new();
        for i in 0..400 {
            combos.insert((
                outer.pick(&[0, 1], client(i)).unwrap(),
                inner.pick(&[0, 1], client(i)).unwrap(),
            ));
        }
        assert_eq!(
            combos.len(),
            4,
            "every inner member gets clients under every outer member"
        );
    }
}

#[cfg(test)]
#[allow(
    clippy::unwrap_used,
    clippy::panic,
    clippy::arithmetic_side_effects,
    clippy::manual_async_fn
)]
mod race_tests {
    use super::*;
    use crate::liveness::{FAIL_AFTER, Liveness};
    use crate::prepared::HopTarget;
    use guru_worker_config::{Ipv6Resolve, KeepAlive, Remote};
    use std::collections::HashMap;
    use std::sync::Mutex;

    /// How a scripted hop answers.
    #[derive(Clone, Copy)]
    enum Answer {
        /// Connected after this many milliseconds.
        Ok(u64),
        /// Refused after this many milliseconds.
        Refused(u64),
        /// Never: the attempt runs into its own time limit.
        Never,
    }

    /// A dialer that answers by hop label, recording when each attempt started
    /// (milliseconds of paused time since the script was made).
    #[derive(Clone)]
    struct Script {
        answers: Arc<HashMap<String, Answer>>,
        started: Arc<Mutex<Vec<(String, u64)>>>,
        epoch: Instant,
    }

    impl Script {
        fn new(answers: &[(&str, Answer)]) -> Self {
            Self {
                answers: Arc::new(
                    answers
                        .iter()
                        .map(|(label, answer)| (label.to_string(), *answer))
                        .collect(),
                ),
                started: Arc::default(),
                epoch: Instant::now(),
            }
        }

        fn started(&self) -> Vec<(String, u64)> {
            self.started.lock().unwrap().clone()
        }

        fn at(&self) -> u64 {
            self.epoch.elapsed().as_millis() as u64
        }
    }

    impl Dialer for Script {
        type Stream = String;

        fn dial(
            &self,
            hop: Arc<Hop>,
            _client: SocketAddr,
            bounds: Bounds,
        ) -> impl Future<Output = Result<String, BoxError>> + Send + 'static {
            let answer = self.answers[&hop.label];
            self.started
                .lock()
                .unwrap()
                .push((hop.label.clone(), self.at()));
            async move {
                let (after, connected) = match answer {
                    Answer::Ok(ms) => (ms, true),
                    Answer::Refused(ms) => (ms, false),
                    Answer::Never => (u64::MAX / 2, false),
                };
                let waited =
                    tokio::time::timeout_at(bounds.connect_by, tokio::time::sleep(ms(after))).await;
                match (waited, connected) {
                    (Err(_), _) => Err("timed out connecting".into()),
                    (Ok(()), true) => Ok(hop.label.clone()),
                    (Ok(()), false) => Err("refused".into()),
                }
            }
        }
    }

    fn ms(n: u64) -> Duration {
        Duration::from_millis(n)
    }

    fn hop(liveness: &Liveness, label: &str) -> Arc<Route> {
        Arc::new(Route::Hop(Arc::new(Hop {
            record: liveness.hop(label.to_string()),
            label: label.to_string(),
            target: HopTarget::Exit {
                destination: Remote::Address("192.0.2.1:9".parse().unwrap()),
                ipv6_resolve: Ipv6Resolve::Tolerated,
                send_pp: None,
                keepalive: KeepAlive::default(),
            },
        })))
    }

    fn balance(members: Vec<Arc<Route>>, pick: Pick) -> Arc<Route> {
        Arc::new(Route::Balance(Balance {
            current: parking_lot::Mutex::new(vec![0; members.len()]),
            members: members.into_iter().map(|m| (m, 1)).collect(),
            pick,
            salt: 7,
        }))
    }

    fn failover(members: Vec<Arc<Route>>) -> Arc<Route> {
        Arc::new(Route::Failover(members))
    }

    fn kill(liveness: &Liveness, label: &str) {
        let record = liveness.hop(label.to_string());
        let now = Instant::now();
        for _ in 0..FAIL_AFTER {
            record.begin(now, Claim::Normal).unwrap().failed(now);
        }
    }

    async fn connect(route: &Route, script: &Script) -> Result<String, BoxError> {
        let deadline = Instant::now() + CONNECT_BUDGET;
        race(
            route,
            "198.51.100.7:4000".parse().unwrap(),
            deadline,
            script,
        )
        .await
    }

    fn started(list: &[(&str, u64)]) -> Vec<(String, u64)> {
        list.iter().map(|(l, at)| (l.to_string(), *at)).collect()
    }

    #[tokio::test(start_paused = true)]
    async fn a_slow_attempt_gets_company_after_the_hedge_delay() {
        let l = Liveness::default();
        let route = balance(vec![hop(&l, "a"), hop(&l, "b")], Pick::RoundRobin);
        let script = Script::new(&[("a", Answer::Never), ("b", Answer::Ok(10))]);
        assert_eq!(connect(&route, &script).await.unwrap(), "b");
        assert_eq!(script.at(), 310);
        assert_eq!(script.started(), started(&[("a", 0), ("b", 300)]));
    }

    #[tokio::test(start_paused = true)]
    async fn a_failure_starts_the_next_choice_at_once() {
        let l = Liveness::default();
        let route = balance(vec![hop(&l, "a"), hop(&l, "b")], Pick::RoundRobin);
        let script = Script::new(&[("a", Answer::Refused(10)), ("b", Answer::Ok(10))]);
        assert_eq!(connect(&route, &script).await.unwrap(), "b");
        assert_eq!(script.started(), started(&[("a", 0), ("b", 10)]));
    }

    #[tokio::test(start_paused = true)]
    async fn a_loser_runs_to_its_end_and_a_hop_that_never_answers_dies() {
        let l = Liveness::default();
        let route = failover(vec![hop(&l, "a"), hop(&l, "b")]);
        let script = Script::new(&[("a", Answer::Never), ("b", Answer::Ok(10))]);
        for _ in 0..FAIL_AFTER {
            assert_eq!(connect(&route, &script).await.unwrap(), "b");
        }
        assert!(!l.hop("a".to_string()).is_dead(), "still running");
        tokio::time::sleep(ATTEMPT_TIMEOUT).await;
        assert!(l.hop("a".to_string()).is_dead());
        let before = script.started().len();
        let at = script.at();
        assert_eq!(connect(&route, &script).await.unwrap(), "b");
        assert_eq!(
            script.started()[before..],
            started(&[("b", at)]),
            "a dead hop is passed over at once"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn a_failover_reaches_its_backup_while_its_balance_hangs() {
        let l = Liveness::default();
        let members = ["a", "b", "c", "d"].map(|label| hop(&l, label)).to_vec();
        let route = failover(vec![balance(members, Pick::RoundRobin), hop(&l, "backup")]);
        let script = Script::new(&[
            ("a", Answer::Never),
            ("b", Answer::Never),
            ("c", Answer::Never),
            ("d", Answer::Never),
            ("backup", Answer::Ok(10)),
        ]);
        assert_eq!(connect(&route, &script).await.unwrap(), "backup");
        assert_eq!(script.at(), 1210);
        let starts: Vec<u64> = script.started().iter().map(|(_, at)| *at).collect();
        assert_eq!(starts, vec![0, 300, 600, 900, 1200]);
    }

    #[tokio::test(start_paused = true)]
    async fn a_failover_member_leaves_attempts_for_the_members_after_it() {
        let l = Liveness::default();
        let members = ["a", "b", "c", "d", "e", "f", "g"]
            .map(|label| hop(&l, label))
            .to_vec();
        let route = failover(vec![balance(members, Pick::RoundRobin), hop(&l, "backup")]);
        let mut answers: Vec<(&str, Answer)> = ["a", "b", "c", "d", "e", "f", "g"]
            .map(|label| (label, Answer::Refused(1)))
            .to_vec();
        answers.push(("backup", Answer::Ok(1)));
        let script = Script::new(&answers);
        assert_eq!(connect(&route, &script).await.unwrap(), "backup");
        assert_eq!(
            script.started().len(),
            MAX_ATTEMPTS as usize,
            "the balance took all but one attempt"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn dead_hops_wait_until_nothing_alive_is_left() {
        let l = Liveness::default();
        let route = failover(vec![hop(&l, "a"), hop(&l, "b")]);
        kill(&l, "a");
        let script = Script::new(&[("a", Answer::Ok(10)), ("b", Answer::Refused(10))]);
        assert_eq!(connect(&route, &script).await.unwrap(), "a");
        assert_eq!(script.started(), started(&[("b", 0), ("a", 10)]));
        assert!(!l.hop("a".to_string()).is_dead(), "it answered");
    }

    #[tokio::test(start_paused = true)]
    async fn a_nested_group_does_not_force_its_dead_members() {
        let l = Liveness::default();
        let route = failover(vec![
            balance(vec![hop(&l, "a"), hop(&l, "b")], Pick::RoundRobin),
            hop(&l, "c"),
        ]);
        kill(&l, "a");
        kill(&l, "b");
        let script = Script::new(&[
            ("a", Answer::Ok(10)),
            ("b", Answer::Ok(10)),
            ("c", Answer::Ok(10)),
        ]);
        assert_eq!(connect(&route, &script).await.unwrap(), "c");
        assert_eq!(script.started(), started(&[("c", 0)]));
    }

    #[tokio::test(start_paused = true)]
    async fn no_more_than_max_attempts_start() {
        let l = Liveness::default();
        let labels = ["a", "b", "c", "d", "e", "f", "g", "h"];
        let route = balance(
            labels.map(|label| hop(&l, label)).to_vec(),
            Pick::RoundRobin,
        );
        let script = Script::new(&labels.map(|label| (label, Answer::Never)));
        let error = connect(&route, &script).await.unwrap_err().to_string();
        assert!(error.contains("timed out connecting"), "{error}");
        assert_eq!(script.started().len(), MAX_ATTEMPTS as usize);
    }

    #[tokio::test(start_paused = true)]
    async fn nothing_waits_past_the_deadline() {
        let l = Liveness::default();
        let route = hop(&l, "a");
        let script = Script::new(&[("a", Answer::Never)]);
        let deadline = Instant::now() + ms(1000);
        let result = race(
            &route,
            "198.51.100.7:4000".parse().unwrap(),
            deadline,
            &script,
        )
        .await;
        assert!(result.is_err());
        assert_eq!(script.at(), 1000);
    }

    #[tokio::test(start_paused = true)]
    async fn round_robin_moves_on_once_per_connection_without_a_hedge() {
        let l = Liveness::default();
        let route = balance(vec![hop(&l, "a"), hop(&l, "b")], Pick::RoundRobin);
        let script = Script::new(&[("a", Answer::Ok(1)), ("b", Answer::Ok(1))]);
        let mut won = Vec::new();
        for _ in 0..4 {
            won.push(connect(&route, &script).await.unwrap());
        }
        assert_eq!(won, ["a", "b", "a", "b"]);
        assert_eq!(script.started().len(), 4, "nothing was hedged");
    }

    #[tokio::test(start_paused = true)]
    async fn a_sticky_balance_waits_longer_before_straying() {
        let l = Liveness::default();
        let route = balance(vec![hop(&l, "a"), hop(&l, "b")], Pick::Sticky);
        let script = Script::new(&[("a", Answer::Never), ("b", Answer::Never)]);
        let _ = connect(&route, &script).await;
        let starts: Vec<u64> = script.started().iter().map(|(_, at)| *at).collect();
        assert_eq!(starts, vec![0, HEDGE_DELAY_MAX.as_millis() as u64]);
    }

    #[tokio::test(start_paused = true)]
    async fn a_hop_two_members_share_is_tried_once() {
        let l = Liveness::default();
        let route = balance(vec![hop(&l, "x"), hop(&l, "x")], Pick::RoundRobin);
        let script = Script::new(&[("x", Answer::Refused(1))]);
        assert!(connect(&route, &script).await.is_err());
        assert_eq!(script.started().len(), 1);
    }

    #[test]
    fn the_hedge_delay_follows_the_hop_within_its_bounds() {
        assert_eq!(hedge_delay(None, false), HEDGE_DELAY);
        assert_eq!(hedge_delay(Some(ms(100)), false), HEDGE_DELAY);
        assert_eq!(hedge_delay(Some(ms(400)), false), ms(600));
        assert_eq!(hedge_delay(Some(ms(5000)), false), HEDGE_DELAY_MAX);
        assert_eq!(hedge_delay(Some(ms(100)), true), HEDGE_DELAY_MAX);
    }
}
