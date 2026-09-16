//! Choosing a next hop for one connection, and connecting to it.
//!
//! A connection walks its forwarding's [`Route`]. A failover takes its members
//! in order; a balance picks by weight (smooth round robin, weighted random, or
//! by client address when sticky). Either one passes over members whose every
//! hop is dead, and when an attempt fails it moves on to the next choice within
//! the same connection, so a client only sees a failure once every choice has
//! failed. What is left untried because it was dead is then tried anyway, in the
//! same order: the worker's record may be out of date.
//!
//! Every connection has a budget of [`MAX_ATTEMPTS`] hop attempts and a
//! deadline; each attempt to connect is bounded by [`ATTEMPT_TIMEOUT`] on top.
//! A relay that confirms (see [`crate::pipe::relay`]) is only connected once it
//! says its own next hop answered, so a dead exit behind a live relay fails the
//! relay here, and the choice moves on.

use crate::BoxError;
use crate::liveness::Availability;
use crate::pipe::{TargetStream, exit, relay};
use crate::prepared::{Balance, Hop, HopTarget, Pick, Route};
use std::net::{IpAddr, SocketAddr};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

/// How long one attempt may take to connect: DNS, TCP, and a TLS or QUIC
/// handshake. A relay's confirmation is bounded by the connection's deadline
/// instead, since the relay may need its own attempts first.
pub const ATTEMPT_TIMEOUT: Duration = Duration::from_secs(5);

/// How long a client connection may take to reach a next hop, all attempts
/// included.
pub const CONNECT_BUDGET: Duration = Duration::from_secs(15);

/// How many hops one client connection may try.
pub const MAX_ATTEMPTS: u32 = 4;

/// One connection's progress through its route.
pub struct Dial {
    client: SocketAddr,
    deadline: Instant,
    attempts_left: u32,
    /// The last failure, for the error the client's connection ends with.
    last_error: Option<String>,
}

impl Dial {
    pub fn new(client: SocketAddr, deadline: Instant) -> Self {
        Self {
            client,
            deadline,
            attempts_left: MAX_ATTEMPTS,
            last_error: None,
        }
    }

    fn exhausted(&self) -> bool {
        self.attempts_left == 0 || Instant::now() >= self.deadline
    }

    fn failure(&self) -> BoxError {
        match &self.last_error {
            Some(error) => error.clone().into(),
            None if self.attempts_left == 0 => "every connection attempt failed".into(),
            None => "no next hop could be tried".into(),
        }
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
    let mut dial = Dial::new(client, deadline);
    // A route that is a single hop has nothing to choose instead, dead or not.
    let forced = matches!(route, Route::Hop(_));
    match walk(route, &mut dial, forced).await {
        Some(stream) => Ok(stream),
        None => Err(dial.failure()),
    }
}

/// Whether anything under `route` may be tried without forcing.
pub fn available(route: &Route, now: Instant) -> bool {
    match route {
        Route::Hop(hop) => hop.record.availability(now) != Availability::Dead,
        Route::Balance(balance) => balance
            .members
            .iter()
            .any(|(member, _)| available(member, now)),
        Route::Failover(members) => members.iter().any(|member| available(member, now)),
    }
}

async fn walk(route: &Route, dial: &mut Dial, forced: bool) -> Option<TargetStream> {
    match route {
        Route::Hop(hop) => attempt(hop, dial, forced).await,
        Route::Failover(members) => {
            let mut skipped = Vec::new();
            for (i, member) in members.iter().enumerate() {
                if dial.exhausted() {
                    return None;
                }
                if !forced && !available(member, Instant::now()) {
                    skipped.push(i);
                    continue;
                }
                if let Some(stream) = Box::pin(walk(member, dial, forced)).await {
                    return Some(stream);
                }
            }
            for i in skipped {
                if dial.exhausted() {
                    return None;
                }
                if let Some(member) = members.get(i)
                    && let Some(stream) = Box::pin(walk(member, dial, true)).await
                {
                    return Some(stream);
                }
            }
            None
        }
        Route::Balance(balance) => {
            let mut untried: Vec<usize> = (0..balance.members.len()).collect();
            let mut skipped = Vec::new();
            loop {
                if dial.exhausted() {
                    return None;
                }
                let now = Instant::now();
                let candidates: Vec<usize> = untried
                    .iter()
                    .copied()
                    .filter(|i| {
                        forced
                            || balance
                                .members
                                .get(*i)
                                .is_some_and(|(member, _)| available(member, now))
                    })
                    .collect();
                let Some(chosen) = balance.pick(&candidates, dial.client.ip()) else {
                    skipped.extend(untried.iter().copied());
                    break;
                };
                untried.retain(|i| *i != chosen);
                if let Some((member, _)) = balance.members.get(chosen)
                    && let Some(stream) = Box::pin(walk(member, dial, forced)).await
                {
                    return Some(stream);
                }
            }
            // What was passed over as dead, best choice first.
            while !skipped.is_empty() && !dial.exhausted() {
                let Some(chosen) = balance.pick(&skipped, dial.client.ip()) else {
                    break;
                };
                skipped.retain(|i| *i != chosen);
                if let Some((member, _)) = balance.members.get(chosen)
                    && let Some(stream) = Box::pin(walk(member, dial, true)).await
                {
                    return Some(stream);
                }
            }
            None
        }
    }
}

/// One attempt on one hop, recorded against it.
async fn attempt(hop: &Hop, dial: &mut Dial, forced: bool) -> Option<TargetStream> {
    let now = Instant::now();
    if dial.exhausted() {
        return None;
    }
    // Another connection holds this dead hop's one try; it counts as tried.
    let claim = hop.record.begin(now, forced)?;
    dial.attempts_left = dial.attempts_left.saturating_sub(1);
    let connect_by = dial
        .deadline
        .min(now.checked_add(ATTEMPT_TIMEOUT).unwrap_or(now));
    let result = match &hop.target {
        HopTarget::Exit {
            destination,
            ipv6_resolve,
            send_pp,
            keepalive,
        } => match tokio::time::timeout_at(
            connect_by.into(),
            exit::connect_exit(destination, *ipv6_resolve, *send_pp, keepalive, dial.client),
        )
        .await
        {
            Ok(Ok(stream)) => Ok(TargetStream::Exit(stream)),
            Ok(Err(error)) => Err(error),
            Err(_) => Err(format!("timed out after {ATTEMPT_TIMEOUT:?}").into()),
        },
        HopTarget::Relay {
            protocol,
            destination,
            ipv6_resolve,
            sni,
            relay_ca,
            keepalive,
            quic,
            confirm,
        } => relay::dial_relay(
            &relay::Dialing {
                protocol: *protocol,
                destination,
                ipv6_resolve: *ipv6_resolve,
                sni: sni.as_deref(),
                relay_ca: relay_ca.as_deref(),
                keepalive,
                quic,
                client_addr: dial.client,
            },
            connect_by,
            confirm.then_some(dial.deadline),
        )
        .await
        .map(TargetStream::Relay),
    };
    match result {
        Ok(stream) => {
            if hop.record.is_dead() {
                tracing::info!(hop = %hop.label, "next hop answers again");
            }
            claim.succeeded();
            Some(stream)
        }
        Err(error) => {
            let was_dead = hop.record.is_dead();
            claim.failed(Instant::now());
            if !was_dead && hop.record.is_dead() {
                tracing::warn!(hop = %hop.label, %error, "next hop marked dead");
            } else {
                tracing::debug!(hop = %hop.label, %error, "next hop attempt failed");
            }
            dial.last_error = Some(format!("{}: {error}", hop.label));
            None
        }
    }
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
        let mut roll = xorshift(state).checked_rem(total)?;
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

fn xorshift(state: &AtomicU64) -> u64 {
    let mut x = state.load(Ordering::Relaxed);
    x ^= x << 13;
    x ^= x >> 7;
    x ^= x << 17;
    state.store(x, Ordering::Relaxed);
    x
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
