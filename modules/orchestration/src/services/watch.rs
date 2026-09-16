//! Per-server signal hub plus a config-view poller.
//!
//! Mutations happen in the dashboard process while streams live in the worker-facing
//! process, so an in-process broadcast alone can never observe a new revision. The
//! poller watches the config view; a stream reacts to any change by attempting a
//! conditional send, so the database — not this process — decides what is sent and
//! to whom.
//!
//! # One session per server
//!
//! Exactly one worker owns a server at a time, and ownership is global rather than
//! per master replica because both halves of it live on the server row:
//!
//! - `refresh_key_generation` is bumped by `Register`, `watch_epoch` by every stream
//!   claim. The pair is a [`WatchFence`]: a stream is authoritative exactly while
//!   the row still carries the pair it won, and the fence only moves forward.
//! - `session_lease_until` is taken by `Register`, refreshed by the live stream's
//!   heartbeat and dropped when that stream ends. `Register` is refused while the
//!   lease is alive, so two workers pointed at one server cannot take turns
//!   stealing it; takeover waits for the incumbent to release or to stop
//!   heartbeating (see [`SessionLease`]).
//!
//! The hub only mirrors that state so a fenced-out stream dies immediately instead
//! of at the next poll. The database, not this process, is the source of truth.

use crate::entities::db::view::{ListServerWatchState, ServerWatchState};
use base::db::Db;
use kanau::processor::Processor;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::broadcast;
use tokio_util::sync::CancellationToken;

const CHANNEL_CAPACITY: usize = 8;

/// How long a worker session owns its server without a heartbeat, and how often a
/// live stream refreshes that ownership.
///
/// The lease is what keeps two workers configured with the same `server_id` from
/// trading the server back and forth: the incumbent renews while its stream lives,
/// and a contender's `Register` is refused until the lease lapses (crash) or the
/// stream releases it (clean restart).
#[derive(Debug, Clone, Copy)]
pub struct SessionLease {
    pub ttl: Duration,
    pub heartbeat: Duration,
}

impl Default for SessionLease {
    fn default() -> Self {
        Self {
            ttl: Duration::from_secs(30),
            heartbeat: Duration::from_secs(10),
        }
    }
}

impl SessionLease {
    /// The deadline a session taken at `now` gets.
    pub fn until(&self, now: chrono::DateTime<chrono::Utc>) -> chrono::DateTime<chrono::Utc> {
        let ttl = chrono::TimeDelta::from_std(self.ttl).unwrap_or(chrono::TimeDelta::MAX);
        now.checked_add_signed(ttl)
            .unwrap_or(chrono::DateTime::<chrono::Utc>::MAX_UTC)
    }
}

/// Identifies the one live watch session of a server.
///
/// Both halves live on the server row: `generation` is bumped by every
/// registration, `epoch` by every stream claim. Ordering is lexicographic, which
/// makes the fence monotonic across master replicas — a stream is authoritative
/// exactly while the row still carries its own pair.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct WatchFence {
    pub generation: i64,
    pub epoch: i64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AgentSignal {
    /// Something about this server's config view changed; try to take a snapshot.
    Changed,
    /// The authoritative session moved on; every stream whose fence differs from
    /// the carried one must end.
    Fenced(WatchFence),
}

/// `(desired, in_flight, applied, failed)` revisions: the whole observable state
/// of a view as far as a stream is concerned.
///
/// `applied` is part of it because it is part of what the conditional take
/// compares (`desired.revision != applied.revision`): clearing a settled server's
/// applied snapshot changes nothing else, and a stream that is not woken for it
/// never takes the snapshot its dependants wait for.
type ViewState = (Option<i64>, Option<i64>, Option<i64>, Option<i64>);

struct Entry {
    tx: broadcast::Sender<AgentSignal>,
    subscribers: usize,
    last: Option<ViewState>,
    fence: WatchFence,
}

#[derive(Clone, Default)]
pub struct WatchHub {
    entries: Arc<Mutex<HashMap<String, Entry>>>,
}

pub struct WatchSubscription {
    pub rx: broadcast::Receiver<AgentSignal>,
    server_key: String,
    entries: Arc<Mutex<HashMap<String, Entry>>>,
}

impl Drop for WatchSubscription {
    fn drop(&mut self) {
        let Ok(mut entries) = self.entries.lock() else {
            return;
        };
        let remove = match entries.get_mut(&self.server_key) {
            Some(entry) => {
                entry.subscribers = entry.subscribers.saturating_sub(1);
                entry.subscribers == 0
            }
            None => false,
        };
        if remove {
            entries.remove(&self.server_key);
        }
    }
}

impl WatchHub {
    /// Registers interest in one server; the guard deregisters on drop.
    ///
    /// Returns `None` when `fence` is already outranked — a claim that lost a race
    /// against a newer registration or a newer stream must never pull the hub's
    /// fence backwards, or it would resurrect a session the database has retired.
    pub fn subscribe(&self, server_key: &str, fence: WatchFence) -> Option<WatchSubscription> {
        let mut entries = match self.entries.lock() {
            Ok(entries) => entries,
            Err(poisoned) => poisoned.into_inner(),
        };
        let entry = entries.entry(server_key.to_string()).or_insert_with(|| {
            let (tx, _) = broadcast::channel(CHANNEL_CAPACITY);
            Entry {
                tx,
                subscribers: 0,
                last: None,
                fence,
            }
        });
        if entry.fence > fence {
            return None;
        }
        if entry.fence < fence {
            entry.fence = fence;
            // Ends the streams this claim replaces before the new receiver exists.
            let _ = entry.tx.send(AgentSignal::Fenced(fence));
        }
        entry.subscribers = entry.subscribers.saturating_add(1);
        let rx = entry.tx.subscribe();
        drop(entries);
        Some(WatchSubscription {
            rx,
            server_key: server_key.to_string(),
            entries: self.entries.clone(),
        })
    }

    /// Called by `RegisterWorker` in this process for instant supersession. A fresh
    /// generation has not claimed a stream yet, hence epoch zero.
    pub fn supersede(&self, server_key: &str, generation: i64) {
        self.advance_fence(
            server_key,
            WatchFence {
                generation,
                epoch: 0,
            },
        );
    }

    /// Moves the fence forward and ends every stream it retires. Older fences are
    /// stale reads and are ignored.
    fn advance_fence(&self, server_key: &str, fence: WatchFence) {
        let mut entries = match self.entries.lock() {
            Ok(entries) => entries,
            Err(poisoned) => poisoned.into_inner(),
        };
        if let Some(entry) = entries.get_mut(server_key)
            && entry.fence < fence
        {
            entry.fence = fence;
            let _ = entry.tx.send(AgentSignal::Fenced(fence));
        }
    }

    pub(crate) fn watched_servers(&self) -> Vec<crate::entities::db::server::ServerId> {
        let entries = match self.entries.lock() {
            Ok(entries) => entries,
            Err(poisoned) => poisoned.into_inner(),
        };
        entries
            .keys()
            .map(|key| crate::utils::ids::server_id(key))
            .collect()
    }

    pub(crate) fn publish(&self, state: &ServerWatchState) {
        let key = state.id.to_string();
        let fence = WatchFence {
            generation: state.refresh_key_generation,
            epoch: state.watch_epoch,
        };
        let mut entries = match self.entries.lock() {
            Ok(entries) => entries,
            Err(poisoned) => poisoned.into_inner(),
        };
        let Some(entry) = entries.get_mut(&key) else {
            return;
        };
        // One snapshot, one epoch, one lock: a poll that is behind the current
        // session says nothing about it, and the next tick re-reads anyway.
        if fence < entry.fence {
            return;
        }
        if fence > entry.fence {
            entry.fence = fence;
            let _ = entry.tx.send(AgentSignal::Fenced(fence));
            return;
        }
        // The hub does not decide what is sendable — it only says "look again".
        // Whether anything is actually handed over is one conditional update in
        // the database, so a spurious wake costs a query and never a wrong send.
        let observed = (
            state.desired_revision,
            state.in_flight_revision,
            state.applied_revision,
            state.failed_revision,
        );
        if entry.last != Some(observed) {
            entry.last = Some(observed);
            let _ = entry.tx.send(AgentSignal::Changed);
        }
    }
}

/// Polls the config view of every watched server until `shutdown`.
pub async fn run_poller(hub: WatchHub, db: Db, interval: Duration, shutdown: CancellationToken) {
    let mut ticker = tokio::time::interval(interval);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        tokio::select! {
            _ = shutdown.cancelled() => return,
            _ = ticker.tick() => {}
        }
        let servers = hub.watched_servers();
        if servers.is_empty() {
            continue;
        }
        match db.process(ListServerWatchState { servers }).await {
            Ok(states) => {
                for state in &states {
                    hub.publish(state);
                }
            }
            Err(e) => tracing::error!(error = %e, "watch poll failed"),
        }
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

    use super::*;

    fn state(key: &str, desired: i64, fence: WatchFence) -> ServerWatchState {
        ServerWatchState {
            id: crate::utils::ids::server_id(key),
            refresh_key_generation: fence.generation,
            watch_epoch: fence.epoch,
            desired_revision: Some(desired),
            in_flight_revision: None,
            applied_revision: None,
            failed_revision: None,
        }
    }

    fn fence(generation: i64, epoch: i64) -> WatchFence {
        WatchFence { generation, epoch }
    }

    #[test]
    fn an_outranked_claim_is_refused_and_leaves_the_fence_alone() {
        let hub = WatchHub::default();
        let mut live = hub.subscribe("s", fence(2, 3)).unwrap();
        assert!(hub.subscribe("s", fence(2, 2)).is_none());
        assert!(hub.subscribe("s", fence(1, 9)).is_none());
        // The live session was never told to go away.
        assert!(matches!(
            live.rx.try_recv(),
            Err(broadcast::error::TryRecvError::Empty)
        ));
    }

    #[test]
    fn a_newer_fence_ends_the_session_it_replaces() {
        let hub = WatchHub::default();
        let mut first = hub.subscribe("s", fence(1, 1)).unwrap();
        let _second = hub.subscribe("s", fence(1, 2)).unwrap();
        assert_eq!(first.rx.try_recv(), Ok(AgentSignal::Fenced(fence(1, 2))));

        // A registration in this process, and a poll that saw one elsewhere.
        hub.supersede("s", 2);
        assert_eq!(first.rx.try_recv(), Ok(AgentSignal::Fenced(fence(2, 0))));
        hub.publish(&state("s", 3, fence(2, 1)));
        assert_eq!(first.rx.try_recv(), Ok(AgentSignal::Fenced(fence(2, 1))));
        // The fence advance is the whole message: config belongs to the session
        // that now owns the server, and the next poll wakes it.
        assert!(matches!(
            first.rx.try_recv(),
            Err(broadcast::error::TryRecvError::Empty)
        ));
        hub.publish(&state("s", 3, fence(2, 1)));
        assert_eq!(first.rx.try_recv(), Ok(AgentSignal::Changed));
    }

    #[test]
    fn a_stale_poll_is_ignored_entirely() {
        let hub = WatchHub::default();
        let mut live = hub.subscribe("s", fence(3, 4)).unwrap();
        // A snapshot from before this session was claimed: neither its fence nor
        // its state may be applied to the session that replaced it.
        hub.publish(&state("s", 9, fence(2, 8)));
        assert!(matches!(
            live.rx.try_recv(),
            Err(broadcast::error::TryRecvError::Empty)
        ));
        hub.publish(&state("s", 9, fence(3, 4)));
        assert_eq!(live.rx.try_recv(), Ok(AgentSignal::Changed));
    }

    #[test]
    fn a_repeated_poll_with_the_same_state_is_not_rebroadcast() {
        let hub = WatchHub::default();
        let mut live = hub.subscribe("s", fence(1, 1)).unwrap();
        hub.publish(&state("s", 4, fence(1, 1)));
        hub.publish(&state("s", 4, fence(1, 1)));
        assert_eq!(live.rx.try_recv(), Ok(AgentSignal::Changed));
        // Waking a stream once per tick would have it re-query the database every
        // poll interval for as long as anything is undeliverable.
        assert!(matches!(
            live.rx.try_recv(),
            Err(broadcast::error::TryRecvError::Empty)
        ));
    }
}
