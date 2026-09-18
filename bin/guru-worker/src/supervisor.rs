use crate::liveness::Liveness;
use crate::prepared::{PreparedForwarding, Settings};
use crate::stats::Stats;
use guru_worker_config::{Config, Forwarding, LogConfig, Transport};
use std::collections::{HashMap, HashSet};
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

type ListenKey = (SocketAddr, Transport);

/// How long removing a listener waits for its accept loop to release the socket on its
/// own before a replacement is bound on the same address. A cancelled TCP loop drops
/// its socket at once; a QUIC endpoint first drains its connections, so this is the
/// grace that drain gets before the takeover forces it down.
const CLOSE_TIMEOUT: Duration = Duration::from_secs(3);

struct ListenerHandle {
    cfg_tx: watch::Sender<Arc<PreparedForwarding>>,
    token: CancellationToken,
    /// The accept loop. It owns the socket, so joining it is the only proof the socket
    /// is really closed.
    task: JoinHandle<()>,
}

impl ListenerHandle {
    /// Whether the accept loop is still running. A finished loop leaves a handle that
    /// can no longer be hot-swapped: its socket is gone and its config receiver with it.
    fn is_alive(&self) -> bool {
        !self.cfg_tx.is_closed() && !self.task.is_finished()
    }

    fn prepared(&self) -> Arc<PreparedForwarding> {
        self.cfg_tx.borrow().clone()
    }

    /// Stops the accept loop and returns only once it has dropped its socket, so the
    /// address is free for a replacement to bind.
    ///
    /// Cancellation lets the loop finish on its own terms — a QUIC endpoint drains its
    /// live connections first. An accept loop that is still holding the socket after
    /// [`CLOSE_TIMEOUT`] is aborted: awaiting the aborted task is what proves the socket
    /// is gone, and the alternative — binding anyway — is the `EADDRINUSE` this close
    /// exists to prevent.
    async fn close(self) {
        self.token.cancel();
        let mut task = self.task;
        if tokio::time::timeout(CLOSE_TIMEOUT, &mut task)
            .await
            .is_err()
        {
            tracing::warn!(
                timeout = ?CLOSE_TIMEOUT,
                "listener did not release its socket in time; forcing it down"
            );
            task.abort();
            let _ = task.await;
        }
    }
}

/// The state of one `[[forwarding]]`, named by its tag. `error` is the reason the tag
/// does not run the shape last asked of it: it may still serve an earlier shape.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PodStatus {
    pub tag: String,
    pub error: Option<String>,
}

/// What [`Supervisor::apply`] did with each forwarding of a config, in config order.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ApplyOutcome {
    pub pods: Vec<PodStatus>,
}

impl ApplyOutcome {
    /// The pods that did not take the shape the config asked for.
    pub fn failed(&self) -> impl Iterator<Item = &PodStatus> {
        self.pods.iter().filter(|p| p.error.is_some())
    }
}

enum PendingSocket {
    Tcp(tokio::net::TcpListener),
    Quic(quinn::Endpoint),
}

/// Whether two listeners contend for the same OS socket: same transport, same port and
/// overlapping addresses — a wildcard address covers every local ip, across families.
fn contends(a: &ListenKey, b: &ListenKey) -> bool {
    a.1 == b.1
        && a.0.port() == b.0.port()
        && (a.0.ip() == b.0.ip() || a.0.ip().is_unspecified() || b.0.ip().is_unspecified())
}

/// Binds the socket a forwarding listens on.
fn bind(key: &ListenKey, prepared: &PreparedForwarding) -> Result<PendingSocket, crate::BoxError> {
    match key.1 {
        Transport::Tcp => crate::listener::bind_tcp(key.0).map(PendingSocket::Tcp),
        Transport::Quic => {
            let sc = prepared
                .quic_server
                .clone()
                .ok_or("quic listener without server config")?;
            crate::listener::bind_quic(key.0, sc).map(PendingSocket::Quic)
        }
    }
}

/// A listener closed to free its address for a replacement, kept so it can be brought
/// back when the replacement fails to bind.
struct Vacated {
    key: ListenKey,
    prepared: Arc<PreparedForwarding>,
    /// The tag that was running on it, if any: only an owned listener is restored.
    owner: Option<String>,
}

/// Owns all running listeners keyed by `(addr, transport)` and applies config diffs
/// per forwarding.
pub struct Supervisor {
    listeners: HashMap<ListenKey, ListenerHandle>,
    /// The listener each tag currently runs. Invariant: the listener at `running[tag]`
    /// holds that tag's prepared forwarding, and no other tag maps to the same key.
    running: HashMap<String, ListenKey>,
    /// Why a tag does not run the shape the last config asked of it.
    errors: HashMap<String, String>,
    /// Top-level settings of the last applied config, which every running listener
    /// is compiled under, re-emitted by [`running_config`].
    ///
    /// [`running_config`]: Supervisor::running_config
    settings: Settings,
    log: LogConfig,
    stats: Arc<Stats>,
    /// What is known about every next hop, kept across applies.
    liveness: Arc<Liveness>,
}

impl Default for Supervisor {
    fn default() -> Self {
        Self::new()
    }
}

impl Supervisor {
    pub fn new() -> Self {
        Self {
            listeners: HashMap::new(),
            running: HashMap::new(),
            errors: HashMap::new(),
            settings: Settings::default(),
            log: LogConfig::default(),
            stats: Arc::new(Stats::default()),
            liveness: Arc::new(Liveness::default()),
        }
    }

    /// What the worker knows about its next hops.
    pub fn liveness(&self) -> Arc<Liveness> {
        self.liveness.clone()
    }

    /// The traffic counters of every tag, shared with the connections it serves.
    pub fn stats(&self) -> Arc<Stats> {
        self.stats.clone()
    }

    /// Applies a config one forwarding at a time.
    ///
    /// Every forwarding is compiled first; each one that compiles is then committed on
    /// its own — hot-swapped into the live listener on its address, or bound afresh
    /// after the listeners in its way have been closed. Forwardings whose socket no
    /// other tag's listener holds go first (a tag may displace its own listener, and
    /// gets it back if the new bind fails); only then may a forwarding claim a socket
    /// another tag ran on before, and only once that tag has moved successfully in this
    /// apply — otherwise the claimant fails and the incumbent stays. A forwarding that
    /// fails at any step keeps the listener its tag ran before, on whatever address
    /// that was, even when the new config no longer names that address. What it keeps
    /// is its shape, not the previous top-level settings: those are the config's for
    /// every pod, so the kept shape is compiled again under them. Only listeners that
    /// no tag of the config still runs are stopped, and they stop accepting without
    /// dropping in-flight connections; `apply` returns once their sockets are released,
    /// so the next config may bind the same addresses.
    ///
    /// The outcome names every forwarding of the config with the error that kept it
    /// from taking its new shape, in config order. The config's `[log] level`
    /// becomes the process log filter before anything else ([`crate::apply_log_level`]).
    pub async fn apply(&mut self, cfg: &Config) -> ApplyOutcome {
        crate::apply_log_level(&cfg.log.level);
        self.log = cfg.log.clone();
        let settings = Settings::of(cfg);
        self.settings = settings.clone();

        // Tags the config dropped run nothing from here on; their listeners are stopped
        // once every surviving tag has been placed.
        let tags: HashSet<&str> = cfg.forwardings.iter().map(|f| f.tag.as_str()).collect();
        self.running.retain(|tag, _| tags.contains(tag.as_str()));
        self.errors.retain(|tag, _| tags.contains(tag.as_str()));
        self.stats.retain(|tag| tags.contains(tag));

        let mut candidates: Vec<(ListenKey, Arc<PreparedForwarding>)> = Vec::new();
        let mut errors: HashMap<String, String> = HashMap::new();
        for f in &cfg.forwardings {
            match PreparedForwarding::build(f, &settings, self.stats.tag(&f.tag), &self.liveness) {
                Ok(prepared) => candidates.push((f.listen_key(), Arc::new(prepared))),
                Err(e) => {
                    errors.insert(f.tag.clone(), e.to_string());
                }
            }
        }

        // Placement order: a candidate blocked by another tag's listener waits until
        // that tag has moved. Each round places every candidate that is free to go as
        // of the start of the round; the rounds end when one places nothing, and what
        // is still blocked then — by a tag that failed, or by a cycle of swaps — fails
        // without touching the incumbent.
        let mut pending = candidates;
        loop {
            let blocked: Vec<Option<String>> = pending
                .iter()
                .map(|(key, prepared)| self.held_by_other(key, &prepared.forwarding.tag))
                .collect();
            let mut deferred = Vec::new();
            let mut placed = false;
            for ((key, prepared), holder) in pending.into_iter().zip(blocked) {
                if holder.is_some() {
                    deferred.push((key, prepared));
                    continue;
                }
                placed = true;
                let tag = prepared.forwarding.tag.clone();
                match self.place(key, prepared).await {
                    Ok(()) => {
                        self.running.insert(tag, key);
                    }
                    Err(e) => {
                        errors.insert(tag, e);
                    }
                }
            }
            if deferred.is_empty() {
                break;
            }
            if !placed {
                for (key, prepared) in deferred {
                    let tag = &prepared.forwarding.tag;
                    let holder = self
                        .held_by_other(&key, tag)
                        .unwrap_or_else(|| "another pod".to_string());
                    errors.insert(
                        tag.clone(),
                        format!(
                            "cannot listen on {}: socket held by pod {holder}, which kept its \
                             previous listener",
                            key.0
                        ),
                    );
                }
                break;
            }
            pending = deferred;
        }

        // A tag that kept its previous listener runs its previous shape under this
        // config's top-level settings: that is what `running_config` describes, what
        // the master records as running and what the last-known-good config replays
        // after a restart, so it has to be what serves now.
        for key in self.running.values() {
            let Some(h) = self.listeners.get(key) else {
                continue;
            };
            let current = h.prepared();
            if current.settings == settings {
                continue;
            }
            match current.with_settings(&settings, &self.liveness) {
                // Replaced even when the accept loop has ended, which `pod_statuses`
                // reports on its own.
                Ok(moved) => {
                    h.cfg_tx.send_replace(Arc::new(moved));
                }
                Err(e) => tracing::error!(
                    tag = %current.forwarding.tag,
                    error = %e,
                    "could not move a listener that kept its previous shape to the new top-level settings"
                ),
            }
        }

        // Listeners no tag runs any more are closed the way a takeover closes them in
        // `place`: a handle merely dropped detaches its accept loop, which keeps the
        // socket until it gets round to ending, and the next config's bind of the same
        // address fails on it. They close side by side, so a config that drops several
        // QUIC listeners waits one `CLOSE_TIMEOUT` at most, not one each.
        let owned: HashSet<ListenKey> = self.running.values().copied().collect();
        let removed: Vec<ListenKey> = self
            .listeners
            .keys()
            .filter(|key| !owned.contains(*key))
            .copied()
            .collect();
        let mut closing = tokio::task::JoinSet::new();
        for key in removed {
            if let Some(h) = self.listeners.remove(&key) {
                tracing::info!(addr = ?key.0, transport = ?key.1, "listener removed");
                closing.spawn(h.close());
            }
        }
        while closing.join_next().await.is_some() {}

        let pods: Vec<PodStatus> = cfg
            .forwardings
            .iter()
            .map(|f| PodStatus {
                tag: f.tag.clone(),
                error: errors.get(&f.tag).cloned(),
            })
            .collect();
        for (tag, error) in &errors {
            tracing::error!(tag = %tag, error = %error, "forwarding not applied");
        }
        self.errors = errors;
        ApplyOutcome { pods }
    }

    /// The tag other than `tag` whose live listener holds a socket `key` contends
    /// for, if any. A listener whose accept loop has ended holds nothing.
    fn held_by_other(&self, key: &ListenKey, tag: &str) -> Option<String> {
        self.running
            .iter()
            .filter(|(owner, _)| owner.as_str() != tag)
            .find(|(_, k)| {
                contends(k, key) && self.listeners.get(k).is_some_and(ListenerHandle::is_alive)
            })
            .map(|(owner, _)| owner.clone())
    }

    /// Puts one compiled forwarding into service on `key`. Every listener in its way
    /// is either the tag's own — closed, and restored if the new bind fails — or one
    /// no tag runs any more.
    async fn place(
        &mut self,
        key: ListenKey,
        prepared: Arc<PreparedForwarding>,
    ) -> Result<(), String> {
        let tag = prepared.forwarding.tag.clone();
        if self
            .listeners
            .get(&key)
            .is_some_and(ListenerHandle::is_alive)
        {
            match self.listeners.get(&key) {
                Some(h) if h.cfg_tx.send(prepared.clone()).is_ok() => {
                    // The listener may have been running a tag that moved away in this
                    // apply; it is this tag's now.
                    self.running.retain(|_, k| *k != key);
                    return Ok(());
                }
                // The accept loop ended between the check and the send: the socket is
                // going away, so the listener is rebuilt like a dead one below.
                _ => {}
            }
        }

        // Close before binding: a replacement cannot take an address while the listener
        // it replaces — or a dead listener still holding the socket — owns it.
        let blocking: Vec<ListenKey> = self
            .listeners
            .keys()
            .filter(|k| contends(k, &key))
            .copied()
            .collect();
        let mut vacated: Vec<Vacated> = Vec::new();
        for k in blocking {
            if let Some(h) = self.listeners.remove(&k) {
                let previous = h.prepared();
                h.close().await;
                let owner = self
                    .running
                    .iter()
                    .find(|(_, running)| **running == k)
                    .map(|(tag, _)| tag.clone());
                if let Some(tag) = &owner {
                    self.running.remove(tag);
                }
                tracing::info!(addr = ?k.0, transport = ?k.1, "listener closed for takeover");
                vacated.push(Vacated {
                    key: k,
                    prepared: previous,
                    owner,
                });
            }
        }

        match bind(&key, &prepared) {
            Ok(socket) => {
                self.spawn(key, prepared, socket);
                Ok(())
            }
            Err(e) => {
                // Only the tag's own listener comes back: anything else that was in
                // the way belonged to a tag that already runs elsewhere.
                vacated.retain(|v| v.owner.as_deref() == Some(tag.as_str()));
                self.restore(vacated);
                Err(e.to_string())
            }
        }
    }

    fn spawn(&mut self, key: ListenKey, prepared: Arc<PreparedForwarding>, socket: PendingSocket) {
        let tag = prepared.forwarding.tag.clone();
        let (cfg_tx, cfg_rx) = watch::channel(prepared);
        let token = CancellationToken::new();
        let task = match socket {
            PendingSocket::Tcp(l) => {
                tokio::spawn(crate::listener::run_tcp(l, cfg_rx, token.clone()))
            }
            PendingSocket::Quic(ep) => {
                tokio::spawn(crate::listener::run_quic(ep, cfg_rx, token.clone()))
            }
        };
        self.listeners.insert(
            key,
            ListenerHandle {
                cfg_tx,
                token,
                task,
            },
        );
        tracing::info!(addr = ?key.0, transport = ?key.1, tag = %tag, "listener started");
    }

    /// Brings back listeners that were closed to free an address whose replacement then
    /// failed to bind, so a failed takeover does not leave that address unserved.
    fn restore(&mut self, vacated: Vec<Vacated>) {
        for Vacated {
            key,
            prepared,
            owner,
        } in vacated
        {
            let Some(owner) = owner else {
                continue;
            };
            match bind(&key, &prepared) {
                Ok(socket) => {
                    self.spawn(key, prepared, socket);
                    self.running.insert(owner, key);
                }
                Err(e) => tracing::error!(
                    addr = ?key.0,
                    transport = ?key.1,
                    tag = %owner,
                    error = %e,
                    "could not restore the listener a failed takeover had stopped"
                ),
            }
        }
    }

    /// The config actually in service: every tag's running shape — the new one when it
    /// applied, the previous one when it did not — under the last config's top-level
    /// settings, which every running listener is compiled under. Forwardings are in tag
    /// order so the same mix renders the same TOML.
    pub fn running_config(&self) -> Config {
        let mut tags: Vec<&String> = self.running.keys().collect();
        tags.sort_unstable();
        let forwardings: Vec<Forwarding> = tags
            .into_iter()
            .filter_map(|tag| self.prepared(tag))
            .map(|prepared| (*prepared.forwarding).clone())
            .collect();
        Config {
            ipv6_resolve: self.settings.ipv6_resolve,
            log: self.log.clone(),
            relay_ca: self.settings.relay_ca.clone(),
            keepalive: self.settings.keepalive,
            quic: self.settings.quic,
            forwardings,
        }
    }

    /// The compiled forwarding the listener of `tag` serves.
    pub fn prepared(&self, tag: &str) -> Option<Arc<PreparedForwarding>> {
        self.listeners
            .get(self.running.get(tag)?)
            .map(ListenerHandle::prepared)
    }

    /// Every tag of the last config with the reason it is not serving what that config
    /// asked for: its apply error, or an accept loop that has since stopped.
    pub fn pod_statuses(&self) -> Vec<PodStatus> {
        let mut tags: Vec<&String> = self.running.keys().chain(self.errors.keys()).collect();
        tags.sort_unstable();
        tags.dedup();
        tags.into_iter()
            .map(|tag| {
                let error = self.errors.get(tag).cloned().or_else(|| {
                    let alive = self
                        .running
                        .get(tag)
                        .and_then(|key| self.listeners.get(key))
                        .is_some_and(ListenerHandle::is_alive);
                    (!alive).then(|| "listener stopped".to_string())
                });
                PodStatus {
                    tag: tag.clone(),
                    error,
                }
            })
            .collect()
    }

    pub fn shutdown_all(&self) {
        for h in self.listeners.values() {
            h.token.cancel();
        }
    }
}
