use crate::prepared::PreparedForwarding;
use guru_worker_config::{Config, Forwarding, Transport};
use std::collections::HashMap;
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

/// A forwarding that failed to prepare, named by its tag.
#[derive(Debug)]
pub struct ApplyError {
    pub tag: String,
    pub error: crate::BoxError,
}

impl std::fmt::Display for ApplyError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}: {}", self.tag, self.error)
    }
}

impl std::error::Error for ApplyError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(self.error.as_ref())
    }
}

enum PendingSocket {
    Tcp(tokio::net::TcpListener),
    Quic(quinn::Endpoint),
}

struct Pending {
    key: ListenKey,
    prepared: Arc<PreparedForwarding>,
    socket: PendingSocket,
}

/// Whether two listeners contend for the same OS socket: same transport, same port and
/// overlapping addresses — a wildcard address covers every local ip, across families.
fn contends(a: &ListenKey, b: &ListenKey) -> bool {
    a.1 == b.1
        && a.0.port() == b.0.port()
        && (a.0.ip() == b.0.ip() || a.0.ip().is_unspecified() || b.0.ip().is_unspecified())
}

/// Binds the socket a forwarding listens on, tagging the failure with its tag.
fn bind(key: &ListenKey, prepared: &PreparedForwarding) -> Result<PendingSocket, ApplyError> {
    let tag = || prepared.forwarding.tag.clone();
    match key.1 {
        Transport::Tcp => crate::listener::bind_tcp(key.0)
            .map(PendingSocket::Tcp)
            .map_err(|error| ApplyError { tag: tag(), error }),
        Transport::Quic => {
            let sc = prepared.quic_server.clone().ok_or_else(|| ApplyError {
                tag: tag(),
                error: "quic listener without server config".into(),
            })?;
            crate::listener::bind_quic(key.0, sc)
                .map(PendingSocket::Quic)
                .map_err(|error| ApplyError { tag: tag(), error })
        }
    }
}

/// Owns all running listeners keyed by `(addr, transport)` and applies config diffs.
pub struct Supervisor {
    listeners: HashMap<ListenKey, ListenerHandle>,
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
        }
    }

    /// Applies a config all-or-nothing.
    ///
    /// Everything fallible — compiling each forwarding and binding every new socket —
    /// happens before anything is mutated, so a failure leaves the running listeners
    /// exactly as they were and releases the sockets bound during the attempt.
    ///
    /// The one address a bind cannot be tried on up front is an address this very
    /// revision vacates: that listener is closed first, and its close is awaited, so a
    /// replacement on the same ip:port binds in the same revision. If that last bind
    /// still fails, the closed listener is brought back before the error is returned.
    ///
    /// On success: removed listeners stop accepting without dropping in-flight
    /// connections, retained listeners hot-swap their config, new listeners are spawned.
    pub async fn apply(&mut self, cfg: &Config) -> Result<(), ApplyError> {
        let desired: HashMap<ListenKey, Forwarding> = cfg
            .forwardings
            .iter()
            .map(|f| (f.listen_key(), f.clone()))
            .collect();

        // --- prepare: nothing below mutates `self` ---
        let mut retained: Vec<(ListenKey, Arc<PreparedForwarding>)> = Vec::new();
        let mut wanted: Vec<(ListenKey, Arc<PreparedForwarding>)> = Vec::new();
        for (key, f) in desired.iter() {
            let prepared = Arc::new(PreparedForwarding::build(f, cfg.ipv6_resolve).map_err(
                |error| ApplyError {
                    tag: f.tag.clone(),
                    error,
                },
            )?);
            // A listener whose accept loop has ended cannot be hot-swapped — it serves
            // nothing — so it is rebuilt rather than retained.
            if self
                .listeners
                .get(key)
                .is_some_and(ListenerHandle::is_alive)
            {
                retained.push((*key, prepared));
            } else {
                wanted.push((*key, prepared));
            }
        }

        let stale: Vec<ListenKey> = self
            .listeners
            .keys()
            .filter(|k| !desired.contains_key(*k))
            .cloned()
            .collect();
        // Removals whose socket stands in the way of a bind this revision needs.
        let (blocking, plain): (Vec<ListenKey>, Vec<ListenKey>) = stale
            .into_iter()
            .partition(|s| wanted.iter().any(|(k, _)| contends(s, k)));

        let mut pending: Vec<Pending> = Vec::new();
        let mut contending: Vec<(ListenKey, Arc<PreparedForwarding>)> = Vec::new();
        for (key, prepared) in wanted {
            if blocking.iter().any(|s| contends(s, &key)) {
                contending.push((key, prepared));
                continue;
            }
            let socket = bind(&key, &prepared)?;
            pending.push(Pending {
                key,
                prepared,
                socket,
            });
        }
        // `desired` is a hash map, so without this the takeover order — and hence which
        // address a partly-failed revision reports — would differ from run to run.
        contending.sort_unstable_by_key(|((addr, transport), _)| {
            (*addr, matches!(transport, Transport::Quic))
        });

        // --- commit ---
        // Close before binding: a replacement cannot take an address while the listener
        // it replaces still owns the socket.
        let mut vacated: Vec<(ListenKey, Arc<PreparedForwarding>)> = Vec::new();
        for k in blocking {
            if let Some(h) = self.listeners.remove(&k) {
                let previous = h.prepared();
                h.close().await;
                tracing::info!(addr = ?k.0, transport = ?k.1, "listener removed");
                vacated.push((k, previous));
            }
        }
        // These sockets are kept apart from `pending` until they are all bound: if one
        // fails, the ones already taken hold addresses the restored listeners need back.
        let mut taken: Vec<Pending> = Vec::new();
        for (key, prepared) in contending {
            match bind(&key, &prepared) {
                Ok(socket) => taken.push(Pending {
                    key,
                    prepared,
                    socket,
                }),
                Err(e) => {
                    drop(taken);
                    self.restore(vacated);
                    return Err(e);
                }
            }
        }
        pending.append(&mut taken);

        for k in plain {
            if let Some(h) = self.listeners.remove(&k) {
                h.token.cancel();
                tracing::info!(addr = ?k.0, transport = ?k.1, "listener removed");
            }
        }

        let mut swap_failed: Option<ApplyError> = None;
        for (key, prepared) in retained {
            let tag = prepared.forwarding.tag.clone();
            let Some(h) = self.listeners.get(&key) else {
                continue;
            };
            if h.cfg_tx.send(prepared).is_err() {
                // The accept loop ended between prepare and commit, so nothing received
                // the new config. Drop the corpse — the next revision rebuilds it — and
                // fail the apply so the worker never acks a config it is not serving.
                self.listeners.remove(&key);
                swap_failed.get_or_insert(ApplyError {
                    tag,
                    error: "listener stopped before its new config was applied".into(),
                });
            }
        }

        for Pending {
            key,
            prepared,
            socket,
        } in pending
        {
            self.spawn(key, prepared, socket);
        }

        match swap_failed {
            Some(e) => Err(e),
            None => Ok(()),
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
    /// failed to bind, so a failed revision does not leave that address unserved.
    fn restore(&mut self, vacated: Vec<(ListenKey, Arc<PreparedForwarding>)>) {
        for (key, prepared) in vacated {
            match bind(&key, &prepared) {
                Ok(socket) => self.spawn(key, prepared, socket),
                Err(e) => tracing::error!(
                    addr = ?key.0,
                    transport = ?key.1,
                    error = %e.error,
                    "could not restore the listener a failed apply had stopped"
                ),
            }
        }
    }

    pub fn shutdown_all(&self) {
        for h in self.listeners.values() {
            h.token.cancel();
        }
    }
}
