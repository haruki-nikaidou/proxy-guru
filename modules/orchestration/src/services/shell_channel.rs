//! The worker-facing half of the remote-shell relay (`workers_grpc`).
//!
//! A worker that opted in holds one `ShellChannel` stream to whichever replica
//! its connection landed on. This replica keeps nothing about shell sessions —
//! those live on the worker — only *which* stream answers for a server right
//! now ([`ShellChannelHub`]), so a down message published by any dashboard
//! replica reaches the worker ([`ForwardShellDown`]) and whatever the worker
//! sends back reaches the replica that asked ([`ShellUpPublisher`]).
//!
//! # Fencing
//!
//! The newest stream per server wins in process: a stream authenticated with a
//! newer refresh-key generation, or a reconnect on the same one, ends the
//! stream it replaces. Across replicas the database decides: before a request
//! or an attach — the two messages that make a worker act — is handed to a
//! stream, the server row's `refresh_key_generation` is read again, and a
//! stream that registration has moved past is ended instead of being trusted.
//! Renewals and detaches only steer a watch the worker already holds, so they
//! skip the read.

use crate::entities::db::server::{FindServerById, ServerId};
use crate::events::shell::{ShellUpMessage, replica_of, shell_up_channel};
use crate::hooks::shell::ShellRouter;
use crate::services::OrchestrationError;
use crate::services::notify::publish_with_retry;
use base::db::Db;
use kanau::message::MessageSer;
use kanau::processor::Processor;
use prost::Message;
use rpguru_sdk::orchestration_agent as pb;
use std::collections::HashMap;
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

/// The bound on publishing one worker message upward. The channel's read loop
/// awaits it, so a Redis that stopped answering must cost a lost event (which
/// the watch notices and re-attaches over) rather than a stalled stream.
const UP_PUBLISH_TIMEOUT: Duration = Duration::from_secs(2);

/// Everything the `ShellChannel` handler and the down subscriber share.
#[derive(Clone)]
pub struct ShellChannels {
    pub db: Db,
    pub hub: ShellChannelHub,
    pub up: ShellUpPublisher,
}

/// Why a held stream was ended by somebody else.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ShellChannelEnd {
    /// The server registered again: the stream's refresh key is dead.
    Reregistered,
    /// The same registration opened a newer stream (a reconnect).
    Replaced,
}

struct Holder {
    id: u64,
    generation: i64,
    tx: mpsc::Sender<pb::ShellDown>,
    ended: CancellationToken,
    reason: Arc<OnceLock<ShellChannelEnd>>,
}

impl Holder {
    fn end(&self, reason: ShellChannelEnd) {
        let _ = self.reason.set(reason);
        self.ended.cancel();
    }
}

#[derive(Default)]
struct HubState {
    next_id: u64,
    holders: HashMap<String, Holder>,
}

/// The live `ShellChannel` of every server whose worker is connected to this
/// replica.
#[derive(Clone, Default)]
pub struct ShellChannelHub {
    state: Arc<Mutex<HubState>>,
}

/// A held slot; dropping it frees the slot unless a newer stream took it.
pub struct ShellChannelLease {
    server: String,
    id: u64,
    ended: CancellationToken,
    reason: Arc<OnceLock<ShellChannelEnd>>,
    state: Arc<Mutex<HubState>>,
}

impl ShellChannelLease {
    /// Completes when another stream or the fencing re-check ended this one.
    pub async fn ended(&self) -> ShellChannelEnd {
        self.ended.cancelled().await;
        self.reason
            .get()
            .copied()
            .unwrap_or(ShellChannelEnd::Reregistered)
    }
}

impl Drop for ShellChannelLease {
    fn drop(&mut self) {
        let mut state = match self.state.lock() {
            Ok(state) => state,
            Err(poisoned) => poisoned.into_inner(),
        };
        if state
            .holders
            .get(&self.server)
            .is_some_and(|holder| holder.id == self.id)
        {
            state.holders.remove(&self.server);
        }
    }
}

/// What a forward needs from the slot, copied out so the lock is not held
/// across the database read.
struct HeldStream {
    id: u64,
    generation: i64,
    tx: mpsc::Sender<pb::ShellDown>,
}

impl ShellChannelHub {
    /// Takes `server`'s slot for a stream authenticated at `generation`, ending
    /// the stream that held it. `None` when the slot is held at a newer
    /// generation: a stream whose key a registration already replaced never
    /// takes over from its successor.
    pub fn hold(
        &self,
        server: &ServerId,
        generation: i64,
        tx: mpsc::Sender<pb::ShellDown>,
    ) -> Option<ShellChannelLease> {
        let mut state = match self.state.lock() {
            Ok(state) => state,
            Err(poisoned) => poisoned.into_inner(),
        };
        let key = server.to_string();
        if let Some(current) = state.holders.get(&key) {
            if current.generation > generation {
                return None;
            }
            current.end(if current.generation < generation {
                ShellChannelEnd::Reregistered
            } else {
                ShellChannelEnd::Replaced
            });
        }
        state.next_id = state.next_id.wrapping_add(1);
        let id = state.next_id;
        let ended = CancellationToken::new();
        let reason = Arc::new(OnceLock::new());
        state.holders.insert(
            key.clone(),
            Holder {
                id,
                generation,
                tx,
                ended: ended.clone(),
                reason: reason.clone(),
            },
        );
        drop(state);
        Some(ShellChannelLease {
            server: key,
            id,
            ended,
            reason,
            state: self.state.clone(),
        })
    }

    fn held(&self, server: &str) -> Option<HeldStream> {
        let state = match self.state.lock() {
            Ok(state) => state,
            Err(poisoned) => poisoned.into_inner(),
        };
        state.holders.get(server).map(|holder| HeldStream {
            id: holder.id,
            generation: holder.generation,
            tx: holder.tx.clone(),
        })
    }

    /// Ends the stream `id` if it still holds `server`'s slot.
    fn end(&self, server: &str, id: u64, reason: ShellChannelEnd) {
        let mut state = match self.state.lock() {
            Ok(state) => state,
            Err(poisoned) => poisoned.into_inner(),
        };
        if state
            .holders
            .get(server)
            .is_some_and(|holder| holder.id == id)
            && let Some(holder) = state.holders.remove(server)
        {
            holder.end(reason);
        }
    }
}

/// One message from a dashboard replica for `server`'s worker.
pub struct ForwardShellDown {
    pub server: ServerId,
    pub down: pb::ShellDown,
}

impl Processor<ForwardShellDown> for ShellChannels {
    /// Whether this replica handed the message to a stream. `false` is the
    /// normal answer on every replica but the one holding the server.
    type Output = bool;
    type Error = OrchestrationError;
    #[tracing::instrument(name = "Service:ForwardShellDown", skip_all, err)]
    async fn process(&self, input: ForwardShellDown) -> Result<Self::Output, Self::Error> {
        let key = input.server.to_string();
        let Some(held) = self.hub.held(&key) else {
            return Ok(false);
        };
        let acts = matches!(
            input.down.message,
            Some(pb::shell_down::Message::Request(_) | pb::shell_down::Message::Attach(_))
        );
        if acts {
            let current = self
                .db
                .process(FindServerById {
                    id: input.server.clone(),
                })
                .await?
                .map(|server| server.refresh_key_generation);
            if current != Some(held.generation) {
                tracing::info!(
                    server = %key,
                    held = held.generation,
                    current = ?current,
                    "ending a shell channel registration has moved past"
                );
                self.hub.end(&key, held.id, ShellChannelEnd::Reregistered);
                return Ok(false);
            }
        }
        // Never waits: a stream whose worker stopped reading must not stall the
        // subscriber every other server's messages arrive through. What is
        // dropped is recovered upstream — a call answers UNAVAILABLE, a watch
        // re-attaches.
        match held.tx.try_send(input.down) {
            Ok(()) => Ok(true),
            Err(mpsc::error::TrySendError::Full(_)) => {
                tracing::warn!(server = %key, "shell channel full; dropping a down message");
                Ok(false)
            }
            Err(mpsc::error::TrySendError::Closed(_)) => Ok(false),
        }
    }
}

/// Where the worker's replies and watch events go.
#[derive(Clone)]
pub enum ShellUpPublisher {
    /// Production: `PUBLISH` on the up channel of the replica the id names.
    Redis(redis::aio::ConnectionManager),
    /// Tests: hand the message to this process's dashboard router, skipping the
    /// round trip. Ids another replica made are dropped, as Redis would deliver
    /// them to nobody here.
    InProcess(ShellRouter),
}

impl ShellUpPublisher {
    /// Sends one worker message to the dashboard replica waiting for it. A
    /// keep-alive, or an id no dashboard replica made, goes nowhere. Failure is
    /// logged: the call it answered reports UNAVAILABLE, the watch it belonged
    /// to sees the gap.
    pub async fn publish(&self, server: &ServerId, up: pb::ShellUp) {
        let id = match &up.message {
            Some(pb::shell_up::Message::Reply(reply)) => reply.request_id.as_str(),
            Some(pb::shell_up::Message::Event(event)) => event.watch_id.as_str(),
            Some(pb::shell_up::Message::KeepAlive(_)) | None => return,
        };
        let Some(replica) = replica_of(id) else {
            tracing::warn!(server = %server, id, "a shell message for an id no replica made");
            return;
        };
        match self {
            Self::InProcess(router) => {
                if router.replica() == replica {
                    router.route(server.as_ref(), up);
                }
            }
            Self::Redis(manager) => {
                let channel = shell_up_channel(replica);
                let message = ShellUpMessage {
                    server_id: server.to_string(),
                    up: up.encode_to_vec(),
                };
                let bytes = match message.to_bytes() {
                    Ok(bytes) => bytes,
                    Err(error) => {
                        tracing::warn!(?error, "encoding a shell up message failed");
                        return;
                    }
                };
                let published = tokio::time::timeout(
                    UP_PUBLISH_TIMEOUT,
                    publish_with_retry(manager, &channel, &bytes),
                )
                .await;
                match published {
                    Ok(Ok(())) => {}
                    Ok(Err(error)) => {
                        tracing::warn!(%error, %channel, "publishing a shell up message failed");
                    }
                    Err(_) => {
                        tracing::warn!(%channel, "publishing a shell up message timed out");
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

    use super::*;

    fn server() -> ServerId {
        ServerId::from_key("shellserverxxxxxxxxx")
    }

    #[tokio::test]
    async fn a_newer_stream_ends_the_one_it_replaces() {
        let hub = ShellChannelHub::default();
        let (tx, _rx) = mpsc::channel(1);
        let first = hub.hold(&server(), 1, tx.clone()).unwrap();
        let second = hub.hold(&server(), 1, tx.clone()).unwrap();
        assert_eq!(first.ended().await, ShellChannelEnd::Replaced);
        let third = hub.hold(&server(), 2, tx.clone()).unwrap();
        assert_eq!(second.ended().await, ShellChannelEnd::Reregistered);
        // An older key cannot take the slot back.
        assert!(hub.hold(&server(), 1, tx).is_none());
        // Dropping a replaced lease leaves the current holder in place.
        drop(first);
        drop(second);
        assert_eq!(hub.held(server().as_ref()).map(|h| h.generation), Some(2));
        drop(third);
        assert!(hub.held(server().as_ref()).is_none());
    }
}
