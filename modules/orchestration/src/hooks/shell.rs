//! The remote-shell relay's Redis subscribers, and the dashboard replica's
//! router that hands what arrives to the call or watch waiting for it.
//!
//! - `dashboard_grpc`: [`run_shell_up_subscriber`] listens on this replica's own
//!   up channel and feeds [`ShellRouter`], where every pending call and open
//!   watch registered its id.
//! - `workers_grpc`: [`run_shell_down_subscriber`] listens on the shared down
//!   channel and hands each message to [`ForwardShellDown`], which delivers it
//!   if this replica holds the server's stream.
//!
//! Pub/sub does not queue, so a subscriber that reconnects has a gap. A call
//! caught in it times out (`UNAVAILABLE`); watches are told to re-attach from
//! their position ([`WatchInput::Resync`]), the same recovery a detected gap
//! gets.

use crate::events::shell::{
    SHELL_DOWN_CHANNEL, ShellDownMessage, ShellUpMessage, shell_up_channel,
};
use crate::services::shell_channel::{ForwardShellDown, ShellChannels};
use crate::utils::ids;
use kanau::message::MessageDe;
use kanau::processor::Processor;
use prost::Message;
use rpguru_sdk::orchestration_agent as pb;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::{mpsc, oneshot};
use tokio_stream::StreamExt;
use tokio_util::sync::CancellationToken;

/// How many inputs one watch may have waiting. Overflow is dropped: the watch
/// sees the gap in the offsets and re-attaches.
const WATCH_INBOX: usize = 256;

/// How often a subscriber pings its connection; a pub/sub socket is otherwise
/// silent, so a half-open one would look like an idle fleet.
const PING_INTERVAL: Duration = Duration::from_secs(15);

/// The first reconnect delay, doubled per failure up to [`MAX_BACKOFF`].
const MIN_BACKOFF: Duration = Duration::from_millis(500);
const MAX_BACKOFF: Duration = Duration::from_secs(10);

/// What a watch's relay is told.
#[derive(Debug)]
pub enum WatchInput {
    /// One event the worker sent for the watch.
    Event(pb::ShellEvent),
    /// The up channel may have lost messages: attach again from the position.
    Resync,
}

struct PendingReply {
    server: String,
    tx: oneshot::Sender<pb::ShellReply>,
}

struct WatchRoute {
    server: String,
    tx: mpsc::Sender<WatchInput>,
}

#[derive(Default)]
struct Routes {
    pending: HashMap<String, PendingReply>,
    watches: HashMap<String, WatchRoute>,
}

/// Routes worker replies and watch events to whoever on this replica waits for
/// them, by the id they echo.
///
/// Every id is `<replica>:<random>` ([`ShellRouter::replica`]), which is how the
/// worker-facing replica knows where to publish the answer. An answer is only
/// accepted from the server the id was issued for.
#[derive(Clone)]
pub struct ShellRouter {
    replica: Arc<str>,
    routes: Arc<Mutex<Routes>>,
}

impl Default for ShellRouter {
    fn default() -> Self {
        Self::new()
    }
}

impl ShellRouter {
    /// A router under a fresh random replica id.
    pub fn new() -> Self {
        Self {
            replica: Arc::from(format!("{:016x}", rand::random::<u64>())),
            routes: Arc::default(),
        }
    }

    /// This replica's id: the suffix of its up channel and the prefix of every
    /// id it issues.
    pub fn replica(&self) -> &str {
        &self.replica
    }

    fn new_id(&self) -> String {
        format!("{}:{:016x}", self.replica, rand::random::<u64>())
    }

    fn routes(&self) -> std::sync::MutexGuard<'_, Routes> {
        match self.routes.lock() {
            Ok(routes) => routes,
            Err(poisoned) => poisoned.into_inner(),
        }
    }

    /// Registers a request id for `server`; the reply to it arrives on the
    /// returned handle, which deregisters on drop.
    pub fn expect_reply(&self, server: &str) -> ReplyWait {
        let id = self.new_id();
        let (tx, rx) = oneshot::channel();
        self.routes().pending.insert(
            id.clone(),
            PendingReply {
                server: server.to_string(),
                tx,
            },
        );
        ReplyWait {
            id,
            rx,
            routes: self.routes.clone(),
        }
    }

    /// Registers a watch id for `server`; its events arrive on the returned
    /// inbox, which deregisters on drop.
    pub fn watch(&self, server: &str) -> WatchInbox {
        let id = self.new_id();
        let (tx, rx) = mpsc::channel(WATCH_INBOX);
        self.routes().watches.insert(
            id.clone(),
            WatchRoute {
                server: server.to_string(),
                tx,
            },
        );
        WatchInbox {
            id,
            rx,
            routes: self.routes.clone(),
        }
    }

    /// Hands one message `server`'s worker sent to the call or watch it answers.
    pub fn route(&self, server: &str, up: pb::ShellUp) {
        match up.message {
            Some(pb::shell_up::Message::Reply(reply)) => {
                let pending = {
                    let mut routes = self.routes();
                    match routes.pending.get(&reply.request_id) {
                        Some(pending) if pending.server == server => {
                            routes.pending.remove(&reply.request_id)
                        }
                        Some(_) => {
                            tracing::warn!(server, "a shell reply from the wrong server; dropped");
                            None
                        }
                        // Answered after the call gave up.
                        None => None,
                    }
                };
                if let Some(pending) = pending {
                    let _ = pending.tx.send(reply);
                }
            }
            Some(pb::shell_up::Message::Event(event)) => {
                let Some(body) = event.event else {
                    return;
                };
                let tx = {
                    let routes = self.routes();
                    match routes.watches.get(&event.watch_id) {
                        Some(watch) if watch.server == server => Some(watch.tx.clone()),
                        Some(_) => {
                            tracing::warn!(server, "a shell event from the wrong server; dropped");
                            None
                        }
                        // A watch that ended; the worker drops it once unrenewed.
                        None => None,
                    }
                };
                if let Some(tx) = tx
                    && tx.try_send(WatchInput::Event(body)).is_err()
                {
                    tracing::debug!("a shell watch fell behind; it re-attaches over the gap");
                }
            }
            Some(pb::shell_up::Message::KeepAlive(_)) | None => {}
        }
    }

    /// Tells every watch on this replica to re-attach.
    pub fn resync(&self) {
        let senders: Vec<_> = self
            .routes()
            .watches
            .values()
            .map(|watch| watch.tx.clone())
            .collect();
        for tx in senders {
            let _ = tx.try_send(WatchInput::Resync);
        }
    }
}

/// A call waiting for its reply.
pub struct ReplyWait {
    pub id: String,
    rx: oneshot::Receiver<pb::ShellReply>,
    routes: Arc<Mutex<Routes>>,
}

impl ReplyWait {
    /// The reply, or `None` when none arrived within `timeout`.
    pub async fn recv(&mut self, timeout: Duration) -> Option<pb::ShellReply> {
        tokio::time::timeout(timeout, &mut self.rx).await.ok()?.ok()
    }
}

impl Drop for ReplyWait {
    fn drop(&mut self) {
        let mut routes = match self.routes.lock() {
            Ok(routes) => routes,
            Err(poisoned) => poisoned.into_inner(),
        };
        routes.pending.remove(&self.id);
    }
}

/// A watch's incoming events.
pub struct WatchInbox {
    pub id: String,
    pub rx: mpsc::Receiver<WatchInput>,
    routes: Arc<Mutex<Routes>>,
}

impl Drop for WatchInbox {
    fn drop(&mut self) {
        let mut routes = match self.routes.lock() {
            Ok(routes) => routes,
            Err(poisoned) => poisoned.into_inner(),
        };
        routes.watches.remove(&self.id);
    }
}

/// `dashboard_grpc`: feeds `router` from this replica's up channel until
/// `shutdown`. Every (re)connect resyncs the watches: whatever was published
/// while the subscriber was away is gone.
pub async fn run_shell_up_subscriber(
    client: redis::Client,
    router: ShellRouter,
    shutdown: CancellationToken,
) {
    let channel = shell_up_channel(router.replica());
    let resync = router.clone();
    subscribe(
        client,
        channel,
        shutdown,
        move || resync.resync(),
        move |payload| {
            let routed = decode_up(payload);
            let router = router.clone();
            async move {
                if let Some((server, up)) = routed {
                    router.route(&server, up);
                }
            }
        },
    )
    .await;
}

/// `workers_grpc`: hands every down message to `channels` until `shutdown`.
///
/// Messages are forwarded one at a time, in order: an attach and the renewal
/// right behind it must reach the worker in that order.
pub async fn run_shell_down_subscriber(
    client: redis::Client,
    channels: ShellChannels,
    shutdown: CancellationToken,
) {
    subscribe(
        client,
        SHELL_DOWN_CHANNEL.to_string(),
        shutdown,
        || {},
        move |payload| {
            let forward = decode_down(payload);
            let channels = channels.clone();
            async move {
                if let Some(forward) = forward
                    && let Err(error) = channels.process(forward).await
                {
                    tracing::warn!(%error, "forwarding a shell down message failed");
                }
            }
        },
    )
    .await;
}

fn decode_up(payload: &[u8]) -> Option<(String, pb::ShellUp)> {
    let message = ShellUpMessage::from_bytes(payload)
        .inspect_err(|error| tracing::warn!(%error, "undecodable shell up envelope"))
        .ok()?;
    let up = pb::ShellUp::decode(message.up.as_slice())
        .inspect_err(|error| tracing::warn!(%error, "undecodable shell up message"))
        .ok()?;
    Some((message.server_id, up))
}

fn decode_down(payload: &[u8]) -> Option<ForwardShellDown> {
    let message = ShellDownMessage::from_bytes(payload)
        .inspect_err(|error| tracing::warn!(%error, "undecodable shell down envelope"))
        .ok()?;
    let down = pb::ShellDown::decode(message.down.as_slice())
        .inspect_err(|error| tracing::warn!(%error, "undecodable shell down message"))
        .ok()?;
    Some(ForwardShellDown {
        server: ids::server_id(&message.server_id),
        down,
    })
}

/// One pub/sub subscription kept alive until `shutdown`: reconnects with
/// backoff on every failure, pings the idle socket, calls `connected` after
/// each successful subscribe and `handle` per message, in order.
///
/// Never returns early on error: a replica whose relay died would answer every
/// shell call `UNAVAILABLE` while looking healthy.
async fn subscribe<C, H, F>(
    client: redis::Client,
    channel: String,
    shutdown: CancellationToken,
    mut connected: C,
    mut handle: H,
) where
    C: FnMut(),
    H: FnMut(&[u8]) -> F,
    F: Future<Output = ()>,
{
    let mut backoff = MIN_BACKOFF;
    loop {
        if shutdown.is_cancelled() {
            return;
        }
        let mut pubsub = match client.get_async_pubsub().await {
            Ok(pubsub) => pubsub,
            Err(error) => {
                tracing::warn!(%error, ?backoff, %channel, "connecting to the shell relay failed");
                if wait_or_shutdown(backoff, &shutdown).await {
                    return;
                }
                backoff = backoff.saturating_mul(2).min(MAX_BACKOFF);
                continue;
            }
        };
        if let Err(error) = pubsub.subscribe(&channel).await {
            tracing::warn!(%error, %channel, "subscribing to the shell relay failed");
            if wait_or_shutdown(backoff, &shutdown).await {
                return;
            }
            backoff = backoff.saturating_mul(2).min(MAX_BACKOFF);
            continue;
        }
        tracing::info!(%channel, "shell relay connected");
        backoff = MIN_BACKOFF;
        connected();

        let (mut sink, mut stream) = pubsub.split();
        let mut ping = tokio::time::interval(PING_INTERVAL);
        ping.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        ping.tick().await; // the subscribe just proved the socket works
        loop {
            tokio::select! {
                () = shutdown.cancelled() => return,
                _ = ping.tick() => {
                    // `redis::Value`: a subscribed connection answers PING with
                    // `["pong", ""]`, which is not a string.
                    if let Err(error) = sink.ping::<redis::Value>().await {
                        tracing::warn!(%error, %channel, "the shell relay connection stopped answering");
                        break;
                    }
                }
                message = stream.next() => {
                    let Some(message) = message else {
                        tracing::warn!(%channel, "the shell relay connection closed");
                        break;
                    };
                    handle(message.get_payload_bytes()).await;
                }
            }
        }
    }
}

/// Sleeps, or returns `true` when the shutdown fired first.
async fn wait_or_shutdown(delay: Duration, shutdown: &CancellationToken) -> bool {
    tokio::select! {
        () = shutdown.cancelled() => true,
        () = tokio::time::sleep(delay) => false,
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

    use super::*;

    fn reply(id: &str) -> pb::ShellUp {
        pb::ShellUp {
            message: Some(pb::shell_up::Message::Reply(pb::ShellReply {
                request_id: id.to_string(),
                result: Some(pb::shell_reply::Result::Done(pb::ShellDone {})),
            })),
        }
    }

    /// A reply is accepted from the server the id was issued for, and from no
    /// other: ids travel through a worker, which must not answer for another.
    #[tokio::test]
    async fn a_reply_only_reaches_its_own_servers_call() {
        let router = ShellRouter::new();
        let mut wait = router.expect_reply("tokyo");
        assert!(wait.id.starts_with(&format!("{}:", router.replica())));
        router.route("osaka", reply(&wait.id));
        assert!(wait.recv(Duration::from_millis(50)).await.is_none());

        let mut wait = router.expect_reply("tokyo");
        router.route("tokyo", reply(&wait.id));
        assert!(wait.recv(Duration::from_millis(50)).await.is_some());
    }

    #[tokio::test]
    async fn a_dropped_registration_stops_routing() {
        let router = ShellRouter::new();
        let wait = router.expect_reply("tokyo");
        let inbox = router.watch("tokyo");
        drop(wait);
        drop(inbox);
        assert!(router.routes().pending.is_empty());
        assert!(router.routes().watches.is_empty());
    }
}
