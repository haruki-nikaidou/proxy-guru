//! The in-process live bus and the Redis subscriber that feeds it.
//!
//! One subscriber per replica, one broadcast channel behind it. Everything that
//! watches — the shared canvas and rollout views, the two health record streams —
//! subscribes to the bus, not to Redis, so the number of Redis connections is a
//! property of the fleet size and not of how many dashboards are open.
//!
//! # Why a `Resync` exists
//!
//! A subscriber that reconnects has a gap: whatever was published while it was
//! away is gone for good, because pub/sub does not queue. Rather than guess, the
//! subscriber broadcasts [`LiveEvent::Resync`] after every successful
//! (re)connect — including the first — and every consumer answers it by reading
//! the database again. The same event is what a lagging consumer sees, so there
//! is exactly one recovery path and clients never have to re-request anything.

use crate::events::live::{LIVE_CHANNEL, LiveMessage};
use kanau::message::MessageDe;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::broadcast;
use tokio_stream::StreamExt;
use tokio_util::sync::CancellationToken;

/// How many events a slow consumer may fall behind before it is told to resync.
///
/// Generous, because falling behind costs a full reload per lagging view; the
/// events themselves are small and a burst is bounded by the fleet's edit rate.
const BUS_CAPACITY: usize = 1024;

/// How often the subscriber pings its Redis connection. A pub/sub connection is
/// otherwise silent, so a half-open socket would look exactly like an idle
/// fleet.
const PING_INTERVAL: Duration = Duration::from_secs(15);

/// The first reconnect delay, doubled per failure up to [`MAX_BACKOFF`].
const MIN_BACKOFF: Duration = Duration::from_millis(500);
const MAX_BACKOFF: Duration = Duration::from_secs(10);

#[derive(Debug, Clone)]
pub enum LiveEvent {
    /// One published change. `Arc` because every watcher on this replica sees
    /// the same message and only reads it.
    Message(Arc<LiveMessage>),
    /// Emitted after every (re)connect: state may have been missed, so reload.
    Resync,
}

/// Fans live events out to every watcher in this process.
#[derive(Clone)]
pub struct LiveBus {
    tx: broadcast::Sender<LiveEvent>,
}

impl LiveBus {
    pub fn new() -> Self {
        Self {
            tx: broadcast::Sender::new(BUS_CAPACITY),
        }
    }

    /// Hands a message to every watcher. No watchers is the normal case for a
    /// replica nobody has a dashboard on, so a send failure is not an error.
    pub fn publish(&self, message: LiveMessage) {
        let _ = self.tx.send(LiveEvent::Message(Arc::new(message)));
    }

    /// Tells every watcher to reload: the bus may have missed events.
    pub fn resync(&self) {
        let _ = self.tx.send(LiveEvent::Resync);
    }

    pub fn subscribe(&self) -> broadcast::Receiver<LiveEvent> {
        self.tx.subscribe()
    }
}

impl Default for LiveBus {
    fn default() -> Self {
        Self::new()
    }
}

/// Feeds `bus` from Redis until `shutdown`.
///
/// Never returns early on error: a master whose live bus died would keep serving
/// dashboards that silently stop updating. Every failure path loops back into a
/// reconnect, and every successful connect broadcasts a resync.
pub async fn run_redis_subscriber(client: redis::Client, bus: LiveBus, shutdown: CancellationToken) {
    let mut backoff = MIN_BACKOFF;
    loop {
        if shutdown.is_cancelled() {
            return;
        }
        let mut pubsub = match client.get_async_pubsub().await {
            Ok(pubsub) => pubsub,
            Err(error) => {
                tracing::warn!(%error, ?backoff, "connecting to the live bus failed");
                if wait_or_shutdown(backoff, &shutdown).await {
                    return;
                }
                backoff = backoff.saturating_mul(2).min(MAX_BACKOFF);
                continue;
            }
        };
        if let Err(error) = pubsub.subscribe(LIVE_CHANNEL).await {
            tracing::warn!(%error, "subscribing to the live channel failed");
            if wait_or_shutdown(backoff, &shutdown).await {
                return;
            }
            backoff = backoff.saturating_mul(2).min(MAX_BACKOFF);
            continue;
        }
        tracing::info!(channel = LIVE_CHANNEL, "live bus connected");
        backoff = MIN_BACKOFF;
        // Before the first message, not after: a watcher that subscribed while
        // the bus was down has to reload, and so does one that just started.
        bus.resync();

        let (mut sink, mut stream) = pubsub.split();
        let mut ping = tokio::time::interval(PING_INTERVAL);
        ping.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        ping.tick().await; // the connect just proved the socket works
        loop {
            tokio::select! {
                () = shutdown.cancelled() => return,
                _ = ping.tick() => {
                    // `redis::Value`, not `String`: a connection in subscribe
                    // mode answers PING with `["pong", ""]`, so asking for a
                    // string makes every keep-alive look like a dead socket and
                    // reconnects — plus resyncs every watcher — on a cadence.
                    if let Err(error) = sink.ping::<redis::Value>().await {
                        tracing::warn!(%error, "the live bus connection stopped answering");
                        break;
                    }
                }
                message = stream.next() => {
                    let Some(message) = message else {
                        tracing::warn!("the live bus connection closed");
                        break;
                    };
                    match LiveMessage::from_bytes(message.get_payload_bytes()) {
                        // A payload this process cannot read is a replica running a
                        // different build, not a reason to drop the connection.
                        Err(error) => tracing::warn!(%error, "undecodable live event"),
                        Ok(message) => bus.publish(message),
                    }
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
