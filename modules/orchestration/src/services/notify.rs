//! The two publishers every mutation uses: the dirty-canvas hint (AMQP) and the
//! live dashboard events (Redis pub/sub).
//!
//! They are deliberately one struct. Every mutation needs both, they fail the
//! same way (log and continue — an operator's edit is already committed), and
//! bundling them keeps a service's dependency list from growing a second
//! messaging field.

use crate::entities::db::canvas::CanvasId;
use crate::events::CanvasDirty;
use crate::events::live::{CanvasChangeKind, LIVE_CHANNEL, LiveMessage, RolloutScope};
use crate::hooks::live::LiveBus;
use kanau::message::MessageSer;
use wakuwaku::amqp::{AmqpMessageSend, AmqpPool};

/// Tells the derivation hook that canvases have pending edits, and the fleet's
/// dashboards that something they render changed.
///
/// Publishing the dirty hint is mandatory in every serving mode: since periodic
/// work became AMQP-driven, the `derive_stale_canvases` sweep is itself a message
/// from the broker, so a master with no broker derives nothing at all.
/// `amqp: None` — and with it `Default` — is for tests that drive
/// `CanvasDeriver` directly instead of through a consumer, not for a broker-less
/// deployment. `live: None` is the same bargain for the live bus.
///
/// Correctness still lives in the canvas generation counters — the write that
/// precedes the publish has already bumped the generation, and the hook
/// re-derives anything whose generation ran ahead of its derivation. That is
/// why a publish failure is logged and swallowed rather than failing the
/// operator's edit: what a lost message costs is latency, provided delivery
/// resumes. A lost *live* event costs a stale dashboard until the next change or
/// the subscriber's next resync, which is the same bargain one level up.
#[derive(Clone, Default)]
pub struct Notifier {
    pub amqp: Option<AmqpPool>,
    pub live: Option<LivePublisher>,
}

/// How long the one retry of a failed live publish may wait for the connection
/// manager's new connection.
pub const LIVE_RETRY_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(1);

/// Where live events go.
#[derive(Clone)]
pub enum LivePublisher {
    /// Production: `PUBLISH` on [`LIVE_CHANNEL`]. The manager reconnects on its
    /// own, so a broker blip costs the publishes attempted during it and nothing
    /// structural — but it only learns of a lost socket from the command that
    /// fails on it, so a publish is retried once on that failure: otherwise the
    /// first edit after a broker restart would silently miss every dashboard.
    /// The retry waits at most [`LIVE_RETRY_TIMEOUT`]: a publish is awaited on
    /// the write path, and a broker that stays down must not hold every
    /// mutation for the manager's whole reconnect backoff.
    Redis(redis::aio::ConnectionManager),
    /// Tests: hand the message straight to this process's bus, skipping the
    /// round trip. The same shortcut `amqp: None` is for.
    InProcess(LiveBus),
}

impl Notifier {
    pub async fn notify(&self, canvas: &CanvasId) {
        let Some(pool) = &self.amqp else {
            return;
        };
        let event = CanvasDirty {
            canvas: canvas.to_string(),
        };
        if let Err(e) = event.send(pool).await {
            tracing::warn!(error = %e, "publishing canvas_dirty failed; the sweep catches it up once the broker is back");
        }
    }

    /// Publishes one live event. Failure is logged, never propagated.
    pub async fn live(&self, message: LiveMessage) {
        let Some(publisher) = &self.live else {
            return;
        };
        match publisher {
            LivePublisher::InProcess(bus) => bus.publish(message),
            LivePublisher::Redis(manager) => {
                let bytes = match message.to_bytes() {
                    Ok(bytes) => bytes,
                    Err(error) => {
                        tracing::warn!(?error, "encoding a live event failed");
                        return;
                    }
                };
                let publish = || async {
                    redis::cmd("PUBLISH")
                        .arg(LIVE_CHANNEL)
                        .arg(&bytes[..])
                        .query_async::<()>(&mut manager.clone())
                        .await
                };
                let result = match publish().await {
                    // A dropped connection, or a cached connect that failed on
                    // one: the manager is replacing it, and the retry goes out on
                    // the new connection if Redis answers in time. Not every I/O
                    // error: a response timeout keeps the connection, where a
                    // second PUBLISH queues behind the first and can deliver the
                    // event twice.
                    Err(error) if error.is_unrecoverable_error() => {
                        match tokio::time::timeout(LIVE_RETRY_TIMEOUT, publish()).await {
                            Ok(result) => result,
                            Err(_) => Err(error),
                        }
                    }
                    result => result,
                };
                if let Err(error) = result {
                    tracing::warn!(%error, "publishing a live event failed");
                }
            }
        }
    }

    /// The canvas the edit happened in, not the root: a view matches the key
    /// against its own tree, so a subcanvas edit still refreshes its parents.
    pub async fn canvas_changed(
        &self,
        canvas: &CanvasId,
        kind: CanvasChangeKind,
        ids: Vec<String>,
    ) {
        self.live(LiveMessage::CanvasChanged {
            canvas: canvas.to_string(),
            kind,
            ids,
        })
        .await;
    }

    pub async fn rollout_changed(&self, scope: RolloutScope) {
        self.live(LiveMessage::RolloutChanged { scope }).await;
    }
}
