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

/// Where live events go.
#[derive(Clone)]
pub enum LivePublisher {
    /// Production: `PUBLISH` on [`LIVE_CHANNEL`]. The manager reconnects on its
    /// own, so a broker blip costs the publishes attempted during it and nothing
    /// structural.
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
                let mut connection = manager.clone();
                if let Err(error) = redis::cmd("PUBLISH")
                    .arg(LIVE_CHANNEL)
                    .arg(&bytes[..])
                    .query_async::<()>(&mut connection)
                    .await
                {
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
