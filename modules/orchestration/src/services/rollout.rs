//! Rollout status, config reads, and the dirty-canvas notifier.

use crate::entities::surreal::canvas::{CanvasId, FindRootCanvas};
use crate::entities::surreal::server::{FindServerById, ServerId};
use crate::entities::surreal::topology::FindCanvasOfServer;
use crate::entities::surreal::view::{
    ConfigSnapshot, FindServerConfigView, ForgetServerAppliedRow, InvalidPod,
};
use crate::events::CanvasDirty;
use crate::services::OrchestrationError;
use crate::utils::ids::record_key;
use auth::entities::surreal::account::AccountRole;
use auth::services::identity::Identity;
use auth::utils::rbac::Permission;
use chrono::{DateTime, Utc};
use kanau::processor::Processor;
use wakuwaku::amqp::{AmqpMessageSend, AmqpPool};
use wakuwaku::surreal::SurrealProcessor;

/// Tells the derivation hook that a canvas has pending edits.
///
/// Publishing is mandatory in every serving mode: since periodic work became
/// AMQP-driven, the `derive_stale_canvases` sweep is itself a message from the
/// broker, so a master with no broker derives nothing at all. `amqp: None` —
/// and with it `Default` — is for tests that drive `CanvasDeriver` directly
/// instead of through a consumer, not for a broker-less deployment.
///
/// Correctness still lives in the canvas generation counters — the write that
/// precedes the publish has already bumped the generation, and the hook
/// re-derives anything whose generation ran ahead of its derivation. That is
/// why a publish failure is logged and swallowed rather than failing the
/// operator's edit: what a lost message costs is latency, provided delivery
/// resumes.
#[derive(Clone, Default)]
pub struct DirtyNotifier {
    pub amqp: Option<AmqpPool>,
}

impl DirtyNotifier {
    pub async fn notify(&self, canvas: &CanvasId) {
        let Some(pool) = &self.amqp else {
            return;
        };
        let event = CanvasDirty {
            canvas: record_key(&canvas.0),
        };
        if let Err(e) = event.send(pool).await {
            tracing::warn!(error = %e, "publishing canvas_dirty failed; the sweep catches it up once the broker is back");
        }
    }
}

#[derive(Clone)]
pub struct RolloutService {
    pub db: SurrealProcessor,
    pub notifier: DirtyNotifier,
}

pub struct GetServerConfig {
    pub actor: Identity,
    pub server: ServerId,
}

impl Processor<GetServerConfig> for RolloutService {
    /// The desired revision and its TOML; `(0, "")` before the first derivation.
    type Output = (i64, String);
    type Error = OrchestrationError;
    #[tracing::instrument(name = "Service:GetServerConfig", skip_all, err)]
    async fn process(&self, input: GetServerConfig) -> Result<Self::Output, Self::Error> {
        input.actor.ensure(Permission::ViewWorkspace)?;
        let view = self
            .db
            .process(FindServerConfigView {
                server: input.server,
            })
            .await?
            .ok_or(OrchestrationError::NotFound)?;
        Ok(match view.desired {
            Some(snapshot) => (snapshot.revision, snapshot.toml),
            None => (0, String::new()),
        })
    }
}

#[derive(Debug, Clone)]
pub struct RolloutStatus {
    pub desired: Option<ConfigSnapshot>,
    pub in_flight: Option<ConfigSnapshot>,
    pub applied: Option<ConfigSnapshot>,
    pub apply_error: Option<String>,
    pub derive_error: Option<String>,
    /// Pods that failed to derive on their own; the rest of this server's config
    /// was derived and published normally.
    pub invalid_pods: Vec<InvalidPod>,
    pub waiting_for: Vec<ServerId>,
    /// The canvas has edits the derivation has not caught up with yet.
    pub derivation_pending: bool,
    pub last_seen_at: Option<DateTime<Utc>>,
}

pub struct GetServerRolloutStatus {
    pub actor: Identity,
    pub server: ServerId,
}

impl Processor<GetServerRolloutStatus> for RolloutService {
    type Output = RolloutStatus;
    type Error = OrchestrationError;
    #[tracing::instrument(name = "Service:GetServerRolloutStatus", skip_all, err)]
    async fn process(&self, input: GetServerRolloutStatus) -> Result<Self::Output, Self::Error> {
        input.actor.ensure(Permission::ViewWorkspace)?;
        let server = self
            .db
            .process(FindServerById {
                id: input.server.clone(),
            })
            .await?
            .ok_or(OrchestrationError::NotFound)?;
        let view = self
            .db
            .process(FindServerConfigView {
                server: input.server,
            })
            .await?
            .ok_or(OrchestrationError::NotFound)?;
        // Only the root of the server's tree carries a meaningful generation.
        let canvas = self
            .db
            .process(FindRootCanvas {
                canvas: server.canvas.clone(),
            })
            .await?
            .ok_or(OrchestrationError::NotFound)?;
        Ok(RolloutStatus {
            desired: view.desired,
            in_flight: view.in_flight,
            applied: view.applied,
            apply_error: view.apply_error,
            derive_error: view.derive_error,
            invalid_pods: view.invalid_pods,
            waiting_for: view.waiting_for,
            derivation_pending: canvas.generation > canvas.derived_generation,
            last_seen_at: server.last_seen_at,
        })
    }
}

/// Forgets what a server was running, so its dependants stop waiting for it.
///
/// The only way out when a server is gone for good: convergence refuses to move a
/// listener that a live `applied` snapshot still points at, and a dead worker
/// never acks. Clearing its slots is an operator asserting "this one is not coming
/// back".
///
/// Admin only, for the same reason as `ForceDeleteNode` and `ForceDisconnect`: the
/// assertion is unverifiable, and if it is wrong about a server that is merely
/// unreachable, convergence will switch its dependants off listeners that are
/// still carrying traffic. This is the one operation that can break a live path
/// without the topology checker ever objecting.
pub struct ForgetServerApplied {
    pub actor: Identity,
    pub server: ServerId,
}

impl Processor<ForgetServerApplied> for RolloutService {
    type Output = ();
    type Error = OrchestrationError;
    #[tracing::instrument(name = "Service:ForgetServerApplied", skip_all, err)]
    async fn process(&self, input: ForgetServerApplied) -> Result<Self::Output, Self::Error> {
        input.actor.ensure(Permission::EditWorkspace)?;
        if input.actor.role != AccountRole::Admin {
            return Err(OrchestrationError::PermissionDenied);
        }
        let canvas = canvas_of_server(&self.db, &input.server).await?;
        self.db
            .process(ForgetServerAppliedRow {
                server: input.server,
                canvas: canvas.clone(),
            })
            .await?;
        self.notifier.notify(&canvas).await;
        Ok(())
    }
}

/// The canvas a server belongs to, or [`OrchestrationError::NotFound`].
pub(crate) async fn canvas_of_server(
    db: &SurrealProcessor,
    server: &ServerId,
) -> Result<CanvasId, OrchestrationError> {
    db.process(FindCanvasOfServer {
        server: server.clone(),
    })
    .await?
    .ok_or(OrchestrationError::NotFound)
}
