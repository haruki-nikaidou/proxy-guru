//! Edge operations. All edge legality lives in the topology checker.

use crate::config::OrchestrationConfig;
use crate::entities::surreal::connection::{
    ConnectPorts, DeleteEdgeRow, EdgeConnectionEntity, EdgeConnectionId, FindEdgeById,
};
use crate::entities::surreal::node::FindNodeById;
use crate::entities::surreal::port::{FindPortById, PortId};
use crate::entities::surreal::topology::LoadCanvasTopology;
use crate::entities::surreal::view::ListServerConfigViewsByCanvases;
use crate::services::OrchestrationError;
use crate::services::converge::ensure_switch_safe;
use crate::services::rollout::DirtyNotifier;
use crate::services::topology::{TopologyEdit, ensure_valid};
use crate::utils::ids;
use crate::utils::ids::record_key;
use auth::entities::surreal::account::AccountRole;
use auth::services::identity::Identity;
use auth::utils::rbac::Permission;
use kanau::processor::Processor;
use wakuwaku::surreal::SurrealProcessor;

#[derive(Clone)]
pub struct EdgeService {
    pub db: SurrealProcessor,
    pub notifier: DirtyNotifier,
    pub config: OrchestrationConfig,
}

/// The canvas an edge endpoint belongs to.
async fn canvas_of_port(
    db: &SurrealProcessor,
    port: &PortId,
) -> Result<crate::entities::surreal::canvas::CanvasId, OrchestrationError> {
    let row = db
        .process(FindPortById { id: port.clone() })
        .await?
        .ok_or(OrchestrationError::NotFound)?;
    let node = db
        .process(FindNodeById { id: row.owner })
        .await?
        .ok_or(OrchestrationError::NotFound)?;
    Ok(node.canvas)
}

pub struct Connect {
    pub actor: Identity,
    pub output_port: PortId,
    pub input_port: PortId,
}

impl Processor<Connect> for EdgeService {
    type Output = EdgeConnectionEntity;
    type Error = OrchestrationError;
    #[tracing::instrument(name = "Service:Connect", skip_all, err)]
    async fn process(&self, input: Connect) -> Result<Self::Output, Self::Error> {
        input.actor.ensure(Permission::EditWorkspace)?;
        let canvas = canvas_of_port(&self.db, &input.output_port).await?;
        let topology = self
            .db
            .process(LoadCanvasTopology {
                canvas: canvas.clone(),
            })
            .await?;
        let projected = topology.project(&[TopologyEdit::AddEdge {
            edge: EdgeConnectionEntity {
                id: ids::edge_id("pending-0"),
                source: input.output_port.clone(),
                target: input.input_port.clone(),
            },
        }]);
        ensure_valid(&projected)?;
        let views = self
            .db
            .process(ListServerConfigViewsByCanvases {
                canvases: projected.canvas_ids(),
            })
            .await?;
        ensure_switch_safe(&projected, &views, &self.config)?;

        let edge = self
            .db
            .process(ConnectPorts {
                source: input.output_port,
                target: input.input_port,
                canvas: canvas.clone(),
            })
            .await?;
        self.notifier.notify(&topology.root).await;
        Ok(edge)
    }
}

pub struct Disconnect {
    pub actor: Identity,
    pub edge: EdgeConnectionId,
}

impl Processor<Disconnect> for EdgeService {
    type Output = ();
    type Error = OrchestrationError;
    #[tracing::instrument(name = "Service:Disconnect", skip_all, err)]
    async fn process(&self, input: Disconnect) -> Result<Self::Output, Self::Error> {
        input.actor.ensure(Permission::EditWorkspace)?;
        let edge = self
            .db
            .process(FindEdgeById {
                id: input.edge.clone(),
            })
            .await?
            .ok_or(OrchestrationError::NotFound)?;
        let canvas = canvas_of_port(&self.db, &edge.source).await?;
        let topology = self
            .db
            .process(LoadCanvasTopology {
                canvas: canvas.clone(),
            })
            .await?;
        let projected = topology.project(&[TopologyEdit::RetireEdge {
            edge: input.edge.clone(),
        }]);
        ensure_valid(&projected)?;
        let views = self
            .db
            .process(ListServerConfigViewsByCanvases {
                canvases: projected.canvas_ids(),
            })
            .await?;
        ensure_switch_safe(&projected, &views, &self.config)?;

        self.db
            .process(DeleteEdgeRow {
                id: input.edge,
                canvas: canvas.clone(),
            })
            .await?;
        self.notifier.notify(&topology.root).await;
        Ok(())
    }
}

/// Deletes an edge without validating the canvas it leaves behind. Admin only.
pub struct ForceDisconnect {
    pub actor: Identity,
    pub edge: EdgeConnectionId,
}

impl Processor<ForceDisconnect> for EdgeService {
    type Output = ();
    type Error = OrchestrationError;
    #[tracing::instrument(name = "Service:ForceDisconnect", skip_all, err)]
    async fn process(&self, input: ForceDisconnect) -> Result<Self::Output, Self::Error> {
        input.actor.ensure(Permission::EditWorkspace)?;
        if input.actor.role != AccountRole::Admin {
            return Err(OrchestrationError::PermissionDenied);
        }
        let edge = self
            .db
            .process(FindEdgeById {
                id: input.edge.clone(),
            })
            .await?
            .ok_or(OrchestrationError::NotFound)?;
        let canvas = canvas_of_port(&self.db, &edge.source).await?;
        tracing::info!(edge = %record_key(&edge.id.0), "force-deleting edge");
        self.db
            .process(DeleteEdgeRow {
                id: input.edge,
                canvas: canvas.clone(),
            })
            .await?;
        self.notifier.notify(&canvas).await;
        Ok(())
    }
}
