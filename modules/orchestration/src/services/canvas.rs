//! Canvas CRUD and validation.

use crate::entities::surreal::canvas::{
    CanvasContents, CanvasEntity, CanvasId, CanvasTree, CreateCanvas as CreateCanvasRow,
    DeleteCanvasRow, FindCanvasById, ListCanvases as ListCanvasesRow, LoadCanvasTree,
    UpdateCanvasMeta,
};
use crate::entities::surreal::node::FindImporterOf;
use crate::entities::surreal::topology::{LoadCanvasContents, LoadCanvasTopology};
use crate::events::live::CanvasChangeKind;
use crate::services::OrchestrationError;
use crate::services::notify::Notifier;
use crate::services::topology::{TopologyProblem, analyze};
use crate::utils::ids::record_key;
use auth::services::identity::Identity;
use auth::utils::rbac::Permission;
use base::db::Db;
use kanau::processor::Processor;

#[derive(Clone)]
pub struct CanvasService {
    pub db: Db,
    pub notifier: Notifier,
}

pub struct CreateCanvas {
    pub actor: Identity,
    pub name: String,
    pub description: String,
}

impl Processor<CreateCanvas> for CanvasService {
    type Output = CanvasEntity;
    type Error = OrchestrationError;
    #[tracing::instrument(name = "Service:CreateCanvas", skip_all, err)]
    async fn process(&self, input: CreateCanvas) -> Result<Self::Output, Self::Error> {
        input.actor.ensure(Permission::EditWorkspace)?;
        if input.name.trim().is_empty() {
            return Err(OrchestrationError::Invalid("name must not be empty".into()));
        }
        Ok(self
            .db
            .process(CreateCanvasRow {
                name: input.name,
                description: input.description,
            })
            .await?)
    }
}

pub struct ListCanvases {
    pub actor: Identity,
    /// `false` lists only root canvases.
    pub include_subcanvases: bool,
}

impl Processor<ListCanvases> for CanvasService {
    type Output = Vec<CanvasEntity>;
    type Error = OrchestrationError;
    #[tracing::instrument(name = "Service:ListCanvases", skip_all, err)]
    async fn process(&self, input: ListCanvases) -> Result<Self::Output, Self::Error> {
        input.actor.ensure(Permission::ViewWorkspace)?;
        Ok(self
            .db
            .process(ListCanvasesRow {
                include_subcanvases: input.include_subcanvases,
            })
            .await?)
    }
}

/// The whole tree containing a canvas, from its root.
pub struct GetCanvasTree {
    pub actor: Identity,
    pub canvas: CanvasId,
}

impl Processor<GetCanvasTree> for CanvasService {
    type Output = CanvasTree;
    type Error = OrchestrationError;
    #[tracing::instrument(name = "Service:GetCanvasTree", skip_all, err)]
    async fn process(&self, input: GetCanvasTree) -> Result<Self::Output, Self::Error> {
        input.actor.ensure(Permission::ViewWorkspace)?;
        self.db
            .process(LoadCanvasTree {
                canvas: input.canvas,
            })
            .await?
            .ok_or(OrchestrationError::NotFound)
    }
}

/// One canvas row by id, without its contents.
pub struct FindCanvas {
    pub actor: Identity,
    pub canvas: CanvasId,
}

impl Processor<FindCanvas> for CanvasService {
    type Output = Option<CanvasEntity>;
    type Error = OrchestrationError;
    #[tracing::instrument(name = "Service:FindCanvas", skip_all, err)]
    async fn process(&self, input: FindCanvas) -> Result<Self::Output, Self::Error> {
        input.actor.ensure(Permission::ViewWorkspace)?;
        Ok(self.db.process(FindCanvasById { id: input.canvas }).await?)
    }
}

pub struct GetCanvas {
    pub actor: Identity,
    pub canvas: CanvasId,
}

impl Processor<GetCanvas> for CanvasService {
    type Output = CanvasContents;
    type Error = OrchestrationError;
    #[tracing::instrument(name = "Service:GetCanvas", skip_all, err)]
    async fn process(&self, input: GetCanvas) -> Result<Self::Output, Self::Error> {
        input.actor.ensure(Permission::ViewWorkspace)?;
        self.db
            .process(LoadCanvasContents {
                canvas: input.canvas,
            })
            .await?
            .ok_or(OrchestrationError::NotFound)
    }
}

pub struct UpdateCanvas {
    pub actor: Identity,
    pub canvas: CanvasId,
    pub name: String,
    pub description: String,
}

impl Processor<UpdateCanvas> for CanvasService {
    type Output = CanvasEntity;
    type Error = OrchestrationError;
    #[tracing::instrument(name = "Service:UpdateCanvas", skip_all, err)]
    async fn process(&self, input: UpdateCanvas) -> Result<Self::Output, Self::Error> {
        input.actor.ensure(Permission::EditWorkspace)?;
        if input.name.trim().is_empty() {
            return Err(OrchestrationError::Invalid("name must not be empty".into()));
        }
        self.db
            .process(FindCanvasById {
                id: input.canvas.clone(),
            })
            .await?
            .ok_or(OrchestrationError::NotFound)?;
        // Metadata only: no revision, no re-derivation — but the dashboard
        // renders the name, so the live event is sent all the same.
        let canvas = self
            .db
            .process(UpdateCanvasMeta {
                id: input.canvas.clone(),
                name: input.name,
                description: input.description,
            })
            .await?;
        self.notifier
            .canvas_changed(
                &input.canvas,
                CanvasChangeKind::CanvasUpdated,
                vec![record_key(&input.canvas.0)],
            )
            .await;
        Ok(canvas)
    }
}

pub struct DeleteCanvas {
    pub actor: Identity,
    pub canvas: CanvasId,
}

impl Processor<DeleteCanvas> for CanvasService {
    type Output = ();
    type Error = OrchestrationError;
    /// Deletes the canvas and its whole tree in one transaction. An imported
    /// canvas cannot be deleted: retire the import node first.
    ///
    /// Workers of the deleted servers keep running their last config: there is no
    /// canvas left to derive an empty one from, and no view row to send it through.
    /// This is the same behaviour as deleting a single server.
    #[tracing::instrument(name = "Service:DeleteCanvas", skip_all, err)]
    async fn process(&self, input: DeleteCanvas) -> Result<Self::Output, Self::Error> {
        input.actor.ensure(Permission::EditWorkspace)?;
        self.db
            .process(FindCanvasById {
                id: input.canvas.clone(),
            })
            .await?
            .ok_or(OrchestrationError::NotFound)?;
        if let Some(importer) = self
            .db
            .process(FindImporterOf {
                canvas: input.canvas.clone(),
            })
            .await?
        {
            return Err(OrchestrationError::Conflict(format!(
                "canvas is imported by node {} in canvas {}; retire that node first",
                importer.name,
                record_key(&importer.canvas.0)
            )));
        }
        // The transaction re-checks the import and throws on a race.
        self.db
            .process(DeleteCanvasRow {
                id: input.canvas.clone(),
            })
            .await?;
        self.notifier
            .canvas_changed(
                &input.canvas,
                CanvasChangeKind::CanvasDeleted,
                vec![record_key(&input.canvas.0)],
            )
            .await;
        Ok(())
    }
}

pub struct ValidateCanvas {
    pub actor: Identity,
    pub canvas: CanvasId,
}

impl Processor<ValidateCanvas> for CanvasService {
    type Output = Vec<TopologyProblem>;
    type Error = OrchestrationError;
    #[tracing::instrument(name = "Service:ValidateCanvas", skip_all, err)]
    async fn process(&self, input: ValidateCanvas) -> Result<Self::Output, Self::Error> {
        input.actor.ensure(Permission::ViewWorkspace)?;
        self.db
            .process(FindCanvasById {
                id: input.canvas.clone(),
            })
            .await?
            .ok_or(OrchestrationError::NotFound)?;
        let topology = self
            .db
            .process(LoadCanvasTopology {
                canvas: input.canvas,
            })
            .await?;
        // A problem on a generated lane is shown on the universal node that
        // generated it: the lane itself is not on the canvas.
        let group_of: std::collections::HashMap<String, crate::entities::surreal::node::NodeId> =
            topology
                .nodes
                .iter()
                .filter_map(|n| {
                    n.node
                        .lane
                        .as_ref()
                        .map(|lane| (record_key(&n.node.id.0), lane.group.clone()))
                })
                .collect();
        let mut problems = analyze(&topology);
        for problem in &mut problems {
            for node in &mut problem.nodes {
                if let Some(group) = group_of.get(&record_key(&node.0)) {
                    *node = group.clone();
                }
            }
            problem.nodes.dedup_by_key(|n| record_key(&n.0));
        }
        Ok(problems)
    }
}
