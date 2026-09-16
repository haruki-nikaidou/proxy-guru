//! Canvas CRUD.
//!
//! Canvases nest by `parent`: a subcanvas is only a way to organise a big graph,
//! so edges cross canvas boundaries freely within a tree, and the tree is what
//! gets checked and derived as a whole.

use crate::entities::db::canvas::{
    CanvasEntity, CanvasId, CanvasTree, CanvasUiPosition, CreateCanvas as CreateCanvasRow,
    DeleteCanvasRow, FindCanvasById, ListCanvases as ListCanvasesRow, LoadCanvasTree,
    UpdateCanvasMeta,
};
use crate::entities::db::edge::EdgeTarget;
use crate::entities::db::graph::LoadCanvasGraph;
use crate::events::live::CanvasChangeKind;
use crate::services::OrchestrationError;
use crate::services::notify::Notifier;
use auth::services::identity::Identity;
use auth::utils::rbac::Permission;
use base::db::Db;
use kanau::processor::Processor;
use std::collections::HashSet;

#[derive(Clone)]
pub struct CanvasService {
    pub db: Db,
    pub notifier: Notifier,
}

pub struct CreateCanvas {
    pub actor: Identity,
    pub name: String,
    pub description: String,
    /// `Some` creates a subcanvas drawn at `position` inside that canvas.
    pub parent: Option<CanvasId>,
    pub position: CanvasUiPosition,
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
        let canvas = self
            .db
            .process(CreateCanvasRow {
                name: input.name,
                description: input.description,
                parent: input.parent.clone(),
                position: input.position,
            })
            .await?;
        if let Some(parent) = &input.parent {
            self.notifier
                .canvas_changed(
                    parent,
                    CanvasChangeKind::CanvasCreated,
                    vec![canvas.id.to_string()],
                )
                .await;
        }
        Ok(canvas)
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

pub struct UpdateCanvas {
    pub actor: Identity,
    pub canvas: CanvasId,
    pub name: String,
    pub description: String,
    /// Where a subcanvas is drawn on its parent; `None` leaves it.
    pub position: Option<CanvasUiPosition>,
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
                position: input.position,
            })
            .await?;
        self.notifier
            .canvas_changed(
                &input.canvas,
                CanvasChangeKind::CanvasUpdated,
                vec![input.canvas.to_string()],
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
    /// Deletes the canvas with everything below it, in one transaction.
    ///
    /// Refused while something outside the subtree still depends on it: an edge
    /// from a pod outside into a pod or exit inside (the pod's route names it),
    /// or a pod outside placed on a server inside. Workers of the deleted
    /// servers keep running their last config, as when a single server is
    /// deleted; the servers of the rest of the tree are re-derived.
    #[tracing::instrument(name = "Service:DeleteCanvas", skip_all, err)]
    async fn process(&self, input: DeleteCanvas) -> Result<Self::Output, Self::Error> {
        input.actor.ensure(Permission::EditWorkspace)?;
        let canvas = self
            .db
            .process(FindCanvasById {
                id: input.canvas.clone(),
            })
            .await?
            .ok_or(OrchestrationError::NotFound)?;
        let graph = self
            .db
            .process(LoadCanvasGraph {
                canvas: input.canvas.clone(),
            })
            .await?;
        let tree = self
            .db
            .process(LoadCanvasTree {
                canvas: input.canvas.clone(),
            })
            .await?
            .ok_or(OrchestrationError::NotFound)?;
        let mut doomed: HashSet<CanvasId> = HashSet::new();
        if let Some(subtree) = find_subtree(&tree, &input.canvas) {
            collect(subtree, &mut doomed);
        }
        let inside_pods: HashSet<&str> = graph
            .pods
            .iter()
            .filter(|p| doomed.contains(&p.canvas))
            .map(|p| p.id.as_str())
            .collect();
        let inside_exits: HashSet<&str> = graph
            .exits
            .iter()
            .filter(|e| doomed.contains(&e.canvas))
            .map(|e| e.id.as_str())
            .collect();
        let inside_servers: HashSet<&str> = graph
            .servers
            .iter()
            .filter(|s| doomed.contains(&s.canvas))
            .map(|s| s.id.as_str())
            .collect();
        let dialing: Vec<&str> = graph
            .edges
            .iter()
            .filter(|edge| !inside_pods.contains(edge.source.as_str()))
            .filter(|edge| match &edge.target {
                EdgeTarget::Pod(pod) => inside_pods.contains(pod.as_str()),
                EdgeTarget::Exit(exit) => inside_exits.contains(exit.as_str()),
            })
            .filter_map(|edge| {
                graph
                    .pods
                    .iter()
                    .find(|p| p.id == edge.source)
                    .map(|p| p.name.as_str())
            })
            .collect();
        if let Some(pod) = dialing.first() {
            return Err(OrchestrationError::Conflict(format!(
                "pod {pod} outside this canvas still leads into it; remove those edges first"
            )));
        }
        if let Some(pod) = graph.pods.iter().find(|p| {
            !doomed.contains(&p.canvas) && inside_servers.contains(p.server.as_str())
        }) {
            return Err(OrchestrationError::Conflict(format!(
                "pod {} outside this canvas runs on a server inside it; move or delete it first",
                pod.name
            )));
        }
        self.db
            .process(DeleteCanvasRow {
                id: input.canvas.clone(),
            })
            .await?;
        if let Some(parent) = &canvas.parent {
            self.notifier.notify(parent).await;
        }
        self.notifier
            .canvas_changed(
                canvas.parent.as_ref().unwrap_or(&input.canvas),
                CanvasChangeKind::CanvasDeleted,
                vec![input.canvas.to_string()],
            )
            .await;
        Ok(())
    }
}

fn find_subtree<'a>(tree: &'a CanvasTree, id: &CanvasId) -> Option<&'a CanvasTree> {
    if tree.canvas.id == *id {
        return Some(tree);
    }
    tree.children.iter().find_map(|child| find_subtree(child, id))
}

fn collect(tree: &CanvasTree, out: &mut HashSet<CanvasId>) {
    out.insert(tree.canvas.id.clone());
    for child in &tree.children {
        collect(child, out);
    }
}
