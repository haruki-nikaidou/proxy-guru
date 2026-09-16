use crate::entities::db::connection::EdgeConnectionEntity;
use crate::entities::db::node::NodeWithPorts;
use crate::entities::db::server::ServerEntity;
use crate::entities::db::tree;
use base::db::{Db, Error};
use db_types::table_record;
use kanau::processor::Processor;
use std::collections::HashMap;

table_record!(CanvasId, "orchestration_canvas");

/// The conflict reported when a canvas that another canvas imports is deleted.
pub const CANVAS_IMPORTED: &str = "canvas is imported by another canvas";

/// The root identity and generation a validated write fences against: the tree
/// as the snapshot read it. Passed to [`crate::entities::db::fence::touch_checked`]
/// so a concurrent edit that advanced the generation — or re-parented the tree —
/// makes the write roll back instead of committing against stale validation.
#[derive(Debug, Clone)]
pub struct CanvasFence {
    pub root: CanvasId,
    pub generation: i64,
}

#[derive(Debug, Clone, sqlx::FromRow)]
pub struct CanvasEntity {
    pub id: CanvasId,
    pub name: String,
    pub description: String,
    /// Bumped by every mutating transaction; the derivation fence.
    pub generation: i64,
    /// The generation the stored config views were derived from.
    pub derived_generation: i64,
}

/// A position on the dashboard canvas, stored as the `position_x` / `position_y`
/// columns of the row it belongs to.
#[derive(Debug, Clone, Copy, PartialEq, Eq, sqlx::FromRow)]
pub struct CanvasUiPosition {
    #[sqlx(rename = "position_x")]
    pub x: i64,
    #[sqlx(rename = "position_y")]
    pub y: i64,
}

/// Everything the dashboard renders for one canvas.
#[derive(Debug, Clone)]
pub struct CanvasContents {
    pub canvas: CanvasEntity,
    /// Root first, parent last; empty for a root.
    pub ancestors: Vec<CanvasEntity>,
    /// The canvases this canvas's import nodes embed.
    pub import_targets: Vec<CanvasEntity>,
    pub servers: Vec<ServerEntity>,
    pub nodes: Vec<NodeWithPorts>,
    pub edges: Vec<EdgeConnectionEntity>,
}

/// A canvas tree, children ordered by id.
#[derive(Debug, Clone)]
pub struct CanvasTree {
    pub canvas: CanvasEntity,
    pub children: Vec<CanvasTree>,
}

#[derive(Debug)]
pub struct CreateCanvas {
    pub name: String,
    pub description: String,
}

impl Processor<CreateCanvas> for Db {
    type Output = CanvasEntity;
    type Error = Error;
    #[tracing::instrument(name = "Query:CreateCanvas", skip_all, err)]
    async fn process(&self, input: CreateCanvas) -> Result<Self::Output, Self::Error> {
        Ok(sqlx::query_as(
            "INSERT INTO orchestration_canvas (id, name, description)
             VALUES ($1, $2, $3) RETURNING *",
        )
        .bind(CanvasId::new())
        .bind(input.name)
        .bind(input.description)
        .fetch_one(self.db())
        .await?)
    }
}

/// Lists canvases; `include_subcanvases: false` lists only roots.
#[derive(Debug)]
pub struct ListCanvases {
    pub include_subcanvases: bool,
}

impl Processor<ListCanvases> for Db {
    type Output = Vec<CanvasEntity>;
    type Error = Error;
    #[tracing::instrument(name = "Query:ListCanvases", skip_all, err, fields(result_count))]
    async fn process(&self, input: ListCanvases) -> Result<Self::Output, Self::Error> {
        let sql = if input.include_subcanvases {
            "SELECT * FROM orchestration_canvas ORDER BY id"
        } else {
            "SELECT * FROM orchestration_canvas
             WHERE id NOT IN (SELECT import_canvas FROM orchestration_node WHERE import_canvas IS NOT NULL)
             ORDER BY id"
        };
        let result: Vec<CanvasEntity> = sqlx::query_as(sql).fetch_all(self.db()).await?;
        tracing::Span::current().record("result_count", result.len());
        Ok(result)
    }
}

/// The root of the tree `canvas` belongs to (`canvas` itself for a root).
#[derive(Debug)]
pub struct FindRootCanvas {
    pub canvas: CanvasId,
}

impl Processor<FindRootCanvas> for Db {
    type Output = Option<CanvasEntity>;
    type Error = Error;
    #[tracing::instrument(name = "Query:FindRootCanvas", skip_all, err)]
    async fn process(&self, input: FindRootCanvas) -> Result<Self::Output, Self::Error> {
        let mut conn = self.db().acquire().await?;
        let root = tree::root_of(&mut conn, &input.canvas).await?;
        Ok(
            sqlx::query_as("SELECT * FROM orchestration_canvas WHERE id = $1")
                .bind(root)
                .fetch_optional(&mut *conn)
                .await?,
        )
    }
}

/// The whole tree containing `canvas`, from its root. `None` when the canvas
/// does not exist.
#[derive(Debug)]
pub struct LoadCanvasTree {
    pub canvas: CanvasId,
}

impl Processor<LoadCanvasTree> for Db {
    type Output = Option<CanvasTree>;
    type Error = Error;
    #[tracing::instrument(name = "Query-Transaction:LoadCanvasTree", skip_all, err)]
    async fn process(&self, input: LoadCanvasTree) -> Result<Self::Output, Self::Error> {
        let mut tx = self.db().begin_with(SNAPSHOT_READ).await?;
        let (root, ids) = tree::whole_tree_of(&mut tx, &input.canvas).await?;
        let canvases: Vec<CanvasEntity> =
            sqlx::query_as("SELECT * FROM orchestration_canvas WHERE id = ANY($1)")
                .bind(&ids)
                .fetch_all(&mut *tx)
                .await?;
        let links: Vec<(CanvasId, CanvasId)> = sqlx::query_as(
            "SELECT canvas, import_canvas FROM orchestration_node
             WHERE canvas = ANY($1) AND import_canvas IS NOT NULL",
        )
        .bind(&ids)
        .fetch_all(&mut *tx)
        .await?;
        tx.commit().await?;
        if canvases.is_empty() {
            return Ok(None);
        }
        let mut by_id: HashMap<CanvasId, CanvasEntity> =
            canvases.into_iter().map(|c| (c.id.clone(), c)).collect();
        let mut children_of: HashMap<CanvasId, Vec<CanvasId>> = HashMap::new();
        for (canvas, target) in links {
            children_of.entry(canvas).or_default().push(target);
        }
        Ok(assemble_tree(&root, &mut by_id, &children_of, 0))
    }
}

/// `BEGIN` for the multi-statement reads: one snapshot for every statement in
/// the transaction, so a tree read while an edit commits is either all before
/// or all after it.
pub(crate) const SNAPSHOT_READ: &str = "BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY";

fn assemble_tree(
    id: &CanvasId,
    by_id: &mut HashMap<CanvasId, CanvasEntity>,
    children_of: &HashMap<CanvasId, Vec<CanvasId>>,
    depth: usize,
) -> Option<CanvasTree> {
    // The tree walk already refuses deeper trees; the guard only keeps a corrupt
    // link set from recursing forever.
    if depth > 32 {
        return None;
    }
    let canvas = by_id.remove(id)?;
    let mut child_ids = children_of.get(id).cloned().unwrap_or_default();
    child_ids.sort();
    let children = child_ids
        .iter()
        .filter_map(|child| assemble_tree(child, by_id, children_of, depth.saturating_add(1)))
        .collect();
    Some(CanvasTree { canvas, children })
}

#[derive(Debug)]
pub struct FindCanvasById {
    pub id: CanvasId,
}

impl Processor<FindCanvasById> for Db {
    type Output = Option<CanvasEntity>;
    type Error = Error;
    #[tracing::instrument(name = "Query:FindCanvasById", skip_all, err)]
    async fn process(&self, input: FindCanvasById) -> Result<Self::Output, Self::Error> {
        Ok(
            sqlx::query_as("SELECT * FROM orchestration_canvas WHERE id = $1")
                .bind(input.id)
                .fetch_optional(self.db())
                .await?,
        )
    }
}

#[derive(Debug)]
pub struct UpdateCanvasMeta {
    pub id: CanvasId,
    pub name: String,
    pub description: String,
}

impl Processor<UpdateCanvasMeta> for Db {
    type Output = CanvasEntity;
    type Error = Error;
    #[tracing::instrument(name = "Query:UpdateCanvasMeta", skip_all, err, fields(canvas_id = %input.id))]
    async fn process(&self, input: UpdateCanvasMeta) -> Result<Self::Output, Self::Error> {
        Ok(sqlx::query_as(
            "UPDATE orchestration_canvas SET name = $2, description = $3 WHERE id = $1 RETURNING *",
        )
        .bind(input.id)
        .bind(input.name)
        .bind(input.description)
        .fetch_one(self.db())
        .await?)
    }
}

/// Deletes a canvas and its whole tree; refuses an imported canvas with
/// [`CANVAS_IMPORTED`] (the service reports the importer to the operator, this
/// is the race guard).
#[derive(Debug)]
pub struct DeleteCanvasRow {
    pub id: CanvasId,
}

impl Processor<DeleteCanvasRow> for Db {
    type Output = ();
    type Error = Error;
    #[tracing::instrument(name = "Query-Transaction:DeleteCanvasRow", skip_all, err, fields(canvas_id = %input.id))]
    async fn process(&self, input: DeleteCanvasRow) -> Result<Self::Output, Self::Error> {
        let mut tx = self.db().begin().await?;
        let importer: Option<crate::entities::db::node::NodeId> =
            sqlx::query_scalar("SELECT id FROM orchestration_node WHERE import_canvas = $1")
                .bind(&input.id)
                .fetch_optional(&mut *tx)
                .await?;
        if importer.is_some() {
            return Err(Error::Conflict(CANVAS_IMPORTED));
        }
        let ids = tree::tree_of(&mut tx, &input.id).await?;
        // Servers, nodes, ports, edges, views, health history and relay leaves
        // all cascade from the canvas rows; the import and pod-server foreign
        // keys are checked at the end of the statement, when the importing nodes
        // and the pods of the tree are gone as well.
        sqlx::query("DELETE FROM orchestration_canvas WHERE id = ANY($1)")
            .bind(&ids)
            .execute(&mut *tx)
            .await?;
        tx.commit().await?;
        Ok(())
    }
}
