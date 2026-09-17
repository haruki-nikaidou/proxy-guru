use crate::entities::db::tree;
use base::db::{Db, Error};
use db_types::table_record;
use kanau::processor::Processor;
use std::collections::HashMap;

table_record!(CanvasId, "orchestration_canvas");

/// The conflict reported when a subcanvas would be created under a canvas that
/// does not exist.
pub const PARENT_MISSING: &str = "the parent canvas does not exist";

/// The root identity and generation a validated write fences against: the tree
/// as the snapshot read it. Passed to [`crate::entities::db::fence::touch_checked`]
/// so a concurrent edit that advanced the generation makes the write roll back
/// instead of committing against stale validation.
#[derive(Debug, Clone)]
pub struct CanvasFence {
    pub root: CanvasId,
    pub generation: i64,
}

#[derive(Debug, Clone)]
pub struct CanvasEntity {
    pub id: CanvasId,
    pub name: String,
    pub description: String,
    /// The canvas this one is drawn inside; `None` for a root.
    pub parent: Option<CanvasId>,
    /// Where it is drawn on its parent.
    pub position: CanvasUiPosition,
    /// Bumped by every mutating transaction; the derivation fence.
    pub generation: i64,
    /// The generation the stored config views were derived from.
    pub derived_generation: i64,
}

/// The `orchestration_canvas` columns a [`CanvasEntity`] is made of, as the
/// query macros hand them over: one field per column, the position still flat.
pub(crate) struct CanvasRow {
    pub(crate) id: CanvasId,
    pub(crate) name: String,
    pub(crate) description: String,
    pub(crate) parent: Option<CanvasId>,
    pub(crate) position_x: i64,
    pub(crate) position_y: i64,
    pub(crate) generation: i64,
    pub(crate) derived_generation: i64,
}

impl From<CanvasRow> for CanvasEntity {
    fn from(row: CanvasRow) -> Self {
        Self {
            id: row.id,
            name: row.name,
            description: row.description,
            parent: row.parent,
            position: CanvasUiPosition {
                x: row.position_x,
                y: row.position_y,
            },
            generation: row.generation,
            derived_generation: row.derived_generation,
        }
    }
}

/// A position on the dashboard canvas, stored as the `position_x` / `position_y`
/// columns of the row it belongs to.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct CanvasUiPosition {
    pub x: i64,
    pub y: i64,
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
    /// `Some` creates a subcanvas drawn at `position` inside that canvas.
    pub parent: Option<CanvasId>,
    pub position: CanvasUiPosition,
}

impl Processor<CreateCanvas> for Db {
    type Output = CanvasEntity;
    type Error = Error;
    #[tracing::instrument(name = "Query-Transaction:CreateCanvas", skip_all, err)]
    async fn process(&self, input: CreateCanvas) -> Result<Self::Output, Self::Error> {
        let mut tx = self.db().begin().await?;
        if let Some(parent) = &input.parent {
            // The parent's tree gets a new member; its depth is checked by the
            // walk, and the root is bumped so the tree's readers notice.
            let exists: Option<CanvasId> = sqlx::query_scalar!(
                r#"SELECT id AS "id: CanvasId" FROM orchestration_canvas WHERE id = $1 FOR UPDATE"#,
                parent as _
            )
            .fetch_optional(&mut *tx)
            .await?;
            if exists.is_none() {
                return Err(Error::Conflict(PARENT_MISSING));
            }
            if tree::ancestors_of(&mut tx, parent).await?.len()
                >= usize::try_from(tree::MAX_DEPTH - 1).unwrap_or(0)
            {
                return Err(Error::Conflict(tree::NESTING_TOO_DEEP));
            }
        }
        let canvas: CanvasEntity = sqlx::query_as!(
            CanvasRow,
            r#"INSERT INTO orchestration_canvas (id, name, description, parent, position_x, position_y)
               VALUES ($1, $2, $3, $4, $5, $6)
               RETURNING id AS "id: CanvasId", name, description, parent AS "parent: CanvasId",
                         position_x, position_y, generation, derived_generation"#,
            CanvasId::new() as _,
            input.name,
            input.description,
            input.parent as _,
            input.position.x,
            input.position.y
        )
        .fetch_one(&mut *tx)
        .await?
        .into();
        tx.commit().await?;
        Ok(canvas)
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
        let rows = if input.include_subcanvases {
            sqlx::query_as!(
                CanvasRow,
                r#"SELECT id AS "id: CanvasId", name, description, parent AS "parent: CanvasId",
                          position_x, position_y, generation, derived_generation
                   FROM orchestration_canvas ORDER BY id"#
            )
            .fetch_all(self.db())
            .await?
        } else {
            sqlx::query_as!(
                CanvasRow,
                r#"SELECT id AS "id: CanvasId", name, description, parent AS "parent: CanvasId",
                          position_x, position_y, generation, derived_generation
                   FROM orchestration_canvas WHERE parent IS NULL ORDER BY id"#
            )
            .fetch_all(self.db())
            .await?
        };
        let result: Vec<CanvasEntity> = rows.into_iter().map(CanvasEntity::from).collect();
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
        Ok(sqlx::query_as!(
            CanvasRow,
            r#"SELECT id AS "id: CanvasId", name, description, parent AS "parent: CanvasId",
                      position_x, position_y, generation, derived_generation
               FROM orchestration_canvas WHERE id = $1"#,
            root as _
        )
        .fetch_optional(&mut *conn)
        .await?
        .map(CanvasEntity::from))
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
        let canvases: Vec<CanvasEntity> = sqlx::query_as!(
            CanvasRow,
            r#"SELECT id AS "id: CanvasId", name, description, parent AS "parent: CanvasId",
                      position_x, position_y, generation, derived_generation
               FROM orchestration_canvas WHERE id = ANY($1)"#,
            &ids as _
        )
        .fetch_all(&mut *tx)
        .await?
        .into_iter()
        .map(CanvasEntity::from)
        .collect();
        tx.commit().await?;
        Ok(assemble_tree(&root, canvases))
    }
}

/// `BEGIN` for the multi-statement reads: one snapshot for every statement in
/// the transaction, so a tree read while an edit commits is either all before
/// or all after it.
pub(crate) const SNAPSHOT_READ: &str = "BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY";

/// The tree below `root` out of a flat list of its canvases.
pub fn assemble_tree(root: &CanvasId, canvases: Vec<CanvasEntity>) -> Option<CanvasTree> {
    let mut children_of: HashMap<CanvasId, Vec<CanvasId>> = HashMap::new();
    for canvas in &canvases {
        if let Some(parent) = &canvas.parent {
            children_of
                .entry(parent.clone())
                .or_default()
                .push(canvas.id.clone());
        }
    }
    let mut by_id: HashMap<CanvasId, CanvasEntity> =
        canvases.into_iter().map(|c| (c.id.clone(), c)).collect();
    assemble(root, &mut by_id, &children_of, 0)
}

fn assemble(
    id: &CanvasId,
    by_id: &mut HashMap<CanvasId, CanvasEntity>,
    children_of: &HashMap<CanvasId, Vec<CanvasId>>,
    depth: usize,
) -> Option<CanvasTree> {
    // The tree walk already refuses deeper trees; the guard only keeps a corrupt
    // parent chain from recursing forever.
    if depth > 32 {
        return None;
    }
    let canvas = by_id.remove(id)?;
    let mut child_ids = children_of.get(id).cloned().unwrap_or_default();
    child_ids.sort();
    let children = child_ids
        .iter()
        .filter_map(|child| assemble(child, by_id, children_of, depth.saturating_add(1)))
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
        Ok(sqlx::query_as!(
            CanvasRow,
            r#"SELECT id AS "id: CanvasId", name, description, parent AS "parent: CanvasId",
                      position_x, position_y, generation, derived_generation
               FROM orchestration_canvas WHERE id = $1"#,
            input.id as _
        )
        .fetch_optional(self.db())
        .await?
        .map(CanvasEntity::from))
    }
}

/// Name, description and, when given, the position on the parent: nothing a
/// worker reads, so no generation bump.
#[derive(Debug)]
pub struct UpdateCanvasMeta {
    pub id: CanvasId,
    pub name: String,
    pub description: String,
    pub position: Option<CanvasUiPosition>,
}

impl Processor<UpdateCanvasMeta> for Db {
    type Output = CanvasEntity;
    type Error = Error;
    #[tracing::instrument(name = "Query:UpdateCanvasMeta", skip_all, err, fields(canvas_id = %input.id))]
    async fn process(&self, input: UpdateCanvasMeta) -> Result<Self::Output, Self::Error> {
        Ok(sqlx::query_file_as!(
            CanvasRow,
            "sql/update_canvas_meta.sql",
            input.id as _,
            input.name,
            input.description,
            input.position.map(|p| p.x),
            input.position.map(|p| p.y)
        )
        .fetch_one(self.db())
        .await?
        .into())
    }
}

/// Deletes a canvas with everything below it: subcanvases, servers, pods,
/// exits, groups, the edges leaving its pods, views and health history. The
/// service refuses first when something outside the subtree still leads into
/// it; the foreign keys are the race guard.
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
        let exists: Option<CanvasId> = sqlx::query_scalar!(
            r#"SELECT id AS "id: CanvasId" FROM orchestration_canvas WHERE id = $1"#,
            &input.id as _
        )
        .fetch_optional(&mut *tx)
        .await?;
        if exists.is_none() {
            return Ok(());
        }
        // The tree's root before any of its rows (`fence`'s lock order): the
        // subtree's view rows are what a derivation commit holding the root
        // rewrites. Deleting a subcanvas is an edit of the tree it leaves; a
        // deleted root takes its bump with it.
        crate::entities::db::fence::touch(&mut tx, &input.id).await?;
        let ids = tree::tree_of(&mut tx, &input.id).await?;
        // Edges do not cascade from their pods: a route names them, and a route
        // is only ever rewritten together with its edges. The subtree's own
        // routes go with it.
        sqlx::query!(
            "DELETE FROM orchestration_edge e USING orchestration_pod p
             WHERE p.id = e.source_pod AND p.canvas = ANY($1)",
            &ids as _
        )
        .execute(&mut *tx)
        .await?;
        sqlx::query!(
            "DELETE FROM orchestration_canvas WHERE id = ANY($1)",
            &ids as _
        )
        .execute(&mut *tx)
        .await?;
        tx.commit().await?;
        Ok(())
    }
}
