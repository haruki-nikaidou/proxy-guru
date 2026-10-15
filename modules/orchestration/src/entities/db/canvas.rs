use crate::entities::db::fence::Touch;
use crate::entities::db::tables::{OrchestrationCanvas, OrchestrationEdge, OrchestrationPod};
use crate::entities::db::tree::{
    AncestorsOf, MAX_DEPTH, NESTING_TOO_DEEP, RootOf, TreeOf, WholeTreeOf,
};
use base::db::Error;
use db_types::table_record;
use sqlx::{PgConnection, Postgres};
use std::collections::HashMap;
use wakuwaku::entity::sqlx::backend::PgSource;
use wakuwaku::entity::sqlx::transaction::{Either, IsolationLevel, Tx, TxEnd, TxFlow, TxState};
use wakuwaku::entity::{Execute, Query, Read, Write, tx_effect};

table_record!(CanvasId, "orchestration_canvas");

/// The conflict reported when a subcanvas would be created under a canvas that
/// does not exist.
pub const PARENT_MISSING: &str = "the parent canvas does not exist";

/// The root identity and generation a validated write fences against: the tree
/// as the snapshot read it. Passed to [`crate::entities::db::fence::TouchChecked`]
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

/// The canvas rows with the given ids, in no particular order.
pub(crate) struct CanvasesByIds<'a> {
    pub(crate) ids: &'a [CanvasId],
}

impl Query for CanvasesByIds<'_> {
    type Effect = Read<OrchestrationCanvas>;
}

impl Execute<PgSource> for CanvasesByIds<'_> {
    type Output = Vec<CanvasEntity>;
    type Error = Error;
    async fn execute(self, conn: &mut PgConnection) -> Result<Self::Output, Self::Error> {
        Ok(sqlx::query_as!(
            CanvasRow,
            r#"SELECT id AS "id: CanvasId", name, description, parent AS "parent: CanvasId",
                      position_x, position_y, generation, derived_generation
               FROM orchestration_canvas WHERE id = ANY($1)"#,
            self.ids as _
        )
        .fetch_all(conn)
        .await?
        .into_iter()
        .map(CanvasEntity::from)
        .collect())
    }
}

#[derive(Debug)]
pub struct CreateCanvas {
    pub name: String,
    pub description: String,
    /// `Some` creates a subcanvas drawn at `position` inside that canvas.
    pub parent: Option<CanvasId>,
    pub position: CanvasUiPosition,
}

/// Locks the parent a subcanvas is created under; `None` when it does not exist.
struct LockParentCanvas<'a> {
    parent: &'a CanvasId,
}

impl Query for LockParentCanvas<'_> {
    type Effect = Write<OrchestrationCanvas>;
}

impl Execute<PgSource> for LockParentCanvas<'_> {
    type Output = Option<CanvasId>;
    type Error = Error;
    async fn execute(self, conn: &mut PgConnection) -> Result<Self::Output, Self::Error> {
        Ok(sqlx::query_scalar!(
            r#"SELECT id AS "id: CanvasId" FROM orchestration_canvas WHERE id = $1 FOR UPDATE"#,
            self.parent as _
        )
        .fetch_optional(conn)
        .await?)
    }
}

struct InsertCanvasRow<'a> {
    canvas: &'a CreateCanvas,
}

impl Query for InsertCanvasRow<'_> {
    type Effect = Write<OrchestrationCanvas>;
}

impl Execute<PgSource> for InsertCanvasRow<'_> {
    type Output = CanvasEntity;
    type Error = Error;
    async fn execute(self, conn: &mut PgConnection) -> Result<Self::Output, Self::Error> {
        let input = self.canvas;
        Ok(sqlx::query_as!(
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
        .fetch_one(conn)
        .await?
        .into())
    }
}

/// Creates a canvas (a root, or a subcanvas under a locked parent) as one
/// transaction. Run it with `db.process(Transaction(CreateCanvasFlow::new(input)))`.
pub enum CreateCanvasFlow {
    Start(CreateCanvas),
    End(TxEnd<CreateCanvasFlow>),
}

impl CreateCanvasFlow {
    pub fn new(input: CreateCanvas) -> Self {
        CreateCanvasFlow::Start(input)
    }
}

tx_effect! {
    /// The tables [`CreateCanvasFlow`] touches.
    pub enum CreateCanvasEffect { write OrchestrationCanvas }
}

impl From<TxEnd<CreateCanvasFlow>> for CreateCanvasFlow {
    fn from(end: TxEnd<CreateCanvasFlow>) -> Self {
        CreateCanvasFlow::End(end)
    }
}

impl TxFlow for CreateCanvasFlow {
    type Capability = CreateCanvasEffect;
    type Database = Postgres;
    type Commit = CanvasEntity;
    type Abort = Error;
    type Report = ();

    #[tracing::instrument(name = "Transaction:CreateCanvas", skip_all)]
    async fn step(tx: Tx<'_, Self>, state: Self) -> TxState<'_, Self> {
        let CreateCanvasFlow::Start(input) = state else {
            return tx.end(state).await;
        };
        let mut tx = tx;
        if let Some(parent) = &input.parent {
            // The parent's tree gets a new member; its depth is checked by the
            // walk, and the root is bumped so the tree's readers notice.
            let (next, exists) = tx.run(LockParentCanvas { parent }).await;
            tx = next;
            match exists {
                Ok(Some(_)) => {}
                Ok(None) => return base::db::rollback(tx, Error::Conflict(PARENT_MISSING)).await,
                Err(e) => return base::db::rollback(tx, e).await,
            }
            let (next, ancestors) = tx.run(AncestorsOf { canvas: parent }).await;
            tx = next;
            let ancestors = match ancestors {
                Ok(ancestors) => ancestors,
                Err(e) => return base::db::rollback(tx, e).await,
            };
            if ancestors.len() >= usize::try_from(MAX_DEPTH - 1).unwrap_or(0) {
                return base::db::rollback(tx, Error::Conflict(NESTING_TOO_DEEP)).await;
            }
        }
        let (tx, canvas) = tx.run(InsertCanvasRow { canvas: &input }).await;
        match canvas {
            Ok(canvas) => base::db::commit(tx, canvas).await,
            Err(e) => base::db::rollback(tx, e).await,
        }
    }

    fn finished(self) -> Either<Self, TxEnd<Self>> {
        match self {
            CreateCanvasFlow::End(end) => Either::Right(end),
            state => Either::Left(state),
        }
    }
}

/// Lists canvases; `include_subcanvases: false` lists only roots.
#[derive(Debug)]
pub struct ListCanvases {
    pub include_subcanvases: bool,
}

impl Query for ListCanvases {
    type Effect = Read<OrchestrationCanvas>;
}

impl Execute<PgSource> for ListCanvases {
    type Output = Vec<CanvasEntity>;
    type Error = Error;
    #[tracing::instrument(name = "Query:ListCanvases", skip_all, err, fields(result_count))]
    async fn execute(self, conn: &mut PgConnection) -> Result<Self::Output, Self::Error> {
        let rows = if self.include_subcanvases {
            sqlx::query_as!(
                CanvasRow,
                r#"SELECT id AS "id: CanvasId", name, description, parent AS "parent: CanvasId",
                          position_x, position_y, generation, derived_generation
                   FROM orchestration_canvas ORDER BY id"#
            )
            .fetch_all(conn)
            .await?
        } else {
            sqlx::query_as!(
                CanvasRow,
                r#"SELECT id AS "id: CanvasId", name, description, parent AS "parent: CanvasId",
                          position_x, position_y, generation, derived_generation
                   FROM orchestration_canvas WHERE parent IS NULL ORDER BY id"#
            )
            .fetch_all(conn)
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

impl Query for FindRootCanvas {
    type Effect = Read<OrchestrationCanvas>;
}

impl Execute<PgSource> for FindRootCanvas {
    type Output = Option<CanvasEntity>;
    type Error = Error;
    #[tracing::instrument(name = "Query:FindRootCanvas", skip_all, err)]
    async fn execute(self, conn: &mut PgConnection) -> Result<Self::Output, Self::Error> {
        let root = RootOf {
            canvas: &self.canvas,
        }
        .execute(&mut *conn)
        .await?;
        Ok(sqlx::query_as!(
            CanvasRow,
            r#"SELECT id AS "id: CanvasId", name, description, parent AS "parent: CanvasId",
                      position_x, position_y, generation, derived_generation
               FROM orchestration_canvas WHERE id = $1"#,
            root as _
        )
        .fetch_optional(conn)
        .await?
        .map(CanvasEntity::from))
    }
}

/// The whole tree containing `canvas`, from its root. `None` when the canvas
/// does not exist.
///
/// A read-only `REPEATABLE READ` transaction: one snapshot for every statement,
/// so a tree read while an edit commits is either all before or all after it.
#[derive(Debug)]
pub struct LoadCanvasTree {
    pub canvas: CanvasId,
}

/// Reads the tree of [`LoadCanvasTree`] in one snapshot. Run it with
/// `db.process(Transaction(LoadCanvasTreeFlow::new(input)))`.
pub enum LoadCanvasTreeFlow {
    Start(LoadCanvasTree),
    End(TxEnd<LoadCanvasTreeFlow>),
}

impl LoadCanvasTreeFlow {
    pub fn new(input: LoadCanvasTree) -> Self {
        LoadCanvasTreeFlow::Start(input)
    }
}

tx_effect! {
    /// The tables [`LoadCanvasTreeFlow`] touches.
    pub enum LoadCanvasTreeEffect { read OrchestrationCanvas }
}

impl From<TxEnd<LoadCanvasTreeFlow>> for LoadCanvasTreeFlow {
    fn from(end: TxEnd<LoadCanvasTreeFlow>) -> Self {
        LoadCanvasTreeFlow::End(end)
    }
}

impl TxFlow for LoadCanvasTreeFlow {
    type Capability = LoadCanvasTreeEffect;
    type Database = Postgres;
    type Commit = Option<CanvasTree>;
    type Abort = Error;
    type Report = ();
    const ISOLATION: Option<IsolationLevel> = Some(IsolationLevel::RepeatableRead);

    #[tracing::instrument(name = "Transaction:LoadCanvasTree", skip_all)]
    async fn step(tx: Tx<'_, Self>, state: Self) -> TxState<'_, Self> {
        let LoadCanvasTreeFlow::Start(input) = state else {
            return tx.end(state).await;
        };
        let (tx, tree) = tx
            .run(WholeTreeOf {
                canvas: &input.canvas,
            })
            .await;
        let (root, ids) = match tree {
            Ok(tree) => tree,
            Err(e) => return base::db::rollback(tx, e).await,
        };
        let (tx, canvases) = tx.run(CanvasesByIds { ids: &ids }).await;
        match canvases {
            Ok(canvases) => base::db::commit(tx, assemble_tree(&root, canvases)).await,
            Err(e) => base::db::rollback(tx, e).await,
        }
    }

    fn finished(self) -> Either<Self, TxEnd<Self>> {
        match self {
            LoadCanvasTreeFlow::End(end) => Either::Right(end),
            state => Either::Left(state),
        }
    }
}

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

impl Query for FindCanvasById {
    type Effect = Read<OrchestrationCanvas>;
}

impl Execute<PgSource> for FindCanvasById {
    type Output = Option<CanvasEntity>;
    type Error = Error;
    #[tracing::instrument(name = "Query:FindCanvasById", skip_all, err)]
    async fn execute(self, conn: &mut PgConnection) -> Result<Self::Output, Self::Error> {
        Ok(sqlx::query_as!(
            CanvasRow,
            r#"SELECT id AS "id: CanvasId", name, description, parent AS "parent: CanvasId",
                      position_x, position_y, generation, derived_generation
               FROM orchestration_canvas WHERE id = $1"#,
            self.id as _
        )
        .fetch_optional(conn)
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

impl Query for UpdateCanvasMeta {
    type Effect = Write<OrchestrationCanvas>;
}

impl Execute<PgSource> for UpdateCanvasMeta {
    type Output = CanvasEntity;
    type Error = Error;
    #[tracing::instrument(name = "Query:UpdateCanvasMeta", skip_all, err, fields(canvas_id = %self.id))]
    async fn execute(self, conn: &mut PgConnection) -> Result<Self::Output, Self::Error> {
        Ok(sqlx::query_file_as!(
            CanvasRow,
            "sql/update_canvas_meta.sql",
            self.id as _,
            self.name,
            self.description,
            self.position.map(|p| p.x),
            self.position.map(|p| p.y)
        )
        .fetch_one(conn)
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

struct CanvasExists<'a> {
    id: &'a CanvasId,
}

impl Query for CanvasExists<'_> {
    type Effect = Read<OrchestrationCanvas>;
}

impl Execute<PgSource> for CanvasExists<'_> {
    type Output = bool;
    type Error = Error;
    async fn execute(self, conn: &mut PgConnection) -> Result<Self::Output, Self::Error> {
        let exists: Option<CanvasId> = sqlx::query_scalar!(
            r#"SELECT id AS "id: CanvasId" FROM orchestration_canvas WHERE id = $1"#,
            self.id as _
        )
        .fetch_optional(conn)
        .await?;
        Ok(exists.is_some())
    }
}

/// Deletes the edges leaving the pods of the given canvases.
struct DeleteEdgesOfCanvases<'a> {
    canvases: &'a [CanvasId],
}

impl Query for DeleteEdgesOfCanvases<'_> {
    type Effect = (Write<OrchestrationEdge>, Read<OrchestrationPod>);
}

impl Execute<PgSource> for DeleteEdgesOfCanvases<'_> {
    type Output = ();
    type Error = Error;
    async fn execute(self, conn: &mut PgConnection) -> Result<Self::Output, Self::Error> {
        sqlx::query!(
            "DELETE FROM orchestration_edge e USING orchestration_pod p
             WHERE p.id = e.source_pod AND p.canvas = ANY($1)",
            self.canvases as _
        )
        .execute(conn)
        .await?;
        Ok(())
    }
}

struct DeleteCanvases<'a> {
    ids: &'a [CanvasId],
}

impl Query for DeleteCanvases<'_> {
    type Effect = Write<OrchestrationCanvas>;
}

impl Execute<PgSource> for DeleteCanvases<'_> {
    type Output = ();
    type Error = Error;
    async fn execute(self, conn: &mut PgConnection) -> Result<Self::Output, Self::Error> {
        sqlx::query!(
            "DELETE FROM orchestration_canvas WHERE id = ANY($1)",
            self.ids as _
        )
        .execute(conn)
        .await?;
        Ok(())
    }
}

/// Deletes the subtree of [`DeleteCanvasRow`]; a missing canvas commits
/// nothing. Run it with `db.process(Transaction(DeleteCanvasRowFlow::new(input)))`.
pub enum DeleteCanvasRowFlow {
    Start(DeleteCanvasRow),
    End(TxEnd<DeleteCanvasRowFlow>),
}

impl DeleteCanvasRowFlow {
    pub fn new(input: DeleteCanvasRow) -> Self {
        DeleteCanvasRowFlow::Start(input)
    }
}

tx_effect! {
    /// The tables [`DeleteCanvasRowFlow`] touches.
    pub enum DeleteCanvasRowEffect {
        write OrchestrationCanvas,
        write OrchestrationEdge,
        read OrchestrationPod,
    }
}

impl From<TxEnd<DeleteCanvasRowFlow>> for DeleteCanvasRowFlow {
    fn from(end: TxEnd<DeleteCanvasRowFlow>) -> Self {
        DeleteCanvasRowFlow::End(end)
    }
}

impl TxFlow for DeleteCanvasRowFlow {
    type Capability = DeleteCanvasRowEffect;
    type Database = Postgres;
    type Commit = ();
    type Abort = Error;
    type Report = ();

    #[tracing::instrument(name = "Transaction:DeleteCanvasRow", skip_all)]
    async fn step(tx: Tx<'_, Self>, state: Self) -> TxState<'_, Self> {
        let DeleteCanvasRowFlow::Start(input) = state else {
            return tx.end(state).await;
        };
        let (tx, exists) = tx.run(CanvasExists { id: &input.id }).await;
        match exists {
            Ok(true) => {}
            Ok(false) => return base::db::commit(tx, ()).await,
            Err(e) => return base::db::rollback(tx, e).await,
        }
        // The tree's root before any of its rows (`fence`'s lock order): the
        // subtree's view rows are what a derivation commit holding the root
        // rewrites. Deleting a subcanvas is an edit of the tree it leaves; a
        // deleted root takes its bump with it.
        let (tx, touched) = tx.run(Touch { canvas: &input.id }).await;
        if let Err(e) = touched {
            return base::db::rollback(tx, e).await;
        }
        let (tx, ids) = tx.run(TreeOf { canvas: &input.id }).await;
        let ids = match ids {
            Ok(ids) => ids,
            Err(e) => return base::db::rollback(tx, e).await,
        };
        // Edges do not cascade from their pods: a route names them, and a route
        // is only ever rewritten together with its edges. The subtree's own
        // routes go with it.
        let (tx, deleted) = tx.run(DeleteEdgesOfCanvases { canvases: &ids }).await;
        if let Err(e) = deleted {
            return base::db::rollback(tx, e).await;
        }
        let (tx, deleted) = tx.run(DeleteCanvases { ids: &ids }).await;
        match deleted {
            Ok(()) => base::db::commit(tx, ()).await,
            Err(e) => base::db::rollback(tx, e).await,
        }
    }

    fn finished(self) -> Either<Self, TxEnd<Self>> {
        match self {
            DeleteCanvasRowFlow::End(end) => Either::Right(end),
            state => Either::Left(state),
        }
    }
}
