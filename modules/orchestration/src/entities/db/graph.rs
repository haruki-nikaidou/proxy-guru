//! The pod graph of one canvas tree, read and written as a whole.
//!
//! Canvases nest by `parent`; a tree is the unit edges may cross, the unit a
//! batch is checked against, and the unit derivation compiles.

use crate::entities::db::canvas::{
    CanvasEntity, CanvasFence, CanvasId, CanvasUiPosition, SNAPSHOT_READ,
};
use crate::entities::db::edge::{EdgeEntity, EdgeId, insert_edge, update_edge};
use crate::entities::db::exit::{ExitEntity, ExitId, insert_exit, update_exit};
use crate::entities::db::fence;
use crate::entities::db::group::{
    GroupEntity, GroupId, groups_of_canvases, insert_group, update_group,
};
use crate::entities::db::pod::{PodEntity, PodId, insert_pod, update_pod};
use crate::entities::db::server::ServerEntity;
use crate::entities::db::tree;
use base::db::{Db, Error};
use kanau::processor::Processor;
use sqlx::PgConnection;

/// Everything the graph of one canvas tree is made of.
#[derive(Debug, Clone)]
pub struct GraphRows {
    pub root: CanvasId,
    /// Every canvas of the tree, root first, then by depth and id.
    pub canvases: Vec<CanvasEntity>,
    /// The servers of every canvas of the tree, by id.
    pub servers: Vec<ServerEntity>,
    pub pods: Vec<PodEntity>,
    pub exits: Vec<ExitEntity>,
    /// Every edge leaving a pod of the tree, by id.
    pub edges: Vec<EdgeEntity>,
    pub groups: Vec<GroupEntity>,
}

impl GraphRows {
    pub fn canvas_ids(&self) -> Vec<CanvasId> {
        self.canvases.iter().map(|c| c.id.clone()).collect()
    }

    /// The fence a write validated against this snapshot must pass. `None` when
    /// the snapshot carries no root row (a canvas that does not exist), so a
    /// validated caller fails closed rather than bumping unconditionally.
    pub fn fence(&self) -> Option<CanvasFence> {
        self.canvases
            .iter()
            .find(|c| c.id == self.root)
            .map(|c| CanvasFence {
                root: self.root.clone(),
                generation: c.generation,
            })
    }

    /// The root's edit counter as this snapshot read it.
    pub fn generation(&self) -> i64 {
        self.canvases
            .iter()
            .find(|c| c.id == self.root)
            .map_or(0, |c| c.generation)
    }
}

/// Reads the graph of the tree containing `canvas` inside the caller's
/// transaction. A canvas that does not exist yields an empty graph whose root
/// is the given id.
pub(crate) async fn load_graph(
    conn: &mut PgConnection,
    canvas: &CanvasId,
) -> Result<GraphRows, Error> {
    let (root, tree) = tree::whole_tree_of(&mut *conn, canvas).await?;
    let mut canvases: Vec<CanvasEntity> =
        sqlx::query_as("SELECT * FROM orchestration_canvas WHERE id = ANY($1)")
            .bind(&tree)
            .fetch_all(&mut *conn)
            .await?;
    canvases.sort_by_key(|c| tree.iter().position(|id| *id == c.id));
    let servers: Vec<ServerEntity> =
        sqlx::query_as("SELECT * FROM orchestration_server WHERE canvas = ANY($1) ORDER BY id")
            .bind(&tree)
            .fetch_all(&mut *conn)
            .await?;
    let pods: Vec<PodEntity> =
        sqlx::query_as("SELECT * FROM orchestration_pod WHERE canvas = ANY($1) ORDER BY id")
            .bind(&tree)
            .fetch_all(&mut *conn)
            .await?;
    let exits: Vec<ExitEntity> =
        sqlx::query_as("SELECT * FROM orchestration_exit WHERE canvas = ANY($1) ORDER BY id")
            .bind(&tree)
            .fetch_all(&mut *conn)
            .await?;
    let edges: Vec<EdgeEntity> = sqlx::query_as(
        "SELECT e.* FROM orchestration_edge e
         JOIN orchestration_pod p ON p.id = e.source_pod
         WHERE p.canvas = ANY($1) ORDER BY e.id",
    )
    .bind(&tree)
    .fetch_all(&mut *conn)
    .await?;
    let groups = groups_of_canvases(conn, &tree).await?;
    Ok(GraphRows {
        root,
        canvases,
        servers,
        pods,
        exits,
        edges,
        groups,
    })
}

/// The graph of the tree containing `canvas`, in one snapshot.
pub struct LoadCanvasGraph {
    pub canvas: CanvasId,
}

impl Processor<LoadCanvasGraph> for Db {
    type Output = GraphRows;
    type Error = Error;
    #[tracing::instrument(name = "Query-Transaction:LoadCanvasGraph", skip_all, err)]
    async fn process(&self, input: LoadCanvasGraph) -> Result<Self::Output, Self::Error> {
        let mut tx = self.db().begin_with(SNAPSHOT_READ).await?;
        let rows = load_graph(&mut tx, &input.canvas).await?;
        tx.commit().await?;
        Ok(rows)
    }
}

/// Which ids exist anywhere, so a batch can tell a new row from one that
/// belongs to another tree.
#[derive(Debug, Clone, Default)]
pub struct TakenIds {
    pub pods: Vec<PodId>,
    pub exits: Vec<ExitId>,
    pub edges: Vec<EdgeId>,
    pub groups: Vec<GroupId>,
}

/// The ids among the given ones that already name a row.
pub struct FindTakenIds {
    pub pods: Vec<PodId>,
    pub exits: Vec<ExitId>,
    pub edges: Vec<EdgeId>,
    pub groups: Vec<GroupId>,
}

impl Processor<FindTakenIds> for Db {
    type Output = TakenIds;
    type Error = Error;
    #[tracing::instrument(name = "Query:FindTakenIds", skip_all, err)]
    async fn process(&self, input: FindTakenIds) -> Result<Self::Output, Self::Error> {
        let mut conn = self.db().acquire().await?;
        Ok(TakenIds {
            pods: sqlx::query_scalar("SELECT id FROM orchestration_pod WHERE id = ANY($1)")
                .bind(&input.pods)
                .fetch_all(&mut *conn)
                .await?,
            exits: sqlx::query_scalar("SELECT id FROM orchestration_exit WHERE id = ANY($1)")
                .bind(&input.exits)
                .fetch_all(&mut *conn)
                .await?,
            edges: sqlx::query_scalar("SELECT id FROM orchestration_edge WHERE id = ANY($1)")
                .bind(&input.edges)
                .fetch_all(&mut *conn)
                .await?,
            groups: sqlx::query_scalar("SELECT id FROM orchestration_group WHERE id = ANY($1)")
                .bind(&input.groups)
                .fetch_all(&mut *conn)
                .await?,
        })
    }
}

/// One checked batch of graph changes, written in one fenced transaction.
///
/// Applied in the order the foreign keys need: groups and edges go first, then
/// pods and exits; new and changed pods and exits are written before the edges
/// that lead to them, and groups last, when every member exists.
#[derive(Debug, Clone, Default)]
pub struct ApplyGraphBatch {
    /// Any canvas of the tree; its root is fenced and bumped.
    pub canvas: Option<CanvasId>,
    /// The snapshot the batch was checked against. `None` bumps unconditionally.
    pub fence: Option<CanvasFence>,
    /// Whether the batch touches pods, exits or edges, and so is fenced and
    /// bumps the generation; a batch of groups alone is neither.
    pub derives: bool,
    pub delete_groups: Vec<GroupId>,
    pub delete_edges: Vec<EdgeId>,
    pub delete_pods: Vec<PodId>,
    pub delete_exits: Vec<ExitId>,
    pub insert_pods: Vec<PodEntity>,
    pub update_pods: Vec<PodEntity>,
    pub insert_exits: Vec<ExitEntity>,
    pub update_exits: Vec<ExitEntity>,
    pub insert_edges: Vec<EdgeEntity>,
    pub update_edges: Vec<EdgeEntity>,
    pub insert_groups: Vec<GroupEntity>,
    pub update_groups: Vec<GroupEntity>,
}

/// A batch whose tree is not named: a caller bug.
pub const BATCH_WITHOUT_CANVAS: &str = "graph batch without a canvas";

impl Processor<ApplyGraphBatch> for Db {
    /// The root generation after the write.
    type Output = i64;
    type Error = Error;
    #[tracing::instrument(
        name = "Query-Transaction:ApplyGraphBatch",
        skip_all,
        err,
        fields(
            delete_edges = input.delete_edges.len(),
            delete_pods = input.delete_pods.len(),
            insert_pods = input.insert_pods.len(),
            update_pods = input.update_pods.len(),
            insert_edges = input.insert_edges.len(),
        )
    )]
    async fn process(&self, input: ApplyGraphBatch) -> Result<Self::Output, Self::Error> {
        let canvas = input.canvas.ok_or(Error::Conflict(BATCH_WITHOUT_CANVAS))?;
        let mut tx = self.db().begin().await?;
        // The fence first: a batch that lost the race writes nothing at all. A
        // batch of groups alone changes nothing a worker runs and takes no fence:
        // the dashboard's drawing is last-write-wins. It still takes the root
        // before its first row (`fence`'s lock order), since its membership rows
        // reference servers a delete may be holding.
        if input.derives {
            fence::touch_checked(&mut tx, &canvas, input.fence.as_ref()).await?;
        } else {
            fence::lock_root(&mut tx, &canvas).await?;
        }
        sqlx::query("DELETE FROM orchestration_group WHERE id = ANY($1)")
            .bind(&input.delete_groups)
            .execute(&mut *tx)
            .await?;
        sqlx::query("DELETE FROM orchestration_edge WHERE id = ANY($1)")
            .bind(&input.delete_edges)
            .execute(&mut *tx)
            .await?;
        // Health history and relay leaves cascade from the pod.
        sqlx::query("DELETE FROM orchestration_pod WHERE id = ANY($1)")
            .bind(&input.delete_pods)
            .execute(&mut *tx)
            .await?;
        sqlx::query("DELETE FROM orchestration_exit WHERE id = ANY($1)")
            .bind(&input.delete_exits)
            .execute(&mut *tx)
            .await?;
        for pod in &input.insert_pods {
            insert_pod(&mut tx, pod).await?;
        }
        for pod in &input.update_pods {
            update_pod(&mut tx, pod).await?;
        }
        for exit in &input.insert_exits {
            insert_exit(&mut tx, exit).await?;
        }
        for exit in &input.update_exits {
            update_exit(&mut tx, exit).await?;
        }
        for edge in &input.insert_edges {
            insert_edge(&mut tx, edge).await?;
        }
        for edge in &input.update_edges {
            update_edge(&mut tx, edge).await?;
        }
        for group in &input.insert_groups {
            insert_group(&mut tx, group).await?;
        }
        for group in &input.update_groups {
            update_group(&mut tx, group).await?;
        }
        let root = tree::root_of(&mut tx, &canvas).await?;
        let generation: i64 =
            sqlx::query_scalar("SELECT generation FROM orchestration_canvas WHERE id = $1")
                .bind(&root)
                .fetch_one(&mut *tx)
                .await?;
        tx.commit().await?;
        Ok(generation)
    }
}

/// That the node model was converted into the pod graph, and what the
/// conversion found.
#[derive(Debug, Clone, sqlx::FromRow)]
pub struct GraphStateEntity {
    pub converted_at: chrono::DateTime<chrono::Utc>,
    #[sqlx(json)]
    pub report: serde_json::Value,
}

#[derive(Debug)]
pub struct FindGraphState;

impl Processor<FindGraphState> for Db {
    type Output = Option<GraphStateEntity>;
    type Error = Error;
    #[tracing::instrument(name = "Query:FindGraphState", skip_all, err)]
    async fn process(&self, _: FindGraphState) -> Result<Self::Output, Self::Error> {
        Ok(sqlx::query_as(
            "SELECT converted_at, report FROM orchestration_graph_state WHERE id = 'current'",
        )
        .fetch_optional(self.db())
        .await?)
    }
}

/// Positions only: nothing a worker reads, so no fence and no bump. Rows that
/// are not on the given tree are left alone. The tree's root is still locked
/// first (`fence`'s lock order): the server and subcanvas rows a move writes are
/// rows that edits and deletes lock after the root.
#[derive(Debug, Default)]
pub struct MoveGraphItems {
    /// The canvases of the tree the items must belong to.
    pub tree: Vec<CanvasId>,
    pub servers: Vec<(crate::entities::db::server::ServerId, CanvasUiPosition)>,
    pub exits: Vec<(ExitId, CanvasUiPosition)>,
    pub canvases: Vec<(CanvasId, CanvasUiPosition)>,
}

impl Processor<MoveGraphItems> for Db {
    type Output = ();
    type Error = Error;
    #[tracing::instrument(name = "Query-Transaction:MoveGraphItems", skip_all, err)]
    async fn process(&self, input: MoveGraphItems) -> Result<Self::Output, Self::Error> {
        let mut tx = self.db().begin().await?;
        if let Some(canvas) = input.tree.first() {
            fence::lock_root(&mut tx, canvas).await?;
        }
        for (server, position) in &input.servers {
            sqlx::query(
                "UPDATE orchestration_server SET position_x = $2, position_y = $3
                 WHERE id = $1 AND canvas = ANY($4)",
            )
            .bind(server)
            .bind(position.x)
            .bind(position.y)
            .bind(&input.tree)
            .execute(&mut *tx)
            .await?;
        }
        for (exit, position) in &input.exits {
            sqlx::query(
                "UPDATE orchestration_exit SET position_x = $2, position_y = $3
                 WHERE id = $1 AND canvas = ANY($4)",
            )
            .bind(exit)
            .bind(position.x)
            .bind(position.y)
            .bind(&input.tree)
            .execute(&mut *tx)
            .await?;
        }
        for (canvas, position) in &input.canvases {
            sqlx::query(
                "UPDATE orchestration_canvas SET position_x = $2, position_y = $3
                 WHERE id = $1 AND id = ANY($4)",
            )
            .bind(canvas)
            .bind(position.x)
            .bind(position.y)
            .bind(&input.tree)
            .execute(&mut *tx)
            .await?;
        }
        tx.commit().await?;
        Ok(())
    }
}
