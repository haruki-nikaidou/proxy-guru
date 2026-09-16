//! The pod graph of one canvas tree, read as a whole.
//!
//! Canvases nest by `parent`; a tree is the unit edges may cross, the unit a
//! batch is checked against, and the unit derivation compiles.

use crate::entities::db::canvas::{CanvasEntity, CanvasId, CanvasUiPosition, SNAPSHOT_READ};
use crate::entities::db::edge::{EdgeEntity, insert_edge};
use crate::entities::db::exit::{ExitEntity, insert_exit};
use crate::entities::db::group::{GroupEntity, groups_of_canvases, insert_group};
use crate::entities::db::pod::{PodEntity, insert_pod};
use crate::entities::db::server::ServerEntity;
use base::db::{Db, Error};
use chrono::{DateTime, Utc};
use kanau::processor::Processor;
use sqlx::PgConnection;
use sqlx::types::Json;

/// Deeper than this and the tree is refused rather than walked further; the
/// parent chain cannot cycle through writes the service accepts, so hitting the
/// cap means corrupt data, not a big tree.
const MAX_DEPTH: i32 = 32;
pub const NESTING_TOO_DEEP: &str = "canvas nesting deeper than 32 levels";

/// The conflict reported when the node model was converted before.
pub const ALREADY_CONVERTED: &str = "the node model was already converted";

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

/// The root of the tree `canvas` belongs to and every canvas of that tree, root
/// first; by `parent`. Empty when the canvas does not exist.
pub(crate) async fn parent_tree_of(
    conn: &mut PgConnection,
    canvas: &CanvasId,
) -> Result<(CanvasId, Vec<CanvasId>), Error> {
    let up: Vec<(CanvasId, i32)> = sqlx::query_as(
        "WITH RECURSIVE up (canvas, parent, depth) AS (
             SELECT id, parent, 0 FROM orchestration_canvas WHERE id = $1
           UNION ALL
             SELECT c.id, c.parent, up.depth + 1
             FROM up JOIN orchestration_canvas c ON c.id = up.parent
             WHERE up.depth < $2
         )
         SELECT canvas, depth FROM up ORDER BY depth",
    )
    .bind(canvas)
    .bind(MAX_DEPTH)
    .fetch_all(&mut *conn)
    .await?;
    let Some((root, depth)) = up.last().cloned() else {
        return Ok((canvas.clone(), Vec::new()));
    };
    if depth >= MAX_DEPTH {
        return Err(Error::Conflict(NESTING_TOO_DEEP));
    }
    let down: Vec<(CanvasId, i32)> = sqlx::query_as(
        "WITH RECURSIVE down (canvas, depth) AS (
             SELECT $1::text, 0
           UNION ALL
             SELECT c.id, down.depth + 1
             FROM down JOIN orchestration_canvas c ON c.parent = down.canvas
             WHERE down.depth < $2
         )
         SELECT canvas, depth FROM down ORDER BY depth, canvas",
    )
    .bind(&root)
    .bind(MAX_DEPTH)
    .fetch_all(conn)
    .await?;
    if down.last().is_some_and(|(_, depth)| *depth >= MAX_DEPTH) {
        return Err(Error::Conflict(NESTING_TOO_DEEP));
    }
    Ok((root, down.into_iter().map(|(canvas, _)| canvas).collect()))
}

/// Reads the graph of the tree containing `canvas` inside the caller's
/// transaction. A canvas that does not exist yields an empty graph whose root
/// is the given id.
pub(crate) async fn load_graph(
    conn: &mut PgConnection,
    canvas: &CanvasId,
) -> Result<GraphRows, Error> {
    let (root, tree) = parent_tree_of(&mut *conn, canvas).await?;
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

/// That the node model was converted into the pod graph, and what the
/// conversion found.
#[derive(Debug, Clone, sqlx::FromRow)]
pub struct GraphStateEntity {
    pub converted_at: DateTime<Utc>,
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

/// A canvas's place in its tree.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CanvasPlacement {
    pub canvas: CanvasId,
    pub parent: CanvasId,
    pub position: CanvasUiPosition,
}

/// One tree's worth of converted rows.
#[derive(Debug, Clone, Default)]
pub struct ConvertedRows {
    pub placements: Vec<CanvasPlacement>,
    pub pods: Vec<PodEntity>,
    pub exits: Vec<ExitEntity>,
    pub edges: Vec<EdgeEntity>,
    pub groups: Vec<GroupEntity>,
}

/// Writes a conversion of the node model in the caller's transaction: every
/// tree's rows, the pods' health history and the marker. `replace` first clears
/// whatever an earlier conversion wrote; without it an earlier conversion is a
/// [`ALREADY_CONVERTED`] conflict.
pub(crate) async fn write_conversion(
    conn: &mut PgConnection,
    trees: &[ConvertedRows],
    replace: bool,
    report: &serde_json::Value,
) -> Result<(), Error> {
    let converted: Option<String> =
        sqlx::query_scalar("SELECT id FROM orchestration_graph_state WHERE id = 'current'")
            .fetch_optional(&mut *conn)
            .await?;
    if converted.is_some() && !replace {
        return Err(Error::Conflict(ALREADY_CONVERTED));
    }
    if replace {
        for statement in [
            "DELETE FROM orchestration_group",
            "DELETE FROM pod_health_record",
            "DELETE FROM orchestration_edge",
            "DELETE FROM orchestration_pod",
            "DELETE FROM orchestration_exit",
            "UPDATE orchestration_canvas SET parent = NULL, position_x = 0, position_y = 0
             WHERE parent IS NOT NULL",
            "DELETE FROM orchestration_graph_state",
        ] {
            sqlx::query(statement).execute(&mut *conn).await?;
        }
    }
    for tree in trees {
        for placement in &tree.placements {
            sqlx::query(
                "UPDATE orchestration_canvas SET parent = $2, position_x = $3, position_y = $4
                 WHERE id = $1",
            )
            .bind(&placement.canvas)
            .bind(&placement.parent)
            .bind(placement.position.x)
            .bind(placement.position.y)
            .execute(&mut *conn)
            .await?;
        }
        for pod in &tree.pods {
            insert_pod(conn, pod).await?;
        }
        for exit in &tree.exits {
            insert_exit(conn, exit).await?;
        }
        for edge in &tree.edges {
            insert_edge(conn, edge).await?;
        }
        for group in &tree.groups {
            insert_group(conn, group).await?;
        }
    }
    // Pods keep the ids their nodes had, and with them their history.
    sqlx::query(
        "INSERT INTO pod_health_record (id, pod, status, message, report_time)
         SELECT h.id, h.node, h.status, h.message, h.report_time
         FROM node_health_record h JOIN orchestration_pod p ON p.id = h.node",
    )
    .execute(&mut *conn)
    .await?;
    sqlx::query(
        "INSERT INTO orchestration_graph_state (id, converted_at, report)
         VALUES ('current', now(), $1)",
    )
    .bind(Json(report))
    .execute(conn)
    .await?;
    Ok(())
}
