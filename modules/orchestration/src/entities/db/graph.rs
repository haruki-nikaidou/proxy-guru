//! The pod graph of one canvas tree, read and written as a whole.
//!
//! Canvases nest by `parent`; a tree is the unit edges may cross, the unit a
//! batch is checked against, and the unit derivation compiles.

use crate::entities::db::canvas::{
    CanvasEntity, CanvasFence, CanvasId, CanvasUiPosition, CanvasesByIds,
};
use crate::entities::db::dns::DnsProviderId;
use crate::entities::db::edge::{EdgeEntity, EdgeId, EdgeRow, InsertEdge, IpFamily, UpdateEdge};
use crate::entities::db::exit::{ExitEntity, ExitId, ExitRow, InsertExit, UpdateExit};
use crate::entities::db::fence::{LockRoot, TouchChecked};
use crate::entities::db::group::{
    GroupEntity, GroupId, GroupsOfCanvases, InsertGroup, UpdateGroup,
};
use crate::entities::db::health::ServerHealthStatus;
use crate::entities::db::pod::{
    IngressKind, InsertPod, PodEntity, PodId, PodRow, ProxyProtocolVersion, UpdatePod,
};
use crate::entities::db::server::{
    ReportedAddresses, ServerEntity, ServerId, ServerIpv6Resolve, ServerLogLevel, ServerQuic,
    ServerRow,
};
use crate::entities::db::tables::{
    OrchestrationCanvas, OrchestrationEdge, OrchestrationExit, OrchestrationGroup,
    OrchestrationGroupMember, OrchestrationPod, OrchestrationServer,
};
use crate::entities::db::tree::{RootOf, WholeTreeOf};
use base::db::Error;
use guru_topology::Route;
use sqlx::types::Json;
use sqlx::{PgConnection, Postgres};
use wakuwaku::entity::sqlx::backend::PgSource;
use wakuwaku::entity::sqlx::transaction::{Either, IsolationLevel, Tx, TxEnd, TxFlow, TxState};
use wakuwaku::entity::{Execute, Query, Read, Write};

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

/// The tables [`LoadGraph`] reads.
pub(crate) type GraphEffect = (
    Read<OrchestrationCanvas>,
    Read<OrchestrationServer>,
    Read<OrchestrationPod>,
    Read<OrchestrationExit>,
    Read<OrchestrationEdge>,
    Read<OrchestrationGroup>,
    Read<OrchestrationGroupMember>,
);

/// Reads the graph of the tree containing `canvas` inside the caller's
/// transaction. A canvas that does not exist yields an empty graph whose root
/// is the given id.
pub(crate) struct LoadGraph<'a> {
    pub(crate) canvas: &'a CanvasId,
}

impl Query for LoadGraph<'_> {
    type Effect = GraphEffect;
}

impl Execute<PgSource> for LoadGraph<'_> {
    type Output = GraphRows;
    type Error = Error;
    async fn execute(self, conn: &mut PgConnection) -> Result<Self::Output, Self::Error> {
        let (root, tree) = WholeTreeOf {
            canvas: self.canvas,
        }
        .execute(&mut *conn)
        .await?;
        let mut canvases = CanvasesByIds { ids: &tree }.execute(&mut *conn).await?;
        canvases.sort_by_key(|c| tree.iter().position(|id| *id == c.id));
        let servers: Vec<ServerEntity> =
            sqlx::query_file_as!(ServerRow, "sql/load_graph_servers.sql", &tree as _)
                .fetch_all(&mut *conn)
                .await?
                .into_iter()
                .map(ServerEntity::from)
                .collect();
        let pods: Vec<PodEntity> =
            sqlx::query_file_as!(PodRow, "sql/load_graph_pods.sql", &tree as _)
                .fetch_all(&mut *conn)
                .await?
                .into_iter()
                .map(PodEntity::try_from)
                .collect::<Result<_, sqlx::Error>>()?;
        let exits: Vec<ExitEntity> = sqlx::query_as!(
            ExitRow,
            r#"SELECT id AS "id: ExitId", canvas AS "canvas: CanvasId", name, comment, destination,
                      send_proxy_protocol AS "send_proxy_protocol: ProxyProtocolVersion",
                      position_x, position_y
               FROM orchestration_exit WHERE canvas = ANY($1) ORDER BY id"#,
            &tree as _
        )
        .fetch_all(&mut *conn)
        .await?
        .into_iter()
        .map(ExitEntity::from)
        .collect();
        let edges: Vec<EdgeEntity> =
            sqlx::query_file_as!(EdgeRow, "sql/load_graph_edges.sql", &tree as _)
                .fetch_all(&mut *conn)
                .await?
                .into_iter()
                .map(EdgeEntity::try_from)
                .collect::<Result<_, sqlx::Error>>()?;
        let groups = GroupsOfCanvases { canvases: &tree }.execute(conn).await?;
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
}

/// The graph of the tree containing `canvas`, in one snapshot: a read-only
/// `REPEATABLE READ` transaction.
pub struct LoadCanvasGraph {
    pub canvas: CanvasId,
}

/// Reads the graph of [`LoadCanvasGraph`] in one snapshot. Run it with
/// `db.process(Transaction(LoadCanvasGraphFlow::new(input)))`.
pub enum LoadCanvasGraphFlow {
    Start(LoadCanvasGraph),
    End(TxEnd<LoadCanvasGraphFlow>),
}

impl LoadCanvasGraphFlow {
    pub fn new(input: LoadCanvasGraph) -> Self {
        LoadCanvasGraphFlow::Start(input)
    }
}

impl Query for LoadCanvasGraphFlow {
    type Effect = GraphEffect;
}

impl From<TxEnd<LoadCanvasGraphFlow>> for LoadCanvasGraphFlow {
    fn from(end: TxEnd<LoadCanvasGraphFlow>) -> Self {
        LoadCanvasGraphFlow::End(end)
    }
}

impl TxFlow for LoadCanvasGraphFlow {
    type Database = Postgres;
    type Commit = GraphRows;
    type Abort = Error;
    type Report = ();
    const ISOLATION: Option<IsolationLevel> = Some(IsolationLevel::RepeatableRead);

    #[tracing::instrument(name = "Transaction:LoadCanvasGraph", skip_all)]
    async fn step(tx: Tx<'_, Self>, state: Self) -> TxState<'_, Self> {
        let LoadCanvasGraphFlow::Start(input) = state else {
            return tx.end(state).await;
        };
        let (tx, rows) = tx
            .run(LoadGraph {
                canvas: &input.canvas,
            })
            .await;
        match rows {
            Ok(rows) => base::db::commit(tx, rows).await,
            Err(e) => base::db::rollback(tx, e).await,
        }
    }

    fn finished(self) -> Either<Self, TxEnd<Self>> {
        match self {
            LoadCanvasGraphFlow::End(end) => Either::Right(end),
            state => Either::Left(state),
        }
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

impl Query for FindTakenIds {
    type Effect = (
        Read<OrchestrationPod>,
        Read<OrchestrationExit>,
        Read<OrchestrationEdge>,
        Read<OrchestrationGroup>,
    );
}

impl Execute<PgSource> for FindTakenIds {
    type Output = TakenIds;
    type Error = Error;
    #[tracing::instrument(name = "Query:FindTakenIds", skip_all, err)]
    async fn execute(self, conn: &mut PgConnection) -> Result<Self::Output, Self::Error> {
        Ok(TakenIds {
            pods: sqlx::query_scalar!(
                r#"SELECT id AS "id: PodId" FROM orchestration_pod WHERE id = ANY($1)"#,
                &self.pods as _
            )
            .fetch_all(&mut *conn)
            .await?,
            exits: sqlx::query_scalar!(
                r#"SELECT id AS "id: ExitId" FROM orchestration_exit WHERE id = ANY($1)"#,
                &self.exits as _
            )
            .fetch_all(&mut *conn)
            .await?,
            edges: sqlx::query_scalar!(
                r#"SELECT id AS "id: EdgeId" FROM orchestration_edge WHERE id = ANY($1)"#,
                &self.edges as _
            )
            .fetch_all(&mut *conn)
            .await?,
            groups: sqlx::query_scalar!(
                r#"SELECT id AS "id: GroupId" FROM orchestration_group WHERE id = ANY($1)"#,
                &self.groups as _
            )
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

struct DeleteGroups<'a> {
    ids: &'a [GroupId],
}

impl Query for DeleteGroups<'_> {
    type Effect = Write<OrchestrationGroup>;
}

impl Execute<PgSource> for DeleteGroups<'_> {
    type Output = ();
    type Error = Error;
    async fn execute(self, conn: &mut PgConnection) -> Result<(), Error> {
        sqlx::query!(
            "DELETE FROM orchestration_group WHERE id = ANY($1)",
            self.ids as _
        )
        .execute(conn)
        .await?;
        Ok(())
    }
}

struct DeleteEdges<'a> {
    ids: &'a [EdgeId],
}

impl Query for DeleteEdges<'_> {
    type Effect = Write<OrchestrationEdge>;
}

impl Execute<PgSource> for DeleteEdges<'_> {
    type Output = ();
    type Error = Error;
    async fn execute(self, conn: &mut PgConnection) -> Result<(), Error> {
        sqlx::query!(
            "DELETE FROM orchestration_edge WHERE id = ANY($1)",
            self.ids as _
        )
        .execute(conn)
        .await?;
        Ok(())
    }
}

/// Health history and relay leaves cascade from the pod.
struct DeletePods<'a> {
    ids: &'a [PodId],
}

impl Query for DeletePods<'_> {
    type Effect = Write<OrchestrationPod>;
}

impl Execute<PgSource> for DeletePods<'_> {
    type Output = ();
    type Error = Error;
    async fn execute(self, conn: &mut PgConnection) -> Result<(), Error> {
        sqlx::query!(
            "DELETE FROM orchestration_pod WHERE id = ANY($1)",
            self.ids as _
        )
        .execute(conn)
        .await?;
        Ok(())
    }
}

struct DeleteExits<'a> {
    ids: &'a [ExitId],
}

impl Query for DeleteExits<'_> {
    type Effect = Write<OrchestrationExit>;
}

impl Execute<PgSource> for DeleteExits<'_> {
    type Output = ();
    type Error = Error;
    async fn execute(self, conn: &mut PgConnection) -> Result<(), Error> {
        sqlx::query!(
            "DELETE FROM orchestration_exit WHERE id = ANY($1)",
            self.ids as _
        )
        .execute(conn)
        .await?;
        Ok(())
    }
}

/// The root generation of the tree containing `canvas`.
struct RootGeneration<'a> {
    canvas: &'a CanvasId,
}

impl Query for RootGeneration<'_> {
    type Effect = Read<OrchestrationCanvas>;
}

impl Execute<PgSource> for RootGeneration<'_> {
    type Output = i64;
    type Error = Error;
    async fn execute(self, conn: &mut PgConnection) -> Result<i64, Error> {
        let root = RootOf {
            canvas: self.canvas,
        }
        .execute(&mut *conn)
        .await?;
        Ok(sqlx::query_scalar!(
            "SELECT generation FROM orchestration_canvas WHERE id = $1",
            root as _
        )
        .fetch_one(conn)
        .await?)
    }
}

/// Writes an [`ApplyGraphBatch`] under the tree's fence; commits the root
/// generation after the write. Run it with
/// `db.process(Transaction(ApplyGraphBatchFlow::new(input)?))`.
// One state plus its end: `Start` carries the whole batch (~376 B) and the
// end only the generation. The value is moved a handful of times per
// transaction; boxing would add a heap allocation to save no memory.
#[allow(clippy::large_enum_variant)]
pub enum ApplyGraphBatchFlow {
    /// The batch, and the canvas it names (checked before `BEGIN`).
    Start(CanvasId, ApplyGraphBatch),
    End(TxEnd<ApplyGraphBatchFlow>),
}

impl ApplyGraphBatchFlow {
    /// Fails with [`BATCH_WITHOUT_CANVAS`] when the batch names no canvas
    /// (before `BEGIN`).
    pub fn new(mut input: ApplyGraphBatch) -> Result<Self, Error> {
        let Some(canvas) = input.canvas.take() else {
            return Err(Error::Conflict(BATCH_WITHOUT_CANVAS));
        };
        Ok(ApplyGraphBatchFlow::Start(canvas, input))
    }
}

impl Query for ApplyGraphBatchFlow {
    type Effect = (
        Write<OrchestrationCanvas>,
        Write<OrchestrationGroup>,
        Write<OrchestrationGroupMember>,
        Write<OrchestrationEdge>,
        Write<OrchestrationPod>,
        Write<OrchestrationExit>,
    );
}

impl From<TxEnd<ApplyGraphBatchFlow>> for ApplyGraphBatchFlow {
    fn from(end: TxEnd<ApplyGraphBatchFlow>) -> Self {
        ApplyGraphBatchFlow::End(end)
    }
}

impl TxFlow for ApplyGraphBatchFlow {
    type Database = Postgres;
    type Commit = i64;
    type Abort = Error;
    type Report = ();

    #[tracing::instrument(name = "Transaction:ApplyGraphBatch", skip_all)]
    async fn step(tx: Tx<'_, Self>, state: Self) -> TxState<'_, Self> {
        let ApplyGraphBatchFlow::Start(canvas, input) = state else {
            return tx.end(state).await;
        };
        let canvas = &canvas;
        // The fence first: a batch that lost the race writes nothing at all. A
        // batch of groups alone changes nothing a worker runs and takes no fence:
        // the dashboard's drawing is last-write-wins. It still takes the root
        // before its first row (`fence`'s lock order), since its membership rows
        // reference servers a delete may be holding.
        let (tx, fenced) = if input.derives {
            tx.run(TouchChecked {
                canvas,
                fence: input.fence.as_ref(),
            })
            .await
        } else {
            tx.run(LockRoot { canvas }).await
        };
        if let Err(e) = fenced {
            return base::db::rollback(tx, e).await;
        }
        let (tx, r) = tx
            .run(DeleteGroups {
                ids: &input.delete_groups,
            })
            .await;
        if let Err(e) = r {
            return base::db::rollback(tx, e).await;
        }
        let (tx, r) = tx
            .run(DeleteEdges {
                ids: &input.delete_edges,
            })
            .await;
        if let Err(e) = r {
            return base::db::rollback(tx, e).await;
        }
        let (tx, r) = tx
            .run(DeletePods {
                ids: &input.delete_pods,
            })
            .await;
        if let Err(e) = r {
            return base::db::rollback(tx, e).await;
        }
        let (mut tx, r) = tx
            .run(DeleteExits {
                ids: &input.delete_exits,
            })
            .await;
        if let Err(e) = r {
            return base::db::rollback(tx, e).await;
        }
        for pod in &input.insert_pods {
            let (next, r) = tx.run(InsertPod { pod }).await;
            tx = next;
            if let Err(e) = r {
                return base::db::rollback(tx, e).await;
            }
        }
        for pod in &input.update_pods {
            let (next, r) = tx.run(UpdatePod { pod }).await;
            tx = next;
            if let Err(e) = r {
                return base::db::rollback(tx, e).await;
            }
        }
        for exit in &input.insert_exits {
            let (next, r) = tx.run(InsertExit { exit }).await;
            tx = next;
            if let Err(e) = r {
                return base::db::rollback(tx, e).await;
            }
        }
        for exit in &input.update_exits {
            let (next, r) = tx.run(UpdateExit { exit }).await;
            tx = next;
            if let Err(e) = r {
                return base::db::rollback(tx, e).await;
            }
        }
        for edge in &input.insert_edges {
            let (next, r) = tx.run(InsertEdge { edge }).await;
            tx = next;
            if let Err(e) = r {
                return base::db::rollback(tx, e).await;
            }
        }
        for edge in &input.update_edges {
            let (next, r) = tx.run(UpdateEdge { edge }).await;
            tx = next;
            if let Err(e) = r {
                return base::db::rollback(tx, e).await;
            }
        }
        for group in &input.insert_groups {
            let (next, r) = tx.run(InsertGroup { group }).await;
            tx = next;
            if let Err(e) = r {
                return base::db::rollback(tx, e).await;
            }
        }
        for group in &input.update_groups {
            let (next, r) = tx.run(UpdateGroup { group }).await;
            tx = next;
            if let Err(e) = r {
                return base::db::rollback(tx, e).await;
            }
        }
        let (tx, generation) = tx.run(RootGeneration { canvas }).await;
        match generation {
            Ok(generation) => base::db::commit(tx, generation).await,
            Err(e) => base::db::rollback(tx, e).await,
        }
    }

    fn finished(self) -> Either<Self, TxEnd<Self>> {
        match self {
            ApplyGraphBatchFlow::End(end) => Either::Right(end),
            state => Either::Left(state),
        }
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
    pub servers: Vec<(ServerId, CanvasUiPosition)>,
    pub exits: Vec<(ExitId, CanvasUiPosition)>,
    pub canvases: Vec<(CanvasId, CanvasUiPosition)>,
}

struct MoveServer<'a> {
    server: &'a ServerId,
    position: CanvasUiPosition,
    tree: &'a [CanvasId],
}

impl Query for MoveServer<'_> {
    type Effect = Write<OrchestrationServer>;
}

impl Execute<PgSource> for MoveServer<'_> {
    type Output = ();
    type Error = Error;
    async fn execute(self, conn: &mut PgConnection) -> Result<(), Error> {
        sqlx::query!(
            "UPDATE orchestration_server SET position_x = $2, position_y = $3
             WHERE id = $1 AND canvas = ANY($4)",
            self.server as _,
            self.position.x,
            self.position.y,
            self.tree as _
        )
        .execute(conn)
        .await?;
        Ok(())
    }
}

struct MoveExit<'a> {
    exit: &'a ExitId,
    position: CanvasUiPosition,
    tree: &'a [CanvasId],
}

impl Query for MoveExit<'_> {
    type Effect = Write<OrchestrationExit>;
}

impl Execute<PgSource> for MoveExit<'_> {
    type Output = ();
    type Error = Error;
    async fn execute(self, conn: &mut PgConnection) -> Result<(), Error> {
        sqlx::query!(
            "UPDATE orchestration_exit SET position_x = $2, position_y = $3
             WHERE id = $1 AND canvas = ANY($4)",
            self.exit as _,
            self.position.x,
            self.position.y,
            self.tree as _
        )
        .execute(conn)
        .await?;
        Ok(())
    }
}

struct MoveCanvas<'a> {
    canvas: &'a CanvasId,
    position: CanvasUiPosition,
    tree: &'a [CanvasId],
}

impl Query for MoveCanvas<'_> {
    type Effect = Write<OrchestrationCanvas>;
}

impl Execute<PgSource> for MoveCanvas<'_> {
    type Output = ();
    type Error = Error;
    async fn execute(self, conn: &mut PgConnection) -> Result<(), Error> {
        sqlx::query!(
            "UPDATE orchestration_canvas SET position_x = $2, position_y = $3
             WHERE id = $1 AND id = ANY($4)",
            self.canvas as _,
            self.position.x,
            self.position.y,
            self.tree as _
        )
        .execute(conn)
        .await?;
        Ok(())
    }
}

/// Moves the items of [`MoveGraphItems`] under the tree's root lock. Run it
/// with `db.process(Transaction(MoveGraphItemsFlow::new(input)))`.
pub enum MoveGraphItemsFlow {
    Start(MoveGraphItems),
    End(TxEnd<MoveGraphItemsFlow>),
}

impl MoveGraphItemsFlow {
    pub fn new(input: MoveGraphItems) -> Self {
        MoveGraphItemsFlow::Start(input)
    }
}

impl Query for MoveGraphItemsFlow {
    type Effect = (
        Write<OrchestrationCanvas>,
        Write<OrchestrationServer>,
        Write<OrchestrationExit>,
    );
}

impl From<TxEnd<MoveGraphItemsFlow>> for MoveGraphItemsFlow {
    fn from(end: TxEnd<MoveGraphItemsFlow>) -> Self {
        MoveGraphItemsFlow::End(end)
    }
}

impl TxFlow for MoveGraphItemsFlow {
    type Database = Postgres;
    type Commit = ();
    type Abort = Error;
    type Report = ();

    #[tracing::instrument(name = "Transaction:MoveGraphItems", skip_all)]
    async fn step(tx: Tx<'_, Self>, state: Self) -> TxState<'_, Self> {
        let MoveGraphItemsFlow::Start(input) = state else {
            return tx.end(state).await;
        };
        let mut tx = tx;
        if let Some(canvas) = input.tree.first() {
            let (next, r) = tx.run(LockRoot { canvas }).await;
            tx = next;
            if let Err(e) = r {
                return base::db::rollback(tx, e).await;
            }
        }
        for (server, position) in &input.servers {
            let (next, r) = tx
                .run(MoveServer {
                    server,
                    position: *position,
                    tree: &input.tree,
                })
                .await;
            tx = next;
            if let Err(e) = r {
                return base::db::rollback(tx, e).await;
            }
        }
        for (exit, position) in &input.exits {
            let (next, r) = tx
                .run(MoveExit {
                    exit,
                    position: *position,
                    tree: &input.tree,
                })
                .await;
            tx = next;
            if let Err(e) = r {
                return base::db::rollback(tx, e).await;
            }
        }
        for (canvas, position) in &input.canvases {
            let (next, r) = tx
                .run(MoveCanvas {
                    canvas,
                    position: *position,
                    tree: &input.tree,
                })
                .await;
            tx = next;
            if let Err(e) = r {
                return base::db::rollback(tx, e).await;
            }
        }
        base::db::commit(tx, ()).await
    }

    fn finished(self) -> Either<Self, TxEnd<Self>> {
        match self {
            MoveGraphItemsFlow::End(end) => Either::Right(end),
            state => Either::Left(state),
        }
    }
}
