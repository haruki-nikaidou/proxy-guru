use crate::entities::db::canvas::{CanvasFence, CanvasId};
use crate::entities::db::fence;
use crate::entities::db::node::NodeId;
use crate::entities::db::port::PortId;
use base::db::{Db, Error};
use db_types::table_record;
use kanau::processor::Processor;

table_record!(EdgeConnectionId, "orchestration_edge_connection");

/// The conflict reported when an edge is drawn on a port that already carries one.
pub const PORT_ALREADY_CONNECTED: &str = "port already carries an edge";

#[derive(Debug, Clone, sqlx::FromRow)]
pub struct EdgeConnectionEntity {
    pub id: EdgeConnectionId,
    #[sqlx(rename = "source_port")]
    pub source: PortId,
    #[sqlx(rename = "target_port")]
    pub target: PortId,
}

/// Names the unique-constraint violation of either endpoint as [`PORT_ALREADY_CONNECTED`].
pub(crate) fn edge_conflict(error: Error) -> Error {
    error
        .conflict_on(
            "orchestration_edge_connection_source_port_key",
            PORT_ALREADY_CONNECTED,
        )
        .conflict_on(
            "orchestration_edge_connection_target_port_key",
            PORT_ALREADY_CONNECTED,
        )
}

#[derive(Debug)]
pub struct ConnectPorts {
    pub source: PortId,
    pub target: PortId,
    pub canvas: CanvasId,
    /// The snapshot the connect was validated against, fencing the write;
    /// `None` bumps unconditionally (see [`fence::touch_checked`]).
    pub fence: Option<CanvasFence>,
}

impl Processor<ConnectPorts> for Db {
    type Output = EdgeConnectionEntity;
    type Error = Error;
    #[tracing::instrument(name = "Query-Transaction:ConnectPorts", skip_all, err)]
    async fn process(&self, input: ConnectPorts) -> Result<Self::Output, Self::Error> {
        let mut tx = self.db().begin().await?;
        let edge = sqlx::query_as(
            "INSERT INTO orchestration_edge_connection (id, source_port, target_port)
             VALUES ($1, $2, $3) RETURNING *",
        )
        .bind(EdgeConnectionId::new())
        .bind(&input.source)
        .bind(&input.target)
        .fetch_one(&mut *tx)
        .await
        .map_err(|e| edge_conflict(Error::from(e)))?;
        fence::touch_checked(&mut tx, &input.canvas, input.fence.as_ref()).await?;
        tx.commit().await?;
        Ok(edge)
    }
}

#[derive(Debug)]
pub struct FindEdgeById {
    pub id: EdgeConnectionId,
}

impl Processor<FindEdgeById> for Db {
    type Output = Option<EdgeConnectionEntity>;
    type Error = Error;
    #[tracing::instrument(name = "Query:FindEdgeById", skip_all, err)]
    async fn process(&self, input: FindEdgeById) -> Result<Self::Output, Self::Error> {
        Ok(
            sqlx::query_as("SELECT * FROM orchestration_edge_connection WHERE id = $1")
                .bind(input.id)
                .fetch_optional(self.db())
                .await?,
        )
    }
}

#[derive(Debug)]
pub struct DeleteEdgeRow {
    pub id: EdgeConnectionId,
    pub canvas: CanvasId,
    /// The snapshot the disconnect was validated against; `None` bumps
    /// unconditionally (an Admin force delete).
    pub fence: Option<CanvasFence>,
}

impl Processor<DeleteEdgeRow> for Db {
    type Output = ();
    type Error = Error;
    #[tracing::instrument(name = "Query-Transaction:DeleteEdgeRow", skip_all, err)]
    async fn process(&self, input: DeleteEdgeRow) -> Result<Self::Output, Self::Error> {
        let mut tx = self.db().begin().await?;
        sqlx::query("DELETE FROM orchestration_edge_connection WHERE id = $1")
            .bind(&input.id)
            .execute(&mut *tx)
            .await?;
        fence::touch_checked(&mut tx, &input.canvas, input.fence.as_ref()).await?;
        tx.commit().await?;
        Ok(())
    }
}

/// The edge between two ports named by their owning node and port key, rather than by
/// port id.
///
/// The connect path needs this because it creates the ports and the edge in one batch: the
/// edge's own id only exists after that write, so the only handle on it afterwards is the
/// pair of ends the operator asked for.
#[derive(Debug)]
pub struct FindEdgeByEnds {
    pub source: NodeId,
    pub source_key: String,
    pub target: NodeId,
    pub target_key: String,
}

impl Processor<FindEdgeByEnds> for Db {
    /// `None` when the batch did not produce the edge, which is a caller-visible failure.
    type Output = Option<EdgeConnectionEntity>;
    type Error = Error;
    #[tracing::instrument(name = "Query:FindEdgeByEnds", skip_all, err)]
    async fn process(&self, input: FindEdgeByEnds) -> Result<Self::Output, Self::Error> {
        Ok(sqlx::query_as(
            "SELECT e.* FROM orchestration_edge_connection e
             JOIN orchestration_port s ON s.id = e.source_port
             JOIN orchestration_port t ON t.id = e.target_port
             WHERE s.owner = $1 AND s.key = $2 AND t.owner = $3 AND t.key = $4",
        )
        .bind(input.source)
        .bind(input.source_key)
        .bind(input.target)
        .bind(input.target_key)
        .fetch_optional(self.db())
        .await?)
    }
}
