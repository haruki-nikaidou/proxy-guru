use crate::entities::surreal::canvas::{CanvasFence, CanvasId};
use crate::entities::surreal::fence::take_fence_error;
use crate::entities::surreal::port::PortId;
use kanau::processor::Processor;
use newtype_record_id::table_record;
use surrealdb_types::SurrealValue;
use wakuwaku::surreal::SurrealProcessor;

table_record!(EdgeConnectionId, "orchestration_edge_connection");

#[derive(Debug, Clone, SurrealValue)]
pub struct EdgeConnectionEntity {
    pub id: EdgeConnectionId,
    #[surreal(rename = "in")]
    pub source: PortId,
    #[surreal(rename = "out")]
    pub target: PortId,
}

#[derive(Debug)]
pub struct ConnectPorts {
    pub source: PortId,
    pub target: PortId,
    pub canvas: CanvasId,
    /// The snapshot the connect was validated against, fencing the write;
    /// `None` bumps unconditionally (see `fn::orchestration_touch_checked`).
    pub fence: Option<CanvasFence>,
}

impl Processor<ConnectPorts> for SurrealProcessor {
    type Output = EdgeConnectionEntity;
    type Error = surrealdb::Error;
    #[tracing::instrument(name = "Query-Transaction:ConnectPorts", skip_all, err)]
    async fn process(&self, input: ConnectPorts) -> Result<Self::Output, Self::Error> {
        // Statement 0 is BEGIN; the RETURN below is statement 3. Pull the fence
        // THROW out of the transaction's error set first: a cancelled write masks
        // it behind a "not executed" error on the earlier RELATE otherwise.
        let mut resp = self
            .db()
            .query(include_str!("../../../sql/connection/connect_ports.surql"))
            .bind(("source", input.source))
            .bind(("target", input.target))
            .bind(("canvas", input.canvas))
            .bind((
                "expected_root",
                input.fence.as_ref().map(|f| f.root.clone()),
            ))
            .bind(("expected", input.fence.as_ref().map(|f| f.generation)))
            .await?;
        if let Some(error) = take_fence_error(&mut resp) {
            return Err(error);
        }
        resp.take::<Option<EdgeConnectionEntity>>(3)?
            .ok_or_else(|| surrealdb::Error::internal("relate returned no row".to_string()))
    }
}

#[derive(Debug)]
pub struct FindEdgeById {
    pub id: EdgeConnectionId,
}

impl Processor<FindEdgeById> for SurrealProcessor {
    type Output = Option<EdgeConnectionEntity>;
    type Error = surrealdb::Error;
    #[tracing::instrument(name = "Query:FindEdgeById", skip_all, err)]
    async fn process(&self, input: FindEdgeById) -> Result<Self::Output, Self::Error> {
        let mut resp = self
            .db()
            .query("SELECT * FROM $id")
            .bind(("id", input.id))
            .await?;
        resp.take::<Option<EdgeConnectionEntity>>(0)
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

impl Processor<DeleteEdgeRow> for SurrealProcessor {
    type Output = ();
    type Error = surrealdb::Error;
    #[tracing::instrument(name = "Query-Transaction:DeleteEdgeRow", skip_all, err)]
    async fn process(&self, input: DeleteEdgeRow) -> Result<Self::Output, Self::Error> {
        let mut resp = self
            .db()
            .query(
                "BEGIN TRANSACTION;
                 DELETE $id;
                 fn::orchestration_touch_checked($canvas, $expected_root, $expected);
                 COMMIT TRANSACTION;",
            )
            .bind(("id", input.id))
            .bind(("canvas", input.canvas))
            .bind((
                "expected_root",
                input.fence.as_ref().map(|f| f.root.clone()),
            ))
            .bind(("expected", input.fence.as_ref().map(|f| f.generation)))
            .await?;
        if let Some(error) = take_fence_error(&mut resp) {
            return Err(error);
        }
        Ok(())
    }
}
