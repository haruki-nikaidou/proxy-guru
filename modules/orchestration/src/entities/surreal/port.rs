use crate::entities::surreal::node::NodeId;
use kanau::processor::Processor;
use newtype_record_id::table_record;
use surrealdb_types::SurrealValue;
use wakuwaku::surreal::SurrealProcessor;

table_record!(PortId, "orchestration_port");

#[derive(Debug, Clone, SurrealValue)]
pub struct PortEntity {
    pub id: PortId,
    pub owner: NodeId,
    pub kind: PortKind,
    pub direction: PortDirection,
    pub key: String,
    pub position: i64,
}

#[derive(Debug, Clone, SurrealValue, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
#[surreal(untagged, rename_all = "snake_case")]
pub enum PortKind {
    DeriveListen,
    DeriveDestination,
    /// A bundle port joins two universal nodes; it carries every channel of the
    /// source and is never walked by derivation (see `services::universal`).
    Bundle,
}

#[derive(Debug, Clone, SurrealValue, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
#[surreal(untagged, rename_all = "snake_case")]
pub enum PortDirection {
    Input,
    Output,
}

#[derive(Debug)]
pub struct FindPortById {
    pub id: PortId,
}

impl Processor<FindPortById> for SurrealProcessor {
    type Output = Option<PortEntity>;
    type Error = surrealdb::Error;
    #[tracing::instrument(name = "Query:FindPortById", skip_all, err)]
    async fn process(&self, input: FindPortById) -> Result<Self::Output, Self::Error> {
        let mut resp = self
            .db()
            .query("SELECT * FROM $id")
            .bind(("id", input.id))
            .await?;
        resp.take::<Option<PortEntity>>(0)
    }
}
