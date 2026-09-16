//! Ports: the connection points of a node, and the two pieces of port logic
//! every reshaping transaction shares.

use crate::entities::db::batch::PortRef;
use crate::entities::db::canvas::CanvasId;
use crate::entities::db::node::{NewPort, NodeId};
use crate::entities::db::tree;
use base::db::{Db, Error};
use db_types::{table_record, text_enum};
use kanau::processor::Processor;
use serde::{Deserialize, Serialize};
use sqlx::PgConnection;

table_record!(PortId, "orchestration_port");

#[derive(Debug, Clone, sqlx::FromRow)]
pub struct PortEntity {
    pub id: PortId,
    pub owner: NodeId,
    pub kind: PortKind,
    pub direction: PortDirection,
    pub key: String,
    pub position: i64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PortKind {
    DeriveListen,
    DeriveDestination,
    /// A bundle port joins two universal nodes; it carries every channel of the
    /// source and is never walked by derivation (see `services::universal`).
    Bundle,
}
text_enum!(PortKind {
    DeriveListen => "derive_listen",
    DeriveDestination => "derive_destination",
    Bundle => "bundle",
});

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PortDirection {
    Input,
    Output,
}
text_enum!(PortDirection {
    Input => "input",
    Output => "output",
});

#[derive(Debug)]
pub struct FindPortById {
    pub id: PortId,
}

impl Processor<FindPortById> for Db {
    type Output = Option<PortEntity>;
    type Error = Error;
    #[tracing::instrument(name = "Query:FindPortById", skip_all, err)]
    async fn process(&self, input: FindPortById) -> Result<Self::Output, Self::Error> {
        Ok(
            sqlx::query_as("SELECT * FROM orchestration_port WHERE id = $1")
                .bind(input.id)
                .fetch_optional(self.db())
                .await?,
        )
    }
}

/// A node's ports, by position.
pub async fn ports_of(conn: &mut PgConnection, node: &NodeId) -> Result<Vec<PortEntity>, Error> {
    Ok(
        sqlx::query_as("SELECT * FROM orchestration_port WHERE owner = $1 ORDER BY position")
            .bind(node)
            .fetch_all(conn)
            .await?,
    )
}

/// Creates a node's ports, one row per [`NewPort`], and returns them by position.
pub async fn insert_ports(
    conn: &mut PgConnection,
    node: &NodeId,
    ports: &[NewPort],
) -> Result<Vec<PortEntity>, Error> {
    for port in ports {
        sqlx::query(
            "INSERT INTO orchestration_port (id, owner, kind, direction, key, position)
             VALUES ($1, $2, $3, $4, $5, $6)",
        )
        .bind(PortId::new())
        .bind(node)
        .bind(port.kind)
        .bind(port.direction)
        .bind(&port.key)
        .bind(port.position)
        .execute(&mut *conn)
        .await?;
    }
    ports_of(conn, node).await
}

/// Reshapes a node's ports in place: a key that survives keeps its row (and its
/// edges); a key that disappears loses its row and, through the cascade, its
/// edges.
pub async fn reshape_ports(
    conn: &mut PgConnection,
    node: &NodeId,
    ports: &[NewPort],
) -> Result<(), Error> {
    let keep: Vec<&str> = ports.iter().map(|p| p.key.as_str()).collect();
    sqlx::query("DELETE FROM orchestration_port WHERE owner = $1 AND NOT (key = ANY($2))")
        .bind(node)
        .bind(&keep)
        .execute(&mut *conn)
        .await?;
    for port in ports {
        sqlx::query(
            "INSERT INTO orchestration_port (id, owner, kind, direction, key, position)
             VALUES ($1, $2, $3, $4, $5, $6)
             ON CONFLICT ON CONSTRAINT orchestration_port_owner_key_key DO UPDATE
                 SET kind = EXCLUDED.kind, direction = EXCLUDED.direction,
                     position = EXCLUDED.position",
        )
        .bind(PortId::new())
        .bind(node)
        .bind(port.kind)
        .bind(port.direction)
        .bind(&port.key)
        .bind(port.position)
        .execute(&mut *conn)
        .await?;
    }
    Ok(())
}

/// Resolves one end of an edge written by a topology batch: `{ port }` names an
/// existing port; `{ node, key }` a port (possibly just created by a reshape) on
/// an existing node; `{ lane_key, key }` a port on the lane node of that key in
/// `canvas`'s tree, which may have been created moments earlier in the same
/// transaction. `None` when nothing matches.
pub async fn resolve_port(
    conn: &mut PgConnection,
    canvas: &CanvasId,
    reference: &PortRef,
) -> Result<Option<PortId>, Error> {
    if let Some(port) = &reference.port {
        return Ok(Some(port.clone()));
    }
    let Some(key) = &reference.key else {
        return Ok(None);
    };
    let node = match (&reference.node, &reference.lane_key) {
        (Some(node), _) => Some(node.clone()),
        (None, Some(lane_key)) => {
            let (_, tree) = tree::whole_tree_of(&mut *conn, canvas).await?;
            sqlx::query_scalar(
                "SELECT id FROM orchestration_node
                 WHERE canvas = ANY($1) AND lane_key = $2 LIMIT 1",
            )
            .bind(&tree)
            .bind(lane_key)
            .fetch_optional(&mut *conn)
            .await?
        }
        (None, None) => None,
    };
    let Some(node) = node else {
        return Ok(None);
    };
    Ok(
        sqlx::query_scalar("SELECT id FROM orchestration_port WHERE owner = $1 AND key = $2")
            .bind(&node)
            .bind(key)
            .fetch_optional(conn)
            .await?,
    )
}
