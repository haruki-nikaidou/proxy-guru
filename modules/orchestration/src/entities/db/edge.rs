//! Edges: a pod's ways on, to another pod (relayed in the protocol that pod
//! listens with) or to an exit.
//!
//! An edge belongs to its source pod, whose route names it. Parallel edges are
//! allowed: two edges between the same pods may dial different addresses.

use crate::entities::db::exit::ExitId;
use crate::entities::db::pod::PodId;
use base::db::Error;
use db_types::table_record;
use sqlx::PgConnection;

table_record!(EdgeId, "orchestration_edge");

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EdgeEntity {
    pub id: EdgeId,
    pub source: PodId,
    pub target: EdgeTarget,
    /// Dial this host (an IP literal or a name) instead of the target pod's own
    /// address.
    pub override_ip: Option<String>,
    /// Dial this port instead of the target pod's.
    pub override_port: Option<u16>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum EdgeTarget {
    Pod(PodId),
    Exit(ExitId),
}

/// An `orchestration_edge` row, one field per column: what the `query_as!`
/// macros fill in before [`EdgeEntity`] folds the two target columns into one
/// [`EdgeTarget`].
pub(crate) struct EdgeRow {
    pub id: EdgeId,
    pub source_pod: PodId,
    pub target_pod: Option<PodId>,
    pub target_exit: Option<ExitId>,
    pub override_ip: Option<String>,
    pub override_port: Option<i32>,
}

impl TryFrom<EdgeRow> for EdgeEntity {
    type Error = sqlx::Error;

    fn try_from(row: EdgeRow) -> Result<Self, sqlx::Error> {
        let id = row.id;
        let target = match (row.target_pod, row.target_exit) {
            (Some(pod), None) => EdgeTarget::Pod(pod),
            (None, Some(exit)) => EdgeTarget::Exit(exit),
            _ => {
                return Err(sqlx::Error::ColumnDecode {
                    index: "target_pod".to_string(),
                    source: format!("edge {id} does not have exactly one target").into(),
                });
            }
        };
        let override_port = row
            .override_port
            .map(|port| {
                u16::try_from(port).map_err(|_| sqlx::Error::ColumnDecode {
                    index: "override_port".to_string(),
                    source: format!("edge {id}: port {port} out of range").into(),
                })
            })
            .transpose()?;
        Ok(EdgeEntity {
            source: row.source_pod,
            target,
            override_ip: row.override_ip,
            override_port,
            id,
        })
    }
}

pub(crate) async fn insert_edge(conn: &mut PgConnection, edge: &EdgeEntity) -> Result<(), Error> {
    let (pod, exit) = match &edge.target {
        EdgeTarget::Pod(pod) => (Some(pod), None),
        EdgeTarget::Exit(exit) => (None, Some(exit)),
    };
    sqlx::query!(
        "INSERT INTO orchestration_edge
             (id, source_pod, target_pod, target_exit, override_ip, override_port)
         VALUES ($1, $2, $3, $4, $5, $6)",
        edge.id as _,
        edge.source as _,
        pod as _,
        exit as _,
        edge.override_ip,
        edge.override_port.map(i32::from)
    )
    .execute(conn)
    .await?;
    Ok(())
}

/// Rewrites what an existing edge dials. Its ends are its identity: moving an
/// edge is deleting it and drawing another.
pub(crate) async fn update_edge(conn: &mut PgConnection, edge: &EdgeEntity) -> Result<(), Error> {
    sqlx::query!(
        "UPDATE orchestration_edge SET override_ip = $2, override_port = $3 WHERE id = $1",
        edge.id as _,
        edge.override_ip,
        edge.override_port.map(i32::from)
    )
    .execute(conn)
    .await?;
    Ok(())
}
