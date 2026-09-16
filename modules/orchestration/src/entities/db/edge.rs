//! Edges: a pod's ways on, to another pod (relayed in the protocol that pod
//! listens with) or to an exit.
//!
//! An edge belongs to its source pod, whose route names it. Parallel edges are
//! allowed: two edges between the same pods may dial different addresses.

use crate::entities::db::exit::ExitId;
use crate::entities::db::pod::PodId;
use base::db::Error;
use db_types::table_record;
use sqlx::postgres::PgRow;
use sqlx::{FromRow, PgConnection, Row};

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

impl FromRow<'_, PgRow> for EdgeEntity {
    fn from_row(row: &PgRow) -> Result<Self, sqlx::Error> {
        let id: EdgeId = row.try_get("id")?;
        let target = match (
            row.try_get::<Option<PodId>, _>("target_pod")?,
            row.try_get::<Option<ExitId>, _>("target_exit")?,
        ) {
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
            .try_get::<Option<i32>, _>("override_port")?
            .map(|port| {
                u16::try_from(port).map_err(|_| sqlx::Error::ColumnDecode {
                    index: "override_port".to_string(),
                    source: format!("edge {id}: port {port} out of range").into(),
                })
            })
            .transpose()?;
        Ok(EdgeEntity {
            source: row.try_get("source_pod")?,
            target,
            override_ip: row.try_get("override_ip")?,
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
    sqlx::query(
        "INSERT INTO orchestration_edge
             (id, source_pod, target_pod, target_exit, override_ip, override_port)
         VALUES ($1, $2, $3, $4, $5, $6)",
    )
    .bind(&edge.id)
    .bind(&edge.source)
    .bind(pod)
    .bind(exit)
    .bind(&edge.override_ip)
    .bind(edge.override_port.map(i32::from))
    .execute(conn)
    .await?;
    Ok(())
}
