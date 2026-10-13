//! Edges: a pod's ways on, to another pod (relayed in the protocol that pod
//! listens with) or to an exit.
//!
//! An edge belongs to its source pod, whose route names it. Parallel edges are
//! allowed: two edges between the same pods may dial different addresses.

use crate::entities::db::exit::ExitId;
use crate::entities::db::pod::PodId;
use crate::entities::db::tables::OrchestrationEdge;
use base::db::Error;
use db_types::{table_record, text_enum};
use serde::{Deserialize, Serialize};
use sqlx::PgConnection;
use wakuwaku::entity::sqlx::backend::PgSource;
use wakuwaku::entity::{Execute, Query, Write};

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
    /// Which of the target pod's addresses to dial when `override_ip` names
    /// none. Only meaningful toward a pod.
    pub ip_family: IpFamily,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum EdgeTarget {
    Pod(PodId),
    Exit(ExitId),
}

/// The stored spelling of [`guru_topology::IpFamily`]: which address family an
/// edge dials the pod it leads to over.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum IpFamily {
    /// The server's effective address, IPv4 when it has one.
    #[default]
    Auto,
    V4,
    V6,
}
text_enum!(IpFamily {
    Auto => "auto",
    V4 => "v4",
    V6 => "v6",
});

impl From<IpFamily> for guru_topology::IpFamily {
    fn from(value: IpFamily) -> Self {
        match value {
            IpFamily::Auto => guru_topology::IpFamily::Auto,
            IpFamily::V4 => guru_topology::IpFamily::V4,
            IpFamily::V6 => guru_topology::IpFamily::V6,
        }
    }
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
    pub ip_family: IpFamily,
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
            ip_family: row.ip_family,
            id,
        })
    }
}

/// Inserts an edge row as it is.
pub(crate) struct InsertEdge<'a> {
    pub(crate) edge: &'a EdgeEntity,
}

impl Query for InsertEdge<'_> {
    type Effect = Write<OrchestrationEdge>;
}

impl Execute<PgSource> for InsertEdge<'_> {
    type Output = ();
    type Error = Error;
    async fn execute(self, conn: &mut PgConnection) -> Result<(), Error> {
        let edge = self.edge;
        let (pod, exit) = match &edge.target {
            EdgeTarget::Pod(pod) => (Some(pod), None),
            EdgeTarget::Exit(exit) => (None, Some(exit)),
        };
        sqlx::query!(
            "INSERT INTO orchestration_edge
             (id, source_pod, target_pod, target_exit, override_ip, override_port, ip_family)
         VALUES ($1, $2, $3, $4, $5, $6, $7)",
            edge.id as _,
            edge.source as _,
            pod as _,
            exit as _,
            edge.override_ip,
            edge.override_port.map(i32::from),
            edge.ip_family as _
        )
        .execute(conn)
        .await?;
        Ok(())
    }
}

/// Rewrites what an existing edge dials. Its ends are its identity: moving an
/// edge is deleting it and drawing another.
pub(crate) struct UpdateEdge<'a> {
    pub(crate) edge: &'a EdgeEntity,
}

impl Query for UpdateEdge<'_> {
    type Effect = Write<OrchestrationEdge>;
}

impl Execute<PgSource> for UpdateEdge<'_> {
    type Output = ();
    type Error = Error;
    async fn execute(self, conn: &mut PgConnection) -> Result<(), Error> {
        let edge = self.edge;
        sqlx::query!(
            "UPDATE orchestration_edge
         SET override_ip = $2, override_port = $3, ip_family = $4
         WHERE id = $1",
            edge.id as _,
            edge.override_ip,
            edge.override_port.map(i32::from),
            edge.ip_family as _
        )
        .execute(conn)
        .await?;
        Ok(())
    }
}

#[cfg(test)]
#[allow(clippy::panic)]
mod tests {
    use super::*;

    #[test]
    fn stored_spellings_match_serde() {
        db_types::assert_text_enum_matches_serde!(IpFamily);
    }
}
