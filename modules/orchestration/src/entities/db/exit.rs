//! Exits: where traffic leaves the fabric, the sinks of the forwarding graph.

use crate::entities::db::canvas::{CanvasId, CanvasUiPosition};
use crate::entities::db::pod::ProxyProtocolVersion;
use base::db::Error;
use db_types::table_record;
use sqlx::PgConnection;

table_record!(ExitId, "orchestration_exit");

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExitEntity {
    pub id: ExitId,
    pub canvas: CanvasId,
    pub name: String,
    pub comment: String,
    /// `host:port`.
    pub destination: String,
    pub send_proxy_protocol: Option<ProxyProtocolVersion>,
    pub position: CanvasUiPosition,
}

/// An `orchestration_exit` row, one field per column: what the `query_as!`
/// macros fill in before [`ExitEntity`] gathers the two position columns.
pub(crate) struct ExitRow {
    pub id: ExitId,
    pub canvas: CanvasId,
    pub name: String,
    pub comment: String,
    pub destination: String,
    pub send_proxy_protocol: Option<ProxyProtocolVersion>,
    pub position_x: i64,
    pub position_y: i64,
}

impl From<ExitRow> for ExitEntity {
    fn from(row: ExitRow) -> Self {
        Self {
            id: row.id,
            canvas: row.canvas,
            name: row.name,
            comment: row.comment,
            destination: row.destination,
            send_proxy_protocol: row.send_proxy_protocol,
            position: CanvasUiPosition {
                x: row.position_x,
                y: row.position_y,
            },
        }
    }
}

pub(crate) async fn insert_exit(conn: &mut PgConnection, exit: &ExitEntity) -> Result<(), Error> {
    sqlx::query!(
        "INSERT INTO orchestration_exit
             (id, canvas, name, comment, destination, send_proxy_protocol, position_x, position_y)
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8)",
        exit.id as _,
        exit.canvas as _,
        exit.name,
        exit.comment,
        exit.destination,
        exit.send_proxy_protocol as _,
        exit.position.x,
        exit.position.y
    )
    .execute(conn)
    .await?;
    Ok(())
}

/// Rewrites every column of an existing exit but its id and canvas.
pub(crate) async fn update_exit(conn: &mut PgConnection, exit: &ExitEntity) -> Result<(), Error> {
    sqlx::query!(
        "UPDATE orchestration_exit
         SET name = $2, comment = $3, destination = $4, send_proxy_protocol = $5,
             position_x = $6, position_y = $7
         WHERE id = $1",
        exit.id as _,
        exit.name,
        exit.comment,
        exit.destination,
        exit.send_proxy_protocol as _,
        exit.position.x,
        exit.position.y
    )
    .execute(conn)
    .await?;
    Ok(())
}
