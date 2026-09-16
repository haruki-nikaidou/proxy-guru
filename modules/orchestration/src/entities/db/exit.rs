//! Exits: where traffic leaves the fabric, the sinks of the forwarding graph.

use crate::entities::db::canvas::{CanvasId, CanvasUiPosition};
use crate::entities::db::pod::ProxyProtocolVersion;
use base::db::Error;
use db_types::table_record;
use sqlx::PgConnection;

table_record!(ExitId, "orchestration_exit");

#[derive(Debug, Clone, PartialEq, Eq, sqlx::FromRow)]
pub struct ExitEntity {
    pub id: ExitId,
    pub canvas: CanvasId,
    pub name: String,
    pub comment: String,
    /// `host:port`.
    pub destination: String,
    pub send_proxy_protocol: Option<ProxyProtocolVersion>,
    #[sqlx(flatten)]
    pub position: CanvasUiPosition,
}

pub(crate) async fn insert_exit(conn: &mut PgConnection, exit: &ExitEntity) -> Result<(), Error> {
    sqlx::query(
        "INSERT INTO orchestration_exit
             (id, canvas, name, comment, destination, send_proxy_protocol, position_x, position_y)
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8)",
    )
    .bind(&exit.id)
    .bind(&exit.canvas)
    .bind(&exit.name)
    .bind(&exit.comment)
    .bind(&exit.destination)
    .bind(exit.send_proxy_protocol)
    .bind(exit.position.x)
    .bind(exit.position.y)
    .execute(conn)
    .await?;
    Ok(())
}

/// Rewrites every column of an existing exit but its id and canvas.
pub(crate) async fn update_exit(conn: &mut PgConnection, exit: &ExitEntity) -> Result<(), Error> {
    sqlx::query(
        "UPDATE orchestration_exit
         SET name = $2, comment = $3, destination = $4, send_proxy_protocol = $5,
             position_x = $6, position_y = $7
         WHERE id = $1",
    )
    .bind(&exit.id)
    .bind(&exit.name)
    .bind(&exit.comment)
    .bind(&exit.destination)
    .bind(exit.send_proxy_protocol)
    .bind(exit.position.x)
    .bind(exit.position.y)
    .execute(conn)
    .await?;
    Ok(())
}
