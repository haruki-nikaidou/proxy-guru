//! Walking a canvas tree.
//!
//! Nesting is stored only on the importing node (`orchestration_node.import_canvas`);
//! a canvas's root, ancestors and tree are computed. These run inside the
//! caller's transaction, so a mutating transaction bumps the root it belongs to
//! at commit time rather than the root a service read a moment earlier.

use crate::entities::db::canvas::CanvasId;
use base::db::Error;
use sqlx::PgConnection;

/// Deeper than this and the tree is refused rather than walked further; a cycle
/// is impossible by construction (`import_canvas` is UNIQUE and an import cannot
/// name an ancestor), so hitting the cap means corrupt data, not a big tree.
const MAX_DEPTH: i32 = 32;
pub const NESTING_TOO_DEEP: &str = "canvas nesting deeper than 32 levels";

fn guard_depth(rows: &[(CanvasId, i32)]) -> Result<(), Error> {
    match rows.last() {
        Some((_, depth)) if *depth >= MAX_DEPTH => Err(Error::Conflict(NESTING_TOO_DEEP)),
        _ => Ok(()),
    }
}

/// `[parent, grandparent, ..., root]`; empty for a root.
pub async fn ancestors_of(
    conn: &mut PgConnection,
    canvas: &CanvasId,
) -> Result<Vec<CanvasId>, Error> {
    let rows: Vec<(CanvasId, i32)> = sqlx::query_as(
        "WITH RECURSIVE up (canvas, depth) AS (
             SELECT n.canvas, 1 FROM orchestration_node n WHERE n.import_canvas = $1
           UNION ALL
             SELECT n.canvas, up.depth + 1
             FROM up JOIN orchestration_node n ON n.import_canvas = up.canvas
             WHERE up.depth < $2
         )
         SELECT canvas, depth FROM up ORDER BY depth",
    )
    .bind(canvas)
    .bind(MAX_DEPTH)
    .fetch_all(conn)
    .await?;
    guard_depth(&rows)?;
    Ok(rows.into_iter().map(|(canvas, _)| canvas).collect())
}

/// The root of the tree `canvas` belongs to; `canvas` itself for a root.
pub async fn root_of(conn: &mut PgConnection, canvas: &CanvasId) -> Result<CanvasId, Error> {
    Ok(ancestors_of(conn, canvas)
        .await?
        .pop()
        .unwrap_or_else(|| canvas.clone()))
}

/// The canvas and every canvas below it, the given canvas first, then by depth.
pub async fn tree_of(conn: &mut PgConnection, canvas: &CanvasId) -> Result<Vec<CanvasId>, Error> {
    let rows: Vec<(CanvasId, i32)> = sqlx::query_as(
        "WITH RECURSIVE tree (canvas, depth) AS (
             SELECT $1::text, 0
           UNION ALL
             SELECT n.import_canvas, tree.depth + 1
             FROM tree JOIN orchestration_node n ON n.canvas = tree.canvas
             WHERE n.import_canvas IS NOT NULL AND tree.depth < $2
         )
         SELECT canvas, depth FROM tree ORDER BY depth, canvas",
    )
    .bind(canvas)
    .bind(MAX_DEPTH)
    .fetch_all(conn)
    .await?;
    guard_depth(&rows)?;
    Ok(rows.into_iter().map(|(canvas, _)| canvas).collect())
}

/// The whole tree containing `canvas`: its root first.
pub async fn whole_tree_of(
    conn: &mut PgConnection,
    canvas: &CanvasId,
) -> Result<(CanvasId, Vec<CanvasId>), Error> {
    let root = root_of(&mut *conn, canvas).await?;
    let tree = tree_of(conn, &root).await?;
    Ok((root, tree))
}
