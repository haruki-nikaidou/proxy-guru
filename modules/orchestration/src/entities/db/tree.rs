//! Walking a canvas tree.
//!
//! A canvas names its `parent`; a canvas without one is a root. The root, the
//! ancestors and the tree are computed, inside the caller's transaction, so a
//! mutating transaction bumps the root it belongs to as it writes rather than
//! the root a service read a moment earlier.
//!
//! Each walk is a helper query: a flow runs it with `tx.run`, another query on
//! its own connection with `.execute(&mut *conn)`.

use crate::entities::db::canvas::CanvasId;
use crate::entities::db::tables::OrchestrationCanvas;
use base::db::Error;
use sqlx::PgConnection;
use wakuwaku::entity::sqlx::backend::PgSource;
use wakuwaku::entity::{Execute, Query, Read};

/// Deeper than this and the tree is refused rather than walked further; a cycle
/// is impossible through the writes the service accepts (a parent is chosen at
/// creation and never changes), so hitting the cap means corrupt data, not a
/// big tree.
pub const MAX_DEPTH: i32 = 32;
pub const NESTING_TOO_DEEP: &str = "canvas nesting deeper than 32 levels";

/// One step of a walk: a canvas and how far it sits from where the walk started.
struct DepthRow {
    canvas: CanvasId,
    depth: i32,
}

fn guard_depth(rows: &[DepthRow]) -> Result<(), Error> {
    match rows.last() {
        Some(row) if row.depth >= MAX_DEPTH => Err(Error::Conflict(NESTING_TOO_DEEP)),
        _ => Ok(()),
    }
}

/// `[parent, grandparent, ..., root]`; empty for a root or a canvas that does
/// not exist.
pub struct AncestorsOf<'a> {
    pub canvas: &'a CanvasId,
}

impl Query for AncestorsOf<'_> {
    type Effect = Read<OrchestrationCanvas>;
}

impl Execute<PgSource> for AncestorsOf<'_> {
    type Output = Vec<CanvasId>;
    type Error = Error;
    async fn execute(self, conn: &mut PgConnection) -> Result<Self::Output, Self::Error> {
        let rows =
            sqlx::query_file_as!(DepthRow, "sql/ancestors_of.sql", self.canvas as _, MAX_DEPTH)
                .fetch_all(conn)
                .await?;
        guard_depth(&rows)?;
        Ok(rows.into_iter().map(|row| row.canvas).collect())
    }
}

/// The root of the tree `canvas` belongs to; `canvas` itself for a root.
pub struct RootOf<'a> {
    pub canvas: &'a CanvasId,
}

impl Query for RootOf<'_> {
    type Effect = Read<OrchestrationCanvas>;
}

impl Execute<PgSource> for RootOf<'_> {
    type Output = CanvasId;
    type Error = Error;
    async fn execute(self, conn: &mut PgConnection) -> Result<Self::Output, Self::Error> {
        Ok(AncestorsOf {
            canvas: self.canvas,
        }
        .execute(conn)
        .await?
        .pop()
        .unwrap_or_else(|| self.canvas.clone()))
    }
}

/// The canvas and every canvas below it, the given canvas first, then by depth
/// and id.
pub struct TreeOf<'a> {
    pub canvas: &'a CanvasId,
}

impl Query for TreeOf<'_> {
    type Effect = Read<OrchestrationCanvas>;
}

impl Execute<PgSource> for TreeOf<'_> {
    type Output = Vec<CanvasId>;
    type Error = Error;
    async fn execute(self, conn: &mut PgConnection) -> Result<Self::Output, Self::Error> {
        let rows = sqlx::query_file_as!(DepthRow, "sql/tree_of.sql", self.canvas as _, MAX_DEPTH)
            .fetch_all(conn)
            .await?;
        guard_depth(&rows)?;
        Ok(rows.into_iter().map(|row| row.canvas).collect())
    }
}

/// The whole tree containing `canvas`: its root first.
pub struct WholeTreeOf<'a> {
    pub canvas: &'a CanvasId,
}

impl Query for WholeTreeOf<'_> {
    type Effect = Read<OrchestrationCanvas>;
}

impl Execute<PgSource> for WholeTreeOf<'_> {
    type Output = (CanvasId, Vec<CanvasId>);
    type Error = Error;
    async fn execute(self, conn: &mut PgConnection) -> Result<Self::Output, Self::Error> {
        let root = RootOf {
            canvas: self.canvas,
        }
        .execute(&mut *conn)
        .await?;
        let tree = TreeOf { canvas: &root }.execute(conn).await?;
        Ok((root, tree))
    }
}
