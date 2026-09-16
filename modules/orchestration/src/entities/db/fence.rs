//! The optimistic generation fence shared by every validated write path.
//!
//! A validated mutation reads a canvas snapshot, checks the topology its edit
//! would produce, and only then writes — in a separate transaction. To stop a
//! concurrent edit that landed in between from letting both stale writes commit
//! (issue #6), the write bumps the root generation with a compare-and-set that
//! fails with [`STALE_GENERATION`] when the root has moved or been re-parented
//! since the read, rolling the whole transaction back.
//!
//! Only the *root* canvas of a tree carries a meaningful `generation`; a
//! subcanvas row keeps `generation == derived_generation`.

use crate::entities::db::canvas::{CanvasFence, CanvasId};
use crate::entities::db::tree;
use base::db::Error;
use sqlx::PgConnection;

/// The conflict a fenced write reports when it loses the race: the canvas
/// advanced past the generation the edit was validated against.
pub const STALE_GENERATION: &str = "orchestration_stale_generation";

/// Marks the tree containing `canvas` as edited: an unconditional bump of its root.
pub async fn touch(conn: &mut PgConnection, canvas: &CanvasId) -> Result<(), Error> {
    let root = tree::root_of(&mut *conn, canvas).await?;
    sqlx::query("UPDATE orchestration_canvas SET generation = generation + 1 WHERE id = $1")
        .bind(&root)
        .execute(conn)
        .await?;
    Ok(())
}

/// Like [`touch`], but fenced: the bump commits only while the tree still is the
/// snapshot the caller validated against. `None` bumps unconditionally (an
/// unvalidated write such as an Admin force delete).
///
/// The conditional UPDATE is itself the lock. Under READ COMMITTED a concurrent
/// bump of the same root row blocks this statement until it commits, after which
/// the `WHERE` is re-evaluated against the new row and matches nothing. The root
/// is re-derived *after* the bump for the same reason: a re-parenting that
/// committed first has already written this row (`claim_root`), so the walk sees it.
pub async fn touch_checked(
    conn: &mut PgConnection,
    canvas: &CanvasId,
    fence: Option<&CanvasFence>,
) -> Result<(), Error> {
    let Some(fence) = fence else {
        return touch(conn, canvas).await;
    };
    let bumped: Option<CanvasId> = sqlx::query_scalar(
        "UPDATE orchestration_canvas SET generation = generation + 1
         WHERE id = $1 AND generation = $2 RETURNING id",
    )
    .bind(&fence.root)
    .bind(fence.generation)
    .fetch_optional(&mut *conn)
    .await?;
    if bumped.is_none() {
        return Err(Error::Conflict(STALE_GENERATION));
    }
    if tree::root_of(conn, canvas).await? != fence.root {
        return Err(Error::Conflict(STALE_GENERATION));
    }
    Ok(())
}

/// Claims a second root a validated write also read (the target of a canvas
/// import, validated together with the importer but a separate tree): a
/// conditional write that levels the soon-to-be subcanvas *and* fences it, so a
/// concurrent edit to the target that moved its generation makes the import roll
/// back.
///
/// Both counters move, so the claim is a real mutation of the row and never a
/// value-idempotent no-op: a competing edit's own bump on the same root then
/// contends with it and one transaction loses.
pub async fn claim_root(
    conn: &mut PgConnection,
    root: &CanvasId,
    expected_generation: i64,
) -> Result<(), Error> {
    let claimed: Option<CanvasId> = sqlx::query_scalar(
        "UPDATE orchestration_canvas
         SET generation = $2 + 1, derived_generation = $2 + 1
         WHERE id = $1 AND generation = $2 RETURNING id",
    )
    .bind(root)
    .bind(expected_generation)
    .fetch_optional(conn)
    .await?;
    claimed.map(|_| ()).ok_or(Error::Conflict(STALE_GENERATION))
}
