//! The optimistic generation fence shared by every validated write path, and
//! the lock order every write to a canvas tree follows.
//!
//! A validated mutation reads a canvas tree, checks the graph its edit would
//! produce, and only then writes — in a separate transaction. To stop a
//! concurrent edit that landed in between from letting both stale writes commit,
//! the write bumps the root generation with a compare-and-set that fails with
//! [`STALE_GENERATION`] when the root has moved since the read, rolling the whole
//! transaction back.
//!
//! Only the *root* canvas of a tree carries a meaningful `generation`; a
//! subcanvas row keeps `generation == derived_generation`.
//!
//! # Lock order
//!
//! A transaction that writes rows of a canvas tree takes the tree's root row
//! before any other row: an edit by bumping it ([`touch`], [`touch_checked`]), a
//! write that changes nothing a worker runs by locking it ([`lock_root`]). The
//! derivation commit's own fence is its first statement too. Writers of one
//! tree therefore queue on the root instead of each holding a row the other
//! wants. Taking the root last deadlocks: on 2026-09-17 a server delete cascaded
//! to the server's view row and then waited on the root, while a derivation
//! holding the root waited on that view row, until PostgreSQL aborted one of
//! them with `deadlock_detected`.
//!
//! A foreign key is a lock as well: inserting a row, or updating a row the
//! transaction already wrote, takes `KEY SHARE` on the row it references.
//!
//! The worker-driven writes (registration, watch claims, acks, health and
//! address reports) never lock a canvas row. Among one server's rows they take
//! the server row before its view row, the order a server delete cascades in.
//! A write that takes a single row lock and waits on nothing after it (a canvas
//! rename, a subcanvas created under its locked parent) cannot close a circle
//! and does without the root.

use crate::entities::db::canvas::{CanvasFence, CanvasId};
use crate::entities::db::tree;
use base::db::Error;
use sqlx::PgConnection;

/// The conflict a fenced write reports when it loses the race: the canvas
/// advanced past the generation the edit was validated against.
pub const STALE_GENERATION: &str = "orchestration_stale_generation";

/// Marks the tree containing `canvas` as edited: an unconditional bump of its root.
/// The transaction's first write to the tree (see the lock order above).
pub async fn touch(conn: &mut PgConnection, canvas: &CanvasId) -> Result<(), Error> {
    let root = tree::root_of(&mut *conn, canvas).await?;
    sqlx::query!(
        "UPDATE orchestration_canvas SET generation = generation + 1 WHERE id = $1",
        &root as _
    )
    .execute(conn)
    .await?;
    Ok(())
}

/// Takes the root row of the tree containing `canvas` without bumping it: the
/// first step of the lock order for a write that changes nothing a worker runs
/// (positions, groups), and so must not send the tree back to derivation.
pub async fn lock_root(conn: &mut PgConnection, canvas: &CanvasId) -> Result<(), Error> {
    let root = tree::root_of(&mut *conn, canvas).await?;
    // The lock a bump takes, so the two queue behind each other. The row the
    // lock comes with is of no interest; the macro form has to take it anyway.
    let _locked = sqlx::query_scalar!(
        "SELECT 1 FROM orchestration_canvas WHERE id = $1 FOR NO KEY UPDATE",
        &root as _
    )
    .fetch_optional(conn)
    .await?;
    Ok(())
}

/// Like [`touch`], but fenced: the bump commits only while the tree still is the
/// snapshot the caller validated against. `None` bumps unconditionally (an
/// unvalidated write).
///
/// The conditional UPDATE is itself the lock. Under READ COMMITTED a concurrent
/// bump of the same root row blocks this statement until it commits, after which
/// the `WHERE` is re-evaluated against the new row and matches nothing. Like
/// [`touch`], it is the transaction's first write to the tree, so a write that
/// lost the race has written nothing.
pub async fn touch_checked(
    conn: &mut PgConnection,
    canvas: &CanvasId,
    fence: Option<&CanvasFence>,
) -> Result<(), Error> {
    let Some(fence) = fence else {
        return touch(conn, canvas).await;
    };
    let bumped: Option<CanvasId> = sqlx::query_scalar!(
        r#"UPDATE orchestration_canvas SET generation = generation + 1
           WHERE id = $1 AND generation = $2 RETURNING id AS "id: CanvasId""#,
        &fence.root as _,
        fence.generation
    )
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
