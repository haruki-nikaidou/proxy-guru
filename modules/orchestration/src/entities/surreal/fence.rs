//! The optimistic generation fence shared by every validated write path.
//!
//! A validated mutation reads a canvas snapshot, checks the topology its edit
//! would produce, and only then writes — in a separate transaction. To stop a
//! concurrent edit that landed in between from letting both stale writes commit
//! (issue #6), the write bumps the root generation with a compare-and-set
//! (`fn::orchestration_touch_checked`) that THROWs [`STALE_GENERATION`] when the
//! root has moved or been re-parented since the read.

/// The token `fn::orchestration_touch_checked` and `fn::orchestration_claim_root`
/// THROW when a fenced write loses the race: the canvas advanced past the
/// generation the edit was validated against, so the write rolls back.
pub const STALE_GENERATION: &str = "orchestration_stale_generation";

/// Extracts the fence THROW from a finished transaction response.
///
/// A cancelled transaction records the causing THROW on its own statement and a
/// masking `not executed` / `cancelled` error on every other statement, so
/// `IndexedResults::check` alone can surface a mask instead of the cause. This
/// scans all statement errors and returns the fence error when present,
/// otherwise the first error; `None` when the transaction committed cleanly (in
/// which case the response's values are left intact for the caller to `take`).
pub(crate) fn take_fence_error(resp: &mut surrealdb::IndexedResults) -> Option<surrealdb::Error> {
    let errors = resp.take_errors();
    if errors.is_empty() {
        return None;
    }
    let mut fallback = None;
    for (_, error) in errors {
        if error.to_string().contains(STALE_GENERATION) {
            return Some(error);
        }
        fallback.get_or_insert(error);
    }
    fallback
}
