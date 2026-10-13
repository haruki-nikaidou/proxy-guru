//! The advisory lock that keeps the `notifier` mode single.
//!
//! Delivery is the one part of this workspace that must not fan out: two
//! notifiers consuming the same topic under one consumer name would race on its
//! offsets, so a restart overlapping a running instance would double every
//! notice both happened to poll. A session-scoped PostgreSQL advisory lock is
//! the cheapest fleet-wide answer — the database is already mandatory, and the
//! lock is released by the connection dying, so a killed notifier does not
//! block its successor.

use crate::services::NotifyError;
use sqlx::pool::PoolConnection;
use sqlx::{PgPool, Postgres};

/// The lock id, `'notify'` in ASCII. Arbitrary but fixed: every notifier in the
/// fleet asks for this one.
pub const NOTIFIER_ADVISORY_LOCK: i64 = 0x6e6f74696679;

/// Takes the notifier lock and hands back the connection holding it.
///
/// The lock lives for as long as the returned connection: keep it bound for the
/// whole mode, and drop it only when the mode ends. It takes the pool itself,
/// not the `Db` handle: the lock is session-scoped, so it needs a connection
/// it can keep. A second notifier gets
/// [`NotifyError::InvalidInput`] instead of the lock.
pub async fn hold_notifier_lock(pool: &PgPool) -> Result<PoolConnection<Postgres>, NotifyError> {
    let mut conn = pool.acquire().await.map_err(base::db::Error::from)?;
    let held = sqlx::query_scalar!(
        r#"SELECT pg_try_advisory_lock($1) AS "held!""#,
        NOTIFIER_ADVISORY_LOCK
    )
    .fetch_one(&mut *conn)
    .await
    .map_err(base::db::Error::from)?;
    if !held {
        return Err(NotifyError::InvalidInput(
            "another notifier already holds the advisory lock; run exactly one".into(),
        ));
    }
    Ok(conn)
}
