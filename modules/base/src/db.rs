//! The database handle every service holds, and the error every query returns.
//!
//! [`Db`] is `wakuwaku`'s thin wrapper over a `PgPool`; entity queries are
//! `Processor` impls on it, services hold a clone of it. There is no second
//! layer: bounding a query is the server's job (`statement_timeout`, set on
//! every connection by [`connect`]) and cancels the statement itself rather than
//! abandoning it, so a client-side timer would only add a second clock that
//! could disagree with the first.
//!
//! Nothing here retries. Only a caller knows whether its operation is
//! idempotent, and the one class of failure that is safe to retry blind is
//! named as such: [`Error::Conflict`] means the transaction definitely did not
//! commit.

use sqlx::migrate::Migrator;
use sqlx::postgres::{PgConnectOptions, PgPoolOptions};
use std::time::Duration;

pub use wakuwaku::sqlx::DatabaseProcessor as Db;

/// The schema, applied by `guru-master` at startup and by `manage-tool db migrate`.
///
/// Declared once, here, so the path to `database/migrations` is spelled in one
/// place; `#[sqlx::test(migrator = "base::db::MIGRATOR")]` reuses it.
pub static MIGRATOR: Migrator = sqlx::migrate!("../../database/migrations");

/// How a process opens the database.
#[derive(Debug, Clone)]
pub struct PoolSettings {
    /// The server-side bound on one statement. A statement that runs past it is
    /// cancelled by the server (SQLSTATE `57014`, which [`is_unavailable`]
    /// classifies as retryable) and the connection stays usable.
    pub statement_timeout: Duration,
    /// Connections this process may hold at once.
    pub max_connections: u32,
    /// Shown in `pg_stat_activity`, so a stuck statement can be traced to a process.
    pub application_name: &'static str,
}

/// Opens a pool and returns the handle services hold.
pub async fn connect(url: &str, settings: PoolSettings) -> Result<Db, sqlx::Error> {
    let millis = settings.statement_timeout.as_millis().to_string();
    let options = url
        .parse::<PgConnectOptions>()?
        .application_name(settings.application_name)
        // Session settings ride in the startup packet, so every connection the
        // pool opens has them from its first statement.
        .options([
            ("statement_timeout", millis.as_str()),
            ("lock_timeout", millis.as_str()),
            // A transaction whose future was dropped mid-way (a client hung up)
            // would otherwise keep its row locks until the pool recycled the
            // connection; the canvas root row is exactly the lock that must not
            // be held by a ghost.
            ("idle_in_transaction_session_timeout", "30000"),
        ]);
    let pool = PgPoolOptions::new()
        .max_connections(settings.max_connections)
        .min_connections(1)
        .acquire_timeout(settings.statement_timeout)
        .idle_timeout(Duration::from_secs(600))
        .max_lifetime(Duration::from_secs(1800))
        .connect_with(options)
        .await?;
    Ok(Db::new(pool))
}

/// What an entity query can fail with.
///
/// Every `Processor` on [`Db`] uses this as its error; `?` on a sqlx call
/// converts through [`From<sqlx::Error>`], which is where the one
/// classification this workspace cares about is made.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// The driver or the server answered with a failure.
    #[error(transparent)]
    Query(sqlx::Error),
    /// The transaction definitely did not commit, and a retry from a fresh read
    /// may succeed: a generation fence lost, a unique constraint the write raced
    /// on, a serialisation failure or a deadlock. The token names which.
    ///
    /// This is the class the old store never distinguished from a fault.
    #[error("conflict: {0}")]
    Conflict(&'static str),
}

/// SQLSTATE `40001`.
pub const SERIALIZATION_FAILURE: &str = "serialization_failure";
/// SQLSTATE `40P01`.
pub const DEADLOCK: &str = "deadlock_detected";

impl From<sqlx::Error> for Error {
    fn from(error: sqlx::Error) -> Self {
        match sqlstate(&error) {
            Some("40001") => Error::Conflict(SERIALIZATION_FAILURE),
            Some("40P01") => Error::Conflict(DEADLOCK),
            _ => Error::Query(error),
        }
    }
}

impl Error {
    pub fn is_conflict(&self) -> bool {
        matches!(self, Error::Conflict(_))
    }

    /// The constraint a unique violation (`23505`) hit, when this is one.
    pub fn unique_violation(&self) -> Option<&str> {
        match self {
            Error::Query(sqlx::Error::Database(e)) if e.is_unique_violation() => e.constraint(),
            _ => None,
        }
    }

    /// The constraint a foreign-key violation (`23503`) hit, when this is one.
    pub fn fk_violation(&self) -> Option<&str> {
        match self {
            Error::Query(sqlx::Error::Database(e)) if e.is_foreign_key_violation() => {
                e.constraint()
            }
            _ => None,
        }
    }

    /// Names a unique violation on `constraint` as the conflict `token`; any
    /// other error passes through unchanged. For the writes whose race the
    /// schema refuses on the caller's behalf.
    #[must_use]
    pub fn conflict_on(self, constraint: &str, token: &'static str) -> Self {
        if self.unique_violation() == Some(constraint) {
            Error::Conflict(token)
        } else {
            self
        }
    }
}

fn sqlstate(error: &sqlx::Error) -> Option<&str> {
    match error {
        sqlx::Error::Database(e) => match e.code() {
            Some(std::borrow::Cow::Borrowed(code)) => Some(code),
            // The Postgres driver hands the code out borrowed; an owned one
            // cannot be returned by reference, and no code we classify is
            // produced owned.
            _ => None,
        },
        _ => None,
    }
}

/// Whether the failure is the database being unreachable, saturated or slow
/// rather than an answer: the pool had no connection, the connection dropped,
/// the server cancelled the statement at `statement_timeout`, or it is shutting
/// down. A gRPC edge answers `UNAVAILABLE` for these and `INTERNAL` for the
/// rest, so a client can tell "try again" from "this is a bug".
pub fn is_unavailable(error: &Error) -> bool {
    match error {
        Error::Conflict(_) => false,
        Error::Query(error) => query_is_unavailable(error),
    }
}

fn query_is_unavailable(error: &sqlx::Error) -> bool {
    match error {
        sqlx::Error::PoolTimedOut
        | sqlx::Error::PoolClosed
        | sqlx::Error::Io(_)
        | sqlx::Error::Protocol(_)
        | sqlx::Error::Tls(_)
        | sqlx::Error::WorkerCrashed => true,
        sqlx::Error::Database(_) => matches!(
            sqlstate(error),
            Some(code)
                if code.starts_with("08")          // connection_exception
                    || code == "57014"             // query_canceled: statement_timeout
                    || code.starts_with("57P")     // admin_shutdown, crash_shutdown, cannot_connect_now
                    || code == "53300"             // too_many_connections
                    || code == "55P03"             // lock_not_available: lock_timeout
        ),
        _ => false,
    }
}

impl From<Error> for wakuwaku::Error {
    fn from(error: Error) -> Self {
        match error {
            Error::Query(e) => wakuwaku::Error::DatabaseError(e),
            // `Io` is the variant wakuwaku documents as "solved by retrying",
            // which is what a conflict is. `status_of` reads it back.
            conflict @ Error::Conflict(_) => wakuwaku::Error::Io(anyhow::Error::new(conflict)),
        }
    }
}

/// The gRPC status a service error deserves.
///
/// `wakuwaku`'s own conversion answers `INTERNAL` for every database error, so
/// anything that wants "try again" told apart from "this is a bug" goes through
/// here instead of through `?`.
pub fn status_of(error: wakuwaku::Error) -> tonic::Status {
    match &error {
        wakuwaku::Error::DatabaseError(e) if query_is_unavailable(e) => {
            tracing::warn!(error = %e, "database unavailable");
            tonic::Status::unavailable("Database unavailable")
        }
        wakuwaku::Error::Io(e) => match e.downcast_ref::<Error>() {
            Some(Error::Conflict(token)) => {
                tonic::Status::aborted(format!("Write conflicted ({token}); retry"))
            }
            _ => tonic::Status::from(error),
        },
        _ => tonic::Status::from(error),
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

    use super::*;

    /// The server bound is the one that matters: the statement is cancelled
    /// there, the error is classified as unavailable, and the connection is
    /// still good for the next query.
    #[sqlx::test(migrations = false)]
    async fn a_statement_past_the_bound_is_cancelled_and_classified(
        pool_options: PgPoolOptions,
        connect_options: PgConnectOptions,
    ) {
        let connect_options = connect_options.options([("statement_timeout", "200")]);
        let pool = pool_options
            .max_connections(1)
            .connect_with(connect_options)
            .await
            .unwrap();
        let started = std::time::Instant::now();
        let error: Error = sqlx::query("SELECT pg_sleep(5)")
            .execute(&pool)
            .await
            .map_err(Error::from)
            .expect_err("the bound fires");
        assert!(
            started.elapsed() < Duration::from_secs(3),
            "{:?}",
            started.elapsed()
        );
        assert!(is_unavailable(&error), "{error}");
        let seven: i64 = sqlx::query_scalar("SELECT 7::bigint")
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(seven, 7, "the connection survived the cancellation");
    }

    #[test]
    fn a_conflict_reaches_the_edge_as_aborted() {
        let status = status_of(Error::Conflict("stale").into());
        assert_eq!(status.code(), tonic::Code::Aborted);
        let status = status_of(Error::Query(sqlx::Error::PoolTimedOut).into());
        assert_eq!(status.code(), tonic::Code::Unavailable);
        let status = status_of(Error::Query(sqlx::Error::RowNotFound).into());
        assert_eq!(status.code(), tonic::Code::Internal);
    }
}
