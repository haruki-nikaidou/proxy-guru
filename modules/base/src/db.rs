//! The database handle every service holds: a `SurrealProcessor` that cannot wait forever.
//!
//! # Why this exists
//!
//! The SurrealDB client has **no request timeout of its own**, on any remote engine:
//! `query_timeout` on the SDK's config object reaches the embedded engines only. A request
//! whose answer is lost is therefore an `.await` that never returns.
//!
//! That is not a hypothetical. Two workers hung on it on 2026-09-16, one inside an update
//! poll and one inside a config acknowledgement, and neither recovered until the master was
//! restarted an hour later. The engine they were on multiplexes every query in a process
//! through one WebSocket and one router task; when that router cannot match a response to a
//! caller it drops the response and logs, and its recovery on reconnect only covers sessions
//! its map still holds as `Ok` (the SDK's own `fail_all_pending_requests` comment says as
//! much, citing surrealdb/surrealdb#7037).
//!
//! The control plane has since moved to `http://`, where each query is an independent request
//! and there is no shared pipe to lose answers in. This bound stays regardless: it is what
//! keeps any future engine, or any network, from turning one lost answer into a process that
//! waits for it forever.
//!
//! # What it fixes
//!
//! Nothing, on its own. It turns an await that never returns into an ordinary error, and
//! almost every caller already knows what to do with one: a worker session ends and
//! reconnects, an AMQP hook nacks and the broker redelivers, the config poller logs and takes
//! the next tick, a live view retries. That recovery code was always correct; it simply never
//! ran, because the future it waited on never resolved.
//!
//! # What it does not do
//!
//! A timeout is not a cancellation. The statement keeps running on the server, so a write
//! that timed out may still commit, and the client keeps one entry in its pending map until
//! an answer arrives or the connection resets.
//!
//! **It therefore does not retry.** Retrying here would retry writes as well as reads, and a
//! `CREATE` whose answer was lost after it committed would be applied twice. Only a caller
//! knows whether its operation is idempotent, so the retry lives with the callers that are:
//! the two authentication middlewares, whose lookups are pure reads.

use kanau::processor::Processor;
use std::time::Duration;
use surrealdb::Surreal;
use surrealdb::engine::any::Any;
use surrealdb_types::ConnectionError;
use wakuwaku::surreal::SurrealProcessor;

/// How long one query may take before it is called lost.
///
/// Measured against the live database while the fleet was running, the queries that actually
/// lose their answers return in 28-81 ms. Three seconds is more than thirty times the honest
/// worst case and still well under the thirty second watch session lease, so a stuck lease
/// renewal fails in time for its stream to end cleanly. It is a backstop, not a latency
/// budget: a caller that needs a tighter bound puts one at its own edge.
///
/// It started at ten seconds. That was long enough that a caller which retried once waited
/// twenty seconds before answering, which is what a lost answer felt like from the dashboard.
pub const DEFAULT_QUERY_TIMEOUT: Duration = Duration::from_secs(3);

/// A [`SurrealProcessor`] whose queries are bounded.
///
/// Every service holds one of these rather than the raw processor, so that every query in
/// the workspace is bounded by construction — including queries written after this type was.
/// The query implementations themselves are untouched: they are still written for
/// `SurrealProcessor`, and the blanket [`Processor`] implementation below forwards to them.
///
/// A legitimately slow call does not need [`Self::raw`]; it can widen its own bound:
///
/// ```ignore
/// self.db.clone().with_timeout(Duration::from_secs(60)).process(BigBatch { .. }).await
/// ```
#[derive(Debug, Clone)]
pub struct Db {
    /// Deliberately private, and deliberately not exposed by a `Deref`. A `Deref` to the
    /// inner processor would compile and behave correctly today, then silently start
    /// resolving `process` to the *unbounded* implementation the first time somebody writes
    /// a query whose error type is not `surrealdb::Error`.
    inner: SurrealProcessor,
    limit: Duration,
}

impl Db {
    /// Wraps a connected handle with [`DEFAULT_QUERY_TIMEOUT`].
    pub fn new(executor: Surreal<Any>) -> Self {
        Self {
            inner: SurrealProcessor::new(executor),
            limit: DEFAULT_QUERY_TIMEOUT,
        }
    }

    /// The same handle with a different bound.
    #[must_use]
    pub fn with_timeout(self, limit: Duration) -> Self {
        Self { limit, ..self }
    }

    /// The bound one query gets.
    pub fn timeout(&self) -> Duration {
        self.limit
    }

    /// The raw executor, for the few callers that must build a statement by hand.
    ///
    /// **Anything awaited through this is not bounded.** Named so that `grep raw()` finds
    /// every such place in one pass.
    pub fn raw(&self) -> &Surreal<Any> {
        // Through the inner processor, not the field: that accessor is what emits the
        // `monotonic_counter.sql` metric, and reading the field directly would stop the
        // remaining hand-written statements from being counted.
        self.inner.db()
    }
}

/// The error a lost query produces, naming the query that was lost.
///
/// A connection-class error on purpose: `Error::connection_details()` is what the gRPC edge
/// reads to answer `UNAVAILABLE` rather than `INTERNAL`, so a caller can tell "try again"
/// from "this is a bug".
fn timed_out<I>(limit: Duration) -> surrealdb::Error {
    surrealdb_types::Error::connection(
        format!(
            "{} did not answer within {limit:?}",
            std::any::type_name::<I>()
        ),
        ConnectionError::ConnectionFailed,
    )
}

/// Whether a database error means "the query never got an answer" rather than "the query was
/// answered, with a refusal".
///
/// True for a timeout from [`Db`] and for a genuine transport failure, both of which the
/// caller should retry. A gRPC edge answers `UNAVAILABLE` for these and `INTERNAL` for the
/// rest, so a client can tell the two apart; before this existed every database failure
/// looked like a bug in the control plane.
pub fn is_unavailable(error: &surrealdb::Error) -> bool {
    error.connection_details().is_some()
}

/// The gRPC status a service error deserves, with connection failures told apart from faults.
///
/// `wakuwaku`'s own conversion answers `INTERNAL` for every database error, so anything that
/// wants the distinction goes through here instead of through `?`.
pub fn status_of(error: wakuwaku::Error) -> tonic::Status {
    match &error {
        wakuwaku::Error::SurrealDbError(e) if is_unavailable(e) => {
            tracing::warn!(error = %e, "database unavailable");
            tonic::Status::unavailable("Database unavailable")
        }
        _ => tonic::Status::from(error),
    }
}

impl<I: Send> Processor<I> for Db
where
    SurrealProcessor: Processor<I, Error = surrealdb::Error>,
{
    type Output = <SurrealProcessor as Processor<I>>::Output;
    /// Pinned to `surrealdb::Error` rather than projected from the inner processor: the
    /// timeout arm has to produce a value of this type, which only the equality constraint
    /// in the `where` clause makes provable.
    type Error = surrealdb::Error;

    fn process(&self, input: I) -> impl Future<Output = Result<Self::Output, Self::Error>> + Send {
        let limit = self.limit;
        let inner = self.inner.process(input);
        // The timer is created inside the block so the clock starts when the future is first
        // polled, not when `process` was called.
        async move {
            match tokio::time::timeout(limit, inner).await {
                Ok(result) => result,
                Err(_) => Err(timed_out::<I>(limit)),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

    use super::*;

    /// A query that really blocks, so the bound is tested against the thing it exists for
    /// rather than against a mock. `sleep()` is a SurrealQL builtin and is permitted by the
    /// default capabilities of an in-memory instance.
    struct Sleep {
        seconds: u32,
    }

    impl Processor<Sleep> for SurrealProcessor {
        type Output = ();
        type Error = surrealdb::Error;
        async fn process(&self, input: Sleep) -> Result<Self::Output, Self::Error> {
            // No `TIMEOUT` clause: the engine clamps `sleep()` to the statement timeout,
            // which would defeat the point.
            self.db()
                .query(format!("RETURN sleep({}s)", input.seconds))
                .await?
                .check()?;
            Ok(())
        }
    }

    /// Answers immediately; proves the wrapper is a pass-through when nothing is wrong.
    struct Ping;

    impl Processor<Ping> for SurrealProcessor {
        type Output = i64;
        type Error = surrealdb::Error;
        async fn process(&self, _: Ping) -> Result<Self::Output, Self::Error> {
            let mut resp = self.db().query("RETURN 7").await?;
            Ok(resp.take::<Option<i64>>(0)?.unwrap_or_default())
        }
    }

    async fn memory() -> Surreal<Any> {
        let db = surrealdb::engine::any::connect("mem://").await.unwrap();
        db.use_ns("test").use_db("test").await.unwrap();
        db
    }

    #[tokio::test]
    async fn a_query_that_does_not_answer_in_time_becomes_an_error() {
        let db = Db::new(memory().await).with_timeout(Duration::from_millis(100));
        let started = std::time::Instant::now();
        let error = db
            .process(Sleep { seconds: 5 })
            .await
            .expect_err("the bound fires");
        assert!(
            started.elapsed() < Duration::from_secs(4),
            "gave up on the query instead of waiting it out, took {:?}",
            started.elapsed()
        );
        assert!(
            error.to_string().contains("Sleep"),
            "the error names the query that was lost: {error}"
        );
        assert!(
            error.connection_details().is_some(),
            "a connection-class error, so the edge can answer UNAVAILABLE: {error}"
        );
    }

    #[tokio::test]
    async fn a_query_that_answers_passes_straight_through() {
        let db = Db::new(memory().await);
        assert_eq!(db.process(Ping).await.unwrap(), 7);
    }
}
