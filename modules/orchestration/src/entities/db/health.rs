//! Health history reported by workers.
//!
//! One [`ServerHealthRecordEntity`] per `HealthReport` a worker sends (the
//! counters are deltas since the previous report) and one
//! [`PodHealthRecordEntity`] per pod per report. Both are raw and trimmed by
//! the retention cron; the current server status is denormalised on
//! `orchestration_server.health_status`.

use crate::entities::db::canvas::CanvasId;
use crate::entities::db::pod::PodId;
use crate::entities::db::server::ServerId;
use crate::entities::db::tables::{
    OrchestrationCanvas, OrchestrationPod, OrchestrationServer, PodHealthRecord, ServerHealthRecord,
};
use base::db::Error;
use db_types::{table_record, text_enum};
use sqlx::{PgConnection, Postgres};
use time::OffsetDateTime;
use wakuwaku::entity::sqlx::backend::PgSource;
use wakuwaku::entity::sqlx::transaction::{Either, Tx, TxEnd, TxFlow, TxState};
use wakuwaku::entity::{Execute, Query, Read, Write};

table_record!(ServerHealthRecordId, "server_health_record");

#[derive(Debug, Clone)]
pub struct ServerHealthRecordEntity {
    pub id: ServerHealthRecordId,
    pub server: ServerId,
    pub status: ServerHealthStatus,
    pub report_time: OffsetDateTime,
    pub upload_bytes: i64,
    pub download_bytes: i64,
    pub current_connections: i64,
    pub max_connections: i64,
}

/// The `rkyv` derives put this enum on the live bus unchanged
/// ([`crate::events::live::ServerHealthLive`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
pub enum ServerHealthStatus {
    /// The server is online and runs what it was asked to.
    Online,
    /// The server is reachable but lags `desired` past the grace period or
    /// failed to apply the latest revision (as a whole or for some pods).
    Degraded,
    /// The server is offline or cannot reach the master.
    Offline,
}
text_enum!(ServerHealthStatus {
    Online => "online",
    Degraded => "degraded",
    Offline => "offline",
});

impl Default for ServerHealthStatus {
    /// A server nobody has heard from.
    fn default() -> Self {
        Self::Offline
    }
}

/// Server records in `[start, end]`, oldest first.
#[derive(Debug)]
pub struct ListServerHealthHistory {
    pub server: ServerId,
    pub start: OffsetDateTime,
    pub end: OffsetDateTime,
}

impl Query for ListServerHealthHistory {
    type Effect = Read<ServerHealthRecord>;
}

impl Execute<PgSource> for ListServerHealthHistory {
    type Output = Vec<ServerHealthRecordEntity>;
    type Error = Error;
    #[tracing::instrument(name = "Query:ListServerHealthHistory", skip_all, err)]
    async fn execute(self, conn: &mut PgConnection) -> Result<Self::Output, Self::Error> {
        Ok(sqlx::query_file_as!(
            ServerHealthRecordEntity,
            "sql/list_server_health_history.sql",
            self.server as _,
            self.start,
            self.end
        )
        .fetch_all(conn)
        .await?)
    }
}

/// What a health write did, for the caller that has to publish it.
///
/// Everything here is read inside the write's own transaction: the row it
/// created, the canvas the server belongs to (a live event is addressed by
/// canvas, and re-reading the server row afterwards could see a moved one), the
/// status the server held *before* the update — the only way to tell a routine
/// report from a status flip — and the two labels a notification needs, so the
/// module that phrases one needs no database of its own.
#[derive(Debug, Clone)]
pub struct HealthWrite {
    pub record: ServerHealthRecordEntity,
    pub canvas: CanvasId,
    pub server_name: String,
    pub canvas_name: String,
    pub previous_status: ServerHealthStatus,
    /// The pod rows written alongside the report; empty for a status flip.
    pub pods: Vec<PodHealthWrite>,
}

/// The server row's health fields, locked for the transaction that reads them,
/// plus the labels [`HealthWrite`] carries.
struct ServerHealthBefore {
    canvas: CanvasId,
    name: String,
    canvas_name: String,
    health_status: ServerHealthStatus,
    refresh_key_generation: i64,
}

/// Locks the server row (`FOR UPDATE`, hence a write) and reads its health fields.
struct LockServerHealth<'a> {
    server: &'a ServerId,
}

impl Query for LockServerHealth<'_> {
    type Effect = (Write<OrchestrationServer>, Read<OrchestrationCanvas>);
}

impl Execute<PgSource> for LockServerHealth<'_> {
    type Output = Option<ServerHealthBefore>;
    type Error = Error;
    async fn execute(self, conn: &mut PgConnection) -> Result<Self::Output, Self::Error> {
        Ok(sqlx::query_file_as!(
            ServerHealthBefore,
            "sql/lock_server_health.sql",
            self.server as _
        )
        .fetch_optional(conn)
        .await?)
    }
}

/// Inserts one server record; `counters` are upload, download, current and
/// max connections.
struct InsertServerRecord<'a> {
    server: &'a ServerId,
    status: ServerHealthStatus,
    report_time: OffsetDateTime,
    counters: [i64; 4],
}

impl Query for InsertServerRecord<'_> {
    type Effect = Write<ServerHealthRecord>;
}

impl Execute<PgSource> for InsertServerRecord<'_> {
    type Output = ServerHealthRecordEntity;
    type Error = Error;
    async fn execute(self, conn: &mut PgConnection) -> Result<Self::Output, Self::Error> {
        let [
            upload_bytes,
            download_bytes,
            current_connections,
            max_connections,
        ] = self.counters;
        Ok(sqlx::query_file_as!(
            ServerHealthRecordEntity,
            "sql/insert_server_record.sql",
            ServerHealthRecordId::new() as _,
            self.server as _,
            self.status as _,
            self.report_time,
            upload_bytes,
            download_bytes,
            current_connections,
            max_connections
        )
        .fetch_one(conn)
        .await?)
    }
}

/// Advances a reporting server's liveness and status.
struct MarkServerReported<'a> {
    server: &'a ServerId,
    report_time: OffsetDateTime,
    status: ServerHealthStatus,
}

impl Query for MarkServerReported<'_> {
    type Effect = Write<OrchestrationServer>;
}

impl Execute<PgSource> for MarkServerReported<'_> {
    type Output = ();
    type Error = Error;
    async fn execute(self, conn: &mut PgConnection) -> Result<(), Error> {
        sqlx::query!(
            "UPDATE orchestration_server
             SET last_health_report_at = $2, last_seen_at = $2, health_status = $3
             WHERE id = $1",
            self.server as _,
            self.report_time,
            self.status as _
        )
        .execute(conn)
        .await?;
        Ok(())
    }
}

/// Writes a status that is not a report; going offline also hands the watch
/// session back.
struct WriteServerHealthStatus<'a> {
    server: &'a ServerId,
    status: ServerHealthStatus,
    offline: bool,
}

impl Query for WriteServerHealthStatus<'_> {
    type Effect = Write<OrchestrationServer>;
}

impl Execute<PgSource> for WriteServerHealthStatus<'_> {
    type Output = ();
    type Error = Error;
    async fn execute(self, conn: &mut PgConnection) -> Result<(), Error> {
        sqlx::query_file!(
            "sql/set_server_health_status.sql",
            self.server as _,
            self.status as _,
            self.offline
        )
        .execute(conn)
        .await?;
        Ok(())
    }
}

/// One accepted report, in one conditional transaction: the server's liveness
/// (`last_health_report_at`, and `last_seen_at` alongside the watch heartbeat)
/// is advanced only while it still belongs to the reporting session
/// (`refresh_key_generation`), and the server and pod records are written only
/// if it did — a stream that lost its server to a re-registration writes nothing.
#[derive(Debug)]
pub struct InsertServerHealthRecord {
    pub server: ServerId,
    pub generation: i64,
    pub status: ServerHealthStatus,
    pub report_time: OffsetDateTime,
    pub upload_bytes: i64,
    pub download_bytes: i64,
    pub current_connections: i64,
    pub max_connections: i64,
    pub pods: Vec<NewPodHealthRecord>,
}

/// Run it with `db.process(Transaction(InsertServerHealthRecordFlow::new(..)))`;
/// commits `None` when the server no longer holds `generation`.
pub enum InsertServerHealthRecordFlow {
    Start(InsertServerHealthRecord),
    End(TxEnd<InsertServerHealthRecordFlow>),
}

impl InsertServerHealthRecordFlow {
    pub fn new(input: InsertServerHealthRecord) -> Self {
        InsertServerHealthRecordFlow::Start(input)
    }
}

impl Query for InsertServerHealthRecordFlow {
    type Effect = (
        Write<OrchestrationServer>,
        Write<ServerHealthRecord>,
        Write<PodHealthRecord>,
        Read<OrchestrationCanvas>,
        Read<OrchestrationPod>,
    );
}

impl From<TxEnd<InsertServerHealthRecordFlow>> for InsertServerHealthRecordFlow {
    fn from(end: TxEnd<InsertServerHealthRecordFlow>) -> Self {
        InsertServerHealthRecordFlow::End(end)
    }
}

impl TxFlow for InsertServerHealthRecordFlow {
    type Database = Postgres;
    type Commit = Option<HealthWrite>;
    type Abort = Error;
    type Report = ();

    #[tracing::instrument(name = "Transaction:InsertServerHealthRecord", skip_all)]
    async fn step(tx: Tx<'_, Self>, state: Self) -> TxState<'_, Self> {
        let InsertServerHealthRecordFlow::Start(input) = state else {
            return tx.end(state).await;
        };
        let (tx, r) = tx
            .run(LockServerHealth {
                server: &input.server,
            })
            .await;
        let before = match r {
            Ok(Some(before)) => before,
            Ok(None) => return base::db::commit(tx, None).await,
            Err(e) => return base::db::rollback(tx, e).await,
        };
        if before.refresh_key_generation != input.generation {
            return base::db::commit(tx, None).await;
        }
        let (tx, r) = tx
            .run(MarkServerReported {
                server: &input.server,
                report_time: input.report_time,
                status: input.status,
            })
            .await;
        if let Err(e) = r {
            return base::db::rollback(tx, e).await;
        }
        let (tx, r) = tx
            .run(InsertServerRecord {
                server: &input.server,
                status: input.status,
                report_time: input.report_time,
                counters: [
                    input.upload_bytes,
                    input.download_bytes,
                    input.current_connections,
                    input.max_connections,
                ],
            })
            .await;
        let record = match r {
            Ok(record) => record,
            Err(e) => return base::db::rollback(tx, e).await,
        };
        let (tx, r) = tx
            .run(InsertPodRecords {
                records: &input.pods,
            })
            .await;
        let pods = match r {
            Ok(pods) => pods,
            Err(e) => return base::db::rollback(tx, e).await,
        };
        let write = HealthWrite {
            record,
            canvas: before.canvas,
            server_name: before.name,
            canvas_name: before.canvas_name,
            previous_status: before.health_status,
            pods,
        };
        base::db::commit(tx, Some(write)).await
    }

    fn finished(self) -> Either<Self, TxEnd<Self>> {
        match self {
            InsertServerHealthRecordFlow::End(end) => Either::Right(end),
            state => Either::Left(state),
        }
    }
}

/// A status change that is not a report (the master noticed the worker is gone):
/// writes the status and a zero-counter record, but leaves
/// `last_health_report_at` alone — only the worker's own report advances it.
///
/// Conditional on the status actually changing and, when `generation` is given,
/// on the server still belonging to that session: a stream closing after the
/// worker re-registered must not clobber the successor's status.
///
/// Going offline also hands the watch session back: the lease is dropped so the
/// next `Register` is accepted at once, and the epoch moves so the stream that
/// held it — possibly a zombie behind a proxy that never noticed its worker die —
/// fails its next renew and ends. Any other status leaves both alone.
#[derive(Debug)]
pub struct SetServerHealthStatus {
    pub server: ServerId,
    pub generation: Option<i64>,
    pub status: ServerHealthStatus,
    pub now: OffsetDateTime,
}

/// Run it with `db.process(Transaction(SetServerHealthStatusFlow::new(..)))`;
/// commits `None` when nothing changed.
pub enum SetServerHealthStatusFlow {
    Start(SetServerHealthStatus),
    End(TxEnd<SetServerHealthStatusFlow>),
}

impl SetServerHealthStatusFlow {
    pub fn new(input: SetServerHealthStatus) -> Self {
        SetServerHealthStatusFlow::Start(input)
    }
}

impl Query for SetServerHealthStatusFlow {
    type Effect = (
        Write<OrchestrationServer>,
        Write<ServerHealthRecord>,
        Read<OrchestrationCanvas>,
    );
}

impl From<TxEnd<SetServerHealthStatusFlow>> for SetServerHealthStatusFlow {
    fn from(end: TxEnd<SetServerHealthStatusFlow>) -> Self {
        SetServerHealthStatusFlow::End(end)
    }
}

impl TxFlow for SetServerHealthStatusFlow {
    type Database = Postgres;
    type Commit = Option<HealthWrite>;
    type Abort = Error;
    type Report = ();

    #[tracing::instrument(name = "Transaction:SetServerHealthStatus", skip_all)]
    async fn step(tx: Tx<'_, Self>, state: Self) -> TxState<'_, Self> {
        let SetServerHealthStatusFlow::Start(input) = state else {
            return tx.end(state).await;
        };
        let (tx, r) = tx
            .run(LockServerHealth {
                server: &input.server,
            })
            .await;
        let before = match r {
            Ok(Some(before)) => before,
            Ok(None) => return base::db::commit(tx, None).await,
            Err(e) => return base::db::rollback(tx, e).await,
        };
        if before.health_status == input.status {
            return base::db::commit(tx, None).await;
        }
        if input
            .generation
            .is_some_and(|generation| generation != before.refresh_key_generation)
        {
            return base::db::commit(tx, None).await;
        }
        let (tx, r) = tx
            .run(WriteServerHealthStatus {
                server: &input.server,
                status: input.status,
                offline: input.status == ServerHealthStatus::Offline,
            })
            .await;
        if let Err(e) = r {
            return base::db::rollback(tx, e).await;
        }
        let (tx, r) = tx
            .run(InsertServerRecord {
                server: &input.server,
                status: input.status,
                report_time: input.now,
                counters: [0; 4],
            })
            .await;
        let record = match r {
            Ok(record) => record,
            Err(e) => return base::db::rollback(tx, e).await,
        };
        let write = HealthWrite {
            record,
            canvas: before.canvas,
            server_name: before.name,
            canvas_name: before.canvas_name,
            previous_status: before.health_status,
            pods: Vec::new(),
        };
        base::db::commit(tx, Some(write)).await
    }

    fn finished(self) -> Either<Self, TxEnd<Self>> {
        match self {
            SetServerHealthStatusFlow::End(end) => Either::Right(end),
            state => Either::Left(state),
        }
    }
}

/// What the liveness sweep decides on.
#[derive(Debug, Clone)]
pub struct ServerLiveness {
    pub id: ServerId,
    pub last_health_report_at: Option<OffsetDateTime>,
    pub health_status: ServerHealthStatus,
}

/// Every server not already `Offline`; the sweep applies the threshold.
#[derive(Debug)]
pub struct ListServersForLivenessSweep;

impl Query for ListServersForLivenessSweep {
    type Effect = Read<OrchestrationServer>;
}

impl Execute<PgSource> for ListServersForLivenessSweep {
    type Output = Vec<ServerLiveness>;
    type Error = Error;
    #[tracing::instrument(name = "Query:ListServersForLivenessSweep", skip_all, err)]
    async fn execute(self, conn: &mut PgConnection) -> Result<Self::Output, Self::Error> {
        Ok(sqlx::query_as!(
            ServerLiveness,
            r#"SELECT id AS "id: ServerId", last_health_report_at,
                      health_status AS "health_status: ServerHealthStatus"
               FROM orchestration_server WHERE health_status <> 'offline'"#
        )
        .fetch_all(conn)
        .await?)
    }
}

table_record!(PodHealthRecordId, "pod_health_record");

#[derive(Debug, Clone)]
pub struct PodHealthRecordEntity {
    pub id: PodHealthRecordId,
    pub pod: PodId,
    pub status: PodHealthStatus,
    pub message: String,
    pub report_time: OffsetDateTime,
}

/// The `rkyv` derives put this enum on the live bus unchanged
/// ([`crate::events::live::PodHealthLive`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
pub enum PodHealthStatus {
    /// The pod's forwarding is applied on its server and runs.
    Ready,
    /// A newer revision changing the pod is derived but not yet applied.
    Deploying,
    /// The pod failed to apply or to run.
    Failed,
}
text_enum!(PodHealthStatus {
    Ready => "ready",
    Deploying => "deploying",
    Failed => "failed",
});

/// Pod records in `[start, end]`, newest first, at most `limit`.
///
/// `id` breaks the tie: one event writes a whole batch of rows with the same
/// `report_time`, so `report_time` alone is not a total order and a paging
/// reader could see the same row twice or miss one.
#[derive(Debug)]
pub struct ListPodHealthHistory {
    pub pod: PodId,
    pub start: OffsetDateTime,
    pub end: OffsetDateTime,
    pub limit: i64,
}

impl Query for ListPodHealthHistory {
    type Effect = Read<PodHealthRecord>;
}

impl Execute<PgSource> for ListPodHealthHistory {
    type Output = Vec<PodHealthRecordEntity>;
    type Error = Error;
    #[tracing::instrument(name = "Query:ListPodHealthHistory", skip_all, err)]
    async fn execute(self, conn: &mut PgConnection) -> Result<Self::Output, Self::Error> {
        Ok(sqlx::query_file_as!(
            PodHealthRecordEntity,
            "sql/list_pod_health_history.sql",
            self.pod as _,
            self.start,
            self.end,
            self.limit
        )
        .fetch_all(conn)
        .await?)
    }
}

/// Pod records in `[start, end]`, oldest first: a live stream's opening
/// snapshot and its recovery read.
///
/// Pod rows are a per-interval series, not a handful of deployment events:
/// every health report writes one row per reported pod, so a week at the
/// default cadence is ~40,000 rows of one pod — over gRPC's default 4 MiB
/// receive limit as a single snapshot once the rows carry a failure message.
/// The snapshot therefore keeps only the newest `limit` rows, the page size
/// `ListPodHealthHistory` answers with; the recovery read after a gap passes
/// `None` and gets every row since the watermark, one message each.
///
/// `report_time` alone is the stream's watermark. Every writer
/// (`InsertPodHealthRecords` from the derive hook, `AckConfig`,
/// `RecordHealthReport`) writes at most one row *per pod* per statement, so
/// two rows of one pod can only share a `report_time` if two writes landed in
/// the same microsecond, which nothing in the fleet does; `id` only makes the
/// order deterministic.
#[derive(Debug)]
pub struct ListPodHealthSince {
    pub pod: PodId,
    pub start: OffsetDateTime,
    pub end: OffsetDateTime,
    /// The newest rows to keep; `None` keeps them all.
    pub limit: Option<i64>,
}

impl Query for ListPodHealthSince {
    type Effect = Read<PodHealthRecord>;
}

impl Execute<PgSource> for ListPodHealthSince {
    type Output = Vec<PodHealthRecordEntity>;
    type Error = Error;
    #[tracing::instrument(name = "Query:ListPodHealthSince", skip_all, err)]
    async fn execute(self, conn: &mut PgConnection) -> Result<Self::Output, Self::Error> {
        // `LIMIT NULL` is no limit at all.
        Ok(sqlx::query_file_as!(
            PodHealthRecordEntity,
            "sql/list_pod_health_since.sql",
            self.pod as _,
            self.start,
            self.end,
            self.limit
        )
        .fetch_all(conn)
        .await?)
    }
}

/// A pod record to insert; the row id is generated.
#[derive(Debug, Clone)]
pub struct NewPodHealthRecord {
    pub pod: PodId,
    pub status: PodHealthStatus,
    pub message: String,
    pub report_time: OffsetDateTime,
}

/// A pod row written, with the labels a notification needs. They are read in
/// the same statement as the insert, so nothing downstream has to re-read the
/// pod — or find it already moved.
#[derive(Debug, Clone)]
pub struct PodHealthWrite {
    pub record: PodHealthRecordEntity,
    pub pod_name: String,
    pub canvas: CanvasId,
    pub canvas_name: String,
}

/// The flat shape the statement selects: `query_as!` matches columns to fields
/// one for one, so the entity's nesting is done here in Rust.
struct PodHealthWriteRow {
    id: PodHealthRecordId,
    pod: PodId,
    status: PodHealthStatus,
    message: String,
    report_time: OffsetDateTime,
    pod_name: String,
    canvas: CanvasId,
    canvas_name: String,
}

impl From<PodHealthWriteRow> for PodHealthWrite {
    fn from(row: PodHealthWriteRow) -> Self {
        Self {
            record: PodHealthRecordEntity {
                id: row.id,
                pod: row.pod,
                status: row.status,
                message: row.message,
                report_time: row.report_time,
            },
            pod_name: row.pod_name,
            canvas: row.canvas,
            canvas_name: row.canvas_name,
        }
    }
}

/// Inserts pod records in one statement and returns the rows written. A record
/// whose pod no longer exists is skipped: a worker reports the pods of the
/// revision it runs, which may name one deleted since.
struct InsertPodRecords<'a> {
    records: &'a [NewPodHealthRecord],
}

impl Query for InsertPodRecords<'_> {
    type Effect = (
        Write<PodHealthRecord>,
        Read<OrchestrationPod>,
        Read<OrchestrationCanvas>,
    );
}

impl Execute<PgSource> for InsertPodRecords<'_> {
    type Output = Vec<PodHealthWrite>;
    type Error = Error;
    async fn execute(self, conn: &mut PgConnection) -> Result<Self::Output, Self::Error> {
        let records = self.records;
        if records.is_empty() {
            return Ok(Vec::new());
        }
        let ids: Vec<PodHealthRecordId> =
            records.iter().map(|_| PodHealthRecordId::new()).collect();
        let pods: Vec<&PodId> = records.iter().map(|r| &r.pod).collect();
        let statuses: Vec<PodHealthStatus> = records.iter().map(|r| r.status).collect();
        let messages: Vec<&str> = records.iter().map(|r| r.message.as_str()).collect();
        let times: Vec<OffsetDateTime> = records.iter().map(|r| r.report_time).collect();
        Ok(sqlx::query_file_as!(
            PodHealthWriteRow,
            "sql/insert_pod_records.sql",
            &ids as _,
            &pods as _,
            &statuses as _,
            &messages as _,
            &times as _
        )
        .fetch_all(conn)
        .await?
        .into_iter()
        .map(Into::into)
        .collect())
    }
}

/// Inserts a batch of pod records in one statement, skipping any whose pod no
/// longer exists. The derivation hook's query (`Deploying` the moment a new
/// revision is published); a report's pod records travel inside
/// [`InsertServerHealthRecord`] instead.
#[derive(Debug)]
pub struct InsertPodHealthRecords {
    pub records: Vec<NewPodHealthRecord>,
}

impl Query for InsertPodHealthRecords {
    type Effect = (
        Write<PodHealthRecord>,
        Read<OrchestrationPod>,
        Read<OrchestrationCanvas>,
    );
}

impl Execute<PgSource> for InsertPodHealthRecords {
    /// The rows written, so the caller can put them on the live bus and
    /// announce them.
    type Output = Vec<PodHealthWrite>;
    type Error = Error;
    #[tracing::instrument(name = "Query:InsertPodHealthRecords", skip_all, err)]
    async fn execute(self, conn: &mut PgConnection) -> Result<Self::Output, Self::Error> {
        InsertPodRecords {
            records: &self.records,
        }
        .execute(conn)
        .await
    }
}

/// Deletes records older than the given cut-offs; the retention cron's query.
#[derive(Debug)]
pub struct DeleteHealthRecordsBefore {
    pub server_records_before: OffsetDateTime,
    pub pod_records_before: OffsetDateTime,
}

impl Query for DeleteHealthRecordsBefore {
    type Effect = (Write<ServerHealthRecord>, Write<PodHealthRecord>);
}

impl Execute<PgSource> for DeleteHealthRecordsBefore {
    type Output = ();
    type Error = Error;
    #[tracing::instrument(name = "Query:DeleteHealthRecordsBefore", skip_all, err)]
    async fn execute(self, conn: &mut PgConnection) -> Result<Self::Output, Self::Error> {
        sqlx::query!(
            "DELETE FROM server_health_record WHERE report_time < $1",
            self.server_records_before
        )
        .execute(&mut *conn)
        .await?;
        sqlx::query!(
            "DELETE FROM pod_health_record WHERE report_time < $1",
            self.pod_records_before
        )
        .execute(conn)
        .await?;
        Ok(())
    }
}
