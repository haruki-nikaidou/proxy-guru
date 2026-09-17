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
use base::db::{Db, Error};
use chrono::{DateTime, Utc};
use db_types::{table_record, text_enum};
use kanau::processor::Processor;
use sqlx::PgConnection;

table_record!(ServerHealthRecordId, "server_health_record");

#[derive(Debug, Clone)]
pub struct ServerHealthRecordEntity {
    pub id: ServerHealthRecordId,
    pub server: ServerId,
    pub status: ServerHealthStatus,
    pub report_time: DateTime<Utc>,
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
    pub start: DateTime<Utc>,
    pub end: DateTime<Utc>,
}

impl Processor<ListServerHealthHistory> for Db {
    type Output = Vec<ServerHealthRecordEntity>;
    type Error = Error;
    #[tracing::instrument(name = "Query:ListServerHealthHistory", skip_all, err)]
    async fn process(&self, input: ListServerHealthHistory) -> Result<Self::Output, Self::Error> {
        Ok(sqlx::query_file_as!(
            ServerHealthRecordEntity,
            "sql/list_server_health_history.sql",
            input.server as _,
            input.start,
            input.end
        )
        .fetch_all(self.db())
        .await?)
    }
}

/// What a health write did, for the caller that has to publish it.
///
/// The three pieces are all read inside the write's own transaction: the row it
/// created, the canvas the server belongs to (a live event is addressed by
/// canvas, and re-reading the server row afterwards could see a moved one) and
/// the status the server held *before* the update, which is the only way to tell
/// a routine report from a status flip.
#[derive(Debug, Clone)]
pub struct HealthWrite {
    pub record: ServerHealthRecordEntity,
    pub canvas: CanvasId,
    pub previous_status: ServerHealthStatus,
    /// The pod rows written alongside the report; empty for a status flip.
    pub pods: Vec<PodHealthRecordEntity>,
}

/// The server row's health fields, locked for the transaction that reads them.
struct ServerHealthBefore {
    canvas: CanvasId,
    health_status: ServerHealthStatus,
    refresh_key_generation: i64,
}

async fn lock_server_health(
    conn: &mut PgConnection,
    server: &ServerId,
) -> Result<Option<ServerHealthBefore>, Error> {
    Ok(sqlx::query_as!(
        ServerHealthBefore,
        r#"SELECT canvas AS "canvas: CanvasId",
                  health_status AS "health_status: ServerHealthStatus",
                  refresh_key_generation
           FROM orchestration_server WHERE id = $1 FOR UPDATE"#,
        server as _
    )
    .fetch_optional(conn)
    .await?)
}

#[allow(clippy::too_many_arguments)]
async fn insert_server_record(
    conn: &mut PgConnection,
    server: &ServerId,
    status: ServerHealthStatus,
    report_time: DateTime<Utc>,
    counters: [i64; 4],
) -> Result<ServerHealthRecordEntity, Error> {
    let [
        upload_bytes,
        download_bytes,
        current_connections,
        max_connections,
    ] = counters;
    Ok(sqlx::query_file_as!(
        ServerHealthRecordEntity,
        "sql/insert_server_record.sql",
        ServerHealthRecordId::new() as _,
        server as _,
        status as _,
        report_time,
        upload_bytes,
        download_bytes,
        current_connections,
        max_connections
    )
    .fetch_one(conn)
    .await?)
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
    pub report_time: DateTime<Utc>,
    pub upload_bytes: i64,
    pub download_bytes: i64,
    pub current_connections: i64,
    pub max_connections: i64,
    pub pods: Vec<NewPodHealthRecord>,
}

impl Processor<InsertServerHealthRecord> for Db {
    /// `None` when the server no longer holds `generation`.
    type Output = Option<HealthWrite>;
    type Error = Error;
    #[tracing::instrument(name = "Query-Transaction:InsertServerHealthRecord", skip_all, err)]
    async fn process(&self, input: InsertServerHealthRecord) -> Result<Self::Output, Self::Error> {
        let mut tx = self.db().begin().await?;
        let Some(before) = lock_server_health(&mut tx, &input.server).await? else {
            return Ok(None);
        };
        if before.refresh_key_generation != input.generation {
            return Ok(None);
        }
        sqlx::query!(
            "UPDATE orchestration_server
             SET last_health_report_at = $2, last_seen_at = $2, health_status = $3
             WHERE id = $1",
            input.server as _,
            input.report_time,
            input.status as _
        )
        .execute(&mut *tx)
        .await?;
        let record = insert_server_record(
            &mut tx,
            &input.server,
            input.status,
            input.report_time,
            [
                input.upload_bytes,
                input.download_bytes,
                input.current_connections,
                input.max_connections,
            ],
        )
        .await?;
        let pods = insert_pod_records(&mut tx, &input.pods).await?;
        tx.commit().await?;
        Ok(Some(HealthWrite {
            record,
            canvas: before.canvas,
            previous_status: before.health_status,
            pods,
        }))
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
    pub now: DateTime<Utc>,
}

impl Processor<SetServerHealthStatus> for Db {
    /// `None` when nothing changed.
    type Output = Option<HealthWrite>;
    type Error = Error;
    #[tracing::instrument(name = "Query-Transaction:SetServerHealthStatus", skip_all, err)]
    async fn process(&self, input: SetServerHealthStatus) -> Result<Self::Output, Self::Error> {
        let mut tx = self.db().begin().await?;
        let Some(before) = lock_server_health(&mut tx, &input.server).await? else {
            return Ok(None);
        };
        if before.health_status == input.status {
            return Ok(None);
        }
        if input
            .generation
            .is_some_and(|generation| generation != before.refresh_key_generation)
        {
            return Ok(None);
        }
        let offline = input.status == ServerHealthStatus::Offline;
        sqlx::query_file!(
            "sql/set_server_health_status.sql",
            input.server as _,
            input.status as _,
            offline
        )
        .execute(&mut *tx)
        .await?;
        let record =
            insert_server_record(&mut tx, &input.server, input.status, input.now, [0; 4]).await?;
        tx.commit().await?;
        Ok(Some(HealthWrite {
            record,
            canvas: before.canvas,
            previous_status: before.health_status,
            pods: Vec::new(),
        }))
    }
}

/// What the liveness sweep decides on.
#[derive(Debug, Clone)]
pub struct ServerLiveness {
    pub id: ServerId,
    pub last_health_report_at: Option<DateTime<Utc>>,
    pub health_status: ServerHealthStatus,
}

/// Every server not already `Offline`; the sweep applies the threshold.
#[derive(Debug)]
pub struct ListServersForLivenessSweep;

impl Processor<ListServersForLivenessSweep> for Db {
    type Output = Vec<ServerLiveness>;
    type Error = Error;
    #[tracing::instrument(name = "Query:ListServersForLivenessSweep", skip_all, err)]
    async fn process(&self, _: ListServersForLivenessSweep) -> Result<Self::Output, Self::Error> {
        Ok(sqlx::query_as!(
            ServerLiveness,
            r#"SELECT id AS "id: ServerId", last_health_report_at,
                      health_status AS "health_status: ServerHealthStatus"
               FROM orchestration_server WHERE health_status <> 'offline'"#
        )
        .fetch_all(self.db())
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
    pub report_time: DateTime<Utc>,
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
    pub start: DateTime<Utc>,
    pub end: DateTime<Utc>,
    pub limit: i64,
}

impl Processor<ListPodHealthHistory> for Db {
    type Output = Vec<PodHealthRecordEntity>;
    type Error = Error;
    #[tracing::instrument(name = "Query:ListPodHealthHistory", skip_all, err)]
    async fn process(&self, input: ListPodHealthHistory) -> Result<Self::Output, Self::Error> {
        Ok(sqlx::query_file_as!(
            PodHealthRecordEntity,
            "sql/list_pod_health_history.sql",
            input.pod as _,
            input.start,
            input.end,
            input.limit
        )
        .fetch_all(self.db())
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
    pub start: DateTime<Utc>,
    pub end: DateTime<Utc>,
    /// The newest rows to keep; `None` keeps them all.
    pub limit: Option<i64>,
}

impl Processor<ListPodHealthSince> for Db {
    type Output = Vec<PodHealthRecordEntity>;
    type Error = Error;
    #[tracing::instrument(name = "Query:ListPodHealthSince", skip_all, err)]
    async fn process(&self, input: ListPodHealthSince) -> Result<Self::Output, Self::Error> {
        // `LIMIT NULL` is no limit at all.
        Ok(sqlx::query_file_as!(
            PodHealthRecordEntity,
            "sql/list_pod_health_since.sql",
            input.pod as _,
            input.start,
            input.end,
            input.limit
        )
        .fetch_all(self.db())
        .await?)
    }
}

/// A pod record to insert; the row id is generated.
#[derive(Debug, Clone)]
pub struct NewPodHealthRecord {
    pub pod: PodId,
    pub status: PodHealthStatus,
    pub message: String,
    pub report_time: DateTime<Utc>,
}

/// Inserts pod records in one statement and returns the rows written.
async fn insert_pod_records(
    conn: &mut PgConnection,
    records: &[NewPodHealthRecord],
) -> Result<Vec<PodHealthRecordEntity>, Error> {
    if records.is_empty() {
        return Ok(Vec::new());
    }
    let ids: Vec<PodHealthRecordId> = records.iter().map(|_| PodHealthRecordId::new()).collect();
    let pods: Vec<&PodId> = records.iter().map(|r| &r.pod).collect();
    let statuses: Vec<PodHealthStatus> = records.iter().map(|r| r.status).collect();
    let messages: Vec<&str> = records.iter().map(|r| r.message.as_str()).collect();
    let times: Vec<DateTime<Utc>> = records.iter().map(|r| r.report_time).collect();
    Ok(sqlx::query_file_as!(
        PodHealthRecordEntity,
        "sql/insert_pod_records.sql",
        &ids as _,
        &pods as _,
        &statuses as _,
        &messages as _,
        &times as _
    )
    .fetch_all(conn)
    .await?)
}

/// Inserts a batch of pod records in one statement. The derivation hook's
/// query (`Deploying` the moment a new revision is published); a report's pod
/// records travel inside [`InsertServerHealthRecord`] instead.
#[derive(Debug)]
pub struct InsertPodHealthRecords {
    pub records: Vec<NewPodHealthRecord>,
}

impl Processor<InsertPodHealthRecords> for Db {
    /// The rows written, so the caller can put them on the live bus.
    type Output = Vec<PodHealthRecordEntity>;
    type Error = Error;
    #[tracing::instrument(name = "Query:InsertPodHealthRecords", skip_all, err)]
    async fn process(&self, input: InsertPodHealthRecords) -> Result<Self::Output, Self::Error> {
        let mut conn = self.db().acquire().await?;
        insert_pod_records(&mut conn, &input.records).await
    }
}

/// Deletes records older than the given cut-offs; the retention cron's query.
#[derive(Debug)]
pub struct DeleteHealthRecordsBefore {
    pub server_records_before: DateTime<Utc>,
    pub pod_records_before: DateTime<Utc>,
}

impl Processor<DeleteHealthRecordsBefore> for Db {
    type Output = ();
    type Error = Error;
    #[tracing::instrument(name = "Query:DeleteHealthRecordsBefore", skip_all, err)]
    async fn process(&self, input: DeleteHealthRecordsBefore) -> Result<Self::Output, Self::Error> {
        sqlx::query!(
            "DELETE FROM server_health_record WHERE report_time < $1",
            input.server_records_before
        )
        .execute(self.db())
        .await?;
        sqlx::query!(
            "DELETE FROM pod_health_record WHERE report_time < $1",
            input.pod_records_before
        )
        .execute(self.db())
        .await?;
        Ok(())
    }
}
