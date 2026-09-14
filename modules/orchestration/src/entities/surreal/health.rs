//! Health history reported by workers.
//!
//! One [`ServerHealthRecordEntity`] per `HealthReport` a worker sends (the
//! counters are deltas since the previous report) and one
//! [`NodeHealthRecordEntity`] per node per report. Both are raw and trimmed by
//! the retention cron; the current server status is denormalised on
//! `orchestration_server.health_status`.

use crate::entities::surreal::node::NodeId;
use crate::entities::surreal::server::ServerId;
use chrono::{DateTime, Utc};
use kanau::processor::Processor;
use newtype_record_id::table_record;
use surrealdb_types::SurrealValue;
use wakuwaku::surreal::SurrealProcessor;

table_record!(ServerHealthRecordId, "server_health_record");

#[derive(Debug, Clone, SurrealValue)]
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

#[derive(Debug, Clone, Copy, PartialEq, Eq, SurrealValue)]
#[surreal(untagged, rename_all = "snake_case")]
pub enum ServerHealthStatus {
    /// The server is online and runs what it was asked to.
    Online,
    /// The server is reachable but lags `desired` past the grace period or
    /// failed to apply the latest revision (as a whole or for some pods).
    Degraded,
    /// The server is offline or cannot reach the master.
    Offline,
}

impl Default for ServerHealthStatus {
    /// A server nobody has heard from.
    fn default() -> Self {
        Self::Offline
    }
}

impl ServerHealthStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Online => "online",
            Self::Degraded => "degraded",
            Self::Offline => "offline",
        }
    }
}

/// Server records in `[start, end]`, oldest first.
#[derive(Debug)]
pub struct ListServerHealthHistory {
    pub server: ServerId,
    pub start: DateTime<Utc>,
    pub end: DateTime<Utc>,
}

impl Processor<ListServerHealthHistory> for SurrealProcessor {
    type Output = Vec<ServerHealthRecordEntity>;
    type Error = surrealdb::Error;
    #[tracing::instrument(name = "Query:ListServerHealthHistory", skip_all, err)]
    async fn process(&self, input: ListServerHealthHistory) -> Result<Self::Output, Self::Error> {
        let mut resp = self
            .db()
            .query(
                "SELECT * FROM server_health_record \
                 WHERE server = $server AND report_time >= $start AND report_time <= $end \
                 ORDER BY report_time ASC",
            )
            .bind(("server", input.server))
            .bind(("start", input.start))
            .bind(("end", input.end))
            .await?;
        resp.take::<Vec<ServerHealthRecordEntity>>(0)
    }
}

/// One accepted report, in one conditional transaction: the server's liveness
/// (`last_health_report_at`, and `last_seen_at` alongside the watch heartbeat)
/// is advanced only while it still belongs to the reporting session
/// (`refresh_key_generation`), and the server and node records are written only
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
    pub nodes: Vec<NewNodeHealthRecord>,
}

impl Processor<InsertServerHealthRecord> for SurrealProcessor {
    /// `false` when the server no longer holds `generation`.
    type Output = bool;
    type Error = surrealdb::Error;
    #[tracing::instrument(name = "Query-Transaction:InsertServerHealthRecord", skip_all, err)]
    async fn process(&self, input: InsertServerHealthRecord) -> Result<Self::Output, Self::Error> {
        // Statement 0 is BEGIN; the RETURN is statement 3.
        let mut resp = self
            .db()
            .query(
                "BEGIN TRANSACTION;
                 LET $matched = UPDATE $server
                     SET last_health_report_at = $report_time, last_seen_at = $report_time,
                         health_status = $status
                     WHERE refresh_key_generation = $generation RETURN AFTER;
                 IF array::len($matched) > 0 {
                     CREATE server_health_record SET server = $server, status = $status,
                         report_time = $report_time, upload_bytes = $upload_bytes,
                         download_bytes = $download_bytes,
                         current_connections = $current_connections,
                         max_connections = $max_connections;
                     IF array::len($nodes) > 0 { INSERT INTO node_health_record $nodes };
                 };
                 RETURN array::len($matched) > 0;
                 COMMIT TRANSACTION;",
            )
            .bind(("server", input.server))
            .bind(("generation", input.generation))
            .bind(("status", input.status))
            .bind(("report_time", input.report_time))
            .bind(("upload_bytes", input.upload_bytes))
            .bind(("download_bytes", input.download_bytes))
            .bind(("current_connections", input.current_connections))
            .bind(("max_connections", input.max_connections))
            .bind(("nodes", input.nodes))
            .await?;
        Ok(resp.take::<Option<bool>>(3)?.unwrap_or(false))
    }
}

/// A status change that is not a report (the master noticed the worker is gone):
/// writes the status and a zero-counter record, but leaves
/// `last_health_report_at` alone — only the worker's own report advances it.
///
/// Conditional on the status actually changing and, when `generation` is given,
/// on the server still belonging to that session: a stream closing after the
/// worker re-registered must not clobber the successor's status.
#[derive(Debug)]
pub struct SetServerHealthStatus {
    pub server: ServerId,
    pub generation: Option<i64>,
    pub status: ServerHealthStatus,
    pub now: DateTime<Utc>,
}

impl Processor<SetServerHealthStatus> for SurrealProcessor {
    /// `false` when nothing changed.
    type Output = bool;
    type Error = surrealdb::Error;
    #[tracing::instrument(name = "Query-Transaction:SetServerHealthStatus", skip_all, err)]
    async fn process(&self, input: SetServerHealthStatus) -> Result<Self::Output, Self::Error> {
        // Statement 0 is BEGIN; the RETURN is statement 3.
        let mut resp = self
            .db()
            .query(
                "BEGIN TRANSACTION;
                 LET $matched = UPDATE $server SET health_status = $status
                     WHERE health_status != $status
                       AND ($generation = NONE OR refresh_key_generation = $generation)
                     RETURN AFTER;
                 IF array::len($matched) > 0 {
                     CREATE server_health_record SET server = $server, status = $status,
                         report_time = $now, upload_bytes = 0, download_bytes = 0,
                         current_connections = 0, max_connections = 0;
                 };
                 RETURN array::len($matched) > 0;
                 COMMIT TRANSACTION;",
            )
            .bind(("server", input.server))
            .bind(("generation", input.generation))
            .bind(("status", input.status))
            .bind(("now", input.now))
            .await?;
        Ok(resp.take::<Option<bool>>(3)?.unwrap_or(false))
    }
}

/// What the liveness sweep decides on.
#[derive(Debug, Clone, SurrealValue)]
pub struct ServerLiveness {
    pub id: ServerId,
    pub last_health_report_at: Option<DateTime<Utc>>,
    pub health_status: ServerHealthStatus,
}

/// Every server not already `Offline`; the sweep applies the threshold.
#[derive(Debug)]
pub struct ListServersForLivenessSweep;

impl Processor<ListServersForLivenessSweep> for SurrealProcessor {
    type Output = Vec<ServerLiveness>;
    type Error = surrealdb::Error;
    #[tracing::instrument(name = "Query:ListServersForLivenessSweep", skip_all, err)]
    async fn process(&self, _: ListServersForLivenessSweep) -> Result<Self::Output, Self::Error> {
        let mut resp = self
            .db()
            .query(
                "SELECT id, last_health_report_at, health_status FROM orchestration_server \
                 WHERE health_status != 'offline'",
            )
            .await?;
        resp.take::<Vec<ServerLiveness>>(0)
    }
}

table_record!(NodeHealthRecordId, "node_health_record");

#[derive(Debug, Clone, SurrealValue)]
pub struct NodeHealthRecordEntity {
    pub id: NodeHealthRecordId,
    pub node: NodeId,
    pub status: NodeHealthStatus,
    pub message: String,
    pub report_time: DateTime<Utc>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, SurrealValue)]
#[surreal(untagged, rename_all = "snake_case")]
pub enum NodeHealthStatus {
    /// The node's config is applied on its server and the pod is healthy.
    Ready,
    /// A newer revision involving this node is derived but not yet applied.
    Deploying,
    /// The pod carrying this node failed to apply or to run.
    Failed,
}

impl NodeHealthStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Ready => "ready",
            Self::Deploying => "deploying",
            Self::Failed => "failed",
        }
    }
}

/// Node records in `[start, end]`, newest first, at most `limit`.
#[derive(Debug)]
pub struct ListNodeHealthHistory {
    pub node: NodeId,
    pub start: DateTime<Utc>,
    pub end: DateTime<Utc>,
    pub limit: i64,
}

impl Processor<ListNodeHealthHistory> for SurrealProcessor {
    type Output = Vec<NodeHealthRecordEntity>;
    type Error = surrealdb::Error;
    #[tracing::instrument(name = "Query:ListNodeHealthHistory", skip_all, err)]
    async fn process(&self, input: ListNodeHealthHistory) -> Result<Self::Output, Self::Error> {
        let mut resp = self
            .db()
            .query(
                "SELECT * FROM node_health_record \
                 WHERE node = $node AND report_time >= $start AND report_time <= $end \
                 ORDER BY report_time DESC LIMIT $limit",
            )
            .bind(("node", input.node))
            .bind(("start", input.start))
            .bind(("end", input.end))
            .bind(("limit", input.limit))
            .await?;
        resp.take::<Vec<NodeHealthRecordEntity>>(0)
    }
}

/// A node record to insert; the row id is generated.
#[derive(Debug, Clone, SurrealValue)]
pub struct NewNodeHealthRecord {
    pub node: NodeId,
    pub status: NodeHealthStatus,
    pub message: String,
    pub report_time: DateTime<Utc>,
}

/// Inserts a batch of node records in one statement. The derivation hook's
/// query (`Deploying` the moment a new revision is published); a report's node
/// records travel inside [`InsertServerHealthRecord`] instead.
#[derive(Debug)]
pub struct InsertNodeHealthRecords {
    pub records: Vec<NewNodeHealthRecord>,
}

impl Processor<InsertNodeHealthRecords> for SurrealProcessor {
    type Output = ();
    type Error = surrealdb::Error;
    #[tracing::instrument(name = "Query:InsertNodeHealthRecords", skip_all, err)]
    async fn process(&self, input: InsertNodeHealthRecords) -> Result<Self::Output, Self::Error> {
        if input.records.is_empty() {
            return Ok(());
        }
        self.db()
            .query("INSERT INTO node_health_record $records")
            .bind(("records", input.records))
            .await?
            .check()?;
        Ok(())
    }
}

/// Deletes records older than the given cut-offs; the retention cron's query.
#[derive(Debug)]
pub struct DeleteHealthRecordsBefore {
    pub server_records_before: DateTime<Utc>,
    pub node_records_before: DateTime<Utc>,
}

impl Processor<DeleteHealthRecordsBefore> for SurrealProcessor {
    type Output = ();
    type Error = surrealdb::Error;
    #[tracing::instrument(name = "Query:DeleteHealthRecordsBefore", skip_all, err)]
    async fn process(&self, input: DeleteHealthRecordsBefore) -> Result<Self::Output, Self::Error> {
        self.db()
            .query("DELETE server_health_record WHERE report_time < $server_before")
            .query("DELETE node_health_record WHERE report_time < $node_before")
            .bind(("server_before", input.server_records_before))
            .bind(("node_before", input.node_records_before))
            .await?
            .check()?;
        Ok(())
    }
}
