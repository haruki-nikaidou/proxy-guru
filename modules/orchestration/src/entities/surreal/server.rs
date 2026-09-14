use crate::entities::surreal::canvas::{CanvasId, CanvasUiPosition};
use crate::entities::surreal::health::ServerHealthStatus;
use chrono::{DateTime, Utc};
use kanau::processor::Processor;
use newtype_record_id::table_record;
use surrealdb_types::SurrealValue;
use wakuwaku::surreal::SurrealProcessor;

table_record!(ServerId, "orchestration_server");

#[derive(Debug, Clone, SurrealValue)]
pub struct ServerEntity {
    pub id: ServerId,
    pub canvas: CanvasId,
    pub name: String,
    pub icon: String,
    pub comment: String,
    pub position: CanvasUiPosition,
    pub ipv6_resolve: ServerIpv6Resolve,
    pub log_level: String,
    pub current_dynamic_refresh_key: Option<String>,
    pub refresh_key_generation: i64,
    /// Monotonic claim counter for the single live `WatchConfig` stream. Bumped by
    /// every claim, so the newest stream always wins and no lease can strand a
    /// server after a master or worker crash.
    pub watch_epoch: i64,
    /// While this is in the future, one worker session owns the server: another
    /// registration is refused until it lapses or the owning stream releases it.
    pub session_lease_until: Option<DateTime<Utc>>,
    pub last_seen_at: Option<DateTime<Utc>>,
    /// When the last health report was accepted. Distinct from `last_seen_at`
    /// (the watch stream's heartbeat) so a live config stream cannot mask a dead
    /// health stream.
    #[surreal(default)]
    pub last_health_report_at: Option<DateTime<Utc>>,
    /// Current liveness, kept by the health pipeline; `Offline` until a worker
    /// reports.
    #[surreal(default)]
    pub health_status: ServerHealthStatus,
}

/// Local mirror of [`guru_worker_config::Ipv6Resolve`]; `SurrealValue` cannot be
/// derived for a foreign type.
#[derive(Debug, Clone, Copy, PartialEq, Eq, SurrealValue)]
#[surreal(untagged, rename_all = "snake_case")]
pub enum ServerIpv6Resolve {
    Required,
    Preferred,
    Tolerated,
    Forbidden,
}

impl From<ServerIpv6Resolve> for guru_worker_config::Ipv6Resolve {
    fn from(value: ServerIpv6Resolve) -> Self {
        match value {
            ServerIpv6Resolve::Required => guru_worker_config::Ipv6Resolve::Required,
            ServerIpv6Resolve::Preferred => guru_worker_config::Ipv6Resolve::Preferred,
            ServerIpv6Resolve::Tolerated => guru_worker_config::Ipv6Resolve::Tolerated,
            ServerIpv6Resolve::Forbidden => guru_worker_config::Ipv6Resolve::Forbidden,
        }
    }
}

table_record!(ServerIpRecordId, "server_ip_record");

#[derive(Debug, Clone, SurrealValue)]
pub struct ServerIpRecordEntity {
    pub id: ServerIpRecordId,
    pub server: ServerId,
    pub ip: String,
    pub country: String,
}

#[derive(Debug, Clone)]
pub struct ServerWithIp {
    pub server: ServerEntity,
    pub ips: Vec<ServerIpRecordEntity>,
}

pub struct CreateServer {
    pub canvas: CanvasId,
    pub name: String,
    pub icon: String,
    pub comment: String,
    pub position: CanvasUiPosition,
    pub ipv6_resolve: ServerIpv6Resolve,
    pub log_level: String,
}

impl Processor<CreateServer> for SurrealProcessor {
    type Output = ServerEntity;
    type Error = surrealdb::Error;
    #[tracing::instrument(name = "Query-Transaction:CreateServer", skip_all, err, fields(canvas = ?input.canvas))]
    async fn process(&self, input: CreateServer) -> Result<Self::Output, Self::Error> {
        // The server and its (empty) config view are one write: no code path may
        // ever observe a server without the row its rollout is tracked in.
        // Statement 0 is BEGIN; the RETURN below is statement 4.
        let mut resp = self
            .db()
            .query(include_str!("../../../sql/server/create_server.surql"))
            .bind(("canvas", input.canvas))
            .bind(("name", input.name))
            .bind(("icon", input.icon))
            .bind(("comment", input.comment))
            .bind(("position", input.position))
            .bind(("ipv6_resolve", input.ipv6_resolve))
            .bind(("log_level", input.log_level))
            .await?;
        resp.take::<Option<ServerEntity>>(4)?
            .ok_or_else(|| surrealdb::Error::internal("create server returned no row".to_string()))
    }
}

#[derive(Debug)]
pub struct FindServerById {
    pub id: ServerId,
}

impl Processor<FindServerById> for SurrealProcessor {
    type Output = Option<ServerEntity>;
    type Error = surrealdb::Error;
    #[tracing::instrument(name = "Query:FindServerById", skip_all, err)]
    async fn process(&self, input: FindServerById) -> Result<Self::Output, Self::Error> {
        let mut resp = self
            .db()
            .query("SELECT * FROM $id")
            .bind(("id", input.id))
            .await?;
        resp.take::<Option<ServerEntity>>(0)
    }
}

#[derive(Debug)]
pub struct ListServersByCanvas {
    pub canvas: CanvasId,
}

impl Processor<ListServersByCanvas> for SurrealProcessor {
    type Output = Vec<ServerEntity>;
    type Error = surrealdb::Error;
    #[tracing::instrument(name = "Query:ListServersByCanvas", skip_all, err)]
    async fn process(&self, input: ListServersByCanvas) -> Result<Self::Output, Self::Error> {
        let mut resp = self
            .db()
            .query("SELECT * FROM orchestration_server WHERE canvas = $canvas")
            .bind(("canvas", input.canvas))
            .await?;
        resp.take::<Vec<ServerEntity>>(0)
    }
}

pub struct UpdateServerSettings {
    pub id: ServerId,
    pub canvas: CanvasId,
    pub name: String,
    pub icon: String,
    pub comment: String,
    pub ipv6_resolve: ServerIpv6Resolve,
    pub log_level: String,
}

impl Processor<UpdateServerSettings> for SurrealProcessor {
    type Output = ServerEntity;
    type Error = surrealdb::Error;
    #[tracing::instrument(name = "Query-Transaction:UpdateServerSettings", skip_all, err, fields(id = ?input.id))]
    async fn process(&self, input: UpdateServerSettings) -> Result<Self::Output, Self::Error> {
        // Statement 0 is BEGIN; the update is statement 1.
        let mut resp = self
            .db()
            .query(include_str!(
                "../../../sql/server/update_server_settings.surql"
            ))
            .bind(("id", input.id))
            .bind(("canvas", input.canvas))
            .bind(("name", input.name))
            .bind(("icon", input.icon))
            .bind(("comment", input.comment))
            .bind(("ipv6_resolve", input.ipv6_resolve))
            .bind(("log_level", input.log_level))
            .await?;
        resp.take::<Option<ServerEntity>>(1)?
            .ok_or_else(|| surrealdb::Error::internal("server not found".to_string()))
    }
}

pub struct MoveServerPosition {
    pub id: ServerId,
    pub position: CanvasUiPosition,
}

impl Processor<MoveServerPosition> for SurrealProcessor {
    type Output = ();
    type Error = surrealdb::Error;
    #[tracing::instrument(name = "Query:MoveServerPosition", skip_all, err, fields(id = ?input.id))]
    async fn process(&self, input: MoveServerPosition) -> Result<Self::Output, Self::Error> {
        self.db()
            .query("UPDATE $id SET position = $position")
            .bind(("id", input.id))
            .bind(("position", input.position))
            .await?
            .check()?;
        Ok(())
    }
}

#[derive(Debug)]
pub struct DeleteServerRow {
    pub id: ServerId,
    pub canvas: CanvasId,
}

impl Processor<DeleteServerRow> for SurrealProcessor {
    type Output = ();
    type Error = surrealdb::Error;
    #[tracing::instrument(name = "Query-Transaction:DeleteServerRow", skip_all, err)]
    async fn process(&self, input: DeleteServerRow) -> Result<Self::Output, Self::Error> {
        self.db()
            .query(include_str!("../../../sql/server/delete_server_row.surql"))
            .bind(("id", input.id))
            .bind(("canvas", input.canvas))
            .await?
            .check()?;
        Ok(())
    }
}

pub struct CreateServerIp {
    pub server: ServerId,
    pub ip: String,
    pub country: String,
}

impl Processor<CreateServerIp> for SurrealProcessor {
    type Output = ServerIpRecordEntity;
    type Error = surrealdb::Error;
    #[tracing::instrument(name = "Query:CreateServerIp", skip_all, err, fields(server = ?input.server))]
    async fn process(&self, input: CreateServerIp) -> Result<Self::Output, Self::Error> {
        let mut resp = self
            .db()
            .query(
                "CREATE ONLY server_ip_record CONTENT { server: $server, ip: $ip, country: $country }",
            )
            .bind(("server", input.server))
            .bind(("ip", input.ip))
            .bind(("country", input.country))
            .await?;
        resp.take::<Option<ServerIpRecordEntity>>(0)?
            .ok_or_else(|| {
                surrealdb::Error::internal("create server ip returned no row".to_string())
            })
    }
}

#[derive(Debug)]
pub struct FindServerIpById {
    pub id: ServerIpRecordId,
}

impl Processor<FindServerIpById> for SurrealProcessor {
    type Output = Option<ServerIpRecordEntity>;
    type Error = surrealdb::Error;
    #[tracing::instrument(name = "Query:FindServerIpById", skip_all, err)]
    async fn process(&self, input: FindServerIpById) -> Result<Self::Output, Self::Error> {
        let mut resp = self
            .db()
            .query("SELECT * FROM $id")
            .bind(("id", input.id))
            .await?;
        resp.take::<Option<ServerIpRecordEntity>>(0)
    }
}

#[derive(Debug)]
pub struct ListServerIpsByCanvas {
    pub canvas: CanvasId,
}

impl Processor<ListServerIpsByCanvas> for SurrealProcessor {
    type Output = Vec<ServerIpRecordEntity>;
    type Error = surrealdb::Error;
    #[tracing::instrument(name = "Query:ListServerIpsByCanvas", skip_all, err)]
    async fn process(&self, input: ListServerIpsByCanvas) -> Result<Self::Output, Self::Error> {
        let mut resp = self
            .db()
            .query("SELECT * FROM server_ip_record WHERE server.canvas = $canvas")
            .bind(("canvas", input.canvas))
            .await?;
        resp.take::<Vec<ServerIpRecordEntity>>(0)
    }
}

#[derive(Debug)]
pub struct DeleteServerIpRow {
    pub id: ServerIpRecordId,
    pub canvas: CanvasId,
}

impl Processor<DeleteServerIpRow> for SurrealProcessor {
    type Output = ();
    type Error = surrealdb::Error;
    #[tracing::instrument(name = "Query-Transaction:DeleteServerIpRow", skip_all, err)]
    async fn process(&self, input: DeleteServerIpRow) -> Result<Self::Output, Self::Error> {
        self.db()
            .query(include_str!(
                "../../../sql/server/delete_server_ip_row.surql"
            ))
            .bind(("id", input.id))
            .bind(("canvas", input.canvas))
            .await?
            .check()?;
        Ok(())
    }
}

/// Takes the server's session lease and reconciles what the worker reports.
///
/// Registration is the one moment the master learns exactly what a worker runs, so
/// it is also where the config view is repaired: a running revision that matches
/// `desired` or `in_flight` is promoted to `applied`, and whatever was left in
/// flight is cleared — the worker is not running it, so it was lost with the
/// session that sent it.
///
/// The rotation is refused while another worker session is still alive, so a
/// second worker configured with the same `server_id` cannot steal a running
/// server: it is rejected for as long as the incumbent heartbeats. Takeover is
/// therefore only possible once the lease lapses (the incumbent crashed) or is
/// released (the incumbent's stream ended).
#[derive(Debug)]
pub struct RegisterWorkerSession {
    pub server: ServerId,
    pub canvas: CanvasId,
    pub digest: String,
    pub now: DateTime<Utc>,
    /// The lease deadline the new session gets.
    pub lease_until: DateTime<Utc>,
    /// The revision the worker says it is running; `0` for a fresh worker.
    pub running_revision: i64,
}

impl Processor<RegisterWorkerSession> for SurrealProcessor {
    /// The rotated row, or `None` when a live session still holds the server.
    type Output = Option<ServerEntity>;
    type Error = surrealdb::Error;
    #[tracing::instrument(name = "Query-Transaction:RegisterWorkerSession", skip_all, err, fields(server = ?input.server))]
    async fn process(&self, input: RegisterWorkerSession) -> Result<Self::Output, Self::Error> {
        // Statement 0 is BEGIN; the RETURN below is statement 3.
        let mut resp = self
            .db()
            .query(include_str!(
                "../../../sql/server/register_worker_session.surql"
            ))
            .bind(("server", input.server))
            .bind(("canvas", input.canvas))
            .bind(("digest", input.digest))
            .bind(("now", input.now))
            .bind(("lease_until", input.lease_until))
            .bind(("running_revision", input.running_revision))
            .await?;
        resp.take::<Option<ServerEntity>>(3)
    }
}

/// Claims the single live `WatchConfig` session of one server.
///
/// The claim only succeeds while the caller still holds the current refresh-key
/// generation — i.e. it is the worker that registration handed the lease to — and
/// it bumps `watch_epoch`, so `(refresh_key_generation, watch_epoch)` totally
/// orders every session a server ever had. That ordering is the fence: a stream is
/// authoritative exactly while the row still carries the pair it won.
///
/// Claiming also voids whatever the previous stream had in flight. `in_flight`
/// means "handed to the live session and not yet acknowledged", and the session it
/// was handed to is precisely what this claim just fenced out: nobody is left to
/// acknowledge it, so leaving it set would strand the revision until the next
/// unrelated edit.
#[derive(Debug)]
pub struct ClaimServerWatchSession {
    pub server: ServerId,
    /// The generation the claiming stream authenticated with.
    pub generation: i64,
    pub now: DateTime<Utc>,
    pub lease_until: DateTime<Utc>,
}

impl Processor<ClaimServerWatchSession> for SurrealProcessor {
    /// The claimed row, or `None` when the generation is no longer current.
    type Output = Option<ServerEntity>;
    type Error = surrealdb::Error;
    #[tracing::instrument(name = "Query-Transaction:ClaimServerWatchSession", skip_all, err)]
    async fn process(&self, input: ClaimServerWatchSession) -> Result<Self::Output, Self::Error> {
        // Statement 0 is BEGIN; the RETURN below is statement 3.
        let mut resp = self
            .db()
            .query(include_str!(
                "../../../sql/server/claim_watch_session.surql"
            ))
            .bind(("server", input.server))
            .bind(("generation", input.generation))
            .bind(("now", input.now))
            .bind(("lease_until", input.lease_until))
            .await?;
        resp.take::<Option<ServerEntity>>(3)
    }
}

/// The heartbeat of a live stream: extends the lease while the fence is still ours.
#[derive(Debug)]
pub struct RenewServerWatchSession {
    pub server: ServerId,
    pub generation: i64,
    pub epoch: i64,
    pub now: DateTime<Utc>,
    pub lease_until: DateTime<Utc>,
}

impl Processor<RenewServerWatchSession> for SurrealProcessor {
    /// `false` once the session has been fenced out; the stream must then end.
    type Output = bool;
    type Error = surrealdb::Error;
    #[tracing::instrument(name = "Query:RenewServerWatchSession", skip_all, err)]
    async fn process(&self, input: RenewServerWatchSession) -> Result<Self::Output, Self::Error> {
        let mut resp = self
            .db()
            .query(
                "UPDATE $server SET session_lease_until = $lease_until, last_seen_at = $now
                 WHERE refresh_key_generation = $generation AND watch_epoch = $epoch
                 RETURN AFTER",
            )
            .bind(("server", input.server))
            .bind(("generation", input.generation))
            .bind(("epoch", input.epoch))
            .bind(("now", input.now))
            .bind(("lease_until", input.lease_until))
            .await?;
        Ok(!resp.take::<Vec<ServerEntity>>(0)?.is_empty())
    }
}

/// Drops the lease when a stream ends cleanly, so a restarting worker can register
/// again immediately instead of waiting the lease out.
#[derive(Debug)]
pub struct ReleaseServerWatchSession {
    pub server: ServerId,
    pub generation: i64,
    pub epoch: i64,
}

impl Processor<ReleaseServerWatchSession> for SurrealProcessor {
    type Output = ();
    type Error = surrealdb::Error;
    #[tracing::instrument(name = "Query:ReleaseServerWatchSession", skip_all, err)]
    async fn process(&self, input: ReleaseServerWatchSession) -> Result<Self::Output, Self::Error> {
        self.db()
            .query(
                "UPDATE $server SET session_lease_until = NONE
                 WHERE refresh_key_generation = $generation AND watch_epoch = $epoch",
            )
            .bind(("server", input.server))
            .bind(("generation", input.generation))
            .bind(("epoch", input.epoch))
            .await?
            .check()?;
        Ok(())
    }
}

pub struct FindServerByRefreshKeyDigest {
    pub digest: String,
}

impl Processor<FindServerByRefreshKeyDigest> for SurrealProcessor {
    type Output = Option<ServerEntity>;
    type Error = surrealdb::Error;
    #[tracing::instrument(name = "Query:FindServerByRefreshKeyDigest", skip_all, err)]
    async fn process(
        &self,
        input: FindServerByRefreshKeyDigest,
    ) -> Result<Self::Output, Self::Error> {
        let mut resp = self
            .db()
            .query(
                "SELECT * FROM orchestration_server WHERE current_dynamic_refresh_key = $digest LIMIT 1",
            )
            .bind(("digest", input.digest))
            .await?;
        resp.take::<Option<ServerEntity>>(0)
    }
}
