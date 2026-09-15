use crate::entities::surreal::canvas::{CanvasId, CanvasUiPosition};
use crate::entities::surreal::health::ServerHealthStatus;
use chrono::{DateTime, Utc};
use kanau::processor::Processor;
use newtype_record_id::table_record;
use std::net::IpAddr;
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
    /// Operator-pinned IPv4 / IPv6; wins over whatever the worker reports.
    #[surreal(default)]
    pub override_v4: Option<String>,
    #[surreal(default)]
    pub override_v6: Option<String>,
    /// Further addresses the operator added (a second public IP, an overlay
    /// address); pods may advertise one of them.
    #[surreal(default)]
    pub extra_addresses: Vec<String>,
    /// What the worker last reported about itself.
    #[surreal(default)]
    pub reported_addresses: Option<ReportedAddresses>,
    /// The peer address the master saw the last registration come from.
    #[surreal(default)]
    pub observed_address: Option<String>,
    #[surreal(default)]
    pub observed_at: Option<DateTime<Utc>>,
    /// The worker crate version the last registration reported; `None` until a
    /// worker that reports one registers.
    #[surreal(default)]
    pub agent_version: Option<String>,
    /// The CPU architecture that worker was built for (`x86_64`, `aarch64`).
    #[surreal(default)]
    pub agent_arch: Option<String>,
    /// The systemd instance the install command creates: `guru-worker@<unit>`.
    #[surreal(default)]
    pub agent_unit: Option<String>,
    /// A pending self-update: the version the operator asked the worker to move
    /// to. Cleared once the worker registers as that version, or on failure.
    #[surreal(default)]
    pub agent_update_requested: Option<String>,
    /// Why the last self-update failed, as the worker reported it.
    #[surreal(default)]
    pub agent_update_error: Option<String>,
    /// SHA-256 of the server's own agent key, which authenticates `Register` in
    /// place of an operator API key. Only the digest is ever stored.
    #[surreal(default)]
    pub agent_key_digest: Option<String>,
    #[surreal(default)]
    pub agent_key_issued_at: Option<DateTime<Utc>>,
}

/// The address set a worker discovers about itself and sends with `Register`
/// and, when it changes, with a health report.
#[derive(Debug, Clone, PartialEq, Eq, SurrealValue)]
pub struct ReportedAddresses {
    pub public_v4: Option<String>,
    pub public_v6: Option<String>,
    pub interfaces: Vec<String>,
    /// ISO 3166-1 alpha-2 country of the public address, when the worker could
    /// look it up.
    #[surreal(default)]
    pub country: Option<String>,
    pub reported_at: DateTime<Utc>,
}

impl ReportedAddresses {
    /// Same addresses, whenever they were reported.
    pub fn same_addresses(&self, other: &Self) -> bool {
        self.public_v4 == other.public_v4
            && self.public_v6 == other.public_v6
            && self.interfaces == other.interfaces
            && self.country == other.country
    }
}

/// Where a server's effective address came from.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AddressSource {
    Override,
    Reported,
    Observed,
}

impl ServerEntity {
    /// The address other servers dial by default: the IPv4 override, else the
    /// reported public IPv4, else the address the master observed, else the same
    /// chain for IPv6. IPv4 first because that is what most peers can reach; a
    /// pod that should be dialed over IPv6 sets its own `advertise_ip`.
    pub fn effective_address(&self) -> Option<(IpAddr, AddressSource)> {
        let parse = |s: &Option<String>| s.as_deref().and_then(|v| v.parse::<IpAddr>().ok());
        let reported = self.reported_addresses.as_ref();
        let observed = parse(&self.observed_address);
        let candidates = [
            (parse(&self.override_v4), AddressSource::Override),
            (
                reported.and_then(|r| parse(&r.public_v4)),
                AddressSource::Reported,
            ),
            (observed.filter(IpAddr::is_ipv4), AddressSource::Observed),
            (parse(&self.override_v6), AddressSource::Override),
            (
                reported.and_then(|r| parse(&r.public_v6)),
                AddressSource::Reported,
            ),
            (observed.filter(IpAddr::is_ipv6), AddressSource::Observed),
        ];
        candidates
            .into_iter()
            .find_map(|(address, source)| address.map(|a| (a, source)))
    }
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

pub struct CreateServer {
    pub canvas: CanvasId,
    pub name: String,
    pub icon: String,
    pub comment: String,
    pub position: CanvasUiPosition,
    pub ipv6_resolve: ServerIpv6Resolve,
    pub log_level: String,
    pub override_v4: Option<String>,
    pub override_v6: Option<String>,
    pub extra_addresses: Vec<String>,
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
            .bind(("override_v4", input.override_v4))
            .bind(("override_v6", input.override_v6))
            .bind(("extra_addresses", input.extra_addresses))
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
    pub override_v4: Option<String>,
    pub override_v6: Option<String>,
    pub extra_addresses: Vec<String>,
    /// The systemd instance the install command targets; `None` clears it.
    pub agent_unit: Option<String>,
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
            .bind(("override_v4", input.override_v4))
            .bind(("override_v6", input.override_v6))
            .bind(("extra_addresses", input.extra_addresses))
            .bind(("agent_unit", input.agent_unit))
            .await?;
        resp.take::<Option<ServerEntity>>(1)?
            .ok_or_else(|| surrealdb::Error::internal("server not found".to_string()))
    }
}

/// Issues (or replaces) a server's own agent key — only its digest is stored —
/// and names the systemd instance the install command carrying it targets.
#[derive(Debug)]
pub struct SetServerAgentKey {
    pub id: ServerId,
    pub digest: String,
    pub unit: String,
    pub now: DateTime<Utc>,
}

impl Processor<SetServerAgentKey> for SurrealProcessor {
    type Output = ServerEntity;
    type Error = surrealdb::Error;
    #[tracing::instrument(name = "Query:SetServerAgentKey", skip_all, err, fields(id = ?input.id))]
    async fn process(&self, input: SetServerAgentKey) -> Result<Self::Output, Self::Error> {
        let mut resp = self
            .db()
            .query(
                "UPDATE $id SET agent_key_digest = $digest, agent_key_issued_at = $now,
                     agent_unit = $unit RETURN AFTER",
            )
            .bind(("id", input.id))
            .bind(("digest", input.digest))
            .bind(("now", input.now))
            .bind(("unit", input.unit))
            .await?;
        resp.take::<Option<ServerEntity>>(0)?
            .ok_or_else(|| surrealdb::Error::internal("server not found".to_string()))
    }
}

/// Marks the published release as what this server's worker should move to.
#[derive(Debug)]
pub struct SetAgentUpdateRequested {
    pub id: ServerId,
    pub version: String,
}

impl Processor<SetAgentUpdateRequested> for SurrealProcessor {
    type Output = ServerEntity;
    type Error = surrealdb::Error;
    #[tracing::instrument(name = "Query:SetAgentUpdateRequested", skip_all, err, fields(id = ?input.id))]
    async fn process(&self, input: SetAgentUpdateRequested) -> Result<Self::Output, Self::Error> {
        let mut resp = self
            .db()
            .query(
                "UPDATE $id SET agent_update_requested = $version, agent_update_error = NONE
                 RETURN AFTER",
            )
            .bind(("id", input.id))
            .bind(("version", input.version))
            .await?;
        resp.take::<Option<ServerEntity>>(0)?
            .ok_or_else(|| surrealdb::Error::internal("server not found".to_string()))
    }
}

/// Settles a pending self-update from what the worker reports: an error ends
/// the request and is kept for the dashboard, a registration as the requested
/// version ends it cleanly, anything else leaves the row alone.
#[derive(Debug)]
pub struct SettleAgentUpdate {
    pub id: ServerId,
    /// The version the worker registered as, when it reported one.
    pub reported_version: Option<String>,
    /// Why the last attempt failed, when the worker reported that.
    pub error: Option<String>,
}

impl Processor<SettleAgentUpdate> for SurrealProcessor {
    type Output = ();
    type Error = surrealdb::Error;
    #[tracing::instrument(name = "Query-Transaction:SettleAgentUpdate", skip_all, err, fields(id = ?input.id))]
    async fn process(&self, input: SettleAgentUpdate) -> Result<Self::Output, Self::Error> {
        self.db()
            .query(include_str!("../../../sql/server/settle_agent_update.surql"))
            .bind(("id", input.id))
            .bind(("reported", input.reported_version))
            .bind(("error", input.error))
            .await?
            .check()?;
        Ok(())
    }
}

/// The server whose agent key has this digest — the key names the server, the
/// caller only confirms it.
pub struct FindServerByAgentKeyDigest {
    pub digest: String,
}

impl Processor<FindServerByAgentKeyDigest> for SurrealProcessor {
    type Output = Option<ServerEntity>;
    type Error = surrealdb::Error;
    #[tracing::instrument(name = "Query:FindServerByAgentKeyDigest", skip_all, err)]
    async fn process(
        &self,
        input: FindServerByAgentKeyDigest,
    ) -> Result<Self::Output, Self::Error> {
        let mut resp = self
            .db()
            .query("SELECT * FROM orchestration_server WHERE agent_key_digest = $digest LIMIT 1")
            .bind(("digest", input.digest))
            .await?;
        resp.take::<Option<ServerEntity>>(0)
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

/// Takes the server's session lease and reconciles what the worker reports.
///
/// Registration is the one moment the master learns exactly what a worker runs, so
/// it is also where the config view is repaired: a running revision that matches
/// `desired` or `in_flight` is promoted to `applied`, a running revision of `0`
/// (nothing running: a fresh install, a wiped state directory) forgets `applied`
/// so the stream resends the desired revision, and whatever was left in flight is
/// cleared — the worker is not running it, so it was lost with the session that
/// sent it.
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
    /// The peer address this registration arrived from, if known.
    pub observed: Option<String>,
    /// What the worker reported about its addresses; `None` keeps the stored set.
    pub reported: Option<ReportedAddresses>,
    /// The worker's build, when it reported one; `None` keeps the stored values.
    pub agent_version: Option<String>,
    pub agent_arch: Option<String>,
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
            .bind(("observed", input.observed))
            .bind(("reported", input.reported))
            .bind(("agent_version", input.agent_version))
            .bind(("agent_arch", input.agent_arch))
            .await?;
        resp.take::<Option<ServerEntity>>(3)
    }
}

/// Replaces a server's reported address set with what a live session just
/// discovered. Fenced on the refresh-key generation, and a no-op (no canvas
/// bump) when nothing changed.
#[derive(Debug)]
pub struct UpdateReportedAddresses {
    pub server: ServerId,
    pub canvas: CanvasId,
    pub generation: i64,
    pub reported: ReportedAddresses,
}

impl Processor<UpdateReportedAddresses> for SurrealProcessor {
    /// `true` when the stored set changed (and the canvas was touched).
    type Output = bool;
    type Error = surrealdb::Error;
    #[tracing::instrument(name = "Query-Transaction:UpdateReportedAddresses", skip_all, err, fields(server = ?input.server))]
    async fn process(&self, input: UpdateReportedAddresses) -> Result<Self::Output, Self::Error> {
        // Statement 0 is BEGIN, 1 the LET, 2 the IF; the RETURN is statement 3.
        let mut resp = self
            .db()
            .query(include_str!(
                "../../../sql/server/update_reported_addresses.surql"
            ))
            .bind(("server", input.server))
            .bind(("canvas", input.canvas))
            .bind(("generation", input.generation))
            .bind(("reported", input.reported))
            .await?;
        Ok(resp.take::<Option<bool>>(3)?.unwrap_or(false))
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
