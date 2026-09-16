use crate::entities::db::canvas::{CanvasFence, CanvasId, CanvasUiPosition};
use crate::entities::db::fence;
use crate::entities::db::health::ServerHealthStatus;
use crate::entities::db::view::ConfigSnapshot;
use base::db::{Db, Error};
use chrono::{DateTime, Utc};
use db_types::{table_record, text_enum};
use kanau::processor::Processor;
use serde::{Deserialize, Serialize};
use sqlx::types::Json;
use std::net::IpAddr;

table_record!(ServerId, "orchestration_server");

/// The conflict reported when a server that still has pods is deleted.
pub const SERVER_HAS_PODS: &str = "server still has live pods";
/// The conflict reported when a server whose universal pod is still bundled is deleted.
pub const UNIVERSAL_POD_BUNDLED: &str =
    "server's universal pod is still bundled; disconnect its bundles first";

#[derive(Debug, Clone, sqlx::FromRow)]
pub struct ServerEntity {
    pub id: ServerId,
    pub canvas: CanvasId,
    pub name: String,
    pub icon: String,
    pub comment: String,
    #[sqlx(flatten)]
    pub position: CanvasUiPosition,
    pub ipv6_resolve: ServerIpv6Resolve,
    pub log_level: String,
    /// This server's side of every QUIC relay link it takes part in.
    #[sqlx(json)]
    pub quic: ServerQuic,
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
    pub last_health_report_at: Option<DateTime<Utc>>,
    /// Current liveness, kept by the health pipeline; `Offline` until a worker
    /// reports.
    pub health_status: ServerHealthStatus,
    /// Operator-pinned IPv4 / IPv6; wins over whatever the worker reports.
    pub override_v4: Option<String>,
    pub override_v6: Option<String>,
    /// Further addresses the operator added (a second public IP, an overlay
    /// address); pods may advertise one of them.
    pub extra_addresses: Vec<String>,
    /// What the worker last reported about itself.
    #[sqlx(json(nullable))]
    pub reported_addresses: Option<ReportedAddresses>,
    /// The peer address the master saw the last registration come from.
    pub observed_address: Option<String>,
    pub observed_at: Option<DateTime<Utc>>,
    /// The worker crate version the last registration reported; `None` until a
    /// worker that reports one registers.
    pub agent_version: Option<String>,
    /// The CPU architecture that worker was built for (`x86_64`, `aarch64`).
    pub agent_arch: Option<String>,
    /// The systemd instance the install command creates: `guru-worker@<unit>`.
    pub agent_unit: Option<String>,
    /// A pending self-update: the version the operator asked the worker to move
    /// to. Cleared once the worker registers as that version, or on failure.
    pub agent_update_requested: Option<String>,
    /// Why the last self-update failed, as the worker reported it.
    pub agent_update_error: Option<String>,
    /// SHA-256 of the server's own agent key, which authenticates `Register` in
    /// place of an operator API key. Only the digest is ever stored.
    pub agent_key_digest: Option<String>,
    pub agent_key_issued_at: Option<DateTime<Utc>>,
}

/// The address set a worker discovers about itself and sends with `Register`
/// and, when it changes, with a health report.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReportedAddresses {
    pub public_v4: Option<String>,
    pub public_v6: Option<String>,
    pub interfaces: Vec<String>,
    /// ISO 3166-1 alpha-2 country of the public address, when the worker could
    /// look it up.
    #[serde(default)]
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

/// Local mirror of [`guru_worker_config::Ipv6Resolve`]: the stored spelling is
/// this module's to own, not the worker config crate's.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ServerIpv6Resolve {
    Required,
    Preferred,
    Tolerated,
    Forbidden,
}
text_enum!(ServerIpv6Resolve {
    Required => "required",
    Preferred => "preferred",
    Tolerated => "tolerated",
    Forbidden => "forbidden",
});

/// How a server sends on its QUIC relay links.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum QuicCongestion {
    /// quinn's Cubic: probes for bandwidth, backs off on loss.
    #[default]
    Cubic,
    /// Hysteria's brutal: sends at `up_mbps` whatever the path does.
    Brutal,
}

/// A server's side of every QUIC relay link it takes part in, as the operator
/// set it. Stored as one `jsonb` document; every field has a zero default, so a
/// server created before the column existed reads as "quinn's defaults".
///
/// `up_mbps` is what this server sends at and `down_mbps` what it can receive;
/// on each link the master pairs them with the peer's, so a server never sends
/// faster than its peer said it can take (see `services::derive`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct ServerQuic {
    pub congestion: QuicCongestion,
    /// Send rate toward every QUIC peer, Mbit/s. `0` = unknown.
    pub up_mbps: u32,
    /// Receive rate from every QUIC peer, Mbit/s. `0` = unknown.
    pub down_mbps: u32,
    /// Per-stream receive window in bytes; `0` derives it from `down_mbps`.
    pub stream_receive_window: u64,
    /// Whole-connection receive window in bytes; `0` leaves it unlimited.
    pub conn_receive_window: u64,
}

impl ServerQuic {
    /// Brutal without a send rate would send at nothing.
    pub fn validate(&self) -> Result<(), String> {
        if self.congestion == QuicCongestion::Brutal && self.up_mbps == 0 {
            return Err("quic: brutal congestion control needs up_mbps".to_string());
        }
        Ok(())
    }

    /// This server's side of a link with `peer`: it sends at the lower of its own
    /// up rate and the peer's down rate, and sizes its windows for the lower of
    /// its own down rate and the peer's up rate. Windows the operator pinned
    /// stay pinned. Without a peer (the worker-wide default) its own numbers
    /// stand.
    pub fn tuning(&self, peer: Option<&ServerQuic>) -> guru_worker_config::QuicTuning {
        fn lower(mine: u32, theirs: u32) -> u32 {
            match (mine, theirs) {
                (0, rate) | (rate, 0) => rate,
                (mine, theirs) => mine.min(theirs),
            }
        }
        let (send_mbps, receive_mbps) = match peer {
            Some(peer) => (
                lower(self.up_mbps, peer.down_mbps),
                lower(self.down_mbps, peer.up_mbps),
            ),
            None => (self.up_mbps, self.down_mbps),
        };
        guru_worker_config::QuicTuning {
            congestion: match self.congestion {
                QuicCongestion::Cubic => guru_worker_config::QuicCongestion::Cubic,
                QuicCongestion::Brutal => guru_worker_config::QuicCongestion::Brutal,
            },
            send_mbps,
            receive_mbps,
            max_streams: 0,
            stream_receive_window: self.stream_receive_window,
            receive_window: self.conn_receive_window,
            send_window: 0,
        }
    }
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

impl Processor<CreateServer> for Db {
    type Output = ServerEntity;
    type Error = Error;
    #[tracing::instrument(name = "Query-Transaction:CreateServer", skip_all, err, fields(canvas = %input.canvas))]
    async fn process(&self, input: CreateServer) -> Result<Self::Output, Self::Error> {
        // The server and its (empty) config view are one write: no code path may
        // ever observe a server without the row its rollout is tracked in.
        let mut tx = self.db().begin().await?;
        let server: ServerEntity = sqlx::query_as(
            "INSERT INTO orchestration_server
                 (id, canvas, name, icon, comment, position_x, position_y, ipv6_resolve,
                  log_level, override_v4, override_v6, extra_addresses)
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
             RETURNING *",
        )
        .bind(ServerId::new())
        .bind(&input.canvas)
        .bind(&input.name)
        .bind(&input.icon)
        .bind(&input.comment)
        .bind(input.position.x)
        .bind(input.position.y)
        .bind(input.ipv6_resolve)
        .bind(&input.log_level)
        .bind(&input.override_v4)
        .bind(&input.override_v6)
        .bind(&input.extra_addresses)
        .fetch_one(&mut *tx)
        .await?;
        sqlx::query("INSERT INTO orchestration_server_config_view (id, server) VALUES ($1, $2)")
            .bind(crate::entities::db::view::ServerConfigViewId::new())
            .bind(&server.id)
            .execute(&mut *tx)
            .await?;
        fence::touch(&mut tx, &input.canvas).await?;
        tx.commit().await?;
        Ok(server)
    }
}

#[derive(Debug)]
pub struct FindServerById {
    pub id: ServerId,
}

impl Processor<FindServerById> for Db {
    type Output = Option<ServerEntity>;
    type Error = Error;
    #[tracing::instrument(name = "Query:FindServerById", skip_all, err)]
    async fn process(&self, input: FindServerById) -> Result<Self::Output, Self::Error> {
        Ok(
            sqlx::query_as("SELECT * FROM orchestration_server WHERE id = $1")
                .bind(input.id)
                .fetch_optional(self.db())
                .await?,
        )
    }
}

#[derive(Debug)]
pub struct ListServersByCanvas {
    pub canvas: CanvasId,
}

impl Processor<ListServersByCanvas> for Db {
    type Output = Vec<ServerEntity>;
    type Error = Error;
    #[tracing::instrument(name = "Query:ListServersByCanvas", skip_all, err)]
    async fn process(&self, input: ListServersByCanvas) -> Result<Self::Output, Self::Error> {
        Ok(
            sqlx::query_as("SELECT * FROM orchestration_server WHERE canvas = $1 ORDER BY id")
                .bind(input.canvas)
                .fetch_all(self.db())
                .await?,
        )
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
    pub quic: ServerQuic,
    pub override_v4: Option<String>,
    pub override_v6: Option<String>,
    pub extra_addresses: Vec<String>,
    /// The systemd instance the install command targets; `None` clears it.
    pub agent_unit: Option<String>,
    /// The snapshot the edit was validated against, fencing the write.
    pub fence: Option<CanvasFence>,
}

impl Processor<UpdateServerSettings> for Db {
    type Output = ServerEntity;
    type Error = Error;
    #[tracing::instrument(name = "Query-Transaction:UpdateServerSettings", skip_all, err, fields(id = %input.id))]
    async fn process(&self, input: UpdateServerSettings) -> Result<Self::Output, Self::Error> {
        let mut tx = self.db().begin().await?;
        let server: ServerEntity = sqlx::query_as(
            "UPDATE orchestration_server
             SET name = $2, icon = $3, comment = $4, ipv6_resolve = $5, log_level = $6,
                 override_v4 = $7, override_v6 = $8, extra_addresses = $9, agent_unit = $10,
                 quic = $11
             WHERE id = $1 RETURNING *",
        )
        .bind(&input.id)
        .bind(&input.name)
        .bind(&input.icon)
        .bind(&input.comment)
        .bind(input.ipv6_resolve)
        .bind(&input.log_level)
        .bind(&input.override_v4)
        .bind(&input.override_v6)
        .bind(&input.extra_addresses)
        .bind(&input.agent_unit)
        .bind(Json(input.quic))
        .fetch_one(&mut *tx)
        .await?;
        fence::touch_checked(&mut tx, &input.canvas, input.fence.as_ref()).await?;
        tx.commit().await?;
        Ok(server)
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

impl Processor<SetServerAgentKey> for Db {
    type Output = ServerEntity;
    type Error = Error;
    #[tracing::instrument(name = "Query:SetServerAgentKey", skip_all, err, fields(id = %input.id))]
    async fn process(&self, input: SetServerAgentKey) -> Result<Self::Output, Self::Error> {
        Ok(sqlx::query_as(
            "UPDATE orchestration_server
             SET agent_key_digest = $2, agent_key_issued_at = $3, agent_unit = $4
             WHERE id = $1 RETURNING *",
        )
        .bind(input.id)
        .bind(input.digest)
        .bind(input.now)
        .bind(input.unit)
        .fetch_one(self.db())
        .await?)
    }
}

/// Marks the published release as what this server's worker should move to.
#[derive(Debug)]
pub struct SetAgentUpdateRequested {
    pub id: ServerId,
    pub version: String,
}

impl Processor<SetAgentUpdateRequested> for Db {
    type Output = ServerEntity;
    type Error = Error;
    #[tracing::instrument(name = "Query:SetAgentUpdateRequested", skip_all, err, fields(id = %input.id))]
    async fn process(&self, input: SetAgentUpdateRequested) -> Result<Self::Output, Self::Error> {
        Ok(sqlx::query_as(
            "UPDATE orchestration_server
             SET agent_update_requested = $2, agent_update_error = NULL
             WHERE id = $1 RETURNING *",
        )
        .bind(input.id)
        .bind(input.version)
        .fetch_one(self.db())
        .await?)
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

impl Processor<SettleAgentUpdate> for Db {
    type Output = ();
    type Error = Error;
    #[tracing::instrument(name = "Query:SettleAgentUpdate", skip_all, err, fields(id = %input.id))]
    async fn process(&self, input: SettleAgentUpdate) -> Result<Self::Output, Self::Error> {
        sqlx::query(
            "UPDATE orchestration_server
             SET agent_update_requested = NULL,
                 agent_update_error = CASE WHEN $2::text IS NOT NULL THEN $2 ELSE NULL END
             WHERE id = $1
               AND ($2::text IS NOT NULL
                    OR (agent_update_requested IS NOT NULL AND agent_update_requested = $3))",
        )
        .bind(input.id)
        .bind(input.error)
        .bind(input.reported_version)
        .execute(self.db())
        .await?;
        Ok(())
    }
}

/// The server whose agent key has this digest — the key names the server, the
/// caller only confirms it.
pub struct FindServerByAgentKeyDigest {
    pub digest: String,
}

impl Processor<FindServerByAgentKeyDigest> for Db {
    type Output = Option<ServerEntity>;
    type Error = Error;
    #[tracing::instrument(name = "Query:FindServerByAgentKeyDigest", skip_all, err)]
    async fn process(
        &self,
        input: FindServerByAgentKeyDigest,
    ) -> Result<Self::Output, Self::Error> {
        Ok(
            sqlx::query_as(
                "SELECT * FROM orchestration_server WHERE agent_key_digest = $1 LIMIT 1",
            )
            .bind(input.digest)
            .fetch_optional(self.db())
            .await?,
        )
    }
}

pub struct MoveServerPosition {
    pub id: ServerId,
    pub position: CanvasUiPosition,
}

impl Processor<MoveServerPosition> for Db {
    type Output = ();
    type Error = Error;
    #[tracing::instrument(name = "Query:MoveServerPosition", skip_all, err, fields(id = %input.id))]
    async fn process(&self, input: MoveServerPosition) -> Result<Self::Output, Self::Error> {
        sqlx::query(
            "UPDATE orchestration_server SET position_x = $2, position_y = $3 WHERE id = $1",
        )
        .bind(input.id)
        .bind(input.position.x)
        .bind(input.position.y)
        .execute(self.db())
        .await?;
        Ok(())
    }
}

/// Deletes a server with its universal pod, config view and health history.
///
/// Refuses ([`SERVER_HAS_PODS`]) while any pod is still placed on the server: a
/// pod whose server is gone belongs to nobody, and derivation could then neither
/// publish nor report it. The service pre-checks the same thing and phrases the
/// message; this is the race guard, and the `pod_server` foreign key is the
/// guard behind the guard. The server's own universal pod is not a pod in that
/// sense: it was created with the server and goes with it, once nothing is
/// bundled into or out of it ([`UNIVERSAL_POD_BUNDLED`]).
#[derive(Debug)]
pub struct DeleteServerRow {
    pub id: ServerId,
    pub canvas: CanvasId,
    /// The snapshot the delete was validated against, fencing the write.
    pub fence: Option<CanvasFence>,
}

impl Processor<DeleteServerRow> for Db {
    type Output = ();
    type Error = Error;
    #[tracing::instrument(name = "Query-Transaction:DeleteServerRow", skip_all, err)]
    async fn process(&self, input: DeleteServerRow) -> Result<Self::Output, Self::Error> {
        let mut tx = self.db().begin().await?;
        let live_pods: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM orchestration_node
             WHERE pod_server = $1 AND spec ->> 'type' <> 'universal_pod'",
        )
        .bind(&input.id)
        .fetch_one(&mut *tx)
        .await?;
        if live_pods > 0 {
            return Err(Error::Conflict(SERVER_HAS_PODS));
        }
        let bundled: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM orchestration_edge_connection e
             JOIN orchestration_port p ON p.id IN (e.source_port, e.target_port)
             JOIN orchestration_node n ON n.id = p.owner
             WHERE n.pod_server = $1 AND n.spec ->> 'type' = 'universal_pod'",
        )
        .bind(&input.id)
        .fetch_one(&mut *tx)
        .await?;
        if bundled > 0 {
            return Err(Error::Conflict(UNIVERSAL_POD_BUNDLED));
        }
        // The universal pod first (its ports and health history cascade), then
        // the server (its view and health history cascade).
        sqlx::query(
            "DELETE FROM orchestration_node WHERE pod_server = $1 AND spec ->> 'type' = 'universal_pod'",
        )
        .bind(&input.id)
        .execute(&mut *tx)
        .await?;
        sqlx::query("DELETE FROM orchestration_server WHERE id = $1")
            .bind(&input.id)
            .execute(&mut *tx)
            .await?;
        fence::touch_checked(&mut tx, &input.canvas, input.fence.as_ref()).await?;
        tx.commit().await?;
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
///
/// Only this server's rows are written: the view row's `seq` is what tells
/// derivation to look again, and the canvas row is never touched, so a fleet
/// registering at once never collides on it.
#[derive(Debug)]
pub struct RegisterWorkerSession {
    pub server: ServerId,
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

/// The three snapshot slots of a view row, as the registration reads them.
#[derive(sqlx::FromRow)]
struct ViewSlots {
    #[sqlx(json(nullable))]
    desired: Option<ConfigSnapshot>,
    #[sqlx(json(nullable))]
    in_flight: Option<ConfigSnapshot>,
    #[sqlx(json(nullable))]
    applied: Option<ConfigSnapshot>,
}

impl Processor<RegisterWorkerSession> for Db {
    /// The rotated row, or `None` when a live session still holds the server.
    type Output = Option<ServerEntity>;
    type Error = Error;
    #[tracing::instrument(name = "Query-Transaction:RegisterWorkerSession", skip_all, err, fields(server = %input.server))]
    async fn process(&self, input: RegisterWorkerSession) -> Result<Self::Output, Self::Error> {
        let mut tx = self.db().begin().await?;
        // `observed_address` is what the master saw this registration come from;
        // `reported_addresses` and the worker's build are replaced only when the
        // worker sent them (`COALESCE` keeps the previous value otherwise, so an
        // older worker cannot blank a known version).
        let rotated: Option<ServerEntity> = sqlx::query_as(
            "UPDATE orchestration_server
             SET current_dynamic_refresh_key = $2,
                 refresh_key_generation = refresh_key_generation + 1,
                 session_lease_until = $3, last_seen_at = $4,
                 observed_address = $5, observed_at = $4,
                 reported_addresses = COALESCE($6, reported_addresses),
                 agent_version = COALESCE($7, agent_version),
                 agent_arch = COALESCE($8, agent_arch)
             WHERE id = $1 AND (session_lease_until IS NULL OR session_lease_until <= $4)
             RETURNING *",
        )
        .bind(&input.server)
        .bind(&input.digest)
        .bind(input.lease_until)
        .bind(input.now)
        .bind(&input.observed)
        .bind(input.reported.as_ref().map(Json))
        .bind(&input.agent_version)
        .bind(&input.agent_arch)
        .fetch_optional(&mut *tx)
        .await?;
        let Some(server) = rotated else {
            tx.rollback().await?;
            return Ok(None);
        };
        let slots: ViewSlots = sqlx::query_as(
            "SELECT desired, in_flight, applied FROM orchestration_server_config_view
             WHERE server = $1 FOR UPDATE",
        )
        .bind(&input.server)
        .fetch_one(&mut *tx)
        .await?;
        let revision_of = |slot: &Option<ConfigSnapshot>| slot.as_ref().map(|s| s.revision);
        let running = input.running_revision;
        let repair = if running > 0 {
            if revision_of(&slots.desired) == Some(running) {
                Some("SET applied = desired, apply_error = NULL, failed_revision = NULL")
            } else if revision_of(&slots.in_flight) == Some(running) {
                Some("SET applied = in_flight, apply_error = NULL, failed_revision = NULL")
            } else if revision_of(&slots.applied) != Some(running) {
                Some("SET apply_error = 'running revision ' || $2::text || ' unknown'")
            } else {
                None
            }
        } else {
            // A worker running nothing (a fresh install, a wiped state directory,
            // a replay that failed) is not running what an earlier session
            // applied: forget that, so the stream hands the desired revision out
            // again instead of treating the server as converged, and dependants
            // stop counting on listeners nobody serves.
            Some(
                "SET applied = NULL, apply_error = NULL, failed_revision = NULL, failed_pods = '[]'",
            )
        };
        if let Some(assignment) = repair {
            sqlx::query(sqlx::AssertSqlSafe(format!(
                "UPDATE orchestration_server_config_view {assignment} WHERE server = $1"
            )))
            .bind(&input.server)
            .bind(running)
            .execute(&mut *tx)
            .await?;
        }
        sqlx::query(
            "UPDATE orchestration_server_config_view SET in_flight = NULL, seq = seq + 1
             WHERE server = $1",
        )
        .bind(&input.server)
        .execute(&mut *tx)
        .await?;
        tx.commit().await?;
        Ok(Some(server))
    }
}

/// Replaces a server's reported address set with what a live session just
/// discovered. Fenced on the refresh-key generation, and a no-op (no view
/// `seq` bump) when nothing changed.
#[derive(Debug)]
pub struct UpdateReportedAddresses {
    pub server: ServerId,
    pub generation: i64,
    pub reported: ReportedAddresses,
}

impl Processor<UpdateReportedAddresses> for Db {
    /// `true` when the stored set changed (and the view's `seq` moved).
    type Output = bool;
    type Error = Error;
    #[tracing::instrument(name = "Query-Transaction:UpdateReportedAddresses", skip_all, err, fields(server = %input.server))]
    async fn process(&self, input: UpdateReportedAddresses) -> Result<Self::Output, Self::Error> {
        let mut tx = self.db().begin().await?;
        let current: Option<(i64, Option<Json<ReportedAddresses>>)> = sqlx::query_as(
            "SELECT refresh_key_generation, reported_addresses FROM orchestration_server
             WHERE id = $1 FOR UPDATE",
        )
        .bind(&input.server)
        .fetch_optional(&mut *tx)
        .await?;
        let Some((generation, stored)) = current else {
            return Ok(false);
        };
        if generation != input.generation {
            return Ok(false);
        }
        if stored.is_some_and(|stored| stored.0.same_addresses(&input.reported)) {
            return Ok(false);
        }
        sqlx::query("UPDATE orchestration_server SET reported_addresses = $2 WHERE id = $1")
            .bind(&input.server)
            .bind(Json(&input.reported))
            .execute(&mut *tx)
            .await?;
        sqlx::query("UPDATE orchestration_server_config_view SET seq = seq + 1 WHERE server = $1")
            .bind(&input.server)
            .execute(&mut *tx)
            .await?;
        tx.commit().await?;
        Ok(true)
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

impl Processor<ClaimServerWatchSession> for Db {
    /// The claimed row, or `None` when the generation is no longer current.
    type Output = Option<ServerEntity>;
    type Error = Error;
    #[tracing::instrument(name = "Query-Transaction:ClaimServerWatchSession", skip_all, err)]
    async fn process(&self, input: ClaimServerWatchSession) -> Result<Self::Output, Self::Error> {
        let mut tx = self.db().begin().await?;
        let claimed: Option<ServerEntity> = sqlx::query_as(
            "UPDATE orchestration_server
             SET watch_epoch = watch_epoch + 1, session_lease_until = $3, last_seen_at = $4
             WHERE id = $1 AND refresh_key_generation = $2 RETURNING *",
        )
        .bind(&input.server)
        .bind(input.generation)
        .bind(input.lease_until)
        .bind(input.now)
        .fetch_optional(&mut *tx)
        .await?;
        if claimed.is_some() {
            sqlx::query(
                "UPDATE orchestration_server_config_view SET in_flight = NULL WHERE server = $1",
            )
            .bind(&input.server)
            .execute(&mut *tx)
            .await?;
        }
        tx.commit().await?;
        Ok(claimed)
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

impl Processor<RenewServerWatchSession> for Db {
    /// `false` once the session has been fenced out; the stream must then end.
    type Output = bool;
    type Error = Error;
    #[tracing::instrument(name = "Query:RenewServerWatchSession", skip_all, err)]
    async fn process(&self, input: RenewServerWatchSession) -> Result<Self::Output, Self::Error> {
        let renewed: Option<ServerId> = sqlx::query_scalar(
            "UPDATE orchestration_server SET session_lease_until = $4, last_seen_at = $5
             WHERE id = $1 AND refresh_key_generation = $2 AND watch_epoch = $3
             RETURNING id",
        )
        .bind(input.server)
        .bind(input.generation)
        .bind(input.epoch)
        .bind(input.lease_until)
        .bind(input.now)
        .fetch_optional(self.db())
        .await?;
        Ok(renewed.is_some())
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

impl Processor<ReleaseServerWatchSession> for Db {
    type Output = ();
    type Error = Error;
    #[tracing::instrument(name = "Query:ReleaseServerWatchSession", skip_all, err)]
    async fn process(&self, input: ReleaseServerWatchSession) -> Result<Self::Output, Self::Error> {
        sqlx::query(
            "UPDATE orchestration_server SET session_lease_until = NULL
             WHERE id = $1 AND refresh_key_generation = $2 AND watch_epoch = $3",
        )
        .bind(input.server)
        .bind(input.generation)
        .bind(input.epoch)
        .execute(self.db())
        .await?;
        Ok(())
    }
}

pub struct FindServerByRefreshKeyDigest {
    pub digest: String,
}

impl Processor<FindServerByRefreshKeyDigest> for Db {
    type Output = Option<ServerEntity>;
    type Error = Error;
    #[tracing::instrument(name = "Query:FindServerByRefreshKeyDigest", skip_all, err)]
    async fn process(
        &self,
        input: FindServerByRefreshKeyDigest,
    ) -> Result<Self::Output, Self::Error> {
        Ok(sqlx::query_as(
            "SELECT * FROM orchestration_server WHERE current_dynamic_refresh_key = $1 LIMIT 1",
        )
        .bind(input.digest)
        .fetch_optional(self.db())
        .await?)
    }
}
