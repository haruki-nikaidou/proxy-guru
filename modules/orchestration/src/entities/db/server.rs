use crate::entities::db::canvas::{CanvasFence, CanvasId, CanvasUiPosition};
use crate::entities::db::fence;
use crate::entities::db::health::ServerHealthStatus;
use crate::entities::db::view::ConfigSnapshot;
use base::db::{Db, Error};
use db_types::{table_record, text_enum};
use kanau::processor::Processor;
use serde::{Deserialize, Serialize};
use sqlx::types::Json;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};
use time::OffsetDateTime;

table_record!(ServerId, "orchestration_server");

/// The conflict reported when a server that still has pods is deleted.
pub const SERVER_HAS_PODS: &str = "server still has pods; delete them first";

#[derive(Debug, Clone)]
pub struct ServerEntity {
    pub id: ServerId,
    pub canvas: CanvasId,
    pub name: String,
    pub icon: String,
    pub comment: String,
    pub position: CanvasUiPosition,
    pub ipv6_resolve: ServerIpv6Resolve,
    pub log_level: ServerLogLevel,
    /// This server's side of every QUIC relay link it takes part in.
    pub quic: ServerQuic,
    pub current_dynamic_refresh_key: Option<String>,
    pub refresh_key_generation: i64,
    /// Monotonic claim counter for the single live `WatchConfig` stream. Bumped by
    /// every claim, so the newest stream always wins and no lease can strand a
    /// server after a master or worker crash.
    pub watch_epoch: i64,
    /// While this is in the future, one worker session owns the server: another
    /// registration is refused until it lapses or the owning stream releases it.
    pub session_lease_until: Option<OffsetDateTime>,
    /// When the session holding `refresh_key_generation` registered; `None` until
    /// a worker ever has.
    pub registered_at: Option<OffsetDateTime>,
    pub last_seen_at: Option<OffsetDateTime>,
    /// When the last health report was accepted. Distinct from `last_seen_at`
    /// (the watch stream's heartbeat) so a live config stream cannot mask a dead
    /// health stream.
    pub last_health_report_at: Option<OffsetDateTime>,
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
    pub reported_addresses: Option<ReportedAddresses>,
    /// The peer address the master saw the last registration come from.
    pub observed_address: Option<String>,
    pub observed_at: Option<OffsetDateTime>,
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
    pub agent_key_issued_at: Option<OffsetDateTime>,
    /// ISO 3166-1 alpha-2 country of `country_address`, upper-case, as the master
    /// looked it up; `None` until a lookup for that address succeeded.
    pub country: Option<String>,
    /// The IPv4 address `country` belongs to. Compared with [`Self::v4_address`]
    /// before the country is shown, so a server whose address moved never wears
    /// the old address's flag.
    pub country_address: Option<String>,
    /// The last lookup for `country_address`, successful or not.
    pub country_checked_at: Option<OffsetDateTime>,
    /// What the server's worker reads beyond the config every worker reads, as
    /// it reported on its last registration (`route_table`, `relay_confirm`).
    pub capabilities: Vec<String>,
}

/// The stored shape of a server row, as the query macros fill it: `position` is
/// two columns and the two `jsonb` documents arrive wrapped. Every read of the
/// table goes through this and converts once.
pub(crate) struct ServerRow {
    pub(crate) id: ServerId,
    pub(crate) canvas: CanvasId,
    pub(crate) name: String,
    pub(crate) icon: String,
    pub(crate) comment: String,
    pub(crate) position_x: i64,
    pub(crate) position_y: i64,
    pub(crate) ipv6_resolve: ServerIpv6Resolve,
    pub(crate) log_level: ServerLogLevel,
    pub(crate) quic: Json<ServerQuic>,
    pub(crate) current_dynamic_refresh_key: Option<String>,
    pub(crate) refresh_key_generation: i64,
    pub(crate) watch_epoch: i64,
    pub(crate) session_lease_until: Option<OffsetDateTime>,
    pub(crate) registered_at: Option<OffsetDateTime>,
    pub(crate) last_seen_at: Option<OffsetDateTime>,
    pub(crate) last_health_report_at: Option<OffsetDateTime>,
    pub(crate) health_status: ServerHealthStatus,
    pub(crate) override_v4: Option<String>,
    pub(crate) override_v6: Option<String>,
    pub(crate) extra_addresses: Vec<String>,
    pub(crate) reported_addresses: Option<Json<ReportedAddresses>>,
    pub(crate) observed_address: Option<String>,
    pub(crate) observed_at: Option<OffsetDateTime>,
    pub(crate) agent_version: Option<String>,
    pub(crate) agent_arch: Option<String>,
    pub(crate) agent_unit: Option<String>,
    pub(crate) agent_update_requested: Option<String>,
    pub(crate) agent_update_error: Option<String>,
    pub(crate) agent_key_digest: Option<String>,
    pub(crate) agent_key_issued_at: Option<OffsetDateTime>,
    pub(crate) country: Option<String>,
    pub(crate) country_address: Option<String>,
    pub(crate) country_checked_at: Option<OffsetDateTime>,
    pub(crate) capabilities: Vec<String>,
}

impl From<ServerRow> for ServerEntity {
    fn from(row: ServerRow) -> Self {
        Self {
            id: row.id,
            canvas: row.canvas,
            name: row.name,
            icon: row.icon,
            comment: row.comment,
            position: CanvasUiPosition {
                x: row.position_x,
                y: row.position_y,
            },
            ipv6_resolve: row.ipv6_resolve,
            log_level: row.log_level,
            quic: row.quic.0,
            current_dynamic_refresh_key: row.current_dynamic_refresh_key,
            refresh_key_generation: row.refresh_key_generation,
            watch_epoch: row.watch_epoch,
            session_lease_until: row.session_lease_until,
            registered_at: row.registered_at,
            last_seen_at: row.last_seen_at,
            last_health_report_at: row.last_health_report_at,
            health_status: row.health_status,
            override_v4: row.override_v4,
            override_v6: row.override_v6,
            extra_addresses: row.extra_addresses,
            reported_addresses: row.reported_addresses.map(|json| json.0),
            observed_address: row.observed_address,
            observed_at: row.observed_at,
            agent_version: row.agent_version,
            agent_arch: row.agent_arch,
            agent_unit: row.agent_unit,
            agent_update_requested: row.agent_update_requested,
            agent_update_error: row.agent_update_error,
            agent_key_digest: row.agent_key_digest,
            agent_key_issued_at: row.agent_key_issued_at,
            country: row.country,
            country_address: row.country_address,
            country_checked_at: row.country_checked_at,
            capabilities: row.capabilities,
        }
    }
}

/// The address set a worker discovers about itself and sends with `Register`
/// and, when it changes, with a health report.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReportedAddresses {
    pub public_v4: Option<String>,
    pub public_v6: Option<String>,
    pub interfaces: Vec<String>,
    #[serde(with = "time::serde::rfc3339")]
    pub reported_at: OffsetDateTime,
}

impl ReportedAddresses {
    /// Same addresses, whenever they were reported.
    pub fn same_addresses(&self, other: &Self) -> bool {
        self.public_v4 == other.public_v4
            && self.public_v6 == other.public_v6
            && self.interfaces == other.interfaces
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
    /// Every address a peer could dial, most preferred first: the IPv4 override,
    /// the reported public IPv4, the observed address when it is IPv4, then the
    /// same chain for IPv6. A slot that is empty or does not parse is `None`.
    fn address_candidates(&self) -> [(Option<IpAddr>, AddressSource); 6] {
        let parse = |s: &Option<String>| s.as_deref().and_then(|v| v.parse::<IpAddr>().ok());
        let reported = self.reported_addresses.as_ref();
        let observed = parse(&self.observed_address);
        [
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
        ]
    }

    /// The address other servers dial by default: the IPv4 override, else the
    /// reported public IPv4, else the address the master observed, else the same
    /// chain for IPv6. IPv4 first because that is what most peers can reach; an
    /// edge that should dial over IPv6 says so with its IP family.
    pub fn effective_address(&self) -> Option<(IpAddr, AddressSource)> {
        self.address_candidates()
            .into_iter()
            .find_map(|(address, source)| address.map(|a| (a, source)))
    }

    /// The server's IPv4 address: the first IPv4 of the same chain, which is the
    /// one the dashboard's IPv4 line shows and the one its country is looked up
    /// for.
    pub fn v4_address(&self) -> Option<Ipv4Addr> {
        self.address_candidates()
            .into_iter()
            .find_map(|(address, _)| match address {
                Some(IpAddr::V4(v4)) => Some(v4),
                _ => None,
            })
    }

    /// The server's IPv6 address: the first IPv6 of the same chain, which is the
    /// one the dashboard's IPv6 line shows and the one edges dialing over IPv6
    /// dial.
    pub fn v6_address(&self) -> Option<Ipv6Addr> {
        self.address_candidates()
            .into_iter()
            .find_map(|(address, _)| match address {
                Some(IpAddr::V6(v6)) => Some(v6),
                _ => None,
            })
    }

    /// The looked-up country, only while it still belongs to the server's IPv4.
    pub fn country_of_v4(&self) -> Option<&str> {
        let current = self.v4_address()?.to_string();
        (self.country_address.as_deref() == Some(current.as_str()))
            .then_some(self.country.as_deref())
            .flatten()
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

/// The level a server's worker logs at, written into its config as `[log] level`.
/// A worker accepts any `tracing` `EnvFilter` directive there; a server row holds
/// one of these five, which is what the dashboard offers.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ServerLogLevel {
    Trace,
    Debug,
    Info,
    Warn,
    Error,
}
text_enum!(ServerLogLevel {
    Trace => "trace",
    Debug => "debug",
    Info => "info",
    Warn => "warn",
    Error => "error",
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
    pub log_level: ServerLogLevel,
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
        // ever observe a server without the row its rollout is tracked in. The
        // tree's root comes first (`fence`'s lock order).
        let mut tx = self.db().begin().await?;
        fence::touch(&mut tx, &input.canvas).await?;
        let server: ServerEntity = sqlx::query_file_as!(
            ServerRow,
            "sql/create_server.sql",
            ServerId::new() as _,
            input.canvas as _,
            input.name,
            input.icon,
            input.comment,
            input.position.x,
            input.position.y,
            input.ipv6_resolve as _,
            input.log_level as _,
            input.override_v4,
            input.override_v6,
            &input.extra_addresses
        )
        .fetch_one(&mut *tx)
        .await?
        .into();
        sqlx::query!(
            "INSERT INTO orchestration_server_config_view (id, server) VALUES ($1, $2)",
            crate::entities::db::view::ServerConfigViewId::new() as _,
            &server.id as _
        )
        .execute(&mut *tx)
        .await?;
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
            sqlx::query_file_as!(ServerRow, "sql/find_server_by_id.sql", input.id as _)
                .fetch_optional(self.db())
                .await?
                .map(ServerEntity::from),
        )
    }
}

/// Every server of every canvas, for the fleet-wide passes that are no one
/// canvas's concern.
#[derive(Debug)]
pub struct ListAllServers;

impl Processor<ListAllServers> for Db {
    type Output = Vec<ServerEntity>;
    type Error = Error;
    #[tracing::instrument(name = "Query:ListAllServers", skip_all, err)]
    async fn process(&self, _: ListAllServers) -> Result<Self::Output, Self::Error> {
        Ok(sqlx::query_file_as!(ServerRow, "sql/list_all_servers.sql")
            .fetch_all(self.db())
            .await?
            .into_iter()
            .map(ServerEntity::from)
            .collect())
    }
}

/// Records a country lookup: `country` (or `None` when it failed) for the IPv4
/// `address`, made at `checked_at`. All three `None` forget the lookup of an
/// address the server no longer has. Nothing is fenced: a lookup that raced an
/// address change stores an answer for the old address, which is never shown and
/// is redone by the next pass.
#[derive(Debug)]
pub struct SetServerCountry {
    pub server: ServerId,
    pub address: Option<String>,
    pub country: Option<String>,
    pub checked_at: Option<OffsetDateTime>,
}

impl Processor<SetServerCountry> for Db {
    type Output = ();
    type Error = Error;
    #[tracing::instrument(name = "Query:SetServerCountry", skip_all, err)]
    async fn process(&self, input: SetServerCountry) -> Result<Self::Output, Self::Error> {
        sqlx::query!(
            "UPDATE orchestration_server
                SET country = $2, country_address = $3, country_checked_at = $4
              WHERE id = $1",
            input.server as _,
            input.country,
            input.address,
            input.checked_at
        )
        .execute(self.db())
        .await?;
        Ok(())
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
        Ok(sqlx::query_file_as!(
            ServerRow,
            "sql/list_servers_by_canvas.sql",
            input.canvas as _
        )
        .fetch_all(self.db())
        .await?
        .into_iter()
        .map(ServerEntity::from)
        .collect())
    }
}

pub struct UpdateServerSettings {
    pub id: ServerId,
    pub canvas: CanvasId,
    pub name: String,
    pub icon: String,
    pub comment: String,
    pub ipv6_resolve: ServerIpv6Resolve,
    pub log_level: ServerLogLevel,
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
        // The root before the server row (`fence`'s lock order), the order a
        // server delete takes them in.
        fence::touch_checked(&mut tx, &input.canvas, input.fence.as_ref()).await?;
        let server: ServerEntity = sqlx::query_file_as!(
            ServerRow,
            "sql/update_server_settings.sql",
            input.id as _,
            input.name,
            input.icon,
            input.comment,
            input.ipv6_resolve as _,
            input.log_level as _,
            input.override_v4,
            input.override_v6,
            &input.extra_addresses,
            input.agent_unit,
            Json(input.quic) as _
        )
        .fetch_one(&mut *tx)
        .await?
        .into();
        tx.commit().await?;
        Ok(server)
    }
}

/// Issues (or replaces) a server's own agent key — only its digest is stored —
/// and names the systemd instance the install command carrying it targets. A
/// fresh install starts the agent's history over: a pending update request and
/// the error of an earlier attempt belong to the install it replaces.
#[derive(Debug)]
pub struct SetServerAgentKey {
    pub id: ServerId,
    pub digest: String,
    pub unit: String,
    pub now: OffsetDateTime,
}

impl Processor<SetServerAgentKey> for Db {
    type Output = ServerEntity;
    type Error = Error;
    #[tracing::instrument(name = "Query:SetServerAgentKey", skip_all, err, fields(id = %input.id))]
    async fn process(&self, input: SetServerAgentKey) -> Result<Self::Output, Self::Error> {
        Ok(sqlx::query_file_as!(
            ServerRow,
            "sql/set_server_agent_key.sql",
            input.id as _,
            input.digest,
            input.now,
            input.unit
        )
        .fetch_one(self.db())
        .await?
        .into())
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
        Ok(sqlx::query_file_as!(
            ServerRow,
            "sql/set_agent_update_requested.sql",
            input.id as _,
            input.version
        )
        .fetch_one(self.db())
        .await?
        .into())
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
        sqlx::query_file!(
            "sql/settle_agent_update.sql",
            input.id as _,
            input.error,
            input.reported_version
        )
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
        Ok(sqlx::query_file_as!(
            ServerRow,
            "sql/find_server_by_agent_key_digest.sql",
            input.digest
        )
        .fetch_optional(self.db())
        .await?
        .map(ServerEntity::from))
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
        sqlx::query!(
            "UPDATE orchestration_server SET position_x = $2, position_y = $3 WHERE id = $1",
            input.id as _,
            input.position.x,
            input.position.y
        )
        .execute(self.db())
        .await?;
        Ok(())
    }
}

/// Deletes a server with its config view and health history.
///
/// Refuses ([`SERVER_HAS_PODS`]) while any pod is still placed on the server: a
/// pod whose server is gone belongs to nobody, and derivation could then neither
/// publish nor report it. The service pre-checks the same thing and phrases the
/// message; this is the race guard, and the pod's `server` foreign key is the
/// guard behind the guard.
///
/// The root is bumped before anything else (`fence`'s lock order). The delete
/// cascades to the server's view row, which a derivation commit rewrites while
/// it holds the root: bumping the root after the cascade made each wait on the
/// other until PostgreSQL aborted one. Holding the root also makes the pod count
/// exact, since every write that places a pod takes the root first.
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
        fence::touch_checked(&mut tx, &input.canvas, input.fence.as_ref()).await?;
        let pods: i64 = sqlx::query_scalar!(
            r#"SELECT count(*) AS "count!" FROM orchestration_pod WHERE server = $1"#,
            &input.id as _
        )
        .fetch_one(&mut *tx)
        .await?;
        if pods > 0 {
            return Err(Error::Conflict(SERVER_HAS_PODS));
        }
        // Its view, health history and group memberships cascade.
        sqlx::query!(
            "DELETE FROM orchestration_server WHERE id = $1",
            &input.id as _
        )
        .execute(&mut *tx)
        .await?;
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
    pub now: OffsetDateTime,
    /// The lease deadline the new session gets.
    pub lease_until: OffsetDateTime,
    /// The revision the worker says it is running; `0` for a fresh worker.
    pub running_revision: i64,
    /// The peer address this registration arrived from, if known.
    pub observed: Option<String>,
    /// What the worker reported about its addresses; `None` keeps the stored set.
    pub reported: Option<ReportedAddresses>,
    /// The worker's build, when it reported one; `None` keeps the stored values.
    pub agent_version: Option<String>,
    pub agent_arch: Option<String>,
    /// What the worker reads beyond the tree form (`route_table`, ...): always
    /// replaced, since a worker too old to report any has none.
    pub capabilities: Vec<String>,
}

/// The three snapshot slots of a view row, as the registration reads them.
struct ViewSlots {
    desired: Option<Json<ConfigSnapshot>>,
    in_flight: Option<Json<ConfigSnapshot>>,
    applied: Option<Json<ConfigSnapshot>>,
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
        let rotated: Option<ServerEntity> = sqlx::query_file_as!(
            ServerRow,
            "sql/register_worker_session.sql",
            &input.server as _,
            input.digest,
            input.lease_until,
            input.now,
            input.observed,
            input.reported.as_ref().map(Json) as _,
            input.agent_version,
            input.agent_arch,
            &input.capabilities
        )
        .fetch_optional(&mut *tx)
        .await?
        .map(ServerEntity::from);
        let Some(server) = rotated else {
            tx.rollback().await?;
            return Ok(None);
        };
        let slots = sqlx::query_as!(
            ViewSlots,
            r#"SELECT desired AS "desired: Json<ConfigSnapshot>",
                      in_flight AS "in_flight: Json<ConfigSnapshot>",
                      applied AS "applied: Json<ConfigSnapshot>"
               FROM orchestration_server_config_view
               WHERE server = $1 FOR UPDATE"#,
            &input.server as _
        )
        .fetch_one(&mut *tx)
        .await?;
        let revision_of = |slot: &Option<Json<ConfigSnapshot>>| slot.as_ref().map(|s| s.revision);
        let running = input.running_revision;
        if running > 0 {
            if revision_of(&slots.desired) == Some(running) {
                sqlx::query!(
                    "UPDATE orchestration_server_config_view
                     SET applied = desired, apply_error = NULL, failed_revision = NULL
                     WHERE server = $1",
                    &input.server as _
                )
                .execute(&mut *tx)
                .await?;
            } else if revision_of(&slots.in_flight) == Some(running) {
                sqlx::query!(
                    "UPDATE orchestration_server_config_view
                     SET applied = in_flight, apply_error = NULL, failed_revision = NULL
                     WHERE server = $1",
                    &input.server as _
                )
                .execute(&mut *tx)
                .await?;
            } else if revision_of(&slots.applied) != Some(running) {
                sqlx::query!(
                    "UPDATE orchestration_server_config_view
                     SET apply_error = 'running revision ' || $2::bigint::text || ' unknown'
                     WHERE server = $1",
                    &input.server as _,
                    running
                )
                .execute(&mut *tx)
                .await?;
            }
        } else {
            // A worker running nothing (a fresh install, a wiped state directory,
            // a replay that failed) is not running what an earlier session
            // applied: forget that, so the stream hands the desired revision out
            // again instead of treating the server as converged, and dependants
            // stop counting on listeners nobody serves.
            sqlx::query!(
                "UPDATE orchestration_server_config_view
                 SET applied = NULL, apply_error = NULL, failed_revision = NULL,
                     failed_pods = '[]'
                 WHERE server = $1",
                &input.server as _
            )
            .execute(&mut *tx)
            .await?;
        }
        sqlx::query!(
            "UPDATE orchestration_server_config_view SET in_flight = NULL, seq = seq + 1
             WHERE server = $1",
            &input.server as _
        )
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
        let current = sqlx::query!(
            r#"SELECT refresh_key_generation,
                      reported_addresses AS "reported_addresses: Json<ReportedAddresses>"
               FROM orchestration_server
               WHERE id = $1 FOR UPDATE"#,
            &input.server as _
        )
        .fetch_optional(&mut *tx)
        .await?;
        let Some(current) = current else {
            return Ok(false);
        };
        if current.refresh_key_generation != input.generation {
            return Ok(false);
        }
        if current
            .reported_addresses
            .is_some_and(|stored| stored.0.same_addresses(&input.reported))
        {
            return Ok(false);
        }
        sqlx::query!(
            "UPDATE orchestration_server SET reported_addresses = $2 WHERE id = $1",
            &input.server as _,
            Json(&input.reported) as _
        )
        .execute(&mut *tx)
        .await?;
        sqlx::query!(
            "UPDATE orchestration_server_config_view SET seq = seq + 1 WHERE server = $1",
            &input.server as _
        )
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
    pub now: OffsetDateTime,
    pub lease_until: OffsetDateTime,
}

impl Processor<ClaimServerWatchSession> for Db {
    /// The claimed row, or `None` when the generation is no longer current.
    type Output = Option<ServerEntity>;
    type Error = Error;
    #[tracing::instrument(name = "Query-Transaction:ClaimServerWatchSession", skip_all, err)]
    async fn process(&self, input: ClaimServerWatchSession) -> Result<Self::Output, Self::Error> {
        let mut tx = self.db().begin().await?;
        let claimed: Option<ServerEntity> = sqlx::query_file_as!(
            ServerRow,
            "sql/claim_server_watch_session.sql",
            &input.server as _,
            input.generation,
            input.lease_until,
            input.now
        )
        .fetch_optional(&mut *tx)
        .await?
        .map(ServerEntity::from);
        if claimed.is_some() {
            sqlx::query!(
                "UPDATE orchestration_server_config_view SET in_flight = NULL WHERE server = $1",
                &input.server as _
            )
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
    pub now: OffsetDateTime,
    pub lease_until: OffsetDateTime,
}

impl Processor<RenewServerWatchSession> for Db {
    /// `false` once the session has been fenced out; the stream must then end.
    type Output = bool;
    type Error = Error;
    #[tracing::instrument(name = "Query:RenewServerWatchSession", skip_all, err)]
    async fn process(&self, input: RenewServerWatchSession) -> Result<Self::Output, Self::Error> {
        let renewed: Option<ServerId> = sqlx::query_scalar!(
            r#"UPDATE orchestration_server SET session_lease_until = $4, last_seen_at = $5
               WHERE id = $1 AND refresh_key_generation = $2 AND watch_epoch = $3
               RETURNING id AS "id: ServerId""#,
            input.server as _,
            input.generation,
            input.epoch,
            input.lease_until,
            input.now
        )
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
        sqlx::query!(
            "UPDATE orchestration_server SET session_lease_until = NULL
             WHERE id = $1 AND refresh_key_generation = $2 AND watch_epoch = $3",
            input.server as _,
            input.generation,
            input.epoch
        )
        .execute(self.db())
        .await?;
        Ok(())
    }
}

/// Takes the watch session away from every offline server whose lease is still
/// renewed for a registration older than `registered_before` that has not
/// reported since: the lease is dropped and the epoch moves, exactly as when a
/// server goes offline.
///
/// Going offline cannot catch this session. A worker that registers while its
/// server is already `Offline` and loses its connection before the first report
/// changes no status, yet a proxy that never saw the connection die — it has
/// nothing to send, so nothing times out — keeps the `WatchConfig` stream open, and
/// the master renews the lease on its own timer for as long as it does. Every
/// registration meanwhile is refused. Without the lease the stream fails its next
/// renew and ends, and the worker's next registration is accepted.
///
/// One statement, so every condition holds for the row as it is written: a report
/// that landed in the meantime has taken the server out of `Offline`, and the
/// session keeps its lease.
#[derive(Debug)]
pub struct RevokeSilentWatchSessions {
    pub now: OffsetDateTime,
    /// Only a registration strictly older than this has been silent long enough.
    pub registered_before: OffsetDateTime,
}

impl Processor<RevokeSilentWatchSessions> for Db {
    /// The servers whose session was revoked.
    type Output = Vec<ServerId>;
    type Error = Error;
    #[tracing::instrument(name = "Query:RevokeSilentWatchSessions", skip_all, err)]
    async fn process(&self, input: RevokeSilentWatchSessions) -> Result<Self::Output, Self::Error> {
        Ok(sqlx::query_file_scalar!(
            "sql/revoke_silent_watch_sessions.sql",
            input.now,
            input.registered_before
        )
        .fetch_all(self.db())
        .await?)
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
        Ok(sqlx::query_file_as!(
            ServerRow,
            "sql/find_server_by_refresh_key_digest.sql",
            input.digest
        )
        .fetch_optional(self.db())
        .await?
        .map(ServerEntity::from))
    }
}

pub struct FindCanvasOfServer {
    pub server: ServerId,
}

impl Processor<FindCanvasOfServer> for Db {
    type Output = Option<CanvasId>;
    type Error = Error;
    #[tracing::instrument(name = "Query:FindCanvasOfServer", skip_all, err)]
    async fn process(&self, input: FindCanvasOfServer) -> Result<Self::Output, Self::Error> {
        Ok(sqlx::query_scalar!(
            r#"SELECT canvas AS "canvas: CanvasId" FROM orchestration_server WHERE id = $1"#,
            input.server as _
        )
        .fetch_optional(self.db())
        .await?)
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    /// `reported_addresses` is a `jsonb` column, so the report's timestamp is
    /// part of the persisted shape and stays RFC 3339 in both directions.
    #[test]
    fn a_stored_address_report_reads_back_from_rfc3339() {
        let stored = r#"{"public_v4":"203.0.113.7","public_v6":null,"interfaces":["eth0"],"reported_at":"2026-09-21T14:14:56.789012Z"}"#;
        let reported: ReportedAddresses = serde_json::from_str(stored).unwrap();
        assert_eq!(
            reported.reported_at.unix_timestamp_nanos(),
            1_790_000_096_789_012_000
        );
        assert_eq!(
            serde_json::to_value(&reported).unwrap()["reported_at"],
            "2026-09-21T14:14:56.789012Z"
        );
    }
}
