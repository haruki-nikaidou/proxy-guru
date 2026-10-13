use crate::entities::db::canvas::{CanvasFence, CanvasId, CanvasUiPosition};
use crate::entities::db::fence::{Touch, TouchChecked};
use crate::entities::db::health::ServerHealthStatus;
use crate::entities::db::view::ConfigSnapshot;
use crate::entities::db::tables::{
    OrchestrationCanvas, OrchestrationPod, OrchestrationServer, OrchestrationServerConfigView,
};
use base::db::{Db, Error};
use db_types::{table_record, text_enum};
use kanau::processor::Processor;
use serde::{Deserialize, Serialize};
use sqlx::types::Json;
use sqlx::{PgConnection, Postgres};
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};
use time::OffsetDateTime;
use wakuwaku::entity::sqlx::backend::PgSource;
use wakuwaku::entity::sqlx::transaction::{Either, Tx, TxEnd, TxFlow, TxState};
use wakuwaku::entity::{Execute, Query, Read, Write};

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

/// Inserts the server row of a [`CreateServer`].
struct InsertServerRow<'a> {
    input: &'a CreateServer,
}

impl Query for InsertServerRow<'_> {
    type Effect = Write<OrchestrationServer>;
}

impl Execute<PgSource> for InsertServerRow<'_> {
    type Output = ServerEntity;
    type Error = Error;
    async fn execute(self, conn: &mut PgConnection) -> Result<Self::Output, Self::Error> {
        let input = self.input;
        Ok(sqlx::query_file_as!(
            ServerRow,
            "sql/create_server.sql",
            ServerId::new() as _,
            &input.canvas as _,
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
        .fetch_one(conn)
        .await?
        .into())
    }
}

/// Inserts the (empty) config view row of a new server.
struct InsertConfigViewRow<'a> {
    server: &'a ServerId,
}

impl Query for InsertConfigViewRow<'_> {
    type Effect = Write<OrchestrationServerConfigView>;
}

impl Execute<PgSource> for InsertConfigViewRow<'_> {
    type Output = ();
    type Error = Error;
    async fn execute(self, conn: &mut PgConnection) -> Result<Self::Output, Self::Error> {
        sqlx::query!(
            "INSERT INTO orchestration_server_config_view (id, server) VALUES ($1, $2)",
            crate::entities::db::view::ServerConfigViewId::new() as _,
            self.server as _
        )
        .execute(conn)
        .await?;
        Ok(())
    }
}

// The terminal state carries the committed row; the flow value lives for one
// transaction, so boxing it would only add an allocation.
#[allow(clippy::large_enum_variant)]
enum CreateServerFlow {
    Start(CreateServer),
    End(TxEnd<CreateServerFlow>),
}

impl Query for CreateServerFlow {
    type Effect = (
        Write<OrchestrationCanvas>,
        Write<OrchestrationServer>,
        Write<OrchestrationServerConfigView>,
    );
}

impl From<TxEnd<CreateServerFlow>> for CreateServerFlow {
    fn from(end: TxEnd<CreateServerFlow>) -> Self {
        CreateServerFlow::End(end)
    }
}

impl TxFlow for CreateServerFlow {
    type Database = Postgres;
    type Commit = ServerEntity;
    type Abort = Result<ServerEntity, Error>;
    type Report = ();

    async fn step(tx: Tx<'_, Self>, state: Self) -> TxState<'_, Self> {
        let CreateServerFlow::Start(input) = state else {
            return tx.end(state).await;
        };
        // The server and its (empty) config view are one write: no code path may
        // ever observe a server without the row its rollout is tracked in. The
        // tree's root comes first (`fence`'s lock order).
        let (tx, r) = tx.run(Touch { canvas: &input.canvas }).await;
        if let Err(e) = r {
            return base::db::rollback(tx, e).await;
        }
        let (tx, r) = tx.run(InsertServerRow { input: &input }).await;
        let server = match r {
            Ok(v) => v,
            Err(e) => return base::db::rollback(tx, e).await,
        };
        let (tx, r) = tx.run(InsertConfigViewRow { server: &server.id }).await;
        if let Err(e) = r {
            return base::db::rollback(tx, e).await;
        }
        base::db::commit(tx, server).await
    }

    fn finished(self) -> Either<Self, TxEnd<Self>> {
        match self {
            CreateServerFlow::End(end) => Either::Right(end),
            state => Either::Left(state),
        }
    }
}

impl Processor<CreateServer> for Db {
    type Output = ServerEntity;
    type Error = Error;
    #[tracing::instrument(name = "Query-Transaction:CreateServer", skip_all, err, fields(canvas = %input.canvas))]
    async fn process(&self, input: CreateServer) -> Result<Self::Output, Self::Error> {
        base::db::transact(self, CreateServerFlow::Start(input)).await
    }
}

#[derive(Debug)]
pub struct FindServerById {
    pub id: ServerId,
}

impl Query for FindServerById {
    type Effect = Read<OrchestrationServer>;
}

impl Execute<PgSource> for FindServerById {
    type Output = Option<ServerEntity>;
    type Error = Error;
    #[tracing::instrument(name = "Query:FindServerById", skip_all, err)]
    async fn execute(self, conn: &mut PgConnection) -> Result<Self::Output, Self::Error> {
        Ok(
            sqlx::query_file_as!(ServerRow, "sql/find_server_by_id.sql", self.id as _)
                .fetch_optional(conn)
                .await?
                .map(ServerEntity::from),
        )
    }
}

/// Every server of every canvas, for the fleet-wide passes that are no one
/// canvas's concern.
#[derive(Debug)]
pub struct ListAllServers;

impl Query for ListAllServers {
    type Effect = Read<OrchestrationServer>;
}

impl Execute<PgSource> for ListAllServers {
    type Output = Vec<ServerEntity>;
    type Error = Error;
    #[tracing::instrument(name = "Query:ListAllServers", skip_all, err)]
    async fn execute(self, conn: &mut PgConnection) -> Result<Self::Output, Self::Error> {
        Ok(sqlx::query_file_as!(ServerRow, "sql/list_all_servers.sql")
            .fetch_all(conn)
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

impl Query for SetServerCountry {
    type Effect = Write<OrchestrationServer>;
}

impl Execute<PgSource> for SetServerCountry {
    type Output = ();
    type Error = Error;
    #[tracing::instrument(name = "Query:SetServerCountry", skip_all, err)]
    async fn execute(self, conn: &mut PgConnection) -> Result<Self::Output, Self::Error> {
        sqlx::query!(
            "UPDATE orchestration_server
                SET country = $2, country_address = $3, country_checked_at = $4
              WHERE id = $1",
            self.server as _,
            self.country,
            self.address,
            self.checked_at
        )
        .execute(conn)
        .await?;
        Ok(())
    }
}

#[derive(Debug)]
pub struct ListServersByCanvas {
    pub canvas: CanvasId,
}

impl Query for ListServersByCanvas {
    type Effect = Read<OrchestrationServer>;
}

impl Execute<PgSource> for ListServersByCanvas {
    type Output = Vec<ServerEntity>;
    type Error = Error;
    #[tracing::instrument(name = "Query:ListServersByCanvas", skip_all, err)]
    async fn execute(self, conn: &mut PgConnection) -> Result<Self::Output, Self::Error> {
        Ok(sqlx::query_file_as!(
            ServerRow,
            "sql/list_servers_by_canvas.sql",
            self.canvas as _
        )
        .fetch_all(conn)
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

/// Writes the settings of an [`UpdateServerSettings`] to the server row.
struct UpdateServerRow<'a> {
    input: &'a UpdateServerSettings,
}

impl Query for UpdateServerRow<'_> {
    type Effect = Write<OrchestrationServer>;
}

impl Execute<PgSource> for UpdateServerRow<'_> {
    type Output = ServerEntity;
    type Error = Error;
    async fn execute(self, conn: &mut PgConnection) -> Result<Self::Output, Self::Error> {
        let input = self.input;
        Ok(sqlx::query_file_as!(
            ServerRow,
            "sql/update_server_settings.sql",
            &input.id as _,
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
        .fetch_one(conn)
        .await?
        .into())
    }
}

// See `CreateServerFlow`.
#[allow(clippy::large_enum_variant)]
enum UpdateServerSettingsFlow {
    Start(UpdateServerSettings),
    End(TxEnd<UpdateServerSettingsFlow>),
}

impl Query for UpdateServerSettingsFlow {
    type Effect = (Write<OrchestrationCanvas>, Write<OrchestrationServer>);
}

impl From<TxEnd<UpdateServerSettingsFlow>> for UpdateServerSettingsFlow {
    fn from(end: TxEnd<UpdateServerSettingsFlow>) -> Self {
        UpdateServerSettingsFlow::End(end)
    }
}

impl TxFlow for UpdateServerSettingsFlow {
    type Database = Postgres;
    type Commit = ServerEntity;
    type Abort = Result<ServerEntity, Error>;
    type Report = ();

    async fn step(tx: Tx<'_, Self>, state: Self) -> TxState<'_, Self> {
        let UpdateServerSettingsFlow::Start(input) = state else {
            return tx.end(state).await;
        };
        // The root before the server row (`fence`'s lock order), the order a
        // server delete takes them in.
        let (tx, r) = tx
            .run(TouchChecked {
                canvas: &input.canvas,
                fence: input.fence.as_ref(),
            })
            .await;
        if let Err(e) = r {
            return base::db::rollback(tx, e).await;
        }
        let (tx, r) = tx.run(UpdateServerRow { input: &input }).await;
        match r {
            Ok(server) => base::db::commit(tx, server).await,
            Err(e) => base::db::rollback(tx, e).await,
        }
    }

    fn finished(self) -> Either<Self, TxEnd<Self>> {
        match self {
            UpdateServerSettingsFlow::End(end) => Either::Right(end),
            state => Either::Left(state),
        }
    }
}

impl Processor<UpdateServerSettings> for Db {
    type Output = ServerEntity;
    type Error = Error;
    #[tracing::instrument(name = "Query-Transaction:UpdateServerSettings", skip_all, err, fields(id = %input.id))]
    async fn process(&self, input: UpdateServerSettings) -> Result<Self::Output, Self::Error> {
        base::db::transact(self, UpdateServerSettingsFlow::Start(input)).await
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

impl Query for SetServerAgentKey {
    type Effect = Write<OrchestrationServer>;
}

impl Execute<PgSource> for SetServerAgentKey {
    type Output = ServerEntity;
    type Error = Error;
    #[tracing::instrument(name = "Query:SetServerAgentKey", skip_all, err, fields(id = %self.id))]
    async fn execute(self, conn: &mut PgConnection) -> Result<Self::Output, Self::Error> {
        Ok(sqlx::query_file_as!(
            ServerRow,
            "sql/set_server_agent_key.sql",
            self.id as _,
            self.digest,
            self.now,
            self.unit
        )
        .fetch_one(conn)
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

impl Query for SetAgentUpdateRequested {
    type Effect = Write<OrchestrationServer>;
}

impl Execute<PgSource> for SetAgentUpdateRequested {
    type Output = ServerEntity;
    type Error = Error;
    #[tracing::instrument(name = "Query:SetAgentUpdateRequested", skip_all, err, fields(id = %self.id))]
    async fn execute(self, conn: &mut PgConnection) -> Result<Self::Output, Self::Error> {
        Ok(sqlx::query_file_as!(
            ServerRow,
            "sql/set_agent_update_requested.sql",
            self.id as _,
            self.version
        )
        .fetch_one(conn)
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

impl Query for SettleAgentUpdate {
    type Effect = Write<OrchestrationServer>;
}

impl Execute<PgSource> for SettleAgentUpdate {
    type Output = ();
    type Error = Error;
    #[tracing::instrument(name = "Query:SettleAgentUpdate", skip_all, err, fields(id = %self.id))]
    async fn execute(self, conn: &mut PgConnection) -> Result<Self::Output, Self::Error> {
        sqlx::query_file!(
            "sql/settle_agent_update.sql",
            self.id as _,
            self.error,
            self.reported_version
        )
        .execute(conn)
        .await?;
        Ok(())
    }
}

/// The server whose agent key has this digest — the key names the server, the
/// caller only confirms it.
pub struct FindServerByAgentKeyDigest {
    pub digest: String,
}

impl Query for FindServerByAgentKeyDigest {
    type Effect = Read<OrchestrationServer>;
}

impl Execute<PgSource> for FindServerByAgentKeyDigest {
    type Output = Option<ServerEntity>;
    type Error = Error;
    #[tracing::instrument(name = "Query:FindServerByAgentKeyDigest", skip_all, err)]
    async fn execute(self, conn: &mut PgConnection) -> Result<Self::Output, Self::Error> {
        Ok(sqlx::query_file_as!(
            ServerRow,
            "sql/find_server_by_agent_key_digest.sql",
            self.digest
        )
        .fetch_optional(conn)
        .await?
        .map(ServerEntity::from))
    }
}

pub struct MoveServerPosition {
    pub id: ServerId,
    pub position: CanvasUiPosition,
}

impl Query for MoveServerPosition {
    type Effect = Write<OrchestrationServer>;
}

impl Execute<PgSource> for MoveServerPosition {
    type Output = ();
    type Error = Error;
    #[tracing::instrument(name = "Query:MoveServerPosition", skip_all, err, fields(id = %self.id))]
    async fn execute(self, conn: &mut PgConnection) -> Result<Self::Output, Self::Error> {
        sqlx::query!(
            "UPDATE orchestration_server SET position_x = $2, position_y = $3 WHERE id = $1",
            self.id as _,
            self.position.x,
            self.position.y
        )
        .execute(conn)
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

/// How many pods are placed on a server.
struct CountPodsOnServer<'a> {
    server: &'a ServerId,
}

impl Query for CountPodsOnServer<'_> {
    type Effect = Read<OrchestrationPod>;
}

impl Execute<PgSource> for CountPodsOnServer<'_> {
    type Output = i64;
    type Error = Error;
    async fn execute(self, conn: &mut PgConnection) -> Result<Self::Output, Self::Error> {
        Ok(sqlx::query_scalar!(
            r#"SELECT count(*) AS "count!" FROM orchestration_pod WHERE server = $1"#,
            self.server as _
        )
        .fetch_one(conn)
        .await?)
    }
}

/// Deletes a server row; its view, health history and group memberships cascade.
struct DeleteServer<'a> {
    server: &'a ServerId,
}

impl Query for DeleteServer<'_> {
    type Effect = Write<OrchestrationServer>;
}

impl Execute<PgSource> for DeleteServer<'_> {
    type Output = ();
    type Error = Error;
    async fn execute(self, conn: &mut PgConnection) -> Result<Self::Output, Self::Error> {
        sqlx::query!(
            "DELETE FROM orchestration_server WHERE id = $1",
            self.server as _
        )
        .execute(conn)
        .await?;
        Ok(())
    }
}

enum DeleteServerRowFlow {
    Start(DeleteServerRow),
    End(TxEnd<DeleteServerRowFlow>),
}

impl Query for DeleteServerRowFlow {
    type Effect = (
        Write<OrchestrationCanvas>,
        Read<OrchestrationPod>,
        Write<OrchestrationServer>,
    );
}

impl From<TxEnd<DeleteServerRowFlow>> for DeleteServerRowFlow {
    fn from(end: TxEnd<DeleteServerRowFlow>) -> Self {
        DeleteServerRowFlow::End(end)
    }
}

impl TxFlow for DeleteServerRowFlow {
    type Database = Postgres;
    type Commit = ();
    type Abort = Result<(), Error>;
    type Report = ();

    async fn step(tx: Tx<'_, Self>, state: Self) -> TxState<'_, Self> {
        let DeleteServerRowFlow::Start(input) = state else {
            return tx.end(state).await;
        };
        let (tx, r) = tx
            .run(TouchChecked {
                canvas: &input.canvas,
                fence: input.fence.as_ref(),
            })
            .await;
        if let Err(e) = r {
            return base::db::rollback(tx, e).await;
        }
        let (tx, r) = tx.run(CountPodsOnServer { server: &input.id }).await;
        let pods = match r {
            Ok(v) => v,
            Err(e) => return base::db::rollback(tx, e).await,
        };
        if pods > 0 {
            return base::db::rollback(tx, Error::Conflict(SERVER_HAS_PODS)).await;
        }
        let (tx, r) = tx.run(DeleteServer { server: &input.id }).await;
        match r {
            Ok(()) => base::db::commit(tx, ()).await,
            Err(e) => base::db::rollback(tx, e).await,
        }
    }

    fn finished(self) -> Either<Self, TxEnd<Self>> {
        match self {
            DeleteServerRowFlow::End(end) => Either::Right(end),
            state => Either::Left(state),
        }
    }
}

impl Processor<DeleteServerRow> for Db {
    type Output = ();
    type Error = Error;
    #[tracing::instrument(name = "Query-Transaction:DeleteServerRow", skip_all, err)]
    async fn process(&self, input: DeleteServerRow) -> Result<Self::Output, Self::Error> {
        base::db::transact(self, DeleteServerRowFlow::Start(input)).await
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

/// Rotates the server's session lease to the registering worker, unless a live
/// session still holds it (`None`).
struct RotateWorkerSession<'a> {
    input: &'a RegisterWorkerSession,
}

impl Query for RotateWorkerSession<'_> {
    type Effect = Write<OrchestrationServer>;
}

impl Execute<PgSource> for RotateWorkerSession<'_> {
    type Output = Option<ServerEntity>;
    type Error = Error;
    async fn execute(self, conn: &mut PgConnection) -> Result<Self::Output, Self::Error> {
        let input = self.input;
        // `observed_address` is what the master saw this registration come from;
        // `reported_addresses` and the worker's build are replaced only when the
        // worker sent them (`COALESCE` keeps the previous value otherwise, so an
        // older worker cannot blank a known version).
        Ok(sqlx::query_file_as!(
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
        .fetch_optional(conn)
        .await?
        .map(ServerEntity::from))
    }
}

/// Locks a server's view row and reads its three snapshot slots.
struct LockViewSlots<'a> {
    server: &'a ServerId,
}

impl Query for LockViewSlots<'_> {
    // `FOR UPDATE` is refused in a READ ONLY transaction.
    type Effect = Write<OrchestrationServerConfigView>;
}

impl Execute<PgSource> for LockViewSlots<'_> {
    type Output = ViewSlots;
    type Error = Error;
    async fn execute(self, conn: &mut PgConnection) -> Result<Self::Output, Self::Error> {
        Ok(sqlx::query_as!(
            ViewSlots,
            r#"SELECT desired AS "desired: Json<ConfigSnapshot>",
                      in_flight AS "in_flight: Json<ConfigSnapshot>",
                      applied AS "applied: Json<ConfigSnapshot>"
               FROM orchestration_server_config_view
               WHERE server = $1 FOR UPDATE"#,
            self.server as _
        )
        .fetch_one(conn)
        .await?)
    }
}

/// Promotes the view's `desired` snapshot to `applied`.
struct PromoteDesired<'a> {
    server: &'a ServerId,
}

impl Query for PromoteDesired<'_> {
    type Effect = Write<OrchestrationServerConfigView>;
}

impl Execute<PgSource> for PromoteDesired<'_> {
    type Output = ();
    type Error = Error;
    async fn execute(self, conn: &mut PgConnection) -> Result<Self::Output, Self::Error> {
        sqlx::query!(
            "UPDATE orchestration_server_config_view
             SET applied = desired, apply_error = NULL, failed_revision = NULL
             WHERE server = $1",
            self.server as _
        )
        .execute(conn)
        .await?;
        Ok(())
    }
}

/// Promotes the view's `in_flight` snapshot to `applied`.
struct PromoteInFlight<'a> {
    server: &'a ServerId,
}

impl Query for PromoteInFlight<'_> {
    type Effect = Write<OrchestrationServerConfigView>;
}

impl Execute<PgSource> for PromoteInFlight<'_> {
    type Output = ();
    type Error = Error;
    async fn execute(self, conn: &mut PgConnection) -> Result<Self::Output, Self::Error> {
        sqlx::query!(
            "UPDATE orchestration_server_config_view
             SET applied = in_flight, apply_error = NULL, failed_revision = NULL
             WHERE server = $1",
            self.server as _
        )
        .execute(conn)
        .await?;
        Ok(())
    }
}

/// Records that the worker runs a revision the view does not know.
struct MarkUnknownRevision<'a> {
    server: &'a ServerId,
    running: i64,
}

impl Query for MarkUnknownRevision<'_> {
    type Effect = Write<OrchestrationServerConfigView>;
}

impl Execute<PgSource> for MarkUnknownRevision<'_> {
    type Output = ();
    type Error = Error;
    async fn execute(self, conn: &mut PgConnection) -> Result<Self::Output, Self::Error> {
        sqlx::query!(
            "UPDATE orchestration_server_config_view
             SET apply_error = 'running revision ' || $2::bigint::text || ' unknown'
             WHERE server = $1",
            self.server as _,
            self.running
        )
        .execute(conn)
        .await?;
        Ok(())
    }
}

/// Forgets what an earlier session applied.
struct ForgetApplied<'a> {
    server: &'a ServerId,
}

impl Query for ForgetApplied<'_> {
    type Effect = Write<OrchestrationServerConfigView>;
}

impl Execute<PgSource> for ForgetApplied<'_> {
    type Output = ();
    type Error = Error;
    async fn execute(self, conn: &mut PgConnection) -> Result<Self::Output, Self::Error> {
        sqlx::query!(
            "UPDATE orchestration_server_config_view
             SET applied = NULL, apply_error = NULL, failed_revision = NULL,
                 failed_pods = '[]'
             WHERE server = $1",
            self.server as _
        )
        .execute(conn)
        .await?;
        Ok(())
    }
}

/// Clears the view's `in_flight` and moves its `seq`.
struct ClearInFlightAndBump<'a> {
    server: &'a ServerId,
}

impl Query for ClearInFlightAndBump<'_> {
    type Effect = Write<OrchestrationServerConfigView>;
}

impl Execute<PgSource> for ClearInFlightAndBump<'_> {
    type Output = ();
    type Error = Error;
    async fn execute(self, conn: &mut PgConnection) -> Result<Self::Output, Self::Error> {
        sqlx::query!(
            "UPDATE orchestration_server_config_view SET in_flight = NULL, seq = seq + 1
             WHERE server = $1",
            self.server as _
        )
        .execute(conn)
        .await?;
        Ok(())
    }
}

// See `CreateServerFlow`.
#[allow(clippy::large_enum_variant)]
enum RegisterWorkerSessionFlow {
    Start(RegisterWorkerSession),
    End(TxEnd<RegisterWorkerSessionFlow>),
}

impl Query for RegisterWorkerSessionFlow {
    type Effect = (
        Write<OrchestrationServer>,
        Write<OrchestrationServerConfigView>,
    );
}

impl From<TxEnd<RegisterWorkerSessionFlow>> for RegisterWorkerSessionFlow {
    fn from(end: TxEnd<RegisterWorkerSessionFlow>) -> Self {
        RegisterWorkerSessionFlow::End(end)
    }
}

impl TxFlow for RegisterWorkerSessionFlow {
    type Database = Postgres;
    /// The rotated row, or `None` when a live session still holds the server.
    type Commit = Option<ServerEntity>;
    type Abort = Result<Option<ServerEntity>, Error>;
    type Report = ();

    async fn step(tx: Tx<'_, Self>, state: Self) -> TxState<'_, Self> {
        let RegisterWorkerSessionFlow::Start(input) = state else {
            return tx.end(state).await;
        };
        let (tx, r) = tx.run(RotateWorkerSession { input: &input }).await;
        let server = match r {
            Ok(Some(server)) => server,
            Ok(None) => return base::db::refuse(tx, None).await,
            Err(e) => return base::db::rollback(tx, e).await,
        };
        let (tx, r) = tx.run(LockViewSlots { server: &input.server }).await;
        let slots = match r {
            Ok(v) => v,
            Err(e) => return base::db::rollback(tx, e).await,
        };
        let revision_of = |slot: &Option<Json<ConfigSnapshot>>| slot.as_ref().map(|s| s.revision);
        let running = input.running_revision;
        let server_id = &input.server;
        let (tx, r) = if running > 0 {
            if revision_of(&slots.desired) == Some(running) {
                tx.run(PromoteDesired { server: server_id }).await
            } else if revision_of(&slots.in_flight) == Some(running) {
                tx.run(PromoteInFlight { server: server_id }).await
            } else if revision_of(&slots.applied) != Some(running) {
                tx.run(MarkUnknownRevision {
                    server: server_id,
                    running,
                })
                .await
            } else {
                (tx, Ok(()))
            }
        } else {
            // A worker running nothing (a fresh install, a wiped state directory,
            // a replay that failed) is not running what an earlier session
            // applied: forget that, so the stream hands the desired revision out
            // again instead of treating the server as converged, and dependants
            // stop counting on listeners nobody serves.
            tx.run(ForgetApplied { server: server_id }).await
        };
        if let Err(e) = r {
            return base::db::rollback(tx, e).await;
        }
        let (tx, r) = tx.run(ClearInFlightAndBump { server: server_id }).await;
        match r {
            Ok(()) => base::db::commit(tx, Some(server)).await,
            Err(e) => base::db::rollback(tx, e).await,
        }
    }

    fn finished(self) -> Either<Self, TxEnd<Self>> {
        match self {
            RegisterWorkerSessionFlow::End(end) => Either::Right(end),
            state => Either::Left(state),
        }
    }
}

impl Processor<RegisterWorkerSession> for Db {
    /// The rotated row, or `None` when a live session still holds the server.
    type Output = Option<ServerEntity>;
    type Error = Error;
    #[tracing::instrument(name = "Query-Transaction:RegisterWorkerSession", skip_all, err, fields(server = %input.server))]
    async fn process(&self, input: RegisterWorkerSession) -> Result<Self::Output, Self::Error> {
        base::db::transact(self, RegisterWorkerSessionFlow::Start(input)).await
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

/// A server's refresh-key generation and reported address set.
struct ServerAddressState {
    refresh_key_generation: i64,
    reported_addresses: Option<Json<ReportedAddresses>>,
}

/// Locks a server row and reads its address state; `None` when it is gone.
struct LockServerAddresses<'a> {
    server: &'a ServerId,
}

impl Query for LockServerAddresses<'_> {
    // `FOR UPDATE` is refused in a READ ONLY transaction.
    type Effect = Write<OrchestrationServer>;
}

impl Execute<PgSource> for LockServerAddresses<'_> {
    type Output = Option<ServerAddressState>;
    type Error = Error;
    async fn execute(self, conn: &mut PgConnection) -> Result<Self::Output, Self::Error> {
        Ok(sqlx::query_as!(
            ServerAddressState,
            r#"SELECT refresh_key_generation,
                      reported_addresses AS "reported_addresses: Json<ReportedAddresses>"
               FROM orchestration_server
               WHERE id = $1 FOR UPDATE"#,
            self.server as _
        )
        .fetch_optional(conn)
        .await?)
    }
}

/// Stores a server's reported address set.
struct StoreReportedAddresses<'a> {
    server: &'a ServerId,
    reported: &'a ReportedAddresses,
}

impl Query for StoreReportedAddresses<'_> {
    type Effect = Write<OrchestrationServer>;
}

impl Execute<PgSource> for StoreReportedAddresses<'_> {
    type Output = ();
    type Error = Error;
    async fn execute(self, conn: &mut PgConnection) -> Result<Self::Output, Self::Error> {
        sqlx::query!(
            "UPDATE orchestration_server SET reported_addresses = $2 WHERE id = $1",
            self.server as _,
            Json(self.reported) as _
        )
        .execute(conn)
        .await?;
        Ok(())
    }
}

/// Moves a server's view `seq`, so derivation looks again.
struct BumpViewSeq<'a> {
    server: &'a ServerId,
}

impl Query for BumpViewSeq<'_> {
    type Effect = Write<OrchestrationServerConfigView>;
}

impl Execute<PgSource> for BumpViewSeq<'_> {
    type Output = ();
    type Error = Error;
    async fn execute(self, conn: &mut PgConnection) -> Result<Self::Output, Self::Error> {
        sqlx::query!(
            "UPDATE orchestration_server_config_view SET seq = seq + 1 WHERE server = $1",
            self.server as _
        )
        .execute(conn)
        .await?;
        Ok(())
    }
}

enum UpdateReportedAddressesFlow {
    Start(UpdateReportedAddresses),
    End(TxEnd<UpdateReportedAddressesFlow>),
}

impl Query for UpdateReportedAddressesFlow {
    type Effect = (
        Write<OrchestrationServer>,
        Write<OrchestrationServerConfigView>,
    );
}

impl From<TxEnd<UpdateReportedAddressesFlow>> for UpdateReportedAddressesFlow {
    fn from(end: TxEnd<UpdateReportedAddressesFlow>) -> Self {
        UpdateReportedAddressesFlow::End(end)
    }
}

impl TxFlow for UpdateReportedAddressesFlow {
    type Database = Postgres;
    /// `true` when the stored set changed (and the view's `seq` moved).
    type Commit = bool;
    type Abort = Result<bool, Error>;
    type Report = ();

    async fn step(tx: Tx<'_, Self>, state: Self) -> TxState<'_, Self> {
        let UpdateReportedAddressesFlow::Start(input) = state else {
            return tx.end(state).await;
        };
        let (tx, r) = tx.run(LockServerAddresses { server: &input.server }).await;
        let current = match r {
            Ok(Some(current)) => current,
            Ok(None) => return base::db::refuse(tx, false).await,
            Err(e) => return base::db::rollback(tx, e).await,
        };
        if current.refresh_key_generation != input.generation {
            return base::db::refuse(tx, false).await;
        }
        if current
            .reported_addresses
            .is_some_and(|stored| stored.0.same_addresses(&input.reported))
        {
            return base::db::refuse(tx, false).await;
        }
        let (tx, r) = tx
            .run(StoreReportedAddresses {
                server: &input.server,
                reported: &input.reported,
            })
            .await;
        if let Err(e) = r {
            return base::db::rollback(tx, e).await;
        }
        let (tx, r) = tx.run(BumpViewSeq { server: &input.server }).await;
        match r {
            Ok(()) => base::db::commit(tx, true).await,
            Err(e) => base::db::rollback(tx, e).await,
        }
    }

    fn finished(self) -> Either<Self, TxEnd<Self>> {
        match self {
            UpdateReportedAddressesFlow::End(end) => Either::Right(end),
            state => Either::Left(state),
        }
    }
}

impl Processor<UpdateReportedAddresses> for Db {
    /// `true` when the stored set changed (and the view's `seq` moved).
    type Output = bool;
    type Error = Error;
    #[tracing::instrument(name = "Query-Transaction:UpdateReportedAddresses", skip_all, err, fields(server = %input.server))]
    async fn process(&self, input: UpdateReportedAddresses) -> Result<Self::Output, Self::Error> {
        base::db::transact(self, UpdateReportedAddressesFlow::Start(input)).await
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

/// Claims the watch session on the server row; `None` when the generation moved.
struct ClaimWatchRow<'a> {
    input: &'a ClaimServerWatchSession,
}

impl Query for ClaimWatchRow<'_> {
    type Effect = Write<OrchestrationServer>;
}

impl Execute<PgSource> for ClaimWatchRow<'_> {
    type Output = Option<ServerEntity>;
    type Error = Error;
    async fn execute(self, conn: &mut PgConnection) -> Result<Self::Output, Self::Error> {
        let input = self.input;
        Ok(sqlx::query_file_as!(
            ServerRow,
            "sql/claim_server_watch_session.sql",
            &input.server as _,
            input.generation,
            input.lease_until,
            input.now
        )
        .fetch_optional(conn)
        .await?
        .map(ServerEntity::from))
    }
}

/// Clears the view's `in_flight` snapshot.
struct ClearInFlight<'a> {
    server: &'a ServerId,
}

impl Query for ClearInFlight<'_> {
    type Effect = Write<OrchestrationServerConfigView>;
}

impl Execute<PgSource> for ClearInFlight<'_> {
    type Output = ();
    type Error = Error;
    async fn execute(self, conn: &mut PgConnection) -> Result<Self::Output, Self::Error> {
        sqlx::query!(
            "UPDATE orchestration_server_config_view SET in_flight = NULL WHERE server = $1",
            self.server as _
        )
        .execute(conn)
        .await?;
        Ok(())
    }
}

// See `CreateServerFlow`.
#[allow(clippy::large_enum_variant)]
enum ClaimServerWatchSessionFlow {
    Start(ClaimServerWatchSession),
    End(TxEnd<ClaimServerWatchSessionFlow>),
}

impl Query for ClaimServerWatchSessionFlow {
    type Effect = (
        Write<OrchestrationServer>,
        Write<OrchestrationServerConfigView>,
    );
}

impl From<TxEnd<ClaimServerWatchSessionFlow>> for ClaimServerWatchSessionFlow {
    fn from(end: TxEnd<ClaimServerWatchSessionFlow>) -> Self {
        ClaimServerWatchSessionFlow::End(end)
    }
}

impl TxFlow for ClaimServerWatchSessionFlow {
    type Database = Postgres;
    /// The claimed row, or `None` when the generation is no longer current.
    type Commit = Option<ServerEntity>;
    type Abort = Result<Option<ServerEntity>, Error>;
    type Report = ();

    async fn step(tx: Tx<'_, Self>, state: Self) -> TxState<'_, Self> {
        let ClaimServerWatchSessionFlow::Start(input) = state else {
            return tx.end(state).await;
        };
        let (tx, r) = tx.run(ClaimWatchRow { input: &input }).await;
        let claimed = match r {
            Ok(v) => v,
            Err(e) => return base::db::rollback(tx, e).await,
        };
        if claimed.is_none() {
            return base::db::commit(tx, None).await;
        }
        let (tx, r) = tx.run(ClearInFlight { server: &input.server }).await;
        match r {
            Ok(()) => base::db::commit(tx, claimed).await,
            Err(e) => base::db::rollback(tx, e).await,
        }
    }

    fn finished(self) -> Either<Self, TxEnd<Self>> {
        match self {
            ClaimServerWatchSessionFlow::End(end) => Either::Right(end),
            state => Either::Left(state),
        }
    }
}

impl Processor<ClaimServerWatchSession> for Db {
    /// The claimed row, or `None` when the generation is no longer current.
    type Output = Option<ServerEntity>;
    type Error = Error;
    #[tracing::instrument(name = "Query-Transaction:ClaimServerWatchSession", skip_all, err)]
    async fn process(&self, input: ClaimServerWatchSession) -> Result<Self::Output, Self::Error> {
        base::db::transact(self, ClaimServerWatchSessionFlow::Start(input)).await
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

impl Query for RenewServerWatchSession {
    type Effect = Write<OrchestrationServer>;
}

impl Execute<PgSource> for RenewServerWatchSession {
    /// `false` once the session has been fenced out; the stream must then end.
    type Output = bool;
    type Error = Error;
    #[tracing::instrument(name = "Query:RenewServerWatchSession", skip_all, err)]
    async fn execute(self, conn: &mut PgConnection) -> Result<Self::Output, Self::Error> {
        let renewed: Option<ServerId> = sqlx::query_scalar!(
            r#"UPDATE orchestration_server SET session_lease_until = $4, last_seen_at = $5
               WHERE id = $1 AND refresh_key_generation = $2 AND watch_epoch = $3
               RETURNING id AS "id: ServerId""#,
            self.server as _,
            self.generation,
            self.epoch,
            self.lease_until,
            self.now
        )
        .fetch_optional(conn)
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

impl Query for ReleaseServerWatchSession {
    type Effect = Write<OrchestrationServer>;
}

impl Execute<PgSource> for ReleaseServerWatchSession {
    type Output = ();
    type Error = Error;
    #[tracing::instrument(name = "Query:ReleaseServerWatchSession", skip_all, err)]
    async fn execute(self, conn: &mut PgConnection) -> Result<Self::Output, Self::Error> {
        sqlx::query!(
            "UPDATE orchestration_server SET session_lease_until = NULL
             WHERE id = $1 AND refresh_key_generation = $2 AND watch_epoch = $3",
            self.server as _,
            self.generation,
            self.epoch
        )
        .execute(conn)
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

impl Query for RevokeSilentWatchSessions {
    type Effect = Write<OrchestrationServer>;
}

impl Execute<PgSource> for RevokeSilentWatchSessions {
    /// The servers whose session was revoked.
    type Output = Vec<ServerId>;
    type Error = Error;
    #[tracing::instrument(name = "Query:RevokeSilentWatchSessions", skip_all, err)]
    async fn execute(self, conn: &mut PgConnection) -> Result<Self::Output, Self::Error> {
        Ok(sqlx::query_file_scalar!(
            "sql/revoke_silent_watch_sessions.sql",
            self.now,
            self.registered_before
        )
        .fetch_all(conn)
        .await?)
    }
}

pub struct FindServerByRefreshKeyDigest {
    pub digest: String,
}

impl Query for FindServerByRefreshKeyDigest {
    type Effect = Read<OrchestrationServer>;
}

impl Execute<PgSource> for FindServerByRefreshKeyDigest {
    type Output = Option<ServerEntity>;
    type Error = Error;
    #[tracing::instrument(name = "Query:FindServerByRefreshKeyDigest", skip_all, err)]
    async fn execute(self, conn: &mut PgConnection) -> Result<Self::Output, Self::Error> {
        Ok(sqlx::query_file_as!(
            ServerRow,
            "sql/find_server_by_refresh_key_digest.sql",
            self.digest
        )
        .fetch_optional(conn)
        .await?
        .map(ServerEntity::from))
    }
}

pub struct FindCanvasOfServer {
    pub server: ServerId,
}

impl Query for FindCanvasOfServer {
    type Effect = Read<OrchestrationServer>;
}

impl Execute<PgSource> for FindCanvasOfServer {
    type Output = Option<CanvasId>;
    type Error = Error;
    #[tracing::instrument(name = "Query:FindCanvasOfServer", skip_all, err)]
    async fn execute(self, conn: &mut PgConnection) -> Result<Self::Output, Self::Error> {
        Ok(sqlx::query_scalar!(
            r#"SELECT canvas AS "canvas: CanvasId" FROM orchestration_server WHERE id = $1"#,
            self.server as _
        )
        .fetch_optional(conn)
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
