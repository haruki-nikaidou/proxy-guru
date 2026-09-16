//! The operator-facing `Orchestration` gRPC service.
//!
//! Handlers are thin: decode ids and specs, call a service, encode the reply. All
//! rules live in `services`.

use crate::entities::db::canvas::{CanvasContents, CanvasEntity, CanvasTree, CanvasUiPosition};
use crate::entities::db::certificate::{CertificateEntity, CertificateStatus};
use crate::entities::db::connection::EdgeConnectionEntity;
use crate::entities::db::dns::DnsProvider;
use crate::entities::db::health::{
    ListNodeHealthAfter, ListServerHealthHistory as ListServerHealthHistoryRows,
    NodeHealthRecordEntity, NodeHealthStatus, ServerHealthRecordEntity, ServerHealthStatus,
};
use crate::entities::db::node::{
    CanvasExportAs, CanvasExportConfig, CanvasImportConfig, EntryConfig, ExitConfig, Lane,
    LaneRole, LoadBalanceAggregateConfig, LoadBalanceDistributeConfig, LoadBalanceMember,
    LoadBalanceMode, NodeEntity, NodeSpec, NodeWithPorts, PodConfig, ProxyProtocolVersion,
    RelayConfig, RelayProtocol, TlsConfig, UniversalPodConfig,
};
use crate::entities::db::port::{PortDirection, PortEntity, PortKind};
use crate::entities::db::server::{AddressSource, ServerEntity, ServerIpv6Resolve};
use crate::entities::db::view::{ConfigSnapshot, ListenerCap};
use crate::events::live::{
    CanvasChangeKind, LiveMessage, NodeHealthLive, ServerHealthLive, live_time,
};
use crate::hooks::live::LiveEvent;
use crate::services::acme::{self, AcmeService};
use crate::services::canvas::{self, CanvasService};
use crate::services::config::{self, OrchestrationConfigService};
use crate::services::dns::{self, DnsProviderService, DnsProviderSummary};
use crate::services::edge::{self, EdgeService};
use crate::services::health::{self, HealthService};
use crate::services::live::{self, LiveService, ViewValue};
use crate::services::node::{self, NodeService};
use crate::services::rollout::{self, RolloutService, RolloutStatus};
use crate::services::server::{self, ServerService};
use crate::services::topology::{ProblemKind, ProblemSeverity, TopologyProblem};
use crate::utils::ids;
use auth::services::session::SessionService;
use chrono::{DateTime, Utc};
use kanau::processor::Processor;
use rpguru_sdk::orchestration as pb;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status};

/// How many events a stream may have queued before the sender waits.
///
/// Small on purpose. A canvas or rollout snapshot is a whole picture, so a
/// client that cannot keep up wants the newest one, not a backlog: the shared
/// view coalesces while this channel is full, and the stream then sends one
/// up-to-date snapshot instead of four stale ones.
const STREAM_CAPACITY: usize = 4;

/// The opening window of a node-health stream when the client sets no `limit`.
/// Smaller than the history RPC's default: a live view shows recent events, and
/// anything older is what `ListNodeHealthHistory` is for.
const DEFAULT_WATCH_NODE_HISTORY: i64 = 50;

/// How many node records one recovery read fetches. The loop pages until a
/// short page, so this bounds memory per round, not how much a stream can
/// catch up on.
const NODE_RECOVERY_PAGE: i64 = 200;

/// How many times a stream's recovery read is retried before it gives up and
/// ends the stream. The `Resync`/`Lagged` signal that triggered the read is
/// already consumed, so a swallowed failure would leave the gap open until the
/// next bus reconnect — which may be hours.
const RECOVERY_ATTEMPTS: usize = 3;
const RECOVERY_BACKOFF: std::time::Duration = std::time::Duration::from_millis(250);

/// Runs a database read up to [`RECOVERY_ATTEMPTS`] times, returning the last
/// error. The closure is re-invoked per attempt because the query owns its
/// bindings.
async fn retry_read<T, E, F, Fut>(mut read: F) -> Result<T, E>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T, E>>,
    E: std::fmt::Display,
{
    let mut attempt = 1;
    loop {
        match read().await {
            Ok(value) => return Ok(value),
            Err(error) if attempt < RECOVERY_ATTEMPTS => {
                tracing::warn!(%error, attempt, "a live stream's recovery read failed; retrying");
                attempt = attempt.saturating_add(1);
                tokio::time::sleep(RECOVERY_BACKOFF).await;
            }
            Err(error) => return Err(error),
        }
    }
}

/// One page of a node-health recovery read, retried.
async fn refetch_node_health(
    db: &base::db::Db,
    node: &crate::entities::db::node::NodeId,
    after: DateTime<Utc>,
    after_id: Option<&str>,
) -> Result<Vec<NodeHealthRecordEntity>, base::db::Error> {
    retry_read(|| {
        db.process(ListNodeHealthAfter {
            node: node.clone(),
            after,
            after_id: after_id.map(ids::node_health_record_id),
            limit: NODE_RECOVERY_PAGE,
        })
    })
    .await
}

#[derive(Clone)]
pub struct OrchestrationGrpc {
    pub canvases: CanvasService,
    pub servers: ServerService,
    pub nodes: NodeService,
    pub edges: EdgeService,
    pub rollout: RolloutService,
    pub health: HealthService,
    pub dns: DnsProviderService,
    pub certificates: AcmeService,
    pub configs: OrchestrationConfigService,
    pub live: LiveService,
    /// Streams re-validate the session that opened them on every keep-alive
    /// tick: a long-lived stream must not outlive the login behind it.
    pub sessions: SessionService,
}

/// A live stream's clock: it paces the keep-alives and re-checks the session.
struct StreamTicker {
    keepalive: tokio::time::Interval,
    sessions: SessionService,
    session_id: String,
}

impl StreamTicker {
    fn new(sessions: SessionService, session_id: String, period: std::time::Duration) -> Self {
        // `interval` fires immediately; starting one period out does not. The
        // difference is load-bearing: an immediate tick would race the opening
        // snapshot and a client could see a keep-alive as its first message.
        // An unrepresentable instant means "now", which only costs one extra
        // keep-alive on a clock nobody has.
        let start = tokio::time::Instant::now()
            .checked_add(period)
            .unwrap_or_else(tokio::time::Instant::now);
        let mut keepalive = tokio::time::interval_at(start, period);
        keepalive.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        Self {
            keepalive,
            sessions,
            session_id,
        }
    }

    /// Waits for the next tick, then re-authenticates. `Err` ends the stream.
    ///
    /// A database failure is *not* an ending: cutting every open dashboard over
    /// one failed read would turn a blip into a fleet-wide reconnect storm. A
    /// session that resolves to nothing is, because that is a logout or an
    /// expiry and the stream has no right to the data any more.
    async fn tick(&mut self) -> Result<(), Status> {
        self.keepalive.tick().await;
        match self
            .sessions
            .process(auth::services::session::AuthenticateSession {
                session_id: self.session_id.clone(),
            })
            .await
        {
            Ok(Some(_)) => Ok(()),
            Ok(None) => Err(Status::unauthenticated("session ended")),
            Err(error) => {
                tracing::warn!(%error, "re-validating a live stream's session failed");
                Ok(())
            }
        }
    }
}

/// The session id a live stream is tied to.
///
/// Read from the metadata rather than the injected identity because the
/// identity does not carry it. `ViewWorkspace` already refuses a machine
/// caller, so an API key cannot open a stream and then find no session here.
fn session_id<T>(request: &Request<T>) -> Result<String, Status> {
    request
        .metadata()
        .get(auth::rpc::middleware::SESSION_ID_METADATA)
        .and_then(|value| value.to_str().ok())
        .map(str::to_owned)
        .ok_or_else(|| Status::unauthenticated("live streams require a session"))
}

impl OrchestrationGrpc {
    /// The canvas an import node embeds, as the lookup `node_to_proto` takes;
    /// empty for every other node kind.
    async fn import_target_of(
        &self,
        actor: &auth::services::identity::Identity,
        node: &NodeEntity,
    ) -> Result<Vec<CanvasEntity>, Status> {
        let NodeSpec::CanvasImport(cfg) = &node.spec else {
            return Ok(Vec::new());
        };
        Ok(self
            .canvases
            .process(canvas::FindCanvas {
                actor: actor.clone(),
                canvas: cfg.canvas.clone(),
            })
            .await?
            .into_iter()
            .collect())
    }
}

/// Both document fields are pretty-printed: an operator edits this text.
fn config_to_proto(
    document: config::ConfigDocument,
) -> Result<rpguru_sdk::base::ConfigDocument, Status> {
    let encode = |value: &serde_json::Value| {
        serde_json::to_string_pretty(value).map_err(|error| {
            tracing::error!(error = %error, "serializing orchestration config document");
            Status::internal("Internal server error")
        })
    };
    Ok(rpguru_sdk::base::ConfigDocument {
        stored: document.stored,
        json: encode(&document.json)?,
        defaults_json: encode(&document.defaults)?,
    })
}

/// The payload an operator typed. Not valid JSON is their typo, not a bug.
fn config_json(json: &str) -> Result<serde_json::Value, Status> {
    serde_json::from_str(json).map_err(|error| {
        Status::invalid_argument(format!("the payload is not valid JSON: {error}"))
    })
}

// --- encoding ---------------------------------------------------------------

fn position_to_proto(position: CanvasUiPosition) -> pb::CanvasUiPosition {
    pb::CanvasUiPosition {
        x: position.x,
        y: position.y,
    }
}

/// An unset `position` means "not given": callers that may leave the node where it
/// is keep the `None`, callers that must have a position default it themselves.
fn position_from_proto(position: Option<pb::CanvasUiPosition>) -> Option<CanvasUiPosition> {
    position.map(|position| CanvasUiPosition {
        x: position.x,
        y: position.y,
    })
}

/// The position as sent, or the canvas origin when the field is unset.
fn position_or_origin(position: Option<pb::CanvasUiPosition>) -> CanvasUiPosition {
    position_from_proto(position).unwrap_or(CanvasUiPosition { x: 0, y: 0 })
}

fn canvas_to_proto(canvas: &CanvasEntity) -> pb::Canvas {
    pb::Canvas {
        id: canvas.id.to_string(),
        name: canvas.name.clone(),
        description: canvas.description.clone(),
    }
}

fn ipv6_to_proto(value: ServerIpv6Resolve) -> i32 {
    match value {
        ServerIpv6Resolve::Required => pb::Ipv6Resolve::Ipv6Required,
        ServerIpv6Resolve::Preferred => pb::Ipv6Resolve::Ipv6Preferred,
        ServerIpv6Resolve::Tolerated => pb::Ipv6Resolve::Ipv6Tolerated,
        ServerIpv6Resolve::Forbidden => pb::Ipv6Resolve::Ipv6Forbidden,
    }
    .into()
}

pub(crate) fn server_health_to_proto(value: ServerHealthStatus) -> i32 {
    match value {
        ServerHealthStatus::Online => pb::ServerHealthStatus::ServerOnline,
        ServerHealthStatus::Degraded => pb::ServerHealthStatus::ServerDegraded,
        ServerHealthStatus::Offline => pb::ServerHealthStatus::ServerOffline,
    }
    .into()
}

fn node_health_to_proto(value: NodeHealthStatus) -> i32 {
    match value {
        NodeHealthStatus::Ready => pb::NodeHealthStatus::NodeReady,
        NodeHealthStatus::Deploying => pb::NodeHealthStatus::NodeDeploying,
        NodeHealthStatus::Failed => pb::NodeHealthStatus::NodeFailed,
    }
    .into()
}

fn server_health_record_to_proto(record: &ServerHealthRecordEntity) -> pb::ServerHealthRecord {
    pb::ServerHealthRecord {
        id: record.id.to_string(),
        server_id: record.server.to_string(),
        status: server_health_to_proto(record.status),
        report_time: record.report_time.to_rfc3339(),
        upload_bytes: record.upload_bytes,
        download_bytes: record.download_bytes,
        current_connections: record.current_connections,
        max_connections: record.max_connections,
    }
}

fn node_health_record_to_proto(record: &NodeHealthRecordEntity) -> pb::NodeHealthRecord {
    pb::NodeHealthRecord {
        id: record.id.to_string(),
        node_id: record.node.to_string(),
        status: node_health_to_proto(record.status),
        message: record.message.clone(),
        report_time: record.report_time.to_rfc3339(),
    }
}

fn dns_provider_to_proto(value: DnsProvider) -> i32 {
    match value {
        DnsProvider::Cloudflare => pb::DnsProviderKind::DnsCloudflare,
        DnsProvider::Vercel => pb::DnsProviderKind::DnsVercel,
    }
    .into()
}

fn dns_provider_from_proto(value: i32) -> Result<DnsProvider, Status> {
    match pb::DnsProviderKind::try_from(value) {
        Ok(pb::DnsProviderKind::DnsCloudflare) => Ok(DnsProvider::Cloudflare),
        Ok(pb::DnsProviderKind::DnsVercel) => Ok(DnsProvider::Vercel),
        _ => Err(Status::invalid_argument("provider must be specified")),
    }
}

fn dns_provider_summary_to_proto(provider: &DnsProviderSummary) -> pb::DnsProvider {
    pb::DnsProvider {
        id: provider.id.to_string(),
        name: provider.name.clone(),
        provider: dns_provider_to_proto(provider.provider),
        account_id: provider.account_id.clone(),
        created_at: provider.created_at.to_rfc3339(),
    }
}

fn certificate_status_to_proto(value: CertificateStatus) -> i32 {
    match value {
        CertificateStatus::Pending => pb::CertificateStatus::CertificatePending,
        CertificateStatus::Issued => pb::CertificateStatus::CertificateIssued,
        CertificateStatus::Failed => pb::CertificateStatus::CertificateFailed,
    }
    .into()
}

/// Key material never crosses the wire: only the row's metadata does.
fn certificate_to_proto(certificate: &CertificateEntity) -> pb::Certificate {
    let time = |t: Option<DateTime<Utc>>| t.map(|t| t.to_rfc3339()).unwrap_or_default();
    pb::Certificate {
        id: certificate.id.to_string(),
        sni: certificate.sni.clone(),
        dns_provider_id: certificate.dns_provider.to_string(),
        domain_id: certificate.domain_id.clone(),
        acme_directory: certificate.acme_directory.clone(),
        status: certificate_status_to_proto(certificate.status),
        not_before: time(certificate.not_before),
        not_after: time(certificate.not_after),
        last_error: certificate.last_error.clone().unwrap_or_default(),
        last_attempt_at: time(certificate.last_attempt_at),
    }
}

fn contents_to_proto(contents: &CanvasContents) -> pb::GetCanvasReply {
    pb::GetCanvasReply {
        canvas: Some(canvas_to_proto(&contents.canvas)),
        servers: contents.servers.iter().map(server_to_proto).collect(),
        nodes: contents
            .nodes
            .iter()
            .map(|n| node_to_proto(n, &contents.import_targets))
            .collect(),
        edges: contents.edges.iter().map(edge_to_proto).collect(),
        ancestors: contents.ancestors.iter().map(canvas_to_proto).collect(),
    }
}

fn rollout_status_to_proto(status: RolloutStatus) -> pb::GetServerRolloutStatusReply {
    pb::GetServerRolloutStatusReply {
        desired: status.desired.as_ref().map(snapshot_to_proto),
        in_flight: status.in_flight.as_ref().map(snapshot_to_proto),
        applied: status.applied.as_ref().map(snapshot_to_proto),
        apply_error: status.apply_error.unwrap_or_default(),
        derive_error: status.derive_error.unwrap_or_default(),
        waiting_for_server_ids: status.waiting_for.iter().map(|id| id.to_string()).collect(),
        invalid_pods: status
            .invalid_pods
            .into_iter()
            .map(|pod| pb::InvalidPod {
                node_id: pod.node.to_string(),
                pod_name: pod.pod,
                listen: pod.listen,
                error: pod.error,
            })
            .collect(),
        derivation_pending: status.derivation_pending,
        last_seen_at: status
            .last_seen_at
            .map(|t| t.to_rfc3339())
            .unwrap_or_default(),
    }
}

/// What a live canvas snapshot says caused it.
fn cause_to_proto(cause: Option<&LiveMessage>) -> (i32, Vec<String>) {
    match cause {
        Some(LiveMessage::CanvasChanged { kind, ids, .. }) => {
            (canvas_change_kind_to_proto(*kind), ids.clone())
        }
        Some(LiveMessage::ServerHealth { server, .. }) => (
            pb::CanvasChangeKind::ServerHealthChanged.into(),
            vec![server.clone()],
        ),
        // The opening snapshot, a refresh after a bus reconnect, or a cause the
        // canvas view does not describe in these terms.
        _ => (pb::CanvasChangeKind::Unspecified.into(), Vec::new()),
    }
}

fn canvas_change_kind_to_proto(kind: CanvasChangeKind) -> i32 {
    match kind {
        CanvasChangeKind::CanvasUpdated => pb::CanvasChangeKind::CanvasUpdated,
        CanvasChangeKind::CanvasDeleted => pb::CanvasChangeKind::CanvasDeleted,
        CanvasChangeKind::ServerCreated => pb::CanvasChangeKind::ServerCreated,
        CanvasChangeKind::ServerUpdated => pb::CanvasChangeKind::ServerUpdated,
        CanvasChangeKind::ServerMoved => pb::CanvasChangeKind::ServerMoved,
        CanvasChangeKind::ServerDeleted => pb::CanvasChangeKind::ServerDeleted,
        CanvasChangeKind::ServerIpChanged => pb::CanvasChangeKind::ServerIpChanged,
        CanvasChangeKind::NodeCreated => pb::CanvasChangeKind::NodeCreated,
        CanvasChangeKind::NodeReplaced => pb::CanvasChangeKind::NodeReplaced,
        CanvasChangeKind::NodeMetaUpdated => pb::CanvasChangeKind::NodeMetaUpdated,
        CanvasChangeKind::NodeRetired => pb::CanvasChangeKind::NodeRetired,
        CanvasChangeKind::NodeDeleted => pb::CanvasChangeKind::NodeDeleted,
        CanvasChangeKind::EdgeConnected => pb::CanvasChangeKind::EdgeConnected,
        CanvasChangeKind::EdgeRetired => pb::CanvasChangeKind::EdgeRetired,
        CanvasChangeKind::EdgeDeleted => pb::CanvasChangeKind::EdgeDeleted,
    }
    .into()
}

/// A bus health record as the wire type. The live payload carries the same
/// fields as the row, so the stream never has to read the record back.
fn server_health_live_to_proto(server: &str, record: &ServerHealthLive) -> pb::ServerHealthRecord {
    pb::ServerHealthRecord {
        id: record.id.clone(),
        server_id: server.to_string(),
        status: server_health_to_proto(record.status),
        report_time: live_time(record.report_time_unix_micros).to_rfc3339(),
        upload_bytes: record.upload_bytes,
        download_bytes: record.download_bytes,
        current_connections: record.current_connections,
        max_connections: record.max_connections,
    }
}

fn node_health_live_to_proto(record: &NodeHealthLive) -> pb::NodeHealthRecord {
    pb::NodeHealthRecord {
        id: record.id.clone(),
        node_id: record.node.clone(),
        status: node_health_to_proto(record.status),
        message: record.message.clone(),
        report_time: live_time(record.report_time_unix_micros).to_rfc3339(),
    }
}

/// The history window as the proto defines it: an empty `end` means now, an
/// empty `start` means one hour before `end`.
fn history_window(start: &str, end: &str) -> Result<(DateTime<Utc>, DateTime<Utc>), Status> {
    fn parse(field: &str, value: &str) -> Result<DateTime<Utc>, Status> {
        DateTime::parse_from_rfc3339(value)
            .map(|t| t.with_timezone(&Utc))
            .map_err(|e| Status::invalid_argument(format!("{field}: {e}")))
    }
    let end = if end.is_empty() {
        Utc::now()
    } else {
        parse("end", end)?
    };
    let start = if start.is_empty() {
        end.checked_sub_signed(chrono::TimeDelta::hours(1))
            .unwrap_or(DateTime::<Utc>::MIN_UTC)
    } else {
        parse("start", start)?
    };
    Ok((start, end))
}

fn ipv6_from_proto(value: i32) -> Result<ServerIpv6Resolve, Status> {
    match pb::Ipv6Resolve::try_from(value) {
        Ok(pb::Ipv6Resolve::Ipv6Required) => Ok(ServerIpv6Resolve::Required),
        Ok(pb::Ipv6Resolve::Ipv6Preferred) => Ok(ServerIpv6Resolve::Preferred),
        Ok(pb::Ipv6Resolve::Ipv6Forbidden) => Ok(ServerIpv6Resolve::Forbidden),
        // Unspecified means "the default policy".
        Ok(pb::Ipv6Resolve::Ipv6Tolerated | pb::Ipv6Resolve::Unspecified) => {
            Ok(ServerIpv6Resolve::Tolerated)
        }
        Err(_) => Err(Status::invalid_argument(format!(
            "ipv6_resolve: unknown value {value}"
        ))),
    }
}

fn server_to_proto(server: &ServerEntity) -> pb::Server {
    pb::Server {
        id: server.id.to_string(),
        canvas_id: server.canvas.to_string(),
        name: server.name.clone(),
        icon: server.icon.clone(),
        comment: server.comment.clone(),
        position: Some(position_to_proto(server.position)),
        ipv6_resolve: ipv6_to_proto(server.ipv6_resolve),
        log_level: server.log_level.clone(),
        last_seen_at: server
            .last_seen_at
            .map(|t| t.to_rfc3339())
            .unwrap_or_default(),
        last_health_report_at: server
            .last_health_report_at
            .map(|t| t.to_rfc3339())
            .unwrap_or_default(),
        health_status: server_health_to_proto(server.health_status),
        addresses: Some(addresses_to_proto(server)),
        agent_version: server.agent_version.clone().unwrap_or_default(),
        agent_arch: server.agent_arch.clone().unwrap_or_default(),
        agent_unit: server.agent_unit.clone().unwrap_or_default(),
        agent_update_requested: server.agent_update_requested.clone().unwrap_or_default(),
        agent_update_error: server.agent_update_error.clone().unwrap_or_default(),
        agent_key_issued_at: server
            .agent_key_issued_at
            .map(|t| t.to_rfc3339())
            .unwrap_or_default(),
    }
}

fn addresses_to_proto(server: &ServerEntity) -> pb::ServerAddresses {
    let reported = server.reported_addresses.as_ref();
    let effective = server.effective_address();
    pb::ServerAddresses {
        v4: Some(pb::AddressSlot {
            reported: reported
                .and_then(|r| r.public_v4.clone())
                .unwrap_or_default(),
            pinned: server.override_v4.clone().unwrap_or_default(),
        }),
        v6: Some(pb::AddressSlot {
            reported: reported
                .and_then(|r| r.public_v6.clone())
                .unwrap_or_default(),
            pinned: server.override_v6.clone().unwrap_or_default(),
        }),
        extra: server.extra_addresses.clone(),
        reported_interfaces: reported.map(|r| r.interfaces.clone()).unwrap_or_default(),
        reported_at: reported
            .map(|r| r.reported_at.to_rfc3339())
            .unwrap_or_default(),
        observed_address: server.observed_address.clone().unwrap_or_default(),
        observed_at: server
            .observed_at
            .map(|t| t.to_rfc3339())
            .unwrap_or_default(),
        effective_address: effective
            .map(|(address, _)| address.to_string())
            .unwrap_or_default(),
        reported_country: reported.and_then(|r| r.country.clone()).unwrap_or_default(),
        effective_source: match effective.map(|(_, source)| source) {
            None => pb::AddressSource::Unspecified,
            Some(AddressSource::Override) => pb::AddressSource::AddressOverride,
            Some(AddressSource::Reported) => pb::AddressSource::AddressReported,
            Some(AddressSource::Observed) => pb::AddressSource::AddressObserved,
        }
        .into(),
    }
}

fn port_kind_to_proto(kind: PortKind) -> i32 {
    match kind {
        PortKind::DeriveListen => pb::PortKind::DeriveListen,
        PortKind::DeriveDestination => pb::PortKind::DeriveDestination,
        PortKind::Bundle => pb::PortKind::Bundle,
    }
    .into()
}

fn lb_mode_to_proto(mode: LoadBalanceMode) -> i32 {
    match mode {
        LoadBalanceMode::RoundRobin => pb::LoadBalanceMode::RoundRobin,
        LoadBalanceMode::Random => pb::LoadBalanceMode::Random,
        LoadBalanceMode::IpHash => pb::LoadBalanceMode::IpHash,
        LoadBalanceMode::Fallback => pb::LoadBalanceMode::Fallback,
    }
    .into()
}

fn lb_mode_from_proto(mode: i32) -> Result<LoadBalanceMode, Status> {
    match pb::LoadBalanceMode::try_from(mode) {
        Ok(pb::LoadBalanceMode::RoundRobin) => Ok(LoadBalanceMode::RoundRobin),
        Ok(pb::LoadBalanceMode::Random) => Ok(LoadBalanceMode::Random),
        Ok(pb::LoadBalanceMode::IpHash) => Ok(LoadBalanceMode::IpHash),
        Ok(pb::LoadBalanceMode::Fallback) => Ok(LoadBalanceMode::Fallback),
        Ok(pb::LoadBalanceMode::Unspecified) | Err(_) => Err(Status::invalid_argument(format!(
            "mode: unknown load balance mode {mode}"
        ))),
    }
}

fn members_to_proto(members: &[LoadBalanceMember]) -> Vec<pb::LoadBalanceMember> {
    members
        .iter()
        .map(|m| pb::LoadBalanceMember {
            slot: m.slot,
            name: m.name.clone(),
        })
        .collect()
}

/// Names are trimmed here; the service checks the list (see
/// `services::node::members_ok`).
fn members_from_proto(members: Vec<pb::LoadBalanceMember>) -> Vec<LoadBalanceMember> {
    members
        .into_iter()
        .map(|m| LoadBalanceMember {
            slot: m.slot,
            name: m.name.trim().to_string(),
        })
        .collect()
}

fn relay_protocol_to_proto(protocol: RelayProtocol) -> i32 {
    match protocol {
        RelayProtocol::TcpRaw => pb::RelayProtocol::RelayTcpRaw,
        RelayProtocol::TcpTls => pb::RelayProtocol::RelayTcpTls,
        RelayProtocol::Quic => pb::RelayProtocol::RelayQuic,
    }
    .into()
}

fn relay_protocol_from_proto(protocol: i32) -> Result<RelayProtocol, Status> {
    match pb::RelayProtocol::try_from(protocol) {
        Ok(pb::RelayProtocol::RelayTcpRaw) => Ok(RelayProtocol::TcpRaw),
        Ok(pb::RelayProtocol::RelayTcpTls) => Ok(RelayProtocol::TcpTls),
        Ok(pb::RelayProtocol::RelayQuic) => Ok(RelayProtocol::Quic),
        Ok(pb::RelayProtocol::Unspecified) | Err(_) => Err(Status::invalid_argument(format!(
            "protocol: unknown relay protocol {protocol}"
        ))),
    }
}

fn lane_to_proto(lane: &Lane) -> pb::Lane {
    pb::Lane {
        key: lane.key.clone(),
        group_node_id: lane.group.to_string(),
        channel_pod_id: lane.channel.to_string(),
        role: match lane.role {
            LaneRole::Distribute => pb::LaneRole::LaneDistribute,
            LaneRole::Relay => pb::LaneRole::LaneRelay,
            LaneRole::Landing => pb::LaneRole::LaneLanding,
            LaneRole::Aggregate => pb::LaneRole::LaneAggregate,
        }
        .into(),
        source_node_id: lane
            .source
            .as_ref()
            .map(|s| s.to_string())
            .unwrap_or_default(),
    }
}

fn port_to_proto(port: &PortEntity) -> pb::Port {
    pb::Port {
        id: port.id.to_string(),
        node_id: port.owner.to_string(),
        kind: port_kind_to_proto(port.kind),
        direction: match port.direction {
            PortDirection::Input => pb::PortDirection::PortInput,
            PortDirection::Output => pb::PortDirection::PortOutput,
        }
        .into(),
        key: port.key.clone(),
        position: port.position,
    }
}

fn proxy_to_proto(value: Option<ProxyProtocolVersion>) -> i32 {
    match value {
        None => pb::ProxyProtocolVersion::Unspecified,
        Some(ProxyProtocolVersion::V1) => pb::ProxyProtocolVersion::ProxyV1,
        Some(ProxyProtocolVersion::V2) => pb::ProxyProtocolVersion::ProxyV2,
    }
    .into()
}

fn proxy_from_proto(field: &str, value: i32) -> Result<Option<ProxyProtocolVersion>, Status> {
    match pb::ProxyProtocolVersion::try_from(value) {
        Ok(pb::ProxyProtocolVersion::ProxyV1) => Ok(Some(ProxyProtocolVersion::V1)),
        Ok(pb::ProxyProtocolVersion::ProxyV2) => Ok(Some(ProxyProtocolVersion::V2)),
        // Unspecified means "no proxy protocol".
        Ok(pb::ProxyProtocolVersion::Unspecified) => Ok(None),
        Err(_) => Err(Status::invalid_argument(format!(
            "{field}: unknown value {value}"
        ))),
    }
}

fn spec_to_proto(spec: &NodeSpec) -> pb::NodeSpec {
    use pb::node_spec::Spec;
    let spec = match spec {
        NodeSpec::Pod(cfg) => Spec::Pod(pb::PodConfig {
            server_id: cfg.server.to_string(),
            port: u32::from(cfg.port),
            bind_ip: cfg.bind_ip.clone().unwrap_or_default(),
            advertise_ip: cfg.advertise_ip.clone().unwrap_or_default(),
        }),
        NodeSpec::Entry(cfg) => Spec::Entry(pb::EntryConfig {
            receive_proxy_protocol: proxy_to_proto(cfg.receive_proxy_protocol),
            tls: cfg.tls.as_ref().map(|tls| pb::TlsConfig {
                sni: tls.sni.clone(),
                dns_provider_id: tls.dns_provider.to_string(),
                domain_id: tls.domain_id.clone(),
                acme_directory: tls.acme_directory.clone(),
            }),
        }),
        NodeSpec::Relay(cfg) => Spec::Relay(pb::RelayConfig {
            protocol: relay_protocol_to_proto(cfg.protocol),
            override_ip_address: cfg.override_ip_address.clone().unwrap_or_default(),
            override_port: cfg.override_port.map(u32::from).unwrap_or_default(),
        }),
        NodeSpec::Exit(cfg) => Spec::Exit(pb::ExitConfig {
            destination: cfg.destination.clone(),
            pass_proxy_protocol: proxy_to_proto(cfg.pass_proxy_protocol),
        }),
        NodeSpec::LoadBalanceDistribute(cfg) => {
            Spec::LoadBalanceDistribute(pb::LoadBalanceDistributeConfig {
                mode: lb_mode_to_proto(cfg.mode),
                protocol: relay_protocol_to_proto(cfg.protocol),
                members: members_to_proto(&cfg.members),
            })
        }
        NodeSpec::LoadBalanceAggregate(cfg) => {
            Spec::LoadBalanceAggregate(pb::LoadBalanceAggregateConfig {
                members: members_to_proto(&cfg.members),
            })
        }
        NodeSpec::UniversalPod(cfg) => Spec::UniversalPod(pb::UniversalPodConfig {
            server_id: cfg.server.to_string(),
        }),

        NodeSpec::CanvasImport(cfg) => Spec::CanvasImport(pb::CanvasImportConfig {
            canvas_id: cfg.canvas.to_string(),
        }),
        NodeSpec::CanvasExport(cfg) => Spec::CanvasExport(pb::CanvasExportConfig {
            kind: port_kind_to_proto(cfg.kind),
            direction: match cfg.direction {
                CanvasExportAs::InputIntoCanvas => pb::CanvasExportAs::InputIntoCanvas,
                CanvasExportAs::OutputOutOfCanvas => pb::CanvasExportAs::OutputOutOfCanvas,
            }
            .into(),
        }),
    };
    pb::NodeSpec { spec: Some(spec) }
}

/// An optional IP on the wire: empty is unset, anything else must parse and is
/// stored in its canonical form.
fn optional_ip(label: &str, raw: &str) -> Result<Option<String>, Status> {
    let raw = raw.trim();
    if raw.is_empty() {
        return Ok(None);
    }
    raw.parse::<std::net::IpAddr>()
        .map(|ip| Some(ip.to_string()))
        .map_err(|_| Status::invalid_argument(format!("{label}: '{raw}' is not an IP address")))
}

fn spec_from_proto(spec: Option<pb::NodeSpec>) -> Result<NodeSpec, Status> {
    use pb::node_spec::Spec;
    let spec = spec
        .and_then(|s| s.spec)
        .ok_or_else(|| Status::invalid_argument("spec is required"))?;
    Ok(match spec {
        Spec::Pod(cfg) => NodeSpec::Pod(PodConfig {
            server: {
                if cfg.server_id.is_empty() {
                    return Err(Status::invalid_argument("server_id is required"));
                }
                ids::server_id(&cfg.server_id)
            },
            bind_ip: optional_ip("bind_ip", &cfg.bind_ip)?,
            advertise_ip: optional_ip("advertise_ip", &cfg.advertise_ip)?,
            port: match u16::try_from(cfg.port) {
                Ok(port) if port != 0 => port,
                _ => {
                    return Err(Status::invalid_argument(format!(
                        "port: {} is out of range (1-65535)",
                        cfg.port
                    )));
                }
            },
        }),
        Spec::Entry(cfg) => NodeSpec::Entry(EntryConfig {
            receive_proxy_protocol: proxy_from_proto(
                "receive_proxy_protocol",
                cfg.receive_proxy_protocol,
            )?,
            tls: cfg.tls.map(|tls| TlsConfig {
                sni: tls.sni,
                dns_provider: ids::dns_provider_id(&tls.dns_provider_id),
                domain_id: tls.domain_id,
                acme_directory: tls.acme_directory,
            }),
        }),
        Spec::Relay(cfg) => NodeSpec::Relay(RelayConfig {
            protocol: relay_protocol_from_proto(cfg.protocol)?,
            override_ip_address: (!cfg.override_ip_address.is_empty())
                .then_some(cfg.override_ip_address),
            override_port: match cfg.override_port {
                0 => None,
                port => Some(
                    u16::try_from(port)
                        .map_err(|_| Status::invalid_argument("override_port out of range"))?,
                ),
            },
        }),
        Spec::Exit(cfg) => NodeSpec::Exit(ExitConfig {
            destination: cfg.destination,
            pass_proxy_protocol: proxy_from_proto("pass_proxy_protocol", cfg.pass_proxy_protocol)?,
        }),
        Spec::LoadBalanceDistribute(cfg) => {
            NodeSpec::LoadBalanceDistribute(LoadBalanceDistributeConfig {
                mode: lb_mode_from_proto(cfg.mode)?,
                // Unspecified is the default: raw TCP, like a relay's default.
                protocol: match pb::RelayProtocol::try_from(cfg.protocol) {
                    Ok(pb::RelayProtocol::Unspecified) => RelayProtocol::TcpRaw,
                    _ => relay_protocol_from_proto(cfg.protocol)?,
                },
                members: members_from_proto(cfg.members),
            })
        }
        Spec::LoadBalanceAggregate(cfg) => {
            NodeSpec::LoadBalanceAggregate(LoadBalanceAggregateConfig {
                members: members_from_proto(cfg.members),
            })
        }
        Spec::UniversalPod(cfg) => {
            if cfg.server_id.is_empty() {
                return Err(Status::invalid_argument("server_id is required"));
            }
            NodeSpec::UniversalPod(UniversalPodConfig {
                server: ids::server_id(&cfg.server_id),
            })
        }

        Spec::CanvasImport(cfg) => {
            if cfg.canvas_id.is_empty() {
                return Err(Status::invalid_argument("canvas_id is required"));
            }
            NodeSpec::CanvasImport(CanvasImportConfig {
                canvas: ids::canvas_id(&cfg.canvas_id),
            })
        }
        Spec::CanvasExport(cfg) => NodeSpec::CanvasExport(CanvasExportConfig {
            kind: match pb::PortKind::try_from(cfg.kind) {
                Ok(pb::PortKind::DeriveListen) => PortKind::DeriveListen,
                Ok(pb::PortKind::DeriveDestination) => PortKind::DeriveDestination,
                Ok(pb::PortKind::Unspecified | pb::PortKind::Bundle) | Err(_) => {
                    return Err(Status::invalid_argument(format!(
                        "kind: unknown port kind {}",
                        cfg.kind
                    )));
                }
            },
            direction: match pb::CanvasExportAs::try_from(cfg.direction) {
                Ok(pb::CanvasExportAs::InputIntoCanvas) => CanvasExportAs::InputIntoCanvas,
                Ok(pb::CanvasExportAs::OutputOutOfCanvas) => CanvasExportAs::OutputOutOfCanvas,
                Ok(pb::CanvasExportAs::Unspecified) | Err(_) => {
                    return Err(Status::invalid_argument(format!(
                        "direction: unknown canvas export direction {}",
                        cfg.direction
                    )));
                }
            },
        }),
    })
}

/// `import_targets` resolves an import node's `import_target`; a node that is
/// not an import, or whose target is not in the list, leaves it unset.
fn node_row_to_proto(
    node: &NodeEntity,
    ports: &[PortEntity],
    import_targets: &[CanvasEntity],
) -> pb::Node {
    let import_target = match &node.spec {
        NodeSpec::CanvasImport(cfg) => {
            let key = cfg.canvas.to_string();
            import_targets
                .iter()
                .find(|c| c.id.to_string() == key)
                .map(canvas_to_proto)
        }
        _ => None,
    };
    pb::Node {
        id: node.id.to_string(),
        canvas_id: node.canvas.to_string(),
        name: node.name.clone(),
        comment: node.comment.clone(),
        spec: Some(spec_to_proto(&node.spec)),
        position: Some(position_to_proto(node.position)),
        ports: ports.iter().map(port_to_proto).collect(),
        import_target,
        lane: node.lane.as_ref().map(lane_to_proto),
    }
}

fn node_to_proto(node: &NodeWithPorts, import_targets: &[CanvasEntity]) -> pb::Node {
    node_row_to_proto(&node.node, &node.ports, import_targets)
}

fn tree_to_proto(tree: &CanvasTree) -> pb::CanvasTreeNode {
    pb::CanvasTreeNode {
        canvas: Some(canvas_to_proto(&tree.canvas)),
        children: tree.children.iter().map(tree_to_proto).collect(),
    }
}

fn edge_to_proto(edge: &EdgeConnectionEntity) -> pb::Edge {
    pb::Edge {
        id: edge.id.to_string(),
        source_port_id: edge.source.to_string(),
        target_port_id: edge.target.to_string(),
    }
}

fn listener_cap_to_proto(cap: &ListenerCap) -> pb::ListenerCap {
    pb::ListenerCap {
        server_id: cap.server_key(),
        port: u32::try_from(cap.port).unwrap_or_default(),
        protocol: cap.protocol.name().to_string(),
    }
}

fn snapshot_to_proto(snapshot: &ConfigSnapshot) -> pb::ConfigSnapshot {
    pb::ConfigSnapshot {
        revision: snapshot.revision,
        created_at: snapshot.created_at.to_rfc3339(),
        forwardings: snapshot
            .forwardings
            .iter()
            .map(|deps| pb::ForwardingDeps {
                serves: Some(listener_cap_to_proto(&deps.serves)),
                points_at: deps.points_at.iter().map(listener_cap_to_proto).collect(),
            })
            .collect(),
    }
}

fn problem_to_proto(problem: &TopologyProblem) -> pb::Problem {
    pb::Problem {
        severity: match problem.severity {
            ProblemSeverity::Error => pb::ProblemSeverity::ProblemError,
            ProblemSeverity::Warning => pb::ProblemSeverity::ProblemWarning,
        }
        .into(),
        kind: match problem.kind {
            ProblemKind::PortKindMismatch => pb::ProblemKind::PortKindMismatch,
            ProblemKind::EdgeDirectionInvalid => pb::ProblemKind::EdgeDirectionInvalid,
            ProblemKind::EdgeSelfNode => pb::ProblemKind::EdgeSelfNode,
            ProblemKind::EdgeCrossCanvas => pb::ProblemKind::EdgeCrossCanvas,
            ProblemKind::PortOversubscribed => pb::ProblemKind::PortOversubscribed,
            ProblemKind::PortShapeInvalid => pb::ProblemKind::PortShapeInvalid,
            ProblemKind::Cycle => pb::ProblemKind::Cycle,
            ProblemKind::DuplicateListen => pb::ProblemKind::DuplicateListen,
            ProblemKind::PodServerForeign => pb::ProblemKind::PodServerForeign,
            ProblemKind::ServerNoAddress => pb::ProblemKind::ServerNoAddress,
            ProblemKind::ExitDestinationInvalid => pb::ProblemKind::ExitDestinationInvalid,
            ProblemKind::IpHashWithoutClientIp => pb::ProblemKind::IpHashWithoutClientIp,
            ProblemKind::CanvasImportSelf => pb::ProblemKind::CanvasImportSelf,
            ProblemKind::CanvasImportAncestor => pb::ProblemKind::CanvasImportAncestor,
            ProblemKind::CanvasImportDuplicate => pb::ProblemKind::CanvasImportDuplicate,
            ProblemKind::CanvasImportUnresolved => pb::ProblemKind::CanvasImportUnresolved,
            ProblemKind::PodPortUnconnected => pb::ProblemKind::PodPortUnconnected,
            ProblemKind::RelaySameServer => pb::ProblemKind::RelaySameServer,
            ProblemKind::DistributeSingleMember => pb::ProblemKind::DistributeSingleMember,
            ProblemKind::ChannelTargetNotPod => pb::ProblemKind::ChannelTargetNotPod,
            ProblemKind::BundleEdgeInvalid => pb::ProblemKind::BundleEdgeInvalid,
            ProblemKind::BundleCycle => pb::ProblemKind::BundleCycle,
            ProblemKind::ChannelNoExit => pb::ProblemKind::ChannelNoExit,
            ProblemKind::ChannelNoTransit => pb::ProblemKind::ChannelNoTransit,
            ProblemKind::LanesStale => pb::ProblemKind::LanesStale,
        }
        .into(),
        message: problem.message.clone(),
        node_ids: problem.nodes.iter().map(|n| n.to_string()).collect(),
        edge_ids: problem.edges.iter().map(|e| e.to_string()).collect(),
        port_ids: problem.ports.iter().map(|p| p.to_string()).collect(),
    }
}

// --- handlers ---------------------------------------------------------------

#[tonic::async_trait]
impl pb::orchestration_server::Orchestration for OrchestrationGrpc {
    async fn create_canvas(
        &self,
        request: Request<pb::CreateCanvasRequest>,
    ) -> Result<Response<pb::CreateCanvasReply>, Status> {
        let actor = auth::rpc::middleware::from_request(&request)?;
        let input = request.into_inner();
        let canvas = self
            .canvases
            .process(canvas::CreateCanvas {
                actor,
                name: input.name,
                description: input.description,
            })
            .await?;
        Ok(Response::new(pb::CreateCanvasReply {
            canvas: Some(canvas_to_proto(&canvas)),
        }))
    }

    async fn list_canvases(
        &self,
        request: Request<pb::ListCanvasesRequest>,
    ) -> Result<Response<pb::ListCanvasesReply>, Status> {
        let actor = auth::rpc::middleware::from_request(&request)?;
        let input = request.into_inner();
        let canvases = self
            .canvases
            .process(canvas::ListCanvases {
                actor,
                include_subcanvases: input.include_subcanvases,
            })
            .await?;
        Ok(Response::new(pb::ListCanvasesReply {
            canvases: canvases.iter().map(canvas_to_proto).collect(),
        }))
    }

    async fn get_canvas(
        &self,
        request: Request<pb::GetCanvasRequest>,
    ) -> Result<Response<pb::GetCanvasReply>, Status> {
        let actor = auth::rpc::middleware::from_request(&request)?;
        let input = request.into_inner();
        let contents = self
            .canvases
            .process(canvas::GetCanvas {
                actor,
                canvas: ids::canvas_id(&input.canvas_id),
            })
            .await?;
        Ok(Response::new(contents_to_proto(&contents)))
    }

    async fn get_canvas_tree(
        &self,
        request: Request<pb::GetCanvasTreeRequest>,
    ) -> Result<Response<pb::GetCanvasTreeReply>, Status> {
        let actor = auth::rpc::middleware::from_request(&request)?;
        let input = request.into_inner();
        let tree = self
            .canvases
            .process(canvas::GetCanvasTree {
                actor,
                canvas: ids::canvas_id(&input.canvas_id),
            })
            .await?;
        Ok(Response::new(pb::GetCanvasTreeReply {
            root: Some(tree_to_proto(&tree)),
        }))
    }

    async fn update_canvas(
        &self,
        request: Request<pb::UpdateCanvasRequest>,
    ) -> Result<Response<pb::UpdateCanvasReply>, Status> {
        let actor = auth::rpc::middleware::from_request(&request)?;
        let input = request.into_inner();
        let canvas = self
            .canvases
            .process(canvas::UpdateCanvas {
                actor,
                canvas: ids::canvas_id(&input.canvas_id),
                name: input.name,
                description: input.description,
            })
            .await?;
        Ok(Response::new(pb::UpdateCanvasReply {
            canvas: Some(canvas_to_proto(&canvas)),
        }))
    }

    async fn delete_canvas(
        &self,
        request: Request<pb::DeleteCanvasRequest>,
    ) -> Result<Response<pb::DeleteCanvasReply>, Status> {
        let actor = auth::rpc::middleware::from_request(&request)?;
        let input = request.into_inner();
        self.canvases
            .process(canvas::DeleteCanvas {
                actor,
                canvas: ids::canvas_id(&input.canvas_id),
            })
            .await?;
        Ok(Response::new(pb::DeleteCanvasReply {}))
    }

    async fn validate_canvas(
        &self,
        request: Request<pb::ValidateCanvasRequest>,
    ) -> Result<Response<pb::ValidateCanvasReply>, Status> {
        let actor = auth::rpc::middleware::from_request(&request)?;
        let input = request.into_inner();
        let problems = self
            .canvases
            .process(canvas::ValidateCanvas {
                actor,
                canvas: ids::canvas_id(&input.canvas_id),
            })
            .await?;
        Ok(Response::new(pb::ValidateCanvasReply {
            problems: problems.iter().map(problem_to_proto).collect(),
        }))
    }

    async fn create_server(
        &self,
        request: Request<pb::CreateServerRequest>,
    ) -> Result<Response<pb::CreateServerReply>, Status> {
        let actor = auth::rpc::middleware::from_request(&request)?;
        let input = request.into_inner();
        let server = self
            .servers
            .process(server::CreateServer {
                actor,
                canvas: ids::canvas_id(&input.canvas_id),
                name: input.name,
                icon: input.icon,
                comment: input.comment,
                position: position_or_origin(input.position),
                ipv6_resolve: ipv6_from_proto(input.ipv6_resolve)?,
                log_level: input.log_level,
                addresses: server::AddressOverrides::parse(
                    &input.override_v4,
                    &input.override_v6,
                    &input.extra_addresses,
                )?,
            })
            .await?;
        Ok(Response::new(pb::CreateServerReply {
            server: Some(server_to_proto(&server)),
        }))
    }

    async fn update_server(
        &self,
        request: Request<pb::UpdateServerRequest>,
    ) -> Result<Response<pb::UpdateServerReply>, Status> {
        let actor = auth::rpc::middleware::from_request(&request)?;
        let input = request.into_inner();
        let server = self
            .servers
            .process(server::UpdateServer {
                actor,
                server: ids::server_id(&input.server_id),
                name: input.name,
                icon: input.icon,
                comment: input.comment,
                ipv6_resolve: ipv6_from_proto(input.ipv6_resolve)?,
                log_level: input.log_level,
                addresses: server::AddressOverrides::parse(
                    &input.override_v4,
                    &input.override_v6,
                    &input.extra_addresses,
                )?,
                agent_unit: server::agent_unit_from(&input.agent_unit)?,
            })
            .await?;
        Ok(Response::new(pb::UpdateServerReply {
            server: Some(server_to_proto(&server)),
        }))
    }

    async fn issue_server_agent_install(
        &self,
        request: Request<pb::IssueServerAgentInstallRequest>,
    ) -> Result<Response<pb::IssueServerAgentInstallReply>, Status> {
        let actor = auth::rpc::middleware::from_request(&request)?;
        let input = request.into_inner();
        let install = self
            .servers
            .process(server::IssueServerAgentInstall {
                actor,
                server: ids::server_id(&input.server_id),
                unit: server::agent_unit_from(&input.unit)?,
            })
            .await?;
        Ok(Response::new(pb::IssueServerAgentInstallReply {
            command: install.command,
            unit: install.unit,
            version: install.version,
            server: Some(server_to_proto(&install.server)),
        }))
    }

    async fn get_agent_release(
        &self,
        request: Request<pb::GetAgentReleaseRequest>,
    ) -> Result<Response<pb::GetAgentReleaseReply>, Status> {
        let actor = auth::rpc::middleware::from_request(&request)?;
        let info = self
            .servers
            .process(server::GetAgentRelease { actor })
            .await?;
        let release = info.release;
        Ok(Response::new(pb::GetAgentReleaseReply {
            version: release
                .as_ref()
                .map(|r| r.version.clone())
                .unwrap_or_default(),
            sha256: release
                .as_ref()
                .map(|r| r.sha256.clone())
                .unwrap_or_default(),
            arch: release.as_ref().map(|r| r.arch.clone()).unwrap_or_default(),
            published_at: release
                .as_ref()
                .map(|r| r.published_at.to_rfc3339())
                .unwrap_or_default(),
            base_url_configured: info.base_url_configured,
        }))
    }

    async fn request_agent_update(
        &self,
        request: Request<pb::RequestAgentUpdateRequest>,
    ) -> Result<Response<pb::RequestAgentUpdateReply>, Status> {
        let actor = auth::rpc::middleware::from_request(&request)?;
        let input = request.into_inner();
        let server = self
            .servers
            .process(server::RequestAgentUpdate {
                actor,
                server: ids::server_id(&input.server_id),
            })
            .await?;
        Ok(Response::new(pb::RequestAgentUpdateReply {
            server: Some(server_to_proto(&server)),
        }))
    }

    async fn delete_server(
        &self,
        request: Request<pb::DeleteServerRequest>,
    ) -> Result<Response<pb::DeleteServerReply>, Status> {
        let actor = auth::rpc::middleware::from_request(&request)?;
        let input = request.into_inner();
        self.servers
            .process(server::DeleteServer {
                actor,
                server: ids::server_id(&input.server_id),
            })
            .await?;
        Ok(Response::new(pb::DeleteServerReply {}))
    }

    async fn move_server(
        &self,
        request: Request<pb::MoveServerRequest>,
    ) -> Result<Response<pb::MoveServerReply>, Status> {
        let actor = auth::rpc::middleware::from_request(&request)?;
        let input = request.into_inner();
        self.servers
            .process(server::MoveServer {
                actor,
                server: ids::server_id(&input.server_id),
                position: position_or_origin(input.position),
            })
            .await?;
        Ok(Response::new(pb::MoveServerReply {}))
    }

    async fn create_node(
        &self,
        request: Request<pb::CreateNodeRequest>,
    ) -> Result<Response<pb::CreateNodeReply>, Status> {
        let actor = auth::rpc::middleware::from_request(&request)?;
        let input = request.into_inner();
        let node = self
            .nodes
            .process(node::CreateNode {
                actor: actor.clone(),
                canvas: ids::canvas_id(&input.canvas_id),
                name: input.name,
                comment: input.comment,
                spec: spec_from_proto(input.spec)?,
                position: position_or_origin(input.position),
                item_count: input.item_count,
            })
            .await?;
        let targets = self.import_target_of(&actor, &node.node).await?;
        Ok(Response::new(pb::CreateNodeReply {
            node: Some(node_to_proto(&node, &targets)),
        }))
    }

    async fn replace_node_spec(
        &self,
        request: Request<pb::ReplaceNodeSpecRequest>,
    ) -> Result<Response<pb::ReplaceNodeSpecReply>, Status> {
        let actor = auth::rpc::middleware::from_request(&request)?;
        let input = request.into_inner();
        let node = self
            .nodes
            .process(node::ReplaceNodeSpec {
                actor,
                node: ids::node_id(&input.node_id),
                spec: spec_from_proto(input.spec)?,
                item_count: input.item_count,
            })
            .await?;
        // An import node is never replaced (the service rejects it), so the
        // reply can only be a non-import node.
        Ok(Response::new(pb::ReplaceNodeSpecReply {
            node: Some(node_to_proto(&node, &[])),
        }))
    }

    async fn update_node_meta(
        &self,
        request: Request<pb::UpdateNodeMetaRequest>,
    ) -> Result<Response<pb::UpdateNodeMetaReply>, Status> {
        let actor = auth::rpc::middleware::from_request(&request)?;
        let input = request.into_inner();
        let node = self
            .nodes
            .process(node::UpdateNodeMeta {
                actor: actor.clone(),
                node: ids::node_id(&input.node_id),
                name: input.name,
                comment: input.comment,
                position: position_from_proto(input.position),
            })
            .await?;
        let targets = self.import_target_of(&actor, &node.node).await?;
        Ok(Response::new(pb::UpdateNodeMetaReply {
            node: Some(node_to_proto(&node, &targets)),
        }))
    }

    async fn retire_node(
        &self,
        request: Request<pb::RetireNodeRequest>,
    ) -> Result<Response<pb::RetireNodeReply>, Status> {
        let actor = auth::rpc::middleware::from_request(&request)?;
        let input = request.into_inner();
        self.nodes
            .process(node::RetireNode {
                actor,
                node: ids::node_id(&input.node_id),
            })
            .await?;
        Ok(Response::new(pb::RetireNodeReply {}))
    }

    async fn force_delete_node(
        &self,
        request: Request<pb::ForceDeleteNodeRequest>,
    ) -> Result<Response<pb::ForceDeleteNodeReply>, Status> {
        let actor = auth::rpc::middleware::from_request(&request)?;
        let input = request.into_inner();
        self.nodes
            .process(node::ForceDeleteNode {
                actor,
                node: ids::node_id(&input.node_id),
            })
            .await?;
        Ok(Response::new(pb::ForceDeleteNodeReply {}))
    }

    async fn connect_ports(
        &self,
        request: Request<pb::ConnectRequest>,
    ) -> Result<Response<pb::ConnectReply>, Status> {
        let actor = auth::rpc::middleware::from_request(&request)?;
        let input = request.into_inner();
        let end =
            |port: &str, handle: Option<pb::UniversalHandle>| -> Result<edge::ConnectEnd, Status> {
                if !port.is_empty() {
                    return Ok(edge::ConnectEnd::Port(ids::port_id(port)));
                }
                let handle = handle.ok_or_else(|| {
                    Status::invalid_argument("each end needs a port id or a universal handle")
                })?;
                if handle.node_id.is_empty() {
                    return Err(Status::invalid_argument("handle: node_id is required"));
                }
                let group = match pb::UniversalGroup::try_from(handle.group) {
                    Ok(pb::UniversalGroup::ChannelOut) => edge::UniversalGroup::ChannelOut,
                    Ok(pb::UniversalGroup::BundleIn) => edge::UniversalGroup::BundleIn,
                    Ok(pb::UniversalGroup::Unspecified) | Err(_) => {
                        return Err(Status::invalid_argument("handle: group is required"));
                    }
                };
                Ok(edge::ConnectEnd::Handle {
                    node: ids::node_id(&handle.node_id),
                    group,
                })
            };
        let output = end(&input.output_port_id, input.output_handle)?;
        let input_end = end(&input.input_port_id, input.input_handle)?;
        let edge = match (output, input_end) {
            (edge::ConnectEnd::Port(output_port), edge::ConnectEnd::Port(input_port)) => {
                self.edges
                    .process(edge::Connect {
                        actor,
                        output_port,
                        input_port,
                    })
                    .await?
            }
            (output, input) => {
                self.edges
                    .process(edge::ConnectUniversal {
                        actor,
                        output,
                        input,
                    })
                    .await?
            }
        };
        Ok(Response::new(pb::ConnectReply {
            edge: Some(edge_to_proto(&edge)),
        }))
    }

    async fn disconnect(
        &self,
        request: Request<pb::DisconnectRequest>,
    ) -> Result<Response<pb::DisconnectReply>, Status> {
        let actor = auth::rpc::middleware::from_request(&request)?;
        let input = request.into_inner();
        self.edges
            .process(edge::Disconnect {
                actor,
                edge: ids::edge_id(&input.edge_id),
            })
            .await?;
        Ok(Response::new(pb::DisconnectReply {}))
    }

    async fn force_disconnect(
        &self,
        request: Request<pb::ForceDisconnectRequest>,
    ) -> Result<Response<pb::ForceDisconnectReply>, Status> {
        let actor = auth::rpc::middleware::from_request(&request)?;
        let input = request.into_inner();
        self.edges
            .process(edge::ForceDisconnect {
                actor,
                edge: ids::edge_id(&input.edge_id),
            })
            .await?;
        Ok(Response::new(pb::ForceDisconnectReply {}))
    }

    async fn get_server_config(
        &self,
        request: Request<pb::GetServerConfigRequest>,
    ) -> Result<Response<pb::GetServerConfigReply>, Status> {
        let actor = auth::rpc::middleware::from_request(&request)?;
        let input = request.into_inner();
        let (revision, toml) = self
            .rollout
            .process(rollout::GetServerConfig {
                actor,
                server: ids::server_id(&input.server_id),
            })
            .await?;
        Ok(Response::new(pb::GetServerConfigReply { revision, toml }))
    }

    async fn get_server_rollout_status(
        &self,
        request: Request<pb::GetServerRolloutStatusRequest>,
    ) -> Result<Response<pb::GetServerRolloutStatusReply>, Status> {
        let actor = auth::rpc::middleware::from_request(&request)?;
        let input = request.into_inner();
        let status = self
            .rollout
            .process(rollout::GetServerRolloutStatus {
                actor,
                server: ids::server_id(&input.server_id),
            })
            .await?;
        Ok(Response::new(rollout_status_to_proto(status)))
    }

    async fn forget_server_applied(
        &self,
        request: Request<pb::ForgetServerAppliedRequest>,
    ) -> Result<Response<pb::ForgetServerAppliedReply>, Status> {
        let actor = auth::rpc::middleware::from_request(&request)?;
        let input = request.into_inner();
        self.rollout
            .process(rollout::ForgetServerApplied {
                actor,
                server: ids::server_id(&input.server_id),
            })
            .await?;
        Ok(Response::new(pb::ForgetServerAppliedReply {}))
    }

    async fn list_server_health_history(
        &self,
        request: Request<pb::ListServerHealthHistoryRequest>,
    ) -> Result<Response<pb::ListServerHealthHistoryReply>, Status> {
        let actor = auth::rpc::middleware::from_request(&request)?;
        let input = request.into_inner();
        let (start, end) = history_window(&input.start, &input.end)?;
        let records = self
            .health
            .process(health::ListServerHealthHistory {
                actor,
                server: ids::server_id(&input.server_id),
                start,
                end,
            })
            .await?;
        Ok(Response::new(pb::ListServerHealthHistoryReply {
            records: records.iter().map(server_health_record_to_proto).collect(),
        }))
    }

    async fn list_node_health_history(
        &self,
        request: Request<pb::ListNodeHealthHistoryRequest>,
    ) -> Result<Response<pb::ListNodeHealthHistoryReply>, Status> {
        let actor = auth::rpc::middleware::from_request(&request)?;
        let input = request.into_inner();
        let (start, end) = history_window(&input.start, &input.end)?;
        let limit = if input.limit == 0 {
            health::DEFAULT_NODE_HISTORY_LIMIT
        } else {
            i64::from(input.limit)
        };
        let records = self
            .health
            .process(health::ListNodeHealthHistory {
                actor,
                node: ids::node_id(&input.node_id),
                start,
                end,
                limit,
            })
            .await?;
        Ok(Response::new(pb::ListNodeHealthHistoryReply {
            records: records.iter().map(node_health_record_to_proto).collect(),
        }))
    }

    async fn create_dns_provider(
        &self,
        request: Request<pb::CreateDnsProviderRequest>,
    ) -> Result<Response<pb::CreateDnsProviderReply>, Status> {
        let actor = auth::rpc::middleware::from_request(&request)?;
        let input = request.into_inner();
        let provider = self
            .dns
            .process(dns::CreateDnsProvider {
                actor,
                name: input.name,
                provider: dns_provider_from_proto(input.provider)?,
                account_id: input.account_id,
                api_secret: input.api_secret,
            })
            .await?;
        Ok(Response::new(pb::CreateDnsProviderReply {
            provider: Some(dns_provider_summary_to_proto(&provider)),
        }))
    }

    async fn list_dns_providers(
        &self,
        request: Request<pb::ListDnsProvidersRequest>,
    ) -> Result<Response<pb::ListDnsProvidersReply>, Status> {
        let actor = auth::rpc::middleware::from_request(&request)?;
        let providers = self.dns.process(dns::ListDnsProviders { actor }).await?;
        Ok(Response::new(pb::ListDnsProvidersReply {
            providers: providers
                .iter()
                .map(dns_provider_summary_to_proto)
                .collect(),
        }))
    }

    async fn update_dns_provider(
        &self,
        request: Request<pb::UpdateDnsProviderRequest>,
    ) -> Result<Response<pb::UpdateDnsProviderReply>, Status> {
        let actor = auth::rpc::middleware::from_request(&request)?;
        let input = request.into_inner();
        let provider = self
            .dns
            .process(dns::UpdateDnsProvider {
                actor,
                id: ids::dns_provider_id(&input.dns_provider_id),
                name: input.name,
                account_id: input.account_id,
                api_secret: Some(input.api_secret).filter(|s| !s.is_empty()),
            })
            .await?;
        Ok(Response::new(pb::UpdateDnsProviderReply {
            provider: Some(dns_provider_summary_to_proto(&provider)),
        }))
    }

    async fn delete_dns_provider(
        &self,
        request: Request<pb::DeleteDnsProviderRequest>,
    ) -> Result<Response<pb::DeleteDnsProviderReply>, Status> {
        let actor = auth::rpc::middleware::from_request(&request)?;
        let input = request.into_inner();
        self.dns
            .process(dns::DeleteDnsProvider {
                actor,
                id: ids::dns_provider_id(&input.dns_provider_id),
            })
            .await?;
        Ok(Response::new(pb::DeleteDnsProviderReply {}))
    }

    async fn list_certificates(
        &self,
        request: Request<pb::ListCertificatesRequest>,
    ) -> Result<Response<pb::ListCertificatesReply>, Status> {
        let actor = auth::rpc::middleware::from_request(&request)?;
        let certificates = self
            .certificates
            .process(acme::ListCertificates { actor })
            .await?;
        Ok(Response::new(pb::ListCertificatesReply {
            certificates: certificates.iter().map(certificate_to_proto).collect(),
        }))
    }

    async fn retry_certificate(
        &self,
        request: Request<pb::RetryCertificateRequest>,
    ) -> Result<Response<pb::RetryCertificateReply>, Status> {
        let actor = auth::rpc::middleware::from_request(&request)?;
        let input = request.into_inner();
        let certificate = self
            .certificates
            .process(acme::RetryCertificate {
                actor,
                id: ids::certificate_id(&input.certificate_id),
            })
            .await?;
        Ok(Response::new(pb::RetryCertificateReply {
            certificate: Some(certificate_to_proto(&certificate)),
        }))
    }

    async fn delete_certificate(
        &self,
        request: Request<pb::DeleteCertificateRequest>,
    ) -> Result<Response<pb::DeleteCertificateReply>, Status> {
        let actor = auth::rpc::middleware::from_request(&request)?;
        let input = request.into_inner();
        self.certificates
            .process(acme::DeleteCertificate {
                actor,
                id: ids::certificate_id(&input.certificate_id),
            })
            .await?;
        Ok(Response::new(pb::DeleteCertificateReply {}))
    }

    async fn get_orchestration_config(
        &self,
        request: Request<pb::GetOrchestrationConfigRequest>,
    ) -> Result<Response<pb::GetOrchestrationConfigReply>, Status> {
        let actor = auth::rpc::middleware::from_request(&request)?;
        let document = self
            .configs
            .process(config::GetModuleConfig { actor })
            .await?;
        Ok(Response::new(pb::GetOrchestrationConfigReply {
            config: Some(config_to_proto(document)?),
        }))
    }

    async fn set_orchestration_config(
        &self,
        request: Request<pb::SetOrchestrationConfigRequest>,
    ) -> Result<Response<pb::SetOrchestrationConfigReply>, Status> {
        let actor = auth::rpc::middleware::from_request(&request)?;
        let input = request.into_inner();
        let json = config_json(&input.json)?;
        let document = self
            .configs
            .process(config::SetModuleConfig { actor, json })
            .await?;
        Ok(Response::new(pb::SetOrchestrationConfigReply {
            config: Some(config_to_proto(document)?),
        }))
    }

    type WatchCanvasStream = ReceiverStream<Result<pb::CanvasEvent, Status>>;

    /// A full refreshed snapshot per change, coalesced.
    ///
    /// The view behind this is shared by every watcher of the canvas, so N open
    /// dashboards cost one reload per change. A client that stops reading gets
    /// fewer, newer snapshots rather than a backlog — which is why there is
    /// nothing for it to re-request.
    async fn watch_canvas(
        &self,
        request: Request<pb::WatchCanvasRequest>,
    ) -> Result<Response<Self::WatchCanvasStream>, Status> {
        let actor = auth::rpc::middleware::from_request(&request)?;
        let session = session_id(&request)?;
        let input = request.into_inner();
        let handle = self
            .live
            .process(live::WatchCanvas {
                actor,
                canvas: ids::canvas_id(&input.canvas_id),
            })
            .await?;
        let (tx, rx) = mpsc::channel(STREAM_CAPACITY);
        let mut ticker = StreamTicker::new(
            self.sessions.clone(),
            session,
            self.live.config.stream_keepalive(),
        );
        tokio::spawn(async move {
            let mut handle = handle;
            let ended: Result<(), Status> = async {
                loop {
                    tokio::select! {
                        _ = tx.closed() => return Ok(()),
                        result = ticker.tick() => {
                            result?;
                            let event = pb::CanvasEvent {
                                event: Some(pb::canvas_event::Event::KeepAlive(pb::KeepAlive {})),
                            };
                            if tx.send(Ok(event)).await.is_err() {
                                return Ok(());
                            }
                        }
                        changed = handle.rx.changed() => {
                            if changed.is_err() {
                                return Ok(());
                            }
                            let value = handle.rx.borrow_and_update().clone();
                            match value {
                                ViewValue::Loading => {}
                                ViewValue::Missing => {
                                    return Err(Status::not_found("canvas not found"));
                                }
                                ViewValue::Ready { state, cause } => {
                                    let (kind, affected_ids) = cause_to_proto(cause.as_deref());
                                    let event = pb::CanvasEvent {
                                        event: Some(pb::canvas_event::Event::Snapshot(
                                            pb::CanvasSnapshot {
                                                generation: state
                                                    .contents
                                                    .ancestors
                                                    .first()
                                                    .unwrap_or(&state.contents.canvas)
                                                    .generation,
                                                cause: kind,
                                                affected_ids,
                                                contents: Some(contents_to_proto(&state.contents)),
                                            },
                                        )),
                                    };
                                    if tx.send(Ok(event)).await.is_err() {
                                        return Ok(());
                                    }
                                }
                            }
                        }
                    }
                }
            }
            .await;
            if let Err(status) = ended {
                let _ = tx.send(Err(status)).await;
            }
        });
        Ok(Response::new(ReceiverStream::new(rx)))
    }

    type WatchRolloutsStream = ReceiverStream<Result<pb::RolloutEvent, Status>>;

    async fn watch_rollouts(
        &self,
        request: Request<pb::WatchRolloutsRequest>,
    ) -> Result<Response<Self::WatchRolloutsStream>, Status> {
        let actor = auth::rpc::middleware::from_request(&request)?;
        let session = session_id(&request)?;
        let input = request.into_inner();
        let handle = self
            .live
            .process(live::WatchRollouts {
                actor,
                canvas: ids::canvas_id(&input.canvas_id),
            })
            .await?;
        let (tx, rx) = mpsc::channel(STREAM_CAPACITY);
        let mut ticker = StreamTicker::new(
            self.sessions.clone(),
            session,
            self.live.config.stream_keepalive(),
        );
        tokio::spawn(async move {
            let mut handle = handle;
            let ended: Result<(), Status> = async {
                loop {
                    tokio::select! {
                        _ = tx.closed() => return Ok(()),
                        result = ticker.tick() => {
                            result?;
                            let event = pb::RolloutEvent {
                                event: Some(pb::rollout_event::Event::KeepAlive(pb::KeepAlive {})),
                            };
                            if tx.send(Ok(event)).await.is_err() {
                                return Ok(());
                            }
                        }
                        changed = handle.rx.changed() => {
                            if changed.is_err() {
                                return Ok(());
                            }
                            let value = handle.rx.borrow_and_update().clone();
                            match value {
                                ViewValue::Loading => {}
                                ViewValue::Missing => {
                                    return Err(Status::not_found("canvas not found"));
                                }
                                ViewValue::Ready { state, .. } => {
                                    let event = pb::RolloutEvent {
                                        event: Some(pb::rollout_event::Event::Snapshot(
                                            pb::RolloutSnapshot {
                                                servers: state
                                                    .servers
                                                    .iter()
                                                    .map(|s| pb::ServerRollout {
                                                        server_id: s.server.id.to_string(),
                                                        canvas_id: s.server.canvas.to_string(),
                                                        status: Some(rollout_status_to_proto(
                                                            s.status.clone(),
                                                        )),
                                                    })
                                                    .collect(),
                                            },
                                        )),
                                    };
                                    if tx.send(Ok(event)).await.is_err() {
                                        return Ok(());
                                    }
                                }
                            }
                        }
                    }
                }
            }
            .await;
            if let Err(status) = ended {
                let _ = tx.send(Err(status)).await;
            }
        });
        Ok(Response::new(ReceiverStream::new(rx)))
    }

    type WatchServerHealthStream = ReceiverStream<Result<pb::ServerHealthEvent, Status>>;

    /// A record log, not a view: every accepted report is forwarded once.
    ///
    /// `last` is the watermark that makes that true across both paths — a
    /// duplicate bus message and a post-reconnect refetch are both filtered by
    /// it. It is a timestamp alone, unlike the node stream's `(time, id)` pair,
    /// because a server writes at most one `server_health_record` per event: two
    /// rows for one server can only share a `report_time` if two writes landed in
    /// the same microsecond, which nothing in the fleet does. The node stream
    /// needs the pair because one event writes a whole batch at one timestamp.
    async fn watch_server_health(
        &self,
        request: Request<pb::WatchServerHealthRequest>,
    ) -> Result<Response<Self::WatchServerHealthStream>, Status> {
        let actor = auth::rpc::middleware::from_request(&request)?;
        let session = session_id(&request)?;
        let input = request.into_inner();
        let (since, _) = history_window(&input.since, "")?;
        let server = ids::server_id(&input.server_id);
        let server_key = server.to_string();
        let watch = self
            .live
            .process(live::WatchServerHealth {
                actor,
                server: server.clone(),
                since,
            })
            .await?;
        let (tx, rx) = mpsc::channel(STREAM_CAPACITY);
        let mut ticker = StreamTicker::new(
            self.sessions.clone(),
            session,
            self.live.config.stream_keepalive(),
        );
        let db = self.live.db.clone();
        tokio::spawn(async move {
            let mut events = watch.events;
            let mut last = watch
                .records
                .last()
                .map(|record| record.report_time)
                .unwrap_or(since);
            let snapshot = pb::ServerHealthEvent {
                event: Some(pb::server_health_event::Event::Snapshot(
                    pb::ServerHealthSnapshot {
                        status: server_health_to_proto(watch.server.health_status),
                        last_seen_at: watch
                            .server
                            .last_seen_at
                            .map(|t| t.to_rfc3339())
                            .unwrap_or_default(),
                        records: watch
                            .records
                            .iter()
                            .map(server_health_record_to_proto)
                            .collect(),
                    },
                )),
            };
            let ended: Result<(), Status> = async {
                if tx.send(Ok(snapshot)).await.is_err() {
                    return Ok(());
                }
                loop {
                    tokio::select! {
                        _ = tx.closed() => return Ok(()),
                        result = ticker.tick() => {
                            result?;
                            let event = pb::ServerHealthEvent {
                                event: Some(pb::server_health_event::Event::KeepAlive(
                                    pb::KeepAlive {},
                                )),
                            };
                            if tx.send(Ok(event)).await.is_err() {
                                return Ok(());
                            }
                        }
                        received = events.recv() => match received {
                            Ok(LiveEvent::Message(message)) => {
                                let LiveMessage::ServerHealth { server: id, record, .. } = &*message
                                else {
                                    continue;
                                };
                                if *id != server_key {
                                    continue;
                                }
                                let time = live_time(record.report_time_unix_micros);
                                if time <= last {
                                    continue;
                                }
                                last = time;
                                let event = pb::ServerHealthEvent {
                                    event: Some(pb::server_health_event::Event::Record(
                                        server_health_live_to_proto(&server_key, record),
                                    )),
                                };
                                if tx.send(Ok(event)).await.is_err() {
                                    return Ok(());
                                }
                            }
                            // The bus may have skipped records; the database has
                            // them all, so read forward from the watermark.
                            Ok(LiveEvent::Resync)
                            | Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {
                                let rows = match retry_read(|| {
                                    db.process(ListServerHealthHistoryRows {
                                        server: server.clone(),
                                        start: last,
                                        end: Utc::now(),
                                    })
                                })
                                .await
                                {
                                    Ok(rows) => rows,
                                    // The signal that got us here is consumed;
                                    // swallowing the failure would leave the gap
                                    // open until the next reconnect, so end the
                                    // stream and let the client come back.
                                    Err(error) => {
                                        return Err(Status::internal(format!(
                                            "refetching server health failed: {error}"
                                        )));
                                    }
                                };
                                for record in &rows {
                                    if record.report_time <= last {
                                        continue;
                                    }
                                    last = record.report_time;
                                    let event = pb::ServerHealthEvent {
                                        event: Some(pb::server_health_event::Event::Record(
                                            server_health_record_to_proto(record),
                                        )),
                                    };
                                    if tx.send(Ok(event)).await.is_err() {
                                        return Ok(());
                                    }
                                }
                            }
                            Err(tokio::sync::broadcast::error::RecvError::Closed) => {
                                return Ok(());
                            }
                        },
                    }
                }
            }
            .await;
            if let Err(status) = ended {
                let _ = tx.send(Err(status)).await;
            }
        });
        Ok(Response::new(ReceiverStream::new(rx)))
    }

    type WatchNodeHealthStream = ReceiverStream<Result<pb::NodeHealthEvent, Status>>;

    /// Node records, newest-first in the snapshot and ascending afterwards.
    async fn watch_node_health(
        &self,
        request: Request<pb::WatchNodeHealthRequest>,
    ) -> Result<Response<Self::WatchNodeHealthStream>, Status> {
        let actor = auth::rpc::middleware::from_request(&request)?;
        let session = session_id(&request)?;
        let input = request.into_inner();
        let node = ids::node_id(&input.node_id);
        let node_key = node.to_string();
        let watch = self
            .live
            .process(live::WatchNodeHealth {
                actor,
                node: node.clone(),
                limit: if input.limit == 0 {
                    DEFAULT_WATCH_NODE_HISTORY
                } else {
                    i64::from(input.limit)
                },
            })
            .await?;
        let (tx, rx) = mpsc::channel(STREAM_CAPACITY);
        let mut ticker = StreamTicker::new(
            self.sessions.clone(),
            session,
            self.live.config.stream_keepalive(),
        );
        let db = self.live.db.clone();
        tokio::spawn(async move {
            let mut events = watch.events;
            // The history comes back newest first, so the cursor is its head.
            // `(report_time, id)` rather than the timestamp alone: one event
            // writes a batch of rows that share a timestamp, and the recovery
            // read pages on this exact tuple.
            let mut cursor: (DateTime<Utc>, Option<String>) = watch
                .records
                .first()
                .map(|record| (record.report_time, Some(record.id.to_string())))
                .unwrap_or((DateTime::UNIX_EPOCH, None));
            let snapshot = pb::NodeHealthEvent {
                event: Some(pb::node_health_event::Event::Snapshot(
                    pb::NodeHealthSnapshot {
                        status: watch
                            .records
                            .first()
                            .map(|record| node_health_to_proto(record.status))
                            .unwrap_or_else(|| pb::NodeHealthStatus::Unspecified.into()),
                        records: watch
                            .records
                            .iter()
                            .map(node_health_record_to_proto)
                            .collect(),
                    },
                )),
            };
            let ended: Result<(), Status> = async {
                if tx.send(Ok(snapshot)).await.is_err() {
                    return Ok(());
                }
                loop {
                    tokio::select! {
                        _ = tx.closed() => return Ok(()),
                        result = ticker.tick() => {
                            result?;
                            let event = pb::NodeHealthEvent {
                                event: Some(pb::node_health_event::Event::KeepAlive(
                                    pb::KeepAlive {},
                                )),
                            };
                            if tx.send(Ok(event)).await.is_err() {
                                return Ok(());
                            }
                        }
                        received = events.recv() => match received {
                            Ok(LiveEvent::Message(message)) => {
                                let LiveMessage::NodeHealth { records } = &*message else {
                                    continue;
                                };
                                // Ascending by the same total order the
                                // recovery read pages on, so a batch that
                                // arrives in any order cannot advance the
                                // cursor past one of its own records.
                                let mut batch: Vec<&NodeHealthLive> =
                                    records.iter().filter(|r| r.node == node_key).collect();
                                batch.sort_by(|a, b| {
                                    (a.report_time_unix_micros, &a.id)
                                        .cmp(&(b.report_time_unix_micros, &b.id))
                                });
                                for record in batch {
                                    let time = live_time(record.report_time_unix_micros);
                                    // The whole tuple: one event writes a batch
                                    // sharing a timestamp, and comparing the
                                    // time alone would drop all but the first.
                                    if (time, Some(&record.id)) <= (cursor.0, cursor.1.as_ref()) {
                                        continue;
                                    }
                                    cursor = (time, Some(record.id.clone()));
                                    let event = pb::NodeHealthEvent {
                                        event: Some(pb::node_health_event::Event::Record(
                                            node_health_live_to_proto(record),
                                        )),
                                    };
                                    if tx.send(Ok(event)).await.is_err() {
                                        return Ok(());
                                    }
                                }
                            }
                            // The bus may have skipped records. Page forward
                            // from the cursor until a short page proves there is
                            // nothing left: a gap wider than one page must not
                            // leave the older rows behind, which is exactly what
                            // a newest-first capped read would do.
                            Ok(LiveEvent::Resync)
                            | Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {
                                loop {
                                    let rows = match refetch_node_health(
                                        &db,
                                        &node,
                                        cursor.0,
                                        cursor.1.as_deref(),
                                    )
                                    .await
                                    {
                                        Ok(rows) => rows,
                                        // Retried already; the signal that got
                                        // us here is consumed, so carrying on
                                        // would leave the gap open until the
                                        // next reconnect. End the stream and let
                                        // the client come back instead.
                                        Err(error) => {
                                            return Err(Status::internal(format!(
                                                "refetching node health failed: {error}"
                                            )));
                                        }
                                    };
                                    let short = rows.len() < NODE_RECOVERY_PAGE as usize;
                                    for record in &rows {
                                        cursor =
                                            (record.report_time, Some(record.id.to_string()));
                                        let event = pb::NodeHealthEvent {
                                            event: Some(pb::node_health_event::Event::Record(
                                                node_health_record_to_proto(record),
                                            )),
                                        };
                                        if tx.send(Ok(event)).await.is_err() {
                                            return Ok(());
                                        }
                                    }
                                    if short {
                                        break;
                                    }
                                }
                            }
                            Err(tokio::sync::broadcast::error::RecvError::Closed) => {
                                return Ok(());
                            }
                        },
                    }
                }
            }
            .await;
            if let Err(status) = ended {
                let _ = tx.send(Err(status)).await;
            }
        });
        Ok(Response::new(ReceiverStream::new(rx)))
    }
}
