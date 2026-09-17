//! The operator-facing `Orchestration` gRPC service.
//!
//! Handlers are thin: decode ids and specs, call a service, encode the reply. All
//! rules live in `services`.

use crate::entities::db::canvas::{CanvasEntity, CanvasTree, CanvasUiPosition};
use crate::entities::db::certificate::{CertificateEntity, CertificateStatus};
use crate::entities::db::dns::DnsProvider;
use crate::entities::db::edge::{EdgeEntity, EdgeTarget};
use crate::entities::db::exit::ExitEntity;
use crate::entities::db::group::{GroupEntity, GroupMember};
use crate::entities::db::health::{
    ListPodHealthSince, ListServerHealthHistory as ListServerHealthHistoryRows,
    PodHealthRecordEntity, PodHealthStatus, ServerHealthRecordEntity, ServerHealthStatus,
};
use crate::entities::db::pod::{PodEntity, PodIngress, ProxyProtocolVersion, TlsConfig};
use crate::entities::db::server::{
    AddressSource, QuicCongestion, ServerEntity, ServerIpv6Resolve, ServerLogLevel, ServerQuic,
};
use crate::entities::db::view::{ConfigSnapshot, ListenerCap};
use crate::events::live::{LiveMessage, PodHealthLive, ServerHealthLive, live_time};
use crate::hooks::live::LiveEvent;
use crate::services::OrchestrationError;
use crate::services::acme::{self, AcmeService};
use crate::services::canvas::{self, CanvasService};
use crate::services::config::{self, OrchestrationConfigService};
use crate::services::dns::{self, DnsProviderService, DnsProviderSummary};
use crate::services::graph::{self, GraphDiagnostic, GraphService, GraphSubject};
use crate::services::health::{self, HealthService};
use crate::services::live::{self, LiveService, ViewValue};
use crate::services::rollout::{self, RolloutService, RolloutStatus};
use crate::services::server::{self, ServerService};
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
/// Small on purpose. A rollout snapshot is a whole picture, so a
/// client that cannot keep up wants the newest one, not a backlog: the shared
/// view coalesces while this channel is full, and the stream then sends one
/// up-to-date snapshot instead of four stale ones.
const STREAM_CAPACITY: usize = 4;

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

#[derive(Clone)]
pub struct OrchestrationGrpc {
    pub canvases: CanvasService,
    pub servers: ServerService,
    pub graph: GraphService,
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
        parent_id: canvas
            .parent
            .as_ref()
            .map(|parent| parent.to_string())
            .unwrap_or_default(),
        position: Some(position_to_proto(canvas.position)),
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

fn pod_health_to_proto(value: PodHealthStatus) -> i32 {
    match value {
        PodHealthStatus::Ready => pb::PodHealthStatus::PodReady,
        PodHealthStatus::Deploying => pb::PodHealthStatus::PodDeploying,
        PodHealthStatus::Failed => pb::PodHealthStatus::PodFailed,
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

fn pod_health_record_to_proto(record: &PodHealthRecordEntity) -> pb::PodHealthRecord {
    pb::PodHealthRecord {
        id: record.id.to_string(),
        pod_id: record.pod.to_string(),
        status: pod_health_to_proto(record.status),
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
                pod_id: pod.pod.to_string(),
                pod_name: pod.name,
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

fn pod_health_live_to_proto(record: &PodHealthLive) -> pb::PodHealthRecord {
    pb::PodHealthRecord {
        id: record.id.clone(),
        pod_id: record.pod.clone(),
        status: pod_health_to_proto(record.status),
        message: record.message.clone(),
        report_time: live_time(record.report_time_unix_micros).to_rfc3339(),
    }
}

/// A tree with its diagnostics as `GetGraph` and `WatchGraph` both answer it.
fn graph_reply(view: &graph::GraphView) -> pb::GetGraphReply {
    let rows = &view.rows;
    pb::GetGraphReply {
        canvases: rows.canvases.iter().map(canvas_to_proto).collect(),
        servers: rows.servers.iter().map(server_to_proto).collect(),
        pods: rows.pods.iter().map(pod_to_proto).collect(),
        exits: rows.exits.iter().map(exit_to_proto).collect(),
        edges: rows.edges.iter().map(edge_to_proto).collect(),
        groups: rows.groups.iter().map(group_to_proto).collect(),
        diagnostics: view.diagnostics.iter().map(diagnostic_to_proto).collect(),
        generation: rows.generation(),
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

/// One of the five levels a server logs at. Surrounding blanks and case are
/// forgiven, since `tracing` itself reads `INFO` as `info`; anything else is not a
/// level.
fn log_level_from_proto(value: &str) -> Result<ServerLogLevel, Status> {
    value.trim().to_ascii_lowercase().parse().map_err(|_| {
        Status::invalid_argument(format!(
            "log_level: unknown level {value:?}; expected one of trace, debug, info, warn, error"
        ))
    })
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

fn quic_to_proto(quic: &ServerQuic) -> pb::QuicSettings {
    pb::QuicSettings {
        congestion: match quic.congestion {
            QuicCongestion::Cubic => pb::QuicCongestion::QuicCubic,
            QuicCongestion::Brutal => pb::QuicCongestion::QuicBrutal,
        } as i32,
        up_mbps: quic.up_mbps,
        down_mbps: quic.down_mbps,
        stream_receive_window: quic.stream_receive_window,
        conn_receive_window: quic.conn_receive_window,
    }
}

/// An absent message is the default; an unknown congestion value is refused.
fn quic_from_proto(quic: Option<pb::QuicSettings>) -> Result<ServerQuic, Status> {
    let Some(quic) = quic else {
        return Ok(ServerQuic::default());
    };
    let congestion = match pb::QuicCongestion::try_from(quic.congestion) {
        Ok(pb::QuicCongestion::QuicCubic | pb::QuicCongestion::Unspecified) => {
            QuicCongestion::Cubic
        }
        Ok(pb::QuicCongestion::QuicBrutal) => QuicCongestion::Brutal,
        Err(_) => {
            return Err(Status::invalid_argument(format!(
                "quic.congestion: unknown value {}",
                quic.congestion
            )));
        }
    };
    Ok(ServerQuic {
        congestion,
        up_mbps: quic.up_mbps,
        down_mbps: quic.down_mbps,
        stream_receive_window: quic.stream_receive_window,
        conn_receive_window: quic.conn_receive_window,
    })
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
        log_level: server.log_level.to_string(),
        quic: Some(quic_to_proto(&server.quic)),
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
        capabilities: server.capabilities.clone(),
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
        country: server.country_of_v4().unwrap_or_default().to_owned(),
        effective_source: match effective.map(|(_, source)| source) {
            None => pb::AddressSource::Unspecified,
            Some(AddressSource::Override) => pb::AddressSource::AddressOverride,
            Some(AddressSource::Reported) => pb::AddressSource::AddressReported,
            Some(AddressSource::Observed) => pb::AddressSource::AddressObserved,
        }
        .into(),
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

fn tree_to_proto(tree: &CanvasTree) -> pb::CanvasTreeNode {
    pb::CanvasTreeNode {
        canvas: Some(canvas_to_proto(&tree.canvas)),
        children: tree.children.iter().map(tree_to_proto).collect(),
    }
}

// --- the graph ---------------------------------------------------------------

fn required(field: &str, value: String) -> Result<String, Status> {
    let value = value.trim().to_string();
    if value.is_empty() {
        return Err(Status::invalid_argument(format!("{field} is required")));
    }
    Ok(value)
}

/// An optional address on the wire: empty is unset, an IP literal is stored in
/// its canonical form, anything else as typed (the graph check names it).
fn optional_address(raw: String) -> Option<String> {
    let raw = raw.trim();
    if raw.is_empty() {
        return None;
    }
    Some(
        raw.parse::<std::net::IpAddr>()
            .map_or_else(|_| raw.to_string(), |ip| ip.to_string()),
    )
}

fn tls_to_proto(tls: &TlsConfig) -> pb::TlsConfig {
    pb::TlsConfig {
        sni: tls.sni.clone(),
        dns_provider_id: tls.dns_provider.to_string(),
        domain_id: tls.domain_id.clone(),
        acme_directory: tls.acme_directory.clone(),
    }
}

fn pod_to_proto(pod: &PodEntity) -> pb::Pod {
    let ingress = match &pod.ingress {
        PodIngress::ClientRaw { .. } => pb::Ingress::ClientRaw,
        PodIngress::ClientTls { .. } => pb::Ingress::ClientTls,
        PodIngress::RelayTcp => pb::Ingress::RelayTcp,
        PodIngress::RelayTls => pb::Ingress::RelayTls,
        PodIngress::RelayQuic => pb::Ingress::RelayQuic,
    };
    pb::Pod {
        id: pod.id.to_string(),
        canvas_id: pod.canvas.to_string(),
        server_id: pod.server.to_string(),
        name: pod.name.clone(),
        comment: pod.comment.clone(),
        port: u32::from(pod.port),
        bind_ip: pod.bind_ip.clone().unwrap_or_default(),
        advertise_ip: pod.advertise_ip.clone().unwrap_or_default(),
        ingress: ingress.into(),
        receive_proxy_protocol: proxy_to_proto(pod.ingress.receive_proxy_protocol()),
        tls: pod.ingress.tls().map(tls_to_proto),
        route_json: pod
            .route
            .as_ref()
            .and_then(|route| serde_json::to_string(route).ok())
            .unwrap_or_default(),
    }
}

fn pod_from_proto(pod: pb::Pod) -> Result<PodEntity, Status> {
    let proxy = proxy_from_proto("receive_proxy_protocol", pod.receive_proxy_protocol)?;
    let ingress = match pb::Ingress::try_from(pod.ingress) {
        Ok(pb::Ingress::ClientRaw) => PodIngress::ClientRaw {
            receive_proxy_protocol: proxy,
        },
        Ok(pb::Ingress::ClientTls) => {
            let tls = pod
                .tls
                .ok_or_else(|| Status::invalid_argument("a CLIENT_TLS pod needs tls"))?;
            PodIngress::ClientTls {
                receive_proxy_protocol: proxy,
                tls: TlsConfig {
                    sni: required("tls.sni", tls.sni)?,
                    dns_provider: ids::dns_provider_id(&required(
                        "tls.dns_provider_id",
                        tls.dns_provider_id,
                    )?),
                    domain_id: tls.domain_id.trim().to_string(),
                    acme_directory: tls.acme_directory.trim().to_string(),
                },
            }
        }
        Ok(pb::Ingress::RelayTcp) => PodIngress::RelayTcp,
        Ok(pb::Ingress::RelayTls) => PodIngress::RelayTls,
        Ok(pb::Ingress::RelayQuic) => PodIngress::RelayQuic,
        Ok(pb::Ingress::Unspecified) | Err(_) => {
            return Err(Status::invalid_argument(format!(
                "ingress: unknown value {}",
                pod.ingress
            )));
        }
    };
    let route = match pod.route_json.trim() {
        "" => None,
        json => Some(serde_json::from_str(json).map_err(|error| {
            Status::invalid_argument(format!("pod {}: route_json: {error}", pod.id))
        })?),
    };
    Ok(PodEntity {
        id: ids::pod_id(&required("pod id", pod.id)?),
        canvas: ids::canvas_id(&required("canvas_id", pod.canvas_id)?),
        server: ids::server_id(&required("server_id", pod.server_id)?),
        name: pod.name.trim().to_string(),
        comment: pod.comment,
        port: u16::try_from(pod.port)
            .map_err(|_| Status::invalid_argument(format!("port {} out of range", pod.port)))?,
        bind_ip: optional_address(pod.bind_ip),
        advertise_ip: optional_address(pod.advertise_ip),
        ingress,
        route,
    })
}

fn exit_to_proto(exit: &ExitEntity) -> pb::Exit {
    pb::Exit {
        id: exit.id.to_string(),
        canvas_id: exit.canvas.to_string(),
        name: exit.name.clone(),
        comment: exit.comment.clone(),
        destination: exit.destination.clone(),
        send_proxy_protocol: proxy_to_proto(exit.send_proxy_protocol),
        position: Some(position_to_proto(exit.position)),
    }
}

fn exit_from_proto(exit: pb::Exit) -> Result<ExitEntity, Status> {
    Ok(ExitEntity {
        id: ids::exit_id(&required("exit id", exit.id)?),
        canvas: ids::canvas_id(&required("canvas_id", exit.canvas_id)?),
        name: exit.name.trim().to_string(),
        comment: exit.comment,
        destination: exit.destination.trim().to_string(),
        send_proxy_protocol: proxy_from_proto("send_proxy_protocol", exit.send_proxy_protocol)?,
        position: position_or_origin(exit.position),
    })
}

fn edge_to_proto(edge: &EdgeEntity) -> pb::Edge {
    pb::Edge {
        id: edge.id.to_string(),
        source_pod_id: edge.source.to_string(),
        target: Some(match &edge.target {
            EdgeTarget::Pod(pod) => pb::edge::Target::TargetPodId(pod.to_string()),
            EdgeTarget::Exit(exit) => pb::edge::Target::TargetExitId(exit.to_string()),
        }),
        override_ip: edge.override_ip.clone().unwrap_or_default(),
        override_port: edge.override_port.map(u32::from).unwrap_or_default(),
    }
}

fn edge_from_proto(edge: pb::Edge) -> Result<EdgeEntity, Status> {
    let target = match edge.target {
        Some(pb::edge::Target::TargetPodId(pod)) => {
            EdgeTarget::Pod(ids::pod_id(&required("target_pod_id", pod)?))
        }
        Some(pb::edge::Target::TargetExitId(exit)) => {
            EdgeTarget::Exit(ids::exit_id(&required("target_exit_id", exit)?))
        }
        None => return Err(Status::invalid_argument("an edge needs a target")),
    };
    Ok(EdgeEntity {
        id: ids::edge_id(&required("edge id", edge.id)?),
        source: ids::pod_id(&required("source_pod_id", edge.source_pod_id)?),
        target,
        override_ip: optional_address(edge.override_ip),
        override_port: match edge.override_port {
            0 => None,
            port => Some(u16::try_from(port).map_err(|_| {
                Status::invalid_argument(format!("override_port {port} out of range"))
            })?),
        },
    })
}

fn group_to_proto(group: &GroupEntity) -> pb::Group {
    pb::Group {
        id: group.id.to_string(),
        canvas_id: group.canvas.to_string(),
        kind: group.kind.clone(),
        name: group.name.clone(),
        props_json: group.props.to_string(),
        members: group
            .members
            .iter()
            .map(|member| pb::GroupMember {
                member: Some(match member {
                    GroupMember::Pod(id) => pb::group_member::Member::PodId(id.to_string()),
                    GroupMember::Edge(id) => pb::group_member::Member::EdgeId(id.to_string()),
                    GroupMember::Exit(id) => pb::group_member::Member::ExitId(id.to_string()),
                    GroupMember::Server(id) => pb::group_member::Member::ServerId(id.to_string()),
                }),
            })
            .collect(),
    }
}

fn group_from_proto(group: pb::Group) -> Result<GroupEntity, Status> {
    let props = match group.props_json.trim() {
        "" => serde_json::Value::Object(serde_json::Map::new()),
        json => serde_json::from_str(json).map_err(|error| {
            Status::invalid_argument(format!("group {}: props_json: {error}", group.id))
        })?,
    };
    let members = group
        .members
        .into_iter()
        .map(|member| match member.member {
            Some(pb::group_member::Member::PodId(id)) => Ok(GroupMember::Pod(ids::pod_id(&id))),
            Some(pb::group_member::Member::EdgeId(id)) => Ok(GroupMember::Edge(ids::edge_id(&id))),
            Some(pb::group_member::Member::ExitId(id)) => Ok(GroupMember::Exit(ids::exit_id(&id))),
            Some(pb::group_member::Member::ServerId(id)) => {
                Ok(GroupMember::Server(ids::server_id(&id)))
            }
            None => Err(Status::invalid_argument("a group member names nothing")),
        })
        .collect::<Result<Vec<_>, Status>>()?;
    Ok(GroupEntity {
        id: ids::group_id(&required("group id", group.id)?),
        canvas: ids::canvas_id(&required("canvas_id", group.canvas_id)?),
        kind: required("kind", group.kind)?,
        name: group.name.trim().to_string(),
        props,
        members,
    })
}

fn diagnostic_to_proto(diagnostic: &GraphDiagnostic) -> pb::Diagnostic {
    use pb::graph_subject::Subject;
    pb::Diagnostic {
        problem: diagnostic.problem.clone(),
        error: diagnostic.error,
        subjects: diagnostic
            .subjects
            .iter()
            .map(|subject| pb::GraphSubject {
                subject: Some(match subject {
                    GraphSubject::Server(id) => Subject::ServerId(id.to_string()),
                    GraphSubject::Pod(id) => Subject::PodId(id.to_string()),
                    GraphSubject::Exit(id) => Subject::ExitId(id.to_string()),
                    GraphSubject::Edge(id) => Subject::EdgeId(id.to_string()),
                    GraphSubject::Group(id) => Subject::GroupId(id.to_string()),
                    GraphSubject::Canvas(id) => Subject::CanvasId(id.to_string()),
                }),
            })
            .collect(),
        message: diagnostic.message.clone(),
    }
}

fn change_from_proto(change: Option<pb::GraphChange>) -> Result<graph::GraphChange, Status> {
    let change = change.unwrap_or_default();
    Ok(graph::GraphChange {
        put_pods: change
            .put_pods
            .into_iter()
            .map(pod_from_proto)
            .collect::<Result<_, _>>()?,
        put_exits: change
            .put_exits
            .into_iter()
            .map(exit_from_proto)
            .collect::<Result<_, _>>()?,
        put_edges: change
            .put_edges
            .into_iter()
            .map(edge_from_proto)
            .collect::<Result<_, _>>()?,
        put_groups: change
            .put_groups
            .into_iter()
            .map(group_from_proto)
            .collect::<Result<_, _>>()?,
        delete_pods: change
            .delete_pod_ids
            .iter()
            .map(|id| ids::pod_id(id))
            .collect(),
        delete_exits: change
            .delete_exit_ids
            .iter()
            .map(|id| ids::exit_id(id))
            .collect(),
        delete_edges: change
            .delete_edge_ids
            .iter()
            .map(|id| ids::edge_id(id))
            .collect(),
        delete_groups: change
            .delete_group_ids
            .iter()
            .map(|id| ids::group_id(id))
            .collect(),
    })
}

fn positions_from_proto<T>(
    items: Vec<pb::ItemPosition>,
    id: impl Fn(&str) -> T,
) -> Vec<(T, CanvasUiPosition)> {
    items
        .into_iter()
        .map(|item| (id(&item.id), position_or_origin(item.position)))
        .collect()
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
                parent: (!input.parent_id.is_empty()).then(|| ids::canvas_id(&input.parent_id)),
                position: position_or_origin(input.position),
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
                position: position_from_proto(input.position),
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

    async fn get_graph(
        &self,
        request: Request<pb::GetGraphRequest>,
    ) -> Result<Response<pb::GetGraphReply>, Status> {
        let actor = auth::rpc::middleware::from_request(&request)?;
        let input = request.into_inner();
        let view = self
            .graph
            .process(graph::GetGraph {
                actor,
                canvas: ids::canvas_id(&input.canvas_id),
            })
            .await?;
        Ok(Response::new(graph_reply(&view)))
    }

    async fn apply_graph(
        &self,
        request: Request<pb::ApplyGraphRequest>,
    ) -> Result<Response<pb::ApplyGraphReply>, Status> {
        let actor = auth::rpc::middleware::from_request(&request)?;
        let input = request.into_inner();
        let outcome = self
            .graph
            .process(graph::ApplyGraph {
                actor,
                canvas: ids::canvas_id(&input.canvas_id),
                change: change_from_proto(input.change)?,
                dry_run: input.dry_run,
                expected_generation: (input.expected_generation != 0)
                    .then_some(input.expected_generation),
            })
            .await?;
        Ok(Response::new(pb::ApplyGraphReply {
            applied: outcome.applied,
            generation: outcome.generation,
            diagnostics: outcome
                .diagnostics
                .iter()
                .map(diagnostic_to_proto)
                .collect(),
            pods: outcome.pods.iter().map(pod_to_proto).collect(),
        }))
    }

    async fn move_items(
        &self,
        request: Request<pb::MoveItemsRequest>,
    ) -> Result<Response<pb::MoveItemsReply>, Status> {
        let actor = auth::rpc::middleware::from_request(&request)?;
        let input = request.into_inner();
        self.graph
            .process(graph::MoveItems {
                actor,
                canvas: ids::canvas_id(&input.canvas_id),
                servers: positions_from_proto(input.servers, ids::server_id),
                exits: positions_from_proto(input.exits, ids::exit_id),
                canvases: positions_from_proto(input.canvases, ids::canvas_id),
            })
            .await?;
        Ok(Response::new(pb::MoveItemsReply {}))
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
                log_level: log_level_from_proto(&input.log_level)?,
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
                log_level: log_level_from_proto(&input.log_level)?,
                quic: quic_from_proto(input.quic)?,
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

    async fn list_pod_health_history(
        &self,
        request: Request<pb::ListPodHealthHistoryRequest>,
    ) -> Result<Response<pb::ListPodHealthHistoryReply>, Status> {
        let actor = auth::rpc::middleware::from_request(&request)?;
        let input = request.into_inner();
        let (start, end) = history_window(&input.start, &input.end)?;
        let limit = if input.limit == 0 {
            health::DEFAULT_POD_HISTORY_LIMIT
        } else {
            i64::from(input.limit)
        };
        let records = self
            .health
            .process(health::ListPodHealthHistory {
                actor,
                pod: ids::pod_id(&input.pod_id),
                start,
                end,
                limit,
            })
            .await?;
        Ok(Response::new(pb::ListPodHealthHistoryReply {
            records: records.iter().map(pod_health_record_to_proto).collect(),
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

    type WatchGraphStream = ReceiverStream<Result<pb::GraphEvent, Status>>;

    async fn watch_graph(
        &self,
        request: Request<pb::WatchGraphRequest>,
    ) -> Result<Response<Self::WatchGraphStream>, Status> {
        let actor = auth::rpc::middleware::from_request(&request)?;
        let session = session_id(&request)?;
        let input = request.into_inner();
        let handle = self
            .live
            .process(live::WatchGraph {
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
                            let event = pb::GraphEvent {
                                event: Some(pb::graph_event::Event::KeepAlive(pb::KeepAlive {})),
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
                                    let event = pb::GraphEvent {
                                        event: Some(pb::graph_event::Event::Snapshot(
                                            graph_reply(&state.view),
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
    /// it. A timestamp alone is enough because a server writes at most one
    /// `server_health_record` per event: two rows for one server can only share
    /// a `report_time` if two writes landed in the same microsecond, which
    /// nothing in the fleet does.
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

    type WatchPodHealthStream = ReceiverStream<Result<pb::PodHealthEvent, Status>>;

    /// The pod counterpart of `watch_server_health`: a record log with the same
    /// `last` watermark. A bus batch carries the rows of every pod its event
    /// touched; only this pod's are forwarded.
    ///
    /// An unknown pod is `NOT_FOUND` *on the stream*, as the view streams
    /// deliver it: a client reads its Watch* streams the same way whether the
    /// record was missing at open or vanished later.
    async fn watch_pod_health(
        &self,
        request: Request<pb::WatchPodHealthRequest>,
    ) -> Result<Response<Self::WatchPodHealthStream>, Status> {
        let actor = auth::rpc::middleware::from_request(&request)?;
        let session = session_id(&request)?;
        let input = request.into_inner();
        let (since, _) = history_window(&input.since, "")?;
        let pod = ids::pod_id(&input.pod_id);
        let pod_key = pod.to_string();
        let watch = match self
            .live
            .process(live::WatchPodHealth {
                actor,
                pod: pod.clone(),
                since,
            })
            .await
        {
            Ok(watch) => watch,
            Err(OrchestrationError::NotFound) => {
                let (tx, rx) = mpsc::channel(1);
                let _ = tx.try_send(Err(Status::not_found("pod not found")));
                return Ok(Response::new(ReceiverStream::new(rx)));
            }
            Err(error) => return Err(error.into()),
        };
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
            let snapshot = pb::PodHealthEvent {
                event: Some(pb::pod_health_event::Event::Snapshot(
                    pb::PodHealthSnapshot {
                        records: watch
                            .records
                            .iter()
                            .map(pod_health_record_to_proto)
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
                            let event = pb::PodHealthEvent {
                                event: Some(pb::pod_health_event::Event::KeepAlive(
                                    pb::KeepAlive {},
                                )),
                            };
                            if tx.send(Ok(event)).await.is_err() {
                                return Ok(());
                            }
                        }
                        received = events.recv() => match received {
                            Ok(LiveEvent::Message(message)) => {
                                let LiveMessage::PodHealth { records } = &*message else {
                                    continue;
                                };
                                for record in records {
                                    if record.pod != pod_key {
                                        continue;
                                    }
                                    let time = live_time(record.report_time_unix_micros);
                                    if time <= last {
                                        continue;
                                    }
                                    last = time;
                                    let event = pb::PodHealthEvent {
                                        event: Some(pb::pod_health_event::Event::Record(
                                            pod_health_live_to_proto(record),
                                        )),
                                    };
                                    if tx.send(Ok(event)).await.is_err() {
                                        return Ok(());
                                    }
                                }
                            }
                            // The bus may have skipped records; the database has
                            // them all, so read forward from the watermark.
                            Ok(LiveEvent::Resync)
                            | Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {
                                let rows = match retry_read(|| {
                                    db.process(ListPodHealthSince {
                                        pod: pod.clone(),
                                        start: last,
                                        end: Utc::now(),
                                        limit: None,
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
                                            "refetching pod health failed: {error}"
                                        )));
                                    }
                                };
                                for record in &rows {
                                    if record.report_time <= last {
                                        continue;
                                    }
                                    last = record.report_time;
                                    let event = pb::PodHealthEvent {
                                        event: Some(pb::pod_health_event::Event::Record(
                                            pod_health_record_to_proto(record),
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
}
