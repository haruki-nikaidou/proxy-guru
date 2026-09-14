//! The operator-facing `Orchestration` gRPC service.
//!
//! Handlers are thin: decode ids and specs, call a service, encode the reply. All
//! rules live in `services`.

use crate::entities::surreal::canvas::{CanvasEntity, CanvasTree, CanvasUiPosition};
use crate::entities::surreal::certificate::{CertificateEntity, CertificateStatus};
use crate::entities::surreal::connection::EdgeConnectionEntity;
use crate::entities::surreal::dns::DnsProvider;
use crate::entities::surreal::health::{
    NodeHealthRecordEntity, NodeHealthStatus, ServerHealthRecordEntity, ServerHealthStatus,
};
use crate::entities::surreal::node::{
    CanvasExportAs, CanvasExportConfig, CanvasImportConfig, EntryConfig, ExitConfig,
    LoadBalanceAggregateConfig, LoadBalanceDistributeConfig, LoadBalanceMode, NodeEntity, NodeSpec,
    NodeWithPorts, PodConfig, ProxyProtocolVersion, RelayConfig, RelayProtocol, TlsConfig,
};
use crate::entities::surreal::port::{PortDirection, PortEntity, PortKind};
use crate::entities::surreal::server::{ServerIpRecordEntity, ServerIpv6Resolve, ServerWithIp};
use crate::entities::surreal::view::{ConfigSnapshot, ListenProtocol, ListenerCap};
use crate::services::acme::{self, AcmeService};
use crate::services::canvas::{self, CanvasService};
use crate::services::dns::{self, DnsProviderService, DnsProviderSummary};
use crate::services::edge::{self, EdgeService};
use crate::services::health::{self, HealthService};
use crate::services::node::{self, NodeService};
use crate::services::rollout::{self, RolloutService};
use crate::services::server::{self, ServerService};
use crate::services::topology::{ProblemKind, ProblemSeverity, TopologyProblem};
use crate::utils::ids;
use chrono::{DateTime, Utc};
use kanau::processor::Processor;
use rpguru_sdk::orchestration as pb;
use tonic::{Request, Response, Status};

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
        id: ids::record_key(&canvas.id.0),
        name: canvas.name.clone(),
        description: canvas.description.clone(),
    }
}

fn ip_to_proto(ip: &ServerIpRecordEntity) -> pb::ServerIp {
    pb::ServerIp {
        id: ids::record_key(&ip.id.0),
        server_id: ids::record_key(&ip.server.0),
        ip: ip.ip.clone(),
        country: ip.country.clone(),
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
        id: ids::record_key(&record.id.0),
        server_id: ids::record_key(&record.server.0),
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
        id: ids::record_key(&record.id.0),
        node_id: ids::record_key(&record.node.0),
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
        id: ids::record_key(&provider.id.0),
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
        id: ids::record_key(&certificate.id.0),
        sni: certificate.sni.clone(),
        dns_provider_id: ids::record_key(&certificate.dns_provider.0),
        domain_id: certificate.domain_id.clone(),
        acme_directory: certificate.acme_directory.clone(),
        status: certificate_status_to_proto(certificate.status),
        not_before: time(certificate.not_before),
        not_after: time(certificate.not_after),
        last_error: certificate.last_error.clone().unwrap_or_default(),
        last_attempt_at: time(certificate.last_attempt_at),
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

fn server_to_proto(server: &ServerWithIp) -> pb::Server {
    pb::Server {
        id: ids::record_key(&server.server.id.0),
        canvas_id: ids::record_key(&server.server.canvas.0),
        name: server.server.name.clone(),
        icon: server.server.icon.clone(),
        comment: server.server.comment.clone(),
        position: Some(position_to_proto(server.server.position)),
        ipv6_resolve: ipv6_to_proto(server.server.ipv6_resolve),
        log_level: server.server.log_level.clone(),
        last_seen_at: server
            .server
            .last_seen_at
            .map(|t| t.to_rfc3339())
            .unwrap_or_default(),
        health_status: server_health_to_proto(server.server.health_status),
        ips: server.ips.iter().map(ip_to_proto).collect(),
    }
}

fn port_to_proto(port: &PortEntity) -> pb::Port {
    pb::Port {
        id: ids::record_key(&port.id.0),
        node_id: ids::record_key(&port.owner.0),
        kind: match port.kind {
            PortKind::DeriveListen => pb::PortKind::DeriveListen,
            PortKind::DeriveDestination => pb::PortKind::DeriveDestination,
        }
        .into(),
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
            ip_record_id: ids::record_key(&cfg.ip.0),
            port: u32::from(cfg.port),
        }),
        NodeSpec::Entry(cfg) => Spec::Entry(pb::EntryConfig {
            receive_proxy_protocol: proxy_to_proto(cfg.receive_proxy_protocol),
            tls: cfg.tls.as_ref().map(|tls| pb::TlsConfig {
                sni: tls.sni.clone(),
                dns_provider_id: ids::record_key(&tls.dns_provider.0),
                domain_id: tls.domain_id.clone(),
                acme_directory: tls.acme_directory.clone(),
            }),
        }),
        NodeSpec::Relay(cfg) => Spec::Relay(pb::RelayConfig {
            protocol: match cfg.protocol {
                RelayProtocol::TcpRaw => pb::RelayProtocol::RelayTcpRaw,
                RelayProtocol::TcpTls => pb::RelayProtocol::RelayTcpTls,
                RelayProtocol::Quic => pb::RelayProtocol::RelayQuic,
            }
            .into(),
            override_ip_address: cfg.override_ip_address.clone().unwrap_or_default(),
            override_port: cfg.override_port.map(u32::from).unwrap_or_default(),
        }),
        NodeSpec::Exit(cfg) => Spec::Exit(pb::ExitConfig {
            destination: cfg.destination.clone(),
            pass_proxy_protocol: proxy_to_proto(cfg.pass_proxy_protocol),
        }),
        NodeSpec::LoadBalanceDistribute(cfg) => {
            Spec::LoadBalanceDistribute(pb::LoadBalanceDistributeConfig {
                mode: match cfg.mode {
                    LoadBalanceMode::RoundRobin => pb::LoadBalanceMode::RoundRobin,
                    LoadBalanceMode::Random => pb::LoadBalanceMode::Random,
                    LoadBalanceMode::IpHash => pb::LoadBalanceMode::IpHash,
                    LoadBalanceMode::Fallback => pb::LoadBalanceMode::Fallback,
                }
                .into(),
            })
        }
        NodeSpec::LoadBalanceAggregate(_) => {
            Spec::LoadBalanceAggregate(pb::LoadBalanceAggregateConfig {})
        }
        NodeSpec::CanvasImport(cfg) => Spec::CanvasImport(pb::CanvasImportConfig {
            canvas_id: ids::record_key(&cfg.canvas.0),
        }),
        NodeSpec::CanvasExport(cfg) => Spec::CanvasExport(pb::CanvasExportConfig {
            kind: match cfg.kind {
                PortKind::DeriveListen => pb::PortKind::DeriveListen,
                PortKind::DeriveDestination => pb::PortKind::DeriveDestination,
            }
            .into(),
            direction: match cfg.direction {
                CanvasExportAs::InputIntoCanvas => pb::CanvasExportAs::InputIntoCanvas,
                CanvasExportAs::OutputOutOfCanvas => pb::CanvasExportAs::OutputOutOfCanvas,
            }
            .into(),
        }),
    };
    pb::NodeSpec { spec: Some(spec) }
}

fn spec_from_proto(spec: Option<pb::NodeSpec>) -> Result<NodeSpec, Status> {
    use pb::node_spec::Spec;
    let spec = spec
        .and_then(|s| s.spec)
        .ok_or_else(|| Status::invalid_argument("spec is required"))?;
    Ok(match spec {
        Spec::Pod(cfg) => NodeSpec::Pod(PodConfig {
            ip: ids::server_ip_id(&cfg.ip_record_id),
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
            protocol: match pb::RelayProtocol::try_from(cfg.protocol) {
                Ok(pb::RelayProtocol::RelayTcpRaw) => RelayProtocol::TcpRaw,
                Ok(pb::RelayProtocol::RelayTcpTls) => RelayProtocol::TcpTls,
                Ok(pb::RelayProtocol::RelayQuic) => RelayProtocol::Quic,
                Ok(pb::RelayProtocol::Unspecified) | Err(_) => {
                    return Err(Status::invalid_argument(format!(
                        "protocol: unknown relay protocol {}",
                        cfg.protocol
                    )));
                }
            },
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
                mode: match pb::LoadBalanceMode::try_from(cfg.mode) {
                    Ok(pb::LoadBalanceMode::RoundRobin) => LoadBalanceMode::RoundRobin,
                    Ok(pb::LoadBalanceMode::Random) => LoadBalanceMode::Random,
                    Ok(pb::LoadBalanceMode::IpHash) => LoadBalanceMode::IpHash,
                    Ok(pb::LoadBalanceMode::Fallback) => LoadBalanceMode::Fallback,
                    Ok(pb::LoadBalanceMode::Unspecified) | Err(_) => {
                        return Err(Status::invalid_argument(format!(
                            "mode: unknown load balance mode {}",
                            cfg.mode
                        )));
                    }
                },
            })
        }
        Spec::LoadBalanceAggregate(_) => {
            NodeSpec::LoadBalanceAggregate(LoadBalanceAggregateConfig {})
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
                Ok(pb::PortKind::Unspecified) | Err(_) => {
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
            let key = ids::record_key(&cfg.canvas.0);
            import_targets
                .iter()
                .find(|c| ids::record_key(&c.id.0) == key)
                .map(canvas_to_proto)
        }
        _ => None,
    };
    pb::Node {
        id: ids::record_key(&node.id.0),
        canvas_id: ids::record_key(&node.canvas.0),
        name: node.name.clone(),
        comment: node.comment.clone(),
        spec: Some(spec_to_proto(&node.spec)),
        position: Some(position_to_proto(node.position)),
        ports: ports.iter().map(port_to_proto).collect(),
        import_target,
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
        id: ids::record_key(&edge.id.0),
        source_port_id: ids::record_key(&edge.source.0),
        target_port_id: ids::record_key(&edge.target.0),
    }
}

fn listener_cap_to_proto(cap: &ListenerCap) -> pb::ListenerCap {
    pb::ListenerCap {
        ip: cap.ip.clone(),
        port: u32::try_from(cap.port).unwrap_or_default(),
        protocol: match cap.protocol {
            ListenProtocol::Raw => "raw",
            ListenProtocol::RelayTcp => "relay_tcp",
            ListenProtocol::RelayTls => "relay_tls",
            ListenProtocol::RelayQuic => "relay_quic",
        }
        .to_string(),
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
            ProblemKind::PodIpForeign => pb::ProblemKind::PodIpForeign,
            ProblemKind::ExitDestinationInvalid => pb::ProblemKind::ExitDestinationInvalid,
            ProblemKind::IpHashWithoutClientIp => pb::ProblemKind::IpHashWithoutClientIp,
            ProblemKind::CanvasImportSelf => pb::ProblemKind::CanvasImportSelf,
            ProblemKind::CanvasImportAncestor => pb::ProblemKind::CanvasImportAncestor,
            ProblemKind::CanvasImportDuplicate => pb::ProblemKind::CanvasImportDuplicate,
            ProblemKind::CanvasImportUnresolved => pb::ProblemKind::CanvasImportUnresolved,
            ProblemKind::PodPortUnconnected => pb::ProblemKind::PodPortUnconnected,
            ProblemKind::RelaySameServer => pb::ProblemKind::RelaySameServer,
            ProblemKind::DistributeSingleMember => pb::ProblemKind::DistributeSingleMember,
        }
        .into(),
        message: problem.message.clone(),
        node_ids: problem
            .nodes
            .iter()
            .map(|n| ids::record_key(&n.0))
            .collect(),
        edge_ids: problem
            .edges
            .iter()
            .map(|e| ids::record_key(&e.0))
            .collect(),
        port_ids: problem
            .ports
            .iter()
            .map(|p| ids::record_key(&p.0))
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
        Ok(Response::new(pb::GetCanvasReply {
            canvas: Some(canvas_to_proto(&contents.canvas)),
            servers: contents.servers.iter().map(server_to_proto).collect(),
            nodes: contents
                .nodes
                .iter()
                .map(|n| node_to_proto(n, &contents.import_targets))
                .collect(),
            edges: contents.edges.iter().map(edge_to_proto).collect(),
            ancestors: contents.ancestors.iter().map(canvas_to_proto).collect(),
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
            })
            .await?;
        Ok(Response::new(pb::CreateServerReply {
            server: Some(server_to_proto(&ServerWithIp {
                server,
                ips: Vec::new(),
            })),
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
            })
            .await?;
        Ok(Response::new(pb::UpdateServerReply {
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

    async fn add_server_ip(
        &self,
        request: Request<pb::AddServerIpRequest>,
    ) -> Result<Response<pb::AddServerIpReply>, Status> {
        let actor = auth::rpc::middleware::from_request(&request)?;
        let input = request.into_inner();
        let ip = self
            .servers
            .process(server::AddServerIp {
                actor,
                server: ids::server_id(&input.server_id),
                ip: input.ip,
                country: input.country,
            })
            .await?;
        Ok(Response::new(pb::AddServerIpReply {
            ip: Some(ip_to_proto(&ip)),
        }))
    }

    async fn remove_server_ip(
        &self,
        request: Request<pb::RemoveServerIpRequest>,
    ) -> Result<Response<pb::RemoveServerIpReply>, Status> {
        let actor = auth::rpc::middleware::from_request(&request)?;
        let input = request.into_inner();
        self.servers
            .process(server::RemoveServerIp {
                actor,
                ip_record: ids::server_ip_id(&input.ip_record_id),
            })
            .await?;
        Ok(Response::new(pb::RemoveServerIpReply {}))
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
        let edge = self
            .edges
            .process(edge::Connect {
                actor,
                output_port: ids::port_id(&input.output_port_id),
                input_port: ids::port_id(&input.input_port_id),
            })
            .await?;
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
        Ok(Response::new(pb::GetServerRolloutStatusReply {
            desired: status.desired.as_ref().map(snapshot_to_proto),
            in_flight: status.in_flight.as_ref().map(snapshot_to_proto),
            applied: status.applied.as_ref().map(snapshot_to_proto),
            apply_error: status.apply_error.unwrap_or_default(),
            derive_error: status.derive_error.unwrap_or_default(),
            waiting_for_server_ids: status
                .waiting_for
                .iter()
                .map(|id| ids::record_key(&id.0))
                .collect(),
            invalid_pods: status
                .invalid_pods
                .into_iter()
                .map(|pod| pb::InvalidPod {
                    node_id: ids::record_key(&pod.node.0),
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
        }))
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
}
