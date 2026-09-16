//! Config derivation: one server's ideal `guru-worker` config from a canvas tree.
//!
//! Derivation is a pure function of a [`CanvasTopology`] snapshot, so the same input
//! always produces byte-identical output — that is what lets a derivation pass skip
//! servers whose config did not actually change. What a server may *safely* run
//! right now is decided afterwards, in [`crate::services::converge`].
//!
//! The snapshot is a whole canvas tree and every walk goes through
//! [`Index::peer`], which resolves import/export boundaries, so a nested graph
//! derives exactly the TOML its flattened equivalent would.
//!
//! Failure is per pod, not per server: each pod's listen and destination walks are
//! caught at the pod, so one malformed chain costs exactly its own forwarding and
//! every healthy pod on the server still rolls out. Only a cross-pod failure — two
//! pods claiming one socket — can fail the whole server.

use crate::config::OrchestrationConfig;
use crate::entities::db::ca::{RelayCertificateEntity, relay_sni};
use crate::entities::db::certificate::{CertificateEntity, CertificateStatus};
use crate::entities::db::node::{
    NodeId, NodeSpec, NodeWithPorts, PodConfig, RelayProtocol as EntityRelayProtocol,
};
use crate::entities::db::port::PortEntity;
use crate::entities::db::server::{ServerEntity, ServerId, ServerQuic};
use crate::entities::db::topology::CanvasTopology;
use crate::entities::db::view::{
    CertificateKind, CertificateRef, ForwardingDeps, InvalidPod, ListenProtocol, ListenerCap,
};
use crate::services::ca::{CA_FILE, acme_cert_paths, relay_cert_paths};
use crate::services::topology::Index;
use guru_worker_config::{
    Config, Forwarding, ForwardingTo, KeepAlive, ListenAs, LoadBalanceGroup, LogConfig, QuicTuning,
    RelayHost, RelayProtocol, Remote, TcpProxyProtocol, TlsHostConfig,
};
use std::net::{IpAddr, Ipv6Addr, SocketAddr};
use std::path::PathBuf;

#[derive(Debug, thiserror::Error)]
pub enum DeriveError {
    #[error("certificate for {sni} is {state}")]
    CertificateNotIssued { sni: String, state: String },
    #[error("relay {node}: internal CA not initialised (run `manage-tool orchestration init-ca`)")]
    RelayCaMissing { node: String },
    #[error("relay {node}: relay certificate not issued yet")]
    RelayCertificateMissing { node: String },
    #[error("node {node} cannot terminate a destination path")]
    UnsupportedSpec { node: String },
    #[error("pod {node}: the import/export chain on one of its ports is not connected through")]
    DanglingBoundary { node: String },
    #[error("pod {node} bind address '{value}' is not an IP address")]
    InvalidBindIp { node: String, value: String },
    #[error("pod {node} advertise address '{value}' is not an IP address")]
    InvalidAdvertiseIp { node: String, value: String },
    #[error("server {server} has no address yet: wait for its worker to register, or pin one")]
    ServerNoAddress { server: String },
    #[error("exit {node} destination '{destination}' is not host:port")]
    InvalidDestination { node: String, destination: String },
    #[error("relay {node} listen input is not fed by a pod")]
    RelayWithoutPod { node: String },
    #[error("cycle through node {node}")]
    Cycle { node: String },
    #[error("server {server} is not part of this canvas")]
    UnknownServer { server: String },
    #[error("derived config is invalid: {0}")]
    Invalid(#[from] guru_worker_config::ConfigError),
}

/// The certificate state a derivation pass runs against.
///
/// Loaded once per pass for the whole tree: every ACME row of every SNI an Entry
/// asks for (all directories; the pair is resolved here), the relay leaf of
/// every relay pod, and whether the internal CA exists at all.
#[derive(Debug, Clone, Default)]
pub struct DerivationCertificates {
    pub acme: Vec<CertificateEntity>,
    pub relay: Vec<RelayCertificateEntity>,
    pub ca_present: bool,
    /// Projection mode for edit-time topology checks, where only the listener
    /// shapes matter and the TOML is discarded: a missing certificate or CA does
    /// not invalidate a pod, placeholder paths are emitted and nothing is pinned.
    /// Never used for a published revision.
    pub assume_issued: bool,
}

impl DerivationCertificates {
    /// See [`Self::assume_issued`].
    pub fn assumed() -> Self {
        Self {
            assume_issued: true,
            ..Self::default()
        }
    }

    fn acme_for(&self, sni: &str, directory: &str) -> Option<&CertificateEntity> {
        self.acme
            .iter()
            .find(|c| c.sni == sni && c.acme_directory == directory)
    }

    fn relay_for(&self, pod: &NodeId) -> Option<&RelayCertificateEntity> {
        self.relay.iter().find(|c| c.pod.0 == pod.0)
    }

    fn ca_usable(&self) -> bool {
        self.ca_present || self.assume_issued
    }
}

/// A server's ideal config: what the canvas says it should serve, ignoring what
/// the rest of the fabric is currently running.
#[derive(Debug, Clone)]
pub struct DerivedConfig {
    pub config: Config,
    /// Index-aligned with `config.forwardings`.
    pub forwardings: Vec<ForwardingDeps>,
    /// Pods that could not be derived. The rest of `config` is unaffected.
    pub invalid: Vec<InvalidPod>,
    /// Every certificate `config` references, sorted and deduplicated: the union
    /// of the entries' [`ForwardingDeps::certificates`].
    pub certificates: Vec<CertificateRef>,
}

/// The sorted, deduplicated union of the entries' certificate refs.
pub fn certificate_union(forwardings: &[ForwardingDeps]) -> Vec<CertificateRef> {
    let mut refs: Vec<CertificateRef> = forwardings
        .iter()
        .flat_map(|deps| deps.certificates.iter().cloned())
        .collect();
    refs.sort();
    refs.dedup();
    refs
}

/// The pods whose listen consumer is a TLS or QUIC relay: the ones that need a
/// relay leaf. Sorted by record key, deduplicated.
pub fn relay_tls_pods(topology: &CanvasTopology) -> Vec<NodeId> {
    let index = Index::build(topology);
    let mut pods: Vec<NodeId> = topology
        .nodes
        .iter()
        .filter(|n| {
            matches!(
                &n.node.spec,
                NodeSpec::Relay(relay)
                    if matches!(relay.protocol, EntityRelayProtocol::TcpTls | EntityRelayProtocol::Quic)
            )
        })
        .filter_map(|relay| index.peer(index.port_by_key(relay, "listen")?))
        .filter(|pod| matches!(pod.node.spec, NodeSpec::Pod(_)))
        .map(|pod| pod.node.id.clone())
        .collect();
    pods.sort_by_key(|pod| pod.to_string());
    pods.dedup_by_key(|pod| pod.to_string());
    pods
}

/// The SNIs of every TLS Entry in the tree, deduplicated.
pub fn tls_snis(topology: &CanvasTopology) -> Vec<String> {
    let mut snis: Vec<String> = topology
        .nodes
        .iter()
        .filter_map(|n| match &n.node.spec {
            NodeSpec::Entry(entry) => entry.tls.as_ref().map(|tls| tls.sni.clone()),
            _ => None,
        })
        .collect();
    snis.sort();
    snis.dedup();
    snis
}

pub fn derive_server_config(
    topology: &CanvasTopology,
    server: &ServerId,
    certificates: &DerivationCertificates,
    config: &OrchestrationConfig,
) -> Result<DerivedConfig, DeriveError> {
    let server_key = server.to_string();
    let server_row = topology
        .servers
        .iter()
        .find(|s| s.id.to_string() == server_key)
        .ok_or_else(|| DeriveError::UnknownServer {
            server: server_key.clone(),
        })?;

    let index = Index::build(topology);
    let dialers = quic_dialers(&index, topology, certificates);
    let mut forwardings = Vec::new();
    let mut deps = Vec::new();
    let mut invalid = Vec::new();

    let mut pods: Vec<&NodeWithPorts> = topology
        .nodes
        .iter()
        .filter(|n| matches!(n.node.spec, NodeSpec::Pod(_)))
        .collect();
    pods.sort_by_key(|n| n.node.id.to_string());

    for pod in pods {
        let NodeSpec::Pod(cfg) = &pod.node.spec else {
            continue;
        };
        // The pod's server link is its attribution; the schema guarantees it
        // resolves, so a pod on another server is simply not this server's.
        if cfg.server.to_string() != server_key {
            continue;
        }
        let (Some(listen_port), Some(destination_port)) = (
            index.port_by_key(pod, "listen"),
            index.port_by_key(pod, "destination"),
        ) else {
            continue;
        };
        if index.edge_on(listen_port).is_none() || index.edge_on(destination_port).is_none() {
            // A pod with an unconnected port is not a rule yet (every server starts
            // with unwired transport pods) and is skipped.
            continue;
        }

        match derive_pod(
            &index,
            pod,
            cfg,
            listen_port,
            destination_port,
            certificates,
            config,
            server_row,
            &dialers,
        ) {
            Ok((forwarding, pod_deps)) => {
                forwardings.push(forwarding);
                deps.push(pod_deps);
            }
            Err(error) => invalid.push(InvalidPod {
                node: pod.node.id.clone(),
                pod: pod.node.name.clone(),
                listen: cfg.listen_display(),
                error: error.to_string(),
            }),
        }
    }

    let config = Config {
        ipv6_resolve: server_row.ipv6_resolve.into(),
        log: LogConfig {
            level: server_row.log_level.clone(),
        },
        relay_ca: certificates.ca_present.then(|| PathBuf::from(CA_FILE)),
        // The defaults, which the TOML then omits: a worker built before the
        // section existed rejects unknown keys.
        keepalive: KeepAlive::default(),
        // The server's own side of every QUIC link; a link whose peer asks for
        // less carries its own numbers on the forwarding.
        quic: server_row.quic.tuning(None),
        forwardings,
    };
    // Every entry validated itself in `derive_pod`; what is left is the cross-pod
    // rule, which no single pod can be blamed for.
    config.validate()?;
    let certificates = certificate_union(&deps);
    Ok(DerivedConfig {
        config,
        forwardings: deps,
        invalid,
        certificates,
    })
}

/// Every QUIC relay hop on the canvas as `(dialing server, listener it dials)`,
/// so a listener can learn who dials it and pair its QUIC numbers with theirs.
/// A hop that cannot be derived (no CA, a dangling relay) is simply absent: its
/// dialer's own derivation reports the problem.
fn quic_dialers(
    index: &Index<'_>,
    topology: &CanvasTopology,
    certificates: &DerivationCertificates,
) -> Vec<(ServerId, ListenerCap)> {
    let mut dialers = Vec::new();
    for pod in &topology.nodes {
        let NodeSpec::Pod(cfg) = &pod.node.spec else {
            continue;
        };
        let Some(producer) = index
            .port_by_key(pod, "destination")
            .and_then(|port| index.peer(port))
        else {
            continue;
        };
        let mut points_at = Vec::new();
        let _ = derive_destination(
            index,
            producer,
            &mut Vec::new(),
            &mut points_at,
            &mut Vec::new(),
            certificates,
            &ServerQuic::default(),
        );
        dialers.extend(
            points_at
                .into_iter()
                .filter(|cap| cap.protocol == ListenProtocol::RelayQuic)
                .map(|cap| (cfg.server.clone(), cap)),
        );
    }
    dialers
}

/// This side of a QUIC link with `peer`, written on the forwarding only when it
/// differs from the server-wide side the top-level `[quic]` already carries.
fn link_tuning(local: &ServerQuic, peer: &ServerQuic) -> Option<QuicTuning> {
    let tuning = local.tuning(Some(peer));
    (tuning != local.tuning(None)).then_some(tuning)
}

/// The listener side of a QUIC link: paired with every server that dials this
/// pod. Several dialers (a pod reached through more than one relay) are folded
/// into the most conservative peer, so the listener never sends faster than the
/// slowest of them said it can take.
fn listener_tuning(
    local: &ServerEntity,
    port: u16,
    dialers: &[(ServerId, ListenerCap)],
    index: &Index<'_>,
) -> Option<QuicTuning> {
    fn lower(a: u32, b: u32) -> u32 {
        match (a, b) {
            (0, rate) | (rate, 0) => rate,
            (a, b) => a.min(b),
        }
    }
    let mut peer: Option<ServerQuic> = None;
    for (dialer, cap) in dialers {
        if cap.server != local.id || cap.port != i64::from(port) {
            continue;
        }
        let Some(server) = index.servers.get(dialer) else {
            continue;
        };
        peer = Some(match peer {
            None => server.quic,
            Some(folded) => ServerQuic {
                up_mbps: lower(folded.up_mbps, server.quic.up_mbps),
                down_mbps: lower(folded.down_mbps, server.quic.down_mbps),
                ..folded
            },
        });
    }
    link_tuning(&local.quic, &peer?)
}

/// One pod's `[[forwarding]]` entry, or why that pod alone cannot be derived.
#[allow(clippy::too_many_arguments)]
fn derive_pod(
    index: &Index<'_>,
    pod: &NodeWithPorts,
    cfg: &PodConfig,
    listen_port: &PortEntity,
    destination_port: &PortEntity,
    certificates: &DerivationCertificates,
    config: &OrchestrationConfig,
    local: &ServerEntity,
    dialers: &[(ServerId, ListenerCap)],
) -> Result<(Forwarding, ForwardingDeps), DeriveError> {
    // No bind means every address of the host: the worker binds `::` dual-stack
    // and falls back to `0.0.0.0` on a host without IPv6.
    let address: IpAddr = cfg
        .bind_ip()
        .map_err(|value| DeriveError::InvalidBindIp {
            node: pod.node.name.clone(),
            value,
        })?
        .unwrap_or(IpAddr::V6(Ipv6Addr::UNSPECIFIED));
    let mut nodes = Vec::new();
    let mut refs = Vec::new();
    let (listen_as, listen_protocol, receive_proxy_protocol) = derive_listen(
        index,
        pod,
        listen_port,
        &mut nodes,
        &mut refs,
        certificates,
        config,
    )?;

    let producer = index
        .peer(destination_port)
        .ok_or_else(|| DeriveError::DanglingBoundary {
            node: pod.node.name.clone(),
        })?;
    let mut visited = Vec::new();
    let mut points_at = Vec::new();
    let to = derive_destination(
        index,
        producer,
        &mut visited,
        &mut points_at,
        &mut nodes,
        certificates,
        &local.quic,
    )?;

    let quic = match listen_protocol {
        ListenProtocol::RelayQuic => listener_tuning(local, cfg.port, dialers, index),
        _ => None,
    };
    let forwarding = Forwarding {
        tag: pod.node.name.clone(),
        listen: SocketAddr::new(address, cfg.port),
        receive_proxy_protocol,
        listen_as,
        quic,
        to,
    };
    forwarding.validate()?;
    // A node reached through several load-balance members appears once: node
    // health writes one row per node per report.
    nodes.sort_by_key(|n| n.to_string());
    nodes.dedup_by_key(|n| n.to_string());
    let deps = ForwardingDeps {
        pod: pod.node.id.clone(),
        serves: ListenerCap {
            server: cfg.server.clone(),
            port: i64::from(cfg.port),
            protocol: listen_protocol,
        },
        points_at,
        nodes,
        certificates: refs,
    };
    Ok((forwarding, deps))
}

/// The listen side of one pod: what the node feeding its `listen` port makes it.
fn derive_listen(
    index: &Index<'_>,
    pod: &NodeWithPorts,
    listen_port: &PortEntity,
    nodes: &mut Vec<NodeId>,
    refs: &mut Vec<CertificateRef>,
    certificates: &DerivationCertificates,
    config: &OrchestrationConfig,
) -> Result<(ListenAs, ListenProtocol, Option<TcpProxyProtocol>), DeriveError> {
    let consumer = index
        .peer(listen_port)
        .ok_or_else(|| DeriveError::DanglingBoundary {
            node: pod.node.name.clone(),
        })?;
    nodes.push(consumer.node.id.clone());
    match &consumer.node.spec {
        NodeSpec::Entry(entry) => {
            let listen_as = match &entry.tls {
                None => ListenAs::Raw,
                Some(tls) => {
                    let directory = config.acme_directory(&tls.acme_directory);
                    let key = match certificates.acme_for(&tls.sni, directory) {
                        Some(c) if c.is_issued() => {
                            let key = c.id.to_string();
                            refs.push(CertificateRef {
                                kind: CertificateKind::Acme,
                                key: key.clone(),
                                version: c.version,
                            });
                            key
                        }
                        _ if certificates.assume_issued => "pending".to_string(),
                        other => {
                            return Err(DeriveError::CertificateNotIssued {
                                sni: tls.sni.clone(),
                                state: certificate_state(other),
                            });
                        }
                    };
                    ListenAs::Tls(tls_host(acme_cert_paths(&key)))
                }
            };
            Ok((
                listen_as,
                ListenProtocol::Raw,
                entry.receive_proxy_protocol.map(Into::into),
            ))
        }
        NodeSpec::Relay(relay) => match relay.protocol {
            // The worker auto-detects PROXY on relay ingest.
            EntityRelayProtocol::TcpRaw => Ok((
                ListenAs::Relay(RelayHost::Tcp),
                ListenProtocol::RelayTcp,
                None,
            )),
            EntityRelayProtocol::TcpTls | EntityRelayProtocol::Quic => {
                if !certificates.ca_usable() {
                    return Err(DeriveError::RelayCaMissing {
                        node: consumer.node.name.clone(),
                    });
                }
                match certificates.relay_for(&pod.node.id) {
                    Some(leaf) => refs.push(CertificateRef {
                        kind: CertificateKind::Relay,
                        key: leaf.id.to_string(),
                        version: leaf.version,
                    }),
                    None if certificates.assume_issued => {}
                    None => {
                        return Err(DeriveError::RelayCertificateMissing {
                            node: consumer.node.name.clone(),
                        });
                    }
                }
                let host = tls_host(relay_cert_paths(pod.node.id.as_ref()));
                Ok(match relay.protocol {
                    EntityRelayProtocol::Quic => (
                        ListenAs::Relay(RelayHost::Quic(host)),
                        ListenProtocol::RelayQuic,
                        None,
                    ),
                    _ => (
                        ListenAs::Relay(RelayHost::TlsOverTcp(host)),
                        ListenProtocol::RelayTls,
                        None,
                    ),
                })
            }
        },
        _ => Err(DeriveError::UnsupportedSpec {
            node: consumer.node.name.clone(),
        }),
    }
}

fn tls_host((full_chain, key): (String, String)) -> TlsHostConfig {
    TlsHostConfig {
        key: PathBuf::from(key),
        full_chain: PathBuf::from(full_chain),
    }
}

/// Why a certificate cannot be served, for the invalid-pod message.
fn certificate_state(certificate: Option<&CertificateEntity>) -> String {
    match certificate {
        None => "pending (not requested yet)".to_string(),
        Some(c) => match c.status {
            CertificateStatus::Failed => format!(
                "failed: {}",
                c.last_error.as_deref().unwrap_or("unknown error")
            ),
            CertificateStatus::Pending | CertificateStatus::Issued => "pending".to_string(),
        },
    }
}

/// Walks the destination side of one pod, collecting into `points_at` every
/// listener on another server this forwarding will dial.
fn derive_destination(
    index: &Index<'_>,
    node: &NodeWithPorts,
    visited: &mut Vec<String>,
    points_at: &mut Vec<ListenerCap>,
    nodes: &mut Vec<NodeId>,
    certificates: &DerivationCertificates,
    local: &ServerQuic,
) -> Result<ForwardingTo, DeriveError> {
    let key = node.node.id.to_string();
    if visited.contains(&key) {
        return Err(DeriveError::Cycle {
            node: node.node.name.clone(),
        });
    }
    visited.push(key);
    nodes.push(node.node.id.clone());

    let result =
        match &node.node.spec {
            NodeSpec::Exit(cfg) => ForwardingTo::Exit {
                destination: Remote::parse(&cfg.destination).map_err(|_| {
                    DeriveError::InvalidDestination {
                        node: node.node.name.clone(),
                        destination: cfg.destination.clone(),
                    }
                })?,
                send_proxy_protocol: cfg.pass_proxy_protocol.map(Into::into),
            },
            NodeSpec::Relay(cfg) => {
                let (protocol, listen_protocol) = match cfg.protocol {
                    EntityRelayProtocol::TcpRaw => (RelayProtocol::Tcp, ListenProtocol::RelayTcp),
                    EntityRelayProtocol::TcpTls => {
                        (RelayProtocol::TlsOverTcp, ListenProtocol::RelayTls)
                    }
                    EntityRelayProtocol::Quic => (RelayProtocol::Quic, ListenProtocol::RelayQuic),
                };
                // The dialer verifies the relay's leaf against the internal CA, so
                // without one there is nothing it could trust.
                if protocol != RelayProtocol::Tcp && !certificates.ca_usable() {
                    return Err(DeriveError::RelayCaMissing {
                        node: node.node.name.clone(),
                    });
                }
                // The relay dials the pod feeding its listen side.
                let listen_port = index.port_by_key(node, "listen");
                let pod = listen_port.and_then(|p| index.peer(p));
                let Some(pod) = pod else {
                    return Err(DeriveError::RelayWithoutPod {
                        node: node.node.name.clone(),
                    });
                };
                let NodeSpec::Pod(pod_cfg) = &pod.node.spec else {
                    return Err(DeriveError::RelayWithoutPod {
                        node: node.node.name.clone(),
                    });
                };
                let far = index.servers.get(&pod_cfg.server).ok_or_else(|| {
                    DeriveError::UnknownServer {
                        server: pod_cfg.server.to_string(),
                    }
                })?;
                // The override says *how* to reach the pod; the pod's identity
                // (server, port, protocol) is what identifies the listener we depend
                // on, whatever address it is dialed on today.
                points_at.push(ListenerCap {
                    server: pod_cfg.server.clone(),
                    port: i64::from(pod_cfg.port),
                    protocol: listen_protocol,
                });
                let host = match &cfg.override_ip_address {
                    Some(host) => host.clone(),
                    None => {
                        let advertised = pod_cfg.advertise_ip().map_err(|value| {
                            DeriveError::InvalidAdvertiseIp {
                                node: pod.node.name.clone(),
                                value,
                            }
                        })?;
                        advertised
                            .or_else(|| far.effective_address().map(|(address, _)| address))
                            .ok_or_else(|| DeriveError::ServerNoAddress {
                                server: far.name.clone(),
                            })?
                            .to_string()
                    }
                };
                let port = cfg.override_port.unwrap_or(pod_cfg.port);
                // An IP literal becomes a socket address directly: an unbracketed IPv6
                // host would otherwise round-trip through `Remote::parse` as a domain
                // name and be handed to the worker's resolver. `override_ip_address` is
                // a free-form string, so a genuine hostname still takes the parse path.
                let destination = match host.parse::<IpAddr>() {
                    Ok(address) => Remote::Address(SocketAddr::new(address, port)),
                    Err(_) => Remote::parse(&format!("{host}:{port}")).map_err(|_| {
                        DeriveError::InvalidDestination {
                            node: node.node.name.clone(),
                            destination: format!("{host}:{port}"),
                        }
                    })?,
                };
                let sni = (protocol != RelayProtocol::Tcp).then(|| relay_sni(&pod.node.id));
                // The dialing side of a QUIC link, paired with the listener's server.
                let quic = (protocol == RelayProtocol::Quic)
                    .then(|| link_tuning(local, &far.quic))
                    .flatten();
                ForwardingTo::Relay {
                    protocol,
                    destination,
                    sni,
                    quic,
                }
            }
            NodeSpec::LoadBalanceDistribute(cfg) => {
                let mut members = smallvec::SmallVec::new();
                for port in inputs_in_order(node) {
                    let Some(member) = index.peer(port) else {
                        continue; // unconnected members are skipped
                    };
                    members.push(derive_destination(
                        index,
                        member,
                        visited,
                        points_at,
                        nodes,
                        certificates,
                        local,
                    )?);
                }
                ForwardingTo::LoadBalance(Box::new(LoadBalanceGroup {
                    strategy: cfg.mode.into(),
                    members,
                }))
            }
            NodeSpec::LoadBalanceAggregate(_) => {
                let port = inputs_in_order(node).into_iter().next().ok_or_else(|| {
                    DeriveError::UnsupportedSpec {
                        node: node.node.name.clone(),
                    }
                })?;
                let Some(source) = index.peer(port) else {
                    return Err(DeriveError::UnsupportedSpec {
                        node: node.node.name.clone(),
                    });
                };
                derive_destination(
                    index,
                    source,
                    visited,
                    points_at,
                    nodes,
                    certificates,
                    local,
                )?
            }
            // Boundary nodes and universal pods are never reached: `Index::peer`
            // resolves through the former and stops at the latter's bundles.
            NodeSpec::Pod(_)
            | NodeSpec::Entry(_)
            | NodeSpec::CanvasImport(_)
            | NodeSpec::CanvasExport(_)
            | NodeSpec::UniversalPod(_) => {
                return Err(DeriveError::UnsupportedSpec {
                    node: node.node.name.clone(),
                });
            }
        };

    visited.pop();
    Ok(result)
}

/// A load-balance node's hand-drawn inputs; its on-demand `lane:` ports belong
/// to the channels it bundles, which are derived through the channel pods.
fn inputs_in_order(node: &NodeWithPorts) -> Vec<&PortEntity> {
    crate::services::topology::manual_inputs(node)
}
