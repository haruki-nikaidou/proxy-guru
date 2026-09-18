//! What every server runs.

use crate::check::check;
use crate::diagnostic::{Diagnostic, Invalid, InvalidPod, Report};
use crate::index::Index;
use crate::legacy;
use crate::model::{
    CertificateRef, Certificates, EdgeId, EdgeTarget, ExitId, Graph, Ingress, IpFamily, Pod, PodId,
    Route, Server, ServerId, ServerQuic, acme_host, lower, relay_host, relay_sni,
};
use guru_worker_config::table::{
    ExitTarget, Group, Policy, RelayTarget, Target, Upstream, Weighted,
};
use guru_worker_config::{
    ConfigError, Forwarding, ListenAs, QuicTuning, RelayHost, RelayProtocol, Remote,
    TcpProxyProtocol, To,
};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use std::net::{IpAddr, Ipv6Addr, SocketAddr};

/// The result of compiling a graph that passed [`check`].
#[derive(Debug, Clone, PartialEq)]
pub struct Compiled {
    /// Every server of the graph, including the ones with nothing to run.
    pub servers: BTreeMap<ServerId, ServerConfig>,
    /// The warnings [`check`] reported.
    pub warnings: Vec<Diagnostic>,
}

/// One server's share of the graph.
#[derive(Debug, Clone, PartialEq)]
pub struct ServerConfig {
    /// One forwarding per compiled pod, by pod id; the tag is the pod id. Each
    /// goes where it goes through a route table when `route_table` is set, and
    /// through the inline tree every worker reads otherwise: weights become
    /// repeated members of a round robin, a failover becomes `fallback`, a
    /// sticky balance becomes `ip_hash`, and nothing asks for confirmation.
    pub forwardings: Vec<Forwarding>,
    /// Whether the forwardings use route tables, as this server's worker reads.
    pub route_table: bool,
    /// What each forwarding depends on, in the same order.
    pub deps: Vec<Deps>,
    /// Pods on this server that could not be compiled, and why.
    pub invalid: Vec<InvalidPod>,
}

impl ServerConfig {
    /// The tags of the forwardings, which are the ids of their pods.
    pub fn tags(&self) -> Vec<&str> {
        self.forwardings.iter().map(|f| f.tag.as_str()).collect()
    }
}

/// A forwarding's place in the fabric.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Deps {
    pub pod: PodId,
    /// The listener this forwarding is.
    pub serves: Listener,
    /// The listeners its relay hops dial, by identity rather than address.
    pub points_at: Vec<Listener>,
    /// The certificates its listener references.
    pub certificates: Vec<CertificateRef>,
    /// Its out-edges.
    pub edges: Vec<EdgeId>,
    /// The pods those edges lead to.
    pub pods: Vec<PodId>,
    /// The exits those edges lead to.
    pub exits: Vec<ExitId>,
}

/// A listener, identified by what it is rather than where it is dialed: the
/// same server, port and protocol is the same listener whatever address the
/// server has today.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct Listener {
    pub server: ServerId,
    pub port: u16,
    pub protocol: ListenProtocol,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ListenProtocol {
    /// Clients connect, with or without TLS.
    Raw,
    RelayTcp,
    RelayTls,
    RelayQuic,
}

/// Compiles every pod of a graph [`check`] accepts; otherwise returns the
/// report with its errors.
pub fn compile(graph: &Graph, certificates: &Certificates) -> Result<Compiled, Report> {
    let report = check(graph);
    if report.has_errors() {
        return Err(report);
    }
    let index = Index::build(graph, &mut Vec::new());

    let mut entries: Vec<Entry<'_>> = Vec::new();
    let mut invalid: BTreeMap<&ServerId, Vec<InvalidPod>> = BTreeMap::new();
    for pod in index.pods.values().copied() {
        if pod.route.is_none()
            || (pod.ingress.is_relay() && index.edges_into_pod(&pod.id).is_empty())
        {
            continue;
        }
        let Some(server) = index.servers.get(&pod.server).copied() else {
            continue;
        };
        match entry(&index, pod, server, certificates) {
            Ok(entry) => entries.push(entry),
            Err(reason) => invalid
                .entry(&server.id)
                .or_default()
                .push(invalid_pod(&index, pod, reason)),
        }
    }
    pair_quic_listeners(&index, &mut entries);

    let mut servers = BTreeMap::new();
    for server in index.servers.values().copied() {
        let route_table = server.capabilities.route_table;
        let mut invalid = invalid.remove(&server.id).unwrap_or_default();
        let mut forwardings = Vec::new();
        let mut deps = Vec::new();
        for entry in entries.iter().filter(|e| e.server.id == server.id) {
            let rendered = if route_table {
                render_table(entry)
            } else {
                legacy::render(entry)
            };
            match rendered {
                Ok(forwarding) => {
                    forwardings.push(forwarding);
                    deps.push(entry.deps.clone());
                }
                Err(error) => invalid.push(rejected(&index, entry.pod, &error)),
            }
        }
        servers.insert(
            server.id.clone(),
            ServerConfig {
                forwardings,
                route_table,
                deps,
                invalid,
            },
        );
    }
    Ok(Compiled {
        servers,
        warnings: report.diagnostics,
    })
}

/// A compiled pod, before it is rendered for its worker.
pub(crate) struct Entry<'g> {
    pub pod: &'g Pod,
    pub server: &'g Server,
    pub route: &'g Route,
    pub listen: SocketAddr,
    pub receive_proxy_protocol: Option<TcpProxyProtocol>,
    pub listen_as: ListenAs,
    pub protocol: ListenProtocol,
    /// The listener side of a QUIC link, filled in once every pod is compiled.
    pub quic: Option<QuicTuning>,
    /// The next hop behind each out-edge.
    pub hops: BTreeMap<&'g EdgeId, Hop>,
    pub deps: Deps,
}

pub(crate) struct Hop {
    pub destination: Remote,
    pub kind: HopKind,
}

pub(crate) enum HopKind {
    Exit {
        send_proxy_protocol: Option<TcpProxyProtocol>,
    },
    Relay {
        protocol: RelayProtocol,
        sni: Option<String>,
        quic: Option<QuicTuning>,
        confirm: bool,
    },
}

fn entry<'g>(
    index: &Index<'g>,
    pod: &'g Pod,
    server: &'g Server,
    certificates: &Certificates,
) -> Result<Entry<'g>, Invalid> {
    let Some(route) = &pod.route else {
        return Err(Invalid::Rejected {
            reason: "the pod has no route".to_string(),
        });
    };
    let bind = match &pod.bind_ip {
        Some(bind) => bind.parse::<IpAddr>().map_err(|_| Invalid::Rejected {
            reason: format!("bind address {bind:?} is not an IP address"),
        })?,
        None => IpAddr::V6(Ipv6Addr::UNSPECIFIED),
    };
    let mut refs = Vec::new();
    let (listen_as, protocol, receive_proxy_protocol) = match &pod.ingress {
        Ingress::ClientRaw {
            receive_proxy_protocol,
        } => (ListenAs::Raw, ListenProtocol::Raw, *receive_proxy_protocol),
        Ingress::ClientTls {
            receive_proxy_protocol,
            sni,
            acme_directory,
        } => {
            let key = (sni.clone(), acme_directory.clone());
            let certificate = match certificates.acme.get(&key) {
                Some(certificate) => {
                    refs.push(certificate.clone());
                    certificate.key.clone()
                }
                None if certificates.assume_issued => "pending".to_string(),
                None => {
                    return Err(Invalid::CertificatePending {
                        sni: sni.clone(),
                        acme_directory: acme_directory.clone(),
                    });
                }
            };
            (
                ListenAs::Tls(acme_host(&certificate)),
                ListenProtocol::Raw,
                *receive_proxy_protocol,
            )
        }
        Ingress::RelayTcp => (
            ListenAs::Relay(RelayHost::Tcp),
            ListenProtocol::RelayTcp,
            None,
        ),
        Ingress::RelayTls | Ingress::RelayQuic => {
            if !certificates.ca_usable() {
                return Err(Invalid::InternalCaMissing);
            }
            match certificates.relay.get(&pod.id) {
                Some(certificate) => refs.push(certificate.clone()),
                None if certificates.assume_issued => {}
                None => return Err(Invalid::RelayCertificateMissing),
            }
            let host = relay_host(&pod.id);
            if pod.ingress == Ingress::RelayQuic {
                (
                    ListenAs::Relay(RelayHost::Quic(host)),
                    ListenProtocol::RelayQuic,
                    None,
                )
            } else {
                (
                    ListenAs::Relay(RelayHost::TlsOverTcp(host)),
                    ListenProtocol::RelayTls,
                    None,
                )
            }
        }
    };

    let mut hops = BTreeMap::new();
    let mut points_at = BTreeSet::new();
    let mut edges = Vec::new();
    let mut pods = BTreeSet::new();
    let mut exits = BTreeSet::new();
    for edge in index.out_of(&pod.id).iter().copied() {
        edges.push(edge.id.clone());
        let hop = match &edge.target {
            EdgeTarget::Exit(id) => {
                let Some(exit) = index.exits.get(id) else {
                    return Err(Invalid::Rejected {
                        reason: format!("edge {} leads to a missing exit", edge.id),
                    });
                };
                exits.insert(exit.id.clone());
                let destination =
                    Remote::parse(&exit.destination).map_err(|error| Invalid::Rejected {
                        reason: error.to_string(),
                    })?;
                Hop {
                    destination,
                    kind: HopKind::Exit {
                        send_proxy_protocol: exit.send_proxy_protocol,
                    },
                }
            }
            EdgeTarget::Pod(id) => {
                let (Some(target), Some(far)) = (
                    index.pods.get(id).copied(),
                    index
                        .pods
                        .get(id)
                        .and_then(|target| index.servers.get(&target.server))
                        .copied(),
                ) else {
                    return Err(Invalid::Rejected {
                        reason: format!("edge {} leads to a missing pod or server", edge.id),
                    });
                };
                pods.insert(target.id.clone());
                let (relay_protocol, listen_protocol) = match target.ingress {
                    Ingress::RelayTcp => (RelayProtocol::Tcp, ListenProtocol::RelayTcp),
                    Ingress::RelayTls => (RelayProtocol::TlsOverTcp, ListenProtocol::RelayTls),
                    Ingress::RelayQuic => (RelayProtocol::Quic, ListenProtocol::RelayQuic),
                    Ingress::ClientRaw { .. } | Ingress::ClientTls { .. } => {
                        return Err(Invalid::Rejected {
                            reason: format!("edge {} leads to a client pod", edge.id),
                        });
                    }
                };
                // The dialer verifies the listener's leaf against the internal
                // CA, so without one there is nothing it could trust.
                if relay_protocol != RelayProtocol::Tcp && !certificates.ca_usable() {
                    return Err(Invalid::InternalCaMissing);
                }
                points_at.insert(Listener {
                    server: far.id.clone(),
                    port: target.port,
                    protocol: listen_protocol,
                });
                let Some(host) = edge.dial_host(target, far) else {
                    return Err(Invalid::TargetWithoutAddress {
                        edge: edge.id.clone(),
                        pod: target.id.clone(),
                        server: far.id.clone(),
                        family: edge.ip_family,
                    });
                };
                let port = edge.override_port.unwrap_or(target.port);
                // An IP literal becomes a socket address directly; an unbracketed
                // IPv6 host would otherwise parse as a domain name.
                let destination = match host.parse::<IpAddr>() {
                    Ok(address) => Remote::Address(SocketAddr::new(address, port)),
                    Err(_) => Remote::parse(&format!("{host}:{port}")).map_err(|error| {
                        Invalid::Rejected {
                            reason: error.to_string(),
                        }
                    })?,
                };
                let quic = (relay_protocol == RelayProtocol::Quic)
                    .then(|| link_tuning(&server.quic, &far.quic))
                    .flatten();
                Hop {
                    destination,
                    kind: HopKind::Relay {
                        protocol: relay_protocol,
                        sni: (relay_protocol != RelayProtocol::Tcp).then(|| relay_sni(&target.id)),
                        quic,
                        confirm: server.capabilities.relay_confirm
                            && far.capabilities.relay_confirm,
                    },
                }
            }
        };
        hops.insert(&edge.id, hop);
    }

    refs.sort();
    refs.dedup();
    Ok(Entry {
        pod,
        server,
        route,
        listen: SocketAddr::new(bind, pod.port),
        receive_proxy_protocol,
        listen_as,
        protocol,
        quic: None,
        hops,
        deps: Deps {
            pod: pod.id.clone(),
            serves: Listener {
                server: server.id.clone(),
                port: pod.port,
                protocol,
            },
            points_at: points_at.into_iter().collect(),
            certificates: refs,
            edges,
            pods: pods.into_iter().collect(),
            exits: exits.into_iter().collect(),
        },
    })
}

/// This side of a QUIC link with `peer`, only when it differs from the
/// server-wide side the worker's top-level `[quic]` already carries.
fn link_tuning(local: &ServerQuic, peer: &ServerQuic) -> Option<QuicTuning> {
    let tuning = local.tuning(Some(peer));
    (tuning != local.tuning(None)).then_some(tuning)
}

/// The listener side of every QUIC link. A pod several servers dial is paired
/// with the most conservative of them, so it never sends faster than the
/// slowest said it can take.
fn pair_quic_listeners(index: &Index<'_>, entries: &mut [Entry<'_>]) {
    for entry in entries.iter_mut() {
        if entry.protocol != ListenProtocol::RelayQuic {
            continue;
        }
        let dialers: BTreeSet<&ServerId> = index
            .edges_into_pod(&entry.pod.id)
            .iter()
            .filter_map(|edge| index.pods.get(&edge.source))
            .map(|pod| &pod.server)
            .collect();
        let mut peer: Option<ServerQuic> = None;
        for dialer in dialers {
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
        entry.quic = peer.and_then(|peer| link_tuning(&entry.server.quic, &peer));
    }
}

fn render_table(entry: &Entry<'_>) -> Result<Forwarding, ConfigError> {
    let mut groups = Vec::new();
    let to = table_member(entry.route, "g", &mut groups);
    let upstreams = entry
        .hops
        .iter()
        .map(|(edge, hop)| Upstream {
            id: upstream_id(edge),
            target: match &hop.kind {
                HopKind::Exit {
                    send_proxy_protocol,
                } => Target::Exit(ExitTarget {
                    destination: hop.destination.clone(),
                    send_proxy_protocol: *send_proxy_protocol,
                }),
                HopKind::Relay {
                    protocol,
                    sni,
                    quic,
                    confirm,
                } => Target::Relay(RelayTarget {
                    protocol: *protocol,
                    destination: hop.destination.clone(),
                    sni: sni.clone(),
                    quic: *quic,
                    confirm: *confirm,
                }),
            },
        })
        .collect();
    let forwarding = Forwarding {
        tag: entry.pod.id.to_string(),
        listen: entry.listen,
        receive_proxy_protocol: entry.receive_proxy_protocol,
        listen_as: entry.listen_as.clone(),
        quic: entry.quic,
        to: To::Route(to),
        groups,
        upstreams,
    };
    forwarding.validate()?;
    Ok(forwarding)
}

/// The table id of an out-edge's upstream. Group ids start with `g`, so the two
/// can never collide whatever the edge ids are.
fn upstream_id(edge: &EdgeId) -> String {
    format!("u:{edge}")
}

/// Adds the groups of `route` (parents first) and returns the id it is
/// referenced by. A group's id is its position in the tree: `g`, `g.0`, `g.1.2`.
fn table_member(route: &Route, path: &str, groups: &mut Vec<Group>) -> String {
    let slot = groups.len();
    let policy = match route {
        Route::Edge(edge) => return upstream_id(edge),
        Route::Balance { members, sticky } => {
            groups.push(placeholder(path));
            Policy::Balance {
                members: members
                    .iter()
                    .enumerate()
                    .map(|(i, member)| Weighted {
                        to: table_member(&member.to, &format!("{path}.{i}"), groups),
                        weight: member.weight,
                    })
                    .collect(),
                sticky: *sticky,
            }
        }
        Route::Failover(members) => {
            groups.push(placeholder(path));
            Policy::Failover {
                members: members
                    .iter()
                    .enumerate()
                    .map(|(i, member)| table_member(member, &format!("{path}.{i}"), groups))
                    .collect(),
            }
        }
    };
    if let Some(group) = groups.get_mut(slot) {
        group.policy = policy;
    }
    path.to_string()
}

/// Holds a group's place in the list while its members are rendered.
fn placeholder(path: &str) -> Group {
    Group {
        id: path.to_string(),
        policy: Policy::Failover {
            members: Vec::new(),
        },
    }
}

fn invalid_pod(index: &Index<'_>, pod: &Pod, reason: Invalid) -> InvalidPod {
    let message = match &reason {
        Invalid::CertificatePending { sni, .. } => {
            format!("certificate for {sni} is not issued yet")
        }
        Invalid::InternalCaMissing => "internal CA not initialised".to_string(),
        Invalid::RelayCertificateMissing => "relay certificate not issued yet".to_string(),
        Invalid::TargetWithoutAddress {
            pod: target,
            server,
            family: IpFamily::Auto,
            ..
        } => format!(
            "pod {} on server {} has no address to dial",
            index.pod_name(target),
            index.server_name(server)
        ),
        Invalid::TargetWithoutAddress {
            pod: target,
            server,
            family,
            ..
        } => format!(
            "pod {} on server {} has no {family} address to dial",
            index.pod_name(target),
            index.server_name(server)
        ),
        Invalid::Rejected { reason } => reason.clone(),
    };
    InvalidPod {
        pod: pod.id.clone(),
        reason,
        message: format!("pod {}: {message}", pod.name),
    }
}

fn rejected(index: &Index<'_>, pod: &Pod, error: &ConfigError) -> InvalidPod {
    invalid_pod(
        index,
        pod,
        Invalid::Rejected {
            reason: error.to_string(),
        },
    )
}
