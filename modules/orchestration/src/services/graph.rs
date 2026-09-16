//! The pod graph as [`guru_topology`] reads it.
//!
//! Rows go in field for field; what the rows leave to the control plane is
//! resolved here: a server's dial address (pinned, reported or observed), the
//! default ACME directory, and which certificates are issued.

use crate::config::OrchestrationConfig;
use crate::entities::db::ca::RelayCertificateEntity;
use crate::entities::db::certificate::CertificateEntity;
use crate::entities::db::edge::{EdgeEntity, EdgeTarget};
use crate::entities::db::exit::ExitEntity;
use crate::entities::db::pod::{PodEntity, PodIngress};
use crate::entities::db::server::{QuicCongestion, ServerEntity, ServerQuic};
use guru_topology as topo;

/// The capability names a worker registers with.
pub const ROUTE_TABLE: &str = "route_table";
pub const RELAY_CONFIRM: &str = "relay_confirm";

pub fn server_capabilities(server: &ServerEntity) -> topo::Capabilities {
    let has = |name: &str| server.capabilities.iter().any(|c| c == name);
    topo::Capabilities {
        route_table: has(ROUTE_TABLE),
        relay_confirm: has(RELAY_CONFIRM),
    }
}

pub fn server_quic(quic: &ServerQuic) -> topo::ServerQuic {
    topo::ServerQuic {
        congestion: match quic.congestion {
            QuicCongestion::Cubic => guru_worker_config::QuicCongestion::Cubic,
            QuicCongestion::Brutal => guru_worker_config::QuicCongestion::Brutal,
        },
        up_mbps: quic.up_mbps,
        down_mbps: quic.down_mbps,
        stream_receive_window: quic.stream_receive_window,
        conn_receive_window: quic.conn_receive_window,
    }
}

pub fn topology_server(server: &ServerEntity) -> topo::Server {
    topo::Server {
        id: topo::ServerId::new(server.id.as_str()),
        name: server.name.clone(),
        dial_address: server.effective_address().map(|(address, _)| address),
        quic: server_quic(&server.quic),
        capabilities: server_capabilities(server),
    }
}

pub fn topology_pod(pod: &PodEntity, config: &OrchestrationConfig) -> topo::Pod {
    let ingress = match &pod.ingress {
        PodIngress::ClientRaw {
            receive_proxy_protocol,
        } => topo::Ingress::ClientRaw {
            receive_proxy_protocol: receive_proxy_protocol.map(Into::into),
        },
        PodIngress::ClientTls {
            receive_proxy_protocol,
            tls,
        } => topo::Ingress::ClientTls {
            receive_proxy_protocol: receive_proxy_protocol.map(Into::into),
            sni: tls.sni.clone(),
            acme_directory: config.acme_directory(&tls.acme_directory).to_string(),
        },
        PodIngress::RelayTcp => topo::Ingress::RelayTcp,
        PodIngress::RelayTls => topo::Ingress::RelayTls,
        PodIngress::RelayQuic => topo::Ingress::RelayQuic,
    };
    topo::Pod {
        id: topo::PodId::new(pod.id.as_str()),
        server: topo::ServerId::new(pod.server.as_str()),
        name: pod.name.clone(),
        port: pod.port,
        bind_ip: pod.bind_ip.clone(),
        advertise_ip: pod.advertise_ip.clone(),
        ingress,
        route: pod.route.clone(),
    }
}

pub fn topology_exit(exit: &ExitEntity) -> topo::Exit {
    topo::Exit {
        id: topo::ExitId::new(exit.id.as_str()),
        name: exit.name.clone(),
        destination: exit.destination.clone(),
        send_proxy_protocol: exit.send_proxy_protocol.map(Into::into),
    }
}

pub fn topology_edge(edge: &EdgeEntity) -> topo::Edge {
    topo::Edge {
        id: topo::EdgeId::new(edge.id.as_str()),
        source: topo::PodId::new(edge.source.as_str()),
        target: match &edge.target {
            EdgeTarget::Pod(pod) => topo::EdgeTarget::Pod(topo::PodId::new(pod.as_str())),
            EdgeTarget::Exit(exit) => topo::EdgeTarget::Exit(topo::ExitId::new(exit.as_str())),
        },
        override_ip: edge.override_ip.clone(),
        override_port: edge.override_port,
    }
}

/// The graph of the given rows. Every server of the tree is passed, pods on
/// any of them.
pub fn topology_graph(
    servers: &[ServerEntity],
    pods: &[PodEntity],
    exits: &[ExitEntity],
    edges: &[EdgeEntity],
    config: &OrchestrationConfig,
) -> topo::Graph {
    topo::Graph {
        servers: servers.iter().map(topology_server).collect(),
        pods: pods.iter().map(|pod| topology_pod(pod, config)).collect(),
        exits: exits.iter().map(topology_exit).collect(),
        edges: edges.iter().map(topology_edge).collect(),
    }
}

/// The certificates a compile runs against: the issued ACME rows by
/// `(sni, acme_directory)` and the relay leaves by pod.
pub fn topology_certificates(
    acme: &[CertificateEntity],
    relay: &[RelayCertificateEntity],
    internal_ca: bool,
) -> topo::Certificates {
    topo::Certificates {
        internal_ca,
        acme: acme
            .iter()
            .filter(|c| c.is_issued())
            .map(|c| {
                (
                    (c.sni.clone(), c.acme_directory.clone()),
                    topo::CertificateRef {
                        kind: topo::CertificateKind::Acme,
                        key: c.id.to_string(),
                        version: c.version,
                    },
                )
            })
            .collect(),
        relay: relay
            .iter()
            .map(|leaf| {
                (
                    topo::PodId::new(leaf.pod.as_str()),
                    topo::CertificateRef {
                        kind: topo::CertificateKind::Relay,
                        key: leaf.id.to_string(),
                        version: leaf.version,
                    },
                )
            })
            .collect(),
        assume_issued: false,
    }
}
