//! Builders shared by the integration tests.

#![allow(dead_code)]

use guru_topology::{
    Capabilities, CertificateKind, CertificateRef, Certificates, Compiled, Edge, EdgeId,
    EdgeTarget, Exit, ExitId, Forwardings, Graph, Ingress, Pod, PodId, Route, Server, ServerId,
    ServerQuic, Sticky, Weighted,
};
use guru_worker_config::table::{Group, Target, Upstream};
use guru_worker_config::{Forwarding, TableForwarding};

/// A graph under construction, with every certificate its relay pods need.
pub struct Fabric {
    pub graph: Graph,
    pub certificates: Certificates,
}

impl Default for Fabric {
    fn default() -> Self {
        Self::new()
    }
}

impl Fabric {
    pub fn new() -> Self {
        Self {
            graph: Graph::default(),
            certificates: Certificates {
                internal_ca: true,
                ..Certificates::default()
            },
        }
    }

    /// A server whose worker reads route tables and confirms relays.
    pub fn server(&mut self, id: &str, address: &str) -> &mut Self {
        self.server_with(
            id,
            Some(address),
            Capabilities {
                route_table: true,
                relay_confirm: true,
            },
        )
    }

    /// A server whose worker only reads the tree form.
    pub fn legacy_server(&mut self, id: &str, address: &str) -> &mut Self {
        self.server_with(id, Some(address), Capabilities::default())
    }

    pub fn server_with(
        &mut self,
        id: &str,
        address: Option<&str>,
        capabilities: Capabilities,
    ) -> &mut Self {
        self.graph.servers.push(Server {
            id: ServerId::new(id),
            name: id.to_string(),
            dial_address: address.map(|a| a.parse().unwrap()),
            quic: ServerQuic::default(),
            capabilities,
        });
        self
    }

    pub fn quic(&mut self, server: &str, up_mbps: u32, down_mbps: u32) -> &mut Self {
        let server = self
            .graph
            .servers
            .iter_mut()
            .find(|s| s.id.as_str() == server)
            .unwrap();
        server.quic.up_mbps = up_mbps;
        server.quic.down_mbps = down_mbps;
        self
    }

    /// A pod; a TLS or QUIC relay pod gets its relay leaf issued.
    pub fn pod(&mut self, id: &str, server: &str, port: u16, ingress: Ingress) -> &mut Self {
        if matches!(ingress, Ingress::RelayTls | Ingress::RelayQuic) {
            self.certificates.relay.insert(
                PodId::new(id),
                CertificateRef {
                    kind: CertificateKind::Relay,
                    key: format!("leaf-{id}"),
                    version: 1,
                },
            );
        }
        self.graph.pods.push(Pod {
            id: PodId::new(id),
            server: ServerId::new(server),
            name: id.to_string(),
            port,
            bind_ip: None,
            advertise_ip: None,
            ingress,
            route: None,
        });
        self
    }

    pub fn exit(&mut self, id: &str, destination: &str) -> &mut Self {
        self.graph.exits.push(Exit {
            id: ExitId::new(id),
            name: id.to_string(),
            destination: destination.to_string(),
            send_proxy_protocol: None,
        });
        self
    }

    /// An edge to a pod when `target` names one, otherwise to an exit.
    pub fn edge(&mut self, id: &str, source: &str, target: &str) -> &mut Self {
        let target = if self.graph.pods.iter().any(|p| p.id.as_str() == target) {
            EdgeTarget::Pod(PodId::new(target))
        } else {
            EdgeTarget::Exit(ExitId::new(target))
        };
        self.graph.edges.push(Edge {
            id: EdgeId::new(id),
            source: PodId::new(source),
            target,
            override_ip: None,
            override_port: None,
        });
        self
    }

    pub fn route(&mut self, pod: &str, route: Route) -> &mut Self {
        self.pod_mut(pod).route = Some(route);
        self
    }

    pub fn pod_mut(&mut self, pod: &str) -> &mut Pod {
        self.graph
            .pods
            .iter_mut()
            .find(|p| p.id.as_str() == pod)
            .unwrap()
    }

    pub fn edge_mut(&mut self, edge: &str) -> &mut Edge {
        self.graph
            .edges
            .iter_mut()
            .find(|e| e.id.as_str() == edge)
            .unwrap()
    }

    pub fn compile(&self) -> Compiled {
        match guru_topology::compile(&self.graph, &self.certificates) {
            Ok(compiled) => compiled,
            Err(report) => panic!("the graph does not compile: {report:#?}"),
        }
    }
}

pub fn raw() -> Ingress {
    Ingress::ClientRaw {
        receive_proxy_protocol: None,
    }
}

pub fn leaf(edge: &str) -> Route {
    Route::Edge(EdgeId::new(edge))
}

pub fn balance(members: Vec<Route>) -> Route {
    weighted(members.into_iter().map(|m| (1, m)).collect())
}

pub fn weighted(members: Vec<(u32, Route)>) -> Route {
    Route::Balance {
        members: members
            .into_iter()
            .map(|(weight, to)| Weighted { weight, to })
            .collect(),
        sticky: None,
    }
}

pub fn sticky(route: Route) -> Route {
    match route {
        Route::Balance { members, .. } => Route::Balance {
            members,
            sticky: Some(Sticky::ClientIp),
        },
        other => other,
    }
}

pub fn failover(members: Vec<Route>) -> Route {
    Route::Failover(members)
}

pub fn leaves(ids: &[&str]) -> Vec<Route> {
    ids.iter().map(|id| leaf(id)).collect()
}

pub fn table<'a>(compiled: &'a Compiled, server: &str) -> &'a [TableForwarding] {
    match &compiled.servers[&ServerId::new(server)].forwardings {
        Forwardings::Table(list) => list,
        Forwardings::Legacy(_) => panic!("server {server} got the tree form"),
    }
}

pub fn legacy<'a>(compiled: &'a Compiled, server: &str) -> &'a [Forwarding] {
    match &compiled.servers[&ServerId::new(server)].forwardings {
        Forwardings::Legacy(list) => list,
        Forwardings::Table(_) => panic!("server {server} got the table form"),
    }
}

pub fn entry<'a>(list: &'a [TableForwarding], pod: &str) -> &'a TableForwarding {
    list.iter()
        .find(|f| f.tag == pod)
        .unwrap_or_else(|| panic!("no forwarding for pod {pod}"))
}

pub fn group<'a>(forwarding: &'a TableForwarding, id: &str) -> &'a Group {
    forwarding
        .groups
        .iter()
        .find(|g| g.id == id)
        .unwrap_or_else(|| panic!("no group {id} in {forwarding:#?}"))
}

pub fn upstream<'a>(forwarding: &'a TableForwarding, edge: &str) -> &'a Upstream {
    let id = format!("u:{edge}");
    forwarding
        .upstreams
        .iter()
        .find(|u| u.id == id)
        .unwrap_or_else(|| panic!("no upstream {id} in {forwarding:#?}"))
}

pub fn relay_target(upstream: &Upstream) -> &guru_worker_config::table::RelayTarget {
    match &upstream.target {
        Target::Relay(relay) => relay,
        Target::Exit(_) => panic!("upstream {} is an exit", upstream.id),
    }
}
