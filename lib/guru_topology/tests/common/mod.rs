//! Builders shared by the integration tests.

#![allow(dead_code)]

use guru_topology::{
    Capabilities, CertificateKind, CertificateRef, Certificates, Compiled, Edge, EdgeId,
    EdgeTarget, Exit, ExitId, Graph, Ingress, IpFamily, Pod, PodId, Route, Server, ServerId,
    ServerQuic, Sticky, Weighted,
};
use guru_worker_config::Forwarding;
use guru_worker_config::table::{Group, Target, Upstream};
use std::net::IpAddr;

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
        let address: Option<IpAddr> = address.map(|a| a.parse().unwrap());
        self.graph.servers.push(Server {
            id: ServerId::new(id),
            name: id.to_string(),
            dial_address: address,
            dial_v4: match address {
                Some(IpAddr::V4(v4)) => Some(v4),
                _ => None,
            },
            dial_v6: match address {
                Some(IpAddr::V6(v6)) => Some(v6),
                _ => None,
            },
            quic: ServerQuic::default(),
            capabilities,
        });
        self
    }

    /// Gives a server an IPv4 and an IPv6 address, either of them `None`; it is
    /// dialed on the IPv4 one by default, as the control plane picks.
    pub fn addresses(&mut self, server: &str, v4: Option<&str>, v6: Option<&str>) -> &mut Self {
        let server = self
            .graph
            .servers
            .iter_mut()
            .find(|s| s.id.as_str() == server)
            .unwrap();
        server.dial_v4 = v4.map(|a| a.parse().unwrap());
        server.dial_v6 = v6.map(|a| a.parse().unwrap());
        server.dial_address = server
            .dial_v4
            .map(IpAddr::V4)
            .or(server.dial_v6.map(IpAddr::V6));
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
            ip_family: IpFamily::Auto,
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

pub fn table<'a>(compiled: &'a Compiled, server: &str) -> &'a [Forwarding] {
    let config = &compiled.servers[&ServerId::new(server)];
    assert!(config.route_table, "server {server} got the tree form");
    &config.forwardings
}

pub fn legacy<'a>(compiled: &'a Compiled, server: &str) -> &'a [Forwarding] {
    let config = &compiled.servers[&ServerId::new(server)];
    assert!(!config.route_table, "server {server} got route tables");
    &config.forwardings
}

pub fn entry<'a>(list: &'a [Forwarding], pod: &str) -> &'a Forwarding {
    list.iter()
        .find(|f| f.tag == pod)
        .unwrap_or_else(|| panic!("no forwarding for pod {pod}"))
}

pub fn group<'a>(forwarding: &'a Forwarding, id: &str) -> &'a Group {
    forwarding
        .groups
        .iter()
        .find(|g| g.id == id)
        .unwrap_or_else(|| panic!("no group {id} in {forwarding:#?}"))
}

pub fn upstream<'a>(forwarding: &'a Forwarding, edge: &str) -> &'a Upstream {
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

/// The route id a table forwarding starts at.
pub fn route_to(forwarding: &Forwarding) -> &str {
    match &forwarding.to {
        guru_worker_config::To::Route(id) => id,
        guru_worker_config::To::Tree(tree) => panic!("expected a route table, got {tree:#?}"),
    }
}

/// The inline tree a tree-form forwarding goes to.
pub fn tree(forwarding: &Forwarding) -> &guru_worker_config::ForwardingTo {
    match &forwarding.to {
        guru_worker_config::To::Tree(tree) => tree,
        guru_worker_config::To::Route(id) => panic!("expected a tree, got route {id}"),
    }
}
