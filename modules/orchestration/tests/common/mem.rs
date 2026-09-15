#![allow(dead_code)]

//! In-memory [`CanvasTopology`] builder for the pure topology/derive tests.

use orchestration::entities::surreal::canvas::{CanvasEntity, CanvasId, CanvasUiPosition};
use orchestration::entities::surreal::connection::{EdgeConnectionEntity, EdgeConnectionId};
use orchestration::entities::surreal::node::{
    CanvasExportAs, CanvasExportConfig, CanvasImportConfig, NodeEntity, NodeId, NodeSpec,
    NodeWithPorts,
};
use orchestration::entities::surreal::port::{PortDirection, PortEntity, PortId, PortKind};
use orchestration::entities::surreal::server::{ServerEntity, ServerId, ServerIpv6Resolve};
use orchestration::entities::surreal::topology::CanvasTopology;
use orchestration::services::node::export_port_direction;
use orchestration::utils::ids;

/// Builds one canvas tree. `new(root)` starts on the root; `canvas(key)` adds a
/// canvas (a stub row: nesting is expressed by import nodes alone) and makes it
/// current for the `server`/`node` calls that follow.
pub struct Builder {
    root: CanvasId,
    canvases: Vec<CanvasEntity>,
    current: CanvasId,
    servers: Vec<ServerEntity>,
    nodes: Vec<NodeWithPorts>,
    edges: Vec<EdgeConnectionEntity>,
}

/// `(port key, kind, direction, position)`
pub type PortSpec = (String, PortKind, PortDirection, i64);

fn spec(key: &str, kind: PortKind, direction: PortDirection, position: i64) -> PortSpec {
    (key.to_string(), kind, direction, position)
}

pub fn pod_ports() -> Vec<PortSpec> {
    vec![
        spec("listen", PortKind::DeriveListen, PortDirection::Output, 0),
        spec(
            "destination",
            PortKind::DeriveDestination,
            PortDirection::Input,
            1,
        ),
    ]
}

pub fn entry_ports() -> Vec<PortSpec> {
    vec![spec(
        "listen",
        PortKind::DeriveListen,
        PortDirection::Input,
        0,
    )]
}

pub fn relay_ports() -> Vec<PortSpec> {
    vec![
        spec("listen", PortKind::DeriveListen, PortDirection::Input, 0),
        spec(
            "destination",
            PortKind::DeriveDestination,
            PortDirection::Output,
            1,
        ),
    ]
}

pub fn exit_ports() -> Vec<PortSpec> {
    vec![spec(
        "destination",
        PortKind::DeriveDestination,
        PortDirection::Output,
        0,
    )]
}

pub fn distribute_ports(members: i64) -> Vec<PortSpec> {
    let mut ports: Vec<PortSpec> = (0..members)
        .map(|i| {
            spec(
                &format!("member_{i}"),
                PortKind::DeriveDestination,
                PortDirection::Input,
                i,
            )
        })
        .collect();
    ports.push(spec(
        "destination",
        PortKind::DeriveDestination,
        PortDirection::Output,
        members,
    ));
    ports
}

pub fn aggregate_ports(copies: i64) -> Vec<PortSpec> {
    let mut ports: Vec<PortSpec> = vec![spec(
        "source",
        PortKind::DeriveDestination,
        PortDirection::Input,
        0,
    )];
    for i in 0..copies {
        ports.push(spec(
            &format!("copy_{i}"),
            PortKind::DeriveDestination,
            PortDirection::Output,
            i + 1,
        ));
    }
    ports
}

/// The operator's members of a load-balance node: slots 1.., named as given.
pub fn members(names: &[&str]) -> Vec<orchestration::entities::surreal::node::LoadBalanceMember> {
    names
        .iter()
        .enumerate()
        .map(|(i, name)| orchestration::entities::surreal::node::LoadBalanceMember {
            slot: u32::try_from(i + 1).unwrap(),
            name: (*name).to_string(),
        })
        .collect()
}

/// The bundle ports of a distribute node's declared members (see [`members`]).
pub fn distribute_member_ports(names: &[&str]) -> Vec<PortSpec> {
    members(names)
        .iter()
        .enumerate()
        .map(|(i, m)| spec(&m.port_key(), PortKind::Bundle, PortDirection::Output, i as i64))
        .collect()
}

/// The bundle ports of an aggregate node's declared members.
pub fn aggregate_member_ports(names: &[&str]) -> Vec<PortSpec> {
    members(names)
        .iter()
        .enumerate()
        .map(|(i, m)| spec(&m.port_key(), PortKind::Bundle, PortDirection::Input, i as i64))
        .collect()
}

/// The single port of an export node inside its canvas.
pub fn export_ports(kind: PortKind, direction: CanvasExportAs) -> Vec<PortSpec> {
    vec![spec("export", kind, export_port_direction(direction), 0)]
}

/// The derived ports of an import node: one per `(export node key, kind,
/// direction)`, keyed by the export node's key (the builder uses keys as record
/// ids), direction mirrored, position by index.
pub fn import_ports(exports: &[(&str, PortKind, CanvasExportAs)]) -> Vec<PortSpec> {
    exports
        .iter()
        .enumerate()
        .map(|(i, (key, kind, direction))| {
            let mirrored = match export_port_direction(*direction) {
                PortDirection::Input => PortDirection::Output,
                PortDirection::Output => PortDirection::Input,
            };
            spec(key, *kind, mirrored, i as i64)
        })
        .collect()
}

pub fn export_spec(kind: PortKind, direction: CanvasExportAs) -> NodeSpec {
    NodeSpec::CanvasExport(CanvasExportConfig { kind, direction })
}

pub fn import_spec(canvas: &str) -> NodeSpec {
    NodeSpec::CanvasImport(CanvasImportConfig {
        canvas: ids::canvas_id(canvas),
    })
}

fn stub_canvas(key: &str) -> CanvasEntity {
    CanvasEntity {
        id: ids::canvas_id(key),
        name: key.to_string(),
        description: String::new(),
        generation: 0,
        derived_generation: 0,
    }
}

impl Builder {
    pub fn new(canvas: &str) -> Self {
        Self {
            root: ids::canvas_id(canvas),
            canvases: vec![stub_canvas(canvas)],
            current: ids::canvas_id(canvas),
            servers: Vec::new(),
            nodes: Vec::new(),
            edges: Vec::new(),
        }
    }

    /// Switches the builder to `key`, adding the canvas on first use.
    pub fn canvas(&mut self, key: &str) -> CanvasId {
        let id = ids::canvas_id(key);
        if !self
            .canvases
            .iter()
            .any(|c| ids::record_key(&c.id.0) == key)
        {
            self.canvases.push(stub_canvas(key));
        }
        self.current = id.clone();
        id
    }

    pub fn server(&mut self, key: &str) -> ServerId {
        let id = ids::server_id(key);
        self.servers.push(ServerEntity {
            id: id.clone(),
            canvas: self.current.clone(),
            name: key.to_string(),
            icon: String::new(),
            comment: String::new(),
            position: CanvasUiPosition { x: 0, y: 0 },
            ipv6_resolve: ServerIpv6Resolve::Tolerated,
            log_level: "info".to_string(),
            current_dynamic_refresh_key: None,
            refresh_key_generation: 0,
            watch_epoch: 0,
            session_lease_until: None,
            last_seen_at: None,
            last_health_report_at: None,
            health_status: orchestration::entities::surreal::health::ServerHealthStatus::Offline,
            override_v4: None,
            override_v6: None,
            extra_addresses: Vec::new(),
            reported_addresses: None,
            observed_address: None,
            observed_at: None,
            agent_version: None,
            agent_arch: None,
        });
        id
    }

    /// Pins an address on a server and hands back its id, so a pod can be placed
    /// on it with `pod(&server, port)`. The family is inferred from the literal;
    /// `key` is ignored (kept so existing call sites read unchanged).
    pub fn ip(&mut self, _key: &str, server: &ServerId, ip: &str) -> ServerId {
        let is_v4 = ip.parse::<std::net::Ipv4Addr>().is_ok();
        let server_key = ids::record_key(&server.0);
        if let Some(row) = self
            .servers
            .iter_mut()
            .find(|s| ids::record_key(&s.id.0) == server_key)
        {
            if is_v4 {
                row.override_v4 = Some(ip.to_string());
            } else {
                row.override_v6 = Some(ip.to_string());
            }
        }
        server.clone()
    }

    pub fn node(&mut self, key: &str, spec: NodeSpec, ports: Vec<PortSpec>) -> NodeId {
        self.named_node(key, key, spec, ports)
    }

    /// Like `node`, at a given y position (export ordering follows it).
    pub fn node_at_y(&mut self, key: &str, spec: NodeSpec, ports: Vec<PortSpec>, y: i64) -> NodeId {
        let id = self.named_node(key, key, spec, ports);
        if let Some(n) = self.nodes.last_mut() {
            n.node.position.y = y;
        }
        id
    }

    pub fn named_node(
        &mut self,
        key: &str,
        name: &str,
        spec: NodeSpec,
        ports: Vec<PortSpec>,
    ) -> NodeId {
        let id = ids::node_id(key);
        let ports = ports
            .into_iter()
            .map(|(port_key, kind, direction, position)| PortEntity {
                id: ids::port_id(&format!("{key}-{port_key}")),
                owner: id.clone(),
                kind,
                direction,
                key: port_key,
                position,
            })
            .collect();
        self.nodes.push(NodeWithPorts {
            node: NodeEntity {
                id: id.clone(),
                canvas: self.current.clone(),
                name: name.to_string(),
                comment: String::new(),
                spec,
                position: CanvasUiPosition { x: 0, y: 0 },
                lane: None,
            },
            ports,
        });
        id
    }

    /// Tags the most recently added node as a lane.
    pub fn lane(&mut self, lane: orchestration::entities::surreal::node::Lane) {
        if let Some(n) = self.nodes.last_mut() {
            n.node.lane = Some(lane);
        }
    }

    /// Connects an output port to an input port, both named `<node key>-<port key>`.
    pub fn connect(&mut self, source: &str, target: &str) -> EdgeConnectionId {
        let id = ids::edge_id(&format!("{source}->{target}"));
        self.edges.push(EdgeConnectionEntity {
            id: id.clone(),
            source: ids::port_id(source),
            target: ids::port_id(target),
        });
        id
    }

    /// Connects two ports verbatim, for the direction/kind violation tests.
    pub fn connect_raw(&mut self, source: PortId, target: PortId) -> EdgeConnectionId {
        let id = ids::edge_id(&format!(
            "{}->{}",
            ids::record_key(&source.0),
            ids::record_key(&target.0)
        ));
        self.edges.push(EdgeConnectionEntity {
            id: id.clone(),
            source,
            target,
        });
        id
    }

    /// The current canvas.
    pub fn canvas_id(&self) -> CanvasId {
        self.current.clone()
    }

    pub fn build(&self) -> CanvasTopology {
        CanvasTopology {
            root: self.root.clone(),
            canvases: self.canvases.clone(),
            servers: self.servers.clone(),
            nodes: self.nodes.clone(),
            edges: self.edges.clone(),
        }
    }
}

/// The port id the builder assigns to `<node key>-<port key>`.
pub fn port(node_key: &str, port_key: &str) -> PortId {
    ids::port_id(&format!("{node_key}-{port_key}"))
}
