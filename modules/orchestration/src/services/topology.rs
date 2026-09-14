//! Topology rules.
//!
//! Every mutation runs this checker on the topology it *would* produce
//! ([`CanvasTopology::project`]) and is rejected before anything is written. The
//! same checker answers `ValidateCanvas`, where warnings are reported alongside
//! errors.
//!
//! The unit of checking is a whole canvas tree. Import and export nodes are not
//! vertices of the traffic graph: [`Index::peer`] resolves *through* them, so a
//! path that crosses a canvas boundary is checked exactly like the flat graph it
//! stands for. What the boundary nodes contribute are the nesting rules
//! (`check_imports`) and the derived port shape of an import node.
//!
//! What passes here is storable, not necessarily derivable: a half-drawn chain
//! (a relay whose `listen` side is not fed yet, a load balancer with no connected
//! members) is deliberately allowed so an operator can save mid-edit. Derivation
//! reports such a pod in `invalid_pods` and leaves every other pod on the server
//! alone — see [`crate::services::derive`].
//!
//! A rule belongs here only when the shape is unsatisfiable no matter what else
//! the operator draws; anything that a later edit can complete belongs in the
//! per-pod report instead.

use crate::entities::surreal::connection::{EdgeConnectionEntity, EdgeConnectionId};
use crate::entities::surreal::node::{NodeEntity, NodeId, NodeSpec, NodeWithPorts, RelayProtocol};
use crate::entities::surreal::port::{PortDirection, PortEntity, PortId, PortKind};
use crate::entities::surreal::server::{ServerEntity, ServerId, ServerIpv6Resolve};
use crate::entities::surreal::topology::CanvasTopology;
use crate::services::node::{export_port_direction, import_port_layout};
use crate::utils::ids::record_key;
use std::collections::{HashMap, HashSet};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProblemSeverity {
    Error,
    Warning,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProblemKind {
    PortKindMismatch,
    EdgeDirectionInvalid,
    EdgeSelfNode,
    EdgeCrossCanvas,
    PortOversubscribed,
    PortShapeInvalid,
    Cycle,
    DuplicateListen,
    PodServerForeign,
    ExitDestinationInvalid,
    IpHashWithoutClientIp,
    PodPortUnconnected,
    RelaySameServer,
    DistributeSingleMember,
    CanvasImportSelf,
    CanvasImportAncestor,
    CanvasImportDuplicate,
    CanvasImportUnresolved,
    ServerNoAddress,
}

#[derive(Debug, Clone)]
pub struct TopologyProblem {
    pub severity: ProblemSeverity,
    pub kind: ProblemKind,
    pub message: String,
    pub nodes: Vec<NodeId>,
    pub edges: Vec<EdgeConnectionId>,
    pub ports: Vec<PortId>,
}

impl TopologyProblem {
    fn error(kind: ProblemKind, message: String) -> Self {
        Self {
            severity: ProblemSeverity::Error,
            kind,
            message,
            nodes: Vec::new(),
            edges: Vec::new(),
            ports: Vec::new(),
        }
    }

    fn warning(kind: ProblemKind, message: String) -> Self {
        Self {
            severity: ProblemSeverity::Warning,
            kind,
            message,
            nodes: Vec::new(),
            edges: Vec::new(),
            ports: Vec::new(),
        }
    }

    fn with_nodes(mut self, nodes: Vec<NodeId>) -> Self {
        self.nodes = nodes;
        self
    }

    fn with_edges(mut self, edges: Vec<EdgeConnectionId>) -> Self {
        self.edges = edges;
        self
    }

    fn with_ports(mut self, ports: Vec<PortId>) -> Self {
        self.ports = ports;
        self
    }
}

#[derive(Debug, thiserror::Error)]
#[error("{}", self.first.message)]
pub struct TopologyError {
    pub first: TopologyProblem,
}

/// One pending change to a topology, used to validate before writing.
#[derive(Debug, Clone)]
pub enum TopologyEdit {
    AddNode {
        node: Box<NodeEntity>,
        ports: Vec<PortEntity>,
    },
    RetireNode {
        node: NodeId,
    },
    /// Replaces a node's port list. Edges on a port whose key disappears are
    /// dropped; a port whose key survives is expected to carry its old id, so
    /// the edges on it stay attached (the write does the same).
    ReshapePorts {
        node: NodeId,
        ports: Vec<PortEntity>,
    },
    AddEdge {
        edge: EdgeConnectionEntity,
    },
    RetireEdge {
        edge: EdgeConnectionId,
    },
    SetServerSettings {
        server: ServerId,
        ipv6_resolve: ServerIpv6Resolve,
        log_level: String,
        override_v4: Option<String>,
        override_v6: Option<String>,
        extra_addresses: Vec<String>,
    },
}

impl CanvasTopology {
    /// The topology that results from applying `edits`, without writing anything.
    pub fn project(&self, edits: &[TopologyEdit]) -> CanvasTopology {
        let mut out = self.clone();
        for edit in edits {
            match edit {
                TopologyEdit::AddNode { node, ports } => out.nodes.push(NodeWithPorts {
                    node: (**node).clone(),
                    ports: ports.clone(),
                }),
                TopologyEdit::RetireNode { node } => {
                    let key = record_key(&node.0);
                    let ports: HashSet<String> = out
                        .nodes
                        .iter()
                        .filter(|n| record_key(&n.node.id.0) == key)
                        .flat_map(|n| n.ports.iter().map(|p| record_key(&p.id.0)))
                        .collect();
                    out.nodes.retain(|n| record_key(&n.node.id.0) != key);
                    out.edges.retain(|e| {
                        !ports.contains(&record_key(&e.source.0))
                            && !ports.contains(&record_key(&e.target.0))
                    });
                }
                TopologyEdit::ReshapePorts { node, ports } => {
                    let key = record_key(&node.0);
                    let Some(target) = out
                        .nodes
                        .iter_mut()
                        .find(|n| record_key(&n.node.id.0) == key)
                    else {
                        continue;
                    };
                    let kept: HashSet<&str> = ports.iter().map(|p| p.key.as_str()).collect();
                    let gone: HashSet<String> = target
                        .ports
                        .iter()
                        .filter(|p| !kept.contains(p.key.as_str()))
                        .map(|p| record_key(&p.id.0))
                        .collect();
                    target.ports = ports.clone();
                    out.edges.retain(|e| {
                        !gone.contains(&record_key(&e.source.0))
                            && !gone.contains(&record_key(&e.target.0))
                    });
                }
                TopologyEdit::AddEdge { edge } => out.edges.push(edge.clone()),
                TopologyEdit::RetireEdge { edge } => {
                    let key = record_key(&edge.0);
                    out.edges.retain(|e| record_key(&e.id.0) != key);
                }
                TopologyEdit::SetServerSettings {
                    server,
                    ipv6_resolve,
                    log_level,
                    override_v4,
                    override_v6,
                    extra_addresses,
                } => {
                    let key = record_key(&server.0);
                    for s in out.servers.iter_mut() {
                        if record_key(&s.id.0) == key {
                            s.ipv6_resolve = *ipv6_resolve;
                            s.log_level = log_level.clone();
                            s.override_v4 = override_v4.clone();
                            s.override_v6 = override_v6.clone();
                            s.extra_addresses = extra_addresses.clone();
                        }
                    }
                }
            }
        }
        out
    }
}

/// Errors first, then warnings.
pub fn analyze(topology: &CanvasTopology) -> Vec<TopologyProblem> {
    let index = Index::build(topology);
    let mut errors = Vec::new();
    let mut warnings = Vec::new();

    check_imports(&index, &mut errors);
    check_edges(&index, topology, &mut errors);
    check_port_shapes(&index, &mut errors);
    check_specs(&index, &mut errors);
    check_duplicate_listen(&index, &mut errors);
    check_cycles(&index, &mut errors);
    check_ip_hash(&index, &mut errors);
    check_server_addresses(&index, &mut warnings);
    check_warnings(&index, &mut warnings);

    errors.append(&mut warnings);
    errors
}

/// The error is boxed: it carries the offending problem with its node, edge, and
/// port lists, which is far larger than the success path.
pub fn ensure_valid(topology: &CanvasTopology) -> Result<(), Box<TopologyError>> {
    match analyze(topology)
        .into_iter()
        .find(|p| p.severity == ProblemSeverity::Error)
    {
        Some(first) => Err(Box::new(TopologyError { first })),
        None => Ok(()),
    }
}

/// How many import/export hops a lookup follows before giving up. A cycle of
/// imports is reported by `check_imports`; the guard only keeps lookups finite.
const MAX_BOUNDARY_HOPS: usize = 64;

/// Lookup tables over one topology snapshot, keyed by record key.
///
/// Shared with the deriver: [`Index::peer`] is the single definition of "what is
/// on the other side of this port", including across canvas boundaries.
pub(crate) struct Index<'a> {
    pub(crate) nodes: HashMap<String, &'a NodeWithPorts>,
    /// port key -> (port, owning node)
    pub(crate) ports: HashMap<String, (&'a PortEntity, &'a NodeWithPorts)>,
    pub(crate) servers: HashMap<String, &'a ServerEntity>,
    pub(crate) canvases: HashSet<String>,
    /// port key -> live edges touching it
    edges_by_port: HashMap<String, Vec<&'a EdgeConnectionEntity>>,
    /// canvas key -> the node importing it
    importer: HashMap<String, &'a NodeWithPorts>,
    /// canvas key -> its export nodes, in `(position.y, key)` order
    exports: HashMap<String, Vec<&'a NodeWithPorts>>,
}

impl<'a> Index<'a> {
    pub(crate) fn build(topology: &'a CanvasTopology) -> Self {
        let mut nodes = HashMap::new();
        let mut ports = HashMap::new();
        let mut importer = HashMap::new();
        let mut exports: HashMap<String, Vec<&NodeWithPorts>> = HashMap::new();
        for node in &topology.nodes {
            nodes.insert(record_key(&node.node.id.0), node);
            for port in &node.ports {
                ports.insert(record_key(&port.id.0), (port, node));
            }
            match &node.node.spec {
                NodeSpec::CanvasImport(cfg) => {
                    importer.insert(record_key(&cfg.canvas.0), node);
                }
                NodeSpec::CanvasExport(_) => exports
                    .entry(record_key(&node.node.canvas.0))
                    .or_default()
                    .push(node),
                _ => {}
            }
        }
        for list in exports.values_mut() {
            list.sort_by_cached_key(|n| (n.node.position.y, record_key(&n.node.id.0)));
        }
        let mut edges_by_port: HashMap<String, Vec<&EdgeConnectionEntity>> = HashMap::new();
        for edge in &topology.edges {
            edges_by_port
                .entry(record_key(&edge.source.0))
                .or_default()
                .push(edge);
            edges_by_port
                .entry(record_key(&edge.target.0))
                .or_default()
                .push(edge);
        }
        Self {
            nodes,
            ports,
            servers: topology
                .servers
                .iter()
                .map(|s| (record_key(&s.id.0), s))
                .collect(),
            canvases: topology
                .canvases
                .iter()
                .map(|c| record_key(&c.id.0))
                .collect(),
            edges_by_port,
            importer,
            exports,
        }
    }

    fn port(&self, id: &PortId) -> Option<(&'a PortEntity, &'a NodeWithPorts)> {
        self.ports.get(&record_key(&id.0)).copied()
    }

    /// The single live edge on a port, if any. Raw: does not look through
    /// boundaries.
    pub(crate) fn edge_on(&self, port: &PortEntity) -> Option<&'a EdgeConnectionEntity> {
        self.edges_by_port
            .get(&record_key(&port.id.0))
            .and_then(|edges| edges.first().copied())
    }

    /// The traffic node on the other side of a port's edge, looking through
    /// import/export boundaries.
    ///
    /// An import node's port stands for the export node of the same key in the
    /// imported canvas, whose single port continues the path; an export node
    /// stands for the mirrored port on the node importing its canvas. `None` when
    /// any hop is missing (no edge, unresolved import, export without importer).
    pub(crate) fn peer(&self, port: &PortEntity) -> Option<&'a NodeWithPorts> {
        let mut current: &PortEntity = port;
        for _ in 0..MAX_BOUNDARY_HOPS {
            let edge = self.edge_on(current)?;
            let other = if record_key(&edge.source.0) == record_key(&current.id.0) {
                &edge.target
            } else {
                &edge.source
            };
            let (far_port, far_node) = self.port(other)?;
            match &far_node.node.spec {
                NodeSpec::CanvasImport(cfg) => {
                    let export = self.nodes.get(&far_port.key).copied()?;
                    if !matches!(export.node.spec, NodeSpec::CanvasExport(_))
                        || record_key(&export.node.canvas.0) != record_key(&cfg.canvas.0)
                    {
                        return None;
                    }
                    current = export.ports.first()?;
                }
                NodeSpec::CanvasExport(_) => {
                    let importer = self.importer.get(&record_key(&far_node.node.canvas.0))?;
                    let key = record_key(&far_node.node.id.0);
                    current = importer.ports.iter().find(|p| p.key == key)?;
                }
                _ => return Some(far_node),
            }
        }
        None
    }

    pub(crate) fn port_by_key(&self, node: &'a NodeWithPorts, key: &str) -> Option<&'a PortEntity> {
        node.ports.iter().find(|p| p.key == key)
    }

    /// `[parent, grandparent, ...]` of a canvas, following import nodes upward.
    fn ancestors(&self, canvas: &str) -> Vec<String> {
        let mut out = Vec::new();
        let mut current = canvas.to_string();
        for _ in 0..MAX_BOUNDARY_HOPS {
            let Some(importer) = self.importer.get(&current) else {
                break;
            };
            current = record_key(&importer.node.canvas.0);
            out.push(current.clone());
        }
        out
    }

    /// The root of the tree a canvas belongs to (the canvas itself on a cycle).
    fn root_of(&self, canvas: &str) -> String {
        let chain = self.ancestors(canvas);
        if chain.len() >= MAX_BOUNDARY_HOPS {
            return canvas.to_string();
        }
        chain.last().cloned().unwrap_or_else(|| canvas.to_string())
    }
}

fn check_imports(index: &Index<'_>, out: &mut Vec<TopologyProblem>) {
    let mut by_target: HashMap<String, Vec<&NodeWithPorts>> = HashMap::new();
    for node in sorted_nodes(index) {
        let NodeSpec::CanvasImport(cfg) = &node.node.spec else {
            continue;
        };
        let target = record_key(&cfg.canvas.0);
        let own = record_key(&node.node.canvas.0);
        if target == own {
            out.push(
                TopologyProblem::error(
                    ProblemKind::CanvasImportSelf,
                    format!("node {} imports its own canvas", node.node.name),
                )
                .with_nodes(vec![node.node.id.clone()]),
            );
            continue;
        }
        if index.ancestors(&own).contains(&target) {
            out.push(
                TopologyProblem::error(
                    ProblemKind::CanvasImportAncestor,
                    format!("node {} imports an ancestor canvas", node.node.name),
                )
                .with_nodes(vec![node.node.id.clone()]),
            );
        }
        if !index.canvases.contains(&target) {
            out.push(
                TopologyProblem::error(
                    ProblemKind::CanvasImportUnresolved,
                    format!(
                        "node {} imports a canvas that does not exist",
                        node.node.name
                    ),
                )
                .with_nodes(vec![node.node.id.clone()]),
            );
        }
        by_target.entry(target).or_default().push(node);
    }
    let mut duplicates: Vec<(String, Vec<&NodeWithPorts>)> = by_target
        .into_iter()
        .filter(|(_, nodes)| nodes.len() > 1)
        .collect();
    duplicates.sort_by(|a, b| a.0.cmp(&b.0));
    for (target, nodes) in duplicates {
        out.push(
            TopologyProblem::error(
                ProblemKind::CanvasImportDuplicate,
                format!("canvas {target} is imported more than once"),
            )
            .with_nodes(nodes.iter().map(|n| n.node.id.clone()).collect()),
        );
    }
}

fn check_edges(index: &Index<'_>, topology: &CanvasTopology, out: &mut Vec<TopologyProblem>) {
    let mut usage: HashMap<String, (usize, PortId)> = HashMap::new();
    for edge in &topology.edges {
        let edge_key = record_key(&edge.id.0);
        for endpoint in [&edge.source, &edge.target] {
            let entry = usage
                .entry(record_key(&endpoint.0))
                .or_insert((0, endpoint.clone()));
            entry.0 = entry.0.saturating_add(1);
        }

        let (Some((source, source_node)), Some((target, target_node))) =
            (index.port(&edge.source), index.port(&edge.target))
        else {
            out.push(
                TopologyProblem::error(
                    ProblemKind::EdgeDirectionInvalid,
                    format!("edge {edge_key} must connect an output port to an input port"),
                )
                .with_edges(vec![edge.id.clone()]),
            );
            continue;
        };
        if source.direction != PortDirection::Output || target.direction != PortDirection::Input {
            out.push(
                TopologyProblem::error(
                    ProblemKind::EdgeDirectionInvalid,
                    format!("edge {edge_key} must connect an output port to an input port"),
                )
                .with_edges(vec![edge.id.clone()])
                .with_ports(vec![edge.source.clone(), edge.target.clone()]),
            );
            continue;
        }
        if source.kind != target.kind {
            out.push(
                TopologyProblem::error(
                    ProblemKind::PortKindMismatch,
                    format!(
                        "edge {edge_key} connects {} to {}",
                        kind_name(source.kind),
                        kind_name(target.kind)
                    ),
                )
                .with_edges(vec![edge.id.clone()])
                .with_ports(vec![edge.source.clone(), edge.target.clone()]),
            );
        }
        if record_key(&source_node.node.id.0) == record_key(&target_node.node.id.0) {
            out.push(
                TopologyProblem::error(
                    ProblemKind::EdgeSelfNode,
                    format!(
                        "edge {edge_key} connects node {} to itself",
                        source_node.node.name
                    ),
                )
                .with_edges(vec![edge.id.clone()])
                .with_nodes(vec![source_node.node.id.clone()]),
            );
        }
        if record_key(&source_node.node.canvas.0) != record_key(&target_node.node.canvas.0) {
            out.push(
                TopologyProblem::error(
                    ProblemKind::EdgeCrossCanvas,
                    format!("edge {edge_key} crosses canvas boundaries"),
                )
                .with_edges(vec![edge.id.clone()]),
            );
        }
    }

    let mut oversubscribed: Vec<_> = usage
        .into_iter()
        .filter(|(_, (count, _))| *count > 1)
        .collect();
    oversubscribed.sort_by(|a, b| a.0.cmp(&b.0));
    for (port_key, (count, port_id)) in oversubscribed {
        let name = index
            .ports
            .get(&port_key)
            .map(|(_, node)| node.node.name.clone())
            .unwrap_or_default();
        out.push(
            TopologyProblem::error(
                ProblemKind::PortOversubscribed,
                format!("port {port_key} of node {name} carries {count} edges"),
            )
            .with_ports(vec![port_id]),
        );
    }
}

/// `(kind, direction, exact count or "at least 2")` a spec's ports must match.
/// `None` for an import node, whose ports are checked against its target's
/// exports instead.
fn expected_ports(spec: &NodeSpec) -> Option<Vec<(PortKind, PortDirection, Multiplicity)>> {
    use Multiplicity::{AtLeastTwo, One};
    use PortDirection::{Input, Output};
    use PortKind::{DeriveDestination, DeriveListen};
    Some(match spec {
        NodeSpec::Pod(_) => vec![(DeriveListen, Output, One), (DeriveDestination, Input, One)],
        NodeSpec::Entry(_) => vec![(DeriveListen, Input, One)],
        NodeSpec::Relay(_) => vec![(DeriveListen, Input, One), (DeriveDestination, Output, One)],
        NodeSpec::Exit(_) => vec![(DeriveDestination, Output, One)],
        NodeSpec::LoadBalanceDistribute(_) => vec![
            (DeriveDestination, Input, AtLeastTwo),
            (DeriveDestination, Output, One),
        ],
        NodeSpec::LoadBalanceAggregate(_) => vec![
            (DeriveDestination, Input, One),
            (DeriveDestination, Output, AtLeastTwo),
        ],
        NodeSpec::CanvasExport(cfg) => {
            vec![(cfg.kind, export_port_direction(cfg.direction), One)]
        }
        NodeSpec::CanvasImport(_) => return None,
    })
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Multiplicity {
    One,
    AtLeastTwo,
}

fn check_port_shapes(index: &Index<'_>, out: &mut Vec<TopologyProblem>) {
    for node in sorted_nodes(index) {
        let ok = match &node.node.spec {
            NodeSpec::CanvasImport(cfg) => {
                let target = record_key(&cfg.canvas.0);
                if !index.canvases.contains(&target) {
                    continue; // reported by `check_imports`
                }
                let exports: Vec<&NodeEntity> = index
                    .exports
                    .get(&target)
                    .map(|list| list.iter().map(|n| &n.node).collect())
                    .unwrap_or_default();
                let expected: HashSet<(String, PortKind, PortDirection)> =
                    import_port_layout(&exports)
                        .into_iter()
                        .map(|p| (p.key, p.kind, p.direction))
                        .collect();
                let actual: HashSet<(String, PortKind, PortDirection)> = node
                    .ports
                    .iter()
                    .map(|p| (p.key.clone(), p.kind, p.direction))
                    .collect();
                expected == actual && node.ports.len() == actual.len()
            }
            spec => {
                let Some(expected) = expected_ports(spec) else {
                    continue;
                };
                let mut counts: HashMap<(PortKind, PortDirection), usize> = HashMap::new();
                for port in &node.ports {
                    let slot = counts.entry((port.kind, port.direction)).or_insert(0);
                    *slot = slot.saturating_add(1);
                }
                let mut ok = counts.len() == expected.len();
                for (kind, direction, multiplicity) in expected {
                    let count = counts.get(&(kind, direction)).copied().unwrap_or(0);
                    ok &= match multiplicity {
                        Multiplicity::One => count == 1,
                        Multiplicity::AtLeastTwo => count >= 2,
                    };
                }
                ok
            }
        };
        if !ok {
            out.push(
                TopologyProblem::error(
                    ProblemKind::PortShapeInvalid,
                    format!("node {} has invalid ports for its spec", node.node.name),
                )
                .with_nodes(vec![node.node.id.clone()]),
            );
        }
    }
}

fn check_specs(index: &Index<'_>, out: &mut Vec<TopologyProblem>) {
    for node in sorted_nodes(index) {
        match &node.node.spec {
            // A pod may listen on any server of its tree: the parent sees the
            // child as a black box, but the tree is derived as one graph.
            NodeSpec::Pod(cfg) => {
                let server_canvas = index
                    .servers
                    .get(&record_key(&cfg.server.0))
                    .map(|server| record_key(&server.canvas.0));
                let foreign = match server_canvas {
                    None => true,
                    Some(canvas) => {
                        index.root_of(&canvas) != index.root_of(&record_key(&node.node.canvas.0))
                    }
                };
                if foreign {
                    out.push(
                        TopologyProblem::error(
                            ProblemKind::PodServerForeign,
                            format!(
                                "pod {} is placed on a server outside this canvas tree",
                                node.node.name
                            ),
                        )
                        .with_nodes(vec![node.node.id.clone()]),
                    );
                }
                for (label, value) in [("bind_ip", &cfg.bind_ip), ("advertise_ip", &cfg.advertise_ip)] {
                    if let Some(raw) = value
                        && raw.parse::<std::net::IpAddr>().is_err()
                    {
                        out.push(
                            TopologyProblem::error(
                                ProblemKind::PortShapeInvalid,
                                format!(
                                    "pod {} {label} '{raw}' is not an IP address",
                                    node.node.name
                                ),
                            )
                            .with_nodes(vec![node.node.id.clone()]),
                        );
                    }
                }
            }
            // An exit whose destination is not filled in yet is the half-drawn case
            // this module documents: a later edit completes it, and derivation
            // already refuses to publish the pod behind it (`invalid_pods`). Only a
            // destination that was actually written and cannot ever parse is an
            // error.
            NodeSpec::Exit(cfg) if cfg.destination.is_empty() => {
                out.push(
                    TopologyProblem::warning(
                        ProblemKind::ExitDestinationInvalid,
                        format!("exit {} has no destination yet", node.node.name),
                    )
                    .with_nodes(vec![node.node.id.clone()]),
                );
            }
            NodeSpec::Exit(cfg) if guru_worker_config::Remote::parse(&cfg.destination).is_err() => {
                out.push(
                    TopologyProblem::error(
                        ProblemKind::ExitDestinationInvalid,
                        format!(
                            "exit {} destination '{}' is not host:port",
                            node.node.name, cfg.destination
                        ),
                    )
                    .with_nodes(vec![node.node.id.clone()]),
                );
            }
            _ => {}
        }
    }
}

/// QUIC iff the pod's listen output is consumed by a QUIC relay.
fn pod_transport(index: &Index<'_>, pod: &NodeWithPorts) -> &'static str {
    let quic = index
        .port_by_key(pod, "listen")
        .and_then(|port| index.peer(port))
        .map(|peer| {
            matches!(&peer.node.spec, NodeSpec::Relay(cfg) if cfg.protocol == RelayProtocol::Quic)
        })
        .unwrap_or(false);
    if quic { "quic" } else { "tcp" }
}

/// Two pods of one server may not claim one socket. A wildcard bind (`None`,
/// `0.0.0.0` or `::`) covers every address, so it clashes with any bind on the
/// same port and transport; two distinct literal addresses may share a port.
fn check_duplicate_listen(index: &Index<'_>, out: &mut Vec<TopologyProblem>) {
    type Bind = Option<std::net::IpAddr>;
    /// `(server key, port, transport)` -> the pods already seen on that socket.
    type Claims<'a> = HashMap<(String, u16, &'static str), Vec<(Bind, &'a NodeWithPorts)>>;
    let mut seen: Claims<'_> = HashMap::new();
    for node in sorted_nodes(index) {
        let NodeSpec::Pod(cfg) = &node.node.spec else {
            continue;
        };
        let transport = pod_transport(index, node);
        // An unparsable bind is reported by `check_specs`; treat it as a wildcard.
        let bind: Bind = match cfg.bind_ip() {
            Ok(Some(ip)) if !ip.is_unspecified() => Some(ip),
            _ => None,
        };
        let key = (record_key(&cfg.server.0), cfg.port, transport);
        let entry = seen.entry(key).or_default();
        let clash = entry.iter().find(|(other, _)| match (other, &bind) {
            (None, _) | (_, None) => true,
            (Some(a), Some(b)) => a == b,
        });
        if let Some((_, first)) = clash {
            out.push(
                TopologyProblem::error(
                    ProblemKind::DuplicateListen,
                    format!(
                        "pods {} and {} both listen on {} ({})",
                        first.node.name,
                        node.node.name,
                        cfg.listen_display(),
                        transport
                    ),
                )
                .with_nodes(vec![first.node.id.clone(), node.node.id.clone()]),
            );
        }
        entry.push((bind, node));
    }
}

/// A server nobody can dial yet: its worker has not registered and no address was
/// pinned. Its pods still derive their listeners; only the pods on *other*
/// servers that dial it stay invalid until an address is known.
fn check_server_addresses(index: &Index<'_>, out: &mut Vec<TopologyProblem>) {
    let mut pods_by_server: HashMap<String, Vec<NodeId>> = HashMap::new();
    for node in sorted_nodes(index) {
        if let NodeSpec::Pod(cfg) = &node.node.spec {
            pods_by_server
                .entry(record_key(&cfg.server.0))
                .or_default()
                .push(node.node.id.clone());
        }
    }
    let mut keys: Vec<&String> = index.servers.keys().collect();
    keys.sort();
    for key in keys {
        let Some(server) = index.servers.get(key) else {
            continue;
        };
        if server.effective_address().is_some() {
            continue;
        }
        out.push(
            TopologyProblem::warning(
                ProblemKind::ServerNoAddress,
                format!(
                    "server {} has no address yet: wait for its worker to register, or pin one",
                    server.name
                ),
            )
            .with_nodes(pods_by_server.remove(key).unwrap_or_default()),
        );
    }
}

/// Whether a node is a vertex of the traffic graph (boundary nodes are not).
fn is_traffic_node(node: &NodeWithPorts) -> bool {
    !matches!(
        node.node.spec,
        NodeSpec::CanvasImport(_) | NodeSpec::CanvasExport(_)
    )
}

/// `consumer -> producers`: a node depends on whatever feeds its inputs, and a relay
/// additionally depends on the pod it dials into. Boundaries are looked through.
fn dependency_graph(index: &Index<'_>) -> HashMap<String, Vec<String>> {
    let mut graph: HashMap<String, Vec<String>> = HashMap::new();
    for node in index.nodes.values().filter(|n| is_traffic_node(n)) {
        let key = record_key(&node.node.id.0);
        let deps = graph.entry(key).or_default();
        for port in node
            .ports
            .iter()
            .filter(|p| p.direction == PortDirection::Input)
        {
            if let Some(peer) = index.peer(port) {
                deps.push(record_key(&peer.node.id.0));
            }
        }
    }
    for node in index.nodes.values() {
        if !matches!(node.node.spec, NodeSpec::Relay(_)) {
            continue;
        }
        // The relay's listen input is fed by the pod it dials; that pod's destination
        // subtree continues this traffic path.
        if let Some(listen) = index.port_by_key(node, "listen")
            && let Some(pod) = index.peer(listen)
        {
            graph
                .entry(record_key(&node.node.id.0))
                .or_default()
                .push(record_key(&pod.node.id.0));
        }
    }
    graph
}

fn check_cycles(index: &Index<'_>, out: &mut Vec<TopologyProblem>) {
    let graph = dependency_graph(index);
    let mut colour: HashMap<&str, u8> = HashMap::new(); // 0 = open, 1 = done
    let mut reported: HashSet<String> = HashSet::new();
    let mut keys: Vec<&String> = graph.keys().collect();
    keys.sort();

    for start in keys {
        if colour.get(start.as_str()).copied().unwrap_or(0) == 1 {
            continue;
        }
        // Iterative DFS carrying the current path, so a back edge names its cycle.
        let mut stack: Vec<(&str, usize)> = vec![(start.as_str(), 0)];
        let mut path: Vec<&str> = vec![start.as_str()];
        let mut on_path: HashSet<&str> = HashSet::from([start.as_str()]);
        while let Some((node, edge_index)) = stack.pop() {
            let deps = graph.get(node).map(Vec::as_slice).unwrap_or(&[]);
            if edge_index >= deps.len() {
                colour.insert(node, 1);
                path.pop();
                on_path.remove(node);
                continue;
            }
            stack.push((node, edge_index.saturating_add(1)));
            let next = deps[edge_index].as_str();
            if on_path.contains(next) {
                let start_at = path.iter().position(|n| *n == next).unwrap_or(0);
                let mut cycle: Vec<String> =
                    path[start_at..].iter().map(|n| (*n).to_string()).collect();
                cycle.sort();
                let signature = cycle.join(",");
                if reported.insert(signature) {
                    let named = index.nodes.get(&cycle[0]).map(|n| n.node.name.clone());
                    out.push(
                        TopologyProblem::error(
                            ProblemKind::Cycle,
                            format!("cycle through node {}", named.unwrap_or_default()),
                        )
                        .with_nodes(
                            cycle
                                .iter()
                                .filter_map(|k| index.nodes.get(k))
                                .map(|n| n.node.id.clone())
                                .collect(),
                        ),
                    );
                }
                continue;
            }
            if colour.get(next).copied().unwrap_or(0) == 1 {
                continue;
            }
            stack.push((next, 0));
            path.push(next);
            on_path.insert(next);
        }
    }
}

/// Whether the destination subtree behind this node hashes on the client address.
fn iphash_reachable(index: &Index<'_>, node: &NodeWithPorts, depth: usize) -> bool {
    if depth > 64 {
        return false; // a cycle is reported separately
    }
    match &node.node.spec {
        NodeSpec::LoadBalanceDistribute(cfg) => {
            if cfg.mode == crate::entities::surreal::node::LoadBalanceMode::IpHash {
                return true;
            }
            node.ports
                .iter()
                .filter(|p| p.direction == PortDirection::Input)
                .filter_map(|p| index.peer(p))
                .any(|member| iphash_reachable(index, member, depth.saturating_add(1)))
        }
        NodeSpec::LoadBalanceAggregate(_) => node
            .ports
            .iter()
            .find(|p| p.direction == PortDirection::Input)
            .and_then(|p| index.peer(p))
            .map(|source| iphash_reachable(index, source, depth.saturating_add(1)))
            .unwrap_or(false),
        _ => false,
    }
}

fn check_ip_hash(index: &Index<'_>, out: &mut Vec<TopologyProblem>) {
    for pod in sorted_nodes(index) {
        if !matches!(pod.node.spec, NodeSpec::Pod(_)) {
            continue;
        }
        let listen_consumer = index.port_by_key(pod, "listen").and_then(|p| index.peer(p));
        let client_ip_known = match listen_consumer.map(|c| &c.node.spec) {
            Some(NodeSpec::Entry(cfg)) => cfg.receive_proxy_protocol.is_some(),
            // A relay hop always carries PROXY v2, and the relay itself is exempt.
            Some(NodeSpec::Relay(_)) => true,
            _ => true,
        };
        if client_ip_known {
            continue;
        }
        let uses_hash = index
            .port_by_key(pod, "destination")
            .and_then(|p| index.peer(p))
            .map(|producer| iphash_reachable(index, producer, 0))
            .unwrap_or(false);
        if uses_hash {
            out.push(
                TopologyProblem::error(
                    ProblemKind::IpHashWithoutClientIp,
                    format!(
                        "load balancer under pod {} uses ip_hash but the client address is unknown",
                        pod.node.name
                    ),
                )
                .with_nodes(vec![pod.node.id.clone()]),
            );
        }
    }
}

fn check_warnings(index: &Index<'_>, out: &mut Vec<TopologyProblem>) {
    for node in sorted_nodes(index) {
        match &node.node.spec {
            // A pod with an unconnected port is simply not derived: every server
            // starts with its transport pods unwired, so that is the normal state,
            // not a warning.
            NodeSpec::Pod(_) => {}
            NodeSpec::Relay(_) => {
                let listen_pod = index
                    .port_by_key(node, "listen")
                    .and_then(|p| index.peer(p));
                let target_pod = index
                    .port_by_key(node, "destination")
                    .and_then(|p| index.peer(p));
                if let (Some(a), Some(b)) = (listen_pod, target_pod)
                    && let (Some(sa), Some(sb)) = (pod_server(a), pod_server(b))
                    && sa == sb
                {
                    out.push(
                        TopologyProblem::warning(
                            ProblemKind::RelaySameServer,
                            format!("relay {} hops within one server", node.node.name),
                        )
                        .with_nodes(vec![node.node.id.clone()]),
                    );
                }
            }
            NodeSpec::LoadBalanceDistribute(_) => {
                let connected = node
                    .ports
                    .iter()
                    .filter(|p| p.direction == PortDirection::Input)
                    .filter(|p| index.edge_on(p).is_some())
                    .count();
                if connected == 1 {
                    out.push(
                        TopologyProblem::warning(
                            ProblemKind::DistributeSingleMember,
                            format!("load balancer {} has a single member", node.node.name),
                        )
                        .with_nodes(vec![node.node.id.clone()]),
                    );
                }
            }
            _ => {}
        }
    }
}

/// The server key a pod listens on.
fn pod_server(pod: &NodeWithPorts) -> Option<String> {
    let NodeSpec::Pod(cfg) = &pod.node.spec else {
        return None;
    };
    Some(record_key(&cfg.server.0))
}

/// Nodes in record-key order, so problems are reported deterministically.
fn sorted_nodes<'a>(index: &Index<'a>) -> Vec<&'a NodeWithPorts> {
    let mut keys: Vec<&String> = index.nodes.keys().collect();
    keys.sort();
    keys.into_iter()
        .filter_map(|k| index.nodes.get(k))
        .copied()
        .collect()
}

fn kind_name(kind: PortKind) -> &'static str {
    match kind {
        PortKind::DeriveListen => "derive_listen",
        PortKind::DeriveDestination => "derive_destination",
    }
}
