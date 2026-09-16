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

use crate::entities::db::canvas::CanvasId;
use crate::entities::db::connection::{EdgeConnectionEntity, EdgeConnectionId};
use crate::entities::db::node::{NodeEntity, NodeId, NodeSpec, NodeWithPorts, RelayProtocol};
use crate::entities::db::port::{PortDirection, PortEntity, PortId, PortKind};
use crate::entities::db::server::{ServerEntity, ServerId, ServerIpv6Resolve};
use crate::entities::db::topology::CanvasTopology;
use crate::services::node::{export_port_direction, import_port_layout};
use crate::services::universal::{self, UniversalPort};
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
    /// A distributor's `chan:` port is connected to something other than the
    /// `destination` of the pod it is named after.
    ChannelTargetNotPod,
    /// A bundle edge between nodes that cannot be bundled, or on mismatched keys.
    BundleEdgeInvalid,
    BundleCycle,
    /// A channel lands on a universal pod that bundles on to nothing (warning).
    ChannelNoExit,
    /// A channel on a distributor that bundles to no server (warning).
    ChannelNoTransit,
    /// The stored lanes differ from the expansion; the next universal edit
    /// regenerates them (warning).
    LanesStale,
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
    pub(crate) fn error(kind: ProblemKind, message: String) -> Self {
        Self {
            severity: ProblemSeverity::Error,
            kind,
            message,
            nodes: Vec::new(),
            edges: Vec::new(),
            ports: Vec::new(),
        }
    }

    pub(crate) fn warning(kind: ProblemKind, message: String) -> Self {
        Self {
            severity: ProblemSeverity::Warning,
            kind,
            message,
            nodes: Vec::new(),
            edges: Vec::new(),
            ports: Vec::new(),
        }
    }

    pub(crate) fn with_nodes(mut self, nodes: Vec<NodeId>) -> Self {
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
    /// Rewrites a node's spec, leaving its ports alone.
    SetSpec {
        node: NodeId,
        spec: NodeSpec,
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
                    let ports: HashSet<&PortId> = out
                        .nodes
                        .iter()
                        .filter(|n| n.node.id == *node)
                        .flat_map(|n| n.ports.iter().map(|p| &p.id))
                        .collect();
                    let doomed = |e: &EdgeConnectionEntity| {
                        ports.contains(&e.source) || ports.contains(&e.target)
                    };
                    out.edges.retain(|e| !doomed(e));
                    out.nodes.retain(|n| n.node.id != *node);
                }
                TopologyEdit::ReshapePorts { node, ports } => {
                    let Some(target) = out.nodes.iter_mut().find(|n| n.node.id == *node) else {
                        continue;
                    };
                    let kept: HashSet<&str> = ports.iter().map(|p| p.key.as_str()).collect();
                    let gone: HashSet<PortId> = target
                        .ports
                        .iter()
                        .filter(|p| !kept.contains(p.key.as_str()))
                        .map(|p| p.id.clone())
                        .collect();
                    target.ports = ports.clone();
                    out.edges
                        .retain(|e| !gone.contains(&e.source) && !gone.contains(&e.target));
                }
                TopologyEdit::AddEdge { edge } => out.edges.push(edge.clone()),
                TopologyEdit::RetireEdge { edge } => out.edges.retain(|e| e.id != *edge),
                TopologyEdit::SetSpec { node, spec } => {
                    if let Some(target) = out.nodes.iter_mut().find(|n| n.node.id == *node) {
                        target.node.spec = spec.clone();
                    }
                }
                TopologyEdit::SetServerSettings {
                    server,
                    ipv6_resolve,
                    log_level,
                    override_v4,
                    override_v6,
                    extra_addresses,
                } => {
                    for s in out.servers.iter_mut() {
                        if s.id == *server {
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
    check_universal(topology, &mut errors, &mut warnings);

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

/// Lookup tables over one topology snapshot, keyed by row id.
///
/// Shared with the deriver: [`Index::peer`] is the single definition of "what is
/// on the other side of this port", including across canvas boundaries.
pub(crate) struct Index<'a> {
    pub(crate) nodes: HashMap<&'a NodeId, &'a NodeWithPorts>,
    pub(crate) ports: HashMap<&'a PortId, (&'a PortEntity, &'a NodeWithPorts)>,
    pub(crate) servers: HashMap<&'a ServerId, &'a ServerEntity>,
    pub(crate) canvases: HashSet<&'a CanvasId>,
    /// port -> live edges touching it
    edges_by_port: HashMap<&'a PortId, Vec<&'a EdgeConnectionEntity>>,
    /// canvas -> the node importing it
    importer: HashMap<&'a CanvasId, &'a NodeWithPorts>,
    /// canvas -> its export nodes, in `(position.y, id)` order
    exports: HashMap<&'a CanvasId, Vec<&'a NodeWithPorts>>,
}

impl<'a> Index<'a> {
    pub(crate) fn build(topology: &'a CanvasTopology) -> Self {
        let mut nodes = HashMap::new();
        let mut ports = HashMap::new();
        let mut importer = HashMap::new();
        let mut exports: HashMap<&CanvasId, Vec<&NodeWithPorts>> = HashMap::new();
        for node in &topology.nodes {
            nodes.insert(&node.node.id, node);
            for port in &node.ports {
                ports.insert(&port.id, (port, node));
            }
            match &node.node.spec {
                NodeSpec::CanvasImport(cfg) => {
                    importer.insert(&cfg.canvas, node);
                }
                NodeSpec::CanvasExport(_) => {
                    exports.entry(&node.node.canvas).or_default().push(node)
                }
                _ => {}
            }
        }
        for list in exports.values_mut() {
            list.sort_by_key(|n| (n.node.position.y, &n.node.id));
        }
        let mut edges_by_port: HashMap<&PortId, Vec<&EdgeConnectionEntity>> = HashMap::new();
        for edge in &topology.edges {
            edges_by_port.entry(&edge.source).or_default().push(edge);
            edges_by_port.entry(&edge.target).or_default().push(edge);
        }
        Self {
            nodes,
            ports,
            servers: topology.servers.iter().map(|s| (&s.id, s)).collect(),
            canvases: topology.canvases.iter().map(|c| &c.id).collect(),
            edges_by_port,
            importer,
            exports,
        }
    }

    pub(crate) fn port(&self, id: &PortId) -> Option<(&'a PortEntity, &'a NodeWithPorts)> {
        self.ports.get(id).copied()
    }

    /// The single live edge on a port, if any. Raw: does not look through
    /// boundaries.
    pub(crate) fn edge_on(&self, port: &PortEntity) -> Option<&'a EdgeConnectionEntity> {
        self.edges_by_port
            .get(&port.id)
            .and_then(|edges| edges.first().copied())
    }

    /// The traffic node on the other side of a port's edge, looking through
    /// import/export boundaries.
    ///
    /// An import node's port stands for the export node of the same key in the
    /// imported canvas, whose single port continues the path; an export node
    /// stands for the mirrored port on the node importing its canvas. `None` when
    /// any hop is missing (no edge, unresolved import, export without importer).
    ///
    /// A load-balance node's channel port pair (`chan:x` / `lane:x`) is looked
    /// through the same way: the operator's edge on one side continues on the
    /// generated edge on the other; arriving on one of its hand-drawn ports is
    /// arriving at the node. A universal pod's bundle ports end the walk:
    /// bundles are not traffic.
    pub(crate) fn peer(&self, port: &PortEntity) -> Option<&'a NodeWithPorts> {
        let mut current: &PortEntity = port;
        for _ in 0..MAX_BOUNDARY_HOPS {
            let edge = self.edge_on(current)?;
            let other = if edge.source == current.id {
                &edge.target
            } else {
                &edge.source
            };
            let (far_port, far_node) = self.port(other)?;
            match &far_node.node.spec {
                NodeSpec::CanvasImport(cfg) => {
                    // A mirrored port is keyed by the export node's id.
                    let export = self
                        .nodes
                        .get(&NodeId::from_key(far_port.key.as_str()))
                        .copied()?;
                    if !matches!(export.node.spec, NodeSpec::CanvasExport(_))
                        || export.node.canvas != cfg.canvas
                    {
                        return None;
                    }
                    current = export.ports.first()?;
                }
                NodeSpec::CanvasExport(_) => {
                    let importer = self.importer.get(&far_node.node.canvas)?;
                    current = importer
                        .ports
                        .iter()
                        .find(|p| p.key == far_node.node.id.as_str())?;
                }
                NodeSpec::LoadBalanceDistribute(_) | NodeSpec::LoadBalanceAggregate(_) => {
                    match universal::channel_twin(&far_port.key) {
                        Some(twin) => current = far_node.ports.iter().find(|p| p.key == twin)?,
                        None => return Some(far_node),
                    }
                }
                // A universal pod's channel pair is looked through like a
                // distribute node's; its bundle ports end the walk.
                NodeSpec::UniversalPod(_) => {
                    let twin = universal::channel_twin(&far_port.key)?;
                    current = far_node.ports.iter().find(|p| p.key == twin)?;
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
    fn ancestors(&self, canvas: &'a CanvasId) -> Vec<&'a CanvasId> {
        let mut out = Vec::new();
        let mut current = canvas;
        for _ in 0..MAX_BOUNDARY_HOPS {
            let Some(importer) = self.importer.get(current) else {
                break;
            };
            current = &importer.node.canvas;
            out.push(current);
        }
        out
    }

    /// The root of the tree a canvas belongs to (the canvas itself on a cycle).
    fn root_of(&self, canvas: &'a CanvasId) -> &'a CanvasId {
        let chain = self.ancestors(canvas);
        if chain.len() >= MAX_BOUNDARY_HOPS {
            return canvas;
        }
        chain.last().copied().unwrap_or(canvas)
    }
}

fn check_imports(index: &Index<'_>, out: &mut Vec<TopologyProblem>) {
    let mut by_target: HashMap<&CanvasId, Vec<&NodeWithPorts>> = HashMap::new();
    for node in sorted_nodes(index) {
        let NodeSpec::CanvasImport(cfg) = &node.node.spec else {
            continue;
        };
        let target = &cfg.canvas;
        let own = &node.node.canvas;
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
        if index.ancestors(own).contains(&target) {
            out.push(
                TopologyProblem::error(
                    ProblemKind::CanvasImportAncestor,
                    format!("node {} imports an ancestor canvas", node.node.name),
                )
                .with_nodes(vec![node.node.id.clone()]),
            );
        }
        if !index.canvases.contains(target) {
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
    let mut duplicates: Vec<(&CanvasId, Vec<&NodeWithPorts>)> = by_target
        .into_iter()
        .filter(|(_, nodes)| nodes.len() > 1)
        .collect();
    duplicates.sort_by(|a, b| a.0.cmp(b.0));
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
    let mut usage: HashMap<&PortId, usize> = HashMap::new();
    // Bundle edges by (source node, target node): a pair is bundled once.
    let mut bundled: HashMap<(&NodeId, &NodeId), Vec<&EdgeConnectionEntity>> = HashMap::new();
    for edge in &topology.edges {
        let edge_key = &edge.id;
        for endpoint in [&edge.source, &edge.target] {
            let count = usage.entry(endpoint).or_insert(0);
            *count = count.saturating_add(1);
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
        if source.kind == PortKind::Bundle {
            if let Some(message) = bundle_edge_problem(source, source_node, target, target_node) {
                out.push(
                    TopologyProblem::error(
                        ProblemKind::BundleEdgeInvalid,
                        format!("edge {edge_key} {message}"),
                    )
                    .with_edges(vec![edge.id.clone()])
                    .with_nodes(vec![
                        source_node.node.id.clone(),
                        target_node.node.id.clone(),
                    ]),
                );
            }
            bundled
                .entry((&source_node.node.id, &target_node.node.id))
                .or_default()
                .push(edge);
        }
        if matches!(
            source_node.node.spec,
            NodeSpec::LoadBalanceDistribute(_) | NodeSpec::UniversalPod(_)
        ) && let Some(UniversalPort::Chan(pod)) = universal::parse_port_key(&source.key)
            && (target.key != "destination"
                || !matches!(target_node.node.spec, NodeSpec::Pod(_))
                || target_node.node.id.as_str() != pod)
        {
            out.push(
                TopologyProblem::error(
                    ProblemKind::ChannelTargetNotPod,
                    format!(
                        "edge {edge_key}: channel port {} of {} must feed the destination of pod {pod}",
                        source.key, source_node.node.name
                    ),
                )
                .with_edges(vec![edge.id.clone()])
                .with_nodes(vec![source_node.node.id.clone()]),
            );
        }
        if source_node.node.id == target_node.node.id {
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
        if source_node.node.canvas != target_node.node.canvas {
            out.push(
                TopologyProblem::error(
                    ProblemKind::EdgeCrossCanvas,
                    format!("edge {edge_key} crosses canvas boundaries"),
                )
                .with_edges(vec![edge.id.clone()]),
            );
        }
    }

    let mut twice: Vec<_> = bundled
        .into_iter()
        .filter(|(_, edges)| edges.len() > 1)
        .collect();
    twice.sort_by(|a, b| a.0.cmp(&b.0));
    for ((source, target), edges) in twice {
        let name = |id: &NodeId| {
            index
                .nodes
                .get(id)
                .map(|n| n.node.name.clone())
                .unwrap_or_default()
        };
        out.push(
            TopologyProblem::error(
                ProblemKind::BundleEdgeInvalid,
                format!(
                    "{} is bundled to {} more than once; one member per far node",
                    name(source),
                    name(target)
                ),
            )
            .with_edges(edges.iter().map(|e| e.id.clone()).collect()),
        );
    }

    let mut oversubscribed: Vec<_> = usage.into_iter().filter(|(_, count)| *count > 1).collect();
    oversubscribed.sort_by(|a, b| a.0.cmp(b.0));
    for (port, count) in oversubscribed {
        let name = index
            .ports
            .get(port)
            .map(|(_, node)| node.node.name.clone())
            .unwrap_or_default();
        out.push(
            TopologyProblem::error(
                ProblemKind::PortOversubscribed,
                format!("port {port} of node {name} carries {count} edges"),
            )
            .with_ports(vec![port.clone()]),
        );
    }
}

/// Why a bundle edge is not one of the allowed shapes — a distribute node or a
/// universal pod bundling to a universal pod or a distribute node, or a
/// universal pod bundling to an aggregate node — leaving through a member or a
/// universal pod's `bundle_out` and arriving on the `bundle_in:` port named
/// after the source, or on an aggregate node's member; `None` when it is.
fn bundle_edge_problem(
    source: &PortEntity,
    source_node: &NodeWithPorts,
    target: &PortEntity,
    target_node: &NodeWithPorts,
) -> Option<String> {
    let source_key = source_node.node.id.as_str();
    let pair_ok = matches!(
        (&source_node.node.spec, &target_node.node.spec),
        (
            NodeSpec::LoadBalanceDistribute(_) | NodeSpec::UniversalPod(_),
            NodeSpec::UniversalPod(_) | NodeSpec::LoadBalanceDistribute(_)
        ) | (NodeSpec::UniversalPod(_), NodeSpec::LoadBalanceAggregate(_))
    );
    if !pair_ok {
        return Some("bundles nodes that cannot be bundled".to_string());
    }
    let source_ok = match universal::parse_port_key(&source.key) {
        Some(UniversalPort::BundleOut) => {
            matches!(source_node.node.spec, NodeSpec::UniversalPod(_))
        }
        None => {
            matches!(source_node.node.spec, NodeSpec::LoadBalanceDistribute(_))
                && universal::is_member_port(source)
        }
        _ => false,
    };
    let target_ok = match universal::parse_port_key(&target.key) {
        Some(UniversalPort::BundleIn(named)) => {
            named == source_key
                && matches!(
                    target_node.node.spec,
                    NodeSpec::UniversalPod(_) | NodeSpec::LoadBalanceDistribute(_)
                )
        }
        None => {
            matches!(target_node.node.spec, NodeSpec::LoadBalanceAggregate(_))
                && universal::is_member_port(target)
        }
        _ => false,
    };
    if !source_ok || !target_ok {
        return Some("joins ports that do not carry a bundle between these nodes".to_string());
    }
    None
}

/// Whether the hand-drawn ports of a load-balance node are exactly its declared
/// members: one bundle port per member, keyed by slot, in the order listed.
fn members_shape_ok(
    members: &[crate::entities::db::node::LoadBalanceMember],
    ports: &[&PortEntity],
    direction: PortDirection,
) -> bool {
    ports.len() == members.len()
        && members.iter().enumerate().all(|(i, member)| {
            ports.iter().any(|p| {
                p.key == member.port_key()
                    && p.kind == PortKind::Bundle
                    && p.direction == direction
                    && usize::try_from(p.position).is_ok_and(|pos| pos == i)
            })
        })
}

/// `(kind, direction, multiplicity)` a spec's hand-drawn ports must match.
/// `None` for an import node, whose ports are checked against its target's
/// exports instead, and for a universal pod, whose ports are all created on
/// demand (see [`universal::universal_port_shape_ok`]). A load-balance node's
/// hand-drawn ports may be absent altogether (a node used through bundles
/// only) but never half there.
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
        NodeSpec::CanvasImport(_) | NodeSpec::UniversalPod(_) => return None,
    })
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Multiplicity {
    One,
    AtLeastTwo,
}

/// Whether the hand-drawn ports of a node match `expected`. A load-balance
/// node may have none at all; otherwise every slot must be there.
fn manual_shape_ok(
    spec: &NodeSpec,
    ports: &[&PortEntity],
    expected: &[(PortKind, PortDirection, Multiplicity)],
) -> bool {
    if ports.is_empty()
        && matches!(
            spec,
            NodeSpec::LoadBalanceDistribute(_) | NodeSpec::LoadBalanceAggregate(_)
        )
    {
        return true;
    }
    let mut counts: HashMap<(PortKind, PortDirection), usize> = HashMap::new();
    for port in ports {
        let slot = counts.entry((port.kind, port.direction)).or_insert(0);
        *slot = slot.saturating_add(1);
    }
    let mut ok = counts.len() == expected.len();
    for (kind, direction, multiplicity) in expected {
        let count = counts.get(&(*kind, *direction)).copied().unwrap_or(0);
        ok &= match multiplicity {
            Multiplicity::One => count == 1,
            Multiplicity::AtLeastTwo => count >= 2,
        };
    }
    ok
}

/// The hand-drawn input ports a node is derived through, in position order: a
/// lane's `member_*` / `source`, never an on-demand `lane:` port and never a
/// member that carries a bundle (an operator's node is derived through its
/// lanes, not itself).
pub(crate) fn manual_inputs(node: &NodeWithPorts) -> Vec<&PortEntity> {
    let mut ports: Vec<&PortEntity> = node
        .ports
        .iter()
        .filter(|p| {
            p.direction == PortDirection::Input
                && p.kind != PortKind::Bundle
                && !universal::is_on_demand(p)
        })
        .collect();
    ports.sort_by_key(|p| p.position);
    ports
}

fn check_port_shapes(index: &Index<'_>, out: &mut Vec<TopologyProblem>) {
    for node in sorted_nodes(index) {
        let ok = match &node.node.spec {
            NodeSpec::CanvasImport(cfg) => {
                let target = &cfg.canvas;
                if !index.canvases.contains(target) {
                    continue; // reported by `check_imports`
                }
                let exports: Vec<&NodeEntity> = index
                    .exports
                    .get(target)
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
            spec if spec.takes_bundles() => {
                let manual: Vec<&PortEntity> = node
                    .ports
                    .iter()
                    .filter(|p| !universal::is_on_demand(p))
                    .collect();
                universal::universal_port_shape_ok(node)
                    && match spec {
                        NodeSpec::LoadBalanceDistribute(cfg) if !cfg.members.is_empty() => {
                            members_shape_ok(&cfg.members, &manual, PortDirection::Output)
                        }
                        NodeSpec::LoadBalanceAggregate(cfg) if !cfg.members.is_empty() => {
                            members_shape_ok(&cfg.members, &manual, PortDirection::Input)
                        }
                        _ => expected_ports(spec)
                            .is_none_or(|expected| manual_shape_ok(spec, &manual, &expected)),
                    }
            }
            spec => {
                let Some(expected) = expected_ports(spec) else {
                    continue;
                };
                let ports: Vec<&PortEntity> = node.ports.iter().collect();
                manual_shape_ok(spec, &ports, &expected)
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
                let server_canvas = index.servers.get(&cfg.server).map(|server| &server.canvas);
                let foreign = match server_canvas {
                    None => true,
                    Some(canvas) => index.root_of(canvas) != index.root_of(&node.node.canvas),
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
                for (label, value) in [
                    ("bind_ip", &cfg.bind_ip),
                    ("advertise_ip", &cfg.advertise_ip),
                ] {
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
    /// `(server, port, transport)` -> the pods already seen on that socket.
    type Claims<'a> = HashMap<(&'a ServerId, u16, &'static str), Vec<(Bind, &'a NodeWithPorts)>>;
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
        let key = (&cfg.server, cfg.port, transport);
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
    let mut pods_by_server: HashMap<&ServerId, Vec<NodeId>> = HashMap::new();
    for node in sorted_nodes(index) {
        if let NodeSpec::Pod(cfg) = &node.node.spec {
            pods_by_server
                .entry(&cfg.server)
                .or_default()
                .push(node.node.id.clone());
        }
    }
    let mut keys: Vec<&&ServerId> = index.servers.keys().collect();
    keys.sort();
    for key in keys {
        let Some(server) = index.servers.get(*key) else {
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

/// Whether a node is a vertex of the traffic graph (boundary nodes and
/// universal nodes are not: both are looked through).
fn is_traffic_node(node: &NodeWithPorts) -> bool {
    !matches!(
        node.node.spec,
        NodeSpec::CanvasImport(_) | NodeSpec::CanvasExport(_)
    ) && !node.node.spec.is_universal()
}

/// What the expansion of the universal nodes reports: a bundle cycle is an
/// error, a channel that is not carried all the way is a warning, and stored
/// lanes that no longer match the expansion are a warning (the next universal
/// edit regenerates them).
fn check_universal(
    topology: &CanvasTopology,
    errors: &mut Vec<TopologyProblem>,
    warnings: &mut Vec<TopologyProblem>,
) {
    if !topology.nodes.iter().any(|n| n.node.spec.takes_bundles()) {
        return;
    }
    let mut desired = universal::expand(topology);
    for problem in std::mem::take(&mut desired.problems) {
        match problem.severity {
            ProblemSeverity::Error => errors.push(problem),
            ProblemSeverity::Warning => warnings.push(problem),
        }
    }
    let stale = match universal::diff(topology, &desired) {
        Ok(plan) => !plan.batch.is_empty(),
        Err(_) => true,
    };
    if stale {
        warnings.push(TopologyProblem::warning(
            ProblemKind::LanesStale,
            "generated lanes are out of date; the next edit of a universal node regenerates them"
                .to_string(),
        ));
    }
}

/// `consumer -> producers`: a node depends on whatever feeds its inputs, and a relay
/// additionally depends on the pod it dials into. Boundaries are looked through.
fn dependency_graph<'a>(index: &Index<'a>) -> HashMap<&'a NodeId, Vec<&'a NodeId>> {
    let mut graph: HashMap<&NodeId, Vec<&NodeId>> = HashMap::new();
    for node in index.nodes.values().filter(|n| is_traffic_node(n)) {
        let deps = graph.entry(&node.node.id).or_default();
        for port in manual_inputs(node) {
            if let Some(peer) = index.peer(port) {
                deps.push(&peer.node.id);
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
            graph.entry(&node.node.id).or_default().push(&pod.node.id);
        }
    }
    graph
}

fn check_cycles(index: &Index<'_>, out: &mut Vec<TopologyProblem>) {
    let graph = dependency_graph(index);
    let mut colour: HashMap<&NodeId, u8> = HashMap::new(); // 0 = open, 1 = done
    let mut reported: HashSet<String> = HashSet::new();
    let mut keys: Vec<&&NodeId> = graph.keys().collect();
    keys.sort();

    for start in keys {
        if colour.get(*start).copied().unwrap_or(0) == 1 {
            continue;
        }
        // Iterative DFS carrying the current path, so a back edge names its cycle.
        let mut stack: Vec<(&NodeId, usize)> = vec![(start, 0)];
        let mut path: Vec<&NodeId> = vec![start];
        let mut on_path: HashSet<&NodeId> = HashSet::from([*start]);
        while let Some((node, edge_index)) = stack.pop() {
            let deps = graph.get(node).map(Vec::as_slice).unwrap_or(&[]);
            if edge_index >= deps.len() {
                colour.insert(node, 1);
                path.pop();
                on_path.remove(node);
                continue;
            }
            stack.push((node, edge_index.saturating_add(1)));
            let next = deps[edge_index];
            if on_path.contains(next) {
                let start_at = path.iter().position(|n| *n == next).unwrap_or(0);
                let mut cycle: Vec<&NodeId> = path[start_at..].to_vec();
                cycle.sort();
                let signature = cycle
                    .iter()
                    .map(|n| n.as_str())
                    .collect::<Vec<_>>()
                    .join(",");
                if reported.insert(signature) {
                    let named = index.nodes.get(cycle[0]).map(|n| n.node.name.clone());
                    out.push(
                        TopologyProblem::error(
                            ProblemKind::Cycle,
                            format!("cycle through node {}", named.unwrap_or_default()),
                        )
                        .with_nodes(
                            cycle
                                .iter()
                                .filter_map(|id| index.nodes.get(*id))
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
            if cfg.mode == crate::entities::db::node::LoadBalanceMode::IpHash {
                return true;
            }
            manual_inputs(node)
                .into_iter()
                .filter_map(|p| index.peer(p))
                .any(|member| iphash_reachable(index, member, depth.saturating_add(1)))
        }
        NodeSpec::LoadBalanceAggregate(_) => manual_inputs(node)
            .into_iter()
            .next()
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
            NodeSpec::LoadBalanceDistribute(cfg) => {
                // An operator's node balances over its wired members; a lane
                // over its thin inputs.
                let connected = if cfg.members.is_empty() {
                    manual_inputs(node)
                        .into_iter()
                        .filter(|p| index.edge_on(p).is_some())
                        .count()
                } else {
                    node.ports
                        .iter()
                        .filter(|p| universal::is_member_port(p) && index.edge_on(p).is_some())
                        .count()
                };
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

/// The server a pod listens on.
fn pod_server(pod: &NodeWithPorts) -> Option<&ServerId> {
    let NodeSpec::Pod(cfg) = &pod.node.spec else {
        return None;
    };
    Some(&cfg.server)
}

/// Nodes in id order, so problems are reported deterministically.
fn sorted_nodes<'a>(index: &Index<'a>) -> Vec<&'a NodeWithPorts> {
    let mut keys: Vec<&&NodeId> = index.nodes.keys().collect();
    keys.sort();
    keys.into_iter()
        .filter_map(|k| index.nodes.get(*k))
        .copied()
        .collect()
}

fn kind_name(kind: PortKind) -> &'static str {
    match kind {
        PortKind::DeriveListen => "derive_listen",
        PortKind::DeriveDestination => "derive_destination",
        PortKind::Bundle => "bundle",
    }
}
