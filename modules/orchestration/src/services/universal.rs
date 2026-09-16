//! Universal nodes: the canvas shorthand for "one rule × N transit servers".
//!
//! One node kind and two adaptive ones let an operator draw a fan-out once
//! instead of once per transit server:
//!
//! - a **load-balance distribute** node takes any number of entry pods (each one
//!   a *channel*, connected to a `chan:<pod>` port) and bundles all of them to
//!   any number of universal pods, relaying with its `protocol`;
//! - a **universal pod** — one per server, created with it — receives bundles
//!   and lands every channel they carry on a generated pod of its server, then
//!   hands the bundle on (`bundle_out`) to another universal pod or to an
//!   aggregate node;
//! - a **load-balance aggregate** node exposes one `chan:<pod>` input per
//!   channel its bundles carry, to be fed by an exit.
//!
//! The rule is the operator's: a distribute node declares its members (named,
//! one bundle port each) and so does an aggregate node; what follows from the
//! members — the bundles collected on the far side, the channels a bundle
//! carries, the lanes — is generated. Lanes are load-balance nodes too, laid
//! out thin (`member_*` / `destination`, `source` / `copy_*`) from a count.
//!
//! None of this is a new traffic model. The universal nodes are *expanded* into
//! ordinary pod, relay and load-balance nodes — the **lanes**, tagged with
//! [`Lane`] — and ordinary edges, and derivation, convergence, certificates and
//! health never see anything else. [`expand`] computes the lanes a topology
//! calls for, [`diff`] turns the difference with what is stored into one
//! [`ApplyTopologyBatch`], and [`prepare`] runs both after any edit that can
//! change the expansion, validating the *expanded* result before anything is
//! written. Lanes are diffed by key, so an edit that leaves a lane's identity
//! alone leaves its row, its port ids and its listening port alone.
//!
//! The distribute and aggregate nodes' per-channel ports come in pairs: the
//! outer `chan:<pod>` the operator connects, and a hidden `lane:<pod>` the
//! generated edges attach to. [`crate::services::topology::Index::peer`] looks
//! through the pair, exactly as it looks through an import/export boundary, so
//! every walk sees the flat graph the expansion stands for. Bundle ports carry
//! nothing: they are the expansion's input, not a traffic path.

use crate::config::OrchestrationConfig;
use crate::entities::surreal::batch::{
    ApplyTopologyBatch, BatchEdge, NewLaneNode, PortRef, PortReshape, SpecUpdate,
};
use crate::entities::surreal::canvas::CanvasUiPosition;
use crate::entities::surreal::connection::EdgeConnectionEntity;
use crate::entities::surreal::node::{
    Lane, LaneRole, LoadBalanceAggregateConfig, LoadBalanceDistributeConfig, LoadBalanceMode,
    MEMBER_PREFIX, NewPort, NodeEntity, NodeId, NodeSpec, NodeWithPorts, PodConfig, RelayConfig,
    RelayProtocol,
};
use crate::entities::surreal::port::{PortDirection, PortEntity, PortKind};
use crate::entities::surreal::server::ServerId;
use crate::entities::surreal::topology::CanvasTopology;
use crate::entities::surreal::view::ListServerConfigViewsByCanvases;
use crate::events::live::CanvasChangeKind;
use crate::services::OrchestrationError;
use crate::services::converge::ensure_switch_safe;
use crate::services::node::{port_layout, port_rows};
use crate::services::notify::Notifier;
use crate::services::server::DEFAULT_POD_PORTS;
use crate::services::topology::{Index, ProblemKind, TopologyEdit, TopologyProblem, ensure_valid};
use crate::utils::ids;
use crate::utils::ids::record_key;
use kanau::processor::Processor;
use rand::Rng;
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use wakuwaku::surreal::SurrealProcessor;

// --- port keys ----------------------------------------------------------------

pub const CHAN_PREFIX: &str = "chan:";
pub const LANE_PREFIX: &str = "lane:";
pub const BUNDLE_IN_PREFIX: &str = "bundle_in:";
/// The single, fixed outgoing bundle port of a universal pod.
pub const BUNDLE_OUT: &str = "bundle_out";

/// The placeholder record-key prefix of a lane node that a projection creates;
/// the lane key follows it.
pub const PENDING_LANE_PREFIX: &str = "pending-lane-";
/// The placeholder record-key prefix of a port a projection creates; the owner
/// key and the port key follow it.
pub const PENDING_PORT_PREFIX: &str = "pending-port-";

pub fn chan_key(pod: &str) -> String {
    format!("{CHAN_PREFIX}{pod}")
}
pub fn lane_key(pod: &str) -> String {
    format!("{LANE_PREFIX}{pod}")
}
pub fn bundle_in_key(source: &str) -> String {
    format!("{BUNDLE_IN_PREFIX}{source}")
}

/// A universal node's port, decoded from its key.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UniversalPort<'a> {
    /// The operator-facing side of a channel.
    Chan(&'a str),
    /// The hidden side of a channel, where generated edges attach.
    Lane(&'a str),
    /// A bundle arriving from the named node (universal pods, distribute nodes).
    BundleIn(&'a str),
    /// A universal pod's fixed outgoing bundle.
    BundleOut,
}

pub fn parse_port_key(key: &str) -> Option<UniversalPort<'_>> {
    if key == BUNDLE_OUT {
        return Some(UniversalPort::BundleOut);
    }
    if let Some(rest) = key.strip_prefix(CHAN_PREFIX) {
        return Some(UniversalPort::Chan(rest));
    }
    if let Some(rest) = key.strip_prefix(LANE_PREFIX) {
        return Some(UniversalPort::Lane(rest));
    }
    if let Some(rest) = key.strip_prefix(BUNDLE_IN_PREFIX) {
        return Some(UniversalPort::BundleIn(rest));
    }
    None
}

/// Whether a port is a declared member of an operator's load-balance node: a
/// bundle port keyed `member_<slot>`. Members are the operator's rule, so they
/// are hand-drawn ports that happen to carry bundles.
pub fn is_member_port(port: &PortEntity) -> bool {
    port.kind == PortKind::Bundle && port.key.starts_with(MEMBER_PREFIX)
}

/// The bundle ports an operator connects by id: a member, or a universal pod's
/// fixed `bundle_out`. The `bundle_in:` ports are created by the connect itself.
pub fn is_operator_bundle_port(port: &PortEntity) -> bool {
    is_member_port(port) || matches!(parse_port_key(&port.key), Some(UniversalPort::BundleOut))
}

/// The hidden twin of a channel port (`chan:x` ⇄ `lane:x`), if `key` is one.
pub fn channel_twin(key: &str) -> Option<String> {
    match parse_port_key(key)? {
        UniversalPort::Chan(pod) => Some(lane_key(pod)),
        UniversalPort::Lane(pod) => Some(chan_key(pod)),
        _ => None,
    }
}

/// Whether a port belongs to the generated side of the graph: a `lane:` port,
/// or any port of a lane node. Such edges are never edited by hand.
pub fn is_managed_port(port: &PortEntity, owner: &NodeEntity) -> bool {
    owner.lane.is_some() || matches!(parse_port_key(&port.key), Some(UniversalPort::Lane(_)))
}

/// Whether a port is one of the on-demand ports of a bundle-capable node (a
/// channel pair or a bundle port), as opposed to a hand-drawn one.
pub fn is_on_demand(port: &PortEntity) -> bool {
    parse_port_key(&port.key).is_some()
}

/// The next channel ordinal in a tree: one past the highest `chan:` position of
/// every distribute and aggregate node. Ordinals are never reused, so a
/// channel's colour survives its neighbours' deletion.
pub fn next_ordinal(topology: &CanvasTopology) -> i64 {
    topology
        .nodes
        .iter()
        .filter(|n| n.node.spec.takes_bundles())
        .flat_map(|n| n.ports.iter())
        .filter(|p| matches!(parse_port_key(&p.key), Some(UniversalPort::Chan(_))))
        .map(|p| p.position.saturating_add(1))
        .max()
        .unwrap_or(0)
}

/// Whether a bundle-capable node's on-demand ports have the shape its kind
/// allows: only the known keys, each with the right kind and direction,
/// `chan:`/`lane:` in pairs, `bundle_in:` only where bundles are collected
/// automatically (universal pods, distribute nodes), and exactly one fixed
/// `bundle_out` on a universal pod. A load-balance node's hand-drawn ports
/// (its members) are skipped here, the checker counts them separately; a
/// universal pod has none.
pub fn universal_port_shape_ok(node: &NodeWithPorts) -> bool {
    use PortDirection::{Input, Output};
    use PortKind::{Bundle, DeriveDestination};
    let mut chans: HashSet<&str> = HashSet::new();
    let mut lanes: HashSet<&str> = HashSet::new();
    let mut fixed_out = 0usize;
    for port in &node.ports {
        let Some(decoded) = parse_port_key(&port.key) else {
            if matches!(node.node.spec, NodeSpec::UniversalPod(_)) {
                return false;
            }
            continue;
        };
        let ok = match (&node.node.spec, decoded) {
            (NodeSpec::LoadBalanceDistribute(_), UniversalPort::Chan(pod)) => {
                chans.insert(pod);
                (port.kind, port.direction) == (DeriveDestination, Output)
            }
            (NodeSpec::LoadBalanceDistribute(_), UniversalPort::Lane(pod)) => {
                lanes.insert(pod);
                (port.kind, port.direction) == (DeriveDestination, Input)
            }
            (NodeSpec::LoadBalanceDistribute(_), UniversalPort::BundleIn(_)) => {
                (port.kind, port.direction) == (Bundle, Input)
            }
            (NodeSpec::UniversalPod(_), UniversalPort::BundleIn(_)) => {
                (port.kind, port.direction) == (Bundle, Input)
            }
            (NodeSpec::UniversalPod(_), UniversalPort::Chan(pod)) => {
                chans.insert(pod);
                (port.kind, port.direction) == (DeriveDestination, Output)
            }
            (NodeSpec::UniversalPod(_), UniversalPort::Lane(pod)) => {
                lanes.insert(pod);
                (port.kind, port.direction) == (DeriveDestination, Input)
            }
            (NodeSpec::UniversalPod(_), UniversalPort::BundleOut) => {
                fixed_out = fixed_out.saturating_add(1);
                (port.kind, port.direction) == (Bundle, Output)
            }
            (NodeSpec::LoadBalanceAggregate(_), UniversalPort::Chan(pod)) => {
                chans.insert(pod);
                (port.kind, port.direction) == (DeriveDestination, Input)
            }
            (NodeSpec::LoadBalanceAggregate(_), UniversalPort::Lane(pod)) => {
                lanes.insert(pod);
                (port.kind, port.direction) == (DeriveDestination, Output)
            }
            _ => false,
        };
        if !ok {
            return false;
        }
    }
    if chans != lanes {
        return false;
    }
    !matches!(node.node.spec, NodeSpec::UniversalPod(_)) || fixed_out == 1
}

/// The port shape of a freshly created universal pod (before any connect).
pub fn initial_ports(spec: &NodeSpec) -> Vec<NewPort> {
    match spec {
        NodeSpec::UniversalPod(_) => vec![NewPort {
            kind: PortKind::Bundle,
            direction: PortDirection::Output,
            key: BUNDLE_OUT.to_string(),
            position: 0,
        }],
        _ => Vec::new(),
    }
}

// --- the expansion --------------------------------------------------------------

/// One end of a generated edge, before ids exist: a port on an existing node or
/// on a lane (which may be about to be created).
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum EndRef {
    Node { node: String, key: String },
    Lane { lane: String, key: String },
}

impl EndRef {
    fn node(node: &NodeId, key: String) -> Self {
        EndRef::Node {
            node: record_key(&node.0),
            key,
        }
    }
    fn lane(lane: &str, key: &str) -> Self {
        EndRef::Lane {
            lane: lane.to_string(),
            key: key.to_string(),
        }
    }
}

/// What a lane must be, independent of the row it may already have.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LaneShape {
    Distribute {
        mode: LoadBalanceMode,
        members: u32,
    },
    Relay {
        protocol: RelayProtocol,
    },
    /// A landing pod. Its port number is not part of the shape: the reconciler
    /// keeps the stored one unless `protocol` (what the relay dialling it
    /// speaks) changed, which is the one edit a listener cannot survive.
    Landing {
        server: ServerId,
        protocol: RelayProtocol,
    },
    Aggregate {
        copies: u32,
    },
}

#[derive(Debug, Clone)]
pub struct DesiredLane {
    pub lane: Lane,
    pub canvas: crate::entities::surreal::canvas::CanvasId,
    pub name: String,
    pub position: CanvasUiPosition,
    pub shape: LaneShape,
}

/// The expansion of one topology: what every universal node's ports must be,
/// which lanes must exist, and which generated edges must join them.
#[derive(Debug, Clone, Default)]
pub struct Desired {
    /// Universal node key -> its full port list.
    pub ports: BTreeMap<String, Vec<NewPort>>,
    /// Lane key -> the lane.
    pub lanes: BTreeMap<String, DesiredLane>,
    pub edges: BTreeSet<(EndRef, EndRef)>,
    /// Errors (a bundle cycle) and warnings (a channel with no transit or no
    /// exit yet) the expansion found.
    pub problems: Vec<TopologyProblem>,
}

/// One channel: an entry pod drawn into a distribute node or straight into a
/// universal pod. Its ordinal is its colour; how it is relayed is decided by
/// whichever distribute node fans it out (raw TCP where none does).
struct Channel<'a> {
    pod: &'a NodeWithPorts,
    ordinal: i64,
}

/// The server a universal pod stands for.
fn universal_server(node: &NodeWithPorts) -> Option<&ServerId> {
    match &node.node.spec {
        NodeSpec::UniversalPod(cfg) => Some(&cfg.server),
        _ => None,
    }
}

fn server_name<'a>(index: &Index<'a>, server: &ServerId) -> String {
    index
        .servers
        .get(&record_key(&server.0))
        .map(|s| s.name.clone())
        .unwrap_or_else(|| record_key(&server.0))
}

/// The display name of the far side of a bundle: a distribute node's own name,
/// a universal pod's server name.
fn source_name(index: &Index<'_>, node: &NodeWithPorts) -> String {
    match universal_server(node) {
        Some(server) => server_name(index, server),
        None => node.node.name.clone(),
    }
}

/// The mode and relay protocol of a distribute node.
fn distribute_cfg(node: &NodeWithPorts) -> Option<(LoadBalanceMode, RelayProtocol)> {
    match &node.node.spec {
        NodeSpec::LoadBalanceDistribute(cfg) => Some((cfg.mode, cfg.protocol)),
        _ => None,
    }
}

/// Deepest chain of nested distribute nodes the fan-out follows; a bundle cycle
/// is reported separately, this only keeps the recursion finite.
const MAX_FANOUT_DEPTH: usize = 64;

/// Everything the materialisation walks, plus what it produces.
struct Expansion<'a> {
    index: Index<'a>,
    universal: BTreeMap<String, &'a NodeWithPorts>,
    /// Bundle targets of each node, in the order of the ports they leave from
    /// (a distribute node's members, as the operator listed them).
    outs: BTreeMap<String, Vec<String>>,
    channels: BTreeMap<String, Channel<'a>>,
    desired: Desired,
}

impl<'a> Expansion<'a> {
    fn lane_at(
        &self,
        group: &NodeWithPorts,
        lane: Lane,
        name: String,
        shape: LaneShape,
    ) -> DesiredLane {
        DesiredLane {
            lane,
            canvas: group.node.canvas.clone(),
            name,
            position: group.node.position,
            shape,
        }
    }

    /// The display form of a `via` path: the names of the nodes the channel came
    /// through, first hop first (`hk1 → tier-2`).
    fn path_names(&self, path: &str) -> String {
        path.split('+')
            .map(|key| {
                self.universal
                    .get(key)
                    .map(|node| source_name(&self.index, node))
                    .unwrap_or_else(|| key.to_string())
            })
            .collect::<Vec<_>>()
            .join(" → ")
    }

    /// The landing pod of channel `c` on universal pod `target`, dialled by a
    /// relay owned by `owner` speaking `protocol`; returns the relay's
    /// destination, the member the fan-out joins. `via` tells one upstream path
    /// apart from another when `owner` fans the channel out for several.
    fn hop(
        &mut self,
        owner: &'a NodeWithPorts,
        target: &'a NodeWithPorts,
        channel: &Channel<'a>,
        protocol: RelayProtocol,
        relay_source: Option<&NodeId>,
        via: Option<&str>,
    ) -> Option<EndRef> {
        let server = universal_server(target)?;
        let pod_id = channel.pod.node.id.clone();
        let pod_name = channel.pod.node.name.clone();
        let landing = Lane::new(
            &target.node.id,
            &pod_id,
            LaneRole::Landing,
            Some(&owner.node.id),
            via,
        );
        let relay = Lane::new(&owner.node.id, &pod_id, LaneRole::Relay, relay_source, via);
        self.desired.edges.insert((
            EndRef::lane(&landing.key, "listen"),
            EndRef::lane(&relay.key, "listen"),
        ));
        let member = EndRef::lane(&relay.key, "destination");
        // A landing pod's name is the worker's tag for it, and one server may
        // hold several landings of one channel: one per upstream path when the
        // owner fans out for several. The path is part of the name so they stay
        // apart on the worker.
        let landing_name = match via {
            None => format!("{pod_name} via {}", source_name(&self.index, owner)),
            Some(path) => format!(
                "{pod_name} via {} from {}",
                source_name(&self.index, owner),
                self.path_names(path)
            ),
        };
        self.desired.lanes.insert(
            landing.key.clone(),
            DesiredLane {
                lane: landing,
                canvas: target.node.canvas.clone(),
                name: landing_name,
                position: target.node.position,
                shape: LaneShape::Landing {
                    server: server.clone(),
                    protocol,
                },
            },
        );
        let relay_name = format!("{pod_name} → {}", server_name(&self.index, server));
        self.desired.lanes.insert(
            relay.key.clone(),
            self.lane_at(owner, relay, relay_name, LaneShape::Relay { protocol }),
        );
        Some(member)
    }

    /// Fans channel `c` out of distribute node `x` over its bundle targets:
    /// a hop per universal pod, a nested fan-out per distribute node. Returns
    /// the one end the upstream side must feed (a relay's destination, or the
    /// generated load balancer's), `None` when nothing is bundled out yet.
    /// `via` is the upstream path that brought the channel here (`None` at the
    /// node the channel starts on); every path gets its own lanes.
    fn fanout(
        &mut self,
        x: &'a NodeWithPorts,
        channel: &Channel<'a>,
        via: Option<&str>,
        depth: usize,
    ) -> Option<EndRef> {
        let (mode, protocol) = distribute_cfg(x)?;
        let key = record_key(&x.node.id.0);
        let pod_id = channel.pod.node.id.clone();
        let pod_name = channel.pod.node.name.clone();
        if depth > MAX_FANOUT_DEPTH {
            return None;
        }
        let targets: Vec<&'a NodeWithPorts> = self
            .outs
            .get(&key)
            .into_iter()
            .flatten()
            .filter_map(|t| self.universal.get(t.as_str()).copied())
            .filter(|t| universal_server(t).is_some() || distribute_cfg(t).is_some())
            .collect();
        if targets.is_empty() {
            self.desired.problems.push(
                TopologyProblem::warning(
                    ProblemKind::ChannelNoTransit,
                    format!(
                        "channel {pod_name} on {} is not bundled to any server yet",
                        x.node.name
                    ),
                )
                .with_nodes(vec![x.node.id.clone(), pod_id]),
            );
            return None;
        }
        let nested_via = match via {
            None => key.clone(),
            Some(path) => format!("{path}+{key}"),
        };
        let mut members: Vec<EndRef> = Vec::new();
        for target in targets {
            let member = if universal_server(target).is_some() {
                self.hop(x, target, channel, protocol, Some(&target.node.id), via)
            } else {
                self.fanout(target, channel, Some(&nested_via), depth.saturating_add(1))
            };
            if let Some(member) = member {
                members.push(member);
            }
        }
        match members.len() {
            0 => None,
            1 => members.pop(),
            n => {
                let fan = Lane::new(&x.node.id, &pod_id, LaneRole::Distribute, None, via);
                for (i, member) in members.into_iter().enumerate() {
                    self.desired
                        .edges
                        .insert((member, EndRef::lane(&fan.key, &format!("member_{i}"))));
                }
                let out = EndRef::lane(&fan.key, "destination");
                self.desired.lanes.insert(
                    fan.key.clone(),
                    self.lane_at(
                        x,
                        fan,
                        format!("{pod_name} · fan-out"),
                        LaneShape::Distribute {
                            mode,
                            members: u32::try_from(n).unwrap_or(u32::MAX),
                        },
                    ),
                );
                Some(out)
            }
        }
    }
}

pub fn expand(topology: &CanvasTopology) -> Desired {
    let index = Index::build(topology);
    let mut desired = Desired::default();

    // The bundle-capable nodes, keyed, and the bundle graph between them.
    let mut universal: BTreeMap<String, &NodeWithPorts> = BTreeMap::new();
    for node in &topology.nodes {
        if node.node.spec.takes_bundles() {
            universal.insert(record_key(&node.node.id.0), node);
        }
    }
    // `outs` follows the order of the source ports (a distribute node's members
    // as listed by the operator), `ins` the order of the target ports (an
    // aggregate node's members); ties by node key.
    let mut out_edges: BTreeMap<String, BTreeSet<(i64, String)>> = BTreeMap::new();
    let mut in_edges: BTreeMap<String, BTreeSet<(i64, String)>> = BTreeMap::new();
    for edge in &topology.edges {
        let (Some((source, source_node)), Some((target, target_node))) =
            (index.port(&edge.source), index.port(&edge.target))
        else {
            continue;
        };
        if source.kind != PortKind::Bundle || target.kind != PortKind::Bundle {
            continue;
        }
        if !source_node.node.spec.takes_bundles() || !target_node.node.spec.takes_bundles() {
            continue;
        }
        let s = record_key(&source_node.node.id.0);
        let t = record_key(&target_node.node.id.0);
        out_edges
            .entry(s.clone())
            .or_default()
            .insert((source.position, t.clone()));
        in_edges.entry(t).or_default().insert((target.position, s));
    }
    let ordered =
        |edges: BTreeMap<String, BTreeSet<(i64, String)>>| -> BTreeMap<String, Vec<String>> {
            edges
                .into_iter()
                .map(|(k, set)| {
                    let mut seen = BTreeSet::new();
                    let list = set
                        .into_iter()
                        .map(|(_, far)| far)
                        .filter(|far| seen.insert(far.clone()))
                        .collect();
                    (k, list)
                })
                .collect()
        };
    let outs = ordered(out_edges);
    let ins = ordered(in_edges);

    // Channels: a `chan:` port of a distribute node or a universal pod whose
    // edge lands on that pod's `destination`. Anything else on such a port is
    // reported by the checker and carries nothing.
    let mut channels: BTreeMap<String, Channel<'_>> = BTreeMap::new();
    let mut own: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
    for (key, node) in &universal {
        if !matches!(
            node.node.spec,
            NodeSpec::LoadBalanceDistribute(_) | NodeSpec::UniversalPod(_)
        ) {
            continue;
        }
        for p in &node.ports {
            let Some(UniversalPort::Chan(pod_key)) = parse_port_key(&p.key) else {
                continue;
            };
            let Some(edge) = index.edge_on(p) else {
                continue;
            };
            let far = if record_key(&edge.source.0) == record_key(&p.id.0) {
                &edge.target
            } else {
                &edge.source
            };
            let Some((far_port, far_node)) = index.port(far) else {
                continue;
            };
            if far_port.key != "destination"
                || !matches!(far_node.node.spec, NodeSpec::Pod(_))
                || record_key(&far_node.node.id.0) != pod_key
            {
                continue;
            }
            channels.insert(
                pod_key.to_string(),
                Channel {
                    pod: far_node,
                    ordinal: p.position,
                },
            );
            own.entry(key.clone())
                .or_default()
                .insert(pod_key.to_string());
        }
    }

    // On-demand ports follow the edges alone, whatever the lanes end up being;
    // a load-balance node's members are carried over untouched. Bundles are
    // collected automatically where they arrive at a universal pod or a
    // distribute node (`bundle_in:<source>`); an aggregate node takes them on
    // its members, which the operator draws.
    for (key, node) in &universal {
        let mut ports: Vec<NewPort> = node
            .ports
            .iter()
            .filter(|p| !is_on_demand(p))
            .map(|p| NewPort {
                kind: p.kind,
                direction: p.direction,
                key: p.key.clone(),
                position: p.position,
            })
            .collect();
        let port = |key: String, kind: PortKind, direction: PortDirection, position: i64| NewPort {
            kind,
            direction,
            key,
            position,
        };
        let starts_channels = matches!(
            node.node.spec,
            NodeSpec::LoadBalanceDistribute(_) | NodeSpec::UniversalPod(_)
        );
        if starts_channels {
            for p in &node.ports {
                if let Some(UniversalPort::Chan(pod)) = parse_port_key(&p.key)
                    && index.edge_on(p).is_some()
                {
                    ports.push(port(
                        chan_key(pod),
                        PortKind::DeriveDestination,
                        PortDirection::Output,
                        p.position,
                    ));
                    ports.push(port(
                        lane_key(pod),
                        PortKind::DeriveDestination,
                        PortDirection::Input,
                        p.position,
                    ));
                }
            }
        }
        if starts_channels {
            for source in ins.get(key).into_iter().flatten() {
                ports.push(port(
                    bundle_in_key(source),
                    PortKind::Bundle,
                    PortDirection::Input,
                    0,
                ));
            }
        }
        if matches!(node.node.spec, NodeSpec::UniversalPod(_)) {
            ports.push(port(
                BUNDLE_OUT.to_string(),
                PortKind::Bundle,
                PortDirection::Output,
                0,
            ));
        }
        // An aggregate node's channel ports are added below, once carried
        // sets are known.
        desired.ports.insert(key.clone(), ports);
    }

    // Topological order over the bundle graph (Kahn); what is left is a cycle.
    let mut indegree: BTreeMap<&str, usize> = universal
        .keys()
        .map(|k| (k.as_str(), ins.get(k).map(Vec::len).unwrap_or(0)))
        .collect();
    let mut ready: Vec<&str> = indegree
        .iter()
        .filter(|(_, d)| **d == 0)
        .map(|(k, _)| *k)
        .collect();
    let mut order: Vec<&str> = Vec::new();
    while let Some(key) = ready.pop() {
        order.push(key);
        for target in outs.get(key).into_iter().flatten() {
            if let Some(d) = indegree.get_mut(target.as_str()) {
                *d = d.saturating_sub(1);
                if *d == 0 {
                    ready.push(target.as_str());
                }
            }
        }
        ready.sort();
        ready.reverse();
    }
    let cyclic: Vec<&str> = indegree
        .iter()
        .filter(|(k, _)| !order.contains(*k))
        .map(|(k, _)| *k)
        .collect();
    if !cyclic.is_empty() {
        desired.problems.push(
            TopologyProblem::error(
                ProblemKind::BundleCycle,
                "bundles form a cycle between universal nodes".to_string(),
            )
            .with_nodes(
                cyclic
                    .iter()
                    .filter_map(|k| universal.get(*k))
                    .map(|n| n.node.id.clone())
                    .collect(),
            ),
        );
    }

    // What each node carries: its own channels plus everything bundled into it.
    let mut carried: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
    for key in &order {
        let mut set = own.get(*key).cloned().unwrap_or_default();
        for source in ins.get(*key).into_iter().flatten() {
            if let Some(upstream) = carried.get(source) {
                set.extend(upstream.iter().cloned());
            }
        }
        carried.insert((*key).to_string(), set);
    }
    // The order outlives the borrows above: the walk below owns the tables.
    let order: Vec<String> = order.iter().map(|k| (*k).to_string()).collect();

    let mut ex = Expansion {
        index,
        universal,
        outs,
        channels,
        desired,
    };

    // Materialisation, in bundle order so a node's landing pods exist before the
    // node decides how to join them. A distribute node's own channels start at
    // its `lane:` port; the channels a universal pod passes into a distribute
    // node start at that universal pod's landing side.
    let mut feeders: BTreeMap<(String, String), Vec<EndRef>> = BTreeMap::new();
    for key in &order {
        let key: &str = key.as_str();
        let Some(node) = ex.universal.get(key).copied() else {
            continue;
        };
        let group = node.node.id.clone();
        match &node.node.spec {
            NodeSpec::LoadBalanceDistribute(_) => {
                for pod_key in own.get(key).into_iter().flatten() {
                    let Some(channel) = ex.channels.get(pod_key) else {
                        continue;
                    };
                    let channel = Channel {
                        pod: channel.pod,
                        ordinal: channel.ordinal,
                    };
                    if let Some(member) = ex.fanout(node, &channel, None, 0) {
                        ex.desired
                            .edges
                            .insert((member, EndRef::node(&group, lane_key(pod_key))));
                    }
                }
            }
            NodeSpec::UniversalPod(_) => {
                let out = ex
                    .outs
                    .get(key)
                    .into_iter()
                    .flatten()
                    .find_map(|t| ex.universal.get(t.as_str()).copied());
                let own_here = own.get(key).cloned().unwrap_or_default();
                for pod_key in carried.get(key).into_iter().flatten() {
                    let Some(channel) = ex.channels.get(pod_key) else {
                        continue;
                    };
                    let channel = Channel {
                        pod: channel.pod,
                        ordinal: channel.ordinal,
                    };
                    let pod_id = channel.pod.node.id.clone();
                    let pod_name = channel.pod.node.name.clone();
                    // An entry pod drawn straight into this node: a raw TCP hop
                    // of its own, landing here like any bundled channel.
                    if own_here.contains(pod_key)
                        && let Some(member) = ex.hop(
                            node,
                            node,
                            &channel,
                            RelayProtocol::TcpRaw,
                            Some(&pod_id),
                            None,
                        )
                    {
                        ex.desired
                            .edges
                            .insert((member, EndRef::node(&group, lane_key(pod_key))));
                    }
                    // The landing pods this node holds for the channel: every
                    // upstream hop created its own (in bundle order, they all
                    // exist by now).
                    let landings: Vec<String> = ex
                        .desired
                        .lanes
                        .iter()
                        .filter(|(_, l)| {
                            l.lane.role == LaneRole::Landing
                                && record_key(&l.lane.group.0) == key
                                && record_key(&l.lane.channel.0) == *pod_key
                        })
                        .map(|(k, _)| k.clone())
                        .collect();
                    let feeder = match landings.as_slice() {
                        [] => continue,
                        [only] => EndRef::lane(only, "destination"),
                        many => {
                            let join = Lane::new(&group, &pod_id, LaneRole::Aggregate, None, None);
                            for (i, landing) in many.iter().enumerate() {
                                ex.desired.edges.insert((
                                    EndRef::lane(&join.key, &format!("copy_{i}")),
                                    EndRef::lane(landing, "destination"),
                                ));
                            }
                            let feeder = EndRef::lane(&join.key, "source");
                            let joined = ex.lane_at(
                                node,
                                join.clone(),
                                format!("{pod_name} · join"),
                                LaneShape::Aggregate {
                                    copies: u32::try_from(many.len()).unwrap_or(u32::MAX),
                                },
                            );
                            ex.desired.lanes.insert(join.key.clone(), joined);
                            feeder
                        }
                    };
                    match out {
                        None => ex.desired.problems.push(
                            TopologyProblem::warning(
                                ProblemKind::ChannelNoExit,
                                format!(
                                    "channel {pod_name} lands on {} but goes nowhere from there",
                                    source_name(&ex.index, node)
                                ),
                            )
                            .with_nodes(vec![group.clone(), pod_id]),
                        ),
                        Some(next) if universal_server(next).is_some() => {
                            // A plain hop to the next server: raw TCP, nothing
                            // on the way says otherwise.
                            if let Some(member) =
                                ex.hop(node, next, &channel, RelayProtocol::TcpRaw, None, None)
                            {
                                ex.desired.edges.insert((member, feeder));
                            }
                        }
                        Some(next) if distribute_cfg(next).is_some() => {
                            // The next tier fans the channel out again, once per
                            // server it arrives from: this one.
                            if let Some(member) = ex.fanout(next, &channel, Some(key), 0) {
                                ex.desired.edges.insert((member, feeder));
                            }
                        }
                        Some(aggregate) => {
                            feeders
                                .entry((record_key(&aggregate.node.id.0), pod_key.clone()))
                                .or_default()
                                .push(feeder);
                        }
                    }
                }
            }
            NodeSpec::LoadBalanceAggregate(_) => {
                let mut ports = ex.desired.ports.remove(key).unwrap_or_default();
                for pod_key in carried.get(key).into_iter().flatten() {
                    let Some(channel) = ex.channels.get(pod_key) else {
                        continue;
                    };
                    let pod_id = channel.pod.node.id.clone();
                    let pod_name = channel.pod.node.name.clone();
                    let ordinal = channel.ordinal;
                    ports.push(NewPort {
                        kind: PortKind::DeriveDestination,
                        direction: PortDirection::Input,
                        key: chan_key(pod_key),
                        position: ordinal,
                    });
                    ports.push(NewPort {
                        kind: PortKind::DeriveDestination,
                        direction: PortDirection::Output,
                        key: lane_key(pod_key),
                        position: ordinal,
                    });
                    let source = EndRef::node(&group, lane_key(pod_key));
                    let mut requests = feeders
                        .remove(&(key.to_string(), pod_key.clone()))
                        .unwrap_or_default();
                    requests.sort();
                    match requests.as_slice() {
                        [] => {}
                        [only] => {
                            ex.desired.edges.insert((source, only.clone()));
                        }
                        many => {
                            let join = Lane::new(&group, &pod_id, LaneRole::Aggregate, None, None);
                            ex.desired
                                .edges
                                .insert((source, EndRef::lane(&join.key, "source")));
                            for (i, request) in many.iter().enumerate() {
                                ex.desired.edges.insert((
                                    EndRef::lane(&join.key, &format!("copy_{i}")),
                                    request.clone(),
                                ));
                            }
                            let joined = ex.lane_at(
                                node,
                                join.clone(),
                                format!("{pod_name} · join"),
                                LaneShape::Aggregate {
                                    copies: u32::try_from(many.len()).unwrap_or(u32::MAX),
                                },
                            );
                            ex.desired.lanes.insert(join.key.clone(), joined);
                        }
                    }
                }
                ex.desired.ports.insert(key.to_string(), ports);
            }
            _ => {}
        }
    }
    ex.desired
}

// --- the diff ------------------------------------------------------------------

/// The edits that take a topology to its expansion, in both forms: projected
/// (to validate) and as the batch that writes them.
#[derive(Debug, Clone, Default)]
pub struct Plan {
    pub edits: Vec<TopologyEdit>,
    pub batch: ApplyTopologyBatch,
}

/// `(key, kind, direction, position)` — what a reshape compares.
fn port_shape(ports: &[PortEntity]) -> BTreeSet<(String, PortKind, PortDirection, i64)> {
    ports
        .iter()
        .map(|p| (p.key.clone(), p.kind, p.direction, p.position))
        .collect()
}

fn new_port_shape(ports: &[NewPort]) -> BTreeSet<(String, PortKind, PortDirection, i64)> {
    ports
        .iter()
        .map(|p| (p.key.clone(), p.kind, p.direction, p.position))
        .collect()
}

/// The protocol the relay dialling a landing pod speaks today, if it is wired.
fn landing_protocol(index: &Index<'_>, pod: &NodeWithPorts) -> Option<RelayProtocol> {
    let listen = index.port_by_key(pod, "listen")?;
    match &index.peer(listen)?.node.spec {
        NodeSpec::Relay(cfg) => Some(cfg.protocol),
        _ => None,
    }
}

/// A port on `server` no pod of the tree listens on yet, from the default
/// range. `taken` also holds the ports handed out earlier in the same plan.
fn free_port(
    taken: &mut HashSet<(String, u16)>,
    server: &ServerId,
) -> Result<u16, OrchestrationError> {
    let server_key = record_key(&server.0);
    let mut rng = rand::rng();
    for _ in 0..4096 {
        let port = rng.random_range(DEFAULT_POD_PORTS);
        if taken.insert((server_key.clone(), port)) {
            return Ok(port);
        }
    }
    Err(OrchestrationError::Conflict(format!(
        "no free port left on server {server_key} in {}-{}",
        DEFAULT_POD_PORTS.start(),
        DEFAULT_POD_PORTS.end()
    )))
}

fn lane_spec(shape: &LaneShape, port: u16, keep: Option<&PodConfig>) -> (NodeSpec, u32) {
    match shape {
        LaneShape::Distribute { mode, members } => (
            NodeSpec::LoadBalanceDistribute(LoadBalanceDistributeConfig {
                mode: *mode,
                protocol: RelayProtocol::TcpRaw,
                members: Vec::new(),
            }),
            *members,
        ),
        LaneShape::Relay { protocol } => (
            NodeSpec::Relay(RelayConfig {
                protocol: *protocol,
                override_ip_address: None,
                override_port: None,
            }),
            0,
        ),
        LaneShape::Landing { server, .. } => (
            NodeSpec::Pod(PodConfig {
                server: server.clone(),
                port,
                bind_ip: keep.and_then(|k| k.bind_ip.clone()),
                advertise_ip: keep.and_then(|k| k.advertise_ip.clone()),
            }),
            0,
        ),
        LaneShape::Aggregate { copies } => (
            NodeSpec::LoadBalanceAggregate(LoadBalanceAggregateConfig::default()),
            *copies,
        ),
    }
}

/// Whether two lane specs differ in anything the expansion decides.
fn spec_differs(current: &NodeSpec, desired: &NodeSpec) -> bool {
    match (current, desired) {
        (NodeSpec::LoadBalanceDistribute(a), NodeSpec::LoadBalanceDistribute(b)) => {
            a.mode != b.mode
        }
        (NodeSpec::LoadBalanceAggregate(_), NodeSpec::LoadBalanceAggregate(_)) => false,
        (NodeSpec::Relay(a), NodeSpec::Relay(b)) => a.protocol != b.protocol,
        (NodeSpec::Pod(a), NodeSpec::Pod(b)) => {
            record_key(&a.server.0) != record_key(&b.server.0) || a.port != b.port
        }
        _ => true,
    }
}

/// The batch form of a projected port: an existing row, or a port the same
/// batch creates on an existing node or on a new lane.
fn port_ref(port: &PortEntity) -> PortRef {
    let id = record_key(&port.id.0);
    if !id.starts_with(PENDING_PORT_PREFIX) {
        return PortRef::existing(port.id.clone());
    }
    let owner = record_key(&port.owner.0);
    match owner.strip_prefix(PENDING_LANE_PREFIX) {
        Some(lane) => PortRef::on_lane(lane, &port.key),
        None => PortRef::on_node(port.owner.clone(), &port.key),
    }
}

/// The edits that reconcile `topology` with `desired`.
pub fn diff(topology: &CanvasTopology, desired: &Desired) -> Result<Plan, OrchestrationError> {
    let index = Index::build(topology);
    let mut plan = Plan::default();
    let canvas = topology
        .nodes
        .iter()
        .find(|n| n.node.spec.takes_bundles())
        .map(|n| n.node.canvas.clone())
        .or_else(|| topology.canvases.first().map(|c| c.id.clone()));
    plan.batch.canvas = canvas;

    // Universal node ports.
    for (key, ports) in &desired.ports {
        let Some(node) = index.nodes.get(key) else {
            continue;
        };
        if port_shape(&node.ports) == new_port_shape(ports) {
            continue;
        }
        plan.edits.push(TopologyEdit::ReshapePorts {
            node: node.node.id.clone(),
            ports: port_rows(&node.node.id, &node.ports, ports),
        });
        plan.batch.reshape.push(PortReshape {
            node: node.node.id.clone(),
            ports: ports.clone(),
        });
    }

    // Lanes: by key.
    let existing: BTreeMap<String, &NodeWithPorts> = topology
        .nodes
        .iter()
        .filter_map(|n| n.node.lane.as_ref().map(|lane| (lane.key.clone(), n)))
        .collect();
    for (key, node) in &existing {
        if !desired.lanes.contains_key(key) {
            plan.edits.push(TopologyEdit::RetireNode {
                node: node.node.id.clone(),
            });
            plan.batch.delete_nodes.push(node.node.id.clone());
        }
    }
    let mut taken: HashSet<(String, u16)> = topology
        .nodes
        .iter()
        .filter_map(|n| match &n.node.spec {
            NodeSpec::Pod(cfg) => Some((record_key(&cfg.server.0), cfg.port)),
            _ => None,
        })
        .collect();
    for (key, want) in &desired.lanes {
        let current = existing.get(key).copied();
        let kept_pod = current.and_then(|n| match &n.node.spec {
            NodeSpec::Pod(cfg) => Some(cfg),
            _ => None,
        });
        let port = match (&want.shape, kept_pod, current) {
            (LaneShape::Landing { protocol, .. }, Some(cfg), Some(node))
                if landing_protocol(&index, node).is_none_or(|p| p == *protocol) =>
            {
                cfg.port
            }
            (LaneShape::Landing { server, .. }, _, _) => free_port(&mut taken, server)?,
            _ => 0,
        };
        let (spec, count) = lane_spec(&want.shape, port, kept_pod);
        let ports = port_layout(&spec, count)?;
        match current {
            Some(node) => {
                if spec_differs(&node.node.spec, &spec) {
                    plan.edits.push(TopologyEdit::SetSpec {
                        node: node.node.id.clone(),
                        spec: spec.clone(),
                    });
                    plan.batch.set_specs.push(SpecUpdate {
                        node: node.node.id.clone(),
                        spec: spec.clone(),
                    });
                }
                if port_shape(&node.ports) != new_port_shape(&ports) {
                    plan.edits.push(TopologyEdit::ReshapePorts {
                        node: node.node.id.clone(),
                        ports: port_rows(&node.node.id, &node.ports, &ports),
                    });
                    plan.batch.reshape.push(PortReshape {
                        node: node.node.id.clone(),
                        ports: ports.clone(),
                    });
                }
            }
            None => {
                let id = ids::node_id(&format!("{PENDING_LANE_PREFIX}{key}"));
                let entity = NodeEntity {
                    id: id.clone(),
                    canvas: want.canvas.clone(),
                    name: want.name.clone(),
                    comment: String::new(),
                    spec: spec.clone(),
                    position: want.position,
                    lane: Some(want.lane.clone()),
                };
                plan.edits.push(TopologyEdit::AddNode {
                    ports: port_rows(&id, &[], &ports),
                    node: Box::new(entity),
                });
                plan.batch.create_nodes.push(NewLaneNode {
                    canvas: want.canvas.clone(),
                    name: want.name.clone(),
                    comment: String::new(),
                    spec,
                    position: want.position,
                    ports,
                    lane: want.lane.clone(),
                });
            }
        }
    }

    // Edges, against the topology as the edits above leave it.
    let shaped = topology.project(&plan.edits);
    let lane_of: HashMap<String, String> = shaped
        .nodes
        .iter()
        .filter_map(|n| {
            n.node
                .lane
                .as_ref()
                .map(|lane| (record_key(&n.node.id.0), lane.key.clone()))
        })
        .collect();
    let by_lane: HashMap<&str, &NodeWithPorts> = shaped
        .nodes
        .iter()
        .filter_map(|n| n.node.lane.as_ref().map(|lane| (lane.key.as_str(), n)))
        .collect();
    let by_node: HashMap<String, &NodeWithPorts> = shaped
        .nodes
        .iter()
        .map(|n| (record_key(&n.node.id.0), n))
        .collect();
    let ports_by_id: HashMap<String, (&PortEntity, &NodeWithPorts)> = shaped
        .nodes
        .iter()
        .flat_map(|n| n.ports.iter().map(move |p| (record_key(&p.id.0), (p, n))))
        .collect();
    let end_of = |port: &PortEntity, owner: &NodeWithPorts| -> EndRef {
        let node_key = record_key(&owner.node.id.0);
        match lane_of.get(&node_key) {
            Some(lane) => EndRef::lane(lane, &port.key),
            None => EndRef::Node {
                node: node_key,
                key: port.key.clone(),
            },
        }
    };
    let managed = |port: &PortEntity, owner: &NodeWithPorts| {
        owner.node.lane.is_some()
            || matches!(parse_port_key(&port.key), Some(UniversalPort::Lane(_)))
    };
    let mut current: BTreeMap<(EndRef, EndRef), &EdgeConnectionEntity> = BTreeMap::new();
    for edge in &shaped.edges {
        let (Some((s, sn)), Some((t, tn))) = (
            ports_by_id.get(&record_key(&edge.source.0)),
            ports_by_id.get(&record_key(&edge.target.0)),
        ) else {
            continue;
        };
        if managed(s, sn) || managed(t, tn) {
            current.insert((end_of(s, sn), end_of(t, tn)), edge);
        }
    }
    let resolve = |end: &EndRef| -> Option<&PortEntity> {
        let (node, key) = match end {
            EndRef::Node { node, key } => (by_node.get(node).copied()?, key),
            EndRef::Lane { lane, key } => (by_lane.get(lane.as_str()).copied()?, key),
        };
        node.ports.iter().find(|p| &p.key == key)
    };
    for (pair, edge) in &current {
        if !desired.edges.contains(pair) {
            plan.edits.push(TopologyEdit::RetireEdge {
                edge: edge.id.clone(),
            });
            plan.batch.delete_edges.push(edge.id.clone());
        }
    }
    for (i, pair) in desired.edges.iter().enumerate() {
        if current.contains_key(pair) {
            continue;
        }
        let (Some(source), Some(target)) = (resolve(&pair.0), resolve(&pair.1)) else {
            return Err(OrchestrationError::Conflict(format!(
                "lane edge endpoint missing: {:?} -> {:?}",
                pair.0, pair.1
            )));
        };
        plan.edits.push(TopologyEdit::AddEdge {
            edge: EdgeConnectionEntity {
                id: ids::edge_id(&format!("pending-edge-{i}")),
                source: source.id.clone(),
                target: target.id.clone(),
            },
        });
        plan.batch.add_edges.push(BatchEdge {
            source: port_ref(source),
            target: port_ref(target),
        });
    }
    Ok(plan)
}

/// Whether the stored lanes of a topology differ from its expansion.
pub fn is_stale(topology: &CanvasTopology) -> bool {
    let desired = expand(topology);
    match diff(topology, &desired) {
        Ok(plan) => !plan.batch.is_empty(),
        Err(_) => true,
    }
}

// --- committing ------------------------------------------------------------------

/// An edit's own part of a reconciling write: the projection to validate and
/// the batch operations that write it.
#[derive(Debug, Clone, Default)]
pub struct Primary {
    pub edits: Vec<TopologyEdit>,
    pub batch: ApplyTopologyBatch,
}

/// A validated write: the primary edit plus the lane regeneration it implies.
#[derive(Debug, Clone)]
pub struct Prepared {
    pub batch: ApplyTopologyBatch,
    /// Whether the expansion changed anything beyond the primary edit. When it
    /// did not, a caller may write its edit through its usual query instead.
    pub reconciled: bool,
    pub root: crate::entities::surreal::canvas::CanvasId,
}

/// Projects the primary edit, expands the result, diffs, and validates the
/// final topology — the checker and the switch-safety rule both run on what the
/// fabric will actually derive.
pub async fn prepare(
    db: &SurrealProcessor,
    config: &OrchestrationConfig,
    topology: &CanvasTopology,
    primary: Primary,
) -> Result<Prepared, OrchestrationError> {
    let projected = topology.project(&primary.edits);
    let desired = expand(&projected);
    let plan = diff(&projected, &desired)?;
    let reconciled = !plan.batch.is_empty();
    let final_topology = projected.project(&plan.edits);
    ensure_valid(&final_topology)?;
    let views = db
        .process(ListServerConfigViewsByCanvases {
            canvases: final_topology.canvas_ids(),
        })
        .await?;
    ensure_switch_safe(&final_topology, &views, config)?;
    let mut batch = primary.batch;
    if batch.canvas.is_none() {
        batch.canvas = plan.batch.canvas.clone();
    }
    batch.extend(plan.batch);
    if batch.canvas.is_none() {
        batch.canvas = Some(topology.root.clone());
    }
    // The whole reconciling write fences against the snapshot it was validated
    // against: a concurrent edit that moved the tree rolls it back.
    batch.fence = Some(topology.fence().ok_or(OrchestrationError::NotFound)?);
    Ok(Prepared {
        batch,
        reconciled,
        root: topology.root.clone(),
    })
}

/// Writes a prepared batch, schedules the derivation and, when the caller names
/// one, publishes the live event for the edit.
///
/// The kind and ids are the caller's because this function does not know what it
/// is writing: the same batch shape carries a node replacement, an edge
/// connection and a disconnection. `None` is for callers that publish their own
/// event (they hold ids this function never sees).
pub async fn apply(
    db: &SurrealProcessor,
    notifier: &Notifier,
    prepared: Prepared,
    change: Option<(CanvasChangeKind, Vec<String>)>,
) -> Result<(), OrchestrationError> {
    let root = prepared.root.clone();
    db.process(prepared.batch).await?;
    notifier.notify(&root).await;
    if let Some((kind, ids)) = change {
        notifier.canvas_changed(&root, kind, ids).await;
    }
    Ok(())
}
