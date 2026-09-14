//! Universal nodes: the canvas shorthand for "one rule × N transit servers".
//!
//! Three node kinds let an operator draw a fan-out once instead of once per
//! transit server:
//!
//! - a **universal distributor** takes any number of entry pods (each one a
//!   *channel*, connected to a `chan:<pod>` port) and bundles all of them to any
//!   number of universal pods;
//! - a **universal pod** — one per server, created with it — receives bundles
//!   and lands every channel they carry on a generated pod of its server, then
//!   hands the bundle on (`bundle_out`) to another universal pod or to an
//!   aggregator;
//! - a **universal aggregator** exposes one `chan:<pod>` input per channel its
//!   bundles carry, to be fed by an exit.
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
//! The distributor's and aggregator's per-channel ports come in pairs: the
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
    NewPort, NodeEntity, NodeId, NodeSpec, NodeWithPorts, PodConfig, RelayConfig, RelayProtocol,
};
use crate::entities::surreal::port::{PortDirection, PortEntity, PortKind};
use crate::entities::surreal::server::ServerId;
use crate::entities::surreal::topology::CanvasTopology;
use crate::entities::surreal::view::ListServerConfigViewsByCanvases;
use crate::services::OrchestrationError;
use crate::services::converge::ensure_switch_safe;
use crate::services::node::{port_layout, port_rows};
use crate::services::rollout::DirtyNotifier;
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
pub const BUNDLE_OUT_PREFIX: &str = "bundle_out:";
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
pub fn bundle_out_key(target: &str) -> String {
    format!("{BUNDLE_OUT_PREFIX}{target}")
}

/// A universal node's port, decoded from its key.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UniversalPort<'a> {
    /// The operator-facing side of a channel.
    Chan(&'a str),
    /// The hidden side of a channel, where generated edges attach.
    Lane(&'a str),
    BundleIn(&'a str),
    /// `Some(target)` on a distributor, `None` for a universal pod's fixed port.
    BundleOut(Option<&'a str>),
}

pub fn parse_port_key(key: &str) -> Option<UniversalPort<'_>> {
    if key == BUNDLE_OUT {
        return Some(UniversalPort::BundleOut(None));
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
    if let Some(rest) = key.strip_prefix(BUNDLE_OUT_PREFIX) {
        return Some(UniversalPort::BundleOut(Some(rest)));
    }
    None
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

/// The next channel ordinal in a tree: one past the highest `chan:` position of
/// every distributor and aggregator. Ordinals are never reused, so a channel's
/// colour survives its neighbours' deletion.
pub fn next_ordinal(topology: &CanvasTopology) -> i64 {
    topology
        .nodes
        .iter()
        .filter(|n| {
            matches!(
                n.node.spec,
                NodeSpec::UniversalDistribute(_) | NodeSpec::UniversalAggregate(_)
            )
        })
        .flat_map(|n| n.ports.iter())
        .filter(|p| matches!(parse_port_key(&p.key), Some(UniversalPort::Chan(_))))
        .map(|p| p.position.saturating_add(1))
        .max()
        .unwrap_or(0)
}

/// Whether a universal node's ports have the shape its kind allows: only the
/// known keys, each with the right kind and direction, `chan:`/`lane:` in
/// pairs, and exactly one fixed `bundle_out` on a universal pod.
pub fn universal_port_shape_ok(node: &NodeWithPorts) -> bool {
    use PortDirection::{Input, Output};
    use PortKind::{Bundle, DeriveDestination};
    let mut chans: HashSet<&str> = HashSet::new();
    let mut lanes: HashSet<&str> = HashSet::new();
    let mut fixed_out = 0usize;
    for port in &node.ports {
        let Some(decoded) = parse_port_key(&port.key) else {
            return false;
        };
        let ok = match (&node.node.spec, decoded) {
            (NodeSpec::UniversalDistribute(_), UniversalPort::Chan(pod)) => {
                chans.insert(pod);
                (port.kind, port.direction) == (DeriveDestination, Output)
            }
            (NodeSpec::UniversalDistribute(_), UniversalPort::Lane(pod)) => {
                lanes.insert(pod);
                (port.kind, port.direction) == (DeriveDestination, Input)
            }
            (NodeSpec::UniversalDistribute(_), UniversalPort::BundleOut(Some(_))) => {
                (port.kind, port.direction) == (Bundle, Output)
            }
            (NodeSpec::UniversalPod(_), UniversalPort::BundleIn(_)) => {
                (port.kind, port.direction) == (Bundle, Input)
            }
            (NodeSpec::UniversalPod(_), UniversalPort::BundleOut(None)) => {
                fixed_out = fixed_out.saturating_add(1);
                (port.kind, port.direction) == (Bundle, Output)
            }
            (NodeSpec::UniversalAggregate(_), UniversalPort::BundleIn(_)) => {
                (port.kind, port.direction) == (Bundle, Input)
            }
            (NodeSpec::UniversalAggregate(_), UniversalPort::Chan(pod)) => {
                chans.insert(pod);
                (port.kind, port.direction) == (DeriveDestination, Input)
            }
            (NodeSpec::UniversalAggregate(_), UniversalPort::Lane(pod)) => {
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

/// The port shape of a freshly created universal node (before any connect).
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

/// One channel: an entry pod connected to a distributor.
struct Channel<'a> {
    pod: &'a NodeWithPorts,
    ordinal: i64,
    mode: LoadBalanceMode,
    protocol: RelayProtocol,
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

/// The display name of the far side of a bundle: a distributor's own name, a
/// universal pod's server name.
fn source_name(index: &Index<'_>, node: &NodeWithPorts) -> String {
    match universal_server(node) {
        Some(server) => server_name(index, server),
        None => node.node.name.clone(),
    }
}

pub fn expand(topology: &CanvasTopology) -> Desired {
    let index = Index::build(topology);
    let mut desired = Desired::default();

    // The universal nodes, keyed, and the bundle graph between them.
    let mut universal: BTreeMap<String, &NodeWithPorts> = BTreeMap::new();
    for node in &topology.nodes {
        if node.node.spec.is_universal() {
            universal.insert(record_key(&node.node.id.0), node);
        }
    }
    let mut outs: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
    let mut ins: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
    for edge in &topology.edges {
        let (Some((source, source_node)), Some((target, target_node))) =
            (index.port(&edge.source), index.port(&edge.target))
        else {
            continue;
        };
        if source.kind != PortKind::Bundle || target.kind != PortKind::Bundle {
            continue;
        }
        if !source_node.node.spec.is_universal() || !target_node.node.spec.is_universal() {
            continue;
        }
        let s = record_key(&source_node.node.id.0);
        let t = record_key(&target_node.node.id.0);
        outs.entry(s.clone()).or_default().insert(t.clone());
        ins.entry(t).or_default().insert(s);
    }

    // Ports follow the edges alone, whatever the lanes end up being.
    for (key, node) in &universal {
        let mut ports: Vec<NewPort> = Vec::new();
        let port = |key: String, kind: PortKind, direction: PortDirection, position: i64| NewPort {
            kind,
            direction,
            key,
            position,
        };
        match &node.node.spec {
            NodeSpec::UniversalDistribute(_) => {
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
                for target in outs.get(key).into_iter().flatten() {
                    ports.push(port(
                        bundle_out_key(target),
                        PortKind::Bundle,
                        PortDirection::Output,
                        0,
                    ));
                }
            }
            NodeSpec::UniversalPod(_) => {
                for source in ins.get(key).into_iter().flatten() {
                    ports.push(port(
                        bundle_in_key(source),
                        PortKind::Bundle,
                        PortDirection::Input,
                        0,
                    ));
                }
                ports.push(port(
                    BUNDLE_OUT.to_string(),
                    PortKind::Bundle,
                    PortDirection::Output,
                    0,
                ));
            }
            NodeSpec::UniversalAggregate(_) => {
                for source in ins.get(key).into_iter().flatten() {
                    ports.push(port(
                        bundle_in_key(source),
                        PortKind::Bundle,
                        PortDirection::Input,
                        0,
                    ));
                }
                // The channel ports are added below, once carried sets are known.
            }
            _ => {}
        }
        desired.ports.insert(key.clone(), ports);
    }

    // Channels: a distributor's `chan:` port whose edge lands on that pod's
    // `destination`. Anything else on such a port is reported by the checker
    // and carries nothing.
    let mut channels: BTreeMap<String, Channel<'_>> = BTreeMap::new();
    let mut own: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
    for (key, node) in &universal {
        let NodeSpec::UniversalDistribute(cfg) = &node.node.spec else {
            continue;
        };
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
                    mode: cfg.mode,
                    protocol: cfg.protocol,
                },
            );
            own.entry(key.clone()).or_default().insert(pod_key.to_string());
        }
    }

    // Topological order over the bundle graph (Kahn); what is left is a cycle.
    let mut indegree: BTreeMap<&str, usize> = universal
        .keys()
        .map(|k| (k.as_str(), ins.get(k).map(BTreeSet::len).unwrap_or(0)))
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

    // Materialisation, in bundle order so a node's landing pods exist before the
    // node decides how to join them.
    let mut feeders: BTreeMap<(String, String), Vec<EndRef>> = BTreeMap::new();
    let is_pod = |k: &str| universal.get(k).and_then(|n| universal_server(n)).is_some();
    let is_aggregate = |k: &str| {
        matches!(
            universal.get(k).map(|n| &n.node.spec),
            Some(NodeSpec::UniversalAggregate(_))
        )
    };
    for key in &order {
        let Some(node) = universal.get(*key) else {
            continue;
        };
        let group = node.node.id.clone();
        let canvas = node.node.canvas.clone();
        let position = node.node.position;
        let lane = |lane: Lane, name: String, shape: LaneShape| DesiredLane {
            lane,
            canvas: canvas.clone(),
            name,
            position,
            shape,
        };
        match &node.node.spec {
            NodeSpec::UniversalDistribute(_) => {
                let targets: Vec<&String> = outs
                    .get(*key)
                    .into_iter()
                    .flatten()
                    .filter(|t| is_pod(t))
                    .collect();
                for pod_key in own.get(*key).into_iter().flatten() {
                    let Some(channel) = channels.get(pod_key) else {
                        continue;
                    };
                    let pod_id = channel.pod.node.id.clone();
                    let pod_name = channel.pod.node.name.clone();
                    if targets.is_empty() {
                        desired.problems.push(
                            TopologyProblem::warning(
                                ProblemKind::ChannelNoTransit,
                                format!(
                                    "channel {pod_name} on {} is not bundled to any server yet",
                                    node.node.name
                                ),
                            )
                            .with_nodes(vec![group.clone(), pod_id]),
                        );
                        continue;
                    }
                    let mut relays: Vec<String> = Vec::new();
                    for target_key in &targets {
                        let Some(target) = universal.get(target_key.as_str()) else {
                            continue;
                        };
                        let Some(server) = universal_server(target) else {
                            continue;
                        };
                        let landing = Lane::new(
                            &target.node.id,
                            &pod_id,
                            LaneRole::Landing,
                            Some(&group),
                        );
                        let relay =
                            Lane::new(&group, &pod_id, LaneRole::Relay, Some(&target.node.id));
                        desired.edges.insert((
                            EndRef::lane(&landing.key, "listen"),
                            EndRef::lane(&relay.key, "listen"),
                        ));
                        relays.push(relay.key.clone());
                        desired.lanes.insert(
                            landing.key.clone(),
                            DesiredLane {
                                lane: landing,
                                canvas: target.node.canvas.clone(),
                                name: format!("{pod_name} via {}", node.node.name),
                                position: target.node.position,
                                shape: LaneShape::Landing {
                                    server: server.clone(),
                                    protocol: channel.protocol,
                                },
                            },
                        );
                        desired.lanes.insert(
                            relay.key.clone(),
                            lane(
                                relay,
                                format!("{pod_name} → {}", server_name(&index, server)),
                                LaneShape::Relay {
                                    protocol: channel.protocol,
                                },
                            ),
                        );
                    }
                    let sink = EndRef::node(&group, lane_key(pod_key));
                    if let [only] = relays.as_slice() {
                        desired
                            .edges
                            .insert((EndRef::lane(only, "destination"), sink));
                    } else {
                        let fan = Lane::new(&group, &pod_id, LaneRole::Distribute, None);
                        for (i, relay) in relays.iter().enumerate() {
                            desired.edges.insert((
                                EndRef::lane(relay, "destination"),
                                EndRef::lane(&fan.key, &format!("member_{i}")),
                            ));
                        }
                        desired
                            .edges
                            .insert((EndRef::lane(&fan.key, "destination"), sink));
                        desired.lanes.insert(
                            fan.key.clone(),
                            lane(
                                fan,
                                format!("{pod_name} · fan-out"),
                                LaneShape::Distribute {
                                    mode: channel.mode,
                                    members: u32::try_from(relays.len()).unwrap_or(u32::MAX),
                                },
                            ),
                        );
                    }
                }
            }
            NodeSpec::UniversalPod(_) => {
                let out = outs
                    .get(*key)
                    .into_iter()
                    .flatten()
                    .find(|t| is_pod(t) || is_aggregate(t));
                for pod_key in carried.get(*key).into_iter().flatten() {
                    let Some(channel) = channels.get(pod_key) else {
                        continue;
                    };
                    let pod_id = channel.pod.node.id.clone();
                    let pod_name = channel.pod.node.name.clone();
                    // The landing pods this node holds for the channel, one per
                    // incoming bundle carrying it.
                    let landings: Vec<String> = ins
                        .get(*key)
                        .into_iter()
                        .flatten()
                        .filter(|s| carried.get(*s).is_some_and(|c| c.contains(pod_key)))
                        .filter_map(|s| universal.get(s.as_str()))
                        .map(|s| Lane::key_for(&group, &pod_id, LaneRole::Landing, Some(&s.node.id)))
                        .collect();
                    let feeder = match landings.as_slice() {
                        [] => continue,
                        [only] => EndRef::lane(only, "destination"),
                        many => {
                            let join = Lane::new(&group, &pod_id, LaneRole::Aggregate, None);
                            for (i, landing) in many.iter().enumerate() {
                                desired.edges.insert((
                                    EndRef::lane(&join.key, &format!("copy_{i}")),
                                    EndRef::lane(landing, "destination"),
                                ));
                            }
                            let feeder = EndRef::lane(&join.key, "source");
                            desired.lanes.insert(
                                join.key.clone(),
                                lane(
                                    join,
                                    format!("{pod_name} · join"),
                                    LaneShape::Aggregate {
                                        copies: u32::try_from(many.len()).unwrap_or(u32::MAX),
                                    },
                                ),
                            );
                            feeder
                        }
                    };
                    match out.and_then(|t| universal.get(t.as_str())) {
                        None => desired.problems.push(
                            TopologyProblem::warning(
                                ProblemKind::ChannelNoExit,
                                format!(
                                    "channel {pod_name} lands on {} but goes nowhere from there",
                                    source_name(&index, node)
                                ),
                            )
                            .with_nodes(vec![group.clone(), pod_id]),
                        ),
                        Some(next) if universal_server(next).is_some() => {
                            let Some(server) = universal_server(next) else {
                                continue;
                            };
                            let landing =
                                Lane::new(&next.node.id, &pod_id, LaneRole::Landing, Some(&group));
                            let relay = Lane::new(&group, &pod_id, LaneRole::Relay, None);
                            desired.edges.insert((
                                EndRef::lane(&landing.key, "listen"),
                                EndRef::lane(&relay.key, "listen"),
                            ));
                            desired
                                .edges
                                .insert((EndRef::lane(&relay.key, "destination"), feeder));
                            desired.lanes.insert(
                                landing.key.clone(),
                                DesiredLane {
                                    lane: landing,
                                    canvas: next.node.canvas.clone(),
                                    name: format!("{pod_name} via {}", source_name(&index, node)),
                                    position: next.node.position,
                                    shape: LaneShape::Landing {
                                        server: server.clone(),
                                        protocol: channel.protocol,
                                    },
                                },
                            );
                            desired.lanes.insert(
                                relay.key.clone(),
                                lane(
                                    relay,
                                    format!("{pod_name} → {}", server_name(&index, server)),
                                    LaneShape::Relay {
                                        protocol: channel.protocol,
                                    },
                                ),
                            );
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
            NodeSpec::UniversalAggregate(_) => {
                let ports = desired.ports.entry((*key).to_string()).or_default();
                for pod_key in carried.get(*key).into_iter().flatten() {
                    let Some(channel) = channels.get(pod_key) else {
                        continue;
                    };
                    let pod_id = channel.pod.node.id.clone();
                    let pod_name = channel.pod.node.name.clone();
                    ports.push(NewPort {
                        kind: PortKind::DeriveDestination,
                        direction: PortDirection::Input,
                        key: chan_key(pod_key),
                        position: channel.ordinal,
                    });
                    ports.push(NewPort {
                        kind: PortKind::DeriveDestination,
                        direction: PortDirection::Output,
                        key: lane_key(pod_key),
                        position: channel.ordinal,
                    });
                    let source = EndRef::node(&group, lane_key(pod_key));
                    let mut requests = feeders
                        .remove(&((*key).to_string(), pod_key.clone()))
                        .unwrap_or_default();
                    requests.sort();
                    match requests.as_slice() {
                        [] => {}
                        [only] => {
                            desired.edges.insert((source, only.clone()));
                        }
                        many => {
                            let join = Lane::new(&group, &pod_id, LaneRole::Aggregate, None);
                            desired
                                .edges
                                .insert((source, EndRef::lane(&join.key, "source")));
                            for (i, request) in many.iter().enumerate() {
                                desired.edges.insert((
                                    EndRef::lane(&join.key, &format!("copy_{i}")),
                                    request.clone(),
                                ));
                            }
                            desired.lanes.insert(
                                join.key.clone(),
                                lane(
                                    join,
                                    format!("{pod_name} · join"),
                                    LaneShape::Aggregate {
                                        copies: u32::try_from(many.len()).unwrap_or(u32::MAX),
                                    },
                                ),
                            );
                        }
                    }
                }
            }
            _ => {}
        }
    }
    desired
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
fn free_port(taken: &mut HashSet<(String, u16)>, server: &ServerId) -> Result<u16, OrchestrationError> {
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
            NodeSpec::LoadBalanceDistribute(LoadBalanceDistributeConfig { mode: *mode }),
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
            NodeSpec::LoadBalanceAggregate(LoadBalanceAggregateConfig {}),
            *copies,
        ),
    }
}

/// Whether two lane specs differ in anything the expansion decides.
fn spec_differs(current: &NodeSpec, desired: &NodeSpec) -> bool {
    match (current, desired) {
        (NodeSpec::LoadBalanceDistribute(a), NodeSpec::LoadBalanceDistribute(b)) => a.mode != b.mode,
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
        .find(|n| n.node.spec.is_universal())
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
    Ok(Prepared {
        batch,
        reconciled,
        root: topology.root.clone(),
    })
}

/// Writes a prepared batch and schedules the derivation.
pub async fn apply(
    db: &SurrealProcessor,
    notifier: &DirtyNotifier,
    prepared: Prepared,
) -> Result<(), OrchestrationError> {
    let root = prepared.root.clone();
    db.process(prepared.batch).await?;
    notifier.notify(&root).await;
    Ok(())
}
