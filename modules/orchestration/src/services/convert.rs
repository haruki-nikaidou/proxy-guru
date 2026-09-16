//! Converting the node model into the pod graph, once.
//!
//! The node model spelled the graph out in drawing terms: a pod node listened
//! as whatever Entry or Relay fed its `listen` port and went wherever the chain
//! behind its `destination` port led, through relays, load balancers,
//! universal-node lanes and canvas boundaries. The walk here is the one
//! derivation made ([`Index::peer`] looks through the boundaries and lanes), so
//! the converted graph means what the node model meant:
//!
//! - every pod node becomes a pod with the same id, server, name and port, so
//!   its relay leaf and its health history stay its own;
//! - the node feeding `listen` becomes the ingress (an Entry: clients, raw or
//!   TLS; a relay: its protocol);
//! - the chain behind `destination` becomes the route: an exit or a relay is
//!   one edge (to the exit, or to the pod the relay dials, with the relay's
//!   address overrides), a load balancer a `balance` (a sticky one for
//!   `ip_hash`, a `failover` for `fallback`), an aggregate node the chain
//!   behind its first input;
//! - an exit node becomes an exit with the same id; an import node makes its
//!   canvas the child of the importing one;
//! - an operator's load-balance distribute and aggregate nodes become groups
//!   the dashboard draws as a splitter and an aggregator.
//!
//! What cannot be kept is dropped and said so. Before anything is written the
//! result is compiled and compared, server by server and pod by pod, with what
//! the node model derives against the same certificates.

use crate::config::OrchestrationConfig;
use crate::entities::db::ca::{FindInternalCa, RelayCertificateEntity};
use crate::entities::db::canvas::{CanvasId, SNAPSHOT_READ};
use crate::entities::db::certificate::CertificateEntity;
use crate::entities::db::edge::{EdgeEntity, EdgeId, EdgeTarget};
use crate::entities::db::exit::{ExitEntity, ExitId};
use crate::entities::db::graph::{CanvasPlacement, ConvertedRows, write_conversion};
use crate::entities::db::group::{GroupEntity, GroupId, GroupMember};
use crate::entities::db::node::{
    LoadBalanceMode, NodeId, NodeSpec, NodeWithPorts, RelayProtocol as NodeRelayProtocol,
};
use crate::entities::db::pod::{IngressKind, PodEntity, PodId, PodIngress};
use crate::entities::db::server::ServerEntity;
use crate::entities::db::topology::{CanvasTopology, load_canvas};
use crate::entities::db::view::{CertificateKind, ListenProtocol as NodeListenProtocol};
use crate::services::OrchestrationError;
use crate::services::derive::{
    DerivationCertificates, DerivedConfig, derive_server_config, relay_tls_pods, tls_snis,
};
use crate::services::graph::{topology_certificates, topology_graph};
use crate::services::topology::{Index, manual_inputs};
use base::db::Db;
use guru_topology as topo;
use guru_worker_config::{ForwardingTo, LoadBalanceStrategy, To};
use kanau::processor::Processor;
use serde::Serialize;
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};

/// How many times pruning re-checks a converted tree before giving up on it.
const MAX_PRUNE_ROUNDS: usize = 64;

#[derive(Clone)]
pub struct ConvertService {
    pub db: Db,
    pub config: OrchestrationConfig,
}

/// Converts every canvas tree of the node model. `dry_run` reports without
/// writing; `replace` discards an earlier conversion first; a conversion whose
/// compiled graph differs from what the node model derives is written only with
/// `accept_differences`.
#[derive(Debug, Clone, Copy, Default)]
pub struct ConvertNodeModel {
    pub dry_run: bool,
    pub replace: bool,
    pub accept_differences: bool,
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct ConversionReport {
    pub trees: Vec<TreeReport>,
    /// Whether the conversion was written.
    pub written: bool,
}

impl ConversionReport {
    pub fn differences(&self) -> usize {
        self.trees.iter().map(|t| t.differences.len()).sum()
    }
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct TreeReport {
    pub root: String,
    pub name: String,
    pub canvases: usize,
    pub pods: usize,
    pub exits: usize,
    pub edges: usize,
    pub groups: usize,
    /// What the node model had that the graph does not keep.
    pub notes: Vec<String>,
    /// The warnings the converted graph checks with.
    pub warnings: Vec<String>,
    /// Where the converted graph compiles to something other than what the
    /// node model derives.
    pub differences: Vec<String>,
}

impl Processor<ConvertNodeModel> for ConvertService {
    type Output = ConversionReport;
    type Error = OrchestrationError;
    #[tracing::instrument(name = "Service:ConvertNodeModel", skip_all, err)]
    async fn process(&self, input: ConvertNodeModel) -> Result<Self::Output, Self::Error> {
        let internal_ca = self.db.process(FindInternalCa).await?.is_some();
        // One snapshot of the whole node model; the writes only touch tables the
        // node model does not read.
        let begin = if input.dry_run {
            SNAPSHOT_READ
        } else {
            "BEGIN ISOLATION LEVEL REPEATABLE READ"
        };
        let mut tx = self
            .db
            .db()
            .begin_with(begin)
            .await
            .map_err(base::db::Error::from)?;
        let roots: Vec<CanvasId> = sqlx::query_scalar(
            "SELECT id FROM orchestration_canvas
             WHERE id NOT IN (SELECT import_canvas FROM orchestration_node
                              WHERE import_canvas IS NOT NULL)
             ORDER BY id",
        )
        .fetch_all(&mut *tx)
        .await
        .map_err(base::db::Error::from)?;

        let mut report = ConversionReport::default();
        let mut trees = Vec::with_capacity(roots.len());
        for root in roots {
            let topology = load_canvas(&mut tx, &root).await?.into_topology();
            let snis = tls_snis(&topology);
            let acme: Vec<CertificateEntity> =
                sqlx::query_as("SELECT * FROM certificate WHERE sni = ANY($1)")
                    .bind(&snis)
                    .fetch_all(&mut *tx)
                    .await
                    .map_err(base::db::Error::from)?;
            let converted = convert_tree(&topology);
            let mut relay_pods: Vec<String> = relay_tls_pods(&topology)
                .iter()
                .map(|pod| pod.to_string())
                .collect();
            relay_pods.extend(
                converted
                    .rows
                    .pods
                    .iter()
                    .filter(|pod| pod.ingress.needs_relay_certificate())
                    .map(|pod| pod.id.to_string()),
            );
            let relay: Vec<RelayCertificateEntity> =
                sqlx::query_as("SELECT * FROM relay_certificate WHERE pod = ANY($1)")
                    .bind(&relay_pods)
                    .fetch_all(&mut *tx)
                    .await
                    .map_err(base::db::Error::from)?;
            let comparison = compare(
                &topology,
                &converted.rows,
                &acme,
                &relay,
                internal_ca,
                &self.config,
            );
            let name = topology
                .canvases
                .iter()
                .find(|c| c.id == topology.root)
                .map(|c| c.name.clone())
                .unwrap_or_default();
            report.trees.push(TreeReport {
                root: root.to_string(),
                name,
                canvases: topology.canvases.len(),
                pods: converted.rows.pods.len(),
                exits: converted.rows.exits.len(),
                edges: converted.rows.edges.len(),
                groups: converted.rows.groups.len(),
                notes: converted.notes,
                warnings: comparison.warnings,
                differences: comparison.differences,
            });
            trees.push(converted.rows);
        }

        let refused = report.differences() > 0 && !input.accept_differences;
        if input.dry_run || refused {
            tx.rollback().await.map_err(base::db::Error::from)?;
            return Ok(report);
        }
        let document = serde_json::to_value(&report).unwrap_or_default();
        write_conversion(&mut tx, &trees, input.replace, &document).await?;
        tx.commit().await.map_err(base::db::Error::from)?;
        report.written = true;
        Ok(report)
    }
}

/// One tree converted, and what did not survive.
#[derive(Debug, Clone, Default)]
pub struct Converted {
    pub rows: ConvertedRows,
    pub notes: Vec<String>,
}

/// The pod graph a node-model tree stands for. Pure: the same tree converts to
/// the same graph, up to the ids of the edges and groups it creates.
pub fn convert_tree(topology: &CanvasTopology) -> Converted {
    let index = Index::build(topology);
    let mut notes = Vec::new();
    let mut nodes: Vec<&NodeWithPorts> = topology.nodes.iter().collect();
    nodes.sort_by(|a, b| a.node.id.cmp(&b.node.id));
    let servers: HashSet<&str> = topology.servers.iter().map(|s| s.id.as_str()).collect();

    let placements = nodes
        .iter()
        .filter_map(|node| match &node.node.spec {
            NodeSpec::CanvasImport(cfg) => Some(CanvasPlacement {
                canvas: cfg.canvas.clone(),
                parent: node.node.canvas.clone(),
                position: node.node.position,
            }),
            _ => None,
        })
        .collect();

    let exits: Vec<ExitEntity> = nodes
        .iter()
        .filter_map(|node| match &node.node.spec {
            NodeSpec::Exit(cfg) => Some(ExitEntity {
                id: ExitId::from_key(node.node.id.as_str()),
                canvas: node.node.canvas.clone(),
                name: node.node.name.clone(),
                comment: node.node.comment.clone(),
                destination: cfg.destination.clone(),
                send_proxy_protocol: cfg.pass_proxy_protocol,
                position: node.node.position,
            }),
            _ => None,
        })
        .collect();

    let mut pods = Vec::new();
    let mut edges = Vec::new();
    let mut families: Vec<(EdgeEntity, Vec<NodeId>)> = Vec::new();
    for node in &nodes {
        let NodeSpec::Pod(cfg) = &node.node.spec else {
            continue;
        };
        if !servers.contains(cfg.server.as_str()) {
            notes.push(format!(
                "pod {}: its server is not on this canvas tree; dropped",
                node.node.name
            ));
            continue;
        }
        let consumer = index
            .port_by_key(node, "listen")
            .and_then(|port| index.peer(port));
        let ingress = match consumer.map(|c| &c.node.spec) {
            Some(NodeSpec::Entry(entry)) => match &entry.tls {
                None => PodIngress::ClientRaw {
                    receive_proxy_protocol: entry.receive_proxy_protocol,
                },
                Some(tls) => PodIngress::ClientTls {
                    receive_proxy_protocol: entry.receive_proxy_protocol,
                    tls: tls.clone(),
                },
            },
            Some(NodeSpec::Relay(relay)) => relay_ingress(relay.protocol),
            _ => PodIngress::RelayTcp,
        };
        let mut walk = PodWalk {
            index: &index,
            source: PodId::from_key(node.node.id.as_str()),
            edges: Vec::new(),
            stack: Vec::new(),
            visited: Vec::new(),
        };
        let producer = index
            .port_by_key(node, "destination")
            .and_then(|port| index.peer(port));
        let route = match producer {
            None => None,
            Some(producer) => match walk.walk(producer) {
                Ok(route) => Some(route),
                Err(why) => {
                    notes.push(format!(
                        "pod {}: {why}; it keeps no route",
                        node.node.name
                    ));
                    walk.edges.clear();
                    None
                }
            },
        };
        for (edge, family) in walk.edges {
            edges.push(edge.clone());
            families.push((edge, family));
        }
        pods.push(PodEntity {
            id: PodId::from_key(node.node.id.as_str()),
            canvas: node.node.canvas.clone(),
            server: cfg.server.clone(),
            name: node.node.name.clone(),
            comment: node.node.comment.clone(),
            port: cfg.port,
            bind_ip: cfg.bind_ip.clone(),
            advertise_ip: cfg.advertise_ip.clone(),
            ingress,
            route,
        });
    }
    // A relay dials in its own protocol; the pod it dials listens in the
    // protocol of whatever feeds its `listen` port, which is that same relay
    // unless the port was fed twice.
    let kinds: HashMap<&PodId, IngressKind> =
        pods.iter().map(|p| (&p.id, p.ingress.kind())).collect();
    for node in &nodes {
        let NodeSpec::Relay(relay) = &node.node.spec else {
            continue;
        };
        let Some(target) = index
            .port_by_key(node, "listen")
            .and_then(|port| index.peer(port))
        else {
            continue;
        };
        let target_id = PodId::from_key(target.node.id.as_str());
        if let Some(kind) = kinds.get(&target_id)
            && *kind != relay_ingress(relay.protocol).kind()
        {
            notes.push(format!(
                "relay {} dials pod {} over {:?}, but the pod listens as {kind}; the pod's listener wins",
                node.node.name, target.node.name, relay.protocol
            ));
        }
    }

    let groups = groups_of(&nodes, &families, &mut notes);
    let mut rows = ConvertedRows {
        placements,
        pods,
        exits,
        edges,
        groups,
    };
    prune(&mut rows, &topology.servers, &mut notes);
    Converted { rows, notes }
}

fn relay_ingress(protocol: NodeRelayProtocol) -> PodIngress {
    match protocol {
        NodeRelayProtocol::TcpRaw => PodIngress::RelayTcp,
        NodeRelayProtocol::TcpTls => PodIngress::RelayTls,
        NodeRelayProtocol::Quic => PodIngress::RelayQuic,
    }
}

/// The walk behind one pod's `destination` port.
struct PodWalk<'t, 'i> {
    index: &'i Index<'t>,
    source: PodId,
    /// Every edge the walk created, with the operator load-balance nodes above it.
    edges: Vec<(EdgeEntity, Vec<NodeId>)>,
    stack: Vec<NodeId>,
    visited: Vec<NodeId>,
}

impl PodWalk<'_, '_> {
    fn walk(&mut self, node: &NodeWithPorts) -> Result<topo::Route, String> {
        if self.visited.contains(&node.node.id) {
            return Err(format!("its chain runs in a cycle through {}", node.node.name));
        }
        if self.visited.len() >= topo::MAX_ROUTE_DEPTH {
            return Err("its chain nests too deep".to_string());
        }
        self.visited.push(node.node.id.clone());
        let route = self.step(node);
        self.visited.pop();
        route
    }

    fn step(&mut self, node: &NodeWithPorts) -> Result<topo::Route, String> {
        match &node.node.spec {
            NodeSpec::Exit(_) => Ok(self.edge(
                EdgeTarget::Exit(ExitId::from_key(node.node.id.as_str())),
                None,
                None,
            )),
            NodeSpec::Relay(relay) => {
                let target = self
                    .index
                    .port_by_key(node, "listen")
                    .and_then(|port| self.index.peer(port));
                match target {
                    Some(pod) if matches!(pod.node.spec, NodeSpec::Pod(_)) => Ok(self.edge(
                        EdgeTarget::Pod(PodId::from_key(pod.node.id.as_str())),
                        relay.override_ip_address.clone(),
                        relay.override_port,
                    )),
                    _ => Err(format!("relay {} dials no pod", node.node.name)),
                }
            }
            NodeSpec::LoadBalanceDistribute(cfg) => {
                self.stack.push(family_of(node));
                let mut members = Vec::new();
                let mut failed = None;
                for port in manual_inputs(node) {
                    let Some(member) = self.index.peer(port) else {
                        continue;
                    };
                    match self.walk(member) {
                        Ok(route) => members.push(route),
                        Err(why) => {
                            failed = Some(why);
                            break;
                        }
                    }
                }
                self.stack.pop();
                if let Some(why) = failed {
                    return Err(why);
                }
                if members.is_empty() {
                    return Err(format!(
                        "load balancer {} has no connected member",
                        node.node.name
                    ));
                }
                let weighted = |members: Vec<topo::Route>| {
                    members
                        .into_iter()
                        .map(|to| topo::Weighted { weight: 1, to })
                        .collect()
                };
                Ok(match cfg.mode {
                    LoadBalanceMode::RoundRobin | LoadBalanceMode::Random => topo::Route::Balance {
                        members: weighted(members),
                        sticky: None,
                    },
                    LoadBalanceMode::IpHash => topo::Route::Balance {
                        members: weighted(members),
                        sticky: Some(topo::Sticky::ClientIp),
                    },
                    LoadBalanceMode::Fallback => topo::Route::Failover(members),
                })
            }
            NodeSpec::LoadBalanceAggregate(_) => {
                let source = manual_inputs(node)
                    .into_iter()
                    .next()
                    .and_then(|port| self.index.peer(port));
                let Some(source) = source else {
                    return Err(format!("aggregate {} has no source", node.node.name));
                };
                self.stack.push(family_of(node));
                let route = self.walk(source);
                self.stack.pop();
                route
            }
            NodeSpec::Pod(_)
            | NodeSpec::Entry(_)
            | NodeSpec::CanvasImport(_)
            | NodeSpec::CanvasExport(_)
            | NodeSpec::UniversalPod(_) => Err(format!(
                "{} cannot carry traffic on",
                node.node.name
            )),
        }
    }

    fn edge(
        &mut self,
        target: EdgeTarget,
        override_ip: Option<String>,
        override_port: Option<u16>,
    ) -> topo::Route {
        let id = EdgeId::new();
        let route = topo::Route::Edge(topo::EdgeId::new(id.as_str()));
        self.edges.push((
            EdgeEntity {
                id,
                source: self.source.clone(),
                target,
                override_ip,
                override_port,
            },
            self.stack.clone(),
        ));
        route
    }
}

/// The operator node a load-balance node belongs to: itself, or the node its
/// lane was generated for.
fn family_of(node: &NodeWithPorts) -> NodeId {
    node.node
        .lane
        .as_ref()
        .map(|lane| lane.group.clone())
        .unwrap_or_else(|| node.node.id.clone())
}

/// A splitter group for every operator distribute node and an aggregator group
/// for every operator aggregate node that carried traffic: the pods dialing
/// through it, the edges below it and what they lead to.
fn groups_of(
    nodes: &[&NodeWithPorts],
    families: &[(EdgeEntity, Vec<NodeId>)],
    notes: &mut Vec<String>,
) -> Vec<GroupEntity> {
    let mut groups = Vec::new();
    for node in nodes {
        if node.node.lane.is_some() {
            continue;
        }
        let (kind, mut props) = match &node.node.spec {
            NodeSpec::LoadBalanceDistribute(cfg) => (
                "splitter",
                serde_json::json!({
                    "mode": serde_json::to_value(cfg.mode).unwrap_or_default(),
                    "protocol": serde_json::to_value(cfg.protocol).unwrap_or_default(),
                }),
            ),
            NodeSpec::LoadBalanceAggregate(_) => ("aggregator", serde_json::json!({})),
            NodeSpec::UniversalPod(_) | NodeSpec::Entry(_) | NodeSpec::Relay(_) => continue,
            _ => continue,
        };
        let mut members = Vec::new();
        let mut seen = HashSet::new();
        let mut push = |member: GroupMember, members: &mut Vec<GroupMember>| {
            if seen.insert(member.clone()) {
                members.push(member);
            }
        };
        for (edge, family) in families {
            if !family.contains(&node.node.id) {
                continue;
            }
            push(GroupMember::Pod(edge.source.clone()), &mut members);
            push(GroupMember::Edge(edge.id.clone()), &mut members);
            push(
                match &edge.target {
                    EdgeTarget::Pod(pod) => GroupMember::Pod(pod.clone()),
                    EdgeTarget::Exit(exit) => GroupMember::Exit(exit.clone()),
                },
                &mut members,
            );
        }
        if members.is_empty() {
            notes.push(format!(
                "{kind} {} carried no traffic; not kept",
                node.node.name
            ));
            continue;
        }
        if let Some(object) = props.as_object_mut() {
            object.insert("x".to_string(), node.node.position.x.into());
            object.insert("y".to_string(), node.node.position.y.into());
            object.insert("node".to_string(), node.node.id.to_string().into());
        }
        groups.push(GroupEntity {
            id: GroupId::new(),
            canvas: node.node.canvas.clone(),
            kind: kind.to_string(),
            name: node.node.name.clone(),
            props,
            members,
        });
    }
    groups
}

/// Drops whatever the converted graph is rejected for, until it checks: a pod
/// or exit the error is about goes, with every route that led to it; an edge
/// the error is about takes its pod's route with it.
fn prune(rows: &mut ConvertedRows, servers: &[ServerEntity], notes: &mut Vec<String>) {
    let config = OrchestrationConfig::default();
    for _ in 0..MAX_PRUNE_ROUNDS {
        let report = topo::check(&topology_graph(
            servers,
            &rows.pods,
            &rows.exits,
            &rows.edges,
            &config,
        ));
        let errors: Vec<&topo::Diagnostic> = report.errors().collect();
        if errors.is_empty() {
            return;
        }
        let mut doomed_pods: BTreeSet<String> = BTreeSet::new();
        let mut doomed_exits: BTreeSet<String> = BTreeSet::new();
        let mut routeless: BTreeSet<String> = BTreeSet::new();
        for error in errors {
            notes.push(format!("{:?}: {}", error.problem, error.message));
            for subject in &error.subjects {
                match subject {
                    topo::Subject::Pod(pod) => {
                        doomed_pods.insert(pod.to_string());
                    }
                    topo::Subject::Exit(exit) => {
                        doomed_exits.insert(exit.to_string());
                    }
                    topo::Subject::Edge(edge) => {
                        if let Some(edge) = rows.edges.iter().find(|e| e.id.as_str() == edge.as_str())
                        {
                            routeless.insert(edge.source.to_string());
                        }
                    }
                    topo::Subject::Server(_) => {}
                }
            }
        }
        for edge in &rows.edges {
            let into_doomed = match &edge.target {
                EdgeTarget::Pod(pod) => doomed_pods.contains(pod.as_str()),
                EdgeTarget::Exit(exit) => doomed_exits.contains(exit.as_str()),
            };
            if into_doomed {
                routeless.insert(edge.source.to_string());
            }
        }
        for pod in &doomed_pods {
            notes.push(format!("pod {pod} dropped"));
        }
        for exit in &doomed_exits {
            notes.push(format!("exit {exit} dropped"));
        }
        for pod in rows.pods.iter_mut() {
            if routeless.contains(pod.id.as_str()) && pod.route.is_some() {
                notes.push(format!("pod {}: route dropped", pod.name));
                pod.route = None;
            }
        }
        rows.pods.retain(|p| !doomed_pods.contains(p.id.as_str()));
        rows.exits.retain(|e| !doomed_exits.contains(e.id.as_str()));
        rows.edges.retain(|e| {
            !routeless.contains(e.source.as_str()) && !doomed_pods.contains(e.source.as_str())
        });
        let edges: HashSet<&str> = rows.edges.iter().map(|e| e.id.as_str()).collect();
        let pods: HashSet<&str> = rows.pods.iter().map(|p| p.id.as_str()).collect();
        let exits: HashSet<&str> = rows.exits.iter().map(|e| e.id.as_str()).collect();
        for group in rows.groups.iter_mut() {
            group.members.retain(|member| match member {
                GroupMember::Pod(id) => pods.contains(id.as_str()),
                GroupMember::Edge(id) => edges.contains(id.as_str()),
                GroupMember::Exit(id) => exits.contains(id.as_str()),
                GroupMember::Server(_) => true,
            });
        }
        rows.groups.retain(|group| !group.members.is_empty());
    }
    notes.push("the converted graph still does not check; giving up on pruning".to_string());
}

#[derive(Debug, Default)]
pub struct Comparison {
    pub warnings: Vec<String>,
    pub differences: Vec<String>,
}

/// Compiles the converted rows as every worker read them before route tables
/// and compares, pod by pod, with what the node model derives.
pub fn compare(
    topology: &CanvasTopology,
    rows: &ConvertedRows,
    acme: &[CertificateEntity],
    relay: &[RelayCertificateEntity],
    internal_ca: bool,
    config: &OrchestrationConfig,
) -> Comparison {
    let mut out = Comparison::default();
    let mut servers = topology.servers.clone();
    for server in servers.iter_mut() {
        server.capabilities.clear();
    }
    let graph = topology_graph(&servers, &rows.pods, &rows.exits, &rows.edges, config);
    let compiled = match topo::compile(&graph, &topology_certificates(acme, relay, internal_ca)) {
        Ok(compiled) => compiled,
        Err(report) => {
            for error in report.errors() {
                out.differences
                    .push(format!("the graph does not check: {}", error.message));
            }
            return out;
        }
    };
    out.warnings = compiled
        .warnings
        .iter()
        .map(|w| format!("{:?}: {}", w.problem, w.message))
        .collect();

    let certificates = DerivationCertificates {
        acme: acme.to_vec(),
        relay: relay.to_vec(),
        ca_present: internal_ca,
        assume_issued: false,
    };
    let names: HashMap<&str, &str> = rows
        .pods
        .iter()
        .map(|p| (p.id.as_str(), p.name.as_str()))
        .collect();
    for server in &topology.servers {
        let label = &server.name;
        let old = match derive_server_config(topology, &server.id, &certificates, config) {
            Ok(old) => old,
            Err(e) => {
                out.differences
                    .push(format!("{label}: the node model does not derive: {e}"));
                continue;
            }
        };
        let Some(new) = compiled
            .servers
            .get(&topo::ServerId::new(server.id.as_str()))
        else {
            out.differences
                .push(format!("{label}: missing from the compiled graph"));
            continue;
        };
        compare_server(label, &old, new, &names, &mut out.differences);
    }
    out
}

fn compare_server(
    label: &str,
    old: &DerivedConfig,
    new: &topo::ServerConfig,
    names: &HashMap<&str, &str>,
    out: &mut Vec<String>,
) {
    let old_served: BTreeMap<&str, usize> = old
        .forwardings
        .iter()
        .enumerate()
        .map(|(i, deps)| (deps.pod.as_str(), i))
        .collect();
    let new_served: BTreeMap<&str, usize> = new
        .deps
        .iter()
        .enumerate()
        .map(|(i, deps)| (deps.pod.as_str(), i))
        .collect();
    let old_invalid: BTreeMap<&str, &str> = old
        .invalid
        .iter()
        .map(|p| (p.node.as_str(), p.error.as_str()))
        .collect();
    let new_invalid: BTreeMap<&str, &str> = new
        .invalid
        .iter()
        .map(|p| (p.pod.as_str(), p.message.as_str()))
        .collect();
    let pods: BTreeSet<&str> = old_served
        .keys()
        .chain(new_served.keys())
        .chain(old_invalid.keys())
        .chain(new_invalid.keys())
        .copied()
        .collect();
    for pod in pods {
        let name = names.get(pod).copied().unwrap_or(pod);
        let at = format!("{label}: pod {name} ({pod})");
        match (old_served.get(pod), new_served.get(pod)) {
            (Some(&o), Some(&n)) => {
                let (Some(of), Some(nf)) =
                    (old.config.forwardings.get(o), new.forwardings.get(n))
                else {
                    continue;
                };
                if of.listen != nf.listen {
                    out.push(format!("{at}: listens on {} instead of {}", nf.listen, of.listen));
                }
                if of.listen_as != nf.listen_as {
                    out.push(format!(
                        "{at}: listens as {:?} instead of {:?}",
                        nf.listen_as, of.listen_as
                    ));
                }
                if of.receive_proxy_protocol != nf.receive_proxy_protocol {
                    out.push(format!(
                        "{at}: reads PROXY {:?} instead of {:?}",
                        nf.receive_proxy_protocol, of.receive_proxy_protocol
                    ));
                }
                if of.quic != nf.quic {
                    out.push(format!(
                        "{at}: QUIC listener {:?} instead of {:?}",
                        nf.quic, of.quic
                    ));
                }
                let old_to = match &of.to {
                    To::Tree(tree) => Some(without_random(tree)),
                    To::Route(_) => None,
                };
                let new_to = nf.to.tree().cloned();
                if old_to != new_to {
                    out.push(format!(
                        "{at}: goes to\n      {new_to:?}\n    instead of\n      {old_to:?}"
                    ));
                }
                if let (Some(od), Some(nd)) = (old.forwardings.get(o), new.deps.get(n)) {
                    let old_points: BTreeSet<(String, i64, &str)> = od
                        .points_at
                        .iter()
                        .map(|cap| (cap.server.to_string(), cap.port, listen_name(cap.protocol)))
                        .collect();
                    let new_points: BTreeSet<(String, i64, &str)> = nd
                        .points_at
                        .iter()
                        .map(|l| {
                            (
                                l.server.to_string(),
                                i64::from(l.port),
                                topo_listen_name(l.protocol),
                            )
                        })
                        .collect();
                    if old_points != new_points {
                        out.push(format!(
                            "{at}: depends on {new_points:?} instead of {old_points:?}"
                        ));
                    }
                    let old_refs: BTreeSet<(bool, &str, i64)> = od
                        .certificates
                        .iter()
                        .map(|r| (r.kind == CertificateKind::Acme, r.key.as_str(), r.version))
                        .collect();
                    let new_refs: BTreeSet<(bool, &str, i64)> = nd
                        .certificates
                        .iter()
                        .map(|r| {
                            (
                                r.kind == topo::CertificateKind::Acme,
                                r.key.as_str(),
                                r.version,
                            )
                        })
                        .collect();
                    if old_refs != new_refs {
                        out.push(format!(
                            "{at}: pins {new_refs:?} instead of {old_refs:?}"
                        ));
                    }
                }
            }
            (Some(_), None) => match new_invalid.get(pod) {
                Some(why) => out.push(format!("{at}: served before, now invalid: {why}")),
                None => out.push(format!("{at}: served before, not any more")),
            },
            (None, Some(_)) => match old_invalid.get(pod) {
                Some(why) => out.push(format!("{at}: was invalid ({why}), now served")),
                None => out.push(format!("{at}: not served before, served now")),
            },
            (None, None) => {
                // Invalid on one side or both; only a changed verdict matters.
                if old_invalid.contains_key(pod) != new_invalid.contains_key(pod) {
                    out.push(format!(
                        "{at}: invalid before {:?}, invalid now {:?}",
                        old_invalid.get(pod),
                        new_invalid.get(pod)
                    ));
                }
            }
        }
    }
}

/// A tree with every `random` balance read as the weighted round robin the
/// graph has instead.
fn without_random(tree: &ForwardingTo) -> ForwardingTo {
    match tree {
        ForwardingTo::LoadBalance(group) => {
            let mut group = group.as_ref().clone();
            if group.strategy == LoadBalanceStrategy::Random {
                group.strategy = LoadBalanceStrategy::RoundRobin;
            }
            group.members = group.members.iter().map(without_random).collect();
            ForwardingTo::LoadBalance(Box::new(group))
        }
        other => other.clone(),
    }
}

fn listen_name(protocol: NodeListenProtocol) -> &'static str {
    protocol.name()
}

fn topo_listen_name(protocol: topo::ListenProtocol) -> &'static str {
    match protocol {
        topo::ListenProtocol::Raw => "raw",
        topo::ListenProtocol::RelayTcp => "relay_tcp",
        topo::ListenProtocol::RelayTls => "relay_tls",
        topo::ListenProtocol::RelayQuic => "relay_quic",
    }
}
