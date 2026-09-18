//! The pod graph: as [`guru_topology`] reads it, and as operators edit it.
//!
//! Rows go into the topology field for field; what the rows leave to the
//! control plane is resolved here: a server's dial address (pinned, reported or
//! observed), the default ACME directory, and which certificates are issued.
//!
//! Edits come as one batch ([`ApplyGraph`]): the batch is applied to a copy of
//! the tree, the result is checked as a whole, and the batch is written in one
//! fenced transaction only when nothing in it is an error. A dry run answers
//! the same diagnostics without writing.

use crate::config::OrchestrationConfig;
use crate::entities::db::ca::RelayCertificateEntity;
use crate::entities::db::canvas::{CanvasId, CanvasTree, CanvasUiPosition, LoadCanvasTree};
use crate::entities::db::certificate::{CertificateEntity, ListCertificatesBySnis};
use crate::entities::db::dns::FindDnsProviderById;
use crate::entities::db::edge::{EdgeEntity, EdgeId, EdgeTarget};
use crate::entities::db::exit::{ExitEntity, ExitId};
use crate::entities::db::graph::{
    ApplyGraphBatch, FindTakenIds, GraphRows, LoadCanvasGraph, MoveGraphItems, TakenIds,
};
use crate::entities::db::group::{GroupEntity, GroupId, GroupMember};
use crate::entities::db::pod::{PodEntity, PodId, PodIngress};
use crate::entities::db::server::{QuicCongestion, ServerEntity, ServerId, ServerQuic};
use crate::entities::db::view::{ListServerConfigViewsByCanvases, ServerConfigViewEntity};
use crate::events::live::CanvasChangeKind;
use crate::services::OrchestrationError;
use crate::services::converge::switch_conflicts;
use crate::services::derive::{DerivationCertificates, DerivedConfig, derive_tree};
use crate::services::notify::Notifier;
use crate::utils::ids;
use auth::services::identity::Identity;
use auth::utils::rbac::Permission;
use base::db::Db;
use guru_topology as topo;
use kanau::processor::Processor;
use rand::Rng;
use std::collections::{BTreeMap, HashSet};
use std::ops::RangeInclusive;

/// Where pods put with port `0` get their port from: high enough to stay clear
/// of anything an operator types by hand.
pub const DEFAULT_POD_PORTS: RangeInclusive<u16> = 40000..=59999;

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
        dial_v4: server.v4_address(),
        dial_v6: server.v6_address(),
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
        ip_family: edge.ip_family.into(),
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

// --- reading and editing the graph -------------------------------------------

/// The pod graph of a canvas tree, and how it checks.
#[derive(Clone)]
pub struct GraphService {
    pub db: Db,
    pub config: OrchestrationConfig,
    pub notifier: Notifier,
}

/// What a diagnostic is about.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GraphSubject {
    Server(ServerId),
    Pod(PodId),
    Exit(ExitId),
    Edge(EdgeId),
    Group(GroupId),
    Canvas(CanvasId),
}

/// A problem with a graph or with a change to it. `problem` is a stable
/// snake_case name: the graph's own ([`topo::Problem`]) or one of the change's
/// (`unknown_*`, `invalid_id`, `id_in_use`, `edge_ends_changed`,
/// `listener_in_use`, `no_free_port`, ...).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GraphDiagnostic {
    pub problem: String,
    pub error: bool,
    pub subjects: Vec<GraphSubject>,
    pub message: String,
}

impl GraphDiagnostic {
    fn error(problem: &str, subjects: Vec<GraphSubject>, message: String) -> Self {
        Self {
            problem: problem.to_string(),
            error: true,
            subjects,
            message,
        }
    }

    fn from_topology(diagnostic: &topo::Diagnostic) -> Self {
        let problem = serde_json::to_value(diagnostic.problem)
            .ok()
            .and_then(|value| value.as_str().map(str::to_string))
            .unwrap_or_default();
        Self {
            problem,
            error: diagnostic.severity() == topo::Severity::Error,
            subjects: diagnostic
                .subjects
                .iter()
                .map(|subject| match subject {
                    topo::Subject::Server(id) => {
                        GraphSubject::Server(ServerId::from_key(id.as_str()))
                    }
                    topo::Subject::Pod(id) => GraphSubject::Pod(PodId::from_key(id.as_str())),
                    topo::Subject::Exit(id) => GraphSubject::Exit(ExitId::from_key(id.as_str())),
                    topo::Subject::Edge(id) => GraphSubject::Edge(EdgeId::from_key(id.as_str())),
                })
                .collect(),
            message: diagnostic.message.clone(),
        }
    }
}

/// The graph of a tree with its diagnostics.
#[derive(Debug, Clone)]
pub struct GraphView {
    pub rows: GraphRows,
    pub diagnostics: Vec<GraphDiagnostic>,
}

/// The diagnostics of a loaded tree, as `GetGraph` and the live graph view
/// report them.
pub fn check_graph(rows: &GraphRows, config: &OrchestrationConfig) -> Vec<GraphDiagnostic> {
    topo::check(&topology_graph(
        &rows.servers,
        &rows.pods,
        &rows.exits,
        &rows.edges,
        config,
    ))
    .diagnostics
    .iter()
    .map(GraphDiagnostic::from_topology)
    .collect()
}

/// The whole tree containing `canvas`.
pub struct GetGraph {
    pub actor: Identity,
    pub canvas: CanvasId,
}

impl Processor<GetGraph> for GraphService {
    type Output = GraphView;
    type Error = OrchestrationError;
    #[tracing::instrument(name = "Service:GetGraph", skip_all, err)]
    async fn process(&self, input: GetGraph) -> Result<Self::Output, Self::Error> {
        input.actor.ensure(Permission::ViewWorkspace)?;
        let rows = self
            .db
            .process(LoadCanvasGraph {
                canvas: input.canvas,
            })
            .await?;
        if rows.canvases.is_empty() {
            return Err(OrchestrationError::NotFound);
        }
        let diagnostics = check_graph(&rows, &self.config);
        Ok(GraphView { rows, diagnostics })
    }
}

/// One batch of changes to a tree's graph. A put replaces the row with its id,
/// or creates it when no row has that id yet; ids of new rows are chosen by the
/// caller (20 characters of `[a-z0-9]`), so one batch can create a pod, the
/// edges into and out of it and the routes naming them together. A pod put with
/// port `0` gets a free port.
#[derive(Debug, Clone, Default)]
pub struct GraphChange {
    pub put_pods: Vec<PodEntity>,
    pub put_exits: Vec<ExitEntity>,
    pub put_edges: Vec<EdgeEntity>,
    pub put_groups: Vec<GroupEntity>,
    pub delete_pods: Vec<PodId>,
    pub delete_exits: Vec<ExitId>,
    pub delete_edges: Vec<EdgeId>,
    pub delete_groups: Vec<GroupId>,
}

impl GraphChange {
    fn touches_graph(&self) -> bool {
        !(self.put_pods.is_empty()
            && self.put_exits.is_empty()
            && self.put_edges.is_empty()
            && self.delete_pods.is_empty()
            && self.delete_exits.is_empty()
            && self.delete_edges.is_empty())
    }
}

/// Checks a batch against the tree containing `canvas` and, unless `dry_run`,
/// writes it: all of it or, when anything in it is an error, none of it.
pub struct ApplyGraph {
    pub actor: Identity,
    pub canvas: CanvasId,
    pub change: GraphChange,
    pub dry_run: bool,
    /// The generation the caller computed the change against; a tree that has
    /// moved on since refuses the change.
    pub expected_generation: Option<i64>,
}

#[derive(Debug, Clone)]
pub struct ApplyOutcome {
    pub applied: bool,
    /// The root generation after the write, or as read when nothing was written.
    pub generation: i64,
    /// Every diagnostic of the graph the change produces, and of the change.
    pub diagnostics: Vec<GraphDiagnostic>,
    /// The pods the change put, as written: a pod put with port `0` carries the
    /// port it was given.
    pub pods: Vec<PodEntity>,
}

impl Processor<ApplyGraph> for GraphService {
    type Output = ApplyOutcome;
    type Error = OrchestrationError;
    #[tracing::instrument(
        name = "Service:ApplyGraph",
        skip_all,
        err,
        fields(canvas = %input.canvas, dry_run = input.dry_run)
    )]
    async fn process(&self, input: ApplyGraph) -> Result<Self::Output, Self::Error> {
        input.actor.ensure(Permission::EditWorkspace)?;
        let rows = self
            .db
            .process(LoadCanvasGraph {
                canvas: input.canvas.clone(),
            })
            .await?;
        if rows.canvases.is_empty() {
            return Err(OrchestrationError::NotFound);
        }
        let generation = rows.generation();
        if input
            .expected_generation
            .is_some_and(|expected| expected != generation)
        {
            return Err(OrchestrationError::Conflict(
                "canvas changed since it was read; reload and retry".into(),
            ));
        }
        let taken = self
            .db
            .process(FindTakenIds {
                pods: input.change.put_pods.iter().map(|p| p.id.clone()).collect(),
                exits: input
                    .change
                    .put_exits
                    .iter()
                    .map(|e| e.id.clone())
                    .collect(),
                edges: input
                    .change
                    .put_edges
                    .iter()
                    .map(|e| e.id.clone())
                    .collect(),
                groups: input
                    .change
                    .put_groups
                    .iter()
                    .map(|g| g.id.clone())
                    .collect(),
            })
            .await?;
        let mut change = input.change;
        let mut diagnostics = self.validate_tls(&change).await?;
        let mut result = rows.clone();
        diagnostics.extend(apply_change(&mut result, &change, &taken));

        let views = self
            .db
            .process(ListServerConfigViewsByCanvases {
                canvases: rows.canvas_ids(),
            })
            .await?;
        diagnostics.extend(allocate_ports(&mut result, &mut change, &views));

        let report = topo::check(&topology_graph(
            &result.servers,
            &result.pods,
            &result.exits,
            &result.edges,
            &self.config,
        ));
        diagnostics.extend(
            report
                .diagnostics
                .iter()
                .map(GraphDiagnostic::from_topology),
        );
        if !report.has_errors() {
            diagnostics.extend(self.switch_diagnostics(&result, &views));
        }

        let pods: Vec<PodEntity> = change
            .put_pods
            .iter()
            .filter_map(|put| result.pods.iter().find(|pod| pod.id == put.id).cloned())
            .collect();
        let refused = diagnostics.iter().any(|d| d.error);
        if refused || input.dry_run {
            return Ok(ApplyOutcome {
                applied: false,
                generation,
                diagnostics,
                pods,
            });
        }

        let derives = change.touches_graph();
        let known_pods: HashSet<&PodId> = rows.pods.iter().map(|p| &p.id).collect();
        let known_exits: HashSet<&ExitId> = rows.exits.iter().map(|e| &e.id).collect();
        let known_edges: HashSet<&EdgeId> = rows.edges.iter().map(|e| &e.id).collect();
        let known_groups: HashSet<&GroupId> = rows.groups.iter().map(|g| &g.id).collect();
        let mut batch = ApplyGraphBatch {
            canvas: Some(rows.root.clone()),
            fence: rows.fence(),
            derives,
            delete_groups: change.delete_groups.clone(),
            delete_edges: change.delete_edges.clone(),
            delete_pods: change.delete_pods.clone(),
            delete_exits: change.delete_exits.clone(),
            ..ApplyGraphBatch::default()
        };
        for pod in &pods {
            if known_pods.contains(&pod.id) {
                batch.update_pods.push(pod.clone());
            } else {
                batch.insert_pods.push(pod.clone());
            }
        }
        for exit in &change.put_exits {
            if known_exits.contains(&exit.id) {
                batch.update_exits.push(exit.clone());
            } else {
                batch.insert_exits.push(exit.clone());
            }
        }
        for edge in &change.put_edges {
            if known_edges.contains(&edge.id) {
                batch.update_edges.push(edge.clone());
            } else {
                batch.insert_edges.push(edge.clone());
            }
        }
        for group in &change.put_groups {
            // Members that went away in this same batch are dropped, as the
            // database would drop them afterwards.
            let group = result
                .groups
                .iter()
                .find(|g| g.id == group.id)
                .cloned()
                .unwrap_or_else(|| group.clone());
            if known_groups.contains(&group.id) {
                batch.update_groups.push(group);
            } else {
                batch.insert_groups.push(group);
            }
        }
        let generation = self.db.process(batch).await?;
        if derives {
            self.notifier.notify(&rows.root).await;
        }
        self.notifier
            .canvas_changed(&input.canvas, CanvasChangeKind::GraphChanged, Vec::new())
            .await;
        Ok(ApplyOutcome {
            applied: true,
            generation,
            diagnostics,
            pods,
        })
    }
}

impl GraphService {
    /// A TLS client pod must name a DNS provider that exists, and pods sharing an
    /// SNI must agree on how its certificate is issued: the certificate row is
    /// keyed by `(sni, directory)` alone, so a later pod naming another provider
    /// or zone would otherwise be silently ignored.
    async fn validate_tls(
        &self,
        change: &GraphChange,
    ) -> Result<Vec<GraphDiagnostic>, OrchestrationError> {
        let mut out = Vec::new();
        for pod in &change.put_pods {
            let Some(tls) = pod.ingress.tls() else {
                continue;
            };
            let subjects = || vec![GraphSubject::Pod(pod.id.clone())];
            let provider = self
                .db
                .process(FindDnsProviderById {
                    id: tls.dns_provider.clone(),
                })
                .await?;
            if provider.is_none() {
                out.push(GraphDiagnostic::error(
                    "unknown_dns_provider",
                    subjects(),
                    format!("pod {}: its DNS provider does not exist", pod.name),
                ));
                continue;
            }
            let rows = self
                .db
                .process(ListCertificatesBySnis {
                    snis: vec![tls.sni.to_ascii_lowercase()],
                })
                .await?;
            let directory = self.config.acme_directory(&tls.acme_directory);
            if let Some(row) = rows.iter().find(|row| {
                row.acme_directory == directory
                    && (row.dns_provider != tls.dns_provider || row.domain_id != tls.domain_id)
            }) {
                out.push(GraphDiagnostic::error(
                    "certificate_issued_elsewhere",
                    subjects(),
                    format!(
                        "pod {}: the certificate for {} is issued through DNS provider {} and domain {}; \
                         every pod sharing an SNI must use the same provider and domain",
                        pod.name, tls.sni, row.dns_provider, row.domain_id
                    ),
                ));
            }
        }
        Ok(out)
    }

    /// Listeners the change would give another protocol while servers still
    /// point at them.
    fn switch_diagnostics(
        &self,
        result: &GraphRows,
        views: &[ServerConfigViewEntity],
    ) -> Vec<GraphDiagnostic> {
        let Ok(projected) = derive_tree(result, &DerivationCertificates::assumed(), &self.config)
        else {
            return Vec::new();
        };
        let projected: BTreeMap<ServerId, DerivedConfig> = projected
            .into_iter()
            .filter_map(|(server, derived)| Some((server, derived.ok()?)))
            .collect();
        switch_conflicts(&projected, views)
            .into_iter()
            .map(|conflict| {
                let pod = result
                    .pods
                    .iter()
                    .find(|p| p.id == conflict.pod)
                    .map_or_else(|| conflict.pod.to_string(), |p| p.name.clone());
                GraphDiagnostic::error(
                    "listener_in_use",
                    vec![GraphSubject::Pod(conflict.pod.clone())],
                    format!(
                        "pod {pod}: port {} is still dialed as {} and cannot become {}; give the pod a new port instead",
                        conflict.port,
                        conflict.in_use.name(),
                        conflict.wanted.name()
                    ),
                )
            })
            .collect()
    }
}

/// Applies a change to a copy of the rows, returning what is wrong with the
/// change itself (the graph it produces is checked afterwards).
fn apply_change(
    rows: &mut GraphRows,
    change: &GraphChange,
    taken: &TakenIds,
) -> Vec<GraphDiagnostic> {
    let mut out = Vec::new();
    let tree: HashSet<&str> = rows.canvases.iter().map(|c| c.id.as_str()).collect();
    let tree: HashSet<String> = tree.into_iter().map(str::to_string).collect();
    let servers: HashSet<String> = rows.servers.iter().map(|s| s.id.to_string()).collect();

    let mut seen: HashSet<String> = HashSet::new();
    let mut check_id =
        |kind: &str, id: &str, subject: GraphSubject, out: &mut Vec<GraphDiagnostic>| {
            if !ids::is_record_key(id) {
                out.push(GraphDiagnostic::error(
                    "invalid_id",
                    vec![subject],
                    format!("{kind} id {id:?} is not 20 characters of a-z and 0-9"),
                ));
                return false;
            }
            if !seen.insert(format!("{kind}:{id}")) {
                out.push(GraphDiagnostic::error(
                    "duplicate_change",
                    vec![subject],
                    format!("{kind} {id} is changed twice in one batch"),
                ));
                return false;
            }
            true
        };

    // Deletes first, of rows that are this tree's.
    for id in &change.delete_edges {
        if check_id(
            "edge",
            id.as_str(),
            GraphSubject::Edge(id.clone()),
            &mut out,
        ) && !rows.edges.iter().any(|e| e.id == *id)
        {
            out.push(unknown("edge", GraphSubject::Edge(id.clone())));
        }
    }
    for id in &change.delete_pods {
        if check_id("pod", id.as_str(), GraphSubject::Pod(id.clone()), &mut out)
            && !rows.pods.iter().any(|p| p.id == *id)
        {
            out.push(unknown("pod", GraphSubject::Pod(id.clone())));
        }
    }
    for id in &change.delete_exits {
        if check_id(
            "exit",
            id.as_str(),
            GraphSubject::Exit(id.clone()),
            &mut out,
        ) && !rows.exits.iter().any(|e| e.id == *id)
        {
            out.push(unknown("exit", GraphSubject::Exit(id.clone())));
        }
    }
    for id in &change.delete_groups {
        if check_id(
            "group",
            id.as_str(),
            GraphSubject::Group(id.clone()),
            &mut out,
        ) && !rows.groups.iter().any(|g| g.id == *id)
        {
            out.push(unknown("group", GraphSubject::Group(id.clone())));
        }
    }
    rows.edges.retain(|e| !change.delete_edges.contains(&e.id));
    rows.pods.retain(|p| !change.delete_pods.contains(&p.id));
    rows.exits.retain(|e| !change.delete_exits.contains(&e.id));
    rows.groups
        .retain(|g| !change.delete_groups.contains(&g.id));

    for pod in &change.put_pods {
        let subject = GraphSubject::Pod(pod.id.clone());
        if !check_id("pod", pod.id.as_str(), subject.clone(), &mut out) {
            continue;
        }
        let existing = rows.pods.iter().position(|p| p.id == pod.id);
        if existing.is_none() && taken.pods.contains(&pod.id) {
            out.push(in_use("pod", subject));
            continue;
        }
        if !tree.contains(pod.canvas.as_str()) {
            out.push(GraphDiagnostic::error(
                "unknown_canvas",
                vec![subject],
                format!("pod {}: its canvas is not part of this tree", pod.name),
            ));
            continue;
        }
        if !servers.contains(pod.server.as_str()) {
            out.push(GraphDiagnostic::error(
                "unknown_server",
                vec![subject],
                format!(
                    "pod {}: its server is not part of this canvas tree",
                    pod.name
                ),
            ));
            continue;
        }
        let mut pod = pod.clone();
        if let PodIngress::ClientTls { tls, .. } = &mut pod.ingress {
            // The certificate row is keyed by the canonical form.
            tls.sni = tls.sni.to_ascii_lowercase();
        }
        match existing {
            Some(index) => rows.pods[index] = pod,
            None => rows.pods.push(pod),
        }
    }
    for exit in &change.put_exits {
        let subject = GraphSubject::Exit(exit.id.clone());
        if !check_id("exit", exit.id.as_str(), subject.clone(), &mut out) {
            continue;
        }
        let existing = rows.exits.iter().position(|e| e.id == exit.id);
        if existing.is_none() && taken.exits.contains(&exit.id) {
            out.push(in_use("exit", subject));
            continue;
        }
        if !tree.contains(exit.canvas.as_str()) {
            out.push(GraphDiagnostic::error(
                "unknown_canvas",
                vec![subject],
                format!("exit {}: its canvas is not part of this tree", exit.name),
            ));
            continue;
        }
        match existing {
            Some(index) => rows.exits[index] = exit.clone(),
            None => rows.exits.push(exit.clone()),
        }
    }
    for edge in &change.put_edges {
        let subject = GraphSubject::Edge(edge.id.clone());
        if !check_id("edge", edge.id.as_str(), subject.clone(), &mut out) {
            continue;
        }
        match rows.edges.iter().position(|e| e.id == edge.id) {
            Some(index) => {
                if rows.edges[index].source != edge.source
                    || rows.edges[index].target != edge.target
                {
                    out.push(GraphDiagnostic::error(
                        "edge_ends_changed",
                        vec![subject],
                        format!(
                            "edge {}: an edge's ends cannot change; delete it and draw another",
                            edge.id
                        ),
                    ));
                    continue;
                }
                rows.edges[index] = edge.clone();
            }
            None => {
                if taken.edges.contains(&edge.id) {
                    out.push(in_use("edge", subject));
                    continue;
                }
                rows.edges.push(edge.clone());
            }
        }
    }
    // An edge must start at a pod of this tree; where it leads is the graph's
    // own check.
    let pods: HashSet<&str> = rows.pods.iter().map(|p| p.id.as_str()).collect();
    for edge in &change.put_edges {
        if rows.edges.iter().any(|e| e.id == edge.id) && !pods.contains(edge.source.as_str()) {
            out.push(GraphDiagnostic::error(
                "unknown_edge_source",
                vec![GraphSubject::Edge(edge.id.clone())],
                format!("edge {} starts at a pod this tree does not have", edge.id),
            ));
        }
    }
    let exits: HashSet<&str> = rows.exits.iter().map(|e| e.id.as_str()).collect();
    let edges: HashSet<&str> = rows.edges.iter().map(|e| e.id.as_str()).collect();
    let member_exists = |member: &GroupMember| match member {
        GroupMember::Pod(id) => pods.contains(id.as_str()),
        GroupMember::Edge(id) => edges.contains(id.as_str()),
        GroupMember::Exit(id) => exits.contains(id.as_str()),
        GroupMember::Server(id) => servers.contains(id.as_str()),
    };
    let mut groups = Vec::new();
    for group in &change.put_groups {
        let subject = GraphSubject::Group(group.id.clone());
        if !check_id("group", group.id.as_str(), subject.clone(), &mut out) {
            continue;
        }
        let existing = rows.groups.iter().position(|g| g.id == group.id);
        if existing.is_none() && taken.groups.contains(&group.id) {
            out.push(in_use("group", subject));
            continue;
        }
        if !tree.contains(group.canvas.as_str()) {
            out.push(GraphDiagnostic::error(
                "unknown_canvas",
                vec![subject],
                format!("group {}: its canvas is not part of this tree", group.id),
            ));
            continue;
        }
        if let Some(missing) = group.members.iter().find(|m| !member_exists(m)) {
            out.push(GraphDiagnostic::error(
                "unknown_group_member",
                vec![subject],
                format!("group {}: member {missing:?} does not exist", group.id),
            ));
            continue;
        }
        groups.push((existing, group.clone()));
    }
    for (existing, group) in groups {
        match existing {
            Some(index) => rows.groups[index] = group,
            None => rows.groups.push(group),
        }
    }
    // What the batch removed leaves the groups that held it.
    for group in rows.groups.iter_mut() {
        group.members.retain(|member| member_exists(member));
    }
    out
}

fn unknown(kind: &str, subject: GraphSubject) -> GraphDiagnostic {
    GraphDiagnostic::error(
        &format!("unknown_{kind}"),
        vec![subject.clone()],
        format!("this canvas tree has no such {kind}: {subject:?}"),
    )
}

fn in_use(kind: &str, subject: GraphSubject) -> GraphDiagnostic {
    GraphDiagnostic::error(
        "id_in_use",
        vec![subject.clone()],
        format!("the {kind} id of {subject:?} is taken by another canvas tree"),
    )
}

/// Gives every pod the change put with port `0` a free port of its server in
/// [`DEFAULT_POD_PORTS`]: one no pod of the server listens on, and no config of
/// the server still serves (a listener held for its dependants included).
fn allocate_ports(
    rows: &mut GraphRows,
    change: &mut GraphChange,
    views: &[ServerConfigViewEntity],
) -> Vec<GraphDiagnostic> {
    let mut out = Vec::new();
    let wanted: Vec<PodId> = change
        .put_pods
        .iter()
        .filter(|pod| pod.port == 0)
        .map(|pod| pod.id.clone())
        .collect();
    let mut rng = rand::rng();
    for id in wanted {
        let Some(index) = rows.pods.iter().position(|p| p.id == id) else {
            continue;
        };
        let server = rows.pods[index].server.clone();
        let mut used: HashSet<i64> = rows
            .pods
            .iter()
            .filter(|p| p.server == server && p.port != 0)
            .map(|p| i64::from(p.port))
            .collect();
        for view in views.iter().filter(|v| v.server == server) {
            for snapshot in [&view.desired, &view.in_flight, &view.applied]
                .into_iter()
                .flatten()
            {
                used.extend(snapshot.forwardings.iter().map(|deps| deps.serves.port));
            }
        }
        let free: Vec<u16> = DEFAULT_POD_PORTS
            .filter(|port| !used.contains(&i64::from(*port)))
            .collect();
        let Some(port) = free.get(rng.random_range(0..free.len().max(1))).copied() else {
            out.push(GraphDiagnostic::error(
                "no_free_port",
                vec![GraphSubject::Pod(id.clone())],
                format!(
                    "pod {}: its server has no free port left",
                    rows.pods[index].name
                ),
            ));
            continue;
        };
        rows.pods[index].port = port;
        if let Some(put) = change.put_pods.iter_mut().find(|p| p.id == id) {
            put.port = port;
        }
    }
    out
}

/// Positions of servers, exits and subcanvases on their canvases.
pub struct MoveItems {
    pub actor: Identity,
    pub canvas: CanvasId,
    pub servers: Vec<(ServerId, CanvasUiPosition)>,
    pub exits: Vec<(ExitId, CanvasUiPosition)>,
    pub canvases: Vec<(CanvasId, CanvasUiPosition)>,
}

impl Processor<MoveItems> for GraphService {
    type Output = ();
    type Error = OrchestrationError;
    #[tracing::instrument(name = "Service:MoveItems", skip_all, err)]
    async fn process(&self, input: MoveItems) -> Result<Self::Output, Self::Error> {
        input.actor.ensure(Permission::EditWorkspace)?;
        let tree = self
            .db
            .process(LoadCanvasTree {
                canvas: input.canvas.clone(),
            })
            .await?
            .ok_or(OrchestrationError::NotFound)?;
        let mut ids = Vec::new();
        collect_tree(&tree, &mut ids);
        self.db
            .process(MoveGraphItems {
                tree: ids,
                servers: input.servers,
                exits: input.exits,
                canvases: input.canvases,
            })
            .await?;
        self.notifier
            .canvas_changed(&input.canvas, CanvasChangeKind::ServerMoved, Vec::new())
            .await;
        Ok(())
    }
}

fn collect_tree(tree: &CanvasTree, out: &mut Vec<CanvasId>) {
    out.push(tree.canvas.id.clone());
    for child in &tree.children {
        collect_tree(child, out);
    }
}
