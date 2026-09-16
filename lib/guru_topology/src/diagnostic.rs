//! What [`crate::check`] and [`crate::compile`] report.

use crate::model::{EdgeId, ExitId, PodId, ServerId};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Severity {
    /// The graph must not be stored.
    Error,
    /// Worth telling the operator; never blocks an edit.
    Warning,
}

/// What a diagnostic is about.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(tag = "kind", content = "id", rename_all = "snake_case")]
pub enum Subject {
    Server(ServerId),
    Pod(PodId),
    Exit(ExitId),
    Edge(EdgeId),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Problem {
    /// Two servers, pods, exits or edges share an id.
    DuplicateId,
    /// A pod is on a server the graph does not have.
    UnknownServer,
    /// An edge starts at a pod the graph does not have.
    UnknownEdgeSource,
    /// An edge starts at an exit, where traffic has already left.
    EdgeFromExit,
    /// An edge ends at a pod or exit the graph does not have.
    UnknownEdgeTarget,
    /// A pod has out-edges but no route over them.
    MissingRoute,
    /// A route names an edge the graph does not have.
    RouteUnknownEdge,
    /// A route names another pod's edge.
    RouteForeignEdge,
    /// One of a pod's out-edges is missing from its route.
    RouteMissingEdge,
    /// A route names the same edge twice.
    RouteDuplicateEdge,
    /// A route nests deeper than [`crate::MAX_ROUTE_DEPTH`].
    RouteTooDeep,
    /// A `balance` or `failover` without members.
    EmptyGroup,
    /// A `balance` member with weight zero.
    ZeroWeight,
    /// An edge leads into a pod clients connect to: one listener cannot take
    /// clients and relayed traffic at once.
    ClientPodDialed,
    /// A pod listens on port zero.
    InvalidPort,
    /// A pod's bind address is not an IP literal.
    InvalidBindIp,
    /// A pod's advertised address is not an IP literal.
    InvalidAdvertiseIp,
    /// An edge's override address is neither an IP literal nor a host name.
    InvalidOverrideAddress,
    /// An edge overrides the port with zero.
    InvalidOverridePort,
    /// An exit's destination is not `host:port`.
    InvalidExitDestination,
    /// A TLS pod's SNI is not a plain host name.
    InvalidSni,
    /// Traffic could come back to a pod it has already passed.
    Cycle,
    /// Two pods on one server claim the same socket.
    ListenerConflict,
    /// A sticky `balance` on a pod that never learns the client address.
    StickyWithoutClientIp,
    /// A `failover` with a single member: there is nothing to fail over to.
    SingleTierFailover,
    /// A pod without out-edges; it is not compiled.
    PodWithoutEdges,
    /// A relay pod nothing dials; it is not compiled.
    RelayPodNotDialed,
    /// An exit no edge leads to.
    ExitNotReached,
}

impl Problem {
    pub fn severity(self) -> Severity {
        match self {
            Problem::SingleTierFailover
            | Problem::PodWithoutEdges
            | Problem::RelayPodNotDialed
            | Problem::ExitNotReached => Severity::Warning,
            _ => Severity::Error,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Diagnostic {
    pub problem: Problem,
    pub subjects: Vec<Subject>,
    pub message: String,
}

impl Diagnostic {
    pub(crate) fn new(problem: Problem, subjects: Vec<Subject>, message: String) -> Self {
        Self {
            problem,
            subjects,
            message,
        }
    }

    pub fn severity(&self) -> Severity {
        self.problem.severity()
    }
}

/// Every diagnostic about a graph, errors first.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Report {
    pub diagnostics: Vec<Diagnostic>,
}

impl Report {
    pub(crate) fn from_unsorted(mut diagnostics: Vec<Diagnostic>) -> Self {
        diagnostics.sort_by(|a, b| {
            (a.severity(), a.problem, &a.subjects, &a.message).cmp(&(
                b.severity(),
                b.problem,
                &b.subjects,
                &b.message,
            ))
        });
        diagnostics.dedup();
        Self { diagnostics }
    }

    pub fn has_errors(&self) -> bool {
        self.errors().next().is_some()
    }

    pub fn errors(&self) -> impl Iterator<Item = &Diagnostic> {
        self.diagnostics
            .iter()
            .filter(|d| d.severity() == Severity::Error)
    }

    pub fn warnings(&self) -> impl Iterator<Item = &Diagnostic> {
        self.diagnostics
            .iter()
            .filter(|d| d.severity() == Severity::Warning)
    }

    /// Whether any diagnostic is this problem.
    pub fn has(&self, problem: Problem) -> bool {
        self.diagnostics.iter().any(|d| d.problem == problem)
    }
}

/// Why a pod that passed [`crate::check`] still produced no forwarding. Only
/// that pod is affected; every other pod on its server compiles as usual.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Invalid {
    /// The ACME certificate a TLS client listener needs is not issued.
    CertificatePending { sni: String, acme_directory: String },
    /// A relay over TLS or QUIC, listening or dialing, without the internal CA.
    InternalCaMissing,
    /// A TLS or QUIC relay listener whose leaf certificate is not issued.
    RelayCertificateMissing,
    /// An edge leads to a pod on a server with no address to dial, and neither
    /// the edge nor the pod names one.
    TargetWithoutAddress {
        edge: EdgeId,
        pod: PodId,
        server: ServerId,
    },
    /// The entry failed the worker config's own validation.
    Rejected { reason: String },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct InvalidPod {
    pub pod: PodId,
    pub reason: Invalid,
    pub message: String,
}
