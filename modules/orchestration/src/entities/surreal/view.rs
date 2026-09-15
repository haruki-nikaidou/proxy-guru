//! The per-server config view: what a server should run, what is in flight, and
//! what it is actually running.
//!
//! Exactly one row exists per server. Each of the three slots holds a whole
//! [`ConfigSnapshot`] — the rendered TOML plus the listener capabilities it serves
//! and points at — so convergence never has to re-derive a config from rows that
//! have since been edited in place.

use crate::entities::surreal::canvas::CanvasId;
use crate::entities::surreal::node::NodeId;
use crate::entities::surreal::server::ServerId;
use crate::entities::surreal::topology::CanvasTopology;
use chrono::{DateTime, Utc};
use kanau::processor::Processor;
use newtype_record_id::table_record;
use surrealdb_types::SurrealValue;
use wakuwaku::surreal::SurrealProcessor;

table_record!(ServerConfigViewId, "orchestration_server_config_view");

/// How a listener speaks to whoever dials it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, SurrealValue)]
#[surreal(untagged, rename_all = "snake_case")]
pub enum ListenProtocol {
    Raw,
    RelayTcp,
    RelayTls,
    RelayQuic,
}

impl ListenProtocol {
    /// The stored spelling, for messages and tags.
    pub fn name(self) -> &'static str {
        match self {
            ListenProtocol::Raw => "raw",
            ListenProtocol::RelayTcp => "relay_tcp",
            ListenProtocol::RelayTls => "relay_tls",
            ListenProtocol::RelayQuic => "relay_quic",
        }
    }
}

/// A listener another server can point at.
///
/// Identity is by content: the same server/port/protocol is the same capability
/// whichever node row produced it, and whatever address the server is dialed on
/// today. That is what lets a server keep serving a listener across an unrelated
/// edit (or an address change) while its dependants still reference it.
#[derive(Debug, Clone, PartialEq, Eq, Hash, SurrealValue)]
pub struct ListenerCap {
    pub server: ServerId,
    pub port: i64,
    pub protocol: ListenProtocol,
}

impl ListenerCap {
    /// Same socket, different protocol: the two cannot coexist on one worker, so a
    /// switch between them can never be made seamlessly.
    pub fn conflicts(&self, other: &Self) -> bool {
        self.server == other.server
            && self.port == other.port
            && self.transport() == other.transport()
            && self.protocol != other.protocol
    }

    /// The server's record key, for messages and ordering.
    pub fn server_key(&self) -> String {
        crate::utils::ids::record_key(&self.server.0)
    }

    pub fn transport(&self) -> guru_worker_config::Transport {
        match self.protocol {
            ListenProtocol::RelayQuic => guru_worker_config::Transport::Quic,
            _ => guru_worker_config::Transport::Tcp,
        }
    }
}

/// One `[[forwarding]]` entry's place in the dependency graph.
#[derive(Debug, Clone, SurrealValue)]
pub struct ForwardingDeps {
    /// The pod node this entry was derived from.
    ///
    /// Identity, not diagnostics: it is how convergence finds the previous shape
    /// of a forwarding whose pod stopped deriving. A socket cannot serve that
    /// purpose, because the edit that broke the pod may also have moved it.
    pub pod: NodeId,
    pub serves: ListenerCap,
    pub points_at: Vec<ListenerCap>,
    /// Every non-pod node this entry was derived through (the Entry or Relay on
    /// the listen side; exits, relays and load balancers on the destination
    /// side), boundaries excluded. Node health follows the pod's through this.
    #[surreal(default)]
    pub nodes: Vec<NodeId>,
    /// The certificates this entry's TOML references (the ACME certificate of a
    /// TLS Entry, the relay leaf of a TLS/QUIC relay listener). A forwarding
    /// carried over from an older snapshot keeps its refs, so a synthesised
    /// snapshot's `certificates` is just the union over its entries.
    #[surreal(default)]
    pub certificates: Vec<CertificateRef>,
}

/// A certificate a snapshot's TOML references, at the version it was derived
/// against. The material itself is fetched when the revision is handed to a
/// worker, so no key ever lives in a snapshot; the pinned version is what makes
/// a renewal a new revision even though the file paths do not change.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, SurrealValue)]
pub struct CertificateRef {
    pub kind: CertificateKind,
    /// The record key of the `certificate` (ACME) or `relay_certificate` row.
    pub key: String,
    pub version: i64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, SurrealValue)]
#[surreal(untagged, rename_all = "snake_case")]
pub enum CertificateKind {
    /// Public ACME certificate, delivered as `certs/acme/<key>/{full_chain,key}.pem`.
    Acme,
    /// Relay leaf, delivered as `certs/relay/<key>/{full_chain,key}.pem`.
    Relay,
}

/// A pod the worker could not apply from the acknowledged revision.
#[derive(Debug, Clone, SurrealValue)]
pub struct PodFailure {
    pub pod: NodeId,
    /// The `[[forwarding]]` tag, i.e. the pod's name at the time.
    pub tag: String,
    pub error: String,
}

/// A pod whose own derivation failed.
///
/// The rest of the server still derives and is published; this is how an operator
/// learns which pod is broken and why. A pod listed here keeps serving whatever
/// its listener last was, so a bad edit cannot drop live traffic.
#[derive(Debug, Clone, SurrealValue)]
pub struct InvalidPod {
    pub node: NodeId,
    /// The pod's name, i.e. the `[[forwarding]]` tag it would have carried.
    pub pod: String,
    /// The socket the pod would serve, as `ip:port`. Diagnostic only.
    pub listen: String,
    pub error: String,
}

/// An immutable rendering of one server's config.
#[derive(Debug, Clone, SurrealValue)]
pub struct ConfigSnapshot {
    pub revision: i64,
    pub toml: String,
    pub created_at: DateTime<Utc>,
    /// Index-aligned with the `[[forwarding]]` entries of `toml`.
    pub forwardings: Vec<ForwardingDeps>,
    /// The certificates `toml` references. Sorted, so two snapshots asking for
    /// the same material compare equal.
    #[surreal(default)]
    pub certificates: Vec<CertificateRef>,
}

#[derive(Debug, Clone, SurrealValue)]
pub struct ServerConfigViewEntity {
    pub id: ServerConfigViewId,
    pub server: ServerId,
    /// The newest derivation. Never sent directly: a stream promotes it to
    /// `in_flight` with a conditional update.
    pub desired: Option<ConfigSnapshot>,
    /// Sent to the worker and not yet acknowledged. At most one at a time.
    pub in_flight: Option<ConfigSnapshot>,
    /// What the worker last told us it is running.
    pub applied: Option<ConfigSnapshot>,
    pub failed_revision: Option<i64>,
    /// Set when the last acknowledged revision could not be applied at all.
    pub apply_error: Option<String>,
    /// Pods of the last acknowledged revision that failed on the worker. The
    /// other pods run the revision; these keep their previous shape, and
    /// `applied` describes that mix.
    #[surreal(default)]
    pub failed_pods: Vec<PodFailure>,
    pub derive_error: Option<String>,
    /// Pods that failed to derive while the rest of this server's config was
    /// published. Empty when `derive_error` is set: that is a whole-server failure.
    pub invalid_pods: Vec<InvalidPod>,
    /// Servers whose `applied` config does not yet serve a listener this server's
    /// ideal config points at.
    pub waiting_for: Vec<ServerId>,
}

#[derive(Debug)]
pub struct FindServerConfigView {
    pub server: ServerId,
}

impl Processor<FindServerConfigView> for SurrealProcessor {
    type Output = Option<ServerConfigViewEntity>;
    type Error = surrealdb::Error;
    #[tracing::instrument(name = "Query:FindServerConfigView", skip_all, err)]
    async fn process(&self, input: FindServerConfigView) -> Result<Self::Output, Self::Error> {
        let mut resp = self
            .db()
            .query("SELECT * FROM orchestration_server_config_view WHERE server = $server LIMIT 1")
            .bind(("server", input.server))
            .await?;
        resp.take::<Option<ServerConfigViewEntity>>(0)
    }
}

/// The config views of every server on the given canvases (a whole tree).
#[derive(Debug)]
pub struct ListServerConfigViewsByCanvases {
    pub canvases: Vec<CanvasId>,
}

impl Processor<ListServerConfigViewsByCanvases> for SurrealProcessor {
    type Output = Vec<ServerConfigViewEntity>;
    type Error = surrealdb::Error;
    #[tracing::instrument(name = "Query:ListServerConfigViewsByCanvases", skip_all, err)]
    async fn process(
        &self,
        input: ListServerConfigViewsByCanvases,
    ) -> Result<Self::Output, Self::Error> {
        let mut resp = self
            .db()
            .query(
                "SELECT * FROM orchestration_server_config_view WHERE server.canvas IN $canvases",
            )
            .bind(("canvases", input.canvases))
            .await?;
        resp.take::<Vec<ServerConfigViewEntity>>(0)
    }
}

/// Hands `desired` to the calling stream by promoting it to `in_flight`.
///
/// The whole decision is one conditional update, so two streams can never be sent
/// the same revision and a fenced-out stream is sent nothing at all: the fence is
/// checked against the server row inside the same transaction.
#[derive(Debug)]
pub struct TakeInFlight {
    pub server: ServerId,
    pub generation: i64,
    pub epoch: i64,
}

impl Processor<TakeInFlight> for SurrealProcessor {
    /// The snapshot to send, or `None` when there is nothing to hand out.
    type Output = Option<ConfigSnapshot>;
    type Error = surrealdb::Error;
    #[tracing::instrument(name = "Query-Transaction:TakeInFlight", skip_all, err)]
    async fn process(&self, input: TakeInFlight) -> Result<Self::Output, Self::Error> {
        // Statement 0 is BEGIN; the RETURN below is statement 3.
        let mut resp = self
            .db()
            .query(include_str!("../../../sql/view/take_in_flight.surql"))
            .bind(("server", input.server))
            .bind(("generation", input.generation))
            .bind(("epoch", input.epoch))
            .await?;
        resp.take::<Option<ConfigSnapshot>>(3)
    }
}

/// Promotes `in_flight` to `applied`, or records why the worker refused it.
///
/// Three outcomes: `error` set — the revision was not applied at all;
/// `applied` set — some pods failed and this is the synthesised mix the worker
/// actually runs (`failed_pods` names them, `failed_revision` is the acked one);
/// neither — every pod applied and `in_flight` is promoted as is.
#[derive(Debug)]
pub struct AckServerConfig {
    pub server: ServerId,
    pub canvas: CanvasId,
    pub revision: i64,
    pub error: Option<String>,
    pub applied: Option<ConfigSnapshot>,
    pub failed_pods: Vec<PodFailure>,
}

impl Processor<AckServerConfig> for SurrealProcessor {
    /// `false` when the acknowledged revision is not the one in flight.
    type Output = bool;
    type Error = surrealdb::Error;
    #[tracing::instrument(name = "Query-Transaction:AckServerConfig", skip_all, err)]
    async fn process(&self, input: AckServerConfig) -> Result<Self::Output, Self::Error> {
        let failed_revision = (!input.failed_pods.is_empty()).then_some(input.revision);
        // Statement 0 is BEGIN; the RETURN below is statement 3.
        let mut resp = self
            .db()
            .query(include_str!("../../../sql/view/ack_server_config.surql"))
            .bind(("server", input.server))
            .bind(("canvas", input.canvas))
            .bind(("revision", input.revision))
            .bind(("error", input.error))
            .bind(("applied", input.applied))
            .bind(("failed_pods", input.failed_pods))
            .bind(("failed_revision", failed_revision))
            .await?;
        Ok(resp.take::<Option<bool>>(3)?.unwrap_or(false))
    }
}

/// Clears a dead server's applied state so servers waiting on it can proceed.
#[derive(Debug)]
pub struct ForgetServerAppliedRow {
    pub server: ServerId,
    pub canvas: CanvasId,
}

impl Processor<ForgetServerAppliedRow> for SurrealProcessor {
    type Output = ();
    type Error = surrealdb::Error;
    #[tracing::instrument(name = "Query-Transaction:ForgetServerAppliedRow", skip_all, err)]
    async fn process(&self, input: ForgetServerAppliedRow) -> Result<Self::Output, Self::Error> {
        self.db()
            .query(include_str!(
                "../../../sql/view/forget_server_applied.surql"
            ))
            .bind(("server", input.server))
            .bind(("canvas", input.canvas))
            .await?
            .check()?;
        Ok(())
    }
}

/// What the watch poller compares between ticks.
#[derive(Debug, Clone, SurrealValue)]
pub struct ServerWatchState {
    pub id: ServerId,
    pub refresh_key_generation: i64,
    pub watch_epoch: i64,
    pub desired_revision: Option<i64>,
    pub in_flight_revision: Option<i64>,
    pub applied_revision: Option<i64>,
    pub failed_revision: Option<i64>,
}

pub struct ListServerWatchState {
    pub servers: Vec<ServerId>,
}

impl Processor<ListServerWatchState> for SurrealProcessor {
    type Output = Vec<ServerWatchState>;
    type Error = surrealdb::Error;
    #[tracing::instrument(name = "Query:ListServerWatchState", skip_all, err)]
    async fn process(&self, input: ListServerWatchState) -> Result<Self::Output, Self::Error> {
        let mut resp = self
            .db()
            .query(include_str!(
                "../../../sql/view/list_server_watch_state.surql"
            ))
            .bind(("servers", input.servers))
            .await?;
        resp.take::<Vec<ServerWatchState>>(0)
    }
}

#[derive(Debug, Clone, SurrealValue)]
struct CanvasGenerations {
    generation: i64,
    derived_generation: i64,
}

/// Everything one derivation pass reads, in a single transaction. The counters
/// are the root's; the topology is the whole tree.
#[derive(Debug, Clone)]
pub struct DerivationInput {
    pub root: CanvasId,
    pub generation: i64,
    pub derived_generation: i64,
    pub topology: CanvasTopology,
    pub views: Vec<ServerConfigViewEntity>,
}

/// Loads the derivation input of the tree containing `canvas`.
pub struct LoadCanvasDerivationInput {
    pub canvas: CanvasId,
}

impl Processor<LoadCanvasDerivationInput> for SurrealProcessor {
    /// `None` when the canvas has been deleted.
    type Output = Option<DerivationInput>;
    type Error = surrealdb::Error;
    #[tracing::instrument(name = "Query-Transaction:LoadCanvasDerivationInput", skip_all, err)]
    async fn process(&self, input: LoadCanvasDerivationInput) -> Result<Self::Output, Self::Error> {
        // Statement 0 is BEGIN, 1-2 the LETs, 3 the root's counters, 4 the
        // canvases, 5-8 the row reads, 9 the views, 10 the root.
        let mut resp = self
            .db()
            .query(include_str!(
                "../../../sql/view/load_derivation_input.surql"
            ))
            .bind(("canvas", input.canvas.clone()))
            .await?;
        let Some(generations) = resp.take::<Option<CanvasGenerations>>(3)? else {
            return Ok(None);
        };
        let root = resp
            .take::<Option<CanvasId>>(10)?
            .unwrap_or_else(|| input.canvas.clone());
        let rows = crate::entities::surreal::topology::group_rows(&mut resp, 4, root.clone())?;
        let views = resp.take::<Vec<ServerConfigViewEntity>>(9)?;
        Ok(Some(DerivationInput {
            root: root.clone(),
            generation: generations.generation,
            derived_generation: generations.derived_generation,
            topology: CanvasTopology {
                root,
                canvases: rows.canvases,
                servers: rows.servers,
                nodes: rows.nodes,
                edges: rows.edges,
            },
            views,
        }))
    }
}

/// One server's outcome of a derivation pass.
///
/// `desired: None` means "leave the slot alone" — either the derivation failed or
/// it produced byte-identical TOML.
#[derive(Debug, Clone, SurrealValue)]
pub struct ViewUpdate {
    pub server: ServerId,
    pub desired: Option<ConfigSnapshot>,
    pub derive_error: Option<String>,
    pub invalid_pods: Vec<InvalidPod>,
    pub waiting_for: Vec<ServerId>,
    /// A new desired revision clears the previous failure, so a fixed config is
    /// offered to the worker again.
    pub clear_failure: bool,
}

/// Commits a whole derivation pass, but only if the root canvas is still at the
/// generation it was derived from. `canvas` must be the tree's root.
pub struct CommitCanvasDerivation {
    pub canvas: CanvasId,
    pub generation: i64,
    pub updates: Vec<ViewUpdate>,
}

impl Processor<CommitCanvasDerivation> for SurrealProcessor {
    /// `false` when the canvas moved on and the pass has to be redone.
    type Output = bool;
    type Error = surrealdb::Error;
    #[tracing::instrument(name = "Query-Transaction:CommitCanvasDerivation", skip_all, err)]
    async fn process(&self, input: CommitCanvasDerivation) -> Result<Self::Output, Self::Error> {
        // Statement 0 is BEGIN, 1 the LET, 2 the IF; the RETURN is statement 3.
        let mut resp = self
            .db()
            .query(include_str!("../../../sql/view/commit_derivation.surql"))
            .bind(("canvas", input.canvas))
            .bind(("generation", input.generation))
            .bind(("updates", input.updates))
            .await?;
        Ok(resp.take::<Option<bool>>(3)?.unwrap_or(false))
    }
}

/// Canvases whose derivation is behind their edits; the cron sweep's input.
pub struct ListStaleCanvases;

impl Processor<ListStaleCanvases> for SurrealProcessor {
    type Output = Vec<CanvasId>;
    type Error = surrealdb::Error;
    #[tracing::instrument(name = "Query:ListStaleCanvases", skip_all, err)]
    async fn process(&self, _input: ListStaleCanvases) -> Result<Self::Output, Self::Error> {
        let mut resp = self
            .db()
            .query(
                "SELECT VALUE id FROM orchestration_canvas WHERE generation > derived_generation",
            )
            .await?;
        resp.take::<Vec<CanvasId>>(0)
    }
}
