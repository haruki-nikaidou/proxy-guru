//! The per-server config view: what a server should run, what is in flight, and
//! what it is actually running.
//!
//! Exactly one row exists per server. Each of the three slots holds a whole
//! [`ConfigSnapshot`] — the rendered TOML plus the listener capabilities it serves
//! and points at — so convergence never has to re-derive a config from rows that
//! have since been edited in place.

use crate::entities::db::canvas::{CanvasId, SNAPSHOT_READ};
use crate::entities::db::fence;
use crate::entities::db::graph::{GraphRows, load_graph};
use crate::entities::db::pod::PodId;
use crate::entities::db::server::ServerId;
use crate::entities::db::tree;
use base::db::{Db, Error};
use chrono::{DateTime, Utc};
use db_types::table_record;
use kanau::processor::Processor;
use serde::{Deserialize, Serialize};
use sqlx::types::Json;

table_record!(ServerConfigViewId, "orchestration_server_config_view");

/// How a listener speaks to whoever dials it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
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
/// whichever pod produced it, and whatever address the server is dialed on
/// today. That is what lets a server keep serving a listener across an unrelated
/// edit (or an address change) while its dependants still reference it.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
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

    /// The server's id as text, for messages and ordering.
    pub fn server_key(&self) -> String {
        self.server.to_string()
    }

    pub fn transport(&self) -> guru_worker_config::Transport {
        match self.protocol {
            ListenProtocol::RelayQuic => guru_worker_config::Transport::Quic,
            _ => guru_worker_config::Transport::Tcp,
        }
    }
}

/// One `[[forwarding]]` entry's place in the dependency graph.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ForwardingDeps {
    /// The pod this entry was compiled from; also the entry's tag.
    ///
    /// Identity, not diagnostics: it is how convergence finds the previous shape
    /// of a forwarding whose pod stopped compiling. A socket cannot serve that
    /// purpose, because the edit that broke the pod may also have moved it.
    pub pod: PodId,
    pub serves: ListenerCap,
    pub points_at: Vec<ListenerCap>,
    /// The certificates this entry's TOML references (the ACME certificate of a
    /// TLS client listener, the relay leaf of a TLS/QUIC relay listener). A forwarding
    /// carried over from an older snapshot keeps its refs, so a synthesised
    /// snapshot's `certificates` is just the union over its entries.
    #[serde(default)]
    pub certificates: Vec<CertificateRef>,
}

/// A certificate a snapshot's TOML references, at the version it was derived
/// against. The material itself is fetched when the revision is handed to a
/// worker, so no key ever lives in a snapshot; the pinned version is what makes
/// a renewal a new revision even though the file paths do not change.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct CertificateRef {
    pub kind: CertificateKind,
    /// The record key of the `certificate` (ACME) or `relay_certificate` row.
    pub key: String,
    pub version: i64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CertificateKind {
    /// Public ACME certificate, delivered as `certs/acme/<key>/{full_chain,key}.pem`.
    Acme,
    /// Relay leaf, delivered as `certs/relay/<key>/{full_chain,key}.pem`.
    Relay,
}

/// A pod the worker could not apply from the acknowledged revision.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PodFailure {
    pub pod: PodId,
    /// The `[[forwarding]]` tag it was applied under.
    pub tag: String,
    pub error: String,
}

/// A pod that could not be compiled on its own (a certificate not issued yet, a
/// target without an address).
///
/// The rest of the server still compiles and is published; this is how an
/// operator learns which pod is broken and why. A pod listed here keeps serving
/// whatever its listener last was, so a missing certificate cannot drop live
/// traffic.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InvalidPod {
    pub pod: PodId,
    /// The pod's name when it was compiled.
    #[serde(default)]
    pub name: String,
    /// The socket the pod would serve, as `ip:port`. Diagnostic only.
    pub listen: String,
    pub error: String,
}

/// An immutable rendering of one server's config.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConfigSnapshot {
    pub revision: i64,
    pub toml: String,
    pub created_at: DateTime<Utc>,
    /// Index-aligned with the `[[forwarding]]` entries of `toml`.
    pub forwardings: Vec<ForwardingDeps>,
    /// The certificates `toml` references. Sorted, so two snapshots asking for
    /// the same material compare equal.
    #[serde(default)]
    pub certificates: Vec<CertificateRef>,
}

#[derive(Debug, Clone)]
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
    pub failed_pods: Vec<PodFailure>,
    pub derive_error: Option<String>,
    /// Pods that failed to derive while the rest of this server's config was
    /// published. Empty when `derive_error` is set: that is a whole-server failure.
    pub invalid_pods: Vec<InvalidPod>,
    /// Servers whose `applied` config does not yet serve a listener this server's
    /// ideal config points at.
    pub waiting_for: Vec<ServerId>,
    /// Bumped by every worker-driven write to this row (ack, registration,
    /// address report). Derivation stamps the tree's sum of these on the canvas
    /// as `derived_view_seq`, which is how a worker moves the fence without
    /// ever writing the canvas row.
    pub seq: i64,
}

/// A view row as the query macros read it: they build the struct themselves, so
/// the `jsonb` columns arrive as [`Json`] and are unwrapped once, here.
struct ServerConfigViewRow {
    id: ServerConfigViewId,
    server: ServerId,
    desired: Option<Json<ConfigSnapshot>>,
    in_flight: Option<Json<ConfigSnapshot>>,
    applied: Option<Json<ConfigSnapshot>>,
    failed_revision: Option<i64>,
    apply_error: Option<String>,
    failed_pods: Json<Vec<PodFailure>>,
    derive_error: Option<String>,
    invalid_pods: Json<Vec<InvalidPod>>,
    waiting_for: Vec<ServerId>,
    seq: i64,
}

impl From<ServerConfigViewRow> for ServerConfigViewEntity {
    fn from(row: ServerConfigViewRow) -> Self {
        Self {
            id: row.id,
            server: row.server,
            desired: row.desired.map(|json| json.0),
            in_flight: row.in_flight.map(|json| json.0),
            applied: row.applied.map(|json| json.0),
            failed_revision: row.failed_revision,
            apply_error: row.apply_error,
            failed_pods: row.failed_pods.0,
            derive_error: row.derive_error,
            invalid_pods: row.invalid_pods.0,
            waiting_for: row.waiting_for,
            seq: row.seq,
        }
    }
}

#[derive(Debug)]
pub struct FindServerConfigView {
    pub server: ServerId,
}

impl Processor<FindServerConfigView> for Db {
    type Output = Option<ServerConfigViewEntity>;
    type Error = Error;
    #[tracing::instrument(name = "Query:FindServerConfigView", skip_all, err)]
    async fn process(&self, input: FindServerConfigView) -> Result<Self::Output, Self::Error> {
        Ok(sqlx::query_file_as!(
            ServerConfigViewRow,
            "sql/find_server_config_view.sql",
            input.server as _
        )
        .fetch_optional(self.db())
        .await?
        .map(Into::into))
    }
}

/// The config views of every server on the given canvases (a whole tree).
#[derive(Debug)]
pub struct ListServerConfigViewsByCanvases {
    pub canvases: Vec<CanvasId>,
}

impl Processor<ListServerConfigViewsByCanvases> for Db {
    type Output = Vec<ServerConfigViewEntity>;
    type Error = Error;
    #[tracing::instrument(name = "Query:ListServerConfigViewsByCanvases", skip_all, err)]
    async fn process(
        &self,
        input: ListServerConfigViewsByCanvases,
    ) -> Result<Self::Output, Self::Error> {
        let mut conn = self.db().acquire().await?;
        views_of_canvases(&mut conn, &input.canvases).await
    }
}

async fn views_of_canvases(
    conn: &mut sqlx::PgConnection,
    canvases: &[CanvasId],
) -> Result<Vec<ServerConfigViewEntity>, Error> {
    let rows = sqlx::query_file_as!(
        ServerConfigViewRow,
        "sql/views_of_canvases.sql",
        canvases as _
    )
    .fetch_all(conn)
    .await?;
    Ok(rows.into_iter().map(Into::into).collect())
}

/// Hands `desired` to the calling stream by promoting it to `in_flight`.
///
/// The whole decision is one conditional update, so two streams can never be sent
/// the same revision and a fenced-out stream is sent nothing at all: the fence is
/// checked against the server row inside the same statement.
#[derive(Debug)]
pub struct TakeInFlight {
    pub server: ServerId,
    pub generation: i64,
    pub epoch: i64,
}

impl Processor<TakeInFlight> for Db {
    /// The snapshot to send, or `None` when there is nothing to hand out.
    type Output = Option<ConfigSnapshot>;
    type Error = Error;
    #[tracing::instrument(name = "Query:TakeInFlight", skip_all, err)]
    async fn process(&self, input: TakeInFlight) -> Result<Self::Output, Self::Error> {
        let taken: Option<Json<ConfigSnapshot>> = sqlx::query_file_scalar!(
            "sql/take_in_flight.sql",
            input.server as _,
            input.generation,
            input.epoch
        )
        .fetch_optional(self.db())
        .await?;
        Ok(taken.map(|json| json.0))
    }
}

/// Promotes `in_flight` to `applied`, or records why the worker refused it.
///
/// Three outcomes: `error` set — the revision was not applied at all;
/// `applied` set — some pods failed and this is the synthesised mix the worker
/// actually runs (`failed_pods` names them, `failed_revision` is the acked one);
/// neither — every pod applied and `in_flight` is promoted as is.
///
/// Unlike [`TakeInFlight`], this update carries no
/// `(refresh_key_generation, watch_epoch)` predicate, deliberately: an ack arrives
/// on a unary RPC authenticated by the refresh-key digest, and re-registration
/// rotates that digest while incrementing `refresh_key_generation`, so a
/// fenced-out worker cannot authenticate an ack at all. Only this server's view
/// row is written; `seq + 1` is what tells derivation something changed.
#[derive(Debug)]
pub struct AckServerConfig {
    pub server: ServerId,
    pub revision: i64,
    pub error: Option<String>,
    pub applied: Option<ConfigSnapshot>,
    pub failed_pods: Vec<PodFailure>,
}

impl Processor<AckServerConfig> for Db {
    /// `false` when the acknowledged revision is not the one in flight.
    type Output = bool;
    type Error = Error;
    #[tracing::instrument(name = "Query:AckServerConfig", skip_all, err)]
    async fn process(&self, input: AckServerConfig) -> Result<Self::Output, Self::Error> {
        let failed_revision = (!input.failed_pods.is_empty()).then_some(input.revision);
        let acked: Option<ServerConfigViewId> = if let Some(error) = &input.error {
            sqlx::query_file_scalar!(
                "sql/ack_server_config_error.sql",
                &input.server as _,
                input.revision,
                error.as_str()
            )
            .fetch_optional(self.db())
            .await?
        } else if let Some(applied) = &input.applied {
            // Some pods failed: `applied` is the mix the worker runs (the revision
            // for the pods that took it, each failed pod's previous shape),
            // synthesised by the service.
            sqlx::query_file_scalar!(
                "sql/ack_server_config_partial.sql",
                &input.server as _,
                input.revision,
                Json(applied) as _,
                failed_revision,
                Json(&input.failed_pods) as _
            )
            .fetch_optional(self.db())
            .await?
        } else {
            sqlx::query_file_scalar!(
                "sql/ack_server_config_applied.sql",
                &input.server as _,
                input.revision
            )
            .fetch_optional(self.db())
            .await?
        };
        Ok(acked.is_some())
    }
}

/// Clears a dead server's applied state so servers waiting on it can proceed.
#[derive(Debug)]
pub struct ForgetServerAppliedRow {
    pub server: ServerId,
    pub canvas: CanvasId,
}

impl Processor<ForgetServerAppliedRow> for Db {
    type Output = ();
    type Error = Error;
    #[tracing::instrument(name = "Query-Transaction:ForgetServerAppliedRow", skip_all, err)]
    async fn process(&self, input: ForgetServerAppliedRow) -> Result<Self::Output, Self::Error> {
        let mut tx = self.db().begin().await?;
        // The root before the view row (`fence`'s lock order), the order a
        // derivation commit takes them in.
        fence::touch(&mut tx, &input.canvas).await?;
        sqlx::query!(
            "UPDATE orchestration_server_config_view
             SET applied = NULL, in_flight = NULL, apply_error = NULL, failed_revision = NULL,
                 failed_pods = '[]'
             WHERE server = $1",
            &input.server as _
        )
        .execute(&mut *tx)
        .await?;
        tx.commit().await?;
        Ok(())
    }
}

/// What the watch poller compares between ticks.
#[derive(Debug, Clone)]
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

impl Processor<ListServerWatchState> for Db {
    type Output = Vec<ServerWatchState>;
    type Error = Error;
    #[tracing::instrument(name = "Query:ListServerWatchState", skip_all, err)]
    async fn process(&self, input: ListServerWatchState) -> Result<Self::Output, Self::Error> {
        Ok(sqlx::query_file_as!(
            ServerWatchState,
            "sql/list_server_watch_state.sql",
            &input.servers as _
        )
        .fetch_all(self.db())
        .await?)
    }
}

/// Everything one derivation pass reads, in a single transaction. The counters
/// are the root's; the graph is the whole tree.
#[derive(Debug, Clone)]
pub struct DerivationInput {
    pub root: CanvasId,
    pub generation: i64,
    pub derived_generation: i64,
    /// The tree's view `seq` sum the last committed pass converged against.
    pub derived_view_seq: i64,
    /// The tree's view `seq` sum as this read saw it.
    pub view_seq: i64,
    pub graph: GraphRows,
    pub views: Vec<ServerConfigViewEntity>,
}

/// Loads the derivation input of the tree containing `canvas`.
pub struct LoadCanvasDerivationInput {
    pub canvas: CanvasId,
}

impl Processor<LoadCanvasDerivationInput> for Db {
    /// `None` when the canvas has been deleted.
    type Output = Option<DerivationInput>;
    type Error = Error;
    #[tracing::instrument(name = "Query-Transaction:LoadCanvasDerivationInput", skip_all, err)]
    async fn process(&self, input: LoadCanvasDerivationInput) -> Result<Self::Output, Self::Error> {
        let mut tx = self.db().begin_with(SNAPSHOT_READ).await?;
        let rows = load_graph(&mut tx, &input.canvas).await?;
        let Some(root) = rows.canvases.iter().find(|c| c.id == rows.root) else {
            return Ok(None);
        };
        let derived_view_seq: i64 = sqlx::query_scalar!(
            "SELECT derived_view_seq FROM orchestration_canvas WHERE id = $1",
            &rows.root as _
        )
        .fetch_one(&mut *tx)
        .await?;
        let canvas_ids: Vec<CanvasId> = rows.canvases.iter().map(|c| c.id.clone()).collect();
        let views = views_of_canvases(&mut tx, &canvas_ids).await?;
        tx.commit().await?;
        let view_seq = views.iter().map(|view| view.seq).sum();
        Ok(Some(DerivationInput {
            root: rows.root.clone(),
            generation: root.generation,
            derived_generation: root.derived_generation,
            derived_view_seq,
            view_seq,
            graph: rows,
            views,
        }))
    }
}

/// One server's outcome of a derivation pass.
///
/// `desired: None` means "leave the slot alone" — either the derivation failed or
/// it produced byte-identical TOML.
#[derive(Debug, Clone)]
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
/// generation it was derived from and no pass has published this state before.
/// `canvas` must be the tree's root; `view_seq` is the tree's view `seq` sum the
/// pass read.
pub struct CommitCanvasDerivation {
    pub canvas: CanvasId,
    pub generation: i64,
    pub view_seq: i64,
    pub updates: Vec<ViewUpdate>,
}

impl Processor<CommitCanvasDerivation> for Db {
    /// `false` when the canvas moved on and the pass has to be redone.
    type Output = bool;
    type Error = Error;
    #[tracing::instrument(name = "Query-Transaction:CommitCanvasDerivation", skip_all, err)]
    async fn process(&self, input: CommitCanvasDerivation) -> Result<Self::Output, Self::Error> {
        let mut tx = self.db().begin().await?;
        // `generation` fences against edits made since the pass read the canvas.
        // `derived_generation < $2 OR derived_view_seq < $3` fences against a
        // second pass that read the same state, so only the first one to arrive
        // publishes its revisions. `view_seq` is the sum of the tree's view `seq`
        // counters as the pass read them: a worker write that lands after the
        // read leaves the sum above the stamp, so the canvas reads as stale again.
        let fenced: Option<CanvasId> = sqlx::query_file_scalar!(
            "sql/commit_canvas_derivation.sql",
            &input.canvas as _,
            input.generation,
            input.view_seq
        )
        .fetch_optional(&mut *tx)
        .await?;
        if fenced.is_none() {
            tx.rollback().await?;
            return Ok(false);
        }
        for update in &input.updates {
            // One statement per view row. PostgreSQL re-checks a row's foreign
            // key when the transaction updates that row a second time, and the
            // check takes KEY SHARE on the server row: a lock after the view row,
            // where a worker's address report locks the server row first.
            // `desired` is left alone when the pass has no new revision;
            // `invalid_pods` is written either way: a pod breaking or being fixed
            // must be visible even when the served config is byte-identical.
            sqlx::query_file!(
                "sql/commit_canvas_derivation_view.sql",
                &update.server as _,
                update.desired.as_ref().map(Json) as _,
                update.derive_error.as_deref(),
                Json(&update.invalid_pods) as _,
                &update.waiting_for as _,
                update.clear_failure
            )
            .execute(&mut *tx)
            .await?;
        }
        // Subcanvas rows carry no generation of their own; keep them level so a
        // canvas that was imported mid-edit can never look stale on its own.
        let members = tree::tree_of(&mut tx, &input.canvas).await?;
        sqlx::query!(
            "UPDATE orchestration_canvas
             SET derived_generation = generation, derived_view_seq = $2
             WHERE id = ANY($1) AND id <> $3",
            &members as _,
            input.view_seq,
            &input.canvas as _
        )
        .execute(&mut *tx)
        .await?;
        tx.commit().await?;
        Ok(true)
    }
}

/// Canvases whose derivation is behind their edits or their workers' acks; the
/// cron sweep's input.
///
/// An operator edit bumped `generation`, or a worker wrote one of the tree's view
/// rows (their `seq` sum moved past what the last committed pass stamped).
/// Subcanvas rows are levelled to the tree's sum on every commit and their own
/// subtree sums to no more, so only a root reads as stale on its own.
pub struct ListStaleCanvases;

impl Processor<ListStaleCanvases> for Db {
    type Output = Vec<CanvasId>;
    type Error = Error;
    #[tracing::instrument(name = "Query:ListStaleCanvases", skip_all, err)]
    async fn process(&self, _input: ListStaleCanvases) -> Result<Self::Output, Self::Error> {
        Ok(sqlx::query_file_scalar!("sql/list_stale_canvases.sql")
            .fetch_all(self.db())
            .await?)
    }
}
