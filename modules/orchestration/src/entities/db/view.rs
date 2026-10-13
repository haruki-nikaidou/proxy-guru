//! The per-server config view: what a server should run, what is in flight, and
//! what it is actually running.
//!
//! Exactly one row exists per server. Each of the three slots holds a whole
//! [`ConfigSnapshot`] — the rendered TOML plus the listener capabilities it serves
//! and points at — so convergence never has to re-derive a config from rows that
//! have since been edited in place.

use crate::entities::db::canvas::CanvasId;
use crate::entities::db::fence::Touch;
use crate::entities::db::graph::{GraphEffect, GraphRows, LoadGraph};
use crate::entities::db::pod::PodId;
use crate::entities::db::server::ServerId;
use crate::entities::db::tables::{
    OrchestrationCanvas, OrchestrationServer, OrchestrationServerConfigView,
};
use crate::entities::db::tree::TreeOf;
use base::db::Error;
use db_types::table_record;
use serde::{Deserialize, Serialize};
use sqlx::types::Json;
use sqlx::{PgConnection, Postgres};
use time::OffsetDateTime;
use wakuwaku::entity::sqlx::backend::PgSource;
use wakuwaku::entity::sqlx::transaction::{Either, IsolationLevel, Tx, TxEnd, TxFlow, TxState};
use wakuwaku::entity::{Execute, Query, Read, Write};

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
    #[serde(with = "time::serde::rfc3339")]
    pub created_at: OffsetDateTime,
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

impl Query for FindServerConfigView {
    type Effect = Read<OrchestrationServerConfigView>;
}

impl Execute<PgSource> for FindServerConfigView {
    type Output = Option<ServerConfigViewEntity>;
    type Error = Error;
    #[tracing::instrument(name = "Query:FindServerConfigView", skip_all, err)]
    async fn execute(self, conn: &mut PgConnection) -> Result<Self::Output, Self::Error> {
        Ok(sqlx::query_file_as!(
            ServerConfigViewRow,
            "sql/find_server_config_view.sql",
            self.server as _
        )
        .fetch_optional(conn)
        .await?
        .map(Into::into))
    }
}

/// The config views of every server on the given canvases (a whole tree).
#[derive(Debug)]
pub struct ListServerConfigViewsByCanvases {
    pub canvases: Vec<CanvasId>,
}

impl Query for ListServerConfigViewsByCanvases {
    type Effect = (
        Read<OrchestrationServerConfigView>,
        Read<OrchestrationServer>,
    );
}

impl Execute<PgSource> for ListServerConfigViewsByCanvases {
    type Output = Vec<ServerConfigViewEntity>;
    type Error = Error;
    #[tracing::instrument(name = "Query:ListServerConfigViewsByCanvases", skip_all, err)]
    async fn execute(self, conn: &mut PgConnection) -> Result<Self::Output, Self::Error> {
        ViewsOfCanvases {
            canvases: &self.canvases,
        }
        .execute(conn)
        .await
    }
}

/// The config views of every server on the given canvases, by server.
struct ViewsOfCanvases<'a> {
    canvases: &'a [CanvasId],
}

impl Query for ViewsOfCanvases<'_> {
    type Effect = (
        Read<OrchestrationServerConfigView>,
        Read<OrchestrationServer>,
    );
}

impl Execute<PgSource> for ViewsOfCanvases<'_> {
    type Output = Vec<ServerConfigViewEntity>;
    type Error = Error;
    async fn execute(self, conn: &mut PgConnection) -> Result<Self::Output, Self::Error> {
        let rows = sqlx::query_file_as!(
            ServerConfigViewRow,
            "sql/views_of_canvases.sql",
            self.canvases as _
        )
        .fetch_all(conn)
        .await?;
        Ok(rows.into_iter().map(Into::into).collect())
    }
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

impl Query for TakeInFlight {
    type Effect = (
        Write<OrchestrationServerConfigView>,
        Read<OrchestrationServer>,
    );
}

impl Execute<PgSource> for TakeInFlight {
    /// The snapshot to send, or `None` when there is nothing to hand out.
    type Output = Option<ConfigSnapshot>;
    type Error = Error;
    #[tracing::instrument(name = "Query:TakeInFlight", skip_all, err)]
    async fn execute(self, conn: &mut PgConnection) -> Result<Self::Output, Self::Error> {
        let taken: Option<Json<ConfigSnapshot>> = sqlx::query_file_scalar!(
            "sql/take_in_flight.sql",
            self.server as _,
            self.generation,
            self.epoch
        )
        .fetch_optional(conn)
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

impl Query for AckServerConfig {
    type Effect = Write<OrchestrationServerConfigView>;
}

impl Execute<PgSource> for AckServerConfig {
    /// `false` when the acknowledged revision is not the one in flight.
    type Output = bool;
    type Error = Error;
    #[tracing::instrument(name = "Query:AckServerConfig", skip_all, err)]
    async fn execute(self, conn: &mut PgConnection) -> Result<Self::Output, Self::Error> {
        let failed_revision = (!self.failed_pods.is_empty()).then_some(self.revision);
        let acked: Option<ServerConfigViewId> = if let Some(error) = &self.error {
            sqlx::query_file_scalar!(
                "sql/ack_server_config_error.sql",
                &self.server as _,
                self.revision,
                error.as_str()
            )
            .fetch_optional(conn)
            .await?
        } else if let Some(applied) = &self.applied {
            // Some pods failed: `applied` is the mix the worker runs (the revision
            // for the pods that took it, each failed pod's previous shape),
            // synthesised by the service.
            sqlx::query_file_scalar!(
                "sql/ack_server_config_partial.sql",
                &self.server as _,
                self.revision,
                Json(applied) as _,
                failed_revision,
                Json(&self.failed_pods) as _
            )
            .fetch_optional(conn)
            .await?
        } else {
            sqlx::query_file_scalar!(
                "sql/ack_server_config_applied.sql",
                &self.server as _,
                self.revision
            )
            .fetch_optional(conn)
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

struct ClearAppliedState<'a> {
    server: &'a ServerId,
}

impl Query for ClearAppliedState<'_> {
    type Effect = Write<OrchestrationServerConfigView>;
}

impl Execute<PgSource> for ClearAppliedState<'_> {
    type Output = ();
    type Error = Error;
    async fn execute(self, conn: &mut PgConnection) -> Result<(), Error> {
        sqlx::query!(
            "UPDATE orchestration_server_config_view
             SET applied = NULL, in_flight = NULL, apply_error = NULL, failed_revision = NULL,
                 failed_pods = '[]'
             WHERE server = $1",
            self.server as _
        )
        .execute(conn)
        .await?;
        Ok(())
    }
}

/// Clears the applied state of [`ForgetServerAppliedRow`] after the tree's
/// root. Run it with `db.process(Transaction(ForgetServerAppliedRowFlow::new(input)))`.
pub enum ForgetServerAppliedRowFlow {
    Start(ForgetServerAppliedRow),
    End(TxEnd<ForgetServerAppliedRowFlow>),
}

impl ForgetServerAppliedRowFlow {
    pub fn new(input: ForgetServerAppliedRow) -> Self {
        ForgetServerAppliedRowFlow::Start(input)
    }
}

impl Query for ForgetServerAppliedRowFlow {
    type Effect = (
        Write<OrchestrationCanvas>,
        Write<OrchestrationServerConfigView>,
    );
}

impl From<TxEnd<ForgetServerAppliedRowFlow>> for ForgetServerAppliedRowFlow {
    fn from(end: TxEnd<ForgetServerAppliedRowFlow>) -> Self {
        ForgetServerAppliedRowFlow::End(end)
    }
}

impl TxFlow for ForgetServerAppliedRowFlow {
    type Database = Postgres;
    type Commit = ();
    type Abort = Error;
    type Report = ();

    #[tracing::instrument(name = "Transaction:ForgetServerAppliedRow", skip_all)]
    async fn step(tx: Tx<'_, Self>, state: Self) -> TxState<'_, Self> {
        let ForgetServerAppliedRowFlow::Start(input) = state else {
            return tx.end(state).await;
        };
        // The root before the view row (`fence`'s lock order), the order a
        // derivation commit takes them in.
        let (tx, touched) = tx
            .run(Touch {
                canvas: &input.canvas,
            })
            .await;
        if let Err(e) = touched {
            return base::db::rollback(tx, e).await;
        }
        let (tx, cleared) = tx
            .run(ClearAppliedState {
                server: &input.server,
            })
            .await;
        match cleared {
            Ok(()) => base::db::commit(tx, ()).await,
            Err(e) => base::db::rollback(tx, e).await,
        }
    }

    fn finished(self) -> Either<Self, TxEnd<Self>> {
        match self {
            ForgetServerAppliedRowFlow::End(end) => Either::Right(end),
            state => Either::Left(state),
        }
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

impl Query for ListServerWatchState {
    type Effect = (
        Read<OrchestrationServerConfigView>,
        Read<OrchestrationServer>,
    );
}

impl Execute<PgSource> for ListServerWatchState {
    type Output = Vec<ServerWatchState>;
    type Error = Error;
    #[tracing::instrument(name = "Query:ListServerWatchState", skip_all, err)]
    async fn execute(self, conn: &mut PgConnection) -> Result<Self::Output, Self::Error> {
        Ok(sqlx::query_file_as!(
            ServerWatchState,
            "sql/list_server_watch_state.sql",
            &self.servers as _
        )
        .fetch_all(conn)
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

/// Loads the derivation input of the tree containing `canvas`, in one
/// read-only `REPEATABLE READ` snapshot.
pub struct LoadCanvasDerivationInput {
    pub canvas: CanvasId,
}

struct DerivedViewSeq<'a> {
    root: &'a CanvasId,
}

impl Query for DerivedViewSeq<'_> {
    type Effect = Read<OrchestrationCanvas>;
}

impl Execute<PgSource> for DerivedViewSeq<'_> {
    type Output = i64;
    type Error = Error;
    async fn execute(self, conn: &mut PgConnection) -> Result<i64, Error> {
        Ok(sqlx::query_scalar!(
            "SELECT derived_view_seq FROM orchestration_canvas WHERE id = $1",
            self.root as _
        )
        .fetch_one(conn)
        .await?)
    }
}

/// Reads a [`DerivationInput`] in one snapshot; commits `None` when the canvas
/// has been deleted. Run it with
/// `db.process(Transaction(LoadCanvasDerivationInputFlow::new(input)))`.
// The end carries the whole derivation input (~248 B), the start one canvas
// id. One value per transaction; boxing would only add an allocation.
#[allow(clippy::large_enum_variant)]
pub enum LoadCanvasDerivationInputFlow {
    Start(LoadCanvasDerivationInput),
    End(TxEnd<LoadCanvasDerivationInputFlow>),
}

impl LoadCanvasDerivationInputFlow {
    pub fn new(input: LoadCanvasDerivationInput) -> Self {
        LoadCanvasDerivationInputFlow::Start(input)
    }
}

impl Query for LoadCanvasDerivationInputFlow {
    type Effect = (GraphEffect, Read<OrchestrationServerConfigView>);
}

impl From<TxEnd<LoadCanvasDerivationInputFlow>> for LoadCanvasDerivationInputFlow {
    fn from(end: TxEnd<LoadCanvasDerivationInputFlow>) -> Self {
        LoadCanvasDerivationInputFlow::End(end)
    }
}

impl TxFlow for LoadCanvasDerivationInputFlow {
    type Database = Postgres;
    type Commit = Option<DerivationInput>;
    type Abort = Error;
    type Report = ();
    const ISOLATION: Option<IsolationLevel> = Some(IsolationLevel::RepeatableRead);

    #[tracing::instrument(name = "Transaction:LoadCanvasDerivationInput", skip_all)]
    async fn step(tx: Tx<'_, Self>, state: Self) -> TxState<'_, Self> {
        let LoadCanvasDerivationInputFlow::Start(input) = state else {
            return tx.end(state).await;
        };
        let (tx, rows) = tx
            .run(LoadGraph {
                canvas: &input.canvas,
            })
            .await;
        let rows = match rows {
            Ok(rows) => rows,
            Err(e) => return base::db::rollback(tx, e).await,
        };
        let Some((generation, derived_generation)) = rows
            .canvases
            .iter()
            .find(|c| c.id == rows.root)
            .map(|root| (root.generation, root.derived_generation))
        else {
            return base::db::commit(tx, None).await;
        };
        let (tx, derived_view_seq) = tx.run(DerivedViewSeq { root: &rows.root }).await;
        let derived_view_seq = match derived_view_seq {
            Ok(seq) => seq,
            Err(e) => return base::db::rollback(tx, e).await,
        };
        let canvas_ids = rows.canvas_ids();
        let (tx, views) = tx
            .run(ViewsOfCanvases {
                canvases: &canvas_ids,
            })
            .await;
        let views = match views {
            Ok(views) => views,
            Err(e) => return base::db::rollback(tx, e).await,
        };
        let view_seq = views.iter().map(|view| view.seq).sum();
        let input = DerivationInput {
            root: rows.root.clone(),
            generation,
            derived_generation,
            derived_view_seq,
            view_seq,
            graph: rows,
            views,
        };
        base::db::commit(tx, Some(input)).await
    }

    fn finished(self) -> Either<Self, TxEnd<Self>> {
        match self {
            LoadCanvasDerivationInputFlow::End(end) => Either::Right(end),
            state => Either::Left(state),
        }
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

/// The derivation fence on the root row; `false` when it did not match.
struct FenceDerivation<'a> {
    canvas: &'a CanvasId,
    generation: i64,
    view_seq: i64,
}

impl Query for FenceDerivation<'_> {
    type Effect = Write<OrchestrationCanvas>;
}

impl Execute<PgSource> for FenceDerivation<'_> {
    type Output = bool;
    type Error = Error;
    async fn execute(self, conn: &mut PgConnection) -> Result<bool, Error> {
        let fenced: Option<CanvasId> = sqlx::query_file_scalar!(
            "sql/commit_canvas_derivation.sql",
            self.canvas as _,
            self.generation,
            self.view_seq
        )
        .fetch_optional(conn)
        .await?;
        Ok(fenced.is_some())
    }
}

struct WriteViewUpdate<'a> {
    update: &'a ViewUpdate,
}

impl Query for WriteViewUpdate<'_> {
    type Effect = Write<OrchestrationServerConfigView>;
}

impl Execute<PgSource> for WriteViewUpdate<'_> {
    type Output = ();
    type Error = Error;
    async fn execute(self, conn: &mut PgConnection) -> Result<(), Error> {
        let update = self.update;
        sqlx::query_file!(
            "sql/commit_canvas_derivation_view.sql",
            &update.server as _,
            update.desired.as_ref().map(Json) as _,
            update.derive_error.as_deref(),
            Json(&update.invalid_pods) as _,
            &update.waiting_for as _,
            update.clear_failure
        )
        .execute(conn)
        .await?;
        Ok(())
    }
}

/// Levels the subcanvas rows of a tree with its root's derivation stamp.
struct LevelSubcanvases<'a> {
    members: &'a [CanvasId],
    root: &'a CanvasId,
    view_seq: i64,
}

impl Query for LevelSubcanvases<'_> {
    type Effect = Write<OrchestrationCanvas>;
}

impl Execute<PgSource> for LevelSubcanvases<'_> {
    type Output = ();
    type Error = Error;
    async fn execute(self, conn: &mut PgConnection) -> Result<(), Error> {
        sqlx::query!(
            "UPDATE orchestration_canvas
             SET derived_generation = generation, derived_view_seq = $2
             WHERE id = ANY($1) AND id <> $3",
            self.members as _,
            self.view_seq,
            self.root as _
        )
        .execute(conn)
        .await?;
        Ok(())
    }
}

/// Commits a derivation pass under its fence; commits `false` (nothing
/// written) when the canvas moved on and the pass has to be redone. Run it
/// with `db.process(Transaction(CommitCanvasDerivationFlow::new(input)))`.
pub enum CommitCanvasDerivationFlow {
    Start(CommitCanvasDerivation),
    End(TxEnd<CommitCanvasDerivationFlow>),
}

impl CommitCanvasDerivationFlow {
    pub fn new(input: CommitCanvasDerivation) -> Self {
        CommitCanvasDerivationFlow::Start(input)
    }
}

impl Query for CommitCanvasDerivationFlow {
    type Effect = (
        Write<OrchestrationCanvas>,
        Write<OrchestrationServerConfigView>,
    );
}

impl From<TxEnd<CommitCanvasDerivationFlow>> for CommitCanvasDerivationFlow {
    fn from(end: TxEnd<CommitCanvasDerivationFlow>) -> Self {
        CommitCanvasDerivationFlow::End(end)
    }
}

impl TxFlow for CommitCanvasDerivationFlow {
    type Database = Postgres;
    type Commit = bool;
    type Abort = Error;
    type Report = ();

    #[tracing::instrument(name = "Transaction:CommitCanvasDerivation", skip_all)]
    async fn step(tx: Tx<'_, Self>, state: Self) -> TxState<'_, Self> {
        let CommitCanvasDerivationFlow::Start(input) = state else {
            return tx.end(state).await;
        };
        // `generation` fences against edits made since the pass read the canvas.
        // `derived_generation < $2 OR derived_view_seq < $3` fences against a
        // second pass that read the same state, so only the first one to arrive
        // publishes its revisions. `view_seq` is the sum of the tree's view `seq`
        // counters as the pass read them: a worker write that lands after the
        // read leaves the sum above the stamp, so the canvas reads as stale again.
        let (tx, fenced) = tx
            .run(FenceDerivation {
                canvas: &input.canvas,
                generation: input.generation,
                view_seq: input.view_seq,
            })
            .await;
        let mut tx = match fenced {
            Ok(true) => tx,
            Ok(false) => return base::db::commit(tx, false).await,
            Err(e) => return base::db::rollback(tx, e).await,
        };
        for update in &input.updates {
            // One statement per view row. PostgreSQL re-checks a row's foreign
            // key when the transaction updates that row a second time, and the
            // check takes KEY SHARE on the server row: a lock after the view row,
            // where a worker's address report locks the server row first.
            // `desired` is left alone when the pass has no new revision;
            // `invalid_pods` is written either way: a pod breaking or being fixed
            // must be visible even when the served config is byte-identical.
            let (next, r) = tx.run(WriteViewUpdate { update }).await;
            tx = next;
            if let Err(e) = r {
                return base::db::rollback(tx, e).await;
            }
        }
        // Subcanvas rows carry no generation of their own; keep them level so a
        // canvas that was imported mid-edit can never look stale on its own.
        let (tx, members) = tx
            .run(TreeOf {
                canvas: &input.canvas,
            })
            .await;
        let members = match members {
            Ok(members) => members,
            Err(e) => return base::db::rollback(tx, e).await,
        };
        let (tx, levelled) = tx
            .run(LevelSubcanvases {
                members: &members,
                root: &input.canvas,
                view_seq: input.view_seq,
            })
            .await;
        match levelled {
            Ok(()) => base::db::commit(tx, true).await,
            Err(e) => base::db::rollback(tx, e).await,
        }
    }

    fn finished(self) -> Either<Self, TxEnd<Self>> {
        match self {
            CommitCanvasDerivationFlow::End(end) => Either::Right(end),
            state => Either::Left(state),
        }
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

impl Query for ListStaleCanvases {
    type Effect = (
        Read<OrchestrationCanvas>,
        Read<OrchestrationServer>,
        Read<OrchestrationServerConfigView>,
    );
}

impl Execute<PgSource> for ListStaleCanvases {
    type Output = Vec<CanvasId>;
    type Error = Error;
    #[tracing::instrument(name = "Query:ListStaleCanvases", skip_all, err)]
    async fn execute(self, conn: &mut PgConnection) -> Result<Self::Output, Self::Error> {
        Ok(sqlx::query_file_scalar!("sql/list_stale_canvases.sql")
            .fetch_all(conn)
            .await?)
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    /// A snapshot is stored as a `jsonb` document, so its timestamp is part of
    /// the persisted shape: `time`'s own `Serialize` writes an array of
    /// calendar components, which no row written so far can be read back from.
    /// The field's `rfc3339` attribute is what keeps both directions on the one
    /// spelling — this is the row a running deployment already holds.
    #[test]
    fn a_stored_snapshot_reads_back_from_rfc3339() {
        let stored = r#"{"revision":7,"toml":"[server]","created_at":"2026-09-21T14:14:56.789012Z","forwardings":[],"certificates":[]}"#;
        let snapshot: ConfigSnapshot = serde_json::from_str(stored).unwrap();
        assert_eq!(
            snapshot.created_at.unix_timestamp_nanos(),
            1_790_000_096_789_012_000
        );
        assert_eq!(
            serde_json::to_value(&snapshot).unwrap()["created_at"],
            "2026-09-21T14:14:56.789012Z"
        );
    }

    /// A whole second is written without a fractional part, and an offset other
    /// than `Z` is accepted on the way in.
    #[test]
    fn a_snapshot_timestamp_round_trips_at_second_resolution() {
        let stored =
            r#"{"revision":1,"toml":"","created_at":"2026-09-21T16:14:56+02:00","forwardings":[]}"#;
        let snapshot: ConfigSnapshot = serde_json::from_str(stored).unwrap();
        assert_eq!(snapshot.created_at.unix_timestamp(), 1_790_000_096);
        let again: ConfigSnapshot =
            serde_json::from_str(&serde_json::to_string(&snapshot).unwrap()).unwrap();
        assert_eq!(again.created_at, snapshot.created_at);
    }
}
