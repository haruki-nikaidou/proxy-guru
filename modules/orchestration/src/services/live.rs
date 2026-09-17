//! Live streams: shared per-key views, and the watch operations behind the
//! `Watch*` RPCs.
//!
//! # Two shapes, on purpose
//!
//! A rollout tree is *state*: every watcher of the same key wants
//! the same picture, and a watcher that fell behind wants the newest picture, not
//! the intermediate ones. Those are [`ViewRegistry`] views — one reload per
//! change per key, shared by reference count, published through
//! [`tokio::sync::watch`] so a slow consumer collapses a burst into one value and
//! can never make the server buffer.
//!
//! Server health is a *log*: the records are the content, and skipping
//! one loses information. Those are forwarded straight from the bus, and a gap
//! (a lag, a bus reconnect) is closed by re-reading the database from the last
//! record the stream actually sent.
//!
//! Either way the client never has to re-request anything, which is why there is
//! no `Resync` on the wire.

use crate::config::OrchestrationConfig;
use crate::entities::db::canvas::CanvasId;
use crate::entities::db::graph::LoadCanvasGraph;
use crate::entities::db::health::{
    ListPodHealthSince, ListServerHealthHistory as ListServerHealthHistoryRows,
    PodHealthRecordEntity, ServerHealthRecordEntity,
};
use crate::entities::db::pod::{FindPodById, PodId};
use crate::entities::db::server::{FindServerById, ServerEntity, ServerId};
use crate::entities::db::view::ListServerConfigViewsByCanvases;
use crate::events::live::{LiveMessage, RolloutScope};
use crate::hooks::live::{LiveBus, LiveEvent};
use crate::services::OrchestrationError;
use crate::services::graph;
use crate::services::health::DEFAULT_POD_HISTORY_LIMIT;
use crate::services::rollout::{RolloutStatus, rollout_status};
use auth::services::identity::Identity;
use auth::utils::rbac::Permission;
use base::db::Db;
use chrono::{DateTime, Utc};
use kanau::processor::Processor;
use std::collections::hash_map::Entry;
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};
use tokio::sync::{Notify, broadcast, watch};
use tokio::task::AbortHandle;

/// How long a view waits before retrying a failed load. The watcher is already
/// connected and has nothing to show, so this is a visible stall — short enough
/// to recover from a blip, long enough not to hammer a database that is down.
const RELOAD_BACKOFF: std::time::Duration = std::time::Duration::from_secs(2);

/// One kind of shared, reloadable state.
pub trait LiveView: Send + Sync + 'static {
    type State: Send + Sync + 'static;

    /// Whether `message` can have changed what `state` shows. Answering `true`
    /// too often costs a reload; answering `false` wrongly leaves a dashboard
    /// stale until the next change, so err towards `true`.
    fn matches(state: &Self::State, message: &LiveMessage) -> bool;

    /// Reads the state from the database. `None` means the watched record does
    /// not exist (any more).
    fn load(
        &self,
        db: &Db,
    ) -> impl Future<Output = Result<Option<Self::State>, OrchestrationError>> + Send;
}

/// What a watcher sees. `Loading` is only ever the initial value: a reload
/// replaces the previous `Ready` in place, so a refresh never blanks a stream.
#[derive(Clone)]
pub enum ViewValue<T> {
    Loading,
    Missing,
    Ready {
        state: Arc<T>,
        /// The change that caused this snapshot, or `None` for the opening one
        /// and for a refresh after a bus reconnect. When several changes
        /// collapsed into one reload, this is the latest of them.
        cause: Option<Arc<LiveMessage>>,
    },
}

struct ViewEntry<T> {
    tx: watch::Sender<ViewValue<T>>,
    subscribers: usize,
    task: AbortHandle,
    /// Asks the view task for one reload outside any bus message.
    reload: Arc<Notify>,
}

type Entries<T> = Arc<Mutex<HashMap<String, ViewEntry<T>>>>;

/// The live views of one kind, keyed by record key and reference counted.
///
/// The point is that N dashboards on one canvas cost one reload per change, not
/// N: the first subscriber spawns the view task, the last one to drop aborts it.
pub struct ViewRegistry<V: LiveView> {
    db: Db,
    bus: LiveBus,
    entries: Entries<V::State>,
}

// Derived by hand: `V` itself is never stored, so `Clone` must not require it.
impl<V: LiveView> Clone for ViewRegistry<V> {
    fn clone(&self) -> Self {
        Self {
            db: self.db.clone(),
            bus: self.bus.clone(),
            entries: self.entries.clone(),
        }
    }
}

/// A watcher's end of a shared view. Dropping it releases the reference.
pub struct ViewHandle<T> {
    pub rx: watch::Receiver<ViewValue<T>>,
    key: String,
    entries: Entries<T>,
}

impl<T> Drop for ViewHandle<T> {
    fn drop(&mut self) {
        let mut entries = match self.entries.lock() {
            Ok(entries) => entries,
            Err(poisoned) => poisoned.into_inner(),
        };
        let remove = match entries.get_mut(&self.key) {
            Some(entry) => {
                entry.subscribers = entry.subscribers.saturating_sub(1);
                entry.subscribers == 0
            }
            None => false,
        };
        if let Some(entry) = remove.then(|| entries.remove(&self.key)).flatten() {
            // Nothing is listening: stop reloading on every change.
            entry.task.abort();
        }
    }
}

impl<V: LiveView> ViewRegistry<V> {
    pub fn new(db: Db, bus: LiveBus) -> Self {
        Self {
            db,
            bus,
            entries: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Joins the view for `key`, spawning it if this is the first subscriber.
    pub fn subscribe(&self, key: String, view: V) -> ViewHandle<V::State> {
        let mut entries = match self.entries.lock() {
            Ok(entries) => entries,
            Err(poisoned) => poisoned.into_inner(),
        };
        let entry = match entries.entry(key.clone()) {
            Entry::Occupied(occupied) => {
                let entry = occupied.into_mut();
                // A joiner is handed what the view last loaded, and that can be
                // older than the database in ways no message announced: fields a
                // routine report moves, a write the view does not match. One
                // re-read per join keeps an open as fresh as a `GetGraph` was, and
                // makes a client's reconnect an actual re-read. A view still
                // loading is about to publish a fresh value anyway.
                if !matches!(*entry.tx.borrow(), ViewValue::Loading) {
                    entry.reload.notify_one();
                }
                entry
            }
            Entry::Vacant(vacant) => {
                let (tx, _) = watch::channel(ViewValue::Loading);
                let reload = Arc::new(Notify::new());
                let task = tokio::spawn(run_view(
                    view,
                    self.db.clone(),
                    self.bus.clone(),
                    tx.clone(),
                    reload.clone(),
                ));
                vacant.insert(ViewEntry {
                    tx,
                    subscribers: 0,
                    task: task.abort_handle(),
                    reload,
                })
            }
        };
        entry.subscribers = entry.subscribers.saturating_add(1);
        let mut rx = entry.tx.subscribe();
        // The stream's first `changed()` must return whatever the view holds
        // now, so a late joiner gets the current snapshot instead of waiting
        // for the next change.
        rx.mark_changed();
        drop(entries);
        ViewHandle {
            rx,
            key,
            entries: self.entries.clone(),
        }
    }

    /// How many distinct keys are being watched. Exposed for tests.
    pub fn active(&self) -> usize {
        match self.entries.lock() {
            Ok(entries) => entries.len(),
            Err(poisoned) => poisoned.into_inner().len(),
        }
    }
}

/// One view's whole life: load, then reload on every matching change until the
/// registry aborts the task.
async fn run_view<V: LiveView>(
    view: V,
    db: Db,
    bus: LiveBus,
    tx: watch::Sender<ViewValue<V::State>>,
    reload: Arc<Notify>,
) {
    // Subscribed *before* the first load: a change committed while the snapshot
    // is being read still lands in the queue, so the opening value can be stale
    // for one reload but never permanently.
    let mut events = bus.subscribe();
    let mut cause: Option<Arc<LiveMessage>> = None;
    loop {
        let value = match load_until_ok(&view, &db).await {
            Some(state) => ViewValue::Ready {
                state: Arc::new(state),
                cause: cause.take(),
            },
            None => ViewValue::Missing,
        };
        // A `Missing` view is kept, not dropped: the stream turns it into a
        // NOT_FOUND and ends, and nothing else should resurrect the key.
        let ready = matches!(value, ViewValue::Ready { .. });
        tx.send_replace(value);

        // Wait for something that concerns us.
        loop {
            let received = tokio::select! {
                received = events.recv() => Some(received),
                () = reload.notified() => None,
            };
            match received {
                // A subscriber joined (`ViewRegistry::subscribe`).
                None => cause = None,
                Some(Ok(LiveEvent::Message(message))) => {
                    // An unreadable (`Missing`) view reloads on anything: the
                    // canvas it watches may have been recreated, and the cost is
                    // one query per fleet-wide change while a dashboard sits on
                    // a deleted canvas.
                    let interested = match (&*tx.borrow(), ready) {
                        (ViewValue::Ready { state, .. }, true) => V::matches(state, &message),
                        _ => true,
                    };
                    if !interested {
                        continue;
                    }
                    cause = Some(message);
                }
                // Both mean "you may have missed something": reload without
                // claiming to know why.
                Some(Ok(LiveEvent::Resync) | Err(broadcast::error::RecvError::Lagged(_))) => {
                    cause = None
                }
                Some(Err(broadcast::error::RecvError::Closed)) => return,
            }
            // Collapse a burst into one reload. The latest matching message wins
            // as the cause; a non-matching one is simply dropped.
            while let Ok(event) = events.try_recv() {
                match event {
                    LiveEvent::Message(message) => {
                        let interested = match (&*tx.borrow(), ready) {
                            (ViewValue::Ready { state, .. }, true) => V::matches(state, &message),
                            _ => true,
                        };
                        if interested {
                            cause = Some(message);
                        }
                    }
                    LiveEvent::Resync => cause = None,
                }
            }
            break;
        }
    }
}

/// Loads until the database answers. A failure here is not the watcher's
/// problem: the stream stays open showing its previous value and the read is
/// retried, which is what a dashboard wants across a failover.
async fn load_until_ok<V: LiveView>(view: &V, db: &Db) -> Option<V::State> {
    loop {
        match view.load(db).await {
            Ok(state) => return state,
            Err(error) => {
                tracing::warn!(%error, "reloading a live view failed");
                tokio::time::sleep(RELOAD_BACKOFF).await;
            }
        }
    }
}

// --- the rollouts view -------------------------------------------------------

/// The rollout state of every server of one canvas tree.
pub struct RolloutsView {
    pub canvas: CanvasId,
}

#[derive(Clone)]
pub struct RolloutsLive {
    pub tree: HashSet<String>,
    pub servers: Vec<ServerRolloutLive>,
}

#[derive(Clone)]
pub struct ServerRolloutLive {
    pub server: ServerEntity,
    pub status: RolloutStatus,
}

impl LiveView for RolloutsView {
    type State = RolloutsLive;

    fn matches(state: &Self::State, message: &LiveMessage) -> bool {
        match message {
            LiveMessage::RolloutChanged {
                scope: RolloutScope::Canvas(canvas),
            } => state.tree.contains(canvas),
            LiveMessage::RolloutChanged {
                scope: RolloutScope::Server(server),
            } => state
                .servers
                .iter()
                .any(|s| s.server.id.to_string() == *server),
            // A server added or deleted changes the set of rows, and an edit
            // moves the tree's generation (`derivation_pending`).
            LiveMessage::CanvasChanged { canvas, .. } => state.tree.contains(canvas),
            _ => false,
        }
    }

    async fn load(&self, db: &Db) -> Result<Option<RolloutsLive>, OrchestrationError> {
        let graph = db
            .process(LoadCanvasGraph {
                canvas: self.canvas.clone(),
            })
            .await?;
        // An empty graph is how the query reports a canvas that is not there.
        let Some(root) = graph.canvases.first() else {
            return Ok(None);
        };
        let views = db
            .process(ListServerConfigViewsByCanvases {
                canvases: graph.canvas_ids(),
            })
            .await?;
        let mut by_server: HashMap<ServerId, _> = views
            .into_iter()
            .map(|view| (view.server.clone(), view))
            .collect();
        let mut servers = Vec::with_capacity(graph.servers.len());
        for server in &graph.servers {
            let Some(view) = by_server.remove(&server.id) else {
                // Same call as the deriver makes: a server without its view row
                // is a broken write, not something to fail a dashboard over.
                tracing::warn!(server = %server.name, "server has no config view row; skipping it");
                continue;
            };
            servers.push(ServerRolloutLive {
                status: rollout_status(view, root, server),
                server: server.clone(),
            });
        }
        let tree = graph.canvases.iter().map(|c| c.id.to_string()).collect();
        Ok(Some(RolloutsLive { tree, servers }))
    }
}

// --- the graph view ----------------------------------------------------------

/// The graph of one canvas tree, as `GetGraph` answers it.
pub struct GraphLiveView {
    pub canvas: CanvasId,
    pub config: OrchestrationConfig,
}

#[derive(Clone)]
pub struct GraphLive {
    /// The canvas ids of the tree.
    pub tree: HashSet<String>,
    /// The server ids of the tree.
    pub servers: HashSet<String>,
    pub view: graph::GraphView,
}

impl LiveView for GraphLiveView {
    type State = GraphLive;

    fn matches(state: &Self::State, message: &LiveMessage) -> bool {
        match message {
            LiveMessage::CanvasChanged { canvas, .. } => state.tree.contains(canvas),
            // Not every write that moves the tree's generation announces a
            // canvas change: `ForgetServerApplied` and `TouchCanvases` (ACME
            // issuance, relay-leaf rotation, `InitInternalCa`) only bump the root
            // and leave the rest to a derivation. A snapshot left on the old
            // generation fences every edit off as stale, so the pass that follows
            // every such bump is what reloads the graph.
            LiveMessage::RolloutChanged {
                scope: RolloutScope::Canvas(canvas),
            } => state.tree.contains(canvas),
            // A routine report changes nothing the graph shows; only a status
            // flip moves a server's badge.
            LiveMessage::ServerHealth {
                server,
                status_changed: true,
                ..
            } => state.servers.contains(server),
            _ => false,
        }
    }

    async fn load(&self, db: &Db) -> Result<Option<GraphLive>, OrchestrationError> {
        let rows = db
            .process(LoadCanvasGraph {
                canvas: self.canvas.clone(),
            })
            .await?;
        // An empty graph is how the query reports a canvas that is not there.
        if rows.canvases.is_empty() {
            return Ok(None);
        }
        let tree = rows.canvases.iter().map(|c| c.id.to_string()).collect();
        let servers = rows.servers.iter().map(|s| s.id.to_string()).collect();
        let diagnostics = graph::check_graph(&rows, &self.config);
        Ok(Some(GraphLive {
            tree,
            servers,
            view: graph::GraphView { rows, diagnostics },
        }))
    }
}

// --- the service -------------------------------------------------------------

#[derive(Clone)]
pub struct LiveService {
    pub db: Db,
    pub bus: LiveBus,
    pub rollouts: ViewRegistry<RolloutsView>,
    pub graph: ViewRegistry<GraphLiveView>,
    /// Read by the gRPC layer for the stream keep-alive cadence and by the
    /// graph view for its diagnostics; the views themselves are event-driven
    /// and have no tunables.
    pub config: OrchestrationConfig,
}

impl LiveService {
    pub fn new(db: Db, bus: LiveBus, config: OrchestrationConfig) -> Self {
        Self {
            rollouts: ViewRegistry::new(db.clone(), bus.clone()),
            graph: ViewRegistry::new(db.clone(), bus.clone()),
            db,
            bus,
            config,
        }
    }
}

pub struct WatchRollouts {
    pub actor: Identity,
    pub canvas: CanvasId,
}

impl Processor<WatchRollouts> for LiveService {
    type Output = ViewHandle<RolloutsLive>;
    type Error = OrchestrationError;
    #[tracing::instrument(name = "Service:WatchRollouts", skip_all, err)]
    async fn process(&self, input: WatchRollouts) -> Result<Self::Output, Self::Error> {
        input.actor.ensure(Permission::ViewWorkspace)?;
        Ok(self.rollouts.subscribe(
            input.canvas.to_string(),
            RolloutsView {
                canvas: input.canvas,
            },
        ))
    }
}

pub struct WatchGraph {
    pub actor: Identity,
    pub canvas: CanvasId,
}

impl Processor<WatchGraph> for LiveService {
    type Output = ViewHandle<GraphLive>;
    type Error = OrchestrationError;
    #[tracing::instrument(name = "Service:WatchGraph", skip_all, err)]
    async fn process(&self, input: WatchGraph) -> Result<Self::Output, Self::Error> {
        input.actor.ensure(Permission::ViewWorkspace)?;
        Ok(self.graph.subscribe(
            input.canvas.to_string(),
            GraphLiveView {
                canvas: input.canvas,
                config: self.config.clone(),
            },
        ))
    }
}

/// The opening snapshot of a server-health stream, plus its feed.
pub struct ServerHealthWatch {
    pub server: ServerEntity,
    /// Oldest first.
    pub records: Vec<ServerHealthRecordEntity>,
    pub events: broadcast::Receiver<LiveEvent>,
}

pub struct WatchServerHealth {
    pub actor: Identity,
    pub server: ServerId,
    pub since: DateTime<Utc>,
}

impl Processor<WatchServerHealth> for LiveService {
    type Output = ServerHealthWatch;
    type Error = OrchestrationError;
    #[tracing::instrument(name = "Service:WatchServerHealth", skip_all, err)]
    async fn process(&self, input: WatchServerHealth) -> Result<Self::Output, Self::Error> {
        input.actor.ensure(Permission::ViewWorkspace)?;
        // Subscribe first: a record written between the read and the subscribe
        // would otherwise be in neither, and a log stream cannot afford a hole.
        let events = self.bus.subscribe();
        let server = self
            .db
            .process(FindServerById {
                id: input.server.clone(),
            })
            .await?
            .ok_or(OrchestrationError::NotFound)?;
        let records = self
            .db
            .process(ListServerHealthHistoryRows {
                server: input.server,
                start: input.since,
                end: Utc::now(),
            })
            .await?;
        Ok(ServerHealthWatch {
            server,
            records,
            events,
        })
    }
}

/// The opening snapshot of a pod-health stream, plus its feed.
pub struct PodHealthWatch {
    /// Oldest first; the newest `DEFAULT_POD_HISTORY_LIMIT` of the window.
    pub records: Vec<PodHealthRecordEntity>,
    pub events: broadcast::Receiver<LiveEvent>,
}

pub struct WatchPodHealth {
    pub actor: Identity,
    pub pod: PodId,
    pub since: DateTime<Utc>,
}

impl Processor<WatchPodHealth> for LiveService {
    type Output = PodHealthWatch;
    type Error = OrchestrationError;
    #[tracing::instrument(name = "Service:WatchPodHealth", skip_all, err)]
    async fn process(&self, input: WatchPodHealth) -> Result<Self::Output, Self::Error> {
        input.actor.ensure(Permission::ViewWorkspace)?;
        // Subscribe first: a record written between the read and the subscribe
        // would otherwise be in neither, and a log stream cannot afford a hole.
        let events = self.bus.subscribe();
        self.db
            .process(FindPodById {
                id: input.pod.clone(),
            })
            .await?
            .ok_or(OrchestrationError::NotFound)?;
        let records = self
            .db
            .process(ListPodHealthSince {
                pod: input.pod,
                start: input.since,
                end: Utc::now(),
                limit: Some(DEFAULT_POD_HISTORY_LIMIT),
            })
            .await?;
        Ok(PodHealthWatch { records, events })
    }
}
