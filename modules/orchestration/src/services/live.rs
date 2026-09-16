//! Live streams: shared per-key views, and the four watch operations behind the
//! `Watch*` RPCs.
//!
//! # Two shapes, on purpose
//!
//! A canvas and a rollout tree are *state*: every watcher of the same key wants
//! the same picture, and a watcher that fell behind wants the newest picture, not
//! the intermediate ones. Those are [`ViewRegistry`] views — one reload per
//! change per key, shared by reference count, published through
//! [`tokio::sync::watch`] so a slow consumer collapses a burst into one value and
//! can never make the server buffer.
//!
//! Server and node health are *logs*: the records are the content, and skipping
//! one loses information. Those are forwarded straight from the bus, and a gap
//! (a lag, a bus reconnect) is closed by re-reading the database from the last
//! record the stream actually sent.
//!
//! Either way the client never has to re-request anything, which is why there is
//! no `Resync` on the wire.

use crate::config::OrchestrationConfig;
use crate::entities::db::canvas::{CanvasContents, CanvasId, CanvasTree, LoadCanvasTree};
use crate::entities::db::health::{
    ListNodeHealthHistory as ListNodeHealthHistoryRows,
    ListServerHealthHistory as ListServerHealthHistoryRows, NodeHealthRecordEntity,
    ServerHealthRecordEntity,
};
use crate::entities::db::node::{FindNodeWithPorts, NodeId};
use crate::entities::db::server::{FindServerById, ServerEntity, ServerId};
use crate::entities::db::topology::{LoadCanvasContents, LoadCanvasTopology};
use crate::entities::db::view::ListServerConfigViewsByCanvases;
use crate::events::live::{LiveMessage, RolloutScope};
use crate::hooks::live::{LiveBus, LiveEvent};
use crate::services::OrchestrationError;
use crate::services::health::DEFAULT_NODE_HISTORY_LIMIT;
use crate::services::rollout::{RolloutStatus, rollout_status};
use auth::services::identity::Identity;
use auth::utils::rbac::Permission;
use base::db::Db;
use chrono::{DateTime, Utc};
use kanau::processor::Processor;
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};
use tokio::sync::{broadcast, watch};
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
        let entry = entries.entry(key.clone()).or_insert_with(|| {
            let (tx, _) = watch::channel(ViewValue::Loading);
            let task = tokio::spawn(run_view(
                view,
                self.db.clone(),
                self.bus.clone(),
                tx.clone(),
            ));
            ViewEntry {
                tx,
                subscribers: 0,
                task: task.abort_handle(),
            }
        });
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
            match events.recv().await {
                Ok(LiveEvent::Message(message)) => {
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
                Ok(LiveEvent::Resync) | Err(broadcast::error::RecvError::Lagged(_)) => cause = None,
                Err(broadcast::error::RecvError::Closed) => return,
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

// --- the canvas view ---------------------------------------------------------

/// Everything the dashboard renders for one canvas, refreshed as a whole.
pub struct CanvasView {
    pub canvas: CanvasId,
}

#[derive(Clone)]
pub struct CanvasLive {
    pub contents: CanvasContents,
    /// Every canvas of the tree this canvas belongs to, as record keys. An edit
    /// in a subcanvas changes what the import node shows here, so the whole tree
    /// is the match set — not just this canvas.
    pub tree: HashSet<String>,
}

impl LiveView for CanvasView {
    type State = CanvasLive;

    fn matches(state: &Self::State, message: &LiveMessage) -> bool {
        match message {
            LiveMessage::CanvasChanged { canvas, .. } => state.tree.contains(canvas),
            // The `Server` message carries `health_status` and `last_seen_at`,
            // so a flip is a rendered change — a routine report is not.
            LiveMessage::ServerHealth {
                canvas,
                status_changed: true,
                ..
            } => state.tree.contains(canvas),
            _ => false,
        }
    }

    async fn load(&self, db: &Db) -> Result<Option<CanvasLive>, OrchestrationError> {
        let Some(contents) = db
            .process(LoadCanvasContents {
                canvas: self.canvas.clone(),
            })
            .await?
        else {
            return Ok(None);
        };
        let tree = db
            .process(LoadCanvasTree {
                canvas: self.canvas.clone(),
            })
            .await?
            .map(|tree| {
                let mut keys = HashSet::new();
                collect_tree(&tree, &mut keys);
                keys
            })
            .unwrap_or_default();
        Ok(Some(CanvasLive { contents, tree }))
    }
}

fn collect_tree(tree: &CanvasTree, out: &mut HashSet<String>) {
    out.insert(tree.canvas.id.to_string());
    for child in &tree.children {
        collect_tree(child, out);
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
        let topology = db
            .process(LoadCanvasTopology {
                canvas: self.canvas.clone(),
            })
            .await?;
        // An empty topology is how the query reports a canvas that is not there.
        let Some(root) = topology.canvases.first() else {
            return Ok(None);
        };
        let views = db
            .process(ListServerConfigViewsByCanvases {
                canvases: topology.canvas_ids(),
            })
            .await?;
        let mut by_server: HashMap<ServerId, _> = views
            .into_iter()
            .map(|view| (view.server.clone(), view))
            .collect();
        let mut servers = Vec::with_capacity(topology.servers.len());
        for server in &topology.servers {
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
        let tree = topology.canvases.iter().map(|c| c.id.to_string()).collect();
        Ok(Some(RolloutsLive { tree, servers }))
    }
}

// --- the service -------------------------------------------------------------

#[derive(Clone)]
pub struct LiveService {
    pub db: Db,
    pub bus: LiveBus,
    pub canvases: ViewRegistry<CanvasView>,
    pub rollouts: ViewRegistry<RolloutsView>,
    /// Read by the gRPC layer for the stream keep-alive cadence; the views
    /// themselves are event-driven and have no tunables.
    pub config: OrchestrationConfig,
}

impl LiveService {
    pub fn new(db: Db, bus: LiveBus, config: OrchestrationConfig) -> Self {
        Self {
            canvases: ViewRegistry::new(db.clone(), bus.clone()),
            rollouts: ViewRegistry::new(db.clone(), bus.clone()),
            db,
            bus,
            config,
        }
    }
}

pub struct WatchCanvas {
    pub actor: Identity,
    pub canvas: CanvasId,
}

impl Processor<WatchCanvas> for LiveService {
    type Output = ViewHandle<CanvasLive>;
    type Error = OrchestrationError;
    #[tracing::instrument(name = "Service:WatchCanvas", skip_all, err)]
    async fn process(&self, input: WatchCanvas) -> Result<Self::Output, Self::Error> {
        input.actor.ensure(Permission::ViewWorkspace)?;
        Ok(self.canvases.subscribe(
            input.canvas.to_string(),
            CanvasView {
                canvas: input.canvas,
            },
        ))
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

/// The opening snapshot of a node-health stream, plus its feed.
pub struct NodeHealthWatch {
    /// Newest first, as the history RPC returns them.
    pub records: Vec<NodeHealthRecordEntity>,
    pub events: broadcast::Receiver<LiveEvent>,
}

pub struct WatchNodeHealth {
    pub actor: Identity,
    pub node: NodeId,
    pub limit: i64,
}

impl Processor<WatchNodeHealth> for LiveService {
    type Output = NodeHealthWatch;
    type Error = OrchestrationError;
    #[tracing::instrument(name = "Service:WatchNodeHealth", skip_all, err)]
    async fn process(&self, input: WatchNodeHealth) -> Result<Self::Output, Self::Error> {
        input.actor.ensure(Permission::ViewWorkspace)?;
        let events = self.bus.subscribe();
        self.db
            .process(FindNodeWithPorts {
                id: input.node.clone(),
            })
            .await?
            .ok_or(OrchestrationError::NotFound)?;
        let records = self
            .db
            .process(ListNodeHealthHistoryRows {
                node: input.node,
                start: DateTime::UNIX_EPOCH,
                end: Utc::now(),
                limit: if input.limit > 0 {
                    input.limit
                } else {
                    DEFAULT_NODE_HISTORY_LIMIT
                },
            })
            .await?;
        Ok(NodeHealthWatch { records, events })
    }
}
