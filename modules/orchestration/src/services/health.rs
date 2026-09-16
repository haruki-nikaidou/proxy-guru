//! Health recording: what workers report, and what the master concludes from
//! silence.
//!
//! A worker streams one `HealthReport` per interval. Each report becomes one
//! `server_health_record` plus one `node_health_record` per node the reported
//! pods carry; the server's current status is denormalised on its row. Nothing
//! here changes a config, so no canvas is ever dirtied.

use crate::config::OrchestrationConfig;
use crate::entities::db::health::{
    DeleteHealthRecordsBefore, HealthWrite, InsertServerHealthRecord,
    ListNodeHealthHistory as ListNodeHealthHistoryRows,
    ListServerHealthHistory as ListServerHealthHistoryRows, ListServersForLivenessSweep,
    NewNodeHealthRecord, NodeHealthRecordEntity, NodeHealthStatus, ServerHealthRecordEntity,
    ServerHealthStatus, SetServerHealthStatus,
};
use crate::entities::db::node::NodeId;
use crate::entities::db::server::{
    FindServerById, ReportedAddresses, RevokeSilentWatchSessions, ServerId,
    UpdateReportedAddresses,
};
use crate::entities::db::view::{
    ConfigSnapshot, FindServerConfigView, ForwardingDeps, ServerConfigViewEntity,
};
use crate::events::live::{CanvasChangeKind, LiveMessage};
use crate::services::OrchestrationError;
use crate::services::agent::{AgentIdentity, PodResult};
use crate::services::notify::Notifier;
use auth::services::identity::Identity;
use auth::utils::rbac::Permission;
use base::db::Db;
use chrono::{DateTime, Utc};
use guru_worker_config::{Config, ConfigError, Forwarding};
use kanau::processor::Processor;
use std::collections::HashMap;
use std::time::Duration;

/// Node records returned by `ListNodeHealthHistory` when the caller sets no limit.
pub const DEFAULT_NODE_HISTORY_LIMIT: i64 = 500;

#[derive(Clone)]
pub struct HealthService {
    pub db: Db,
    pub config: OrchestrationConfig,
    pub notifier: Notifier,
}

impl HealthService {
    /// Puts one accepted health write on the live bus: the server record every
    /// server-health stream follows, plus the node rows it carried.
    ///
    /// `status_changed` is what canvas views filter on — a report every interval
    /// per server would otherwise reload every open canvas.
    async fn publish(&self, server: &ServerId, write: &HealthWrite) {
        self.notifier
            .live(LiveMessage::ServerHealth {
                server: server.to_string(),
                canvas: write.canvas.to_string(),
                record: (&write.record).into(),
                status_changed: write.previous_status != write.record.status,
            })
            .await;
        if !write.nodes.is_empty() {
            self.notifier
                .live(LiveMessage::NodeHealth {
                    records: write.nodes.iter().map(Into::into).collect(),
                })
                .await;
        }
    }
}

/// One `[[forwarding]]` of a stored snapshot: the entry as the worker sees it,
/// paired with its place in the graph. `ConfigSnapshot.forwardings` is
/// index-aligned with the rendered TOML, which is what makes the zip sound.
pub(crate) struct SnapshotEntry<'a> {
    pub forwarding: Forwarding,
    pub deps: &'a ForwardingDeps,
}

impl SnapshotEntry<'_> {
    /// The tag is the pod's name at derivation time; it is how a worker names
    /// the entry back to the master.
    pub fn tag(&self) -> &str {
        &self.forwarding.tag
    }
}

/// A stored snapshot re-read: its top-level settings with the forwardings drained
/// into `entries`, so a caller that re-renders a mix keeps everything else as is.
pub(crate) struct ParsedSnapshot<'a> {
    pub config: Config,
    pub entries: Vec<SnapshotEntry<'a>>,
}

/// Parses the TOML once and zips its entries with the snapshot's dependencies.
pub(crate) fn parse_snapshot(snapshot: &ConfigSnapshot) -> Result<ParsedSnapshot<'_>, ConfigError> {
    let mut config = Config::from_toml_str(&snapshot.toml)?;
    let entries = std::mem::take(&mut config.forwardings)
        .into_iter()
        .zip(&snapshot.forwardings)
        .map(|(forwarding, deps)| SnapshotEntry { forwarding, deps })
        .collect();
    Ok(ParsedSnapshot { config, entries })
}

/// A stored snapshot that no longer parses is a broken row, not a broken
/// worker: log it and treat the slot as empty so the report is still recorded.
fn entries_or_empty(snapshot: Option<&ConfigSnapshot>) -> Vec<SnapshotEntry<'_>> {
    let Some(snapshot) = snapshot else {
        return Vec::new();
    };
    match parse_snapshot(snapshot) {
        Ok(parsed) => parsed.entries,
        Err(error) => {
            tracing::error!(revision = snapshot.revision, %error, "stored snapshot is not valid TOML");
            Vec::new()
        }
    }
}

/// The counters and pod outcomes of one worker report.
#[derive(Debug, Clone)]
pub struct HealthReportInput {
    /// The revision the worker says it is running; `0` for a fresh worker.
    pub running_revision: i64,
    /// Deltas since the previous report.
    pub upload_bytes: i64,
    pub download_bytes: i64,
    pub current_connections: i64,
    /// High-water mark since the previous report.
    pub max_connections: i64,
    /// One entry per running `[[forwarding]]`.
    pub pods: Vec<PodResult>,
    /// A refreshed address set, sent only when the worker's discovery changed.
    pub reported: Option<ReportedAddresses>,
}

pub struct RecordHealthReport {
    pub agent: AgentIdentity,
    pub report: HealthReportInput,
}

impl Processor<RecordHealthReport> for HealthService {
    type Output = ();
    type Error = OrchestrationError;
    #[tracing::instrument(name = "Service:RecordHealthReport", skip_all, err)]
    async fn process(&self, input: RecordHealthReport) -> Result<Self::Output, Self::Error> {
        let view = self
            .db
            .process(FindServerConfigView {
                server: input.agent.server.clone(),
            })
            .await?
            .ok_or(OrchestrationError::NotFound)?;
        let now = Utc::now();
        let report = input.report;
        let server_id = input.agent.server.clone();
        // The generation is checked inside the write itself, so a report from a
        // session that was superseded between two statements lands nowhere.
        let write = self
            .db
            .process(InsertServerHealthRecord {
                server: input.agent.server,
                generation: input.agent.generation,
                status: server_status(
                    &view,
                    report.running_revision,
                    now,
                    self.config.degraded_grace(),
                ),
                report_time: now,
                upload_bytes: report.upload_bytes,
                download_bytes: report.download_bytes,
                current_connections: report.current_connections,
                max_connections: report.max_connections,
                nodes: node_records(&view, &report.pods, now),
            })
            .await?;
        let Some(write) = write else {
            return Err(OrchestrationError::PermissionDenied);
        };
        self.publish(&server_id, &write).await;
        // A changed address set re-derives every destination that dials this
        // server. The write is fenced on the generation and touches the canvas
        // only when something actually changed; the stale-canvas sweep picks the
        // touch up.
        if let Some(reported) = report.reported {
            let server = self
                .db
                .process(FindServerById {
                    id: server_id.clone(),
                })
                .await?
                .ok_or(OrchestrationError::NotFound)?;
            let unchanged = server
                .reported_addresses
                .as_ref()
                .is_some_and(|stored| stored.same_addresses(&reported));
            if !unchanged {
                self.db
                    .process(UpdateReportedAddresses {
                        server: server_id.clone(),
                        generation: input.agent.generation,
                        reported,
                    })
                    .await?;
                self.notifier
                    .canvas_changed(
                        &server.canvas,
                        CanvasChangeKind::ServerIpChanged,
                        vec![server_id.to_string()],
                    )
                    .await;
            }
        }
        Ok(())
    }
}

/// What a reachable server's report means, given what it should be running.
/// A worker running something other than what the master recorded as applied
/// (a fresh worker still on `0` included) is out of sync, hence `Degraded`.
fn server_status(
    view: &ServerConfigViewEntity,
    running_revision: i64,
    now: DateTime<Utc>,
    grace: Duration,
) -> ServerHealthStatus {
    if view.apply_error.is_some() || !view.failed_pods.is_empty() {
        return ServerHealthStatus::Degraded;
    }
    let applied = view
        .applied
        .as_ref()
        .map_or(0, |snapshot| snapshot.revision);
    if running_revision != applied {
        return ServerHealthStatus::Degraded;
    }
    let lagging = view.desired.as_ref().is_some_and(|desired| {
        desired.revision > applied
            && now
                .signed_duration_since(desired.created_at)
                .to_std()
                .is_ok_and(|lag| lag > grace)
    });
    if lagging {
        ServerHealthStatus::Degraded
    } else {
        ServerHealthStatus::Online
    }
}

/// One verdict per node, the worst of every pod's that runs through it
/// (`Failed` > `Deploying` > `Ready`), with the message of the winning status.
/// Shared by the report path and the ack path so a node shared by several pods
/// gets exactly one row per event.
#[derive(Default)]
pub(crate) struct NodeVerdicts<'a> {
    verdicts: HashMap<&'a NodeId, (NodeHealthStatus, &'a str)>,
}

impl<'a> NodeVerdicts<'a> {
    /// Applies `status` to the pod and every node its forwarding runs through.
    pub fn record(&mut self, deps: &'a ForwardingDeps, status: NodeHealthStatus, message: &'a str) {
        for node in std::iter::once(&deps.pod).chain(&deps.nodes) {
            self.verdicts
                .entry(node)
                .and_modify(|current| {
                    if severity(status) > severity(current.0) {
                        *current = (status, message);
                    }
                })
                .or_insert((status, message));
        }
    }

    pub fn into_records(self, now: DateTime<Utc>) -> Vec<NewNodeHealthRecord> {
        self.verdicts
            .into_iter()
            .map(|(node, (status, message))| NewNodeHealthRecord {
                node: node.clone(),
                status,
                message: message.to_string(),
                report_time: now,
            })
            .collect()
    }
}

fn severity(status: NodeHealthStatus) -> u8 {
    match status {
        NodeHealthStatus::Ready => 0,
        NodeHealthStatus::Deploying => 1,
        NodeHealthStatus::Failed => 2,
    }
}

/// The node records one report produces: for every reported pod, the pod itself
/// and every node its forwarding was derived through.
///
/// A tag resolves through `applied` first — that is what the worker runs — and
/// `desired` otherwise, for the moment between a worker applying a revision and
/// the master taking its ack. A tag in neither is not ours to judge.
fn node_records(
    view: &ServerConfigViewEntity,
    pods: &[PodResult],
    now: DateTime<Utc>,
) -> Vec<NewNodeHealthRecord> {
    let applied = entries_or_empty(view.applied.as_ref());
    let desired = entries_or_empty(view.desired.as_ref());
    let mut verdicts = NodeVerdicts::default();
    for pod in pods {
        let in_applied = applied.iter().find(|entry| entry.tag() == pod.tag);
        let in_desired = desired.iter().find(|entry| entry.tag() == pod.tag);
        let Some(entry) = in_applied.or(in_desired) else {
            continue;
        };
        let failure = pod.error.as_deref().or_else(|| {
            view.failed_pods
                .iter()
                .find(|failed| failed.pod.0 == entry.deps.pod.0)
                .map(|failed| failed.error.as_str())
        });
        let (status, message) = match failure {
            Some(error) => (NodeHealthStatus::Failed, error),
            None => {
                let pending = match (in_desired, in_applied) {
                    (Some(desired), Some(applied)) => desired.forwarding != applied.forwarding,
                    (Some(_), None) => true,
                    (None, _) => false,
                };
                let status = if pending {
                    NodeHealthStatus::Deploying
                } else {
                    NodeHealthStatus::Ready
                };
                (status, "")
            }
        };
        verdicts.record(entry.deps, status, message);
    }
    verdicts.into_records(now)
}

/// The worker is gone: its report stream closed, or it fell silent past the
/// threshold. `generation` set means "only if the server still belongs to this
/// session": a stream that lost its server to a re-registration must not mark
/// the successor's server offline. Already `Offline` is a no-op.
///
/// Going offline also releases the watch session (lease dropped, epoch bumped):
/// the worker's config stream ends and its next registration is accepted at
/// once, instead of being refused for as long as a stream the master cannot tell
/// is dead keeps renewing the lease.
pub struct MarkServerOffline {
    pub server: ServerId,
    pub generation: Option<i64>,
}

impl Processor<MarkServerOffline> for HealthService {
    /// Whether the status changed.
    type Output = bool;
    type Error = OrchestrationError;
    #[tracing::instrument(name = "Service:MarkServerOffline", skip_all, err)]
    async fn process(&self, input: MarkServerOffline) -> Result<Self::Output, Self::Error> {
        let write = self
            .db
            .process(SetServerHealthStatus {
                server: input.server.clone(),
                generation: input.generation,
                status: ServerHealthStatus::Offline,
                now: Utc::now(),
            })
            .await?;
        if let Some(write) = &write {
            self.publish(&input.server, write).await;
        }
        Ok(write.is_some())
    }
}

/// One liveness sweep: every server that has not reported within
/// `health_offline_after()` (or never did) goes `Offline`, and hands its watch
/// session back (see [`MarkServerOffline`]).
///
/// A server that is already `Offline` has nothing to flip, but its watch session
/// may still be held by a registration that never reported — one made after the
/// flip, whose connection died before its first report. That session is revoked
/// once it is older than the same threshold (see [`RevokeSilentWatchSessions`]).
pub struct SweepLiveness {
    pub now: DateTime<Utc>,
}

/// What one liveness sweep changed.
#[derive(Debug, Default)]
pub struct SweepOutcome {
    /// Servers that went `Offline` for lack of reports.
    pub flipped: Vec<ServerId>,
    /// Servers already `Offline` whose watch session was revoked.
    pub revoked: Vec<ServerId>,
}

impl Processor<SweepLiveness> for HealthService {
    type Output = SweepOutcome;
    type Error = OrchestrationError;
    #[tracing::instrument(name = "Service:SweepLiveness", skip_all, err)]
    async fn process(&self, input: SweepLiveness) -> Result<Self::Output, Self::Error> {
        let threshold = self.config.health_offline_after();
        let mut flipped = Vec::new();
        for server in self.db.process(ListServersForLivenessSweep).await? {
            let silent = server.last_health_report_at.is_none_or(|last| {
                input
                    .now
                    .signed_duration_since(last)
                    .to_std()
                    .is_ok_and(|silence| silence > threshold)
            });
            if !silent {
                continue;
            }
            let write = self
                .db
                .process(SetServerHealthStatus {
                    server: server.id.clone(),
                    generation: None,
                    status: ServerHealthStatus::Offline,
                    now: input.now,
                })
                .await?;
            // `None` means the write changed nothing — the server went offline
            // between the list and the update, or lost its session. Either way it
            // is not this sweep's flip, and the caller's contract is the flips.
            if let Some(write) = &write {
                self.publish(&server.id, write).await;
                flipped.push(server.id);
            }
        }
        // A threshold too large to subtract leaves no registration old enough.
        let registered_before = chrono::TimeDelta::from_std(threshold)
            .ok()
            .and_then(|threshold| input.now.checked_sub_signed(threshold))
            .unwrap_or(DateTime::<Utc>::MIN_UTC);
        let revoked = self
            .db
            .process(RevokeSilentWatchSessions {
                now: input.now,
                registered_before,
            })
            .await?;
        Ok(SweepOutcome { flipped, revoked })
    }
}

/// One retention pass: drops raw records older than their TTL.
pub struct TrimHealthHistory {
    pub now: DateTime<Utc>,
}

impl Processor<TrimHealthHistory> for HealthService {
    type Output = ();
    type Error = OrchestrationError;
    #[tracing::instrument(name = "Service:TrimHealthHistory", skip_all, err)]
    async fn process(&self, input: TrimHealthHistory) -> Result<Self::Output, Self::Error> {
        self.db
            .process(DeleteHealthRecordsBefore {
                server_records_before: input
                    .now
                    .checked_sub_signed(
                        chrono::Duration::from_std(self.config.server_health_ttl())
                            .unwrap_or(chrono::TimeDelta::MAX),
                    )
                    .unwrap_or(DateTime::<Utc>::MIN_UTC),
                node_records_before: input
                    .now
                    .checked_sub_signed(
                        chrono::Duration::from_std(self.config.node_health_ttl())
                            .unwrap_or(chrono::TimeDelta::MAX),
                    )
                    .unwrap_or(DateTime::<Utc>::MIN_UTC),
            })
            .await?;
        Ok(())
    }
}

pub struct ListServerHealthHistory {
    pub actor: Identity,
    pub server: ServerId,
    pub start: DateTime<Utc>,
    pub end: DateTime<Utc>,
}

impl Processor<ListServerHealthHistory> for HealthService {
    type Output = Vec<ServerHealthRecordEntity>;
    type Error = OrchestrationError;
    #[tracing::instrument(name = "Service:ListServerHealthHistory", skip_all, err)]
    async fn process(&self, input: ListServerHealthHistory) -> Result<Self::Output, Self::Error> {
        input.actor.ensure(Permission::ViewWorkspace)?;
        Ok(self
            .db
            .process(ListServerHealthHistoryRows {
                server: input.server,
                start: input.start,
                end: input.end,
            })
            .await?)
    }
}

pub struct ListNodeHealthHistory {
    pub actor: Identity,
    pub node: NodeId,
    pub start: DateTime<Utc>,
    pub end: DateTime<Utc>,
    pub limit: i64,
}

impl Processor<ListNodeHealthHistory> for HealthService {
    type Output = Vec<NodeHealthRecordEntity>;
    type Error = OrchestrationError;
    #[tracing::instrument(name = "Service:ListNodeHealthHistory", skip_all, err)]
    async fn process(&self, input: ListNodeHealthHistory) -> Result<Self::Output, Self::Error> {
        input.actor.ensure(Permission::ViewWorkspace)?;
        Ok(self
            .db
            .process(ListNodeHealthHistoryRows {
                node: input.node,
                start: input.start,
                end: input.end,
                limit: input.limit,
            })
            .await?)
    }
}
