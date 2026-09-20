//! Live dashboard events: the Redis pub/sub contract between master replicas.
//!
//! These are *not* AMQP messages. AMQP carries work (a canvas needs deriving, a
//! periodic job is due) and is therefore queued and acked; the messages here
//! carry *notice that something changed* to whichever replicas happen to hold an
//! open `Watch*` stream. A replica with no watchers must drop them, which is
//! exactly what pub/sub does and what a queue does not.
//!
//! The payload is a hint in the same sense [`crate::events::CanvasDirty`] is: a
//! watcher reloads from the database when it sees one, so a lost message costs a
//! stale dashboard until the next change, and the subscriber's reconnect
//! broadcasts [`crate::hooks::live::LiveEvent::Resync`] so nothing stays stale
//! across a broker blip.
//!
//! Encoding matches the AMQP events (rkyv via `kanau`), so the two halves of the
//! fleet never disagree about a wire format.

use crate::entities::db::certificate::CertificateStatus;
use crate::entities::db::health::{
    PodHealthRecordEntity, PodHealthStatus, ServerHealthRecordEntity, ServerHealthStatus,
};
use kanau::{RkyvMessageDe, RkyvMessageSer};
use time::OffsetDateTime;

/// The single Redis pub/sub channel every replica subscribes to.
///
/// One channel, not one per canvas: a replica needs a subscription per *watched
/// key* to filter server-side, and re-subscribing on every opened stream buys
/// nothing over matching in process — the fleet's whole change rate is a handful
/// of messages per second.
pub const LIVE_CHANNEL: &str = "guru:orchestration:live";

/// **Live event**
///
/// Published by: every mutation, the derivation hook, the health paths and the
/// ACME service, through [`crate::services::notify::Notifier`].
/// Consumed by: [`crate::hooks::live::run_redis_subscriber`] on every replica,
/// which hands it to [`crate::hooks::live::LiveBus`] and from there to the
/// shared views and streams in [`crate::services::live`].
/// Route: Redis channel [`LIVE_CHANNEL`].
#[derive(
    Debug, Clone, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, RkyvMessageSer, RkyvMessageDe,
)]
pub enum LiveMessage {
    /// Anything the dashboard renders for a canvas changed.
    ///
    /// Published by: every canvas, server and graph mutation in `services`, plus
    /// `RecordHealthReport` and `RegisterWorker` when a server's reported
    /// addresses move.
    /// Consumed by: [`crate::services::live::RolloutsView`] (the set of servers
    /// and the tree's generation may have moved with it) and
    /// [`crate::services::live::GraphLiveView`].
    ///
    /// `canvas` is the canvas the edit happened in (not the root); views match it
    /// against their own tree, so a subcanvas edit refreshes its ancestors.
    CanvasChanged {
        canvas: String,
        kind: CanvasChangeKind,
        /// The records the change is about, as text.
        ids: Vec<String>,
    },
    /// A server's rollout state (`desired`/`in_flight`/`applied`/errors) or a
    /// tree's derivation moved.
    ///
    /// Published by: [`crate::hooks::derive::CanvasDeriver`] after a committed
    /// pass (scoped to the tree), `AckConfig`, `RegisterWorker`,
    /// `ForgetServerApplied`, and the worker stream's conditional take (scoped to
    /// the one server).
    /// Consumed by: [`crate::services::live::RolloutsView`] and, when scoped to a
    /// tree, [`crate::services::live::GraphLiveView`] (a derivation follows every
    /// bump of the tree's generation, including the ones that announce no
    /// `CanvasChanged`).
    RolloutChanged { scope: RolloutScope },
    /// One accepted `server_health_record` row: a report, an ack verdict, or the
    /// master flipping a silent server offline.
    ///
    /// Published by: `HealthService` (`RecordHealthReport`, `MarkServerOffline`,
    /// `SweepLiveness`) and `AckConfig`.
    /// Consumed by: the `WatchServerHealth` stream (every row) and
    /// [`crate::services::live::GraphLiveView`] (only when `status_changed`).
    ServerHealth {
        server: String,
        canvas: String,
        record: ServerHealthLive,
        /// Whether the server's denormalised `health_status` changed with it.
        status_changed: bool,
    },
    /// A batch of `pod_health_record` rows written in one statement.
    ///
    /// Published by: the derivation hook (`Deploying` the moment a revision is
    /// published), `AckConfig` (the settled verdicts) and `RecordHealthReport`
    /// (the rows that travelled inside the report's transaction).
    /// Consumed by: the `WatchPodHealth` stream (the records of its pod).
    PodHealth { records: Vec<PodHealthLive> },
    /// A `certificate` row changed status or version.
    ///
    /// Published by: `AcmeService` (`IssueCertificate` on success and on either
    /// failure path, `RetryCertificate`, `DeleteCertificate`).
    /// Consumed by: nobody yet — published so the table is complete and a future
    /// certificate stream needs no publisher changes.
    CertificateChanged {
        certificate: String,
        status: CertificateStatus,
        not_after_unix_secs: Option<i64>,
        error: Option<String>,
    },
}

/// Which mutation produced a [`LiveMessage::CanvasChanged`]. The dashboard uses
/// it to decide what to animate; a watcher's reload does not depend on it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
pub enum CanvasChangeKind {
    CanvasCreated,
    CanvasUpdated,
    CanvasDeleted,
    ServerCreated,
    ServerUpdated,
    ServerMoved,
    ServerDeleted,
    ServerIpChanged,
    /// Pods, exits, edges or groups changed.
    GraphChanged,
}

/// What a rollout change is scoped to: a whole tree (a derivation pass) or one
/// server (a take, an ack, a forget).
#[derive(Debug, Clone, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
pub enum RolloutScope {
    Canvas(String),
    Server(String),
}

/// A `server_health_record` row as it travels on the bus. Timestamps are
/// microseconds since the epoch: `time` types are not `rkyv`-encodable, and
/// the record's own resolution is a report interval.
#[derive(Debug, Clone, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
pub struct ServerHealthLive {
    pub id: String,
    pub status: ServerHealthStatus,
    pub report_time_unix_micros: i64,
    pub upload_bytes: i64,
    pub download_bytes: i64,
    pub current_connections: i64,
    pub max_connections: i64,
}

/// A `pod_health_record` row as it travels on the bus.
#[derive(Debug, Clone, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
pub struct PodHealthLive {
    pub id: String,
    pub pod: String,
    pub status: PodHealthStatus,
    pub message: String,
    pub report_time_unix_micros: i64,
}

impl From<&ServerHealthRecordEntity> for ServerHealthLive {
    fn from(record: &ServerHealthRecordEntity) -> Self {
        Self {
            id: record.id.to_string(),
            status: record.status,
            report_time_unix_micros: (record.report_time.unix_timestamp_nanos() / 1_000) as i64,
            upload_bytes: record.upload_bytes,
            download_bytes: record.download_bytes,
            current_connections: record.current_connections,
            max_connections: record.max_connections,
        }
    }
}

impl From<&PodHealthRecordEntity> for PodHealthLive {
    fn from(record: &PodHealthRecordEntity) -> Self {
        Self {
            id: record.id.to_string(),
            pod: record.pod.to_string(),
            status: record.status,
            message: record.message.clone(),
            report_time_unix_micros: (record.report_time.unix_timestamp_nanos() / 1_000) as i64,
        }
    }
}

/// A bus timestamp back as an [`OffsetDateTime`]. Out of range is impossible for
/// a value this module produced, so it collapses to the epoch rather than
/// erroring: the only use is an ordering comparison against the last record a
/// stream sent, and the epoch loses it.
pub fn live_time(unix_micros: i64) -> OffsetDateTime {
    OffsetDateTime::from_unix_timestamp_nanos(i128::from(unix_micros).saturating_mul(1_000))
        .unwrap_or(OffsetDateTime::UNIX_EPOCH)
}
