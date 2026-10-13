//! Event definitions: the edit hint and the health facts, published to Iggy.
//!
//! Both live in the shared stream (`base::events::STREAM`), each on its own
//! topic named by its partition key's `SUPER_PARTITION_NAME`; [`TOPICS`] lists
//! them for `base::events::ensure_topology`. Periodic work is not an event any
//! more: it is the `CronJob`s in [`crate::hooks`], run by `guru-master --mode
//! cron`.
//!
//! The two Redis pub/sub contracts live beside them: [`live`] (dashboard change
//! notices) and [`shell`] (the remote-shell relay).

pub mod live;
pub mod shell;

use crate::entities::db::health::{
    HealthWrite, PodHealthStatus, PodHealthWrite, ServerHealthStatus,
};
use kanau::{RkyvMessageDe, RkyvMessageSer};
use wakuwaku::iggy::{
    Event, EventAtomicOrdering, EventBody, EventTypeTag, Idempotent, IggySuperPartitionName,
    NonAssociative, PartitionKey,
};

/// The main topics this module publishes to, created at startup.
pub const TOPICS: [&str; 2] = [
    CanvasDirtyKey::SUPER_PARTITION_NAME.0,
    HealthChangedKey::SUPER_PARTITION_NAME.0,
];

/// **Public event**
///
/// A canvas has edits that have not been derived yet.
///
/// Published by: every config-affecting mutation (dashboard), plus `Register`,
/// `AckConfig` and `ForgetServerApplied` (workers and dashboard).
/// Consumed by: [`crate::hooks::derive::CanvasDirtyHandler`] (`--mode consumer`).
/// Topic: `orchestration_canvas_dirty` (keyed by canvas), tag 1.
///
/// The payload is only a hint: the canvas generation counter, not this message, is
/// what decides whether a derivation is needed, so a lost message costs latency
/// (until the `derive_stale_canvases` cron job) and never correctness. For the
/// same reason it is [`Idempotent`]: a run of hints for one canvas collapses to
/// one derivation.
#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, RkyvMessageSer, RkyvMessageDe,
)]
pub struct CanvasDirty {
    /// The canvas id, as text.
    pub canvas: String,
}

/// **Public event**
///
/// The health facts one write settled: a server whose status flipped, and the
/// pod rows a write produced.
///
/// Published by: [`crate::services::health::HealthService`] (report, stream
/// close and liveness sweep), `AckConfig`
/// ([`crate::services::agent::AgentService`]) and
/// [`crate::hooks::derive::CanvasDeriver`].
/// Consumed by: `notify`'s fan-out hook (`--mode consumer`).
/// Topic: `orchestration_health_changed` (keyed by the first fact's canvas), tag 1.
///
/// The facts carry their own labels (server, pod and canvas names) so a
/// consumer can phrase a notification without a database of its own. A lost
/// message costs one notification and never state: what the fleet runs is
/// decided by the rows the publisher already committed.
#[derive(
    Debug, Clone, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, RkyvMessageSer, RkyvMessageDe,
)]
pub struct HealthChanged {
    pub facts: Vec<HealthFact>,
}

/// One health fact, as the publisher observed it.
#[derive(Debug, Clone, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
pub enum HealthFact {
    /// A server's status differs from the one it held before the write.
    Server {
        server: String,
        server_name: String,
        canvas: String,
        canvas_name: String,
        status: ServerHealthStatus,
        at_unix_micros: i64,
    },
    /// A pod row was written; whether it is a change is the consumer's call.
    Pod {
        pod: String,
        pod_name: String,
        canvas: String,
        canvas_name: String,
        status: PodHealthStatus,
        message: String,
        at_unix_micros: i64,
    },
}

impl HealthFact {
    /// The canvas the fact happened in, as text.
    pub fn canvas(&self) -> &str {
        match self {
            HealthFact::Server { canvas, .. } | HealthFact::Pod { canvas, .. } => canvas,
        }
    }

    /// The fact a server write settled, or `None` when the write only recorded
    /// the status the server already held: a routine report is not news.
    pub fn server(write: &HealthWrite) -> Option<Self> {
        if write.previous_status == write.record.status {
            return None;
        }
        Some(Self::Server {
            server: write.record.server.to_string(),
            server_name: write.server_name.clone(),
            canvas: write.canvas.to_string(),
            canvas_name: write.canvas_name.clone(),
            status: write.record.status,
            at_unix_micros: (write.record.report_time.unix_timestamp_nanos() / 1_000) as i64,
        })
    }

    /// The fact one written pod row carries. Whether it is a change is the
    /// consumer's call: a report writes one row per pod every interval.
    pub fn pod(write: &PodHealthWrite) -> Self {
        Self::Pod {
            pod: write.record.pod.to_string(),
            pod_name: write.pod_name.clone(),
            canvas: write.canvas.to_string(),
            canvas_name: write.canvas_name.clone(),
            status: write.record.status,
            message: write.record.message.clone(),
            at_unix_micros: (write.record.report_time.unix_timestamp_nanos() / 1_000) as i64,
        }
    }
}

/// Every fact one server write settled: the status flip, if there was one, then
/// the pod rows the write carried.
pub fn health_facts(write: &HealthWrite) -> Vec<HealthFact> {
    HealthFact::server(write)
        .into_iter()
        .chain(write.pods.iter().map(HealthFact::pod))
        .collect()
}

/// Partition key of [`CanvasDirty`]: the canvas id. Hints for one canvas stay in
/// order on one partition; the deriver fences on generations, so the order is
/// not load-bearing.
#[derive(Hash)]
pub struct CanvasDirtyKey(pub String);

impl PartitionKey for CanvasDirtyKey {
    type PartitionMerge = std::hash::DefaultHasher;
    const SUPER_PARTITION_NAME: IggySuperPartitionName =
        IggySuperPartitionName("orchestration_canvas_dirty");
}

impl EventBody for CanvasDirty {}

impl Event for CanvasDirty {
    const TYPE_TAG: EventTypeTag = EventTypeTag::new(1);
    type Key = CanvasDirtyKey;
    type Algebra = Idempotent;

    fn key(&self) -> CanvasDirtyKey {
        CanvasDirtyKey(self.canvas.clone())
    }

    fn atomic_ordering(&self) -> EventAtomicOrdering {
        EventAtomicOrdering::Relaxed
    }
}

/// Partition key of [`HealthChanged`]: the canvas of its first fact (one write
/// settles the facts of one server, so of one canvas).
#[derive(Hash)]
pub struct HealthChangedKey(pub String);

impl PartitionKey for HealthChangedKey {
    type PartitionMerge = std::hash::DefaultHasher;
    const SUPER_PARTITION_NAME: IggySuperPartitionName =
        IggySuperPartitionName("orchestration_health_changed");
}

impl EventBody for HealthChanged {}

impl Event for HealthChanged {
    const TYPE_TAG: EventTypeTag = EventTypeTag::new(1);
    type Key = HealthChangedKey;
    type Algebra = NonAssociative;

    /// `""` for an empty event, which the [`Notifier`](crate::services::notify::Notifier)
    /// never publishes.
    fn key(&self) -> HealthChangedKey {
        HealthChangedKey(
            self.facts
                .first()
                .map(HealthFact::canvas)
                .unwrap_or_default()
                .to_owned(),
        )
    }

    fn atomic_ordering(&self) -> EventAtomicOrdering {
        EventAtomicOrdering::Relaxed
    }
}
