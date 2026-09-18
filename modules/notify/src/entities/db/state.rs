//! What the fan-out last announced about one subject.
//!
//! A worker reports its pods every interval, so the same `Ready` row is written
//! over and over; the master's liveness sweep can report `Offline` for a server
//! that was already offline. Without a memory of the last announcement, every
//! one of those would be a notification.
//!
//! Each query below records the new status and answers with the one it replaced:
//! `None` means this subject was never seen, which is deliberately *not* news —
//! otherwise deploying this module would notify every recipient about every
//! server and pod in the fleet at once. A subject whose row is gone (deleted
//! between the write and the fan-out) records nothing and reads as a first
//! sighting: there is nobody left to announce.

use base::db::{Db, Error};
use chrono::{DateTime, Utc};
use kanau::processor::Processor;
use orchestration::entities::db::health::{PodHealthStatus, ServerHealthStatus};
use orchestration::entities::db::pod::PodId;
use orchestration::entities::db::server::ServerId;

/// Records one server's status and answers with the previous one.
pub struct ObserveServerStatus {
    pub server: ServerId,
    pub status: ServerHealthStatus,
    pub at: DateTime<Utc>,
}

impl Processor<ObserveServerStatus> for Db {
    /// The status last announced, or `None` for a first sighting.
    type Output = Option<ServerHealthStatus>;
    type Error = Error;
    #[tracing::instrument(name = "Query:ObserveServerStatus", skip_all, err)]
    async fn process(&self, input: ObserveServerStatus) -> Result<Self::Output, Self::Error> {
        Ok(sqlx::query_file_scalar!(
            "sql/observe_server_status.sql",
            input.server as _,
            input.status as _,
            input.at
        )
        .fetch_one(self.db())
        .await?)
    }
}

/// What one pod's row held before this observation.
#[derive(Debug, Clone)]
pub struct PodObservation {
    pub pod: PodId,
    pub previous: Option<PodHealthStatus>,
}

/// Records a batch of pod statuses and answers with the previous one of each.
///
/// `pods` and `statuses` are index-aligned, and a pod may appear **once**:
/// `ON CONFLICT DO UPDATE` errors if one statement hits the same row twice, so
/// callers dedupe before asking.
pub struct ObservePodStatuses {
    pub pods: Vec<PodId>,
    pub statuses: Vec<PodHealthStatus>,
    pub at: DateTime<Utc>,
}

impl Processor<ObservePodStatuses> for Db {
    /// One entry per input pod, in no particular order.
    type Output = Vec<PodObservation>;
    type Error = Error;
    #[tracing::instrument(name = "Query:ObservePodStatuses", skip_all, err)]
    async fn process(&self, input: ObservePodStatuses) -> Result<Self::Output, Self::Error> {
        if input.pods.is_empty() {
            return Ok(Vec::new());
        }
        Ok(sqlx::query_file_as!(
            PodObservation,
            "sql/observe_pod_statuses.sql",
            &input.pods as _,
            &input.statuses as _,
            input.at
        )
        .fetch_all(self.db())
        .await?)
    }
}
