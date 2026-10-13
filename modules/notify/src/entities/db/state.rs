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

use crate::entities::db::tables::{NotifyPodState, NotifyServerState};
use base::db::Error;
use orchestration::entities::db::health::{PodHealthStatus, ServerHealthStatus};
use orchestration::entities::db::pod::PodId;
use orchestration::entities::db::server::ServerId;
use orchestration::entities::db::tables::{OrchestrationPod, OrchestrationServer};
use sqlx::PgConnection;
use time::OffsetDateTime;
use wakuwaku::entity::sqlx::backend::PgSource;
use wakuwaku::entity::{Execute, Query, Read, Write};

/// Records one server's status and answers with the previous one.
pub struct ObserveServerStatus {
    pub server: ServerId,
    pub status: ServerHealthStatus,
    pub at: OffsetDateTime,
}

impl Query for ObserveServerStatus {
    type Effect = (Write<NotifyServerState>, Read<OrchestrationServer>);
}

impl Execute<PgSource> for ObserveServerStatus {
    /// The status last announced, or `None` for a first sighting.
    type Output = Option<ServerHealthStatus>;
    type Error = Error;
    #[tracing::instrument(name = "Query:ObserveServerStatus", skip_all, err)]
    async fn execute(self, conn: &mut PgConnection) -> Result<Self::Output, Self::Error> {
        Ok(sqlx::query_file_scalar!(
            "sql/observe_server_status.sql",
            self.server as _,
            self.status as _,
            self.at
        )
        .fetch_one(&mut *conn)
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
    pub at: OffsetDateTime,
}

impl Query for ObservePodStatuses {
    type Effect = (Write<NotifyPodState>, Read<OrchestrationPod>);
}

impl Execute<PgSource> for ObservePodStatuses {
    /// One entry per input pod, in no particular order.
    type Output = Vec<PodObservation>;
    type Error = Error;
    #[tracing::instrument(name = "Query:ObservePodStatuses", skip_all, err)]
    async fn execute(self, conn: &mut PgConnection) -> Result<Self::Output, Self::Error> {
        if self.pods.is_empty() {
            return Ok(Vec::new());
        }
        Ok(sqlx::query_file_as!(
            PodObservation,
            "sql/observe_pod_statuses.sql",
            &self.pods as _,
            &self.statuses as _,
            self.at
        )
        .fetch_all(&mut *conn)
        .await?)
    }
}
