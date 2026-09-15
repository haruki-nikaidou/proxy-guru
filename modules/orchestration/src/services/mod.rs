//! Business logic: canvas CRUD, topology rules, config derivation and rollout.
//!
//! Every config-affecting operation follows the same pipeline:
//!
//! 1. authorize the actor,
//! 2. resolve the canvas it targets,
//! 3. load the current [`CanvasTopology`](crate::entities::surreal::topology::CanvasTopology),
//! 4. validate the topology the change *would* produce and reject it before writing,
//! 5. reject it as well if it would break a listener the fabric still depends on
//!    ([`converge::ensure_switch_safe`]),
//! 6. write the rows and bump the canvas generation in one transaction,
//! 7. publish [`CanvasDirty`](crate::events::CanvasDirty) so the derivation hook
//!    picks the canvas up ([`notify::Notifier`]), together with the live
//!    dashboard event for the same change.

pub mod acme;
pub mod agent;
pub mod ca;
pub mod canvas;
pub mod config;
pub mod converge;
pub mod derive;
pub mod dns;
pub mod edge;
pub mod health;
pub mod live;
pub mod node;
pub mod notify;
pub mod rollout;
pub mod server;
pub mod topology;
pub mod universal;
pub mod watch;

use crate::services::converge::ConvergeError;
use crate::services::derive::DeriveError;
use crate::services::topology::TopologyError;
use crate::utils::secret::SecretError;

#[derive(Debug, thiserror::Error)]
pub enum OrchestrationError {
    #[error(transparent)]
    Core(#[from] wakuwaku::Error),
    #[error(transparent)]
    Db(#[from] surrealdb::Error),
    #[error("topology: {0}")]
    Topology(#[from] Box<TopologyError>),
    #[error("derive: {0}")]
    Derive(#[from] DeriveError),
    #[error("converge: {0}")]
    Converge(#[from] ConvergeError),
    #[error("secret: {0}")]
    Secret(#[from] SecretError),
    /// Certificate generation or parsing failed (rcgen / x509).
    #[error("certificate: {0}")]
    Certificate(String),
    #[error("{0}")]
    Invalid(String),
    #[error("{0}")]
    Conflict(String),
    #[error("not found")]
    NotFound,
    #[error("permission denied")]
    PermissionDenied,
}

impl From<OrchestrationError> for tonic::Status {
    fn from(error: OrchestrationError) -> Self {
        match error {
            OrchestrationError::Core(e) => tonic::Status::from(e),
            OrchestrationError::Db(e) => {
                tracing::error!(error = %e, "database error");
                tonic::Status::internal("Database error")
            }
            OrchestrationError::Topology(e) => tonic::Status::failed_precondition(e.to_string()),
            OrchestrationError::Derive(e) => tonic::Status::failed_precondition(e.to_string()),
            OrchestrationError::Converge(e) => tonic::Status::failed_precondition(e.to_string()),
            OrchestrationError::Secret(e) => {
                tracing::error!(error = %e, "secret error");
                tonic::Status::internal("Secret error")
            }
            OrchestrationError::Certificate(e) => {
                tracing::error!(error = %e, "certificate error");
                tonic::Status::internal("Certificate error")
            }
            OrchestrationError::Invalid(message) => tonic::Status::invalid_argument(message),
            OrchestrationError::Conflict(message) => tonic::Status::failed_precondition(message),
            OrchestrationError::NotFound => tonic::Status::not_found("Not found"),
            OrchestrationError::PermissionDenied => {
                tonic::Status::permission_denied("Permission denied")
            }
        }
    }
}

/// What a hook hands back to the AMQP consumer, which decides from the variant
/// whether to requeue the message or ack it:
///
/// - a database failure stays a database failure, so the delivery is requeued —
///   the next consumer may well succeed;
/// - a missing row, a permission failure and every rule violation become the
///   matching non-retryable variant, keeping their message, so the delivery is
///   acked and logged instead of looping forever on work that cannot succeed.
impl From<OrchestrationError> for wakuwaku::Error {
    fn from(error: OrchestrationError) -> Self {
        match error {
            OrchestrationError::Core(e) => e,
            OrchestrationError::Db(e) => e.into(),
            OrchestrationError::NotFound => wakuwaku::Error::NotFound,
            OrchestrationError::PermissionDenied => wakuwaku::Error::PermissionsDenied,
            other => wakuwaku::Error::BusinessPanic(anyhow::anyhow!("{other}")),
        }
    }
}
