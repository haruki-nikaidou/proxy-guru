//! Business logic: canvas CRUD, the pod graph, config derivation and rollout.
//!
//! Every config-affecting operation follows the same pipeline:
//!
//! 1. authorize the actor,
//! 2. resolve the canvas it targets,
//! 3. load the current [`GraphRows`](crate::entities::db::graph::GraphRows) of its tree,
//! 4. check the graph the change *would* produce and reject it before writing,
//! 5. reject it as well if it would break a listener the fabric still depends on
//!    ([`converge::switch_conflicts`]),
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
pub mod country;
pub mod derive;
pub mod dns;
pub mod graph;
pub mod health;
pub mod live;
pub mod notify;
pub mod rollout;
pub mod server;
pub mod shell;
pub mod shell_channel;
pub mod watch;

use crate::services::converge::ConvergeError;
use crate::services::derive::DeriveError;
use crate::services::shell::ShellError;
use crate::utils::secret::SecretError;

#[derive(Debug, thiserror::Error)]
pub enum OrchestrationError {
    #[error(transparent)]
    Db(base::db::Error),
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
    /// A remote-shell call the worker refused or did not answer.
    #[error("shell: {0}")]
    Shell(#[from] ShellError),
    /// A transaction's `COMMIT` was sent but its answer was lost: it may or may
    /// not have committed, so it is neither a conflict nor safe to retry blind.
    #[error("commit outcome unknown: {0}")]
    CommitUnknown(sqlx::Error),
    /// A broken invariant: logged, answered as `INTERNAL`.
    #[error(transparent)]
    Internal(anyhow::Error),
}

use crate::entities::db::fence::STALE_GENERATION;
use wakuwaku::entity::sqlx::transaction::TxError;

impl From<base::db::Error> for OrchestrationError {
    fn from(error: base::db::Error) -> Self {
        match error {
            // A fenced write that lost the race is not a database fault: the edit
            // was validated against a topology a concurrent edit has since moved,
            // so the write rolled itself back. Surface it as a retryable conflict,
            // not an opaque internal error.
            base::db::Error::Conflict(STALE_GENERATION) => OrchestrationError::Conflict(
                "canvas changed since it was validated; reload and retry".into(),
            ),
            // Every other conflict names what was refused; the transaction did
            // not commit and the caller may retry from a fresh read.
            base::db::Error::Conflict(token) => OrchestrationError::Conflict(token.into()),
            other => OrchestrationError::Db(other),
        }
    }
}

impl From<auth::services::identity::PermissionDenied> for OrchestrationError {
    fn from(_: auth::services::identity::PermissionDenied) -> Self {
        OrchestrationError::PermissionDenied
    }
}

/// How a transaction flow failed to end as it planned. A flow's own rollback
/// arrives as `TxOutput::RolledBack(base::db::Error)` and converts through
/// `From<base::db::Error>`; this is the driver's side.
impl From<TxError> for OrchestrationError {
    fn from(error: TxError) -> Self {
        match error {
            // No connection or no `BEGIN`: the flow never ran. A rejected
            // serializable `COMMIT` is a conflict; the transaction rolled back.
            TxError::Begin(e) | TxError::CommitRejected(e) => base::db::Error::from(e).into(),
            TxError::CommitUnknown(e) => OrchestrationError::CommitUnknown(e),
            // A flow that committed after a failed statement, or whose
            // `finished` disagrees with its end: a bug in the flow.
            error @ (TxError::Aborted | TxError::EndMismatch) => {
                OrchestrationError::Internal(anyhow::Error::new(error))
            }
        }
    }
}

impl From<OrchestrationError> for tonic::Status {
    fn from(error: OrchestrationError) -> Self {
        match error {
            // A database that did not answer is not a bug in the control plane: it is
            // retryable, and a caller told `INTERNAL` has no reason to try again.
            OrchestrationError::Db(e) if base::db::is_unavailable(&e) => {
                tracing::warn!(error = %e, "database unavailable");
                tonic::Status::unavailable("Database unavailable")
            }
            OrchestrationError::Db(e) => {
                tracing::error!(error = %e, "database error");
                tonic::Status::internal("Database error")
            }
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
            OrchestrationError::Shell(e) => e.into(),
            OrchestrationError::CommitUnknown(e) => {
                tracing::error!(error = %e, "commit outcome unknown");
                tonic::Status::unknown("Commit outcome unknown; check before retrying")
            }
            OrchestrationError::Internal(e) => {
                tracing::error!(error = %e, "internal error");
                tonic::Status::internal("Internal server error")
            }
        }
    }
}

impl OrchestrationError {
    /// How an Iggy consumer treats a hook that failed with this error:
    ///
    /// - a database that did not answer is transient: the chunk is retried in
    ///   place until it answers;
    /// - any other database failure or a conflict is retryable: the key goes to
    ///   the retry topic, and the next attempt may well succeed. So is a commit
    ///   whose outcome was lost: every hook's work is idempotent (a derivation
    ///   pass is fenced on the generation it read);
    /// - a missing row, a permission failure and every rule violation are
    ///   unrecoverable: the event is dropped and logged instead of looping
    ///   forever on work that cannot succeed.
    pub fn into_handle_error(self) -> wakuwaku::iggy::error::HandleError {
        use wakuwaku::iggy::error::{ErrorClass, HandleError};
        let class = match &self {
            OrchestrationError::Db(e) if base::db::is_unavailable(e) => ErrorClass::Transient,
            OrchestrationError::Db(_)
            | OrchestrationError::Conflict(_)
            | OrchestrationError::CommitUnknown(_) => ErrorClass::Retryable,
            _ => ErrorClass::Unrecoverable,
        };
        HandleError::new(class, self)
    }
}
