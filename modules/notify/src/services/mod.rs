//! Business logic: who hears about a health change, and how they hear it.
//!
//! Two halves, joined by the broker:
//!
//! - [`fanout`] runs wherever the orchestration consumers run. It filters the
//!   health facts down to real changes ([`crate::entities::db::state`]), reads
//!   the settings, and publishes one notice per audience.
//! - [`delivery`] runs in the single `notifier` instance. It renders a notice
//!   once and sends it over [`email`] and [`telegram`].
//!
//! [`config`] is the dashboard's read/replace pair for the module's own config
//! document, and [`setting`] is the read/replace pair for the settings rows.

pub mod config;
pub mod delivery;
pub mod email;
pub mod fanout;
pub mod setting;
pub mod telegram;

#[derive(Debug, thiserror::Error)]
pub enum NotifyError {
    #[error(transparent)]
    Core(#[from] wakuwaku::Error),
    #[error(transparent)]
    Database(#[from] base::db::Error),
    #[error(transparent)]
    Config(#[from] base::services::config::ConfigError),
    #[error("email: {0}")]
    Email(String),
    #[error("telegram: {0}")]
    Telegram(String),
    /// The channel a caller asked for is not configured in this installation.
    #[error("the channel is not configured")]
    Disabled,
    #[error("not found")]
    NotFound,
    #[error("{0}")]
    InvalidInput(String),
    #[error("permission denied")]
    PermissionDenied,
}

/// What a hook hands back to the AMQP consumer, which decides from the variant
/// whether to requeue the delivery or ack it: a database failure stays one, so
/// the next consumer may succeed, and everything else is non-retryable and gets
/// acked with its message logged.
impl From<NotifyError> for wakuwaku::Error {
    fn from(error: NotifyError) -> Self {
        match error {
            NotifyError::Core(e) => e,
            NotifyError::Database(e) => e.into(),
            NotifyError::NotFound => wakuwaku::Error::NotFound,
            NotifyError::PermissionDenied => wakuwaku::Error::PermissionsDenied,
            other => wakuwaku::Error::BusinessPanic(anyhow::anyhow!("{other}")),
        }
    }
}

impl From<NotifyError> for tonic::Status {
    fn from(error: NotifyError) -> Self {
        match error {
            // A database that did not answer is not a bug in the control
            // plane: it is retryable, and a caller told `INTERNAL` has no
            // reason to try again.
            NotifyError::Core(e) => base::db::status_of(e),
            NotifyError::Database(e) if base::db::is_unavailable(&e) => {
                tracing::warn!(error = %e, "database unavailable");
                tonic::Status::unavailable("Database unavailable")
            }
            NotifyError::Database(e) => {
                tracing::error!(error = %e, "database error");
                tonic::Status::internal("Database error")
            }
            // The operator wrote this payload: naming what is wrong with it is
            // the point of validating before the row is touched.
            NotifyError::Config(base::services::config::ConfigError::Decode { key, source }) => {
                tonic::Status::invalid_argument(format!(
                    "the payload is not a valid `{key}` configuration: {source}"
                ))
            }
            NotifyError::Config(e) => {
                tracing::error!(error = %e, "config error");
                tonic::Status::internal("Config error")
            }
            NotifyError::Email(message) | NotifyError::Telegram(message) => {
                tracing::error!(%message, "delivery failed");
                tonic::Status::internal("Delivery failed")
            }
            NotifyError::Disabled => {
                tonic::Status::failed_precondition("The channel is not configured")
            }
            NotifyError::NotFound => tonic::Status::not_found("Not found"),
            NotifyError::InvalidInput(message) => tonic::Status::invalid_argument(message),
            NotifyError::PermissionDenied => tonic::Status::permission_denied("Permission denied"),
        }
    }
}
