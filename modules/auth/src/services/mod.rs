//! Business logic layer.
//!
//! A service is a stateful `Processor`: a
//! `Clone`-able struct that owns its dependencies (database, Redis, event
//! publisher, other services, loaded config) and implements one `Processor` per
//! operation it supports.
//!
//! Services orchestrate [`entities`](crate::entities), publish
//! [`events`](crate::events), and are the layer the [`rpc`](crate::rpc) edge
//! calls into. Keep transport concerns (protobuf, HTTP) out of here — a service
//! should be usable regardless of how it is invoked. Every operation fails with
//! [`AuthError`], which the edge turns into a status with `?`.

pub mod account;
pub mod api_key;
pub mod config;
pub mod identity;
pub mod session;

pub use account::{
    AccountService, ChangeEmailResult, ChangeOwnEmail, ChangeOwnPassword, ChangePasswordResult,
    DeleteAccount, ListAccounts, RegisterAccount, RegisterResult, SetAccountRole,
};
pub use api_key::{
    ApiKeyService, AuthenticateApiKey, CreateApiKey, CreatedApiKey, ListApiKeys, RevokeApiKey,
};
pub use config::{AuthConfigService, ConfigDocument, GetModuleConfig, SetModuleConfig};
pub use identity::{Identity, IdentityKind, PermissionDenied};
pub use session::{AuthenticateSession, Login, LoginResult, Logout, SessionService};

/// What an auth operation can fail with.
#[derive(Debug, thiserror::Error)]
pub enum AuthError {
    #[error(transparent)]
    Database(#[from] base::db::Error),
    #[error("not found")]
    NotFound,
    #[error("permission denied")]
    PermissionDenied,
    #[error(transparent)]
    Internal(anyhow::Error),
}

impl From<PermissionDenied> for AuthError {
    fn from(_: PermissionDenied) -> Self {
        AuthError::PermissionDenied
    }
}

impl From<AuthError> for tonic::Status {
    fn from(error: AuthError) -> Self {
        match error {
            AuthError::Database(e) => base::db::status_of(e),
            AuthError::NotFound => tonic::Status::not_found("Not found"),
            AuthError::PermissionDenied => tonic::Status::permission_denied("Permission denied"),
            AuthError::Internal(e) => {
                tracing::error!(error = %e, "internal error");
                tonic::Status::internal("Internal server error")
            }
        }
    }
}
