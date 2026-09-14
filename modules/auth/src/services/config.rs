//! The `auth` configuration document: read it as stored, replace it wholesale.
//!
//! This is the dashboard's counterpart to `manage-tool config get auth` /
//! `set auth`, and it behaves identically. Reads go through the untyped
//! [`FindRawConfig`] query and are never decoded: a row that no longer matches
//! [`AuthConfig`] is exactly what an operator must see, and a write is the way
//! back out. Writes *are* decoded first, so a payload of the wrong shape is
//! rejected and the row keeps its previous contents.
//!
//! A stored value takes effect when the masters restart — there is no live
//! reload, and adding one would mean every service re-reading its config per
//! call.

use crate::config::AuthConfig;
use crate::services::identity::Identity;
use crate::utils::rbac::Permission;
use base::entities::surreal::app_config::{ConfigJson, FindRawConfig};
use base::services::config::{ConfigError, ConfigStore, StoreConfig, decode, defaults};
use kanau::processor::Processor;

/// Reads and replaces the `auth` config row. Admin only.
#[derive(Debug, Clone)]
pub struct AuthConfigService {
    pub configs: ConfigStore,
}

/// One config key as an operator sees it: the row if there is one, plus what
/// seeding would have written, so a caller can always offer "reset to
/// defaults".
#[derive(Debug, Clone)]
pub struct ConfigDocument {
    /// Whether the key has a row at all. An unseeded installation is normal.
    pub stored: bool,
    /// The stored payload, or [`Self::defaults`] when there is no row.
    pub json: serde_json::Value,
    /// The payload `manage-tool config seed` would write.
    pub defaults: serde_json::Value,
}

#[derive(Debug, thiserror::Error)]
pub enum AuthConfigError {
    #[error("permission denied")]
    PermissionDenied,
    #[error(transparent)]
    Config(#[from] ConfigError),
    #[error(transparent)]
    Database(#[from] surrealdb::Error),
}

impl From<AuthConfigError> for tonic::Status {
    fn from(error: AuthConfigError) -> Self {
        match error {
            AuthConfigError::PermissionDenied => {
                tonic::Status::permission_denied("Permission denied")
            }
            // The operator wrote this payload: naming what is wrong with it is
            // the whole point of validating before the row is touched.
            // `ConfigError::Decode` phrases itself for the startup *read* of a
            // stored row, which this is not — only its `serde` source applies.
            AuthConfigError::Config(ConfigError::Decode { key, source }) => {
                tonic::Status::invalid_argument(format!(
                    "the payload is not a valid `{key}` configuration: {source}"
                ))
            }
            AuthConfigError::Config(error @ ConfigError::Encode { .. }) => {
                tracing::error!(error = %error, "serializing auth config");
                tonic::Status::internal("Internal server error")
            }
            AuthConfigError::Config(ConfigError::Database(error))
            | AuthConfigError::Database(error) => {
                tracing::error!(error = %error, "database error");
                tonic::Status::internal("Database error")
            }
        }
    }
}

/// Only an Admin with a human session may see or change the values the whole
/// installation runs on; `ManageConfig` is granted to no other role.
fn ensure_admin(actor: &Identity) -> Result<(), AuthConfigError> {
    actor
        .ensure(Permission::ManageConfig)
        .map_err(|_| AuthConfigError::PermissionDenied)
}

/// The current document, read raw.
async fn read(configs: &ConfigStore) -> Result<ConfigDocument, AuthConfigError> {
    let defaults = defaults::<AuthConfig>()?;
    match configs
        .db
        .process(FindRawConfig {
            key: AuthConfig::KEY,
        })
        .await?
    {
        Some(json) => Ok(ConfigDocument {
            stored: true,
            json,
            defaults,
        }),
        None => Ok(ConfigDocument {
            stored: false,
            json: defaults.clone(),
            defaults,
        }),
    }
}

pub struct GetModuleConfig {
    pub actor: Identity,
}

impl Processor<GetModuleConfig> for AuthConfigService {
    type Output = ConfigDocument;
    type Error = AuthConfigError;
    #[tracing::instrument(skip_all, err, name = "Service:GetModuleConfig")]
    async fn process(&self, input: GetModuleConfig) -> Result<Self::Output, Self::Error> {
        ensure_admin(&input.actor)?;
        read(&self.configs).await
    }
}

/// Replace the whole document. The payload is decoded into [`AuthConfig`]
/// before anything is written, so a wrong-shaped one leaves the row alone.
pub struct SetModuleConfig {
    pub actor: Identity,
    pub json: serde_json::Value,
}

impl Processor<SetModuleConfig> for AuthConfigService {
    type Output = ConfigDocument;
    type Error = AuthConfigError;
    #[tracing::instrument(skip_all, err, name = "Service:SetModuleConfig")]
    async fn process(&self, input: SetModuleConfig) -> Result<Self::Output, Self::Error> {
        ensure_admin(&input.actor)?;
        let config = decode::<AuthConfig>(input.json)?;
        self.configs.process(StoreConfig(config)).await?;
        // Re-read rather than echo: what the caller gets back is the row, as
        // the next startup will read it.
        read(&self.configs).await
    }
}
