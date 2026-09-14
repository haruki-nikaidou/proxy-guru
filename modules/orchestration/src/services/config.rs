//! The `orchestration` configuration document: read it as stored, replace it
//! wholesale.
//!
//! This is the dashboard's counterpart to `manage-tool config get
//! orchestration` / `set orchestration`, and it behaves identically. Reads go
//! through the untyped [`FindRawConfig`] query and are never decoded: a row
//! that no longer matches [`OrchestrationConfig`] is exactly what an operator
//! must see, and a write is the way back out. Writes *are* decoded first, so a
//! payload of the wrong shape is rejected and the row keeps its previous
//! contents.
//!
//! A stored value takes effect when the masters restart — health thresholds,
//! retention windows and certificate lifetimes are read once at startup and
//! handed to the services by value.

use crate::config::OrchestrationConfig;
use crate::services::OrchestrationError;
use auth::services::identity::Identity;
use auth::utils::rbac::Permission;
use base::entities::surreal::app_config::{ConfigJson, FindRawConfig};
use base::services::config::{ConfigError, ConfigStore, StoreConfig, decode, defaults};
use kanau::processor::Processor;

/// Reads and replaces the `orchestration` config row. Admin only.
#[derive(Debug, Clone)]
pub struct OrchestrationConfigService {
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

/// A config failure in this module's own vocabulary: a decode failure is the
/// operator's payload being wrong ([`OrchestrationError::Invalid`], which the
/// edge turns into `INVALID_ARGUMENT` carrying the message), everything else is
/// the database.
///
/// Reads here are raw, so a decode failure can only come from a payload the
/// operator just submitted. `ConfigError::Decode` phrases itself for the
/// startup read of a *stored* row, so only its `serde` source is reused.
fn config_error(error: ConfigError) -> OrchestrationError {
    match error {
        ConfigError::Database(error) => OrchestrationError::Db(error),
        ConfigError::Decode { key, source } => OrchestrationError::Invalid(format!(
            "the payload is not a valid `{key}` configuration: {source}"
        )),
        other => OrchestrationError::Invalid(other.to_string()),
    }
}

/// Only an Admin with a human session may see or change the values the whole
/// fleet runs on; `ManageConfig` is granted to no other role.
fn ensure_admin(actor: &Identity) -> Result<(), OrchestrationError> {
    actor
        .ensure(Permission::ManageConfig)
        .map_err(|_| OrchestrationError::PermissionDenied)
}

/// The current document, read raw.
async fn read(configs: &ConfigStore) -> Result<ConfigDocument, OrchestrationError> {
    let defaults = defaults::<OrchestrationConfig>().map_err(config_error)?;
    match configs
        .db
        .process(FindRawConfig {
            key: OrchestrationConfig::KEY,
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

impl Processor<GetModuleConfig> for OrchestrationConfigService {
    type Output = ConfigDocument;
    type Error = OrchestrationError;
    #[tracing::instrument(skip_all, err, name = "Service:GetModuleConfig")]
    async fn process(&self, input: GetModuleConfig) -> Result<Self::Output, Self::Error> {
        ensure_admin(&input.actor)?;
        read(&self.configs).await
    }
}

/// Replace the whole document. The payload is decoded into
/// [`OrchestrationConfig`] before anything is written, so a wrong-shaped one
/// leaves the row alone.
pub struct SetModuleConfig {
    pub actor: Identity,
    pub json: serde_json::Value,
}

impl Processor<SetModuleConfig> for OrchestrationConfigService {
    type Output = ConfigDocument;
    type Error = OrchestrationError;
    #[tracing::instrument(skip_all, err, name = "Service:SetModuleConfig")]
    async fn process(&self, input: SetModuleConfig) -> Result<Self::Output, Self::Error> {
        ensure_admin(&input.actor)?;
        let config = decode::<OrchestrationConfig>(input.json).map_err(config_error)?;
        self.configs
            .process(StoreConfig(config))
            .await
            .map_err(config_error)?;
        // Re-read rather than echo: what the caller gets back is the row, as
        // the next startup will read it.
        read(&self.configs).await
    }
}
