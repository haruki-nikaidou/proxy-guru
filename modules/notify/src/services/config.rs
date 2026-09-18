//! The `notify` configuration document: read it as stored, replace it
//! wholesale.
//!
//! This is the dashboard's counterpart to `manage-tool config get notify` /
//! `set notify`, and it behaves identically. Reads go through the untyped
//! [`FindRawConfig`] query and are never decoded: a row that no longer matches
//! [`NotifyConfig`] is exactly what an operator must see, and a write is the way
//! back out. Writes *are* decoded first, so a payload of the wrong shape is
//! rejected and the row keeps its previous contents.
//!
//! A stored value takes effect when the masters restart — the SMTP transport
//! and the Telegram client are built once, at startup, from this document.

use crate::config::NotifyConfig;
use crate::services::NotifyError;
use auth::services::identity::Identity;
use auth::utils::rbac::Permission;
use base::entities::db::app_config::{ConfigJson, FindRawConfig};
use base::services::config::{ConfigStore, StoreConfig, decode, defaults};
use kanau::processor::Processor;

/// Reads and replaces the `notify` config row. Admin only.
#[derive(Debug, Clone)]
pub struct NotifyConfigService {
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

/// Only an Admin with a human session may see or change the values the whole
/// installation runs on; `ManageConfig` is granted to no other role.
fn ensure_admin(actor: &Identity) -> Result<(), NotifyError> {
    actor
        .ensure(Permission::ManageConfig)
        .map_err(|_| NotifyError::PermissionDenied)
}

/// The current document, read raw.
async fn read(configs: &ConfigStore) -> Result<ConfigDocument, NotifyError> {
    let defaults = defaults::<NotifyConfig>()?;
    match configs
        .db
        .process(FindRawConfig {
            key: NotifyConfig::KEY,
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

impl Processor<GetModuleConfig> for NotifyConfigService {
    type Output = ConfigDocument;
    type Error = NotifyError;
    #[tracing::instrument(skip_all, err, name = "Service:GetModuleConfig")]
    async fn process(&self, input: GetModuleConfig) -> Result<Self::Output, Self::Error> {
        ensure_admin(&input.actor)?;
        read(&self.configs).await
    }
}

/// Replace the whole document. The payload is decoded into [`NotifyConfig`]
/// before anything is written, so a wrong-shaped one leaves the row alone.
pub struct SetModuleConfig {
    pub actor: Identity,
    pub json: serde_json::Value,
}

impl Processor<SetModuleConfig> for NotifyConfigService {
    type Output = ConfigDocument;
    type Error = NotifyError;
    #[tracing::instrument(skip_all, err, name = "Service:SetModuleConfig")]
    async fn process(&self, input: SetModuleConfig) -> Result<Self::Output, Self::Error> {
        ensure_admin(&input.actor)?;
        let config = decode::<NotifyConfig>(input.json)?;
        self.configs.process(StoreConfig(config)).await?;
        // Re-read rather than echo: what the caller gets back is the row, as
        // the next startup will read it.
        read(&self.configs).await
    }
}
