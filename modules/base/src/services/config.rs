//! The configuration store: typed reads and writes over the `app_config` table.
//!
//! A config struct is bound to a key with
//! [`ConfigJson`](crate::entities::surreal::app_config::ConfigJson) and stored
//! as one JSON document. [`LoadConfig`] is a startup-time read: services keep
//! holding their config by value, they just get it from the database instead of
//! [`Default`], so every process in a fleet agrees without any matching
//! environment. Live reload is not part of this.
//!
//! An absent key yields `T::default()` — an unseeded installation is a normal
//! state. A payload that does not deserialize is a [`ConfigError::Decode`],
//! never a silent fallback: substituting defaults for a corrupt or incompatible
//! row would swap an operator's whole config (moving ACME from staging to the
//! production directory, say) behind a log line. Rows written before a field was
//! added stay readable through `#[serde(default)]` on the config struct, which
//! keeps additive changes cheap without hiding real corruption.

use crate::db::Db;
use crate::entities::surreal::app_config::{
    ConfigJson, FindRawConfig, InsertRawConfigIfAbsent, UpsertRawConfig,
};
use kanau::processor::Processor;
use std::marker::PhantomData;

#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    #[error(transparent)]
    Database(#[from] surrealdb::Error),
    #[error("stored config for key `{key}` does not match its type: {source}")]
    Decode {
        key: &'static str,
        #[source]
        source: serde_json::Error,
    },
    #[error("serializing config for key `{key}`: {source}")]
    Encode {
        key: &'static str,
        #[source]
        source: serde_json::Error,
    },
}

/// Reads and writes typed configuration. The database is the only source of
/// truth; there is no cache to invalidate.
#[derive(Debug, Clone)]
pub struct ConfigStore {
    pub db: Db,
}

/// Read one config. An absent key yields `T::default()`.
pub struct LoadConfig<T>(PhantomData<fn() -> T>);

impl<T> LoadConfig<T> {
    pub const fn new() -> Self {
        Self(PhantomData)
    }
}

impl<T> Default for LoadConfig<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T: ConfigJson> Processor<LoadConfig<T>> for ConfigStore {
    type Output = T;
    type Error = ConfigError;
    #[tracing::instrument(name = "Service:LoadConfig", skip_all, err, fields(key = T::KEY))]
    async fn process(&self, _input: LoadConfig<T>) -> Result<Self::Output, Self::Error> {
        let Some(content) = self.db.process(FindRawConfig { key: T::KEY }).await? else {
            return Ok(T::default());
        };
        decode(content)
    }
}

/// Replace one config with this value.
pub struct StoreConfig<T>(pub T);

impl<T: ConfigJson> Processor<StoreConfig<T>> for ConfigStore {
    type Output = ();
    type Error = ConfigError;
    #[tracing::instrument(name = "Service:StoreConfig", skip_all, err, fields(key = T::KEY))]
    async fn process(&self, input: StoreConfig<T>) -> Result<Self::Output, Self::Error> {
        let content = serde_json::to_value(input.0).map_err(|source| ConfigError::Encode {
            key: T::KEY,
            source,
        })?;
        self.db
            .process(UpsertRawConfig {
                key: T::KEY,
                content,
            })
            .await?;
        Ok(())
    }
}

/// Write `T::default()` only when the key is absent; `true` when it was
/// created. Seeding never overwrites an operator's edit, so it is safe to run
/// after every schema sync.
pub struct SeedConfig<T>(PhantomData<fn() -> T>);

impl<T> SeedConfig<T> {
    pub const fn new() -> Self {
        Self(PhantomData)
    }
}

impl<T> Default for SeedConfig<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T: ConfigJson> Processor<SeedConfig<T>> for ConfigStore {
    type Output = bool;
    type Error = ConfigError;
    #[tracing::instrument(name = "Service:SeedConfig", skip_all, err, fields(key = T::KEY))]
    async fn process(&self, _input: SeedConfig<T>) -> Result<Self::Output, Self::Error> {
        let content = defaults::<T>()?;
        Ok(self
            .db
            .process(InsertRawConfigIfAbsent {
                key: T::KEY,
                content,
            })
            .await?)
    }
}

/// Decode a stored payload into its config type, naming the key on failure.
///
/// Callers that accept a payload from an operator (`manage-tool config set`)
/// use this before [`StoreConfig`], so a payload of the wrong shape is rejected
/// with the same error the startup read would give it, and the row keeps its
/// previous contents.
pub fn decode<T: ConfigJson>(content: serde_json::Value) -> Result<T, ConfigError> {
    serde_json::from_value(content).map_err(|source| ConfigError::Decode {
        key: T::KEY,
        source,
    })
}

/// The default payload for one key, as it is stored.
pub fn defaults<T: ConfigJson>() -> Result<serde_json::Value, ConfigError> {
    serde_json::to_value(T::default()).map_err(|source| ConfigError::Encode {
        key: T::KEY,
        source,
    })
}
