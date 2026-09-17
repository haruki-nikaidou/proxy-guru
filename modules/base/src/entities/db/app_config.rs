//! The installation-wide configuration store: one `app_config` row per config
//! key, holding the whole serialised config struct.
//!
//! This layer is deliberately untyped: payloads go in and come out as
//! [`serde_json::Value`], so every query keeps `Error = db::Error` and typed
//! (de)serialization — whose failures are `serde_json::Error` — stays a service
//! concern ([`crate::services::config`]).

use crate::db::{Db, Error};
use kanau::processor::Processor;
use serde::Serialize;
use serde::de::DeserializeOwned;

/// A config struct bound to the key it is stored under.
///
/// One key per module (`"auth"`, `"orchestration"`): the whole struct is one
/// row, so a module's settings are read and written as a unit.
///
/// Implementors should carry `#[serde(default)]` on the struct so a row written
/// before a field was added still loads — the missing field falls back to
/// [`Default`] instead of failing the read.
pub trait ConfigJson: Default + Serialize + DeserializeOwned + Send + Sync + 'static {
    const KEY: &'static str;
}

/// The stored payload for one key, or `None` when the key was never written.
pub struct FindRawConfig {
    pub key: &'static str,
}

impl Processor<FindRawConfig> for Db {
    type Output = Option<serde_json::Value>;
    type Error = Error;
    #[tracing::instrument(name = "Query:FindRawConfig", skip_all, err)]
    async fn process(&self, input: FindRawConfig) -> Result<Self::Output, Self::Error> {
        Ok(sqlx::query_scalar!(
            "SELECT content FROM app_config WHERE key = $1",
            input.key
        )
        .fetch_optional(self.db())
        .await?)
    }
}

/// Overwrite one key's payload, creating the row when it is absent.
pub struct UpsertRawConfig {
    pub key: &'static str,
    pub content: serde_json::Value,
}

impl Processor<UpsertRawConfig> for Db {
    type Output = ();
    type Error = Error;
    #[tracing::instrument(name = "Query:UpsertRawConfig", skip_all, err)]
    async fn process(&self, input: UpsertRawConfig) -> Result<Self::Output, Self::Error> {
        sqlx::query!(
            "INSERT INTO app_config (key, content) VALUES ($1, $2)
             ON CONFLICT (key) DO UPDATE SET content = EXCLUDED.content",
            input.key,
            input.content
        )
        .execute(self.db())
        .await?;
        Ok(())
    }
}

/// Write one key's payload only when the key is absent, reporting whether the
/// row was created.
///
/// This is what seeding is built on: re-seeding an installation must never
/// clobber a value an operator has changed.
pub struct InsertRawConfigIfAbsent {
    pub key: &'static str,
    pub content: serde_json::Value,
}

impl Processor<InsertRawConfigIfAbsent> for Db {
    type Output = bool;
    type Error = Error;
    #[tracing::instrument(name = "Query:InsertRawConfigIfAbsent", skip_all, err)]
    async fn process(&self, input: InsertRawConfigIfAbsent) -> Result<Self::Output, Self::Error> {
        // `DO NOTHING` returns no row on conflict, so the key comes back exactly
        // when the row was created.
        let created: Option<String> = sqlx::query_scalar!(
            "INSERT INTO app_config (key, content) VALUES ($1, $2)
             ON CONFLICT (key) DO NOTHING RETURNING key",
            input.key,
            input.content
        )
        .fetch_optional(self.db())
        .await?;
        Ok(created.is_some())
    }
}
