//! The installation-wide configuration store: one `app_config` row per config
//! key, holding the whole serialised config struct.
//!
//! This layer is deliberately untyped: payloads go in and come out as
//! [`serde_json::Value`], so every query keeps `Error = surrealdb::Error` and
//! typed (de)serialization — whose failures are `serde_json::Error` — stays a
//! service concern ([`crate::services::config`]).
//!
//! The record key is the config key, so a lookup is a record read rather than
//! an index scan.

use kanau::processor::Processor;
use newtype_record_id::table_record;
use serde::Serialize;
use serde::de::DeserializeOwned;
use surrealdb_types::SurrealValue;
use wakuwaku::surreal::SurrealProcessor;

table_record!(AppConfigId, "app_config");

#[derive(Debug, Clone, SurrealValue)]
pub struct AppConfigEntity {
    pub id: AppConfigId,
    pub key: String,
    pub content: serde_json::Value,
}

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

impl Processor<FindRawConfig> for SurrealProcessor {
    type Output = Option<serde_json::Value>;
    type Error = surrealdb::Error;
    #[tracing::instrument(name = "Query:FindRawConfig", skip_all, err)]
    async fn process(&self, input: FindRawConfig) -> Result<Self::Output, Self::Error> {
        let mut resp = self
            .db()
            .query("SELECT * FROM type::record('app_config', $key)")
            .bind(("key", input.key.to_string()))
            .await?;
        Ok(resp
            .take::<Option<AppConfigEntity>>(0)?
            .map(|row| row.content))
    }
}

/// Overwrite one key's payload, creating the row when it is absent.
pub struct UpsertRawConfig {
    pub key: &'static str,
    pub content: serde_json::Value,
}

impl Processor<UpsertRawConfig> for SurrealProcessor {
    type Output = ();
    type Error = surrealdb::Error;
    #[tracing::instrument(name = "Query:UpsertRawConfig", skip_all, err)]
    async fn process(&self, input: UpsertRawConfig) -> Result<Self::Output, Self::Error> {
        self.db()
            .query(
                "UPSERT type::record('app_config', $key) CONTENT { key: $key, content: $content }",
            )
            .bind(("key", input.key.to_string()))
            .bind(("content", input.content))
            .await?
            .check()?;
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

impl Processor<InsertRawConfigIfAbsent> for SurrealProcessor {
    type Output = bool;
    type Error = surrealdb::Error;
    #[tracing::instrument(name = "Query:InsertRawConfigIfAbsent", skip_all, err)]
    async fn process(&self, input: InsertRawConfigIfAbsent) -> Result<Self::Output, Self::Error> {
        let mut resp = self
            .db()
            .query(
                "IF record::exists(type::record('app_config', $key)) { RETURN false } \
                 ELSE { CREATE type::record('app_config', $key) CONTENT { key: $key, content: $content }; RETURN true }",
            )
            .bind(("key", input.key.to_string()))
            .bind(("content", input.content))
            .await?;
        resp.take::<Option<bool>>(0)?.ok_or_else(|| {
            surrealdb::Error::internal("seeding app_config returned no result".to_string())
        })
    }
}
