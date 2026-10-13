//! The installation-wide configuration store: one `app_config` row per config
//! key, holding the whole serialised config struct.
//!
//! This layer is deliberately untyped: payloads go in and come out as
//! [`serde_json::Value`], so every query keeps `Error = db::Error` and typed
//! (de)serialization — whose failures are `serde_json::Error` — stays a service
//! concern ([`crate::services::config`]).

use crate::db::Error;
use crate::entities::db::tables::AppConfig;
use serde::Serialize;
use serde::de::DeserializeOwned;
use sqlx::PgConnection;
use wakuwaku::entity::sqlx::backend::PgSource;
use wakuwaku::entity::{Execute, Query, Read, Write};

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

impl Query for FindRawConfig {
    type Effect = Read<AppConfig>;
}

impl Execute<PgSource> for FindRawConfig {
    type Output = Option<serde_json::Value>;
    type Error = Error;
    #[tracing::instrument(name = "Query:FindRawConfig", skip_all, err)]
    async fn execute(self, conn: &mut PgConnection) -> Result<Self::Output, Self::Error> {
        Ok(
            sqlx::query_scalar!("SELECT content FROM app_config WHERE key = $1", self.key)
                .fetch_optional(&mut *conn)
                .await?,
        )
    }
}

/// Overwrite one key's payload, creating the row when it is absent.
pub struct UpsertRawConfig {
    pub key: &'static str,
    pub content: serde_json::Value,
}

impl Query for UpsertRawConfig {
    type Effect = Write<AppConfig>;
}

impl Execute<PgSource> for UpsertRawConfig {
    type Output = ();
    type Error = Error;
    #[tracing::instrument(name = "Query:UpsertRawConfig", skip_all, err)]
    async fn execute(self, conn: &mut PgConnection) -> Result<Self::Output, Self::Error> {
        sqlx::query!(
            "INSERT INTO app_config (key, content) VALUES ($1, $2)
             ON CONFLICT (key) DO UPDATE SET content = EXCLUDED.content",
            self.key,
            self.content
        )
        .execute(&mut *conn)
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

impl Query for InsertRawConfigIfAbsent {
    type Effect = Write<AppConfig>;
}

impl Execute<PgSource> for InsertRawConfigIfAbsent {
    type Output = bool;
    type Error = Error;
    #[tracing::instrument(name = "Query:InsertRawConfigIfAbsent", skip_all, err)]
    async fn execute(self, conn: &mut PgConnection) -> Result<Self::Output, Self::Error> {
        // `DO NOTHING` returns no row on conflict, so the key comes back exactly
        // when the row was created.
        let created: Option<String> = sqlx::query_scalar!(
            "INSERT INTO app_config (key, content) VALUES ($1, $2)
             ON CONFLICT (key) DO NOTHING RETURNING key",
            self.key,
            self.content
        )
        .fetch_optional(&mut *conn)
        .await?;
        Ok(created.is_some())
    }
}
