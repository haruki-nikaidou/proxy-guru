//! Module configuration.
//!
//! Put the strongly typed configuration for this module here. Configuration
//! lives in the database: one `app_config` row per key, holding the whole
//! struct as a JSON document (see
//! [`crate::entities::db::app_config`]). The database is the only source
//! of truth, so every process in a fleet loads the same values without any
//! matching environment; `manage-tool config seed` writes the defaults and
//! `manage-tool config set` replaces them.
//!
//! Define a `serde`-(de)serializable struct that implements [`Default`] and
//! bind it to a stable key with
//! [`ConfigJson`](crate::entities::db::app_config::ConfigJson):
//!
//! ```
//! use base::entities::db::app_config::ConfigJson;
//! use serde::{Deserialize, Serialize};
//!
//! // `#[serde(default)]` on the struct is load-bearing: a row written before
//! // `max_items` existed still loads, with `Default` filling the gap.
//! #[derive(Debug, Clone, Serialize, Deserialize, Default)]
//! #[serde(default)]
//! pub struct ExampleConfig {
//!     pub feature_enabled: bool,
//!     pub max_items: u32,
//! }
//!
//! impl ConfigJson for ExampleConfig {
//!     const KEY: &'static str = "example";
//! }
//! ```
//!
//! Services hold the struct by value and receive it at construction time; load
//! it once during startup with
//! [`LoadConfig`](crate::services::config::LoadConfig), not per request, and
//! register the key as a `ConfigKey` variant in `manage-tool` so it is seeded
//! and inspectable.
//!
//! `base` itself has no settings of its own — it owns the store, not a key.
