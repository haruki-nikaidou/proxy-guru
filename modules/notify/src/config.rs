//! Module configuration.
//!
//! Put the strongly typed configuration for this module here. Configuration
//! lives in the database: one `app_config` row per key, holding the whole
//! struct as a JSON document. The database is the only source of truth — there
//! is no cache — and `manage-tool config seed` writes the defaults.
//!
//! Define a `serde`-(de)serializable struct that implements `Default` and bind
//! it to a stable key with `base`'s `ConfigJson`:
//!
//! ```ignore
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
//! Load it once during startup with `base::services::config::LoadConfig` and
//! hand the value to the services; register the key in `manage-tool`'s
//! `ConfigKey` enum. To let an Admin read and replace it from the dashboard,
//! add a typed `Get<Module>Config` / `Set<Module>Config` pair to this module's
//! gRPC service, answering with `guru.base.ConfigDocument`; the module that
//! names `ExampleConfig` is the one that validates a payload for it.
