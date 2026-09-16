//! Module configuration.
//!
//! [`AuthConfig`] is stored in the database under the `"auth"` key and loaded
//! once during startup through `base`'s configuration store
//! (`base::services::config::LoadConfig`); services hold the value.
//! `manage-tool config seed` writes the defaults, `manage-tool config set auth
//! '<json>'` changes them, and the new value takes effect when the masters
//! restart.

use base::entities::db::app_config::ConfigJson;
use serde::{Deserialize, Serialize};

/// Tunable authentication settings.
///
/// `#[serde(default)]` keeps a row written before a field existed readable: the
/// missing field falls back to [`Default`] instead of failing the startup read.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct AuthConfig {
    /// How long a session may sit idle (no activity) before it is treated as
    /// expired and rejected, in seconds. Use slides the deadline; the slide is
    /// recorded at most once a minute (`services::session::ACTIVITY_SLACK`).
    pub session_idle_ttl_secs: i64,
}

impl Default for AuthConfig {
    fn default() -> Self {
        Self {
            // One week of idle time.
            session_idle_ttl_secs: 7 * 24 * 3600,
        }
    }
}

impl ConfigJson for AuthConfig {
    const KEY: &'static str = "auth";
}
