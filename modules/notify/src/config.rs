//! Module configuration.
//!
//! [`NotifyConfig`] is stored in the database under the `"notify"` key and
//! loaded once during startup through `base`'s configuration store
//! (`base::services::config::LoadConfig`); the delivery service holds the struct
//! by value. `manage-tool config seed` writes these defaults,
//! `manage-tool config set notify '<json>'` changes them (as does an Admin in
//! the dashboard), and the new values take effect when the masters restart.
//!
//! What is *not* here: the SMTP password and the Telegram bot token. This
//! document is readable by every Admin through the dashboard, so the two
//! secrets come from the environment instead
//! ([`crate::utils::secret::NotifySecrets`]).

use crate::entities::db::setting::Language;
use base::entities::db::app_config::ConfigJson;
use serde::{Deserialize, Serialize};
use std::time::Duration;

/// The public Telegram Bot API. An operator behind a filtered network can point
/// this at a reverse proxy of their own.
pub const DEFAULT_TELEGRAM_API_BASE: &str = "https://api.telegram.org";

/// Operator-tunable notification settings: the SMTP relay, the Telegram API,
/// and how hard a delivery tries.
///
/// `#[serde(default)]` keeps a row written before a field existed readable: the
/// missing field falls back to [`Default`] instead of failing the startup read.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct NotifyConfig {
    /// The SMTP relay's host. Empty disables email entirely — every mail
    /// destination is then logged and skipped.
    pub smtp_host: String,
    pub smtp_port: u16,
    /// Whether the relay is upgraded with STARTTLS. `false` speaks plain SMTP,
    /// which is for a relay on localhost (or a test sink) only.
    pub smtp_starttls: bool,
    /// The SMTP user. Empty, or an unset `GURU_SMTP_PASSWORD`, sends
    /// unauthenticated.
    pub smtp_username: String,
    /// The `From` header, in either `addr@example.com` or `Name <addr@example.com>` form.
    pub smtp_from: String,
    /// The Telegram Bot API's base URL, without a trailing slash.
    pub telegram_api_base: String,
    /// How many times one send is attempted before the notice is given up on.
    pub delivery_attempts: u32,
    /// How long to wait between two attempts.
    pub delivery_retry_delay_secs: u64,
    /// The language a row that never named one is rendered in.
    pub default_language: Language,
}

impl Default for NotifyConfig {
    fn default() -> Self {
        Self {
            smtp_host: String::new(),
            smtp_port: 587,
            smtp_starttls: true,
            smtp_username: String::new(),
            smtp_from: "guru <noreply@example.com>".to_string(),
            telegram_api_base: DEFAULT_TELEGRAM_API_BASE.to_string(),
            delivery_attempts: 3,
            delivery_retry_delay_secs: 5,
            default_language: Language::En,
        }
    }
}

impl ConfigJson for NotifyConfig {
    const KEY: &'static str = "notify";
}

impl NotifyConfig {
    pub fn delivery_retry_delay(&self) -> Duration {
        Duration::from_secs(self.delivery_retry_delay_secs)
    }

    /// Whether a mail relay is configured at all.
    pub fn email_configured(&self) -> bool {
        !self.smtp_host.trim().is_empty()
    }
}
