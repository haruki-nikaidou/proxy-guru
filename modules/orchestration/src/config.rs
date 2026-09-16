//! Module configuration.
//!
//! [`OrchestrationConfig`] is stored in the database under the
//! `"orchestration"` key and loaded once during startup through `base`'s
//! configuration store (`base::services::config::LoadConfig`); services and
//! hooks hold the struct by value. `manage-tool config seed` writes these
//! defaults, `manage-tool config set orchestration '<json>'` changes them, and
//! the new values take effect when the masters restart.

use base::entities::db::app_config::ConfigJson;
use serde::{Deserialize, Serialize};
use std::time::Duration;

pub const LETS_ENCRYPT_DIRECTORY: &str = "https://acme-v02.api.letsencrypt.org/directory";
pub const LETS_ENCRYPT_STAGING_DIRECTORY: &str =
    "https://acme-staging-v02.api.letsencrypt.org/directory";

/// Operator-tunable orchestration settings: health thresholds and retention,
/// ACME defaults, relay-certificate lifetimes.
///
/// `#[serde(default)]` keeps a row written before a field existed readable: the
/// missing field falls back to [`Default`] instead of failing the startup read.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct OrchestrationConfig {
    /// How often a worker sends a `HealthReport`. Workers are told nothing; this
    /// is the master's expectation and sizes the offline threshold.
    pub health_report_interval_secs: u64,
    /// A server that has not reported for this many intervals is `Offline`.
    pub health_offline_after_intervals: u64,
    /// How long a server may lag `desired` before it counts as `Degraded`.
    pub degraded_grace_secs: u64,
    /// Retention of raw `server_health_record` rows.
    pub server_health_ttl_secs: u64,
    /// Retention of raw `node_health_record` rows.
    pub node_health_ttl_secs: u64,
    /// The ACME directory an Entry uses when its `TlsConfig.acme_directory` is
    /// empty.
    pub default_acme_directory: String,
    /// Renew an ACME certificate this long before `not_after`.
    pub acme_renew_before_secs: u64,
    /// After a failed ACME attempt, wait this long before retrying.
    pub acme_retry_after_secs: u64,
    /// Validity of the leaf certificates the internal CA issues to relay pods.
    pub relay_cert_valid_secs: u64,
    /// Rotate a relay leaf this long before `not_after`.
    pub relay_cert_renew_before_secs: u64,
    /// How often the stale-canvas derivation sweep may run.
    ///
    /// This and the four cadences below gate the *execution* of a periodic job,
    /// not its scheduling: the `cron` scheduler publishes each signal on a fixed
    /// cadence (it opens no database), and the consumer claims a run only once
    /// per interval. A value below the signal's own cadence therefore means
    /// "every signal", and a larger one slows the job down fleet-wide.
    pub sweep_interval_secs: u64,
    /// How often silent servers are flipped to `Offline`.
    pub liveness_interval_secs: u64,
    /// How often health history is trimmed to its TTLs.
    pub health_retention_interval_secs: u64,
    /// How often ACME certificates are ensured, issued and renewed.
    pub acme_interval_secs: u64,
    /// How often expiring relay leaves are rotated.
    pub relay_rotation_interval_secs: u64,
    /// Whether the worker API trusts `x-real-ip` / `x-forwarded-for` when it
    /// records the address a registration came from. Leave on behind the
    /// documented TLS-terminating proxy; turn off when `:50052` is exposed
    /// directly, or a worker could spoof its observed address.
    pub trust_proxy_address_headers: bool,
    /// How often an idle `Watch*` stream sends an empty keep-alive and
    /// re-checks the session that opened it. Keep it under any proxy idle
    /// timeout in front of the dashboard API.
    pub stream_keepalive_secs: u64,
    /// The public origin workers dial and the install command downloads from,
    /// e.g. `https://guru.example.com`. Empty means the dashboard cannot render
    /// an install command.
    pub agent_public_base_url: String,
    /// The URL path under that origin where the published agent artifacts are
    /// served from (`manage-tool agent publish` writes them; nginx serves the
    /// directory). Leading slash, no trailing one.
    pub agent_download_path: String,
    /// How often a live worker asks whether an update was requested for it.
    pub agent_update_poll_secs: u64,
}

impl Default for OrchestrationConfig {
    fn default() -> Self {
        Self {
            health_report_interval_secs: 15,
            health_offline_after_intervals: 3,
            degraded_grace_secs: 60,
            server_health_ttl_secs: 7 * 24 * 60 * 60,
            node_health_ttl_secs: 7 * 24 * 60 * 60,
            default_acme_directory: LETS_ENCRYPT_DIRECTORY.to_string(),
            acme_renew_before_secs: 30 * 24 * 60 * 60,
            acme_retry_after_secs: 60 * 60,
            relay_cert_valid_secs: 30 * 24 * 60 * 60,
            relay_cert_renew_before_secs: 10 * 24 * 60 * 60,
            sweep_interval_secs: 30,
            liveness_interval_secs: 30,
            health_retention_interval_secs: 300,
            acme_interval_secs: 60,
            relay_rotation_interval_secs: 3600,
            trust_proxy_address_headers: true,
            stream_keepalive_secs: 15,
            agent_public_base_url: String::new(),
            agent_download_path: "/agent".to_string(),
            agent_update_poll_secs: 60,
        }
    }
}

impl ConfigJson for OrchestrationConfig {
    const KEY: &'static str = "orchestration";
}

impl OrchestrationConfig {
    /// Where the published agent artifacts are: `{base_url}{download_path}`,
    /// normalised to no trailing slash. `None` until a base URL is configured.
    pub fn agent_download_base(&self) -> Option<String> {
        let base = self.agent_public_base_url.trim().trim_end_matches('/');
        if base.is_empty() {
            return None;
        }
        let path = self.agent_download_path.trim().trim_matches('/');
        Some(if path.is_empty() {
            base.to_string()
        } else {
            format!("{base}/{path}")
        })
    }

    pub fn agent_update_poll(&self) -> Duration {
        Duration::from_secs(self.agent_update_poll_secs)
    }

    pub fn health_report_interval(&self) -> Duration {
        Duration::from_secs(self.health_report_interval_secs)
    }

    /// No report for this long means the worker is gone.
    pub fn health_offline_after(&self) -> Duration {
        Duration::from_secs(
            self.health_report_interval_secs
                .saturating_mul(self.health_offline_after_intervals),
        )
    }

    pub fn degraded_grace(&self) -> Duration {
        Duration::from_secs(self.degraded_grace_secs)
    }

    pub fn server_health_ttl(&self) -> Duration {
        Duration::from_secs(self.server_health_ttl_secs)
    }

    pub fn node_health_ttl(&self) -> Duration {
        Duration::from_secs(self.node_health_ttl_secs)
    }

    pub fn acme_renew_before(&self) -> Duration {
        Duration::from_secs(self.acme_renew_before_secs)
    }

    pub fn acme_retry_after(&self) -> Duration {
        Duration::from_secs(self.acme_retry_after_secs)
    }

    pub fn relay_cert_valid(&self) -> Duration {
        Duration::from_secs(self.relay_cert_valid_secs)
    }

    pub fn relay_cert_renew_before(&self) -> Duration {
        Duration::from_secs(self.relay_cert_renew_before_secs)
    }

    pub fn sweep_interval(&self) -> Duration {
        Duration::from_secs(self.sweep_interval_secs)
    }

    pub fn liveness_interval(&self) -> Duration {
        Duration::from_secs(self.liveness_interval_secs)
    }

    pub fn health_retention_interval(&self) -> Duration {
        Duration::from_secs(self.health_retention_interval_secs)
    }

    pub fn acme_interval(&self) -> Duration {
        Duration::from_secs(self.acme_interval_secs)
    }

    pub fn relay_rotation_interval(&self) -> Duration {
        Duration::from_secs(self.relay_rotation_interval_secs)
    }

    /// The keep-alive cadence of a live stream, never zero:
    /// `tokio::time::interval` panics on a zero period, and an operator who
    /// writes `0` means "as often as reasonable", not "crash the API".
    pub fn stream_keepalive(&self) -> Duration {
        Duration::from_secs(self.stream_keepalive_secs.max(1))
    }

    /// The directory an Entry resolves to: its own, or the default when empty.
    pub fn acme_directory<'a>(&'a self, requested: &'a str) -> &'a str {
        if requested.is_empty() {
            &self.default_acme_directory
        } else {
            requested
        }
    }
}
