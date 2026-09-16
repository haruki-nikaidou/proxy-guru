#![deny(clippy::unwrap_used)]
#![deny(clippy::expect_used)]
#![deny(clippy::panic)]
#![warn(clippy::arithmetic_side_effects)]

//! The guru data-plane worker.
//!
//! The binary is a thin wrapper around [`run`]; everything else lives here so that
//! integration tests can drive the supervisor and the control-plane agent directly.

pub mod addresses;
pub mod agent;
pub mod certs;
pub mod cli;
pub mod keepalive;
pub mod listener;
pub mod liveness;
pub mod pipe;
pub mod prepared;
pub mod quic;
pub mod resolver;
pub mod state;
pub mod stats;
pub mod supervisor;
pub mod tls;
pub mod update;

use std::sync::atomic::Ordering;

pub type BoxError = Box<dyn std::error::Error + Send + Sync + 'static>;

/// Default config path used by standalone mode when `--config` is absent.
pub const DEFAULT_CONFIG_PATH: &str = "/etc/guru-worker/config.toml";

/// The process log filter once [`init_tracing`] installed it: the handle that
/// swaps it, and the directive in force.
struct LogFilter {
    handle: tracing_subscriber::reload::Handle<
        tracing_subscriber::EnvFilter,
        tracing_subscriber::Registry,
    >,
    level: parking_lot::Mutex<String>,
}

static LOG_FILTER: std::sync::OnceLock<LogFilter> = std::sync::OnceLock::new();

/// Installs the process logger, filtered by `level` — a `tracing` `EnvFilter`
/// directive — until [`apply_log_level`] swaps the filter.
pub fn init_tracing(level: &str) {
    use tracing_subscriber::layer::SubscriberExt;
    use tracing_subscriber::util::SubscriberInitExt;
    let (filter, handle) =
        tracing_subscriber::reload::Layer::new(tracing_subscriber::EnvFilter::new(level));
    tracing_subscriber::registry()
        .with(filter)
        .with(tracing_subscriber::fmt::layer())
        .init();
    let _ = LOG_FILTER.set(LogFilter {
        handle,
        level: parking_lot::Mutex::new(level.to_owned()),
    });
}

/// Makes `level` the process log filter, without a restart. Every applied config
/// calls it with its `[log] level`, so the level set in the dashboard reaches an
/// agent as soon as its config does: a directive already in force costs nothing,
/// and before [`init_tracing`] ran (the integration tests drive the supervisor
/// without it) there is no filter to swap.
pub fn apply_log_level(level: &str) {
    let Some(filter) = LOG_FILTER.get() else {
        return;
    };
    let mut current = filter.level.lock();
    if *current == level {
        return;
    }
    // Logged under the old filter, which is the one the operator is watching.
    tracing::info!(from = %current, to = level, "switching log level");
    match filter
        .handle
        .reload(tracing_subscriber::EnvFilter::new(level))
    {
        Ok(()) => *current = level.to_owned(),
        Err(error) => tracing::warn!(error = %error, level, "switching log level failed"),
    }
}

/// Logs a config's non-fatal warnings.
///
/// Parsing no longer reports them: `Config::from_toml_str` only parses and validates,
/// so every caller that accepts a config logs its lint output itself.
fn log_lint(cfg: &guru_worker_config::Config) {
    for warning in cfg.lint() {
        tracing::warn!(warning = %warning, "config lint");
    }
}

/// Renders the failed pods of an apply as one error line, or `None` when all applied.
fn apply_failures(outcome: &supervisor::ApplyOutcome) -> Option<String> {
    let failures: Vec<String> = outcome
        .failed()
        .map(|p| format!("{}: {}", p.tag, p.error.as_deref().unwrap_or_default()))
        .collect();
    (!failures.is_empty()).then(|| failures.join("; "))
}

/// Runs the worker until SIGTERM/SIGINT.
///
/// Standalone mode (no `--master`) loads a config file and reloads it on SIGHUP.
/// Agent mode registers with `guru-master` and applies every config revision it streams.
pub async fn run(cli: cli::Cli) -> Result<(), BoxError> {
    let _ = quinn::rustls::crypto::ring::default_provider().install_default();
    match cli.master.clone() {
        None => run_standalone(cli).await,
        Some(master) => run_agent(cli, master).await,
    }
}

async fn run_standalone(cli: cli::Cli) -> Result<(), BoxError> {
    let path = cli
        .config
        .clone()
        .unwrap_or_else(|| std::path::PathBuf::from(DEFAULT_CONFIG_PATH));
    let cfg = guru_worker_config::Config::load(&path)?;
    init_tracing(&cfg.log.level);
    tracing::info!(path = %path.display(), "loaded config");
    log_lint(&cfg);

    let mut sup = supervisor::Supervisor::new();
    // Standalone has nothing to fall back on: a forwarding that cannot start is fatal.
    if let Some(failures) = apply_failures(&sup.apply(&cfg).await) {
        return Err(failures.into());
    }

    use tokio::signal::unix::{SignalKind, signal};
    let mut hup = signal(SignalKind::hangup())?;
    let mut term = signal(SignalKind::terminate())?;
    let mut int = signal(SignalKind::interrupt())?;
    loop {
        tokio::select! {
            _ = hup.recv() => {
                match guru_worker_config::Config::load(&path) {
                    Ok(c) => {
                        log_lint(&c);
                        match apply_failures(&sup.apply(&c).await) {
                            Some(failures) => tracing::error!(
                                error = %failures,
                                "reload applied partially; failed forwardings keep their previous listener"
                            ),
                            None => tracing::info!("config reloaded"),
                        }
                    }
                    Err(e) => tracing::error!(error = %e, "reload failed; keeping running config"),
                }
            }
            _ = term.recv() => break,
            _ = int.recv() => break,
        }
    }
    tracing::info!("shutting down");
    sup.shutdown_all();
    Ok(())
}

/// Reads the operator API key from `GURU_API_KEY` or `--api-key-file`.
///
/// The key never appears on the command line, where `ps` would expose it to every
/// local user. Exactly one of the two sources must be present.
fn read_api_key(file: Option<&std::path::Path>) -> Result<String, BoxError> {
    let from_env = std::env::var(cli::API_KEY_ENV)
        .ok()
        .filter(|key| !key.is_empty());
    match (from_env, file) {
        (Some(_), Some(_)) => Err(format!(
            "agent mode takes the operator API key from either {} or --api-key-file, not both",
            cli::API_KEY_ENV
        )
        .into()),
        (Some(key), None) => Ok(key),
        (None, Some(path)) => {
            let text = std::fs::read_to_string(path)
                .map_err(|e| format!("--api-key-file {}: {e}", path.display()))?;
            let key = text.trim_end().to_string();
            if key.is_empty() {
                return Err(format!("--api-key-file {} is empty", path.display()).into());
            }
            Ok(key)
        }
        (None, None) => Err(format!(
            "agent mode requires the operator API key in {} or --api-key-file",
            cli::API_KEY_ENV
        )
        .into()),
    }
}

/// How often a worker asks for updates when the master's register reply does
/// not say.
const DEFAULT_UPDATE_POLL_SECS: u64 = 60;

async fn run_agent(cli: cli::Cli, master: String) -> Result<(), BoxError> {
    let Some(server_id) = cli.server.clone() else {
        return Err("agent mode requires --server".into());
    };
    let api_key = read_api_key(cli.api_key_file.as_deref())?;
    init_tracing(&cli.log_level);

    // What the worker reports as running: only a successful apply may advance it.
    let applied_revision = std::sync::Arc::new(std::sync::atomic::AtomicI64::new(0));
    let sup = std::sync::Arc::new(tokio::sync::Mutex::new(supervisor::Supervisor::new()));
    // Interrupted certificate swaps are finished first, so the replayed config never
    // compiles half a key pair.
    certs::recover(&cli.state_dir);
    if let Some(good) = state::load(&cli.state_dir) {
        match guru_worker_config::Config::from_toml_str(&good.toml) {
            Ok(mut cfg) => {
                for warning in cfg.lint() {
                    tracing::warn!(revision = good.revision, warning = %warning, "config lint");
                }
                cfg.resolve_paths(&cli.state_dir);
                // The stored config is the mix that was running, so anything less than
                // all of it is not that revision: report `0` and let the master resend.
                match apply_failures(&sup.lock().await.apply(&cfg).await) {
                    None => {
                        applied_revision.store(good.revision, Ordering::Relaxed);
                        tracing::info!(revision = good.revision, "applied last-known-good config")
                    }
                    Some(failures) => {
                        tracing::error!(error = %failures, "last-known-good config applied partially")
                    }
                }
            }
            Err(e) => tracing::error!(error = %e, "last-known-good config is invalid"),
        }
    }

    let shutdown = tokio_util::sync::CancellationToken::new();
    // An installed update ends the process the same way a signal does; systemd
    // starts the new version. A rollback the start guard performed since the
    // last run is reported with the next registration.
    let update_done = std::sync::Arc::new(tokio::sync::Notify::new());
    let agent = tokio::spawn(agent::run(
        agent::AgentOptions {
            master,
            api_key,
            server_id,
            state_dir: cli.state_dir.clone(),
            applied_revision,
            health_interval: std::time::Duration::from_secs(cli.health_interval),
            sources: addresses::Sources {
                ipv4_urls: cli.public_ipv4_urls.clone(),
                ipv6_urls: cli.public_ipv6_urls.clone(),
            },
            update_poll: std::time::Duration::from_secs(DEFAULT_UPDATE_POLL_SECS),
            self_update: !cli.no_self_update,
            update_done: update_done.clone(),
            last_update_error: parking_lot::Mutex::new(update::take_failed()),
            unary_timeout: agent::UNARY_TIMEOUT,
        },
        sup.clone(),
        shutdown.clone(),
    ));

    use tokio::signal::unix::{SignalKind, signal};
    let mut term = signal(SignalKind::terminate())?;
    let mut int = signal(SignalKind::interrupt())?;
    tokio::select! {
        _ = term.recv() => {}
        _ = int.recv() => {}
        _ = update_done.notified() => {
            tracing::info!("update installed; exiting so systemd starts the new version");
        }
    }
    tracing::info!("shutting down");
    shutdown.cancel();
    let _ = agent.await;
    sup.lock().await.shutdown_all();
    Ok(())
}

#[cfg(test)]
mod tests {
    #[test]
    fn a_log_level_without_a_logger_changes_nothing() {
        super::apply_log_level("debug");
        assert!(super::LOG_FILTER.get().is_none());
    }
}
