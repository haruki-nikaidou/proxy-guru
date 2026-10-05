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
#[cfg(feature = "remote-shell")]
pub mod shell;
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
    let remote_shell = remote_shell(&cli)?;
    match cli.master.clone() {
        None => run_standalone(cli).await,
        Some(master) => run_agent(cli, master, remote_shell).await,
    }
}

/// The remote shell this worker serves, when its host opted in with
/// `--remote-shell`. An opt-in that cannot be honoured stops the worker instead of
/// being ignored: standalone mode (nothing to reach the shell through), a plaintext
/// master without `--remote-shell-allow-plaintext`, or no `bash` on `PATH`.
#[cfg(feature = "remote-shell")]
fn remote_shell(cli: &cli::Cli) -> Result<Option<agent::RemoteShell>, BoxError> {
    if !cli.remote_shell {
        return Ok(None);
    }
    let Some(master) = cli.master.as_deref() else {
        return Err(
            "--remote-shell needs agent mode (--master): the dashboard reaches a \
             shell only through the master"
                .into(),
        );
    };
    let scheme = master
        .parse::<tonic::codegen::http::Uri>()
        .map_err(|e| format!("--master {master}: {e}"))?;
    if scheme.scheme_str() != Some("https") && !cli.remote_shell_allow_plaintext {
        return Err(format!(
            "--remote-shell with a plaintext master ({master}) would send commands and their \
             output unencrypted; use an https:// master or pass --remote-shell-allow-plaintext"
        )
        .into());
    }
    let bash = shell::find_bash(std::env::var_os("PATH").as_deref())?;
    Ok(Some(shell::ShellTable::new(shell::ShellSettings {
        bash,
        buffer_bytes: usize::try_from(cli.remote_shell_buffer_bytes).unwrap_or(usize::MAX),
        idle_timeout: std::time::Duration::from_secs(cli.remote_shell_idle_timeout),
        max_sessions: usize::try_from(cli.remote_shell_max_sessions).unwrap_or(usize::MAX),
    })))
}

/// A build without the `remote-shell` feature refuses the opt-in rather than
/// ignoring it.
#[cfg(not(feature = "remote-shell"))]
fn remote_shell(cli: &cli::Cli) -> Result<Option<agent::RemoteShell>, BoxError> {
    if cli.remote_shell {
        return Err(
            "--remote-shell: this guru-worker was built without the `remote-shell` \
             feature; use an official build or rebuild with default features"
                .into(),
        );
    }
    Ok(None)
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

async fn run_agent(
    cli: cli::Cli,
    master: String,
    remote_shell: Option<agent::RemoteShell>,
) -> Result<(), BoxError> {
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
    #[cfg(feature = "remote-shell")]
    let shells = remote_shell.clone();
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
            remote_shell,
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
    // Shells first, while the channel may still carry their end to the dashboard.
    #[cfg(feature = "remote-shell")]
    if let Some(shells) = shells {
        shells.shutdown().await;
    }
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

    type TestResult = Result<(), super::BoxError>;

    fn cli(args: &[&str]) -> Result<super::cli::Cli, clap::Error> {
        use clap::Parser;
        super::cli::Cli::try_parse_from(std::iter::once("guru-worker").chain(args.iter().copied()))
    }

    /// Why the remote shell was refused, or `None` when it was accepted.
    fn refusal(args: &[&str]) -> Result<Option<String>, clap::Error> {
        Ok(super::remote_shell(&cli(args)?)
            .err()
            .map(|e| e.to_string()))
    }

    #[tokio::test]
    async fn the_remote_shell_is_off_unless_asked_for() -> TestResult {
        let agent = ["--master", "http://10.0.0.1:50052", "--server", "s"];
        assert!(matches!(super::remote_shell(&cli(&agent)?), Ok(None)));
        assert!(matches!(super::remote_shell(&cli(&[])?), Ok(None)));
        Ok(())
    }

    #[cfg(feature = "remote-shell")]
    #[tokio::test]
    async fn an_opt_in_that_cannot_be_honoured_stops_the_worker() -> TestResult {
        let standalone = refusal(&["--remote-shell"])?;
        assert!(
            standalone
                .as_deref()
                .is_some_and(|e| e.contains("agent mode")),
            "{standalone:?}"
        );
        let plaintext = refusal(&[
            "--remote-shell",
            "--master",
            "http://10.0.0.1:50052",
            "--server",
            "s",
        ])?;
        assert!(
            plaintext
                .as_deref()
                .is_some_and(|e| e.contains("--remote-shell-allow-plaintext")),
            "{plaintext:?}"
        );
        // With bash on PATH, as on every test machine.
        for accepted in [
            &[
                "--remote-shell",
                "--master",
                "https://guru.example.com",
                "--server",
                "s",
            ][..],
            &[
                "--remote-shell",
                "--remote-shell-allow-plaintext",
                "--master",
                "http://10.0.0.1:50052",
                "--server",
                "s",
            ],
        ] {
            assert!(
                matches!(super::remote_shell(&cli(accepted)?), Ok(Some(_))),
                "{accepted:?}"
            );
        }
        Ok(())
    }

    #[cfg(not(feature = "remote-shell"))]
    #[tokio::test]
    async fn a_build_without_the_feature_refuses_the_opt_in() -> TestResult {
        let refused = refusal(&[
            "--remote-shell",
            "--master",
            "https://guru.example.com",
            "--server",
            "s",
        ])?;
        assert!(
            refused
                .as_deref()
                .is_some_and(|e| e.contains("`remote-shell` feature")),
            "{refused:?}"
        );
        Ok(())
    }

    #[test]
    fn the_remote_shell_settings_keep_their_floors() -> TestResult {
        for (flag, below) in [
            ("--remote-shell-buffer-bytes", "4095"),
            ("--remote-shell-idle-timeout", "0"),
            ("--remote-shell-max-sessions", "0"),
        ] {
            assert!(cli(&[flag, below]).is_err(), "{flag} {below}");
        }
        let defaults = cli(&[])?;
        assert_eq!(
            (
                defaults.remote_shell,
                defaults.remote_shell_buffer_bytes,
                defaults.remote_shell_idle_timeout,
                defaults.remote_shell_max_sessions,
                defaults.remote_shell_allow_plaintext,
            ),
            (false, 1_048_576, 1800, 4, false)
        );
        Ok(())
    }
}
