use clap::Parser;
use std::path::PathBuf;

/// Name of the environment variable holding the operator API key.
///
/// Deliberately not a flag: argv is world-readable through `ps`.
pub const API_KEY_ENV: &str = "GURU_API_KEY";

#[derive(Debug, Clone, Parser)]
#[command(
    name = "guru-worker",
    version,
    about = "guru data-plane worker",
    after_help = "Agent mode needs the operator API key in the GURU_API_KEY environment \
                  variable or in the file named by --api-key-file (exactly one of the two). \
                  The key is never accepted on the command line."
)]
pub struct Cli {
    /// Standalone mode: path of the TOML config to load and reload on SIGHUP.
    #[arg(
        short = 'c',
        long,
        env = "GURU_WORKER_CONFIG",
        conflicts_with = "master"
    )]
    pub config: Option<PathBuf>,
    /// Agent mode: `guru-master` worker endpoint — `http://10.0.0.1:50052` on a private
    /// network (plaintext h2c), or `https://guru.example.com` behind a TLS-terminating
    /// proxy (certificate verified against the system roots; SNI is the URI host).
    #[arg(long, env = "GURU_MASTER", requires_all = ["server"])]
    pub master: Option<String>,
    /// File holding the operator API key used once per session to register with the
    /// master; trailing whitespace is trimmed. Alternative to `GURU_API_KEY`.
    #[arg(long, env = "GURU_API_KEY_FILE")]
    pub api_key_file: Option<PathBuf>,
    /// `orchestration_server` record key.
    #[arg(long, env = "GURU_SERVER_ID")]
    pub server: Option<String>,
    #[arg(long, env = "GURU_STATE_DIR", default_value = "/var/lib/guru-worker")]
    pub state_dir: PathBuf,
    /// Agent mode: seconds between two health reports to the master. Only a fallback:
    /// a master that sends `health_report_interval_secs` in its register reply
    /// overrides it for that session.
    #[arg(
        long,
        env = "GURU_HEALTH_INTERVAL_SECS",
        default_value_t = 15,
        value_parser = clap::value_parser!(u64).range(1..)
    )]
    pub health_interval: u64,
    /// Agent mode: comma-separated URLs answering with the caller's public IPv4
    /// as plain text, walked from a rotating start before registering and every
    /// minute after. Empty disables the lookup (interface addresses are still
    /// reported).
    #[arg(
        long,
        env = "GURU_PUBLIC_IPV4_URLS",
        default_value = crate::addresses::DEFAULT_PUBLIC_IPV4_URLS
    )]
    pub public_ipv4_urls: String,
    /// Agent mode: the same for IPv6.
    #[arg(
        long,
        env = "GURU_PUBLIC_IPV6_URLS",
        default_value = crate::addresses::DEFAULT_PUBLIC_IPV6_URLS
    )]
    pub public_ipv6_urls: String,
    /// Agent mode: a URL answering with the two-letter country of the caller's
    /// public address, shown next to the server in the dashboard. Empty disables it.
    #[arg(long, env = "GURU_GEO_URL", default_value = crate::addresses::DEFAULT_GEO_URL)]
    pub geo_url: String,
    #[arg(long, env = "GURU_LOG_LEVEL", default_value = "info")]
    pub log_level: String,
    /// Agent mode: never self-update, even when the dashboard asks. A requested
    /// update is reported back as refused, so the operator sees why.
    #[arg(long, env = "GURU_NO_SELF_UPDATE")]
    pub no_self_update: bool,
}
