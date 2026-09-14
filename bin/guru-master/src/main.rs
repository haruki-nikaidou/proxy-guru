//! # `guru-master`
//!
//! The control plane. One binary, four run modes selected with `--mode`:
//!
//! - `dashboard_grpc` — the operator API (`Auth` + `Orchestration`),
//! - `workers_grpc` — the worker API (`WorkerAgent`) plus the config-view poller,
//! - `consumer` — the AMQP derivation hook,
//! - `cron` — periodic jobs: the stale-canvas derivation sweep, the health
//!   liveness sweep and retention, ACME issuance/renewal and relay leaf rotation.
//!
//! Wiring only: every rule lives in the modules under `modules/`.

#![deny(clippy::unwrap_used)]
#![deny(clippy::expect_used)]
#![deny(clippy::panic)]

use amqprs::callbacks::{DefaultChannelCallback, DefaultConnectionCallback};
use amqprs::channel::BasicQosArguments;
use amqprs::connection::{Connection, OpenConnectionArguments};
use auth::config::AuthConfig;
use auth::rpc::{AuthGrpc, AuthLayer};
use auth::services::account::AccountService;
use auth::services::api_key::ApiKeyService;
use auth::services::session::SessionService;
use auth::utils::password::Argon2PasswordAlgorithm;
use base::services::config::{ConfigStore, LoadConfig};
use clap::Parser;
use kanau::processor::Processor;
use orchestration::config::OrchestrationConfig;
use orchestration::events::CanvasDirty;
use orchestration::hooks::derive::{self, CanvasDeriver};
use orchestration::hooks::{acme as acme_hooks, health as health_hooks};
use orchestration::rpc::agent_middleware::AgentLayer;
use orchestration::rpc::{OrchestrationGrpc, WorkerAgentGrpc};
use orchestration::services::acme::{AcmeService, InstantAcmeIssuer};
use orchestration::services::agent::AgentService;
use orchestration::services::ca::CaService;
use orchestration::services::canvas::CanvasService;
use orchestration::services::dns::DnsProviderService;
use orchestration::services::edge::EdgeService;
use orchestration::services::health::HealthService;
use orchestration::services::node::NodeService;
use orchestration::services::rollout::{DirtyNotifier, RolloutService};
use orchestration::services::server::ServerService;
use orchestration::services::watch::{self, SessionLease, WatchHub};
use orchestration::utils::secret::SecretKey;
use rpguru_sdk::auth::auth_server::AuthServer;
use rpguru_sdk::orchestration::orchestration_server::OrchestrationServer;
use rpguru_sdk::orchestration_agent::worker_agent_server::WorkerAgentServer;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use surrealdb::opt::auth::Root;
use tokio_util::sync::CancellationToken;
use tonic::transport::Server;
use tracing_subscriber::EnvFilter;
use wakuwaku::amqp::{AmqpMessageProcessor, AmqpPool, AmqpRouting, setup_consumer};
use wakuwaku::surreal::SurrealProcessor;

#[derive(Debug, Clone, Copy, clap::ValueEnum)]
enum WorkerMode {
    #[value(name = "dashboard_grpc")]
    DashboardGrpc,
    #[value(name = "workers_grpc")]
    WorkersGrpc,
    #[value(name = "consumer")]
    Consumer,
    #[value(name = "cron")]
    Cron,
}

#[derive(Debug, Parser)]
#[command(name = "guru-master", about = "guru control plane")]
struct Cli {
    #[arg(
        long,
        env = "GURU_WORKER_MODE",
        value_enum,
        default_value = "dashboard_grpc"
    )]
    mode: WorkerMode,
    #[arg(
        long,
        env = "GURU_DASHBOARD_GRPC_ADDR",
        default_value = "0.0.0.0:50051"
    )]
    dashboard_addr: SocketAddr,
    #[arg(long, env = "GURU_WORKERS_GRPC_ADDR", default_value = "0.0.0.0:50052")]
    workers_addr: SocketAddr,
    #[arg(long, env = "SURREALDB_HOST", default_value = "ws://127.0.0.1:8000")]
    address: String,
    #[arg(long, env = "SURREALDB_USER", default_value = "root")]
    username: String,
    #[arg(long, env = "SURREALDB_PASSWORD", default_value = "root")]
    password: String,
    #[arg(long, env = "SURREALDB_NAMESPACE")]
    namespace: String,
    #[arg(long, env = "SURREALDB_NAME")]
    database: String,
    #[arg(
        long,
        env = "AMQP_URI",
        help = "Broker URI, e.g. amqp://guru:guru@127.0.0.1:5672/%2f. Required in \
                every mode that talks to the broker: dashboard_grpc, workers_grpc \
                and consumer."
    )]
    amqp_uri: Option<String>,
    // A zero interval panics `tokio::time::interval`, so the range is enforced
    // here: clap applies the parser to the environment variable as well.
    #[arg(
        long,
        env = "GURU_SWEEP_INTERVAL_SECS",
        default_value = "30",
        value_parser = clap::value_parser!(u64).range(1..)
    )]
    sweep_interval_secs: u64,
    #[arg(
        long,
        env = "GURU_WATCH_POLL_MS",
        default_value = "1000",
        value_parser = clap::value_parser!(u64).range(1..)
    )]
    watch_poll_ms: u64,
    #[arg(long, env = "GURU_LOG_LEVEL", default_value = "info")]
    log_level: String,
    #[arg(
        long,
        env = "GURU_ACME_INTERVAL_SECS",
        default_value = "60",
        value_parser = clap::value_parser!(u64).range(1..)
    )]
    acme_interval_secs: u64,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::new(&cli.log_level))
        .init();

    let db = surrealdb::engine::any::connect(&cli.address).await?;
    db.signin(Root {
        username: cli.username.clone(),
        password: cli.password.clone(),
    })
    .await?;
    db.use_ns(&cli.namespace).use_db(&cli.database).await?;
    let db = SurrealProcessor::new(db);
    // Environment only, never argv: the key would otherwise be visible in process
    // listings. `manage-tool generate-master-key` prints a fresh one.
    let secrets = SecretKey::from_env().map_err(|e| format!("master key: {e}"))?;
    // Operator-tunable settings live in the database, so every process in the
    // fleet runs the same values without any matching environment. An unseeded
    // installation reads the defaults; a corrupt row fails startup rather than
    // silently swapping an operator's config for `Default`.
    let configs = ConfigStore { db: db.clone() };
    let auth_config: AuthConfig = configs
        .process(LoadConfig::new())
        .await
        .map_err(|e| e.to_string())?;
    let config: OrchestrationConfig = configs
        .process(LoadConfig::new())
        .await
        .map_err(|e| e.to_string())?;
    tracing::debug!(?auth_config, ?config, "loaded configuration");

    let hasher = Argon2PasswordAlgorithm::default();
    let sessions = SessionService {
        db: db.clone(),
        hasher: hasher.clone(),
        config: auth_config,
    };
    let api_keys = ApiKeyService { db: db.clone() };
    let health = HealthService {
        db: db.clone(),
        config: config.clone(),
    };
    let ca = CaService {
        db: db.clone(),
        secrets: secrets.clone(),
        config: config.clone(),
    };
    let deriver = CanvasDeriver {
        db: db.clone(),
        secrets: secrets.clone(),
        config: config.clone(),
    };

    match cli.mode {
        WorkerMode::DashboardGrpc => {
            let (_connection, notifier) = notifier(cli.amqp_uri.as_deref()).await?;
            let accounts = AccountService {
                db: db.clone(),
                hasher,
            };
            let orchestration = OrchestrationGrpc {
                canvases: CanvasService {
                    db: db.clone(),
                    notifier: notifier.clone(),
                },
                servers: ServerService {
                    db: db.clone(),
                    notifier: notifier.clone(),
                    config: config.clone(),
                },
                nodes: NodeService {
                    db: db.clone(),
                    notifier: notifier.clone(),
                    config: config.clone(),
                },
                edges: EdgeService {
                    db: db.clone(),
                    notifier: notifier.clone(),
                    config: config.clone(),
                },
                rollout: RolloutService {
                    db: db.clone(),
                    notifier: notifier.clone(),
                },
                health,
                dns: DnsProviderService {
                    db: db.clone(),
                    secrets: secrets.clone(),
                },
                certificates: AcmeService {
                    db: db.clone(),
                    secrets,
                    config,
                    notifier,
                    http: reqwest::Client::new(),
                    issuer: Arc::new(InstantAcmeIssuer),
                },
            };
            let auth = AuthGrpc {
                accounts,
                sessions: sessions.clone(),
                api_keys: api_keys.clone(),
            };
            tracing::info!(addr = %cli.dashboard_addr, "serving operator API");
            Server::builder()
                .layer(AuthLayer::new(sessions, api_keys))
                .add_service(AuthServer::new(auth))
                .add_service(OrchestrationServer::new(orchestration))
                .serve_with_shutdown(cli.dashboard_addr, shutdown())
                .await?;
        }
        WorkerMode::WorkersGrpc => {
            let (_connection, notifier) = notifier(cli.amqp_uri.as_deref()).await?;
            let hub = WatchHub::default();
            let lease = SessionLease::default();
            let agents = AgentService {
                db: db.clone(),
                hub: hub.clone(),
                lease,
                notifier,
            };
            let token = CancellationToken::new();
            let poller = tokio::spawn(watch::run_poller(
                hub.clone(),
                db.clone(),
                Duration::from_millis(cli.watch_poll_ms),
                token.clone(),
            ));
            let workers = WorkerAgentGrpc {
                agents: agents.clone(),
                health,
                ca,
                db: db.clone(),
                hub,
                lease,
            };
            tracing::info!(addr = %cli.workers_addr, "serving worker API");
            // `AuthLayer` is required here too: `Register` authenticates with an
            // operator API key before any refresh key exists.
            //
            // Keepalive is load-bearing: a worker that dies without closing its TCP
            // connection would otherwise keep renewing its session lease and lock
            // its replacement out of `Register`.
            Server::builder()
                .http2_keepalive_interval(Some(lease.heartbeat))
                .http2_keepalive_timeout(Some(lease.heartbeat))
                .layer(AuthLayer::new(sessions, api_keys))
                .layer(AgentLayer::new(agents))
                .add_service(WorkerAgentServer::new(workers))
                .serve_with_shutdown(cli.workers_addr, shutdown())
                .await?;
            token.cancel();
            // A panicking poller must not be absorbed: without it no worker ever
            // learns of a new revision, so the process exits non-zero.
            if let Err(error) = poller.await {
                tracing::error!(%error, "the config-view poller task failed");
                return Err(error.into());
            }
        }
        WorkerMode::Consumer => {
            let uri = amqp_uri(cli.amqp_uri.as_deref())?;
            let (connection, pool) = amqp_pool(uri).await?;
            let channel = CanvasDeriver::ensure_queue(&pool).await?;
            channel
                .register_callback(DefaultChannelCallback)
                .await
                .map_err(|e| format!("registering the channel callback failed: {e}"))?;
            // A bounded prefetch keeps one consumer from hoarding the whole backlog
            // while its peers idle; derivation is a database transaction, not a
            // cheap ack.
            channel
                .basic_qos(BasicQosArguments::new(0, 8, false))
                .await
                .map_err(|e| format!("setting the consumer prefetch failed: {e}"))?;
            setup_consumer::<CanvasDirty, CanvasDeriver>(&channel, Arc::new(deriver))
                .await
                .map_err(|e| format!("binding the consumer failed: {e}"))?;
            tracing::info!(queue = CanvasDeriver::QUEUE, "consuming canvas edits");
            // `amqprs` does not reconnect, and a dead consumer in a live process
            // is silent: broker loss ends this mode so the supervisor restarts it.
            let lost = tokio::select! {
                () = shutdown() => false,
                _ = connection.listen_network_io_failure() => true,
            };
            if lost {
                return Err("the AMQP connection was lost: restart once the broker \
                            at AMQP_URI is reachable again"
                    .into());
            }
            drop(channel);
            connection
                .close()
                .await
                .map_err(|e| format!("closing the AMQP connection failed: {e}"))?;
        }
        WorkerMode::Cron => {
            // The broker stays optional here: an issuance touches its canvases, and
            // the sweeper in this very process picks them up; the message only
            // shortens the delay when a broker is configured.
            let (_connection, notifier) = match cli.amqp_uri.as_deref() {
                Some(_) => {
                    let (connection, notifier) = notifier(cli.amqp_uri.as_deref()).await?;
                    (Some(connection), notifier)
                }
                None => (None, DirtyNotifier::default()),
            };
            let token = CancellationToken::new();
            let acme = AcmeService {
                db: db.clone(),
                secrets,
                config,
                notifier,
                http: reqwest::Client::new(),
                issuer: Arc::new(InstantAcmeIssuer),
            };
            let mut jobs = tokio::task::JoinSet::new();
            jobs.spawn(derive::run_sweeper(
                deriver.clone(),
                Duration::from_secs(cli.sweep_interval_secs),
                token.clone(),
            ));
            jobs.spawn(health_hooks::run_liveness_sweep(
                health.clone(),
                Duration::from_secs(30),
                token.clone(),
            ));
            jobs.spawn(health_hooks::run_health_retention(
                health,
                Duration::from_secs(300),
                token.clone(),
            ));
            jobs.spawn(acme_hooks::run_acme_renewal(
                acme,
                Duration::from_secs(cli.acme_interval_secs),
                token.clone(),
            ));
            jobs.spawn(derive::run_relay_cert_rotation(
                deriver,
                Duration::from_secs(3600),
                token.clone(),
            ));
            tracing::info!(
                sweep_interval_secs = cli.sweep_interval_secs,
                acme_interval_secs = cli.acme_interval_secs,
                "running cron worker"
            );
            shutdown().await;
            token.cancel();
            // A panicking job must not be absorbed: the process exits non-zero so
            // the supervisor restarts it.
            while let Some(joined) = jobs.join_next().await {
                if let Err(error) = joined {
                    tracing::error!(%error, "a cron job task failed");
                    return Err(error.into());
                }
            }
        }
    }
    Ok(())
}

/// Opens an AMQP connection and its channel pool, declaring the exchange.
///
/// The connection is returned so the caller can keep it alive: dropping it closes
/// every pooled channel.
async fn amqp_pool(uri: &str) -> Result<(Connection, AmqpPool), Box<dyn std::error::Error>> {
    let args = OpenConnectionArguments::try_from(uri)
        .map_err(|e| format!("parsing AMQP_URI failed: {e}"))?;
    let connection = Connection::open(&args)
        .await
        .map_err(|e| format!("connecting to AMQP failed: {e}"))?;
    connection
        .register_callback(DefaultConnectionCallback)
        .await
        .map_err(|e| format!("registering the AMQP callback failed: {e}"))?;
    let pool = AmqpPool::connect(connection.clone()).await;
    CanvasDirty::ensure_exchange(&pool)
        .await
        .map_err(|e| format!("declaring the orchestration exchange failed: {e}"))?;
    Ok((connection, pool))
}

/// The dirty-canvas notifier for a serving mode. The broker is mandatory: a
/// serving master that cannot publish dirty-canvas events would accept edits
/// nothing derives.
async fn notifier(
    uri: Option<&str>,
) -> Result<(Connection, DirtyNotifier), Box<dyn std::error::Error>> {
    let (connection, pool) = amqp_pool(amqp_uri(uri)?).await?;
    Ok((connection, DirtyNotifier { amqp: Some(pool) }))
}

/// The configured broker URI, or an actionable error: AMQP is not optional.
fn amqp_uri(uri: Option<&str>) -> Result<&str, Box<dyn std::error::Error>> {
    uri.ok_or_else(|| {
        "AMQP is required: set AMQP_URI (or pass --amqp-uri), for example \
         amqp://guru:guru@127.0.0.1:5672/%2f"
            .into()
    })
}

/// Completes on SIGTERM or SIGINT.
async fn shutdown() {
    use tokio::signal::unix::{SignalKind, signal};
    let mut term = match signal(SignalKind::terminate()) {
        Ok(s) => s,
        Err(e) => {
            tracing::error!(error = %e, "cannot listen for SIGTERM");
            return;
        }
    };
    let mut int = match signal(SignalKind::interrupt()) {
        Ok(s) => s,
        Err(e) => {
            tracing::error!(error = %e, "cannot listen for SIGINT");
            return;
        }
    };
    tokio::select! {
        _ = term.recv() => {}
        _ = int.recv() => {}
    }
    tracing::info!("shutting down");
}
