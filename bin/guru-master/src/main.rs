//! # `guru-master`
//!
//! The control plane. One binary, five run modes selected with `--mode`:
//!
//! - `dashboard_grpc` — the operator API (`Auth` + `Orchestration` + `Notify`),
//!   including the live `Watch*` streams, which it feeds from Redis pub/sub so a
//!   change made on one replica reaches the dashboards attached to the others,
//!   and the remote-shell calls, relayed over Redis to the replica holding the
//!   worker's `ShellChannel`,
//! - `workers_grpc` — the worker API (`WorkerAgent`) plus the config-view poller
//!   and the remote-shell relay's worker-facing end,
//! - `consumer` — the Iggy event handlers: the derivation of dirty canvases and
//!   the notification fan-out,
//! - `notifier` — the notification *delivery*: exactly one instance, which it
//!   enforces with a PostgreSQL advisory lock,
//! - `cron` — every periodic job, as in-process wakuwaku `CronJob`s; each run
//!   claims its tick in the database, so several cron processes run a pass once.
//!
//! Every mode needs PostgreSQL (`GURU_DATABASE_URL`) and Iggy (`IGGY_URL`); all
//! but `notifier` also need Redis (`REDIS_URL`) and the master key. The stream
//! and topics are created at startup (`base::events::ensure_topology`).
//!
//! Wiring only: every rule lives in the modules under `modules/`.

#![deny(clippy::unwrap_used)]
#![deny(clippy::expect_used)]
#![deny(clippy::panic)]

use auth::config::AuthConfig;
use auth::rpc::{AuthGrpc, AuthLayer};
use auth::services::account::AccountService;
use auth::services::api_key::ApiKeyService;
use auth::services::config::AuthConfigService;
use auth::services::session::SessionService;
use auth::utils::password::Argon2PasswordAlgorithm;
use base::db::{Db, PoolSettings};
use base::events::{STREAM, all_partitions};
use base::services::config::{ConfigStore, LoadConfig};
use clap::Parser;
use kanau::processor::Processor;
use notify::config::NotifyConfig;
use notify::events::{HealthNotifyGroupEvent, HealthNotifyPersonalEvent};
use notify::hooks::delivery::NoticeDelivery;
use notify::hooks::fanout::HealthFanout;
use notify::rpc::NotifyGrpc;
use notify::services::config::NotifyConfigService;
use notify::services::delivery::DeliveryService;
use notify::services::fanout::{FanoutService, NoticePublisher};
use notify::services::setting::SettingService;
use notify::utils::lock::hold_notifier_lock;
use notify::utils::secret::NotifySecrets;
use orchestration::config::OrchestrationConfig;
use orchestration::events::{CanvasDirty, HealthChanged};
use orchestration::hooks::acme::RenewCertificatesJob;
use orchestration::hooks::country::ResolveServerCountriesJob;
use orchestration::hooks::derive::{
    CanvasDeriver, CanvasDirtyHandler, DeriveStaleCanvasesJob, RotateRelayCertificatesJob,
};
use orchestration::hooks::health::{SweepLivenessJob, TrimHealthHistoryJob};
use orchestration::hooks::live::{LiveBus, run_redis_subscriber};
use orchestration::hooks::shell::{
    ShellRouter, run_shell_down_subscriber, run_shell_up_subscriber,
};
use orchestration::rpc::agent_middleware::AgentLayer;
use orchestration::rpc::{OrchestrationGrpc, WorkerAgentGrpc};
use orchestration::services::acme::{AcmeService, InstantAcmeIssuer};
use orchestration::services::agent::AgentService;
use orchestration::services::ca::CaService;
use orchestration::services::canvas::CanvasService;
use orchestration::services::config::OrchestrationConfigService;
use orchestration::services::country::CountryService;
use orchestration::services::dns::DnsProviderService;
use orchestration::services::graph::GraphService;
use orchestration::services::health::HealthService;
use orchestration::services::live::LiveService;
use orchestration::services::notify::{LivePublisher, Notifier};
use orchestration::services::rollout::RolloutService;
use orchestration::services::server::ServerService;
use orchestration::services::shell::{ShellDownPublisher, ShellService, ShellTimings};
use orchestration::services::shell_channel::{ShellChannelHub, ShellChannels, ShellUpPublisher};
use orchestration::services::watch::{self, SessionLease, WatchHub};
use orchestration::utils::secret::SecretKey;
use rpguru_sdk::auth::auth_server::AuthServer;
use rpguru_sdk::notify::notify_server::NotifyServer;
use rpguru_sdk::orchestration::orchestration_server::OrchestrationServer;
use rpguru_sdk::orchestration_agent::worker_agent_server::WorkerAgentServer;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio_util::sync::CancellationToken;
use tonic::service::Routes;
use tonic::transport::Server;
use tracing_subscriber::EnvFilter;
use wakuwaku::deps::iggy::prelude::IggyClient;
use wakuwaku::iggy::{ConsumerConfig, Publisher};
use wakuwaku::{CronJob, CronJobInterval, ServiceBuilder};

/// How long `--mode cron` waits for the runs in progress at shutdown before it
/// aborts them: inside systemd's `TimeoutStopSec=30s`, so the process exits on
/// its own terms.
const CRON_DRAIN: Duration = Duration::from_secs(20);

#[derive(Debug, Clone, Copy, clap::ValueEnum)]
enum WorkerMode {
    #[value(name = "dashboard_grpc")]
    DashboardGrpc,
    #[value(name = "workers_grpc")]
    WorkersGrpc,
    #[value(name = "consumer")]
    Consumer,
    /// Notification delivery. Run exactly one: the mode takes a PostgreSQL
    /// advisory lock and refuses to start beside a peer that holds it.
    #[value(name = "notifier")]
    Notifier,
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
    /// PostgreSQL connection URL, e.g. `postgres://guru:secret@127.0.0.1:15432/guru`.
    // Optional at parse time so a missing value fails with a message that says
    // what to do, rather than clap's generic one; every mode opens the database.
    #[arg(long, env = "GURU_DATABASE_URL")]
    database_url: Option<String>,
    #[arg(
        long,
        env = "IGGY_URL",
        help = "Iggy server, e.g. iggy://iggy:iggy@127.0.0.1:8090. Required in every mode."
    )]
    iggy_url: Option<String>,
    #[arg(
        long,
        env = "GURU_IGGY_PARTITIONS",
        value_delimiter = ',',
        help = "Partitions this consumer reads (consumer mode only); empty = all. \
                Replicas need disjoint sets."
    )]
    iggy_partitions: Vec<u32>,
    #[arg(
        long,
        env = "REDIS_URL",
        help = "Redis URL, e.g. redis://127.0.0.1:6379/. Required in every mode \
                but notifier: it carries the live dashboard events between replicas."
    )]
    redis_url: Option<String>,
    // A zero interval panics `tokio::time::interval`, so the range is enforced
    // here: clap applies the parser to the environment variable as well.
    #[arg(
        long,
        env = "GURU_WATCH_POLL_MS",
        default_value = "1000",
        value_parser = clap::value_parser!(u64).range(1..)
    )]
    watch_poll_ms: u64,
    /// The server-side bound on one database statement (`statement_timeout`). It cannot
    /// live in `app_config` like the operator's other settings: reading that config goes
    /// through the very connection being configured.
    #[arg(
        long,
        env = "GURU_DB_STATEMENT_TIMEOUT_MS",
        default_value = "5000",
        value_parser = clap::value_parser!(u64).range(1..)
    )]
    db_statement_timeout_ms: u64,
    /// Connections this process may hold at once.
    #[arg(
        long,
        env = "GURU_DB_POOL_SIZE",
        default_value = "10",
        value_parser = clap::value_parser!(u32).range(1..)
    )]
    db_pool_size: u32,
    #[arg(long, env = "GURU_LOG_LEVEL", default_value = "info")]
    log_level: String,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // A workspace build that also compiles `guru-worker` unifies rustls to both the
    // `aws-lc-rs` and `ring` providers; `instant-acme` then panics in
    // `ClientConfig::builder()` unless one is installed process-wide.
    let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
    let cli = Cli::parse();
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::new(&cli.log_level))
        .init();

    // An empty value counts as unset: an exported-but-empty `GURU_DATABASE_URL`
    // reaches clap as `Some("")` and would fail deep in the driver instead of
    // here, where the message can say what to do.
    let Some(database_url) = cli.database_url.as_deref().filter(|v| !v.is_empty()) else {
        return Err(
            "every mode opens the database: set GURU_DATABASE_URL (or pass --database-url)".into(),
        );
    };
    let pool = base::db::connect(
        database_url,
        PoolSettings {
            statement_timeout: Duration::from_millis(cli.db_statement_timeout_ms),
            max_connections: cli.db_pool_size,
            application_name: "guru-master",
        },
    )
    .await?;
    // Every instance runs the migrations at startup; the migrator takes an
    // advisory lock, so four instances starting together apply them once.
    base::db::MIGRATOR.run(&pool).await?;
    let db = base::db::handle(pool.clone());
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
    let notify_config: NotifyConfig = configs
        .process(LoadConfig::new())
        .await
        .map_err(|e| e.to_string())?;
    tracing::debug!(
        ?auth_config,
        ?config,
        ?notify_config,
        "loaded configuration"
    );

    // Iggy is opened, and its topology ensured, by every mode: the serving modes
    // publish, `consumer` and `notifier` consume, and every topic must exist
    // before the first publish or the first consumer starts on it.
    let iggy = base::events::connect(iggy_url(cli.iggy_url.as_deref())?)
        .await
        .map_err(|e| format!("connecting to Iggy failed: {e}"))?;
    let topics: Vec<&str> = orchestration::events::TOPICS
        .into_iter()
        .chain(notify::events::TOPICS)
        .collect();
    base::events::ensure_topology(&iggy, &topics)
        .await
        .map_err(|e| format!("creating the Iggy stream and topics failed: {e}"))?;

    // Dispatched before the master key and the live bus: delivery decrypts
    // nothing and publishes nothing, so handing a notifier host the key that
    // opens every stored secret would widen the blast radius for no gain.
    if let WorkerMode::Notifier = cli.mode {
        return run_notifier(&pool, iggy, notify_config).await;
    }

    // Environment only, never argv: the key would otherwise be visible in process
    // listings. `manage-tool generate-master-key` prints a fresh one.
    let secrets = SecretKey::from_env().map_err(|e| format!("master key: {e}"))?;

    // The event publisher and Redis are opened once for every remaining mode:
    // all of them publish dirty-canvas hints or health facts, and all of them
    // publish live events (a derivation pass moves what a dashboard renders just
    // as much as an operator's edit does). The two gRPC modes also relay
    // remote-shell traffic over the same Redis.
    let events = Publisher::new(iggy.clone(), STREAM)?;
    let redis = redis::Client::open(redis_url(cli.redis_url.as_deref())?)?;
    let live_publisher = redis::aio::ConnectionManager::new(redis.clone()).await?;
    let notifier = Notifier {
        events: Some(events.clone()),
        live: Some(LivePublisher::Redis(live_publisher.clone())),
    };

    // The values every mode builds its services from. Each mode extends this
    // one builder with the services and components it runs.
    let shared = ServiceBuilder::new(db.clone())
        .push(configs)
        .push(auth_config)
        .push(config)
        .push(notify_config)
        .push(secrets)
        .push(notifier)
        .push(events)
        .push(Argon2PasswordAlgorithm::default());

    match cli.mode {
        WorkerMode::DashboardGrpc => {
            let bus = LiveBus::new();
            let live_token = CancellationToken::new();
            let subscriber = tokio::spawn(run_redis_subscriber(
                redis.clone(),
                bus.clone(),
                live_token.clone(),
            ));
            // This replica's end of the shell relay: workers' answers come back
            // on its own channel, named by the router's random replica id.
            let router = ShellRouter::new();
            let shell_subscriber = tokio::spawn(run_shell_up_subscriber(
                redis.clone(),
                router.clone(),
                live_token.clone(),
            ));
            let (services, add_grpc) = shared
                .compose_subservice(|s| SessionService {
                    db: s.provide::<Db, _>().clone(),
                    hasher: s.provide::<Argon2PasswordAlgorithm, _>().clone(),
                    config: s.provide::<AuthConfig, _>().clone(),
                })
                .compose_subservice(|s| ApiKeyService {
                    db: s.provide::<Db, _>().clone(),
                })
                .compose_grpc_service(|s| {
                    AuthServer::new(AuthGrpc {
                        accounts: AccountService {
                            db: s.provide::<Db, _>().clone(),
                            hasher: s.provide::<Argon2PasswordAlgorithm, _>().clone(),
                        },
                        sessions: s.provide::<SessionService, _>().clone(),
                        api_keys: s.provide::<ApiKeyService, _>().clone(),
                        configs: AuthConfigService {
                            configs: s.provide::<ConfigStore, _>().clone(),
                        },
                    })
                })
                .compose_grpc_service(|s| {
                    let db = s.provide::<Db, _>();
                    let config = s.provide::<OrchestrationConfig, _>();
                    let notifier = s.provide::<Notifier, _>();
                    let secrets = s.provide::<SecretKey, _>();
                    OrchestrationServer::new(OrchestrationGrpc {
                        canvases: CanvasService {
                            db: db.clone(),
                            notifier: notifier.clone(),
                        },
                        servers: ServerService {
                            db: db.clone(),
                            notifier: notifier.clone(),
                            config: config.clone(),
                        },
                        graph: GraphService {
                            db: db.clone(),
                            notifier: notifier.clone(),
                            config: config.clone(),
                        },
                        rollout: RolloutService {
                            db: db.clone(),
                            notifier: notifier.clone(),
                        },
                        health: HealthService {
                            db: db.clone(),
                            config: config.clone(),
                            notifier: notifier.clone(),
                        },
                        dns: DnsProviderService {
                            db: db.clone(),
                            secrets: secrets.clone(),
                        },
                        certificates: AcmeService {
                            db: db.clone(),
                            secrets: secrets.clone(),
                            config: config.clone(),
                            notifier: notifier.clone(),
                            http: reqwest::Client::new(),
                            issuer: Arc::new(InstantAcmeIssuer),
                        },
                        // The same store the startup read above used: the
                        // dashboard hands an Admin the row itself, and the
                        // value it writes is what the next restart loads.
                        configs: OrchestrationConfigService {
                            configs: s.provide::<ConfigStore, _>().clone(),
                        },
                        live: LiveService::new(db.clone(), bus, config.clone()),
                        sessions: s.provide::<SessionService, _>().clone(),
                        shell: ShellService {
                            db: db.clone(),
                            router,
                            down: ShellDownPublisher::Redis(live_publisher),
                            timings: ShellTimings::default(),
                        },
                    })
                })
                // Settings only: the dashboard publishes a test notice on the
                // same topic the fan-out does, and the notifier delivers it.
                .compose_grpc_service(|s| {
                    NotifyServer::new(NotifyGrpc {
                        settings: SettingService {
                            db: s.provide::<Db, _>().clone(),
                            config: s.provide::<NotifyConfig, _>().clone(),
                        },
                        fanout: FanoutService {
                            db: s.provide::<Db, _>().clone(),
                            publisher: NoticePublisher::Iggy(s.provide::<Publisher, _>().clone()),
                        },
                        configs: NotifyConfigService {
                            configs: s.provide::<ConfigStore, _>().clone(),
                        },
                    })
                })
                .into_router_fn();
            let auth_layer = AuthLayer::new(
                services.provide::<SessionService, _>().clone(),
                services.provide::<ApiKeyService, _>().clone(),
            );
            tracing::info!(addr = %cli.dashboard_addr, "serving operator API");
            // Keepalive is load-bearing for the `Watch*` streams: a browser or
            // bridge that dies without closing its TCP connection would
            // otherwise hold a shared view open forever.
            let router = Server::builder()
                .http2_keepalive_interval(Some(Duration::from_secs(30)))
                .http2_keepalive_timeout(Some(Duration::from_secs(20)))
                .layer(auth_layer)
                .add_routes(Routes::default());
            add_grpc(router)
                .serve_with_shutdown(cli.dashboard_addr, shutdown())
                .await?;
            live_token.cancel();
            // A panicking subscriber must not be absorbed: without it every open
            // dashboard silently stops updating, so the process exits non-zero.
            if let Err(error) = subscriber.await {
                tracing::error!(%error, "the live bus subscriber task failed");
                return Err(error.into());
            }
            // Likewise the shell relay: without it every shell call times out.
            if let Err(error) = shell_subscriber.await {
                tracing::error!(%error, "the shell relay subscriber task failed");
                return Err(error.into());
            }
        }
        WorkerMode::WorkersGrpc => {
            let hub = WatchHub::default();
            let lease = SessionLease::default();
            let token = CancellationToken::new();
            let poller = tokio::spawn(watch::run_poller(
                hub.clone(),
                db.clone(),
                Duration::from_millis(cli.watch_poll_ms),
                token.clone(),
            ));
            // The worker-facing end of the shell relay: dashboard replicas
            // publish down, the replica holding a worker's channel forwards.
            let shells = ShellChannels {
                db: db.clone(),
                hub: ShellChannelHub::default(),
                up: ShellUpPublisher::Redis(live_publisher),
            };
            let shell_subscriber = tokio::spawn(run_shell_down_subscriber(
                redis.clone(),
                shells.clone(),
                token.clone(),
            ));
            let (services, add_grpc) = shared
                .compose_subservice(|s| SessionService {
                    db: s.provide::<Db, _>().clone(),
                    hasher: s.provide::<Argon2PasswordAlgorithm, _>().clone(),
                    config: s.provide::<AuthConfig, _>().clone(),
                })
                .compose_subservice(|s| ApiKeyService {
                    db: s.provide::<Db, _>().clone(),
                })
                .compose_subservice(|s| AgentService {
                    db: s.provide::<Db, _>().clone(),
                    hub: hub.clone(),
                    lease,
                    notifier: s.provide::<Notifier, _>().clone(),
                    config: s.provide::<OrchestrationConfig, _>().clone(),
                })
                .compose_subservice(|s| HealthService {
                    db: s.provide::<Db, _>().clone(),
                    config: s.provide::<OrchestrationConfig, _>().clone(),
                    notifier: s.provide::<Notifier, _>().clone(),
                })
                .compose_subservice(|s| CaService {
                    db: s.provide::<Db, _>().clone(),
                    secrets: s.provide::<SecretKey, _>().clone(),
                    config: s.provide::<OrchestrationConfig, _>().clone(),
                })
                .compose_grpc_service(|s| {
                    WorkerAgentServer::new(WorkerAgentGrpc {
                        agents: s.provide::<AgentService, _>().clone(),
                        health: s.provide::<HealthService, _>().clone(),
                        ca: s.provide::<CaService, _>().clone(),
                        db: s.provide::<Db, _>().clone(),
                        hub,
                        lease,
                        shells,
                    })
                })
                .into_router_fn();
            tracing::info!(addr = %cli.workers_addr, "serving worker API");
            // `AuthLayer` is required here too: `Register` authenticates with an
            // operator API key before any refresh key exists.
            //
            // Keepalive catches a worker that dies without closing its TCP
            // connection when the master faces it directly. Behind the documented
            // TLS-terminating proxy the pings are answered by the proxy, so the
            // lease also lets go whenever the health pipeline marks the server
            // offline (`set_server_health_status.surql`).
            let router = Server::builder()
                .http2_keepalive_interval(Some(lease.heartbeat))
                .http2_keepalive_timeout(Some(lease.heartbeat))
                .layer(AuthLayer::new(
                    services.provide::<SessionService, _>().clone(),
                    services.provide::<ApiKeyService, _>().clone(),
                ))
                .layer(AgentLayer::new(services.provide::<AgentService, _>().clone()))
                .add_routes(Routes::default());
            add_grpc(router)
                .serve_with_shutdown(cli.workers_addr, shutdown())
                .await?;
            token.cancel();
            // A panicking poller must not be absorbed: without it no worker ever
            // learns of a new revision, so the process exits non-zero.
            if let Err(error) = poller.await {
                tracing::error!(%error, "the config-view poller task failed");
                return Err(error.into());
            }
            if let Err(error) = shell_subscriber.await {
                tracing::error!(%error, "the shell relay subscriber task failed");
                return Err(error.into());
            }
        }
        WorkerMode::Consumer => {
            // Each replica reads the partitions it is given; replicas sharing a
            // consumer name need disjoint sets, or they race on the offsets.
            let partitions = if cli.iggy_partitions.is_empty() {
                all_partitions()
            } else {
                cli.iggy_partitions.clone()
            };
            // One consumer per topic: a consumer reads exactly one main topic.
            let (services, canvas) = shared
                .compose_subservice(|s| CanvasDeriver {
                    db: s.provide::<Db, _>().clone(),
                    secrets: s.provide::<SecretKey, _>().clone(),
                    config: s.provide::<OrchestrationConfig, _>().clone(),
                    notifier: s.provide::<Notifier, _>().clone(),
                })
                .compose_subservice(|s| FanoutService {
                    db: s.provide::<Db, _>().clone(),
                    publisher: NoticePublisher::Iggy(s.provide::<Publisher, _>().clone()),
                })
                .iggy_consumer::<CanvasDirtyHandler, CanvasDirty, _>()
                .start(
                    iggy.clone(),
                    ConsumerConfig::new(
                        STREAM,
                        "guru_orchestration_canvas_dirty",
                        partitions.clone(),
                    ),
                )
                .await
                .map_err(|e| format!("starting the canvas_dirty consumer failed: {e}"))?;
            let (_, health_facts) = services
                .iggy_consumer::<HealthFanout, HealthChanged, _>()
                .start(
                    iggy,
                    ConsumerConfig::new(STREAM, "guru_notify_health_changed", partitions),
                )
                .await
                .map_err(|e| format!("starting the health_changed consumer failed: {e}"))?;
            tracing::info!("consuming");
            shutdown().await;
            canvas.shutdown().await;
            health_facts.shutdown().await;
        }
        WorkerMode::Cron => {
            // Every job runs in this process, once at start and then on its own
            // interval; each run claims its tick, so a second cron process is a
            // standby, not a duplicate.
            let (_, cron) = shared
                .compose_subservice(|s| CanvasDeriver {
                    db: s.provide::<Db, _>().clone(),
                    secrets: s.provide::<SecretKey, _>().clone(),
                    config: s.provide::<OrchestrationConfig, _>().clone(),
                    notifier: s.provide::<Notifier, _>().clone(),
                })
                .compose_subservice(|s| HealthService {
                    db: s.provide::<Db, _>().clone(),
                    config: s.provide::<OrchestrationConfig, _>().clone(),
                    notifier: s.provide::<Notifier, _>().clone(),
                })
                .compose_subservice(|s| AcmeService {
                    db: s.provide::<Db, _>().clone(),
                    secrets: s.provide::<SecretKey, _>().clone(),
                    config: s.provide::<OrchestrationConfig, _>().clone(),
                    notifier: s.provide::<Notifier, _>().clone(),
                    http: reqwest::Client::new(),
                    issuer: Arc::new(InstantAcmeIssuer),
                })
                .compose_subservice(|s| CountryService {
                    db: s.provide::<Db, _>().clone(),
                    config: s.provide::<OrchestrationConfig, _>().clone(),
                    http: reqwest::Client::new(),
                })
                .cron_job::<DeriveStaleCanvasesJob, _>()
                .cron_job::<RotateRelayCertificatesJob, _>()
                .cron_job::<SweepLivenessJob, _>()
                .cron_job::<TrimHealthHistoryJob, _>()
                .cron_job::<RenewCertificatesJob, _>()
                .cron_job::<ResolveServerCountriesJob, _>()
                .start();
            tracing::info!(
                derive_stale_canvases_secs = interval_secs(DeriveStaleCanvasesJob::INTERVAL),
                rotate_relay_certificates_secs =
                    interval_secs(RotateRelayCertificatesJob::INTERVAL),
                sweep_liveness_secs = interval_secs(SweepLivenessJob::INTERVAL),
                trim_health_history_secs = interval_secs(TrimHealthHistoryJob::INTERVAL),
                renew_certificates_secs = interval_secs(RenewCertificatesJob::INTERVAL),
                resolve_server_countries_secs =
                    interval_secs(ResolveServerCountriesJob::INTERVAL),
                "running periodic jobs"
            );
            shutdown().await;
            if tokio::time::timeout(CRON_DRAIN, cron.shutdown())
                .await
                .is_err()
            {
                tracing::warn!(
                    drain_secs = CRON_DRAIN.as_secs(),
                    "cron jobs did not finish within the drain timeout; aborted"
                );
            }
        }
        // Dispatched above, before the master key and the live bus: delivery
        // needs neither. Reaching it would mean that early return was removed,
        // which is a wiring bug, not a mode.
        WorkerMode::Notifier => {
            return Err("notifier is dispatched before the master key: \
                        the early return in main was lost"
                .into());
        }
    }
    Ok(())
}

/// `--mode notifier`: the one instance that delivers notices.
///
/// It holds no secret but the channels' own credentials, publishes nothing, and
/// subscribes to no live bus — so it takes neither `GURU_MASTER_KEY` nor
/// `REDIS_URL`, only the database (for the single-instance lock and the
/// `notify` config it was handed) and Iggy.
async fn run_notifier(
    pool: &sqlx::PgPool,
    iggy: Arc<IggyClient>,
    config: NotifyConfig,
) -> Result<(), Box<dyn std::error::Error>> {
    // Exactly one instance delivers: two would double every notice a restart
    // overlapped. The lock lives as long as this connection, so a killed
    // notifier does not block its successor.
    let _lock = hold_notifier_lock(pool).await.map_err(|e| e.to_string())?;
    let secrets = NotifySecrets::from_env();
    let delivery = DeliveryService::new(&config, &secrets).map_err(|e| e.to_string())?;
    for channel in delivery.channels() {
        tracing::info!(channel, "notification channel");
    }
    // Both notices share one topic; the handler is registered once per event.
    let (_, runtime) = ServiceBuilder::new(delivery)
        .iggy_consumer::<NoticeDelivery, HealthNotifyGroupEvent, _>()
        .iggy_consumer::<NoticeDelivery, HealthNotifyPersonalEvent, _>()
        .start(
            iggy,
            ConsumerConfig::new(STREAM, "guru_notify_health_notice", all_partitions()),
        )
        .await
        .map_err(|e| format!("starting the notice consumer failed: {e}"))?;
    tracing::info!("consuming");
    shutdown().await;
    runtime.shutdown().await;
    Ok(())
}

/// A job's interval in seconds, for the startup log. `CronJobInterval` keeps
/// its own `duration()` private, so every variant is matched here.
fn interval_secs(interval: CronJobInterval) -> u64 {
    match interval {
        CronJobInterval::AfterFinish(d)
        | CronJobInterval::AtLeast(d)
        | CronJobInterval::AfterStart(d) => d.as_secs(),
    }
}

/// The configured Redis URL, or an actionable error: the live bus is not
/// optional either. A serving master without it would accept edits whose
/// events never reach the dashboards attached to the other replicas.
fn redis_url(url: Option<&str>) -> Result<&str, Box<dyn std::error::Error>> {
    url.ok_or_else(|| {
        "Redis is required: set REDIS_URL (or pass --redis-url), for example \
         redis://127.0.0.1:6379/"
            .into()
    })
}

/// The configured Iggy URL, or an actionable error: Iggy is not optional. Every
/// serving mode publishes the dirty-canvas hint, and the consumers read it.
fn iggy_url(url: Option<&str>) -> Result<&str, Box<dyn std::error::Error>> {
    url.filter(|v| !v.is_empty()).ok_or_else(|| {
        "Iggy is required: set IGGY_URL (or pass --iggy-url), for example \
         iggy://iggy:iggy@127.0.0.1:8090"
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
