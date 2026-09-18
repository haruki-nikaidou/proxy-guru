//! # `guru-master`
//!
//! The control plane. One binary, five run modes selected with `--mode`:
//!
//! - `dashboard_grpc` — the operator API (`Auth` + `Orchestration` + `Notify`),
//!   including the live `Watch*` streams, which it feeds from Redis pub/sub so a
//!   change made on one replica reaches the dashboards attached to the others,
//! - `workers_grpc` — the worker API (`WorkerAgent`) plus the config-view poller,
//! - `consumer` — the AMQP derivation hook, every periodic job, and the
//!   notification fan-out,
//! - `notifier` — the notification *delivery*: exactly one instance, which it
//!   enforces with a PostgreSQL advisory lock,
//! - `cron` — the scheduler: it publishes one execution signal per due job and
//!   opens neither a database connection nor the master key.
//!
//! Wiring only: every rule lives in the modules under `modules/`.

#![deny(clippy::unwrap_used)]
#![deny(clippy::expect_used)]
#![deny(clippy::panic)]

use amqprs::callbacks::{DefaultChannelCallback, DefaultConnectionCallback};
use amqprs::channel::{BasicQosArguments, Channel};
use amqprs::connection::{Connection, OpenConnectionArguments};
use auth::config::AuthConfig;
use auth::rpc::{AuthGrpc, AuthLayer};
use auth::services::account::AccountService;
use auth::services::api_key::ApiKeyService;
use auth::services::config::AuthConfigService;
use auth::services::session::SessionService;
use auth::utils::password::Argon2PasswordAlgorithm;
use base::db::PoolSettings;
use base::services::config::{ConfigStore, LoadConfig};
use clap::Parser;
use kanau::message::MessageDe;
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
use orchestration::events::{
    CanvasDirty, DeriveStaleCanvasesSignal, HealthChanged, RenewCertificatesSignal,
    ResolveServerCountriesSignal, RotateRelayCertificatesSignal, SweepLivenessSignal,
    TrimHealthHistorySignal,
};
use orchestration::hooks::acme::AcmeCronHook;
use orchestration::hooks::country::CountryCronHook;
use orchestration::hooks::derive::CanvasDeriver;
use orchestration::hooks::health::HealthCronHook;
use orchestration::hooks::live::{LiveBus, run_redis_subscriber};
use orchestration::hooks::schedule::IntervalJob;
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
use orchestration::services::watch::{self, SessionLease, WatchHub};
use orchestration::utils::secret::SecretKey;
use rpguru_sdk::auth::auth_server::AuthServer;
use rpguru_sdk::notify::notify_server::NotifyServer;
use rpguru_sdk::orchestration::orchestration_server::OrchestrationServer;
use rpguru_sdk::orchestration_agent::worker_agent_server::WorkerAgentServer;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use time::OffsetDateTime;
use tokio::time::MissedTickBehavior;
use tokio_util::sync::CancellationToken;
use tonic::transport::Server;
use tracing_subscriber::EnvFilter;
use wakuwaku::amqp::{
    AmqpMessageProcessor, AmqpMessageSend, AmqpPool, AmqpRouting, setup_consumer,
};
use wakuwaku::interval_job::IntervalJobExecutionSignal;

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
    // Optional at parse time, required by every mode that opens the database:
    // `cron` is a clock with a broker and nothing else, and a clock that refused
    // to start without a database URL would still have a database dependency,
    // just an unused one.
    #[arg(long, env = "GURU_DATABASE_URL")]
    database_url: Option<String>,
    #[arg(
        long,
        env = "AMQP_URI",
        help = "Broker URI, e.g. amqp://guru:guru@127.0.0.1:5672/%2f. Required in \
                every mode: dashboard_grpc and workers_grpc publish, consumer \
                consumes, and cron publishes the periodic execution signals."
    )]
    amqp_uri: Option<String>,
    #[arg(
        long,
        env = "REDIS_URL",
        help = "Redis URL, e.g. redis://127.0.0.1:6379/. Required in every mode \
                but cron: it carries the live dashboard events between replicas."
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

    // Dispatched before the database, the master key and the configuration: the
    // scheduler needs none of them. It only decides when a job is due and
    // publishes a signal; `--mode consumer` loads the configuration, claims the
    // run and does the work.
    if let WorkerMode::Cron = cli.mode {
        return run_cron(&cli).await;
    }

    // An empty value counts as unset: an exported-but-empty `GURU_DATABASE_URL`
    // reaches clap as `Some("")` and would fail deep in the driver instead of
    // here, where the message can say what to do.
    let Some(database_url) = cli.database_url.as_deref().filter(|v| !v.is_empty()) else {
        return Err(
            "this mode opens the database: set GURU_DATABASE_URL (or pass --database-url)".into(),
        );
    };
    let db = base::db::connect(
        database_url,
        PoolSettings {
            statement_timeout: Duration::from_millis(cli.db_statement_timeout_ms),
            max_connections: cli.db_pool_size,
            application_name: "guru-master",
        },
    )
    .await?;
    // Every serving instance runs the migrations at startup; the migrator takes
    // an advisory lock, so four instances starting together apply them once.
    base::db::MIGRATOR.run(db.db()).await?;
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

    // Dispatched before the master key and the live bus: delivery decrypts
    // nothing and publishes nothing, so handing a notifier host the key that
    // opens every stored secret would widen the blast radius for no gain.
    if let WorkerMode::Notifier = cli.mode {
        return run_notifier(&cli, &db, notify_config).await;
    }

    // Environment only, never argv: the key would otherwise be visible in process
    // listings. `manage-tool generate-master-key` prints a fresh one.
    let secrets = SecretKey::from_env().map_err(|e| format!("master key: {e}"))?;

    let hasher = Argon2PasswordAlgorithm::default();
    let sessions = SessionService {
        db: db.clone(),
        hasher: hasher.clone(),
        config: auth_config,
    };
    let api_keys = ApiKeyService { db: db.clone() };
    // The broker and Redis are opened once for every serving mode: all three
    // publish dirty-canvas events, and all three publish live events (a
    // consumer's derivation pass moves what a dashboard renders just as much as
    // an operator's edit does).
    let uri = amqp_uri(cli.amqp_uri.as_deref())?;
    let (connection, pool) = amqp_pool(uri).await?;
    let redis = redis::Client::open(redis_url(cli.redis_url.as_deref())?)?;
    let notifier = Notifier {
        amqp: Some(pool.clone()),
        live: Some(LivePublisher::Redis(
            redis::aio::ConnectionManager::new(redis.clone()).await?,
        )),
    };
    let health = HealthService {
        db: db.clone(),
        config: config.clone(),
        notifier: notifier.clone(),
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
        notifier: notifier.clone(),
    };

    match cli.mode {
        WorkerMode::DashboardGrpc => {
            let bus = LiveBus::new();
            let live_token = CancellationToken::new();
            let subscriber = tokio::spawn(run_redis_subscriber(
                redis.clone(),
                bus.clone(),
                live_token.clone(),
            ));
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
                graph: GraphService {
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
                    config: config.clone(),
                    notifier,
                    http: reqwest::Client::new(),
                    issuer: Arc::new(InstantAcmeIssuer),
                },
                // The same store the startup read above used: the dashboard
                // hands an Admin the row itself, and the value it writes is
                // what the next restart loads.
                configs: OrchestrationConfigService {
                    configs: configs.clone(),
                },
                live: LiveService::new(db.clone(), bus, config.clone()),
                sessions: sessions.clone(),
            };
            let auth = AuthGrpc {
                accounts,
                sessions: sessions.clone(),
                api_keys: api_keys.clone(),
                configs: AuthConfigService {
                    configs: configs.clone(),
                },
            };
            // Settings only: the dashboard publishes a test notice on the same
            // exchange the fan-out does, and the notifier delivers it.
            let notify = NotifyGrpc {
                settings: SettingService {
                    db: db.clone(),
                    config: notify_config,
                },
                fanout: FanoutService {
                    db: db.clone(),
                    publisher: NoticePublisher::Amqp(pool.clone()),
                },
                configs: NotifyConfigService { configs },
            };
            tracing::info!(addr = %cli.dashboard_addr, "serving operator API");
            // Keepalive is load-bearing for the `Watch*` streams: a browser or
            // bridge that dies without closing its TCP connection would
            // otherwise hold a shared view open forever.
            let serving = Server::builder()
                .http2_keepalive_interval(Some(Duration::from_secs(30)))
                .http2_keepalive_timeout(Some(Duration::from_secs(20)))
                .layer(AuthLayer::new(sessions, api_keys))
                .add_service(AuthServer::new(auth))
                .add_service(OrchestrationServer::new(orchestration))
                .add_service(NotifyServer::new(notify))
                .serve_with_shutdown(cli.dashboard_addr, shutdown());
            let lost = tokio::select! {
                served = serving => {
                    served?;
                    false
                }
                _ = connection.listen_network_io_failure() => true,
            };
            live_token.cancel();
            if lost {
                return Err(BROKER_LOST.into());
            }
            // A panicking subscriber must not be absorbed: without it every open
            // dashboard silently stops updating, so the process exits non-zero.
            if let Err(error) = subscriber.await {
                tracing::error!(%error, "the live bus subscriber task failed");
                return Err(error.into());
            }
        }
        WorkerMode::WorkersGrpc => {
            let hub = WatchHub::default();
            let lease = SessionLease::default();
            let agents = AgentService {
                db: db.clone(),
                hub: hub.clone(),
                lease,
                notifier,
                config: config.clone(),
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
            // Keepalive catches a worker that dies without closing its TCP
            // connection when the master faces it directly. Behind the documented
            // TLS-terminating proxy the pings are answered by the proxy, so the
            // lease also lets go whenever the health pipeline marks the server
            // offline (`set_server_health_status.surql`).
            let serving = Server::builder()
                .http2_keepalive_interval(Some(lease.heartbeat))
                .http2_keepalive_timeout(Some(lease.heartbeat))
                .layer(AuthLayer::new(sessions, api_keys))
                .layer(AgentLayer::new(agents))
                .add_service(WorkerAgentServer::new(workers))
                .serve_with_shutdown(cli.workers_addr, shutdown());
            let lost = tokio::select! {
                served = serving => {
                    served?;
                    false
                }
                _ = connection.listen_network_io_failure() => true,
            };
            token.cancel();
            if lost {
                return Err(BROKER_LOST.into());
            }
            // A panicking poller must not be absorbed: without it no worker ever
            // learns of a new revision, so the process exits non-zero.
            if let Err(error) = poller.await {
                tracing::error!(%error, "the config-view poller task failed");
                return Err(error.into());
            }
        }
        WorkerMode::Consumer => {
            let country = CountryCronHook {
                country: CountryService {
                    db: db.clone(),
                    config: config.clone(),
                    http: reqwest::Client::new(),
                },
            };
            let acme = AcmeCronHook {
                acme: AcmeService {
                    db: db.clone(),
                    secrets,
                    config,
                    notifier,
                    http: reqwest::Client::new(),
                    issuer: Arc::new(InstantAcmeIssuer),
                },
            };
            let health = HealthCronHook { health };
            let fanout = HealthFanout {
                fanout: FanoutService {
                    db: db.clone(),
                    publisher: NoticePublisher::Amqp(pool.clone()),
                },
            };
            // Every channel is kept alive for the lifetime of the mode: dropping
            // one cancels its consumer without a word.
            let channels = vec![
                bind_consumer::<CanvasDirty, _>(&pool, &deriver).await?,
                bind_consumer::<DeriveStaleCanvasesSignal, _>(&pool, &deriver).await?,
                bind_consumer::<RotateRelayCertificatesSignal, _>(&pool, &deriver).await?,
                bind_consumer::<SweepLivenessSignal, _>(&pool, &health).await?,
                bind_consumer::<TrimHealthHistorySignal, _>(&pool, &health).await?,
                bind_consumer::<RenewCertificatesSignal, _>(&pool, &acme).await?,
                bind_consumer::<ResolveServerCountriesSignal, _>(&pool, &country).await?,
                bind_consumer::<HealthChanged, _>(&pool, &fanout).await?,
            ];
            let lost = tokio::select! {
                () = shutdown() => false,
                _ = connection.listen_network_io_failure() => true,
            };
            if lost {
                return Err(BROKER_LOST.into());
            }
            drop(channels);
            connection
                .close()
                .await
                .map_err(|e| format!("closing the AMQP connection failed: {e}"))?;
        }
        // Dispatched above, before the master key and the live bus: delivery
        // needs neither. Reaching it would mean that early return was removed,
        // which is a wiring bug, not a mode.
        WorkerMode::Notifier => {
            return Err("notifier is dispatched before the master key: \
                        the early return in main was lost"
                .into());
        }
        // Dispatched above, before any of the setup this arm would otherwise
        // share: the scheduler opens no database. Reaching it would mean that
        // early return was removed, which is a wiring bug, not a mode.
        WorkerMode::Cron => {
            return Err("cron is dispatched before the database setup: \
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
/// `notify` config it was handed) and the broker.
async fn run_notifier(
    cli: &Cli,
    db: &base::db::Db,
    config: NotifyConfig,
) -> Result<(), Box<dyn std::error::Error>> {
    // Exactly one instance delivers: two would double every notice a restart
    // overlapped. The lock lives as long as this connection, so a killed
    // notifier does not block its successor.
    let _lock = hold_notifier_lock(db).await.map_err(|e| e.to_string())?;
    let secrets = NotifySecrets::from_env();
    let delivery = NoticeDelivery {
        delivery: DeliveryService::new(&config, &secrets).map_err(|e| e.to_string())?,
    };
    for channel in delivery.delivery.channels() {
        tracing::info!(channel, "notification channel");
    }
    let (connection, pool) = amqp_pool(amqp_uri(cli.amqp_uri.as_deref())?).await?;
    let channels = vec![
        bind_consumer::<HealthNotifyGroupEvent, _>(&pool, &delivery).await?,
        bind_consumer::<HealthNotifyPersonalEvent, _>(&pool, &delivery).await?,
    ];
    let lost = tokio::select! {
        () = shutdown() => false,
        _ = connection.listen_network_io_failure() => true,
    };
    if lost {
        return Err(BROKER_LOST.into());
    }
    drop(channels);
    connection
        .close()
        .await
        .map_err(|e| format!("closing the AMQP connection failed: {e}"))?;
    Ok(())
}

/// Why every mode ends on broker loss. `amqprs` does not reconnect: a dead
/// connection in a live process is silent — a consumer stops consuming, and a
/// publisher's every `canvas_dirty` fails until only the 30 s sweep moves
/// rollouts along. Ending the mode lets the supervisor restart it against a
/// reachable broker.
const BROKER_LOST: &str =
    "the AMQP connection was lost: restart once the broker at AMQP_URI is reachable again";

/// How often the scheduler asks every job whether it is due.
///
/// This is the scan resolution, not a cadence: each signal's period is its own
/// `EVERY_SECS`, and a job fires on the first scan at or after it elapses. Five
/// seconds keeps the shortest period (30 s) within a sixth of itself while
/// costing one wakeup per five seconds in an idle fleet.
const SCAN_INTERVAL: Duration = Duration::from_secs(5);

/// `--mode cron`: a clock with a broker and nothing else.
///
/// It opens no database connection and reads no master key, because it runs no
/// periodic work — it publishes one execution signal per due job and the
/// `consumer` fleet claims and runs the passes. The broker is therefore
/// mandatory here: the signal *is* the work.
async fn run_cron(cli: &Cli) -> Result<(), Box<dyn std::error::Error>> {
    let uri = amqp_uri(cli.amqp_uri.as_deref())?;
    let (connection, pool) = amqp_pool(uri).await?;
    // Declared before the first publication, and by the publisher: the exchange
    // is direct, so a signal sent while no consumer has ever bound its queue is
    // silently discarded. This makes a scheduler that starts first harmless.
    declare_queue::<DeriveStaleCanvasesSignal, CanvasDeriver>(&pool).await?;
    declare_queue::<RotateRelayCertificatesSignal, CanvasDeriver>(&pool).await?;
    declare_queue::<SweepLivenessSignal, HealthCronHook>(&pool).await?;
    declare_queue::<TrimHealthHistorySignal, HealthCronHook>(&pool).await?;
    declare_queue::<RenewCertificatesSignal, AcmeCronHook>(&pool).await?;
    declare_queue::<ResolveServerCountriesSignal, CountryCronHook>(&pool).await?;

    // One clock per job, so a scan that visits all six does not flatten their
    // cadences: the hourly rotation fires on one scan in 720, not on every scan
    // that the 30 s sweep fires on.
    let mut derive_stale = IntervalJob::<DeriveStaleCanvasesSignal>::default();
    let mut rotate_relay = IntervalJob::<RotateRelayCertificatesSignal>::default();
    let mut sweep_liveness = IntervalJob::<SweepLivenessSignal>::default();
    let mut trim_health = IntervalJob::<TrimHealthHistorySignal>::default();
    let mut renew_certificates = IntervalJob::<RenewCertificatesSignal>::default();
    let mut resolve_countries = IntervalJob::<ResolveServerCountriesSignal>::default();

    // `Delay` rather than the default burst: a scan that ran late has nothing to
    // catch up on, because a job compares timestamps instead of counting ticks.
    let mut scan = tokio::time::interval(SCAN_INTERVAL);
    scan.set_missed_tick_behavior(MissedTickBehavior::Delay);

    tracing::info!(
        scan_interval_secs = SCAN_INTERVAL.as_secs(),
        derive_stale_canvases_secs = DeriveStaleCanvasesSignal::EVERY_SECS,
        rotate_relay_certificates_secs = RotateRelayCertificatesSignal::EVERY_SECS,
        sweep_liveness_secs = SweepLivenessSignal::EVERY_SECS,
        trim_health_history_secs = TrimHealthHistorySignal::EVERY_SECS,
        renew_certificates_secs = RenewCertificatesSignal::EVERY_SECS,
        resolve_server_countries_secs = ResolveServerCountriesSignal::EVERY_SECS,
        "scheduling periodic execution signals"
    );

    let lost = {
        let mut stop = std::pin::pin!(shutdown());
        // Subscribed once, outside the loop: re-subscribing per scan would miss
        // a failure that happened between two scans.
        let mut broker_lost = std::pin::pin!(connection.listen_network_io_failure());
        loop {
            tokio::select! {
                () = &mut stop => break false,
                _ = &mut broker_lost => break true,
                _ = scan.tick() => {
                    let now = OffsetDateTime::now_utc();
                    publish_due(&mut derive_stale, &pool, now).await;
                    publish_due(&mut rotate_relay, &pool, now).await;
                    publish_due(&mut sweep_liveness, &pool, now).await;
                    publish_due(&mut trim_health, &pool, now).await;
                    publish_due(&mut renew_certificates, &pool, now).await;
                    publish_due(&mut resolve_countries, &pool, now).await;
                }
            }
        }
    };
    if lost {
        return Err(BROKER_LOST.into());
    }
    drop(pool);
    connection
        .close()
        .await
        .map_err(|e| format!("closing the AMQP connection failed: {e}"))?;
    Ok(())
}

/// Declares the queue `H` consumes `S` from and drops the channel: the scheduler
/// publishes and never consumes, it only needs the topology to exist.
async fn declare_queue<S, H>(pool: &AmqpPool) -> Result<(), Box<dyn std::error::Error>>
where
    S: AmqpMessageSend + MessageDe,
    H: AmqpMessageProcessor<S>,
{
    let channel = H::ensure_queue(pool)
        .await
        .map_err(|e| format!("declaring the queue {} failed: {e}", H::QUEUE))?;
    drop(channel);
    Ok(())
}

/// Publishes one job's signal when its cadence has elapsed.
///
/// A publish failure is logged and the scan continues: the broker may be briefly
/// gone, and the job's next cadence publishes again. Ending the scheduler over a
/// single failed publish would stop the other four jobs too.
async fn publish_due<S: IntervalJobExecutionSignal>(
    job: &mut IntervalJob<S>,
    pool: &AmqpPool,
    now: OffsetDateTime,
) {
    let Some(signal) = job.due(now) else {
        return;
    };
    match signal.send(pool).await {
        Ok(()) => tracing::debug!(job = S::ROUTING_KEY, "published an execution signal"),
        Err(error) => tracing::error!(
            %error,
            job = S::ROUTING_KEY,
            "publishing an execution signal failed; the next cadence retries"
        ),
    }
}

/// Binds one consumer on its own channel and returns the channel, which the
/// caller must keep alive: dropping it cancels the consumer.
///
/// The prefetch is bounded so one consumer cannot hoard a backlog its idle peers
/// could be working through; every pass here is a database transaction, not a
/// cheap ack.
async fn bind_consumer<M, H>(
    pool: &AmqpPool,
    hook: &H,
) -> Result<Channel, Box<dyn std::error::Error>>
where
    M: AmqpMessageSend + MessageDe + Send + Sync + 'static,
    M::DeError: Send,
    H: AmqpMessageProcessor<M> + Clone + Send + Sync + 'static,
{
    let channel = H::ensure_queue(pool)
        .await
        .map_err(|e| format!("declaring the queue {} failed: {e}", H::QUEUE))?;
    channel
        .register_callback(DefaultChannelCallback)
        .await
        .map_err(|e| format!("registering the channel callback failed: {e}"))?;
    channel
        .basic_qos(BasicQosArguments::new(0, 8, false))
        .await
        .map_err(|e| format!("setting the consumer prefetch failed: {e}"))?;
    setup_consumer::<M, H>(&channel, Arc::new(hook.clone()))
        .await
        .map_err(|e| format!("binding the consumer failed: {e}"))?;
    tracing::info!(queue = H::QUEUE, key = M::ROUTING_KEY, "consuming");
    Ok(channel)
}

/// Opens an AMQP connection and its channel pool, declaring both exchanges.
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
    // `AmqpMessageSend::send` does not declare, so the `notify` exchange needs
    // its own call: a dashboard publishing a test notice would otherwise fail
    // until a notifier had run once.
    HealthNotifyGroupEvent::ensure_exchange(&pool)
        .await
        .map_err(|e| format!("declaring the notify exchange failed: {e}"))?;
    Ok((connection, pool))
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
