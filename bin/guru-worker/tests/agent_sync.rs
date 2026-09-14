//! End-to-end worker ↔ master config sync against an in-process master.
//!
//! The master here is the real thing: `mem://` SurrealDB with both schemas, the
//! real auth services and middleware, the real `WorkerAgent` service, the real
//! config-view poller and the real derivation sweep. The worker side is
//! `guru_worker::agent`, driving a real `Supervisor` that binds real sockets.

#![allow(clippy::unwrap_used, clippy::panic, clippy::expect_used)]

use auth::entities::surreal::account::{AccountRole, CreateAccount};
use auth::rpc::AuthLayer;
use auth::services::api_key::{ApiKeyService, CreateApiKey};
use auth::services::identity::{Identity, IdentityKind};
use auth::services::session::SessionService;
use auth::utils::password::{Argon2PasswordAlgorithm, PasswordAlgorithm};
use guru_worker::agent::{self, AgentOptions};
use guru_worker::state;
use guru_worker::supervisor::Supervisor;
use guru_worker_config::{
    Config, Forwarding, ForwardingTo, Ipv6Resolve, ListenAs, LogConfig, Remote, TlsHostConfig,
};
use kanau::processor::Processor;
use orchestration::config::OrchestrationConfig;
use orchestration::entities::surreal::canvas::CanvasUiPosition;
use orchestration::entities::surreal::health::{ListServerHealthHistory, ServerHealthStatus};
use orchestration::entities::surreal::node::{EntryConfig, ExitConfig, NodeSpec, PodConfig};
use orchestration::entities::surreal::server::{FindServerById, ServerId, ServerIpv6Resolve};
use orchestration::entities::surreal::view::{FindServerConfigView, ServerConfigViewEntity};
use orchestration::hooks::derive::{self, CanvasDeriver};
use orchestration::rpc::WorkerAgentGrpc;
use orchestration::rpc::agent_middleware::AgentLayer;
use orchestration::services::agent::AgentService;
use orchestration::services::ca::CaService;
use orchestration::services::canvas::{CanvasService, CreateCanvas};
use orchestration::services::edge::{Connect, EdgeService};
use orchestration::services::health::HealthService;
use orchestration::services::node::{CreateNode, NodeService, ReplaceNodeSpec};
use orchestration::services::rollout::DirtyNotifier;
use orchestration::services::server::{AddServerIp, CreateServer, ServerService};
use orchestration::services::watch::{self, SessionLease, WatchHub};
use orchestration::utils::ids;
use orchestration::utils::secret::SecretKey;
use rpguru_sdk::orchestration_agent::worker_agent_client::WorkerAgentClient;
use rpguru_sdk::orchestration_agent::worker_agent_server::{WorkerAgent, WorkerAgentServer};
use rpguru_sdk::orchestration_agent::{
    AckConfigReply, AckConfigRequest, CertificateFile, ConfigRevision, HealthReport, PodStatus,
    RegisterReply, RegisterRequest, ReportHealthReply, WatchConfigRequest,
};
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::AtomicI64;
use std::time::Duration;
use tokio::sync::{Mutex, mpsc};
use tokio_stream::wrappers::ReceiverStream;
use tokio_util::sync::CancellationToken;
use tonic::{Request, Response, Status};
use wakuwaku::surreal::SurrealProcessor;

type TestResult = Result<(), Box<dyn std::error::Error>>;

/// Nothing in this file may wait forever: a regression anywhere in the master —
/// the deriver, the poller or the session bookkeeping — must fail the test instead
/// of hanging CI. Every receive and every RPC goes through here.
const PATIENCE: Duration = Duration::from_secs(30);

async fn within<F: Future>(what: &str, fut: F) -> F::Output {
    match tokio::time::timeout(PATIENCE, fut).await {
        Ok(value) => value,
        Err(_) => panic!("timed out after {PATIENCE:?} waiting for {what}"),
    }
}

fn operator() -> Identity {
    Identity {
        account_id: auth::entities::surreal::account::AccountId(surrealdb::types::RecordId::new(
            "auth_account",
            "bootstrap",
        )),
        role: AccountRole::Admin,
        kind: IdentityKind::Session,
    }
}

fn pos() -> CanvasUiPosition {
    CanvasUiPosition { x: 0, y: 0 }
}

fn free_port() -> u16 {
    let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let port = l.local_addr().unwrap().port();
    drop(l);
    port
}

/// The canvas has one pod, `edge`: an ack for any of its revisions names just that.
fn edge_ok() -> PodStatus {
    PodStatus {
        tag: "edge".to_string(),
        error: None,
    }
}

struct Master {
    db: SurrealProcessor,
    hub: WatchHub,
    addr: SocketAddr,
    shutdown: CancellationToken,
}

/// Boots a master process image: schemas, an operator API key, the worker gRPC
/// service and the revision poller.
async fn boot_master(lease: SessionLease) -> Result<(Master, String), Box<dyn std::error::Error>> {
    let db = surrealdb::engine::any::connect("mem://").await?;
    db.use_ns("test").use_db("test").await?;
    let sp = SurrealProcessor::new(db);
    for schema in ["auth", "orchestration"] {
        let ddl = std::fs::read_to_string(format!(
            "{}/../../database/schema/{schema}.surql",
            env!("CARGO_MANIFEST_DIR")
        ))?;
        sp.db().query(ddl).await?.check()?;
    }

    let hasher = Argon2PasswordAlgorithm::default();
    let account = sp
        .process(CreateAccount {
            email: "ops@example.com".to_string(),
            password_hash: hasher.hash_password("hunter2hunter2")?,
            role: AccountRole::Maintainer,
        })
        .await?;
    let api_keys = ApiKeyService { db: sp.clone() };
    let created = api_keys
        .process(CreateApiKey {
            actor: Identity {
                account_id: account.id.clone(),
                role: AccountRole::Maintainer,
                kind: IdentityKind::Session,
            },
            name: "worker".to_string(),
        })
        .await?;

    let hub = WatchHub::default();
    let shutdown = CancellationToken::new();
    let addr = serve(&sp, &hub, &shutdown, lease).await?;
    Ok((
        Master {
            db: sp,
            hub,
            addr,
            shutdown,
        },
        created.secret,
    ))
}

/// Starts one tonic server (a master "process") and returns its address.
async fn serve(
    db: &SurrealProcessor,
    hub: &WatchHub,
    shutdown: &CancellationToken,
    lease: SessionLease,
) -> Result<SocketAddr, Box<dyn std::error::Error>> {
    let hasher = Argon2PasswordAlgorithm::default();
    let sessions = SessionService {
        db: db.clone(),
        hasher,
        config: auth::config::AuthConfig::default(),
    };
    let api_keys = ApiKeyService { db: db.clone() };
    let agents = AgentService {
        db: db.clone(),
        hub: hub.clone(),
        lease,
        notifier: DirtyNotifier::default(),
    };
    let secrets = SecretKey::from_base64(&SecretKey::generate_base64())?;
    let config = OrchestrationConfig::default();
    let service = WorkerAgentGrpc {
        agents: agents.clone(),
        health: HealthService {
            db: db.clone(),
            config: config.clone(),
        },
        ca: CaService {
            db: db.clone(),
            secrets: secrets.clone(),
            config: config.clone(),
        },
        db: db.clone(),
        hub: hub.clone(),
        lease,
    };

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;
    let token = shutdown.clone();
    tokio::spawn(async move {
        let _ = tonic::transport::Server::builder()
            .layer(AuthLayer::new(sessions, api_keys))
            .layer(AgentLayer::new(agents))
            .add_service(WorkerAgentServer::new(service))
            .serve_with_incoming_shutdown(
                tokio_stream::wrappers::TcpListenerStream::new(listener),
                async move { token.cancelled().await },
            )
            .await;
    });
    let poller_token = shutdown.clone();
    tokio::spawn(watch::run_poller(
        hub.clone(),
        db.clone(),
        Duration::from_millis(50),
        poller_token,
    ));
    // No broker in the test image, so this calls the consumer's derivation pass
    // directly on a timer: a harness standing in for `--mode consumer`, not a
    // supported topology. It stops with the master, so a test that restarts one
    // never has two sweepers writing the same canvases.
    let deriver = CanvasDeriver {
        db: db.clone(),
        secrets,
        config,
    };
    let sweeper_token = shutdown.clone();
    tokio::spawn(async move {
        // `interval` with `Delay`, not a sleep after each pass: the tests wait on
        // a real listener appearing, so the sweep has to keep a 50 ms period
        // instead of 50 ms plus however long a pass took.
        let mut ticker = tokio::time::interval(Duration::from_millis(50));
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            tokio::select! {
                () = sweeper_token.cancelled() => return,
                _ = ticker.tick() => {}
            }
            if let Err(error) = derive::sweep_stale_canvases(&deriver).await {
                tracing::debug!(%error, "the test sweeper failed a pass");
            }
        }
    });
    Ok(addr)
}

struct Canvas {
    server: ServerId,
    pod: orchestration::entities::surreal::node::NodeId,
    listen: SocketAddr,
    ip: orchestration::entities::surreal::server::ServerIpRecordId,
}

/// One server with a single `pod -> entry` / `exit -> pod` chain on loopback.
async fn build_canvas(db: &SurrealProcessor) -> Result<Canvas, Box<dyn std::error::Error>> {
    let canvases = CanvasService {
        db: db.clone(),
        notifier: DirtyNotifier::default(),
    };
    let servers = ServerService {
        db: db.clone(),
        notifier: DirtyNotifier::default(),
        config: OrchestrationConfig::default(),
    };
    let nodes = NodeService {
        db: db.clone(),
        notifier: DirtyNotifier::default(),
        config: OrchestrationConfig::default(),
    };
    let edges = EdgeService {
        db: db.clone(),
        notifier: DirtyNotifier::default(),
        config: OrchestrationConfig::default(),
    };

    let canvas = canvases
        .process(CreateCanvas {
            actor: operator(),
            name: "prod".to_string(),
            description: String::new(),
        })
        .await?;
    let server = servers
        .process(CreateServer {
            actor: operator(),
            canvas: canvas.id.clone(),
            name: "tokyo".to_string(),
            icon: String::new(),
            comment: String::new(),
            position: pos(),
            ipv6_resolve: ServerIpv6Resolve::Tolerated,
            log_level: "info".to_string(),
        })
        .await?;
    let ip = servers
        .process(AddServerIp {
            actor: operator(),
            server: server.id.clone(),
            ip: "127.0.0.1".to_string(),
            country: "jp".to_string(),
        })
        .await?;
    let port = free_port();
    let pod = nodes
        .process(CreateNode {
            actor: operator(),
            canvas: canvas.id.clone(),
            name: "edge".to_string(),
            comment: String::new(),
            spec: NodeSpec::Pod(PodConfig {
                ip: ip.id.clone(),
                port,
            }),
            position: pos(),
            item_count: 0,
        })
        .await?;
    let entry = nodes
        .process(CreateNode {
            actor: operator(),
            canvas: canvas.id.clone(),
            name: "entry".to_string(),
            comment: String::new(),
            spec: NodeSpec::Entry(EntryConfig {
                receive_proxy_protocol: None,
                tls: None,
            }),
            position: pos(),
            item_count: 0,
        })
        .await?;
    let exit = nodes
        .process(CreateNode {
            actor: operator(),
            canvas: canvas.id.clone(),
            name: "exit".to_string(),
            comment: String::new(),
            spec: NodeSpec::Exit(ExitConfig {
                destination: "127.0.0.1:9".to_string(),
                pass_proxy_protocol: None,
            }),
            position: pos(),
            item_count: 0,
        })
        .await?;
    let port_of = |node: &orchestration::entities::surreal::node::NodeWithPorts, key: &str| {
        node.ports
            .iter()
            .find(|p| p.key == key)
            .map(|p| p.id.clone())
            .unwrap()
    };
    edges
        .process(Connect {
            actor: operator(),
            output_port: port_of(&pod, "listen"),
            input_port: port_of(&entry, "listen"),
        })
        .await?;
    edges
        .process(Connect {
            actor: operator(),
            output_port: port_of(&exit, "destination"),
            input_port: port_of(&pod, "destination"),
        })
        .await?;

    Ok(Canvas {
        server: server.id,
        pod: pod.node.id,
        listen: format!("127.0.0.1:{port}").parse()?,
        ip: ip.id,
    })
}

/// Waits until `check` holds for the server's config view, or fails the test.
async fn wait_for<F>(db: &SurrealProcessor, server: &ServerId, what: &str, check: F)
where
    F: Fn(&ServerConfigViewEntity) -> bool,
{
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    loop {
        let view = db
            .process(FindServerConfigView {
                server: server.clone(),
            })
            .await
            .unwrap()
            .unwrap();
        if check(&view) {
            return;
        }
        if std::time::Instant::now() > deadline {
            panic!(
                "timed out waiting for {what}: desired={:?} in_flight={:?} applied={:?} error={:?}",
                view.desired.as_ref().map(|s| s.revision),
                view.in_flight.as_ref().map(|s| s.revision),
                view.applied.as_ref().map(|s| s.revision),
                view.apply_error,
            );
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

#[tokio::test]
async fn worker_applies_config_reports_health_and_survives_a_bad_pod() -> TestResult {
    let (master, api_key) = boot_master(SessionLease::default()).await?;
    let canvas = build_canvas(&master.db).await?;
    let state_dir = std::env::temp_dir().join(format!("guru-worker-test-{}", free_port()));
    let _ = std::fs::remove_dir_all(&state_dir);

    let sup = Arc::new(Mutex::new(Supervisor::new()));
    let agent_shutdown = CancellationToken::new();
    let started = chrono::Utc::now();
    let agent_task = tokio::spawn(agent::run(
        AgentOptions {
            master: format!("http://{}", master.addr),
            api_key: api_key.clone(),
            server_id: ids::record_key(&canvas.server.0),
            state_dir: state_dir.clone(),
            applied_revision: Arc::new(AtomicI64::new(0)),
            health_interval: Duration::from_millis(200),
        },
        sup.clone(),
        agent_shutdown.clone(),
    ));

    // The worker registers, receives the derived config and applies it.
    wait_for(&master.db, &canvas.server, "the first apply", |view| {
        view.applied.is_some()
            && view.applied.as_ref().map(|s| s.revision)
                == view.desired.as_ref().map(|s| s.revision)
    })
    .await;
    within(
        "the derived listener to accept",
        tokio::net::TcpStream::connect(canvas.listen),
    )
    .await
    .expect("the derived listener is bound");
    let good = state::load(&state_dir).expect("last-known-good was persisted");
    let first_revision = good.revision;
    assert!(good.toml.contains(&canvas.listen.to_string()));

    // Health reports flow on their own stream: the master records each one and
    // rates a converged worker `Online`.
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    let records = loop {
        let records = master
            .db
            .process(ListServerHealthHistory {
                server: canvas.server.clone(),
                start: started - chrono::TimeDelta::seconds(1),
                end: chrono::Utc::now() + chrono::TimeDelta::seconds(1),
            })
            .await?;
        if records
            .iter()
            .any(|r| r.status == ServerHealthStatus::Online)
        {
            break records;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "no online health record was written: {records:?}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    };
    assert!(records.iter().all(|r| r.server.0 == canvas.server.0));
    let server = master
        .db
        .process(FindServerById {
            id: canvas.server.clone(),
        })
        .await?
        .unwrap();
    assert_eq!(server.health_status, ServerHealthStatus::Online);
    assert!(server.last_health_report_at.is_some());

    // A pod that cannot bind is acked as failed: the rest of the revision applies,
    // the pod keeps its previous listener and the master records the mix.
    let taken = std::net::TcpListener::bind("127.0.0.1:0")?;
    let taken_port = taken.local_addr()?.port();
    let nodes = NodeService {
        db: master.db.clone(),
        notifier: DirtyNotifier::default(),
        config: OrchestrationConfig::default(),
    };
    nodes
        .process(ReplaceNodeSpec {
            actor: operator(),
            node: canvas.pod.clone(),
            spec: NodeSpec::Pod(PodConfig {
                ip: canvas.ip.clone(),
                port: taken_port,
            }),
            item_count: 0,
        })
        .await?;
    wait_for(&master.db, &canvas.server, "the failed pod", |view| {
        !view.failed_pods.is_empty()
    })
    .await;

    let view = master
        .db
        .process(FindServerConfigView {
            server: canvas.server.clone(),
        })
        .await?
        .unwrap();
    let bad_revision = view.desired.as_ref().map(|s| s.revision);
    assert!(bad_revision > Some(first_revision));
    assert_eq!(
        view.failed_revision, bad_revision,
        "the revision with the failed pod is recorded as such"
    );
    assert_eq!(
        view.applied.as_ref().map(|s| s.revision),
        bad_revision,
        "the mix the worker runs is recorded under the new revision"
    );
    assert!(view.in_flight.is_none(), "the send is not left open");
    assert!(
        view.apply_error.is_none(),
        "a per-pod failure is not a whole-revision failure: {:?}",
        view.apply_error
    );
    let failed = &view.failed_pods[0];
    assert_eq!(failed.tag, "edge");
    assert_eq!(failed.pod.0, canvas.pod.0);
    assert!(
        view.applied
            .as_ref()
            .is_some_and(|s| s.toml.contains(&canvas.listen.to_string())),
        "the recorded mix keeps the pod's previous shape"
    );
    within(
        "the previous listener to accept",
        tokio::net::TcpStream::connect(canvas.listen),
    )
    .await
    .expect("the previous listener still serves");
    let good = state::load(&state_dir).unwrap();
    assert_eq!(
        Some(good.revision),
        bad_revision,
        "last-known-good is the mix under the new revision"
    );
    assert!(
        good.toml.contains(&canvas.listen.to_string()),
        "last-known-good holds the shape actually running: {}",
        good.toml
    );

    agent_shutdown.cancel();
    let _ = within("the agent task to stop", agent_task).await;
    master.shutdown.cancel();
    sup.lock().await.shutdown_all();
    drop(taken);
    let _ = std::fs::remove_dir_all(&state_dir);
    Ok(())
}

/// Registers with the operator API key and returns the refresh key.
async fn register(
    client: &mut WorkerAgentClient<tonic::transport::Channel>,
    server_key: &str,
    api_key: &str,
) -> Result<String, tonic::Status> {
    let mut request = tonic::Request::new(RegisterRequest {
        server_id: server_key.to_string(),
        running_revision: 0,
    });
    let key = api_key.parse().map_err(|_| Status::internal("api key"))?;
    request.metadata_mut().insert("x-api-key", key);
    Ok(within("Register", client.register(request))
        .await?
        .into_inner()
        .refresh_key)
}

/// Opens a `WatchConfig` stream and takes its first revision, so the session is
/// claimed — and that revision is in flight — by the time the call returns.
async fn watch(
    client: &mut WorkerAgentClient<tonic::transport::Channel>,
    refresh_key: &str,
) -> Result<(tonic::Streaming<ConfigRevision>, ConfigRevision), Box<dyn std::error::Error>> {
    let mut request = tonic::Request::new(WatchConfigRequest {});
    request
        .metadata_mut()
        .insert("x-refresh-key", refresh_key.parse()?);
    let mut stream = within("WatchConfig", client.watch_config(request))
        .await?
        .into_inner();
    let first = within("the first revision", stream.message())
        .await?
        .expect("the first revision arrives");
    Ok((stream, first))
}

/// An ack that applied every forwarding of `revision`. The sweeper may have derived
/// the server before its canvas was complete, so the pods come from the revision
/// actually handed over, not from what the canvas ends up with.
fn ack_all_applied(revision: &ConfigRevision) -> AckConfigRequest {
    let cfg = Config::from_toml_str(&revision.toml).expect("the revision is valid TOML");
    AckConfigRequest {
        revision: revision.revision,
        error: None,
        pods: cfg
            .forwardings
            .iter()
            .map(|f| PodStatus {
                tag: f.tag.clone(),
                error: None,
            })
            .collect(),
    }
}

#[tokio::test]
async fn a_heartbeating_stream_keeps_its_session_against_a_second_worker() -> TestResult {
    // A lease far shorter than the window this test keeps the stream open for: once
    // the first grant's deadline has passed, only the heartbeat can still hold it.
    let ttl = Duration::from_secs(3);
    let lease = SessionLease {
        ttl,
        heartbeat: Duration::from_millis(250),
    };
    let (master, api_key) = boot_master(lease).await?;
    let canvas = build_canvas(&master.db).await?;
    let server_key = ids::record_key(&canvas.server.0);

    let mut client = within(
        "a channel to the master",
        WorkerAgentClient::connect(format!("http://{}", master.addr)),
    )
    .await?;
    let first_key = register(&mut client, &server_key, &api_key).await?;
    let (stream, revision) = watch(&mut client, &first_key).await?;
    // The master stamped the lease before this line, so whatever deadline it granted
    // has certainly passed by then: a refusal after it can only come from a renewal.
    let grant_expired_by = std::time::Instant::now() + ttl;

    // A second worker aimed at the same server stays locked out instead of trading the
    // server back and forth with the incumbent. Polled rather than measured against one
    // absolute margin: every attempt must be refused, and the loop only ends once an
    // attempt made after the original grant lapsed was refused too.
    loop {
        let attempted_at = std::time::Instant::now();
        let status = register(&mut client, &server_key, &api_key)
            .await
            .expect_err("a live session must not be stolen");
        assert_eq!(status.code(), tonic::Code::FailedPrecondition);
        if attempted_at > grant_expired_by {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    // The incumbent is untouched and still owns the stream: it can ack exactly the
    // revision the master handed it.
    let mut ack = tonic::Request::new(ack_all_applied(&revision));
    ack.metadata_mut()
        .insert("x-refresh-key", first_key.parse()?);
    within("AckConfig", client.ack_config(ack)).await?;

    drop(stream);
    master.shutdown.cancel();
    Ok(())
}

#[tokio::test]
async fn an_ended_stream_hands_the_session_back_at_once() -> TestResult {
    // The default lease is far longer than this test waits, so a successful
    // handover can only come from an explicit release, never from expiry.
    let lease = SessionLease::default();
    let (master, api_key) = boot_master(lease).await?;
    let canvas = build_canvas(&master.db).await?;
    let server_key = ids::record_key(&canvas.server.0);

    let mut client = within(
        "a channel to the master",
        WorkerAgentClient::connect(format!("http://{}", master.addr)),
    )
    .await?;
    let first_key = register(&mut client, &server_key, &api_key).await?;
    let (stream, _) = watch(&mut client, &first_key).await?;
    drop(stream);

    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    let second_key = loop {
        match register(&mut client, &server_key, &api_key).await {
            Ok(key) => break key,
            Err(e) if std::time::Instant::now() < deadline => {
                assert_eq!(e.code(), tonic::Code::FailedPrecondition);
                tokio::time::sleep(Duration::from_millis(25)).await;
            }
            Err(e) => panic!("the released session was never handed over: {e}"),
        }
    };
    assert_ne!(first_key, second_key);
    // 5 s deadline vs a 30 s lease: expiry cannot explain the handover.

    // The rotated-away key is dead.
    let mut stale = tonic::Request::new(AckConfigRequest {
        revision: 1,
        error: None,
        pods: vec![edge_ok()],
    });
    stale
        .metadata_mut()
        .insert("x-refresh-key", first_key.parse()?);
    assert_eq!(
        within(
            "AckConfig with the superseded key",
            client.ack_config(stale)
        )
        .await
        .expect_err("the superseded key must be rejected")
        .code(),
        tonic::Code::Unauthenticated
    );

    master.shutdown.cancel();
    Ok(())
}

#[tokio::test]
async fn a_newer_stream_fences_the_previous_one() -> TestResult {
    let (master, api_key) = boot_master(SessionLease::default()).await?;
    let canvas = build_canvas(&master.db).await?;
    let server_key = ids::record_key(&canvas.server.0);

    let mut client = within(
        "a channel to the master",
        WorkerAgentClient::connect(format!("http://{}", master.addr)),
    )
    .await?;
    let refresh_key = register(&mut client, &server_key, &api_key).await?;
    let (mut first, _) = watch(&mut client, &refresh_key).await?;
    let _second = watch(&mut client, &refresh_key).await?;

    // One connection per server: the older stream is told to go away instead of
    // lingering as a second consumer of the same server's config.
    let status = within("the fenced stream to end", first.message())
        .await
        .expect_err("the fenced stream must be ended by the master");
    assert_eq!(status.code(), tonic::Code::Aborted);

    // The loser releases on its way out, but the release is scoped to its own
    // session: the survivor still owns the server.
    let status = register(&mut client, &server_key, &api_key)
        .await
        .expect_err("the surviving session still owns the server");
    assert_eq!(status.code(), tonic::Code::FailedPrecondition);

    master.shutdown.cancel();
    Ok(())
}

#[tokio::test]
async fn a_rotated_refresh_key_ends_an_open_stream() -> TestResult {
    let (master, api_key) = boot_master(SessionLease::default()).await?;
    let canvas = build_canvas(&master.db).await?;
    let server_key = ids::record_key(&canvas.server.0);

    let mut client = within(
        "a channel to the master",
        WorkerAgentClient::connect(format!("http://{}", master.addr)),
    )
    .await?;
    let first_key = register(&mut client, &server_key, &api_key).await?;
    let (mut stream, _) = watch(&mut client, &first_key).await?;

    // Simulate the incumbent going silent: its lease lapses, so a replacement worker
    // is allowed to take the server over.
    master
        .db
        .db()
        .query("UPDATE $server SET session_lease_until = $past")
        .bind(("server", canvas.server.clone()))
        .bind(("past", chrono::Utc::now() - chrono::TimeDelta::seconds(60)))
        .await?
        .check()?;
    let second_key = register(&mut client, &server_key, &api_key).await?;
    assert_ne!(first_key, second_key);

    let status = within("the superseded stream to end", stream.message())
        .await
        .expect_err("the superseded stream must be ended by the master");
    assert_eq!(status.code(), tonic::Code::Unauthenticated);

    master.shutdown.cancel();
    Ok(())
}

#[tokio::test]
async fn a_refresh_key_survives_a_master_restart() -> TestResult {
    let (master, api_key) = boot_master(SessionLease::default()).await?;
    let canvas = build_canvas(&master.db).await?;
    let server_key = ids::record_key(&canvas.server.0);

    let mut client = within(
        "a channel to the master",
        WorkerAgentClient::connect(format!("http://{}", master.addr)),
    )
    .await?;
    let refresh_key = register(&mut client, &server_key, &api_key).await?;

    // The digest lives in the database, not in the master's memory.
    master.shutdown.cancel();
    tokio::time::sleep(Duration::from_millis(50)).await;
    let restart = CancellationToken::new();
    let addr = serve(&master.db, &master.hub, &restart, SessionLease::default()).await?;
    let mut client = within(
        "a channel to the restarted master",
        WorkerAgentClient::connect(format!("http://{addr}")),
    )
    .await?;
    // Take the snapshot through a stream, the way a worker does: the ack is only
    // valid for the revision the database handed this session.
    let (stream, revision) = watch(&mut client, &refresh_key).await?;
    let mut ack = tonic::Request::new(ack_all_applied(&revision));
    ack.metadata_mut()
        .insert("x-refresh-key", refresh_key.parse()?);
    within("AckConfig after the restart", client.ack_config(ack))
        .await
        .expect("the refresh key survives a master restart");
    let view = master
        .db
        .process(FindServerConfigView {
            server: canvas.server.clone(),
        })
        .await?
        .unwrap();
    assert_eq!(
        view.applied.as_ref().map(|s| s.revision),
        Some(revision.revision),
        "the acked revision is what the server is recorded as running"
    );
    drop(stream);

    restart.cancel();
    Ok(())
}

/// The master reduced to the worker-facing contract: hands out one fixed revision and
/// records what the worker sends back. Exercises the file and health paths without a
/// database — the real master's side of both is covered in the orchestration crate.
struct FakeMaster {
    revision: ConfigRevision,
    acks: mpsc::UnboundedSender<AckConfigRequest>,
    health: mpsc::UnboundedSender<HealthReport>,
    /// Keeps every config stream open: a closed stream would end the worker's session.
    streams: parking_lot::Mutex<Vec<mpsc::Sender<Result<ConfigRevision, Status>>>>,
}

#[tonic::async_trait]
impl WorkerAgent for FakeMaster {
    async fn register(
        &self,
        _: Request<RegisterRequest>,
    ) -> Result<Response<RegisterReply>, Status> {
        Ok(Response::new(RegisterReply {
            refresh_key: "fake".to_string(),
            health_report_interval_secs: 0,
        }))
    }

    type WatchConfigStream = ReceiverStream<Result<ConfigRevision, Status>>;

    async fn watch_config(
        &self,
        _: Request<WatchConfigRequest>,
    ) -> Result<Response<Self::WatchConfigStream>, Status> {
        let (tx, rx) = mpsc::channel(4);
        tx.send(Ok(self.revision.clone()))
            .await
            .map_err(|_| Status::internal("stream"))?;
        self.streams.lock().push(tx);
        Ok(Response::new(ReceiverStream::new(rx)))
    }

    async fn ack_config(
        &self,
        request: Request<AckConfigRequest>,
    ) -> Result<Response<AckConfigReply>, Status> {
        let _ = self.acks.send(request.into_inner());
        Ok(Response::new(AckConfigReply {}))
    }

    async fn report_health(
        &self,
        request: Request<tonic::Streaming<HealthReport>>,
    ) -> Result<Response<ReportHealthReply>, Status> {
        let mut reports = request.into_inner();
        while let Some(report) = reports.message().await? {
            let _ = self.health.send(report);
        }
        Ok(Response::new(ReportHealthReply {}))
    }
}

#[tokio::test]
async fn worker_writes_delivered_certificates_serves_tls_and_reports_health() -> TestResult {
    let key = rcgen::KeyPair::generate()?;
    let cert = rcgen::CertificateParams::new(vec!["edge.test".to_string()])?.self_signed(&key)?;
    let listen: SocketAddr = format!("127.0.0.1:{}", free_port()).parse()?;
    let cfg = Config {
        ipv6_resolve: Ipv6Resolve::Tolerated,
        log: LogConfig::default(),
        relay_ca: Some("certs/ca.pem".into()),
        forwardings: vec![Forwarding {
            tag: "edge".to_string(),
            listen,
            receive_proxy_protocol: None,
            listen_as: ListenAs::Tls(TlsHostConfig {
                key: "certs/acme/edge/key.pem".into(),
                full_chain: "certs/acme/edge/full_chain.pem".into(),
            }),
            to: ForwardingTo::Exit {
                destination: Remote::parse("127.0.0.1:9")?,
                send_proxy_protocol: None,
            },
        }],
    };
    let revision = ConfigRevision {
        revision: 7,
        toml: cfg.to_toml_string()?,
        files: vec![
            CertificateFile {
                path: "certs/acme/edge/full_chain.pem".to_string(),
                pem: cert.pem(),
            },
            CertificateFile {
                path: "certs/acme/edge/key.pem".to_string(),
                pem: key.serialize_pem(),
            },
            CertificateFile {
                path: "certs/ca.pem".to_string(),
                pem: cert.pem(),
            },
        ],
    };

    let (acks_tx, mut acks) = mpsc::unbounded_channel();
    let (health_tx, mut health) = mpsc::unbounded_channel();
    let master = FakeMaster {
        revision,
        acks: acks_tx,
        health: health_tx,
        streams: parking_lot::Mutex::default(),
    };
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let master_addr = listener.local_addr()?;
    let master_shutdown = CancellationToken::new();
    let token = master_shutdown.clone();
    tokio::spawn(async move {
        let _ = tonic::transport::Server::builder()
            .add_service(WorkerAgentServer::new(master))
            .serve_with_incoming_shutdown(
                tokio_stream::wrappers::TcpListenerStream::new(listener),
                async move { token.cancelled().await },
            )
            .await;
    });

    let state_dir = std::env::temp_dir().join(format!("guru-worker-certs-{}", free_port()));
    let _ = std::fs::remove_dir_all(&state_dir);
    // A stale swap from an "earlier" run: `recover` must not be needed for a fresh
    // write, but a leftover `.old` beside the target must not survive a good apply.
    let sup = Arc::new(Mutex::new(Supervisor::new()));
    let agent_shutdown = CancellationToken::new();
    let agent_task = tokio::spawn(agent::run(
        AgentOptions {
            master: format!("http://{master_addr}"),
            api_key: "unused".to_string(),
            server_id: "edge-server".to_string(),
            state_dir: state_dir.clone(),
            applied_revision: Arc::new(AtomicI64::new(0)),
            health_interval: Duration::from_millis(200),
        },
        sup.clone(),
        agent_shutdown.clone(),
    ));

    let ack = within("the ack", acks.recv())
        .await
        .expect("an ack arrives");
    assert_eq!(ack.revision, 7);
    assert_eq!(ack.error, None, "the revision applied");
    assert_eq!(ack.pods, vec![edge_ok()]);

    // The files are where the TOML says, the key is private, and nothing of the swap
    // is left behind.
    use std::os::unix::fs::PermissionsExt;
    let key_path = state_dir.join("certs/acme/edge/key.pem");
    let chain_path = state_dir.join("certs/acme/edge/full_chain.pem");
    let ca_path = state_dir.join("certs/ca.pem");
    assert_eq!(std::fs::read_to_string(&key_path)?, key.serialize_pem());
    assert_eq!(std::fs::read_to_string(&chain_path)?, cert.pem());
    assert_eq!(std::fs::read_to_string(&ca_path)?, cert.pem());
    assert_eq!(
        std::fs::metadata(&key_path)?.permissions().mode() & 0o777,
        0o600,
        "the private key is readable by the worker alone"
    );
    assert!(!state_dir.join("certs/acme/edge.new").exists());
    assert!(!state_dir.join("certs/acme/edge.old").exists());

    // The listener terminates TLS with the delivered certificate: a client trusting
    // only that certificate completes the handshake.
    let tcp = within(
        "a connection to the TLS listener",
        tokio::net::TcpStream::connect(listen),
    )
    .await?;
    within(
        "the TLS handshake",
        guru_worker::tls::connect_tls("edge.test", tcp, Some(&ca_path)),
    )
    .await
    .expect("the handshake against the delivered certificate succeeds");

    // Health reports name the running revision and every pod.
    let report = within("a health report", health.recv())
        .await
        .expect("a report arrives");
    assert_eq!(report.running_revision, 7);
    assert_eq!(report.pods, vec![edge_ok()]);

    // What is persisted for a restart is the running config with its paths resolved.
    let good = state::load(&state_dir).expect("last-known-good was persisted");
    assert_eq!(good.revision, 7);
    let replay = Config::from_toml_str(&good.toml)?;
    assert_eq!(replay.relay_ca.as_deref(), Some(ca_path.as_path()));

    agent_shutdown.cancel();
    let _ = within("the agent task to stop", agent_task).await;
    master_shutdown.cancel();
    sup.lock().await.shutdown_all();
    let _ = std::fs::remove_dir_all(&state_dir);
    Ok(())
}
