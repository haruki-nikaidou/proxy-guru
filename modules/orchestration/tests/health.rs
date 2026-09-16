//! Health recording: what a worker's reports and acks turn into, and what the
//! master concludes when the reports stop.

#![allow(clippy::unwrap_used, clippy::panic, clippy::expect_used)]

mod common;

use chrono::{DateTime, DurationRound, TimeDelta, Utc};
use common::*;
use guru_worker_config::{Config, ForwardingTo, Remote};
use kanau::processor::Processor;
use orchestration::config::OrchestrationConfig;
use orchestration::entities::db::canvas::CanvasEntity;
use orchestration::entities::db::edge::EdgeEntity;
use orchestration::entities::db::exit::ExitEntity;
use orchestration::entities::db::health::{
    InsertServerHealthRecord, ListPodHealthHistory, ListServerHealthHistory, NewPodHealthRecord,
    PodHealthRecordEntity, PodHealthStatus, ServerHealthRecordEntity, ServerHealthStatus,
};
use orchestration::entities::db::pod::{PodEntity, PodId};
use orchestration::entities::db::server::{
    ClaimServerWatchSession, FindServerById, RenewServerWatchSession, ServerEntity, ServerId,
    ServerIpv6Resolve, ServerLogLevel,
};
use orchestration::entities::db::view::{ListStaleCanvases, TakeInFlight};
use orchestration::events::SweepLivenessSignal;
use orchestration::hooks::health::HealthCronHook;
use orchestration::services::OrchestrationError;
use orchestration::services::agent::{
    AckConfig, AgentIdentity, PodResult, RegisterCredential, RegisterWorker,
};
use orchestration::services::canvas as canvas_service;
use orchestration::services::graph::GraphChange;
use orchestration::services::health::HealthService;
use orchestration::services::health::{
    HealthReportInput, MarkServerOffline, RecordHealthReport, SweepLiveness, TrimHealthHistory,
};
use orchestration::services::server::{AddressOverrides, CreateServer};

/// One server, two client pods: `web` (443 → exit `web-out`) and `api`
/// (8443 → exit `api-out`).
struct Fixture {
    canvas: CanvasId,
    canvas_row: CanvasEntity,
    server: ServerId,
    server_row: ServerEntity,
    web: PodEntity,
    web_out: ExitEntity,
    api: PodEntity,
    api_out: ExitEntity,
}

type CanvasId = orchestration::entities::db::canvas::CanvasId;

/// A canvas with one server carrying one IP; the graph goes on top.
async fn base(w: &World) -> Result<(CanvasEntity, ServerEntity), Box<dyn std::error::Error>> {
    let canvas = w
        .canvases
        .process(canvas_service::CreateCanvas {
            actor: operator(),
            name: "prod".to_string(),
            description: String::new(),
            parent: None,
            position: pos0(),
        })
        .await?;
    let server = w
        .servers
        .process(CreateServer {
            actor: operator(),
            canvas: canvas.id.clone(),
            name: "tokyo".to_string(),
            icon: String::new(),
            comment: String::new(),
            position: pos0(),
            ipv6_resolve: ServerIpv6Resolve::Tolerated,
            log_level: ServerLogLevel::Info,
            addresses: AddressOverrides {
                override_v4: Some("203.0.113.10".to_string()),
                override_v6: None,
                extra_addresses: Vec::new(),
            },
        })
        .await?;
    Ok((canvas, server))
}

/// A client pod on `server` going straight to a new exit, as one change.
fn to_exit(
    canvas: &CanvasEntity,
    server: &ServerEntity,
    name: &str,
    port: u16,
    destination: &str,
) -> (PodEntity, ExitEntity, EdgeEntity, GraphChange) {
    let pod = client(canvas, server, name, port, None);
    let out = exit(canvas, &format!("{name}-out"), destination);
    let edge = edge_to_exit(&format!("{name}-edge"), &pod, &out);
    let pod = routed(pod, via(&edge));
    let change = GraphChange {
        put_pods: vec![pod.clone()],
        put_exits: vec![out.clone()],
        put_edges: vec![edge.clone()],
        ..GraphChange::default()
    };
    (pod, out, edge, change)
}

fn merge(changes: Vec<GraphChange>) -> GraphChange {
    let mut out = GraphChange::default();
    for change in changes {
        out.put_pods.extend(change.put_pods);
        out.put_exits.extend(change.put_exits);
        out.put_edges.extend(change.put_edges);
    }
    out
}

async fn fixture(w: &World) -> Result<Fixture, Box<dyn std::error::Error>> {
    let (canvas, server) = base(w).await?;
    let (web, web_out, _, web_change) = to_exit(&canvas, &server, "web", 443, "10.0.0.5:8080");
    let (api, api_out, _, api_change) = to_exit(&canvas, &server, "api", 8443, "10.0.0.6:9090");
    w.apply(&canvas, merge(vec![web_change, api_change])).await?;
    w.derive(&canvas.id).await?;
    Ok(Fixture {
        canvas: canvas.id.clone(),
        canvas_row: canvas,
        server: server.id.clone(),
        server_row: server,
        web,
        web_out,
        api,
        api_out,
    })
}

async fn server_row(w: &World, server: &ServerId) -> ServerEntity {
    w.db.process(FindServerById { id: server.clone() })
        .await
        .unwrap()
        .unwrap()
}

/// Registers a worker for the server and returns the identity its refresh key
/// would resolve to.
async fn register(
    w: &World,
    server: &ServerId,
) -> Result<AgentIdentity, Box<dyn std::error::Error>> {
    w.agents
        .process(RegisterWorker {
            credential: RegisterCredential::Operator(machine()),
            server_id: server.clone(),
            running_revision: 0,
            observed: None,
            reported: None,
            agent_version: None,
            agent_arch: None,
            capabilities: Vec::new(),
            last_update_error: None,
        })
        .await?;
    let row = server_row(w, server).await;
    Ok(AgentIdentity {
        server: row.id,
        generation: row.refresh_key_generation,
    })
}

/// Takes what is offered and acknowledges it with the given per-pod verdicts.
async fn take_and_ack(
    w: &World,
    agent: &AgentIdentity,
    pods: Vec<PodResult>,
) -> Result<i64, Box<dyn std::error::Error>> {
    let row = server_row(w, &agent.server).await;
    let snapshot =
        w.db.process(TakeInFlight {
            server: agent.server.clone(),
            generation: row.refresh_key_generation,
            epoch: row.watch_epoch,
        })
        .await?
        .ok_or("nothing in flight")?;
    w.agents
        .process(AckConfig {
            agent: agent.clone(),
            revision: snapshot.revision,
            error: None,
            pods,
        })
        .await?;
    Ok(snapshot.revision)
}

/// The pod named `name` applied fine; the tag a worker reports is the pod's id.
fn ok(name: &str) -> PodResult {
    PodResult {
        tag: key(name),
        error: None,
    }
}

fn failed(name: &str, error: &str) -> PodResult {
    PodResult {
        tag: key(name),
        error: Some(error.to_string()),
    }
}

fn report(running_revision: i64, pods: Vec<PodResult>) -> HealthReportInput {
    HealthReportInput {
        running_revision,
        upload_bytes: 10,
        download_bytes: 20,
        current_connections: 3,
        max_connections: 5,
        pods,
        reported: None,
    }
}

/// A report claiming to run `running_revision`.
async fn record_running(
    w: &World,
    agent: &AgentIdentity,
    running_revision: i64,
    pods: Vec<PodResult>,
) -> Result<(), OrchestrationError> {
    w.health
        .process(RecordHealthReport {
            agent: agent.clone(),
            report: report(running_revision, pods),
        })
        .await
}

/// A truthful report: the worker runs exactly what the master recorded as applied.
async fn record(
    w: &World,
    agent: &AgentIdentity,
    pods: Vec<PodResult>,
) -> Result<(), OrchestrationError> {
    let running_revision = w
        .view(&agent.server)
        .await
        .ok()
        .and_then(|view| view.applied)
        .map_or(0, |applied| applied.revision);
    record_running(w, agent, running_revision, pods).await
}

async fn server_history(w: &World, server: &ServerId) -> Vec<ServerHealthRecordEntity> {
    w.db.process(ListServerHealthHistory {
        server: server.clone(),
        start: Utc::now() - TimeDelta::days(30),
        end: Utc::now() + TimeDelta::days(1),
    })
    .await
    .unwrap()
}

/// The newest record of a pod, if any.
async fn latest_pod(w: &World, pod: &PodId) -> Option<PodHealthRecordEntity> {
    w.db.process(ListPodHealthHistory {
        pod: pod.clone(),
        start: Utc::now() - TimeDelta::days(30),
        end: Utc::now() + TimeDelta::days(1),
        limit: 1,
    })
    .await
    .unwrap()
    .into_iter()
    .next()
}

async fn assert_pods(w: &World, pods: &[&PodEntity], status: PodHealthStatus, message: &str) {
    for pod in pods {
        let record = latest_pod(w, &pod.id)
            .await
            .unwrap_or_else(|| panic!("{} has no record", pod.name));
        assert_eq!(record.status, status, "{}", pod.name);
        assert_eq!(record.message, message, "{}", pod.name);
    }
}

fn destination_of(config: &Config, name: &str) -> Remote {
    let tag = key(name);
    let forwarding = config
        .forwardings
        .iter()
        .find(|f| f.tag == tag)
        .unwrap_or_else(|| panic!("no forwarding tagged {tag}"));
    match forwarding.to.tree().unwrap() {
        ForwardingTo::Exit { destination, .. } => destination.clone(),
        other => panic!("{tag} is not an exit forwarding: {other:?}"),
    }
}

#[sqlx::test(migrator = "base::db::MIGRATOR")]
async fn a_report_records_the_server_and_every_pod(
    pool: sqlx::PgPool,
) -> TestResult {
    let w = world(pool).await?;
    let f = fixture(&w).await?;
    let agent = register(&w, &f.server).await?;
    take_and_ack(&w, &agent, vec![ok("web"), ok("api")]).await?;
    // A clean ack already brings the server Online, with a zero-counter record.
    let [ack_record] = server_history(&w, &f.server).await.try_into().unwrap();
    assert_eq!(ack_record.status, ServerHealthStatus::Online);
    assert_eq!(
        (ack_record.upload_bytes, ack_record.max_connections),
        (0, 0)
    );
    let row = server_row(&w, &f.server).await;
    assert_eq!(row.health_status, ServerHealthStatus::Online);
    assert!(row.last_health_report_at.is_none(), "only a report sets it");

    record(&w, &agent, vec![ok("web"), ok("api")]).await?;

    let [_, record] = server_history(&w, &f.server).await.try_into().unwrap();
    assert_eq!(record.status, ServerHealthStatus::Online);
    assert_eq!(
        (
            record.upload_bytes,
            record.download_bytes,
            record.current_connections,
            record.max_connections
        ),
        (10, 20, 3, 5)
    );
    let row = server_row(&w, &f.server).await;
    assert_eq!(row.health_status, ServerHealthStatus::Online);
    assert_eq!(row.last_health_report_at, Some(record.report_time));
    assert_eq!(row.last_seen_at, Some(record.report_time));
    assert_pods(&w, &[&f.web, &f.api], PodHealthStatus::Ready, "").await;
    Ok(())
}

/// The master recorded revision 1 as applied; a worker that says it runs
/// something else is out of sync, whatever the pods say.
#[sqlx::test(migrator = "base::db::MIGRATOR")]
async fn a_report_with_a_stale_running_revision_is_degraded(pool: sqlx::PgPool) -> TestResult {
    let w = world(pool).await?;
    let f = fixture(&w).await?;
    let agent = register(&w, &f.server).await?;
    let applied = take_and_ack(&w, &agent, vec![ok("web"), ok("api")]).await?;

    record_running(&w, &agent, applied - 1, vec![ok("web"), ok("api")]).await?;

    let row = server_row(&w, &f.server).await;
    assert_eq!(row.health_status, ServerHealthStatus::Degraded);
    assert_eq!(
        server_history(&w, &f.server).await.last().unwrap().status,
        ServerHealthStatus::Degraded
    );

    record(&w, &agent, vec![ok("web"), ok("api")]).await?;
    assert_eq!(
        server_row(&w, &f.server).await.health_status,
        ServerHealthStatus::Online
    );
    Ok(())
}

#[sqlx::test(migrator = "base::db::MIGRATOR")]
async fn a_revision_refused_as_a_whole_fails_every_pod_at_ack_time(
    pool: sqlx::PgPool,
) -> TestResult {
    let w = world(pool).await?;
    let f = fixture(&w).await?;
    let agent = register(&w, &f.server).await?;
    let row = server_row(&w, &f.server).await;
    let snapshot =
        w.db.process(TakeInFlight {
            server: f.server.clone(),
            generation: row.refresh_key_generation,
            epoch: row.watch_epoch,
        })
        .await?
        .unwrap();
    w.agents
        .process(AckConfig {
            agent: agent.clone(),
            revision: snapshot.revision,
            error: Some("config: cannot write certs".to_string()),
            pods: Vec::new(),
        })
        .await?;

    let view = w.view(&f.server).await?;
    assert_eq!(
        view.apply_error.as_deref(),
        Some("config: cannot write certs")
    );
    assert!(view.applied.is_none());
    assert_eq!(
        server_row(&w, &f.server).await.health_status,
        ServerHealthStatus::Degraded
    );
    assert_pods(
        &w,
        &[&f.web, &f.api],
        PodHealthStatus::Failed,
        "config: cannot write certs",
    )
    .await;
    Ok(())
}

#[sqlx::test(migrator = "base::db::MIGRATOR")]
async fn a_report_from_a_superseded_session_is_refused_and_records_nothing(
    pool: sqlx::PgPool,
) -> TestResult {
    let w = world(pool).await?;
    let f = fixture(&w).await?;
    let agent = register(&w, &f.server).await?;
    let stale = AgentIdentity {
        server: agent.server.clone(),
        generation: agent.generation - 1,
    };

    // The derivation hook may already have written `Deploying` records for the
    // pods; a refused report must add nothing on top.
    let before = latest_pod(&w, &f.web.id).await.map(|r| r.id.0);
    let refused = record(&w, &stale, vec![ok("web"), ok("api")]).await;
    assert!(
        matches!(refused, Err(OrchestrationError::PermissionDenied)),
        "{refused:?}"
    );
    assert!(server_history(&w, &f.server).await.is_empty());
    assert_eq!(
        latest_pod(&w, &f.web.id).await.map(|r| r.id.0),
        before
    );
    assert_eq!(
        server_row(&w, &f.server).await.health_status,
        ServerHealthStatus::Offline
    );
    Ok(())
}

#[sqlx::test(migrator = "base::db::MIGRATOR")]
async fn a_pod_whose_desired_entry_changed_is_deploying_until_applied(
    pool: sqlx::PgPool,
) -> TestResult {
    let w = world(pool).await?;
    let f = fixture(&w).await?;
    let agent = register(&w, &f.server).await?;
    take_and_ack(&w, &agent, vec![ok("web"), ok("api")]).await?;

    // Move api's exit: a new desired revision the worker has not been handed.
    w.apply(
        &f.canvas_row,
        GraphChange {
            put_exits: vec![ExitEntity {
                destination: "10.0.0.7:9090".to_string(),
                ..f.api_out.clone()
            }],
            ..GraphChange::default()
        },
    )
    .await?;
    w.derive(&f.canvas).await?;
    let view = w.view(&f.server).await?;
    assert_eq!(view.desired.as_ref().unwrap().revision, 2);
    assert_eq!(view.applied.as_ref().unwrap().revision, 1);

    record(&w, &agent, vec![ok("web"), ok("api")]).await?;

    assert_pods(&w, &[&f.api], PodHealthStatus::Deploying, "").await;
    assert_pods(&w, &[&f.web], PodHealthStatus::Ready, "").await;
    // Lagging within the grace period is not degraded.
    assert_eq!(
        server_history(&w, &f.server).await.last().unwrap().status,
        ServerHealthStatus::Online
    );
    Ok(())
}

#[sqlx::test(migrator = "base::db::MIGRATOR")]
async fn a_partial_apply_keeps_the_failed_pods_old_shape_and_degrades_the_server(
    pool: sqlx::PgPool,
) -> TestResult {
    let w = world(pool).await?;
    let f = fixture(&w).await?;
    let agent = register(&w, &f.server).await?;
    take_and_ack(&w, &agent, vec![ok("web"), ok("api")]).await?;

    // Revision 2: web's exit moves, api's exit moves, and a brand-new pod appears.
    let (fresh, _, _, fresh_change) =
        to_exit(&f.canvas_row, &f.server_row, "fresh", 9443, "10.0.0.8:1000");
    let mut change = fresh_change;
    change.put_exits.push(ExitEntity {
        destination: "10.0.0.5:8081".to_string(),
        ..f.web_out.clone()
    });
    change.put_exits.push(ExitEntity {
        destination: "10.0.0.7:9090".to_string(),
        ..f.api_out.clone()
    });
    w.apply(&f.canvas_row, change).await?;
    w.derive(&f.canvas).await?;

    let revision = take_and_ack(
        &w,
        &agent,
        vec![
            ok("web"),
            failed("api", "bind 203.0.113.10:8443: address in use"),
            failed("fresh", "bind 203.0.113.10:9443: permission denied"),
        ],
    )
    .await?;
    assert_eq!(revision, 2);

    let view = w.view(&f.server).await?;
    assert!(view.in_flight.is_none());
    assert_eq!(view.apply_error, None);
    assert_eq!(view.failed_revision, Some(2));
    let mut failed_tags: Vec<&str> = view.failed_pods.iter().map(|p| p.tag.as_str()).collect();
    failed_tags.sort_unstable();
    assert_eq!(failed_tags, [key("api"), key("fresh")]);

    let applied = view.applied.as_ref().expect("the mix is applied");
    assert_eq!(applied.revision, 2);
    let config = Config::from_toml_str(&applied.toml)?;
    assert_eq!(
        destination_of(&config, "web"),
        Remote::parse("10.0.0.5:8081")?,
        "the healthy pod runs the new revision"
    );
    assert_eq!(
        destination_of(&config, "api"),
        Remote::parse("10.0.0.6:9090")?,
        "the failed pod keeps the shape it was running"
    );
    assert!(
        !config.forwardings.iter().any(|f| f.tag == key("fresh")),
        "a failed pod with no previous shape runs nothing"
    );
    assert_eq!(
        applied.forwardings.len(),
        config.forwardings.len(),
        "the dependency list stays index-aligned with the TOML"
    );
    let api_deps = applied
        .forwardings
        .iter()
        .find(|d| d.pod == f.api.id)
        .expect("api keeps its dependency entry");
    assert_eq!(api_deps.serves.port, 8443);

    // The verdict is visible the moment the ack lands, before any report.
    assert_eq!(
        server_row(&w, &f.server).await.health_status,
        ServerHealthStatus::Degraded
    );
    let [first_ack, ack_record] = server_history(&w, &f.server).await.try_into().unwrap();
    assert_eq!(first_ack.status, ServerHealthStatus::Online);
    assert_eq!(ack_record.status, ServerHealthStatus::Degraded);
    assert_eq!((ack_record.upload_bytes, ack_record.download_bytes), (0, 0));
    assert_pods(
        &w,
        &[&f.api],
        PodHealthStatus::Failed,
        "bind 203.0.113.10:8443: address in use",
    )
    .await;
    assert_pods(
        &w,
        &[&fresh],
        PodHealthStatus::Failed,
        "bind 203.0.113.10:9443: permission denied",
    )
    .await;
    assert_pods(&w, &[&f.web], PodHealthStatus::Ready, "").await;

    // The worker reports what it actually runs.
    record(&w, &agent, vec![ok("web"), ok("api")]).await?;

    let row = server_row(&w, &f.server).await;
    assert_eq!(row.health_status, ServerHealthStatus::Degraded);
    assert_eq!(
        server_history(&w, &f.server).await.last().unwrap().status,
        ServerHealthStatus::Degraded
    );
    assert_pods(
        &w,
        &[&f.api],
        PodHealthStatus::Failed,
        "bind 203.0.113.10:8443: address in use",
    )
    .await;
    assert_pods(&w, &[&f.web], PodHealthStatus::Ready, "").await;
    Ok(())
}

#[sqlx::test(migrator = "base::db::MIGRATOR")]
async fn an_ack_must_name_every_pod_of_the_revision_exactly_once(pool: sqlx::PgPool) -> TestResult {
    let w = world(pool).await?;
    let f = fixture(&w).await?;
    let agent = register(&w, &f.server).await?;
    let row = server_row(&w, &f.server).await;
    let snapshot =
        w.db.process(TakeInFlight {
            server: f.server.clone(),
            generation: row.refresh_key_generation,
            epoch: row.watch_epoch,
        })
        .await?
        .unwrap();

    for pods in [
        vec![ok("web")],
        vec![ok("web"), ok("api"), ok("api")],
        vec![ok("web"), ok("ghost")],
    ] {
        let refused = w
            .agents
            .process(AckConfig {
                agent: agent.clone(),
                revision: snapshot.revision,
                error: None,
                pods,
            })
            .await;
        assert!(
            matches!(&refused, Err(OrchestrationError::Invalid(m)) if m == "pod results do not match the revision"),
            "{refused:?}"
        );
    }
    let view = w.view(&f.server).await?;
    assert_eq!(
        view.in_flight.as_ref().map(|s| s.revision),
        Some(snapshot.revision),
        "a refused ack leaves the view untouched"
    );
    assert!(view.applied.is_none());
    Ok(())
}

#[sqlx::test(migrator = "base::db::MIGRATOR")]
async fn silence_past_the_threshold_marks_the_server_offline(pool: sqlx::PgPool) -> TestResult {
    let w = world(pool).await?;
    let f = fixture(&w).await?;
    let agent = register(&w, &f.server).await?;
    take_and_ack(&w, &agent, vec![ok("web"), ok("api")]).await?;
    record(&w, &agent, vec![ok("web"), ok("api")]).await?;
    let reported_at = server_row(&w, &f.server)
        .await
        .last_health_report_at
        .unwrap();
    let threshold = w.health.config.health_offline_after();
    let held = server_row(&w, &f.server).await;
    assert!(
        held.session_lease_until.is_some(),
        "registering took the lease"
    );
    assert!(
        register(&w, &f.server).await.is_err(),
        "a second worker is refused while the session is live"
    );

    let before = reported_at + threshold - TimeDelta::seconds(1);
    assert!(
        w.health
            .process(SweepLiveness { now: before })
            .await?
            .flipped
            .is_empty()
    );
    let row = server_row(&w, &f.server).await;
    assert_eq!(row.health_status, ServerHealthStatus::Online);
    assert_eq!(row.session_lease_until, held.session_lease_until);
    assert_eq!(row.watch_epoch, held.watch_epoch);

    let after = reported_at + threshold + TimeDelta::seconds(1);
    let swept = w.health.process(SweepLiveness { now: after }).await?;
    assert_eq!(
        swept.flipped.iter().map(|id| id.0.clone()).collect::<Vec<_>>(),
        vec![f.server.0.clone()]
    );
    assert!(
        swept.revoked.is_empty(),
        "the flip already handed the session back"
    );
    let row = server_row(&w, &f.server).await;
    assert_eq!(row.health_status, ServerHealthStatus::Offline);
    assert_eq!(
        row.last_health_report_at,
        Some(reported_at),
        "only a report advances the report time"
    );
    // Going offline hands the watch session back: the stream that held it (a
    // zombie behind a proxy, say) is fenced out, and the next registration —
    // the worker's replacement, or the worker itself after a restart — is
    // accepted at once instead of being refused for as long as that stream
    // keeps renewing.
    assert_eq!(row.session_lease_until, None, "the lease is dropped");
    assert_eq!(row.watch_epoch, held.watch_epoch + 1, "the epoch moves");
    let now = Utc::now();
    assert!(
        !w.db
            .process(RenewServerWatchSession {
                server: f.server.clone(),
                generation: agent.generation,
                epoch: held.watch_epoch,
                now,
                lease_until: now + TimeDelta::seconds(30),
            })
            .await?,
        "the stream that held the lease cannot renew it"
    );
    let successor = register(&w, &f.server).await?;
    assert_eq!(successor.generation, agent.generation + 1);
    let history = server_history(&w, &f.server).await;
    let last = history.last().unwrap();
    assert_eq!(last.status, ServerHealthStatus::Offline);
    assert_eq!((last.upload_bytes, last.download_bytes), (0, 0));
    assert!(
        w.health
            .process(SweepLiveness { now: after })
            .await?
            .flipped
            .is_empty(),
        "an offline server is not flipped again"
    );
    Ok(())
}

/// The zombie the flip cannot reach, as it happened on a live host: a worker's
/// connection dies, the server goes offline and the session is handed back; the
/// worker registers again while the server is still offline, and that connection
/// dies too, before the first report. A proxy that never noticed keeps the new
/// watch stream open, so the master renews the lease for nobody and refuses every
/// registration after it. The status never changes again, so only the sweep can
/// take the session back — and only once the registration is older than the
/// offline threshold, so a worker that has just registered keeps its session.
#[sqlx::test(migrator = "base::db::MIGRATOR")]
async fn an_offline_server_held_by_a_session_that_never_reported_is_revoked(
    pool: sqlx::PgPool,
) -> TestResult {
    let w = world(pool).await?;
    let f = fixture(&w).await?;
    let first = register(&w, &f.server).await?;
    take_and_ack(&w, &first, vec![ok("web"), ok("api")]).await?;
    record(&w, &first, vec![ok("web"), ok("api")]).await?;
    assert!(
        w.health
            .process(MarkServerOffline {
                server: f.server.clone(),
                generation: Some(first.generation),
            })
            .await?
    );

    let agent = register(&w, &f.server).await?;
    let now = Utc::now();
    let claimed =
        w.db.process(ClaimServerWatchSession {
            server: f.server.clone(),
            generation: agent.generation,
            now,
            lease_until: now + TimeDelta::seconds(30),
        })
        .await?
        .unwrap();
    assert_eq!(claimed.health_status, ServerHealthStatus::Offline);
    let registered_at = claimed.registered_at.unwrap();
    assert!(
        claimed.last_health_report_at.unwrap() < registered_at,
        "the last report is the previous session's"
    );
    let threshold = w.health.config.health_offline_after();
    // The stream keeps renewing whatever the clock says, as the master's own
    // heartbeat does for a stream behind a proxy.
    let renew = async |at: DateTime<Utc>| {
        w.db.process(RenewServerWatchSession {
            server: f.server.clone(),
            generation: agent.generation,
            epoch: claimed.watch_epoch,
            now: at,
            lease_until: at + TimeDelta::seconds(30),
        })
        .await
        .unwrap()
    };

    let before = registered_at + threshold - TimeDelta::seconds(1);
    assert!(renew(before).await);
    let swept = w.health.process(SweepLiveness { now: before }).await?;
    assert!(swept.flipped.is_empty() && swept.revoked.is_empty());
    let row = server_row(&w, &f.server).await;
    assert_eq!(row.watch_epoch, claimed.watch_epoch);
    assert!(
        row.session_lease_until.is_some(),
        "a registration younger than the threshold keeps its session"
    );

    let after = registered_at + threshold + TimeDelta::seconds(1);
    assert!(renew(after).await);
    let history = server_history(&w, &f.server).await.len();
    let swept = w.health.process(SweepLiveness { now: after }).await?;
    assert!(swept.flipped.is_empty(), "the server was offline already");
    assert_eq!(
        swept.revoked.iter().map(|id| id.0.clone()).collect::<Vec<_>>(),
        vec![f.server.0.clone()]
    );
    let row = server_row(&w, &f.server).await;
    assert_eq!(row.health_status, ServerHealthStatus::Offline);
    assert_eq!(row.session_lease_until, None, "the lease is dropped");
    assert_eq!(row.watch_epoch, claimed.watch_epoch + 1, "the epoch moves");
    assert_eq!(
        server_history(&w, &f.server).await.len(),
        history,
        "no status changed, so no record is written"
    );
    assert!(
        !renew(after).await,
        "the stream that held the lease cannot renew it"
    );
    let successor = register(&w, &f.server).await?;
    assert_eq!(successor.generation, agent.generation + 1);
    Ok(())
}

/// A long-lived session that keeps reporting is never revoked, however old its
/// registration: its reports keep the server out of `Offline`.
#[sqlx::test(migrator = "base::db::MIGRATOR")]
async fn a_session_that_keeps_reporting_is_never_revoked(pool: sqlx::PgPool) -> TestResult {
    let w = world(pool).await?;
    let f = fixture(&w).await?;
    let agent = register(&w, &f.server).await?;
    take_and_ack(&w, &agent, vec![ok("web"), ok("api")]).await?;
    let held = server_row(&w, &f.server).await;
    // A report an hour into the session, and a sweep just after it.
    let reported_at = held.registered_at.unwrap() + TimeDelta::hours(1);
    assert!(
        w.db.process(InsertServerHealthRecord {
            server: f.server.clone(),
            generation: agent.generation,
            status: ServerHealthStatus::Online,
            report_time: reported_at,
            upload_bytes: 0,
            download_bytes: 0,
            current_connections: 0,
            max_connections: 0,
            pods: Vec::new(),
        })
        .await?
        .is_some()
    );
    let now = reported_at + TimeDelta::seconds(1);
    assert!(
        w.db.process(RenewServerWatchSession {
            server: f.server.clone(),
            generation: agent.generation,
            epoch: held.watch_epoch,
            now,
            lease_until: now + TimeDelta::seconds(30),
        })
        .await?
    );

    let swept = w.health.process(SweepLiveness { now }).await?;
    assert!(swept.flipped.is_empty() && swept.revoked.is_empty());
    let row = server_row(&w, &f.server).await;
    assert_eq!(row.health_status, ServerHealthStatus::Online);
    assert_eq!(row.watch_epoch, held.watch_epoch);
    assert!(row.session_lease_until.is_some());
    Ok(())
}

#[sqlx::test(migrator = "base::db::MIGRATOR")]
async fn a_closing_stream_marks_its_own_server_offline_but_not_a_successors(
    pool: sqlx::PgPool,
) -> TestResult {
    let w = world(pool).await?;
    let f = fixture(&w).await?;
    let agent = register(&w, &f.server).await?;
    take_and_ack(&w, &agent, vec![ok("web"), ok("api")]).await?;
    record(&w, &agent, vec![ok("web"), ok("api")]).await?;

    let held = server_row(&w, &f.server).await;
    let stale = MarkServerOffline {
        server: f.server.clone(),
        generation: Some(agent.generation - 1),
    };
    assert!(!w.health.process(stale).await?);
    let row = server_row(&w, &f.server).await;
    assert_eq!(row.health_status, ServerHealthStatus::Online);
    assert_eq!(row.session_lease_until, held.session_lease_until);
    assert_eq!(row.watch_epoch, held.watch_epoch);

    assert!(
        w.health
            .process(MarkServerOffline {
                server: f.server.clone(),
                generation: Some(agent.generation),
            })
            .await?
    );
    let row = server_row(&w, &f.server).await;
    assert_eq!(row.health_status, ServerHealthStatus::Offline);
    assert_eq!(
        row.session_lease_until, None,
        "the stream's end frees the server"
    );
    assert_eq!(row.watch_epoch, held.watch_epoch + 1);
    assert!(
        !w.health
            .process(MarkServerOffline {
                server: f.server.clone(),
                generation: Some(agent.generation),
            })
            .await?,
        "already offline is a no-op"
    );
    // ack (Online), report (Online), stream close (Offline); no-ops add nothing.
    assert_eq!(server_history(&w, &f.server).await.len(), 3);
    Ok(())
}

#[sqlx::test(migrator = "base::db::MIGRATOR")]
async fn retention_deletes_only_records_older_than_their_ttl(pool: sqlx::PgPool) -> TestResult {
    let w = world(pool).await?;
    let f = fixture(&w).await?;
    let agent = register(&w, &f.server).await?;
    // `timestamptz` keeps microseconds; a nanosecond `now` would not round-trip.
    let now = Utc::now().duration_trunc(TimeDelta::microseconds(1))?;
    let insert = async |report_time: DateTime<Utc>| {
        w.db.process(InsertServerHealthRecord {
            server: f.server.clone(),
            generation: agent.generation,
            status: ServerHealthStatus::Online,
            report_time,
            upload_bytes: 1,
            download_bytes: 1,
            current_connections: 1,
            max_connections: 1,
            pods: vec![NewPodHealthRecord {
                pod: f.web.id.clone(),
                status: PodHealthStatus::Ready,
                message: String::new(),
                report_time,
            }],
        })
        .await
    };
    let stale = now - w.health.config.server_health_ttl() - TimeDelta::hours(1);
    let fresh = now - w.health.config.server_health_ttl() + TimeDelta::hours(1);
    assert!(insert(stale).await?.is_some());
    assert!(insert(fresh).await?.is_some());

    w.health.process(TrimHealthHistory { now }).await?;

    let servers = server_history(&w, &f.server).await;
    assert_eq!(
        servers.iter().map(|r| r.report_time).collect::<Vec<_>>(),
        vec![fresh]
    );
    // The derivation hook wrote its own `Deploying` record for `web` at publish
    // time; only the stale row we inserted must be gone.
    let pods =
        w.db.process(ListPodHealthHistory {
            pod: f.web.id.clone(),
            start: now - TimeDelta::days(30),
            end: now,
            limit: 10,
        })
        .await?;
    let times: Vec<_> = pods.iter().map(|r| r.report_time).collect();
    assert!(times.contains(&fresh), "{times:?}");
    assert!(!times.contains(&stale), "{times:?}");
    Ok(())
}

// --- periodic execution: the run claim ---------------------------------------

/// A liveness sweep that is guaranteed to flip the server: the report is
/// backdated past the offline threshold, so the pass needs no fake clock.
async fn backdate_report(w: &World, f: &Fixture, generation: i64) -> DateTime<Utc> {
    let stale = Utc::now() - w.health.config.health_offline_after() - TimeDelta::hours(1);
    assert!(
        w.db.process(InsertServerHealthRecord {
            server: f.server.clone(),
            generation,
            status: ServerHealthStatus::Online,
            report_time: stale,
            upload_bytes: 0,
            download_bytes: 0,
            current_connections: 0,
            max_connections: 0,
            pods: Vec::new(),
        })
        .await
        .unwrap()
        .is_some()
    );
    stale
}

fn signal(tick: DateTime<Utc>) -> SweepLivenessSignal {
    SweepLivenessSignal {
        tick_unix_secs: tick.timestamp(),
    }
}

/// The cron scheduler publishes on a fixed cadence and AMQP is at-least-once, so
/// the same pass reaches the consumers repeatedly. Both fences of the run claim
/// are load-bearing, and this is the only place the pass is observed through the
/// hook rather than the service.
#[sqlx::test(migrator = "base::db::MIGRATOR")]
async fn a_periodic_signal_runs_its_pass_once_per_interval_and_never_twice_per_tick(
    pool: sqlx::PgPool,
) -> TestResult {
    let w = world(pool).await?;
    let f = fixture(&w).await?;
    let agent = register(&w, &f.server).await?;
    backdate_report(&w, &f, agent.generation).await;

    let hook = HealthCronHook {
        health: w.health.clone(),
    };
    // A consumer that claims with a zero interval: the interval fence is wide
    // open, so only the tick fence can refuse it.
    let eager = HealthCronHook {
        health: HealthService {
            db: w.db.clone(),
            config: OrchestrationConfig {
                liveness_interval_secs: 0,
                ..w.config.clone()
            },
            notifier: w.notifier.clone(),
        },
    };
    let tick = Utc::now();

    hook.process(signal(tick)).await?;
    assert_eq!(
        server_row(&w, &f.server).await.health_status,
        ServerHealthStatus::Offline,
        "the first signal runs the pass"
    );

    backdate_report(&w, &f, agent.generation).await;
    hook.process(signal(tick)).await?;
    assert_eq!(
        server_row(&w, &f.server).await.health_status,
        ServerHealthStatus::Online,
        "a redelivery of the same tick does nothing"
    );

    hook.process(signal(tick + TimeDelta::seconds(5))).await?;
    assert_eq!(
        server_row(&w, &f.server).await.health_status,
        ServerHealthStatus::Online,
        "a newer tick less than the interval past the last one is refused"
    );

    eager.process(signal(tick)).await?;
    assert_eq!(
        server_row(&w, &f.server).await.health_status,
        ServerHealthStatus::Online,
        "a tick that already ran is refused even with no interval to wait for"
    );

    // The regression this fence exists for: the interval is measured between
    // ticks, not from the moment a consumer got round to the last message. These
    // two ticks are exactly one interval apart while the run that stamped the row
    // happened milliseconds ago, and the pass must still run — measuring from the
    // run would subtract the processing delay from every period and drop every
    // other signal whenever the interval equals the publication cadence.
    hook.process(signal(tick + TimeDelta::seconds(30))).await?;
    assert_eq!(
        server_row(&w, &f.server).await.health_status,
        ServerHealthStatus::Offline,
        "a tick one full interval after the last one runs the pass again"
    );
    Ok(())
}

/// Two consumers handed the same signal at the same moment: one runs, the other
/// finds the run claimed. Without this, a horizontally scaled consumer would run
/// every pass as many times as it has instances.
#[sqlx::test(migrator = "base::db::MIGRATOR")]
async fn two_consumers_handed_one_signal_run_the_pass_once(pool: sqlx::PgPool) -> TestResult {
    let w = world(pool).await?;
    let f = fixture(&w).await?;
    let agent = register(&w, &f.server).await?;
    backdate_report(&w, &f, agent.generation).await;
    let hook = HealthCronHook {
        health: w.health.clone(),
    };
    let tick = Utc::now();

    let (a, b) = tokio::join!(hook.process(signal(tick)), hook.process(signal(tick)));
    a?;
    b?;

    let history = server_history(&w, &f.server).await;
    let offline = history
        .iter()
        .filter(|r| r.status == ServerHealthStatus::Offline)
        .count();
    assert_eq!(
        offline, 1,
        "exactly one of the two deliveries swept: {history:?}"
    );
    Ok(())
}

// --- many workers at once ---------------------------------------------------

/// Five servers on one canvas, each running one pod, derived once. Returns the
/// canvas and each server with its pod and that pod's edge.
async fn five_servers(
    w: &World,
) -> Result<(CanvasEntity, Vec<(ServerId, PodEntity, EdgeEntity)>), Box<dyn std::error::Error>> {
    let canvas = w
        .canvases
        .process(canvas_service::CreateCanvas {
            actor: operator(),
            name: "fleet".to_string(),
            description: String::new(),
            parent: None,
            position: pos0(),
        })
        .await?;
    let mut servers = Vec::new();
    let mut changes = Vec::new();
    for i in 0..5u8 {
        let server = w
            .servers
            .process(CreateServer {
                actor: operator(),
                canvas: canvas.id.clone(),
                name: format!("s{i}"),
                icon: String::new(),
                comment: String::new(),
                position: pos0(),
                ipv6_resolve: ServerIpv6Resolve::Tolerated,
                log_level: ServerLogLevel::Info,
                addresses: AddressOverrides {
                    override_v4: Some(format!("203.0.113.{}", 10 + i)),
                    override_v6: None,
                    extra_addresses: Vec::new(),
                },
            })
            .await?;
        let (pod, _, edge, change) =
            to_exit(&canvas, &server, &format!("pod-{i}"), 443, "10.0.0.5:8080");
        changes.push(change);
        servers.push((server.id, pod, edge));
    }
    w.apply(&canvas, merge(changes)).await?;
    w.derive(&canvas.id).await?;
    Ok((canvas, servers))
}

async fn canvas_generation(w: &World, canvas: &CanvasId) -> i64 {
    w.db.process(orchestration::entities::db::canvas::FindCanvasById { id: canvas.clone() })
        .await
        .unwrap()
        .unwrap()
        .generation
}

async fn is_stale(w: &World, canvas: &CanvasId) -> Result<bool, Box<dyn std::error::Error>> {
    Ok(w.db.process(ListStaleCanvases).await?.contains(canvas))
}

/// Workers apply one revision within the same instant and all acknowledge at
/// once. Every ack must land: an ack writes only its own server's rows, so
/// there is no shared row for five of them to collide on.
#[sqlx::test(migrator = "base::db::MIGRATOR")]
async fn five_workers_acking_at_once_all_land(pool: sqlx::PgPool) -> TestResult {
    let w = world(pool).await?;
    let (canvas, servers) = five_servers(&w).await?;
    let canvas = canvas.id;
    let mut acks = Vec::new();
    for (server, pod, _) in &servers {
        let agent = register(&w, server).await?;
        let row = server_row(&w, server).await;
        let snapshot =
            w.db.process(TakeInFlight {
                server: server.clone(),
                generation: row.refresh_key_generation,
                epoch: row.watch_epoch,
            })
            .await?
            .ok_or("nothing in flight")?;
        acks.push((agent, snapshot.revision, pod.name.clone()));
    }
    // Start the ack round caught up, so what the acks change is visible on its
    // own.
    w.derive(&canvas).await?;
    let generation = canvas_generation(&w, &canvas).await;
    assert!(!is_stale(&w, &canvas).await?, "the canvas starts caught up");

    let mut handles = Vec::new();
    for (agent, revision, tag) in acks {
        let agents = w.agents.clone();
        handles.push(tokio::spawn(async move {
            agents
                .process(AckConfig {
                    agent,
                    revision,
                    error: None,
                    pods: vec![ok(&tag)],
                })
                .await
        }));
    }
    for handle in handles {
        handle.await??;
    }
    for (server, _, _) in &servers {
        let view = w.view(server).await?;
        assert!(
            view.in_flight.is_none() && view.applied.is_some(),
            "{server:?}: the ack landed"
        );
    }

    // The acks make the canvas stale without touching its generation, and one
    // pass clears it.
    assert_eq!(
        canvas_generation(&w, &canvas).await,
        generation,
        "an ack does not bump the canvas generation"
    );
    assert!(is_stale(&w, &canvas).await?, "acks make the canvas stale");
    w.derive(&canvas).await?;
    assert!(!is_stale(&w, &canvas).await?, "one pass catches up");
    Ok(())
}

/// Five workers registering in the same instant all get their session: like an
/// ack, a registration writes only its own server's rows.
#[sqlx::test(migrator = "base::db::MIGRATOR")]
async fn five_workers_registering_at_once_all_land(pool: sqlx::PgPool) -> TestResult {
    let w = world(pool).await?;
    let (_canvas, servers) = five_servers(&w).await?;
    let mut handles = Vec::new();
    for (server, _, _) in &servers {
        let agents = w.agents.clone();
        let server = server.clone();
        handles.push(tokio::spawn(async move {
            agents
                .process(RegisterWorker {
                    credential: RegisterCredential::Operator(machine()),
                    server_id: server,
                    running_revision: 0,
                    observed: None,
                    reported: None,
                    agent_version: None,
                    agent_arch: None,
                    capabilities: Vec::new(),
                    last_update_error: None,
                })
                .await
        }));
    }
    for handle in handles {
        handle.await??;
    }
    for (server, _, _) in &servers {
        assert_eq!(server_row(&w, server).await.refresh_key_generation, 1);
    }
    Ok(())
}

/// A deleted server takes its view row, and its share of the staleness
/// counter, with it. The canvas still reads as stale, because deleting a server
/// is an edit and edits bump the generation: nothing a worker did is hidden.
#[sqlx::test(migrator = "base::db::MIGRATOR")]
async fn deleting_a_server_after_an_ack_keeps_the_canvas_stale(pool: sqlx::PgPool) -> TestResult {
    let w = world(pool).await?;
    let (canvas_row, servers) = five_servers(&w).await?;
    let canvas = canvas_row.id.clone();
    let mut agents = Vec::new();
    for (server, pod, _) in &servers {
        let agent = register(&w, server).await?;
        take_and_ack(&w, &agent, vec![ok(&pod.name)]).await?;
        agents.push(agent);
    }
    w.derive(&canvas).await?;
    assert!(!is_stale(&w, &canvas).await?);

    // One more ack on s0, which needs a new revision: move its pod.
    let (_, pod0, _) = &servers[0];
    w.apply(
        &canvas_row,
        GraphChange {
            put_pods: vec![PodEntity {
                port: 444,
                ..pod0.clone()
            }],
            ..GraphChange::default()
        },
    )
    .await?;
    w.derive(&canvas).await?;
    take_and_ack(&w, &agents[0], vec![ok(&pod0.name)]).await?;
    assert!(is_stale(&w, &canvas).await?);

    // Delete s4, whose view row carried a larger counter than s0's ack added.
    // Its pod goes first: a server with pods cannot be deleted.
    w.apply(
        &canvas_row,
        GraphChange {
            delete_pods: vec![servers[4].1.id.clone()],
            delete_edges: vec![servers[4].2.id.clone()],
            ..GraphChange::default()
        },
    )
    .await?;
    w.servers
        .process(orchestration::services::server::DeleteServer {
            actor: operator(),
            server: servers[4].0.clone(),
        })
        .await?;
    assert!(
        is_stale(&w, &canvas).await?,
        "the edit keeps the canvas stale even though the counter sum shrank"
    );
    w.derive(&canvas).await?;
    assert!(!is_stale(&w, &canvas).await?);
    Ok(())
}
