//! Health recording: what a worker's reports and acks turn into, and what the
//! master concludes when the reports stop.

#![allow(clippy::unwrap_used, clippy::panic, clippy::expect_used)]

mod common;

use chrono::{DateTime, TimeDelta, Utc};
use common::*;
use guru_worker_config::{Config, ForwardingTo, Remote};
use kanau::processor::Processor;
use orchestration::config::OrchestrationConfig;
use orchestration::entities::surreal::health::{
    InsertServerHealthRecord, ListNodeHealthHistory, ListServerHealthHistory, NewNodeHealthRecord,
    NodeHealthRecordEntity, NodeHealthStatus, ServerHealthRecordEntity, ServerHealthStatus,
};
use orchestration::entities::surreal::node::{
    EntryConfig, ExitConfig, LoadBalanceAggregateConfig, NodeId, NodeSpec, NodeWithPorts, PodConfig,
};
use orchestration::entities::surreal::server::{
    FindServerById, ServerEntity, ServerId, ServerIpv6Resolve,
};
use orchestration::entities::surreal::view::TakeInFlight;
use orchestration::events::SweepLivenessSignal;
use orchestration::hooks::health::HealthCronHook;
use orchestration::services::OrchestrationError;
use orchestration::services::agent::{
    AckConfig,
    AgentIdentity,
    PodResult,
    RegisterCredential,
    RegisterWorker,
};
use orchestration::services::canvas as canvas_service;
use orchestration::services::edge::Connect;
use orchestration::services::health::HealthService;
use orchestration::services::health::{
    HealthReportInput, MarkServerOffline, RecordHealthReport, SweepLiveness, TrimHealthHistory,
};
use orchestration::services::node::{CreateNode, ReplaceNodeSpec};
use orchestration::services::server::{AddressOverrides, CreateServer};

/// One server, two pods: `web` (443, entry `web-in` → exit `web-out`) and
/// `api` (8443, entry `api-in` → exit `api-out`).
struct Fixture {
    canvas: CanvasId,
    server: ServerId,
    ip: ServerIpRecordId,
    web: NodeWithPorts,
    web_in: NodeWithPorts,
    web_out: NodeWithPorts,
    api: NodeWithPorts,
    api_in: NodeWithPorts,
    api_out: NodeWithPorts,
}

fn exit_spec(destination: &str) -> NodeSpec {
    NodeSpec::Exit(ExitConfig {
        destination: destination.to_string(),
        pass_proxy_protocol: None,
    })
}

fn entry_spec() -> NodeSpec {
    NodeSpec::Entry(EntryConfig {
        receive_proxy_protocol: None,
        tls: None,
    })
}

async fn create(
    w: &World,
    canvas: &CanvasId,
    name: &str,
    spec: NodeSpec,
) -> Result<NodeWithPorts, OrchestrationError> {
    create_with(w, canvas, name, spec, 0).await
}

async fn create_with(
    w: &World,
    canvas: &CanvasId,
    name: &str,
    spec: NodeSpec,
    item_count: u32,
) -> Result<NodeWithPorts, OrchestrationError> {
    w.nodes
        .process(CreateNode {
            actor: operator(),
            canvas: canvas.clone(),
            name: name.to_string(),
            comment: String::new(),
            spec,
            position: pos0(),
            item_count,
        })
        .await
}

async fn connect(
    w: &World,
    output: orchestration::entities::surreal::port::PortId,
    input: orchestration::entities::surreal::port::PortId,
) -> Result<(), OrchestrationError> {
    w.edges
        .process(Connect {
            actor: operator(),
            output_port: output,
            input_port: input,
        })
        .await?;
    Ok(())
}

/// Wires `entry -> pod -> exit`.
async fn wire(
    w: &World,
    pod: &NodeWithPorts,
    entry: &NodeWithPorts,
    exit: &NodeWithPorts,
) -> Result<(), OrchestrationError> {
    connect(w, port_of(pod, "listen"), port_of(entry, "listen")).await?;
    connect(w, port_of(exit, "destination"), port_of(pod, "destination")).await
}

type CanvasId = orchestration::entities::surreal::canvas::CanvasId;
// Kept as a name for the third tuple element, now the server id (addresses live
// on the server).
type ServerIpRecordId = orchestration::entities::surreal::server::ServerId;

/// A canvas with one server carrying one IP; the topology goes on top.
async fn base(
    w: &World,
) -> Result<(CanvasId, ServerId, ServerIpRecordId), Box<dyn std::error::Error>> {
    let canvas = w
        .canvases
        .process(canvas_service::CreateCanvas {
            actor: operator(),
            name: "prod".to_string(),
            description: String::new(),
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
            log_level: "info".to_string(),
            addresses: AddressOverrides {
                override_v4: Some("203.0.113.10".to_string()),
                override_v6: None,
                extra_addresses: Vec::new(),
            },
        })
        .await?;
    Ok((canvas.id, server.id.clone(), server.id))
}

fn pod_spec_on(server: &ServerIpRecordId, port: u16) -> NodeSpec {
    NodeSpec::Pod(PodConfig {
        server: server.clone(),
        port,
        bind_ip: None,
        advertise_ip: None,
    })
}

async fn fixture(w: &World) -> Result<Fixture, Box<dyn std::error::Error>> {
    let (canvas, server, ip) = base(w).await?;
    let web = create(w, &canvas, "web", pod_spec_on(&ip, 443)).await?;
    let web_in = create(w, &canvas, "web-in", entry_spec()).await?;
    let web_out = create(w, &canvas, "web-out", exit_spec("10.0.0.5:8080")).await?;
    let api = create(w, &canvas, "api", pod_spec_on(&ip, 8443)).await?;
    let api_in = create(w, &canvas, "api-in", entry_spec()).await?;
    let api_out = create(w, &canvas, "api-out", exit_spec("10.0.0.6:9090")).await?;
    wire(w, &web, &web_in, &web_out).await?;
    wire(w, &api, &api_in, &api_out).await?;
    w.derive(&canvas).await?;
    Ok(Fixture {
        canvas,
        server,
        ip,
        web,
        web_in,
        web_out,
        api,
        api_in,
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

fn ok(tag: &str) -> PodResult {
    PodResult {
        tag: tag.to_string(),
        error: None,
    }
}

fn failed(tag: &str, error: &str) -> PodResult {
    PodResult {
        tag: tag.to_string(),
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

/// The newest record of a node, if any.
async fn latest_node(w: &World, node: &NodeId) -> Option<NodeHealthRecordEntity> {
    w.db.process(ListNodeHealthHistory {
        node: node.clone(),
        start: Utc::now() - TimeDelta::days(30),
        end: Utc::now() + TimeDelta::days(1),
        limit: 1,
    })
    .await
    .unwrap()
    .into_iter()
    .next()
}

async fn assert_nodes(
    w: &World,
    nodes: &[&NodeWithPorts],
    status: NodeHealthStatus,
    message: &str,
) {
    for node in nodes {
        let record = latest_node(w, &node.node.id)
            .await
            .unwrap_or_else(|| panic!("{} has no record", node.node.name));
        assert_eq!(record.status, status, "{}", node.node.name);
        assert_eq!(record.message, message, "{}", node.node.name);
    }
}

fn destination_of(config: &Config, tag: &str) -> Remote {
    let forwarding = config
        .forwardings
        .iter()
        .find(|f| f.tag == tag)
        .unwrap_or_else(|| panic!("no forwarding tagged {tag}"));
    match &forwarding.to {
        ForwardingTo::Exit { destination, .. } => destination.clone(),
        other => panic!("{tag} is not an exit forwarding: {other:?}"),
    }
}

#[tokio::test]
async fn a_report_records_the_server_and_every_node_the_pods_carry() -> TestResult {
    let w = world().await?;
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
    assert_nodes(
        &w,
        &[&f.web, &f.web_in, &f.web_out, &f.api, &f.api_in, &f.api_out],
        NodeHealthStatus::Ready,
        "",
    )
    .await;
    Ok(())
}

/// The master recorded revision 1 as applied; a worker that says it runs
/// something else is out of sync, whatever the pods say.
#[tokio::test]
async fn a_report_with_a_stale_running_revision_is_degraded() -> TestResult {
    let w = world().await?;
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

/// Two pods feeding one exit through an aggregate: the aggregate and the exit
/// get exactly one row per event, carrying the worst of the two pods' verdicts.
#[tokio::test]
async fn a_node_shared_by_two_pods_gets_one_row_with_the_worst_verdict() -> TestResult {
    let w = world().await?;
    let (canvas, server, ip) = base(&w).await?;
    let web = create(&w, &canvas, "web", pod_spec_on(&ip, 443)).await?;
    let web_in = create(&w, &canvas, "web-in", entry_spec()).await?;
    let api = create(&w, &canvas, "api", pod_spec_on(&ip, 8443)).await?;
    let api_in = create(&w, &canvas, "api-in", entry_spec()).await?;
    // A thin aggregate, laid out the way the expansion lays out its lanes (an
    // operator's aggregate node takes bundles on named members instead).
    let thin = |key: &str, direction, position| orchestration::entities::surreal::node::NewPort {
        kind: orchestration::entities::surreal::port::PortKind::DeriveDestination,
        direction,
        key: key.to_string(),
        position,
    };
    let agg = w
        .db
        .process(orchestration::entities::surreal::node::CreateNodeRow {
            canvas: canvas.clone(),
            name: "agg".to_string(),
            comment: String::new(),
            spec: NodeSpec::LoadBalanceAggregate(LoadBalanceAggregateConfig::default()),
            position: pos0(),
            ports: vec![
                thin("source", orchestration::entities::surreal::port::PortDirection::Input, 0),
                thin("copy_0", orchestration::entities::surreal::port::PortDirection::Output, 1),
                thin("copy_1", orchestration::entities::surreal::port::PortDirection::Output, 2),
            ],
            import_sync: None,
        })
        .await?;
    let exit = create(&w, &canvas, "shared-out", exit_spec("10.0.0.5:8080")).await?;
    connect(&w, port_of(&web, "listen"), port_of(&web_in, "listen")).await?;
    connect(&w, port_of(&api, "listen"), port_of(&api_in, "listen")).await?;
    connect(&w, port_of(&agg, "copy_0"), port_of(&web, "destination")).await?;
    connect(&w, port_of(&agg, "copy_1"), port_of(&api, "destination")).await?;
    connect(&w, port_of(&exit, "destination"), port_of(&agg, "source")).await?;
    w.derive(&canvas).await?;
    let agent = register(&w, &server).await?;

    let rows = async |node: &NodeWithPorts| {
        w.db.process(ListNodeHealthHistory {
            node: node.node.id.clone(),
            start: Utc::now() - TimeDelta::days(30),
            end: Utc::now() + TimeDelta::days(1),
            limit: 100,
        })
        .await
    };
    let before = rows(&exit).await?.len();

    take_and_ack(&w, &agent, vec![ok("web"), failed("api", "boom")]).await?;
    let after_ack = rows(&exit).await?;
    assert_eq!(after_ack.len(), before + 1, "one row per node per ack");
    assert_eq!(after_ack[0].status, NodeHealthStatus::Failed);
    assert_eq!(after_ack[0].message, "boom");
    assert_eq!(rows(&agg).await?.len(), before + 1);

    record(&w, &agent, vec![ok("web"), ok("api")]).await?;
    let after_report = rows(&exit).await?;
    assert_eq!(
        after_report.len(),
        before + 2,
        "one row per node per report"
    );
    assert_eq!(
        after_report[0].status,
        NodeHealthStatus::Failed,
        "the failed pod's verdict wins"
    );
    assert_eq!(after_report[0].message, "boom");
    assert_nodes(&w, &[&web, &web_in], NodeHealthStatus::Ready, "").await;
    Ok(())
}

#[tokio::test]
async fn a_revision_refused_as_a_whole_fails_every_pod_at_ack_time() -> TestResult {
    let w = world().await?;
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
    assert_nodes(
        &w,
        &[&f.web, &f.web_in, &f.web_out, &f.api, &f.api_in, &f.api_out],
        NodeHealthStatus::Failed,
        "config: cannot write certs",
    )
    .await;
    Ok(())
}

#[tokio::test]
async fn a_report_from_a_superseded_session_is_refused_and_records_nothing() -> TestResult {
    let w = world().await?;
    let f = fixture(&w).await?;
    let agent = register(&w, &f.server).await?;
    let stale = AgentIdentity {
        server: agent.server.clone(),
        generation: agent.generation - 1,
    };

    // The derivation hook may already have written `Deploying` records for the
    // nodes; a refused report must add nothing on top.
    let before = latest_node(&w, &f.web.node.id).await.map(|r| r.id.0);
    let refused = record(&w, &stale, vec![ok("web"), ok("api")]).await;
    assert!(
        matches!(refused, Err(OrchestrationError::PermissionDenied)),
        "{refused:?}"
    );
    assert!(server_history(&w, &f.server).await.is_empty());
    assert_eq!(
        latest_node(&w, &f.web.node.id).await.map(|r| r.id.0),
        before
    );
    assert_eq!(
        server_row(&w, &f.server).await.health_status,
        ServerHealthStatus::Offline
    );
    Ok(())
}

#[tokio::test]
async fn a_pod_whose_desired_entry_changed_is_deploying_until_applied() -> TestResult {
    let w = world().await?;
    let f = fixture(&w).await?;
    let agent = register(&w, &f.server).await?;
    take_and_ack(&w, &agent, vec![ok("web"), ok("api")]).await?;

    // Move api's exit: a new desired revision the worker has not been handed.
    w.nodes
        .process(ReplaceNodeSpec {
            actor: operator(),
            node: f.api_out.node.id.clone(),
            spec: exit_spec("10.0.0.7:9090"),
            item_count: 0,
        })
        .await?;
    w.derive(&f.canvas).await?;
    let view = w.view(&f.server).await?;
    assert_eq!(view.desired.as_ref().unwrap().revision, 2);
    assert_eq!(view.applied.as_ref().unwrap().revision, 1);

    record(&w, &agent, vec![ok("web"), ok("api")]).await?;

    assert_nodes(
        &w,
        &[&f.api, &f.api_in, &f.api_out],
        NodeHealthStatus::Deploying,
        "",
    )
    .await;
    assert_nodes(
        &w,
        &[&f.web, &f.web_in, &f.web_out],
        NodeHealthStatus::Ready,
        "",
    )
    .await;
    // Lagging within the grace period is not degraded.
    assert_eq!(
        server_history(&w, &f.server).await.last().unwrap().status,
        ServerHealthStatus::Online
    );
    Ok(())
}

#[tokio::test]
async fn a_partial_apply_keeps_the_failed_pods_old_shape_and_degrades_the_server() -> TestResult {
    let w = world().await?;
    let f = fixture(&w).await?;
    let agent = register(&w, &f.server).await?;
    take_and_ack(&w, &agent, vec![ok("web"), ok("api")]).await?;

    // Revision 2: web's exit moves, api's exit moves, and a brand-new pod appears.
    for (node, destination) in [(&f.web_out, "10.0.0.5:8081"), (&f.api_out, "10.0.0.7:9090")] {
        w.nodes
            .process(ReplaceNodeSpec {
                actor: operator(),
                node: node.node.id.clone(),
                spec: exit_spec(destination),
                item_count: 0,
            })
            .await?;
    }
    let fresh = create(
        &w,
        &f.canvas,
        "fresh",
        NodeSpec::Pod(PodConfig {
            server: f.ip.clone(),
            port: 9443,
            bind_ip: None,
            advertise_ip: None,
        }),
    )
    .await?;
    let fresh_in = create(&w, &f.canvas, "fresh-in", entry_spec()).await?;
    let fresh_out = create(&w, &f.canvas, "fresh-out", exit_spec("10.0.0.8:1000")).await?;
    wire(&w, &fresh, &fresh_in, &fresh_out).await?;
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
    assert_eq!(failed_tags, ["api", "fresh"]);

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
        !config.forwardings.iter().any(|f| f.tag == "fresh"),
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
        .find(|d| d.pod.0 == f.api.node.id.0)
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
    assert_nodes(
        &w,
        &[&f.api, &f.api_in, &f.api_out],
        NodeHealthStatus::Failed,
        "bind 203.0.113.10:8443: address in use",
    )
    .await;
    assert_nodes(
        &w,
        &[&fresh, &fresh_in, &fresh_out],
        NodeHealthStatus::Failed,
        "bind 203.0.113.10:9443: permission denied",
    )
    .await;
    assert_nodes(
        &w,
        &[&f.web, &f.web_in, &f.web_out],
        NodeHealthStatus::Ready,
        "",
    )
    .await;

    // The worker reports what it actually runs.
    record(&w, &agent, vec![ok("web"), ok("api")]).await?;

    let row = server_row(&w, &f.server).await;
    assert_eq!(row.health_status, ServerHealthStatus::Degraded);
    assert_eq!(
        server_history(&w, &f.server).await.last().unwrap().status,
        ServerHealthStatus::Degraded
    );
    assert_nodes(
        &w,
        &[&f.api, &f.api_in, &f.api_out],
        NodeHealthStatus::Failed,
        "bind 203.0.113.10:8443: address in use",
    )
    .await;
    assert_nodes(
        &w,
        &[&f.web, &f.web_in, &f.web_out],
        NodeHealthStatus::Ready,
        "",
    )
    .await;
    Ok(())
}

#[tokio::test]
async fn an_ack_must_name_every_pod_of_the_revision_exactly_once() -> TestResult {
    let w = world().await?;
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

#[tokio::test]
async fn silence_past_the_threshold_marks_the_server_offline() -> TestResult {
    let w = world().await?;
    let f = fixture(&w).await?;
    let agent = register(&w, &f.server).await?;
    take_and_ack(&w, &agent, vec![ok("web"), ok("api")]).await?;
    record(&w, &agent, vec![ok("web"), ok("api")]).await?;
    let reported_at = server_row(&w, &f.server)
        .await
        .last_health_report_at
        .unwrap();
    let threshold = w.health.config.health_offline_after();

    let before = reported_at + threshold - TimeDelta::seconds(1);
    assert!(
        w.health
            .process(SweepLiveness { now: before })
            .await?
            .is_empty()
    );
    assert_eq!(
        server_row(&w, &f.server).await.health_status,
        ServerHealthStatus::Online
    );

    let after = reported_at + threshold + TimeDelta::seconds(1);
    let flipped = w.health.process(SweepLiveness { now: after }).await?;
    assert_eq!(
        flipped.iter().map(|id| id.0.clone()).collect::<Vec<_>>(),
        vec![f.server.0.clone()]
    );
    let row = server_row(&w, &f.server).await;
    assert_eq!(row.health_status, ServerHealthStatus::Offline);
    assert_eq!(
        row.last_health_report_at,
        Some(reported_at),
        "only a report advances the report time"
    );
    let history = server_history(&w, &f.server).await;
    let last = history.last().unwrap();
    assert_eq!(last.status, ServerHealthStatus::Offline);
    assert_eq!((last.upload_bytes, last.download_bytes), (0, 0));
    assert!(
        w.health
            .process(SweepLiveness { now: after })
            .await?
            .is_empty(),
        "an offline server is not flipped again"
    );
    Ok(())
}

#[tokio::test]
async fn a_closing_stream_marks_its_own_server_offline_but_not_a_successors() -> TestResult {
    let w = world().await?;
    let f = fixture(&w).await?;
    let agent = register(&w, &f.server).await?;
    take_and_ack(&w, &agent, vec![ok("web"), ok("api")]).await?;
    record(&w, &agent, vec![ok("web"), ok("api")]).await?;

    let stale = MarkServerOffline {
        server: f.server.clone(),
        generation: Some(agent.generation - 1),
    };
    assert!(!w.health.process(stale).await?);
    assert_eq!(
        server_row(&w, &f.server).await.health_status,
        ServerHealthStatus::Online
    );

    assert!(
        w.health
            .process(MarkServerOffline {
                server: f.server.clone(),
                generation: Some(agent.generation),
            })
            .await?
    );
    assert_eq!(
        server_row(&w, &f.server).await.health_status,
        ServerHealthStatus::Offline
    );
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

#[tokio::test]
async fn retention_deletes_only_records_older_than_their_ttl() -> TestResult {
    let w = world().await?;
    let f = fixture(&w).await?;
    let agent = register(&w, &f.server).await?;
    let now = Utc::now();
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
            nodes: vec![NewNodeHealthRecord {
                node: f.web.node.id.clone(),
                status: NodeHealthStatus::Ready,
                message: String::new(),
                report_time,
            }],
        })
        .await
    };
    let stale = now - w.health.config.server_health_ttl() - TimeDelta::hours(1);
    let fresh = now - w.health.config.server_health_ttl() + TimeDelta::hours(1);
    assert!(insert(stale).await?);
    assert!(insert(fresh).await?);

    w.health.process(TrimHealthHistory { now }).await?;

    let servers = server_history(&w, &f.server).await;
    assert_eq!(
        servers.iter().map(|r| r.report_time).collect::<Vec<_>>(),
        vec![fresh]
    );
    // The derivation hook wrote its own `Deploying` record for `web` at publish
    // time; only the stale row we inserted must be gone.
    let nodes =
        w.db.process(ListNodeHealthHistory {
            node: f.web.node.id.clone(),
            start: now - TimeDelta::days(30),
            end: now,
            limit: 10,
        })
        .await?;
    let times: Vec<_> = nodes.iter().map(|r| r.report_time).collect();
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
            nodes: Vec::new(),
        })
        .await
        .unwrap()
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
#[tokio::test]
async fn a_periodic_signal_runs_its_pass_once_per_interval_and_never_twice_per_tick() -> TestResult
{
    let w = world().await?;
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
#[tokio::test]
async fn two_consumers_handed_one_signal_run_the_pass_once() -> TestResult {
    let w = world().await?;
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
