//! Live streams at the service level: what a shared view publishes, when it is
//! reloaded and shared, and what the two health record feeds forward.
//!
//! The bus here is the in-process one (`LivePublisher::InProcess`), so these
//! tests exercise everything except the Redis hop — `live_redis.rs` covers that.

#![allow(clippy::unwrap_used, clippy::panic, clippy::expect_used)]

mod common;

use chrono::{TimeDelta, Utc};
use common::*;
use kanau::processor::Processor;
use orchestration::entities::db::agent_release::PublishAgentRelease;
use orchestration::entities::db::canvas::CanvasId;
use orchestration::entities::db::health::{
    InsertNodeHealthRecords, ListNodeHealthAfter, NewNodeHealthRecord, NodeHealthStatus,
    ServerHealthStatus,
};
use orchestration::entities::db::node::{
    CanvasExportAs, CanvasExportConfig, CanvasImportConfig, EntryConfig, ExitConfig, NodeSpec,
    NodeWithPorts, PodConfig,
};
use orchestration::entities::db::port::{PortId, PortKind};
use orchestration::entities::db::server::{ServerId, ServerIpv6Resolve};
use orchestration::entities::db::view::TakeInFlight;
use orchestration::events::live::{CanvasChangeKind, LiveMessage};
use orchestration::hooks::live::LiveEvent;
use orchestration::services::OrchestrationError;
use orchestration::services::agent::{
    AckConfig, AgentIdentity, PodResult, PollAgentUpdate, RegisterCredential, RegisterWorker,
};
use orchestration::services::canvas as canvas_service;
use orchestration::services::edge::Connect;
use orchestration::services::health::{HealthReportInput, RecordHealthReport, SweepLiveness};
use orchestration::services::live::{
    CanvasLive, RolloutsLive, ViewHandle, ViewValue, WatchCanvas, WatchNodeHealth, WatchRollouts,
    WatchServerHealth,
};
use orchestration::services::node::CreateNode;
use orchestration::services::server::{
    AddressOverrides, CreateServer, IssueServerAgentInstall, MoveServer, RequestAgentUpdate,
};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::broadcast;

/// How long a test waits for an event it expects. Generous: the bus is
/// in-process, so anything slower than this is a hang, not a slow machine.
const WAIT: Duration = Duration::from_secs(5);

// --- helpers -----------------------------------------------------------------

async fn canvas_named(w: &World, name: &str) -> Result<CanvasId, Box<dyn std::error::Error>> {
    Ok(w.canvases
        .process(canvas_service::CreateCanvas {
            actor: operator(),
            name: name.to_string(),
            description: String::new(),
        })
        .await?
        .id)
}

async fn make_server(
    w: &World,
    canvas: &CanvasId,
    name: &str,
    address: &str,
) -> Result<ServerId, Box<dyn std::error::Error>> {
    Ok(w.servers
        .process(CreateServer {
            actor: operator(),
            canvas: canvas.clone(),
            name: name.to_string(),
            icon: String::new(),
            comment: String::new(),
            position: pos0(),
            ipv6_resolve: ServerIpv6Resolve::Tolerated,
            log_level: "info".to_string(),
            addresses: AddressOverrides {
                override_v4: Some(address.to_string()),
                override_v6: None,
                extra_addresses: Vec::new(),
            },
        })
        .await?
        .id)
}

async fn make_node(
    w: &World,
    canvas: &CanvasId,
    name: &str,
    spec: NodeSpec,
) -> Result<NodeWithPorts, OrchestrationError> {
    w.nodes
        .process(CreateNode {
            actor: operator(),
            canvas: canvas.clone(),
            name: name.to_string(),
            comment: String::new(),
            spec,
            position: pos0(),
            item_count: 0,
        })
        .await
}

async fn connect(w: &World, output: PortId, input: PortId) -> Result<(), OrchestrationError> {
    w.edges
        .process(Connect {
            actor: operator(),
            output_port: output,
            input_port: input,
        })
        .await?;
    Ok(())
}

fn pod_on(server: &ServerId, port: u16) -> NodeSpec {
    NodeSpec::Pod(PodConfig {
        server: server.clone(),
        port,
        bind_ip: None,
        advertise_ip: None,
    })
}

/// A wired canvas with one server and one pod, ready to derive.
async fn wired(w: &World) -> Result<(CanvasId, ServerId), Box<dyn std::error::Error>> {
    let canvas = canvas_named(w, "prod").await?;
    let server = make_server(w, &canvas, "tokyo", "203.0.113.10").await?;
    let pod = make_node(w, &canvas, "web", pod_on(&server, 443)).await?;
    let entry = make_node(
        w,
        &canvas,
        "web-in",
        NodeSpec::Entry(EntryConfig {
            receive_proxy_protocol: None,
            tls: None,
        }),
    )
    .await?;
    let exit = make_node(
        w,
        &canvas,
        "web-out",
        NodeSpec::Exit(ExitConfig {
            destination: "10.0.0.5:8080".to_string(),
            pass_proxy_protocol: None,
        }),
    )
    .await?;
    connect(w, port_of(&pod, "listen"), port_of(&entry, "listen")).await?;
    connect(
        w,
        port_of(&exit, "destination"),
        port_of(&pod, "destination"),
    )
    .await?;
    Ok((canvas, server))
}

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
    let row =
        w.db.process(orchestration::entities::db::server::FindServerById { id: server.clone() })
            .await?
            .ok_or("server vanished")?;
    Ok(AgentIdentity {
        server: row.id,
        generation: row.refresh_key_generation,
    })
}

/// Takes the pending revision and acks it with the given verdicts.
async fn take_and_ack(
    w: &World,
    agent: &AgentIdentity,
    pods: Vec<PodResult>,
    error: Option<String>,
) -> Result<i64, Box<dyn std::error::Error>> {
    let row =
        w.db.process(orchestration::entities::db::server::FindServerById {
            id: agent.server.clone(),
        })
        .await?
        .ok_or("server vanished")?;
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
            error,
            pods,
        })
        .await?;
    Ok(snapshot.revision)
}

/// A pod that applied cleanly. `tag` is the pod node's name, which is what the
/// rendered forwarding is keyed by.
fn pod_ok(tag: &str) -> PodResult {
    PodResult {
        tag: tag.to_string(),
        error: None,
    }
}

/// Waits for the next value a view publishes that is not `Loading`.
async fn next_value<T: Clone + Send + Sync + 'static>(handle: &mut ViewHandle<T>) -> ViewValue<T> {
    tokio::time::timeout(WAIT, async {
        loop {
            handle.rx.changed().await.expect("the view task is alive");
            let value = handle.rx.borrow_and_update().clone();
            if !matches!(value, ViewValue::Loading) {
                return value;
            }
        }
    })
    .await
    .expect("a view value arrived")
}

fn ready<T>(value: &ViewValue<T>) -> (&Arc<T>, Option<&LiveMessage>) {
    match value {
        ViewValue::Ready { state, cause } => (state, cause.as_deref()),
        ViewValue::Loading => panic!("still loading"),
        ViewValue::Missing => panic!("the watched record is missing"),
    }
}

/// Waits for the next live message of interest, skipping resyncs and anything
/// the predicate rejects.
async fn next_message(
    events: &mut broadcast::Receiver<LiveEvent>,
    mut wanted: impl FnMut(&LiveMessage) -> bool,
) -> Arc<LiveMessage> {
    tokio::time::timeout(WAIT, async {
        loop {
            match events.recv().await.expect("the bus is alive") {
                LiveEvent::Message(message) if wanted(&message) => return message,
                _ => continue,
            }
        }
    })
    .await
    .expect("a live message arrived")
}

fn report(revision: i64) -> HealthReportInput {
    HealthReportInput {
        running_revision: revision,
        upload_bytes: 1,
        download_bytes: 2,
        current_connections: 3,
        max_connections: 4,
        pods: Vec::new(),
        reported: None,
    }
}

// --- the canvas view ---------------------------------------------------------

/// The opening snapshot has no cause, and every later edit — including a
/// metadata-only one that does not bump the generation — produces a new full
/// snapshot naming what changed.
#[sqlx::test(migrator = "base::db::MIGRATOR")]
async fn canvas_view_snapshot_then_full_refresh(pool: sqlx::PgPool) -> TestResult {
    let w = world(pool).await?;
    let canvas = canvas_named(&w, "prod").await?;
    let first = make_server(&w, &canvas, "tokyo", "203.0.113.10").await?;

    let mut handle = w
        .live
        .process(WatchCanvas {
            actor: operator(),
            canvas: canvas.clone(),
        })
        .await?;

    let opening = next_value(&mut handle).await;
    let (state, cause) = ready(&opening);
    assert!(cause.is_none(), "the opening snapshot has no cause");
    assert_eq!(state.contents.servers.len(), 1);

    let second = make_server(&w, &canvas, "osaka", "203.0.113.11").await?;
    let created = next_value(&mut handle).await;
    let (state, cause) = ready(&created);
    match cause.expect("a cause") {
        LiveMessage::CanvasChanged { kind, ids, .. } => {
            assert_eq!(*kind, CanvasChangeKind::ServerCreated);
            assert_eq!(ids, &vec![second.to_string()]);
        }
        other => panic!("unexpected cause: {other:?}"),
    }
    assert_eq!(state.contents.servers.len(), 2);

    // Metadata only: the generation does not move, so nothing but the live event
    // could tell a dashboard the server was dragged.
    let before = state.contents.canvas.generation;
    w.servers
        .process(MoveServer {
            actor: operator(),
            server: first.clone(),
            position: pos(120, 340),
        })
        .await?;
    let moved = next_value(&mut handle).await;
    let (state, cause) = ready(&moved);
    match cause.expect("a cause") {
        LiveMessage::CanvasChanged { kind, ids, .. } => {
            assert_eq!(*kind, CanvasChangeKind::ServerMoved);
            assert_eq!(ids, &vec![first.to_string()]);
        }
        other => panic!("unexpected cause: {other:?}"),
    }
    assert_eq!(state.contents.canvas.generation, before);
    let dragged = state
        .contents
        .servers
        .iter()
        .find(|s| s.id == first)
        .expect("the server is still there");
    assert_eq!((dragged.position.x, dragged.position.y), (120, 340));
    Ok(())
}

/// The agent's install and update state lives on the server row but feeds
/// nothing derived, so only the live event can tell the panel that a key was
/// issued, an update was requested, or the worker reported why it failed.
#[sqlx::test(migrator = "base::db::MIGRATOR")]
async fn agent_state_changes_refresh_the_canvas_view(pool: sqlx::PgPool) -> TestResult {
    let mut w = world(pool).await?;
    w.servers.config.agent_public_base_url = "https://guru.test".to_string();
    w.agents.config.agent_public_base_url = "https://guru.test".to_string();
    w.db.process(PublishAgentRelease {
        version: "0.2.0-beta".to_string(),
        sha256: "c".repeat(64),
        arch: "x86_64".to_string(),
        now: Utc::now(),
    })
    .await?;
    let canvas = canvas_named(&w, "prod").await?;
    let server = make_server(&w, &canvas, "tokyo", "203.0.113.10").await?;
    w.agents
        .process(RegisterWorker {
            credential: RegisterCredential::Operator(machine()),
            server_id: server.clone(),
            running_revision: 0,
            observed: None,
            reported: None,
            agent_version: Some("0.1.0".to_string()),
            agent_arch: Some("x86_64".to_string()),
            last_update_error: None,
        })
        .await?;

    let mut handle = w
        .live
        .process(WatchCanvas {
            actor: operator(),
            canvas: canvas.clone(),
        })
        .await?;
    let opening = next_value(&mut handle).await;
    let (state, _) = ready(&opening);
    let before = state.contents.canvas.generation;

    let server_updated = |value: &ViewValue<CanvasLive>| {
        let (state, cause) = ready(value);
        match cause.expect("a cause") {
            LiveMessage::CanvasChanged { kind, ids, .. } => {
                assert_eq!(*kind, CanvasChangeKind::ServerUpdated);
                assert_eq!(ids, &vec![server.to_string()]);
            }
            other => panic!("unexpected cause: {other:?}"),
        }
        assert_eq!(state.contents.canvas.generation, before, "metadata only");
        state
            .contents
            .servers
            .iter()
            .find(|s| s.id == server)
            .expect("the server is still there")
            .clone()
    };

    w.servers
        .process(IssueServerAgentInstall {
            actor: operator(),
            server: server.clone(),
            unit: Some("tokyo-1".to_string()),
        })
        .await?;
    let issued = server_updated(&next_value(&mut handle).await);
    assert_eq!(issued.agent_unit.as_deref(), Some("tokyo-1"));
    assert!(issued.agent_key_issued_at.is_some());

    w.servers
        .process(RequestAgentUpdate {
            actor: operator(),
            server: server.clone(),
        })
        .await?;
    let requested = server_updated(&next_value(&mut handle).await);
    assert_eq!(
        requested.agent_update_requested.as_deref(),
        Some("0.2.0-beta")
    );

    let identity = AgentIdentity {
        server: server.clone(),
        generation: issued.refresh_key_generation,
    };
    let offered = w
        .agents
        .process(PollAgentUpdate {
            agent: identity.clone(),
            last_error: None,
        })
        .await?;
    assert!(
        offered.is_some(),
        "the poll offers the update without settling it"
    );
    w.agents
        .process(PollAgentUpdate {
            agent: identity,
            last_error: Some("checksum mismatch".to_string()),
        })
        .await?;
    let failed = server_updated(&next_value(&mut handle).await);
    assert_eq!(failed.agent_update_requested, None);
    assert_eq!(
        failed.agent_update_error.as_deref(),
        Some("checksum mismatch")
    );
    Ok(())
}

/// An edit in a subcanvas refreshes the parent that imports it: the import
/// node's ports are the subcanvas's exports, so the parent's picture changed.
#[sqlx::test(migrator = "base::db::MIGRATOR")]
async fn subcanvas_edit_refreshes_parent_view(pool: sqlx::PgPool) -> TestResult {
    let w = world(pool).await?;
    let root = canvas_named(&w, "root").await?;
    let sub = canvas_named(&w, "sub").await?;
    let import = make_node(
        &w,
        &root,
        "sub",
        NodeSpec::CanvasImport(CanvasImportConfig {
            canvas: sub.clone(),
        }),
    )
    .await?;
    assert!(import.ports.is_empty(), "nothing is exported yet");

    let mut handle = w
        .live
        .process(WatchCanvas {
            actor: operator(),
            canvas: root.clone(),
        })
        .await?;
    let opening = next_value(&mut handle).await;
    ready(&opening);

    make_node(
        &w,
        &sub,
        "out",
        NodeSpec::CanvasExport(CanvasExportConfig {
            kind: PortKind::DeriveListen,
            direction: CanvasExportAs::InputIntoCanvas,
        }),
    )
    .await?;

    let refreshed = next_value(&mut handle).await;
    let (state, _) = ready(&refreshed);
    let node = state
        .contents
        .nodes
        .iter()
        .find(|n| n.node.id == import.node.id)
        .expect("the import node is still there");
    assert_eq!(
        node.ports.len(),
        1,
        "the import mirrors the new export (ports are keyed by the export's record key)"
    );
    Ok(())
}

/// Two watchers of one canvas share a single view: the same `Arc` reaches both,
/// and the registry drops the key once neither holds a handle.
#[sqlx::test(migrator = "base::db::MIGRATOR")]
async fn views_are_shared_and_dropped_at_zero(pool: sqlx::PgPool) -> TestResult {
    let w = world(pool).await?;
    let canvas = canvas_named(&w, "prod").await?;

    let mut first = w
        .live
        .process(WatchCanvas {
            actor: operator(),
            canvas: canvas.clone(),
        })
        .await?;
    let mut second = w
        .live
        .process(WatchCanvas {
            actor: operator(),
            canvas: canvas.clone(),
        })
        .await?;
    assert_eq!(w.live.canvases.active(), 1, "one view, two subscribers");

    let a = next_value(&mut first).await;
    let b = next_value(&mut second).await;
    let (state_a, _) = ready(&a);
    let (state_b, _) = ready(&b);
    assert!(
        Arc::ptr_eq(state_a, state_b),
        "both watchers see the very same snapshot"
    );

    drop(first);
    assert_eq!(w.live.canvases.active(), 1, "one subscriber is left");
    drop(second);
    assert_eq!(w.live.canvases.active(), 0, "the view is gone");
    Ok(())
}

/// A deleted canvas becomes `Missing`, which is what turns the stream into a
/// `NOT_FOUND` instead of a silent hang.
#[sqlx::test(migrator = "base::db::MIGRATOR")]
async fn deleted_canvas_ends_with_missing(pool: sqlx::PgPool) -> TestResult {
    let w = world(pool).await?;
    let canvas = canvas_named(&w, "doomed").await?;
    let mut handle = w
        .live
        .process(WatchCanvas {
            actor: operator(),
            canvas: canvas.clone(),
        })
        .await?;
    ready(&next_value(&mut handle).await);

    w.canvases
        .process(canvas_service::DeleteCanvas {
            actor: operator(),
            canvas: canvas.clone(),
        })
        .await?;
    assert!(
        matches!(next_value(&mut handle).await, ViewValue::Missing),
        "the view reports the canvas is gone"
    );
    Ok(())
}

// --- the rollouts view -------------------------------------------------------

/// The rollout view follows both halves of a rollout: the derivation that
/// publishes a revision, and the ack that records it as applied.
#[sqlx::test(migrator = "base::db::MIGRATOR")]
async fn rollouts_follow_derive_and_ack(pool: sqlx::PgPool) -> TestResult {
    let w = world(pool).await?;
    let (canvas, server) = wired(&w).await?;

    let mut handle: ViewHandle<RolloutsLive> = w
        .live
        .process(WatchRollouts {
            actor: operator(),
            canvas: canvas.clone(),
        })
        .await?;
    let opening = next_value(&mut handle).await;
    let (state, _) = ready(&opening);
    assert_eq!(state.servers.len(), 1);
    assert!(state.servers[0].status.desired.is_none());
    assert!(
        state.servers[0].status.derivation_pending,
        "the edits are not derived yet"
    );

    w.derive(&canvas).await?;
    let derived = loop {
        let value = next_value(&mut handle).await;
        let (state, _) = ready(&value);
        if state.servers[0].status.desired.is_some() {
            break value;
        }
    };
    let (state, _) = ready(&derived);
    assert_eq!(
        state.servers[0].status.desired.as_ref().unwrap().revision,
        1
    );
    assert!(!state.servers[0].status.derivation_pending);

    let agent = register(&w, &server).await?;
    // Registration re-derives; make sure the revision is published again before
    // the worker takes it.
    w.derive(&canvas).await?;
    let revision = take_and_ack(&w, &agent, vec![pod_ok("web")], None).await?;
    let applied = loop {
        let value = next_value(&mut handle).await;
        let (state, _) = ready(&value);
        if state.servers[0].status.applied.is_some() {
            break value;
        }
    };
    let (state, _) = ready(&applied);
    assert_eq!(
        state.servers[0].status.applied.as_ref().unwrap().revision,
        revision
    );
    Ok(())
}

// --- the health feeds --------------------------------------------------------

/// A server-health watch opens with the history it was asked for, then sees one
/// event per accepted report — including the master's own offline flip.
#[sqlx::test(migrator = "base::db::MIGRATOR")]
async fn server_health_stream_dedupes_and_sees_offline(pool: sqlx::PgPool) -> TestResult {
    let w = world(pool).await?;
    let (canvas, server) = wired(&w).await?;
    w.derive(&canvas).await?;
    let agent = register(&w, &server).await?;
    let server_key = server.to_string();

    w.health
        .process(RecordHealthReport {
            agent: agent.clone(),
            report: report(0),
        })
        .await?;

    let watch = w
        .live
        .process(WatchServerHealth {
            actor: operator(),
            server: server.clone(),
            since: Utc::now() - TimeDelta::hours(1),
        })
        .await?;
    let mut events = watch.events;
    assert_eq!(
        watch.records.len(),
        1,
        "the snapshot carries the report already written"
    );
    let first = watch.records.last().unwrap().report_time;

    w.health
        .process(RecordHealthReport {
            agent: agent.clone(),
            report: report(0),
        })
        .await?;
    let message = next_message(
        &mut events,
        |m| matches!(m, LiveMessage::ServerHealth { server, .. } if *server == server_key),
    )
    .await;
    let LiveMessage::ServerHealth { record, .. } = &*message else {
        panic!("filtered above");
    };
    assert!(
        orchestration::events::live::live_time(record.report_time_unix_micros) > first,
        "the second report is newer than the snapshot's last record"
    );

    // The master, not the worker: a sweep far in the future flips the server.
    let flipped = w
        .health
        .process(SweepLiveness {
            now: Utc::now() + TimeDelta::days(1),
        })
        .await?;
    assert!(flipped.contains(&server));
    let message = next_message(&mut events, |m| {
        matches!(
            m,
            LiveMessage::ServerHealth { record, .. } if record.status == ServerHealthStatus::Offline
        )
    })
    .await;
    let LiveMessage::ServerHealth {
        status_changed,
        canvas: event_canvas,
        ..
    } = &*message
    else {
        panic!("filtered above");
    };
    assert!(status_changed, "an offline flip is a status change");
    assert_eq!(*event_canvas, canvas.to_string());
    Ok(())
}

/// A node goes `Deploying` when a revision is published and `Ready` when the
/// worker acks it; a failed pod carries the worker's message.
#[sqlx::test(migrator = "base::db::MIGRATOR")]
async fn node_health_deploying_then_ready_then_failed(pool: sqlx::PgPool) -> TestResult {
    let w = world(pool).await?;
    let (canvas, server) = wired(&w).await?;

    let watch = w
        .live
        .process(WatchNodeHealth {
            actor: operator(),
            node: {
                // The pod node: its health is what a worker's verdict settles.
                let contents =
                    w.db.process(orchestration::entities::db::topology::LoadCanvasContents {
                        canvas: canvas.clone(),
                    })
                    .await?
                    .expect("the canvas exists");
                contents
                    .nodes
                    .iter()
                    .find(|n| n.node.name == "web")
                    .expect("the pod is there")
                    .node
                    .id
                    .clone()
            },
            limit: 0,
        })
        .await?;
    let mut events = watch.events;
    let node_key = {
        let contents =
            w.db.process(orchestration::entities::db::topology::LoadCanvasContents {
                canvas: canvas.clone(),
            })
            .await?
            .expect("the canvas exists");
        contents
            .nodes
            .iter()
            .find(|n| n.node.name == "web")
            .unwrap()
            .node
            .id
            .to_string()
    };
    let of_node = |m: &LiveMessage| match m {
        LiveMessage::NodeHealth { records } => records.iter().any(|r| r.node == node_key),
        _ => false,
    };

    w.derive(&canvas).await?;
    let message = next_message(&mut events, of_node).await;
    let LiveMessage::NodeHealth { records } = &*message else {
        panic!("filtered above");
    };
    assert_eq!(
        records.iter().find(|r| r.node == node_key).unwrap().status,
        NodeHealthStatus::Deploying
    );

    let agent = register(&w, &server).await?;
    w.derive(&canvas).await?;
    take_and_ack(&w, &agent, vec![pod_ok("web")], None).await?;
    let message = next_message(&mut events, |m| match m {
        LiveMessage::NodeHealth { records } => records
            .iter()
            .any(|r| r.node == node_key && r.status == NodeHealthStatus::Ready),
        _ => false,
    })
    .await;
    assert!(matches!(&*message, LiveMessage::NodeHealth { .. }));

    // A whole-revision failure: every pod of the revision fails with the
    // worker's message, and so does every node its forwarding runs through.
    // Editing the exit's destination is what makes a *new* revision; a
    // metadata-only edit would leave the worker nothing to take.
    let exit_id = {
        let contents =
            w.db.process(orchestration::entities::db::topology::LoadCanvasContents {
                canvas: canvas.clone(),
            })
            .await?
            .expect("the canvas exists");
        contents
            .nodes
            .iter()
            .find(|n| n.node.name == "web-out")
            .expect("the exit is there")
            .node
            .id
            .clone()
    };
    w.nodes
        .process(orchestration::services::node::ReplaceNodeSpec {
            actor: operator(),
            node: exit_id,
            spec: NodeSpec::Exit(ExitConfig {
                destination: "10.0.0.9:9999".to_string(),
                pass_proxy_protocol: None,
            }),
            item_count: 0,
        })
        .await?;
    w.derive(&canvas).await?;
    take_and_ack(&w, &agent, vec![], Some("boom".to_string())).await?;
    let message = next_message(&mut events, |m| match m {
        LiveMessage::NodeHealth { records } => records
            .iter()
            .any(|r| r.node == node_key && r.status == NodeHealthStatus::Failed),
        _ => false,
    })
    .await;
    let LiveMessage::NodeHealth { records } = &*message else {
        panic!("filtered above");
    };
    assert_eq!(
        records
            .iter()
            .find(|r| r.node == node_key && r.status == NodeHealthStatus::Failed)
            .unwrap()
            .message,
        "boom"
    );
    Ok(())
}

/// A canvas view answers a resync by reloading: whatever was written while the
/// bus was down still reaches the watcher.
#[sqlx::test(migrator = "base::db::MIGRATOR")]
async fn resync_reloads_a_view_that_missed_its_events(pool: sqlx::PgPool) -> TestResult {
    let w = world(pool).await?;
    let canvas = canvas_named(&w, "prod").await?;
    let mut handle: ViewHandle<CanvasLive> = w
        .live
        .process(WatchCanvas {
            actor: operator(),
            canvas: canvas.clone(),
        })
        .await?;
    ready(&next_value(&mut handle).await);

    // A world whose services publish nothing: this is what a replica sees while
    // its subscriber is disconnected.
    let deaf = World {
        servers: orchestration::services::server::ServerService {
            db: w.db.clone(),
            notifier: Default::default(),
            config: w.config.clone(),
        },
        ..w
    };
    make_server(&deaf, &canvas, "tokyo", "203.0.113.10").await?;
    make_server(&deaf, &canvas, "osaka", "203.0.113.11").await?;

    deaf.bus.resync();
    let refreshed = loop {
        let value = next_value(&mut handle).await;
        let (state, cause) = ready(&value);
        assert!(cause.is_none(), "a resync reload names no cause");
        if state.contents.servers.len() == 2 {
            break value;
        }
    };
    ready(&refreshed);
    Ok(())
}

/// A watcher already open on a standalone canvas must learn when that canvas is
/// imported into a tree: its own match set contains only itself, so the event
/// published for the *parent* would never reach it and its `ancestors` would
/// stay empty until the tab was reloaded by hand.
#[sqlx::test(migrator = "base::db::MIGRATOR")]
async fn importing_a_watched_canvas_refreshes_its_own_view(pool: sqlx::PgPool) -> TestResult {
    let w = world(pool).await?;
    let root = canvas_named(&w, "root").await?;
    let sub = canvas_named(&w, "sub").await?;

    // Watch the target *before* it is imported; at this point it is a root.
    let mut handle: ViewHandle<CanvasLive> = w
        .live
        .process(WatchCanvas {
            actor: operator(),
            canvas: sub.clone(),
        })
        .await?;
    let opening = next_value(&mut handle).await;
    let (state, _) = ready(&opening);
    assert!(
        state.contents.ancestors.is_empty(),
        "a standalone canvas has no ancestors"
    );

    make_node(
        &w,
        &root,
        "sub",
        NodeSpec::CanvasImport(CanvasImportConfig {
            canvas: sub.clone(),
        }),
    )
    .await?;

    let refreshed = next_value(&mut handle).await;
    let (state, _) = ready(&refreshed);
    assert_eq!(
        state
            .contents
            .ancestors
            .iter()
            .map(|c| c.name.as_str())
            .collect::<Vec<_>>(),
        vec!["root"],
        "the imported canvas sees its new parent"
    );
    Ok(())
}

/// The recovery read of a node-health stream, driven exactly as the handler
/// drives it, across more rows than one page holds and with every row sharing a
/// timestamp — the case a newest-first capped read would silently truncate and
/// a timestamp-only cursor would collapse to a single record.
#[sqlx::test(migrator = "base::db::MIGRATOR")]
async fn node_health_recovery_pages_through_identical_timestamps(pool: sqlx::PgPool) -> TestResult {
    let w = world(pool).await?;
    let canvas = canvas_named(&w, "prod").await?;
    let server = make_server(&w, &canvas, "tokyo", "203.0.113.10").await?;
    let pod = make_node(&w, &canvas, "web", pod_on(&server, 443)).await?;

    // One timestamp for every row: this is what a single derivation or ack
    // writes, and it is what makes `report_time` alone not a total order.
    let at = Utc::now();
    const ROWS: usize = 450;
    let written =
        w.db.process(InsertNodeHealthRecords {
            records: (0..ROWS)
                .map(|i| NewNodeHealthRecord {
                    node: pod.node.id.clone(),
                    status: NodeHealthStatus::Deploying,
                    message: format!("row {i}"),
                    report_time: at,
                })
                .collect(),
        })
        .await?;
    assert_eq!(written.len(), ROWS);

    // The handler's loop: page on `(report_time, id)` until a short page.
    const PAGE: i64 = 200;
    let mut cursor: (chrono::DateTime<Utc>, Option<String>) = (chrono::DateTime::UNIX_EPOCH, None);
    let mut seen: Vec<String> = Vec::new();
    loop {
        let rows =
            w.db.process(ListNodeHealthAfter {
                node: pod.node.id.clone(),
                after: cursor.0,
                after_id: cursor
                    .1
                    .as_deref()
                    .map(orchestration::utils::ids::node_health_record_id),
                limit: PAGE,
            })
            .await?;
        let short = rows.len() < PAGE as usize;
        for row in &rows {
            cursor = (row.report_time, Some(row.id.to_string()));
            seen.push(row.id.to_string());
        }
        if short {
            break;
        }
    }

    assert_eq!(seen.len(), ROWS, "every row is delivered exactly once");
    let unique: std::collections::HashSet<&String> = seen.iter().collect();
    assert_eq!(unique.len(), ROWS, "no row is delivered twice");
    let mut sorted = seen.clone();
    sorted.sort();
    assert_eq!(seen, sorted, "pages arrive in the cursor's own order");
    Ok(())
}
