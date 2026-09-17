//! Live streams at the service level: what the bus carries for each kind of
//! change, when a shared view is reloaded and shared, and what the server-health
//! feed forwards.
//!
//! The bus here is the in-process one (`LivePublisher::InProcess`), so these
//! tests exercise everything except the Redis hop — `live_redis.rs` covers that.

#![allow(clippy::unwrap_used, clippy::panic, clippy::expect_used)]

mod common;

use chrono::{SubsecRound, TimeDelta, Utc};
use common::*;
use kanau::processor::Processor;
use orchestration::entities::db::agent_release::PublishAgentRelease;
use orchestration::entities::db::canvas::{CanvasEntity, CanvasId};
use orchestration::entities::db::certificate::TouchCanvases;
use orchestration::entities::db::exit::ExitEntity;
use orchestration::entities::db::health::{
    InsertPodHealthRecords, NewPodHealthRecord, PodHealthStatus, ServerHealthStatus,
};
use orchestration::entities::db::pod::PodId;
use orchestration::entities::db::server::{
    FindServerById, ServerId, ServerIpv6Resolve, ServerLogLevel,
};
use orchestration::entities::db::view::TakeInFlight;
use orchestration::events::live::{CanvasChangeKind, LiveMessage};
use orchestration::hooks::live::LiveEvent;
use orchestration::services::agent::{
    AckConfig, AgentIdentity, PodResult, PollAgentUpdate, RegisterCredential, RegisterWorker,
};
use orchestration::services::canvas as canvas_service;
use orchestration::services::graph::GraphChange;
use orchestration::services::health::{
    DEFAULT_POD_HISTORY_LIMIT, HealthReportInput, RecordHealthReport, SweepLiveness,
};
use orchestration::services::live::{
    GraphLive, RolloutsLive, ViewHandle, ViewValue, WatchGraph, WatchPodHealth, WatchRollouts,
    WatchServerHealth,
};
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

async fn canvas_named(
    w: &World,
    name: &str,
    parent: Option<&CanvasId>,
) -> Result<CanvasEntity, Box<dyn std::error::Error>> {
    Ok(w.canvases
        .process(canvas_service::CreateCanvas {
            actor: operator(),
            name: name.to_string(),
            description: String::new(),
            parent: parent.cloned(),
            position: pos0(),
        })
        .await?)
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
            log_level: ServerLogLevel::Info,
            addresses: AddressOverrides {
                override_v4: Some(address.to_string()),
                override_v6: None,
                extra_addresses: Vec::new(),
            },
        })
        .await?
        .id)
}

/// A canvas with one server and one pod exiting to `web-out`, ready to derive.
async fn wired(
    w: &World,
) -> Result<(CanvasEntity, ServerId, ExitEntity), Box<dyn std::error::Error>> {
    let canvas = canvas_named(w, "prod", None).await?;
    let server = make_server(w, &canvas.id, "tokyo", "203.0.113.10").await?;
    let row =
        w.db.process(FindServerById { id: server.clone() })
            .await?
            .unwrap();
    let web = client(&canvas, &row, "web", 443, None);
    let out = exit(&canvas, "web-out", "10.0.0.5:8080");
    let edge = edge_to_exit("web-edge", &web, &out);
    w.apply(
        &canvas,
        GraphChange {
            put_pods: vec![routed(web, via(&edge))],
            put_exits: vec![out.clone()],
            put_edges: vec![edge],
            ..GraphChange::default()
        },
    )
    .await?;
    Ok((canvas, server, out))
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
            capabilities: Vec::new(),
            last_update_error: None,
        })
        .await?;
    let row =
        w.db.process(FindServerById { id: server.clone() })
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
        w.db.process(FindServerById {
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

/// The pod named `name` applied cleanly; its forwarding is tagged with its id.
fn pod_ok(name: &str) -> PodResult {
    PodResult {
        tag: key(name),
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

fn changed(kind: CanvasChangeKind) -> impl FnMut(&LiveMessage) -> bool {
    move |m| matches!(m, LiveMessage::CanvasChanged { kind: k, .. } if *k == kind)
}

// --- what the bus carries ----------------------------------------------------

/// Every edit announces itself with what it changed, the metadata-only ones
/// (which do not bump the generation) included.
#[sqlx::test(migrator = "base::db::MIGRATOR")]
async fn edits_are_announced_with_their_kind(pool: sqlx::PgPool) -> TestResult {
    let w = world(pool).await?;
    let mut events = w.bus.subscribe();
    let canvas = canvas_named(&w, "prod", None).await?;

    let tokyo = make_server(&w, &canvas.id, "tokyo", "203.0.113.10").await?;
    let message = next_message(&mut events, changed(CanvasChangeKind::ServerCreated)).await;
    let LiveMessage::CanvasChanged {
        canvas: event_canvas,
        ids,
        ..
    } = &*message
    else {
        panic!("filtered above");
    };
    assert_eq!(*event_canvas, canvas.id.to_string());
    assert_eq!(ids, &vec![tokyo.to_string()]);

    w.servers
        .process(MoveServer {
            actor: operator(),
            server: tokyo.clone(),
            position: pos(120, 340),
        })
        .await?;
    next_message(&mut events, changed(CanvasChangeKind::ServerMoved)).await;

    let row =
        w.db.process(FindServerById { id: tokyo.clone() })
            .await?
            .unwrap();
    w.apply(
        &canvas,
        GraphChange {
            put_pods: vec![client(&canvas, &row, "web", 443, None)],
            ..GraphChange::default()
        },
    )
    .await?;
    next_message(&mut events, changed(CanvasChangeKind::GraphChanged)).await;

    let sub = canvas_named(&w, "sub", Some(&canvas.id)).await?;
    let message = next_message(&mut events, changed(CanvasChangeKind::CanvasCreated)).await;
    let LiveMessage::CanvasChanged {
        canvas: event_canvas,
        ids,
        ..
    } = &*message
    else {
        panic!("filtered above");
    };
    assert_eq!(
        *event_canvas,
        canvas.id.to_string(),
        "a subcanvas is announced on its parent"
    );
    assert_eq!(ids, &vec![sub.id.to_string()]);
    Ok(())
}

/// The agent's install and update state lives on the server row but feeds
/// nothing derived, so only the live event can tell the panel that a key was
/// issued, an update was requested, or the worker reported why it failed.
#[sqlx::test(migrator = "base::db::MIGRATOR")]
async fn agent_state_changes_are_announced(pool: sqlx::PgPool) -> TestResult {
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
    let canvas = canvas_named(&w, "prod", None).await?;
    let server = make_server(&w, &canvas.id, "tokyo", "203.0.113.10").await?;
    w.agents
        .process(RegisterWorker {
            credential: RegisterCredential::Operator(machine()),
            server_id: server.clone(),
            running_revision: 0,
            observed: None,
            reported: None,
            agent_version: Some("0.1.0".to_string()),
            agent_arch: Some("x86_64".to_string()),
            capabilities: vec!["route_table".to_string(), "bogus".to_string()],
            last_update_error: None,
        })
        .await?;
    let mut events = w.bus.subscribe();
    let server_updated = |m: &LiveMessage| {
        matches!(
            m,
            LiveMessage::CanvasChanged { kind: CanvasChangeKind::ServerUpdated, ids, .. }
                if ids == &vec![server.to_string()]
        )
    };

    w.servers
        .process(IssueServerAgentInstall {
            actor: operator(),
            server: server.clone(),
            unit: Some("tokyo-1".to_string()),
        })
        .await?;
    next_message(&mut events, server_updated).await;
    let issued =
        w.db.process(FindServerById { id: server.clone() })
            .await?
            .unwrap();
    assert_eq!(issued.agent_unit.as_deref(), Some("tokyo-1"));
    assert!(issued.agent_key_issued_at.is_some());

    w.servers
        .process(RequestAgentUpdate {
            actor: operator(),
            server: server.clone(),
        })
        .await?;
    next_message(&mut events, server_updated).await;

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
    next_message(&mut events, server_updated).await;
    let failed =
        w.db.process(FindServerById { id: server.clone() })
            .await?
            .unwrap();
    assert_eq!(failed.agent_update_requested, None);
    assert_eq!(
        failed.agent_update_error.as_deref(),
        Some("checksum mismatch")
    );
    assert_eq!(
        failed.capabilities,
        vec!["route_table".to_string()],
        "only the capabilities the master compiles for are kept"
    );
    Ok(())
}

// --- the rollouts view -------------------------------------------------------

/// Two watchers of one tree share a single view: the same `Arc` reaches both,
/// and the registry drops the key once neither holds a handle.
#[sqlx::test(migrator = "base::db::MIGRATOR")]
async fn views_are_shared_and_dropped_at_zero(pool: sqlx::PgPool) -> TestResult {
    let w = world(pool).await?;
    let canvas = canvas_named(&w, "prod", None).await?;

    let mut first = w
        .live
        .process(WatchRollouts {
            actor: operator(),
            canvas: canvas.id.clone(),
        })
        .await?;
    let mut second = w
        .live
        .process(WatchRollouts {
            actor: operator(),
            canvas: canvas.id.clone(),
        })
        .await?;
    assert_eq!(w.live.rollouts.active(), 1, "one view, two subscribers");

    let a = next_value(&mut first).await;
    let b = next_value(&mut second).await;
    let (state_a, _) = ready(&a);
    let (state_b, _) = ready(&b);
    assert!(
        Arc::ptr_eq(state_a, state_b),
        "both watchers see the very same snapshot"
    );

    drop(first);
    assert_eq!(w.live.rollouts.active(), 1, "one subscriber is left");
    drop(second);
    assert_eq!(w.live.rollouts.active(), 0, "the view is gone");
    Ok(())
}

/// A deleted canvas becomes `Missing`, which is what turns the stream into a
/// `NOT_FOUND` instead of a silent hang.
#[sqlx::test(migrator = "base::db::MIGRATOR")]
async fn deleted_canvas_ends_with_missing(pool: sqlx::PgPool) -> TestResult {
    let w = world(pool).await?;
    let canvas = canvas_named(&w, "doomed", None).await?;
    let mut handle = w
        .live
        .process(WatchRollouts {
            actor: operator(),
            canvas: canvas.id.clone(),
        })
        .await?;
    ready(&next_value(&mut handle).await);

    w.canvases
        .process(canvas_service::DeleteCanvas {
            actor: operator(),
            canvas: canvas.id.clone(),
        })
        .await?;
    assert!(
        matches!(next_value(&mut handle).await, ViewValue::Missing),
        "the view reports the canvas is gone"
    );
    Ok(())
}

/// A server created on a subcanvas joins the rollouts of the whole tree.
#[sqlx::test(migrator = "base::db::MIGRATOR")]
async fn a_subcanvas_edit_refreshes_the_trees_rollouts(pool: sqlx::PgPool) -> TestResult {
    let w = world(pool).await?;
    let root = canvas_named(&w, "root", None).await?;
    let sub = canvas_named(&w, "sub", Some(&root.id)).await?;
    let mut handle = w
        .live
        .process(WatchRollouts {
            actor: operator(),
            canvas: root.id.clone(),
        })
        .await?;
    let opening = next_value(&mut handle).await;
    let (state, _) = ready(&opening);
    assert!(state.servers.is_empty());

    make_server(&w, &sub.id, "osaka", "198.51.100.10").await?;
    let refreshed = loop {
        let value = next_value(&mut handle).await;
        let (state, _) = ready(&value);
        if !state.servers.is_empty() {
            break value;
        }
    };
    let (state, _) = ready(&refreshed);
    assert_eq!(state.servers[0].server.name, "osaka");
    Ok(())
}

/// The rollout view follows both halves of a rollout: the derivation that
/// publishes a revision, and the ack that records it as applied.
#[sqlx::test(migrator = "base::db::MIGRATOR")]
async fn rollouts_follow_derive_and_ack(pool: sqlx::PgPool) -> TestResult {
    let w = world(pool).await?;
    let (canvas, server, _) = wired(&w).await?;

    let mut handle: ViewHandle<RolloutsLive> = w
        .live
        .process(WatchRollouts {
            actor: operator(),
            canvas: canvas.id.clone(),
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

    w.derive(&canvas.id).await?;
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
    w.derive(&canvas.id).await?;
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

/// A view answers a resync by reloading: whatever was written while the bus was
/// down still reaches the watcher.
#[sqlx::test(migrator = "base::db::MIGRATOR")]
async fn resync_reloads_a_view_that_missed_its_events(pool: sqlx::PgPool) -> TestResult {
    let w = world(pool).await?;
    let canvas = canvas_named(&w, "prod", None).await?;
    let mut handle: ViewHandle<RolloutsLive> = w
        .live
        .process(WatchRollouts {
            actor: operator(),
            canvas: canvas.id.clone(),
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
    make_server(&deaf, &canvas.id, "tokyo", "203.0.113.10").await?;
    make_server(&deaf, &canvas.id, "osaka", "203.0.113.11").await?;

    deaf.bus.resync();
    let refreshed = loop {
        let value = next_value(&mut handle).await;
        let (state, cause) = ready(&value);
        assert!(cause.is_none(), "a resync reload names no cause");
        if state.servers.len() == 2 {
            break value;
        }
    };
    ready(&refreshed);
    Ok(())
}

// --- the graph view ----------------------------------------------------------

/// The graph view follows edits and health *status* flips, and ignores the
/// routine report that changes nothing it shows: a fleet reporting every
/// interval must not reload every dashboard every interval.
#[sqlx::test(migrator = "base::db::MIGRATOR")]
async fn graph_view_reloads_on_a_status_flip_not_a_report(pool: sqlx::PgPool) -> TestResult {
    let w = world(pool).await?;
    let (canvas, server, _) = wired(&w).await?;
    w.derive(&canvas.id).await?;
    let agent = register(&w, &server).await?;

    let mut handle = w
        .live
        .process(WatchGraph {
            actor: operator(),
            canvas: canvas.id.clone(),
        })
        .await?;
    assert_eq!(w.live.graph.active(), 1, "one view while the handle lives");
    let status_of = |value: &ViewValue<GraphLive>| {
        let (state, _) = ready(value);
        let servers = &state.view.rows.servers;
        assert_eq!(servers.len(), 1, "the tree has one server");
        servers[0].health_status
    };
    assert_eq!(
        status_of(&next_value(&mut handle).await),
        ServerHealthStatus::Offline,
        "a server that never reported opens offline"
    );

    // The first report flips the server online; it may also announce the
    // reported address as a canvas change, so wait until the flip is visible.
    w.health
        .process(RecordHealthReport {
            agent: agent.clone(),
            report: report(0),
        })
        .await?;
    tokio::time::timeout(WAIT, async {
        loop {
            if status_of(&next_value(&mut handle).await) == ServerHealthStatus::Online {
                break;
            }
        }
    })
    .await
    .expect("the view sees the server come online");

    // The same report again changes nothing the graph shows: no reload.
    w.health
        .process(RecordHealthReport {
            agent: agent.clone(),
            report: report(0),
        })
        .await?;
    assert!(
        tokio::time::timeout(Duration::from_millis(500), handle.rx.changed())
            .await
            .is_err(),
        "a routine report does not reload the graph"
    );

    // The master's own flip does.
    w.health
        .process(SweepLiveness {
            now: Utc::now() + TimeDelta::days(1),
        })
        .await?;
    assert_eq!(
        status_of(&next_value(&mut handle).await),
        ServerHealthStatus::Offline,
        "the sweep's flip reaches the view"
    );

    drop(handle);
    assert_eq!(w.live.graph.active(), 0, "the view is gone");
    Ok(())
}

fn generation_of(value: &ViewValue<GraphLive>) -> i64 {
    ready(value).0.view.rows.generation()
}

/// Waits until a graph handle shows `generation`.
async fn graph_reaches(handle: &mut ViewHandle<GraphLive>, generation: i64) {
    tokio::time::timeout(WAIT, async {
        loop {
            if generation_of(&next_value(handle).await) == generation {
                return;
            }
        }
    })
    .await
    .expect("the graph view reached the generation");
}

/// Some writes move the tree's generation without announcing a canvas change
/// (`TouchCanvases` after an ACME issuance or a relay-leaf rotation,
/// `ForgetServerApplied`). The derivation that follows them is what reloads
/// the graph view; a view left on the old generation fences every edit off as
/// stale.
#[sqlx::test(migrator = "base::db::MIGRATOR")]
async fn graph_view_follows_a_bump_through_its_derivation(pool: sqlx::PgPool) -> TestResult {
    let w = world(pool).await?;
    let (canvas, _, _) = wired(&w).await?;
    w.derive(&canvas.id).await?;

    let mut handle = w
        .live
        .process(WatchGraph {
            actor: operator(),
            canvas: canvas.id.clone(),
        })
        .await?;
    let before = generation_of(&next_value(&mut handle).await);

    w.db.process(TouchCanvases {
        canvases: vec![canvas.id.clone()],
    })
    .await?;
    w.derive(&canvas.id).await?;
    graph_reaches(&mut handle, before + 1).await;
    Ok(())
}

/// A watcher joining a view that is already loaded triggers a re-read, so a
/// write the view does not match (here a bare generation bump) cannot stay
/// invisible to every later open, or to a client's reconnect.
#[sqlx::test(migrator = "base::db::MIGRATOR")]
async fn joining_a_loaded_view_rereads_it(pool: sqlx::PgPool) -> TestResult {
    let w = world(pool).await?;
    let (canvas, _, _) = wired(&w).await?;
    let watch = || {
        w.live.process(WatchGraph {
            actor: operator(),
            canvas: canvas.id.clone(),
        })
    };

    let mut first = watch().await?;
    let before = generation_of(&next_value(&mut first).await);
    w.db.process(TouchCanvases {
        canvases: vec![canvas.id.clone()],
    })
    .await?;
    assert!(
        tokio::time::timeout(Duration::from_millis(300), first.rx.changed())
            .await
            .is_err(),
        "nothing the view matches was published"
    );

    let mut second = watch().await?;
    assert_eq!(w.live.graph.active(), 1, "the joiner shares the view");
    graph_reaches(&mut second, before + 1).await;
    graph_reaches(&mut first, before + 1).await;
    Ok(())
}

// --- the health feeds --------------------------------------------------------

/// A server-health watch opens with the history it was asked for, then sees one
/// event per accepted report — including the master's own offline flip.
#[sqlx::test(migrator = "base::db::MIGRATOR")]
async fn server_health_stream_dedupes_and_sees_offline(pool: sqlx::PgPool) -> TestResult {
    let w = world(pool).await?;
    let (canvas, server, _) = wired(&w).await?;
    w.derive(&canvas.id).await?;
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
    let swept = w
        .health
        .process(SweepLiveness {
            now: Utc::now() + TimeDelta::days(1),
        })
        .await?;
    assert!(swept.flipped.contains(&server));
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
    assert_eq!(*event_canvas, canvas.id.to_string());
    Ok(())
}

/// A pod goes `Deploying` when a revision is published and `Ready` when the
/// worker acks it; a revision refused as a whole fails it with the worker's
/// message.
#[sqlx::test(migrator = "base::db::MIGRATOR")]
async fn pod_health_is_deploying_then_ready_then_failed(pool: sqlx::PgPool) -> TestResult {
    let w = world(pool).await?;
    let (canvas, server, out) = wired(&w).await?;
    let mut events = w.bus.subscribe();
    let web = key("web");
    let of_web = |status: PodHealthStatus| {
        let web = web.clone();
        move |m: &LiveMessage| match m {
            LiveMessage::PodHealth { records } => {
                records.iter().any(|r| r.pod == web && r.status == status)
            }
            _ => false,
        }
    };

    w.derive(&canvas.id).await?;
    next_message(&mut events, of_web(PodHealthStatus::Deploying)).await;

    let agent = register(&w, &server).await?;
    w.derive(&canvas.id).await?;
    take_and_ack(&w, &agent, vec![pod_ok("web")], None).await?;
    next_message(&mut events, of_web(PodHealthStatus::Ready)).await;

    // A pod watch opened now carries the history so far, oldest first.
    let watch = w
        .live
        .process(WatchPodHealth {
            actor: operator(),
            pod: PodId::from_key(key("web")),
            since: Utc::now() - TimeDelta::hours(1),
        })
        .await?;
    let statuses: Vec<_> = watch.records.iter().map(|r| r.status).collect();
    assert_eq!(
        statuses,
        vec![PodHealthStatus::Deploying, PodHealthStatus::Ready],
        "the snapshot lists the pod's events in order"
    );
    assert!(
        watch
            .records
            .windows(2)
            .all(|w| w[0].report_time <= w[1].report_time),
        "oldest first"
    );

    // Moving the exit is what makes a *new* revision for the worker to refuse.
    w.apply(
        &canvas,
        GraphChange {
            put_exits: vec![ExitEntity {
                destination: "10.0.0.9:9999".to_string(),
                ..out
            }],
            ..GraphChange::default()
        },
    )
    .await?;
    w.derive(&canvas.id).await?;
    take_and_ack(&w, &agent, vec![], Some("boom".to_string())).await?;
    let message = next_message(&mut events, of_web(PodHealthStatus::Failed)).await;
    let LiveMessage::PodHealth { records } = &*message else {
        panic!("filtered above");
    };
    assert_eq!(
        records
            .iter()
            .find(|r| r.pod == web && r.status == PodHealthStatus::Failed)
            .unwrap()
            .message,
        "boom"
    );
    Ok(())
}

/// Every report writes a row per pod, so a pod watch opens with the newest page
/// of its window only, oldest first, rather than the whole per-interval series.
#[sqlx::test(migrator = "base::db::MIGRATOR")]
async fn a_pod_watch_opens_with_the_newest_page_only(pool: sqlx::PgPool) -> TestResult {
    let w = world(pool).await?;
    wired(&w).await?;
    let pod = pod_id("web");
    // PostgreSQL keeps microseconds.
    let now = Utc::now().trunc_subsecs(6);
    let total = DEFAULT_POD_HISTORY_LIMIT + 10;
    w.db.process(InsertPodHealthRecords {
        records: (0..total)
            .map(|i| NewPodHealthRecord {
                pod: pod.clone(),
                status: PodHealthStatus::Ready,
                message: String::new(),
                report_time: now - TimeDelta::seconds(total - i),
            })
            .collect(),
    })
    .await?;

    let watch = w
        .live
        .process(WatchPodHealth {
            actor: operator(),
            pod,
            since: now - TimeDelta::hours(1),
        })
        .await?;
    let times: Vec<_> = watch.records.iter().map(|r| r.report_time).collect();
    assert_eq!(times.len(), DEFAULT_POD_HISTORY_LIMIT as usize);
    assert!(
        times.windows(2).all(|pair| pair[0] < pair[1]),
        "oldest first"
    );
    assert_eq!(
        times.last(),
        Some(&(now - TimeDelta::seconds(1))),
        "the newest row is in"
    );
    assert_eq!(
        times.first(),
        Some(&(now - TimeDelta::seconds(DEFAULT_POD_HISTORY_LIMIT))),
        "the oldest rows are the ones left out"
    );
    Ok(())
}
