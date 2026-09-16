//! Entity-layer queries against an in-memory SurrealDB with the real schema.

#![allow(clippy::unwrap_used, clippy::panic, clippy::expect_used)]

mod common;

use base::db::Db;
use common::*;
use kanau::processor::Processor;
use orchestration::entities::db::agent_release::{FindAgentRelease, PublishAgentRelease};
use orchestration::entities::db::canvas::{
    DeleteCanvasRow, FindCanvasById, ListCanvases, LoadCanvasTree, UpdateCanvasMeta,
};
use orchestration::entities::db::graph::{ApplyGraphBatch, LoadCanvasGraph};
use orchestration::entities::db::group::{GroupEntity, GroupId, GroupMember};
use orchestration::entities::db::pod::{PodIngress, ProxyProtocolVersion, TlsConfig};
use orchestration::entities::db::server::{
    ClaimServerWatchSession, DeleteServerRow, FindCanvasOfServer, FindServerById,
    FindServerByRefreshKeyDigest, ListServersByCanvas, MoveServerPosition, QuicCongestion,
    RegisterWorkerSession, ReleaseServerWatchSession, RenewServerWatchSession,
    ServerIpv6Resolve, ServerLogLevel, ServerQuic, UpdateServerSettings,
};
use orchestration::entities::db::view::{
    AckServerConfig, FindServerConfigView, ListServerWatchState, TakeInFlight,
};
use orchestration::services::OrchestrationError;

/// A batch that inserts the given pods, exits and edges into `canvas`'s tree.
fn inserting(
    canvas: &orchestration::entities::db::canvas::CanvasEntity,
    pods: Vec<orchestration::entities::db::pod::PodEntity>,
    exits: Vec<orchestration::entities::db::exit::ExitEntity>,
    edges: Vec<orchestration::entities::db::edge::EdgeEntity>,
) -> ApplyGraphBatch {
    ApplyGraphBatch {
        canvas: Some(canvas.id.clone()),
        derives: true,
        insert_pods: pods,
        insert_exits: exits,
        insert_edges: edges,
        ..ApplyGraphBatch::default()
    }
}

#[sqlx::test(migrator = "base::db::MIGRATOR")]
async fn canvas_crud_round_trip(pool: sqlx::PgPool) -> TestResult {
    let sp = setup(pool);
    let c = canvas(&sp, "prod").await?;
    assert_eq!(
        sp.process(ListCanvases {
            include_subcanvases: false
        })
        .await?
        .len(),
        1
    );

    let updated = sp
        .process(UpdateCanvasMeta {
            id: c.id.clone(),
            name: "prod2".to_string(),
            description: "desc".to_string(),
            position: None,
        })
        .await?;
    assert_eq!(updated.name, "prod2");
    assert_eq!(
        sp.process(FindCanvasById { id: c.id.clone() })
            .await?
            .unwrap()
            .description,
        "desc"
    );

    let s = server(&sp, &c, "tokyo").await?;
    sp.process(DeleteCanvasRow { id: c.id.clone() }).await?;
    assert!(sp.process(FindCanvasById { id: c.id }).await?.is_none());
    assert!(
        sp.process(FindServerConfigView {
            server: s.id.clone()
        })
        .await?
        .is_none(),
        "the cascade takes the config view with the server"
    );
    assert!(sp.process(FindServerById { id: s.id }).await?.is_none());
    Ok(())
}

#[sqlx::test(migrator = "base::db::MIGRATOR")]
async fn creating_a_server_creates_its_empty_config_view(pool: sqlx::PgPool) -> TestResult {
    let sp = setup(pool);
    let c = canvas(&sp, "prod").await?;
    let s = server(&sp, &c, "tokyo").await?;
    assert_eq!(s.refresh_key_generation, 0);
    assert!(s.last_seen_at.is_none());

    let view = sp
        .process(FindServerConfigView {
            server: s.id.clone(),
        })
        .await?
        .expect("a server is never observable without its config view");
    assert!(view.desired.is_none());
    assert!(view.in_flight.is_none());
    assert!(view.applied.is_none());
    assert!(view.failed_revision.is_none());
    assert!(view.waiting_for.is_empty());

    let updated = sp
        .process(UpdateServerSettings {
            id: s.id.clone(),
            canvas: c.id.clone(),
            name: "tokyo-1".to_string(),
            icon: "jp".to_string(),
            comment: "primary".to_string(),
            ipv6_resolve: ServerIpv6Resolve::Preferred,
            log_level: ServerLogLevel::Debug,
            quic: ServerQuic {
                congestion: QuicCongestion::Brutal,
                up_mbps: 1000,
                down_mbps: 1000,
                stream_receive_window: 67_108_864,
                conn_receive_window: 268_435_456,
            },
            override_v4: None,
            override_v6: None,
            extra_addresses: Vec::new(),
            agent_unit: None,
            fence: None,
        })
        .await?;
    assert_eq!(updated.ipv6_resolve, ServerIpv6Resolve::Preferred);
    assert_eq!(updated.log_level, ServerLogLevel::Debug);
    assert_eq!(updated.quic.congestion, QuicCongestion::Brutal);
    assert_eq!(updated.quic.up_mbps, 1000);
    assert_eq!(updated.quic.conn_receive_window, 268_435_456);

    sp.process(MoveServerPosition {
        id: s.id.clone(),
        position: pos(7, 9),
    })
    .await?;
    assert_eq!(
        sp.process(FindServerById { id: s.id.clone() })
            .await?
            .unwrap()
            .position,
        pos(7, 9)
    );
    assert_eq!(
        sp.process(ListServersByCanvas {
            canvas: c.id.clone()
        })
        .await?
        .len(),
        1
    );
    assert_eq!(
        sp.process(FindCanvasOfServer {
            server: s.id.clone()
        })
        .await?
        .unwrap()
        .0,
        c.id.0
    );
    Ok(())
}

#[sqlx::test(migrator = "base::db::MIGRATOR")]
async fn a_worker_session_is_owned_by_one_registration_at_a_time(pool: sqlx::PgPool) -> TestResult {
    let sp = setup(pool);
    let c = canvas(&sp, "prod").await?;
    let s = server(&sp, &c, "tokyo").await?;

    let start = chrono::Utc::now();
    let lease = chrono::TimeDelta::seconds(30);
    let register = |digest: &str, now: chrono::DateTime<chrono::Utc>| RegisterWorkerSession {
        server: s.id.clone(),
        digest: digest.to_string(),
        now,
        lease_until: now + lease,
        running_revision: 0,
        observed: None,
        reported: None,
        agent_version: None,
        agent_arch: None,
        capabilities: Vec::new(),
    };

    let row = sp
        .process(register("digest-1", start))
        .await?
        .expect("the first registration takes the free session");
    assert_eq!(row.refresh_key_generation, 1);
    let found = sp
        .process(FindServerByRefreshKeyDigest {
            digest: "digest-1".to_string(),
        })
        .await?
        .unwrap();
    assert_eq!(found.id.0, s.id.0);
    assert!(found.last_seen_at.is_some());

    // A contender is refused while the session lease is still alive.
    assert!(
        sp.process(register(
            "digest-contender",
            start + chrono::TimeDelta::seconds(5)
        ))
        .await?
        .is_none(),
        "a live session must not be stolen by another registration"
    );
    assert!(
        sp.process(FindServerByRefreshKeyDigest {
            digest: "digest-1".to_string()
        })
        .await?
        .is_some(),
        "the refused registration left the incumbent's key in place"
    );

    // A heartbeat keeps ownership; a stale epoch cannot renew.
    assert!(
        sp.process(RenewServerWatchSession {
            server: s.id.clone(),
            generation: 1,
            epoch: 0,
            now: start + chrono::TimeDelta::seconds(10),
            lease_until: start + chrono::TimeDelta::seconds(40),
        })
        .await?
    );
    assert!(
        !sp.process(RenewServerWatchSession {
            server: s.id.clone(),
            generation: 1,
            epoch: 7,
            now: start + chrono::TimeDelta::seconds(10),
            lease_until: start + chrono::TimeDelta::seconds(40),
        })
        .await?,
        "a fenced-out session must not be able to hold the lease"
    );

    // Once the lease lapses the server can be taken over.
    let row = sp
        .process(register("digest-2", start + chrono::TimeDelta::seconds(41)))
        .await?
        .expect("a lapsed lease releases the server");
    assert_eq!(row.refresh_key_generation, 2);
    assert!(
        sp.process(FindServerByRefreshKeyDigest {
            digest: "digest-1".to_string()
        })
        .await?
        .is_none(),
        "the superseded digest must no longer resolve"
    );

    // Claiming a stream fences the previous one and releasing frees the server.
    let claimed = sp
        .process(ClaimServerWatchSession {
            server: s.id.clone(),
            generation: 2,
            now: start + chrono::TimeDelta::seconds(42),
            lease_until: start + chrono::TimeDelta::seconds(72),
        })
        .await?
        .expect("the current generation may claim the stream");
    assert_eq!(claimed.watch_epoch, 1);
    assert!(
        sp.process(ClaimServerWatchSession {
            server: s.id.clone(),
            generation: 1,
            now: start + chrono::TimeDelta::seconds(42),
            lease_until: start + chrono::TimeDelta::seconds(72),
        })
        .await?
        .is_none(),
        "a superseded generation must not claim a stream"
    );
    sp.process(ReleaseServerWatchSession {
        server: s.id.clone(),
        generation: 2,
        epoch: 1,
    })
    .await?;
    let row = sp
        .process(register("digest-3", start + chrono::TimeDelta::seconds(43)))
        .await?
        .expect("a released session lets the next worker register at once");
    assert_eq!(row.refresh_key_generation, 3);

    let watch = sp
        .process(ListServerWatchState {
            servers: vec![s.id.clone()],
        })
        .await?;
    assert_eq!(watch.len(), 1);
    assert_eq!(watch[0].refresh_key_generation, 3);
    assert_eq!(watch[0].watch_epoch, 1);
    assert_eq!(watch[0].desired_revision, None);
    Ok(())
}

/// Seeds a `desired` snapshot the way a derivation pass would.
async fn seed_desired(
    sp: &Db,
    server: &orchestration::entities::db::server::ServerId,
    revision: i64,
) -> Result<(), base::db::Error> {
    let snapshot = orchestration::entities::db::view::ConfigSnapshot {
        revision,
        toml: format!("# revision {revision}"),
        created_at: chrono::Utc::now(),
        forwardings: Vec::new(),
        certificates: Vec::new(),
    };
    sqlx::query("UPDATE orchestration_server_config_view SET desired = $2 WHERE server = $1")
        .bind(server)
        .bind(sqlx::types::Json(&snapshot))
        .execute(sp.db())
        .await?;
    Ok(())
}

#[sqlx::test(migrator = "base::db::MIGRATOR")]
async fn take_in_flight_and_ack_move_the_slots(pool: sqlx::PgPool) -> TestResult {
    let sp = setup(pool);
    let c = canvas(&sp, "prod").await?;
    let s = server(&sp, &c, "tokyo").await?;
    seed_desired(&sp, &s.id, 1).await?;

    let taken = sp
        .process(TakeInFlight {
            server: s.id.clone(),
            generation: 0,
            epoch: 0,
        })
        .await?
        .expect("a fresh desired revision is handed to the stream that asks");
    assert_eq!(taken.revision, 1);
    assert!(
        sp.process(TakeInFlight {
            server: s.id.clone(),
            generation: 0,
            epoch: 0,
        })
        .await?
        .is_none(),
        "a revision already in flight is never handed out twice"
    );
    assert!(
        sp.process(TakeInFlight {
            server: s.id.clone(),
            generation: 9,
            epoch: 0,
        })
        .await?
        .is_none(),
        "a fenced-out session is handed nothing at all"
    );

    assert!(
        sp.process(AckServerConfig {
            server: s.id.clone(),
            revision: 1,
            error: None,
            applied: None,
            failed_pods: Vec::new(),
        })
        .await?
    );
    let view = sp
        .process(FindServerConfigView {
            server: s.id.clone(),
        })
        .await?
        .unwrap();
    assert_eq!(view.applied.map(|s| s.revision), Some(1));
    assert!(view.in_flight.is_none());

    // A failed apply blocks the revision instead of being retried forever.
    seed_desired(&sp, &s.id, 2).await?;
    let taken = sp
        .process(TakeInFlight {
            server: s.id.clone(),
            generation: 0,
            epoch: 0,
        })
        .await?
        .expect("the next revision is offered once");
    assert_eq!(taken.revision, 2);
    assert!(
        sp.process(AckServerConfig {
            server: s.id.clone(),
            revision: 2,
            error: Some("cannot bind".to_string()),
            applied: None,
            failed_pods: Vec::new(),
        })
        .await?
    );
    let view = sp
        .process(FindServerConfigView {
            server: s.id.clone(),
        })
        .await?
        .unwrap();
    assert_eq!(view.failed_revision, Some(2));
    assert_eq!(view.apply_error.as_deref(), Some("cannot bind"));
    assert_eq!(view.applied.map(|s| s.revision), Some(1));
    assert!(
        sp.process(TakeInFlight {
            server: s.id.clone(),
            generation: 0,
            epoch: 0,
        })
        .await?
        .is_none(),
        "a revision the worker refused is not pushed at it again"
    );

    assert!(
        !sp.process(AckServerConfig {
            server: s.id.clone(),
            revision: 7,
            error: None,
            applied: None,
            failed_pods: Vec::new(),
        })
        .await?,
        "an ack for a revision that is not in flight changes nothing"
    );
    let view = sp
        .process(FindServerConfigView {
            server: s.id.clone(),
        })
        .await?
        .unwrap();
    assert_eq!(view.failed_revision, Some(2), "and leaves the slots alone");
    assert_eq!(view.applied.map(|s| s.revision), Some(1));
    Ok(())
}

#[sqlx::test(migrator = "base::db::MIGRATOR")]
async fn a_new_stream_is_offered_what_the_previous_one_never_acked(
    pool: sqlx::PgPool,
) -> TestResult {
    let sp = setup(pool);
    let c = canvas(&sp, "prod").await?;
    let s = server(&sp, &c, "tokyo").await?;
    seed_desired(&sp, &s.id, 1).await?;

    // The first stream is handed the snapshot and then dies without acking.
    let claimed = sp
        .process(ClaimServerWatchSession {
            server: s.id.clone(),
            generation: 0,
            now: chrono::Utc::now(),
            lease_until: chrono::Utc::now() + chrono::TimeDelta::seconds(30),
        })
        .await?
        .unwrap();
    let taken = sp
        .process(TakeInFlight {
            server: s.id.clone(),
            generation: 0,
            epoch: claimed.watch_epoch,
        })
        .await?
        .expect("the first stream is handed the snapshot");
    assert_eq!(taken.revision, 1);

    // Its replacement must not be left waiting for an ack that can never come.
    let claimed = sp
        .process(ClaimServerWatchSession {
            server: s.id.clone(),
            generation: 0,
            now: chrono::Utc::now(),
            lease_until: chrono::Utc::now() + chrono::TimeDelta::seconds(30),
        })
        .await?
        .unwrap();
    let taken = sp
        .process(TakeInFlight {
            server: s.id.clone(),
            generation: 0,
            epoch: claimed.watch_epoch,
        })
        .await?
        .expect("the fenced-out session's snapshot is offered to the new one");
    assert_eq!(taken.revision, 1);
    Ok(())
}

#[sqlx::test(migrator = "base::db::MIGRATOR")]
async fn register_promotes_a_reported_desired_revision(pool: sqlx::PgPool) -> TestResult {
    let sp = setup(pool);
    let c = canvas(&sp, "prod").await?;
    let s = server(&sp, &c, "tokyo").await?;
    seed_desired(&sp, &s.id, 3).await?;
    sp.process(TakeInFlight {
        server: s.id.clone(),
        generation: 0,
        epoch: 0,
    })
    .await?
    .expect("the snapshot is in flight when the worker restarts");

    let now = chrono::Utc::now();
    sp.process(RegisterWorkerSession {
        server: s.id.clone(),
        digest: "digest-1".to_string(),
        now,
        lease_until: now + chrono::TimeDelta::seconds(30),
        running_revision: 3,
        observed: None,
        reported: None,
        agent_version: None,
        agent_arch: None,
        capabilities: Vec::new(),
    })
    .await?
    .expect("the free session is taken");

    let view = sp
        .process(FindServerConfigView {
            server: s.id.clone(),
        })
        .await?
        .unwrap();
    assert!(
        view.apply_error.is_none(),
        "a revision the master derived is accounted for: {:?}",
        view.apply_error
    );
    assert!(
        view.in_flight.is_none(),
        "registration clears whatever the previous session had in flight"
    );
    assert_eq!(
        view.applied.map(|s| s.revision),
        Some(3),
        "a worker that reports the desired revision is running it"
    );
    Ok(())
}

#[sqlx::test(migrator = "base::db::MIGRATOR")]
async fn register_promotes_a_reported_in_flight_revision(pool: sqlx::PgPool) -> TestResult {
    let sp = setup(pool);
    let c = canvas(&sp, "prod").await?;
    let s = server(&sp, &c, "tokyo").await?;
    seed_desired(&sp, &s.id, 1).await?;
    sp.process(TakeInFlight {
        server: s.id.clone(),
        generation: 0,
        epoch: 0,
    })
    .await?
    .expect("the snapshot is in flight when the worker restarts");
    // A derivation pass moves on while the worker is away, so what it reports on
    // its way back is the in-flight revision and no longer the desired one.
    seed_desired(&sp, &s.id, 2).await?;

    let now = chrono::Utc::now();
    sp.process(RegisterWorkerSession {
        server: s.id.clone(),
        digest: "digest-1".to_string(),
        now,
        lease_until: now + chrono::TimeDelta::seconds(30),
        running_revision: 1,
        observed: None,
        reported: None,
        agent_version: None,
        agent_arch: None,
        capabilities: Vec::new(),
    })
    .await?
    .expect("the free session is taken");

    let view = sp
        .process(FindServerConfigView {
            server: s.id.clone(),
        })
        .await?
        .unwrap();
    assert!(
        view.apply_error.is_none(),
        "the in-flight revision is one the master handed out: {:?}",
        view.apply_error
    );
    assert!(
        view.in_flight.is_none(),
        "registration clears whatever the previous session had in flight"
    );
    assert_eq!(
        view.applied.map(|s| s.revision),
        Some(1),
        "a worker that reports the in-flight revision is running it"
    );
    assert_eq!(
        view.desired.map(|s| s.revision),
        Some(2),
        "and the newer derivation is still there to converge to"
    );
    Ok(())
}

#[sqlx::test(migrator = "base::db::MIGRATOR")]
async fn register_rejects_an_unknown_running_revision(pool: sqlx::PgPool) -> TestResult {
    let sp = setup(pool);
    let c = canvas(&sp, "prod").await?;
    let s = server(&sp, &c, "tokyo").await?;
    seed_desired(&sp, &s.id, 3).await?;

    let now = chrono::Utc::now();
    sp.process(RegisterWorkerSession {
        server: s.id.clone(),
        digest: "digest-1".to_string(),
        now,
        lease_until: now + chrono::TimeDelta::seconds(30),
        running_revision: 7,
        observed: None,
        reported: None,
        agent_version: None,
        agent_arch: None,
        capabilities: Vec::new(),
    })
    .await?
    .expect("the free session is taken");

    let view = sp
        .process(FindServerConfigView {
            server: s.id.clone(),
        })
        .await?
        .unwrap();
    assert!(
        view.applied.is_none(),
        "the master never advances applied to a revision it cannot account for"
    );
    assert_eq!(
        view.apply_error.as_deref(),
        Some("running revision 7 unknown")
    );
    assert_eq!(
        view.desired.map(|s| s.revision),
        Some(3),
        "and leaves the derivation it does know about alone"
    );
    Ok(())
}

/// A server delete is refused while a pod is still placed on it, so derivation
/// can never end up with a pod whose server is gone. Called directly, as a
/// racing caller would reach it: the service pre-check runs in an earlier
/// transaction, so the invariant has to hold inside the delete itself.
#[sqlx::test(migrator = "base::db::MIGRATOR")]
async fn deleting_a_server_with_a_live_pod_is_refused(pool: sqlx::PgPool) -> TestResult {
    let sp = setup(pool);
    let c = canvas(&sp, "prod").await?;
    let s = server(&sp, &c, "tokyo").await?;
    let pod = client(&c, &s, "pod", 443, None);
    sp.process(inserting(&c, vec![pod.clone()], Vec::new(), Vec::new()))
        .await?;

    sp.process(DeleteServerRow {
        id: s.id.clone(),
        canvas: c.id.clone(),
        fence: None,
    })
    .await
    .expect_err("the server still has a live pod");
    assert!(
        sp.process(FindServerById { id: s.id.clone() })
            .await?
            .is_some(),
        "the refused delete left the server in place"
    );

    // Once the pod is gone the delete succeeds.
    sp.process(ApplyGraphBatch {
        canvas: Some(c.id.clone()),
        derives: true,
        delete_pods: vec![pod.id.clone()],
        ..ApplyGraphBatch::default()
    })
    .await?;
    sp.process(DeleteServerRow {
        id: s.id.clone(),
        canvas: c.id.clone(),
        fence: None,
    })
    .await?;
    assert!(sp.process(FindServerById { id: s.id }).await?.is_none());
    Ok(())
}

/// A pod's server is a foreign key: a batch naming a server no row carries
/// fails inside its transaction, and nothing of the batch is written.
#[sqlx::test(migrator = "base::db::MIGRATOR")]
async fn a_pod_cannot_reference_a_server_that_does_not_exist(pool: sqlx::PgPool) -> TestResult {
    let sp = setup(pool);
    let c = canvas(&sp, "prod").await?;
    let s = server(&sp, &c, "tokyo").await?;
    sp.process(inserting(
        &c,
        vec![client(&c, &s, "pod", 443, None)],
        Vec::new(),
        Vec::new(),
    ))
    .await?;

    let mut ghost = client(&c, &s, "ghost pod", 443, None);
    ghost.server = orchestration::utils::ids::server_id("ghost");
    let sound = client(&c, &s, "sound pod", 444, None);
    sp.process(inserting(&c, vec![sound, ghost], Vec::new(), Vec::new()))
        .await
        .expect_err("a dangling server link must not be storable");
    let graph = sp
        .process(LoadCanvasGraph {
            canvas: c.id.clone(),
        })
        .await?;
    let names: Vec<&str> = graph.pods.iter().map(|p| p.name.as_str()).collect();
    assert_eq!(
        names,
        ["pod"],
        "the rejected batch left no row behind, and the earlier pod is untouched"
    );
    Ok(())
}

/// Every column of the graph survives a round trip: a TLS client pod with its
/// route document, a relay pod, an exit, edges with and without overrides, and
/// a group whose members keep their order.
#[sqlx::test(migrator = "base::db::MIGRATOR")]
async fn a_graph_round_trips_through_its_rows(pool: sqlx::PgPool) -> TestResult {
    let sp = setup(pool);
    let c = canvas(&sp, "prod").await?;
    let tokyo = server(&sp, &c, "tokyo").await?;
    let osaka = server_at(&sp, &c, "osaka", "198.51.100.10").await?;
    let provider = orchestration::utils::ids::dns_provider_id("cf");
    sqlx::query(
        "INSERT INTO dns_provider (id, name, provider, account_id, api_secret, created_at)
         VALUES ($1, 'cf', 'cloudflare', '', 'enc1:secret', now())",
    )
    .bind(&provider)
    .execute(sp.db())
    .await?;

    let mut entry = pod(
        &c,
        &tokyo,
        "entry",
        443,
        PodIngress::ClientTls {
            receive_proxy_protocol: Some(ProxyProtocolVersion::V2),
            tls: TlsConfig {
                sni: "example.com".to_string(),
                dns_provider: provider.clone(),
                domain_id: "zone".to_string(),
                acme_directory: String::new(),
            },
        },
    );
    entry.bind_ip = Some("0.0.0.0".to_string());
    let mut hop = pod(&c, &osaka, "hop", 9443, PodIngress::RelayQuic);
    hop.advertise_ip = Some("2001:db8::10".to_string());
    let origin = exit(&c, "origin", "10.0.0.5:8080");
    let mut near = edge_to_pod("near", &entry, &hop);
    near.override_ip = Some("hop.example.net".to_string());
    near.override_port = Some(19443);
    let far = edge_to_pod("far", &entry, &hop);
    let out = edge_to_exit("out", &hop, &origin);
    let entry = routed(
        entry,
        guru_topology::Route::Failover(vec![via(&near), via(&far)]),
    );
    let hop = routed(hop, via(&out));
    let group = GroupEntity {
        id: GroupId::from_key(key("splitter")),
        canvas: c.id.clone(),
        kind: "splitter".to_string(),
        name: "fan-out".to_string(),
        props: serde_json::json!({"x": 10, "y": -4}),
        members: vec![
            GroupMember::Edge(near.id.clone()),
            GroupMember::Pod(entry.id.clone()),
            GroupMember::Server(osaka.id.clone()),
            GroupMember::Exit(origin.id.clone()),
        ],
    };
    let mut batch = inserting(
        &c,
        vec![entry.clone(), hop.clone()],
        vec![origin.clone()],
        vec![near.clone(), far.clone(), out.clone()],
    );
    batch.insert_groups = vec![group.clone()];
    sp.process(batch).await?;

    let graph = sp
        .process(LoadCanvasGraph {
            canvas: c.id.clone(),
        })
        .await?;
    assert_eq!(graph.root, c.id);
    assert_eq!(graph.servers.len(), 2);
    let mut pods = graph.pods.clone();
    pods.sort_by(|a, b| a.name.cmp(&b.name));
    assert_eq!(pods, vec![entry, hop]);
    assert_eq!(graph.exits, vec![origin]);
    let mut edges = graph.edges.clone();
    edges.sort_by(|a, b| a.id.cmp(&b.id));
    let mut expected = vec![near, far, out];
    expected.sort_by(|a, b| a.id.cmp(&b.id));
    assert_eq!(edges, expected);
    assert_eq!(graph.groups, vec![group]);
    Ok(())
}

#[sqlx::test(migrator = "base::db::MIGRATOR")]
async fn a_graph_write_bumps_the_canvas_generation(pool: sqlx::PgPool) -> TestResult {
    let sp = setup(pool);
    let c = canvas(&sp, "prod").await?;
    let s = server(&sp, &c, "tokyo").await?;
    let before = sp
        .process(FindCanvasById { id: c.id.clone() })
        .await?
        .unwrap()
        .generation;
    let entry = client(&c, &s, "pod", 443, None);
    let origin = exit(&c, "exit", "10.0.0.5:8080");
    let out = edge_to_exit("out", &entry, &origin);
    let entry = routed(entry, via(&out));
    let generation = sp
        .process(inserting(&c, vec![entry.clone()], vec![origin], vec![out.clone()]))
        .await?;
    assert_eq!(generation, before + 1);
    let after = sp
        .process(FindCanvasById { id: c.id.clone() })
        .await?
        .unwrap();
    assert_eq!(after.generation, before + 1);
    assert!(
        after.generation > after.derived_generation,
        "an edit leaves the canvas visibly underived until a pass catches up"
    );

    let mut unrouted = entry.clone();
    unrouted.route = None;
    sp.process(ApplyGraphBatch {
        canvas: Some(c.id.clone()),
        derives: true,
        delete_edges: vec![out.id.clone()],
        update_pods: vec![unrouted],
        ..ApplyGraphBatch::default()
    })
    .await?;
    assert_eq!(
        sp.process(FindCanvasById { id: c.id.clone() })
            .await?
            .unwrap()
            .generation,
        before + 2
    );

    // A batch of groups alone is drawing, not topology: no bump.
    sp.process(ApplyGraphBatch {
        canvas: Some(c.id.clone()),
        derives: false,
        insert_groups: vec![GroupEntity {
            id: GroupId::from_key(key("rule")),
            canvas: c.id.clone(),
            kind: "rule".to_string(),
            name: String::new(),
            props: serde_json::json!({}),
            members: vec![GroupMember::Pod(entry.id.clone())],
        }],
        ..ApplyGraphBatch::default()
    })
    .await?;
    assert_eq!(
        sp.process(FindCanvasById { id: c.id })
            .await?
            .unwrap()
            .generation,
        before + 2
    );
    Ok(())
}

/// The fence primitive: a write checked against a snapshot must not commit once
/// another edit has advanced the canvas past that snapshot's generation. Two
/// writes are fenced against the *same* generation — as two concurrent requests
/// that each read the pre-state would be — deterministically here, so the loser
/// is always the second. It rolls back whole, rows and generation bump alike,
/// and surfaces as a `Conflict`.
#[sqlx::test(migrator = "base::db::MIGRATOR")]
async fn a_second_write_fenced_on_a_superseded_generation_is_rejected(
    pool: sqlx::PgPool,
) -> TestResult {
    let sp = setup(pool);
    let c = canvas(&sp, "prod").await?;
    let s = server(&sp, &c, "tokyo").await?;

    // The generation two concurrent editors would both check against.
    let fence = sp
        .process(LoadCanvasGraph {
            canvas: c.id.clone(),
        })
        .await?
        .fence()
        .expect("a loaded canvas has a root");

    let mut first = inserting(&c, vec![client(&c, &s, "pod1", 443, None)], Vec::new(), Vec::new());
    first.fence = Some(fence.clone());
    let bumped = sp.process(first).await?;
    assert_eq!(bumped, fence.generation + 1);

    let mut second = inserting(&c, vec![client(&c, &s, "pod2", 444, None)], Vec::new(), Vec::new());
    second.fence = Some(fence);
    let err = sp.process(second).await.expect_err("the loser is rejected");
    assert!(
        err.to_string().contains("orchestration_stale_generation"),
        "the fence names its sentinel: {err}"
    );
    assert!(
        matches!(
            OrchestrationError::from(err),
            OrchestrationError::Conflict(_)
        ),
        "a stale fence is a conflict, not an opaque database error"
    );

    // The loser left nothing behind: no second pod, no extra bump.
    let after = sp
        .process(LoadCanvasGraph {
            canvas: c.id.clone(),
        })
        .await?;
    assert_eq!(after.pods.len(), 1, "only the winning pod persists");
    assert_eq!(after.generation(), bumped);
    Ok(())
}

/// Canvases nest by parent: the tree is read from any of its canvases, only
/// roots are listed, a subcanvas edit bumps the root, and deleting a canvas
/// takes its subtree with the edges leaving the subtree's pods.
#[sqlx::test(migrator = "base::db::MIGRATOR")]
async fn subcanvases_form_a_tree_by_parent(pool: sqlx::PgPool) -> TestResult {
    let sp = setup(pool);
    let root = canvas(&sp, "root").await?;
    let sub = subcanvas(&sp, &root, "sub").await?;
    let subsub = subcanvas(&sp, &sub, "subsub").await?;
    assert_eq!(sub.parent.as_ref(), Some(&root.id));

    let roots = sp
        .process(ListCanvases {
            include_subcanvases: false,
        })
        .await?;
    assert_eq!(roots.len(), 1);
    assert_eq!(
        sp.process(ListCanvases {
            include_subcanvases: true,
        })
        .await?
        .len(),
        3
    );
    let tree = sp
        .process(LoadCanvasTree {
            canvas: subsub.id.clone(),
        })
        .await?
        .unwrap();
    assert_eq!(tree.canvas.id, root.id);
    assert_eq!(tree.children[0].canvas.id, sub.id);
    assert_eq!(tree.children[0].children[0].canvas.id, subsub.id);

    // A pod drawn two levels down on a server of the root, dialing an exit on
    // the root.
    let s = server(&sp, &root, "tokyo").await?;
    let deep = client(&subsub, &s, "deep", 443, None);
    let origin = exit(&root, "origin", "10.0.0.5:8080");
    let out = edge_to_exit("out", &deep, &origin);
    let deep = routed(deep, via(&out));
    let before = sp
        .process(LoadCanvasGraph {
            canvas: root.id.clone(),
        })
        .await?
        .generation();
    sp.process(inserting(&subsub, vec![deep], vec![origin], vec![out]))
        .await?;
    let graph = sp
        .process(LoadCanvasGraph {
            canvas: sub.id.clone(),
        })
        .await?;
    assert_eq!(graph.root, root.id, "any canvas of the tree reads the whole tree");
    assert_eq!(graph.canvases.len(), 3);
    assert_eq!((graph.pods.len(), graph.exits.len(), graph.edges.len()), (1, 1, 1));
    assert_eq!(graph.generation(), before + 1, "the root is what an edit bumps");

    sp.process(DeleteCanvasRow { id: sub.id.clone() }).await?;
    let graph = sp
        .process(LoadCanvasGraph {
            canvas: root.id.clone(),
        })
        .await?;
    assert_eq!(graph.canvases.len(), 1);
    assert!(graph.pods.is_empty() && graph.edges.is_empty());
    assert_eq!(graph.exits.len(), 1, "the root's exit stays");
    assert!(
        sp.process(FindCanvasById { id: subsub.id })
            .await?
            .is_none()
    );
    Ok(())
}

#[sqlx::test(migrator = "base::db::MIGRATOR")]
async fn register_of_a_worker_running_nothing_forgets_the_applied_revision(
    pool: sqlx::PgPool,
) -> TestResult {
    let sp = setup(pool);
    let c = canvas(&sp, "prod").await?;
    let s = server(&sp, &c, "tokyo").await?;
    seed_desired(&sp, &s.id, 3).await?;
    let register =
        |digest: &str, now: chrono::DateTime<chrono::Utc>, running: i64| RegisterWorkerSession {
            server: s.id.clone(),
            digest: digest.to_string(),
            now,
            lease_until: now,
            running_revision: running,
            observed: None,
            reported: None,
            agent_version: None,
            agent_arch: None,
            capabilities: Vec::new(),
        };

    // The first worker ran revision 3: registering as such records it as applied,
    // and there is nothing left to hand a stream.
    let now = chrono::Utc::now();
    sp.process(register("digest-1", now, 3))
        .await?
        .expect("the free session is taken");
    let view = sp
        .process(FindServerConfigView {
            server: s.id.clone(),
        })
        .await?
        .unwrap();
    assert_eq!(view.applied.as_ref().map(|s| s.revision), Some(3));
    assert!(
        sp.process(TakeInFlight {
            server: s.id.clone(),
            generation: 1,
            epoch: 0,
        })
        .await?
        .is_none(),
        "desired equals applied: converged, nothing to send"
    );

    // The host was reinstalled: the new worker runs nothing. Treating the server
    // as still converged would leave it running nothing forever.
    let later = now + chrono::TimeDelta::seconds(1);
    let row = sp
        .process(register("digest-2", later, 0))
        .await?
        .expect("the lapsed lease is taken over");
    let view = sp
        .process(FindServerConfigView {
            server: s.id.clone(),
        })
        .await?
        .unwrap();
    assert!(
        view.applied.is_none(),
        "a worker running nothing has applied nothing"
    );
    assert!(view.in_flight.is_none());
    assert_eq!(view.apply_error, None);
    let taken = sp
        .process(TakeInFlight {
            server: s.id.clone(),
            generation: row.refresh_key_generation,
            epoch: 0,
        })
        .await?
        .expect("the desired revision is offered to the new worker again");
    assert_eq!(taken.revision, 3);
    Ok(())
}

#[sqlx::test(migrator = "base::db::MIGRATOR")]
async fn registration_records_the_worker_build_and_keeps_it_when_unreported(
    pool: sqlx::PgPool,
) -> TestResult {
    let sp = setup(pool);
    let c = canvas(&sp, "prod").await?;
    let s = server(&sp, &c, "tokyo").await?;
    assert_eq!(s.agent_version, None);

    let now = chrono::Utc::now();
    let register = |digest: &str, now: chrono::DateTime<chrono::Utc>, build: Option<&str>| {
        RegisterWorkerSession {
            server: s.id.clone(),
            digest: digest.to_string(),
            now,
            lease_until: now,
            running_revision: 0,
            observed: None,
            reported: None,
            agent_version: build.map(str::to_owned),
            agent_arch: build.map(|_| "x86_64".to_string()),
            capabilities: Vec::new(),
        }
    };

    // A worker that reports its build has it recorded.
    let row = sp
        .process(register("digest-1", now, Some("0.2.0-beta")))
        .await?
        .expect("the free session is taken");
    assert_eq!(row.agent_version.as_deref(), Some("0.2.0-beta"));
    assert_eq!(row.agent_arch.as_deref(), Some("x86_64"));

    // An older worker that reports nothing (empty on the wire, `None` here)
    // must not blank what is known.
    let later = now + chrono::TimeDelta::seconds(1);
    let row = sp
        .process(register("digest-2", later, None))
        .await?
        .expect("the lapsed lease is taken over");
    assert_eq!(row.agent_version.as_deref(), Some("0.2.0-beta"));
    assert_eq!(row.agent_arch.as_deref(), Some("x86_64"));

    // A newer build replaces it.
    let row = sp
        .process(register(
            "digest-3",
            later + chrono::TimeDelta::seconds(1),
            Some("0.3.0"),
        ))
        .await?
        .expect("the lapsed lease is taken over");
    assert_eq!(row.agent_version.as_deref(), Some("0.3.0"));
    Ok(())
}

#[sqlx::test(migrator = "base::db::MIGRATOR")]
async fn agent_release_publish_replaces_the_single_row(pool: sqlx::PgPool) -> TestResult {
    let sp = setup(pool);
    assert!(sp.process(FindAgentRelease).await?.is_none());

    let first = chrono::Utc::now();
    sp.process(PublishAgentRelease {
        version: "0.2.0-beta".to_string(),
        sha256: "a".repeat(64),
        arch: "x86_64".to_string(),
        now: first,
    })
    .await?;
    let row = sp.process(FindAgentRelease).await?.expect("published");
    assert_eq!(row.version, "0.2.0-beta");
    assert_eq!(row.sha256, "a".repeat(64));
    assert_eq!(row.arch, "x86_64");

    // A second publish replaces the row rather than adding one.
    sp.process(PublishAgentRelease {
        version: "0.3.0".to_string(),
        sha256: "b".repeat(64),
        arch: "x86_64".to_string(),
        now: first + chrono::TimeDelta::seconds(60),
    })
    .await?;
    let row = sp.process(FindAgentRelease).await?.expect("published");
    assert_eq!(row.version, "0.3.0");
    assert_eq!(row.sha256, "b".repeat(64));
    Ok(())
}
