//! Entity-layer queries against an in-memory SurrealDB with the real schema.

#![allow(clippy::unwrap_used, clippy::panic, clippy::expect_used)]

mod common;

use common::*;
use kanau::processor::Processor;
use orchestration::entities::surreal::canvas::{
    DeleteCanvasRow, FindCanvasById, ListCanvases, UpdateCanvasMeta,
};
use orchestration::entities::surreal::connection::{
    ConnectPorts, DeleteEdgeRow, EdgeConnectionId, FindEdgeById,
};
use orchestration::entities::surreal::node::{
    DeleteNodeRow, ExitConfig, FindNodeById, FindNodeWithPorts, NodeSpec, PodConfig,
    UpdateNodeMetaRow, UpdateNodeSpecRow,
};
use orchestration::entities::surreal::port::PortId;
use orchestration::entities::surreal::server::{
    ClaimServerWatchSession, DeleteServerRow, FindServerById, FindServerByRefreshKeyDigest,
    ListServersByCanvas, MoveServerPosition, RegisterWorkerSession, ReleaseServerWatchSession,
    RenewServerWatchSession, ServerIpv6Resolve, UpdateServerSettings,
};
use orchestration::entities::surreal::topology::{
    FindCanvasOfServer, LoadCanvasContents, LoadCanvasTopology,
};
use orchestration::entities::surreal::view::{
    AckServerConfig, FindServerConfigView, ListServerWatchState, TakeInFlight,
};

fn exit_spec(dest: &str) -> NodeSpec {
    NodeSpec::Exit(ExitConfig {
        destination: dest.to_string(),
        pass_proxy_protocol: None,
    })
}

#[tokio::test]
async fn canvas_crud_round_trip() -> TestResult {
    let sp = setup().await?;
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

#[tokio::test]
async fn creating_a_server_creates_its_empty_config_view() -> TestResult {
    let sp = setup().await?;
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
            log_level: "debug".to_string(),
            override_v4: None,
            override_v6: None,
            extra_addresses: Vec::new(),
        })
        .await?;
    assert_eq!(updated.ipv6_resolve, ServerIpv6Resolve::Preferred);
    assert_eq!(updated.log_level, "debug");

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

#[tokio::test]
async fn a_worker_session_is_owned_by_one_registration_at_a_time() -> TestResult {
    let sp = setup().await?;
    let c = canvas(&sp, "prod").await?;
    let s = server(&sp, &c, "tokyo").await?;

    let start = chrono::Utc::now();
    let lease = chrono::TimeDelta::seconds(30);
    let register = |digest: &str, now: chrono::DateTime<chrono::Utc>| RegisterWorkerSession {
        server: s.id.clone(),
        canvas: c.id.clone(),
        digest: digest.to_string(),
        now,
        lease_until: now + lease,
        running_revision: 0,
        observed: None,
        reported: None,
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
    sp: &wakuwaku::surreal::SurrealProcessor,
    server: &orchestration::entities::surreal::server::ServerId,
    revision: i64,
) -> Result<(), surrealdb::Error> {
    sp.db()
        .query(
            "UPDATE orchestration_server_config_view
                 SET desired = { revision: $revision, toml: $toml, created_at: time::now(), forwardings: [] }
                 WHERE server = $server",
        )
        .bind(("server", server.clone()))
        .bind(("revision", revision))
        .bind(("toml", format!("# revision {revision}")))
        .await?
        .check()?;
    Ok(())
}

#[tokio::test]
async fn take_in_flight_and_ack_move_the_slots() -> TestResult {
    let sp = setup().await?;
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
            canvas: c.id.clone(),
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
            canvas: c.id.clone(),
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
            canvas: c.id.clone(),
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

#[tokio::test]
async fn a_new_stream_is_offered_what_the_previous_one_never_acked() -> TestResult {
    let sp = setup().await?;
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

#[tokio::test]
async fn register_promotes_a_reported_desired_revision() -> TestResult {
    let sp = setup().await?;
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
        canvas: c.id.clone(),
        digest: "digest-1".to_string(),
        now,
        lease_until: now + chrono::TimeDelta::seconds(30),
        running_revision: 3,
        observed: None,
        reported: None,
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

#[tokio::test]
async fn register_promotes_a_reported_in_flight_revision() -> TestResult {
    let sp = setup().await?;
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
        canvas: c.id.clone(),
        digest: "digest-1".to_string(),
        now,
        lease_until: now + chrono::TimeDelta::seconds(30),
        running_revision: 1,
        observed: None,
        reported: None,
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

#[tokio::test]
async fn register_rejects_an_unknown_running_revision() -> TestResult {
    let sp = setup().await?;
    let c = canvas(&sp, "prod").await?;
    let s = server(&sp, &c, "tokyo").await?;
    seed_desired(&sp, &s.id, 3).await?;

    let now = chrono::Utc::now();
    sp.process(RegisterWorkerSession {
        server: s.id.clone(),
        canvas: c.id.clone(),
        digest: "digest-1".to_string(),
        now,
        lease_until: now + chrono::TimeDelta::seconds(30),
        running_revision: 7,
        observed: None,
        reported: None,
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
#[tokio::test]
async fn deleting_a_server_with_a_live_pod_is_refused() -> TestResult {
    let sp = setup().await?;
    let c = canvas(&sp, "prod").await?;
    let s = server(&sp, &c, "tokyo").await?;
    let pod = node(&sp, &c, "pod", pod_spec(&s, 443), pod_ports()).await?;

    sp.process(DeleteServerRow {
        id: s.id.clone(),
        canvas: c.id.clone(),
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
    sp.process(DeleteNodeRow {
        id: pod.node.id.clone(),
        canvas: c.id.clone(),
        import_sync: None,
        frees_canvas: None,
    })
    .await?;
    sp.process(DeleteServerRow {
        id: s.id.clone(),
        canvas: c.id.clone(),
    })
    .await?;
    assert!(sp.process(FindServerById { id: s.id }).await?.is_none());
    Ok(())
}

/// The pod -> server link is what lets derivation attribute a per-pod failure to
/// a server, so the schema refuses a link that is dangling or points into another
/// canvas. Written through the real `CreateNodeRow` path on purpose: the guard
/// sits on `spec.config.server`, and only the actual `NodeSpec` encoding proves
/// it guards where the rows really land.
#[tokio::test]
async fn a_pod_cannot_reference_a_server_outside_its_tree() -> TestResult {
    let sp = setup().await?;
    let c = canvas(&sp, "prod").await?;
    let other = canvas(&sp, "staging").await?;
    let s = server(&sp, &c, "tokyo").await?;

    // A sound reference is accepted...
    node(&sp, &c, "pod", pod_spec(&s, 443), pod_ports()).await?;

    // ...placing a pod in another canvas on this server is not: derivation loads
    // one tree at a time, so a cross-tree link reads exactly like a missing one.
    node(&sp, &other, "foreign-pod", pod_spec(&s, 443), pod_ports())
        .await
        .expect_err("a pod may not reach into another tree for its server");

    // ...and one pointing at a server id no row carries is refused inside
    // `CreateNodeRow`'s transaction, so nothing is written.
    let ghost = orchestration::utils::ids::server_id("ghost");
    let spec = NodeSpec::Pod(PodConfig {
        server: ghost,
        port: 443,
        bind_ip: None,
        advertise_ip: None,
    });
    node(&sp, &c, "ghost-pod", spec, pod_ports())
        .await
        .expect_err("a dangling server link must not be storable");
    let topology = sp
        .process(LoadCanvasTopology {
            canvas: c.id.clone(),
        })
        .await?;
    let names: Vec<&str> = topology
        .nodes
        .iter()
        .map(|n| n.node.name.as_str())
        .collect();
    assert_eq!(
        names,
        ["pod"],
        "the rejected pods left no row behind, and the sound one is untouched"
    );
    Ok(())
}

#[tokio::test]
async fn create_node_writes_node_and_ports_together() -> TestResult {
    let sp = setup().await?;
    let c = canvas(&sp, "prod").await?;
    let s = server(&sp, &c, "tokyo").await?;

    let pod = node(&sp, &c, "pod", pod_spec(&s, 443), pod_ports()).await?;
    assert_eq!(pod.ports.len(), 2);

    let loaded = sp
        .process(FindNodeWithPorts {
            id: pod.node.id.clone(),
        })
        .await?
        .unwrap();
    assert_eq!(loaded.ports.len(), 2);
    match loaded.node.spec {
        NodeSpec::Pod(cfg) => {
            assert_eq!(cfg.port, 443);
            assert_eq!(cfg.server.0, s.id.0);
        }
        other => panic!("expected pod spec, got {other:?}"),
    }

    let before = sp
        .process(FindCanvasById { id: c.id.clone() })
        .await?
        .unwrap()
        .generation;
    let meta = sp
        .process(UpdateNodeMetaRow {
            id: pod.node.id.clone(),
            canvas: c.id.clone(),
            name: "edge".to_string(),
            comment: "renamed".to_string(),
            position: Some(pos(3, 4)),
            import_sync: None,
        })
        .await?;
    assert_eq!(meta.node.name, "edge");
    assert_eq!(meta.node.position, pos(3, 4));
    // The name is the forwarding tag in the derived config, so the rename has to
    // schedule a derivation, and the query itself has to be what decides that.
    assert!(meta.renamed, "the rename must be reported by the query");
    let after_rename = sp
        .process(FindCanvasById { id: c.id.clone() })
        .await?
        .unwrap()
        .generation;
    assert!(
        after_rename > before,
        "a rename must bump the canvas generation: {before} -> {after_rename}"
    );

    // A move that carries the same name is invisible to workers: no rename, no
    // generation bump, so it does not schedule a pointless derivation.
    let moved = sp
        .process(UpdateNodeMetaRow {
            id: pod.node.id.clone(),
            canvas: c.id.clone(),
            name: "edge".to_string(),
            comment: "moved".to_string(),
            position: Some(pos(9, 9)),
            import_sync: None,
        })
        .await?;
    assert!(!moved.renamed);
    assert_eq!(moved.node.position, pos(9, 9));
    assert_eq!(
        sp.process(FindCanvasById { id: c.id.clone() })
            .await?
            .unwrap()
            .generation,
        after_rename,
        "a move must not bump the canvas generation"
    );
    Ok(())
}

#[tokio::test]
async fn deleting_a_node_removes_its_ports_and_edges() -> TestResult {
    let sp = setup().await?;
    let c = canvas(&sp, "prod").await?;
    let s = server(&sp, &c, "tokyo").await?;
    let pod = node(&sp, &c, "pod", pod_spec(&s, 443), pod_ports()).await?;
    let exit = node(&sp, &c, "exit", exit_spec("10.0.0.5:8080"), exit_ports()).await?;
    let edge = sp
        .process(ConnectPorts {
            source: port_of(&exit, "destination"),
            target: port_of(&pod, "destination"),
            canvas: c.id.clone(),
        })
        .await?;
    let before = sp
        .process(FindCanvasById { id: c.id.clone() })
        .await?
        .unwrap()
        .generation;

    sp.process(DeleteNodeRow {
        id: pod.node.id.clone(),
        canvas: c.id.clone(),
        import_sync: None,
        frees_canvas: None,
    })
    .await?;
    assert!(
        sp.process(FindNodeById { id: pod.node.id })
            .await?
            .is_none()
    );
    assert!(sp.process(FindEdgeById { id: edge.id }).await?.is_none());
    // Unfiltered on purpose: every canvas-scoped port or edge query reaches the
    // canvas through `owner`/`in`, which dereferences to NONE once the node row
    // is gone, so rows the cascade orphaned are invisible to them.
    let mut resp = sp
        .db()
        .query("SELECT VALUE id FROM orchestration_port")
        .await?;
    let ports_left = resp.take::<Vec<PortId>>(0)?;
    assert_eq!(
        ports_left.len(),
        1,
        "the deleted node's ports are gone from the table, not just from its canvas"
    );
    assert_eq!(
        ports_left[0].0,
        port_of(&exit, "destination").0,
        "and the port that survives is the untouched node's"
    );
    let mut resp = sp
        .db()
        .query("SELECT VALUE id FROM orchestration_edge_connection")
        .await?;
    assert!(
        resp.take::<Vec<EdgeConnectionId>>(0)?.is_empty(),
        "the edge that hung off those ports is gone from the table too"
    );
    let topology = sp
        .process(LoadCanvasTopology {
            canvas: c.id.clone(),
        })
        .await?;
    assert_eq!(topology.nodes.len(), 1);
    assert_eq!(
        sp.process(FindCanvasById { id: c.id })
            .await?
            .unwrap()
            .generation,
        before + 1,
        "the write and the generation bump are one transaction"
    );
    Ok(())
}

#[tokio::test]
async fn updating_a_spec_keeps_edges_on_surviving_ports() -> TestResult {
    let sp = setup().await?;
    let c = canvas(&sp, "prod").await?;
    let s = server(&sp, &c, "tokyo").await?;
    let pod = node(&sp, &c, "pod", pod_spec(&s, 443), pod_ports()).await?;
    let exit = node(&sp, &c, "exit", exit_spec("10.0.0.5:8080"), exit_ports()).await?;
    let edge = sp
        .process(ConnectPorts {
            source: port_of(&exit, "destination"),
            target: port_of(&pod, "destination"),
            canvas: c.id.clone(),
        })
        .await?;

    // Same port keys: the rows survive, so the edge is never touched.
    let updated = sp
        .process(UpdateNodeSpecRow {
            id: pod.node.id.clone(),
            canvas: c.id.clone(),
            spec: pod_spec(&s, 8443),
            ports: pod_ports(),
            import_sync: None,
        })
        .await?;
    assert_eq!(updated.node.id.0, pod.node.id.0);
    match &updated.node.spec {
        NodeSpec::Pod(cfg) => assert_eq!(cfg.port, 8443),
        other => panic!("expected pod spec, got {other:?}"),
    }
    assert_eq!(
        port_of(&updated, "destination").0,
        port_of(&pod, "destination").0,
        "a surviving port keeps its identity"
    );
    assert!(
        sp.process(FindEdgeById {
            id: edge.id.clone()
        })
        .await?
        .is_some(),
        "and with it the edge attached to it"
    );

    // A layout that drops the key takes that port and its edge with it.
    let narrowed: Vec<_> = pod_ports()
        .into_iter()
        .filter(|p| p.key != "destination")
        .collect();
    let updated = sp
        .process(UpdateNodeSpecRow {
            id: pod.node.id.clone(),
            canvas: c.id.clone(),
            spec: pod_spec(&s, 8443),
            ports: narrowed,
            import_sync: None,
        })
        .await?;
    assert_eq!(updated.ports.len(), 1);
    assert!(updated.ports.iter().all(|p| p.key != "destination"));
    assert!(sp.process(FindEdgeById { id: edge.id }).await?.is_none());
    Ok(())
}

#[tokio::test]
async fn an_edge_write_bumps_the_canvas_generation() -> TestResult {
    let sp = setup().await?;
    let c = canvas(&sp, "prod").await?;
    let s = server(&sp, &c, "tokyo").await?;
    let pod = node(&sp, &c, "pod", pod_spec(&s, 443), pod_ports()).await?;
    let exit = node(&sp, &c, "exit", exit_spec("10.0.0.5:8080"), exit_ports()).await?;
    let before = sp
        .process(FindCanvasById { id: c.id.clone() })
        .await?
        .unwrap()
        .generation;

    let edge = sp
        .process(ConnectPorts {
            source: port_of(&exit, "destination"),
            target: port_of(&pod, "destination"),
            canvas: c.id.clone(),
        })
        .await?;
    let after_connect = sp
        .process(FindCanvasById { id: c.id.clone() })
        .await?
        .unwrap();
    assert_eq!(after_connect.generation, before + 1);
    assert!(
        after_connect.generation > after_connect.derived_generation,
        "an edit leaves the canvas visibly underived until a pass catches up"
    );

    sp.process(DeleteEdgeRow {
        id: edge.id.clone(),
        canvas: c.id.clone(),
    })
    .await?;
    assert!(sp.process(FindEdgeById { id: edge.id }).await?.is_none());
    assert_eq!(
        sp.process(FindCanvasById { id: c.id })
            .await?
            .unwrap()
            .generation,
        before + 2
    );
    Ok(())
}

#[tokio::test]
async fn canvas_contents_render_the_whole_canvas() -> TestResult {
    let sp = setup().await?;
    let c = canvas(&sp, "prod").await?;
    let s = server(&sp, &c, "tokyo").await?;
    node(&sp, &c, "pod", pod_spec(&s, 443), pod_ports()).await?;

    let contents = sp
        .process(LoadCanvasContents {
            canvas: c.id.clone(),
        })
        .await?
        .unwrap();
    assert_eq!(contents.canvas.name, "prod");
    assert_eq!(contents.servers.len(), 1);
    assert_eq!(contents.nodes.len(), 1);

    let topology = sp
        .process(LoadCanvasTopology {
            canvas: c.id.clone(),
        })
        .await?;
    assert_eq!(topology.root.0, c.id.0);
    assert_eq!(topology.canvases.len(), 1);
    assert_eq!(topology.nodes.len(), 1);
    assert_eq!(topology.servers.len(), 1);
    Ok(())
}

#[tokio::test]
async fn register_of_a_worker_running_nothing_forgets_the_applied_revision() -> TestResult {
    let sp = setup().await?;
    let c = canvas(&sp, "prod").await?;
    let s = server(&sp, &c, "tokyo").await?;
    seed_desired(&sp, &s.id, 3).await?;
    let register = |digest: &str, now: chrono::DateTime<chrono::Utc>, running: i64| {
        RegisterWorkerSession {
            server: s.id.clone(),
            canvas: c.id.clone(),
            digest: digest.to_string(),
            now,
            lease_until: now,
            running_revision: running,
            observed: None,
            reported: None,
        }
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
    assert!(view.applied.is_none(), "a worker running nothing has applied nothing");
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
