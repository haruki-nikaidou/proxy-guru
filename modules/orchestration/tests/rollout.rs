//! Rollout acceptance: a change reaches every worker without breaking a path that
//! is still carrying traffic.

#![allow(clippy::unwrap_used, clippy::panic, clippy::expect_used)]

mod common;

use common::*;
use guru_worker_config::Config;
use kanau::processor::Processor;
use orchestration::entities::db::canvas::{CanvasEntity, CanvasId, FindCanvasById};
use orchestration::entities::db::pod::{PodEntity, PodIngress};
use orchestration::entities::db::server::{
    FindServerById, ServerId, ServerIpv6Resolve, ServerLogLevel,
};
use orchestration::entities::db::view::{
    AckServerConfig, ListStaleCanvases, ListenProtocol, ListenerCap, ServerConfigViewEntity,
    TakeInFlight,
};
use orchestration::services::agent::{
    AckConfig, AgentIdentity, PodResult, RegisterCredential, RegisterWorker,
};
use orchestration::services::graph::GraphChange;
use orchestration::services::rollout::ForgetServerApplied;
use orchestration::services::server::{AddressOverrides, CreateServer};
use orchestration::services::{OrchestrationError, canvas as canvas_service};

/// Takes and acknowledges whatever the database offers this server, the way a
/// worker would.
///
/// Registration happens once per server: a worker holds its session for as long as
/// it lives, and a second registration would be refused while that session is.
async fn ack_current(w: &World, server: &ServerId) -> Result<(), Box<dyn std::error::Error>> {
    let registered =
        w.db.process(FindServerById { id: server.clone() })
            .await?
            .is_some_and(|row| row.refresh_key_generation > 0);
    if !registered {
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
    }
    let row =
        w.db.process(FindServerById { id: server.clone() })
            .await?
            .unwrap();
    let Some(snapshot) =
        w.db.process(TakeInFlight {
            server: server.clone(),
            generation: row.refresh_key_generation,
            epoch: row.watch_epoch,
        })
        .await?
    else {
        return Ok(());
    };
    w.db.process(AckServerConfig {
        server: server.clone(),
        revision: snapshot.revision,
        error: None,
        applied: None,
        failed_pods: Vec::new(),
    })
    .await?;
    Ok(())
}

/// Derives and acknowledges until nothing is left to roll out.
///
/// A cold start needs one pass per hop: nothing is applied yet, so the first pass
/// can only hand each server the forwardings whose targets depend on nothing.
async fn settle(w: &World, f: &Fixture) -> Result<(), Box<dyn std::error::Error>> {
    for _ in 0..8 {
        w.derive(&f.canvas).await?;
        ack_current(w, &f.tokyo).await?;
        ack_current(w, &f.osaka).await?;
        let mut caught_up = !w.db.process(ListStaleCanvases).await?.contains(&f.canvas);
        for server in [&f.tokyo, &f.osaka] {
            let view = w.view(server).await?;
            caught_up &= view.in_flight.is_none()
                && view.applied.as_ref().map(|s| s.revision)
                    == view.desired.as_ref().map(|s| s.revision);
        }
        if caught_up {
            return Ok(());
        }
    }
    panic!("the fabric never settled")
}

fn cap(server: &ServerId, port: i64, protocol: ListenProtocol) -> ListenerCap {
    ListenerCap {
        server: server.clone(),
        port,
        protocol,
    }
}

fn serves(
    view: &ServerConfigViewEntity,
    slot: &Option<orchestration::entities::db::view::ConfigSnapshot>,
) -> Vec<ListenerCap> {
    let _ = view;
    slot.as_ref()
        .map(|s| s.forwardings.iter().map(|d| d.serves.clone()).collect())
        .unwrap_or_default()
}

fn points_at(slot: &Option<orchestration::entities::db::view::ConfigSnapshot>) -> Vec<ListenerCap> {
    slot.as_ref()
        .map(|s| {
            s.forwardings
                .iter()
                .flat_map(|d| d.points_at.iter().cloned())
                .collect()
        })
        .unwrap_or_default()
}

/// Two servers, one relay hop: `tokyo` fronts `443` and forwards to `osaka`, whose
/// pod listens on `9443` and exits to a backend.
struct Fixture {
    canvas: CanvasId,
    canvas_row: CanvasEntity,
    tokyo: ServerId,
    osaka: ServerId,
    osaka_hop: PodEntity,
}

async fn relay_chain(w: &World) -> Result<Fixture, Box<dyn std::error::Error>> {
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

    let mut servers = Vec::new();
    for (name, ip) in [("tokyo", "203.0.113.10"), ("osaka", "198.51.100.10")] {
        let server = w
            .servers
            .process(CreateServer {
                actor: operator(),
                canvas: canvas.id.clone(),
                name: name.to_string(),
                icon: String::new(),
                comment: String::new(),
                position: pos0(),
                ipv6_resolve: ServerIpv6Resolve::Tolerated,
                log_level: ServerLogLevel::Info,
                addresses: AddressOverrides {
                    override_v4: Some(ip.to_string()),
                    override_v6: None,
                    extra_addresses: Vec::new(),
                },
            })
            .await?;
        servers.push(server);
    }
    let ingress = client(&canvas, &servers[0], "ingress", 443, None);
    let osaka_hop = pod(&canvas, &servers[1], "osaka-hop", 9443, PodIngress::RelayTcp);
    let origin = exit(&canvas, "exit", "10.0.0.5:8080");
    let to_osaka = edge_to_pod("to-osaka", &ingress, &osaka_hop);
    let out = edge_to_exit("out", &osaka_hop, &origin);
    let osaka_hop = routed(osaka_hop, via(&out));
    w.apply(
        &canvas,
        GraphChange {
            put_pods: vec![routed(ingress, via(&to_osaka)), osaka_hop.clone()],
            put_exits: vec![origin],
            put_edges: vec![to_osaka, out],
            ..GraphChange::default()
        },
    )
    .await?;
    Ok(Fixture {
        canvas: canvas.id.clone(),
        canvas_row: canvas,
        tokyo: servers[0].id.clone(),
        osaka: servers[1].id.clone(),
        osaka_hop,
    })
}

#[sqlx::test(migrator = "base::db::MIGRATOR")]
async fn a_relay_switches_only_after_its_target_serves_the_new_listener(
    pool: sqlx::PgPool,
) -> TestResult {
    let w = world(pool).await?;
    let f = relay_chain(&w).await?;

    // 1. The cold start converges and both workers adopt what they are given.
    settle(&w, &f).await?;
    for server in [&f.tokyo, &f.osaka] {
        let view = w.view(server).await?;
        assert_eq!(
            view.applied.as_ref().map(|s| s.revision),
            view.desired.as_ref().map(|s| s.revision),
            "both servers start caught up"
        );
    }
    let tokyo_start = w.view(&f.tokyo).await?.desired.unwrap().revision;

    // 2. Move osaka's pod to a new port. Osaka must serve both listeners, and tokyo
    //    must keep pointing at the old one.
    move_osaka_hop(&w, &f, 9444).await?;
    w.derive(&f.canvas).await?;

    let osaka_view = w.view(&f.osaka).await?;
    let osaka_serves = serves(&osaka_view, &osaka_view.desired);
    assert!(
        osaka_serves.contains(&cap(&f.osaka, 9443, ListenProtocol::RelayTcp)),
        "the old listener stays up while tokyo still dials it: {osaka_serves:?}"
    );
    assert!(
        osaka_serves.contains(&cap(&f.osaka, 9444, ListenProtocol::RelayTcp)),
        "and the new one comes up alongside it: {osaka_serves:?}"
    );

    let tokyo_view = w.view(&f.tokyo).await?;
    assert_eq!(
        points_at(&tokyo_view.desired),
        vec![cap(&f.osaka, 9443, ListenProtocol::RelayTcp)],
        "tokyo must not be pointed at a listener nobody serves yet"
    );
    assert_eq!(
        tokyo_view
            .waiting_for
            .iter()
            .map(|s| s.0.clone())
            .collect::<Vec<_>>(),
        vec![f.osaka.0.clone()],
        "and it says what it is waiting for"
    );
    assert_eq!(
        tokyo_view.desired.as_ref().map(|s| s.revision),
        Some(tokyo_start),
        "an unchanged config is not a new revision, so the worker is not restarted"
    );

    // 3. Osaka adopts both listeners; only now may tokyo switch.
    ack_current(&w, &f.osaka).await?;
    w.derive(&f.canvas).await?;
    let tokyo_view = w.view(&f.tokyo).await?;
    assert_eq!(
        points_at(&tokyo_view.desired),
        vec![cap(&f.osaka, 9444, ListenProtocol::RelayTcp)],
        "the switch happens once the target is proven to serve the new listener"
    );
    assert!(tokyo_view.waiting_for.is_empty());
    let osaka_serves = serves(&osaka_view, &w.view(&f.osaka).await?.desired);
    assert!(
        osaka_serves.contains(&cap(&f.osaka, 9443, ListenProtocol::RelayTcp)),
        "osaka may not drop the old listener while tokyo still runs a config using it"
    );

    // 4. Tokyo adopts the switch, so the old listener is finally free.
    ack_current(&w, &f.tokyo).await?;
    w.derive(&f.canvas).await?;
    let osaka_serves = serves(&osaka_view, &w.view(&f.osaka).await?.desired);
    assert_eq!(
        osaka_serves,
        vec![cap(&f.osaka, 9444, ListenProtocol::RelayTcp)],
        "nothing points at 9443 any more, so it is dropped"
    );

    // 5. The fabric settles: everything applied, nothing in flight, nothing pending.
    ack_current(&w, &f.osaka).await?;
    w.derive(&f.canvas).await?;
    for server in [&f.tokyo, &f.osaka] {
        let view = w.view(server).await?;
        assert_eq!(
            view.applied.as_ref().map(|s| s.revision),
            view.desired.as_ref().map(|s| s.revision)
        );
        assert!(view.in_flight.is_none());
        assert!(view.derive_error.is_none(), "{:?}", view.derive_error);
    }
    let canvas =
        w.db.process(FindCanvasById {
            id: f.canvas.clone(),
        })
        .await?
        .unwrap();
    assert_eq!(
        canvas.derived_generation, canvas.generation,
        "a settled canvas has nothing left to derive"
    );
    Ok(())
}

/// Moves osaka's pod to another port, which is how a worker comes to run two
/// listeners of one pod: the new one, and the old one tokyo still dials.
async fn move_osaka_hop(
    w: &World,
    f: &Fixture,
    port: u16,
) -> Result<(), Box<dyn std::error::Error>> {
    w.apply(
        &f.canvas_row,
        GraphChange {
            put_pods: vec![PodEntity {
                port,
                ..f.osaka_hop.clone()
            }],
            ..GraphChange::default()
        },
    )
    .await?;
    Ok(())
}

fn tags_by_port(config: &Config) -> Vec<(String, u16)> {
    let mut tags: Vec<(String, u16)> = config
        .forwardings
        .iter()
        .map(|f| (f.tag.clone(), f.listen.port()))
        .collect();
    tags.sort_by_key(|(_, port)| *port);
    tags
}

/// A worker keys its listeners by tag and an ack must name every tag of a revision
/// exactly once, so the two listeners a moved pod serves during the switch need
/// tags of their own — and the held one must keep its tag from pass to pass, or
/// every pass would be a new revision.
#[sqlx::test(migrator = "base::db::MIGRATOR")]
async fn a_moved_listener_is_held_under_its_own_tag_until_its_dependant_switches(
    pool: sqlx::PgPool,
) -> TestResult {
    let w = world(pool).await?;
    let f = relay_chain(&w).await?;
    settle(&w, &f).await?;

    move_osaka_hop(&w, &f, 9444).await?;
    w.derive(&f.canvas).await?;
    let view = w.view(&f.osaka).await?;
    assert!(view.derive_error.is_none(), "{:?}", view.derive_error);
    let desired = view.desired.expect("osaka has a desired config");
    let config = Config::from_toml_str(&desired.toml)?;
    assert_eq!(
        tags_by_port(&config),
        vec![
            (format!("{} (9443/relay_tcp)", key("osaka-hop")), 9443),
            (key("osaka-hop"), 9444),
        ],
        "the listener tokyo still dials is held under a tag of its own"
    );

    // The worker acks each forwarding by its tag, through the same service the
    // real worker reaches.
    let row =
        w.db.process(FindServerById {
            id: f.osaka.clone(),
        })
        .await?
        .unwrap();
    let snapshot =
        w.db.process(TakeInFlight {
            server: f.osaka.clone(),
            generation: row.refresh_key_generation,
            epoch: row.watch_epoch,
        })
        .await?
        .expect("the moved pod is offered to osaka");
    w.agents
        .process(AckConfig {
            agent: AgentIdentity {
                server: f.osaka.clone(),
                generation: row.refresh_key_generation,
            },
            revision: snapshot.revision,
            error: None,
            pods: config
                .forwardings
                .iter()
                .map(|f| PodResult {
                    tag: f.tag.clone(),
                    error: None,
                })
                .collect(),
        })
        .await?;

    // Tokyo has not switched yet, so osaka's next pass holds the same two entries
    // under the same tags: a new revision here would restart the worker for nothing.
    w.derive(&f.canvas).await?;
    let again = w.view(&f.osaka).await?.desired.unwrap();
    assert_eq!(
        again.revision, desired.revision,
        "the held tag is stable across passes: {}",
        again.toml
    );

    // Tokyo adopts the switch; the held listener goes, tag and all.
    ack_current(&w, &f.tokyo).await?;
    w.derive(&f.canvas).await?;
    let settled = Config::from_toml_str(&w.view(&f.osaka).await?.desired.unwrap().toml)?;
    assert_eq!(
        tags_by_port(&settled),
        vec![(key("osaka-hop"), 9444)]
    );
    Ok(())
}

#[sqlx::test(migrator = "base::db::MIGRATOR")]
async fn a_protocol_change_on_a_referenced_listener_is_rejected(pool: sqlx::PgPool) -> TestResult {
    let w = world(pool).await?;
    let f = relay_chain(&w).await?;
    settle(&w, &f).await?;

    // Make osaka's pod listen over TLS instead of plain TCP: same port and
    // transport, but tokyo's running config still dials it as plain TCP. The two
    // cannot coexist on one worker, so there is no seamless path.
    let outcome = w
        .try_apply(
            &f.canvas_row,
            GraphChange {
                put_pods: vec![PodEntity {
                    ingress: PodIngress::RelayTls,
                    ..f.osaka_hop.clone()
                }],
                ..GraphChange::default()
            },
        )
        .await?;
    assert!(!outcome.applied, "a protocol switch under a live dependant must be refused");
    let refusal = outcome
        .diagnostics
        .iter()
        .find(|d| d.error && d.problem == "listener_in_use")
        .unwrap_or_else(|| panic!("{:?}", outcome.diagnostics));
    assert!(
        refusal.message.contains("osaka-hop") && refusal.message.contains("9443"),
        "the refusal names the pod and its port: {}",
        refusal.message
    );
    Ok(())
}

#[sqlx::test(migrator = "base::db::MIGRATOR")]
async fn forgetting_what_a_server_runs_is_admin_only(pool: sqlx::PgPool) -> TestResult {
    let w = world(pool).await?;
    let f = relay_chain(&w).await?;
    settle(&w, &f).await?;

    // A Maintainer may edit the canvas, but not assert that a live server is dead:
    // convergence trusts `applied`, so a wrong assertion switches tokyo off a
    // listener osaka may still be serving.
    let err = w
        .rollout
        .process(ForgetServerApplied {
            actor: maintainer(),
            server: f.osaka.clone(),
        })
        .await
        .expect_err("only an admin may declare a server gone");
    assert!(
        matches!(err, OrchestrationError::PermissionDenied),
        "{err:?}"
    );
    assert!(
        w.view(&f.osaka).await?.applied.is_some(),
        "the refused call left the server's state alone"
    );

    w.rollout
        .process(ForgetServerApplied {
            actor: operator(),
            server: f.osaka.clone(),
        })
        .await?;
    let view = w.view(&f.osaka).await?;
    assert!(view.applied.is_none());
    assert!(view.in_flight.is_none());
    Ok(())
}

#[sqlx::test(migrator = "base::db::MIGRATOR")]
async fn a_worker_credential_cannot_edit_the_workspace(pool: sqlx::PgPool) -> TestResult {
    let w = world(pool).await?;
    let err = w
        .canvases
        .process(canvas_service::CreateCanvas {
            actor: machine(),
            name: "prod".to_string(),
            description: String::new(),
            parent: None,
            position: pos0(),
        })
        .await
        .expect_err("api keys may not edit the workspace");
    assert!(matches!(err, OrchestrationError::Core(_)), "{err:?}");

    // ...and a human session may not register a worker.
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
            name: "a".to_string(),
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
    let err = w
        .agents
        .process(RegisterWorker {
            credential: RegisterCredential::Operator(operator()),
            server_id: server.id,
            running_revision: 0,
            observed: None,
            reported: None,
            agent_version: None,
            agent_arch: None,
            capabilities: Vec::new(),
            last_update_error: None,
        })
        .await
        .expect_err("human sessions may not register workers");
    assert!(matches!(err, OrchestrationError::Core(_)), "{err:?}");
    Ok(())
}

/// A pod that stops compiling keeps carrying what it already serves: the listener
/// stays in `desired`, the reason lands in `invalid_pods`, and the server is not
/// failed as a whole.
#[sqlx::test(migrator = "base::db::MIGRATOR")]
async fn a_pod_that_stops_deriving_keeps_serving_its_listener(pool: sqlx::PgPool) -> TestResult {
    let w = world(pool).await?;
    let f = relay_chain(&w).await?;
    settle(&w, &f).await?;

    let before = w.view(&f.tokyo).await?;
    let served = serves(&before, &before.applied);
    assert_eq!(
        served,
        vec![cap(&f.tokyo, 443, ListenProtocol::Raw)],
        "tokyo serves its ingress listener before the edit"
    );
    assert!(before.invalid_pods.is_empty(), "{:?}", before.invalid_pods);

    // Osaka loses the only address tokyo could dial it on, so tokyo's ingress
    // pod alone stops compiling.
    let osaka = w
        .db
        .process(FindServerById {
            id: f.osaka.clone(),
        })
        .await?
        .unwrap();
    w.servers
        .process(orchestration::services::server::UpdateServer {
            actor: operator(),
            server: f.osaka.clone(),
            name: osaka.name,
            icon: osaka.icon,
            comment: osaka.comment,
            ipv6_resolve: osaka.ipv6_resolve,
            log_level: osaka.log_level,
            quic: osaka.quic,
            addresses: AddressOverrides::default(),
            agent_unit: osaka.agent_unit,
        })
        .await?;
    w.derive(&f.canvas).await?;

    let after = w.view(&f.tokyo).await?;
    assert!(
        after.derive_error.is_none(),
        "one broken pod is not a server failure: {:?}",
        after.derive_error
    );
    let [invalid] = after.invalid_pods.as_slice() else {
        panic!(
            "expected the ingress pod to be reported, got {:?}",
            after.invalid_pods
        );
    };
    assert_eq!(invalid.pod, pod_id("ingress"));
    assert_eq!(invalid.name, "ingress");
    assert_eq!(invalid.listen, "[::]:443");
    assert_eq!(
        serves(&after, &after.desired),
        served,
        "the listener it already carries is held, not dropped"
    );
    Ok(())
}
