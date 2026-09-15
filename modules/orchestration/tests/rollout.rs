//! Rollout acceptance: a change reaches every worker without breaking a path that
//! is still carrying traffic.

#![allow(clippy::unwrap_used, clippy::panic, clippy::expect_used)]

mod common;

use common::*;
use kanau::processor::Processor;
use orchestration::entities::surreal::canvas::{CanvasId, FindCanvasById};
use orchestration::entities::surreal::node::{
    EntryConfig, ExitConfig, NodeSpec, NodeWithPorts, PodConfig, RelayConfig, RelayProtocol,
};
use orchestration::entities::surreal::server::{FindServerById, ServerId, ServerIpv6Resolve};
use orchestration::entities::surreal::view::{
    AckServerConfig, ListenProtocol, ListenerCap, ServerConfigViewEntity, TakeInFlight,
};
use orchestration::services::agent::{RegisterCredential, RegisterWorker};
use orchestration::services::edge::{Connect, Disconnect};
use orchestration::services::node::{CreateNode, ReplaceNodeSpec};
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
        canvas: row.canvas,
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
        let canvas =
            w.db.process(FindCanvasById {
                id: f.canvas.clone(),
            })
            .await?
            .unwrap();
        let mut caught_up = canvas.generation == canvas.derived_generation;
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
    slot: &Option<orchestration::entities::surreal::view::ConfigSnapshot>,
) -> Vec<ListenerCap> {
    let _ = view;
    slot.as_ref()
        .map(|s| s.forwardings.iter().map(|d| d.serves.clone()).collect())
        .unwrap_or_default()
}

fn points_at(
    slot: &Option<orchestration::entities::surreal::view::ConfigSnapshot>,
) -> Vec<ListenerCap> {
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
    tokyo: ServerId,
    osaka: ServerId,
    osaka_hop: NodeWithPorts,
    osaka_hop_listen: orchestration::entities::surreal::port::PortId,
    to_osaka_listen: orchestration::entities::surreal::port::PortId,
}

async fn relay_chain(w: &World) -> Result<Fixture, Box<dyn std::error::Error>> {
    let canvas = w
        .canvases
        .process(canvas_service::CreateCanvas {
            actor: operator(),
            name: "prod".to_string(),
            description: String::new(),
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
                log_level: "info".to_string(),
                addresses: AddressOverrides {
                    override_v4: Some(ip.to_string()),
                    override_v6: None,
                    extra_addresses: Vec::new(),
                },
            })
            .await?;
        servers.push((server.id.clone(), server.id));
    }
    let (tokyo, tokyo_ip) = servers[0].clone();
    let (osaka, osaka_ip) = servers[1].clone();

    let create = async |name: &str, spec: NodeSpec| {
        w.nodes
            .process(CreateNode {
                actor: operator(),
                canvas: canvas.id.clone(),
                name: name.to_string(),
                comment: String::new(),
                spec,
                position: pos0(),
                item_count: 0,
            })
            .await
    };

    let ingress = create(
        "ingress",
        NodeSpec::Pod(PodConfig {
            server: tokyo_ip,
            port: 443,
            bind_ip: None,
            advertise_ip: None,
        }),
    )
    .await?;
    let entry = create(
        "entry",
        NodeSpec::Entry(EntryConfig {
            receive_proxy_protocol: None,
            tls: None,
        }),
    )
    .await?;
    let to_osaka = create(
        "to-osaka",
        NodeSpec::Relay(RelayConfig {
            protocol: RelayProtocol::TcpRaw,
            override_ip_address: None,
            override_port: None,
        }),
    )
    .await?;
    let osaka_hop = create(
        "osaka-hop",
        NodeSpec::Pod(PodConfig {
            server: osaka_ip,
            port: 9443,
            bind_ip: None,
            advertise_ip: None,
        }),
    )
    .await?;
    let exit = create(
        "exit",
        NodeSpec::Exit(ExitConfig {
            destination: "10.0.0.5:8080".to_string(),
            pass_proxy_protocol: None,
        }),
    )
    .await?;

    let connect = async |output, input| {
        w.edges
            .process(Connect {
                actor: operator(),
                output_port: output,
                input_port: input,
            })
            .await
    };
    connect(port_of(&ingress, "listen"), port_of(&entry, "listen")).await?;
    connect(
        port_of(&to_osaka, "destination"),
        port_of(&ingress, "destination"),
    )
    .await?;
    // The relay is the listening side: the pod on the receiving server feeds it.
    connect(port_of(&osaka_hop, "listen"), port_of(&to_osaka, "listen")).await?;
    connect(
        port_of(&exit, "destination"),
        port_of(&osaka_hop, "destination"),
    )
    .await?;

    Ok(Fixture {
        canvas: canvas.id,
        tokyo,
        osaka,
        osaka_hop_listen: port_of(&osaka_hop, "listen"),
        to_osaka_listen: port_of(&to_osaka, "listen"),
        osaka_hop,
    })
}

#[tokio::test]
async fn a_relay_switches_only_after_its_target_serves_the_new_listener() -> TestResult {
    let w = world().await?;
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
    w.nodes
        .process(ReplaceNodeSpec {
            actor: operator(),
            node: f.osaka_hop.node.id.clone(),
            spec: NodeSpec::Pod(PodConfig {
                server: match &f.osaka_hop.node.spec {
                    NodeSpec::Pod(cfg) => cfg.server.clone(),
                    other => panic!("expected a pod, got {other:?}"),
                },
                port: 9444,
                bind_ip: None,
                advertise_ip: None,
            }),
            item_count: 0,
        })
        .await?;
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

#[tokio::test]
async fn a_protocol_change_on_a_referenced_listener_is_rejected() -> TestResult {
    let w = world().await?;
    let f = relay_chain(&w).await?;
    settle(&w, &f).await?;

    // Feed osaka's pod from an Entry instead of the relay: same ip:port, but it
    // would become a raw listener while tokyo's running config still dials it as a
    // relay. The two cannot coexist on one worker, so there is no seamless path.
    let mut resp =
        w.db.db()
            .query("SELECT * FROM orchestration_edge_connection")
            .await?;
    let edge = resp
        .take::<Vec<orchestration::entities::surreal::connection::EdgeConnectionEntity>>(0)?
        .into_iter()
        .find(|e| e.source.0 == f.osaka_hop_listen.0 && e.target.0 == f.to_osaka_listen.0)
        .expect("the relay feeds the osaka pod");
    w.edges
        .process(Disconnect {
            actor: operator(),
            edge: edge.id,
        })
        .await?;
    let entry = w
        .nodes
        .process(CreateNode {
            actor: operator(),
            canvas: f.canvas.clone(),
            name: "osaka-entry".to_string(),
            comment: String::new(),
            spec: NodeSpec::Entry(EntryConfig {
                receive_proxy_protocol: None,
                tls: None,
            }),
            position: pos0(),
            item_count: 0,
        })
        .await?;

    let err = w
        .edges
        .process(Connect {
            actor: operator(),
            output_port: f.osaka_hop_listen.clone(),
            input_port: port_of(&entry, "listen"),
        })
        .await
        .expect_err("a protocol switch under a live dependant must be refused");
    match err {
        OrchestrationError::Conflict(message) => {
            assert!(
                message.contains("osaka:9443"),
                "the error names the listener by server and port: {message}"
            );
        }
        other => panic!("expected a conflict, got {other:?}"),
    }
    Ok(())
}

#[tokio::test]
async fn forgetting_what_a_server_runs_is_admin_only() -> TestResult {
    let w = world().await?;
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

#[tokio::test]
async fn a_worker_credential_cannot_edit_the_workspace() -> TestResult {
    let w = world().await?;
    let err = w
        .canvases
        .process(canvas_service::CreateCanvas {
            actor: machine(),
            name: "prod".to_string(),
            description: String::new(),
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
            log_level: "info".to_string(),
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
            last_update_error: None,
        })
        .await
        .expect_err("human sessions may not register workers");
    assert!(matches!(err, OrchestrationError::Core(_)), "{err:?}");
    Ok(())
}

/// A pod that stops deriving keeps carrying what it already serves: the listener
/// stays in `desired`, the reason lands in `invalid_pods`, and the server is not
/// failed as a whole.
#[tokio::test]
async fn a_pod_that_stops_deriving_keeps_serving_its_listener() -> TestResult {
    let w = world().await?;
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

    // Cut the relay off from the pod feeding its listen side. Tokyo's ingress pod
    // dials that relay, so the pod alone stops deriving.
    let mut resp =
        w.db.db()
            .query("SELECT * FROM orchestration_edge_connection")
            .await?;
    let edge = resp
        .take::<Vec<orchestration::entities::surreal::connection::EdgeConnectionEntity>>(0)?
        .into_iter()
        .find(|e| {
            [&e.source, &e.target].iter().any(|p| {
                orchestration::utils::ids::record_key(&p.0)
                    == orchestration::utils::ids::record_key(&f.to_osaka_listen.0)
            })
        })
        .ok_or("the relay listen edge must exist")?;
    w.edges
        .process(Disconnect {
            actor: operator(),
            edge: edge.id,
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
    assert_eq!(invalid.pod, "ingress");
    assert_eq!(invalid.listen, "[::]:443");
    assert_eq!(
        serves(&after, &after.desired),
        served,
        "the listener it already carries is held, not dropped"
    );
    Ok(())
}
