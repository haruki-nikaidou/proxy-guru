//! Transport contracts of the operator API that a client cannot infer from the
//! services alone.

#![allow(clippy::unwrap_used, clippy::panic, clippy::expect_used)]

mod common;

use base::services::config::ConfigStore;
use common::*;
use kanau::processor::Processor;
use orchestration::config::OrchestrationConfig;
use orchestration::rpc::OrchestrationGrpc;
use orchestration::services::config::OrchestrationConfigService;
use rpguru_sdk::orchestration as pb;
use rpguru_sdk::orchestration::orchestration_server::Orchestration;
use tonic::Request;

fn grpc(w: &World) -> OrchestrationGrpc {
    OrchestrationGrpc {
        canvases: w.canvases.clone(),
        servers: w.servers.clone(),
        nodes: w.nodes.clone(),
        edges: w.edges.clone(),
        rollout: w.rollout.clone(),
        health: w.health.clone(),
        dns: w.dns.clone(),
        certificates: w.certificates.clone(),
        configs: OrchestrationConfigService {
            configs: ConfigStore { db: w.db.clone() },
        },
        live: w.live.clone(),
        sessions: w.sessions.clone(),
    }
}

/// A request carrying both the injected identity and the session metadata a
/// live stream needs.
fn as_session<T>(message: T, token: &str) -> Request<T> {
    let mut request = as_operator(message);
    request.metadata_mut().insert(
        auth::rpc::middleware::SESSION_ID_METADATA,
        token.parse().expect("a session token is valid metadata"),
    );
    request
}

/// A request carrying the identity the auth middleware would have injected.
fn as_operator<T>(message: T) -> Request<T> {
    let mut request = Request::new(message);
    request.extensions_mut().insert(operator());
    request
}

async fn create_canvas(api: &OrchestrationGrpc, name: &str) -> pb::Canvas {
    api.create_canvas(as_operator(pb::CreateCanvasRequest {
        name: name.to_string(),
        description: String::new(),
    }))
    .await
    .unwrap()
    .into_inner()
    .canvas
    .unwrap()
}

/// `Node.import_target` is set on every reply that carries an import node, not
/// only on `GetCanvas`: a client must not need a second round trip to label the
/// node it just created or moved.
#[tokio::test]
async fn mutation_replies_carry_the_import_target() -> TestResult {
    let w = world().await?;
    let api = grpc(&w);
    let root = create_canvas(&api, "root").await;
    let sub = create_canvas(&api, "sub").await;

    let created = api
        .create_node(as_operator(pb::CreateNodeRequest {
            canvas_id: root.id.clone(),
            name: "sub".to_string(),
            comment: String::new(),
            spec: Some(pb::NodeSpec {
                spec: Some(pb::node_spec::Spec::CanvasImport(pb::CanvasImportConfig {
                    canvas_id: sub.id.clone(),
                })),
            }),
            position: None,
            item_count: 0,
        }))
        .await?
        .into_inner()
        .node
        .unwrap();
    let target = created
        .import_target
        .expect("CreateNode reply names the target");
    assert_eq!(
        (target.id.as_str(), target.name.as_str()),
        (sub.id.as_str(), "sub")
    );

    let moved = api
        .update_node_meta(as_operator(pb::UpdateNodeMetaRequest {
            node_id: created.id.clone(),
            name: "sub".to_string(),
            comment: "moved".to_string(),
            position: Some(pb::CanvasUiPosition { x: 5, y: 5 }),
        }))
        .await?
        .into_inner()
        .node
        .unwrap();
    let target = moved
        .import_target
        .expect("UpdateNodeMeta reply names the target");
    assert_eq!(target.id, sub.id);

    // A non-import node never carries one.
    let exit = api
        .create_node(as_operator(pb::CreateNodeRequest {
            canvas_id: root.id.clone(),
            name: "exit".to_string(),
            comment: String::new(),
            spec: Some(pb::NodeSpec {
                spec: Some(pb::node_spec::Spec::Exit(pb::ExitConfig {
                    destination: "10.0.0.5:8080".to_string(),
                    pass_proxy_protocol: pb::ProxyProtocolVersion::Unspecified.into(),
                })),
            }),
            position: None,
            item_count: 0,
        }))
        .await?
        .into_inner()
        .node
        .unwrap();
    assert!(exit.import_target.is_none());
    Ok(())
}

/// `ConnectPorts` takes a universal handle in place of a port id; the reply's
/// edge starts on the port the handle created.
#[tokio::test]
async fn connect_ports_accepts_universal_handles() -> TestResult {
    let w = world().await?;
    let api = grpc(&w);
    let root = create_canvas(&api, "root").await;
    let server = api
        .create_server(as_operator(pb::CreateServerRequest {
            canvas_id: root.id.clone(),
            name: "us".to_string(),
            icon: String::new(),
            comment: String::new(),
            position: None,
            ipv6_resolve: pb::Ipv6Resolve::Ipv6Tolerated.into(),
            log_level: "info".to_string(),
            override_v4: "198.51.100.1".to_string(),
            override_v6: String::new(),
            extra_addresses: Vec::new(),
        }))
        .await?
        .into_inner()
        .server
        .unwrap();
    let create = |name: &str, spec: pb::node_spec::Spec| {
        as_operator(pb::CreateNodeRequest {
            canvas_id: root.id.clone(),
            name: name.to_string(),
            comment: String::new(),
            spec: Some(pb::NodeSpec { spec: Some(spec) }),
            position: None,
            item_count: 0,
        })
    };
    let pod = api
        .create_node(create(
            "p0",
            pb::node_spec::Spec::Pod(pb::PodConfig {
                port: 10000,
                server_id: server.id.clone(),
                bind_ip: String::new(),
                advertise_ip: String::new(),
            }),
        ))
        .await?
        .into_inner()
        .node
        .unwrap();
    let ud = api
        .create_node(create(
            "fan",
            pb::node_spec::Spec::LoadBalanceDistribute(pb::LoadBalanceDistributeConfig {
                mode: pb::LoadBalanceMode::RoundRobin.into(),
                protocol: pb::RelayProtocol::RelayTcpRaw.into(),
                members: vec![pb::LoadBalanceMember {
                    slot: 1,
                    name: "hk-1".to_string(),
                }],
            }),
        ))
        .await?
        .into_inner()
        .node
        .unwrap();
    assert_eq!(
        ud.ports.len(),
        1,
        "one bundle port per member: {:?}",
        ud.ports
    );
    assert_eq!(ud.ports[0].key, "member_1");
    assert_eq!(ud.ports[0].kind, i32::from(pb::PortKind::Bundle));
    let destination = pod.ports.iter().find(|p| p.key == "destination").unwrap();
    let edge = api
        .connect_ports(as_operator(pb::ConnectRequest {
            output_port_id: String::new(),
            input_port_id: destination.id.clone(),
            output_handle: Some(pb::UniversalHandle {
                node_id: ud.id.clone(),
                group: pb::UniversalGroup::ChannelOut.into(),
            }),
            input_handle: None,
        }))
        .await?
        .into_inner()
        .edge
        .unwrap();
    assert_eq!(edge.target_port_id, destination.id);
    let canvas = api
        .get_canvas(as_operator(pb::GetCanvasRequest {
            canvas_id: root.id.clone(),
        }))
        .await?
        .into_inner();
    let ud_now = canvas.nodes.iter().find(|n| n.id == ud.id).unwrap();
    let chan = ud_now
        .ports
        .iter()
        .find(|p| p.id == edge.source_port_id)
        .expect("the edge starts on the created channel port");
    assert_eq!(chan.key, format!("chan:{}", pod.id));
    assert_eq!(chan.kind, i32::from(pb::PortKind::DeriveDestination));
    // The universal pod the server came with is reported with its fixed port.
    let up = canvas
        .nodes
        .iter()
        .find(|n| {
            matches!(
                &n.spec,
                Some(pb::NodeSpec {
                    spec: Some(pb::node_spec::Spec::UniversalPod(_))
                })
            )
        })
        .expect("the server's universal pod");
    assert_eq!(up.ports.len(), 1);
    assert_eq!(up.ports[0].kind, i32::from(pb::PortKind::Bundle));
    assert!(up.lane.is_none());

    // A missing end is an argument error, not a crash.
    let err = api
        .connect_ports(as_operator(pb::ConnectRequest {
            output_port_id: String::new(),
            input_port_id: destination.id.clone(),
            output_handle: None,
            input_handle: None,
        }))
        .await
        .expect_err("no port and no handle");
    assert_eq!(err.code(), tonic::Code::InvalidArgument);
    Ok(())
}

// --- live streams ------------------------------------------------------------

/// Pulls the next stream item, or `None` when nothing arrives in time.
async fn next_item<T>(
    stream: &mut (impl tokio_stream::Stream<Item = Result<T, tonic::Status>> + Unpin),
    within: std::time::Duration,
) -> Option<Result<T, tonic::Status>> {
    tokio::time::timeout(within, tokio_stream::StreamExt::next(stream))
        .await
        .ok()
        .flatten()
}

/// The three transport contracts of a live stream: it needs a session, it
/// keeps itself alive, and it ends the moment that session does.
#[tokio::test]
async fn watch_canvas_stream_keepalive_and_session_cut() -> TestResult {
    let w = world_with(OrchestrationConfig {
        stream_keepalive_secs: 1,
        ..OrchestrationConfig::default()
    })
    .await?;
    let api = grpc(&w);
    let canvas = create_canvas(&api, "prod").await;
    let token = w.login().await?;

    // No session metadata: refused at open, before any view is spawned.
    let err = api
        .watch_canvas(as_operator(pb::WatchCanvasRequest {
            canvas_id: canvas.id.clone(),
        }))
        .await
        .expect_err("a stream without a session");
    assert_eq!(err.code(), tonic::Code::Unauthenticated);

    let mut stream = api
        .watch_canvas(as_session(
            pb::WatchCanvasRequest {
                canvas_id: canvas.id.clone(),
            },
            &token,
        ))
        .await?
        .into_inner();

    let first = next_item(&mut stream, std::time::Duration::from_secs(5))
        .await
        .expect("an opening item")?;
    assert!(
        matches!(first.event, Some(pb::canvas_event::Event::Snapshot(_))),
        "a stream opens with a snapshot"
    );

    let idle = next_item(&mut stream, std::time::Duration::from_millis(2500))
        .await
        .expect("a keep-alive on an idle stream")?;
    assert!(
        matches!(idle.event, Some(pb::canvas_event::Event::KeepAlive(_))),
        "an idle stream sends keep-alives"
    );

    w.sessions
        .process(auth::services::session::Logout {
            session_id: token.clone(),
        })
        .await?;
    // The next tick re-validates and finds nothing.
    let cut = loop {
        match next_item(&mut stream, std::time::Duration::from_secs(5))
            .await
            .expect("the stream reacts to the logout")
        {
            Ok(_) => continue,
            Err(status) => break status,
        }
    };
    assert_eq!(cut.code(), tonic::Code::Unauthenticated);
    assert!(
        next_item(&mut stream, std::time::Duration::from_millis(500))
            .await
            .is_none(),
        "the stream is over"
    );
    Ok(())
}

/// A watcher that stops reading is not owed a backlog: the bounded channel plus
/// the coalescing view mean it receives fewer, newer snapshots — and the last
/// one it can read is the current state.
#[tokio::test]
async fn paused_client_gets_newest_not_backlog() -> TestResult {
    let w = world().await?;
    let api = grpc(&w);
    let canvas = create_canvas(&api, "prod").await;
    let token = w.login().await?;
    let server = api
        .create_server(as_operator(pb::CreateServerRequest {
            canvas_id: canvas.id.clone(),
            name: "tokyo".to_string(),
            icon: String::new(),
            comment: String::new(),
            position: None,
            ipv6_resolve: i32::from(pb::Ipv6Resolve::Ipv6Tolerated),
            log_level: "info".to_string(),
            override_v4: String::new(),
            override_v6: String::new(),
            extra_addresses: Vec::new(),
        }))
        .await?
        .into_inner()
        .server
        .expect("a server");

    let mut stream = api
        .watch_canvas(as_session(
            pb::WatchCanvasRequest {
                canvas_id: canvas.id.clone(),
            },
            &token,
        ))
        .await?
        .into_inner();

    const MOVES: i64 = 40;
    for step in 1..=MOVES {
        api.move_server(as_operator(pb::MoveServerRequest {
            server_id: server.id.clone(),
            position: Some(pb::CanvasUiPosition { x: step, y: step }),
        }))
        .await?;
    }

    // Drain everything that is ready without waiting for more.
    let mut snapshots = Vec::new();
    while let Some(item) = next_item(&mut stream, std::time::Duration::from_millis(400)).await {
        if let Some(pb::canvas_event::Event::Snapshot(snapshot)) = item?.event {
            snapshots.push(snapshot);
        }
    }
    assert!(
        snapshots.len() < MOVES as usize,
        "{} snapshots for {MOVES} moves: nothing coalesced",
        snapshots.len()
    );
    let last = snapshots.last().expect("at least the opening snapshot");
    let position = last
        .contents
        .as_ref()
        .expect("a snapshot carries the contents")
        .servers
        .iter()
        .find(|s| s.id == server.id)
        .expect("the server is still there")
        .position
        .expect("a server has a position");
    assert_eq!(
        (position.x, position.y),
        (MOVES, MOVES),
        "the last snapshot carries the final position"
    );
    Ok(())
}

/// The rollout snapshot covers the whole tree, not just the canvas asked about.
#[tokio::test]
async fn watch_rollouts_covers_the_whole_tree() -> TestResult {
    let w = world().await?;
    let api = grpc(&w);
    let root = create_canvas(&api, "root").await;
    let sub = create_canvas(&api, "sub").await;
    api.create_node(as_operator(pb::CreateNodeRequest {
        canvas_id: root.id.clone(),
        name: "sub".to_string(),
        comment: String::new(),
        spec: Some(pb::NodeSpec {
            spec: Some(pb::node_spec::Spec::CanvasImport(pb::CanvasImportConfig {
                canvas_id: sub.id.clone(),
            })),
        }),
        position: None,
        item_count: 0,
    }))
    .await?;
    for (canvas, name, address) in [
        (&root.id, "tokyo", "203.0.113.10"),
        (&sub.id, "osaka", "203.0.113.11"),
    ] {
        api.create_server(as_operator(pb::CreateServerRequest {
            canvas_id: canvas.clone(),
            name: name.to_string(),
            icon: String::new(),
            comment: String::new(),
            position: None,
            ipv6_resolve: i32::from(pb::Ipv6Resolve::Ipv6Tolerated),
            log_level: "info".to_string(),
            override_v4: address.to_string(),
            override_v6: String::new(),
            extra_addresses: Vec::new(),
        }))
        .await?;
    }
    let token = w.login().await?;

    // Asked about the subcanvas; the answer covers the tree it belongs to.
    let mut stream = api
        .watch_rollouts(as_session(
            pb::WatchRolloutsRequest {
                canvas_id: sub.id.clone(),
            },
            &token,
        ))
        .await?
        .into_inner();
    let first = next_item(&mut stream, std::time::Duration::from_secs(5))
        .await
        .expect("an opening item")?;
    let Some(pb::rollout_event::Event::Snapshot(snapshot)) = first.event else {
        panic!("a stream opens with a snapshot, got {first:?}");
    };
    let mut canvases: Vec<_> = snapshot
        .servers
        .iter()
        .map(|s| s.canvas_id.clone())
        .collect();
    canvases.sort();
    let mut expected = vec![root.id.clone(), sub.id.clone()];
    expected.sort();
    assert_eq!(canvases, expected, "every server of the tree is listed");
    Ok(())
}

/// An unknown canvas is a `NOT_FOUND` on the stream, not a silent wait.
#[tokio::test]
async fn watch_canvas_reports_a_missing_canvas() -> TestResult {
    let w = world().await?;
    let api = grpc(&w);
    let token = w.login().await?;
    let mut stream = api
        .watch_canvas(as_session(
            pb::WatchCanvasRequest {
                canvas_id: "nope".to_string(),
            },
            &token,
        ))
        .await?
        .into_inner();
    let status = next_item(&mut stream, std::time::Duration::from_secs(5))
        .await
        .expect("an item")
        .expect_err("a missing canvas");
    assert_eq!(status.code(), tonic::Code::NotFound);
    Ok(())
}
