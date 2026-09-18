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
use orchestration::services::graph::GraphChange;
use rpguru_sdk::orchestration as pb;
use rpguru_sdk::orchestration::orchestration_server::Orchestration;
use tonic::Request;

fn grpc(w: &World) -> OrchestrationGrpc {
    OrchestrationGrpc {
        canvases: w.canvases.clone(),
        servers: w.servers.clone(),
        graph: w.graph.clone(),
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
    create_subcanvas(api, name, "").await
}

async fn create_subcanvas(api: &OrchestrationGrpc, name: &str, parent: &str) -> pb::Canvas {
    api.create_canvas(as_operator(pb::CreateCanvasRequest {
        name: name.to_string(),
        description: String::new(),
        parent_id: parent.to_string(),
        position: None,
    }))
    .await
    .unwrap()
    .into_inner()
    .canvas
    .unwrap()
}

async fn create_server(
    api: &OrchestrationGrpc,
    canvas: &str,
    name: &str,
    address: &str,
) -> pb::Server {
    api.create_server(as_operator(pb::CreateServerRequest {
        canvas_id: canvas.to_string(),
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
    .await
    .unwrap()
    .into_inner()
    .server
    .unwrap()
}

fn json(text: &str) -> serde_json::Value {
    serde_json::from_str(text).unwrap()
}

/// A whole graph crosses the wire and comes back as it was sent: the ids the
/// client chose, the route and group documents, edge overrides and IP
/// families (an unspecified one comes back as what it means, auto), and the
/// port picked for a pod put with port 0. A dry run writes nothing.
#[sqlx::test(migrator = "base::db::MIGRATOR")]
async fn a_graph_round_trips_through_the_wire(pool: sqlx::PgPool) -> TestResult {
    let w = world(pool).await?;
    let api = grpc(&w);
    let root = create_canvas(&api, "root").await;
    let sub = create_subcanvas(&api, "sub", &root.id).await;
    assert_eq!(sub.parent_id, root.id);
    let tokyo = create_server(&api, &root.id, "tokyo", "203.0.113.10").await;
    let osaka = create_server(&api, &root.id, "osaka", "198.51.100.10").await;

    let entry = pb::Pod {
        id: key("entry"),
        canvas_id: root.id.clone(),
        server_id: tokyo.id.clone(),
        name: "entry".to_string(),
        port: 443,
        ingress: pb::Ingress::ClientRaw.into(),
        receive_proxy_protocol: pb::ProxyProtocolVersion::ProxyV2.into(),
        route_json: format!(
            r#"{{"balance":[{{"weight":3,"to":{{"edge":"{}"}}}},{{"to":{{"edge":"{}"}}}}],"sticky":"client_ip"}}"#,
            key("near"),
            key("far")
        ),
        ..Default::default()
    };
    let hop = pb::Pod {
        id: key("hop"),
        canvas_id: sub.id.clone(),
        server_id: osaka.id.clone(),
        name: "hop".to_string(),
        port: 0,
        ingress: pb::Ingress::RelayQuic.into(),
        advertise_ip: "2001:DB8::10".to_string(),
        route_json: format!(r#"{{"edge":"{}"}}"#, key("out")),
        ..Default::default()
    };
    let origin = pb::Exit {
        id: key("origin"),
        canvas_id: sub.id.clone(),
        name: "origin".to_string(),
        destination: "10.0.0.5:8080".to_string(),
        send_proxy_protocol: pb::ProxyProtocolVersion::ProxyV1.into(),
        position: Some(pb::CanvasUiPosition { x: 5, y: -7 }),
        ..Default::default()
    };
    let edge = |name: &str, source: &str, target: pb::edge::Target| pb::Edge {
        id: key(name),
        source_pod_id: key(source),
        target: Some(target),
        ..Default::default()
    };
    let mut near = edge("near", "entry", pb::edge::Target::TargetPodId(key("hop")));
    near.override_ip = "hop.example.net".to_string();
    near.override_port = 19443;
    let mut far = edge("far", "entry", pb::edge::Target::TargetPodId(key("hop")));
    far.ip_family = pb::IpFamily::V6.into();
    let out = edge("out", "hop", pb::edge::Target::TargetExitId(key("origin")));
    let group = pb::Group {
        id: key("splitter"),
        canvas_id: root.id.clone(),
        kind: "splitter".to_string(),
        name: "fan-out".to_string(),
        props_json: r#"{"x":1,"y":2}"#.to_string(),
        members: vec![
            pb::GroupMember {
                member: Some(pb::group_member::Member::PodId(key("entry"))),
            },
            pb::GroupMember {
                member: Some(pb::group_member::Member::EdgeId(key("near"))),
            },
        ],
    };
    let change = pb::GraphChange {
        put_pods: vec![entry.clone(), hop.clone()],
        put_exits: vec![origin.clone()],
        put_edges: vec![near.clone(), far.clone(), out.clone()],
        put_groups: vec![group.clone()],
        ..Default::default()
    };

    let dry = api
        .apply_graph(as_operator(pb::ApplyGraphRequest {
            canvas_id: sub.id.clone(),
            change: Some(change.clone()),
            dry_run: true,
            expected_generation: 0,
        }))
        .await?
        .into_inner();
    assert!(!dry.applied);
    assert!(
        dry.diagnostics.iter().all(|d| !d.error),
        "{:?}",
        dry.diagnostics
    );
    let empty = api
        .get_graph(as_operator(pb::GetGraphRequest {
            canvas_id: root.id.clone(),
        }))
        .await?
        .into_inner();
    assert!(empty.pods.is_empty(), "a dry run writes nothing");

    let applied = api
        .apply_graph(as_operator(pb::ApplyGraphRequest {
            canvas_id: sub.id.clone(),
            change: Some(change),
            dry_run: false,
            expected_generation: empty.generation,
        }))
        .await?
        .into_inner();
    assert!(applied.applied, "{:?}", applied.diagnostics);
    assert_eq!(applied.generation, empty.generation + 1);
    let picked = applied
        .pods
        .iter()
        .find(|p| p.id == key("hop"))
        .unwrap()
        .port;
    assert!((40000..=59999).contains(&picked), "{picked}");

    let graph = api
        .get_graph(as_operator(pb::GetGraphRequest {
            canvas_id: sub.id.clone(),
        }))
        .await?
        .into_inner();
    assert_eq!(graph.generation, applied.generation);
    assert_eq!(
        graph
            .canvases
            .iter()
            .map(|c| c.id.clone())
            .collect::<Vec<_>>(),
        vec![root.id.clone(), sub.id.clone()],
        "the whole tree, root first"
    );
    assert_eq!(graph.servers.len(), 2);
    let find_pod = |id: &str| graph.pods.iter().find(|p| p.id == id).unwrap().clone();
    let got_entry = find_pod(&key("entry"));
    assert_eq!(json(&got_entry.route_json), json(&entry.route_json));
    assert_eq!(
        pb::Pod {
            route_json: String::new(),
            ..got_entry
        },
        pb::Pod {
            route_json: String::new(),
            ..entry
        }
    );
    let got_hop = find_pod(&key("hop"));
    assert_eq!(got_hop.port, picked);
    assert_eq!(
        got_hop.advertise_ip, "2001:db8::10",
        "an IP is stored canonically"
    );
    assert_eq!(graph.exits, vec![origin]);
    let mut edges = graph.edges.clone();
    edges.sort_by(|a, b| a.id.cmp(&b.id));
    let mut sent = vec![near, far, out];
    for edge in &mut sent {
        if edge.ip_family == i32::from(pb::IpFamily::Unspecified) {
            edge.ip_family = pb::IpFamily::Auto.into();
        }
    }
    sent.sort_by(|a, b| a.id.cmp(&b.id));
    assert_eq!(edges, sent);
    assert_eq!(graph.groups.len(), 1);
    assert_eq!(json(&graph.groups[0].props_json), json(&group.props_json));
    assert_eq!(graph.groups[0].members, group.members);
    Ok(())
}

/// What a client can get wrong: a document that does not parse and an edge
/// without a target are argument errors; a graph that does not check is an
/// answer, with the diagnostics naming what is wrong, and nothing written.
#[sqlx::test(migrator = "base::db::MIGRATOR")]
async fn a_bad_graph_change_is_refused_with_what_is_wrong(pool: sqlx::PgPool) -> TestResult {
    let w = world(pool).await?;
    let api = grpc(&w);
    let root = create_canvas(&api, "root").await;
    let tokyo = create_server(&api, &root.id, "tokyo", "203.0.113.10").await;
    let pod = |name: &str, route_json: String| pb::Pod {
        id: key(name),
        canvas_id: root.id.clone(),
        server_id: tokyo.id.clone(),
        name: name.to_string(),
        port: 443,
        ingress: pb::Ingress::ClientRaw.into(),
        route_json,
        ..Default::default()
    };
    let apply = |change: pb::GraphChange| {
        api.apply_graph(as_operator(pb::ApplyGraphRequest {
            canvas_id: root.id.clone(),
            change: Some(change),
            dry_run: false,
            expected_generation: 0,
        }))
    };

    let err = apply(pb::GraphChange {
        put_pods: vec![pod("entry", "{\"edge\":".to_string())],
        ..Default::default()
    })
    .await
    .expect_err("a route that is not JSON");
    assert_eq!(err.code(), tonic::Code::InvalidArgument);

    let err = apply(pb::GraphChange {
        put_edges: vec![pb::Edge {
            id: key("lost"),
            source_pod_id: key("entry"),
            ..Default::default()
        }],
        ..Default::default()
    })
    .await
    .expect_err("an edge needs a target");
    assert_eq!(err.code(), tonic::Code::InvalidArgument);

    let err = apply(pb::GraphChange {
        put_edges: vec![pb::Edge {
            id: key("odd"),
            source_pod_id: key("entry"),
            target: Some(pb::edge::Target::TargetExitId(key("origin"))),
            ip_family: 99,
            ..Default::default()
        }],
        ..Default::default()
    })
    .await
    .expect_err("an IP family nobody defined");
    assert_eq!(err.code(), tonic::Code::InvalidArgument);

    // A route that names an edge the pod does not have, and an id that is not a
    // record key.
    let refused = apply(pb::GraphChange {
        put_pods: vec![
            pod("entry", format!(r#"{{"edge":"{}"}}"#, key("ghost"))),
            pb::Pod {
                id: "NOT-A-KEY".to_string(),
                ..pod("other", String::new())
            },
        ],
        ..Default::default()
    })
    .await?
    .into_inner();
    assert!(!refused.applied);
    let problems: Vec<&str> = refused
        .diagnostics
        .iter()
        .filter(|d| d.error)
        .map(|d| d.problem.as_str())
        .collect();
    assert!(problems.contains(&"invalid_id"), "{problems:?}");
    assert!(problems.contains(&"route_unknown_edge"), "{problems:?}");
    let graph = api
        .get_graph(as_operator(pb::GetGraphRequest {
            canvas_id: root.id.clone(),
        }))
        .await?
        .into_inner();
    assert!(
        graph.pods.is_empty(),
        "nothing of a refused batch is written"
    );

    // A change computed against a generation the tree has moved past.
    let err = api
        .apply_graph(as_operator(pb::ApplyGraphRequest {
            canvas_id: root.id.clone(),
            change: Some(pb::GraphChange::default()),
            dry_run: false,
            expected_generation: graph.generation + 7,
        }))
        .await
        .expect_err("a stale generation");
    assert_eq!(err.code(), tonic::Code::FailedPrecondition);
    Ok(())
}

/// A server logs at one of five levels: anything else is an argument error,
/// while case and surrounding blanks are forgiven the way `tracing` forgives them.
#[sqlx::test(migrator = "base::db::MIGRATOR")]
async fn a_server_log_level_is_one_of_five(pool: sqlx::PgPool) -> TestResult {
    let w = world(pool).await?;
    let api = grpc(&w);
    let root = create_canvas(&api, "root").await;
    let create = |log_level: &str| {
        as_operator(pb::CreateServerRequest {
            canvas_id: root.id.clone(),
            name: "us".to_string(),
            icon: String::new(),
            comment: String::new(),
            position: None,
            ipv6_resolve: pb::Ipv6Resolve::Ipv6Tolerated.into(),
            log_level: log_level.to_string(),
            override_v4: "198.51.100.1".to_string(),
            override_v6: String::new(),
            extra_addresses: Vec::new(),
        })
    };
    for level in ["verbose", "warning", "", "info,guru_worker=debug"] {
        let err = api
            .create_server(create(level))
            .await
            .expect_err("not a level");
        assert_eq!(err.code(), tonic::Code::InvalidArgument, "{level:?}");
    }
    let server = api
        .create_server(create(" DEBUG "))
        .await?
        .into_inner()
        .server
        .unwrap();
    assert_eq!(server.log_level, "debug");

    let err = api
        .update_server(as_operator(pb::UpdateServerRequest {
            server_id: server.id.clone(),
            name: server.name.clone(),
            log_level: "loud".to_string(),
            ..Default::default()
        }))
        .await
        .expect_err("not a level");
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
#[sqlx::test(migrator = "base::db::MIGRATOR")]
async fn watch_rollouts_stream_keepalive_and_session_cut(pool: sqlx::PgPool) -> TestResult {
    let w = world_with(
        pool,
        OrchestrationConfig {
            stream_keepalive_secs: 1,
            ..OrchestrationConfig::default()
        },
    )
    .await?;
    let api = grpc(&w);
    let canvas = create_canvas(&api, "prod").await;
    let token = w.login().await?;

    // No session metadata: refused at open, before any view is spawned.
    let err = api
        .watch_rollouts(as_operator(pb::WatchRolloutsRequest {
            canvas_id: canvas.id.clone(),
        }))
        .await
        .expect_err("a stream without a session");
    assert_eq!(err.code(), tonic::Code::Unauthenticated);

    let mut stream = api
        .watch_rollouts(as_session(
            pb::WatchRolloutsRequest {
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
        matches!(first.event, Some(pb::rollout_event::Event::Snapshot(_))),
        "a stream opens with a snapshot"
    );

    let idle = next_item(&mut stream, std::time::Duration::from_millis(2500))
        .await
        .expect("a keep-alive on an idle stream")?;
    assert!(
        matches!(idle.event, Some(pb::rollout_event::Event::KeepAlive(_))),
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
#[sqlx::test(migrator = "base::db::MIGRATOR")]
async fn paused_client_gets_newest_not_backlog(pool: sqlx::PgPool) -> TestResult {
    let w = world(pool).await?;
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
        .watch_rollouts(as_session(
            pb::WatchRolloutsRequest {
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
        if let Some(pb::rollout_event::Event::Snapshot(snapshot)) = item?.event {
            snapshots.push(snapshot);
        }
    }
    assert!(
        snapshots.len() < MOVES as usize,
        "{} snapshots for {MOVES} moves: nothing coalesced",
        snapshots.len()
    );
    let last = snapshots.last().expect("at least the opening snapshot");
    assert!(
        last.servers.iter().any(|s| s.server_id == server.id),
        "the last snapshot carries the current state"
    );
    Ok(())
}

/// The rollout snapshot covers the whole tree, not just the canvas asked about.
#[sqlx::test(migrator = "base::db::MIGRATOR")]
async fn watch_rollouts_covers_the_whole_tree(pool: sqlx::PgPool) -> TestResult {
    let w = world(pool).await?;
    let api = grpc(&w);
    let root = create_canvas(&api, "root").await;
    let sub = create_subcanvas(&api, "sub", &root.id).await;
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
#[sqlx::test(migrator = "base::db::MIGRATOR")]
async fn watch_rollouts_reports_a_missing_canvas(pool: sqlx::PgPool) -> TestResult {
    let w = world(pool).await?;
    let api = grpc(&w);
    let token = w.login().await?;
    let mut stream = api
        .watch_rollouts(as_session(
            pb::WatchRolloutsRequest {
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

/// The graph stream opens with the tree and sends a new snapshot per edit;
/// an unknown canvas is a `NOT_FOUND`.
#[sqlx::test(migrator = "base::db::MIGRATOR")]
async fn watch_graph_snapshots_each_edit_and_reports_a_missing_canvas(
    pool: sqlx::PgPool,
) -> TestResult {
    let w = world(pool).await?;
    let api = grpc(&w);
    let token = w.login().await?;
    let canvas = create_canvas(&api, "prod").await;
    let mut stream = api
        .watch_graph(as_session(
            pb::WatchGraphRequest {
                canvas_id: canvas.id.clone(),
            },
            &token,
        ))
        .await?
        .into_inner();
    let first = next_item(&mut stream, std::time::Duration::from_secs(5))
        .await
        .expect("an opening item")?;
    let Some(pb::graph_event::Event::Snapshot(snapshot)) = first.event else {
        panic!("a stream opens with a snapshot, got {first:?}");
    };
    assert!(
        snapshot.servers.is_empty(),
        "an empty canvas has no servers"
    );
    assert_eq!(snapshot.canvases.len(), 1, "the tree is the one canvas");

    let server = create_server(&api, &canvas.id, "tokyo", "203.0.113.10").await;
    let next = next_item(&mut stream, std::time::Duration::from_secs(5))
        .await
        .expect("a snapshot follows the edit")?;
    let Some(pb::graph_event::Event::Snapshot(snapshot)) = next.event else {
        panic!("an edit sends a snapshot, got {next:?}");
    };
    assert_eq!(
        snapshot
            .servers
            .iter()
            .map(|s| s.id.as_str())
            .collect::<Vec<_>>(),
        vec![server.id.as_str()],
        "the new server is in the next snapshot"
    );

    let mut stream = api
        .watch_graph(as_session(
            pb::WatchGraphRequest {
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

/// A pod-health stream opens with the pod's history and then forwards each
/// deployment event; an unknown pod is a `NOT_FOUND` on the stream.
#[sqlx::test(migrator = "base::db::MIGRATOR")]
async fn watch_pod_health_opens_with_history_then_follows_deploys(
    pool: sqlx::PgPool,
) -> TestResult {
    let w = world(pool).await?;
    let api = grpc(&w);
    let token = w.login().await?;
    let canvas = canvas(&w.db, "prod").await?;
    let tokyo = server_at(&w.db, &canvas, "tokyo", "203.0.113.10").await?;
    let web = client(&canvas, &tokyo, "web", 443, None);
    let out = exit(&canvas, "web-out", "10.0.0.5:8080");
    let edge = edge_to_exit("web-edge", &web, &out);
    w.apply(
        &canvas,
        GraphChange {
            put_pods: vec![routed(web, via(&edge))],
            put_exits: vec![out],
            put_edges: vec![edge],
            ..GraphChange::default()
        },
    )
    .await?;

    let mut stream = api
        .watch_pod_health(as_session(
            pb::WatchPodHealthRequest {
                pod_id: key("web"),
                since: String::new(),
            },
            &token,
        ))
        .await?
        .into_inner();
    let first = next_item(&mut stream, std::time::Duration::from_secs(5))
        .await
        .expect("an opening item")?;
    let Some(pb::pod_health_event::Event::Snapshot(snapshot)) = first.event else {
        panic!("a stream opens with a snapshot, got {first:?}");
    };
    assert!(snapshot.records.is_empty(), "nothing deployed yet");

    w.derive(&canvas.id).await?;
    let next = next_item(&mut stream, std::time::Duration::from_secs(5))
        .await
        .expect("a record follows the derivation")?;
    let Some(pb::pod_health_event::Event::Record(record)) = next.event else {
        panic!("a derivation sends a record, got {next:?}");
    };
    assert_eq!(record.pod_id, key("web"));
    assert_eq!(record.status, pb::PodHealthStatus::PodDeploying as i32);

    let mut stream = api
        .watch_pod_health(as_session(
            pb::WatchPodHealthRequest {
                pod_id: "nope".to_string(),
                since: String::new(),
            },
            &token,
        ))
        .await?
        .into_inner();
    let status = next_item(&mut stream, std::time::Duration::from_secs(5))
        .await
        .expect("an item")
        .expect_err("a missing pod");
    assert_eq!(status.code(), tonic::Code::NotFound);
    assert!(
        next_item(&mut stream, std::time::Duration::from_millis(200))
            .await
            .is_none(),
        "the stream is over"
    );
    Ok(())
}
