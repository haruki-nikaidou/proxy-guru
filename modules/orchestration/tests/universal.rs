//! Universal nodes: the expansion of a distributor / universal pod / aggregator
//! picture into lanes, what survives which edit, and what the fabric derives
//! from it.

#![allow(clippy::unwrap_used, clippy::panic, clippy::expect_used)]

mod common;

use common::*;
use kanau::processor::Processor;
use orchestration::entities::surreal::canvas::{CanvasId, FindCanvasById};
use orchestration::entities::surreal::connection::EdgeConnectionEntity;
use orchestration::entities::surreal::node::{
    EntryConfig, ExitConfig, FindNodeWithPorts, LaneRole, LoadBalanceAggregateConfig,
    LoadBalanceDistributeConfig, LoadBalanceMode, NodeId, NodeSpec, NodeWithPorts, PodConfig,
    RelayProtocol,
};
use orchestration::entities::surreal::port::{PortDirection, PortEntity, PortId, PortKind};
use orchestration::entities::surreal::server::{FindServerById, ServerId, ServerIpv6Resolve};
use orchestration::entities::surreal::topology::{CanvasTopology, LoadCanvasTopology};
use orchestration::entities::surreal::view::{AckServerConfig, ListenProtocol, TakeInFlight};
use orchestration::services::agent::{RegisterCredential, RegisterWorker};
use orchestration::services::canvas::ValidateCanvas;
use orchestration::services::edge::{Connect, ConnectEnd, ConnectUniversal, Disconnect, UniversalGroup};
use orchestration::services::node::{CreateNode, ReplaceNodeSpec, RetireNode};
use orchestration::services::server::{AddressOverrides, CreateServer, DeleteServer};
use orchestration::services::topology::{ProblemKind, ProblemSeverity};
use orchestration::services::universal;
use orchestration::services::OrchestrationError;
use orchestration::utils::ids::record_key;
use std::collections::BTreeMap;

// --- fixture -----------------------------------------------------------------

/// The picture from the design: two entry pods on `us`, one distributor, two
/// transit servers, one aggregator, two exits.
struct Picture {
    canvas: CanvasId,
    us: ServerId,
    hk1: ServerId,
    hk2: ServerId,
    /// The universal pod of each transit server.
    up1: NodeWithPorts,
    up2: NodeWithPorts,
    ud: NodeWithPorts,
    ua: NodeWithPorts,
    p0: NodeWithPorts,
    p1: NodeWithPorts,
    e0: NodeWithPorts,
    e1: NodeWithPorts,
}

async fn create_server(w: &World, canvas: &CanvasId, name: &str, ip: &str) -> ServerId {
    w.servers
        .process(CreateServer {
            actor: operator(),
            canvas: canvas.clone(),
            name: name.to_string(),
            icon: String::new(),
            comment: String::new(),
            position: pos0(),
            ipv6_resolve: ServerIpv6Resolve::Tolerated,
            log_level: "info".to_string(),
            addresses: AddressOverrides::parse(ip, "", &[]).unwrap(),
        })
        .await
        .unwrap()
        .id
}

async fn create(w: &World, canvas: &CanvasId, name: &str, spec: NodeSpec) -> NodeWithPorts {
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
        .unwrap_or_else(|e| panic!("create {name}: {e}"))
}

async fn universal_pod_of(w: &World, canvas: &CanvasId, server: &ServerId) -> NodeWithPorts {
    let topology = topology(w, canvas).await;
    let key = record_key(&server.0);
    topology
        .nodes
        .into_iter()
        .find(|n| matches!(&n.node.spec, NodeSpec::UniversalPod(cfg) if record_key(&cfg.server.0) == key))
        .expect("every server has a universal pod")
}

async fn topology(w: &World, canvas: &CanvasId) -> CanvasTopology {
    w.db.process(LoadCanvasTopology {
        canvas: canvas.clone(),
    })
    .await
    .unwrap()
}

async fn connect(w: &World, output: &PortId, input: &PortId) -> EdgeConnectionEntity {
    w.edges
        .process(Connect {
            actor: operator(),
            output_port: output.clone(),
            input_port: input.clone(),
        })
        .await
        .unwrap_or_else(|e| panic!("connect: {e}"))
}

fn handle(node: &NodeWithPorts, group: UniversalGroup) -> ConnectEnd {
    ConnectEnd::Handle {
        node: node.node.id.clone(),
        group,
    }
}

async fn connect_universal(
    w: &World,
    output: ConnectEnd,
    input: ConnectEnd,
) -> Result<EdgeConnectionEntity, OrchestrationError> {
    w.edges
        .process(ConnectUniversal {
            actor: operator(),
            output,
            input,
        })
        .await
}

/// A bundle from the next free member of `from` (or a universal pod's
/// `bundle_out`) to `to`: onto its bundle handle, or onto the next free member
/// of an aggregate node.
async fn bundle(w: &World, from: &NodeWithPorts, to: &NodeWithPorts) -> EdgeConnectionEntity {
    let from = reload(w, &from.node.id).await;
    let to = reload(w, &to.node.id).await;
    let topology = topology(w, &from.node.canvas).await;
    let used = |port: &PortEntity| {
        let key = record_key(&port.id.0);
        topology
            .edges
            .iter()
            .any(|e| record_key(&e.source.0) == key || record_key(&e.target.0) == key)
    };
    let mut outs: Vec<&PortEntity> = from
        .ports
        .iter()
        .filter(|p| p.kind == PortKind::Bundle && p.direction == PortDirection::Output && !used(p))
        .collect();
    outs.sort_by_key(|p| p.position);
    let out = outs
        .first()
        .unwrap_or_else(|| panic!("{} has no free bundle output", from.node.name));
    let input = match &to.node.spec {
        NodeSpec::LoadBalanceAggregate(_) => {
            let mut ins: Vec<&PortEntity> = to
                .ports
                .iter()
                .filter(|p| universal::is_member_port(p) && p.direction == PortDirection::Input && !used(p))
                .collect();
            ins.sort_by_key(|p| p.position);
            ConnectEnd::Port(
                ins.first()
                    .unwrap_or_else(|| panic!("{} has no free member", to.node.name))
                    .id
                    .clone(),
            )
        }
        _ => handle(&to, UniversalGroup::BundleIn),
    };
    connect_universal(w, ConnectEnd::Port(out.id.clone()), input)
        .await
        .unwrap_or_else(|e| panic!("bundle {} -> {}: {e}", from.node.name, to.node.name))
}

fn pod(server: &ServerId, port: u16) -> NodeSpec {
    NodeSpec::Pod(PodConfig {
        server: server.clone(),
        port,
        bind_ip: None,
        advertise_ip: None,
    })
}

fn exit(destination: &str) -> NodeSpec {
    NodeSpec::Exit(ExitConfig {
        destination: destination.to_string(),
        pass_proxy_protocol: None,
    })
}

/// A distribute node with two members, as most pictures here fan out to two.
fn distributor(mode: LoadBalanceMode, protocol: RelayProtocol) -> NodeSpec {
    distributor_with(mode, protocol, &["m1", "m2"])
}

fn members(names: &[&str]) -> Vec<orchestration::entities::surreal::node::LoadBalanceMember> {
    names
        .iter()
        .enumerate()
        .map(|(i, name)| orchestration::entities::surreal::node::LoadBalanceMember {
            slot: u32::try_from(i + 1).unwrap(),
            name: (*name).to_string(),
        })
        .collect()
}

fn distributor_with(mode: LoadBalanceMode, protocol: RelayProtocol, names: &[&str]) -> NodeSpec {
    NodeSpec::LoadBalanceDistribute(LoadBalanceDistributeConfig {
        mode,
        protocol,
        members: members(names),
    })
}

fn aggregator() -> NodeSpec {
    aggregator_with(&["m1", "m2"])
}

fn aggregator_with(names: &[&str]) -> NodeSpec {
    NodeSpec::LoadBalanceAggregate(LoadBalanceAggregateConfig {
        members: members(names),
    })
}

async fn picture(w: &World) -> Picture {
    let canvas = w
        .canvases
        .process(orchestration::services::canvas::CreateCanvas {
            actor: operator(),
            name: "prod".to_string(),
            description: String::new(),
        })
        .await
        .unwrap()
        .id;
    let us = create_server(w, &canvas, "us", "198.51.100.1").await;
    let hk1 = create_server(w, &canvas, "hk1", "203.0.113.1").await;
    let hk2 = create_server(w, &canvas, "hk2", "203.0.113.2").await;
    let up1 = universal_pod_of(w, &canvas, &hk1).await;
    let up2 = universal_pod_of(w, &canvas, &hk2).await;

    let p0 = create(w, &canvas, "ingress-10000", pod(&us, 10000)).await;
    let p1 = create(w, &canvas, "ingress-10001", pod(&us, 10001)).await;
    for (i, p) in [&p0, &p1].into_iter().enumerate() {
        let entry = create(
            w,
            &canvas,
            &format!("entry-{i}"),
            NodeSpec::Entry(EntryConfig {
                receive_proxy_protocol: None,
                tls: None,
            }),
        )
        .await;
        connect(w, &port_of(p, "listen"), &port_of(&entry, "listen")).await;
    }
    let ud = create(
        w,
        &canvas,
        "fan",
        distributor(LoadBalanceMode::RoundRobin, RelayProtocol::TcpRaw),
    )
    .await;
    let ua = create(
        w,
        &canvas,
        "join",
        aggregator(),
    )
    .await;
    let e0 = create(w, &canvas, "exit-0", exit("10.0.0.5:8080")).await;
    let e1 = create(w, &canvas, "exit-1", exit("10.0.0.6:8080")).await;

    // Channels, then bundles, then exits: the order an operator would draw.
    for p in [&p0, &p1] {
        connect_universal(
            w,
            handle(&ud, UniversalGroup::ChannelOut),
            ConnectEnd::Port(port_of(p, "destination")),
        )
        .await
        .unwrap_or_else(|e| panic!("channel {}: {e}", p.node.name));
    }
    let ud = reload(w, &ud.node.id).await;
    bundle(w, &ud, &up1).await;
    bundle(w, &ud, &up2).await;
    let up1 = reload(w, &up1.node.id).await;
    let up2 = reload(w, &up2.node.id).await;
    bundle(w, &up1, &ua).await;
    bundle(w, &up2, &ua).await;
    let ua = reload(w, &ua.node.id).await;
    for (p, e) in [(&p0, &e0), (&p1, &e1)] {
        let chan = port_of(&ua, &universal::chan_key(&record_key(&p.node.id.0)));
        connect(w, &port_of(e, "destination"), &chan).await;
    }
    Picture {
        canvas,
        us,
        hk1,
        hk2,
        up1: reload(w, &up1.node.id).await,
        up2: reload(w, &up2.node.id).await,
        ud: reload(w, &ud.node.id).await,
        ua: reload(w, &ua.node.id).await,
        p0,
        p1,
        e0,
        e1,
    }
}

async fn reload(w: &World, node: &NodeId) -> NodeWithPorts {
    w.db.process(FindNodeWithPorts { id: node.clone() })
        .await
        .unwrap()
        .expect("node exists")
}

/// The lanes of a tree, by key.
async fn lanes(w: &World, canvas: &CanvasId) -> BTreeMap<String, NodeWithPorts> {
    topology(w, canvas)
        .await
        .nodes
        .into_iter()
        .filter_map(|n| n.node.lane.as_ref().map(|l| (l.key.clone(), n.clone())))
        .collect()
}

fn count(lanes: &BTreeMap<String, NodeWithPorts>, role: LaneRole) -> usize {
    lanes
        .values()
        .filter(|n| n.node.lane.as_ref().is_some_and(|l| l.role == role))
        .count()
}

fn landing_ports(lanes: &BTreeMap<String, NodeWithPorts>) -> BTreeMap<String, u16> {
    lanes
        .iter()
        .filter_map(|(key, n)| match &n.node.spec {
            NodeSpec::Pod(cfg) => Some((key.clone(), cfg.port)),
            _ => None,
        })
        .collect()
}

fn ids_of(lanes: &BTreeMap<String, NodeWithPorts>) -> BTreeMap<String, String> {
    lanes
        .iter()
        .map(|(k, n)| (k.clone(), record_key(&n.node.id.0)))
        .collect()
}

async fn problems(w: &World, canvas: &CanvasId) -> Vec<(ProblemSeverity, ProblemKind)> {
    w.canvases
        .process(ValidateCanvas {
            actor: operator(),
            canvas: canvas.clone(),
        })
        .await
        .unwrap()
        .into_iter()
        .map(|p| (p.severity, p.kind))
        .collect()
}

async fn assert_clean(w: &World, canvas: &CanvasId) {
    assert_clean_but(w, canvas, &[]).await;
}

/// A canvas with no problems beyond the address warning and `allowed`.
async fn assert_clean_but(w: &World, canvas: &CanvasId, allowed: &[ProblemKind]) {
    let found = problems(w, canvas).await;
    assert!(
        found.iter().all(|(s, k)| *s == ProblemSeverity::Warning
            && (*k == ProblemKind::ServerNoAddress || allowed.contains(k))),
        "expected a clean canvas, got {found:?}"
    );
}

fn edges_touching(topology: &CanvasTopology, node: &NodeWithPorts) -> Vec<EdgeConnectionEntity> {
    let ports: Vec<String> = node.ports.iter().map(|p| record_key(&p.id.0)).collect();
    topology
        .edges
        .iter()
        .filter(|e| ports.contains(&record_key(&e.source.0)) || ports.contains(&record_key(&e.target.0)))
        .cloned()
        .collect()
}

async fn disconnect(w: &World, edge: &EdgeConnectionEntity) -> Result<(), OrchestrationError> {
    w.edges
        .process(Disconnect {
            actor: operator(),
            edge: edge.id.clone(),
        })
        .await
}

// --- expansion ---------------------------------------------------------------

/// Two channels over two transit servers into one aggregator expand into: one
/// fan-out and two relays per channel on the distributor's side, one landing
/// pod per channel per transit server, and one join per channel at the
/// aggregator. The aggregator's channel ports carry the channels' ordinals.
#[tokio::test]
async fn the_picture_expands_into_lanes() -> TestResult {
    let w = world().await?;
    let p = picture(&w).await;
    let lanes = lanes(&w, &p.canvas).await;
    assert_eq!(count(&lanes, LaneRole::Distribute), 2, "{lanes:#?}");
    assert_eq!(count(&lanes, LaneRole::Relay), 4);
    assert_eq!(count(&lanes, LaneRole::Landing), 4);
    assert_eq!(count(&lanes, LaneRole::Aggregate), 2);
    assert_eq!(lanes.len(), 12);

    // Landing pods sit on the transit servers, two per server, distinct ports.
    let ports = landing_ports(&lanes);
    for server in [&p.hk1, &p.hk2] {
        let key = record_key(&server.0);
        let mine: Vec<u16> = lanes
            .values()
            .filter_map(|n| match &n.node.spec {
                NodeSpec::Pod(cfg) if record_key(&cfg.server.0) == key => Some(cfg.port),
                _ => None,
            })
            .collect();
        assert_eq!(mine.len(), 2);
        assert_ne!(mine[0], mine[1]);
        assert!(mine.iter().all(|port| (40000..=59999).contains(port)));
    }
    assert_eq!(ports.len(), 4);

    let p0 = record_key(&p.p0.node.id.0);
    let p1 = record_key(&p.p1.node.id.0);
    let ud_chan0 = p.ud.ports.iter().find(|x| x.key == universal::chan_key(&p0)).unwrap();
    let ud_chan1 = p.ud.ports.iter().find(|x| x.key == universal::chan_key(&p1)).unwrap();
    assert_eq!((ud_chan0.position, ud_chan1.position), (0, 1));
    let ua_chan0 = p.ua.ports.iter().find(|x| x.key == universal::chan_key(&p0)).unwrap();
    let ua_chan1 = p.ua.ports.iter().find(|x| x.key == universal::chan_key(&p1)).unwrap();
    assert_eq!((ua_chan0.position, ua_chan1.position), (0, 1));
    assert_eq!(
        p.ud.ports.len(),
        6,
        "chan/lane per channel plus the two members: {:?}",
        p.ud.ports.iter().map(|x| &x.key).collect::<Vec<_>>()
    );
    assert_eq!(p.up1.ports.len(), 2, "bundle_in plus fixed bundle_out");
    assert_eq!(p.ua.ports.len(), 6, "two members, chan/lane per channel");

    assert_clean(&w, &p.canvas).await;
    let topology = topology(&w, &p.canvas).await;
    assert!(!universal::is_stale(&topology), "a fresh write is its own expansion");
    Ok(())
}

/// Lanes are diffed by key: an edit elsewhere on the canvas, or cutting and
/// redrawing an exit, leaves every lane row and every landing port in place.
#[tokio::test]
async fn unrelated_edits_keep_lane_identity() -> TestResult {
    let w = world().await?;
    let p = picture(&w).await;
    let before = lanes(&w, &p.canvas).await;

    let topology = topology(&w, &p.canvas).await;
    let exit_edge = edges_touching(&topology, &p.e1).into_iter().next().unwrap();
    disconnect(&w, &exit_edge).await?;
    let ua = reload(&w, &p.ua.node.id).await;
    let chan = port_of(&ua, &universal::chan_key(&record_key(&p.p1.node.id.0)));
    connect(&w, &port_of(&p.e1, "destination"), &chan).await;

    let after = lanes(&w, &p.canvas).await;
    assert_eq!(ids_of(&before), ids_of(&after));
    assert_eq!(landing_ports(&before), landing_ports(&after));
    assert_clean(&w, &p.canvas).await;
    Ok(())
}

/// Changing the distributor's mode rewrites the fan-out lanes in place;
/// changing its protocol re-rolls every landing port (a listener cannot change
/// protocol) while the lane rows keep their identity.
#[tokio::test]
async fn distributor_edits_flow_into_the_lanes() -> TestResult {
    let w = world().await?;
    let p = picture(&w).await;
    let before = lanes(&w, &p.canvas).await;

    w.nodes
        .process(ReplaceNodeSpec {
            actor: operator(),
            node: p.ud.node.id.clone(),
            spec: distributor(LoadBalanceMode::Random, RelayProtocol::TcpRaw),
            item_count: 0,
        })
        .await?;
    let after = lanes(&w, &p.canvas).await;
    assert_eq!(ids_of(&before), ids_of(&after));
    assert_eq!(landing_ports(&before), landing_ports(&after));
    for lane in after.values() {
        if let NodeSpec::LoadBalanceDistribute(cfg) = &lane.node.spec {
            assert_eq!(cfg.mode, LoadBalanceMode::Random);
        }
    }

    w.nodes
        .process(ReplaceNodeSpec {
            actor: operator(),
            node: p.ud.node.id.clone(),
            spec: distributor(LoadBalanceMode::Random, RelayProtocol::Quic),
            item_count: 0,
        })
        .await?;
    let rerolled = lanes(&w, &p.canvas).await;
    assert_eq!(ids_of(&before), ids_of(&rerolled));
    for (key, port) in landing_ports(&before) {
        assert_ne!(landing_ports(&rerolled)[&key], port, "landing {key} kept its port");
    }
    for lane in rerolled.values() {
        if let NodeSpec::Relay(cfg) = &lane.node.spec {
            assert_eq!(cfg.protocol, RelayProtocol::Quic);
        }
    }
    let ud = reload(&w, &p.ud.node.id).await;
    assert_eq!(ud.ports.len(), 6, "a spec edit leaves the on-demand ports alone");
    assert_clean(&w, &p.canvas).await;
    Ok(())
}

/// A landing pod's port is the operator's to change; the reconciler keeps it.
#[tokio::test]
async fn a_landing_port_edit_survives_reconciliation() -> TestResult {
    let w = world().await?;
    let p = picture(&w).await;
    let lanes_before = lanes(&w, &p.canvas).await;
    let (key, landing) = lanes_before
        .iter()
        .find(|(_, n)| matches!(n.node.spec, NodeSpec::Pod(_)))
        .unwrap();
    let NodeSpec::Pod(cfg) = &landing.node.spec else {
        unreachable!()
    };
    w.nodes
        .process(ReplaceNodeSpec {
            actor: operator(),
            node: landing.node.id.clone(),
            spec: pod(&cfg.server, 45001),
            item_count: 0,
        })
        .await?;
    // Any universal edit reconciles; the edited port must come through it.
    w.nodes
        .process(ReplaceNodeSpec {
            actor: operator(),
            node: p.ud.node.id.clone(),
            spec: distributor(LoadBalanceMode::Fallback, RelayProtocol::TcpRaw),
            item_count: 0,
        })
        .await?;
    let after = lanes(&w, &p.canvas).await;
    assert_eq!(landing_ports(&after)[key], 45001);

    // Moving it to another server is not.
    let err = w
        .nodes
        .process(ReplaceNodeSpec {
            actor: operator(),
            node: landing.node.id.clone(),
            spec: pod(&p.us, 45002),
            item_count: 0,
        })
        .await
        .expect_err("a landing pod stays on its server");
    assert!(matches!(err, OrchestrationError::Conflict(_)), "{err}");
    Ok(())
}

/// Cutting the bundle to one transit server takes that server's landing pods and
/// relays away and collapses each fan-out into a direct edge; cutting a channel
/// takes every lane of that channel, and the aggregator's port for it, away.
#[tokio::test]
async fn disconnects_shrink_the_expansion() -> TestResult {
    let w = world().await?;
    let p = picture(&w).await;
    let topology = topology(&w, &p.canvas).await;
    let ud_to_hk2 = edges_touching(&topology, &p.ud)
        .into_iter()
        .find(|e| p.up2.ports.iter().any(|x| record_key(&x.id.0) == record_key(&e.target.0)))
        .expect("the bundle to hk2");
    disconnect(&w, &ud_to_hk2).await?;
    let lanes_now = lanes(&w, &p.canvas).await;
    assert_eq!(count(&lanes_now, LaneRole::Landing), 2);
    assert_eq!(count(&lanes_now, LaneRole::Relay), 2);
    assert_eq!(count(&lanes_now, LaneRole::Distribute), 0, "one target: direct");
    assert_eq!(count(&lanes_now, LaneRole::Aggregate), 0, "one feeder: direct");
    let hk2 = record_key(&p.hk2.0);
    assert!(lanes_now.values().all(|n| !matches!(&n.node.spec, NodeSpec::Pod(cfg) if record_key(&cfg.server.0) == hk2)));
    let ud = reload(&w, &p.ud.node.id).await;
    assert_eq!(ud.ports.len(), 6, "the member stays, unwired");
    let up2 = reload(&w, &p.up2.node.id).await;
    assert_eq!(up2.ports.len(), 1, "only the fixed bundle_out is left");
    assert_clean_but(&w, &p.canvas, &[ProblemKind::DistributeSingleMember]).await;

    let topology = self::topology(&w, &p.canvas).await;
    let chan0 = edges_touching(&topology, &p.p0)
        .into_iter()
        .find(|e| p.ud.ports.iter().any(|x| record_key(&x.id.0) == record_key(&e.source.0)))
        .expect("the channel edge of p0");
    disconnect(&w, &chan0).await?;
    let lanes_now = lanes(&w, &p.canvas).await;
    let p0 = record_key(&p.p0.node.id.0);
    assert!(lanes_now.values().all(|n| record_key(&n.node.lane.as_ref().unwrap().channel.0) != p0));
    assert_eq!(lanes_now.len(), 2, "landing + relay of the remaining channel");
    let ua = reload(&w, &p.ua.node.id).await;
    assert!(ua.ports.iter().all(|x| x.key != universal::chan_key(&p0)));
    let topology = self::topology(&w, &p.canvas).await;
    assert!(edges_touching(&topology, &p.e0).is_empty(), "exit-0 lost its edge with the channel");
    assert_clean_but(&w, &p.canvas, &[ProblemKind::DistributeSingleMember]).await;
    Ok(())
}

/// Retiring an entry pod that is a channel retires the channel.
#[tokio::test]
async fn retiring_a_channel_pod_retires_its_lanes() -> TestResult {
    let w = world().await?;
    let p = picture(&w).await;
    w.nodes
        .process(RetireNode {
            actor: operator(),
            node: p.p1.node.id.clone(),
        })
        .await?;
    let lanes_now = lanes(&w, &p.canvas).await;
    assert_eq!(lanes_now.len(), 6);
    let p1 = record_key(&p.p1.node.id.0);
    assert!(lanes_now.values().all(|n| record_key(&n.node.lane.as_ref().unwrap().channel.0) != p1));
    assert_clean(&w, &p.canvas).await;
    Ok(())
}

/// The generated side is not the operator's: lanes, wired universal nodes, a
/// universal pod and a server that still lands channels all refuse.
#[tokio::test]
async fn managed_things_refuse_manual_edits() -> TestResult {
    let w = world().await?;
    let p = picture(&w).await;
    let lanes_now = lanes(&w, &p.canvas).await;
    let lane = lanes_now.values().next().unwrap();
    let conflict = |r: Result<(), OrchestrationError>| {
        let err = r.expect_err("refused");
        assert!(matches!(err, OrchestrationError::Conflict(_)), "{err}");
    };
    conflict(
        w.nodes
            .process(RetireNode {
                actor: operator(),
                node: lane.node.id.clone(),
            })
            .await,
    );
    conflict(
        w.nodes
            .process(RetireNode {
                actor: operator(),
                node: p.ud.node.id.clone(),
            })
            .await,
    );
    conflict(
        w.nodes
            .process(RetireNode {
                actor: operator(),
                node: p.up1.node.id.clone(),
            })
            .await,
    );
    conflict(
        w.servers
            .process(DeleteServer {
                actor: operator(),
                server: p.hk1.clone(),
            })
            .await,
    );
    let topology = topology(&w, &p.canvas).await;
    let generated = edges_touching(&topology, lane).into_iter().next().unwrap();
    conflict(disconnect(&w, &generated).await);
    assert!(
        w.db.process(FindServerById { id: p.hk1.clone() })
            .await?
            .is_some()
    );
    Ok(())
}

/// A handle connect only accepts the two shapes it exists for.
#[tokio::test]
async fn handle_connects_are_checked() -> TestResult {
    let w = world().await?;
    let p = picture(&w).await;
    let invalid = |r: Result<EdgeConnectionEntity, OrchestrationError>| {
        let err = r.expect_err("refused");
        assert!(
            matches!(err, OrchestrationError::Invalid(_) | OrchestrationError::Conflict(_)),
            "{err}"
        );
    };
    // A channel must start at a pod's destination.
    invalid(
        connect_universal(
            &w,
            handle(&p.ud, UniversalGroup::ChannelOut),
            ConnectEnd::Port(port_of(&p.e0, "destination")),
        )
        .await,
    );
    // The same pod twice.
    invalid(
        connect_universal(
            &w,
            handle(&p.ud, UniversalGroup::ChannelOut),
            ConnectEnd::Port(port_of(&p.p0, "destination")),
        )
        .await,
    );
    // A wired member again, and a universal pod bundling out twice.
    let m1 = ConnectEnd::Port(port_of(&p.ud, "member_1"));
    invalid(connect_universal(&w, m1.clone(), handle(&p.up2, UniversalGroup::BundleIn)).await);
    let up1_out = ConnectEnd::Port(port_of(&p.up1, universal::BUNDLE_OUT));
    invalid(connect_universal(&w, up1_out.clone(), handle(&p.up2, UniversalGroup::BundleIn)).await);
    // A distributor straight into an aggregator, and onto a wired member.
    let hk3 = create_server(&w, &p.canvas, "hk3", "203.0.113.3").await;
    let up3 = universal_pod_of(&w, &p.canvas, &hk3).await;
    let ua_m1 = ConnectEnd::Port(port_of(&p.ua, "member_1"));
    invalid(connect_universal(&w, m1, ua_m1.clone()).await);
    invalid(connect_universal(&w, ConnectEnd::Port(port_of(&up3, universal::BUNDLE_OUT)), ua_m1).await);
    // A universal pod or a distribute node takes bundles on its handle only.
    invalid(connect_universal(&w, ConnectEnd::Port(port_of(&up3, universal::BUNDLE_OUT)), ConnectEnd::Port(port_of(&p.up1, &universal::bundle_in_key(&record_key(&p.ud.node.id.0))))).await);
    // A bundle port never joins a thin port.
    let err = w
        .edges
        .process(Connect {
            actor: operator(),
            output_port: port_of(&up3, universal::BUNDLE_OUT),
            input_port: port_of(&p.p0, "destination"),
        })
        .await
        .expect_err("bundle to thin");
    assert!(matches!(err, OrchestrationError::Invalid(_)), "{err}");
    Ok(())
}

/// A distributor with channels but no bundle, and a universal pod with a bundle
/// in but nothing out, are half-drawn: warnings, not errors, and no lanes past
/// the point where the drawing stops.
#[tokio::test]
async fn half_drawn_pictures_warn() -> TestResult {
    let w = world().await?;
    let canvas = w
        .canvases
        .process(orchestration::services::canvas::CreateCanvas {
            actor: operator(),
            name: "half".to_string(),
            description: String::new(),
        })
        .await?
        .id;
    let us = create_server(&w, &canvas, "us", "198.51.100.1").await;
    let hk = create_server(&w, &canvas, "hk", "203.0.113.1").await;
    let up = universal_pod_of(&w, &canvas, &hk).await;
    let p0 = create(&w, &canvas, "p0", pod(&us, 10000)).await;
    let ud = create(&w, &canvas, "fan", distributor(LoadBalanceMode::RoundRobin, RelayProtocol::TcpRaw)).await;
    connect_universal(&w, handle(&ud, UniversalGroup::ChannelOut), ConnectEnd::Port(port_of(&p0, "destination"))).await?;
    let found = problems(&w, &canvas).await;
    assert!(found.contains(&(ProblemSeverity::Warning, ProblemKind::ChannelNoTransit)), "{found:?}");
    assert!(lanes(&w, &canvas).await.is_empty());

    let ud = reload(&w, &ud.node.id).await;
    bundle(&w, &ud, &up).await;
    let found = problems(&w, &canvas).await;
    assert!(found.contains(&(ProblemSeverity::Warning, ProblemKind::ChannelNoExit)), "{found:?}");
    assert!(!found.contains(&(ProblemSeverity::Warning, ProblemKind::ChannelNoTransit)));
    assert!(found.iter().all(|(s, _)| *s == ProblemSeverity::Warning), "{found:?}");
    let lanes_now = lanes(&w, &canvas).await;
    assert_eq!(count(&lanes_now, LaneRole::Landing), 1);
    assert_eq!(count(&lanes_now, LaneRole::Relay), 1);
    assert!(lanes_now.values().all(|n| !matches!(n.node.spec, NodeSpec::UniversalPod(_))));
    Ok(())
}

/// A chain of universal pods (hk1 -> hk2 -> aggregator) relays through: the
/// second hop lands the channel again and dials back into the first.
#[tokio::test]
async fn universal_pods_chain() -> TestResult {
    let w = world().await?;
    let canvas = w
        .canvases
        .process(orchestration::services::canvas::CreateCanvas {
            actor: operator(),
            name: "chain".to_string(),
            description: String::new(),
        })
        .await?
        .id;
    let us = create_server(&w, &canvas, "us", "198.51.100.1").await;
    let hk1 = create_server(&w, &canvas, "hk1", "203.0.113.1").await;
    let hk2 = create_server(&w, &canvas, "hk2", "203.0.113.2").await;
    let up1 = universal_pod_of(&w, &canvas, &hk1).await;
    let up2 = universal_pod_of(&w, &canvas, &hk2).await;
    let p0 = create(&w, &canvas, "p0", pod(&us, 10000)).await;
    let ud = create(&w, &canvas, "fan", distributor(LoadBalanceMode::RoundRobin, RelayProtocol::TcpTls)).await;
    let ua = create(&w, &canvas, "join", aggregator()).await;
    connect_universal(&w, handle(&ud, UniversalGroup::ChannelOut), ConnectEnd::Port(port_of(&p0, "destination"))).await?;
    let ud = reload(&w, &ud.node.id).await;
    bundle(&w, &ud, &up1).await;
    let up1 = reload(&w, &up1.node.id).await;
    bundle(&w, &up1, &up2).await;
    let up2 = reload(&w, &up2.node.id).await;
    bundle(&w, &up2, &ua).await;
    let lanes_now = lanes(&w, &canvas).await;
    assert_eq!(count(&lanes_now, LaneRole::Landing), 2);
    assert_eq!(count(&lanes_now, LaneRole::Relay), 2);
    assert_eq!(count(&lanes_now, LaneRole::Distribute), 0);
    assert_eq!(count(&lanes_now, LaneRole::Aggregate), 0);
    let ua = reload(&w, &ua.node.id).await;
    assert!(ua.ports.iter().any(|x| x.key == universal::chan_key(&record_key(&p0.node.id.0))));
    // A cycle is refused outright.
    let up2 = reload(&w, &up2.node.id).await;
    let err = connect_universal(
        &w,
        ConnectEnd::Port(port_of(&up2, universal::BUNDLE_OUT)),
        handle(&up1, UniversalGroup::BundleIn),
    )
    .await
    .expect_err("hk2 already bundles out");
    assert!(matches!(err, OrchestrationError::Conflict(_)), "{err}");
    // `fan` has one member wired: the single-member warning is expected.
    assert_clean_but(&w, &canvas, &[ProblemKind::DistributeSingleMember]).await;
    Ok(())
}

// --- derivation ----------------------------------------------------------------

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
    let row = w.db.process(FindServerById { id: server.clone() }).await?.unwrap();
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

async fn settle(w: &World, canvas: &CanvasId, servers: &[&ServerId]) -> Result<(), Box<dyn std::error::Error>> {
    for _ in 0..8 {
        w.derive(canvas).await?;
        for server in servers {
            ack_current(w, server).await?;
        }
        let row = w.db.process(FindCanvasById { id: canvas.clone() }).await?.unwrap();
        let mut caught_up = row.generation == row.derived_generation;
        for server in servers {
            let view = w.view(server).await?;
            caught_up &= view.in_flight.is_none()
                && view.applied.as_ref().map(|s| s.revision) == view.desired.as_ref().map(|s| s.revision);
        }
        if caught_up {
            return Ok(());
        }
    }
    panic!("the fabric never settled")
}

/// What the workers get: the entry server load-balances each channel over both
/// transit servers' landing pods, and each transit server serves its two landing
/// pods straight to the exits.
#[tokio::test]
async fn the_picture_derives_the_flat_fabric() -> TestResult {
    let w = world().await?;
    let p = picture(&w).await;
    settle(&w, &p.canvas, &[&p.us, &p.hk1, &p.hk2]).await?;

    let us = w.view(&p.us).await?;
    let applied = us.applied.as_ref().expect("us converged");
    assert_eq!(applied.forwardings.len(), 2, "{}", applied.toml);
    assert!(us.invalid_pods.is_empty(), "{:?}", us.invalid_pods);
    let lanes_now = lanes(&w, &p.canvas).await;
    let landing = landing_ports(&lanes_now);
    for deps in &applied.forwardings {
        assert_eq!(deps.serves.protocol, ListenProtocol::Raw);
        let mut targets: Vec<(String, i64)> = deps
            .points_at
            .iter()
            .map(|cap| (cap.server_key(), cap.port))
            .collect();
        targets.sort();
        assert_eq!(targets.len(), 2, "each channel dials both transit servers");
        assert!(targets.iter().all(|(_, port)| landing.values().any(|l| i64::from(*l) == *port)));
        assert!(deps.points_at.iter().all(|cap| cap.protocol == ListenProtocol::RelayTcp));
    }
    let config = guru_worker_config::Config::from_toml_str(&applied.toml)?;
    for forwarding in &config.forwardings {
        assert!(
            matches!(&forwarding.to, guru_worker_config::ForwardingTo::LoadBalance(g) if g.members.len() == 2),
            "{}",
            applied.toml
        );
    }

    for server in [&p.hk1, &p.hk2] {
        let view = w.view(server).await?;
        let applied = view.applied.as_ref().expect("transit converged");
        assert_eq!(applied.forwardings.len(), 2, "{}", applied.toml);
        assert!(view.invalid_pods.is_empty(), "{:?}", view.invalid_pods);
        let config = guru_worker_config::Config::from_toml_str(&applied.toml)?;
        let mut exits: Vec<String> = config
            .forwardings
            .iter()
            .map(|f| match &f.to {
                guru_worker_config::ForwardingTo::Exit { destination, .. } => format!("{destination:?}"),
                other => panic!("landing pod should exit, got {other:?}"),
            })
            .collect();
        exits.sort();
        assert_eq!(exits.len(), 2);
        assert!(exits[0].contains("10.0.0.5") && exits[1].contains("10.0.0.6"), "{exits:?}");
        assert!(applied.forwardings.iter().all(|d| d.serves.protocol == ListenProtocol::RelayTcp));
    }
    Ok(())
}

// --- members are the operator's rule ------------------------------------------

/// A distribute node's members are what the operator declares: renaming one
/// keeps its port, its bundle and every lane; adding one adds an empty bundle
/// port and changes nothing else; removing a wired one is refused; the counted
/// layout and a bad list are refused outright. The aggregate node's members
/// behave the same.
#[tokio::test]
async fn members_are_the_operators_rule() -> TestResult {
    let w = world().await?;
    let p = picture(&w).await;
    let before = lanes(&w, &p.canvas).await;
    let m1 = port_of(&p.ud, "member_1");
    let replace = |names: Vec<(u32, &str)>| ReplaceNodeSpec {
        actor: operator(),
        node: p.ud.node.id.clone(),
        spec: NodeSpec::LoadBalanceDistribute(LoadBalanceDistributeConfig {
            mode: LoadBalanceMode::RoundRobin,
            protocol: RelayProtocol::TcpRaw,
            members: names
                .into_iter()
                .map(|(slot, name)| orchestration::entities::surreal::node::LoadBalanceMember {
                    slot,
                    name: name.to_string(),
                })
                .collect(),
        }),
        item_count: 0,
    };

    // Rename, and reorder: the ports keep their ids, the bundles stay, the
    // lanes are untouched.
    let renamed = w.nodes.process(replace(vec![(2, "hk2"), (1, "hk-114")])).await?;
    assert_eq!(port_of(&renamed, "member_1"), m1);
    let positions: Vec<(String, i64)> = {
        let mut m: Vec<_> = renamed
            .ports
            .iter()
            .filter(|x| universal::is_member_port(x))
            .map(|x| (x.key.clone(), x.position))
            .collect();
        m.sort();
        m
    };
    assert_eq!(positions, [("member_1".to_string(), 1), ("member_2".to_string(), 0)]);
    assert_eq!(ids_of(&before), ids_of(&lanes(&w, &p.canvas).await));
    assert_eq!(landing_ports(&before), landing_ports(&lanes(&w, &p.canvas).await));
    assert_clean(&w, &p.canvas).await;

    // A third member: one more bundle port, nothing bundled yet.
    let grown = w.nodes.process(replace(vec![(1, "hk-114"), (2, "hk2"), (3, "hk3")])).await?;
    assert_eq!(grown.ports.len(), 7, "{:?}", grown.ports.iter().map(|x| &x.key).collect::<Vec<_>>());
    assert_eq!(ids_of(&before), ids_of(&lanes(&w, &p.canvas).await));
    let hk3 = create_server(&w, &p.canvas, "hk3", "203.0.113.3").await;
    let up3 = universal_pod_of(&w, &p.canvas, &hk3).await;
    bundle(&w, &grown, &up3).await;
    let lanes_now = lanes(&w, &p.canvas).await;
    assert_eq!(count(&lanes_now, LaneRole::Landing), 6, "two channels on three servers");
    for lane in lanes_now.values() {
        if let NodeSpec::LoadBalanceDistribute(cfg) = &lane.node.spec {
            assert_eq!(cfg.members.len(), 0, "lanes are laid out thin");
            assert_eq!(lane.ports.iter().filter(|x| x.key.starts_with("member_")).count(), 3);
        }
    }

    // Removing a wired member is refused; the unwired one goes.
    let err = w.nodes.process(replace(vec![(1, "hk-114"), (3, "hk3")])).await.expect_err("hk2 is wired");
    assert!(matches!(err, OrchestrationError::Conflict(_)), "{err}");
    let topology_now = topology(&w, &p.canvas).await;
    let up3 = reload(&w, &up3.node.id).await;
    let to_hk3 = edges_touching(&topology_now, &up3).into_iter().next().expect("bundle to hk3");
    disconnect(&w, &to_hk3).await?;
    let shrunk = w.nodes.process(replace(vec![(1, "hk-114"), (2, "hk2")])).await?;
    assert_eq!(shrunk.ports.len(), 6);
    assert_eq!(ids_of(&before), ids_of(&lanes(&w, &p.canvas).await));

    // Bad lists and the counted layout are refused.
    let invalid = |r: Result<NodeWithPorts, OrchestrationError>| {
        let err = r.expect_err("refused");
        assert!(matches!(err, OrchestrationError::Invalid(_)), "{err}");
    };
    invalid(w.nodes.process(replace(vec![])).await);
    invalid(w.nodes.process(replace(vec![(1, "same"), (2, "same")])).await);
    invalid(w.nodes.process(replace(vec![(1, "a"), (1, "b")])).await);
    invalid(w.nodes.process(replace(vec![(1, "  ")])).await);
    let mut counted = replace(vec![(1, "a"), (2, "b")]);
    counted.item_count = 2;
    invalid(w.nodes.process(counted).await);
    invalid(
        w.nodes
            .process(CreateNode {
                actor: operator(),
                canvas: p.canvas.clone(),
                name: "counted".to_string(),
                comment: String::new(),
                spec: distributor(LoadBalanceMode::RoundRobin, RelayProtocol::TcpRaw),
                position: pos0(),
                item_count: 2,
            })
            .await,
    );

    // The aggregate node: renaming keeps the exits' channel ports and the
    // bundles; a wired member cannot be dropped either.
    let ua_before = reload(&w, &p.ua.node.id).await;
    let ua = w
        .nodes
        .process(ReplaceNodeSpec {
            actor: operator(),
            node: p.ua.node.id.clone(),
            spec: aggregator_with(&["hk-114", "hk2"]),
            item_count: 0,
        })
        .await?;
    assert_eq!(ua.ports.len(), ua_before.ports.len());
    assert_eq!(port_of(&ua, "member_1"), port_of(&ua_before, "member_1"));
    assert_eq!(ids_of(&before), ids_of(&lanes(&w, &p.canvas).await));
    let err = w
        .nodes
        .process(ReplaceNodeSpec {
            actor: operator(),
            node: p.ua.node.id.clone(),
            spec: aggregator_with(&["hk-114"]),
            item_count: 0,
        })
        .await
        .expect_err("hk2's bundle lands on member 2");
    assert!(matches!(err, OrchestrationError::Conflict(_)), "{err}");
    assert_clean(&w, &p.canvas).await;
    Ok(())
}

// --- thin lines into universal pods and multi-tier fan-out ---------------------

/// An entry pod drawn straight into a server's universal pod is a raw TCP hop
/// of its own: one landing pod and one relay, no balancing, and the channel
/// then travels on like any bundled one.
#[tokio::test]
async fn a_thin_line_lands_a_channel_on_one_server() -> TestResult {
    let w = world().await?;
    let canvas = w
        .canvases
        .process(orchestration::services::canvas::CreateCanvas {
            actor: operator(),
            name: "thin".to_string(),
            description: String::new(),
        })
        .await?
        .id;
    let us = create_server(&w, &canvas, "us", "198.51.100.1").await;
    let hk = create_server(&w, &canvas, "hk", "203.0.113.1").await;
    let up = universal_pod_of(&w, &canvas, &hk).await;
    let p0 = create(&w, &canvas, "p0", pod(&us, 10000)).await;
    let entry = create(
        &w,
        &canvas,
        "entry",
        NodeSpec::Entry(EntryConfig {
            receive_proxy_protocol: None,
            tls: None,
        }),
    )
    .await;
    connect(&w, &port_of(&p0, "listen"), &port_of(&entry, "listen")).await;
    connect_universal(&w, handle(&up, UniversalGroup::ChannelOut), ConnectEnd::Port(port_of(&p0, "destination"))).await?;
    let lanes_now = lanes(&w, &canvas).await;
    assert_eq!(count(&lanes_now, LaneRole::Landing), 1, "{lanes_now:#?}");
    assert_eq!(count(&lanes_now, LaneRole::Relay), 1);
    assert_eq!(lanes_now.len(), 2);
    for lane in lanes_now.values() {
        if let NodeSpec::Relay(cfg) = &lane.node.spec {
            assert_eq!(cfg.protocol, RelayProtocol::TcpRaw);
        }
    }
    let up = reload(&w, &up.node.id).await;
    assert!(up.ports.iter().any(|x| x.key == universal::chan_key(&record_key(&p0.node.id.0))));
    let found = problems(&w, &canvas).await;
    assert!(found.contains(&(ProblemSeverity::Warning, ProblemKind::ChannelNoExit)), "{found:?}");

    // Bundle on to an aggregate node and give the channel an exit.
    let ua = create(&w, &canvas, "join", aggregator()).await;
    bundle(&w, &up, &ua).await;
    let ua = reload(&w, &ua.node.id).await;
    let exit0 = create(&w, &canvas, "exit", exit("10.0.0.5:8080")).await;
    connect(&w, &port_of(&exit0, "destination"), &port_of(&ua, &universal::chan_key(&record_key(&p0.node.id.0)))).await;
    assert_clean(&w, &canvas).await;

    settle(&w, &canvas, &[&us, &hk]).await?;
    let view = w.view(&us).await?;
    let applied = view.applied.as_ref().expect("us converged");
    assert_eq!(applied.forwardings.len(), 1, "{}", applied.toml);
    assert_eq!(applied.forwardings[0].points_at.len(), 1);
    assert_eq!(applied.forwardings[0].points_at[0].server_key(), record_key(&hk.0));
    assert_eq!(applied.forwardings[0].points_at[0].protocol, ListenProtocol::RelayTcp);
    let hk_view = w.view(&hk).await?;
    assert_eq!(hk_view.applied.as_ref().unwrap().forwardings.len(), 1);
    Ok(())
}

/// A distribute node bundled *into* fans everything it receives out again: the
/// second tier gets one fan-out per upstream server, and the aggregate node at
/// the end joins every landing pod of a channel.
#[tokio::test]
async fn a_second_tier_fans_out_per_upstream_server() -> TestResult {
    let w = world().await?;
    let p = picture(&w).await;
    // Cut the two first-tier servers loose from the aggregate node and route
    // them through a second distribute node onto two more servers.
    let topology_now = topology(&w, &p.canvas).await;
    for up in [&p.up1, &p.up2] {
        let out = edges_touching(&topology_now, up)
            .into_iter()
            .find(|e| p.ua.ports.iter().any(|x| record_key(&x.id.0) == record_key(&e.target.0)))
            .expect("bundle into the aggregate node");
        disconnect(&w, &out).await?;
    }
    let hk3 = create_server(&w, &p.canvas, "hk3", "203.0.113.3").await;
    let hk4 = create_server(&w, &p.canvas, "hk4", "203.0.113.4").await;
    let up3 = universal_pod_of(&w, &p.canvas, &hk3).await;
    let up4 = universal_pod_of(&w, &p.canvas, &hk4).await;
    // Raw TCP throughout: the in-memory world has no internal CA to sign relay
    // leaves with, and the protocol re-roll is covered elsewhere.
    let ud2 = create(&w, &p.canvas, "tier-2", distributor(LoadBalanceMode::RoundRobin, RelayProtocol::TcpRaw)).await;
    let up1 = reload(&w, &p.up1.node.id).await;
    let up2 = reload(&w, &p.up2.node.id).await;
    bundle(&w, &up1, &ud2).await;
    bundle(&w, &up2, &reload(&w, &ud2.node.id).await).await;
    let ud2 = reload(&w, &ud2.node.id).await;
    bundle(&w, &ud2, &up3).await;
    bundle(&w, &reload(&w, &ud2.node.id).await, &up4).await;
    let up3 = reload(&w, &up3.node.id).await;
    let up4 = reload(&w, &up4.node.id).await;
    bundle(&w, &up3, &p.ua).await;
    bundle(&w, &up4, &reload(&w, &p.ua.node.id).await).await;
    let ua = reload(&w, &p.ua.node.id).await;
    for (pod, e) in [(&p.p0, &p.e0), (&p.p1, &p.e1)] {
        let chan = port_of(&ua, &universal::chan_key(&record_key(&pod.node.id.0)));
        let topology_now = topology(&w, &p.canvas).await;
        if edges_touching(&topology_now, e).is_empty() {
            connect(&w, &port_of(e, "destination"), &chan).await;
        }
    }
    assert_clean(&w, &p.canvas).await;

    let lanes_now = lanes(&w, &p.canvas).await;
    // Per channel: tier 1 = 2 landings + 2 relays + 1 fan-out (as before);
    // tier 2 = per upstream server (2): 2 landings + 2 relays + 1 fan-out;
    // aggregate joins: hk3 and hk4 each join their 2 landings, the aggregate
    // node joins 2 feeders.
    assert_eq!(count(&lanes_now, LaneRole::Landing), 2 * (2 + 4), "{lanes_now:#?}");
    assert_eq!(count(&lanes_now, LaneRole::Relay), 2 * (2 + 4));
    assert_eq!(count(&lanes_now, LaneRole::Distribute), 2 * (1 + 2));
    assert_eq!(count(&lanes_now, LaneRole::Aggregate), 2 * (2 + 1));
    let tier2_relays = lanes_now
        .values()
        .filter(|n| {
            matches!(n.node.spec, NodeSpec::Relay(_))
                && n.node.lane.as_ref().is_some_and(|l| record_key(&l.group.0) == record_key(&ud2.node.id.0))
        })
        .count();
    assert_eq!(tier2_relays, 8, "the second tier owns one relay per channel, server and upstream path");

    settle(&w, &p.canvas, &[&p.us, &p.hk1, &p.hk2, &hk3, &hk4]).await?;
    for server in [&p.hk1, &p.hk2] {
        let view = w.view(server).await?;
        let applied = view.applied.as_ref().expect("tier 1 converged");
        assert!(view.invalid_pods.is_empty(), "{:?}", view.invalid_pods);
        let config = guru_worker_config::Config::from_toml_str(&applied.toml)?;
        assert_eq!(config.forwardings.len(), 2);
        for f in &config.forwardings {
            assert!(
                matches!(&f.to, guru_worker_config::ForwardingTo::LoadBalance(g) if g.members.len() == 2),
                "tier 1 landing pods balance over tier 2: {}",
                applied.toml
            );
        }
    }
    for server in [&hk3, &hk4] {
        let view = w.view(server).await?;
        let applied = view.applied.as_ref().expect("tier 2 converged");
        assert!(view.invalid_pods.is_empty(), "{:?}", view.invalid_pods);
        assert_eq!(applied.forwardings.len(), 4, "two channels from two upstream servers: {}", applied.toml);
    }
    Ok(())
}

/// A distribute node bundled to another one nests strategies: a fallback over
/// two round-robin groups derives as one nested load-balance tree.
#[tokio::test]
async fn nested_distribute_nodes_nest_strategies() -> TestResult {
    let w = world().await?;
    let canvas = w
        .canvases
        .process(orchestration::services::canvas::CreateCanvas {
            actor: operator(),
            name: "nested".to_string(),
            description: String::new(),
        })
        .await?
        .id;
    let us = create_server(&w, &canvas, "us", "198.51.100.1").await;
    let servers = ["a", "b", "c", "d"];
    let mut ups = Vec::new();
    let mut ids = Vec::new();
    for (i, name) in servers.iter().enumerate() {
        let id = create_server(&w, &canvas, name, &format!("203.0.113.{}", i + 1)).await;
        ups.push(universal_pod_of(&w, &canvas, &id).await);
        ids.push(id);
    }
    let p0 = create(&w, &canvas, "p0", pod(&us, 10000)).await;
    let entry = create(&w, &canvas, "entry", NodeSpec::Entry(EntryConfig { receive_proxy_protocol: None, tls: None })).await;
    connect(&w, &port_of(&p0, "listen"), &port_of(&entry, "listen")).await;
    let outer = create(&w, &canvas, "outer", distributor(LoadBalanceMode::Fallback, RelayProtocol::TcpRaw)).await;
    let inner_a = create(&w, &canvas, "inner-a", distributor(LoadBalanceMode::RoundRobin, RelayProtocol::TcpRaw)).await;
    let inner_b = create(&w, &canvas, "inner-b", distributor(LoadBalanceMode::RoundRobin, RelayProtocol::TcpRaw)).await;
    connect_universal(&w, handle(&outer, UniversalGroup::ChannelOut), ConnectEnd::Port(port_of(&p0, "destination"))).await?;
    bundle(&w, &reload(&w, &outer.node.id).await, &inner_a).await;
    bundle(&w, &reload(&w, &outer.node.id).await, &inner_b).await;
    bundle(&w, &reload(&w, &inner_a.node.id).await, &ups[0]).await;
    bundle(&w, &reload(&w, &inner_a.node.id).await, &ups[1]).await;
    bundle(&w, &reload(&w, &inner_b.node.id).await, &ups[2]).await;
    bundle(&w, &reload(&w, &inner_b.node.id).await, &ups[3]).await;
    let ua = create(&w, &canvas, "join", aggregator_with(&["a", "b", "c", "d"])).await;
    for up in &ups {
        bundle(&w, &reload(&w, &up.node.id).await, &reload(&w, &ua.node.id).await).await;
    }
    let ua = reload(&w, &ua.node.id).await;
    let exit0 = create(&w, &canvas, "exit", exit("10.0.0.5:8080")).await;
    connect(&w, &port_of(&exit0, "destination"), &port_of(&ua, &universal::chan_key(&record_key(&p0.node.id.0)))).await;
    assert_clean(&w, &canvas).await;

    let lanes_now = lanes(&w, &canvas).await;
    assert_eq!(count(&lanes_now, LaneRole::Landing), 4, "{lanes_now:#?}");
    assert_eq!(count(&lanes_now, LaneRole::Relay), 4);
    assert_eq!(count(&lanes_now, LaneRole::Distribute), 3, "outer + two inner");
    assert_eq!(count(&lanes_now, LaneRole::Aggregate), 1);

    settle(&w, &canvas, &[&us, &ids[0], &ids[1], &ids[2], &ids[3]]).await?;
    let view = w.view(&us).await?;
    let applied = view.applied.as_ref().expect("us converged");
    assert!(view.invalid_pods.is_empty(), "{:?}", view.invalid_pods);
    let config = guru_worker_config::Config::from_toml_str(&applied.toml)?;
    let guru_worker_config::ForwardingTo::LoadBalance(outer_group) = &config.forwardings[0].to else {
        panic!("outer should balance: {}", applied.toml);
    };
    assert_eq!(outer_group.strategy, guru_worker_config::LoadBalanceStrategy::Fallback);
    assert_eq!(outer_group.members.len(), 2);
    for member in &outer_group.members {
        let guru_worker_config::ForwardingTo::LoadBalance(inner) = member else {
            panic!("inner members should be groups: {}", applied.toml);
        };
        assert_eq!(inner.strategy, guru_worker_config::LoadBalanceStrategy::RoundRobin);
        assert_eq!(inner.members.len(), 2);
    }
    Ok(())
}

/// The bundle out of a distribute node carries everything that came in: two
/// channels by bundle from an upstream server plus one drawn in directly land
/// as three pods on every server it bundles to.
#[tokio::test]
async fn bundles_and_thin_lines_add_up() -> TestResult {
    let w = world().await?;
    let p = picture(&w).await;
    // hk1 carries the two channels; a new distribute node takes hk1's bundle
    // plus a third entry pod, and bundles to hk2 only.
    let topology_now = topology(&w, &p.canvas).await;
    for up in [&p.up1] {
        let out = edges_touching(&topology_now, up)
            .into_iter()
            .find(|e| p.ua.ports.iter().any(|x| record_key(&x.id.0) == record_key(&e.target.0)))
            .expect("bundle into the aggregate node");
        disconnect(&w, &out).await?;
    }
    let ud2 = create(&w, &p.canvas, "tier-2", distributor(LoadBalanceMode::RoundRobin, RelayProtocol::TcpRaw)).await;
    let p2 = create(&w, &p.canvas, "ingress-10002", pod(&p.us, 10002)).await;
    let entry = create(&w, &p.canvas, "entry-2", NodeSpec::Entry(EntryConfig { receive_proxy_protocol: None, tls: None })).await;
    connect(&w, &port_of(&p2, "listen"), &port_of(&entry, "listen")).await;
    bundle(&w, &reload(&w, &p.up1.node.id).await, &ud2).await;
    connect_universal(&w, handle(&reload(&w, &ud2.node.id).await, UniversalGroup::ChannelOut), ConnectEnd::Port(port_of(&p2, "destination"))).await?;
    bundle(&w, &reload(&w, &ud2.node.id).await, &reload(&w, &p.up2.node.id).await).await;
    let lanes_now = lanes(&w, &p.canvas).await;
    let hk2 = record_key(&p.hk2.0);
    let on_hk2 = lanes_now
        .values()
        .filter(|n| matches!(&n.node.spec, NodeSpec::Pod(cfg) if record_key(&cfg.server.0) == hk2))
        .count();
    // Two channels from the first tier (direct from `fan`) plus the same two
    // via hk1 and tier-2, plus the third drawn into tier-2.
    assert_eq!(on_hk2, 2 + 2 + 1, "{lanes_now:#?}");
    let ud2 = reload(&w, &ud2.node.id).await;
    assert!(ud2.ports.iter().any(|x| x.key.starts_with("bundle_in:")));
    assert!(ud2.ports.iter().any(|x| x.key == universal::chan_key(&record_key(&p2.node.id.0))));
    Ok(())
}
