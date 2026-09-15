//! Topology rules: one scenario per reported problem kind.

#![allow(clippy::unwrap_used, clippy::panic, clippy::expect_used)]

#[path = "common/mem.rs"]
mod mem;

use mem::*;
use orchestration::entities::surreal::server::ServerId;
use orchestration::entities::surreal::node::{
    CanvasExportAs, EntryConfig, ExitConfig, LoadBalanceAggregateConfig,
    LoadBalanceDistributeConfig, LoadBalanceMode, NodeSpec, PodConfig, ProxyProtocolVersion,
    RelayConfig, RelayProtocol,
};
use orchestration::entities::surreal::port::{PortDirection, PortEntity, PortKind};
use orchestration::services::topology::{
    ProblemKind, ProblemSeverity, TopologyProblem, analyze, ensure_valid,
};
use orchestration::utils::ids;

fn kinds(problems: &[TopologyProblem], severity: ProblemSeverity) -> Vec<ProblemKind> {
    problems
        .iter()
        .filter(|p| p.severity == severity)
        .map(|p| p.kind)
        .collect()
}

fn errors(problems: &[TopologyProblem]) -> Vec<ProblemKind> {
    kinds(problems, ProblemSeverity::Error)
}

fn warnings(problems: &[TopologyProblem]) -> Vec<ProblemKind> {
    kinds(problems, ProblemSeverity::Warning)
}

fn exit(dest: &str) -> NodeSpec {
    NodeSpec::Exit(ExitConfig {
        destination: dest.to_string(),
        pass_proxy_protocol: None,
    })
}

fn entry(pp: Option<ProxyProtocolVersion>) -> NodeSpec {
    NodeSpec::Entry(EntryConfig {
        receive_proxy_protocol: pp,
        tls: None,
    })
}

fn pod(server: &ServerId, port: u16) -> NodeSpec {
    NodeSpec::Pod(PodConfig {
        server: server.clone(),
        port,
        bind_ip: None,
        advertise_ip: None,
    })
}

fn relay(protocol: RelayProtocol) -> NodeSpec {
    NodeSpec::Relay(RelayConfig {
        protocol,
        override_ip_address: None,
        override_port: None,
    })
}

/// pod -> entry, exit -> pod: the smallest valid canvas.
fn valid_builder() -> Builder {
    let mut b = Builder::new("prod");
    let s = b.server("tokyo");
    let ip = b.ip("ip1", &s, "203.0.113.10");
    b.node("pod", pod(&ip, 443), pod_ports());
    b.node("entry", entry(None), entry_ports());
    b.node("exit", exit("10.0.0.5:8080"), exit_ports());
    b.connect("pod-listen", "entry-listen");
    b.connect("exit-destination", "pod-destination");
    b
}

#[test]
fn a_valid_canvas_has_no_problems() {
    let topology = valid_builder().build();
    let problems = analyze(&topology);
    assert!(problems.is_empty(), "{problems:?}");
    assert!(ensure_valid(&topology).is_ok());
}

#[test]
fn edge_direction_must_be_output_to_input() {
    let mut b = Builder::new("prod");
    let s = b.server("tokyo");
    let ip = b.ip("ip1", &s, "203.0.113.10");
    b.node("pod", pod(&ip, 443), pod_ports());
    b.node("entry", entry(None), entry_ports());
    b.node("exit", exit("10.0.0.5:8080"), exit_ports());
    // Both endpoints are inputs.
    b.connect_raw(port("entry", "listen"), port("pod", "destination"));
    let problems = analyze(&b.build());
    assert!(errors(&problems).contains(&ProblemKind::EdgeDirectionInvalid));
}

#[test]
fn edges_must_join_ports_of_the_same_kind() {
    let mut b = Builder::new("prod");
    let s = b.server("tokyo");
    let ip = b.ip("ip1", &s, "203.0.113.10");
    b.node("pod", pod(&ip, 443), pod_ports());
    b.node("pod2", pod(&ip, 8443), pod_ports());
    // listen output -> destination input
    b.connect("pod-listen", "pod2-destination");
    let problems = analyze(&b.build());
    assert!(errors(&problems).contains(&ProblemKind::PortKindMismatch));
}

#[test]
fn a_node_cannot_connect_to_itself() {
    let mut b = Builder::new("prod");
    b.node(
        "lb",
        NodeSpec::LoadBalanceDistribute(LoadBalanceDistributeConfig {
            mode: LoadBalanceMode::RoundRobin,
            protocol: RelayProtocol::TcpRaw,
        }),
        distribute_ports(2),
    );
    b.connect("lb-destination", "lb-member_0");
    let problems = analyze(&b.build());
    assert!(errors(&problems).contains(&ProblemKind::EdgeSelfNode));
}

#[test]
fn edges_may_not_cross_canvases() {
    let mut b = valid_builder();
    // Same shape, but the exit lives on another canvas of the same tree.
    b.canvas("other");
    b.node("exit2", exit("10.0.0.6:8080"), exit_ports());
    b.connect("exit2-destination", "pod-destination");
    let mut topology = b.build();
    topology
        .edges
        .retain(|e| ids::record_key(&e.id.0) != "exit-destination->pod-destination");
    let problems = analyze(&topology);
    assert!(errors(&problems).contains(&ProblemKind::EdgeCrossCanvas));
}

#[test]
fn a_port_carries_at_most_one_edge() {
    let mut b = valid_builder();
    b.node("exit2", exit("10.0.0.6:8080"), exit_ports());
    b.connect("exit2-destination", "pod-destination");
    let problems = analyze(&b.build());
    assert!(errors(&problems).contains(&ProblemKind::PortOversubscribed));
}

#[test]
fn a_node_must_have_the_ports_its_spec_requires() {
    let mut b = Builder::new("prod");
    let s = b.server("tokyo");
    let ip = b.ip("ip1", &s, "203.0.113.10");
    // A pod without its destination input.
    b.node(
        "pod",
        pod(&ip, 443),
        vec![(
            "listen".to_string(),
            PortKind::DeriveListen,
            PortDirection::Output,
            0,
        )],
    );
    let problems = analyze(&b.build());
    assert!(errors(&problems).contains(&ProblemKind::PortShapeInvalid));
}

#[test]
fn importing_yourself_is_rejected() {
    let mut b = Builder::new("prod");
    b.node("import", import_spec("prod"), vec![]);
    let problems = analyze(&b.build());
    assert_eq!(errors(&problems), vec![ProblemKind::CanvasImportSelf]);
}

#[test]
fn importing_an_ancestor_is_rejected() {
    let mut b = Builder::new("root");
    b.node("import_sub", import_spec("sub"), vec![]);
    b.canvas("sub");
    b.node("import_root", import_spec("root"), vec![]);
    let problems = analyze(&b.build());
    assert!(
        errors(&problems).contains(&ProblemKind::CanvasImportAncestor),
        "{problems:?}"
    );
}

#[test]
fn importing_a_canvas_twice_is_rejected() {
    let mut b = Builder::new("root");
    b.node("import_a", import_spec("sub"), vec![]);
    b.node("import_b", import_spec("sub"), vec![]);
    b.canvas("sub");
    let problems = analyze(&b.build());
    assert_eq!(errors(&problems), vec![ProblemKind::CanvasImportDuplicate]);
}

#[test]
fn importing_a_missing_canvas_is_rejected() {
    let mut b = Builder::new("root");
    b.node("import", import_spec("ghost"), vec![]);
    let problems = analyze(&b.build());
    assert_eq!(errors(&problems), vec![ProblemKind::CanvasImportUnresolved]);
}

#[test]
fn an_import_node_mirrors_the_exports_of_its_target() {
    let mut b = Builder::new("root");
    b.node(
        "import",
        import_spec("sub"),
        import_ports(&[(
            "out",
            PortKind::DeriveDestination,
            CanvasExportAs::OutputOutOfCanvas,
        )]),
    );
    b.canvas("sub");
    b.node(
        "out",
        export_spec(
            PortKind::DeriveDestination,
            CanvasExportAs::OutputOutOfCanvas,
        ),
        export_ports(
            PortKind::DeriveDestination,
            CanvasExportAs::OutputOutOfCanvas,
        ),
    );
    assert_eq!(errors(&analyze(&b.build())), vec![]);

    // A stale extra port on the importer no longer matches the exports.
    let mut stale = b.build();
    stale.nodes[0].ports.push(PortEntity {
        id: ids::port_id("stale"),
        owner: ids::node_id("import"),
        kind: PortKind::DeriveListen,
        direction: PortDirection::Input,
        key: "gone".to_string(),
        position: 1,
    });
    assert_eq!(
        errors(&analyze(&stale)),
        vec![ProblemKind::PortShapeInvalid]
    );
}

#[test]
fn a_pod_must_reference_an_ip_of_this_canvas() {
    let mut b = Builder::new("prod");
    let s = b.server("tokyo");
    b.ip("ip1", &s, "203.0.113.10");
    b.node("pod", pod(&ids::server_id("foreign"), 443), pod_ports());
    b.node("entry", entry(None), entry_ports());
    b.node("exit", exit("10.0.0.5:8080"), exit_ports());
    b.connect("pod-listen", "entry-listen");
    b.connect("exit-destination", "pod-destination");
    let problems = analyze(&b.build());
    assert_eq!(errors(&problems), vec![ProblemKind::PodServerForeign]);
}

/// A pod in a subcanvas listening on a server of the root, with its whole chain
/// inside the subcanvas.
fn pod_in_sub_on_root_server(import: bool) -> Builder {
    let mut b = Builder::new("root");
    let s = b.server("tokyo");
    let ip = b.ip("ip1", &s, "203.0.113.10");
    if import {
        b.node("import", import_spec("sub"), vec![]);
    }
    b.canvas("sub");
    b.node("pod", pod(&ip, 443), pod_ports());
    b.node("entry", entry(None), entry_ports());
    b.node("exit", exit("10.0.0.5:8080"), exit_ports());
    b.connect("pod-listen", "entry-listen");
    b.connect("exit-destination", "pod-destination");
    b
}

#[test]
fn a_pod_may_use_a_server_anywhere_in_its_tree() {
    let problems = analyze(&pod_in_sub_on_root_server(true).build());
    assert_eq!(errors(&problems), vec![], "{problems:?}");
}

#[test]
fn a_pod_may_not_use_a_server_of_another_tree() {
    let problems = analyze(&pod_in_sub_on_root_server(false).build());
    assert_eq!(errors(&problems), vec![ProblemKind::PodServerForeign]);
}

/// pod -> relay in root; the relay's destination goes into `sub`, whose export
/// feeds the very pod the relay listens to.
#[test]
fn a_cycle_through_an_import_boundary_is_detected() {
    let mut b = Builder::new("root");
    let s = b.server("tokyo");
    let ip = b.ip("ip1", &s, "203.0.113.10");
    b.node("pod", pod(&ip, 443), pod_ports());
    b.node("relay", relay(RelayProtocol::TcpRaw), relay_ports());
    b.node(
        "import",
        import_spec("sub"),
        import_ports(&[
            (
                "into",
                PortKind::DeriveDestination,
                CanvasExportAs::InputIntoCanvas,
            ),
            (
                "out",
                PortKind::DeriveDestination,
                CanvasExportAs::OutputOutOfCanvas,
            ),
        ]),
    );
    b.connect("pod-listen", "relay-listen");
    b.connect("relay-destination", "import-into");
    b.connect("import-out", "pod-destination");
    b.canvas("sub");
    b.node(
        "into",
        export_spec(PortKind::DeriveDestination, CanvasExportAs::InputIntoCanvas),
        export_ports(PortKind::DeriveDestination, CanvasExportAs::InputIntoCanvas),
    );
    b.node(
        "out",
        export_spec(
            PortKind::DeriveDestination,
            CanvasExportAs::OutputOutOfCanvas,
        ),
        export_ports(
            PortKind::DeriveDestination,
            CanvasExportAs::OutputOutOfCanvas,
        ),
    );
    b.connect("into-export", "out-export");
    let problems = analyze(&b.build());
    assert!(
        errors(&problems).contains(&ProblemKind::Cycle),
        "{problems:?}"
    );
}

#[test]
fn projection_reshapes_import_ports() {
    use orchestration::services::topology::TopologyEdit;
    let mut b = Builder::new("root");
    let s = b.server("tokyo");
    let ip = b.ip("ip1", &s, "203.0.113.10");
    b.node("pod", pod(&ip, 443), pod_ports());
    b.node("entry", entry(None), entry_ports());
    b.node(
        "import",
        import_spec("sub"),
        import_ports(&[(
            "out",
            PortKind::DeriveDestination,
            CanvasExportAs::OutputOutOfCanvas,
        )]),
    );
    b.connect("pod-listen", "entry-listen");
    b.connect("import-out", "pod-destination");
    b.canvas("sub");
    b.node(
        "out",
        export_spec(
            PortKind::DeriveDestination,
            CanvasExportAs::OutputOutOfCanvas,
        ),
        export_ports(
            PortKind::DeriveDestination,
            CanvasExportAs::OutputOutOfCanvas,
        ),
    );
    let topology = b.build();
    assert_eq!(topology.edges.len(), 2);

    // The export goes away: its mirrored port and the edge on it go with it.
    let projected = topology.project(&[
        TopologyEdit::RetireNode {
            node: ids::node_id("out"),
        },
        TopologyEdit::ReshapePorts {
            node: ids::node_id("import"),
            ports: vec![],
        },
    ]);
    assert_eq!(projected.edges.len(), 1, "the dropped port took its edge");
    assert!(
        ensure_valid(&projected).is_ok(),
        "{:?}",
        analyze(&projected)
    );

    // A kept key keeps its row and edge, even when re-kinded.
    let projected = topology.project(&[TopologyEdit::ReshapePorts {
        node: ids::node_id("import"),
        ports: vec![PortEntity {
            id: port("import", "out"),
            owner: ids::node_id("import"),
            kind: PortKind::DeriveListen,
            direction: PortDirection::Output,
            key: "out".to_string(),
            position: 0,
        }],
    }]);
    assert_eq!(projected.edges.len(), 2, "the kept key kept its edge");
}

#[test]
fn an_exit_destination_must_be_host_port() {
    let mut b = Builder::new("prod");
    let s = b.server("tokyo");
    let ip = b.ip("ip1", &s, "203.0.113.10");
    b.node("pod", pod(&ip, 443), pod_ports());
    b.node("entry", entry(None), entry_ports());
    b.node("exit", exit("no-port-here"), exit_ports());
    b.connect("pod-listen", "entry-listen");
    b.connect("exit-destination", "pod-destination");
    let problems = analyze(&b.build());
    assert_eq!(errors(&problems), vec![ProblemKind::ExitDestinationInvalid]);
}

/// An exit an operator has not filled in yet stays storable: it is the mid-edit
/// case this checker documents, and derivation refuses the pod behind it anyway.
#[test]
fn an_unset_exit_destination_is_a_warning_not_an_error() {
    let mut b = Builder::new("prod");
    let s = b.server("tokyo");
    let ip = b.ip("ip1", &s, "203.0.113.10");
    b.node("pod", pod(&ip, 443), pod_ports());
    b.node("entry", entry(None), entry_ports());
    b.node("exit", exit(""), exit_ports());
    b.connect("pod-listen", "entry-listen");
    b.connect("exit-destination", "pod-destination");
    let topology = b.build();
    let problems = analyze(&topology);
    assert_eq!(errors(&problems), Vec::<ProblemKind>::new());
    assert!(warnings(&problems).contains(&ProblemKind::ExitDestinationInvalid));
    ensure_valid(&topology).expect("a half-drawn exit must remain storable");
}

#[test]
fn two_pods_may_not_share_a_listen_address() {
    let mut b = Builder::new("prod");
    let s = b.server("tokyo");
    let ip = b.ip("ip1", &s, "203.0.113.10");
    for tag in ["pod_a", "pod_b"] {
        b.node(tag, pod(&ip, 443), pod_ports());
        b.named_node(
            &format!("{tag}_entry"),
            &format!("{tag}_entry"),
            entry(None),
            entry_ports(),
        );
        b.named_node(
            &format!("{tag}_exit"),
            &format!("{tag}_exit"),
            exit("10.0.0.5:8080"),
            exit_ports(),
        );
        b.connect(&format!("{tag}-listen"), &format!("{tag}_entry-listen"));
        b.connect(
            &format!("{tag}_exit-destination"),
            &format!("{tag}-destination"),
        );
    }
    let problems = analyze(&b.build());
    assert_eq!(errors(&problems), vec![ProblemKind::DuplicateListen]);
}

#[test]
fn a_relay_loop_is_a_cycle() {
    let mut b = Builder::new("prod");
    let s = b.server("tokyo");
    let ip = b.ip("ip1", &s, "203.0.113.10");
    b.node("pod", pod(&ip, 443), pod_ports());
    b.node("relay", relay(RelayProtocol::TcpRaw), relay_ports());
    // The relay dials the very pod it feeds.
    b.connect("pod-listen", "relay-listen");
    b.connect("relay-destination", "pod-destination");
    let problems = analyze(&b.build());
    assert!(
        errors(&problems).contains(&ProblemKind::Cycle),
        "{problems:?}"
    );
}

#[test]
fn ip_hash_needs_the_client_address() {
    let mut b = Builder::new("prod");
    let s = b.server("tokyo");
    let ip = b.ip("ip1", &s, "203.0.113.10");
    b.node("pod", pod(&ip, 443), pod_ports());
    b.node("entry", entry(None), entry_ports());
    b.node(
        "lb",
        NodeSpec::LoadBalanceDistribute(LoadBalanceDistributeConfig {
            mode: LoadBalanceMode::IpHash,
            protocol: RelayProtocol::TcpRaw,
        }),
        distribute_ports(2),
    );
    b.node("exit_a", exit("10.0.0.5:8080"), exit_ports());
    b.node("exit_b", exit("10.0.0.6:8080"), exit_ports());
    b.connect("pod-listen", "entry-listen");
    b.connect("lb-destination", "pod-destination");
    b.connect("exit_a-destination", "lb-member_0");
    b.connect("exit_b-destination", "lb-member_1");
    let problems = analyze(&b.build());
    assert_eq!(errors(&problems), vec![ProblemKind::IpHashWithoutClientIp]);

    // With PROXY protocol on the entry the client address is known again.
    let mut ok = Builder::new("prod");
    let s = ok.server("tokyo");
    let ip = ok.ip("ip1", &s, "203.0.113.10");
    ok.node("pod", pod(&ip, 443), pod_ports());
    ok.node(
        "entry",
        entry(Some(ProxyProtocolVersion::V2)),
        entry_ports(),
    );
    ok.node(
        "lb",
        NodeSpec::LoadBalanceDistribute(LoadBalanceDistributeConfig {
            mode: LoadBalanceMode::IpHash,
            protocol: RelayProtocol::TcpRaw,
        }),
        distribute_ports(2),
    );
    ok.node("exit_a", exit("10.0.0.5:8080"), exit_ports());
    ok.node("exit_b", exit("10.0.0.6:8080"), exit_ports());
    ok.connect("pod-listen", "entry-listen");
    ok.connect("lb-destination", "pod-destination");
    ok.connect("exit_a-destination", "lb-member_0");
    ok.connect("exit_b-destination", "lb-member_1");
    assert!(errors(&analyze(&ok.build())).is_empty());
}

#[test]
fn ip_hash_behind_an_aggregate_is_still_reachable() {
    let mut b = Builder::new("prod");
    let s = b.server("tokyo");
    let ip = b.ip("ip1", &s, "203.0.113.10");
    b.node("pod", pod(&ip, 443), pod_ports());
    b.node("entry", entry(None), entry_ports());
    b.node(
        "agg",
        NodeSpec::LoadBalanceAggregate(LoadBalanceAggregateConfig {}),
        aggregate_ports(2),
    );
    b.node(
        "lb",
        NodeSpec::LoadBalanceDistribute(LoadBalanceDistributeConfig {
            mode: LoadBalanceMode::IpHash,
            protocol: RelayProtocol::TcpRaw,
        }),
        distribute_ports(2),
    );
    b.node("exit_a", exit("10.0.0.5:8080"), exit_ports());
    b.node("exit_b", exit("10.0.0.6:8080"), exit_ports());
    b.connect("pod-listen", "entry-listen");
    b.connect("agg-copy_0", "pod-destination");
    b.connect("lb-destination", "agg-source");
    b.connect("exit_a-destination", "lb-member_0");
    b.connect("exit_b-destination", "lb-member_1");
    let problems = analyze(&b.build());
    assert!(errors(&problems).contains(&ProblemKind::IpHashWithoutClientIp));
}

/// Every server starts with unwired transport pods, so an unconnected pod is the
/// normal state: it derives nothing and is not reported.
#[test]
fn an_unconnected_pod_port_is_not_reported() {
    let mut b = Builder::new("prod");
    let s = b.server("tokyo");
    let ip = b.ip("ip1", &s, "203.0.113.10");
    b.node("pod", pod(&ip, 443), pod_ports());
    let topology = b.build();
    let problems = analyze(&topology);
    assert!(errors(&problems).is_empty(), "{problems:?}");
    assert!(warnings(&problems).is_empty(), "{problems:?}");
    assert!(
        ensure_valid(&topology).is_ok(),
        "warnings never block a write"
    );
}

#[test]
fn a_relay_hopping_within_one_server_is_a_warning() {
    let mut b = Builder::new("prod");
    let s = b.server("tokyo");
    let ip = b.ip("ip1", &s, "203.0.113.10");
    b.node("pod_in", pod(&ip, 443), pod_ports());
    b.node("pod_out", pod(&ip, 8443), pod_ports());
    b.node("entry", entry(None), entry_ports());
    b.node("relay", relay(RelayProtocol::TcpRaw), relay_ports());
    b.node("exit", exit("10.0.0.5:8080"), exit_ports());
    b.connect("pod_in-listen", "relay-listen");
    b.connect("relay-destination", "pod_out-destination");
    b.connect("pod_out-listen", "entry-listen");
    b.connect("exit-destination", "pod_in-destination");
    let problems = analyze(&b.build());
    assert!(errors(&problems).is_empty(), "{problems:?}");
    assert!(warnings(&problems).contains(&ProblemKind::RelaySameServer));
}

#[test]
fn a_single_member_load_balancer_is_a_warning() {
    let mut b = Builder::new("prod");
    let s = b.server("tokyo");
    let ip = b.ip("ip1", &s, "203.0.113.10");
    b.node("pod", pod(&ip, 443), pod_ports());
    b.node("entry", entry(None), entry_ports());
    b.node(
        "lb",
        NodeSpec::LoadBalanceDistribute(LoadBalanceDistributeConfig {
            mode: LoadBalanceMode::RoundRobin,
            protocol: RelayProtocol::TcpRaw,
        }),
        distribute_ports(2),
    );
    b.node("exit_a", exit("10.0.0.5:8080"), exit_ports());
    b.connect("pod-listen", "entry-listen");
    b.connect("lb-destination", "pod-destination");
    b.connect("exit_a-destination", "lb-member_0");
    let problems = analyze(&b.build());
    assert!(errors(&problems).is_empty(), "{problems:?}");
    assert!(warnings(&problems).contains(&ProblemKind::DistributeSingleMember));
}

#[test]
fn projection_validates_a_change_before_it_is_written() {
    use orchestration::services::topology::TopologyEdit;
    let b = valid_builder();
    let topology = b.build();

    // Retiring the exit leaves the pod's destination unconnected: a warning, not an error.
    let exit_id = topology.nodes[2].node.id.clone();
    let projected = topology.project(&[TopologyEdit::RetireNode { node: exit_id }]);
    assert_eq!(projected.nodes.len(), 2);
    assert_eq!(projected.edges.len(), 1, "the exit's edge went with it");
    assert!(ensure_valid(&projected).is_ok());

    // Adding a second edge into an occupied port is rejected before writing.
    let extra = orchestration::entities::surreal::connection::EdgeConnectionEntity {
        id: ids::edge_id("pending-0"),
        source: port("exit", "destination"),
        target: port("pod", "destination"),
    };
    let projected = topology.project(&[TopologyEdit::AddEdge { edge: extra }]);
    let err = ensure_valid(&projected).expect_err("oversubscribed port must be rejected");
    assert_eq!(err.first.kind, ProblemKind::PortOversubscribed);
}

// --- universal nodes -----------------------------------------------------------

use orchestration::entities::surreal::node::{Lane, LaneRole, UniversalPodConfig};
use orchestration::services::universal;

fn bundle_port(key: &str, direction: PortDirection) -> (String, PortKind, PortDirection, i64) {
    (key.to_string(), PortKind::Bundle, direction, 0)
}

fn channel_ports(pod: &str, out_first: bool, ordinal: i64) -> Vec<(String, PortKind, PortDirection, i64)> {
    let (chan_dir, lane_dir) = if out_first {
        (PortDirection::Output, PortDirection::Input)
    } else {
        (PortDirection::Input, PortDirection::Output)
    };
    vec![
        (universal::chan_key(pod), PortKind::DeriveDestination, chan_dir, ordinal),
        (universal::lane_key(pod), PortKind::DeriveDestination, lane_dir, ordinal),
    ]
}

fn ud(mode: LoadBalanceMode, protocol: RelayProtocol) -> NodeSpec {
    NodeSpec::LoadBalanceDistribute(LoadBalanceDistributeConfig { mode, protocol })
}

fn ua() -> NodeSpec {
    NodeSpec::LoadBalanceAggregate(LoadBalanceAggregateConfig {})
}

fn up(server: &ServerId) -> NodeSpec {
    NodeSpec::UniversalPod(UniversalPodConfig {
        server: server.clone(),
    })
}

/// A distributor whose one channel is bundled to one universal pod, with the
/// lanes that expansion calls for drawn by hand: one landing pod, one relay.
fn expanded_builder() -> Builder {
    let mut b = Builder::new("prod");
    let us = b.server("us");
    b.ip("_", &us, "198.51.100.1");
    let hk = b.server("hk");
    b.ip("_", &hk, "203.0.113.1");
    b.node("p0", pod(&us, 10000), pod_ports());
    b.node("entry", entry(None), entry_ports());
    b.connect("p0-listen", "entry-listen");
    let mut ud_ports = channel_ports("p0", true, 0);
    ud_ports.push(bundle_port(&universal::bundle_out_key("hk-up"), PortDirection::Output));
    b.node("ud", ud(LoadBalanceMode::RoundRobin, RelayProtocol::TcpRaw), ud_ports);
    b.connect("ud-chan:p0", "p0-destination");
    b.node(
        "hk-up",
        up(&hk),
        vec![
            bundle_port(&universal::bundle_in_key("ud"), PortDirection::Input),
            bundle_port(universal::BUNDLE_OUT, PortDirection::Output),
        ],
    );
    b.connect("ud-bundle_out:hk-up", "hk-up-bundle_in:ud");
    // The lanes.
    let landing = Lane::new(&ids::node_id("hk-up"), &ids::node_id("p0"), LaneRole::Landing, Some(&ids::node_id("ud")), None);
    b.node("landing", pod(&hk, 45000), pod_ports());
    b.lane(landing);
    let relay_lane = Lane::new(&ids::node_id("ud"), &ids::node_id("p0"), LaneRole::Relay, Some(&ids::node_id("hk-up")), None);
    b.node("relay", relay(RelayProtocol::TcpRaw), relay_ports());
    b.lane(relay_lane);
    b.connect("landing-listen", "relay-listen");
    b.connect("relay-destination", "ud-lane:p0");
    b
}

/// The hand-drawn expansion is what the reconciler would produce: no lanes to
/// add or remove, and the checker looks through the channel pair, so the entry
/// pod's destination resolves to the relay and derivation sees a flat chain.
#[test]
fn a_matching_expansion_is_not_stale_and_derives_through_the_channel() {
    let topology = expanded_builder().build();
    let problems = analyze(&topology);
    assert!(errors(&problems).is_empty(), "{problems:?}");
    assert_eq!(warnings(&problems), [ProblemKind::ChannelNoExit], "{problems:?}");
    assert!(!universal::is_stale(&topology));

    let derived = orchestration::services::derive::derive_server_config(
        &topology,
        &ids::server_id("us"),
        &orchestration::services::derive::DerivationCertificates::default(),
        &orchestration::config::OrchestrationConfig::default(),
    )
    .unwrap();
    assert_eq!(derived.forwardings.len(), 1, "{:?}", derived.invalid);
    assert_eq!(derived.forwardings[0].points_at.len(), 1);
    assert_eq!(derived.forwardings[0].points_at[0].port, 45000);
}

/// Dropping a lane by hand leaves the expansion stale (a warning), never an error.
#[test]
fn missing_lanes_are_a_warning() {
    let mut b = expanded_builder();
    let topology = b.build();
    let relay_id = ids::node_id("relay");
    let projected = topology.project(&[orchestration::services::topology::TopologyEdit::RetireNode { node: relay_id }]);
    let problems = analyze(&projected);
    assert!(errors(&problems).is_empty(), "{problems:?}");
    assert!(warnings(&problems).contains(&ProblemKind::LanesStale), "{problems:?}");
    let _ = &mut b;
}

/// A universal pod's own channel pair (an entry pod drawn straight into it) is
/// looked through like a distribute node's, so the checker sees the flat hop.
#[test]
fn a_universal_pod_may_start_a_channel() {
    let mut b = Builder::new("prod");
    let us = b.server("us");
    b.ip("_", &us, "198.51.100.1");
    let hk = b.server("hk");
    b.ip("_", &hk, "203.0.113.1");
    b.node("p0", pod(&us, 10000), pod_ports());
    let mut up_ports = channel_ports("p0", true, 0);
    up_ports.push(bundle_port(universal::BUNDLE_OUT, PortDirection::Output));
    b.node("hk-up", up(&hk), up_ports);
    b.connect("hk-up-chan:p0", "p0-destination");
    let problems = analyze(&b.build());
    assert!(!errors(&problems).contains(&ProblemKind::PortShapeInvalid), "{problems:?}");
    assert!(!errors(&problems).contains(&ProblemKind::ChannelTargetNotPod), "{problems:?}");
    // Nothing generated yet: the expansion is stale until the write lands.
    assert!(warnings(&problems).contains(&ProblemKind::LanesStale), "{problems:?}");
}

#[test]
fn bundles_may_enter_a_distribute_node() {
    // universal pod -> distribute (next tier) and distribute -> distribute (nesting)
    let mut b = Builder::new("prod");
    let hk = b.server("hk");
    b.node(
        "hk-up",
        up(&hk),
        vec![bundle_port(universal::BUNDLE_OUT, PortDirection::Output)],
    );
    b.node(
        "ud2",
        ud(LoadBalanceMode::RoundRobin, RelayProtocol::TcpRaw),
        vec![
            bundle_port(&universal::bundle_in_key("hk-up"), PortDirection::Input),
            bundle_port(&universal::bundle_out_key("ud3"), PortDirection::Output),
        ],
    );
    b.node(
        "ud3",
        ud(LoadBalanceMode::Fallback, RelayProtocol::TcpRaw),
        vec![bundle_port(&universal::bundle_in_key("ud2"), PortDirection::Input)],
    );
    b.connect("hk-up-bundle_out", "ud2-bundle_in:hk-up");
    b.connect("ud2-bundle_out:ud3", "ud3-bundle_in:ud2");
    let problems = analyze(&b.build());
    assert!(!errors(&problems).contains(&ProblemKind::BundleEdgeInvalid), "{problems:?}");
    assert!(!errors(&problems).contains(&ProblemKind::PortShapeInvalid), "{problems:?}");
}

#[test]
fn bundles_only_join_the_allowed_pairs_on_matching_keys() {
    // A distributor bundled straight into an aggregator.
    let mut b = Builder::new("prod");
    b.node(
        "ud",
        ud(LoadBalanceMode::RoundRobin, RelayProtocol::TcpRaw),
        vec![bundle_port(&universal::bundle_out_key("ua"), PortDirection::Output)],
    );
    b.node(
        "ua",
        ua(),
        vec![bundle_port(&universal::bundle_in_key("ud"), PortDirection::Input)],
    );
    b.connect("ud-bundle_out:ua", "ua-bundle_in:ud");
    let problems = analyze(&b.build());
    assert!(errors(&problems).contains(&ProblemKind::BundleEdgeInvalid), "{problems:?}");

    // The right pair, but the ports are not named after each other.
    let mut b = Builder::new("prod");
    let hk = b.server("hk");
    b.node(
        "ud",
        ud(LoadBalanceMode::RoundRobin, RelayProtocol::TcpRaw),
        vec![bundle_port(&universal::bundle_out_key("other"), PortDirection::Output)],
    );
    b.node(
        "hk-up",
        up(&hk),
        vec![
            bundle_port(&universal::bundle_in_key("ud"), PortDirection::Input),
            bundle_port(universal::BUNDLE_OUT, PortDirection::Output),
        ],
    );
    b.connect("ud-bundle_out:other", "hk-up-bundle_in:ud");
    let problems = analyze(&b.build());
    assert!(errors(&problems).contains(&ProblemKind::BundleEdgeInvalid), "{problems:?}");
}

#[test]
fn a_bundle_cycle_is_an_error() {
    let mut b = Builder::new("prod");
    let a = b.server("a");
    let c = b.server("c");
    b.node(
        "a-up",
        up(&a),
        vec![
            bundle_port(&universal::bundle_in_key("c-up"), PortDirection::Input),
            bundle_port(universal::BUNDLE_OUT, PortDirection::Output),
        ],
    );
    b.node(
        "c-up",
        up(&c),
        vec![
            bundle_port(&universal::bundle_in_key("a-up"), PortDirection::Input),
            bundle_port(universal::BUNDLE_OUT, PortDirection::Output),
        ],
    );
    b.connect("a-up-bundle_out", "c-up-bundle_in:a-up");
    b.connect("c-up-bundle_out", "a-up-bundle_in:c-up");
    let problems = analyze(&b.build());
    assert!(errors(&problems).contains(&ProblemKind::BundleCycle), "{problems:?}");
}

#[test]
fn a_channel_must_feed_the_pod_it_is_named_after() {
    let mut b = Builder::new("prod");
    let us = b.server("us");
    b.ip("_", &us, "198.51.100.1");
    b.node("p0", pod(&us, 10000), pod_ports());
    b.node("p1", pod(&us, 10001), pod_ports());
    b.node("ud", ud(LoadBalanceMode::RoundRobin, RelayProtocol::TcpRaw), channel_ports("p0", true, 0));
    b.connect("ud-chan:p0", "p1-destination");
    let problems = analyze(&b.build());
    assert!(errors(&problems).contains(&ProblemKind::ChannelTargetNotPod), "{problems:?}");
}

#[test]
fn universal_ports_must_have_their_kind_shape() {
    // A universal pod without its fixed bundle_out.
    let mut b = Builder::new("prod");
    let hk = b.server("hk");
    b.node("hk-up", up(&hk), vec![]);
    let problems = analyze(&b.build());
    assert!(errors(&problems).contains(&ProblemKind::PortShapeInvalid), "{problems:?}");

    // A distributor with a channel port but no lane twin.
    let mut b = Builder::new("prod");
    b.node(
        "ud",
        ud(LoadBalanceMode::RoundRobin, RelayProtocol::TcpRaw),
        vec![(universal::chan_key("p0"), PortKind::DeriveDestination, PortDirection::Output, 0)],
    );
    let problems = analyze(&b.build());
    assert!(errors(&problems).contains(&ProblemKind::PortShapeInvalid), "{problems:?}");

    // An aggregate node with its pair the right way round is fine, with or
    // without hand-drawn ports next to it.
    let mut b = Builder::new("prod");
    b.node("ua", ua(), channel_ports("p0", false, 0));
    let mut mixed = channel_ports("p0", false, 0);
    mixed.extend(aggregate_ports(2));
    b.node("ua2", ua(), mixed);
    let problems = analyze(&b.build());
    assert!(!errors(&problems).contains(&ProblemKind::PortShapeInvalid), "{problems:?}");

    // Hand-drawn ports are all or nothing: a lone `source` is half a node.
    let mut b = Builder::new("prod");
    b.node(
        "ua",
        ua(),
        vec![("source".to_string(), PortKind::DeriveDestination, PortDirection::Input, 0)],
    );
    let problems = analyze(&b.build());
    assert!(errors(&problems).contains(&ProblemKind::PortShapeInvalid), "{problems:?}");
}

/// A distribute node's `lane:` inputs are not members: a node whose only
/// hand-drawn member is wired is the single-member case, one whose channels are
/// wired is not.
#[test]
fn channel_lanes_are_not_members() {
    let mut b = Builder::new("prod");
    let us = b.server("us");
    b.ip("_", &us, "198.51.100.1");
    b.node("p0", pod(&us, 10000), pod_ports());
    b.node("ud", ud(LoadBalanceMode::RoundRobin, RelayProtocol::TcpRaw), channel_ports("p0", true, 0));
    b.connect("ud-chan:p0", "p0-destination");
    b.node("exit", exit("10.0.0.5:8080"), exit_ports());
    b.connect("exit-destination", "ud-lane:p0");
    let problems = analyze(&b.build());
    assert!(!warnings(&problems).contains(&ProblemKind::DistributeSingleMember), "{problems:?}");
}
