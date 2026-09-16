//! Converting the node model into the pod graph: every shape the node model
//! derives converts to a graph that compiles to the same forwardings.

#![allow(clippy::unwrap_used, clippy::panic, clippy::expect_used)]

#[path = "common/mem.rs"]
mod mem;

use guru_topology::{Route, Sticky};
use mem::*;
use orchestration::config::OrchestrationConfig;
use orchestration::entities::db::ca::{RelayCertificateEntity, relay_sni};
use orchestration::entities::db::certificate::{CertificateEntity, CertificateStatus};
use orchestration::entities::db::edge::EdgeTarget;
use orchestration::entities::db::node::{
    CanvasExportAs, EntryConfig, ExitConfig, LoadBalanceAggregateConfig,
    LoadBalanceDistributeConfig, LoadBalanceMode, NodeSpec, PodConfig, ProxyProtocolVersion,
    RelayConfig, RelayProtocol, TlsConfig,
};
use orchestration::entities::db::pod::{PodEntity, PodIngress};
use orchestration::entities::db::port::PortKind;
use orchestration::entities::db::server::{QuicCongestion, ServerId, ServerQuic};
use orchestration::entities::db::topology::CanvasTopology;
use orchestration::services::convert::{Converted, compare, convert_tree};
use orchestration::utils::ids;

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

fn distribute(mode: LoadBalanceMode) -> NodeSpec {
    NodeSpec::LoadBalanceDistribute(LoadBalanceDistributeConfig {
        mode,
        protocol: RelayProtocol::TcpRaw,
        members: Vec::new(),
    })
}

fn certificate(key: &str, sni: &str, directory: &str, version: i64) -> CertificateEntity {
    let now = chrono::Utc::now();
    CertificateEntity {
        id: ids::certificate_id(key),
        sni: sni.to_string(),
        dns_provider: ids::dns_provider_id("cf"),
        domain_id: "zone".to_string(),
        acme_directory: directory.to_string(),
        status: CertificateStatus::Issued,
        acme_account_key: Some("enc1:acct".to_string()),
        private_key_pem: Some("enc1:key".to_string()),
        full_chain_pem: Some("-----BEGIN CERTIFICATE-----".to_string()),
        not_before: Some(now),
        not_after: Some(now + chrono::Duration::days(60)),
        last_error: None,
        last_attempt_at: Some(now),
        version,
        created_at: now,
    }
}

fn relay_leaf(key: &str, pod: &str, version: i64) -> RelayCertificateEntity {
    let now = chrono::Utc::now();
    let pod = ids::node_id(pod);
    RelayCertificateEntity {
        id: ids::relay_certificate_id(key),
        sni: relay_sni(&pod),
        pod,
        private_key_pem: "enc1:key".to_string(),
        certificate_pem: "-----BEGIN CERTIFICATE-----".to_string(),
        not_before: now,
        not_after: now + chrono::Duration::days(30),
        version,
    }
}

struct Material {
    acme: Vec<CertificateEntity>,
    relay: Vec<RelayCertificateEntity>,
    internal_ca: bool,
}

fn no_material() -> Material {
    Material {
        acme: Vec::new(),
        relay: Vec::new(),
        internal_ca: false,
    }
}

/// Converts and asserts the graph compiles to exactly what the node model derives.
fn converted_alike(topology: &CanvasTopology, material: &Material) -> Converted {
    let converted = convert_tree(topology);
    let comparison = compare(
        topology,
        &converted.rows,
        &material.acme,
        &material.relay,
        material.internal_ca,
        &OrchestrationConfig::default(),
    );
    assert!(
        comparison.differences.is_empty(),
        "differences: {:#?}\nnotes: {:#?}",
        comparison.differences,
        converted.notes
    );
    converted
}

fn pod_named<'a>(converted: &'a Converted, name: &str) -> &'a PodEntity {
    converted
        .rows
        .pods
        .iter()
        .find(|p| p.name == name)
        .unwrap_or_else(|| panic!("no pod {name}"))
}

/// The targets of a route's leaves, in order, as `pod:<key>` / `exit:<key>`.
fn leaf_targets(converted: &Converted, route: &Route) -> Vec<String> {
    route
        .leaves()
        .into_iter()
        .map(|leaf| {
            let edge = converted
                .rows
                .edges
                .iter()
                .find(|e| e.id.as_str() == leaf.as_str())
                .unwrap();
            match &edge.target {
                EdgeTarget::Pod(pod) => format!("pod:{pod}"),
                EdgeTarget::Exit(exit) => format!("exit:{exit}"),
            }
        })
        .collect()
}

#[test]
fn a_fallback_load_balancer_becomes_a_failover_in_port_order() {
    let mut b = Builder::new("prod");
    let s = b.server("tokyo");
    let ip = b.ip("ip1", &s, "203.0.113.10");
    b.named_node("pod", "edge", pod(&ip, 443), pod_ports());
    b.node(
        "entry",
        entry(Some(ProxyProtocolVersion::V2)),
        entry_ports(),
    );
    b.node("lb", distribute(LoadBalanceMode::Fallback), distribute_ports(3));
    b.node("exit_c", exit("10.0.0.7:8080"), exit_ports());
    b.node("exit_a", exit("10.0.0.5:8080"), exit_ports());
    b.node("exit_b", exit("backend.internal:9000"), exit_ports());
    b.connect("pod-listen", "entry-listen");
    b.connect("lb-destination", "pod-destination");
    b.connect("exit_c-destination", "lb-member_2");
    b.connect("exit_a-destination", "lb-member_0");
    b.connect("exit_b-destination", "lb-member_1");

    let converted = converted_alike(&b.build(), &no_material());
    let pod = pod_named(&converted, "edge");
    assert_eq!(pod.id.as_str(), "pod", "a pod keeps its node's id");
    assert_eq!(
        pod.ingress,
        PodIngress::ClientRaw {
            receive_proxy_protocol: Some(ProxyProtocolVersion::V2)
        }
    );
    let route = pod.route.as_ref().unwrap();
    assert!(matches!(route, Route::Failover(members) if members.len() == 3));
    assert_eq!(
        leaf_targets(&converted, route),
        ["exit:exit_a", "exit:exit_b", "exit:exit_c"]
    );
    assert_eq!(converted.rows.exits.len(), 3);
    assert_eq!(converted.rows.groups.len(), 1);
    assert_eq!(converted.rows.groups[0].kind, "splitter");
}

#[test]
fn nested_modes_become_nested_balances() {
    let mut b = Builder::new("prod");
    let s = b.server("tokyo");
    let ip = b.ip("ip1", &s, "203.0.113.10");
    b.named_node("pod", "edge", pod(&ip, 443), pod_ports());
    b.node(
        "entry",
        entry(Some(ProxyProtocolVersion::V1)),
        entry_ports(),
    );
    b.node("outer", distribute(LoadBalanceMode::RoundRobin), distribute_ports(2));
    b.node("sticky", distribute(LoadBalanceMode::IpHash), distribute_ports(2));
    b.node("random", distribute(LoadBalanceMode::Random), distribute_ports(1));
    b.node("exit_a", exit("10.0.0.5:8080"), exit_ports());
    b.node("exit_b", exit("10.0.0.6:8080"), exit_ports());
    b.node("exit_c", exit("10.0.0.7:8080"), exit_ports());
    b.connect("pod-listen", "entry-listen");
    b.connect("outer-destination", "pod-destination");
    b.connect("sticky-destination", "outer-member_0");
    b.connect("random-destination", "outer-member_1");
    b.connect("exit_a-destination", "sticky-member_0");
    b.connect("exit_b-destination", "sticky-member_1");
    b.connect("exit_c-destination", "random-member_0");

    let converted = converted_alike(&b.build(), &no_material());
    let route = pod_named(&converted, "edge").route.clone().unwrap();
    let Route::Balance { members, sticky } = &route else {
        panic!("{route:?}");
    };
    assert_eq!(*sticky, None);
    assert!(matches!(
        &members[0].to,
        Route::Balance { sticky: Some(Sticky::ClientIp), members } if members.len() == 2
    ));
    assert!(matches!(
        &members[1].to,
        Route::Balance { sticky: None, members } if members.len() == 1
    ));
}

#[test]
fn an_aggregate_gives_each_pod_its_own_edge_to_the_source() {
    let mut b = Builder::new("prod");
    let s = b.server("tokyo");
    let ip = b.ip("ip1", &s, "203.0.113.10");
    b.named_node("pod_a", "alpha", pod(&ip, 443), pod_ports());
    b.named_node("pod_b", "beta", pod(&ip, 8443), pod_ports());
    b.node("entry_a", entry(None), entry_ports());
    b.node("entry_b", entry(None), entry_ports());
    b.node(
        "agg",
        NodeSpec::LoadBalanceAggregate(LoadBalanceAggregateConfig::default()),
        aggregate_ports(2),
    );
    b.node("exit", exit("10.0.0.5:8080"), exit_ports());
    b.connect("pod_a-listen", "entry_a-listen");
    b.connect("pod_b-listen", "entry_b-listen");
    b.connect("agg-copy_0", "pod_a-destination");
    b.connect("agg-copy_1", "pod_b-destination");
    b.connect("exit-destination", "agg-source");

    let converted = converted_alike(&b.build(), &no_material());
    for name in ["alpha", "beta"] {
        let route = pod_named(&converted, name).route.clone().unwrap();
        assert_eq!(leaf_targets(&converted, &route), ["exit:exit"]);
    }
    assert_eq!(converted.rows.edges.len(), 2);
    let aggregator = &converted.rows.groups[0];
    assert_eq!(aggregator.kind, "aggregator");
    assert_eq!(aggregator.members.len(), 5, "{:?}", aggregator.members);
}

/// tokyo -> osaka -> singapore over raw TCP; the osaka hop dials an override.
fn relay_chain() -> (Builder, [ServerId; 3]) {
    let mut b = Builder::new("prod");
    let tokyo = b.server("tokyo");
    let osaka = b.server("osaka");
    let singapore = b.server("singapore");
    let ip_tokyo = b.ip("ip_tokyo", &tokyo, "203.0.113.10");
    let ip_osaka = b.ip("ip_osaka", &osaka, "198.51.100.10");
    let ip_sg = b.ip("ip_sg", &singapore, "192.0.2.10");
    b.named_node("pod_tokyo", "ingress", pod(&ip_tokyo, 443), pod_ports());
    b.node("entry", entry(None), entry_ports());
    b.named_node(
        "relay_osaka",
        "to-osaka",
        NodeSpec::Relay(RelayConfig {
            protocol: RelayProtocol::TcpRaw,
            override_ip_address: Some("2001:db8::10".to_string()),
            override_port: Some(19443),
        }),
        relay_ports(),
    );
    b.named_node("pod_osaka", "osaka-hop", pod(&ip_osaka, 9443), pod_ports());
    b.named_node(
        "relay_sg",
        "to-singapore",
        relay(RelayProtocol::TcpRaw),
        relay_ports(),
    );
    b.named_node("pod_sg", "singapore-hop", pod(&ip_sg, 9443), pod_ports());
    b.node("exit", exit("10.0.0.5:8080"), exit_ports());
    b.connect("pod_tokyo-listen", "entry-listen");
    b.connect("relay_osaka-destination", "pod_tokyo-destination");
    b.connect("pod_osaka-listen", "relay_osaka-listen");
    b.connect("relay_sg-destination", "pod_osaka-destination");
    b.connect("pod_sg-listen", "relay_sg-listen");
    b.connect("exit-destination", "pod_sg-destination");
    (b, [tokyo, osaka, singapore])
}

#[test]
fn a_relay_becomes_an_edge_to_the_pod_it_dials() {
    let (b, _) = relay_chain();
    let converted = converted_alike(&b.build(), &no_material());
    let tokyo = pod_named(&converted, "ingress");
    let route = tokyo.route.clone().unwrap();
    assert_eq!(leaf_targets(&converted, &route), ["pod:pod_osaka"]);
    let edge = converted
        .rows
        .edges
        .iter()
        .find(|e| e.source.as_str() == "pod_tokyo")
        .unwrap();
    assert_eq!(edge.override_ip.as_deref(), Some("2001:db8::10"));
    assert_eq!(edge.override_port, Some(19443));
    assert_eq!(pod_named(&converted, "osaka-hop").ingress, PodIngress::RelayTcp);
    assert!(converted.rows.groups.is_empty());
}

/// tokyo (entry) relays to osaka over `protocol`, osaka exits.
fn secure_relay(protocol: RelayProtocol) -> (Builder, ServerId, ServerId) {
    let mut b = Builder::new("prod");
    let tokyo = b.server("tokyo");
    let osaka = b.server("osaka");
    let ip_tokyo = b.ip("ip_tokyo", &tokyo, "203.0.113.10");
    let ip_osaka = b.ip("ip_osaka", &osaka, "198.51.100.10");
    b.named_node("pod_tokyo", "ingress", pod(&ip_tokyo, 443), pod_ports());
    b.node("entry", entry(None), entry_ports());
    b.named_node("relay_osaka", "to-osaka", relay(protocol), relay_ports());
    b.named_node("pod_osaka", "osaka-hop", pod(&ip_osaka, 9443), pod_ports());
    b.node("exit", exit("10.0.0.5:8080"), exit_ports());
    b.connect("pod_tokyo-listen", "entry-listen");
    b.connect("relay_osaka-destination", "pod_tokyo-destination");
    b.connect("pod_osaka-listen", "relay_osaka-listen");
    b.connect("exit-destination", "pod_osaka-destination");
    (b, tokyo, osaka)
}

#[test]
fn secure_relays_convert_with_and_without_their_material() {
    for (protocol, ingress) in [
        (RelayProtocol::TcpTls, PodIngress::RelayTls),
        (RelayProtocol::Quic, PodIngress::RelayQuic),
    ] {
        let (mut b, tokyo, osaka) = secure_relay(protocol);
        b.server_quic(
            &tokyo,
            ServerQuic {
                congestion: QuicCongestion::Brutal,
                up_mbps: 200,
                down_mbps: 2000,
                stream_receive_window: 67_108_864,
                conn_receive_window: 0,
            },
        );
        b.server_quic(
            &osaka,
            ServerQuic {
                congestion: QuicCongestion::Brutal,
                up_mbps: 1000,
                down_mbps: 100,
                stream_receive_window: 0,
                conn_receive_window: 0,
            },
        );
        let topology = b.build();
        // Nothing issued: the same pods are invalid on both sides.
        converted_alike(&topology, &no_material());
        // A CA but no leaf.
        converted_alike(
            &topology,
            &Material {
                acme: Vec::new(),
                relay: Vec::new(),
                internal_ca: true,
            },
        );
        let converted = converted_alike(
            &topology,
            &Material {
                acme: Vec::new(),
                relay: vec![relay_leaf("leaf1", "pod_osaka", 2)],
                internal_ca: true,
            },
        );
        assert_eq!(pod_named(&converted, "osaka-hop").ingress, ingress);
    }
}

#[test]
fn a_tls_entry_converts_with_its_certificate() {
    let mut b = Builder::new("prod");
    let s = b.server("tokyo");
    let ip = b.ip("ip1", &s, "203.0.113.10");
    b.node("pod", pod(&ip, 443), pod_ports());
    b.node(
        "entry",
        NodeSpec::Entry(EntryConfig {
            receive_proxy_protocol: None,
            tls: Some(TlsConfig {
                sni: "example.com".to_string(),
                dns_provider: ids::dns_provider_id("cf"),
                domain_id: "zone".to_string(),
                acme_directory: String::new(),
            }),
        }),
        entry_ports(),
    );
    b.node("exit", exit("10.0.0.5:8080"), exit_ports());
    b.connect("pod-listen", "entry-listen");
    b.connect("exit-destination", "pod-destination");
    let topology = b.build();

    converted_alike(&topology, &no_material());
    let directory = OrchestrationConfig::default().default_acme_directory;
    let converted = converted_alike(
        &topology,
        &Material {
            acme: vec![certificate("cert1", "example.com", &directory, 3)],
            relay: Vec::new(),
            internal_ca: false,
        },
    );
    let pod = pod_named(&converted, "pod");
    assert!(matches!(
        &pod.ingress,
        PodIngress::ClientTls { tls, .. } if tls.sni == "example.com" && tls.acme_directory.is_empty()
    ));
}

/// root -> sub -> subsub, a destination crossing both boundaries.
#[test]
fn imports_become_parents_and_boundaries_disappear() {
    let dest_out = || {
        (
            export_spec(
                PortKind::DeriveDestination,
                CanvasExportAs::OutputOutOfCanvas,
            ),
            export_ports(
                PortKind::DeriveDestination,
                CanvasExportAs::OutputOutOfCanvas,
            ),
        )
    };
    let mut b = Builder::new("root");
    let a = b.server("a");
    let ip_a = b.ip("ip_a", &a, "203.0.113.10");
    b.named_node("pod_a", "deep", pod(&ip_a, 443), pod_ports());
    b.node("entry_a", entry(None), entry_ports());
    b.node(
        "import_sub",
        import_spec("sub"),
        import_ports(&[(
            "sub_out",
            PortKind::DeriveDestination,
            CanvasExportAs::OutputOutOfCanvas,
        )]),
    );
    b.connect("pod_a-listen", "entry_a-listen");
    b.connect("import_sub-sub_out", "pod_a-destination");

    b.canvas("sub");
    let (spec, ports) = dest_out();
    b.node("sub_out", spec, ports);
    b.node(
        "import_subsub",
        import_spec("subsub"),
        import_ports(&[(
            "subsub_out",
            PortKind::DeriveDestination,
            CanvasExportAs::OutputOutOfCanvas,
        )]),
    );
    b.connect("import_subsub-subsub_out", "sub_out-export");

    b.canvas("subsub");
    let (spec, ports) = dest_out();
    b.node("subsub_out", spec, ports);
    b.node("exit_deep", exit("10.0.0.5:8080"), exit_ports());
    b.connect("exit_deep-destination", "subsub_out-export");

    let converted = converted_alike(&b.build(), &no_material());
    let mut placements: Vec<(String, String)> = converted
        .rows
        .placements
        .iter()
        .map(|p| (p.canvas.to_string(), p.parent.to_string()))
        .collect();
    placements.sort();
    assert_eq!(
        placements,
        [
            ("sub".to_string(), "root".to_string()),
            ("subsub".to_string(), "sub".to_string())
        ]
    );
    let route = pod_named(&converted, "deep").route.clone().unwrap();
    assert_eq!(leaf_targets(&converted, &route), ["exit:exit_deep"]);
    let exit = &converted.rows.exits[0];
    assert_eq!(exit.canvas.as_str(), "subsub", "an exit stays on its canvas");
}

#[test]
fn a_chain_that_leads_nowhere_leaves_the_pod_without_a_route_and_says_so() {
    let mut b = Builder::new("prod");
    let s = b.server("tokyo");
    let ip = b.ip("ip1", &s, "203.0.113.10");
    b.named_node("pod", "edge", pod(&ip, 443), pod_ports());
    b.node("entry", entry(None), entry_ports());
    b.named_node("relay", "nowhere", relay(RelayProtocol::TcpRaw), relay_ports());
    b.connect("pod-listen", "entry-listen");
    b.connect("relay-destination", "pod-destination");
    let topology = b.build();

    let converted = convert_tree(&topology);
    assert!(pod_named(&converted, "edge").route.is_none());
    assert!(converted.rows.edges.is_empty());
    assert!(
        converted
            .notes
            .iter()
            .any(|n| n.contains("relay nowhere dials no pod")),
        "{:?}",
        converted.notes
    );
    // The node model called the pod invalid, which kept its listener; the graph
    // simply has nothing to compile for it. The comparison says so.
    let comparison = compare(
        &topology,
        &converted.rows,
        &[],
        &[],
        false,
        &OrchestrationConfig::default(),
    );
    assert_eq!(comparison.differences.len(), 1, "{:?}", comparison.differences);
}

#[test]
fn an_exit_the_graph_rejects_is_pruned_with_the_routes_to_it() {
    let mut b = Builder::new("prod");
    let s = b.server("tokyo");
    let ip = b.ip("ip1", &s, "203.0.113.10");
    b.named_node("pod", "edge", pod(&ip, 443), pod_ports());
    b.node("entry", entry(None), entry_ports());
    b.node("exit", exit("not an address"), exit_ports());
    b.connect("pod-listen", "entry-listen");
    b.connect("exit-destination", "pod-destination");

    let converted = convert_tree(&b.build());
    assert!(converted.rows.exits.is_empty());
    assert!(converted.rows.edges.is_empty());
    assert!(pod_named(&converted, "edge").route.is_none());
    assert!(
        converted.notes.iter().any(|n| n.contains("exit exit dropped")),
        "{:?}",
        converted.notes
    );
}
