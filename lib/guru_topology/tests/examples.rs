//! The worked examples the topology was designed against, one test each.

mod common;

use common::*;
use guru_topology::{
    CertificateKind, CertificateRef, Diagnostic, EdgeId, Invalid, IpFamily, ListenProtocol,
    Listener, PodId, Problem, Report, Route, ServerId, Severity, Subject, check, compile,
};
use guru_worker_config::table::{Policy, Target, Weighted};
use guru_worker_config::{
    Config, ForwardingTo, Ipv6Resolve, KeepAlive, ListenAs, LoadBalanceStrategy, LogConfig,
    QuicTuning, RelayHost, RelayProtocol, Remote, TcpProxyProtocol, TlsHostConfig,
};
use std::path::PathBuf;

fn socket(text: &str) -> Remote {
    Remote::Address(text.parse().unwrap())
}

fn weights(members: &[(&str, u32)]) -> Vec<Weighted> {
    members
        .iter()
        .map(|(to, weight)| Weighted {
            to: to.to_string(),
            weight: *weight,
        })
        .collect()
}

fn names(ids: &[&str]) -> Vec<String> {
    ids.iter().map(|id| id.to_string()).collect()
}

/// The tree form of a server, as a worker would read it back.
fn round_trip_legacy(forwardings: &[guru_worker_config::Forwarding]) {
    let config = Config {
        ipv6_resolve: Ipv6Resolve::default(),
        log: LogConfig::default(),
        relay_ca: Some(PathBuf::from("certs/ca.pem")),
        keepalive: KeepAlive::default(),
        quic: QuicTuning::default(),
        forwardings: forwardings.to_vec(),
    };
    let text = config.to_toml_string().unwrap();
    let parsed = Config::from_toml_str(&text).unwrap();
    assert_eq!(parsed, config, "emitted:\n{text}");
}

fn errors(fabric: &Fabric) -> Report {
    let report = check(&fabric.graph);
    assert!(
        compile(&fabric.graph, &fabric.certificates).is_err(),
        "a graph with errors must not compile: {report:#?}"
    );
    report
}

// --- 1. China Mobile Cloud over five Gcore and four AWS servers -------------

fn mobile_cloud(route_table: bool) -> Fabric {
    let mut f = Fabric::new();
    let server = |f: &mut Fabric, id: &str, address: &str| {
        if route_table {
            f.server(id, address);
        } else {
            f.legacy_server(id, address);
        }
    };
    server(&mut f, "mobile", "198.51.100.1");
    f.pod("p", "mobile", 443, raw()).exit("x", "10.0.0.5:8080");
    for i in 1..=5 {
        let (srv, pod) = (format!("gc{i}"), format!("gc{i}-in"));
        server(&mut f, &srv, &format!("203.0.113.{i}"));
        f.pod(&pod, &srv, 40000, guru_topology::Ingress::RelayQuic)
            .edge(&format!("p-gc{i}"), "p", &pod)
            .edge(&format!("gc{i}-x"), &pod, "x")
            .route(&pod, leaf(&format!("gc{i}-x")));
    }
    for i in 1..=4 {
        let (srv, pod) = (format!("aws{i}"), format!("aws{i}-in"));
        server(&mut f, &srv, &format!("192.0.2.{i}"));
        f.pod(&pod, &srv, 40000, guru_topology::Ingress::RelayTcp)
            .edge(&format!("p-aws{i}"), "p", &pod)
            .edge(&format!("aws{i}-x"), &pod, "x")
            .route(&pod, leaf(&format!("aws{i}-x")));
    }
    let gcore: Vec<Route> = (1..=5).map(|i| leaf(&format!("p-gc{i}"))).collect();
    let aws: Vec<Route> = (1..=4).map(|i| leaf(&format!("p-aws{i}"))).collect();
    f.route("p", failover(vec![balance(gcore), balance(aws)]));
    f
}

#[test]
fn mobile_cloud_fails_over_from_gcore_to_aws() {
    let compiled = mobile_cloud(true).compile();
    assert!(compiled.warnings.is_empty(), "{:#?}", compiled.warnings);

    let mobile = table(&compiled, "mobile");
    assert_eq!(mobile.len(), 1);
    let p = entry(mobile, "p");
    assert_eq!(route_to(p), "g");
    assert_eq!(
        group(p, "g").policy,
        Policy::Failover {
            members: names(&["g.0", "g.1"])
        }
    );
    let gcore: Vec<String> = (1..=5).map(|i| format!("u:p-gc{i}")).collect();
    let gcore: Vec<(&str, u32)> = gcore.iter().map(|id| (id.as_str(), 1)).collect();
    assert_eq!(
        group(p, "g.0").policy,
        Policy::Balance {
            members: weights(&gcore),
            sticky: None
        }
    );
    let aws: Vec<String> = (1..=4).map(|i| format!("u:p-aws{i}")).collect();
    let aws: Vec<(&str, u32)> = aws.iter().map(|id| (id.as_str(), 1)).collect();
    assert_eq!(
        group(p, "g.1").policy,
        Policy::Balance {
            members: weights(&aws),
            sticky: None
        }
    );

    let gc3 = relay_target(upstream(p, "p-gc3"));
    assert_eq!(gc3.protocol, RelayProtocol::Quic);
    assert_eq!(gc3.destination, socket("203.0.113.3:40000"));
    assert_eq!(gc3.sni.as_deref(), Some("gc3-in.relay.guru.internal"));
    assert!(gc3.confirm);
    assert_eq!(
        gc3.quic, None,
        "equal rates on both sides need no link tuning"
    );

    let aws2 = relay_target(upstream(p, "p-aws2"));
    assert_eq!(aws2.protocol, RelayProtocol::Tcp);
    assert_eq!(aws2.destination, socket("192.0.2.2:40000"));
    assert_eq!(aws2.sni, None);

    let deps = &compiled.servers[&ServerId::new("mobile")].deps[0];
    assert_eq!(deps.pod, PodId::new("p"));
    assert_eq!(deps.points_at.len(), 9);
    let quic = deps
        .points_at
        .iter()
        .filter(|l| l.protocol == ListenProtocol::RelayQuic)
        .count();
    assert_eq!((quic, deps.points_at.len() - quic), (5, 4));

    let gc2 = entry(table(&compiled, "gc2"), "gc2-in");
    assert_eq!(
        gc2.listen_as,
        ListenAs::Relay(RelayHost::Quic(TlsHostConfig {
            key: PathBuf::from("certs/relay/gc2-in/key.pem"),
            full_chain: PathBuf::from("certs/relay/gc2-in/full_chain.pem"),
        }))
    );
    assert_eq!(route_to(gc2), "u:gc2-x");
    assert!(gc2.groups.is_empty());
    match &upstream(gc2, "gc2-x").target {
        Target::Exit(exit) => assert_eq!(exit.destination, socket("10.0.0.5:8080")),
        Target::Relay(_) => panic!("gc2-in should exit"),
    }
    assert_eq!(
        compiled.servers[&ServerId::new("gc2")].deps[0].certificates,
        vec![CertificateRef {
            kind: CertificateKind::Relay,
            key: "leaf-gc2-in".to_string(),
            version: 1,
        }]
    );
    assert_eq!(
        entry(table(&compiled, "aws1"), "aws1-in").listen_as,
        ListenAs::Relay(RelayHost::Tcp)
    );
}

#[test]
fn mobile_cloud_degrades_for_workers_without_route_tables() {
    let compiled = mobile_cloud(false).compile();
    let mobile = legacy(&compiled, "mobile");
    let ForwardingTo::LoadBalance(top) = tree(&mobile[0]) else {
        panic!("expected a group: {:#?}", mobile[0].to);
    };
    assert_eq!(top.strategy, LoadBalanceStrategy::Fallback);
    assert_eq!(top.members.len(), 2);
    for (member, (count, protocol)) in top
        .members
        .iter()
        .zip([(5, RelayProtocol::Quic), (4, RelayProtocol::Tcp)])
    {
        let ForwardingTo::LoadBalance(tier) = member else {
            panic!("expected a tier: {member:#?}");
        };
        assert_eq!(tier.strategy, LoadBalanceStrategy::RoundRobin);
        assert_eq!(tier.members.len(), count);
        assert!(tier.members.iter().all(|m| matches!(
            m,
            ForwardingTo::Relay { protocol: p, .. } if *p == protocol
        )));
    }
    round_trip_legacy(mobile);
    round_trip_legacy(legacy(&compiled, "gc1"));
}

#[test]
fn the_order_of_the_input_does_not_matter() {
    let fabric = mobile_cloud(true);
    let mut reversed = fabric.graph.clone();
    reversed.servers.reverse();
    reversed.pods.reverse();
    reversed.exits.reverse();
    reversed.edges.reverse();
    assert_eq!(
        compile(&reversed, &fabric.certificates),
        compile(&fabric.graph, &fabric.certificates)
    );
}

// --- 2. Half the traffic to each of two exits -------------------------------

#[test]
fn a_transit_pod_splits_evenly_between_two_exits() {
    let mut f = Fabric::new();
    f.server("us", "203.0.113.10")
        .server("hk", "203.0.113.20")
        .pod("p", "us", 443, raw())
        .pod("hk-in", "hk", 40000, guru_topology::Ingress::RelayTcp)
        .exit("a", "10.0.0.1:80")
        .exit("b", "10.0.0.2:80")
        .edge("p-hk", "p", "hk-in")
        .edge("hk-a", "hk-in", "a")
        .edge("hk-b", "hk-in", "b")
        .route("p", leaf("p-hk"))
        .route("hk-in", balance(leaves(&["hk-a", "hk-b"])));
    let compiled = f.compile();

    let hk = entry(table(&compiled, "hk"), "hk-in");
    assert_eq!(route_to(hk), "g");
    assert_eq!(
        group(hk, "g").policy,
        Policy::Balance {
            members: weights(&[("u:hk-a", 1), ("u:hk-b", 1)]),
            sticky: None
        }
    );
    assert_eq!(route_to(entry(table(&compiled, "us"), "p")), "u:p-hk");
}

// --- 3. Four parallel edges between two pods --------------------------------

#[test]
fn parallel_edges_are_separate_next_hops() {
    let mut f = Fabric::new();
    f.server("s1", "198.51.100.1")
        .server("s2", "192.0.2.10")
        .pod("p", "s1", 443, raw())
        .pod("q", "s2", 40001, guru_topology::Ingress::RelayTcp)
        .exit("x", "10.0.0.5:8080");
    for e in ["e1", "e2", "e3", "e4"] {
        f.edge(e, "p", "q");
    }
    f.edge("q-x", "q", "x")
        .route("p", balance(leaves(&["e1", "e2", "e3", "e4"])))
        .route("q", leaf("q-x"));
    for e in ["e3", "e4"] {
        f.edge_mut(e).override_ip = Some("2001:db8::1".to_string());
    }
    let compiled = f.compile();

    let p = entry(table(&compiled, "s1"), "p");
    assert_eq!(p.upstreams.len(), 4);
    assert_eq!(
        relay_target(upstream(p, "e1")).destination,
        socket("192.0.2.10:40001")
    );
    assert_eq!(
        relay_target(upstream(p, "e3")).destination,
        socket("[2001:db8::1]:40001")
    );
    let deps = &compiled.servers[&ServerId::new("s1")].deps[0];
    assert_eq!(
        deps.points_at,
        vec![Listener {
            server: ServerId::new("s2"),
            port: 40001,
            protocol: ListenProtocol::RelayTcp,
        }],
        "four edges to one pod depend on one listener"
    );
    assert_eq!(
        deps.edges,
        ["e1", "e2", "e3", "e4"].map(EdgeId::new).to_vec()
    );
    assert_eq!(deps.pods, vec![PodId::new("q")]);
}

// --- 4. Three failover tiers, the last one straight out ---------------------

#[test]
fn the_last_failover_tier_can_leave_directly() {
    let mut f = Fabric::new();
    f.server("mobile", "198.51.100.1")
        .pod("p", "mobile", 443, raw())
        .exit("x", "10.0.0.5:8080")
        .exit("z", "10.9.9.9:443");
    for (i, name) in ["g1", "g2", "a1", "a2"].iter().enumerate() {
        let pod = format!("{name}-in");
        f.server(name, &format!("203.0.113.{}", i + 1))
            .pod(&pod, name, 40000, guru_topology::Ingress::RelayTcp)
            .edge(&format!("p-{name}"), "p", &pod)
            .edge(&format!("{name}-x"), &pod, "x")
            .route(&pod, leaf(&format!("{name}-x")));
    }
    f.edge("p-z", "p", "z").route(
        "p",
        failover(vec![
            balance(leaves(&["p-g1", "p-g2"])),
            balance(leaves(&["p-a1", "p-a2"])),
            leaf("p-z"),
        ]),
    );
    let compiled = f.compile();

    let p = entry(table(&compiled, "mobile"), "p");
    assert_eq!(
        group(p, "g").policy,
        Policy::Failover {
            members: names(&["g.0", "g.1", "u:p-z"])
        }
    );
    assert!(matches!(upstream(p, "p-z").target, Target::Exit(_)));
    assert_eq!(
        compiled.servers[&ServerId::new("mobile")].deps[0].exits,
        vec![guru_topology::ExitId::new("z")]
    );
}

// --- 5. A balance over failovers --------------------------------------------

#[test]
fn a_balance_can_spread_over_failover_pairs() {
    let mut f = Fabric::new();
    f.server("edge", "198.51.100.1")
        .pod("p", "edge", 443, raw())
        .exit("x", "10.0.0.5:8080");
    for (i, name) in ["a", "b", "c", "d"].iter().enumerate() {
        f.server(name, &format!("203.0.113.{}", i + 1))
            .pod(name, name, 40000, guru_topology::Ingress::RelayTls)
            .edge(&format!("p-{name}"), "p", name)
            .edge(&format!("{name}-x"), name, "x")
            .route(name, leaf(&format!("{name}-x")));
    }
    f.route(
        "p",
        balance(vec![
            failover(leaves(&["p-a", "p-b"])),
            failover(leaves(&["p-c", "p-d"])),
        ]),
    );
    let compiled = f.compile();

    let p = entry(table(&compiled, "edge"), "p");
    let ids: Vec<&str> = p.groups.iter().map(|g| g.id.as_str()).collect();
    assert_eq!(
        ids,
        ["g", "g.0", "g.1"],
        "parents come before their members"
    );
    assert_eq!(
        group(p, "g").policy,
        Policy::Balance {
            members: weights(&[("g.0", 1), ("g.1", 1)]),
            sticky: None
        }
    );
    assert_eq!(
        group(p, "g.1").policy,
        Policy::Failover {
            members: names(&["u:p-c", "u:p-d"])
        }
    );
    let tls = relay_target(upstream(p, "p-a"));
    assert_eq!(tls.protocol, RelayProtocol::TlsOverTcp);
    assert_eq!(tls.sni.as_deref(), Some("a.relay.guru.internal"));
}

// --- 6. Graphs that must be refused -----------------------------------------

/// Two servers, a client pod `p` on `s1` and a relay pod `q` on `s2`, and an
/// exit `x`, nothing connected yet.
fn two_pods() -> Fabric {
    let mut f = Fabric::new();
    f.server("s1", "198.51.100.1")
        .server("s2", "192.0.2.10")
        .pod("p", "s1", 443, raw())
        .pod("q", "s2", 40000, guru_topology::Ingress::RelayTcp)
        .exit("x", "10.0.0.5:8080");
    f
}

#[test]
fn a_loop_between_pods_is_refused() {
    let mut f = Fabric::new();
    f.server("s1", "198.51.100.1")
        .server("s2", "192.0.2.10")
        .pod("a", "s1", 40000, guru_topology::Ingress::RelayTcp)
        .pod("b", "s2", 40000, guru_topology::Ingress::RelayTcp)
        .edge("ab", "a", "b")
        .edge("ba", "b", "a")
        .route("a", leaf("ab"))
        .route("b", leaf("ba"));
    let report = errors(&f);
    let cycle = report
        .diagnostics
        .iter()
        .find(|d| d.problem == Problem::Cycle)
        .unwrap();
    assert_eq!(
        cycle.subjects,
        vec![
            Subject::Pod(PodId::new("a")),
            Subject::Pod(PodId::new("b")),
            Subject::Edge(EdgeId::new("ab")),
            Subject::Edge(EdgeId::new("ba")),
        ]
    );

    let mut own = Fabric::new();
    own.server("s1", "198.51.100.1")
        .pod("a", "s1", 40000, guru_topology::Ingress::RelayTcp)
        .edge("aa", "a", "a")
        .route("a", leaf("aa"));
    assert!(errors(&own).has(Problem::Cycle));
}

#[test]
fn a_route_covers_exactly_the_pods_out_edges() {
    let mut missing = two_pods();
    missing
        .edge("e1", "p", "q")
        .edge("e2", "p", "x")
        .edge("q-x", "q", "x")
        .route("p", leaf("e1"))
        .route("q", leaf("q-x"));
    assert!(errors(&missing).has(Problem::RouteMissingEdge));

    let mut twice = two_pods();
    twice
        .edge("e1", "p", "q")
        .edge("q-x", "q", "x")
        .route("p", balance(leaves(&["e1", "e1"])))
        .route("q", leaf("q-x"));
    assert!(errors(&twice).has(Problem::RouteDuplicateEdge));

    let mut foreign = two_pods();
    foreign
        .edge("e1", "p", "q")
        .edge("q-x", "q", "x")
        .route("p", balance(leaves(&["e1", "q-x"])))
        .route("q", leaf("q-x"));
    assert!(errors(&foreign).has(Problem::RouteForeignEdge));

    let mut unknown = two_pods();
    unknown
        .edge("e1", "p", "q")
        .edge("q-x", "q", "x")
        .route("p", balance(leaves(&["e1", "nope"])))
        .route("q", leaf("q-x"));
    assert!(errors(&unknown).has(Problem::RouteUnknownEdge));

    let mut none = two_pods();
    none.edge("e1", "p", "q")
        .edge("q-x", "q", "x")
        .route("q", leaf("q-x"));
    assert!(errors(&none).has(Problem::MissingRoute));
}

#[test]
fn a_client_pod_cannot_be_dialed() {
    let mut f = two_pods();
    f.pod("r", "s2", 443, raw())
        .edge("p-r", "p", "r")
        .edge("r-x", "r", "x")
        .route("p", leaf("p-r"))
        .route("r", leaf("r-x"));
    assert!(errors(&f).has(Problem::ClientPodDialed));
}

#[test]
fn two_pods_cannot_share_a_socket() {
    let conflict = |bind_a: Option<&str>, bind_b: Option<&str>, quic_b: bool| {
        let mut f = Fabric::new();
        f.server("s1", "198.51.100.1")
            .server("s2", "192.0.2.10")
            .pod("dialer", "s2", 443, raw())
            .pod("a", "s1", 8443, guru_topology::Ingress::RelayTcp)
            .pod(
                "b",
                "s1",
                8443,
                if quic_b {
                    guru_topology::Ingress::RelayQuic
                } else {
                    guru_topology::Ingress::RelayTcp
                },
            )
            .exit("x", "10.0.0.5:8080")
            .edge("d-a", "dialer", "a")
            .edge("d-b", "dialer", "b")
            .edge("a-x", "a", "x")
            .edge("b-x", "b", "x")
            .route("dialer", balance(leaves(&["d-a", "d-b"])))
            .route("a", leaf("a-x"))
            .route("b", leaf("b-x"));
        f.pod_mut("a").bind_ip = bind_a.map(str::to_string);
        f.pod_mut("b").bind_ip = bind_b.map(str::to_string);
        check(&f.graph).has(Problem::ListenerConflict)
    };
    assert!(conflict(None, None, false), "two wildcards");
    assert!(
        conflict(None, Some("10.0.0.1"), false),
        "a wildcard covers a literal"
    );
    assert!(
        conflict(Some("0.0.0.0"), Some("10.0.0.1"), false),
        "0.0.0.0 is a wildcard"
    );
    assert!(
        conflict(Some("10.0.0.1"), Some("10.0.0.1"), false),
        "the same literal"
    );
    assert!(
        conflict(Some("2001:db8::1"), Some("2001:0db8:0:0:0:0:0:1"), false),
        "two spellings of one address"
    );
    assert!(
        !conflict(Some("10.0.0.1"), Some("10.0.0.2"), false),
        "two literals"
    );
    assert!(
        !conflict(None, None, true),
        "TCP and QUIC use different sockets"
    );
}

#[test]
fn sticky_balancing_needs_the_client_address() {
    let sticky_pod = |ingress: guru_topology::Ingress| {
        let mut f = two_pods();
        f.pod("r", "s2", 40001, guru_topology::Ingress::RelayTcp)
            .edge("p-q", "p", "q")
            .edge("p-r", "p", "r")
            .edge("q-x", "q", "x")
            .edge("r-x", "r", "x")
            .route("p", sticky(balance(leaves(&["p-q", "p-r"]))))
            .route("q", leaf("q-x"))
            .route("r", leaf("r-x"));
        f.pod_mut("p").ingress = ingress;
        check(&f.graph).has(Problem::StickyWithoutClientIp)
    };
    assert!(sticky_pod(raw()));
    assert!(!sticky_pod(guru_topology::Ingress::ClientRaw {
        receive_proxy_protocol: Some(TcpProxyProtocol::V2)
    }));

    let mut relayed = two_pods();
    relayed
        .pod("r", "s2", 40001, guru_topology::Ingress::RelayTcp)
        .exit("y", "10.0.0.6:8080")
        .edge("p-q", "p", "q")
        .edge("q-x", "q", "x")
        .edge("q-y", "q", "y")
        .route("p", leaf("p-q"))
        .route("q", sticky(balance(leaves(&["q-x", "q-y"]))));
    assert!(!check(&relayed.graph).has(Problem::StickyWithoutClientIp));
}

#[test]
fn groups_need_members_and_weights() {
    let mut empty = two_pods();
    empty
        .edge("p-x", "p", "x")
        .route("p", failover(vec![leaf("p-x"), balance(vec![])]));
    assert!(errors(&empty).has(Problem::EmptyGroup));

    let mut zero = two_pods();
    zero.edge("p-x", "p", "x")
        .edge("p-q", "p", "q")
        .edge("q-x", "q", "x")
        .route("p", weighted(vec![(0, leaf("p-x")), (1, leaf("p-q"))]))
        .route("q", leaf("q-x"));
    assert!(errors(&zero).has(Problem::ZeroWeight));
}

#[test]
fn broken_references_and_values_are_refused() {
    let mut from_exit = two_pods();
    from_exit
        .edge("p-x", "p", "x")
        .edge("x-q", "x", "q")
        .route("p", leaf("p-x"));
    assert!(errors(&from_exit).has(Problem::EdgeFromExit));

    let mut values = two_pods();
    values
        .pod(
            "t",
            "s1",
            0,
            guru_topology::Ingress::ClientTls {
                receive_proxy_protocol: None,
                sni: "localhost".to_string(),
                acme_directory: "https://acme.example/directory".to_string(),
            },
        )
        .exit("bad", "no-port")
        .edge("p-q", "p", "q")
        .edge("q-x", "q", "x")
        .edge("t-bad", "t", "bad")
        .route("p", leaf("p-q"))
        .route("q", leaf("q-x"))
        .route("t", leaf("t-bad"));
    values.pod_mut("p").bind_ip = Some("nope".to_string());
    values.pod_mut("q").advertise_ip = Some("300.1.1.1".to_string());
    values.edge_mut("p-q").override_ip = Some("has space".to_string());
    values.edge_mut("q-x").override_port = Some(0);
    let report = errors(&values);
    for problem in [
        Problem::InvalidPort,
        Problem::InvalidSni,
        Problem::InvalidExitDestination,
        Problem::InvalidBindIp,
        Problem::InvalidAdvertiseIp,
        Problem::InvalidOverrideAddress,
        Problem::InvalidOverridePort,
    ] {
        assert!(report.has(problem), "{problem:?} missing from {report:#?}");
    }

    let mut dangling = two_pods();
    dangling
        .pod("lost", "nowhere", 80, raw())
        .edge("p-gone", "p", "gone")
        .route("p", leaf("p-gone"));
    dangling.graph.pods.push(dangling.graph.pods[0].clone());
    let report = errors(&dangling);
    for problem in [
        Problem::UnknownServer,
        Problem::UnknownEdgeTarget,
        Problem::DuplicateId,
    ] {
        assert!(report.has(problem), "{problem:?} missing from {report:#?}");
    }
}

#[test]
fn an_absurdly_deep_route_is_reported_not_walked() {
    let mut route = leaf("p-x");
    for _ in 0..200 {
        route = failover(vec![route]);
    }
    let mut f = two_pods();
    f.edge("p-x", "p", "x").route("p", route);
    assert!(errors(&f).has(Problem::RouteTooDeep));
}

#[test]
fn warnings_never_block_compiling() {
    let mut f = two_pods();
    f.server("s3", "192.0.2.30")
        .pod("idle", "s3", 40000, guru_topology::Ingress::RelayTcp)
        .exit("unused", "10.0.0.9:80")
        .edge("p-q", "p", "q")
        .edge("q-x", "q", "x")
        .route("p", failover(vec![leaf("p-q")]))
        .route("q", leaf("q-x"));
    let report = check(&f.graph);
    assert!(!report.has_errors(), "{report:#?}");
    for problem in [
        Problem::SingleTierFailover,
        Problem::PodWithoutEdges,
        Problem::RelayPodNotDialed,
        Problem::ExitNotReached,
    ] {
        assert!(report.has(problem), "{problem:?} missing from {report:#?}");
    }
    let compiled = f.compile();
    assert_eq!(compiled.warnings, report.diagnostics);
    assert!(
        compiled.servers[&ServerId::new("s3")]
            .forwardings
            .is_empty()
    );
}

// --- 7. Pods that pass the check but cannot run yet -------------------------

#[test]
fn a_pending_certificate_invalidates_only_its_pod() {
    let directory = "https://acme.example/directory";
    let mut f = two_pods();
    f.pod(
        "t",
        "s1",
        8443,
        guru_topology::Ingress::ClientTls {
            receive_proxy_protocol: None,
            sni: "example.com".to_string(),
            acme_directory: directory.to_string(),
        },
    )
    .edge("t-x", "t", "x")
    .edge("p-x", "p", "x")
    .route("t", leaf("t-x"))
    .route("p", leaf("p-x"));

    let compiled = f.compile();
    let s1 = &compiled.servers[&ServerId::new("s1")];
    assert_eq!(s1.tags(), ["p"]);
    assert_eq!(s1.invalid.len(), 1);
    assert_eq!(s1.invalid[0].pod, PodId::new("t"));
    assert!(matches!(
        s1.invalid[0].reason,
        Invalid::CertificatePending { .. }
    ));

    f.certificates.acme.insert(
        ("example.com".to_string(), directory.to_string()),
        CertificateRef {
            kind: CertificateKind::Acme,
            key: "cert-1".to_string(),
            version: 3,
        },
    );
    let compiled = f.compile();
    let t = entry(table(&compiled, "s1"), "t");
    assert_eq!(
        t.listen_as,
        ListenAs::Tls(TlsHostConfig {
            key: PathBuf::from("certs/acme/cert-1/key.pem"),
            full_chain: PathBuf::from("certs/acme/cert-1/full_chain.pem"),
        })
    );
}

#[test]
fn a_server_without_an_address_invalidates_the_pods_dialing_it() {
    let mut f = Fabric::new();
    f.server("s1", "198.51.100.1")
        .server_with("s2", None, guru_topology::Capabilities::default())
        .pod("p", "s1", 443, raw())
        .pod("q", "s2", 40000, guru_topology::Ingress::RelayTcp)
        .exit("x", "10.0.0.5:8080")
        .edge("p-q", "p", "q")
        .edge("q-x", "q", "x")
        .route("p", leaf("p-q"))
        .route("q", leaf("q-x"));

    let compiled = f.compile();
    let s1 = &compiled.servers[&ServerId::new("s1")];
    assert!(s1.forwardings.is_empty());
    assert!(matches!(
        &s1.invalid[0].reason,
        Invalid::TargetWithoutAddress { pod, .. } if pod.as_str() == "q"
    ));
    assert_eq!(
        compiled.servers[&ServerId::new("s2")].tags(),
        ["q"],
        "q's own listener does not need its address"
    );

    f.edge_mut("p-q").override_ip = Some("192.0.2.99".to_string());
    let compiled = f.compile();
    assert_eq!(
        relay_target(upstream(entry(table(&compiled, "s1"), "p"), "p-q")).destination,
        socket("192.0.2.99:40000")
    );
}

#[test]
fn relays_over_tls_or_quic_need_the_internal_ca_and_a_leaf() {
    let build = || {
        let mut f = two_pods();
        f.pod("l", "s2", 40001, guru_topology::Ingress::RelayQuic)
            .edge("p-l", "p", "l")
            .edge("l-x", "l", "x")
            .route("p", leaf("p-l"))
            .route("l", leaf("l-x"));
        f
    };

    let mut no_ca = build();
    no_ca.certificates.internal_ca = false;
    let compiled = no_ca.compile();
    for server in ["s1", "s2"] {
        let config = &compiled.servers[&ServerId::new(server)];
        assert!(
            matches!(config.invalid[0].reason, Invalid::InternalCaMissing),
            "{server}: {config:#?}"
        );
    }

    let mut no_leaf = build();
    no_leaf.certificates.relay.clear();
    let compiled = no_leaf.compile();
    assert_eq!(compiled.servers[&ServerId::new("s1")].tags(), ["p"]);
    assert!(matches!(
        compiled.servers[&ServerId::new("s2")].invalid[0].reason,
        Invalid::RelayCertificateMissing
    ));

    no_leaf.certificates.assume_issued = true;
    let compiled = no_leaf.compile();
    assert_eq!(compiled.servers[&ServerId::new("s2")].tags(), ["l"]);
}

// --- 8. QUIC rates paired across a link -------------------------------------

#[test]
fn a_quic_listener_pairs_with_the_slowest_of_its_dialers() {
    let mut f = Fabric::new();
    f.server("b", "203.0.113.1")
        .server("s1", "198.51.100.1")
        .server("s2", "198.51.100.2")
        .server("s3", "198.51.100.3")
        .quic("b", 1000, 1000)
        .quic("s1", 300, 500)
        .quic("s2", 200, 800)
        .quic("s3", 2000, 2000)
        .pod("l", "b", 40000, guru_topology::Ingress::RelayQuic)
        .exit("x", "10.0.0.5:8080")
        .edge("l-x", "l", "x")
        .route("l", leaf("l-x"));
    for dialer in ["s1", "s2", "s3"] {
        let pod = format!("{dialer}-p");
        f.pod(&pod, dialer, 443, raw())
            .edge(&format!("{dialer}-l"), &pod, "l")
            .route(&pod, leaf(&format!("{dialer}-l")));
    }
    let compiled = f.compile();

    let l = entry(table(&compiled, "b"), "l");
    assert_eq!(
        l.quic,
        Some(QuicTuning {
            send_mbps: 500,
            receive_mbps: 200,
            ..QuicTuning::default()
        }),
        "send no faster than the slowest dialer receives, expect no more than the slowest sends"
    );

    let s1 = relay_target(upstream(entry(table(&compiled, "s1"), "s1-p"), "s1-l"));
    assert_eq!(s1.quic, None, "s1's own rates already fit b");
    let s3 = relay_target(upstream(entry(table(&compiled, "s3"), "s3-p"), "s3-l"));
    assert_eq!(
        s3.quic,
        Some(QuicTuning {
            send_mbps: 1000,
            receive_mbps: 1000,
            ..QuicTuning::default()
        })
    );
}

// --- 9. Workers that only read the tree form --------------------------------

#[test]
fn weights_become_repeated_members_for_old_workers() {
    let mut f = Fabric::new();
    f.legacy_server("edge", "198.51.100.1")
        .legacy_server("a", "203.0.113.1")
        .legacy_server("b", "203.0.113.2")
        .pod(
            "p",
            "edge",
            443,
            guru_topology::Ingress::ClientRaw {
                receive_proxy_protocol: Some(TcpProxyProtocol::V2),
            },
        )
        .pod("a-in", "a", 40000, guru_topology::Ingress::RelayTcp)
        .pod("b-in", "b", 40000, guru_topology::Ingress::RelayTcp)
        .exit("x", "10.0.0.5:8080")
        .edge("p-a", "p", "a-in")
        .edge("p-b", "p", "b-in")
        .edge("a-x", "a-in", "x")
        .edge("b-x", "b-in", "x")
        .route("p", weighted(vec![(2, leaf("p-a")), (1, leaf("p-b"))]))
        .route("a-in", leaf("a-x"))
        .route("b-in", leaf("b-x"));

    let compiled = f.compile();
    let edge = legacy(&compiled, "edge");
    let ForwardingTo::LoadBalance(group) = tree(&edge[0]) else {
        panic!("expected a group: {:#?}", edge[0].to);
    };
    assert_eq!(group.strategy, LoadBalanceStrategy::RoundRobin);
    let destinations: Vec<&Remote> = group
        .members
        .iter()
        .map(|m| match m {
            ForwardingTo::Relay { destination, .. } => destination,
            other => panic!("expected relays: {other:#?}"),
        })
        .collect();
    let (a, b) = (socket("203.0.113.1:40000"), socket("203.0.113.2:40000"));
    assert_eq!(destinations, [&a, &a, &b]);
    round_trip_legacy(edge);

    f.route(
        "p",
        sticky(weighted(vec![(2, leaf("p-a")), (1, leaf("p-b"))])),
    );
    let compiled = f.compile();
    let ForwardingTo::LoadBalance(group) = tree(&legacy(&compiled, "edge")[0]) else {
        panic!("expected a group");
    };
    assert_eq!(group.strategy, LoadBalanceStrategy::IpHash);
}

// --- 10. The address family an edge dials over ------------------------------

/// Client pod `p` on `s1` dials relay pod `q` on `s2` through `p-q`; `s2` has
/// an IPv4 and an IPv6 address, and `q` exits to `x`.
fn dual_stack() -> Fabric {
    let mut f = Fabric::new();
    f.server("s1", "198.51.100.1")
        .server("s2", "192.0.2.10")
        .addresses("s2", Some("192.0.2.10"), Some("2001:db8::10"))
        .pod("p", "s1", 443, raw())
        .pod("q", "s2", 40000, guru_topology::Ingress::RelayTcp)
        .exit("x", "10.0.0.5:8080")
        .edge("p-q", "p", "q")
        .edge("q-x", "q", "x")
        .route("p", leaf("p-q"))
        .route("q", leaf("q-x"));
    f
}

/// Where `p` dials `q`.
fn dialed(f: &Fabric) -> Remote {
    let compiled = f.compile();
    relay_target(upstream(entry(table(&compiled, "s1"), "p"), "p-q"))
        .destination
        .clone()
}

fn family_warnings(f: &Fabric) -> Vec<Diagnostic> {
    check(&f.graph)
        .diagnostics
        .into_iter()
        .filter(|d| d.problem == Problem::DialFamilyUnreachable)
        .collect()
}

#[test]
fn an_edge_dials_the_address_family_it_asks_for() {
    let mut f = dual_stack();
    assert_eq!(
        dialed(&f),
        socket("192.0.2.10:40000"),
        "auto dials the server's IPv4 address"
    );
    f.edge_mut("p-q").ip_family = IpFamily::V6;
    assert_eq!(dialed(&f), socket("[2001:db8::10]:40000"));
    f.edge_mut("p-q").ip_family = IpFamily::V4;
    assert_eq!(dialed(&f), socket("192.0.2.10:40000"));
    assert!(family_warnings(&f).is_empty());

    f.addresses("s2", None, Some("2001:db8::10"));
    f.edge_mut("p-q").ip_family = IpFamily::Auto;
    assert_eq!(
        dialed(&f),
        socket("[2001:db8::10]:40000"),
        "auto takes IPv6 when that is all the server has"
    );
}

#[test]
fn an_advertised_address_stands_in_for_its_own_family_only() {
    let mut f = dual_stack();
    f.pod_mut("q").advertise_ip = Some("10.8.0.2".to_string());
    assert_eq!(dialed(&f), socket("10.8.0.2:40000"));
    f.edge_mut("p-q").ip_family = IpFamily::V4;
    assert_eq!(dialed(&f), socket("10.8.0.2:40000"));
    f.edge_mut("p-q").ip_family = IpFamily::V6;
    assert_eq!(
        dialed(&f),
        socket("[2001:db8::10]:40000"),
        "an IPv4 advertisement does not stand in for the server's IPv6"
    );
    f.pod_mut("q").advertise_ip = Some("fd00::2".to_string());
    assert_eq!(dialed(&f), socket("[fd00::2]:40000"));
}

#[test]
fn an_override_address_wins_over_the_family() {
    let mut f = dual_stack();
    f.addresses("s2", Some("192.0.2.10"), None);
    let edge = f.edge_mut("p-q");
    edge.ip_family = IpFamily::V6;
    edge.override_ip = Some("192.0.2.99".to_string());
    assert_eq!(dialed(&f), socket("192.0.2.99:40000"));
    assert!(family_warnings(&f).is_empty());
}

#[test]
fn a_family_the_server_lacks_warns_and_invalidates_the_pods_dialing_over_it() {
    let mut f = dual_stack();
    f.addresses("s2", Some("192.0.2.10"), None)
        .pod("p2", "s1", 444, raw())
        .edge("p2-q", "p2", "q")
        .route("p2", leaf("p2-q"));
    f.edge_mut("p-q").ip_family = IpFamily::V6;
    f.edge_mut("p2-q").ip_family = IpFamily::V6;

    let report = check(&f.graph);
    assert!(!report.has_errors(), "{report:#?}");
    let [warning] = family_warnings(&f).try_into().unwrap();
    assert_eq!(warning.severity(), Severity::Warning);
    assert_eq!(
        warning.subjects,
        vec![
            Subject::Server(ServerId::new("s2")),
            Subject::Edge(EdgeId::new("p-q")),
            Subject::Edge(EdgeId::new("p2-q")),
        ]
    );
    assert_eq!(
        warning.message,
        "IPv6 edges from pods p, p2 cannot reach server s2: it has no IPv6 address"
    );

    let compiled = f.compile();
    assert_eq!(compiled.warnings, report.diagnostics);
    let s1 = &compiled.servers[&ServerId::new("s1")];
    assert!(s1.forwardings.is_empty());
    assert!(matches!(
        &s1.invalid[0].reason,
        Invalid::TargetWithoutAddress { pod, family: IpFamily::V6, .. } if pod.as_str() == "q"
    ));
    assert_eq!(
        s1.invalid[0].message,
        "pod p: pod q on server s2 has no IPv6 address to dial"
    );
}

#[test]
fn an_edge_on_auto_is_not_warned_about() {
    let mut f = dual_stack();
    f.addresses("s2", None, None);
    assert!(family_warnings(&f).is_empty());
    let compiled = f.compile();
    let s1 = &compiled.servers[&ServerId::new("s1")];
    assert!(matches!(
        &s1.invalid[0].reason,
        Invalid::TargetWithoutAddress {
            family: IpFamily::Auto,
            ..
        }
    ));
    assert_eq!(
        s1.invalid[0].message,
        "pod p: pod q on server s2 has no address to dial"
    );
}

#[test]
fn a_listener_on_the_other_family_only_is_warned_about() {
    let mut f = dual_stack();
    f.edge_mut("p-q").ip_family = IpFamily::V6;
    f.pod_mut("q").bind_ip = Some("0.0.0.0".to_string());
    let [warning] = family_warnings(&f).try_into().unwrap();
    assert_eq!(
        warning.subjects,
        vec![
            Subject::Pod(PodId::new("q")),
            Subject::Edge(EdgeId::new("p-q"))
        ]
    );
    assert_eq!(
        warning.message,
        "IPv6 edges from pod p cannot reach pod q: it listens on 0.0.0.0 only"
    );

    f.pod_mut("q").bind_ip = Some("::".to_string());
    assert!(family_warnings(&f).is_empty(), "`::` takes both families");
    f.pod_mut("q").bind_ip = Some("2001:db8::10".to_string());
    assert!(family_warnings(&f).is_empty());
    f.edge_mut("p-q").ip_family = IpFamily::V4;
    assert_eq!(family_warnings(&f).len(), 1);
}

// --- The stored form of a route ---------------------------------------------

#[test]
fn a_route_is_the_documented_json() {
    let text = r#"{ "failover": [
        { "balance": [ { "weight": 2, "to": { "edge": "e1" } }, { "to": { "edge": "e2" } } ],
          "sticky": "client_ip" },
        { "balance": [ { "to": { "edge": "e6" } }, { "to": { "edge": "e7" } } ] },
        { "edge": "e9" }
    ] }"#;
    let route: Route = serde_json::from_str(text).unwrap();
    assert_eq!(
        route,
        failover(vec![
            sticky(weighted(vec![(2, leaf("e1")), (1, leaf("e2"))])),
            balance(leaves(&["e6", "e7"])),
            leaf("e9"),
        ])
    );
    assert_eq!(
        serde_json::to_value(&route).unwrap(),
        serde_json::from_str::<serde_json::Value>(text).unwrap()
    );

    for bad in [
        r#"{ "edge": "e1", "failover": [] }"#,
        r#"{ "failover": [], "sticky": "client_ip" }"#,
        r#"{ "edge": "e1", "colour": "red" }"#,
        r#"{}"#,
    ] {
        assert!(serde_json::from_str::<Route>(bad).is_err(), "{bad} parsed");
    }
}
