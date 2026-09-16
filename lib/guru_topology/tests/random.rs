//! Properties that hold for any graph, checked on seeded random ones so a
//! failure names the seed that reproduces it.

use guru_topology::{
    Capabilities, CertificateKind, CertificateRef, Certificates, Compiled, Edge, EdgeId,
    EdgeTarget, Exit, ExitId, Forwardings, Graph, Ingress, Pod, PodId, Problem, Route, Server,
    ServerId, ServerQuic, Sticky, Weighted, check, compile,
};
use guru_worker_config::{Config, Ipv6Resolve, KeepAlive, LogConfig, QuicTuning, TcpProxyProtocol};
use std::collections::HashSet;

/// SplitMix64: small, fast and the same everywhere.
struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    fn below(&mut self, n: usize) -> usize {
        if n == 0 {
            0
        } else {
            (self.next() % n as u64) as usize
        }
    }

    /// True with probability `percent`/100.
    fn chance(&mut self, percent: u64) -> bool {
        self.next() % 100 < percent
    }

    fn pick<'a, T>(&mut self, items: &'a [T]) -> &'a T {
        &items[self.below(items.len())]
    }

    fn shuffle<T>(&mut self, items: &mut [T]) {
        for i in (1..items.len()).rev() {
            let j = self.below(i + 1);
            items.swap(i, j);
        }
    }
}

/// How hard a generated graph tries to be valid.
#[derive(Clone, Copy, PartialEq)]
enum Mode {
    /// Acyclic by construction, routes that cover their edges, distinct ports:
    /// most of these compile, which is what exercises `compile`.
    Plausible,
    /// Anything goes: dangling ids, loops, shared sockets, broken routes.
    Chaos,
}

fn random_graph(seed: u64, mode: Mode) -> (Graph, Certificates) {
    let mut rng = Rng(seed);
    let plausible = mode == Mode::Plausible;
    let mut graph = Graph::default();
    let mut certificates = Certificates {
        internal_ca: rng.chance(90),
        assume_issued: rng.chance(10),
        ..Certificates::default()
    };

    let server_count = 1 + rng.below(4);
    for i in 0..server_count {
        graph.servers.push(Server {
            id: ServerId::new(format!("s{i}")),
            name: format!("server {i}"),
            dial_address: rng
                .chance(85)
                .then(|| format!("10.0.{i}.1").parse().unwrap()),
            quic: ServerQuic {
                up_mbps: *rng.pick(&[0, 100, 500, 1000]),
                down_mbps: *rng.pick(&[0, 100, 500, 1000]),
                ..ServerQuic::default()
            },
            capabilities: Capabilities {
                route_table: rng.chance(60),
                relay_confirm: rng.chance(50),
            },
        });
    }

    let pod_count = rng.below(12);
    for i in 0..pod_count {
        let ingress = match rng.below(5) {
            0 => Ingress::ClientRaw {
                receive_proxy_protocol: rng.chance(50).then_some(TcpProxyProtocol::V2),
            },
            1 => Ingress::ClientTls {
                receive_proxy_protocol: None,
                sni: "example.com".to_string(),
                acme_directory: "https://acme.example/directory".to_string(),
            },
            2 => Ingress::RelayTcp,
            3 => Ingress::RelayTls,
            _ => Ingress::RelayQuic,
        };
        let id = PodId::new(format!("p{i:02}"));
        if matches!(ingress, Ingress::RelayTls | Ingress::RelayQuic) && rng.chance(85) {
            certificates.relay.insert(
                id.clone(),
                CertificateRef {
                    kind: CertificateKind::Relay,
                    key: format!("leaf-{id}"),
                    version: 1,
                },
            );
        }
        if matches!(ingress, Ingress::ClientTls { .. }) && rng.chance(70) {
            certificates.acme.insert(
                (
                    "example.com".to_string(),
                    "https://acme.example/directory".to_string(),
                ),
                CertificateRef {
                    kind: CertificateKind::Acme,
                    key: "acme-1".to_string(),
                    version: 1,
                },
            );
        }
        let server = if !plausible && rng.chance(5) {
            ServerId::new("s-missing")
        } else {
            ServerId::new(format!("s{}", rng.below(server_count)))
        };
        let port = if plausible {
            40000 + i as u16
        } else {
            *rng.pick(&[0, 443, 8443, 40000])
        };
        let bind_ip = match rng.below(20) {
            0..=13 => None,
            14..=16 => Some("10.9.9.9".to_string()),
            17 | 18 => Some("0.0.0.0".to_string()),
            _ if plausible => None,
            _ => Some("not-an-address".to_string()),
        };
        graph.pods.push(Pod {
            id,
            server,
            name: format!("pod {i}"),
            port,
            bind_ip,
            advertise_ip: rng.chance(10).then(|| "192.0.2.7".to_string()),
            ingress,
            route: None,
        });
    }

    let exit_count = rng.below(4);
    for i in 0..exit_count {
        graph.exits.push(Exit {
            id: ExitId::new(format!("x{i}")),
            name: format!("exit {i}"),
            destination: if !plausible && rng.chance(10) {
                "no port".to_string()
            } else {
                format!("10.1.0.{i}:80")
            },
            send_proxy_protocol: rng.chance(30).then_some(TcpProxyProtocol::V1),
        });
    }

    let edge_count = rng.below(18);
    for i in 0..edge_count {
        if pod_count == 0 {
            break;
        }
        let from = rng.below(pod_count);
        let source = if !plausible && rng.chance(5) && exit_count > 0 {
            PodId::new(format!("x{}", rng.below(exit_count)))
        } else {
            graph.pods[from].id.clone()
        };
        let target = if plausible {
            // Only forward, and only into relay pods, so the graph stays a DAG
            // that never dials a client pod.
            let later: Vec<&Pod> = graph.pods[from + 1..]
                .iter()
                .filter(|p| p.ingress.is_relay())
                .collect();
            if !later.is_empty() && (exit_count == 0 || rng.chance(50)) {
                EdgeTarget::Pod(rng.pick(&later).id.clone())
            } else if exit_count > 0 {
                EdgeTarget::Exit(ExitId::new(format!("x{}", rng.below(exit_count))))
            } else {
                continue;
            }
        } else if rng.chance(5) {
            EdgeTarget::Pod(PodId::new("p-missing"))
        } else if rng.chance(70) || exit_count == 0 {
            EdgeTarget::Pod(graph.pods[rng.below(pod_count)].id.clone())
        } else {
            EdgeTarget::Exit(ExitId::new(format!("x{}", rng.below(exit_count))))
        };
        graph.edges.push(Edge {
            id: EdgeId::new(format!("e{i:02}")),
            source,
            target,
            override_ip: match rng.below(20) {
                0 => Some("2001:db8::1".to_string()),
                1 => Some("relay.example.net".to_string()),
                2 if !plausible => Some("bad host".to_string()),
                _ => None,
            },
            override_port: rng.chance(5).then_some(if plausible { 9000 } else { 0 }),
        });
    }

    for p in 0..pod_count {
        let id = graph.pods[p].id.clone();
        let mut own: Vec<EdgeId> = graph
            .edges
            .iter()
            .filter(|e| e.source == id)
            .map(|e| e.id.clone())
            .collect();
        let knows_client_ip = graph.pods[p].ingress.knows_client_ip();
        graph.pods[p].route = if plausible {
            rng.shuffle(&mut own);
            (!own.is_empty()).then(|| random_route(&mut rng, &own, knows_client_ip, 0))
        } else if rng.chance(15) {
            None
        } else {
            let mut leaves = own;
            if rng.chance(30) {
                leaves.push(EdgeId::new(format!("e{:02}", rng.below(edge_count + 2))));
            }
            if leaves.is_empty() || rng.chance(10) {
                leaves.push(EdgeId::new("e-missing"));
            }
            Some(random_route(&mut rng, &leaves, true, 0))
        };
    }
    (graph, certificates)
}

/// A route whose leaves are exactly `leaves`, in some nesting.
fn random_route(rng: &mut Rng, leaves: &[EdgeId], sticky_ok: bool, depth: usize) -> Route {
    if leaves.len() == 1 && (depth > 3 || rng.chance(60)) {
        return Route::Edge(leaves[0].clone());
    }
    // Past a few levels, always split, so the nesting ends.
    let parts = if depth > 3 {
        leaves.len().clamp(2, 3)
    } else {
        1 + rng.below(leaves.len().min(3))
    };
    let size = leaves.len().div_ceil(parts);
    let members: Vec<Route> = leaves
        .chunks(size.max(1))
        .map(|chunk| random_route(rng, chunk, sticky_ok, depth + 1))
        .collect();
    if rng.chance(50) {
        Route::Failover(members)
    } else {
        Route::Balance {
            members: members
                .into_iter()
                .map(|to| Weighted {
                    weight: 1 + rng.below(4) as u32,
                    to,
                })
                .collect(),
            sticky: (sticky_ok && rng.chance(20)).then_some(Sticky::ClientIp),
        }
    }
}

fn shuffled(graph: &Graph, seed: u64) -> Graph {
    let mut rng = Rng(seed ^ 0xDEAD_BEEF);
    let mut graph = graph.clone();
    rng.shuffle(&mut graph.servers);
    rng.shuffle(&mut graph.pods);
    rng.shuffle(&mut graph.exits);
    rng.shuffle(&mut graph.edges);
    graph
}

fn assert_properties(seed: u64, graph: &Graph, certificates: &Certificates) -> bool {
    let report = check(graph);
    assert_eq!(
        report,
        check(graph),
        "seed {seed}: check is not deterministic"
    );
    let compiled = compile(graph, certificates);
    assert_eq!(
        compiled,
        compile(graph, certificates),
        "seed {seed}: compile is not deterministic"
    );
    assert_eq!(
        compiled.is_err(),
        report.has_errors(),
        "seed {seed}: compile must fail exactly when check reports an error"
    );
    let Ok(compiled) = compiled else {
        return false;
    };

    every_pod_is_accounted_for(seed, graph, &compiled);
    for (server, config) in &compiled.servers {
        assert_eq!(
            config.deps.len(),
            config.forwardings.len(),
            "seed {seed}: deps out of step on {server}"
        );
        for (deps, tag) in config.deps.iter().zip(config.forwardings.tags()) {
            assert_eq!(deps.pod.as_str(), tag, "seed {seed}");
        }
        match &config.forwardings {
            Forwardings::Table(list) => {
                let mut sockets = HashSet::new();
                for forwarding in list {
                    forwarding
                        .validate()
                        .unwrap_or_else(|e| panic!("seed {seed}: {e}: {forwarding:#?}"));
                    assert!(
                        forwarding.lint().is_empty(),
                        "seed {seed}: unreached table entries in {forwarding:#?}"
                    );
                    assert!(
                        sockets.insert(forwarding.listen_key()),
                        "seed {seed}: two forwardings on one socket of {server}"
                    );
                }
            }
            Forwardings::Legacy(list) => {
                let config = Config {
                    ipv6_resolve: Ipv6Resolve::default(),
                    log: LogConfig::default(),
                    relay_ca: None,
                    keepalive: KeepAlive::default(),
                    quic: QuicTuning::default(),
                    forwardings: list.clone(),
                };
                let text = config
                    .to_toml_string()
                    .unwrap_or_else(|e| panic!("seed {seed}: {e}"));
                let parsed = Config::from_toml_str(&text)
                    .unwrap_or_else(|e| panic!("seed {seed}: {e}\n{text}"));
                assert_eq!(parsed, config, "seed {seed}");
            }
        }
    }

    assert_eq!(
        compile(&shuffled(graph, seed), certificates),
        Ok(compiled),
        "seed {seed}: the input order changed the result"
    );
    true
}

/// A pod ends up in exactly one place: compiled on its server, listed as
/// invalid there, or left out with the warning that says why.
fn every_pod_is_accounted_for(seed: u64, graph: &Graph, compiled: &Compiled) {
    for pod in &graph.pods {
        let config = &compiled.servers[&pod.server];
        let compiled_here = config.forwardings.tags().contains(&pod.id.as_str());
        let invalid_here = config.invalid.iter().any(|i| i.pod == pod.id);
        let warned = compiled.warnings.iter().any(|w| {
            matches!(
                w.problem,
                Problem::PodWithoutEdges | Problem::RelayPodNotDialed
            ) && w
                .subjects
                .contains(&guru_topology::Subject::Pod(pod.id.clone()))
        });
        let places = [compiled_here, invalid_here, warned]
            .iter()
            .filter(|b| **b)
            .count();
        assert_eq!(
            places, 1,
            "seed {seed}: pod {} is compiled={compiled_here} invalid={invalid_here} warned={warned}",
            pod.id
        );
    }
}

#[test]
fn plausible_graphs_compile_consistently() {
    let mut compiled = 0;
    let total = 600;
    for seed in 0..total {
        let (graph, certificates) = random_graph(seed, Mode::Plausible);
        if assert_properties(seed, &graph, &certificates) {
            compiled += 1;
        }
    }
    assert!(
        compiled * 2 > total,
        "only {compiled} of {total} plausible graphs compiled; the generator no longer exercises compile"
    );
}

#[test]
fn chaotic_graphs_never_panic_and_stay_consistent() {
    for seed in 10_000..10_600 {
        let (graph, certificates) = random_graph(seed, Mode::Chaos);
        assert_properties(seed, &graph, &certificates);
    }
}

#[test]
fn deep_and_wide_graphs_stay_fast() {
    // A long relay chain and a pod with many edges: nothing here is quadratic
    // enough, or recursive enough, to matter.
    let mut graph = Graph::default();
    graph.servers.push(Server {
        id: ServerId::new("s"),
        name: "s".to_string(),
        dial_address: Some("10.0.0.1".parse().unwrap()),
        quic: ServerQuic::default(),
        capabilities: Capabilities {
            route_table: true,
            relay_confirm: true,
        },
    });
    graph.exits.push(Exit {
        id: ExitId::new("x"),
        name: "x".to_string(),
        destination: "10.0.0.2:80".to_string(),
        send_proxy_protocol: None,
    });
    let chain = 5_000;
    for i in 0..chain {
        let ingress = if i == 0 {
            Ingress::ClientRaw {
                receive_proxy_protocol: None,
            }
        } else {
            Ingress::RelayTcp
        };
        let edge = EdgeId::new(format!("e{i}"));
        graph.edges.push(Edge {
            id: edge.clone(),
            source: PodId::new(format!("p{i}")),
            target: if i + 1 == chain {
                EdgeTarget::Exit(ExitId::new("x"))
            } else {
                EdgeTarget::Pod(PodId::new(format!("p{}", i + 1)))
            },
            override_ip: None,
            override_port: None,
        });
        graph.pods.push(Pod {
            id: PodId::new(format!("p{i}")),
            server: ServerId::new("s"),
            name: format!("p{i}"),
            port: 1 + i as u16,
            bind_ip: None,
            advertise_ip: None,
            ingress,
            route: Some(Route::Edge(edge)),
        });
    }
    let started = std::time::Instant::now();
    let compiled = compile(&graph, &Certificates::default()).unwrap();
    assert_eq!(
        compiled.servers[&ServerId::new("s")].forwardings.len(),
        chain
    );
    assert!(
        started.elapsed() < std::time::Duration::from_secs(10),
        "compiling a {chain}-pod chain took {:?}",
        started.elapsed()
    );

    let mut looped = graph.clone();
    looped.edges.last_mut().unwrap().target = EdgeTarget::Pod(PodId::new("p1"));
    assert!(check(&looped).has(Problem::Cycle));
}
