#![allow(clippy::unwrap_used, clippy::panic, clippy::expect_used)]

//! How a worker picks a next hop: route tables and trees alike pass over dead
//! hops, balance by weight, fail over in order, and learn through a confirming
//! relay that the relay's own next hop is gone.

use guru_worker::supervisor::Supervisor;
use guru_worker_config::table::{
    ExitTarget, Group, Policy, RelayTarget, Target, Upstream, Weighted,
};
use guru_worker_config::{
    Config, Forwarding, ForwardingTo, Ipv6Resolve, KeepAlive, ListenAs, LoadBalanceGroup,
    LoadBalanceStrategy, LogConfig, QuicTuning, RelayHost, RelayProtocol, Remote, To,
};
use std::net::SocketAddr;
use std::time::Duration;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};

fn free_ports(n: usize) -> Vec<u16> {
    let probes: Vec<std::net::TcpListener> = (0..n)
        .map(|_| std::net::TcpListener::bind("127.0.0.1:0").unwrap())
        .collect();
    probes
        .iter()
        .map(|l| l.local_addr().unwrap().port())
        .collect()
}

fn local(port: u16) -> SocketAddr {
    format!("127.0.0.1:{port}").parse().unwrap()
}

/// A backend that greets every connection with its name, then echoes.
async fn backend(name: &'static str) -> SocketAddr {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        loop {
            let Ok((mut stream, _)) = listener.accept().await else {
                return;
            };
            tokio::spawn(async move {
                if stream
                    .write_all(format!("{name}\n").as_bytes())
                    .await
                    .is_err()
                {
                    return;
                }
                let (mut reader, mut writer) = stream.split();
                let _ = tokio::io::copy(&mut reader, &mut writer).await;
            });
        }
    });
    addr
}

/// An address nothing listens on: connecting is refused at once.
fn refused() -> SocketAddr {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    drop(listener);
    addr
}

/// Which backend a new connection through `listen` reaches, or `None` when the
/// worker could not connect it anywhere.
async fn reaches(listen: SocketAddr) -> Option<String> {
    let stream = tokio::net::TcpStream::connect(listen).await.unwrap();
    let mut reader = BufReader::new(stream);
    let mut line = String::new();
    match tokio::time::timeout(Duration::from_secs(20), reader.read_line(&mut line)).await {
        Ok(Ok(n)) if n > 0 => Some(line.trim().to_string()),
        Ok(_) => None,
        Err(_) => panic!("no answer through {listen}"),
    }
}

fn exit(id: &str, addr: SocketAddr) -> Upstream {
    Upstream {
        id: id.to_string(),
        target: Target::Exit(ExitTarget {
            destination: Remote::Address(addr),
            send_proxy_protocol: None,
        }),
    }
}

fn relay(id: &str, addr: SocketAddr, confirm: bool) -> Upstream {
    Upstream {
        id: id.to_string(),
        target: Target::Relay(RelayTarget {
            protocol: RelayProtocol::Tcp,
            destination: Remote::Address(addr),
            sni: None,
            quic: None,
            confirm,
        }),
    }
}

fn failover(id: &str, members: &[&str]) -> Group {
    Group {
        id: id.to_string(),
        policy: Policy::Failover {
            members: members.iter().map(|m| m.to_string()).collect(),
        },
    }
}

fn balance(id: &str, members: &[(&str, u32)]) -> Group {
    Group {
        id: id.to_string(),
        policy: Policy::Balance {
            members: members
                .iter()
                .map(|(to, weight)| Weighted {
                    to: to.to_string(),
                    weight: *weight,
                })
                .collect(),
            sticky: None,
        },
    }
}

fn table(
    tag: &str,
    listen: SocketAddr,
    listen_as: ListenAs,
    to: &str,
    groups: Vec<Group>,
    upstreams: Vec<Upstream>,
) -> Forwarding {
    Forwarding {
        tag: tag.to_string(),
        listen,
        receive_proxy_protocol: None,
        listen_as,
        quic: None,
        to: To::Route(to.to_string()),
        groups,
        upstreams,
    }
}

fn config(forwardings: Vec<Forwarding>) -> Config {
    Config {
        ipv6_resolve: Ipv6Resolve::Tolerated,
        log: LogConfig::default(),
        relay_ca: None,
        keepalive: KeepAlive::default(),
        quic: QuicTuning::default(),
        forwardings,
    }
}

async fn apply(sup: &mut Supervisor, forwardings: Vec<Forwarding>) {
    let cfg = config(forwardings);
    for f in &cfg.forwardings {
        f.validate().unwrap();
    }
    let outcome = sup.apply(&cfg).await;
    assert!(
        outcome.failed().next().is_none(),
        "every forwarding applied: {:?}",
        outcome.pods
    );
}

#[tokio::test]
async fn a_failover_passes_over_a_dead_hop_and_remembers_it() {
    let live = backend("live").await;
    let dead = refused();
    let listen = local(free_ports(1)[0]);
    let mut sup = Supervisor::new();
    apply(
        &mut sup,
        vec![table(
            "p",
            listen,
            ListenAs::Raw,
            "g",
            vec![failover("g", &["u:dead", "u:live"])],
            vec![exit("u:dead", dead), exit("u:live", live)],
        )],
    )
    .await;

    for _ in 0..6 {
        assert_eq!(reaches(listen).await.as_deref(), Some("live"));
    }
    assert_eq!(
        sup.liveness().dead(),
        vec![format!("exit|{dead}")],
        "the refused exit is known to be dead"
    );
    sup.shutdown_all();
}

#[tokio::test]
async fn a_balance_follows_its_weights_exactly() {
    let heavy = backend("heavy").await;
    let light = backend("light").await;
    let listen = local(free_ports(1)[0]);
    let mut sup = Supervisor::new();
    apply(
        &mut sup,
        vec![table(
            "p",
            listen,
            ListenAs::Raw,
            "g",
            vec![balance("g", &[("u:heavy", 3), ("u:light", 1)])],
            vec![exit("u:heavy", heavy), exit("u:light", light)],
        )],
    )
    .await;

    let mut counts = std::collections::HashMap::new();
    for _ in 0..40 {
        *counts.entry(reaches(listen).await.unwrap()).or_insert(0) += 1;
    }
    assert_eq!(counts.get("heavy"), Some(&30), "{counts:?}");
    assert_eq!(counts.get("light"), Some(&10), "{counts:?}");
    sup.shutdown_all();
}

#[tokio::test]
async fn a_balance_inside_a_failover_moves_to_the_next_tier_only_when_empty() {
    let first = backend("first").await;
    let second = backend("second").await;
    let (dead_a, dead_b) = (refused(), refused());
    let listen = local(free_ports(1)[0]);
    let mut sup = Supervisor::new();
    apply(
        &mut sup,
        vec![table(
            "p",
            listen,
            ListenAs::Raw,
            "g",
            vec![
                failover("g", &["g.0", "g.1"]),
                balance("g.0", &[("u:a", 1), ("u:b", 1), ("u:first", 1)]),
                balance("g.1", &[("u:second", 1)]),
            ],
            vec![
                exit("u:a", dead_a),
                exit("u:b", dead_b),
                exit("u:first", first),
                exit("u:second", second),
            ],
        )],
    )
    .await;
    for _ in 0..8 {
        assert_eq!(
            reaches(listen).await.as_deref(),
            Some("first"),
            "a tier with one live member still serves"
        );
    }
    sup.shutdown_all();
}

#[tokio::test]
async fn a_tree_fallback_uses_the_same_runtime() {
    let live = backend("live").await;
    let dead = refused();
    let listen = local(free_ports(1)[0]);
    let mut sup = Supervisor::new();
    let tree = ForwardingTo::LoadBalance(Box::new(LoadBalanceGroup {
        strategy: LoadBalanceStrategy::RoundRobin,
        members: smallvec::smallvec![
            ForwardingTo::Exit {
                destination: Remote::Address(dead),
                send_proxy_protocol: None,
            },
            ForwardingTo::Exit {
                destination: Remote::Address(live),
                send_proxy_protocol: None,
            },
        ],
    }));
    apply(
        &mut sup,
        vec![Forwarding {
            tag: "p".to_string(),
            listen,
            receive_proxy_protocol: None,
            listen_as: ListenAs::Raw,
            quic: None,
            to: To::Tree(tree),
            groups: Vec::new(),
            upstreams: Vec::new(),
        }],
    )
    .await;
    for _ in 0..6 {
        assert_eq!(
            reaches(listen).await.as_deref(),
            Some("live"),
            "a round robin no longer hands clients to a refused member"
        );
    }
    sup.shutdown_all();
}

/// Two workers: `transit` relays to an exit, `entry` fails over from a relay to
/// `transit` to a direct exit.
async fn two_hops(transit_exit: SocketAddr, confirm: bool) -> (Supervisor, Supervisor, SocketAddr) {
    let direct = backend("direct").await;
    let ports = free_ports(2);
    let (entry_listen, transit_listen) = (local(ports[0]), local(ports[1]));
    let mut transit = Supervisor::new();
    apply(
        &mut transit,
        vec![table(
            "transit",
            transit_listen,
            ListenAs::Relay(RelayHost::Tcp),
            "u:exit",
            vec![],
            vec![exit("u:exit", transit_exit)],
        )],
    )
    .await;
    let mut entry = Supervisor::new();
    apply(
        &mut entry,
        vec![table(
            "entry",
            entry_listen,
            ListenAs::Raw,
            "g",
            vec![failover("g", &["u:relay", "u:direct"])],
            vec![
                relay("u:relay", transit_listen, confirm),
                exit("u:direct", direct),
            ],
        )],
    )
    .await;
    (entry, transit, entry_listen)
}

#[tokio::test]
async fn a_confirming_relay_carries_traffic_when_its_next_hop_answers() {
    let far = backend("far").await;
    let (entry, transit, listen) = two_hops(far, true).await;
    for _ in 0..3 {
        assert_eq!(reaches(listen).await.as_deref(), Some("far"));
    }
    // The payload still flows both ways after the confirmation byte.
    let mut stream = BufReader::new(tokio::net::TcpStream::connect(listen).await.unwrap());
    let mut greeting = String::new();
    stream.read_line(&mut greeting).await.unwrap();
    stream.get_mut().write_all(b"ping\n").await.unwrap();
    let mut echoed = String::new();
    stream.read_line(&mut echoed).await.unwrap();
    assert_eq!((greeting.trim(), echoed.trim()), ("far", "ping"));
    entry.shutdown_all();
    transit.shutdown_all();
}

#[tokio::test]
async fn a_confirming_relay_whose_next_hop_is_dead_fails_over_at_the_dialer() {
    let (entry, transit, listen) = two_hops(refused(), true).await;
    for _ in 0..5 {
        assert_eq!(
            reaches(listen).await.as_deref(),
            Some("direct"),
            "the dead exit behind the relay moves the entry to its next tier"
        );
    }
    assert!(
        entry
            .liveness()
            .dead()
            .iter()
            .any(|hop| hop.starts_with("relay|")),
        "the relay is known to be of no use: {:?}",
        entry.liveness().dead()
    );
    entry.shutdown_all();
    transit.shutdown_all();
}

#[tokio::test]
async fn a_relay_not_asked_to_confirm_behaves_as_before() {
    let far = backend("far").await;
    let (entry, transit, listen) = two_hops(far, false).await;
    assert_eq!(reaches(listen).await.as_deref(), Some("far"));

    // Without a confirmation the entry cannot tell that the relay's exit is
    // gone: the connection is simply closed on the client.
    let (blind_entry, blind_transit, blind) = two_hops(refused(), false).await;
    assert_eq!(reaches(blind).await, None);
    entry.shutdown_all();
    transit.shutdown_all();
    blind_entry.shutdown_all();
    blind_transit.shutdown_all();
}

#[tokio::test]
async fn a_dead_hop_is_tried_again_once_everything_else_fails() {
    // Both members dead: every connection still tries (forced), so the first
    // one to come back is found without waiting out the retry period.
    let listen = local(free_ports(1)[0]);
    let spare = free_ports(1)[0];
    let dead = refused();
    let mut sup = Supervisor::new();
    apply(
        &mut sup,
        vec![table(
            "p",
            listen,
            ListenAs::Raw,
            "g",
            vec![failover("g", &["u:a", "u:b"])],
            vec![exit("u:a", dead), exit("u:b", local(spare))],
        )],
    )
    .await;
    for _ in 0..3 {
        assert_eq!(reaches(listen).await, None);
    }
    assert_eq!(sup.liveness().dead().len(), 2);

    // `b` comes up on its port.
    let listener = tokio::net::TcpListener::bind(local(spare)).await.unwrap();
    tokio::spawn(async move {
        while let Ok((mut stream, _)) = listener.accept().await {
            let _ = stream.write_all(b"back\n").await;
        }
    });
    assert_eq!(reaches(listen).await.as_deref(), Some("back"));
    assert_eq!(sup.liveness().dead(), vec![format!("exit|{dead}")]);
    sup.shutdown_all();
}

// --- relays over TLS and QUIC -------------------------------------------------

const RELAY_SNI: &str = "pod1.relay.guru.internal";

/// A CA and one leaf it signed for [`RELAY_SNI`], written to `dir`.
struct Pki {
    ca: std::path::PathBuf,
    leaf: guru_worker_config::TlsHostConfig,
}

fn pki(dir: &std::path::Path) -> Pki {
    use rcgen::{BasicConstraints, CertificateParams, DnType, IsCa, Issuer, KeyPair};
    let ca_params = || {
        let mut params = CertificateParams::default();
        params.distinguished_name = rcgen::DistinguishedName::new();
        params
            .distinguished_name
            .push(DnType::CommonName, "guru internal relay CA");
        params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
        params
    };
    let ca_key = KeyPair::generate().unwrap();
    let ca_cert = ca_params().self_signed(&ca_key).unwrap();
    let issuer = Issuer::new(ca_params(), ca_key);
    let leaf_key = KeyPair::generate().unwrap();
    let leaf = CertificateParams::new(vec![RELAY_SNI.to_string()])
        .unwrap()
        .signed_by(&leaf_key, &issuer)
        .unwrap();
    std::fs::create_dir_all(dir).unwrap();
    let ca = dir.join("ca.pem");
    let full_chain = dir.join("full_chain.pem");
    let key = dir.join("key.pem");
    std::fs::write(&ca, ca_cert.pem()).unwrap();
    std::fs::write(&full_chain, format!("{}{}", leaf.pem(), ca_cert.pem())).unwrap();
    std::fs::write(&key, leaf_key.serialize_pem()).unwrap();
    Pki {
        ca,
        leaf: guru_worker_config::TlsHostConfig { key, full_chain },
    }
}

/// A relay worker terminating `protocol` with `leaf` and exiting to `exit`.
async fn secure_relay(
    protocol: RelayProtocol,
    listen: SocketAddr,
    leaf: &guru_worker_config::TlsHostConfig,
    exit_to: SocketAddr,
) -> Supervisor {
    let host = match protocol {
        RelayProtocol::TlsOverTcp => RelayHost::TlsOverTcp(leaf.clone()),
        RelayProtocol::Quic => RelayHost::Quic(leaf.clone()),
        RelayProtocol::Tcp => RelayHost::Tcp,
    };
    let mut sup = Supervisor::new();
    apply(
        &mut sup,
        vec![table(
            "relay",
            listen,
            ListenAs::Relay(host),
            "u:exit",
            vec![],
            vec![exit("u:exit", exit_to)],
        )],
    )
    .await;
    sup
}

/// An entry worker failing over from a confirming relay hop to `fallback`.
async fn confirming_entry(
    protocol: RelayProtocol,
    listen: SocketAddr,
    relay_at: SocketAddr,
    ca: &std::path::Path,
    fallback: SocketAddr,
) -> Supervisor {
    let mut cfg = config(vec![table(
        "entry",
        listen,
        ListenAs::Raw,
        "g",
        vec![failover("g", &["u:relay", "u:fallback"])],
        vec![
            Upstream {
                id: "u:relay".to_string(),
                target: Target::Relay(RelayTarget {
                    protocol,
                    destination: Remote::Address(relay_at),
                    sni: Some(RELAY_SNI.to_string()),
                    quic: None,
                    confirm: true,
                }),
            },
            exit("u:fallback", fallback),
        ],
    )]);
    cfg.relay_ca = Some(ca.to_path_buf());
    let mut sup = Supervisor::new();
    let outcome = sup.apply(&cfg).await;
    assert!(outcome.failed().next().is_none(), "{:?}", outcome.pods);
    sup
}

/// A relay asked to confirm answers over TLS and QUIC alike: traffic goes through
/// it while its exit answers, and the entry fails over the moment it does not.
async fn confirmed(protocol: RelayProtocol) {
    let _ = quinn::rustls::crypto::ring::default_provider().install_default();
    let dir = std::env::temp_dir().join(format!(
        "guru-worker-confirm-{:?}-{}",
        protocol,
        std::process::id()
    ));
    let _ = std::fs::remove_dir_all(&dir);
    let pki = pki(&dir);
    let far = backend("far").await;
    let fallback = backend("fallback").await;
    let ports = free_ports(4);
    let (relay_live, relay_dead) = (local(ports[0]), local(ports[1]));
    let (entry_live, entry_dead) = (local(ports[2]), local(ports[3]));

    let mut workers = vec![
        secure_relay(protocol, relay_live, &pki.leaf, far).await,
        secure_relay(protocol, relay_dead, &pki.leaf, refused()).await,
        confirming_entry(protocol, entry_live, relay_live, &pki.ca, fallback).await,
        confirming_entry(protocol, entry_dead, relay_dead, &pki.ca, fallback).await,
    ];
    assert_eq!(
        reaches(entry_live).await.as_deref(),
        Some("far"),
        "a confirmed hop carries the connection ({protocol:?})"
    );
    assert_eq!(
        reaches(entry_dead).await.as_deref(),
        Some("fallback"),
        "a relay whose exit is dead sends the entry to its fallback ({protocol:?})"
    );
    for worker in &mut workers {
        worker.shutdown_all();
    }
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn a_tls_relay_confirms_its_next_hop() {
    confirmed(RelayProtocol::TlsOverTcp).await;
}

#[tokio::test]
async fn a_quic_relay_confirms_its_next_hop() {
    confirmed(RelayProtocol::Quic).await;
}
