#![allow(clippy::unwrap_used, clippy::panic, clippy::expect_used)]

//! Relay hops over TLS and QUIC verify the next hop against `relay_ca`: two
//! supervisors in one process, an entry worker dialing a relay worker whose leaf is
//! signed by the internal CA, and an entry worker trusting the wrong CA getting nothing.

use guru_worker::supervisor::{ApplyOutcome, Supervisor};
use guru_worker_config::{
    Config, Forwarding, ForwardingTo, Ipv6Resolve, KeepAlive, ListenAs, LogConfig, QuicCongestion,
    QuicTuning, RelayHost, RelayProtocol, Remote, TlsHostConfig,
};
use rcgen::{BasicConstraints, CertificateParams, DnType, IsCa, Issuer, KeyPair};
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

const RELAY_SNI: &str = "pod1.relay.guru.internal";

fn free_ports(n: usize) -> Vec<SocketAddr> {
    let probes: Vec<std::net::TcpListener> = (0..n)
        .map(|_| std::net::TcpListener::bind("127.0.0.1:0").unwrap())
        .collect();
    probes.iter().map(|l| l.local_addr().unwrap()).collect()
}

fn ca_params(name: &str) -> CertificateParams {
    let mut params = CertificateParams::default();
    params.distinguished_name = rcgen::DistinguishedName::new();
    params.distinguished_name.push(DnType::CommonName, name);
    params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    params
}

/// A CA and one leaf it signed for [`RELAY_SNI`], written to `dir`.
struct Pki {
    ca: PathBuf,
    leaf: TlsHostConfig,
}

fn pki(dir: &Path, name: &str) -> Pki {
    let ca_key = KeyPair::generate().unwrap();
    let ca_cert = ca_params(name).self_signed(&ca_key).unwrap();
    let issuer = Issuer::new(ca_params(name), ca_key);
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
        leaf: TlsHostConfig { key, full_chain },
    }
}

fn config(relay_ca: Option<PathBuf>, forwarding: Forwarding) -> Config {
    Config {
        ipv6_resolve: Ipv6Resolve::Tolerated,
        log: LogConfig::default(),
        relay_ca,
        keepalive: KeepAlive::default(),
        quic: QuicTuning::default(),
        forwardings: vec![forwarding],
    }
}

fn assert_all_applied(outcome: &ApplyOutcome) {
    assert!(
        outcome.failed().next().is_none(),
        "every forwarding applied: {:?}",
        outcome.pods
    );
}

/// A TCP echo server; returns its address.
async fn echo_server() -> SocketAddr {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        loop {
            let Ok((mut stream, _)) = listener.accept().await else {
                break;
            };
            tokio::spawn(async move {
                let (mut r, mut w) = stream.split();
                let _ = tokio::io::copy(&mut r, &mut w).await;
            });
        }
    });
    addr
}

/// The relay worker: a relay listener on `listen` terminating TLS/QUIC with `leaf`,
/// forwarding to `echo`.
fn relay_worker(
    protocol: RelayProtocol,
    listen: SocketAddr,
    leaf: &TlsHostConfig,
    echo: SocketAddr,
) -> Config {
    let host = match protocol {
        RelayProtocol::TlsOverTcp => RelayHost::TlsOverTcp(leaf.clone()),
        RelayProtocol::Quic => RelayHost::Quic(leaf.clone()),
        RelayProtocol::Tcp => RelayHost::Tcp,
    };
    config(
        None,
        Forwarding {
            tag: "relay".to_string(),
            listen,
            receive_proxy_protocol: None,
            listen_as: ListenAs::Relay(host),
            quic: None,
            to: guru_worker_config::To::Tree(ForwardingTo::Exit {
                destination: Remote::Address(echo),
                send_proxy_protocol: None,
            }),
            groups: Vec::new(),
            upstreams: Vec::new(),
        },
    )
}

/// The entry worker: a raw listener on `listen` dialing the relay at `relay` over
/// `protocol`, trusting `ca`.
fn entry_worker(
    protocol: RelayProtocol,
    listen: SocketAddr,
    relay: SocketAddr,
    ca: &Path,
) -> Config {
    config(
        Some(ca.to_path_buf()),
        Forwarding {
            tag: "entry".to_string(),
            listen,
            receive_proxy_protocol: None,
            listen_as: ListenAs::Raw,
            quic: None,
            to: guru_worker_config::To::Tree(ForwardingTo::Relay {
                protocol,
                destination: Remote::Address(relay),
                sni: Some(RELAY_SNI.to_string()),
                quic: None,
            }),
            groups: Vec::new(),
            upstreams: Vec::new(),
        },
    )
}

/// Sends `payload` through the entry worker at `entry` and returns what came back
/// before the connection ended or the wait ran out.
async fn round_trip(entry: SocketAddr, payload: &[u8]) -> Vec<u8> {
    let mut client = tokio::net::TcpStream::connect(entry).await.unwrap();
    client.write_all(payload).await.unwrap();
    let mut got = Vec::new();
    let mut buf = [0u8; 256];
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    while got.len() < payload.len() {
        match tokio::time::timeout_at(deadline, client.read(&mut buf)).await {
            Ok(Ok(0)) | Ok(Err(_)) | Err(_) => break,
            Ok(Ok(n)) => got.extend_from_slice(&buf[..n]),
        }
    }
    got
}

async fn chain(protocol: RelayProtocol) {
    let _ = quinn::rustls::crypto::ring::default_provider().install_default();
    let dir = std::env::temp_dir().join(format!(
        "guru-worker-relay-{:?}-{}",
        protocol,
        std::process::id()
    ));
    let _ = std::fs::remove_dir_all(&dir);
    let good = pki(&dir.join("good"), "guru internal relay CA");
    let other = pki(&dir.join("other"), "some other CA");
    let echo = echo_server().await;
    let ports = free_ports(3);
    let (relay_addr, entry_addr, wrong_entry_addr) = (ports[0], ports[1], ports[2]);

    let mut relay = Supervisor::new();
    assert_all_applied(
        &relay
            .apply(&relay_worker(protocol, relay_addr, &good.leaf, echo))
            .await,
    );
    let mut entry = Supervisor::new();
    assert_all_applied(
        &entry
            .apply(&entry_worker(protocol, entry_addr, relay_addr, &good.ca))
            .await,
    );

    let payload = b"hello through the relay";
    assert_eq!(
        round_trip(entry_addr, payload).await,
        payload,
        "bytes echo back through entry -> relay ({protocol:?}) -> echo"
    );

    // An entry worker trusting a different CA cannot complete the hop: the relay's
    // leaf does not chain to it, the dial fails and the client sees the connection
    // end with nothing echoed.
    let mut wrong_entry = Supervisor::new();
    assert_all_applied(
        &wrong_entry
            .apply(&entry_worker(
                protocol,
                wrong_entry_addr,
                relay_addr,
                &other.ca,
            ))
            .await,
    );
    assert!(
        round_trip(wrong_entry_addr, payload).await.is_empty(),
        "a relay hop that does not chain to relay_ca yields nothing ({protocol:?})"
    );

    relay.shutdown_all();
    entry.shutdown_all();
    wrong_entry.shutdown_all();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn tls_relay_hop_is_verified_against_relay_ca() {
    chain(RelayProtocol::TlsOverTcp).await;
}

#[tokio::test]
async fn quic_relay_hop_is_verified_against_relay_ca() {
    chain(RelayProtocol::Quic).await;
}

/// The relay worker with the link's QUIC side set: `quic` is what it sends to
/// and expects from the entry.
fn relay_worker_quic(
    listen: SocketAddr,
    leaf: &TlsHostConfig,
    echo: SocketAddr,
    quic: QuicTuning,
) -> Config {
    let mut cfg = relay_worker(RelayProtocol::Quic, listen, leaf, echo);
    cfg.quic = quic;
    cfg
}

/// Sends `payload` through the entry and waits for all of it back, returning the
/// bytes and how long the round trip took.
async fn timed_round_trip(entry: SocketAddr, payload: &[u8]) -> (Vec<u8>, Duration) {
    let started = tokio::time::Instant::now();
    let client = tokio::net::TcpStream::connect(entry).await.unwrap();
    let writer = {
        let payload = payload.to_vec();
        let (read, mut write) = client.into_split();
        tokio::spawn(async move {
            write.write_all(&payload).await.unwrap();
            write.shutdown().await.unwrap();
        });
        read
    };
    let mut read = writer;
    let mut got = Vec::with_capacity(payload.len());
    let mut buf = vec![0u8; 64 * 1024];
    let deadline = started + Duration::from_secs(30);
    while got.len() < payload.len() {
        match tokio::time::timeout_at(deadline, read.read(&mut buf)).await {
            Ok(Ok(0)) | Ok(Err(_)) | Err(_) => break,
            Ok(Ok(n)) => got.extend_from_slice(&buf[..n]),
        }
    }
    (got, started.elapsed())
}

async fn wait_until(what: &str, mut ok: impl FnMut() -> bool) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
    while !ok() {
        assert!(
            tokio::time::Instant::now() < deadline,
            "timed out waiting: {what}"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

#[tokio::test]
async fn quic_relay_multiplexes_every_connection_on_one_link() {
    let _ = quinn::rustls::crypto::ring::default_provider().install_default();
    let dir = std::env::temp_dir().join(format!("guru-worker-quic-pool-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    let pki = pki(&dir, "guru internal relay CA");
    let echo = echo_server().await;
    let ports = free_ports(2);
    let (relay_addr, entry_addr) = (ports[0], ports[1]);

    let mut relay = Supervisor::new();
    assert_all_applied(
        &relay
            .apply(&relay_worker(
                RelayProtocol::Quic,
                relay_addr,
                &pki.leaf,
                echo,
            ))
            .await,
    );
    let mut entry = Supervisor::new();
    assert_all_applied(
        &entry
            .apply(&entry_worker(
                RelayProtocol::Quic,
                entry_addr,
                relay_addr,
                &pki.ca,
            ))
            .await,
    );
    let client = guru_worker::tls::quic_client(Some(&pki.ca)).unwrap();
    assert_eq!(client.connections(), 0, "nothing dialed yet");

    // Twenty clients at once: twenty streams, one QUIC connection.
    let mut clients = tokio::task::JoinSet::new();
    for i in 0..20u8 {
        clients.spawn(async move {
            let payload = vec![i; 4096];
            let got = round_trip(entry_addr, &payload).await;
            assert_eq!(got, payload, "client {i} echoed");
        });
    }
    while let Some(done) = clients.join_next().await {
        done.unwrap();
    }
    assert_eq!(client.connections(), 1, "every stream shared the one link");

    // The relay goes away: the pooled connection dies and the next client gets a
    // fresh one, without anyone restarting the entry.
    relay
        .apply(&config(
            None,
            Forwarding {
                tag: "unused".to_string(),
                listen: free_ports(1)[0],
                receive_proxy_protocol: None,
                listen_as: ListenAs::Raw,
                quic: None,
                to: guru_worker_config::To::Tree(ForwardingTo::Exit {
                    destination: Remote::Address(echo),
                    send_proxy_protocol: None,
                }),
                groups: Vec::new(),
                upstreams: Vec::new(),
            },
        ))
        .await;
    wait_until("the dead link to be dropped", || client.connections() == 0).await;
    assert_all_applied(
        &relay
            .apply(&relay_worker(
                RelayProtocol::Quic,
                relay_addr,
                &pki.leaf,
                echo,
            ))
            .await,
    );
    let payload = b"back again over a new connection";
    assert_eq!(round_trip(entry_addr, payload).await, payload);
    assert_eq!(client.connections(), 1);

    relay.shutdown_all();
    entry.shutdown_all();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn quic_relay_brutal_sends_at_the_configured_rate() {
    let _ = quinn::rustls::crypto::ring::default_provider().install_default();
    let dir = std::env::temp_dir().join(format!("guru-worker-quic-brutal-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    let pki = pki(&dir, "guru internal relay CA");
    let echo = echo_server().await;
    let ports = free_ports(2);
    let (relay_addr, entry_addr) = (ports[0], ports[1]);

    // The relay sends the echo back at 40 Mbit/s: 5 MB/s.
    let mbps = 40u32;
    let mut relay = Supervisor::new();
    assert_all_applied(
        &relay
            .apply(&relay_worker_quic(
                relay_addr,
                &pki.leaf,
                echo,
                QuicTuning {
                    congestion: QuicCongestion::Brutal,
                    send_mbps: mbps,
                    receive_mbps: 1000,
                    ..QuicTuning::default()
                },
            ))
            .await,
    );
    let mut entry = Supervisor::new();
    assert_all_applied(
        &entry
            .apply(&entry_worker(
                RelayProtocol::Quic,
                entry_addr,
                relay_addr,
                &pki.ca,
            ))
            .await,
    );

    let payload: Vec<u8> = (0..4_000_000u32).map(|i| (i % 251) as u8).collect();
    let nominal = Duration::from_secs_f64(payload.len() as f64 * 8.0 / (f64::from(mbps) * 1e6));
    let (got, elapsed) = timed_round_trip(entry_addr, &payload).await;
    assert_eq!(got.len(), payload.len(), "the whole payload came back");
    assert_eq!(got, payload);
    // Loopback would move 4 MB in well under 100 ms; brutal paces it out to the
    // rate (0.8 s nominal). The bounds are loose: the pacer may run up to a
    // quarter fast, the test box may be slow.
    assert!(
        elapsed >= nominal.mul_f64(0.6),
        "brutal paced the reply: {elapsed:?} for a nominal {nominal:?}"
    );
    assert!(
        elapsed <= nominal.mul_f64(4.0),
        "brutal did not stall: {elapsed:?} for a nominal {nominal:?}"
    );

    relay.shutdown_all();
    entry.shutdown_all();
    let _ = std::fs::remove_dir_all(&dir);
}
