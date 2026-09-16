#![allow(clippy::unwrap_used, clippy::panic, clippy::expect_used)]

//! `[keepalive]` on a QUIC relay hop: a connection that is quiet stays up past the
//! idle timeout because both ends ping, and a hop whose packets stop arriving is
//! torn down within the idle timeout, releasing the client connection behind it.

use guru_worker::supervisor::{ApplyOutcome, Supervisor};
use guru_worker_config::{
    Config, Forwarding, ForwardingTo, Ipv6Resolve, KeepAlive, ListenAs, LogConfig, QuicTuning,
    RelayHost, RelayProtocol, Remote, TlsHostConfig,
};
use rcgen::{BasicConstraints, CertificateParams, DnType, IsCa, Issuer, KeyPair};
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

const RELAY_SNI: &str = "pod1.relay.guru.internal";

/// Short enough to test against, long enough that a ping always lands first.
const KEEPALIVE: KeepAlive = KeepAlive {
    tcp_idle_secs: 60,
    tcp_interval_secs: 10,
    tcp_retries: 3,
    quic_ping_secs: 1,
    quic_idle_secs: 2,
};

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

struct Pki {
    ca: PathBuf,
    leaf: TlsHostConfig,
}

fn pki(dir: &Path) -> Pki {
    let ca_key = KeyPair::generate().unwrap();
    let ca_cert = ca_params("guru internal relay CA")
        .self_signed(&ca_key)
        .unwrap();
    let issuer = Issuer::new(ca_params("guru internal relay CA"), ca_key);
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
        keepalive: KEEPALIVE,
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

fn relay_worker(listen: SocketAddr, leaf: &TlsHostConfig, echo: SocketAddr) -> Config {
    config(
        None,
        Forwarding {
            tag: "relay".to_string(),
            listen,
            receive_proxy_protocol: None,
            listen_as: ListenAs::Relay(RelayHost::Quic(leaf.clone())),
            quic: None,
            to: ForwardingTo::Exit {
                destination: Remote::Address(echo),
                send_proxy_protocol: None,
            },
        },
    )
}

fn entry_worker(listen: SocketAddr, relay: SocketAddr, ca: &Path) -> Config {
    config(
        Some(ca.to_path_buf()),
        Forwarding {
            tag: "entry".to_string(),
            listen,
            receive_proxy_protocol: None,
            listen_as: ListenAs::Raw,
            quic: None,
            to: ForwardingTo::Relay {
                protocol: RelayProtocol::Quic,
                destination: Remote::Address(relay),
                sni: Some(RELAY_SNI.to_string()),
                quic: None,
            },
        },
    )
}

/// A UDP forwarder in front of the relay's QUIC socket that can be told to drop
/// every packet in both directions: to the entry worker the hop then looks like a
/// host that fell off the network, not one that closed.
async fn udp_forwarder(upstream: SocketAddr) -> (SocketAddr, Arc<AtomicBool>) {
    let front = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let addr = front.local_addr().unwrap();
    let back = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
    back.connect(upstream).await.unwrap();
    let blackhole = Arc::new(AtomicBool::new(false));
    let dropping = blackhole.clone();
    tokio::spawn(async move {
        let mut client: Option<SocketAddr> = None;
        let mut inbound = [0u8; 65535];
        let mut outbound = [0u8; 65535];
        loop {
            tokio::select! {
                received = front.recv_from(&mut inbound) => {
                    let Ok((n, from)) = received else { break };
                    client = Some(from);
                    if !dropping.load(Ordering::Relaxed) {
                        let _ = back.send(&inbound[..n]).await;
                    }
                }
                received = back.recv(&mut outbound) => {
                    let Ok(n) = received else { break };
                    if let (Some(to), false) = (client, dropping.load(Ordering::Relaxed)) {
                        let _ = front.send_to(&outbound[..n], to).await;
                    }
                }
            }
        }
    });
    (addr, blackhole)
}

async fn echoes(client: &mut tokio::net::TcpStream, payload: &[u8]) -> bool {
    client.write_all(payload).await.unwrap();
    let mut got = vec![0u8; payload.len()];
    match tokio::time::timeout(Duration::from_secs(5), client.read_exact(&mut got)).await {
        Ok(Ok(_)) => got == payload,
        Ok(Err(_)) | Err(_) => false,
    }
}

struct Chain {
    relay: Supervisor,
    entry: Supervisor,
    entry_addr: SocketAddr,
    dir: PathBuf,
}

/// Entry → (forwarder) → QUIC relay → echo, all in-process.
async fn chain(name: &str, via_forwarder: bool) -> (Chain, Option<Arc<AtomicBool>>) {
    let _ = quinn::rustls::crypto::ring::default_provider().install_default();
    let dir = std::env::temp_dir().join(format!(
        "guru-worker-keepalive-{name}-{}",
        std::process::id()
    ));
    let _ = std::fs::remove_dir_all(&dir);
    let pki = pki(&dir);
    let echo = echo_server().await;
    let ports = free_ports(2);
    let (relay_addr, entry_addr) = (ports[0], ports[1]);

    let mut relay = Supervisor::new();
    assert_all_applied(
        &relay
            .apply(&relay_worker(relay_addr, &pki.leaf, echo))
            .await,
    );

    let (dial, blackhole) = if via_forwarder {
        let (addr, blackhole) = udp_forwarder(relay_addr).await;
        (addr, Some(blackhole))
    } else {
        (relay_addr, None)
    };
    let mut entry = Supervisor::new();
    assert_all_applied(&entry.apply(&entry_worker(entry_addr, dial, &pki.ca)).await);
    (
        Chain {
            relay,
            entry,
            entry_addr,
            dir,
        },
        blackhole,
    )
}

impl Chain {
    fn finish(self) {
        self.relay.shutdown_all();
        self.entry.shutdown_all();
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

#[tokio::test]
async fn a_quiet_quic_relay_stream_outlives_the_idle_timeout() {
    let (chain, _) = chain("quiet", false).await;
    let mut client = tokio::net::TcpStream::connect(chain.entry_addr)
        .await
        .unwrap();
    assert!(echoes(&mut client, b"first").await, "the hop works");

    // Twice the idle timeout with nothing to say: only the pings keep the QUIC
    // connection under this stream alive.
    tokio::time::sleep(Duration::from_secs(u64::from(KEEPALIVE.quic_idle_secs) * 2)).await;
    assert!(
        echoes(&mut client, b"second").await,
        "a quiet stream is still open after {}s of silence",
        KEEPALIVE.quic_idle_secs * 2
    );
    chain.finish();
}

#[tokio::test]
async fn a_vanished_quic_hop_releases_the_client_connection() {
    let (chain, blackhole) = chain("vanished", true).await;
    let blackhole = blackhole.unwrap();
    let mut client = tokio::net::TcpStream::connect(chain.entry_addr)
        .await
        .unwrap();
    assert!(echoes(&mut client, b"before").await, "the hop works");
    assert_eq!(
        chain.entry.stats().snapshot_and_reset().current_connections,
        1,
        "one client connection is open on the entry worker"
    );

    blackhole.store(true, Ordering::Relaxed);
    // The entry's QUIC connection times out, its splice ends and the client is
    // closed; the relay's connection times out the same way, releasing the echo leg.
    let allowance = Duration::from_secs(u64::from(KEEPALIVE.quic_idle_secs) * 3);
    let mut buf = [0u8; 16];
    let ended = tokio::time::timeout(allowance, client.read(&mut buf)).await;
    assert!(
        matches!(ended, Ok(Ok(0)) | Ok(Err(_))),
        "the client connection ends within {allowance:?} of the hop vanishing: {ended:?}"
    );
    let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
    loop {
        let entry = chain.entry.stats().snapshot_and_reset().current_connections;
        let relay = chain.relay.stats().snapshot_and_reset().current_connections;
        if entry == 0 && relay == 0 {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "both legs are released (entry {entry}, relay {relay})"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    chain.finish();
}
