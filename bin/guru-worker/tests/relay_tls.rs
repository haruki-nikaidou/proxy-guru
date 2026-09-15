#![allow(clippy::unwrap_used, clippy::panic, clippy::expect_used)]

//! Relay hops over TLS and QUIC verify the next hop against `relay_ca`: two
//! supervisors in one process, an entry worker dialing a relay worker whose leaf is
//! signed by the internal CA, and an entry worker trusting the wrong CA getting nothing.

use guru_worker::supervisor::{ApplyOutcome, Supervisor};
use guru_worker_config::{
    Config, Forwarding, ForwardingTo, Ipv6Resolve, KeepAlive, ListenAs, LogConfig, RelayHost,
    RelayProtocol, Remote, TlsHostConfig,
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
            to: ForwardingTo::Exit {
                destination: Remote::Address(echo),
                send_proxy_protocol: None,
            },
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
            to: ForwardingTo::Relay {
                protocol,
                destination: Remote::Address(relay),
                sni: Some(RELAY_SNI.to_string()),
            },
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
