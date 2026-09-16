#![allow(clippy::unwrap_used, clippy::panic, clippy::expect_used)]

//! What a QUIC relay hop actually carries, one way, over loopback.
//!
//! Run with `cargo test --release -p guru-worker --test quic_throughput -- --ignored --nocapture`.
//! Not a test of correctness: it prints the rate so a human can compare it with
//! the rate the config asked for.

use guru_worker::supervisor::Supervisor;
use guru_worker_config::{
    Config, Forwarding, ForwardingTo, Ipv6Resolve, KeepAlive, ListenAs, LogConfig, QuicCongestion,
    QuicTuning, RelayHost, RelayProtocol, Remote, TlsHostConfig,
};
use rcgen::{BasicConstraints, CertificateParams, DnType, IsCa, Issuer, KeyPair};
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

const SNI: &str = "pod1.relay.guru.internal";

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

fn pki(dir: &Path) -> (PathBuf, TlsHostConfig) {
    let ca_key = KeyPair::generate().unwrap();
    let ca_cert = ca_params("bench CA").self_signed(&ca_key).unwrap();
    let issuer = Issuer::new(ca_params("bench CA"), ca_key);
    let leaf_key = KeyPair::generate().unwrap();
    let leaf = CertificateParams::new(vec![SNI.to_string()])
        .unwrap()
        .signed_by(&leaf_key, &issuer)
        .unwrap();
    std::fs::create_dir_all(dir).unwrap();
    let (ca, full_chain, key) = (
        dir.join("ca.pem"),
        dir.join("full_chain.pem"),
        dir.join("key.pem"),
    );
    std::fs::write(&ca, ca_cert.pem()).unwrap();
    std::fs::write(&full_chain, format!("{}{}", leaf.pem(), ca_cert.pem())).unwrap();
    std::fs::write(&key, leaf_key.serialize_pem()).unwrap();
    (ca, TlsHostConfig { key, full_chain })
}

/// A backend that answers any byte with `bytes` bytes and closes.
async fn bulk_source(bytes: usize) -> SocketAddr {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        while let Ok((mut stream, _)) = listener.accept().await {
            tokio::spawn(async move {
                let mut one = [0u8; 1];
                if stream.read(&mut one).await.is_err() {
                    return;
                }
                let chunk = vec![7u8; 64 * 1024];
                let mut sent = 0;
                while sent < bytes {
                    let n = chunk.len().min(bytes - sent);
                    if stream.write_all(&chunk[..n]).await.is_err() {
                        return;
                    }
                    sent += n;
                }
                let _ = stream.shutdown().await;
            });
        }
    });
    addr
}

fn config(relay_ca: Option<PathBuf>, quic: QuicTuning, forwarding: Forwarding) -> Config {
    Config {
        ipv6_resolve: Ipv6Resolve::Tolerated,
        log: LogConfig::default(),
        relay_ca,
        keepalive: KeepAlive::default(),
        quic,
        forwardings: vec![forwarding],
    }
}

/// Bytes through one QUIC relay hop whose sending side is configured with `quic`.
async fn measure(name: &str, quic: QuicTuning, payload: usize) -> (Duration, usize) {
    let _ = quinn::rustls::crypto::ring::default_provider().install_default();
    let _ = tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::new("guru_worker=debug,warn"))
        .try_init();
    // A distinct CA path per run: `tls::quic_client` caches one dialer per path.
    let dir = std::env::temp_dir().join(format!("guru-quic-bench-{}-{name}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    let (ca, leaf) = pki(&dir);
    let source = bulk_source(payload).await;
    let ports = free_ports(2);
    let (relay_addr, entry_addr) = (ports[0], ports[1]);

    // The relay is the sending side of the download: it is the QUIC listener.
    let mut relay = Supervisor::new();
    let outcome = relay
        .apply(&config(
            None,
            quic,
            Forwarding {
                tag: "relay".to_string(),
                listen: relay_addr,
                receive_proxy_protocol: None,
                listen_as: ListenAs::Relay(RelayHost::Quic(leaf)),
                quic: None,
                to: ForwardingTo::Exit {
                    destination: Remote::Address(source),
                    send_proxy_protocol: None,
                },
            },
        ))
        .await;
    for pod in outcome.failed() {
        println!("RELAY POD FAILED {}: {:?}", pod.tag, pod.error);
    }
    let mut entry = Supervisor::new();
    let outcome = entry
        .apply(&config(
            Some(ca),
            quic,
            Forwarding {
                tag: "entry".to_string(),
                listen: entry_addr,
                receive_proxy_protocol: None,
                listen_as: ListenAs::Raw,
                quic: None,
                to: ForwardingTo::Relay {
                    protocol: RelayProtocol::Quic,
                    destination: Remote::Address(relay_addr),
                    sni: Some(SNI.to_string()),
                    quic: None,
                },
            },
        ))
        .await;
    for pod in outcome.failed() {
        println!("ENTRY POD FAILED {}: {:?}", pod.tag, pod.error);
    }

    let mut client = tokio::net::TcpStream::connect(entry_addr).await.unwrap();
    client.write_all(b"g").await.unwrap();
    let started = Instant::now();
    let mut got = 0usize;
    let mut buf = vec![0u8; 256 * 1024];
    loop {
        match tokio::time::timeout(Duration::from_secs(60), client.read(&mut buf)).await {
            Ok(Ok(0)) | Ok(Err(_)) | Err(_) => break,
            Ok(Ok(n)) => {
                got += n;
                if got >= payload {
                    break;
                }
            }
        }
    }
    let elapsed = started.elapsed();
    relay.shutdown_all();
    entry.shutdown_all();
    let _ = std::fs::remove_dir_all(&dir);
    (elapsed, got)
}

fn report(name: &str, elapsed: Duration, got: usize) {
    let mbps = (got as f64 * 8.0) / elapsed.as_secs_f64() / 1e6;
    println!("{name}: {got} bytes in {elapsed:?} = {mbps:.1} Mbps");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "a benchmark: run it by name with --nocapture"]
async fn quic_hop_throughput() {
    let payload = 64 * 1024 * 1024;
    let (elapsed, got) = measure("default", QuicTuning::default(), payload).await;
    report("cubic, quinn defaults", elapsed, got);

    let (elapsed, got) = measure(
        "cubic-tuned",
        QuicTuning {
            congestion: QuicCongestion::Cubic,
            send_mbps: 1000,
            receive_mbps: 1000,
            stream_receive_window: 67_108_864,
            receive_window: 268_435_456,
            ..QuicTuning::default()
        },
        payload,
    )
    .await;
    report("cubic, tuned windows", elapsed, got);

    let (elapsed, got) = measure(
        "brutal",
        QuicTuning {
            congestion: QuicCongestion::Brutal,
            send_mbps: 1000,
            receive_mbps: 1000,
            stream_receive_window: 67_108_864,
            receive_window: 268_435_456,
            ..QuicTuning::default()
        },
        payload,
    )
    .await;
    report("brutal 1000 Mbps", elapsed, got);
}
