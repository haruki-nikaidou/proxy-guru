//! Config derivation: golden TOML per topology shape.
//!
//! Set `UPDATE_GOLDEN=1` to rewrite the files under `tests/golden/`.

#![allow(clippy::unwrap_used, clippy::panic, clippy::expect_used)]

#[path = "common/mem.rs"]
mod mem;

use guru_worker_config::{ForwardingTo, ListenAs, RelayHost, TlsHostConfig};
use mem::*;
use orchestration::config::OrchestrationConfig;
use orchestration::entities::db::ca::{RelayCertificateEntity, relay_sni};
use orchestration::entities::db::certificate::{CertificateEntity, CertificateStatus};
use orchestration::entities::db::node::{
    CanvasExportAs, EntryConfig, ExitConfig, LoadBalanceAggregateConfig,
    LoadBalanceDistributeConfig, LoadBalanceMode, NodeSpec, PodConfig, ProxyProtocolVersion,
    RelayConfig, RelayProtocol, TlsConfig,
};
use orchestration::entities::db::port::PortKind;
use orchestration::entities::db::server::{QuicCongestion, ServerId, ServerQuic};
use orchestration::entities::db::topology::CanvasTopology;
use orchestration::entities::db::view::{
    CertificateKind, CertificateRef, InvalidPod, ListenProtocol,
};
use orchestration::services::derive::{
    DerivationCertificates, DeriveError, DerivedConfig, derive_server_config, relay_tls_pods,
};
use orchestration::utils::ids;
use std::path::Path;

fn config() -> OrchestrationConfig {
    OrchestrationConfig::default()
}

/// Derives with no certificate material at all: what every non-TLS shape sees.
fn derive(topology: &CanvasTopology, server: &ServerId) -> Result<DerivedConfig, DeriveError> {
    derive_server_config(
        topology,
        server,
        &DerivationCertificates::default(),
        &config(),
    )
}

/// Renders twice against the same material and asserts byte-stability.
fn derived_with(
    topology: &CanvasTopology,
    server: &ServerId,
    certificates: &DerivationCertificates,
) -> String {
    let render = || {
        derive_server_config(topology, server, certificates, &config())
            .unwrap_or_else(|e| panic!("derive failed: {e}"))
            .config
            .to_toml_string()
            .unwrap_or_else(|e| panic!("rendering failed: {e}"))
    };
    let once = render();
    assert_eq!(once, render(), "derivation is not byte-stable");
    once
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

/// True only for an explicit `UPDATE_GOLDEN=1` / `UPDATE_GOLDEN=true`, so a stray
/// `UPDATE_GOLDEN=0` in the environment cannot turn the suite into a self-comparison.
fn regenerating() -> bool {
    matches!(
        std::env::var("UPDATE_GOLDEN").as_deref(),
        Ok("1") | Ok("true")
    )
}

/// Compares against `tests/golden/<name>.toml` and re-parses the emitted text.
fn assert_golden(name: &str, toml: &str) {
    let path = std::path::Path::new(concat!(env!("CARGO_MANIFEST_DIR"), "/tests/golden"))
        .join(format!("{name}.toml"));
    if regenerating() {
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(&path, toml).unwrap();
    } else {
        let expected = std::fs::read_to_string(&path)
            .unwrap_or_else(|e| panic!("read {}: {e}", path.display()));
        assert_eq!(toml, expected, "derived config for {name} changed");
    }
    guru_worker_config::Config::from_toml_str(toml)
        .unwrap_or_else(|e| panic!("emitted config for {name} does not parse: {e}"));
}

/// Derives twice from independently built snapshots: `hooks::derive` only skips a
/// server whose config did not change, so the same topology must render identical
/// bytes every time.
fn derived(builder: &Builder, server: &ServerId) -> String {
    let once = render(builder, server);
    let twice = render(builder, server);
    assert_eq!(once, twice, "derivation is not byte-stable");
    once
}

fn render(builder: &Builder, server: &ServerId) -> String {
    derive(&builder.build(), server)
        .unwrap_or_else(|e| panic!("derive failed: {e}"))
        .config
        .to_toml_string()
        .unwrap_or_else(|e| panic!("rendering failed: {e}"))
}

#[test]
fn single_pod_to_entry_and_exit() {
    let mut b = Builder::new("prod");
    let s = b.server("tokyo");
    let ip = b.ip("ip1", &s, "203.0.113.10");
    b.named_node("pod", "edge", pod(&ip, 443), pod_ports());
    b.node("entry", entry(None), entry_ports());
    b.node("exit", exit("10.0.0.5:8080"), exit_ports());
    b.connect("pod-listen", "entry-listen");
    b.connect("exit-destination", "pod-destination");

    let result = derive(&b.build(), &s).unwrap();
    assert_golden("single_pod", &result.config.to_toml_string().unwrap());
    assert_eq!(
        result.forwardings.len(),
        1,
        "one pod, one forwarding, one listener capability"
    );
    assert_eq!(result.forwardings[0].serves.port, 443);
    assert!(
        result.forwardings[0].points_at.is_empty(),
        "an exit destination is not a listener this fabric serves"
    );
}

#[test]
fn load_balance_members_follow_port_position() {
    let mut b = Builder::new("prod");
    let s = b.server("tokyo");
    let ip = b.ip("ip1", &s, "203.0.113.10");
    b.named_node("pod", "edge", pod(&ip, 443), pod_ports());
    b.node(
        "entry",
        entry(Some(ProxyProtocolVersion::V2)),
        entry_ports(),
    );
    b.node(
        "lb",
        NodeSpec::LoadBalanceDistribute(LoadBalanceDistributeConfig {
            mode: LoadBalanceMode::Fallback,
            protocol: RelayProtocol::TcpRaw,
            members: Vec::new(),
        }),
        distribute_ports(3),
    );
    // Deliberately connected out of order: emission must follow the port position.
    b.node("exit_c", exit("10.0.0.7:8080"), exit_ports());
    b.node("exit_a", exit("10.0.0.5:8080"), exit_ports());
    b.node("exit_b", exit("backend.internal:9000"), exit_ports());
    b.connect("pod-listen", "entry-listen");
    b.connect("lb-destination", "pod-destination");
    b.connect("exit_c-destination", "lb-member_2");
    b.connect("exit_a-destination", "lb-member_0");
    b.connect("exit_b-destination", "lb-member_1");

    let toml = derived(&b, &s);
    assert_golden("load_balance_fallback", &toml);
    let config = guru_worker_config::Config::from_toml_str(&toml).unwrap();
    let members = match config.forwardings[0].to.tree().unwrap() {
        guru_worker_config::ForwardingTo::LoadBalance(g) => g.members.clone(),
        other => panic!("expected a load balance group, got {other:?}"),
    };
    let destinations: Vec<String> = members
        .iter()
        .map(|m| match m {
            guru_worker_config::ForwardingTo::Exit { destination, .. } => {
                format!("{destination:?}")
            }
            other => panic!("expected exits, got {other:?}"),
        })
        .collect();
    assert!(destinations[0].contains("10.0.0.5"));
    assert!(destinations[1].contains("backend.internal"));
    assert!(destinations[2].contains("10.0.0.7"));
}

#[test]
fn an_aggregate_feeds_the_same_subtree_to_two_pods() {
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

    assert_golden("aggregate_two_pods", &derived(&b, &s));
}

/// tokyo -> osaka -> singapore, each hop a raw TCP relay.
fn relay_chain() -> (Builder, ServerId, ServerId, ServerId) {
    let mut b = Builder::new("prod");
    let tokyo = b.server("tokyo");
    let osaka = b.server("osaka");
    let singapore = b.server("singapore");
    let ip_tokyo = b.ip("ip_tokyo", &tokyo, "203.0.113.10");
    let ip_osaka = b.ip("ip_osaka", &osaka, "198.51.100.10");
    let ip_sg = b.ip("ip_sg", &singapore, "192.0.2.10");

    // Tokyo takes client traffic and relays it to Osaka.
    b.named_node("pod_tokyo", "ingress", pod(&ip_tokyo, 443), pod_ports());
    b.node("entry", entry(None), entry_ports());
    b.named_node(
        "relay_osaka",
        "to-osaka",
        relay(RelayProtocol::TcpRaw),
        relay_ports(),
    );
    // Osaka's pod terminates the relay and hands off to the next relay.
    b.named_node("pod_osaka", "osaka-hop", pod(&ip_osaka, 9443), pod_ports());
    b.named_node(
        "relay_sg",
        "to-singapore",
        relay(RelayProtocol::TcpRaw),
        relay_ports(),
    );
    // Singapore's pod exits to the origin.
    b.named_node("pod_sg", "singapore-hop", pod(&ip_sg, 9443), pod_ports());
    b.node("exit", exit("10.0.0.5:8080"), exit_ports());

    b.connect("pod_tokyo-listen", "entry-listen");
    b.connect("relay_osaka-destination", "pod_tokyo-destination");
    b.connect("pod_osaka-listen", "relay_osaka-listen");
    b.connect("relay_sg-destination", "pod_osaka-destination");
    b.connect("pod_sg-listen", "relay_sg-listen");
    b.connect("exit-destination", "pod_sg-destination");
    (b, tokyo, osaka, singapore)
}

#[test]
fn a_two_hop_relay_chain_derives_each_server() {
    let (b, tokyo, osaka, singapore) = relay_chain();
    assert_golden("relay_chain_tokyo", &derived(&b, &tokyo));
    assert_golden("relay_chain_osaka", &derived(&b, &osaka));
    assert_golden("relay_chain_singapore", &derived(&b, &singapore));
}

fn tls_entry(sni: &str, acme_directory: &str) -> NodeSpec {
    NodeSpec::Entry(EntryConfig {
        receive_proxy_protocol: None,
        tls: Some(TlsConfig {
            sni: sni.to_string(),
            dns_provider: ids::dns_provider_id("cf"),
            domain_id: "zone".to_string(),
            acme_directory: acme_directory.to_string(),
        }),
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

/// One pod behind a TLS entry.
fn tls_pod(acme_directory: &str) -> (Builder, ServerId) {
    let mut b = Builder::new("prod");
    let s = b.server("tokyo");
    let ip = b.ip("ip1", &s, "203.0.113.10");
    b.node("pod", pod(&ip, 443), pod_ports());
    b.node(
        "entry",
        tls_entry("example.com", acme_directory),
        entry_ports(),
    );
    b.node("exit", exit("10.0.0.5:8080"), exit_ports());
    b.connect("pod-listen", "entry-listen");
    b.connect("exit-destination", "pod-destination");
    (b, s)
}

fn only_invalid(result: &DerivedConfig) -> &InvalidPod {
    assert!(
        result.config.forwardings.is_empty(),
        "the only pod is invalid, so nothing is served: {:?}",
        result.config.forwardings
    );
    let [invalid] = result.invalid.as_slice() else {
        panic!("expected exactly one invalid pod, got {:?}", result.invalid);
    };
    invalid
}

#[test]
fn a_tls_entry_without_an_issued_certificate_invalidates_only_its_pod() {
    let (b, s) = tls_pod("https://acme.example/directory");
    let topology = b.build();

    let result = derive(&topology, &s).expect("the server still derives");
    let invalid = only_invalid(&result);
    assert_eq!(invalid.pod, "pod");
    assert_eq!(invalid.listen, "[::]:443");
    assert!(
        invalid
            .error
            .contains("certificate for example.com is pending"),
        "the stored reason must name the certificate: {}",
        invalid.error
    );

    // A failed attempt names its error; a certificate for another directory
    // does not count.
    let mut failed = certificate("c1", "example.com", "https://acme.example/directory", 0);
    failed.status = CertificateStatus::Failed;
    failed.private_key_pem = None;
    failed.full_chain_pem = None;
    failed.last_error = Some("dns propagation timed out".to_string());
    let other = certificate("c2", "example.com", "https://other.example/directory", 4);
    let certificates = DerivationCertificates {
        acme: vec![failed, other],
        ..Default::default()
    };
    let result = derive_server_config(&topology, &s, &certificates, &config()).unwrap();
    let invalid = only_invalid(&result);
    assert!(
        invalid
            .error
            .contains("certificate for example.com is failed: dns propagation timed out"),
        "{}",
        invalid.error
    );
    assert!(result.certificates.is_empty());
}

#[test]
fn a_tls_entry_with_an_issued_certificate_listens_as_tls_and_pins_its_version() {
    // An empty directory resolves to the configured default.
    let (b, s) = tls_pod("");
    let certificates = DerivationCertificates {
        acme: vec![certificate(
            "c1",
            "example.com",
            &config().default_acme_directory,
            7,
        )],
        ..Default::default()
    };
    let result = derive_server_config(&b.build(), &s, &certificates, &config()).unwrap();
    assert!(result.invalid.is_empty(), "{:?}", result.invalid);
    let [forwarding] = result.config.forwardings.as_slice() else {
        panic!("expected one forwarding: {:?}", result.config.forwardings);
    };
    assert_eq!(
        forwarding.listen_as,
        ListenAs::Tls(TlsHostConfig {
            key: "certs/acme/c1/key.pem".into(),
            full_chain: "certs/acme/c1/full_chain.pem".into(),
        })
    );
    let pinned = vec![CertificateRef {
        kind: CertificateKind::Acme,
        key: "c1".to_string(),
        version: 7,
    }];
    assert_eq!(result.certificates, pinned);
    assert_eq!(result.forwardings[0].certificates, pinned);
    assert_eq!(result.forwardings[0].serves.protocol, ListenProtocol::Raw);
    assert!(result.config.relay_ca.is_none(), "no CA, no relay_ca");
    assert_golden("tls_entry", &result.config.to_toml_string().unwrap());
}

/// tokyo (entry -> pod_tokyo -> relay) -> osaka (pod_osaka -> exit), with the
/// relay hop in the given protocol.
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
fn a_secure_relay_needs_the_internal_ca_on_both_ends() {
    for protocol in [RelayProtocol::TcpTls, RelayProtocol::Quic] {
        let (b, tokyo, osaka) = secure_relay(protocol);
        let topology = b.build();
        let relay_pods: Vec<String> = relay_tls_pods(&topology)
            .iter()
            .map(|p| p.to_string())
            .collect();
        assert_eq!(
            relay_pods,
            ["pod_osaka"],
            "only the relay's listening pod needs a leaf"
        );

        // No CA: neither end can be built.
        for server in [&tokyo, &osaka] {
            let result = derive(&topology, server).unwrap();
            let invalid = only_invalid(&result);
            assert!(
                invalid.error.contains("internal CA not initialised"),
                "{protocol:?} {}: {}",
                invalid.pod,
                invalid.error
            );
        }

        // CA but no leaf yet: the dialer derives (it only needs the CA), the
        // listener waits for its leaf.
        let certificates = DerivationCertificates {
            ca_present: true,
            ..Default::default()
        };
        let result = derive_server_config(&topology, &tokyo, &certificates, &config()).unwrap();
        assert!(result.invalid.is_empty(), "{:?}", result.invalid);
        assert_eq!(
            result.config.relay_ca.as_deref(),
            Some(Path::new("certs/ca.pem"))
        );
        let result = derive_server_config(&topology, &osaka, &certificates, &config()).unwrap();
        let invalid = only_invalid(&result);
        assert!(
            invalid.error.contains("relay certificate not issued yet"),
            "{}",
            invalid.error
        );
    }
}

#[test]
fn a_secure_relay_with_a_leaf_derives_listener_dialer_and_pins() {
    for (protocol, wire, listen_protocol) in [
        (
            RelayProtocol::TcpTls,
            guru_worker_config::RelayProtocol::TlsOverTcp,
            ListenProtocol::RelayTls,
        ),
        (
            RelayProtocol::Quic,
            guru_worker_config::RelayProtocol::Quic,
            ListenProtocol::RelayQuic,
        ),
    ] {
        let (b, tokyo, osaka) = secure_relay(protocol);
        let topology = b.build();
        let certificates = DerivationCertificates {
            relay: vec![relay_leaf("leaf1", "pod_osaka", 2)],
            ca_present: true,
            ..Default::default()
        };
        let sni = relay_sni(&ids::node_id("pod_osaka"));
        assert_eq!(sni, "pod_osaka.relay.guru.internal");
        let host = TlsHostConfig {
            key: "certs/relay/pod_osaka/key.pem".into(),
            full_chain: "certs/relay/pod_osaka/full_chain.pem".into(),
        };

        // The listening end.
        let result = derive_server_config(&topology, &osaka, &certificates, &config()).unwrap();
        assert!(result.invalid.is_empty(), "{:?}", result.invalid);
        let expected = match protocol {
            RelayProtocol::Quic => RelayHost::Quic(host.clone()),
            _ => RelayHost::TlsOverTcp(host.clone()),
        };
        assert_eq!(
            result.config.forwardings[0].listen_as,
            ListenAs::Relay(expected)
        );
        assert_eq!(result.forwardings[0].serves.protocol, listen_protocol);
        let pinned = vec![CertificateRef {
            kind: CertificateKind::Relay,
            key: "leaf1".to_string(),
            version: 2,
        }];
        assert_eq!(result.certificates, pinned);
        assert_eq!(result.forwardings[0].certificates, pinned);
        assert_eq!(
            result.config.relay_ca.as_deref(),
            Some(Path::new("certs/ca.pem"))
        );

        // The dialing end verifies the leaf by SNI and points at the secure listener.
        let result = derive_server_config(&topology, &tokyo, &certificates, &config()).unwrap();
        assert!(result.invalid.is_empty(), "{:?}", result.invalid);
        match result.config.forwardings[0].to.tree().unwrap() {
            ForwardingTo::Relay {
                protocol, sni: got, ..
            } => {
                assert_eq!(*protocol, wire);
                assert_eq!(got.as_deref(), Some(sni.as_str()));
            }
            other => panic!("expected a relay destination, got {other:?}"),
        }
        assert_eq!(result.forwardings[0].points_at[0].protocol, listen_protocol);
        assert!(result.certificates.is_empty(), "the dialer pins nothing");
        assert_eq!(
            result.config.relay_ca.as_deref(),
            Some(Path::new("certs/ca.pem"))
        );

        let name = match protocol {
            RelayProtocol::Quic => "secure_relay_quic",
            _ => "secure_relay_tls",
        };
        assert_golden(
            &format!("{name}_tokyo"),
            &derived_with(&topology, &tokyo, &certificates),
        );
        assert_golden(
            &format!("{name}_osaka"),
            &derived_with(&topology, &osaka, &certificates),
        );
    }
}

/// Each end of a QUIC link gets the worker-wide `[quic]` of its own server and,
/// on the link's forwarding, the numbers paired with the peer: it sends at the
/// lower of its up rate and the peer's down rate, and receives at the lower of
/// its down rate and the peer's up rate. Equal settings on both ends leave the
/// forwardings bare; a TLS relay never carries any of it.
#[test]
fn quic_rates_are_paired_per_link() {
    use guru_worker_config::{QuicCongestion as WireCongestion, QuicTuning};

    let (mut b, tokyo, osaka) = secure_relay(RelayProtocol::Quic);
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
            down_mbps: 1000,
            stream_receive_window: 0,
            conn_receive_window: 268_435_456,
        },
    );
    let topology = b.build();
    let certificates = DerivationCertificates {
        ca_present: true,
        assume_issued: true,
        ..DerivationCertificates::default()
    };

    // Tokyo dials: it may send its full 200 (Osaka takes 1000) but Osaka only
    // sends 1000, not the 2000 Tokyo could take.
    let tokyo_cfg = derive_server_config(&topology, &tokyo, &certificates, &config())
        .unwrap()
        .config;
    assert_eq!(
        tokyo_cfg.quic,
        QuicTuning {
            congestion: WireCongestion::Brutal,
            send_mbps: 200,
            receive_mbps: 2000,
            stream_receive_window: 67_108_864,
            ..QuicTuning::default()
        }
    );
    match tokyo_cfg.forwardings[0].to.tree().unwrap() {
        ForwardingTo::Relay { quic, .. } => assert_eq!(
            *quic,
            Some(QuicTuning {
                congestion: WireCongestion::Brutal,
                send_mbps: 200,
                receive_mbps: 1000,
                stream_receive_window: 67_108_864,
                ..QuicTuning::default()
            })
        ),
        other => panic!("expected a relay destination, got {other:?}"),
    }
    assert!(
        tokyo_cfg.forwardings[0].quic.is_none(),
        "a raw listener carries nothing"
    );

    // Osaka listens: it sends at its 1000 (Tokyo takes 2000) and receives 200.
    let osaka_cfg = derive_server_config(&topology, &osaka, &certificates, &config())
        .unwrap()
        .config;
    assert_eq!(osaka_cfg.quic.receive_window, 268_435_456);
    assert_eq!(
        osaka_cfg.forwardings[0].quic,
        Some(QuicTuning {
            congestion: WireCongestion::Brutal,
            send_mbps: 1000,
            receive_mbps: 200,
            receive_window: 268_435_456,
            ..QuicTuning::default()
        })
    );
    // Both parse on a worker: the link numbers ride along as `[forwarding.quic]`.
    for cfg in [&tokyo_cfg, &osaka_cfg] {
        let text = cfg.to_toml_string().unwrap();
        assert_eq!(
            &guru_worker_config::Config::from_toml_str(&text).unwrap(),
            cfg
        );
    }

    // The same numbers on both ends: nothing to pair, the forwardings stay bare.
    let mut b = secure_relay(RelayProtocol::Quic).0;
    let same = ServerQuic {
        congestion: QuicCongestion::Brutal,
        up_mbps: 1000,
        down_mbps: 1000,
        stream_receive_window: 67_108_864,
        conn_receive_window: 268_435_456,
    };
    b.server_quic(&tokyo, same);
    b.server_quic(&osaka, same);
    let topology = b.build();
    for server in [&tokyo, &osaka] {
        let cfg = derive_server_config(&topology, server, &certificates, &config())
            .unwrap()
            .config;
        assert_eq!(cfg.quic.send_mbps, 1000);
        assert!(cfg.forwardings[0].quic.is_none());
        assert!(matches!(
            cfg.forwardings[0].to.tree().unwrap(),
            ForwardingTo::Relay { quic: None, .. } | ForwardingTo::Exit { .. }
        ));
    }

    // A TLS relay between the same servers gets the worker-wide section only.
    let (mut b, tokyo, osaka) = secure_relay(RelayProtocol::TcpTls);
    b.server_quic(&tokyo, same);
    b.server_quic(&osaka, same);
    let topology = b.build();
    let cfg = derive_server_config(&topology, &tokyo, &certificates, &config())
        .unwrap()
        .config;
    assert_eq!(cfg.quic.send_mbps, 1000);
    assert!(matches!(
        cfg.forwardings[0].to.tree().unwrap(),
        ForwardingTo::Relay { quic: None, .. }
    ));
}

#[test]
fn a_pod_with_an_unconnected_port_is_skipped() {
    let mut b = Builder::new("prod");
    let s = b.server("tokyo");
    let ip = b.ip("ip1", &s, "203.0.113.10");
    b.node("pod", pod(&ip, 443), pod_ports());
    let result = derive(&b.build(), &s).unwrap();
    assert!(result.config.forwardings.is_empty());
    assert!(result.forwardings.is_empty());
}

/// The point of per-pod isolation: a half-drawn relay must not cost the server
/// its other pods.
#[test]
fn a_broken_pod_leaves_its_neighbour_deriving() {
    let mut b = Builder::new("prod");
    let s = b.server("tokyo");
    let ip = b.ip("ip1", &s, "203.0.113.10");

    // Healthy: entry -> good -> exit.
    b.node("good", pod(&ip, 443), pod_ports());
    b.node("in", entry(None), entry_ports());
    b.node("out", exit("10.0.0.5:8080"), exit_ports());
    b.connect("good-listen", "in-listen");
    b.connect("out-destination", "good-destination");

    // Broken: dials a relay whose own listen side nothing feeds yet.
    b.node("half", pod(&ip, 8443), pod_ports());
    b.node("in2", entry(None), entry_ports());
    b.node("hop", relay(RelayProtocol::TcpRaw), relay_ports());
    b.connect("half-listen", "in2-listen");
    b.connect("hop-destination", "half-destination");

    let result = derive(&b.build(), &s).expect("the healthy pod still derives");
    let tags: Vec<&str> = result
        .config
        .forwardings
        .iter()
        .map(|f| f.tag.as_str())
        .collect();
    assert_eq!(tags, ["good"], "the healthy pod is served on its own");
    assert_eq!(result.forwardings.len(), 1);
    let [invalid] = result.invalid.as_slice() else {
        panic!("expected exactly one invalid pod, got {:?}", result.invalid);
    };
    assert_eq!(invalid.pod, "half");
    assert_eq!(invalid.listen, "[::]:8443");
}

/// A load-balance group with no connected members used to reach the worker as an
/// empty group and fail the whole server at validation.
#[test]
fn an_empty_load_balance_group_invalidates_only_its_pod() {
    let mut b = Builder::new("prod");
    let s = b.server("tokyo");
    let ip = b.ip("ip1", &s, "203.0.113.10");

    b.node("good", pod(&ip, 443), pod_ports());
    b.node("in", entry(None), entry_ports());
    b.node("out", exit("10.0.0.5:8080"), exit_ports());
    b.connect("good-listen", "in-listen");
    b.connect("out-destination", "good-destination");

    b.node("lb-pod", pod(&ip, 8443), pod_ports());
    b.node("in2", entry(None), entry_ports());
    b.node(
        "lb",
        NodeSpec::LoadBalanceDistribute(LoadBalanceDistributeConfig {
            mode: LoadBalanceMode::RoundRobin,
            protocol: RelayProtocol::TcpRaw,
            members: Vec::new(),
        }),
        distribute_ports(2),
    );
    b.connect("lb-pod-listen", "in2-listen");
    b.connect("lb-destination", "lb-pod-destination");

    let result = derive(&b.build(), &s).expect("the healthy pod still derives");
    let tags: Vec<&str> = result
        .config
        .forwardings
        .iter()
        .map(|f| f.tag.as_str())
        .collect();
    assert_eq!(tags, ["good"]);
    let [invalid] = result.invalid.as_slice() else {
        panic!("expected exactly one invalid pod, got {:?}", result.invalid);
    };
    assert_eq!(invalid.pod, "lb-pod");
}

/// An `OutputOutOfCanvas` export of kind `DeriveDestination`: its port inside the
/// subcanvas is an input, the mirrored import port an output.
fn dest_out() -> (NodeSpec, Vec<PortSpec>) {
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
}

/// The exact topology of `load_balance_members_follow_port_position`, with the
/// load balancer and its exits moved into a subcanvas: the pod's destination in
/// the root is fed by the import node's mirrored port. Byte-identical TOML is the
/// proof that the tree derives as its flattened graph.
#[test]
fn a_destination_through_an_import_node_derives_like_the_flat_graph() {
    let mut b = Builder::new("prod");
    let s = b.server("tokyo");
    let ip = b.ip("ip1", &s, "203.0.113.10");
    b.named_node("pod", "edge", pod(&ip, 443), pod_ports());
    b.node(
        "entry",
        entry(Some(ProxyProtocolVersion::V2)),
        entry_ports(),
    );
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
    let (spec, ports) = dest_out();
    b.node("out", spec, ports);
    b.node(
        "lb",
        NodeSpec::LoadBalanceDistribute(LoadBalanceDistributeConfig {
            mode: LoadBalanceMode::Fallback,
            protocol: RelayProtocol::TcpRaw,
            members: Vec::new(),
        }),
        distribute_ports(3),
    );
    b.node("exit_c", exit("10.0.0.7:8080"), exit_ports());
    b.node("exit_a", exit("10.0.0.5:8080"), exit_ports());
    b.node("exit_b", exit("backend.internal:9000"), exit_ports());
    b.connect("lb-destination", "out-export");
    b.connect("exit_c-destination", "lb-member_2");
    b.connect("exit_a-destination", "lb-member_0");
    b.connect("exit_b-destination", "lb-member_1");

    let toml = derived(&b, &s);
    let flat = std::fs::read_to_string(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/tests/golden/load_balance_fallback.toml"
    ))
    .unwrap();
    assert_eq!(
        toml, flat,
        "the nested graph must derive the flat graph's TOML"
    );
    let result = derive(&b.build(), &s).unwrap();
    assert!(result.invalid.is_empty(), "{:?}", result.invalid);
    assert!(result.forwardings[0].points_at.is_empty());
}

/// root -> sub -> subsub. Server `a` (root) runs a pod whose destination crosses
/// both boundaries to an exit in `subsub`; server `b` (root) runs an unrelated
/// pod with its own exit.
fn three_levels() -> (Builder, ServerId, ServerId) {
    let mut b = Builder::new("root");
    let a = b.server("a");
    let ip_a = b.ip("ip_a", &a, "203.0.113.10");
    let bb = b.server("b");
    let ip_b = b.ip("ip_b", &bb, "203.0.113.20");
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
    b.named_node("pod_b", "shallow", pod(&ip_b, 443), pod_ports());
    b.node("entry_b", entry(None), entry_ports());
    b.node("exit_b", exit("10.0.0.9:8080"), exit_ports());
    b.connect("pod_b-listen", "entry_b-listen");
    b.connect("exit_b-destination", "pod_b-destination");

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
    (b, a, bb)
}

#[test]
fn three_level_nesting_derives_each_server() {
    let (b, a, bb) = three_levels();
    assert_golden("nested_three_levels_a", &derived(&b, &a));
    assert_golden("nested_three_levels_b", &derived(&b, &bb));
}

#[test]
fn a_boundary_that_is_not_wired_through_invalidates_only_its_pod() {
    let (b, a, bb) = three_levels();
    let mut topology = b.build();
    // The innermost export is left unconnected: the chain from `deep` dead-ends
    // inside `subsub`, while `shallow` on the other server is untouched.
    topology
        .edges
        .retain(|e| e.id.to_string() != "exit_deep-destination->subsub_out-export");
    let result = derive(&topology, &a).unwrap();
    assert!(result.config.forwardings.is_empty());
    let [invalid] = result.invalid.as_slice() else {
        panic!("expected exactly one invalid pod, got {:?}", result.invalid);
    };
    assert_eq!(invalid.pod, "deep");
    assert!(
        invalid.error.contains("not connected through"),
        "{}",
        invalid.error
    );
    let other = derive(&topology, &bb).unwrap();
    assert_eq!(other.config.forwardings.len(), 1);
    assert!(other.invalid.is_empty());
}
