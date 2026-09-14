//! The internal CA: initialisation, relay leaf issuance and rotation, delivery
//! bundles, and the derivation hook's use of all three.

#![allow(clippy::unwrap_used, clippy::panic, clippy::expect_used)]

mod common;

use chrono::Utc;
use common::*;
use kanau::processor::Processor;
use orchestration::entities::surreal::ca::{
    FindInternalCa, ListRelayCertificatesByPods, RelayCertificateEntity, StoreRelayCertificate,
    relay_sni,
};
use orchestration::entities::surreal::canvas::CanvasId;
use orchestration::entities::surreal::certificate::{EnsureCertificate, StoreIssuedCertificate};
use orchestration::entities::surreal::dns::{CreateDnsProvider, DnsProvider};
use orchestration::entities::surreal::health::{ListNodeHealthHistory, NodeHealthStatus};
use orchestration::entities::surreal::node::{
    EntryConfig, ExitConfig, NodeId, NodeSpec, NodeWithPorts, PodConfig, RelayConfig,
    RelayProtocol, TlsConfig,
};
use orchestration::entities::surreal::server::{FindServerById, ServerId, ServerIpv6Resolve};
use orchestration::entities::surreal::view::{
    AckServerConfig, CertificateKind, CertificateRef, TakeInFlight,
};
use orchestration::hooks::derive::rotate_expiring_relay_certificates;
use orchestration::services::OrchestrationError;
use orchestration::services::agent::RegisterWorker;
use orchestration::services::ca::{
    BundleCertificates, CA_COMMON_NAME, CA_FILE, CertificateFile, EnsureRelayCertificates,
    InitInternalCa,
};
use orchestration::services::canvas as canvas_service;
use orchestration::services::edge::Connect;
use orchestration::services::node::CreateNode;
use orchestration::services::server::{AddressOverrides, CreateServer};
use orchestration::utils::ids::record_key;
use x509_parser::prelude::*;

fn parse_pem(pem: &str) -> Vec<u8> {
    let (_, parsed) = x509_parser::pem::parse_x509_pem(pem.as_bytes()).expect("valid PEM");
    parsed.contents
}

/// The leaf verifies against the CA, names `sni` in its SAN and CN, and is not a CA.
fn assert_leaf_signed_by(leaf_pem: &str, ca_pem: &str, sni: &str) {
    let (leaf_der, ca_der) = (parse_pem(leaf_pem), parse_pem(ca_pem));
    let (_, leaf) = X509Certificate::from_der(&leaf_der).unwrap();
    let (_, ca) = X509Certificate::from_der(&ca_der).unwrap();
    assert_eq!(leaf.issuer(), ca.subject());
    leaf.verify_signature(Some(ca.public_key()))
        .expect("the leaf signature verifies against the CA key");
    let sans: Vec<String> = leaf
        .subject_alternative_name()
        .unwrap()
        .expect("a SAN extension")
        .value
        .general_names
        .iter()
        .map(|n| match n {
            GeneralName::DNSName(name) => (*name).to_string(),
            other => panic!("unexpected SAN {other:?}"),
        })
        .collect();
    assert_eq!(sans, [sni]);
    let cn = leaf
        .subject()
        .iter_common_name()
        .next()
        .unwrap()
        .as_str()
        .unwrap();
    assert_eq!(cn, sni);
    assert!(!leaf.is_ca());
    assert!(ca.is_ca());
    assert_eq!(
        ca.subject()
            .iter_common_name()
            .next()
            .unwrap()
            .as_str()
            .unwrap(),
        CA_COMMON_NAME
    );
}

#[tokio::test]
async fn init_ca_refuses_a_second_init() -> TestResult {
    let w = world().await?;
    assert!(w.db.process(FindInternalCa).await?.is_none());

    let initialized = w.ca.process(InitInternalCa).await?;
    assert!(initialized.certificate_pem.contains("BEGIN CERTIFICATE"));
    assert!(initialized.touched_canvases.is_empty());
    let row = w.db.process(FindInternalCa).await?.expect("the CA row");
    assert_eq!(row.certificate_pem, initialized.certificate_pem);
    assert!(
        row.private_key_pem.starts_with("enc1:"),
        "the key is stored encrypted"
    );
    assert!(row.not_after > Utc::now() + chrono::Duration::days(3600));
    assert!(
        w.secrets
            .decrypt_str(&row.private_key_pem)?
            .contains("PRIVATE KEY")
    );

    let again = w.ca.process(InitInternalCa).await.unwrap_err();
    assert!(matches!(again, OrchestrationError::Conflict(_)), "{again}");
    let unchanged = w.db.process(FindInternalCa).await?.expect("the CA row");
    assert_eq!(unchanged.certificate_pem, row.certificate_pem);
    Ok(())
}

#[tokio::test]
async fn relay_leaves_are_issued_stable_and_rotated_when_expiring() -> TestResult {
    let w = world().await?;
    let pod = orchestration::utils::ids::node_id("pod_a");

    let missing =
        w.ca.process(EnsureRelayCertificates {
            pods: vec![pod.clone()],
        })
        .await
        .unwrap_err();
    assert!(
        matches!(&missing, OrchestrationError::Conflict(m) if m.contains("not initialised")),
        "{missing}"
    );

    let ca = w.ca.process(InitInternalCa).await?;
    let issued =
        w.ca.process(EnsureRelayCertificates {
            pods: vec![pod.clone()],
        })
        .await?;
    let [leaf] = issued.as_slice() else {
        panic!("one leaf per pod: {issued:?}");
    };
    assert_eq!(leaf.sni, relay_sni(&pod));
    assert_eq!(leaf.version, 1);
    assert_leaf_signed_by(&leaf.certificate_pem, &ca.certificate_pem, &leaf.sni);
    assert!(leaf.private_key_pem.starts_with("enc1:"));
    let valid_for = leaf.not_after - Utc::now();
    let expected = chrono::Duration::from_std(w.config.relay_cert_valid())?;
    assert!(
        valid_for > expected - chrono::Duration::minutes(1) && valid_for <= expected,
        "validity is config.relay_cert_valid: {valid_for}"
    );

    // Valid for long enough: untouched.
    let again =
        w.ca.process(EnsureRelayCertificates {
            pods: vec![pod.clone()],
        })
        .await?;
    assert_eq!(again[0].version, 1);
    assert_eq!(again[0].certificate_pem, leaf.certificate_pem);

    // Backdate the leaf into the renewal window: the next ensure rotates it.
    w.db.process(StoreRelayCertificate {
        pod: pod.clone(),
        sni: leaf.sni.clone(),
        private_key_pem: leaf.private_key_pem.clone(),
        certificate_pem: leaf.certificate_pem.clone(),
        not_before: leaf.not_before,
        not_after: Utc::now() + chrono::Duration::hours(1),
        expected_version: None,
    })
    .await?
    .expect("the unconditional store writes the row");
    let rotated =
        w.ca.process(EnsureRelayCertificates {
            pods: vec![pod.clone()],
        })
        .await?;
    assert_eq!(
        rotated[0].version, 3,
        "the manual store and the rotation each bumped it"
    );
    assert_ne!(rotated[0].certificate_pem, leaf.certificate_pem);
    assert_leaf_signed_by(&rotated[0].certificate_pem, &ca.certificate_pem, &leaf.sni);
    let stored =
        w.db.process(ListRelayCertificatesByPods {
            pods: vec![pod.clone()],
        })
        .await?;
    assert_eq!(stored.len(), 1, "one row per pod: {stored:?}");
    assert_eq!(stored[0].version, 3);
    Ok(())
}

#[tokio::test]
async fn bundles_carry_decrypted_keys_at_the_worker_paths() -> TestResult {
    let w = world().await?;
    let ca = w.ca.process(InitInternalCa).await?;
    let pod = orchestration::utils::ids::node_id("pod_a");
    let leaf =
        w.ca.process(EnsureRelayCertificates {
            pods: vec![pod.clone()],
        })
        .await?
        .remove(0);

    let dns =
        w.db.process(CreateDnsProvider {
            name: "cf".to_string(),
            provider: DnsProvider::Cloudflare,
            account_id: String::new(),
            api_secret: w.secrets.encrypt_str("token")?,
            now: Utc::now(),
        })
        .await?;
    let certificate =
        w.db.process(EnsureCertificate {
            sni: "example.com".to_string(),
            dns_provider: dns.id.clone(),
            domain_id: "zone".to_string(),
            acme_directory: w.config.default_acme_directory.clone(),
            now: Utc::now(),
        })
        .await?;
    w.db.process(StoreIssuedCertificate {
        id: certificate.id.clone(),
        acme_account_key: w.secrets.encrypt_str("acct")?,
        private_key_pem: w.secrets.encrypt_str("ACME KEY PEM")?,
        full_chain_pem: "ACME CHAIN PEM".to_string(),
        not_before: Utc::now(),
        not_after: Utc::now() + chrono::Duration::days(60),
        now: Utc::now(),
    })
    .await?;

    let refs = vec![
        CertificateRef {
            kind: CertificateKind::Acme,
            key: record_key(&certificate.id.0),
            version: 1,
        },
        CertificateRef {
            kind: CertificateKind::Relay,
            key: record_key(&leaf.id.0),
            version: leaf.version,
        },
    ];
    let files =
        w.ca.process(BundleCertificates {
            refs: &refs,
            ca: true,
        })
        .await?;
    let acme_key = format!("certs/acme/{}/key.pem", record_key(&certificate.id.0));
    let acme_chain = format!(
        "certs/acme/{}/full_chain.pem",
        record_key(&certificate.id.0)
    );
    let expected = vec![
        CertificateFile {
            path: acme_chain,
            pem: "ACME CHAIN PEM".to_string(),
        },
        CertificateFile {
            path: acme_key,
            pem: "ACME KEY PEM".to_string(),
        },
        CertificateFile {
            path: "certs/relay/pod_a/full_chain.pem".to_string(),
            pem: leaf.certificate_pem.clone(),
        },
        CertificateFile {
            path: "certs/relay/pod_a/key.pem".to_string(),
            pem: w.secrets.decrypt_str(&leaf.private_key_pem)?,
        },
        CertificateFile {
            path: CA_FILE.to_string(),
            pem: ca.certificate_pem.clone(),
        },
    ];
    assert_eq!(files, expected);
    assert!(files[3].pem.contains("PRIVATE KEY"));

    let without_ca =
        w.ca.process(BundleCertificates {
            refs: &refs[..1],
            ca: false,
        })
        .await?;
    assert_eq!(without_ca, expected[..2]);
    Ok(())
}

// --- derivation ------------------------------------------------------------

struct Fixture {
    canvas: CanvasId,
    tokyo: ServerId,
    osaka: ServerId,
    ingress: NodeWithPorts,
    osaka_hop: NodeWithPorts,
    entry: NodeWithPorts,
}

/// tokyo (entry -> ingress -> relay) -> osaka (osaka-hop -> exit).
async fn relay_chain(
    w: &World,
    relay: RelayProtocol,
    tls: Option<TlsConfig>,
) -> Result<Fixture, Box<dyn std::error::Error>> {
    let canvas = w
        .canvases
        .process(canvas_service::CreateCanvas {
            actor: operator(),
            name: "prod".to_string(),
            description: String::new(),
        })
        .await?;
    let mut servers = Vec::new();
    for (name, ip) in [("tokyo", "203.0.113.10"), ("osaka", "198.51.100.10")] {
        let server = w
            .servers
            .process(CreateServer {
                actor: operator(),
                canvas: canvas.id.clone(),
                name: name.to_string(),
                icon: String::new(),
                comment: String::new(),
                position: pos0(),
                ipv6_resolve: ServerIpv6Resolve::Tolerated,
                log_level: "info".to_string(),
                addresses: AddressOverrides {
                    override_v4: Some(ip.to_string()),
                    override_v6: None,
                    extra_addresses: Vec::new(),
                },
            })
            .await?;
        servers.push((server.id.clone(), server.id));
    }
    let (tokyo, tokyo_ip) = servers[0].clone();
    let (osaka, osaka_ip) = servers[1].clone();

    let create = async |name: &str, spec: NodeSpec| {
        w.nodes
            .process(CreateNode {
                actor: operator(),
                canvas: canvas.id.clone(),
                name: name.to_string(),
                comment: String::new(),
                spec,
                position: pos0(),
                item_count: 0,
            })
            .await
    };
    let ingress = create(
        "ingress",
        NodeSpec::Pod(PodConfig {
            server: tokyo_ip,
            port: 443,
            bind_ip: None,
            advertise_ip: None,
        }),
    )
    .await?;
    let entry = create(
        "entry",
        NodeSpec::Entry(EntryConfig {
            receive_proxy_protocol: None,
            tls,
        }),
    )
    .await?;
    let to_osaka = create(
        "to-osaka",
        NodeSpec::Relay(RelayConfig {
            protocol: relay,
            override_ip_address: None,
            override_port: None,
        }),
    )
    .await?;
    let osaka_hop = create(
        "osaka-hop",
        NodeSpec::Pod(PodConfig {
            server: osaka_ip,
            port: 9443,
            bind_ip: None,
            advertise_ip: None,
        }),
    )
    .await?;
    let exit = create(
        "exit",
        NodeSpec::Exit(ExitConfig {
            destination: "10.0.0.5:8080".to_string(),
            pass_proxy_protocol: None,
        }),
    )
    .await?;
    let connect = async |output, input| {
        w.edges
            .process(Connect {
                actor: operator(),
                output_port: output,
                input_port: input,
            })
            .await
    };
    connect(port_of(&ingress, "listen"), port_of(&entry, "listen")).await?;
    connect(
        port_of(&to_osaka, "destination"),
        port_of(&ingress, "destination"),
    )
    .await?;
    connect(port_of(&osaka_hop, "listen"), port_of(&to_osaka, "listen")).await?;
    connect(
        port_of(&exit, "destination"),
        port_of(&osaka_hop, "destination"),
    )
    .await?;
    Ok(Fixture {
        canvas: canvas.id,
        tokyo,
        osaka,
        ingress,
        osaka_hop,
        entry,
    })
}

async fn leaf_of(w: &World, pod: &NodeId) -> RelayCertificateEntity {
    w.db.process(ListRelayCertificatesByPods {
        pods: vec![pod.clone()],
    })
    .await
    .unwrap()
    .remove(0)
}

/// Takes and acknowledges whatever the database offers this server, the way a
/// worker would.
async fn ack_current(w: &World, server: &ServerId) -> Result<(), Box<dyn std::error::Error>> {
    let registered =
        w.db.process(FindServerById { id: server.clone() })
            .await?
            .is_some_and(|row| row.refresh_key_generation > 0);
    if !registered {
        w.agents
            .process(RegisterWorker {
                actor: machine(),
                server_id: server.clone(),
                running_revision: 0,
                observed: None,
                reported: None,
            })
            .await?;
    }
    let row =
        w.db.process(FindServerById { id: server.clone() })
            .await?
            .unwrap();
    let Some(snapshot) =
        w.db.process(TakeInFlight {
            server: server.clone(),
            generation: row.refresh_key_generation,
            epoch: row.watch_epoch,
        })
        .await?
    else {
        return Ok(());
    };
    w.db.process(AckServerConfig {
        server: server.clone(),
        canvas: row.canvas,
        revision: snapshot.revision,
        error: None,
        applied: None,
        failed_pods: Vec::new(),
    })
    .await?;
    Ok(())
}

#[tokio::test]
async fn a_relay_tls_canvas_derives_once_the_ca_exists_and_follows_leaf_versions() -> TestResult {
    let w = world().await?;
    let f = relay_chain(&w, RelayProtocol::TcpTls, None).await?;

    // Without a CA both ends are invalid and nothing is published.
    w.derive(&f.canvas).await?;
    for server in [&f.tokyo, &f.osaka] {
        let view = w.view(server).await?;
        // Only invalid pods: the server publishes an empty config.
        let desired = view.desired.as_ref().expect("an empty revision");
        assert!(desired.forwardings.is_empty(), "{view:?}");
        assert_eq!(desired.revision, 1);
        assert_eq!(view.invalid_pods.len(), 1, "{view:?}");
        assert!(
            view.invalid_pods[0]
                .error
                .contains("internal CA not initialised"),
            "{}",
            view.invalid_pods[0].error
        );
    }

    // Initialising the CA touches the canvas; the next pass issues the leaf
    // and publishes both servers.
    let ca = w.ca.process(InitInternalCa).await?;
    assert_eq!(
        ca.touched_canvases
            .iter()
            .map(|c| record_key(&c.0))
            .collect::<Vec<_>>(),
        [record_key(&f.canvas.0)]
    );
    w.derive(&f.canvas).await?;
    let leaf = leaf_of(&w, &f.osaka_hop.node.id).await;
    assert_eq!(leaf.sni, relay_sni(&f.osaka_hop.node.id));
    assert_leaf_signed_by(&leaf.certificate_pem, &ca.certificate_pem, &leaf.sni);
    let relay_ref = CertificateRef {
        kind: CertificateKind::Relay,
        key: record_key(&leaf.id.0),
        version: 1,
    };

    let osaka = w.view(&f.osaka).await?;
    let desired = osaka.desired.as_ref().expect("osaka publishes");
    assert_eq!(desired.revision, 2);
    assert_eq!(desired.certificates, vec![relay_ref.clone()]);
    assert_eq!(desired.forwardings[0].certificates, vec![relay_ref.clone()]);
    let osaka_key = record_key(&f.osaka_hop.node.id.0);
    assert!(
        desired
            .toml
            .contains(&format!("key = \"certs/relay/{osaka_key}/key.pem\""))
            && desired.toml.contains("relay_type = \"tls\"")
            && desired.toml.contains("relay_ca = \"certs/ca.pem\""),
        "{}",
        desired.toml
    );
    // Tokyo waits until osaka actually serves the secure listener (convergence
    // rule 1), then dials it by SNI on the next pass.
    let tokyo = w.view(&f.tokyo).await?;
    assert!(tokyo.invalid_pods.is_empty(), "{:?}", tokyo.invalid_pods);
    assert_eq!(
        tokyo
            .waiting_for
            .iter()
            .map(|s| record_key(&s.0))
            .collect::<Vec<_>>(),
        [record_key(&f.osaka.0)]
    );
    ack_current(&w, &f.osaka).await?;
    w.derive(&f.canvas).await?;
    let tokyo = w.view(&f.tokyo).await?;
    let desired = tokyo.desired.as_ref().expect("tokyo publishes");
    // 1: empty, 2: empty but with `relay_ca` once the CA exists, 3: the dialer.
    assert_eq!(desired.revision, 3);
    assert!(desired.certificates.is_empty());
    assert!(
        desired.toml.contains("protocol = \"tls\"")
            && desired
                .toml
                .contains(&format!("sni = \"{}\"", relay_sni(&f.osaka_hop.node.id)))
            && desired.toml.contains("relay_ca = \"certs/ca.pem\""),
        "{}",
        desired.toml
    );
    assert!(osaka.invalid_pods.is_empty() && tokyo.invalid_pods.is_empty());

    // A pass with nothing changed publishes nothing new.
    w.db.process(
        orchestration::entities::surreal::certificate::TouchCanvases {
            canvases: vec![f.canvas.clone()],
        },
    )
    .await?;
    w.derive(&f.canvas).await?;
    assert_eq!(w.view(&f.osaka).await?.desired.unwrap().revision, 2);

    // Rotation: the leaf expires within the renewal window, the cron re-issues
    // it and the same TOML ships as a new revision pinning the new version.
    w.db.process(StoreRelayCertificate {
        pod: f.osaka_hop.node.id.clone(),
        sni: leaf.sni.clone(),
        private_key_pem: leaf.private_key_pem.clone(),
        certificate_pem: leaf.certificate_pem.clone(),
        not_before: leaf.not_before,
        not_after: Utc::now() + chrono::Duration::hours(1),
        expected_version: None,
    })
    .await?
    .expect("the unconditional store writes the row");
    rotate_expiring_relay_certificates(&w.deriver).await?;
    let rotated = leaf_of(&w, &f.osaka_hop.node.id).await;
    assert_eq!(rotated.version, 3);
    assert_ne!(rotated.certificate_pem, leaf.certificate_pem);
    let osaka = w.view(&f.osaka).await?;
    let desired = osaka.desired.as_ref().unwrap();
    assert_eq!(desired.revision, 3, "{desired:?}");
    assert_eq!(
        desired.certificates,
        vec![CertificateRef {
            version: 3,
            ..relay_ref
        }]
    );
    assert_eq!(
        w.view(&f.tokyo).await?.desired.unwrap().revision,
        3,
        "the dialer pins nothing, so it is not restarted"
    );

    // What the worker would be handed for osaka's revision.
    let files =
        w.ca.process(BundleCertificates {
            refs: &desired.certificates,
            ca: true,
        })
        .await?;
    let paths: Vec<&str> = files.iter().map(|f| f.path.as_str()).collect();
    assert_eq!(
        paths,
        [
            format!("certs/relay/{osaka_key}/full_chain.pem"),
            format!("certs/relay/{osaka_key}/key.pem"),
            CA_FILE.to_string(),
        ]
    );
    assert_eq!(files[0].pem, rotated.certificate_pem);
    assert_eq!(files[2].pem, ca.certificate_pem);
    Ok(())
}

#[tokio::test]
async fn publishing_a_tls_entry_marks_its_nodes_deploying() -> TestResult {
    let w = world().await?;
    let dns =
        w.db.process(CreateDnsProvider {
            name: "cf".to_string(),
            provider: DnsProvider::Cloudflare,
            account_id: String::new(),
            api_secret: w.secrets.encrypt_str("token")?,
            now: Utc::now(),
        })
        .await?;
    let f = relay_chain(
        &w,
        RelayProtocol::TcpRaw,
        Some(TlsConfig {
            sni: "example.com".to_string(),
            dns_provider: dns.id.clone(),
            domain_id: "zone".to_string(),
            acme_directory: String::new(),
        }),
    )
    .await?;

    // Pending certificate: the ingress pod is invalid, osaka is unaffected.
    let certificate =
        w.db.process(EnsureCertificate {
            sni: "example.com".to_string(),
            dns_provider: dns.id,
            domain_id: "zone".to_string(),
            acme_directory: w.config.default_acme_directory.clone(),
            now: Utc::now(),
        })
        .await?;
    w.derive(&f.canvas).await?;
    let tokyo = w.view(&f.tokyo).await?;
    assert!(tokyo.desired.as_ref().unwrap().forwardings.is_empty());
    assert!(
        tokyo.invalid_pods[0]
            .error
            .contains("certificate for example.com is pending"),
        "{:?}",
        tokyo.invalid_pods
    );
    assert!(w.view(&f.osaka).await?.desired.is_some());
    // Osaka serves its listener, so tokyo's forwarding can be published once its
    // certificate arrives. That first publish already marked osaka's path
    // (osaka-hop and the relay) Deploying.
    ack_current(&w, &f.osaka).await?;
    let since = Utc::now();
    let history = async |node: &NodeId| {
        w.db.process(ListNodeHealthHistory {
            node: node.clone(),
            start: since - chrono::Duration::hours(1),
            end: since + chrono::Duration::hours(1),
            limit: 10,
        })
        .await
    };
    assert!(history(&f.ingress.node.id).await?.is_empty());

    // Issued: the pod publishes as TLS and every node on its path is Deploying.
    w.db.process(StoreIssuedCertificate {
        id: certificate.id.clone(),
        acme_account_key: w.secrets.encrypt_str("acct")?,
        private_key_pem: w.secrets.encrypt_str("ACME KEY PEM")?,
        full_chain_pem: "ACME CHAIN PEM".to_string(),
        not_before: Utc::now(),
        not_after: Utc::now() + chrono::Duration::days(60),
        now: Utc::now(),
    })
    .await?;
    w.db.process(
        orchestration::entities::surreal::certificate::TouchCanvases {
            canvases: vec![f.canvas.clone()],
        },
    )
    .await?;
    w.derive(&f.canvas).await?;
    let tokyo = w.view(&f.tokyo).await?;
    let desired = tokyo.desired.as_ref().expect("tokyo publishes");
    let cert_key = record_key(&certificate.id.0);
    assert!(
        desired.toml.contains(&format!(
            "full_chain = \"certs/acme/{cert_key}/full_chain.pem\""
        )),
        "{}",
        desired.toml
    );
    assert_eq!(
        desired.certificates,
        vec![CertificateRef {
            kind: CertificateKind::Acme,
            key: cert_key,
            version: 1,
        }]
    );
    for node in [&f.ingress.node.id, &f.entry.node.id] {
        let records = history(node).await?;
        assert_eq!(records.len(), 1, "{records:?}");
        assert_eq!(records[0].status, NodeHealthStatus::Deploying);
    }
    // The relay is on the ingress path too (destination side) and on osaka's
    // (listen side): one record from each server's publish.
    let relay_records = history(
        &desired.forwardings[0]
            .nodes
            .iter()
            .find(|n| record_key(&n.0) != record_key(&f.entry.node.id.0))
            .cloned()
            .expect("the relay node"),
    )
    .await?;
    assert_eq!(relay_records.len(), 2, "{relay_records:?}");
    assert!(
        relay_records
            .iter()
            .all(|r| r.status == NodeHealthStatus::Deploying)
    );

    // A pass that changes nothing adds no records.
    w.db.process(
        orchestration::entities::surreal::certificate::TouchCanvases {
            canvases: vec![f.canvas.clone()],
        },
    )
    .await?;
    w.derive(&f.canvas).await?;
    assert_eq!(history(&f.ingress.node.id).await?.len(), 1);
    Ok(())
}

/// Two consumers handed the same rotation signal, or one handed a duplicate: the
/// pass selects the expiring leaves without locking them, so only the version
/// fence on the write keeps a leaf from being re-issued twice and shipping two
/// revisions for the same rotation.
///
/// This is integration coverage of the cron path: depending on which future the
/// executor polls first, either both passes select the leaf and the fence
/// refuses one write, or the second pass runs after the first and finds nothing
/// expiring left to select. Both orders end with one rotation, which is what is
/// asserted here; `two_stores_fenced_on_the_same_version_write_once` pins the
/// fence itself with no scheduling assumption at all.
#[tokio::test]
async fn two_overlapping_rotation_passes_rotate_a_leaf_once() -> TestResult {
    let w = world().await?;
    w.ca.process(InitInternalCa).await?;
    let pod = orchestration::utils::ids::node_id("pod_a");
    let leaf =
        w.ca.process(EnsureRelayCertificates {
            pods: vec![pod.clone()],
        })
        .await?
        .remove(0);
    // Backdate it into the renewal window so both passes select it.
    w.db.process(StoreRelayCertificate {
        pod: pod.clone(),
        sni: leaf.sni.clone(),
        private_key_pem: leaf.private_key_pem.clone(),
        certificate_pem: leaf.certificate_pem.clone(),
        not_before: leaf.not_before,
        not_after: Utc::now() + chrono::Duration::hours(1),
        expected_version: None,
    })
    .await?
    .expect("the unconditional store writes the row");
    let before = leaf_of(&w, &pod).await;
    assert_eq!(before.version, 2);

    let (first, second) = tokio::join!(
        rotate_expiring_relay_certificates(&w.deriver),
        rotate_expiring_relay_certificates(&w.deriver)
    );
    first?;
    second?;

    let after = leaf_of(&w, &pod).await;
    assert_eq!(
        after.version, 3,
        "one rotation across both passes, not one each"
    );
    assert_ne!(after.certificate_pem, before.certificate_pem);
    assert!(after.not_after > Utc::now() + chrono::Duration::days(1));
    Ok(())
}

/// The fence itself, with no scheduling assumption: both writes carry the same
/// `expected_version`, so only one of them can land. That is what keeps a
/// renewal or a rotation from overwriting material another writer just stored,
/// and it holds whichever future the executor polls first.
///
/// This is the entity level, where a lost race can also arrive as the engine
/// aborting the losing transaction rather than matching nothing — SurrealDB does
/// not serialise two transactions writing one row. Either way exactly one write
/// lands, and the version is the witness. (The service turns that abort back
/// into a refusal, which is what `two_overlapping_ensures_replace_an_expiring_leaf_once`
/// pins.)
#[tokio::test]
async fn two_stores_fenced_on_the_same_version_write_once() -> TestResult {
    let w = world().await?;
    w.ca.process(InitInternalCa).await?;
    let pod = orchestration::utils::ids::node_id("pod_a");
    let leaf =
        w.ca.process(EnsureRelayCertificates {
            pods: vec![pod.clone()],
        })
        .await?
        .remove(0);
    assert_eq!(leaf.version, 1);

    // Same input but for the material, so the stored row names its winner.
    let store = |certificate_pem: &str| StoreRelayCertificate {
        pod: pod.clone(),
        sni: leaf.sni.clone(),
        private_key_pem: leaf.private_key_pem.clone(),
        certificate_pem: certificate_pem.to_string(),
        not_before: leaf.not_before,
        not_after: leaf.not_after,
        expected_version: Some(leaf.version),
    };
    let (first, second) = tokio::join!(w.db.process(store("first")), w.db.process(store("second")));
    let landed: Vec<RelayCertificateEntity> = [first, second]
        .into_iter()
        // A refused fence is `Ok(None)`; an aborted losing transaction is an
        // `Err`. Both mean "did not land", and neither may be two.
        .filter_map(|outcome| outcome.ok().flatten())
        .collect();
    assert_eq!(
        landed.len(),
        1,
        "exactly one of the two compare-and-sets lands: {landed:?}"
    );

    let stored = leaf_of(&w, &pod).await;
    assert_eq!(
        stored.version,
        leaf.version + 1,
        "the version moved once, not once per writer"
    );
    assert_eq!(
        stored.certificate_pem, landed[0].certificate_pem,
        "the row holds the material of the writer that won"
    );
    assert!(matches!(
        stored.certificate_pem.as_str(),
        "first" | "second"
    ));
    Ok(())
}

/// Two derivation passes over the same pod overlap (two canvases derived at
/// once, or one derivation racing the rotation cron) and both see a leaf inside
/// the renewal window. The ensure path fences its replacement on the version it
/// read, so the pod ends with exactly one new leaf however the race falls.
///
/// Both passes must succeed. SurrealDB aborts the losing transaction instead of
/// letting its `WHERE` match nothing, so the service translates that abort back
/// into a lost race by re-reading the row: an overlap it is built to absorb must
/// not surface as a failed derivation pass.
#[tokio::test]
async fn two_overlapping_ensures_replace_an_expiring_leaf_once() -> TestResult {
    let w = world().await?;
    let ca = w.ca.process(InitInternalCa).await?;
    let pod = orchestration::utils::ids::node_id("pod_a");
    let leaf =
        w.ca.process(EnsureRelayCertificates {
            pods: vec![pod.clone()],
        })
        .await?
        .remove(0);
    // Backdate it into the renewal window so both passes want to replace it.
    w.db.process(StoreRelayCertificate {
        pod: pod.clone(),
        sni: leaf.sni.clone(),
        private_key_pem: leaf.private_key_pem.clone(),
        certificate_pem: leaf.certificate_pem.clone(),
        not_before: leaf.not_before,
        not_after: Utc::now() + chrono::Duration::hours(1),
        expected_version: None,
    })
    .await?
    .expect("the unconditional store writes the row");
    let before = leaf_of(&w, &pod).await;
    assert_eq!(before.version, 2);

    let (first, second) = tokio::join!(
        w.ca.process(EnsureRelayCertificates {
            pods: vec![pod.clone()]
        }),
        w.ca.process(EnsureRelayCertificates {
            pods: vec![pod.clone()]
        })
    );
    let outcomes = [("first", first?), ("second", second?)];

    let stored = leaf_of(&w, &pod).await;
    assert_eq!(
        stored.version, 3,
        "one replacement across both passes, not one each"
    );
    assert_ne!(stored.certificate_pem, before.certificate_pem);
    assert_leaf_signed_by(&stored.certificate_pem, &ca.certificate_pem, &leaf.sni);
    for (label, leaves) in outcomes {
        let [returned] = leaves.as_slice() else {
            panic!("one leaf per pod from the {label} pass: {leaves:?}");
        };
        assert_eq!(
            returned.certificate_pem, stored.certificate_pem,
            "the {label} pass returned the leaf that is stored, not one it lost"
        );
        assert_eq!(returned.version, stored.version);
    }
    Ok(())
}
