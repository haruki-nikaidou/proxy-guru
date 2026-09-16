//! Config derivation: every server's ideal `guru-worker` config from the pod
//! graph of a canvas tree.
//!
//! The graph compiles in [`guru_topology`]; this module feeds it the rows and
//! the certificate state, and renders each server's share as a whole worker
//! config. Derivation is a pure function of its input, so the same graph always
//! produces byte-identical output — that is what lets a derivation pass skip
//! servers whose config did not actually change. What a server may *safely* run
//! right now is decided afterwards, in [`crate::services::converge`].
//!
//! Failure is per pod, not per server: a pod whose certificate is not issued or
//! whose target has no address is reported invalid and costs exactly its own
//! forwarding. A graph that does not check at all (which a checked write cannot
//! produce) fails every server of the tree.

use crate::config::OrchestrationConfig;
use crate::entities::db::ca::RelayCertificateEntity;
use crate::entities::db::certificate::{CertificateEntity, CertificateStatus};
use crate::entities::db::graph::GraphRows;
use crate::entities::db::pod::{PodEntity, PodId, PodIngress};
use crate::entities::db::server::{ServerEntity, ServerId};
use crate::entities::db::view::{
    CertificateKind, CertificateRef, ForwardingDeps, InvalidPod, ListenProtocol, ListenerCap,
};
use crate::services::ca::CA_FILE;
use crate::services::graph::{topology_certificates, topology_graph};
use guru_topology as topo;
use guru_worker_config::{Config, KeepAlive, LogConfig};
use std::collections::{BTreeMap, HashMap};
use std::path::PathBuf;

#[derive(Debug, thiserror::Error)]
pub enum DeriveError {
    #[error("the graph does not check: {0}")]
    Rejected(String),
    #[error("server {server} is not part of this canvas tree")]
    UnknownServer { server: String },
    #[error("derived config is invalid: {0}")]
    Invalid(#[from] guru_worker_config::ConfigError),
}

/// The certificate state a derivation pass runs against.
///
/// Loaded once per pass for the whole tree: every ACME row of every SNI a TLS
/// client pod asks for (all directories; the pair is resolved in the compile),
/// the relay leaf of every TLS or QUIC relay pod, and whether the internal CA
/// exists at all.
#[derive(Debug, Clone, Default)]
pub struct DerivationCertificates {
    pub acme: Vec<CertificateEntity>,
    pub relay: Vec<RelayCertificateEntity>,
    pub ca_present: bool,
    /// Projection mode for edit-time checks, where only the listener shapes
    /// matter and the TOML is discarded: a missing certificate or CA does not
    /// invalidate a pod, placeholder paths are emitted and nothing is pinned.
    /// Never used for a published revision.
    pub assume_issued: bool,
}

impl DerivationCertificates {
    /// See [`Self::assume_issued`].
    pub fn assumed() -> Self {
        Self {
            assume_issued: true,
            ..Self::default()
        }
    }

    fn topology(&self) -> topo::Certificates {
        let mut certificates = topology_certificates(&self.acme, &self.relay, self.ca_present);
        certificates.assume_issued = self.assume_issued;
        certificates
    }
}

/// A server's ideal config: what the graph says it should serve, ignoring what
/// the rest of the fabric is currently running.
#[derive(Debug, Clone)]
pub struct DerivedConfig {
    pub config: Config,
    /// Index-aligned with `config.forwardings`.
    pub forwardings: Vec<ForwardingDeps>,
    /// Pods that could not be compiled. The rest of `config` is unaffected.
    pub invalid: Vec<InvalidPod>,
    /// Every certificate `config` references, sorted and deduplicated: the union
    /// of the entries' [`ForwardingDeps::certificates`].
    pub certificates: Vec<CertificateRef>,
}

/// The sorted, deduplicated union of the entries' certificate refs.
pub fn certificate_union(forwardings: &[ForwardingDeps]) -> Vec<CertificateRef> {
    let mut refs: Vec<CertificateRef> = forwardings
        .iter()
        .flat_map(|deps| deps.certificates.iter().cloned())
        .collect();
    refs.sort();
    refs.dedup();
    refs
}

/// The pods that listen as TLS or QUIC relays: the ones that need a relay leaf.
/// Sorted by id.
pub fn relay_tls_pods(graph: &GraphRows) -> Vec<PodId> {
    let mut pods: Vec<PodId> = graph
        .pods
        .iter()
        .filter(|pod| pod.ingress.needs_relay_certificate())
        .map(|pod| pod.id.clone())
        .collect();
    pods.sort();
    pods
}

/// The SNIs of every TLS client pod in the tree, deduplicated.
pub fn tls_snis(graph: &GraphRows) -> Vec<String> {
    let mut snis: Vec<String> = graph
        .pods
        .iter()
        .filter_map(|pod| match &pod.ingress {
            PodIngress::ClientTls { tls, .. } => Some(tls.sni.clone()),
            _ => None,
        })
        .collect();
    snis.sort();
    snis.dedup();
    snis
}

/// Every server of the tree, compiled at once.
pub fn derive_tree(
    graph: &GraphRows,
    certificates: &DerivationCertificates,
    config: &OrchestrationConfig,
) -> Result<BTreeMap<ServerId, Result<DerivedConfig, DeriveError>>, DeriveError> {
    let topology = topology_graph(
        &graph.servers,
        &graph.pods,
        &graph.exits,
        &graph.edges,
        config,
    );
    let compiled = topo::compile(&topology, &certificates.topology()).map_err(|report| {
        DeriveError::Rejected(
            report
                .errors()
                .next()
                .map(|error| error.message.clone())
                .unwrap_or_default(),
        )
    })?;
    let pods: HashMap<&str, &PodEntity> =
        graph.pods.iter().map(|pod| (pod.id.as_str(), pod)).collect();
    let mut out = BTreeMap::new();
    for server in &graph.servers {
        let Some(share) = compiled
            .servers
            .get(&topo::ServerId::new(server.id.as_str()))
        else {
            continue;
        };
        out.insert(
            server.id.clone(),
            render_server(server, share, &pods, certificates),
        );
    }
    Ok(out)
}

/// One server's ideal config.
pub fn derive_server_config(
    graph: &GraphRows,
    server: &ServerId,
    certificates: &DerivationCertificates,
    config: &OrchestrationConfig,
) -> Result<DerivedConfig, DeriveError> {
    derive_tree(graph, certificates, config)?
        .remove(server)
        .unwrap_or_else(|| {
            Err(DeriveError::UnknownServer {
                server: server.to_string(),
            })
        })
}

fn render_server(
    server: &ServerEntity,
    share: &topo::ServerConfig,
    pods: &HashMap<&str, &PodEntity>,
    certificates: &DerivationCertificates,
) -> Result<DerivedConfig, DeriveError> {
    let forwardings: Vec<ForwardingDeps> = share.deps.iter().map(forwarding_deps).collect();
    let invalid = share
        .invalid
        .iter()
        .map(|invalid| {
            let pod = pods.get(invalid.pod.as_str());
            InvalidPod {
                pod: PodId::from_key(invalid.pod.as_str()),
                name: pod.map(|p| p.name.clone()).unwrap_or_default(),
                listen: pod.map(|p| p.listen_display()).unwrap_or_default(),
                error: invalid_message(invalid, certificates),
            }
        })
        .collect();
    let config = Config {
        ipv6_resolve: server.ipv6_resolve.into(),
        log: LogConfig {
            level: server.log_level.as_str().to_owned(),
        },
        relay_ca: certificates.ca_present.then(|| PathBuf::from(CA_FILE)),
        // The defaults, which the TOML then omits: a worker built before the
        // section existed rejects unknown keys.
        keepalive: KeepAlive::default(),
        // The server's own side of every QUIC link; a link whose peer asks for
        // less carries its own numbers on the forwarding.
        quic: server.quic.tuning(None),
        forwardings: share.forwardings.clone(),
    };
    config.validate()?;
    let certificates = certificate_union(&forwardings);
    Ok(DerivedConfig {
        config,
        forwardings,
        invalid,
        certificates,
    })
}

fn forwarding_deps(deps: &topo::Deps) -> ForwardingDeps {
    ForwardingDeps {
        pod: PodId::from_key(deps.pod.as_str()),
        serves: listener_cap(&deps.serves),
        points_at: deps.points_at.iter().map(listener_cap).collect(),
        certificates: deps
            .certificates
            .iter()
            .map(|r| CertificateRef {
                kind: match r.kind {
                    topo::CertificateKind::Acme => CertificateKind::Acme,
                    topo::CertificateKind::Relay => CertificateKind::Relay,
                },
                key: r.key.clone(),
                version: r.version,
            })
            .collect(),
    }
}

fn listener_cap(listener: &topo::Listener) -> ListenerCap {
    ListenerCap {
        server: ServerId::from_key(listener.server.as_str()),
        port: i64::from(listener.port),
        protocol: match listener.protocol {
            topo::ListenProtocol::Raw => ListenProtocol::Raw,
            topo::ListenProtocol::RelayTcp => ListenProtocol::RelayTcp,
            topo::ListenProtocol::RelayTls => ListenProtocol::RelayTls,
            topo::ListenProtocol::RelayQuic => ListenProtocol::RelayQuic,
        },
    }
}

/// Why a pod could not be compiled, in the words the operator acts on.
fn invalid_message(invalid: &topo::InvalidPod, certificates: &DerivationCertificates) -> String {
    match &invalid.reason {
        topo::Invalid::CertificatePending {
            sni,
            acme_directory,
        } => {
            let row = certificates
                .acme
                .iter()
                .find(|c| c.sni == *sni && c.acme_directory == *acme_directory);
            let state = match row {
                None => "pending (not requested yet)".to_string(),
                Some(c) => match c.status {
                    CertificateStatus::Failed => format!(
                        "failed: {}",
                        c.last_error.as_deref().unwrap_or("unknown error")
                    ),
                    CertificateStatus::Pending | CertificateStatus::Issued => {
                        "pending".to_string()
                    }
                },
            };
            format!("certificate for {sni} is {state}")
        }
        topo::Invalid::InternalCaMissing => {
            "internal CA not initialised (run `manage-tool orchestration init-ca`)".to_string()
        }
        topo::Invalid::RelayCertificateMissing => "relay certificate not issued yet".to_string(),
        topo::Invalid::TargetWithoutAddress { .. } | topo::Invalid::Rejected { .. } => {
            invalid.message.clone()
        }
    }
}
