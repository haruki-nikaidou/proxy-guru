//! The internal CA: relay leaf issuance and certificate delivery.
//!
//! Relay TLS/QUIC links between workers are authenticated by a private CA the
//! master owns. `manage-tool orchestration init-ca` creates it once
//! ([`InitInternalCa`]); the derivation hook issues one leaf per relay pod
//! ([`EnsureRelayCertificates`]) and the rotation cron re-issues leaves that are
//! about to expire. Every private key is stored encrypted with the master key and
//! only decrypted here, at the moment it is handed to a worker
//! ([`BundleCertificates`]).
//!
//! The CA row stores only PEMs; the issuer rcgen needs to sign a leaf is rebuilt
//! from the same parameters the CA was generated with ([`ca_params`]), which is
//! what keeps the leaf's authority key identifier equal to the CA's subject key
//! identifier without parsing the CA certificate.

use crate::config::OrchestrationConfig;
use crate::entities::surreal::ca::{
    CreateInternalCa, FindInternalCa, InternalCaEntity, ListRelayCertificatesByIds,
    ListRelayCertificatesByPods, RelayCertificateEntity, StoreRelayCertificate, relay_sni,
};
use crate::entities::surreal::canvas::CanvasId;
use crate::entities::surreal::certificate::{ListCertificatesByIds, TouchCanvases};
use crate::entities::surreal::node::{ListCanvasesWithRelayTls, NodeId};
use crate::entities::surreal::view::{CertificateKind, CertificateRef};
use crate::services::OrchestrationError;
use crate::utils::ids::{self, record_key};
use crate::utils::secret::SecretKey;
use base::db::Db;
use chrono::{DateTime, Duration as ChronoDuration, Utc};
use kanau::processor::Processor;
use rcgen::{
    BasicConstraints, CertificateParams, DnType, ExtendedKeyUsagePurpose, IsCa, Issuer, KeyPair,
    KeyUsagePurpose, SanType,
};
use std::collections::HashMap;
use time::OffsetDateTime;

pub const CA_COMMON_NAME: &str = "guru internal relay CA";
const CA_VALID_DAYS: i64 = 3650;
/// Leaves are backdated by this much so a worker whose clock lags the master
/// still accepts a freshly issued certificate.
const NOT_BEFORE_SKEW_SECS: i64 = 300;

/// Where the internal CA certificate lands on every worker, relative to its
/// state dir; the TOML's `relay_ca`.
pub const CA_FILE: &str = "certs/ca.pem";

/// `certs/acme/<certificate-record-key>/{full_chain,key}.pem`.
pub fn acme_cert_paths(key: &str) -> (String, String) {
    (
        format!("certs/acme/{key}/full_chain.pem"),
        format!("certs/acme/{key}/key.pem"),
    )
}

/// `certs/relay/<pod-record-key>/{full_chain,key}.pem`.
pub fn relay_cert_paths(pod_key: &str) -> (String, String) {
    (
        format!("certs/relay/{pod_key}/full_chain.pem"),
        format!("certs/relay/{pod_key}/key.pem"),
    )
}

/// One file of a config revision, as delivered to the worker.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CertificateFile {
    pub path: String,
    pub pem: String,
}

#[derive(Clone)]
pub struct CaService {
    pub db: Db,
    pub secrets: SecretKey,
    pub config: OrchestrationConfig,
}

fn certificate_error(error: impl std::fmt::Display) -> OrchestrationError {
    OrchestrationError::Certificate(error.to_string())
}

fn to_offset(at: DateTime<Utc>) -> Result<OffsetDateTime, OrchestrationError> {
    OffsetDateTime::from_unix_timestamp(at.timestamp()).map_err(certificate_error)
}

/// The parameters the CA certificate was (and the issuer is) built from.
fn ca_params() -> CertificateParams {
    let mut params = CertificateParams::default();
    params.distinguished_name = rcgen::DistinguishedName::new();
    params
        .distinguished_name
        .push(DnType::CommonName, CA_COMMON_NAME);
    params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    params.key_usages = vec![
        KeyUsagePurpose::KeyCertSign,
        KeyUsagePurpose::CrlSign,
        KeyUsagePurpose::DigitalSignature,
    ];
    params
}

/// Generates the CA and stores it. Fails with `Conflict` when one exists:
/// replacing the root would invalidate every relay leaf at once.
///
/// Every tree holding a TLS/QUIC relay is then touched: their pods were reported
/// invalid for lack of a CA, and the re-derivation is what issues their first
/// leaves (the rotation cron only renews existing ones).
pub struct InitInternalCa;

#[derive(Debug, Clone)]
pub struct InitializedCa {
    /// The CA certificate PEM, for the operator to keep.
    pub certificate_pem: String,
    /// Root canvases marked for re-derivation.
    pub touched_canvases: Vec<CanvasId>,
}

impl Processor<InitInternalCa> for CaService {
    type Output = InitializedCa;
    type Error = OrchestrationError;
    #[tracing::instrument(name = "Service:InitInternalCa", skip_all, err)]
    async fn process(&self, _: InitInternalCa) -> Result<Self::Output, Self::Error> {
        let now = Utc::now();
        let not_after = now
            .checked_add_signed(ChronoDuration::days(CA_VALID_DAYS))
            .unwrap_or(DateTime::<Utc>::MAX_UTC);
        let key = KeyPair::generate().map_err(certificate_error)?;
        let mut params = ca_params();
        params.not_before = to_offset(
            now.checked_sub_signed(ChronoDuration::seconds(NOT_BEFORE_SKEW_SECS))
                .unwrap_or(DateTime::<Utc>::MIN_UTC),
        )?;
        params.not_after = to_offset(not_after)?;
        let certificate = params.self_signed(&key).map_err(certificate_error)?;
        let certificate_pem = certificate.pem();
        let created = self
            .db
            .process(CreateInternalCa {
                certificate_pem: certificate_pem.clone(),
                private_key_pem: self.secrets.encrypt_str(&key.serialize_pem())?,
                not_after,
                now,
            })
            .await?;
        if !created {
            return Err(OrchestrationError::Conflict(
                "the internal CA is already initialised".into(),
            ));
        }
        let touched_canvases = self.db.process(ListCanvasesWithRelayTls).await?;
        self.db
            .process(TouchCanvases {
                canvases: touched_canvases.clone(),
            })
            .await?;
        Ok(InitializedCa {
            certificate_pem,
            touched_canvases,
        })
    }
}

/// Makes sure every given pod has a relay leaf that is not about to expire,
/// issuing or rotating as needed. Returns the current leaf of every pod, in the
/// order the pods were given.
///
/// Every replacement is fenced on the version this pass read, because the
/// rotation cron replaces the same leaves and two derivations of the same
/// canvas can overlap: the pass that loses adopts the winner's freshly issued
/// row rather than issuing a leaf of its own. Only the first issuance of a
/// pod's leaf is unfenced, and two of those collide on the
/// `relay_certificate_pod` UNIQUE index instead.
pub struct EnsureRelayCertificates {
    pub pods: Vec<NodeId>,
}

impl Processor<EnsureRelayCertificates> for CaService {
    type Output = Vec<RelayCertificateEntity>;
    type Error = OrchestrationError;
    #[tracing::instrument(name = "Service:EnsureRelayCertificates", skip_all, err)]
    async fn process(&self, input: EnsureRelayCertificates) -> Result<Self::Output, Self::Error> {
        if input.pods.is_empty() {
            return Ok(Vec::new());
        }
        let ca = self.db.process(FindInternalCa).await?.ok_or_else(|| {
            OrchestrationError::Conflict(
                "the internal CA is not initialised (run `manage-tool orchestration init-ca`)"
                    .into(),
            )
        })?;
        let existing = self
            .db
            .process(ListRelayCertificatesByPods {
                pods: input.pods.clone(),
            })
            .await?;
        let mut by_pod: HashMap<String, RelayCertificateEntity> = existing
            .into_iter()
            .map(|leaf| (record_key(&leaf.pod.0), leaf))
            .collect();

        let now = Utc::now();
        let renew_at = now
            .checked_add_signed(
                ChronoDuration::from_std(self.config.relay_cert_renew_before())
                    .map_err(certificate_error)?,
            )
            .unwrap_or(DateTime::<Utc>::MAX_UTC);
        let mut issuer = None;
        let mut leaves = Vec::with_capacity(input.pods.len());
        for pod in input.pods {
            let sni = relay_sni(&pod);
            let pod_key = record_key(&pod.0);
            let expected_version = match by_pod.remove(&pod_key) {
                // Still the right SNI and outside the renewal window: untouched.
                Some(leaf) if leaf.sni == sni && leaf.not_after > renew_at => {
                    leaves.push(leaf);
                    continue;
                }
                // A leaf that must be replaced. Another derivation pass or the
                // rotation cron may be replacing the same one right now, so the
                // write is fenced on the version this pass read: without the
                // fence both would issue a leaf and the later write would
                // overwrite the other's material while bumping `version` again.
                Some(leaf) => Some(leaf.version),
                // No leaf at all: a `CREATE`, which has no prior version to
                // fence against. Two concurrent creates for the same pod
                // collide on the `relay_certificate_pod` UNIQUE index and one
                // transaction fails; that is pre-existing behaviour and reaches
                // the caller as a database error it already logs and retries,
                // so it is deliberately not swallowed here.
                None => None,
            };
            let signer = match &issuer {
                Some(signer) => signer,
                None => issuer.insert(self.issuer(&ca)?),
            };
            let issued = self
                .issue_leaf(signer, &pod, sni, now, expected_version)
                .await?;
            let leaf = match issued {
                Some(leaf) => leaf,
                // The fence refused the write: the other writer replaced this
                // leaf between our read and our write. Its row is therefore a
                // freshly issued leaf from the same CA, so this pass adopts it
                // instead of issuing a second one.
                None if expected_version.is_some() => {
                    tracing::debug!(
                        pod = %pod_key,
                        "another writer replaced this relay leaf first; using its row"
                    );
                    self.db
                        .process(ListRelayCertificatesByPods { pods: vec![pod] })
                        .await?
                        .pop()
                        .ok_or_else(|| {
                            OrchestrationError::Conflict(format!(
                                "the relay certificate of pod {pod_key} was deleted while it was being replaced"
                            ))
                        })?
                }
                // Unfenced, so the row is always written: a missing one is a bug.
                None => {
                    return Err(OrchestrationError::Db(surrealdb::Error::internal(
                        "relay certificate row was not written".to_string(),
                    )));
                }
            };
            leaves.push(leaf);
        }
        Ok(leaves)
    }
}

/// Re-issues one leaf regardless of its expiry: the rotation cron's step.
///
/// `expected_version` is the version the caller read. Rotation runs from an
/// AMQP signal that may be delivered more than once, so the re-issue is fenced
/// on it: `None` means another consumer rotated this leaf first and this pass
/// must leave it alone.
pub struct RotateRelayCertificate {
    pub pod: NodeId,
    pub expected_version: i64,
}

impl Processor<RotateRelayCertificate> for CaService {
    type Output = Option<RelayCertificateEntity>;
    type Error = OrchestrationError;
    #[tracing::instrument(name = "Service:RotateRelayCertificate", skip_all, err)]
    async fn process(&self, input: RotateRelayCertificate) -> Result<Self::Output, Self::Error> {
        let ca = self.db.process(FindInternalCa).await?.ok_or_else(|| {
            OrchestrationError::Conflict("the internal CA is not initialised".into())
        })?;
        let issuer = self.issuer(&ca)?;
        let sni = relay_sni(&input.pod);
        self.issue_leaf(
            &issuer,
            &input.pod,
            sni,
            Utc::now(),
            Some(input.expected_version),
        )
        .await
    }
}

impl CaService {
    fn issuer(
        &self,
        ca: &InternalCaEntity,
    ) -> Result<Issuer<'static, KeyPair>, OrchestrationError> {
        let key_pem = self.secrets.decrypt_str(&ca.private_key_pem)?;
        let key = KeyPair::from_pem(&key_pem).map_err(certificate_error)?;
        Ok(Issuer::new(ca_params(), key))
    }

    /// Signs a fresh leaf for `pod` and stores it. `expected_version` fences
    /// the write on the version the caller read, which every path that replaces
    /// an existing leaf passes; `None` output is that lost race, never a
    /// failure. Only the create path (no leaf yet) may pass `None`, where the
    /// write is unconditional.
    async fn issue_leaf(
        &self,
        issuer: &Issuer<'_, KeyPair>,
        pod: &NodeId,
        sni: String,
        now: DateTime<Utc>,
        expected_version: Option<i64>,
    ) -> Result<Option<RelayCertificateEntity>, OrchestrationError> {
        let not_before = now
            .checked_sub_signed(ChronoDuration::seconds(NOT_BEFORE_SKEW_SECS))
            .unwrap_or(DateTime::<Utc>::MIN_UTC);
        let not_after = now
            .checked_add_signed(
                ChronoDuration::from_std(self.config.relay_cert_valid())
                    .map_err(certificate_error)?,
            )
            .unwrap_or(DateTime::<Utc>::MAX_UTC);
        let key = KeyPair::generate().map_err(certificate_error)?;
        let mut params = CertificateParams::default();
        params.distinguished_name = rcgen::DistinguishedName::new();
        params
            .distinguished_name
            .push(DnType::CommonName, sni.as_str());
        params.subject_alt_names = vec![SanType::DnsName(
            sni.as_str().try_into().map_err(certificate_error)?,
        )];
        params.is_ca = IsCa::ExplicitNoCa;
        params.key_usages = vec![KeyUsagePurpose::DigitalSignature];
        params.extended_key_usages = vec![ExtendedKeyUsagePurpose::ServerAuth];
        params.use_authority_key_identifier_extension = true;
        params.not_before = to_offset(not_before)?;
        params.not_after = to_offset(not_after)?;
        let certificate = params.signed_by(&key, issuer).map_err(certificate_error)?;
        let stored = self
            .db
            .process(StoreRelayCertificate {
                pod: pod.clone(),
                sni,
                private_key_pem: self.secrets.encrypt_str(&key.serialize_pem())?,
                certificate_pem: certificate.pem(),
                not_before,
                not_after,
                expected_version,
            })
            .await;
        match (stored, expected_version) {
            (Ok(row), _) => Ok(row),
            // A fenced write whose transaction the engine aborted. SurrealDB does
            // not serialise two transactions writing one row — it fails the
            // second instead of letting its `WHERE` match nothing — so this is
            // the same lost race as an empty result, and reporting it as an error
            // would make every overlapping derivation fail a pass it is supposed
            // to absorb. Only the row itself can tell the two apart: past the
            // version we fenced on means somebody else wrote it.
            (Err(error), Some(expected)) => {
                let current = self
                    .db
                    .process(ListRelayCertificatesByPods {
                        pods: vec![pod.clone()],
                    })
                    .await?
                    .pop();
                match current {
                    Some(row) if row.version > expected => Ok(None),
                    _ => Err(error.into()),
                }
            }
            (Err(error), None) => Err(error.into()),
        }
    }
}

/// The files a config revision needs, with keys decrypted: what `try_send`
/// attaches to a `ConfigRevision`. The material is the rows' *current* content,
/// never older than the versions the refs pin.
pub struct BundleCertificates<'a> {
    pub refs: &'a [CertificateRef],
    /// Include `certs/ca.pem`; set whenever the TOML's `relay_ca` is.
    pub ca: bool,
}

impl Processor<BundleCertificates<'_>> for CaService {
    type Output = Vec<CertificateFile>;
    type Error = OrchestrationError;
    #[tracing::instrument(name = "Service:BundleCertificates", skip_all, err)]
    async fn process(&self, input: BundleCertificates<'_>) -> Result<Self::Output, Self::Error> {
        let mut acme_ids = Vec::new();
        let mut relay_ids = Vec::new();
        for reference in input.refs {
            match reference.kind {
                CertificateKind::Acme => acme_ids.push(ids::certificate_id(&reference.key)),
                CertificateKind::Relay => relay_ids.push(ids::relay_certificate_id(&reference.key)),
            }
        }
        let mut files = Vec::with_capacity(
            input
                .refs
                .len()
                .saturating_mul(2)
                .saturating_add(usize::from(input.ca)),
        );
        for certificate in self
            .db
            .process(ListCertificatesByIds { ids: acme_ids })
            .await?
        {
            let (Some(key), Some(full_chain)) =
                (&certificate.private_key_pem, &certificate.full_chain_pem)
            else {
                return Err(OrchestrationError::Certificate(format!(
                    "certificate {} for {} is referenced but not issued",
                    record_key(&certificate.id.0),
                    certificate.sni
                )));
            };
            let (chain_path, key_path) = acme_cert_paths(&record_key(&certificate.id.0));
            files.push(CertificateFile {
                path: chain_path,
                pem: full_chain.clone(),
            });
            files.push(CertificateFile {
                path: key_path,
                pem: self.secrets.decrypt_str(key)?,
            });
        }
        for leaf in self
            .db
            .process(ListRelayCertificatesByIds { ids: relay_ids })
            .await?
        {
            let (chain_path, key_path) = relay_cert_paths(&record_key(&leaf.pod.0));
            files.push(CertificateFile {
                path: chain_path,
                pem: leaf.certificate_pem,
            });
            files.push(CertificateFile {
                path: key_path,
                pem: self.secrets.decrypt_str(&leaf.private_key_pem)?,
            });
        }
        if input.ca {
            let ca = self.db.process(FindInternalCa).await?.ok_or_else(|| {
                OrchestrationError::Certificate(
                    "the config references the internal CA, which is not initialised".into(),
                )
            })?;
            files.push(CertificateFile {
                path: CA_FILE.to_string(),
                pem: ca.certificate_pem,
            });
        }
        Ok(files)
    }
}
