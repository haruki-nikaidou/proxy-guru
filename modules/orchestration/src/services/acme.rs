//! ACME (DNS-01) issuance and renewal of the public certificates TLS client pods ask
//! for.
//!
//! The pipeline is [`IssueCertificate`]: load the `certificate` row, decrypt the
//! DNS provider token, run the ACME order through an [`AcmeIssuer`] that answers
//! the challenge with a [`DnsChallengeProvider`], store the material, and touch
//! every canvas serving the SNI so its servers get a revision pinning the new
//! version. Both the ACME client and the DNS providers sit behind traits: the
//! production pair is [`InstantAcmeIssuer`] (instant-acme, propagation checked
//! against public resolvers with hickory) and [`CloudflareDns`] / [`VercelDns`]
//! over reqwest; tests substitute fakes.
//!
//! A failed attempt is recorded on the row (`last_error`, `last_attempt_at`) and
//! retried by the cron after `acme_retry_after`, or at once through
//! [`RetryCertificate`]. The TXT record is removed on every path, success or
//! failure.

use crate::config::OrchestrationConfig;
use crate::entities::db::certificate::{
    CertificateEntity, CertificateId, CertificateStatus, DeleteCertificateRow, EnsureCertificate,
    FindCertificateById, ListCanvasesUsingSni, ListCertificates as ListCertificatesRow,
    ListTlsRequests, MarkCertificateAttemptFailed, RetryCertificateRow, StoreIssuedCertificate,
    TouchCanvases,
};
use crate::entities::db::dns::{DnsProvider, DnsProviderEntity, FindDnsProviderById};
use crate::events::live::LiveMessage;
use crate::services::OrchestrationError;
use crate::services::notify::Notifier;
use crate::utils::secret::{SecretError, SecretKey};
use auth::entities::db::account::AccountRole;
use auth::services::identity::Identity;
use auth::utils::rbac::Permission;
use base::db::Db;
use chrono::{DateTime, Utc};
use hickory_resolver::TokioResolver;
use hickory_resolver::config::{CLOUDFLARE, GOOGLE, ResolverConfig};
use hickory_resolver::net::runtime::TokioRuntimeProvider;
use hickory_resolver::proto::rr::RData;
use instant_acme::{
    Account, AccountCredentials, AuthorizationStatus, ChallengeType, Identifier, NewAccount,
    NewOrder, OrderStatus, RetryPolicy,
};
use kanau::processor::Processor;
use serde::Deserialize;
use std::collections::HashSet;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

pub const CLOUDFLARE_API: &str = "https://api.cloudflare.com/client/v4";
pub const VERCEL_API: &str = "https://api.vercel.com";
/// TTL of the challenge TXT record; the shortest the providers accept.
const TXT_TTL: u32 = 60;
const PROPAGATION_POLL: Duration = Duration::from_secs(5);
const PROPAGATION_TIMEOUT: Duration = Duration::from_secs(120);
/// How long to wait for the CA to validate the challenge and sign the order.
const ORDER_TIMEOUT: Duration = Duration::from_secs(120);

pub type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

#[derive(Debug, thiserror::Error)]
pub enum AcmeError {
    #[error("dns provider: {0}")]
    Dns(String),
    #[error("dns provider request failed: {0}")]
    Http(#[from] reqwest::Error),
    #[error("TXT record {name} did not propagate within {timeout:?}")]
    Propagation { name: String, timeout: Duration },
    #[error("acme: {0}")]
    Acme(#[from] instant_acme::Error),
    #[error("acme: {0}")]
    Order(String),
    #[error("issued certificate could not be parsed: {0}")]
    Parse(String),
    #[error(transparent)]
    Secret(#[from] SecretError),
    #[error("{0}")]
    Invalid(String),
}

/// The DNS record name a DNS-01 challenge for `sni` is answered on.
pub fn challenge_record_name(sni: &str) -> String {
    format!("_acme-challenge.{sni}")
}

/// A DNS name the CA can issue for: labels of `[A-Za-z0-9-]`, no wildcard, at
/// least one dot, no trailing dot or whitespace. Lower-cases the input so the
/// row key is canonical; every query that joins Entries to certificate rows
/// lower-cases the pod side the same way.
pub fn validate_sni(sni: &str) -> Result<String, AcmeError> {
    let sni = sni.to_ascii_lowercase();
    let labels: Vec<&str> = sni.split('.').collect();
    let label_ok = |label: &str| {
        !label.is_empty()
            && label.len() <= 63
            && !label.starts_with('-')
            && !label.ends_with('-')
            && label
                .bytes()
                .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-')
    };
    if sni.len() > 253 || labels.len() < 2 || !labels.iter().all(|l| label_ok(l)) {
        return Err(AcmeError::Invalid(format!("'{sni}' is not a hostname")));
    }
    Ok(sni)
}

// --- DNS providers -----------------------------------------------------------

/// Answers a DNS-01 challenge by publishing and removing one TXT record.
pub trait DnsChallengeProvider: Send + Sync {
    /// Publishes `value` at the fully qualified `name` (`_acme-challenge.<sni>`);
    /// returns the provider's id for the record so it can be deleted.
    fn create_txt<'a>(
        &'a self,
        name: &'a str,
        value: &'a str,
    ) -> BoxFuture<'a, Result<String, AcmeError>>;
    fn delete_txt<'a>(&'a self, record_id: &'a str) -> BoxFuture<'a, Result<(), AcmeError>>;
}

/// Cloudflare: records live in a zone (`TlsConfig.domain_id`), names are fully
/// qualified.
pub struct CloudflareDns {
    pub http: reqwest::Client,
    pub base_url: String,
    pub zone_id: String,
    pub api_token: String,
}

#[derive(Deserialize)]
struct CloudflareEnvelope {
    success: bool,
    #[serde(default)]
    errors: Vec<CloudflareMessage>,
    #[serde(default)]
    result: Option<CloudflareRecord>,
}

#[derive(Deserialize)]
struct CloudflareMessage {
    #[serde(default)]
    code: i64,
    #[serde(default)]
    message: String,
}

#[derive(Deserialize)]
struct CloudflareRecord {
    id: String,
}

impl CloudflareEnvelope {
    fn into_result(
        self,
        status: reqwest::StatusCode,
    ) -> Result<Option<CloudflareRecord>, AcmeError> {
        if self.success {
            return Ok(self.result);
        }
        let detail = self
            .errors
            .iter()
            .map(|e| format!("{} ({})", e.message, e.code))
            .collect::<Vec<_>>()
            .join("; ");
        Err(AcmeError::Dns(format!(
            "cloudflare returned {status}: {detail}"
        )))
    }
}

impl DnsChallengeProvider for CloudflareDns {
    fn create_txt<'a>(
        &'a self,
        name: &'a str,
        value: &'a str,
    ) -> BoxFuture<'a, Result<String, AcmeError>> {
        Box::pin(async move {
            let response = self
                .http
                .post(format!(
                    "{}/zones/{}/dns_records",
                    self.base_url, self.zone_id
                ))
                .bearer_auth(&self.api_token)
                .json(&serde_json::json!({
                    "type": "TXT",
                    "name": name,
                    "content": value,
                    "ttl": TXT_TTL,
                }))
                .send()
                .await?;
            let status = response.status();
            let envelope: CloudflareEnvelope = response.json().await?;
            envelope
                .into_result(status)?
                .map(|record| record.id)
                .ok_or_else(|| AcmeError::Dns("cloudflare returned no record id".into()))
        })
    }

    fn delete_txt<'a>(&'a self, record_id: &'a str) -> BoxFuture<'a, Result<(), AcmeError>> {
        Box::pin(async move {
            let response = self
                .http
                .delete(format!(
                    "{}/zones/{}/dns_records/{record_id}",
                    self.base_url, self.zone_id
                ))
                .bearer_auth(&self.api_token)
                .send()
                .await?;
            let status = response.status();
            let envelope: CloudflareEnvelope = response.json().await?;
            envelope.into_result(status).map(|_| ())
        })
    }
}

/// Vercel: records live under a registered domain (`TlsConfig.domain_id`) and
/// are named relative to it; `teamId` scopes the call to a team account.
pub struct VercelDns {
    pub http: reqwest::Client,
    pub base_url: String,
    pub domain: String,
    /// The team id, `None` for a personal account.
    pub team_id: Option<String>,
    pub api_token: String,
}

/// The record name relative to the registered domain: `_acme-challenge.a.example.com`
/// under `example.com` is `_acme-challenge.a`; the apex is `_acme-challenge`.
pub fn vercel_record_name(name: &str, domain: &str) -> String {
    match name.strip_suffix(domain).and_then(|s| s.strip_suffix('.')) {
        Some(relative) if !relative.is_empty() => relative.to_string(),
        _ => name.to_string(),
    }
}

#[derive(Deserialize)]
struct VercelCreated {
    uid: String,
}

#[derive(Deserialize)]
struct VercelFailure {
    error: VercelMessage,
}

#[derive(Deserialize)]
struct VercelMessage {
    #[serde(default)]
    code: String,
    #[serde(default)]
    message: String,
}

impl VercelDns {
    fn team_query(&self) -> Vec<(&'static str, &str)> {
        self.team_id
            .as_deref()
            .filter(|t| !t.is_empty())
            .map(|t| vec![("teamId", t)])
            .unwrap_or_default()
    }

    async fn failure(response: reqwest::Response) -> AcmeError {
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        let detail = serde_json::from_str::<VercelFailure>(&body)
            .map(|f| format!("{} ({})", f.error.message, f.error.code))
            .unwrap_or(body);
        AcmeError::Dns(format!("vercel returned {status}: {detail}"))
    }
}

impl DnsChallengeProvider for VercelDns {
    fn create_txt<'a>(
        &'a self,
        name: &'a str,
        value: &'a str,
    ) -> BoxFuture<'a, Result<String, AcmeError>> {
        Box::pin(async move {
            let response = self
                .http
                .post(format!(
                    "{}/v2/domains/{}/records",
                    self.base_url, self.domain
                ))
                .query(&self.team_query())
                .bearer_auth(&self.api_token)
                .json(&serde_json::json!({
                    "name": vercel_record_name(name, &self.domain),
                    "type": "TXT",
                    "value": value,
                    "ttl": TXT_TTL,
                }))
                .send()
                .await?;
            if !response.status().is_success() {
                return Err(Self::failure(response).await);
            }
            let created: VercelCreated = response.json().await?;
            Ok(created.uid)
        })
    }

    fn delete_txt<'a>(&'a self, record_id: &'a str) -> BoxFuture<'a, Result<(), AcmeError>> {
        Box::pin(async move {
            let response = self
                .http
                .delete(format!(
                    "{}/v2/domains/{}/records/{record_id}",
                    self.base_url, self.domain
                ))
                .query(&self.team_query())
                .bearer_auth(&self.api_token)
                .send()
                .await?;
            if !response.status().is_success() {
                return Err(Self::failure(response).await);
            }
            Ok(())
        })
    }
}

// --- ACME issuer -------------------------------------------------------------

/// Everything one issuance needs from the outside world.
pub struct IssueRequest<'a> {
    pub directory: &'a str,
    pub sni: &'a str,
    /// Serialized instant-acme credentials from a previous issuance for this
    /// row, plaintext.
    pub account_credentials: Option<&'a str>,
    pub dns: &'a dyn DnsChallengeProvider,
}

pub struct IssuedMaterial {
    /// Serialized account credentials to keep for the renewal, plaintext.
    pub account_credentials: String,
    pub private_key_pem: String,
    pub full_chain_pem: String,
}

/// Runs one ACME order end to end. [`InstantAcmeIssuer`] talks to a real CA;
/// tests substitute a fake.
pub trait AcmeIssuer: Send + Sync {
    fn issue<'a>(
        &'a self,
        request: IssueRequest<'a>,
    ) -> BoxFuture<'a, Result<IssuedMaterial, AcmeError>>;
}

/// The production issuer: instant-acme, with the challenge TXT checked against
/// Cloudflare's and Google's public resolvers before the CA is told to validate.
#[derive(Debug, Clone, Copy, Default)]
pub struct InstantAcmeIssuer;

impl AcmeIssuer for InstantAcmeIssuer {
    fn issue<'a>(
        &'a self,
        request: IssueRequest<'a>,
    ) -> BoxFuture<'a, Result<IssuedMaterial, AcmeError>> {
        Box::pin(async move {
            let (account, credentials) =
                load_account(request.directory, request.account_credentials).await?;
            let mut published = Vec::new();
            let result = run_order(&account, request.sni, request.dns, &mut published).await;
            for record_id in &published {
                if let Err(e) = request.dns.delete_txt(record_id).await {
                    tracing::warn!(error = %e, record_id, "deleting the challenge TXT record failed");
                }
            }
            let (private_key_pem, full_chain_pem) = result?;
            Ok(IssuedMaterial {
                account_credentials: credentials,
                private_key_pem,
                full_chain_pem,
            })
        })
    }
}

/// Restores the stored account, or registers a new one. Credentials that no
/// longer restore (a rotated master key, a CA that forgot the account) are
/// replaced rather than fatal: the row is what identifies the certificate, the
/// account is only how we talk to the CA.
async fn load_account(
    directory: &str,
    stored: Option<&str>,
) -> Result<(Account, String), AcmeError> {
    if let Some(json) = stored {
        match serde_json::from_str::<AccountCredentials>(json) {
            Ok(credentials) => match Account::builder()?.from_credentials(credentials).await {
                Ok(account) => return Ok((account, json.to_string())),
                Err(e) => {
                    tracing::warn!(error = %e, directory, "stored ACME account could not be restored; registering a new one")
                }
            },
            Err(e) => {
                tracing::warn!(error = %e, directory, "stored ACME account key is unreadable; registering a new one")
            }
        }
    }
    let (account, credentials) = Account::builder()?
        .create(
            &NewAccount {
                contact: &[],
                terms_of_service_agreed: true,
                only_return_existing: false,
            },
            directory.to_string(),
            None,
        )
        .await?;
    let json = serde_json::to_string(&credentials)
        .map_err(|e| AcmeError::Order(format!("serializing account credentials: {e}")))?;
    Ok((account, json))
}

/// Places the order, answers every DNS-01 authorization (publishing into
/// `published` so the caller can clean up whatever happened), finalizes, and
/// returns `(private_key_pem, full_chain_pem)`.
async fn run_order(
    account: &Account,
    sni: &str,
    dns: &dyn DnsChallengeProvider,
    published: &mut Vec<String>,
) -> Result<(String, String), AcmeError> {
    let identifier = Identifier::Dns(sni.to_string());
    let mut order = account
        .new_order(&NewOrder::new(std::slice::from_ref(&identifier)))
        .await?;
    let mut authorizations = order.authorizations();
    while let Some(result) = authorizations.next().await {
        let mut authz = result?;
        match authz.status {
            AuthorizationStatus::Pending => {}
            AuthorizationStatus::Valid => continue,
            other => {
                return Err(AcmeError::Order(format!(
                    "authorization for {sni} is {other:?}"
                )));
            }
        }
        let mut challenge = authz.challenge(ChallengeType::Dns01).ok_or_else(|| {
            AcmeError::Order(format!("the CA offers no dns-01 challenge for {sni}"))
        })?;
        let name = challenge_record_name(&challenge.identifier().to_string());
        let value = challenge.key_authorization().dns_value();
        published.push(dns.create_txt(&name, &value).await?);
        wait_for_txt(&name, &value).await?;
        challenge.set_ready().await?;
    }
    let policy = RetryPolicy::default().timeout(ORDER_TIMEOUT);
    let status = order.poll_ready(&policy).await?;
    if status != OrderStatus::Ready {
        return Err(AcmeError::Order(format!(
            "order for {sni} became {status:?}"
        )));
    }
    let private_key_pem = order.finalize().await?;
    let full_chain_pem = order.poll_certificate(&policy).await?;
    Ok((private_key_pem, full_chain_pem))
}

/// Polls public resolvers until every one of them returns `value` for `name`,
/// so the CA's own resolvers stand a fair chance.
async fn wait_for_txt(name: &str, value: &str) -> Result<(), AcmeError> {
    let resolvers = [
        ResolverConfig::udp_and_tcp(&CLOUDFLARE),
        ResolverConfig::udp_and_tcp(&GOOGLE),
    ]
    .into_iter()
    .map(|config| {
        let mut builder =
            TokioResolver::builder_with_config(config, TokioRuntimeProvider::default());
        builder.options_mut().cache_size = 0;
        builder.build()
    })
    .collect::<Result<Vec<_>, _>>()
    .map_err(|e| AcmeError::Order(format!("building a resolver: {e}")))?;
    let fqdn = format!("{name}.");
    let visible_everywhere = async {
        loop {
            let mut visible = true;
            for resolver in &resolvers {
                let seen = match resolver.txt_lookup(fqdn.as_str()).await {
                    Ok(lookup) => lookup.answers().iter().any(|record| match &record.data {
                        RData::TXT(txt) => txt
                            .txt_data
                            .iter()
                            .any(|chunk| chunk.as_ref() == value.as_bytes()),
                        _ => false,
                    }),
                    Err(_) => false,
                };
                if !seen {
                    visible = false;
                    break;
                }
            }
            if visible {
                return;
            }
            tokio::time::sleep(PROPAGATION_POLL).await;
        }
    };
    tokio::time::timeout(PROPAGATION_TIMEOUT, visible_everywhere)
        .await
        .map_err(|_| AcmeError::Propagation {
            name: name.to_string(),
            timeout: PROPAGATION_TIMEOUT,
        })
}

/// `(not_before, not_after)` of the leaf, the first block of the chain.
pub fn leaf_validity(full_chain_pem: &str) -> Result<(DateTime<Utc>, DateTime<Utc>), AcmeError> {
    let (_, pem) = x509_parser::pem::parse_x509_pem(full_chain_pem.as_bytes())
        .map_err(|e| AcmeError::Parse(e.to_string()))?;
    let leaf = pem
        .parse_x509()
        .map_err(|e| AcmeError::Parse(e.to_string()))?;
    let validity = leaf.validity();
    let to_utc = |t: i64| {
        DateTime::<Utc>::from_timestamp(t, 0)
            .ok_or_else(|| AcmeError::Parse(format!("timestamp {t} out of range")))
    };
    Ok((
        to_utc(validity.not_before.timestamp())?,
        to_utc(validity.not_after.timestamp())?,
    ))
}

// --- service -----------------------------------------------------------------

#[derive(Clone)]
pub struct AcmeService {
    pub db: Db,
    pub secrets: SecretKey,
    pub config: OrchestrationConfig,
    pub notifier: Notifier,
    pub http: reqwest::Client,
    pub issuer: Arc<dyn AcmeIssuer>,
}

impl AcmeService {
    fn challenge_provider(
        &self,
        row: &DnsProviderEntity,
        domain_id: &str,
    ) -> Result<Box<dyn DnsChallengeProvider>, AcmeError> {
        let api_token = self.secrets.decrypt_str(&row.api_secret)?;
        Ok(match row.provider {
            DnsProvider::Cloudflare => Box::new(CloudflareDns {
                http: self.http.clone(),
                base_url: CLOUDFLARE_API.to_string(),
                zone_id: domain_id.to_string(),
                api_token,
            }),
            DnsProvider::Vercel => Box::new(VercelDns {
                http: self.http.clone(),
                base_url: VERCEL_API.to_string(),
                domain: domain_id.to_string(),
                team_id: Some(row.account_id.clone()).filter(|t| !t.is_empty()),
                api_token,
            }),
        })
    }

    /// The whole order for one row, up to (not including) the database writes
    /// that record the outcome.
    async fn obtain(&self, row: &CertificateEntity) -> Result<IssuedMaterial, AcmeError> {
        let sni = validate_sni(&row.sni)?;
        let provider = self
            .db
            .process(FindDnsProviderById {
                id: row.dns_provider.clone(),
            })
            .await
            .map_err(|e| AcmeError::Order(format!("loading the dns provider: {e}")))?
            .ok_or_else(|| {
                AcmeError::Invalid("the certificate's dns provider no longer exists".into())
            })?;
        let dns = self.challenge_provider(&provider, &row.domain_id)?;
        let account_credentials = match &row.acme_account_key {
            Some(stored) => Some(self.secrets.decrypt_str(stored)?),
            None => None,
        };
        self.issuer
            .issue(IssueRequest {
                directory: self.config.acme_directory(&row.acme_directory),
                sni: &sni,
                account_credentials: account_credentials.as_deref(),
                dns: dns.as_ref(),
            })
            .await
    }

    /// Puts one certificate row's new state on the live bus. No RPC consumes it
    /// yet; it exists so the issue table is complete and a future certificate
    /// stream needs no publisher changes.
    async fn publish_certificate(
        &self,
        id: &CertificateId,
        status: CertificateStatus,
        not_after: Option<DateTime<Utc>>,
        error: Option<String>,
    ) {
        self.notifier
            .live(LiveMessage::CertificateChanged {
                certificate: id.to_string(),
                status,
                not_after_unix_secs: not_after.map(|t| t.timestamp()),
                error,
            })
            .await;
    }
}

/// Issues or renews one certificate row and records the outcome on it. An ACME
/// failure is an [`IssueOutcome::Failed`], not an error: it is stored on the row
/// and retried later. Errors are database failures.
pub struct IssueCertificate {
    pub id: CertificateId,
}

#[derive(Debug)]
pub enum IssueOutcome {
    Issued(Box<CertificateEntity>),
    Failed { sni: String, error: String },
}

impl Processor<IssueCertificate> for AcmeService {
    type Output = IssueOutcome;
    type Error = OrchestrationError;
    #[tracing::instrument(name = "Service:IssueCertificate", skip_all, err, fields(certificate = ?input.id))]
    async fn process(&self, input: IssueCertificate) -> Result<Self::Output, Self::Error> {
        let row = self
            .db
            .process(FindCertificateById {
                id: input.id.clone(),
            })
            .await?
            .ok_or(OrchestrationError::NotFound)?;
        // What the row holds *after* a failed attempt: `MarkCertificateAttemptFailed`
        // keeps an already-issued certificate `Issued` (its material is still
        // valid and still served) and only fails a `Pending` one. Publishing a
        // flat `Failed` would tell a dashboard a live certificate had died.
        let after_failure = if row.status == CertificateStatus::Issued {
            CertificateStatus::Issued
        } else {
            CertificateStatus::Failed
        };
        let material = match self.obtain(&row).await {
            Ok(material) => material,
            Err(e) => {
                let error = e.to_string();
                self.db
                    .process(MarkCertificateAttemptFailed {
                        id: input.id.clone(),
                        error: error.clone(),
                        now: Utc::now(),
                    })
                    .await?;
                self.publish_certificate(
                    &input.id,
                    after_failure,
                    row.not_after,
                    Some(error.clone()),
                )
                .await;
                return Ok(IssueOutcome::Failed {
                    sni: row.sni,
                    error,
                });
            }
        };
        let (not_before, not_after) = match leaf_validity(&material.full_chain_pem) {
            Ok(validity) => validity,
            Err(e) => {
                let error = e.to_string();
                self.db
                    .process(MarkCertificateAttemptFailed {
                        id: input.id.clone(),
                        error: error.clone(),
                        now: Utc::now(),
                    })
                    .await?;
                self.publish_certificate(
                    &input.id,
                    after_failure,
                    row.not_after,
                    Some(error.clone()),
                )
                .await;
                return Ok(IssueOutcome::Failed {
                    sni: row.sni,
                    error,
                });
            }
        };
        let stored = self
            .db
            .process(StoreIssuedCertificate {
                id: input.id.clone(),
                acme_account_key: self.secrets.encrypt_str(&material.account_credentials)?,
                private_key_pem: self.secrets.encrypt_str(&material.private_key_pem)?,
                full_chain_pem: material.full_chain_pem,
                not_before,
                not_after,
                now: Utc::now(),
            })
            .await?;
        self.publish_certificate(
            &input.id,
            stored.status,
            stored.not_after,
            stored.last_error.clone(),
        )
        .await;
        let canvases = self
            .db
            .process(ListCanvasesUsingSni {
                sni: stored.sni.clone(),
            })
            .await?;
        self.db
            .process(TouchCanvases {
                canvases: canvases.clone(),
            })
            .await?;
        for canvas in &canvases {
            self.notifier.notify(canvas).await;
        }
        Ok(IssueOutcome::Issued(Box::new(stored)))
    }
}

/// Makes sure a `certificate` row exists for every `(sni, acme_directory)` an
/// pod asks for; the cron's first step. A pod whose SNI is not a hostname
/// is skipped with a warning: the row would never issue. Returns the ids of the
/// rows ensured.
pub struct EnsureRequestedCertificates;

impl Processor<EnsureRequestedCertificates> for AcmeService {
    type Output = Vec<CertificateId>;
    type Error = OrchestrationError;
    #[tracing::instrument(name = "Service:EnsureRequestedCertificates", skip_all, err)]
    async fn process(&self, _: EnsureRequestedCertificates) -> Result<Self::Output, Self::Error> {
        let requests = self.db.process(ListTlsRequests).await?;
        let mut seen = HashSet::new();
        let mut ensured = Vec::new();
        for request in requests {
            let sni = match validate_sni(&request.tls.sni) {
                Ok(sni) => sni,
                Err(e) => {
                    tracing::warn!(error = %e, canvas = ?request.canvas, "skipping a pod's tls request");
                    continue;
                }
            };
            let directory = self
                .config
                .acme_directory(&request.tls.acme_directory)
                .to_string();
            if !seen.insert((sni.clone(), directory.clone())) {
                continue;
            }
            let row = self
                .db
                .process(EnsureCertificate {
                    sni,
                    dns_provider: request.tls.dns_provider,
                    domain_id: request.tls.domain_id,
                    acme_directory: directory,
                    now: Utc::now(),
                })
                .await?;
            ensured.push(row.id);
        }
        Ok(ensured)
    }
}

pub struct ListCertificates {
    pub actor: Identity,
}

impl Processor<ListCertificates> for AcmeService {
    type Output = Vec<CertificateEntity>;
    type Error = OrchestrationError;
    #[tracing::instrument(name = "Service:ListCertificates", skip_all, err)]
    async fn process(&self, input: ListCertificates) -> Result<Self::Output, Self::Error> {
        input.actor.ensure(Permission::ViewWorkspace)?;
        Ok(self.db.process(ListCertificatesRow).await?)
    }
}

fn ensure_admin(actor: &Identity) -> Result<(), OrchestrationError> {
    actor.ensure(Permission::EditWorkspace)?;
    if actor.role != AccountRole::Admin {
        return Err(OrchestrationError::PermissionDenied);
    }
    Ok(())
}

/// Admin only: clears a failure so the next cron pass retries, or forces the
/// renewal of an issued certificate.
pub struct RetryCertificate {
    pub actor: Identity,
    pub id: CertificateId,
}

impl Processor<RetryCertificate> for AcmeService {
    type Output = CertificateEntity;
    type Error = OrchestrationError;
    #[tracing::instrument(name = "Service:RetryCertificate", skip_all, err)]
    async fn process(&self, input: RetryCertificate) -> Result<Self::Output, Self::Error> {
        ensure_admin(&input.actor)?;
        let row = self
            .db
            .process(RetryCertificateRow {
                id: input.id.clone(),
            })
            .await?
            .ok_or(OrchestrationError::NotFound)?;
        self.publish_certificate(&input.id, row.status, row.not_after, row.last_error.clone())
            .await;
        Ok(row)
    }
}

/// Admin only. Refused while a pod still asks for the certificate's
/// `(sni, acme_directory)`: the cron would recreate the row and start over,
/// and until then every server serving the SNI would lose its listener.
pub struct DeleteCertificate {
    pub actor: Identity,
    pub id: CertificateId,
}

impl Processor<DeleteCertificate> for AcmeService {
    type Output = ();
    type Error = OrchestrationError;
    #[tracing::instrument(name = "Service:DeleteCertificate", skip_all, err)]
    async fn process(&self, input: DeleteCertificate) -> Result<Self::Output, Self::Error> {
        ensure_admin(&input.actor)?;
        let row = self
            .db
            .process(FindCertificateById {
                id: input.id.clone(),
            })
            .await?
            .ok_or(OrchestrationError::NotFound)?;
        let referenced = self
            .db
            .process(ListTlsRequests)
            .await?
            .into_iter()
            .filter(|request| {
                validate_sni(&request.tls.sni).is_ok_and(|sni| sni == row.sni)
                    && self.config.acme_directory(&request.tls.acme_directory) == row.acme_directory
            })
            .count();
        if referenced > 0 {
            return Err(OrchestrationError::Conflict(format!(
                "certificate for {} is still used by {referenced} pod(s)",
                row.sni
            )));
        }
        self.db
            .process(DeleteCertificateRow {
                id: input.id.clone(),
            })
            .await?;
        // The row is gone; the status is the one it held, and `error` says so —
        // a consumer that renders the certificate list drops it either way.
        self.publish_certificate(
            &input.id,
            row.status,
            row.not_after,
            Some("deleted".to_string()),
        )
        .await;
        Ok(())
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use super::*;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;
    use tokio::sync::Mutex;

    #[derive(Debug, Clone)]
    struct Recorded {
        method: String,
        target: String,
        authorization: Option<String>,
        body: String,
    }

    /// A one-request-per-connection HTTP/1.1 responder that records what it saw
    /// and answers with the canned `(status, body)` pairs in order.
    async fn mock_server(
        responses: Vec<(u16, &'static str)>,
    ) -> (String, Arc<Mutex<Vec<Recorded>>>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base_url = format!("http://{}", listener.local_addr().unwrap());
        let seen = Arc::new(Mutex::new(Vec::new()));
        let log = seen.clone();
        tokio::spawn(async move {
            let mut responses = responses.into_iter();
            while let Ok((mut socket, _)) = listener.accept().await {
                let mut buf = Vec::new();
                let head_end = loop {
                    let mut chunk = [0u8; 1024];
                    let n = socket.read(&mut chunk).await.unwrap();
                    if n == 0 {
                        break None;
                    }
                    buf.extend_from_slice(&chunk[..n]);
                    if let Some(pos) = buf.windows(4).position(|w| w == b"\r\n\r\n") {
                        break Some(pos.saturating_add(4));
                    }
                };
                let Some(head_end) = head_end else { continue };
                let head = String::from_utf8_lossy(&buf[..head_end]).to_string();
                let mut lines = head.lines();
                let request_line = lines.next().unwrap_or_default();
                let mut parts = request_line.split(' ');
                let method = parts.next().unwrap_or_default().to_string();
                let target = parts.next().unwrap_or_default().to_string();
                let mut content_length = 0usize;
                let mut authorization = None;
                for line in lines {
                    if let Some((k, v)) = line.split_once(':') {
                        match k.to_ascii_lowercase().as_str() {
                            "content-length" => content_length = v.trim().parse().unwrap(),
                            "authorization" => authorization = Some(v.trim().to_string()),
                            _ => {}
                        }
                    }
                }
                while buf.len() < head_end.saturating_add(content_length) {
                    let mut chunk = [0u8; 1024];
                    let n = socket.read(&mut chunk).await.unwrap();
                    if n == 0 {
                        break;
                    }
                    buf.extend_from_slice(&chunk[..n]);
                }
                let body = String::from_utf8_lossy(&buf[head_end..]).to_string();
                log.lock().await.push(Recorded {
                    method,
                    target,
                    authorization,
                    body,
                });
                let (status, body) = responses.next().unwrap_or((500, "{}"));
                let reply = format!(
                    "HTTP/1.1 {status} X\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                    body.len()
                );
                socket.write_all(reply.as_bytes()).await.unwrap();
                socket.shutdown().await.ok();
            }
        });
        (base_url, seen)
    }

    fn body_json(recorded: &Recorded) -> serde_json::Value {
        serde_json::from_str(&recorded.body).unwrap()
    }

    #[tokio::test]
    async fn cloudflare_request_shapes() {
        let (base_url, seen) = mock_server(vec![
            (
                200,
                r#"{"success":true,"errors":[],"result":{"id":"rec123"}}"#,
            ),
            (
                200,
                r#"{"success":true,"errors":[],"result":{"id":"rec123"}}"#,
            ),
        ])
        .await;
        let dns = CloudflareDns {
            http: reqwest::Client::new(),
            base_url,
            zone_id: "zone1".into(),
            api_token: "cf-token".into(),
        };
        let id = dns
            .create_txt("_acme-challenge.a.example.com", "v4lue")
            .await
            .unwrap();
        assert_eq!(id, "rec123");
        dns.delete_txt(&id).await.unwrap();

        let seen = seen.lock().await.clone();
        assert_eq!(seen.len(), 2);
        assert_eq!(seen[0].method, "POST");
        assert_eq!(seen[0].target, "/zones/zone1/dns_records");
        assert_eq!(seen[0].authorization.as_deref(), Some("Bearer cf-token"));
        assert_eq!(
            body_json(&seen[0]),
            serde_json::json!({"type":"TXT","name":"_acme-challenge.a.example.com","content":"v4lue","ttl":60})
        );
        assert_eq!(seen[1].method, "DELETE");
        assert_eq!(seen[1].target, "/zones/zone1/dns_records/rec123");
        assert_eq!(seen[1].authorization.as_deref(), Some("Bearer cf-token"));
    }

    #[tokio::test]
    async fn cloudflare_api_error_is_reported() {
        let (base_url, _) = mock_server(vec![(
            403,
            r#"{"success":false,"errors":[{"code":10000,"message":"Authentication error"}],"result":null}"#,
        )])
        .await;
        let dns = CloudflareDns {
            http: reqwest::Client::new(),
            base_url,
            zone_id: "zone1".into(),
            api_token: "bad".into(),
        };
        let err = dns
            .create_txt("_acme-challenge.a.example.com", "v")
            .await
            .unwrap_err();
        let message = err.to_string();
        assert!(message.contains("403"), "{message}");
        assert!(message.contains("Authentication error"), "{message}");
    }

    #[tokio::test]
    async fn vercel_request_shapes_with_team() {
        let (base_url, seen) =
            mock_server(vec![(200, r#"{"uid":"rec_abc","updated":1}"#), (200, "{}")]).await;
        let dns = VercelDns {
            http: reqwest::Client::new(),
            base_url,
            domain: "example.com".into(),
            team_id: Some("team_1".into()),
            api_token: "vc-token".into(),
        };
        let id = dns
            .create_txt("_acme-challenge.a.example.com", "v4lue")
            .await
            .unwrap();
        assert_eq!(id, "rec_abc");
        dns.delete_txt(&id).await.unwrap();

        let seen = seen.lock().await.clone();
        assert_eq!(seen.len(), 2);
        assert_eq!(seen[0].method, "POST");
        assert_eq!(
            seen[0].target,
            "/v2/domains/example.com/records?teamId=team_1"
        );
        assert_eq!(seen[0].authorization.as_deref(), Some("Bearer vc-token"));
        assert_eq!(
            body_json(&seen[0]),
            serde_json::json!({"name":"_acme-challenge.a","type":"TXT","value":"v4lue","ttl":60})
        );
        assert_eq!(seen[1].method, "DELETE");
        assert_eq!(
            seen[1].target,
            "/v2/domains/example.com/records/rec_abc?teamId=team_1"
        );
        assert_eq!(seen[1].authorization.as_deref(), Some("Bearer vc-token"));
    }

    #[tokio::test]
    async fn vercel_personal_account_has_no_team_query() {
        let (base_url, seen) = mock_server(vec![(200, r#"{"uid":"rec_abc"}"#)]).await;
        let dns = VercelDns {
            http: reqwest::Client::new(),
            base_url,
            domain: "example.com".into(),
            team_id: None,
            api_token: "vc-token".into(),
        };
        dns.create_txt("_acme-challenge.example.com", "v")
            .await
            .unwrap();
        let seen = seen.lock().await.clone();
        assert_eq!(seen[0].target, "/v2/domains/example.com/records");
        assert_eq!(body_json(&seen[0])["name"], "_acme-challenge");
    }

    #[tokio::test]
    async fn vercel_api_error_is_reported() {
        let (base_url, _) = mock_server(vec![(
            400,
            r#"{"error":{"code":"bad_request","message":"Invalid record"}}"#,
        )])
        .await;
        let dns = VercelDns {
            http: reqwest::Client::new(),
            base_url,
            domain: "example.com".into(),
            team_id: None,
            api_token: "vc-token".into(),
        };
        let err = dns
            .create_txt("_acme-challenge.example.com", "v")
            .await
            .unwrap_err();
        let message = err.to_string();
        assert!(message.contains("400"), "{message}");
        assert!(message.contains("Invalid record"), "{message}");
    }

    #[test]
    fn record_names() {
        assert_eq!(
            challenge_record_name("a.example.com"),
            "_acme-challenge.a.example.com"
        );
        assert_eq!(
            vercel_record_name("_acme-challenge.a.b.example.com", "example.com"),
            "_acme-challenge.a.b"
        );
        assert_eq!(
            vercel_record_name("_acme-challenge.example.com", "example.com"),
            "_acme-challenge"
        );
        // A name outside the domain is passed through rather than mangled.
        assert_eq!(
            vercel_record_name("_acme-challenge.other.org", "example.com"),
            "_acme-challenge.other.org"
        );
        assert_eq!(
            vercel_record_name("_acme-challenge.notexample.com", "example.com"),
            "_acme-challenge.notexample.com"
        );
    }

    #[test]
    fn sni_validation() {
        assert_eq!(validate_sni("A.Example.COM").unwrap(), "a.example.com");
        for bad in [
            "",
            "localhost",
            "*.example.com",
            "-a.example.com",
            "a_b.example.com",
            "a..com",
            "a b.com",
            "a.example.com.",
            " a.example.com",
        ] {
            assert!(validate_sni(bad).is_err(), "{bad:?} should be rejected");
        }
    }

    #[test]
    fn leaf_validity_reads_first_block() {
        let key = rcgen::KeyPair::generate().unwrap();
        let mut params = rcgen::CertificateParams::new(vec!["a.example.com".to_string()]).unwrap();
        params.not_before = rcgen::date_time_ymd(2026, 1, 2);
        params.not_after = rcgen::date_time_ymd(2026, 4, 2);
        let cert = params.self_signed(&key).unwrap();
        let chain = format!("{}{}", cert.pem(), cert.pem());
        let (not_before, not_after) = leaf_validity(&chain).unwrap();
        assert_eq!(not_before.to_rfc3339(), "2026-01-02T00:00:00+00:00");
        assert_eq!(not_after.to_rfc3339(), "2026-04-02T00:00:00+00:00");
        assert!(leaf_validity("not pem").is_err());
    }
}
