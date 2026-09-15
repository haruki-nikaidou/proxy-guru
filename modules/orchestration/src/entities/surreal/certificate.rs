//! Public certificates obtained through ACME (DNS-01) for Entry nodes.
//!
//! One row per `(sni, acme_directory)`; every Entry whose `TlsConfig` names that
//! pair serves the same certificate. `acme_account_key` and `private_key_pem`
//! are encrypted with the master key; `full_chain_pem` is public. `version`
//! increments on every issuance and renewal, and a config snapshot pins the
//! version it was derived against (see [`crate::entities::surreal::view`]), so a
//! renewal becomes a new revision for every server serving the certificate.

use crate::entities::surreal::canvas::CanvasId;
use crate::entities::surreal::dns::DnsProviderId;
use crate::entities::surreal::node::TlsConfig;
use crate::utils::ids::record_key;
use chrono::{DateTime, Utc};
use kanau::processor::Processor;
use newtype_record_id::table_record;
use surrealdb_types::SurrealValue;
use wakuwaku::surreal::SurrealProcessor;

table_record!(CertificateId, "certificate");

#[derive(Debug, Clone, SurrealValue)]
pub struct CertificateEntity {
    pub id: CertificateId,
    pub sni: String,
    pub dns_provider: DnsProviderId,
    /// Cloudflare: zone id. Vercel: the registered domain (`example.com`).
    pub domain_id: String,
    pub acme_directory: String,
    pub status: CertificateStatus,
    /// Encrypted PEM of the ACME account key, once an account exists.
    pub acme_account_key: Option<String>,
    /// Encrypted PEM. Set together with `full_chain_pem` when `status` is `Issued`.
    pub private_key_pem: Option<String>,
    pub full_chain_pem: Option<String>,
    pub not_before: Option<DateTime<Utc>>,
    pub not_after: Option<DateTime<Utc>>,
    pub last_error: Option<String>,
    pub last_attempt_at: Option<DateTime<Utc>>,
    pub version: i64,
    pub created_at: DateTime<Utc>,
}

/// The `rkyv` derives put this enum on the live bus unchanged
/// ([`crate::events::live::LiveMessage::CertificateChanged`]).
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    SurrealValue,
    rkyv::Archive,
    rkyv::Serialize,
    rkyv::Deserialize,
)]
#[surreal(untagged, rename_all = "snake_case")]
pub enum CertificateStatus {
    /// Wanted by an Entry, not yet issued.
    Pending,
    Issued,
    /// The last attempt failed; `last_error` says why. Retried after
    /// `acme_retry_after`, or immediately via `RetryCertificate`.
    Failed,
}

impl CertificateEntity {
    /// An issued certificate that can be delivered right now.
    pub fn is_issued(&self) -> bool {
        self.status == CertificateStatus::Issued
            && self.private_key_pem.is_some()
            && self.full_chain_pem.is_some()
    }
}

/// Creates the `Pending` row for a `(sni, acme_directory)` pair, or returns the
/// existing one untouched. `dns_provider` / `domain_id` are only written on
/// creation: the first Entry to ask wins, later Entries share the certificate.
#[derive(Debug)]
pub struct EnsureCertificate {
    pub sni: String,
    pub dns_provider: DnsProviderId,
    pub domain_id: String,
    pub acme_directory: String,
    pub now: DateTime<Utc>,
}

impl Processor<EnsureCertificate> for SurrealProcessor {
    type Output = CertificateEntity;
    type Error = surrealdb::Error;
    #[tracing::instrument(name = "Query-Transaction:EnsureCertificate", skip_all, err, fields(sni = %input.sni))]
    async fn process(&self, input: EnsureCertificate) -> Result<Self::Output, Self::Error> {
        // Statement 0 is BEGIN; the RETURN below is statement 3.
        let mut resp = self
            .db()
            .query(
                "BEGIN TRANSACTION;
                 LET $existing = (SELECT * FROM certificate WHERE sni = $sni AND acme_directory = $acme_directory LIMIT 1);
                 LET $row = IF array::len($existing) > 0 { $existing[0] } ELSE {
                     (CREATE ONLY certificate CONTENT {
                         sni: $sni, dns_provider: $dns_provider, domain_id: $domain_id,
                         acme_directory: $acme_directory, status: 'pending', created_at: $now
                     })
                 };
                 RETURN $row;
                 COMMIT TRANSACTION;",
            )
            .bind(("sni", input.sni))
            .bind(("dns_provider", input.dns_provider))
            .bind(("domain_id", input.domain_id))
            .bind(("acme_directory", input.acme_directory))
            .bind(("now", input.now))
            .await?;
        resp.take::<Option<CertificateEntity>>(3)?.ok_or_else(|| {
            surrealdb::Error::internal("ensure certificate returned no row".to_string())
        })
    }
}

#[derive(Debug)]
pub struct ListCertificates;

impl Processor<ListCertificates> for SurrealProcessor {
    type Output = Vec<CertificateEntity>;
    type Error = surrealdb::Error;
    #[tracing::instrument(name = "Query:ListCertificates", skip_all, err)]
    async fn process(&self, _: ListCertificates) -> Result<Self::Output, Self::Error> {
        let mut resp = self
            .db()
            .query("SELECT * FROM certificate ORDER BY sni ASC, acme_directory ASC")
            .await?;
        resp.take::<Vec<CertificateEntity>>(0)
    }
}

#[derive(Debug)]
pub struct FindCertificateById {
    pub id: CertificateId,
}

impl Processor<FindCertificateById> for SurrealProcessor {
    type Output = Option<CertificateEntity>;
    type Error = surrealdb::Error;
    #[tracing::instrument(name = "Query:FindCertificateById", skip_all, err)]
    async fn process(&self, input: FindCertificateById) -> Result<Self::Output, Self::Error> {
        let mut resp = self
            .db()
            .query("SELECT * FROM $id")
            .bind(("id", input.id))
            .await?;
        resp.take::<Option<CertificateEntity>>(0)
    }
}

/// Every certificate for any of the given SNIs, all directories. Derivation
/// resolves `(sni, acme_directory)` from this in memory.
#[derive(Debug)]
pub struct ListCertificatesBySnis {
    pub snis: Vec<String>,
}

impl Processor<ListCertificatesBySnis> for SurrealProcessor {
    type Output = Vec<CertificateEntity>;
    type Error = surrealdb::Error;
    #[tracing::instrument(name = "Query:ListCertificatesBySnis", skip_all, err)]
    async fn process(&self, input: ListCertificatesBySnis) -> Result<Self::Output, Self::Error> {
        if input.snis.is_empty() {
            return Ok(Vec::new());
        }
        let mut resp = self
            .db()
            .query("SELECT * FROM certificate WHERE sni IN $snis")
            .bind(("snis", input.snis))
            .await?;
        resp.take::<Vec<CertificateEntity>>(0)
    }
}

/// The rows a config snapshot pins, fetched when the revision is handed to a
/// worker.
#[derive(Debug)]
pub struct ListCertificatesByIds {
    pub ids: Vec<CertificateId>,
}

impl Processor<ListCertificatesByIds> for SurrealProcessor {
    type Output = Vec<CertificateEntity>;
    type Error = surrealdb::Error;
    #[tracing::instrument(name = "Query:ListCertificatesByIds", skip_all, err)]
    async fn process(&self, input: ListCertificatesByIds) -> Result<Self::Output, Self::Error> {
        if input.ids.is_empty() {
            return Ok(Vec::new());
        }
        let mut resp = self
            .db()
            .query("SELECT * FROM certificate WHERE id IN $ids")
            .bind(("ids", input.ids))
            .await?;
        resp.take::<Vec<CertificateEntity>>(0)
    }
}

/// Certificates the renewal cron should work on: `Pending` and `Failed` rows
/// whose `last_attempt_at` is before `retry_before` (or unset), and `Issued`
/// rows whose `not_after` is before `renew_before` (throttled the same way after
/// a failed renewal) or whose `last_attempt_at` was cleared by
/// [`RetryCertificateRow`] (a forced renewal).
///
/// `last_attempt_at` is stamped by [`ClaimCertificateAttempt`] before the order
/// starts, not only when it ends, so this window is what bounds attempts: a row
/// whose attempt is still running, or whose attempt crashed mid-order, is out of
/// the listing until `acme_retry_after` has passed. That is what keeps two
/// consumers handed the same signal from hammering a rate-limited CA. The claim
/// itself no longer looks at time at all — it only refuses a listing that has
/// gone stale.
#[derive(Debug)]
pub struct ListCertificatesDue {
    pub renew_before: DateTime<Utc>,
    pub retry_before: DateTime<Utc>,
}

impl Processor<ListCertificatesDue> for SurrealProcessor {
    type Output = Vec<CertificateEntity>;
    type Error = surrealdb::Error;
    #[tracing::instrument(name = "Query:ListCertificatesDue", skip_all, err)]
    async fn process(&self, input: ListCertificatesDue) -> Result<Self::Output, Self::Error> {
        let mut resp = self
            .db()
            .query(
                "SELECT * FROM certificate WHERE
                    (status = 'pending' AND (last_attempt_at = NONE OR last_attempt_at < $retry_before))
                    OR (status = 'failed' AND (last_attempt_at = NONE OR last_attempt_at < $retry_before))
                    OR (status = 'issued' AND (
                        last_attempt_at = NONE
                        OR (not_after != NONE AND not_after < $renew_before AND last_attempt_at < $retry_before)
                    ))
                 ORDER BY created_at ASC",
            )
            .bind(("renew_before", input.renew_before))
            .bind(("retry_before", input.retry_before))
            .await?;
        resp.take::<Vec<CertificateEntity>>(0)
    }
}

/// Claims the next ACME attempt for one row: an exact compare-and-set on
/// `last_attempt_at`, stamping `now` only when the row still carries the value
/// the pass observed in [`ListCertificatesDue`].
///
/// An order takes minutes and rows are ordered one at a time, so a pass holds
/// its listing for a long time — by the time it reaches a row, another pass may
/// already have renewed it. A time window would let that happen: once
/// `acme_retry_after` elapsed, a claim would succeed and order a certificate
/// that is no longer due. Matching the observed value instead makes a stale
/// listing harmless, and subsumes the two-consumers case — both read the same
/// value, only the first `UPDATE` matches.
#[derive(Debug)]
pub struct ClaimCertificateAttempt {
    pub id: CertificateId,
    pub now: DateTime<Utc>,
    /// `last_attempt_at` as the pass read it; the claim is refused when anything
    /// touched the row since, which is what makes a stale listing harmless.
    pub seen_attempt_at: Option<DateTime<Utc>>,
}

impl Processor<ClaimCertificateAttempt> for SurrealProcessor {
    type Output = bool;
    type Error = surrealdb::Error;
    #[tracing::instrument(name = "Query:ClaimCertificateAttempt", skip_all, err, fields(certificate = ?input.id))]
    async fn process(&self, input: ClaimCertificateAttempt) -> Result<Self::Output, Self::Error> {
        let mut resp = self
            .db()
            .query(
                "UPDATE $id SET last_attempt_at = $now
                 WHERE last_attempt_at = $seen_attempt_at
                 RETURN AFTER",
            )
            .bind(("id", input.id))
            .bind(("now", input.now))
            .bind(("seen_attempt_at", input.seen_attempt_at))
            .await?;
        Ok(!resp.take::<Vec<CertificateEntity>>(0)?.is_empty())
    }
}

/// Records a successful issuance or renewal: secrets already encrypted; sets
/// `status = Issued`, clears `last_error`, stamps `last_attempt_at = now`, bumps
/// `version`.
#[derive(Debug)]
pub struct StoreIssuedCertificate {
    pub id: CertificateId,
    pub acme_account_key: String,
    pub private_key_pem: String,
    pub full_chain_pem: String,
    pub not_before: DateTime<Utc>,
    pub not_after: DateTime<Utc>,
    pub now: DateTime<Utc>,
}

impl Processor<StoreIssuedCertificate> for SurrealProcessor {
    type Output = CertificateEntity;
    type Error = surrealdb::Error;
    #[tracing::instrument(name = "Query:StoreIssuedCertificate", skip_all, err, fields(certificate = ?input.id))]
    async fn process(&self, input: StoreIssuedCertificate) -> Result<Self::Output, Self::Error> {
        let mut resp = self
            .db()
            .query(
                "UPDATE $id SET
                    status = 'issued',
                    acme_account_key = $acme_account_key,
                    private_key_pem = $private_key_pem,
                    full_chain_pem = $full_chain_pem,
                    not_before = $not_before,
                    not_after = $not_after,
                    last_error = NONE,
                    last_attempt_at = $now,
                    version += 1
                 RETURN AFTER",
            )
            .bind(("id", input.id))
            .bind(("acme_account_key", input.acme_account_key))
            .bind(("private_key_pem", input.private_key_pem))
            .bind(("full_chain_pem", input.full_chain_pem))
            .bind(("not_before", input.not_before))
            .bind(("not_after", input.not_after))
            .bind(("now", input.now))
            .await?;
        resp.take::<Option<CertificateEntity>>(0)?
            .ok_or_else(|| surrealdb::Error::internal("certificate not found".to_string()))
    }
}

/// Records a failed attempt. An `Issued` certificate that fails to renew keeps
/// its material and status, only `last_error` / `last_attempt_at` change; a
/// `Pending` one becomes `Failed`.
#[derive(Debug)]
pub struct MarkCertificateAttemptFailed {
    pub id: CertificateId,
    pub error: String,
    pub now: DateTime<Utc>,
}

impl Processor<MarkCertificateAttemptFailed> for SurrealProcessor {
    type Output = ();
    type Error = surrealdb::Error;
    #[tracing::instrument(name = "Query:MarkCertificateAttemptFailed", skip_all, err, fields(certificate = ?input.id))]
    async fn process(
        &self,
        input: MarkCertificateAttemptFailed,
    ) -> Result<Self::Output, Self::Error> {
        self.db()
            .query(
                "UPDATE $id SET
                    status = IF status = 'issued' { 'issued' } ELSE { 'failed' },
                    last_error = $error,
                    last_attempt_at = $now",
            )
            .bind(("id", input.id))
            .bind(("error", input.error))
            .bind(("now", input.now))
            .await?
            .check()?;
        Ok(())
    }
}

/// Clears the failure state so the next cron pass retries: a `Failed` row goes
/// back to `Pending`; an `Issued` row keeps its material and status but loses
/// `last_attempt_at`, which [`ListCertificatesDue`] reads as a forced renewal.
#[derive(Debug)]
pub struct RetryCertificateRow {
    pub id: CertificateId,
}

impl Processor<RetryCertificateRow> for SurrealProcessor {
    /// `None` when the certificate does not exist.
    type Output = Option<CertificateEntity>;
    type Error = surrealdb::Error;
    #[tracing::instrument(name = "Query:RetryCertificateRow", skip_all, err, fields(certificate = ?input.id))]
    async fn process(&self, input: RetryCertificateRow) -> Result<Self::Output, Self::Error> {
        let mut resp = self
            .db()
            .query(
                "UPDATE $id SET
                    status = IF status = 'issued' { 'issued' } ELSE { 'pending' },
                    last_error = NONE,
                    last_attempt_at = NONE
                 RETURN AFTER",
            )
            .bind(("id", input.id))
            .await?;
        resp.take::<Option<CertificateEntity>>(0)
    }
}

#[derive(Debug)]
pub struct DeleteCertificateRow {
    pub id: CertificateId,
}

impl Processor<DeleteCertificateRow> for SurrealProcessor {
    type Output = ();
    type Error = surrealdb::Error;
    #[tracing::instrument(name = "Query:DeleteCertificateRow", skip_all, err, fields(certificate = ?input.id))]
    async fn process(&self, input: DeleteCertificateRow) -> Result<Self::Output, Self::Error> {
        self.db()
            .query("DELETE $id")
            .bind(("id", input.id))
            .await?
            .check()?;
        Ok(())
    }
}

/// The `TlsConfig` blocks Entry nodes currently ask for, one per Entry, with the
/// canvas of each; the cron ensures a certificate row exists for each distinct
/// `(sni, acme_directory)`.
#[derive(Debug)]
pub struct ListTlsRequests;

#[derive(Debug, Clone, SurrealValue)]
pub struct TlsRequest {
    pub canvas: CanvasId,
    pub tls: TlsConfig,
}

impl Processor<ListTlsRequests> for SurrealProcessor {
    type Output = Vec<TlsRequest>;
    type Error = surrealdb::Error;
    #[tracing::instrument(name = "Query:ListTlsRequests", skip_all, err)]
    async fn process(&self, _: ListTlsRequests) -> Result<Self::Output, Self::Error> {
        let mut resp = self
            .db()
            .query(
                "SELECT canvas, spec.config.tls AS tls FROM orchestration_node
                 WHERE spec.type = 'entry' AND spec.config.tls != NONE",
            )
            .await?;
        resp.take::<Vec<TlsRequest>>(0)
    }
}

/// Canvases holding an Entry whose `TlsConfig.sni` is the given one; issuance
/// and renewal touch them so their servers get a new revision.
#[derive(Debug)]
pub struct ListCanvasesUsingSni {
    pub sni: String,
}

impl Processor<ListCanvasesUsingSni> for SurrealProcessor {
    type Output = Vec<CanvasId>;
    type Error = surrealdb::Error;
    #[tracing::instrument(name = "Query:ListCanvasesUsingSni", skip_all, err, fields(sni = %input.sni))]
    async fn process(&self, input: ListCanvasesUsingSni) -> Result<Self::Output, Self::Error> {
        let mut resp = self
            .db()
            .query(
                "SELECT VALUE canvas FROM orchestration_node
                 WHERE spec.type = 'entry' AND string::lowercase(spec.config.tls.sni) = $sni",
            )
            .bind(("sni", input.sni))
            .await?;
        let mut canvases = resp.take::<Vec<CanvasId>>(0)?;
        canvases.sort_by_key(|c| record_key(&c.0));
        canvases.dedup_by_key(|c| record_key(&c.0));
        Ok(canvases)
    }
}

/// Bumps the generation of the tree each canvas belongs to
/// (`fn::orchestration_touch`), so the derivation hook re-derives its servers.
/// Certificate issuance and relay leaf rotation use it: the config did not
/// change, the material it pins did.
#[derive(Debug)]
pub struct TouchCanvases {
    pub canvases: Vec<CanvasId>,
}

impl Processor<TouchCanvases> for SurrealProcessor {
    type Output = ();
    type Error = surrealdb::Error;
    #[tracing::instrument(name = "Query-Transaction:TouchCanvases", skip_all, err)]
    async fn process(&self, input: TouchCanvases) -> Result<Self::Output, Self::Error> {
        if input.canvases.is_empty() {
            return Ok(());
        }
        self.db()
            .query(
                "BEGIN TRANSACTION;
                 FOR $canvas IN $canvases { fn::orchestration_touch($canvas) };
                 COMMIT TRANSACTION;",
            )
            .bind(("canvases", input.canvases))
            .await?
            .check()?;
        Ok(())
    }
}
