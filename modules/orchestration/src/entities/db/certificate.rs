//! Public certificates obtained through ACME (DNS-01) for TLS client pods.
//!
//! One row per `(sni, acme_directory)`; every pod whose `TlsConfig` names that
//! pair serves the same certificate. `acme_account_key` and `private_key_pem`
//! are encrypted with the master key; `full_chain_pem` is public. `version`
//! increments on every issuance and renewal, and a config snapshot pins the
//! version it was derived against (see [`crate::entities::db::view`]), so a
//! renewal becomes a new revision for every server serving the certificate.

use crate::entities::db::canvas::CanvasId;
use crate::entities::db::dns::DnsProviderId;
use crate::entities::db::fence;
use crate::entities::db::pod::TlsConfig;
use base::db::{Db, Error};
use chrono::{DateTime, Utc};
use db_types::{table_record, text_enum};
use kanau::processor::Processor;

table_record!(CertificateId, "certificate");

#[derive(Debug, Clone, sqlx::FromRow)]
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
#[derive(Debug, Clone, Copy, PartialEq, Eq, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
pub enum CertificateStatus {
    /// Wanted by a pod, not yet issued.
    Pending,
    Issued,
    /// The last attempt failed; `last_error` says why. Retried after
    /// `acme_retry_after`, or immediately via `RetryCertificate`.
    Failed,
}
text_enum!(CertificateStatus {
    Pending => "pending",
    Issued => "issued",
    Failed => "failed",
});

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
/// creation: the first pod to ask wins, later pods share the certificate.
#[derive(Debug)]
pub struct EnsureCertificate {
    pub sni: String,
    pub dns_provider: DnsProviderId,
    pub domain_id: String,
    pub acme_directory: String,
    pub now: DateTime<Utc>,
}

impl Processor<EnsureCertificate> for Db {
    type Output = CertificateEntity;
    type Error = Error;
    #[tracing::instrument(name = "Query:EnsureCertificate", skip_all, err, fields(sni = %input.sni))]
    async fn process(&self, input: EnsureCertificate) -> Result<Self::Output, Self::Error> {
        // `DO NOTHING` returns no row when the pair exists; a second statement
        // then reads the one that won, whether it was ours or a concurrent
        // pod's.
        let created: Option<CertificateEntity> = sqlx::query_as(
            "INSERT INTO certificate
                 (id, sni, dns_provider, domain_id, acme_directory, status, created_at)
             VALUES ($1, $2, $3, $4, $5, 'pending', $6)
             ON CONFLICT ON CONSTRAINT certificate_sni_acme_directory_key DO NOTHING
             RETURNING *",
        )
        .bind(CertificateId::new())
        .bind(&input.sni)
        .bind(&input.dns_provider)
        .bind(&input.domain_id)
        .bind(&input.acme_directory)
        .bind(input.now)
        .fetch_optional(self.db())
        .await?;
        if let Some(row) = created {
            return Ok(row);
        }
        Ok(
            sqlx::query_as("SELECT * FROM certificate WHERE sni = $1 AND acme_directory = $2")
                .bind(&input.sni)
                .bind(&input.acme_directory)
                .fetch_one(self.db())
                .await?,
        )
    }
}

#[derive(Debug)]
pub struct ListCertificates;

impl Processor<ListCertificates> for Db {
    type Output = Vec<CertificateEntity>;
    type Error = Error;
    #[tracing::instrument(name = "Query:ListCertificates", skip_all, err)]
    async fn process(&self, _: ListCertificates) -> Result<Self::Output, Self::Error> {
        Ok(
            sqlx::query_as("SELECT * FROM certificate ORDER BY sni ASC, acme_directory ASC")
                .fetch_all(self.db())
                .await?,
        )
    }
}

#[derive(Debug)]
pub struct FindCertificateById {
    pub id: CertificateId,
}

impl Processor<FindCertificateById> for Db {
    type Output = Option<CertificateEntity>;
    type Error = Error;
    #[tracing::instrument(name = "Query:FindCertificateById", skip_all, err)]
    async fn process(&self, input: FindCertificateById) -> Result<Self::Output, Self::Error> {
        Ok(sqlx::query_as("SELECT * FROM certificate WHERE id = $1")
            .bind(input.id)
            .fetch_optional(self.db())
            .await?)
    }
}

/// Every certificate for any of the given SNIs, all directories. Derivation
/// resolves `(sni, acme_directory)` from this in memory.
#[derive(Debug)]
pub struct ListCertificatesBySnis {
    pub snis: Vec<String>,
}

impl Processor<ListCertificatesBySnis> for Db {
    type Output = Vec<CertificateEntity>;
    type Error = Error;
    #[tracing::instrument(name = "Query:ListCertificatesBySnis", skip_all, err)]
    async fn process(&self, input: ListCertificatesBySnis) -> Result<Self::Output, Self::Error> {
        if input.snis.is_empty() {
            return Ok(Vec::new());
        }
        Ok(
            sqlx::query_as("SELECT * FROM certificate WHERE sni = ANY($1)")
                .bind(&input.snis)
                .fetch_all(self.db())
                .await?,
        )
    }
}

/// The rows a config snapshot pins, fetched when the revision is handed to a
/// worker.
#[derive(Debug)]
pub struct ListCertificatesByIds {
    pub ids: Vec<CertificateId>,
}

impl Processor<ListCertificatesByIds> for Db {
    type Output = Vec<CertificateEntity>;
    type Error = Error;
    #[tracing::instrument(name = "Query:ListCertificatesByIds", skip_all, err)]
    async fn process(&self, input: ListCertificatesByIds) -> Result<Self::Output, Self::Error> {
        if input.ids.is_empty() {
            return Ok(Vec::new());
        }
        Ok(
            sqlx::query_as("SELECT * FROM certificate WHERE id = ANY($1)")
                .bind(&input.ids)
                .fetch_all(self.db())
                .await?,
        )
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

impl Processor<ListCertificatesDue> for Db {
    type Output = Vec<CertificateEntity>;
    type Error = Error;
    #[tracing::instrument(name = "Query:ListCertificatesDue", skip_all, err)]
    async fn process(&self, input: ListCertificatesDue) -> Result<Self::Output, Self::Error> {
        Ok(sqlx::query_as(
            "SELECT * FROM certificate WHERE
                 (status IN ('pending', 'failed')
                     AND (last_attempt_at IS NULL OR last_attempt_at < $2))
                 OR (status = 'issued' AND (
                     last_attempt_at IS NULL
                     OR (not_after IS NOT NULL AND not_after < $1 AND last_attempt_at < $2)))
             ORDER BY created_at ASC",
        )
        .bind(input.renew_before)
        .bind(input.retry_before)
        .fetch_all(self.db())
        .await?)
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

impl Processor<ClaimCertificateAttempt> for Db {
    type Output = bool;
    type Error = Error;
    #[tracing::instrument(name = "Query:ClaimCertificateAttempt", skip_all, err, fields(certificate = %input.id))]
    async fn process(&self, input: ClaimCertificateAttempt) -> Result<Self::Output, Self::Error> {
        let claimed: Option<CertificateId> = sqlx::query_scalar(
            "UPDATE certificate SET last_attempt_at = $2
             WHERE id = $1 AND last_attempt_at IS NOT DISTINCT FROM $3
             RETURNING id",
        )
        .bind(input.id)
        .bind(input.now)
        .bind(input.seen_attempt_at)
        .fetch_optional(self.db())
        .await?;
        Ok(claimed.is_some())
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

impl Processor<StoreIssuedCertificate> for Db {
    type Output = CertificateEntity;
    type Error = Error;
    #[tracing::instrument(name = "Query:StoreIssuedCertificate", skip_all, err, fields(certificate = %input.id))]
    async fn process(&self, input: StoreIssuedCertificate) -> Result<Self::Output, Self::Error> {
        Ok(sqlx::query_as(
            "UPDATE certificate SET
                 status = 'issued', acme_account_key = $2, private_key_pem = $3,
                 full_chain_pem = $4, not_before = $5, not_after = $6,
                 last_error = NULL, last_attempt_at = $7, version = version + 1
             WHERE id = $1 RETURNING *",
        )
        .bind(input.id)
        .bind(input.acme_account_key)
        .bind(input.private_key_pem)
        .bind(input.full_chain_pem)
        .bind(input.not_before)
        .bind(input.not_after)
        .bind(input.now)
        .fetch_one(self.db())
        .await?)
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

impl Processor<MarkCertificateAttemptFailed> for Db {
    type Output = ();
    type Error = Error;
    #[tracing::instrument(name = "Query:MarkCertificateAttemptFailed", skip_all, err, fields(certificate = %input.id))]
    async fn process(
        &self,
        input: MarkCertificateAttemptFailed,
    ) -> Result<Self::Output, Self::Error> {
        sqlx::query(
            "UPDATE certificate SET
                 status = CASE WHEN status = 'issued' THEN 'issued' ELSE 'failed' END,
                 last_error = $2, last_attempt_at = $3
             WHERE id = $1",
        )
        .bind(input.id)
        .bind(input.error)
        .bind(input.now)
        .execute(self.db())
        .await?;
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

impl Processor<RetryCertificateRow> for Db {
    /// `None` when the certificate does not exist.
    type Output = Option<CertificateEntity>;
    type Error = Error;
    #[tracing::instrument(name = "Query:RetryCertificateRow", skip_all, err, fields(certificate = %input.id))]
    async fn process(&self, input: RetryCertificateRow) -> Result<Self::Output, Self::Error> {
        Ok(sqlx::query_as(
            "UPDATE certificate SET
                 status = CASE WHEN status = 'issued' THEN 'issued' ELSE 'pending' END,
                 last_error = NULL, last_attempt_at = NULL
             WHERE id = $1 RETURNING *",
        )
        .bind(input.id)
        .fetch_optional(self.db())
        .await?)
    }
}

#[derive(Debug)]
pub struct DeleteCertificateRow {
    pub id: CertificateId,
}

impl Processor<DeleteCertificateRow> for Db {
    type Output = ();
    type Error = Error;
    #[tracing::instrument(name = "Query:DeleteCertificateRow", skip_all, err, fields(certificate = %input.id))]
    async fn process(&self, input: DeleteCertificateRow) -> Result<Self::Output, Self::Error> {
        sqlx::query("DELETE FROM certificate WHERE id = $1")
            .bind(input.id)
            .execute(self.db())
            .await?;
        Ok(())
    }
}

/// The TLS certificates client pods currently ask for, one per pod, with the
/// canvas of each; the cron ensures a certificate row exists for each distinct
/// `(sni, acme_directory)`.
#[derive(Debug)]
pub struct ListTlsRequests;

#[derive(Debug, Clone)]
pub struct TlsRequest {
    pub canvas: CanvasId,
    pub tls: TlsConfig,
}

#[derive(sqlx::FromRow)]
struct TlsRequestRow {
    canvas: CanvasId,
    tls_sni: String,
    tls_dns_provider: crate::entities::db::dns::DnsProviderId,
    tls_domain_id: String,
    tls_acme_directory: String,
}

impl Processor<ListTlsRequests> for Db {
    type Output = Vec<TlsRequest>;
    type Error = Error;
    #[tracing::instrument(name = "Query:ListTlsRequests", skip_all, err)]
    async fn process(&self, _: ListTlsRequests) -> Result<Self::Output, Self::Error> {
        let rows: Vec<TlsRequestRow> = sqlx::query_as(
            "SELECT canvas, tls_sni, tls_dns_provider, tls_domain_id, tls_acme_directory
             FROM orchestration_pod WHERE ingress = 'client_tls' ORDER BY id",
        )
        .fetch_all(self.db())
        .await?;
        Ok(rows
            .into_iter()
            .map(|row| TlsRequest {
                canvas: row.canvas,
                tls: TlsConfig {
                    sni: row.tls_sni,
                    dns_provider: row.tls_dns_provider,
                    domain_id: row.tls_domain_id,
                    acme_directory: row.tls_acme_directory,
                },
            })
            .collect())
    }
}

/// Canvases holding a TLS client pod for the given SNI; issuance and renewal
/// touch them so their servers get a new revision.
#[derive(Debug)]
pub struct ListCanvasesUsingSni {
    pub sni: String,
}

impl Processor<ListCanvasesUsingSni> for Db {
    type Output = Vec<CanvasId>;
    type Error = Error;
    #[tracing::instrument(name = "Query:ListCanvasesUsingSni", skip_all, err, fields(sni = %input.sni))]
    async fn process(&self, input: ListCanvasesUsingSni) -> Result<Self::Output, Self::Error> {
        Ok(sqlx::query_scalar(
            "SELECT DISTINCT canvas FROM orchestration_pod
             WHERE ingress = 'client_tls' AND lower(tls_sni) = $1
             ORDER BY canvas",
        )
        .bind(input.sni)
        .fetch_all(self.db())
        .await?)
    }
}

/// Bumps the generation of the tree each canvas belongs to
/// ([`fence::touch`]), so the derivation hook re-derives its servers.
/// Certificate issuance and relay leaf rotation use it: the config did not
/// change, the material it pins did.
#[derive(Debug)]
pub struct TouchCanvases {
    pub canvases: Vec<CanvasId>,
}

impl Processor<TouchCanvases> for Db {
    type Output = ();
    type Error = Error;
    #[tracing::instrument(name = "Query-Transaction:TouchCanvases", skip_all, err)]
    async fn process(&self, input: TouchCanvases) -> Result<Self::Output, Self::Error> {
        if input.canvases.is_empty() {
            return Ok(());
        }
        let mut tx = self.db().begin().await?;
        for canvas in &input.canvases {
            fence::touch(&mut tx, canvas).await?;
        }
        tx.commit().await?;
        Ok(())
    }
}
