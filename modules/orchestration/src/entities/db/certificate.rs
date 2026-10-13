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
use crate::entities::db::tables::{Certificate, OrchestrationCanvas, OrchestrationPod};
use base::db::{Db, Error};
use db_types::{table_record, text_enum};
use kanau::processor::Processor;
use sqlx::{PgConnection, Postgres};
use time::OffsetDateTime;
use wakuwaku::entity::sqlx::backend::PgSource;
use wakuwaku::entity::sqlx::transaction::{Either, Tx, TxEnd, TxFlow, TxState};
use wakuwaku::entity::{Execute, Query, Read, Write};

table_record!(CertificateId, "certificate");

#[derive(Debug, Clone)]
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
    pub not_before: Option<OffsetDateTime>,
    pub not_after: Option<OffsetDateTime>,
    pub last_error: Option<String>,
    pub last_attempt_at: Option<OffsetDateTime>,
    pub version: i64,
    pub created_at: OffsetDateTime,
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
    pub now: OffsetDateTime,
}

impl Query for EnsureCertificate {
    type Effect = Write<Certificate>;
}

impl Execute<PgSource> for EnsureCertificate {
    type Output = CertificateEntity;
    type Error = Error;
    #[tracing::instrument(name = "Query:EnsureCertificate", skip_all, err, fields(sni = %self.sni))]
    async fn execute(self, conn: &mut PgConnection) -> Result<Self::Output, Self::Error> {
        // `DO NOTHING` returns no row when the pair exists; a second statement
        // then reads the one that won, whether it was ours or a concurrent
        // pod's.
        let created = sqlx::query_file_as!(
            CertificateEntity,
            "sql/ensure_certificate_insert.sql",
            CertificateId::new() as _,
            self.sni,
            self.dns_provider as _,
            self.domain_id,
            self.acme_directory,
            self.now
        )
        .fetch_optional(&mut *conn)
        .await?;
        if let Some(row) = created {
            return Ok(row);
        }
        Ok(sqlx::query_file_as!(
            CertificateEntity,
            "sql/ensure_certificate_find.sql",
            self.sni,
            self.acme_directory
        )
        .fetch_one(conn)
        .await?)
    }
}

#[derive(Debug)]
pub struct ListCertificates;

impl Query for ListCertificates {
    type Effect = Read<Certificate>;
}

impl Execute<PgSource> for ListCertificates {
    type Output = Vec<CertificateEntity>;
    type Error = Error;
    #[tracing::instrument(name = "Query:ListCertificates", skip_all, err)]
    async fn execute(self, conn: &mut PgConnection) -> Result<Self::Output, Self::Error> {
        Ok(
            sqlx::query_file_as!(CertificateEntity, "sql/list_certificates.sql")
                .fetch_all(conn)
                .await?,
        )
    }
}

#[derive(Debug)]
pub struct FindCertificateById {
    pub id: CertificateId,
}

impl Query for FindCertificateById {
    type Effect = Read<Certificate>;
}

impl Execute<PgSource> for FindCertificateById {
    type Output = Option<CertificateEntity>;
    type Error = Error;
    #[tracing::instrument(name = "Query:FindCertificateById", skip_all, err)]
    async fn execute(self, conn: &mut PgConnection) -> Result<Self::Output, Self::Error> {
        Ok(sqlx::query_file_as!(
            CertificateEntity,
            "sql/find_certificate_by_id.sql",
            self.id as _
        )
        .fetch_optional(conn)
        .await?)
    }
}

/// Every certificate for any of the given SNIs, all directories. Derivation
/// resolves `(sni, acme_directory)` from this in memory.
#[derive(Debug)]
pub struct ListCertificatesBySnis {
    pub snis: Vec<String>,
}

impl Query for ListCertificatesBySnis {
    type Effect = Read<Certificate>;
}

impl Execute<PgSource> for ListCertificatesBySnis {
    type Output = Vec<CertificateEntity>;
    type Error = Error;
    #[tracing::instrument(name = "Query:ListCertificatesBySnis", skip_all, err)]
    async fn execute(self, conn: &mut PgConnection) -> Result<Self::Output, Self::Error> {
        if self.snis.is_empty() {
            return Ok(Vec::new());
        }
        Ok(sqlx::query_file_as!(
            CertificateEntity,
            "sql/list_certificates_by_snis.sql",
            &self.snis
        )
        .fetch_all(conn)
        .await?)
    }
}

/// The rows a config snapshot pins, fetched when the revision is handed to a
/// worker.
#[derive(Debug)]
pub struct ListCertificatesByIds {
    pub ids: Vec<CertificateId>,
}

impl Query for ListCertificatesByIds {
    type Effect = Read<Certificate>;
}

impl Execute<PgSource> for ListCertificatesByIds {
    type Output = Vec<CertificateEntity>;
    type Error = Error;
    #[tracing::instrument(name = "Query:ListCertificatesByIds", skip_all, err)]
    async fn execute(self, conn: &mut PgConnection) -> Result<Self::Output, Self::Error> {
        if self.ids.is_empty() {
            return Ok(Vec::new());
        }
        Ok(sqlx::query_file_as!(
            CertificateEntity,
            "sql/list_certificates_by_ids.sql",
            &self.ids as _
        )
        .fetch_all(conn)
        .await?)
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
    pub renew_before: OffsetDateTime,
    pub retry_before: OffsetDateTime,
}

impl Query for ListCertificatesDue {
    type Effect = Read<Certificate>;
}

impl Execute<PgSource> for ListCertificatesDue {
    type Output = Vec<CertificateEntity>;
    type Error = Error;
    #[tracing::instrument(name = "Query:ListCertificatesDue", skip_all, err)]
    async fn execute(self, conn: &mut PgConnection) -> Result<Self::Output, Self::Error> {
        Ok(sqlx::query_file_as!(
            CertificateEntity,
            "sql/list_certificates_due.sql",
            self.renew_before,
            self.retry_before
        )
        .fetch_all(conn)
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
    pub now: OffsetDateTime,
    /// `last_attempt_at` as the pass read it; the claim is refused when anything
    /// touched the row since, which is what makes a stale listing harmless.
    pub seen_attempt_at: Option<OffsetDateTime>,
}

impl Query for ClaimCertificateAttempt {
    type Effect = Write<Certificate>;
}

impl Execute<PgSource> for ClaimCertificateAttempt {
    type Output = bool;
    type Error = Error;
    #[tracing::instrument(name = "Query:ClaimCertificateAttempt", skip_all, err, fields(certificate = %self.id))]
    async fn execute(self, conn: &mut PgConnection) -> Result<Self::Output, Self::Error> {
        let claimed = sqlx::query_scalar!(
            r#"UPDATE certificate SET last_attempt_at = $2
               WHERE id = $1 AND last_attempt_at IS NOT DISTINCT FROM $3
               RETURNING id AS "id: CertificateId""#,
            self.id as _,
            self.now,
            self.seen_attempt_at
        )
        .fetch_optional(conn)
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
    pub not_before: OffsetDateTime,
    pub not_after: OffsetDateTime,
    pub now: OffsetDateTime,
}

impl Query for StoreIssuedCertificate {
    type Effect = Write<Certificate>;
}

impl Execute<PgSource> for StoreIssuedCertificate {
    type Output = CertificateEntity;
    type Error = Error;
    #[tracing::instrument(name = "Query:StoreIssuedCertificate", skip_all, err, fields(certificate = %self.id))]
    async fn execute(self, conn: &mut PgConnection) -> Result<Self::Output, Self::Error> {
        Ok(sqlx::query_file_as!(
            CertificateEntity,
            "sql/store_issued_certificate.sql",
            self.id as _,
            self.acme_account_key,
            self.private_key_pem,
            self.full_chain_pem,
            self.not_before,
            self.not_after,
            self.now
        )
        .fetch_one(conn)
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
    pub now: OffsetDateTime,
}

impl Query for MarkCertificateAttemptFailed {
    type Effect = Write<Certificate>;
}

impl Execute<PgSource> for MarkCertificateAttemptFailed {
    type Output = ();
    type Error = Error;
    #[tracing::instrument(name = "Query:MarkCertificateAttemptFailed", skip_all, err, fields(certificate = %self.id))]
    async fn execute(self, conn: &mut PgConnection) -> Result<Self::Output, Self::Error> {
        sqlx::query!(
            "UPDATE certificate SET
                 status = CASE WHEN status = 'issued' THEN 'issued' ELSE 'failed' END,
                 last_error = $2, last_attempt_at = $3
             WHERE id = $1",
            self.id as _,
            self.error,
            self.now
        )
        .execute(conn)
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

impl Query for RetryCertificateRow {
    type Effect = Write<Certificate>;
}

impl Execute<PgSource> for RetryCertificateRow {
    /// `None` when the certificate does not exist.
    type Output = Option<CertificateEntity>;
    type Error = Error;
    #[tracing::instrument(name = "Query:RetryCertificateRow", skip_all, err, fields(certificate = %self.id))]
    async fn execute(self, conn: &mut PgConnection) -> Result<Self::Output, Self::Error> {
        Ok(sqlx::query_file_as!(
            CertificateEntity,
            "sql/retry_certificate_row.sql",
            self.id as _
        )
        .fetch_optional(conn)
        .await?)
    }
}

#[derive(Debug)]
pub struct DeleteCertificateRow {
    pub id: CertificateId,
}

impl Query for DeleteCertificateRow {
    type Effect = Write<Certificate>;
}

impl Execute<PgSource> for DeleteCertificateRow {
    type Output = ();
    type Error = Error;
    #[tracing::instrument(name = "Query:DeleteCertificateRow", skip_all, err, fields(certificate = %self.id))]
    async fn execute(self, conn: &mut PgConnection) -> Result<Self::Output, Self::Error> {
        sqlx::query!("DELETE FROM certificate WHERE id = $1", self.id as _)
            .execute(conn)
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

struct TlsRequestRow {
    canvas: CanvasId,
    tls_sni: String,
    tls_dns_provider: crate::entities::db::dns::DnsProviderId,
    tls_domain_id: String,
    tls_acme_directory: String,
}

impl Query for ListTlsRequests {
    type Effect = Read<OrchestrationPod>;
}

impl Execute<PgSource> for ListTlsRequests {
    type Output = Vec<TlsRequest>;
    type Error = Error;
    #[tracing::instrument(name = "Query:ListTlsRequests", skip_all, err)]
    async fn execute(self, conn: &mut PgConnection) -> Result<Self::Output, Self::Error> {
        let rows = sqlx::query_file_as!(TlsRequestRow, "sql/list_tls_requests.sql")
            .fetch_all(conn)
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

impl Query for ListCanvasesUsingSni {
    type Effect = Read<OrchestrationPod>;
}

impl Execute<PgSource> for ListCanvasesUsingSni {
    type Output = Vec<CanvasId>;
    type Error = Error;
    #[tracing::instrument(name = "Query:ListCanvasesUsingSni", skip_all, err, fields(sni = %self.sni))]
    async fn execute(self, conn: &mut PgConnection) -> Result<Self::Output, Self::Error> {
        Ok(sqlx::query_scalar!(
            r#"SELECT DISTINCT canvas AS "canvas: CanvasId" FROM orchestration_pod
               WHERE ingress = 'client_tls' AND lower(tls_sni) = $1
               ORDER BY canvas"#,
            self.sni
        )
        .fetch_all(conn)
        .await?)
    }
}

/// Bumps the generation of the tree each canvas belongs to
/// ([`fence::Touch`]), so the derivation hook re-derives its servers.
/// Certificate issuance and relay leaf rotation use it: the config did not
/// change, the material it pins did.
#[derive(Debug)]
pub struct TouchCanvases {
    pub canvases: Vec<CanvasId>,
}

enum TouchCanvasesFlow {
    Start(TouchCanvases),
    End(TxEnd<TouchCanvasesFlow>),
}

impl Query for TouchCanvasesFlow {
    type Effect = Write<OrchestrationCanvas>;
}

impl From<TxEnd<TouchCanvasesFlow>> for TouchCanvasesFlow {
    fn from(end: TxEnd<TouchCanvasesFlow>) -> Self {
        TouchCanvasesFlow::End(end)
    }
}

impl TxFlow for TouchCanvasesFlow {
    type Database = Postgres;
    type Commit = ();
    type Abort = Result<(), Error>;
    type Report = ();

    async fn step(tx: Tx<'_, Self>, state: Self) -> TxState<'_, Self> {
        let TouchCanvasesFlow::Start(input) = state else {
            return tx.end(state).await;
        };
        let mut tx = tx;
        for canvas in &input.canvases {
            let (next, r) = tx.run(fence::Touch { canvas }).await;
            tx = next;
            if let Err(e) = r {
                return base::db::rollback(tx, e).await;
            }
        }
        base::db::commit(tx, ()).await
    }

    fn finished(self) -> Either<Self, TxEnd<Self>> {
        match self {
            TouchCanvasesFlow::End(end) => Either::Right(end),
            state => Either::Left(state),
        }
    }
}

impl Processor<TouchCanvases> for Db {
    type Output = ();
    type Error = Error;
    #[tracing::instrument(name = "Query-Transaction:TouchCanvases", skip_all, err)]
    async fn process(&self, input: TouchCanvases) -> Result<Self::Output, Self::Error> {
        if input.canvases.is_empty() {
            return Ok(());
        }
        base::db::transact(self, TouchCanvasesFlow::Start(input)).await
    }
}
