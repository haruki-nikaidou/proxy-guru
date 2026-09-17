//! The internal CA and the relay leaf certificates it issues.
//!
//! Relay TLS/QUIC links between workers are authenticated by certificates the
//! master signs: one leaf per pod whose listener is a TLS or QUIC relay, with
//! SAN `<pod-key>.relay.guru.internal` ([`relay_sni`]). Workers trust only the
//! CA certificate, which every config revision delivers as `certs/ca.pem`.
//! Private keys are stored encrypted with the master key.

use crate::entities::db::pod::PodId;
use base::db::{Db, Error};
use chrono::{DateTime, Utc};
use db_types::table_record;
use kanau::processor::Processor;

table_record!(InternalCaId, "internal_ca");

/// The key of the single CA row.
pub const INTERNAL_CA_KEY: &str = "master";
pub const RELAY_SNI_SUFFIX: &str = ".relay.guru.internal";

/// The id of the single CA row.
pub fn internal_ca_id() -> InternalCaId {
    InternalCaId::from_key(INTERNAL_CA_KEY)
}

/// The SNI a relay listener presents and its dialers verify.
pub fn relay_sni(pod: &PodId) -> String {
    format!("{pod}{RELAY_SNI_SUFFIX}")
}

#[derive(Debug, Clone)]
pub struct InternalCaEntity {
    pub id: InternalCaId,
    pub certificate_pem: String,
    /// Encrypted.
    pub private_key_pem: String,
    pub not_after: DateTime<Utc>,
    pub created_at: DateTime<Utc>,
}

/// Creates the CA row (`manage-tool orchestration init-ca`). Returns `false`
/// without writing when one already exists: replacing the root would invalidate
/// every relay leaf at once, so there is deliberately no overwrite path.
#[derive(Debug)]
pub struct CreateInternalCa {
    pub certificate_pem: String,
    pub private_key_pem: String,
    pub not_after: DateTime<Utc>,
    pub now: DateTime<Utc>,
}

impl Processor<CreateInternalCa> for Db {
    /// `false` when a CA already exists; nothing is written then.
    type Output = bool;
    type Error = Error;
    #[tracing::instrument(name = "Query:CreateInternalCa", skip_all, err)]
    async fn process(&self, input: CreateInternalCa) -> Result<Self::Output, Self::Error> {
        let created = sqlx::query_scalar!(
            r#"INSERT INTO internal_ca (id, certificate_pem, private_key_pem, not_after, created_at)
               VALUES ($1, $2, $3, $4, $5)
               ON CONFLICT (id) DO NOTHING RETURNING id AS "id: InternalCaId""#,
            internal_ca_id() as _,
            input.certificate_pem,
            input.private_key_pem,
            input.not_after,
            input.now
        )
        .fetch_optional(self.db())
        .await?;
        Ok(created.is_some())
    }
}

#[derive(Debug)]
pub struct FindInternalCa;

impl Processor<FindInternalCa> for Db {
    type Output = Option<InternalCaEntity>;
    type Error = Error;
    #[tracing::instrument(name = "Query:FindInternalCa", skip_all, err)]
    async fn process(&self, _: FindInternalCa) -> Result<Self::Output, Self::Error> {
        Ok(sqlx::query_as!(
            InternalCaEntity,
            r#"SELECT id AS "id: InternalCaId", certificate_pem, private_key_pem,
                      not_after, created_at
               FROM internal_ca WHERE id = $1"#,
            internal_ca_id() as _
        )
        .fetch_optional(self.db())
        .await?)
    }
}

table_record!(RelayCertificateId, "relay_certificate");

#[derive(Debug, Clone)]
pub struct RelayCertificateEntity {
    pub id: RelayCertificateId,
    pub pod: PodId,
    pub sni: String,
    /// Encrypted.
    pub private_key_pem: String,
    pub certificate_pem: String,
    pub not_before: DateTime<Utc>,
    pub not_after: DateTime<Utc>,
    pub version: i64,
}

/// Creates the leaf for a pod, or replaces it and bumps `version`.
///
/// `expected_version` fences every path that replaces a leaf: the rotation cron
/// (whose signal may be delivered more than once) and the derivation pass that
/// renews an expiring one both read a leaf before signing its successor, and
/// only the writer whose read is still current may store its material and ship
/// a new revision. With `Some(v)` the write lands only while the pod's leaf is
/// still at `version = v` — a lost race (and a leaf that has since been
/// deleted) writes nothing and returns `None`. Only a pod's first issuance
/// passes `None`: it has no prior version to fence against and must create the
/// row; a second concurrent create for the same pod finds the row and replaces it.
#[derive(Debug)]
pub struct StoreRelayCertificate {
    pub pod: PodId,
    pub sni: String,
    pub private_key_pem: String,
    pub certificate_pem: String,
    pub not_before: DateTime<Utc>,
    pub not_after: DateTime<Utc>,
    pub expected_version: Option<i64>,
}

impl Processor<StoreRelayCertificate> for Db {
    /// `None` when the compare-and-set lost.
    type Output = Option<RelayCertificateEntity>;
    type Error = Error;
    #[tracing::instrument(name = "Query:StoreRelayCertificate", skip_all, err)]
    async fn process(&self, input: StoreRelayCertificate) -> Result<Self::Output, Self::Error> {
        let row = match input.expected_version {
            Some(expected) => {
                sqlx::query_file_as!(
                    RelayCertificateEntity,
                    "sql/store_relay_certificate_update.sql",
                    input.pod as _,
                    input.sni,
                    input.private_key_pem,
                    input.certificate_pem,
                    input.not_before,
                    input.not_after,
                    expected
                )
                .fetch_optional(self.db())
                .await?
            }
            None => {
                sqlx::query_file_as!(
                    RelayCertificateEntity,
                    "sql/store_relay_certificate_insert.sql",
                    RelayCertificateId::new() as _,
                    input.pod as _,
                    input.sni,
                    input.private_key_pem,
                    input.certificate_pem,
                    input.not_before,
                    input.not_after
                )
                .fetch_optional(self.db())
                .await?
            }
        };
        Ok(row)
    }
}

#[derive(Debug)]
pub struct ListRelayCertificatesByPods {
    pub pods: Vec<PodId>,
}

impl Processor<ListRelayCertificatesByPods> for Db {
    type Output = Vec<RelayCertificateEntity>;
    type Error = Error;
    #[tracing::instrument(name = "Query:ListRelayCertificatesByPods", skip_all, err)]
    async fn process(
        &self,
        input: ListRelayCertificatesByPods,
    ) -> Result<Self::Output, Self::Error> {
        if input.pods.is_empty() {
            return Ok(Vec::new());
        }
        Ok(sqlx::query_as!(
            RelayCertificateEntity,
            r#"SELECT id AS "id: RelayCertificateId", pod AS "pod: PodId", sni,
                      private_key_pem, certificate_pem, not_before, not_after, version
               FROM relay_certificate WHERE pod = ANY($1)"#,
            &input.pods as _
        )
        .fetch_all(self.db())
        .await?)
    }
}

#[derive(Debug)]
pub struct ListRelayCertificatesByIds {
    pub ids: Vec<RelayCertificateId>,
}

impl Processor<ListRelayCertificatesByIds> for Db {
    type Output = Vec<RelayCertificateEntity>;
    type Error = Error;
    #[tracing::instrument(name = "Query:ListRelayCertificatesByIds", skip_all, err)]
    async fn process(
        &self,
        input: ListRelayCertificatesByIds,
    ) -> Result<Self::Output, Self::Error> {
        if input.ids.is_empty() {
            return Ok(Vec::new());
        }
        Ok(sqlx::query_as!(
            RelayCertificateEntity,
            r#"SELECT id AS "id: RelayCertificateId", pod AS "pod: PodId", sni,
                      private_key_pem, certificate_pem, not_before, not_after, version
               FROM relay_certificate WHERE id = ANY($1)"#,
            &input.ids as _
        )
        .fetch_all(self.db())
        .await?)
    }
}

/// Leaves whose `not_after` is before the cut-off: the rotation cron's input.
#[derive(Debug)]
pub struct ListRelayCertificatesExpiringBefore {
    pub before: DateTime<Utc>,
}

impl Processor<ListRelayCertificatesExpiringBefore> for Db {
    type Output = Vec<RelayCertificateEntity>;
    type Error = Error;
    #[tracing::instrument(name = "Query:ListRelayCertificatesExpiringBefore", skip_all, err)]
    async fn process(
        &self,
        input: ListRelayCertificatesExpiringBefore,
    ) -> Result<Self::Output, Self::Error> {
        Ok(sqlx::query_as!(
            RelayCertificateEntity,
            r#"SELECT id AS "id: RelayCertificateId", pod AS "pod: PodId", sni,
                      private_key_pem, certificate_pem, not_before, not_after, version
               FROM relay_certificate WHERE not_after < $1"#,
            input.before
        )
        .fetch_all(self.db())
        .await?)
    }
}
