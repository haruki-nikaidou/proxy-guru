//! The internal CA and the relay leaf certificates it issues.
//!
//! Relay TLS/QUIC links between workers are authenticated by certificates the
//! master signs: one leaf per pod whose listener is a TLS or QUIC relay, with
//! SAN `<pod-key>.relay.guru.internal` ([`relay_sni`]). Workers trust only the
//! CA certificate, which every config revision delivers as `certs/ca.pem`.
//! Private keys are stored encrypted with the master key.

use crate::entities::surreal::node::NodeId;
use crate::utils::ids::record_key;
use chrono::{DateTime, Utc};
use kanau::processor::Processor;
use newtype_record_id::table_record;
use surrealdb_types::SurrealValue;
use wakuwaku::surreal::SurrealProcessor;

table_record!(InternalCaId, "internal_ca");

/// The record key of the single CA row.
pub const INTERNAL_CA_KEY: &str = "master";
pub const RELAY_SNI_SUFFIX: &str = ".relay.guru.internal";

/// The id of the single CA row.
pub fn internal_ca_id() -> InternalCaId {
    InternalCaId(surrealdb::types::RecordId::new(
        "internal_ca",
        INTERNAL_CA_KEY,
    ))
}

/// The SNI a relay listener presents and its dialers verify.
pub fn relay_sni(pod: &NodeId) -> String {
    format!("{}{RELAY_SNI_SUFFIX}", record_key(&pod.0))
}

#[derive(Debug, Clone, SurrealValue)]
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

impl Processor<CreateInternalCa> for SurrealProcessor {
    /// `false` when a CA already exists; nothing is written then.
    type Output = bool;
    type Error = surrealdb::Error;
    #[tracing::instrument(name = "Query-Transaction:CreateInternalCa", skip_all, err)]
    async fn process(&self, input: CreateInternalCa) -> Result<Self::Output, Self::Error> {
        // Statement 0 is BEGIN, 1 the LET; the RETURN is statement 2.
        let mut resp = self
            .db()
            .query(
                "BEGIN TRANSACTION;
                 LET $created = IF record::exists($id) { [] } ELSE {
                     CREATE $id CONTENT {
                         certificate_pem: $certificate_pem, private_key_pem: $private_key_pem,
                         not_after: $not_after, created_at: $now
                     }
                 };
                 RETURN array::len($created) > 0;
                 COMMIT TRANSACTION;",
            )
            .bind(("id", internal_ca_id()))
            .bind(("certificate_pem", input.certificate_pem))
            .bind(("private_key_pem", input.private_key_pem))
            .bind(("not_after", input.not_after))
            .bind(("now", input.now))
            .await?;
        Ok(resp.take::<Option<bool>>(2)?.unwrap_or(false))
    }
}

#[derive(Debug)]
pub struct FindInternalCa;

impl Processor<FindInternalCa> for SurrealProcessor {
    type Output = Option<InternalCaEntity>;
    type Error = surrealdb::Error;
    #[tracing::instrument(name = "Query:FindInternalCa", skip_all, err)]
    async fn process(&self, _: FindInternalCa) -> Result<Self::Output, Self::Error> {
        let mut resp = self
            .db()
            .query("SELECT * FROM ONLY $id")
            .bind(("id", internal_ca_id()))
            .await?;
        resp.take::<Option<InternalCaEntity>>(0)
    }
}

table_record!(RelayCertificateId, "relay_certificate");

#[derive(Debug, Clone, SurrealValue)]
pub struct RelayCertificateEntity {
    pub id: RelayCertificateId,
    pub pod: NodeId,
    pub sni: String,
    /// Encrypted.
    pub private_key_pem: String,
    pub certificate_pem: String,
    pub not_before: DateTime<Utc>,
    pub not_after: DateTime<Utc>,
    pub version: i64,
}

/// Creates the leaf for a pod, or replaces it and bumps `version` (rotation).
#[derive(Debug)]
pub struct StoreRelayCertificate {
    pub pod: NodeId,
    pub sni: String,
    pub private_key_pem: String,
    pub certificate_pem: String,
    pub not_before: DateTime<Utc>,
    pub not_after: DateTime<Utc>,
}

impl Processor<StoreRelayCertificate> for SurrealProcessor {
    type Output = RelayCertificateEntity;
    type Error = surrealdb::Error;
    #[tracing::instrument(name = "Query-Transaction:StoreRelayCertificate", skip_all, err)]
    async fn process(&self, input: StoreRelayCertificate) -> Result<Self::Output, Self::Error> {
        // Statement 0 is BEGIN, 1-2 the LETs; the RETURN is statement 3.
        let mut resp = self
            .db()
            .query(
                "BEGIN TRANSACTION;
                 LET $existing = (SELECT VALUE id FROM relay_certificate WHERE pod = $pod LIMIT 1)[0];
                 LET $row = IF $existing = NONE {
                     CREATE ONLY relay_certificate CONTENT {
                         pod: $pod, sni: $sni, private_key_pem: $private_key_pem,
                         certificate_pem: $certificate_pem, not_before: $not_before,
                         not_after: $not_after, version: 1
                     }
                 } ELSE {
                     UPDATE ONLY $existing SET
                         sni = $sni, private_key_pem = $private_key_pem,
                         certificate_pem = $certificate_pem, not_before = $not_before,
                         not_after = $not_after, version += 1
                     RETURN AFTER
                 };
                 RETURN $row;
                 COMMIT TRANSACTION;",
            )
            .bind(("pod", input.pod))
            .bind(("sni", input.sni))
            .bind(("private_key_pem", input.private_key_pem))
            .bind(("certificate_pem", input.certificate_pem))
            .bind(("not_before", input.not_before))
            .bind(("not_after", input.not_after))
            .await?;
        resp.take::<Option<RelayCertificateEntity>>(3)?
            .ok_or_else(|| {
                surrealdb::Error::internal("relay certificate row was not written".to_string())
            })
    }
}

#[derive(Debug)]
pub struct ListRelayCertificatesByPods {
    pub pods: Vec<NodeId>,
}

impl Processor<ListRelayCertificatesByPods> for SurrealProcessor {
    type Output = Vec<RelayCertificateEntity>;
    type Error = surrealdb::Error;
    #[tracing::instrument(name = "Query:ListRelayCertificatesByPods", skip_all, err)]
    async fn process(
        &self,
        input: ListRelayCertificatesByPods,
    ) -> Result<Self::Output, Self::Error> {
        if input.pods.is_empty() {
            return Ok(Vec::new());
        }
        let mut resp = self
            .db()
            .query("SELECT * FROM relay_certificate WHERE pod IN $pods")
            .bind(("pods", input.pods))
            .await?;
        resp.take::<Vec<RelayCertificateEntity>>(0)
    }
}

#[derive(Debug)]
pub struct ListRelayCertificatesByIds {
    pub ids: Vec<RelayCertificateId>,
}

impl Processor<ListRelayCertificatesByIds> for SurrealProcessor {
    type Output = Vec<RelayCertificateEntity>;
    type Error = surrealdb::Error;
    #[tracing::instrument(name = "Query:ListRelayCertificatesByIds", skip_all, err)]
    async fn process(
        &self,
        input: ListRelayCertificatesByIds,
    ) -> Result<Self::Output, Self::Error> {
        if input.ids.is_empty() {
            return Ok(Vec::new());
        }
        let mut resp = self
            .db()
            .query("SELECT * FROM relay_certificate WHERE id IN $ids")
            .bind(("ids", input.ids))
            .await?;
        resp.take::<Vec<RelayCertificateEntity>>(0)
    }
}

/// Leaves whose `not_after` is before the cut-off: the rotation cron's input.
#[derive(Debug)]
pub struct ListRelayCertificatesExpiringBefore {
    pub before: DateTime<Utc>,
}

impl Processor<ListRelayCertificatesExpiringBefore> for SurrealProcessor {
    type Output = Vec<RelayCertificateEntity>;
    type Error = surrealdb::Error;
    #[tracing::instrument(name = "Query:ListRelayCertificatesExpiringBefore", skip_all, err)]
    async fn process(
        &self,
        input: ListRelayCertificatesExpiringBefore,
    ) -> Result<Self::Output, Self::Error> {
        let mut resp = self
            .db()
            .query("SELECT * FROM relay_certificate WHERE not_after < $before")
            .bind(("before", input.before))
            .await?;
        resp.take::<Vec<RelayCertificateEntity>>(0)
    }
}
