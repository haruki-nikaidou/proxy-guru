//! DNS providers: the accounts that answer ACME DNS-01 challenges.
//!
//! `api_secret` is stored encrypted with the master key
//! ([`crate::utils::secret::SecretKey`]) and is never returned by the API; the
//! service layer encrypts before calling [`CreateDnsProvider`] /
//! [`UpdateDnsProvider`] and decrypts only inside the ACME pipeline.

use chrono::{DateTime, Utc};
use kanau::processor::Processor;
use newtype_record_id::table_record;
use surrealdb_types::SurrealValue;
use wakuwaku::surreal::SurrealProcessor;

table_record!(DnsProviderId, "dns_provider");

#[derive(Debug, Clone, SurrealValue)]
pub struct DnsProviderEntity {
    pub id: DnsProviderId,
    pub name: String,
    pub provider: DnsProvider,
    /// Provider specific: unused for Cloudflare (the zone id lives on the Entry's
    /// `TlsConfig.domain_id`), the team id for Vercel (empty for a personal
    /// account).
    pub account_id: String,
    /// Encrypted (`enc1:...`).
    pub api_secret: String,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, SurrealValue)]
#[surreal(untagged, rename_all = "snake_case")]
pub enum DnsProvider {
    Cloudflare,
    Vercel,
}

/// `api_secret` already encrypted.
#[derive(Debug)]
pub struct CreateDnsProvider {
    pub name: String,
    pub provider: DnsProvider,
    pub account_id: String,
    pub api_secret: String,
    pub now: DateTime<Utc>,
}

impl Processor<CreateDnsProvider> for SurrealProcessor {
    type Output = DnsProviderEntity;
    type Error = surrealdb::Error;
    #[tracing::instrument(name = "Query:CreateDnsProvider", skip_all, err)]
    async fn process(&self, input: CreateDnsProvider) -> Result<Self::Output, Self::Error> {
        let mut resp = self
            .db()
            .query(
                "CREATE ONLY dns_provider CONTENT {
                    name: $name, provider: $provider, account_id: $account_id,
                    api_secret: $api_secret, created_at: $now
                }",
            )
            .bind(("name", input.name))
            .bind(("provider", input.provider))
            .bind(("account_id", input.account_id))
            .bind(("api_secret", input.api_secret))
            .bind(("now", input.now))
            .await?;
        resp.take::<Option<DnsProviderEntity>>(0)?.ok_or_else(|| {
            surrealdb::Error::internal("create dns provider returned no row".to_string())
        })
    }
}

#[derive(Debug)]
pub struct ListDnsProviders;

impl Processor<ListDnsProviders> for SurrealProcessor {
    type Output = Vec<DnsProviderEntity>;
    type Error = surrealdb::Error;
    #[tracing::instrument(name = "Query:ListDnsProviders", skip_all, err)]
    async fn process(&self, _: ListDnsProviders) -> Result<Self::Output, Self::Error> {
        let mut resp = self
            .db()
            .query("SELECT * FROM dns_provider ORDER BY created_at ASC")
            .await?;
        resp.take::<Vec<DnsProviderEntity>>(0)
    }
}

#[derive(Debug)]
pub struct FindDnsProviderById {
    pub id: DnsProviderId,
}

impl Processor<FindDnsProviderById> for SurrealProcessor {
    type Output = Option<DnsProviderEntity>;
    type Error = surrealdb::Error;
    #[tracing::instrument(name = "Query:FindDnsProviderById", skip_all, err)]
    async fn process(&self, input: FindDnsProviderById) -> Result<Self::Output, Self::Error> {
        let mut resp = self
            .db()
            .query("SELECT * FROM $id")
            .bind(("id", input.id))
            .await?;
        resp.take::<Option<DnsProviderEntity>>(0)
    }
}

/// `api_secret: None` keeps the stored secret.
#[derive(Debug)]
pub struct UpdateDnsProvider {
    pub id: DnsProviderId,
    pub name: String,
    pub account_id: String,
    pub api_secret: Option<String>,
}

impl Processor<UpdateDnsProvider> for SurrealProcessor {
    /// `None` when the provider does not exist.
    type Output = Option<DnsProviderEntity>;
    type Error = surrealdb::Error;
    #[tracing::instrument(name = "Query:UpdateDnsProvider", skip_all, err)]
    async fn process(&self, input: UpdateDnsProvider) -> Result<Self::Output, Self::Error> {
        let mut resp = self
            .db()
            .query(
                "UPDATE $id SET
                    name = $name,
                    account_id = $account_id,
                    api_secret = IF $api_secret != NONE { $api_secret } ELSE { api_secret }
                 RETURN AFTER",
            )
            .bind(("id", input.id))
            .bind(("name", input.name))
            .bind(("account_id", input.account_id))
            .bind(("api_secret", input.api_secret))
            .await?;
        resp.take::<Option<DnsProviderEntity>>(0)
    }
}

/// Fails (returns `false`) while an Entry still references the provider.
#[derive(Debug)]
pub struct DeleteDnsProviderRow {
    pub id: DnsProviderId,
}

impl Processor<DeleteDnsProviderRow> for SurrealProcessor {
    /// `false` when an Entry still references the provider; nothing is written then.
    type Output = bool;
    type Error = surrealdb::Error;
    #[tracing::instrument(name = "Query-Transaction:DeleteDnsProviderRow", skip_all, err)]
    async fn process(&self, input: DeleteDnsProviderRow) -> Result<Self::Output, Self::Error> {
        // Statement 0 is BEGIN; the RETURN below is statement 3.
        let mut resp = self
            .db()
            .query(
                "BEGIN TRANSACTION;
                 LET $used = (SELECT VALUE id FROM orchestration_node WHERE spec.config.tls.dns_provider = $id);
                 IF array::len($used) == 0 { DELETE $id };
                 RETURN array::len($used) == 0;
                 COMMIT TRANSACTION;",
            )
            .bind(("id", input.id))
            .await?;
        Ok(resp.take::<Option<bool>>(3)?.unwrap_or(false))
    }
}

/// How many Entry nodes reference the provider through `spec.config.tls`.
#[derive(Debug)]
pub struct CountNodesUsingDnsProvider {
    pub id: DnsProviderId,
}

impl Processor<CountNodesUsingDnsProvider> for SurrealProcessor {
    type Output = i64;
    type Error = surrealdb::Error;
    #[tracing::instrument(name = "Query:CountNodesUsingDnsProvider", skip_all, err)]
    async fn process(
        &self,
        input: CountNodesUsingDnsProvider,
    ) -> Result<Self::Output, Self::Error> {
        let mut resp = self
            .db()
            .query(
                "RETURN array::len(SELECT VALUE id FROM orchestration_node WHERE spec.config.tls.dns_provider = $id)",
            )
            .bind(("id", input.id))
            .await?;
        Ok(resp.take::<Option<i64>>(0)?.unwrap_or(0))
    }
}
