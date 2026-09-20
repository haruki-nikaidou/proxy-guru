//! DNS providers: the accounts that answer ACME DNS-01 challenges.
//!
//! `api_secret` is stored encrypted with the master key
//! ([`crate::utils::secret::SecretKey`]) and is never returned by the API; the
//! service layer encrypts before calling [`CreateDnsProvider`] /
//! [`UpdateDnsProvider`] and decrypts only inside the ACME pipeline.

use base::db::{Db, Error};
use db_types::{table_record, text_enum};
use kanau::processor::Processor;
use serde::{Deserialize, Serialize};
use time::OffsetDateTime;

table_record!(DnsProviderId, "dns_provider");

#[derive(Debug, Clone)]
pub struct DnsProviderEntity {
    pub id: DnsProviderId,
    pub name: String,
    pub provider: DnsProvider,
    /// Provider specific: unused for Cloudflare (the zone id lives on the pod's
    /// `TlsConfig.domain_id`), the team id for Vercel (empty for a personal
    /// account).
    pub account_id: String,
    /// Encrypted (`enc1:...`).
    pub api_secret: String,
    pub created_at: OffsetDateTime,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DnsProvider {
    Cloudflare,
    Vercel,
}
text_enum!(DnsProvider {
    Cloudflare => "cloudflare",
    Vercel => "vercel",
});

/// `api_secret` already encrypted.
#[derive(Debug)]
pub struct CreateDnsProvider {
    pub name: String,
    pub provider: DnsProvider,
    pub account_id: String,
    pub api_secret: String,
    pub now: OffsetDateTime,
}

impl Processor<CreateDnsProvider> for Db {
    type Output = DnsProviderEntity;
    type Error = Error;
    #[tracing::instrument(name = "Query:CreateDnsProvider", skip_all, err)]
    async fn process(&self, input: CreateDnsProvider) -> Result<Self::Output, Self::Error> {
        Ok(sqlx::query_as!(
            DnsProviderEntity,
            r#"INSERT INTO dns_provider (id, name, provider, account_id, api_secret, created_at)
               VALUES ($1, $2, $3, $4, $5, $6)
               RETURNING id AS "id: DnsProviderId", name, provider AS "provider: DnsProvider",
                         account_id, api_secret, created_at"#,
            DnsProviderId::new() as _,
            input.name,
            input.provider as _,
            input.account_id,
            input.api_secret,
            input.now
        )
        .fetch_one(self.db())
        .await?)
    }
}

#[derive(Debug)]
pub struct ListDnsProviders;

impl Processor<ListDnsProviders> for Db {
    type Output = Vec<DnsProviderEntity>;
    type Error = Error;
    #[tracing::instrument(name = "Query:ListDnsProviders", skip_all, err)]
    async fn process(&self, _: ListDnsProviders) -> Result<Self::Output, Self::Error> {
        Ok(sqlx::query_as!(
            DnsProviderEntity,
            r#"SELECT id AS "id: DnsProviderId", name, provider AS "provider: DnsProvider",
                      account_id, api_secret, created_at
               FROM dns_provider ORDER BY created_at"#
        )
        .fetch_all(self.db())
        .await?)
    }
}

#[derive(Debug)]
pub struct FindDnsProviderById {
    pub id: DnsProviderId,
}

impl Processor<FindDnsProviderById> for Db {
    type Output = Option<DnsProviderEntity>;
    type Error = Error;
    #[tracing::instrument(name = "Query:FindDnsProviderById", skip_all, err)]
    async fn process(&self, input: FindDnsProviderById) -> Result<Self::Output, Self::Error> {
        Ok(sqlx::query_as!(
            DnsProviderEntity,
            r#"SELECT id AS "id: DnsProviderId", name, provider AS "provider: DnsProvider",
                      account_id, api_secret, created_at
               FROM dns_provider WHERE id = $1"#,
            input.id as _
        )
        .fetch_optional(self.db())
        .await?)
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

impl Processor<UpdateDnsProvider> for Db {
    /// `None` when the provider does not exist.
    type Output = Option<DnsProviderEntity>;
    type Error = Error;
    #[tracing::instrument(name = "Query:UpdateDnsProvider", skip_all, err)]
    async fn process(&self, input: UpdateDnsProvider) -> Result<Self::Output, Self::Error> {
        Ok(sqlx::query_as!(
            DnsProviderEntity,
            r#"UPDATE dns_provider
               SET name = $2, account_id = $3, api_secret = COALESCE($4, api_secret)
               WHERE id = $1
               RETURNING id AS "id: DnsProviderId", name, provider AS "provider: DnsProvider",
                         account_id, api_secret, created_at"#,
            input.id as _,
            input.name,
            input.account_id,
            input.api_secret
        )
        .fetch_optional(self.db())
        .await?)
    }
}

/// Fails (returns `false`) while a pod or a certificate still references the
/// provider: both are foreign keys, so the database refuses the delete.
#[derive(Debug)]
pub struct DeleteDnsProviderRow {
    pub id: DnsProviderId,
}

impl Processor<DeleteDnsProviderRow> for Db {
    /// `false` when something still references the provider; nothing is written then.
    type Output = bool;
    type Error = Error;
    #[tracing::instrument(name = "Query:DeleteDnsProviderRow", skip_all, err)]
    async fn process(&self, input: DeleteDnsProviderRow) -> Result<Self::Output, Self::Error> {
        match sqlx::query!("DELETE FROM dns_provider WHERE id = $1", input.id as _)
            .execute(self.db())
            .await
            .map_err(Error::from)
        {
            Ok(_) => Ok(true),
            Err(error) if error.fk_violation().is_some() => Ok(false),
            Err(error) => Err(error),
        }
    }
}

/// How many TLS client pods answer their ACME challenge through the provider.
#[derive(Debug)]
pub struct CountPodsUsingDnsProvider {
    pub id: DnsProviderId,
}

impl Processor<CountPodsUsingDnsProvider> for Db {
    type Output = i64;
    type Error = Error;
    #[tracing::instrument(name = "Query:CountPodsUsingDnsProvider", skip_all, err)]
    async fn process(&self, input: CountPodsUsingDnsProvider) -> Result<Self::Output, Self::Error> {
        Ok(sqlx::query_scalar!(
            r#"SELECT count(*) AS "count!" FROM orchestration_pod WHERE tls_dns_provider = $1"#,
            input.id as _
        )
        .fetch_one(self.db())
        .await?)
    }
}
