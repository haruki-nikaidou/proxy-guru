//! DNS provider accounts: the credentials that answer ACME DNS-01 challenges.
//!
//! The whole CRUD is Admin only. The API token is encrypted with the master key
//! before it reaches the database and is never read back out of this module:
//! listings return a [`DnsProviderSummary`] without it, and only the ACME
//! pipeline decrypts it.

use crate::entities::db::dns::{
    CountPodsUsingDnsProvider, CreateDnsProvider as CreateDnsProviderRow, DeleteDnsProviderRow,
    DnsProvider, DnsProviderEntity, DnsProviderId, FindDnsProviderById,
    ListDnsProviders as ListDnsProvidersRow, UpdateDnsProvider as UpdateDnsProviderRow,
};
use crate::services::OrchestrationError;
use crate::utils::secret::SecretKey;
use auth::entities::db::account::AccountRole;
use auth::services::identity::Identity;
use auth::utils::rbac::Permission;
use base::db::Db;
use chrono::{DateTime, Utc};
use kanau::processor::Processor;

#[derive(Clone)]
pub struct DnsProviderService {
    pub db: Db,
    pub secrets: SecretKey,
}

/// A provider as the API sees it: everything but the secret.
#[derive(Debug, Clone)]
pub struct DnsProviderSummary {
    pub id: DnsProviderId,
    pub name: String,
    pub provider: DnsProvider,
    pub account_id: String,
    pub created_at: DateTime<Utc>,
}

impl From<DnsProviderEntity> for DnsProviderSummary {
    fn from(row: DnsProviderEntity) -> Self {
        Self {
            id: row.id,
            name: row.name,
            provider: row.provider,
            account_id: row.account_id,
            created_at: row.created_at,
        }
    }
}

/// Providers hold credentials that can rewrite public DNS: only an Admin with a
/// human session may touch them.
fn ensure_admin(actor: &Identity) -> Result<(), OrchestrationError> {
    actor.ensure(Permission::EditWorkspace)?;
    if actor.role != AccountRole::Admin {
        return Err(OrchestrationError::PermissionDenied);
    }
    Ok(())
}

fn validated_name(name: String) -> Result<String, OrchestrationError> {
    let name = name.trim().to_string();
    if name.is_empty() {
        return Err(OrchestrationError::Invalid("name must not be empty".into()));
    }
    Ok(name)
}

pub struct CreateDnsProvider {
    pub actor: Identity,
    pub name: String,
    pub provider: DnsProvider,
    pub account_id: String,
    /// Plaintext; encrypted here.
    pub api_secret: String,
}

impl Processor<CreateDnsProvider> for DnsProviderService {
    type Output = DnsProviderSummary;
    type Error = OrchestrationError;
    #[tracing::instrument(name = "Service:CreateDnsProvider", skip_all, err)]
    async fn process(&self, input: CreateDnsProvider) -> Result<Self::Output, Self::Error> {
        ensure_admin(&input.actor)?;
        let name = validated_name(input.name)?;
        if input.api_secret.is_empty() {
            return Err(OrchestrationError::Invalid(
                "api_secret must not be empty".into(),
            ));
        }
        let api_secret = self.secrets.encrypt_str(&input.api_secret)?;
        let row = self
            .db
            .process(CreateDnsProviderRow {
                name,
                provider: input.provider,
                account_id: input.account_id.trim().to_string(),
                api_secret,
                now: Utc::now(),
            })
            .await?;
        Ok(row.into())
    }
}

pub struct ListDnsProviders {
    pub actor: Identity,
}

impl Processor<ListDnsProviders> for DnsProviderService {
    type Output = Vec<DnsProviderSummary>;
    type Error = OrchestrationError;
    #[tracing::instrument(name = "Service:ListDnsProviders", skip_all, err)]
    async fn process(&self, input: ListDnsProviders) -> Result<Self::Output, Self::Error> {
        ensure_admin(&input.actor)?;
        Ok(self
            .db
            .process(ListDnsProvidersRow)
            .await?
            .into_iter()
            .map(DnsProviderSummary::from)
            .collect())
    }
}

/// `api_secret: None` (or empty on the wire) keeps the stored secret.
pub struct UpdateDnsProvider {
    pub actor: Identity,
    pub id: DnsProviderId,
    pub name: String,
    pub account_id: String,
    pub api_secret: Option<String>,
}

impl Processor<UpdateDnsProvider> for DnsProviderService {
    type Output = DnsProviderSummary;
    type Error = OrchestrationError;
    #[tracing::instrument(name = "Service:UpdateDnsProvider", skip_all, err)]
    async fn process(&self, input: UpdateDnsProvider) -> Result<Self::Output, Self::Error> {
        ensure_admin(&input.actor)?;
        let name = validated_name(input.name)?;
        let api_secret = match input.api_secret.filter(|s| !s.is_empty()) {
            Some(plain) => Some(self.secrets.encrypt_str(&plain)?),
            None => None,
        };
        self.db
            .process(FindDnsProviderById {
                id: input.id.clone(),
            })
            .await?
            .ok_or(OrchestrationError::NotFound)?;
        let row = self
            .db
            .process(UpdateDnsProviderRow {
                id: input.id,
                name,
                account_id: input.account_id.trim().to_string(),
                api_secret,
            })
            .await?
            .ok_or(OrchestrationError::NotFound)?;
        Ok(row.into())
    }
}

/// Refused ([`OrchestrationError::Conflict`]) while a TLS client pod still names the
/// provider in its `TlsConfig`: the certificate that pod asked for could never
/// be renewed again.
pub struct DeleteDnsProvider {
    pub actor: Identity,
    pub id: DnsProviderId,
}

impl Processor<DeleteDnsProvider> for DnsProviderService {
    type Output = ();
    type Error = OrchestrationError;
    #[tracing::instrument(name = "Service:DeleteDnsProvider", skip_all, err)]
    async fn process(&self, input: DeleteDnsProvider) -> Result<Self::Output, Self::Error> {
        ensure_admin(&input.actor)?;
        self.db
            .process(FindDnsProviderById {
                id: input.id.clone(),
            })
            .await?
            .ok_or(OrchestrationError::NotFound)?;
        let used = self
            .db
            .process(CountPodsUsingDnsProvider {
                id: input.id.clone(),
            })
            .await?;
        if used > 0 {
            return Err(OrchestrationError::Conflict(format!(
                "dns provider is still used by {used} pod(s)"
            )));
        }
        // The row query re-checks inside its transaction: a pod created between
        // the count and the delete still refuses it.
        if !self
            .db
            .process(DeleteDnsProviderRow { id: input.id })
            .await?
        {
            return Err(OrchestrationError::Conflict(
                "dns provider is still used by a pod".into(),
            ));
        }
        Ok(())
    }
}
