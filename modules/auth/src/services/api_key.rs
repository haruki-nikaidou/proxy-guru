//! API-key lifecycle and machine authentication.

use base::db::Db;
use kanau::processor::Processor;
use time::OffsetDateTime;

use crate::entities::db::account::{AccountRole, FindAccountById};
use crate::entities::db::api_key::{
    ApiKeyId, ApiKeyOmitSecret, CreateNewApiKey, DeleteApiKey, FindApiKeyByDigest, FindApiKeyById,
    ListApiKeysByOwner,
};
use crate::services::identity::{Identity, IdentityKind};
use crate::utils::rbac::Permission;
use crate::utils::token::{generate_api_key_secret, sha256_hex};

/// API-key operations.
#[derive(Clone)]
pub struct ApiKeyService {
    pub db: Db,
}

/// Create a new API key owned by the caller.
pub struct CreateApiKey {
    pub actor: Identity,
    pub name: String,
}

/// A freshly created API key. `secret` is the plaintext credential, returned
/// exactly once and never recoverable afterwards (only its digest is stored).
pub struct CreatedApiKey {
    pub id: ApiKeyId,
    pub secret: String,
}

impl Processor<CreateApiKey> for ApiKeyService {
    type Output = CreatedApiKey;
    type Error = wakuwaku::Error;
    #[tracing::instrument(name = "Service:CreateApiKey", skip_all, err)]
    async fn process(&self, input: CreateApiKey) -> Result<Self::Output, Self::Error> {
        input.actor.ensure(Permission::ManageApiKeys)?;
        let secret = generate_api_key_secret();
        let secret_sha256 = sha256_hex(&secret);
        let id = self
            .db
            .process(CreateNewApiKey {
                name: input.name,
                owner: input.actor.account_id,
                secret_sha256,
                created_at: OffsetDateTime::now_utc(),
            })
            .await?;
        Ok(CreatedApiKey { id, secret })
    }
}

/// List the caller's API keys (secrets omitted).
pub struct ListApiKeys {
    pub actor: Identity,
}

impl Processor<ListApiKeys> for ApiKeyService {
    type Output = Vec<ApiKeyOmitSecret>;
    type Error = wakuwaku::Error;
    #[tracing::instrument(name = "Service:ListApiKeys", skip_all, err)]
    async fn process(&self, input: ListApiKeys) -> Result<Self::Output, Self::Error> {
        input.actor.ensure(Permission::ManageApiKeys)?;
        let keys = self
            .db
            .process(ListApiKeysByOwner {
                owner: input.actor.account_id,
            })
            .await?;
        Ok(keys)
    }
}

/// Revoke an API key. The caller must own the key, or be an admin.
pub struct RevokeApiKey {
    pub actor: Identity,
    pub id: ApiKeyId,
}

impl Processor<RevokeApiKey> for ApiKeyService {
    type Output = ();
    type Error = wakuwaku::Error;
    #[tracing::instrument(name = "Service:RevokeApiKey", skip_all, err)]
    async fn process(&self, input: RevokeApiKey) -> Result<Self::Output, Self::Error> {
        input.actor.ensure(Permission::ManageApiKeys)?;
        let key = self
            .db
            .process(FindApiKeyById {
                id: input.id.clone(),
            })
            .await?
            .ok_or(wakuwaku::Error::NotFound)?;
        let owns = key.owner == input.actor.account_id;
        if !owns && input.actor.role != AccountRole::Admin {
            return Err(wakuwaku::Error::PermissionsDenied);
        }
        self.db.process(DeleteApiKey { id: input.id }).await?;
        Ok(())
    }
}

/// Resolve an API-key secret into a machine [`Identity`].
pub struct AuthenticateApiKey {
    pub secret: String,
}

impl Processor<AuthenticateApiKey> for ApiKeyService {
    type Output = Option<Identity>;
    type Error = wakuwaku::Error;
    #[tracing::instrument(name = "Service:AuthenticateApiKey", skip_all, err)]
    async fn process(&self, input: AuthenticateApiKey) -> Result<Self::Output, Self::Error> {
        let digest = sha256_hex(&input.secret);
        let key = match self
            .db
            .process(FindApiKeyByDigest {
                secret_sha256: digest,
            })
            .await?
        {
            Some(key) => key,
            None => return Ok(None),
        };
        // Reload the owner's role every time so a downgraded owner loses access.
        let account = match self
            .db
            .process(FindAccountById {
                id: key.owner.clone(),
            })
            .await?
        {
            Some(account) => account,
            None => return Ok(None),
        };
        // Observers may not use API keys.
        if account.role == AccountRole::Observer {
            return Ok(None);
        }
        Ok(Some(Identity {
            account_id: key.owner,
            role: account.role,
            kind: IdentityKind::ApiKey,
        }))
    }
}
