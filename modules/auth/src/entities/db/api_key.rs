use crate::entities::db::account::AccountId;
use base::db::{Db, Error};
use chrono::{DateTime, Utc};
use db_types::table_record;
use kanau::processor::Processor;

table_record!(ApiKeyId, "api_key");

#[derive(Clone)]
pub struct ApiKeyEntity {
    pub id: ApiKeyId,
    pub name: String,
    pub owner: AccountId,
    pub secret_sha256: String,
    pub created_at: DateTime<Utc>,
}

pub struct CreateNewApiKey {
    pub name: String,
    pub owner: AccountId,
    pub secret_sha256: String,
    pub created_at: DateTime<Utc>,
}

impl Processor<CreateNewApiKey> for Db {
    type Output = ApiKeyId;
    type Error = Error;
    #[tracing::instrument(name = "Query:CreateNewApiKey", skip_all, err)]
    async fn process(&self, input: CreateNewApiKey) -> Result<Self::Output, Self::Error> {
        Ok(sqlx::query_scalar!(
            r#"INSERT INTO api_key (id, name, owner, secret_sha256, created_at)
               VALUES ($1, $2, $3, $4, $5) RETURNING id AS "id: ApiKeyId""#,
            ApiKeyId::new() as _,
            input.name,
            input.owner as _,
            input.secret_sha256,
            input.created_at
        )
        .fetch_one(self.db())
        .await?)
    }
}

pub struct FindApiKeyByDigest {
    pub secret_sha256: String,
}

impl Processor<FindApiKeyByDigest> for Db {
    type Output = Option<ApiKeyEntity>;
    type Error = Error;
    #[tracing::instrument(name = "Query:FindApiKeyByDigest", skip_all, err)]
    async fn process(&self, input: FindApiKeyByDigest) -> Result<Self::Output, Self::Error> {
        Ok(sqlx::query_as!(
            ApiKeyEntity,
            r#"SELECT id AS "id: ApiKeyId", name, owner AS "owner: AccountId", secret_sha256, created_at
               FROM api_key WHERE secret_sha256 = $1"#,
            input.secret_sha256
        )
        .fetch_optional(self.db())
        .await?)
    }
}

pub struct FindApiKeyById {
    pub id: ApiKeyId,
}

impl Processor<FindApiKeyById> for Db {
    type Output = Option<ApiKeyEntity>;
    type Error = Error;
    #[tracing::instrument(name = "Query:FindApiKeyById", skip_all, err)]
    async fn process(&self, input: FindApiKeyById) -> Result<Self::Output, Self::Error> {
        Ok(sqlx::query_as!(
            ApiKeyEntity,
            r#"SELECT id AS "id: ApiKeyId", name, owner AS "owner: AccountId", secret_sha256, created_at
               FROM api_key WHERE id = $1"#,
            input.id as _
        )
        .fetch_optional(self.db())
        .await?)
    }
}

pub struct ListApiKeysByOwner {
    pub owner: AccountId,
}

#[derive(Clone)]
pub struct ApiKeyOmitSecret {
    pub id: ApiKeyId,
    pub name: String,
    pub owner: AccountId,
    pub created_at: DateTime<Utc>,
}

impl Processor<ListApiKeysByOwner> for Db {
    type Output = Vec<ApiKeyOmitSecret>;
    type Error = Error;
    #[tracing::instrument(name = "Query:ListApiKeysByOwner", skip_all, err)]
    async fn process(&self, input: ListApiKeysByOwner) -> Result<Self::Output, Self::Error> {
        Ok(sqlx::query_as!(
            ApiKeyOmitSecret,
            r#"SELECT id AS "id: ApiKeyId", name, owner AS "owner: AccountId", created_at
               FROM api_key WHERE owner = $1 ORDER BY created_at"#,
            input.owner as _
        )
        .fetch_all(self.db())
        .await?)
    }
}

pub struct DeleteApiKey {
    pub id: ApiKeyId,
}

impl Processor<DeleteApiKey> for Db {
    type Output = ();
    type Error = Error;
    #[tracing::instrument(name = "Query:DeleteApiKey", skip_all, err)]
    async fn process(&self, input: DeleteApiKey) -> Result<Self::Output, Self::Error> {
        sqlx::query!("DELETE FROM api_key WHERE id = $1", input.id as _)
            .execute(self.db())
            .await?;
        Ok(())
    }
}

pub struct DeleteApiKeysByOwner {
    pub owner: AccountId,
}

impl Processor<DeleteApiKeysByOwner> for Db {
    type Output = ();
    type Error = Error;
    #[tracing::instrument(name = "Query:DeleteApiKeysByOwner", skip_all, err)]
    async fn process(&self, input: DeleteApiKeysByOwner) -> Result<Self::Output, Self::Error> {
        sqlx::query!("DELETE FROM api_key WHERE owner = $1", input.owner as _)
            .execute(self.db())
            .await?;
        Ok(())
    }
}
