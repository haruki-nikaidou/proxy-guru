use crate::entities::db::account::AccountId;
use crate::entities::db::tables;
use crate::utils::validation::ApiKeyName;
use db_types::table_record;
use sqlx::PgConnection;
use time::OffsetDateTime;
use wakuwaku::entity::sqlx::backend::PgSource;
use wakuwaku::entity::{Execute, Query, Read, Write};
use wakuwaku::proof::Proven;

table_record!(ApiKeyId, "api_key");

#[derive(Clone)]
pub struct ApiKeyEntity {
    pub id: ApiKeyId,
    pub name: String,
    pub owner: AccountId,
    pub secret_sha256: String,
    pub created_at: OffsetDateTime,
}

pub struct CreateNewApiKey {
    pub name: Proven<String, ApiKeyName>,
    pub owner: AccountId,
    pub secret_sha256: String,
    pub created_at: OffsetDateTime,
}

impl Query for CreateNewApiKey {
    type Effect = Write<tables::ApiKey>;
}

impl Execute<PgSource> for CreateNewApiKey {
    type Output = ApiKeyId;
    type Error = base::db::Error;
    #[tracing::instrument(name = "Query:CreateNewApiKey", skip_all, err)]
    async fn execute(self, conn: &mut PgConnection) -> Result<Self::Output, Self::Error> {
        sqlx::query_scalar!(
            r#"INSERT INTO api_key (id, name, owner, secret_sha256, created_at)
               VALUES ($1, $2, $3, $4, $5) RETURNING id AS "id: ApiKeyId""#,
            ApiKeyId::new() as _,
            self.name.subject(),
            self.owner as _,
            self.secret_sha256,
            self.created_at
        )
        .fetch_one(&mut *conn)
        .await
        .map_err(Into::into)
    }
}

pub struct FindApiKeyByDigest {
    pub secret_sha256: String,
}

impl Query for FindApiKeyByDigest {
    type Effect = Read<tables::ApiKey>;
}

impl Execute<PgSource> for FindApiKeyByDigest {
    type Output = Option<ApiKeyEntity>;
    type Error = base::db::Error;
    #[tracing::instrument(name = "Query:FindApiKeyByDigest", skip_all, err)]
    async fn execute(self, conn: &mut PgConnection) -> Result<Self::Output, Self::Error> {
        sqlx::query_as!(
            ApiKeyEntity,
            r#"SELECT id AS "id: ApiKeyId", name, owner AS "owner: AccountId", secret_sha256, created_at
               FROM api_key WHERE secret_sha256 = $1"#,
            self.secret_sha256
        )
        .fetch_optional(&mut *conn)
        .await
        .map_err(Into::into)
    }
}

pub struct FindApiKeyById {
    pub id: ApiKeyId,
}

impl Query for FindApiKeyById {
    type Effect = Read<tables::ApiKey>;
}

impl Execute<PgSource> for FindApiKeyById {
    type Output = Option<ApiKeyEntity>;
    type Error = base::db::Error;
    #[tracing::instrument(name = "Query:FindApiKeyById", skip_all, err)]
    async fn execute(self, conn: &mut PgConnection) -> Result<Self::Output, Self::Error> {
        sqlx::query_as!(
            ApiKeyEntity,
            r#"SELECT id AS "id: ApiKeyId", name, owner AS "owner: AccountId", secret_sha256, created_at
               FROM api_key WHERE id = $1"#,
            self.id as _
        )
        .fetch_optional(&mut *conn)
        .await
        .map_err(Into::into)
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
    pub created_at: OffsetDateTime,
}

impl Query for ListApiKeysByOwner {
    type Effect = Read<tables::ApiKey>;
}

impl Execute<PgSource> for ListApiKeysByOwner {
    type Output = Vec<ApiKeyOmitSecret>;
    type Error = base::db::Error;
    #[tracing::instrument(name = "Query:ListApiKeysByOwner", skip_all, err)]
    async fn execute(self, conn: &mut PgConnection) -> Result<Self::Output, Self::Error> {
        sqlx::query_as!(
            ApiKeyOmitSecret,
            r#"SELECT id AS "id: ApiKeyId", name, owner AS "owner: AccountId", created_at
               FROM api_key WHERE owner = $1 ORDER BY created_at"#,
            self.owner as _
        )
        .fetch_all(&mut *conn)
        .await
        .map_err(Into::into)
    }
}

pub struct DeleteApiKey {
    pub id: ApiKeyId,
}

impl Query for DeleteApiKey {
    type Effect = Write<tables::ApiKey>;
}

impl Execute<PgSource> for DeleteApiKey {
    type Output = ();
    type Error = base::db::Error;
    #[tracing::instrument(name = "Query:DeleteApiKey", skip_all, err)]
    async fn execute(self, conn: &mut PgConnection) -> Result<Self::Output, Self::Error> {
        sqlx::query!("DELETE FROM api_key WHERE id = $1", self.id as _)
            .execute(&mut *conn)
            .await?;
        Ok(())
    }
}
