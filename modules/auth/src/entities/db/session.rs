use super::account::AccountId;
use base::db::{Db, Error};
use chrono::{DateTime, Utc};
use db_types::table_record;
use kanau::processor::Processor;

// A session's id is the opaque session token itself.
table_record!(SessionId, "auth_session");

#[derive(Debug, Clone, sqlx::FromRow)]
pub struct SessionEntity {
    pub id: SessionId,
    pub account_id: AccountId,
    pub user_agent: String,
    pub created_at: DateTime<Utc>,
    pub last_active_at: DateTime<Utc>,
}

pub struct FindSessionById {
    pub session_id: String,
}

impl Processor<FindSessionById> for Db {
    type Output = Option<SessionEntity>;
    type Error = Error;
    #[tracing::instrument(name = "Query:FindSessionById", skip_all, err)]
    async fn process(&self, input: FindSessionById) -> Result<Self::Output, Self::Error> {
        Ok(sqlx::query_as("SELECT * FROM auth_session WHERE id = $1")
            .bind(input.session_id)
            .fetch_optional(self.db())
            .await?)
    }
}

/// Create a session whose id is the opaque session token itself.
pub struct CreateSession {
    pub token: String,
    pub account_id: AccountId,
    pub user_agent: String,
    pub created_at: DateTime<Utc>,
    pub last_active_at: DateTime<Utc>,
}

impl Processor<CreateSession> for Db {
    type Output = SessionEntity;
    type Error = Error;
    #[tracing::instrument(name = "Query:CreateSession", skip_all, err)]
    async fn process(&self, input: CreateSession) -> Result<Self::Output, Self::Error> {
        Ok(sqlx::query_as(
            "INSERT INTO auth_session (id, account_id, user_agent, created_at, last_active_at)
             VALUES ($1, $2, $3, $4, $5) RETURNING *",
        )
        .bind(SessionId::from_key(input.token))
        .bind(input.account_id)
        .bind(input.user_agent)
        .bind(input.created_at)
        .bind(input.last_active_at)
        .fetch_one(self.db())
        .await?)
    }
}

pub struct DeleteSession {
    pub session_id: String,
}

impl Processor<DeleteSession> for Db {
    type Output = ();
    type Error = Error;
    #[tracing::instrument(name = "Query:DeleteSession", skip_all, err)]
    async fn process(&self, input: DeleteSession) -> Result<Self::Output, Self::Error> {
        sqlx::query("DELETE FROM auth_session WHERE id = $1")
            .bind(input.session_id)
            .execute(self.db())
            .await?;
        Ok(())
    }
}

pub struct UpdateSession {
    pub id: String,
    pub last_active_at: DateTime<Utc>,
}

impl Processor<UpdateSession> for Db {
    type Output = ();
    type Error = Error;
    #[tracing::instrument(name = "Query:UpdateSession", skip_all, err)]
    async fn process(&self, input: UpdateSession) -> Result<Self::Output, Self::Error> {
        sqlx::query("UPDATE auth_session SET last_active_at = $2 WHERE id = $1")
            .bind(input.id)
            .bind(input.last_active_at)
            .execute(self.db())
            .await?;
        Ok(())
    }
}

pub struct DeleteSessionsByAccount {
    pub account_id: AccountId,
}

impl Processor<DeleteSessionsByAccount> for Db {
    type Output = ();
    type Error = Error;
    #[tracing::instrument(name = "Query:DeleteSessionsByAccount", skip_all, err)]
    async fn process(&self, input: DeleteSessionsByAccount) -> Result<Self::Output, Self::Error> {
        sqlx::query("DELETE FROM auth_session WHERE account_id = $1")
            .bind(input.account_id)
            .execute(self.db())
            .await?;
        Ok(())
    }
}
