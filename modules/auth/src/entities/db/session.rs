use super::account::AccountId;
use crate::entities::db::tables;
use db_types::table_record;
use sqlx::PgConnection;
use time::OffsetDateTime;
use wakuwaku::entity::sqlx::backend::PgSource;
use wakuwaku::entity::{Execute, Query, Read, Write};

// A session's id is the opaque session token itself.
table_record!(SessionId, "auth_session");

#[derive(Debug, Clone)]
pub struct SessionEntity {
    pub id: SessionId,
    pub account_id: AccountId,
    pub user_agent: String,
    pub created_at: OffsetDateTime,
    pub last_active_at: OffsetDateTime,
}

pub struct FindSessionById {
    pub session_id: String,
}

impl Query for FindSessionById {
    type Effect = Read<tables::AuthSession>;
}

impl Execute<PgSource> for FindSessionById {
    type Output = Option<SessionEntity>;
    type Error = base::db::Error;
    #[tracing::instrument(name = "Query:FindSessionById", skip_all, err)]
    async fn execute(self, conn: &mut PgConnection) -> Result<Self::Output, Self::Error> {
        sqlx::query_as!(
            SessionEntity,
            r#"SELECT id AS "id: SessionId", account_id AS "account_id: AccountId", user_agent,
                      created_at, last_active_at
               FROM auth_session WHERE id = $1"#,
            self.session_id
        )
        .fetch_optional(&mut *conn)
        .await
        .map_err(Into::into)
    }
}

/// Create a session whose id is the opaque session token itself.
pub struct CreateSession {
    pub token: String,
    pub account_id: AccountId,
    pub user_agent: String,
    pub created_at: OffsetDateTime,
    pub last_active_at: OffsetDateTime,
}

impl Query for CreateSession {
    type Effect = Write<tables::AuthSession>;
}

impl Execute<PgSource> for CreateSession {
    type Output = SessionEntity;
    type Error = base::db::Error;
    #[tracing::instrument(name = "Query:CreateSession", skip_all, err)]
    async fn execute(self, conn: &mut PgConnection) -> Result<Self::Output, Self::Error> {
        sqlx::query_as!(
            SessionEntity,
            r#"INSERT INTO auth_session (id, account_id, user_agent, created_at, last_active_at)
               VALUES ($1, $2, $3, $4, $5)
               RETURNING id AS "id: SessionId", account_id AS "account_id: AccountId", user_agent,
                         created_at, last_active_at"#,
            SessionId::from_key(self.token) as _,
            self.account_id as _,
            self.user_agent,
            self.created_at,
            self.last_active_at
        )
        .fetch_one(&mut *conn)
        .await
        .map_err(Into::into)
    }
}

pub struct DeleteSession {
    pub session_id: String,
}

impl Query for DeleteSession {
    type Effect = Write<tables::AuthSession>;
}

impl Execute<PgSource> for DeleteSession {
    type Output = ();
    type Error = base::db::Error;
    #[tracing::instrument(name = "Query:DeleteSession", skip_all, err)]
    async fn execute(self, conn: &mut PgConnection) -> Result<Self::Output, Self::Error> {
        sqlx::query!("DELETE FROM auth_session WHERE id = $1", self.session_id)
            .execute(&mut *conn)
            .await?;
        Ok(())
    }
}

pub struct UpdateSession {
    pub id: String,
    pub last_active_at: OffsetDateTime,
}

impl Query for UpdateSession {
    type Effect = Write<tables::AuthSession>;
}

impl Execute<PgSource> for UpdateSession {
    type Output = ();
    type Error = base::db::Error;
    #[tracing::instrument(name = "Query:UpdateSession", skip_all, err)]
    async fn execute(self, conn: &mut PgConnection) -> Result<Self::Output, Self::Error> {
        sqlx::query!(
            "UPDATE auth_session SET last_active_at = $2 WHERE id = $1",
            self.id,
            self.last_active_at
        )
        .execute(&mut *conn)
        .await?;
        Ok(())
    }
}
