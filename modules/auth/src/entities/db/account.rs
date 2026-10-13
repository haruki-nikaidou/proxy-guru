use crate::entities::db::tables;
use crate::utils::validation::CanonicalEmail;
use db_types::{table_record, text_enum};
use sqlx::PgConnection;
use wakuwaku::entity::sqlx::backend::PgSource;
use wakuwaku::entity::{Execute, Query, Read, Write};
use wakuwaku::proof::Proven;

table_record!(AccountId, "auth_account");

/// The unique constraint on `auth_account.email`. It decides whether an address is
/// taken: a lookup before the write would leave a window in which two writes for
/// one address both pass it.
pub const EMAIL_KEY: &str = "auth_account_email_key";

#[derive(Debug, Clone)]
pub struct AccountEntity {
    pub id: AccountId,
    pub email: String,
    pub password_hash: String,
    pub role: AccountRole,
}

/// Account roles, stored as text (`"admin"` / `"maintainer"` / `"observer"`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AccountRole {
    Admin,
    Maintainer,
    Observer,
}
text_enum!(AccountRole {
    Admin => "admin",
    Maintainer => "maintainer",
    Observer => "observer",
});

pub struct FindAccountByEmail<'a> {
    pub email: &'a str,
}

impl<'a> Query for FindAccountByEmail<'a> {
    type Effect = Read<tables::AuthAccount>;
}

impl<'a> Execute<PgSource> for FindAccountByEmail<'a> {
    type Output = Option<AccountEntity>;
    type Error = base::db::Error;
    #[tracing::instrument(name = "Query:FindAccountByEmail", skip_all, err)]
    async fn execute(self, conn: &mut PgConnection) -> Result<Self::Output, Self::Error> {
        sqlx::query_as!(
            AccountEntity,
            r#"SELECT id AS "id: AccountId", email, password_hash, role AS "role: AccountRole"
               FROM auth_account WHERE email = $1"#,
            self.email
        )
        .fetch_optional(&mut *conn)
        .await
        .map_err(Into::into)
    }
}

pub struct CreateAccount {
    pub email: Proven<String, CanonicalEmail>,
    pub password_hash: String,
    pub role: AccountRole,
}

impl Query for CreateAccount {
    type Effect = Write<tables::AuthAccount>;
}

impl Execute<PgSource> for CreateAccount {
    /// `None` when another account already has the email; nothing is written then.
    type Output = Option<AccountEntity>;
    type Error = base::db::Error;
    #[tracing::instrument(name = "Query:CreateAccount", skip_all, err)]
    async fn execute(self, conn: &mut PgConnection) -> Result<Self::Output, Self::Error> {
        sqlx::query_as!(
            AccountEntity,
            r#"INSERT INTO auth_account (id, email, password_hash, role)
               VALUES ($1, $2, $3, $4)
               ON CONFLICT (email) DO NOTHING
               RETURNING id AS "id: AccountId", email, password_hash, role AS "role: AccountRole""#,
            AccountId::new() as _,
            self.email.subject(),
            self.password_hash,
            self.role as _
        )
        .fetch_optional(&mut *conn)
        .await
        .map_err(Into::into)
    }
}

pub struct UpdateAccountPassword {
    pub id: AccountId,
    pub password_hash: String,
}

impl Query for UpdateAccountPassword {
    type Effect = Write<tables::AuthAccount>;
}

impl Execute<PgSource> for UpdateAccountPassword {
    type Output = ();
    type Error = base::db::Error;
    #[tracing::instrument(name = "Query:UpdateAccountPassword", skip_all, err)]
    async fn execute(self, conn: &mut PgConnection) -> Result<Self::Output, Self::Error> {
        sqlx::query!(
            "UPDATE auth_account SET password_hash = $2 WHERE id = $1",
            self.id as _,
            self.password_hash
        )
        .execute(&mut *conn)
        .await?;
        Ok(())
    }
}

pub struct UpdateAccountEmail {
    pub id: AccountId,
    pub email: Proven<String, CanonicalEmail>,
}

impl Query for UpdateAccountEmail {
    type Effect = Write<tables::AuthAccount>;
}

impl Execute<PgSource> for UpdateAccountEmail {
    /// `false` when another account already has the address; nothing is written then.
    type Output = bool;
    type Error = base::db::Error;
    #[tracing::instrument(name = "Query:UpdateAccountEmail", skip_all, err)]
    async fn execute(self, conn: &mut PgConnection) -> Result<Self::Output, Self::Error> {
        match sqlx::query!(
            "UPDATE auth_account SET email = $2 WHERE id = $1",
            self.id as _,
            self.email.subject()
        )
        .execute(&mut *conn)
        .await
        .map_err(base::db::Error::from)
        {
            Ok(_) => Ok(true),
            Err(error) if error.unique_violation() == Some(EMAIL_KEY) => Ok(false),
            Err(error) => Err(error),
        }
    }
}

pub struct FindAccountById {
    pub id: AccountId,
}

impl Query for FindAccountById {
    type Effect = Read<tables::AuthAccount>;
}

impl Execute<PgSource> for FindAccountById {
    type Output = Option<AccountEntity>;
    type Error = base::db::Error;
    #[tracing::instrument(name = "Query:FindAccountById", skip_all, err)]
    async fn execute(self, conn: &mut PgConnection) -> Result<Self::Output, Self::Error> {
        sqlx::query_as!(
            AccountEntity,
            r#"SELECT id AS "id: AccountId", email, password_hash, role AS "role: AccountRole"
               FROM auth_account WHERE id = $1"#,
            self.id as _
        )
        .fetch_optional(&mut *conn)
        .await
        .map_err(Into::into)
    }
}

pub struct ListAccounts;

impl Query for ListAccounts {
    type Effect = Read<tables::AuthAccount>;
}

impl Execute<PgSource> for ListAccounts {
    type Output = Vec<AccountEntity>;
    type Error = base::db::Error;
    #[tracing::instrument(name = "Query:ListAccounts", skip_all, err)]
    async fn execute(self, conn: &mut PgConnection) -> Result<Self::Output, Self::Error> {
        sqlx::query_as!(
            AccountEntity,
            r#"SELECT id AS "id: AccountId", email, password_hash, role AS "role: AccountRole"
               FROM auth_account ORDER BY email"#
        )
        .fetch_all(&mut *conn)
        .await
        .map_err(Into::into)
    }
}

pub struct UpdateAccountRole {
    pub id: AccountId,
    pub role: AccountRole,
}

impl Query for UpdateAccountRole {
    type Effect = Write<tables::AuthAccount>;
}

impl Execute<PgSource> for UpdateAccountRole {
    /// `false` when no account has the id.
    type Output = bool;
    type Error = base::db::Error;
    #[tracing::instrument(name = "Query:UpdateAccountRole", skip_all, err)]
    async fn execute(self, conn: &mut PgConnection) -> Result<Self::Output, Self::Error> {
        let updated: Option<AccountId> = sqlx::query_scalar!(
            r#"UPDATE auth_account SET role = $2 WHERE id = $1 RETURNING id AS "id: AccountId""#,
            self.id as _,
            self.role as _
        )
        .fetch_optional(&mut *conn)
        .await?;
        Ok(updated.is_some())
    }
}

/// Deleting an account takes its sessions and API keys with it (`ON DELETE CASCADE`).
pub struct DeleteAccount {
    pub id: AccountId,
}

impl Query for DeleteAccount {
    type Effect = Write<tables::AuthAccount>;
}

impl Execute<PgSource> for DeleteAccount {
    /// `false` when no account has the id.
    type Output = bool;
    type Error = base::db::Error;
    #[tracing::instrument(name = "Query:DeleteAccount", skip_all, err)]
    async fn execute(self, conn: &mut PgConnection) -> Result<Self::Output, Self::Error> {
        let deleted: Option<AccountId> = sqlx::query_scalar!(
            r#"DELETE FROM auth_account WHERE id = $1 RETURNING id AS "id: AccountId""#,
            self.id as _
        )
        .fetch_optional(&mut *conn)
        .await?;
        Ok(deleted.is_some())
    }
}
