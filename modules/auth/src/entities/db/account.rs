use base::db::{Db, Error};
use db_types::{table_record, text_enum};
use kanau::processor::Processor;

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

impl<'a> Processor<FindAccountByEmail<'a>> for Db {
    type Output = Option<AccountEntity>;
    type Error = Error;
    #[tracing::instrument(name = "Query:FindAccountByEmail", skip_all, err)]
    async fn process(&self, input: FindAccountByEmail<'a>) -> Result<Self::Output, Self::Error> {
        Ok(sqlx::query_as!(
            AccountEntity,
            r#"SELECT id AS "id: AccountId", email, password_hash, role AS "role: AccountRole"
               FROM auth_account WHERE email = $1"#,
            input.email
        )
        .fetch_optional(self.db())
        .await?)
    }
}

pub struct CreateAccount {
    pub email: String,
    pub password_hash: String,
    pub role: AccountRole,
}

impl Processor<CreateAccount> for Db {
    /// `None` when another account already has the email; nothing is written then.
    type Output = Option<AccountEntity>;
    type Error = Error;
    #[tracing::instrument(name = "Query:CreateAccount", skip_all, err)]
    async fn process(&self, input: CreateAccount) -> Result<Self::Output, Self::Error> {
        Ok(sqlx::query_as!(
            AccountEntity,
            r#"INSERT INTO auth_account (id, email, password_hash, role)
               VALUES ($1, $2, $3, $4)
               ON CONFLICT (email) DO NOTHING
               RETURNING id AS "id: AccountId", email, password_hash, role AS "role: AccountRole""#,
            AccountId::new() as _,
            input.email,
            input.password_hash,
            input.role as _
        )
        .fetch_optional(self.db())
        .await?)
    }
}

pub struct UpdateAccountPassword {
    pub id: AccountId,
    pub password_hash: String,
}

impl Processor<UpdateAccountPassword> for Db {
    type Output = ();
    type Error = Error;
    #[tracing::instrument(name = "Query:UpdateAccountPassword", skip_all, err)]
    async fn process(&self, input: UpdateAccountPassword) -> Result<Self::Output, Self::Error> {
        sqlx::query!(
            "UPDATE auth_account SET password_hash = $2 WHERE id = $1",
            input.id as _,
            input.password_hash
        )
        .execute(self.db())
        .await?;
        Ok(())
    }
}

pub struct UpdateAccountEmail {
    pub id: AccountId,
    pub new_email: String,
}

impl Processor<UpdateAccountEmail> for Db {
    /// `false` when another account already has the address; nothing is written then.
    type Output = bool;
    type Error = Error;
    #[tracing::instrument(name = "Query:UpdateAccountEmail", skip_all, err)]
    async fn process(&self, input: UpdateAccountEmail) -> Result<Self::Output, Self::Error> {
        match sqlx::query!(
            "UPDATE auth_account SET email = $2 WHERE id = $1",
            input.id as _,
            input.new_email
        )
        .execute(self.db())
        .await
        .map_err(Error::from)
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

impl Processor<FindAccountById> for Db {
    type Output = Option<AccountEntity>;
    type Error = Error;
    #[tracing::instrument(name = "Query:FindAccountById", skip_all, err)]
    async fn process(&self, input: FindAccountById) -> Result<Self::Output, Self::Error> {
        Ok(sqlx::query_as!(
            AccountEntity,
            r#"SELECT id AS "id: AccountId", email, password_hash, role AS "role: AccountRole"
               FROM auth_account WHERE id = $1"#,
            input.id as _
        )
        .fetch_optional(self.db())
        .await?)
    }
}

pub struct ListAccounts;

impl Processor<ListAccounts> for Db {
    type Output = Vec<AccountEntity>;
    type Error = Error;
    #[tracing::instrument(name = "Query:ListAccounts", skip_all, err)]
    async fn process(&self, _input: ListAccounts) -> Result<Self::Output, Self::Error> {
        Ok(sqlx::query_as!(
            AccountEntity,
            r#"SELECT id AS "id: AccountId", email, password_hash, role AS "role: AccountRole"
               FROM auth_account ORDER BY email"#
        )
        .fetch_all(self.db())
        .await?)
    }
}

pub struct UpdateAccountRole {
    pub id: AccountId,
    pub role: AccountRole,
}

impl Processor<UpdateAccountRole> for Db {
    /// `false` when no account has the id.
    type Output = bool;
    type Error = Error;
    #[tracing::instrument(name = "Query:UpdateAccountRole", skip_all, err)]
    async fn process(&self, input: UpdateAccountRole) -> Result<Self::Output, Self::Error> {
        let updated: Option<AccountId> = sqlx::query_scalar!(
            r#"UPDATE auth_account SET role = $2 WHERE id = $1 RETURNING id AS "id: AccountId""#,
            input.id as _,
            input.role as _
        )
        .fetch_optional(self.db())
        .await?;
        Ok(updated.is_some())
    }
}

/// Deleting an account takes its sessions and API keys with it (`ON DELETE CASCADE`).
pub struct DeleteAccount {
    pub id: AccountId,
}

impl Processor<DeleteAccount> for Db {
    /// `false` when no account has the id.
    type Output = bool;
    type Error = Error;
    #[tracing::instrument(name = "Query:DeleteAccount", skip_all, err)]
    async fn process(&self, input: DeleteAccount) -> Result<Self::Output, Self::Error> {
        let deleted: Option<AccountId> = sqlx::query_scalar!(
            r#"DELETE FROM auth_account WHERE id = $1 RETURNING id AS "id: AccountId""#,
            input.id as _
        )
        .fetch_optional(self.db())
        .await?;
        Ok(deleted.is_some())
    }
}
