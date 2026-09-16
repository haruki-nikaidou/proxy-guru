use base::db::{Db, Error};
use db_types::{table_record, text_enum};
use kanau::processor::Processor;

table_record!(AccountId, "auth_account");

#[derive(Debug, Clone, sqlx::FromRow)]
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
        Ok(
            sqlx::query_as("SELECT * FROM auth_account WHERE email = $1")
                .bind(input.email)
                .fetch_optional(self.db())
                .await?,
        )
    }
}

pub struct CreateAccount {
    pub email: String,
    pub password_hash: String,
    pub role: AccountRole,
}

impl Processor<CreateAccount> for Db {
    type Output = AccountEntity;
    type Error = Error;
    #[tracing::instrument(name = "Query:CreateAccount", skip_all, err)]
    async fn process(&self, input: CreateAccount) -> Result<Self::Output, Self::Error> {
        Ok(sqlx::query_as(
            "INSERT INTO auth_account (id, email, password_hash, role)
             VALUES ($1, $2, $3, $4) RETURNING *",
        )
        .bind(AccountId::new())
        .bind(input.email)
        .bind(input.password_hash)
        .bind(input.role)
        .fetch_one(self.db())
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
        sqlx::query("UPDATE auth_account SET password_hash = $2 WHERE id = $1")
            .bind(input.id)
            .bind(input.password_hash)
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
    type Output = ();
    type Error = Error;
    #[tracing::instrument(name = "Query:UpdateAccountEmail", skip_all, err)]
    async fn process(&self, input: UpdateAccountEmail) -> Result<Self::Output, Self::Error> {
        sqlx::query("UPDATE auth_account SET email = $2 WHERE id = $1")
            .bind(input.id)
            .bind(input.new_email)
            .execute(self.db())
            .await?;
        Ok(())
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
        Ok(sqlx::query_as("SELECT * FROM auth_account WHERE id = $1")
            .bind(input.id)
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
        Ok(sqlx::query_as("SELECT * FROM auth_account ORDER BY email")
            .fetch_all(self.db())
            .await?)
    }
}

pub struct UpdateAccountRole {
    pub id: AccountId,
    pub role: AccountRole,
}

impl Processor<UpdateAccountRole> for Db {
    type Output = ();
    type Error = Error;
    #[tracing::instrument(name = "Query:UpdateAccountRole", skip_all, err)]
    async fn process(&self, input: UpdateAccountRole) -> Result<Self::Output, Self::Error> {
        sqlx::query("UPDATE auth_account SET role = $2 WHERE id = $1")
            .bind(input.id)
            .bind(input.role)
            .execute(self.db())
            .await?;
        Ok(())
    }
}

/// Deleting an account takes its sessions and API keys with it (`ON DELETE CASCADE`).
pub struct DeleteAccount {
    pub id: AccountId,
}

impl Processor<DeleteAccount> for Db {
    type Output = ();
    type Error = Error;
    #[tracing::instrument(name = "Query:DeleteAccount", skip_all, err)]
    async fn process(&self, input: DeleteAccount) -> Result<Self::Output, Self::Error> {
        sqlx::query("DELETE FROM auth_account WHERE id = $1")
            .bind(input.id)
            .execute(self.db())
            .await?;
        Ok(())
    }
}
