//! Account management and self-service operations.

use base::db::Db;
use kanau::processor::Processor;

use crate::entities::surreal::account::{
    AccountEntity, AccountId, AccountRole, CreateAccount, DeleteAccount as DeleteAccountEntity,
    FindAccountByEmail, FindAccountById, ListAccounts as ListAccountsEntity, UpdateAccountEmail,
    UpdateAccountPassword, UpdateAccountRole,
};
use crate::entities::surreal::api_key::DeleteApiKeysByOwner;
use crate::entities::surreal::session::DeleteSessionsByAccount;
use crate::services::identity::Identity;
use crate::utils::password::{Argon2PasswordAlgorithm, PasswordAlgorithm};
use crate::utils::rbac::Permission;

/// Normalize an email for storage and lookup so the UNIQUE index and every
/// query agree on a single canonical form.
fn normalize_email(email: &str) -> String {
    email.trim().to_lowercase()
}

/// Account lifecycle and self-service operations.
#[derive(Clone)]
pub struct AccountService {
    pub db: Db,
    pub hasher: Argon2PasswordAlgorithm,
}

/// Register a new account (admin-only).
pub struct RegisterAccount {
    pub actor: Identity,
    pub email: String,
    pub password: String,
    pub role: AccountRole,
}

/// Outcome of [`RegisterAccount`].
pub enum RegisterResult {
    Created(AccountEntity),
    EmailTaken,
}

impl Processor<RegisterAccount> for AccountService {
    type Output = RegisterResult;
    type Error = wakuwaku::Error;
    #[tracing::instrument(name = "Service:RegisterAccount", skip_all, err)]
    async fn process(&self, input: RegisterAccount) -> Result<Self::Output, Self::Error> {
        input.actor.ensure(Permission::ManageAccounts)?;
        let email = normalize_email(&input.email);
        if self
            .db
            .process(FindAccountByEmail { email: &email })
            .await?
            .is_some()
        {
            return Ok(RegisterResult::EmailTaken);
        }
        let password_hash = self
            .hasher
            .hash_password(&input.password)
            .map_err(|e| wakuwaku::Error::BusinessPanic(anyhow::anyhow!(e)))?;
        let account = self
            .db
            .process(CreateAccount {
                email,
                password_hash,
                role: input.role,
            })
            .await?;
        Ok(RegisterResult::Created(account))
    }
}

/// List all accounts (admin-only).
pub struct ListAccounts {
    pub actor: Identity,
}

impl Processor<ListAccounts> for AccountService {
    type Output = Vec<AccountEntity>;
    type Error = wakuwaku::Error;
    #[tracing::instrument(name = "Service:ListAccounts", skip_all, err)]
    async fn process(&self, input: ListAccounts) -> Result<Self::Output, Self::Error> {
        input.actor.ensure(Permission::ManageAccounts)?;
        let accounts = self.db.process(ListAccountsEntity).await?;
        Ok(accounts)
    }
}

/// Change another account's role (admin-only).
pub struct SetAccountRole {
    pub actor: Identity,
    pub target: AccountId,
    pub role: AccountRole,
}

impl Processor<SetAccountRole> for AccountService {
    type Output = ();
    type Error = wakuwaku::Error;
    #[tracing::instrument(name = "Service:SetAccountRole", skip_all, err)]
    async fn process(&self, input: SetAccountRole) -> Result<Self::Output, Self::Error> {
        input.actor.ensure(Permission::ManageAccounts)?;
        self.db
            .process(UpdateAccountRole {
                id: input.target,
                role: input.role,
            })
            .await?;
        Ok(())
    }
}

/// Delete an account and cascade-remove its sessions and API keys (admin-only).
pub struct DeleteAccount {
    pub actor: Identity,
    pub target: AccountId,
}

impl Processor<DeleteAccount> for AccountService {
    type Output = ();
    type Error = wakuwaku::Error;
    #[tracing::instrument(name = "Service:DeleteAccount", skip_all, err)]
    async fn process(&self, input: DeleteAccount) -> Result<Self::Output, Self::Error> {
        input.actor.ensure(Permission::ManageAccounts)?;
        self.db
            .process(DeleteSessionsByAccount {
                account_id: input.target.clone(),
            })
            .await?;
        self.db
            .process(DeleteApiKeysByOwner {
                owner: input.target.clone(),
            })
            .await?;
        self.db
            .process(DeleteAccountEntity { id: input.target })
            .await?;
        Ok(())
    }
}

/// Change the caller's own password.
pub struct ChangeOwnPassword {
    pub actor: Identity,
    pub current_password: String,
    pub new_password: String,
}

/// Outcome of [`ChangeOwnPassword`].
pub enum ChangePasswordResult {
    Changed,
    WrongPassword,
}

impl Processor<ChangeOwnPassword> for AccountService {
    type Output = ChangePasswordResult;
    type Error = wakuwaku::Error;
    #[tracing::instrument(name = "Service:ChangeOwnPassword", skip_all, err)]
    async fn process(&self, input: ChangeOwnPassword) -> Result<Self::Output, Self::Error> {
        input.actor.require_human()?;
        let account = self
            .db
            .process(FindAccountById {
                id: input.actor.account_id.clone(),
            })
            .await?
            .ok_or(wakuwaku::Error::NotFound)?;
        if !self
            .hasher
            .verify_password(&input.current_password, &account.password_hash)
        {
            return Ok(ChangePasswordResult::WrongPassword);
        }
        let password_hash = self
            .hasher
            .hash_password(&input.new_password)
            .map_err(|e| wakuwaku::Error::BusinessPanic(anyhow::anyhow!(e)))?;
        self.db
            .process(UpdateAccountPassword {
                id: account.id,
                password_hash,
            })
            .await?;
        Ok(ChangePasswordResult::Changed)
    }
}

/// Change the caller's own email address.
pub struct ChangeOwnEmail {
    pub actor: Identity,
    pub new_email: String,
    pub current_password: String,
}

/// Outcome of [`ChangeOwnEmail`].
pub enum ChangeEmailResult {
    Changed,
    WrongPassword,
    EmailTaken,
}

impl Processor<ChangeOwnEmail> for AccountService {
    type Output = ChangeEmailResult;
    type Error = wakuwaku::Error;
    #[tracing::instrument(name = "Service:ChangeOwnEmail", skip_all, err)]
    async fn process(&self, input: ChangeOwnEmail) -> Result<Self::Output, Self::Error> {
        input.actor.require_human()?;
        let account = self
            .db
            .process(FindAccountById {
                id: input.actor.account_id.clone(),
            })
            .await?
            .ok_or(wakuwaku::Error::NotFound)?;
        if !self
            .hasher
            .verify_password(&input.current_password, &account.password_hash)
        {
            return Ok(ChangeEmailResult::WrongPassword);
        }
        let new_email = normalize_email(&input.new_email);
        if let Some(existing) = self
            .db
            .process(FindAccountByEmail { email: &new_email })
            .await?
            && existing.id.0 != account.id.0
        {
            return Ok(ChangeEmailResult::EmailTaken);
        }
        self.db
            .process(UpdateAccountEmail {
                id: account.id,
                new_email,
            })
            .await?;
        Ok(ChangeEmailResult::Changed)
    }
}
