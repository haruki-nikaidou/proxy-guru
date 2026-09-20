//! gRPC `Auth` service implementation: a thin adapter over the services.

use kanau::processor::Processor;
use time::OffsetDateTime;
use time::format_description::well_known::Rfc3339;
use tonic::{Request, Response, Status};

use rpguru_sdk::auth as pb;

use crate::entities::db::account::{AccountEntity, AccountId, AccountRole, FindAccountById};
use crate::entities::db::api_key::{ApiKeyId, ApiKeyOmitSecret};
use crate::rpc::middleware::{SESSION_ID_METADATA, from_request};
use crate::services::api_key::ListApiKeys;
use crate::services::config::{AuthConfigService, GetModuleConfig, SetModuleConfig};
use crate::services::identity::IdentityKind;
use crate::services::{
    AccountService, ApiKeyService, ChangeEmailResult, ChangeOwnEmail, ChangeOwnPassword,
    ChangePasswordResult, ConfigDocument, CreateApiKey, DeleteAccount, ListAccounts, Login,
    LoginResult, Logout, RegisterAccount, RegisterResult, RevokeApiKey, SessionService,
    SetAccountRole,
};

/// The concrete gRPC `Auth` service, wiring the auth services together.
#[derive(Clone)]
pub struct AuthGrpc {
    pub accounts: AccountService,
    pub sessions: SessionService,
    pub api_keys: ApiKeyService,
    pub configs: AuthConfigService,
}

fn role_to_proto(role: AccountRole) -> i32 {
    let role = match role {
        AccountRole::Admin => pb::Role::Admin,
        AccountRole::Maintainer => pb::Role::Maintainer,
        AccountRole::Observer => pb::Role::Observer,
    };
    role as i32
}

fn role_from_proto(role: i32) -> Result<AccountRole, Status> {
    match pb::Role::try_from(role) {
        Ok(pb::Role::Admin) => Ok(AccountRole::Admin),
        Ok(pb::Role::Maintainer) => Ok(AccountRole::Maintainer),
        Ok(pb::Role::Observer) => Ok(AccountRole::Observer),
        _ => Err(Status::invalid_argument("invalid or unspecified role")),
    }
}

fn account_to_proto(account: AccountEntity) -> pb::Account {
    pb::Account {
        id: account.id.to_string(),
        email: account.email,
        role: role_to_proto(account.role),
    }
}

fn api_key_to_proto(key: ApiKeyOmitSecret) -> pb::ApiKeySummary {
    pb::ApiKeySummary {
        id: key.id.to_string(),
        name: key.name,
        owner_account_id: key.owner.to_string(),
        created_at: rfc3339(key.created_at),
    }
}

/// RFC 3339, the one shape a timestamp crosses the API in. Formatting a value
/// that came from the database cannot fail; an empty string is the fallback
/// rather than a panic on the request path.
fn rfc3339(at: OffsetDateTime) -> String {
    at.format(&Rfc3339).unwrap_or_default()
}

/// Both document fields are pretty-printed: an operator edits this text.
fn config_to_proto(document: ConfigDocument) -> Result<rpguru_sdk::base::ConfigDocument, Status> {
    let encode = |value: &serde_json::Value| {
        serde_json::to_string_pretty(value).map_err(|error| {
            tracing::error!(error = %error, "serializing auth config document");
            Status::internal("Internal server error")
        })
    };
    Ok(rpguru_sdk::base::ConfigDocument {
        stored: document.stored,
        json: encode(&document.json)?,
        defaults_json: encode(&document.defaults)?,
    })
}

/// The payload an operator typed. Not valid JSON is their typo, not a bug.
fn config_json(json: &str) -> Result<serde_json::Value, Status> {
    serde_json::from_str(json).map_err(|error| {
        Status::invalid_argument(format!("the payload is not valid JSON: {error}"))
    })
}

#[tonic::async_trait]
impl pb::auth_server::Auth for AuthGrpc {
    async fn login(
        &self,
        request: Request<pb::LoginRequest>,
    ) -> Result<Response<pb::LoginReply>, Status> {
        let req = request.into_inner();
        let reply = match self
            .sessions
            .process(Login {
                email: req.email,
                password: req.password,
                user_agent: req.user_agent,
            })
            .await
            .map_err(base::db::status_of)?
        {
            LoginResult::Success(session_id) => pb::LoginReply {
                result: pb::LoginResult::Success as i32,
                session_id,
            },
            LoginResult::InvalidCredentials => pb::LoginReply {
                result: pb::LoginResult::InvalidCredentials as i32,
                session_id: String::new(),
            },
        };
        Ok(Response::new(reply))
    }

    async fn logout(
        &self,
        request: Request<pb::LogoutRequest>,
    ) -> Result<Response<pb::LogoutReply>, Status> {
        if let Some(session_id) = request
            .metadata()
            .get(SESSION_ID_METADATA)
            .and_then(|value| value.to_str().ok())
            .map(str::to_owned)
        {
            self.sessions
                .process(Logout { session_id })
                .await
                .map_err(base::db::status_of)?;
        }
        Ok(Response::new(pb::LogoutReply {}))
    }

    async fn current_identity(
        &self,
        request: Request<pb::CurrentIdentityRequest>,
    ) -> Result<Response<pb::CurrentIdentityReply>, Status> {
        let identity = from_request(&request)?;
        let account = self
            .accounts
            .db
            .process(FindAccountById {
                id: identity.account_id.clone(),
            })
            .await
            .map_err(wakuwaku::Error::from)?
            .ok_or_else(|| Status::not_found("account not found"))?;
        let kind = match identity.kind {
            IdentityKind::Session => pb::IdentityKind::Session,
            IdentityKind::ApiKey => pb::IdentityKind::ApiKey,
        };
        Ok(Response::new(pb::CurrentIdentityReply {
            account_id: account.id.to_string(),
            email: account.email,
            role: role_to_proto(identity.role),
            kind: kind as i32,
        }))
    }

    async fn change_password(
        &self,
        request: Request<pb::ChangePasswordRequest>,
    ) -> Result<Response<pb::ChangePasswordReply>, Status> {
        let actor = from_request(&request)?;
        let req = request.into_inner();
        let result = self
            .accounts
            .process(ChangeOwnPassword {
                actor,
                current_password: req.current_password,
                new_password: req.new_password,
            })
            .await
            .map_err(base::db::status_of)?;
        let result = match result {
            ChangePasswordResult::Changed => pb::ChangePasswordResult::Changed,
            ChangePasswordResult::WrongPassword => pb::ChangePasswordResult::WrongPassword,
        };
        Ok(Response::new(pb::ChangePasswordReply {
            result: result as i32,
        }))
    }

    async fn change_email(
        &self,
        request: Request<pb::ChangeEmailRequest>,
    ) -> Result<Response<pb::ChangeEmailReply>, Status> {
        let actor = from_request(&request)?;
        let req = request.into_inner();
        let result = self
            .accounts
            .process(ChangeOwnEmail {
                actor,
                new_email: req.new_email,
                current_password: req.current_password,
            })
            .await
            .map_err(base::db::status_of)?;
        let result = match result {
            ChangeEmailResult::Changed => pb::ChangeEmailResult::ChangeEmailChanged,
            ChangeEmailResult::WrongPassword => pb::ChangeEmailResult::ChangeEmailWrongPassword,
            ChangeEmailResult::EmailTaken => pb::ChangeEmailResult::EmailTaken,
        };
        Ok(Response::new(pb::ChangeEmailReply {
            result: result as i32,
        }))
    }

    async fn create_account(
        &self,
        request: Request<pb::CreateAccountRequest>,
    ) -> Result<Response<pb::CreateAccountReply>, Status> {
        let actor = from_request(&request)?;
        let req = request.into_inner();
        let role = role_from_proto(req.role)?;
        let reply = match self
            .accounts
            .process(RegisterAccount {
                actor,
                email: req.email,
                password: req.password,
                role,
            })
            .await
            .map_err(base::db::status_of)?
        {
            RegisterResult::Created(account) => pb::CreateAccountReply {
                result: pb::CreateAccountResult::Created as i32,
                account: Some(account_to_proto(account)),
            },
            RegisterResult::EmailTaken => pb::CreateAccountReply {
                result: pb::CreateAccountResult::CreateAccountEmailTaken as i32,
                account: None,
            },
        };
        Ok(Response::new(reply))
    }

    async fn list_accounts(
        &self,
        request: Request<pb::ListAccountsRequest>,
    ) -> Result<Response<pb::ListAccountsReply>, Status> {
        let actor = from_request(&request)?;
        let accounts = self
            .accounts
            .process(ListAccounts { actor })
            .await
            .map_err(base::db::status_of)?;
        Ok(Response::new(pb::ListAccountsReply {
            accounts: accounts.into_iter().map(account_to_proto).collect(),
        }))
    }

    async fn update_account_role(
        &self,
        request: Request<pb::UpdateAccountRoleRequest>,
    ) -> Result<Response<pb::UpdateAccountRoleReply>, Status> {
        let actor = from_request(&request)?;
        let req = request.into_inner();
        let target = AccountId::from_key(&req.account_id);
        let role = role_from_proto(req.role)?;
        self.accounts
            .process(SetAccountRole {
                actor,
                target: target.clone(),
                role,
            })
            .await
            .map_err(base::db::status_of)?;
        let account = self
            .accounts
            .db
            .process(FindAccountById { id: target })
            .await
            .map_err(wakuwaku::Error::from)?
            .ok_or_else(|| Status::not_found("account not found"))?;
        Ok(Response::new(pb::UpdateAccountRoleReply {
            account: Some(account_to_proto(account)),
        }))
    }

    async fn delete_account(
        &self,
        request: Request<pb::DeleteAccountRequest>,
    ) -> Result<Response<pb::DeleteAccountReply>, Status> {
        let actor = from_request(&request)?;
        let req = request.into_inner();
        let target = AccountId::from_key(&req.account_id);
        self.accounts
            .process(DeleteAccount { actor, target })
            .await
            .map_err(base::db::status_of)?;
        Ok(Response::new(pb::DeleteAccountReply {}))
    }

    async fn create_api_key(
        &self,
        request: Request<pb::CreateApiKeyRequest>,
    ) -> Result<Response<pb::CreateApiKeyReply>, Status> {
        let actor = from_request(&request)?;
        let req = request.into_inner();
        let created = self
            .api_keys
            .process(CreateApiKey {
                actor,
                name: req.name,
            })
            .await
            .map_err(base::db::status_of)?;
        Ok(Response::new(pb::CreateApiKeyReply {
            id: created.id.to_string(),
            secret: created.secret,
        }))
    }

    async fn list_api_keys(
        &self,
        request: Request<pb::ListApiKeysRequest>,
    ) -> Result<Response<pb::ListApiKeysReply>, Status> {
        let actor = from_request(&request)?;
        let keys = self
            .api_keys
            .process(ListApiKeys { actor })
            .await
            .map_err(base::db::status_of)?;
        Ok(Response::new(pb::ListApiKeysReply {
            keys: keys.into_iter().map(api_key_to_proto).collect(),
        }))
    }

    async fn revoke_api_key(
        &self,
        request: Request<pb::RevokeApiKeyRequest>,
    ) -> Result<Response<pb::RevokeApiKeyReply>, Status> {
        let actor = from_request(&request)?;
        let req = request.into_inner();
        let id = ApiKeyId::from_key(&req.id);
        self.api_keys
            .process(RevokeApiKey { actor, id })
            .await
            .map_err(base::db::status_of)?;
        Ok(Response::new(pb::RevokeApiKeyReply {}))
    }

    async fn get_auth_config(
        &self,
        request: Request<pb::GetAuthConfigRequest>,
    ) -> Result<Response<pb::GetAuthConfigReply>, Status> {
        let actor = from_request(&request)?;
        let document = self.configs.process(GetModuleConfig { actor }).await?;
        Ok(Response::new(pb::GetAuthConfigReply {
            config: Some(config_to_proto(document)?),
        }))
    }

    async fn set_auth_config(
        &self,
        request: Request<pb::SetAuthConfigRequest>,
    ) -> Result<Response<pb::SetAuthConfigReply>, Status> {
        let actor = from_request(&request)?;
        let req = request.into_inner();
        let json = config_json(&req.json)?;
        let document = self
            .configs
            .process(SetModuleConfig { actor, json })
            .await?;
        Ok(Response::new(pb::SetAuthConfigReply {
            config: Some(config_to_proto(document)?),
        }))
    }
}
