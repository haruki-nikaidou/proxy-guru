//! Human session lifecycle: login, session authentication, and logout.

use std::sync::LazyLock;

use base::db::Db;
use chrono::{Duration, Utc};
use kanau::processor::Processor;

use crate::config::AuthConfig;
use crate::entities::surreal::account::FindAccountByEmail;
use crate::entities::surreal::session::{
    CreateSession, DeleteSession, FindSessionById, UpdateSession,
};
use crate::services::identity::{Identity, IdentityKind};
use crate::utils::password::{Argon2PasswordAlgorithm, PasswordAlgorithm};
use crate::utils::token::generate_session_token;

/// A hash verified against on the "account not found" path so that login timing
/// does not reveal whether an email exists. Computed once with the default
/// algorithm; verifying any password against it always fails.
static DUMMY_HASH: LazyLock<String> = LazyLock::new(|| {
    Argon2PasswordAlgorithm::default()
        .hash_password("dummy-password-for-constant-time-login")
        .unwrap_or_default()
});

/// Normalize an email for lookup so login agrees with the stored canonical form.
fn normalize_email(email: &str) -> String {
    email.trim().to_lowercase()
}

/// How stale a session's recorded activity may get before it is written again.
///
/// The idle deadline is a week. Recording activity to the second would mean a
/// write on every request, all of one operator's requests to the same row —
/// contention by construction, and what took the dashboard down on 2026-09-16
/// the moment the database transport stopped serialising them by accident.
/// With a minute of slack a session is written at most once a minute and
/// expires at most a minute earlier than the deadline says.
const ACTIVITY_SLACK: Duration = Duration::minutes(1);

/// Session operations for human callers.
#[derive(Clone)]
pub struct SessionService {
    pub db: Db,
    pub hasher: Argon2PasswordAlgorithm,
    pub config: AuthConfig,
}

/// Authenticate an email + password and open a session.
pub struct Login {
    pub email: String,
    pub password: String,
    pub user_agent: String,
}

/// Outcome of [`Login`]. `Success` carries the opaque session token.
pub enum LoginResult {
    Success(String),
    InvalidCredentials,
}

impl Processor<Login> for SessionService {
    type Output = LoginResult;
    type Error = wakuwaku::Error;
    #[tracing::instrument(name = "Service:Login", skip_all, err)]
    async fn process(&self, input: Login) -> Result<Self::Output, Self::Error> {
        let email = normalize_email(&input.email);
        let account = self
            .db
            .process(FindAccountByEmail { email: &email })
            .await?;
        let account = match account {
            Some(account) => account,
            None => {
                // Equalize timing: verify against a dummy hash before failing.
                let _ = self.hasher.verify_password(&input.password, &DUMMY_HASH);
                return Ok(LoginResult::InvalidCredentials);
            }
        };
        if !self
            .hasher
            .verify_password(&input.password, &account.password_hash)
        {
            return Ok(LoginResult::InvalidCredentials);
        }
        let token = generate_session_token();
        let now = Utc::now();
        self.db
            .process(CreateSession {
                token: token.clone(),
                account_id: account.id,
                user_agent: input.user_agent,
                created_at: now,
                last_active_at: now,
            })
            .await?;
        Ok(LoginResult::Success(token))
    }
}

/// Resolve a session id into an [`Identity`], sliding its idle expiry when it is due.
pub struct AuthenticateSession {
    pub session_id: String,
}

impl Processor<AuthenticateSession> for SessionService {
    type Output = Option<Identity>;
    type Error = wakuwaku::Error;
    #[tracing::instrument(name = "Service:AuthenticateSession", skip_all, err)]
    async fn process(&self, input: AuthenticateSession) -> Result<Self::Output, Self::Error> {
        let session = match self
            .db
            .process(FindSessionById {
                session_id: input.session_id.clone(),
            })
            .await?
        {
            Some(session) => session,
            None => return Ok(None),
        };
        let now = Utc::now();
        if now.signed_duration_since(session.last_active_at)
            > Duration::seconds(self.config.session_idle_ttl_secs)
        {
            self.db
                .process(DeleteSession {
                    session_id: input.session_id,
                })
                .await?;
            return Ok(None);
        }
        let account = match self
            .db
            .process(crate::entities::surreal::account::FindAccountById {
                id: session.account_id.clone(),
            })
            .await?
        {
            Some(account) => account,
            None => return Ok(None),
        };
        // The decision is made: the session exists, is not idle, and its account
        // is still there. Sliding the deadline is bookkeeping, done only once the
        // record is stale, and a refused write is a log line rather than a denied
        // request — at worst the session expires `ACTIVITY_SLACK` early.
        if now.signed_duration_since(session.last_active_at) > ACTIVITY_SLACK
            && let Err(error) = self
                .db
                .process(UpdateSession {
                    id: input.session_id,
                    last_active_at: now,
                })
                .await
        {
            tracing::warn!(%error, "could not slide the session's idle expiry");
        }
        Ok(Some(Identity {
            account_id: account.id,
            role: account.role,
            kind: IdentityKind::Session,
        }))
    }
}

/// End a session.
pub struct Logout {
    pub session_id: String,
}

impl Processor<Logout> for SessionService {
    type Output = ();
    type Error = wakuwaku::Error;
    #[tracing::instrument(name = "Service:Logout", skip_all, err)]
    async fn process(&self, input: Logout) -> Result<Self::Output, Self::Error> {
        self.db
            .process(DeleteSession {
                session_id: input.session_id,
            })
            .await?;
        Ok(())
    }
}
