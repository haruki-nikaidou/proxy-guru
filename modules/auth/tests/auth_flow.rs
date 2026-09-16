//! End-to-end authentication + RBAC flow against a real PostgreSQL database.
//!
//! `#[sqlx::test]` gives every test a fresh, migrated database; the services are
//! then exercised exactly as the RPC edge would.

#![allow(clippy::unwrap_used, clippy::panic)]

use auth::config::AuthConfig;
use auth::entities::db::account::{AccountRole, CreateAccount};
use auth::entities::db::session::FindSessionById;
use auth::services::account::{
    AccountService, ChangeOwnPassword, ChangePasswordResult, RegisterAccount, RegisterResult,
    SetAccountRole,
};
use auth::services::api_key::{ApiKeyService, AuthenticateApiKey, CreateApiKey};
use auth::services::identity::{Identity, IdentityKind};
use auth::services::session::{AuthenticateSession, Login, LoginResult, Logout, SessionService};
use auth::utils::password::{Argon2PasswordAlgorithm, PasswordAlgorithm};
use auth::utils::rbac::Permission;
use base::db::Db;
use kanau::processor::Processor;

type TestResult = Result<(), Box<dyn std::error::Error>>;

/// Build the services over the test's database.
fn setup(pool: sqlx::PgPool) -> (Db, AccountService, SessionService, ApiKeyService) {
    let sp = Db::new(pool);
    let hasher = Argon2PasswordAlgorithm::default();
    let accounts = AccountService {
        db: sp.clone(),
        hasher: hasher.clone(),
    };
    let sessions = SessionService {
        db: sp.clone(),
        hasher: hasher.clone(),
        config: AuthConfig::default(),
    };
    let api_keys = ApiKeyService { db: sp.clone() };

    (sp, accounts, sessions, api_keys)
}

fn session_identity(
    account_id: auth::entities::db::account::AccountId,
    role: AccountRole,
) -> Identity {
    Identity {
        account_id,
        role,
        kind: IdentityKind::Session,
    }
}

#[sqlx::test(migrator = "base::db::MIGRATOR")]
async fn full_auth_flow(pool: sqlx::PgPool) -> TestResult {
    let (sp, accounts, sessions, api_keys) = setup(pool);
    let hasher = Argon2PasswordAlgorithm::default();

    // 1. Bootstrap an admin directly via the entity layer.
    let admin = sp
        .process(CreateAccount {
            email: "admin@example.com".to_string(),
            password_hash: hasher.hash_password("admin-password")?,
            role: AccountRole::Admin,
        })
        .await?;
    let admin_identity = session_identity(admin.id.clone(), AccountRole::Admin);

    // 2. Admin registers a maintainer and an observer.
    let maintainer = match accounts
        .process(RegisterAccount {
            actor: admin_identity.clone(),
            email: "maintainer@example.com".to_string(),
            password: "maintainer-password".to_string(),
            role: AccountRole::Maintainer,
        })
        .await?
    {
        RegisterResult::Created(account) => account,
        RegisterResult::EmailTaken => panic!("maintainer email unexpectedly taken"),
    };
    let observer = match accounts
        .process(RegisterAccount {
            actor: admin_identity.clone(),
            email: "observer@example.com".to_string(),
            password: "observer-password".to_string(),
            role: AccountRole::Observer,
        })
        .await?
    {
        RegisterResult::Created(account) => account,
        RegisterResult::EmailTaken => panic!("observer email unexpectedly taken"),
    };

    // 3. Duplicate registration is rejected (case/whitespace-normalized email).
    let duplicate = accounts
        .process(RegisterAccount {
            actor: admin_identity.clone(),
            email: "  Maintainer@Example.com ".to_string(),
            password: "another-password".to_string(),
            role: AccountRole::Observer,
        })
        .await?;
    assert!(matches!(duplicate, RegisterResult::EmailTaken));

    // 4. Login: correct, wrong password, unknown email.
    let token = match sessions
        .process(Login {
            email: "maintainer@example.com".to_string(),
            password: "maintainer-password".to_string(),
            user_agent: "test-agent".to_string(),
        })
        .await?
    {
        LoginResult::Success(token) => token,
        LoginResult::InvalidCredentials => panic!("valid login rejected"),
    };
    assert!(matches!(
        sessions
            .process(Login {
                email: "maintainer@example.com".to_string(),
                password: "wrong-password".to_string(),
                user_agent: "test-agent".to_string(),
            })
            .await?,
        LoginResult::InvalidCredentials
    ));
    assert!(matches!(
        sessions
            .process(Login {
                email: "nobody@example.com".to_string(),
                password: "whatever".to_string(),
                user_agent: "test-agent".to_string(),
            })
            .await?,
        LoginResult::InvalidCredentials
    ));

    // 5. The session resolves to a maintainer identity.
    let resolved = sessions
        .process(AuthenticateSession {
            session_id: token.clone(),
        })
        .await?
        .expect("session should resolve");
    assert_eq!(resolved.role, AccountRole::Maintainer);
    assert_eq!(resolved.kind, IdentityKind::Session);

    // 6. RBAC denials: maintainer cannot register; observer cannot make keys.
    let maintainer_identity = session_identity(maintainer.id.clone(), AccountRole::Maintainer);
    let observer_identity = session_identity(observer.id.clone(), AccountRole::Observer);
    assert!(matches!(
        accounts
            .process(RegisterAccount {
                actor: maintainer_identity.clone(),
                email: "sneaky@example.com".to_string(),
                password: "pw".to_string(),
                role: AccountRole::Observer,
            })
            .await,
        Err(wakuwaku::Error::PermissionsDenied)
    ));
    assert!(matches!(
        api_keys
            .process(CreateApiKey {
                actor: observer_identity.clone(),
                name: "observer-key".to_string(),
            })
            .await,
        Err(wakuwaku::Error::PermissionsDenied)
    ));

    // 7. Maintainer creates an API key; it authenticates as a machine identity,
    //    but that machine identity may not manage keys inside the auth module.
    let created_key = api_keys
        .process(CreateApiKey {
            actor: maintainer_identity.clone(),
            name: "ci-key".to_string(),
        })
        .await?;
    let api_identity = api_keys
        .process(AuthenticateApiKey {
            secret: created_key.secret.clone(),
        })
        .await?
        .expect("api key should authenticate");
    assert_eq!(api_identity.kind, IdentityKind::ApiKey);
    assert_eq!(api_identity.role, AccountRole::Maintainer);
    assert!(matches!(
        api_identity.ensure(Permission::ManageApiKeys),
        Err(wakuwaku::Error::PermissionsDenied)
    ));

    // 8. Downgrading the key owner to Observer revokes API-key authentication.
    accounts
        .process(SetAccountRole {
            actor: admin_identity.clone(),
            target: maintainer.id.clone(),
            role: AccountRole::Observer,
        })
        .await?;
    assert!(
        api_keys
            .process(AuthenticateApiKey {
                secret: created_key.secret.clone(),
            })
            .await?
            .is_none()
    );

    // 9. Logout invalidates the session.
    sessions
        .process(Logout {
            session_id: token.clone(),
        })
        .await?;
    assert!(
        sessions
            .process(AuthenticateSession { session_id: token })
            .await?
            .is_none()
    );

    // 10. Self-service password change, then re-login with the new password.
    assert!(matches!(
        accounts
            .process(ChangeOwnPassword {
                actor: admin_identity.clone(),
                current_password: "wrong-current".to_string(),
                new_password: "brand-new-password".to_string(),
            })
            .await?,
        ChangePasswordResult::WrongPassword
    ));
    assert!(matches!(
        accounts
            .process(ChangeOwnPassword {
                actor: admin_identity.clone(),
                current_password: "admin-password".to_string(),
                new_password: "brand-new-password".to_string(),
            })
            .await?,
        ChangePasswordResult::Changed
    ));
    assert!(matches!(
        sessions
            .process(Login {
                email: "admin@example.com".to_string(),
                password: "brand-new-password".to_string(),
                user_agent: "test-agent".to_string(),
            })
            .await?,
        LoginResult::Success(_)
    ));

    Ok(())
}

/// Authenticating slides the idle deadline, but records the slide only once it
/// is stale: a burst of requests on one session must not be a burst of writes
/// to one row.
#[sqlx::test(migrator = "base::db::MIGRATOR")]
async fn activity_is_recorded_once_per_slack_not_once_per_request(
    pool: sqlx::PgPool,
) -> TestResult {
    let (sp, _accounts, sessions, _api_keys) = setup(pool);
    let hasher = Argon2PasswordAlgorithm::default();
    sp.process(CreateAccount {
        email: "op@example.com".to_string(),
        password_hash: hasher.hash_password("pw")?,
        role: AccountRole::Maintainer,
    })
    .await?;
    let LoginResult::Success(token) = sessions
        .process(Login {
            email: "op@example.com".to_string(),
            password: "pw".to_string(),
            user_agent: "test-agent".to_string(),
        })
        .await?
    else {
        panic!("login should succeed");
    };
    let recorded = |sp: &Db, token: &str| {
        let sp = sp.clone();
        let token = token.to_string();
        async move {
            sp.process(FindSessionById { session_id: token })
                .await
                .map(|s| s.expect("session row exists").last_active_at)
        }
    };
    let at_login = recorded(&sp, &token).await?;

    // Fresh record: authenticating a few times in a row writes nothing.
    for _ in 0..3 {
        assert!(
            sessions
                .process(AuthenticateSession {
                    session_id: token.clone(),
                })
                .await?
                .is_some()
        );
    }
    assert_eq!(recorded(&sp, &token).await?, at_login);

    // Stale record: the next authentication brings it forward.
    let backdated = at_login - chrono::Duration::minutes(5);
    sqlx::query("UPDATE auth_session SET last_active_at = $2 WHERE id = $1")
        .bind(&token)
        .bind(backdated)
        .execute(sp.db())
        .await?;
    assert!(
        sessions
            .process(AuthenticateSession {
                session_id: token.clone(),
            })
            .await?
            .is_some()
    );
    assert!(recorded(&sp, &token).await? > backdated);

    // Idle past the deadline: rejected, and the row is gone.
    let expired =
        at_login - chrono::Duration::seconds(AuthConfig::default().session_idle_ttl_secs + 1);
    sqlx::query("UPDATE auth_session SET last_active_at = $2 WHERE id = $1")
        .bind(&token)
        .bind(expired)
        .execute(sp.db())
        .await?;
    assert!(
        sessions
            .process(AuthenticateSession {
                session_id: token.clone(),
            })
            .await?
            .is_none()
    );
    assert!(
        sp.process(FindSessionById { session_id: token })
            .await?
            .is_none()
    );
    Ok(())
}
