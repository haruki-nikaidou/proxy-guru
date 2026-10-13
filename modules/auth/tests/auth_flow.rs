//! End-to-end authentication + RBAC flow against a real PostgreSQL database.
//!
//! `#[sqlx::test]` gives every test a fresh, migrated database; the services are
//! then exercised exactly as the RPC edge would.

#![allow(clippy::unwrap_used, clippy::panic)]

use auth::config::AuthConfig;
use auth::entities::db::account::{
    AccountId, AccountRole, CreateAccount, FindAccountById, UpdateAccountEmail,
};
use auth::entities::db::session::FindSessionById;
use auth::services::AuthError;
use auth::services::account::{
    AccountService, ChangeEmailResult, ChangeOwnEmail, ChangeOwnPassword, ChangePasswordResult,
    DeleteAccount, RegisterAccount, RegisterResult, SetAccountRole,
};
use auth::services::api_key::{ApiKeyService, AuthenticateApiKey, CreateApiKey};
use auth::services::identity::{Identity, IdentityKind, PermissionDenied};
use auth::services::session::{AuthenticateSession, Login, LoginResult, Logout, SessionService};
use auth::utils::password::{Argon2PasswordAlgorithm, PasswordAlgorithm};
use auth::utils::rbac::Permission;
use auth::utils::validation::{
    API_KEY_NAME, AcceptablePassword, ApiKeyName, CanonicalEmail, NEW_PASSWORD, prove_email,
};
use base::db::Db;
use kanau::processor::Processor;
use wakuwaku::proof::Proven;

fn email(raw: &str) -> Proven<String, CanonicalEmail> {
    prove_email(raw).unwrap()
}

fn password(raw: &str) -> Proven<String, AcceptablePassword> {
    NEW_PASSWORD.prove(raw.to_owned()).unwrap()
}

fn key_name(raw: &str) -> Proven<String, ApiKeyName> {
    API_KEY_NAME.prove(raw.to_owned()).unwrap()
}

type TestResult = Result<(), Box<dyn std::error::Error>>;

/// Build the services over the test's database.
fn setup(pool: sqlx::PgPool) -> (Db, AccountService, SessionService, ApiKeyService) {
    let sp = base::db::handle(pool);
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

/// Creates an account through the entity layer; returns the identity a session of
/// it carries.
async fn create(
    sp: &Db,
    email: &str,
    password: &str,
    role: AccountRole,
) -> Result<Identity, Box<dyn std::error::Error>> {
    let account = sp
        .process(CreateAccount {
            email: prove_email(email).unwrap(),
            password_hash: Argon2PasswordAlgorithm::default().hash_password(password)?,
            role,
        })
        .await?
        .ok_or("the email is already taken")?;
    Ok(session_identity(account.id, role))
}

#[sqlx::test(migrator = "base::db::MIGRATOR")]
async fn full_auth_flow(pool: sqlx::PgPool) -> TestResult {
    let (sp, accounts, sessions, api_keys) = setup(pool);
    let hasher = Argon2PasswordAlgorithm::default();

    // 1. Bootstrap an admin directly via the entity layer.
    let admin = sp
        .process(CreateAccount {
            email: email("admin@example.com"),
            password_hash: hasher.hash_password("admin-password")?,
            role: AccountRole::Admin,
        })
        .await?
        .expect("a fresh database has no account with this email");
    let admin_identity = session_identity(admin.id.clone(), AccountRole::Admin);

    // 2. Admin registers a maintainer and an observer.
    let maintainer = match accounts
        .process(RegisterAccount {
            actor: admin_identity.clone(),
            email: email("maintainer@example.com"),
            password: password("maintainer-password"),
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
            email: email("observer@example.com"),
            password: password("observer-password"),
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
            email: email("  Maintainer@Example.com "),
            password: password("another-password"),
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
                email: email("sneaky@example.com"),
                password: password("maintainer-password-2"),
                role: AccountRole::Observer,
            })
            .await,
        Err(AuthError::PermissionDenied)
    ));
    assert!(matches!(
        api_keys
            .process(CreateApiKey {
                actor: observer_identity.clone(),
                name: key_name("observer-key"),
            })
            .await,
        Err(AuthError::PermissionDenied)
    ));

    // 7. Maintainer creates an API key; it authenticates as a machine identity,
    //    but that machine identity may not manage keys inside the auth module.
    let created_key = api_keys
        .process(CreateApiKey {
            actor: maintainer_identity.clone(),
            name: key_name("ci-key"),
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
        Err(PermissionDenied)
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
                new_password: password("brand-new-password"),
            })
            .await?,
        ChangePasswordResult::WrongPassword
    ));
    assert!(matches!(
        accounts
            .process(ChangeOwnPassword {
                actor: admin_identity.clone(),
                current_password: "admin-password".to_string(),
                new_password: password("brand-new-password"),
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
    let (sp, _accounts, sessions, _api_keys) = setup(pool.clone());
    let hasher = Argon2PasswordAlgorithm::default();
    sp.process(CreateAccount {
        email: email("op@example.com"),
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
    let backdated = at_login - time::Duration::minutes(5);
    sqlx::query!(
        "UPDATE auth_session SET last_active_at = $2 WHERE id = $1",
        token,
        backdated
    )
    .execute(&pool)
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
        at_login - time::Duration::seconds(AuthConfig::default().session_idle_ttl_secs + 1);
    sqlx::query!(
        "UPDATE auth_session SET last_active_at = $2 WHERE id = $1",
        token,
        expired
    )
    .execute(&pool)
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

/// Whether an address is taken is the unique constraint's answer, and it comes back
/// as a result, not an error: a registration or an address change that raced
/// another for the same address reaches its write with nothing any lookup could
/// have found, and still has to be told the address is taken.
#[sqlx::test(migrator = "base::db::MIGRATOR")]
async fn a_write_to_a_taken_address_is_refused_not_failed(pool: sqlx::PgPool) -> TestResult {
    let (sp, _accounts, _sessions, _api_keys) = setup(pool);
    let alice = create(
        &sp,
        "alice@example.com",
        "alice-password",
        AccountRole::Maintainer,
    )
    .await?;
    let bob = create(
        &sp,
        "bob@example.com",
        "bob-password",
        AccountRole::Maintainer,
    )
    .await?;

    let second = sp
        .process(CreateAccount {
            email: email("alice@example.com"),
            password_hash: "never-used".to_string(),
            role: AccountRole::Observer,
        })
        .await?;
    assert!(second.is_none(), "a second account for one address");
    assert!(
        !sp.process(UpdateAccountEmail {
            id: bob.account_id.clone(),
            email: email("alice@example.com"),
        })
        .await?,
        "moving onto another account's address"
    );
    assert!(
        sp.process(UpdateAccountEmail {
            id: alice.account_id.clone(),
            email: email("alice@example.com"),
        })
        .await?,
        "an account's own address is no conflict"
    );
    Ok(())
}

/// Changing one's address to one another account has is answered as taken, and to
/// one's own address as changed.
#[sqlx::test(migrator = "base::db::MIGRATOR")]
async fn changing_to_another_accounts_address_is_taken(pool: sqlx::PgPool) -> TestResult {
    let (sp, accounts, _sessions, _api_keys) = setup(pool);
    let alice = create(
        &sp,
        "alice@example.com",
        "alice-password",
        AccountRole::Maintainer,
    )
    .await?;
    create(
        &sp,
        "bob@example.com",
        "bob-password",
        AccountRole::Maintainer,
    )
    .await?;
    let change = |new_email: &str| {
        accounts.process(ChangeOwnEmail {
            actor: alice.clone(),
            new_email: email(new_email),
            current_password: "alice-password".to_string(),
        })
    };

    assert!(matches!(
        change(" Bob@Example.com ").await?,
        ChangeEmailResult::EmailTaken
    ));
    assert!(matches!(
        change("alice@example.com").await?,
        ChangeEmailResult::Changed
    ));
    assert!(matches!(
        change("alice@example.org").await?,
        ChangeEmailResult::Changed
    ));
    Ok(())
}

/// Changing the role of, or deleting, an account that does not exist is `NotFound`,
/// not a success that changed nothing; deleting one that exists removes it.
#[sqlx::test(migrator = "base::db::MIGRATOR")]
async fn changing_or_deleting_a_missing_account_is_not_found(pool: sqlx::PgPool) -> TestResult {
    let (sp, accounts, _sessions, _api_keys) = setup(pool);
    let admin = create(
        &sp,
        "admin@example.com",
        "admin-password",
        AccountRole::Admin,
    )
    .await?;
    let missing = AccountId::new();
    assert!(matches!(
        accounts
            .process(SetAccountRole {
                actor: admin.clone(),
                target: missing.clone(),
                role: AccountRole::Observer,
            })
            .await,
        Err(AuthError::NotFound)
    ));
    assert!(matches!(
        accounts
            .process(DeleteAccount {
                actor: admin.clone(),
                target: missing,
            })
            .await,
        Err(AuthError::NotFound)
    ));

    let observer = create(
        &sp,
        "observer@example.com",
        "observer-password",
        AccountRole::Observer,
    )
    .await?;
    accounts
        .process(DeleteAccount {
            actor: admin,
            target: observer.account_id.clone(),
        })
        .await?;
    assert!(
        sp.process(FindAccountById {
            id: observer.account_id
        })
        .await?
        .is_none()
    );
    Ok(())
}
