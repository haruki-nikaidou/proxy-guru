//! The auth edge must never turn "the database did not answer" into "you are not logged in".
//!
//! The dashboard's error boundary reacts to `UNAUTHENTICATED` by clearing the session cookie
//! and redirecting to the login page. So the middleware conflating a failed credential lookup
//! with a rejected one does not merely lose a request: it logs the operator out. That
//! happened fifteen times in three days on the live control plane, back when the shared
//! database connection could come back from a reconnect half-configured.

#![allow(clippy::unwrap_used, clippy::panic, clippy::expect_used)]

use auth::entities::db::account::{AccountRole, CreateAccount};
use auth::rpc::middleware::{AuthLayer, SESSION_ID_METADATA, from_request};
use auth::services::api_key::ApiKeyService;
use auth::services::session::{Login, LoginResult, SessionService};
use auth::utils::password::{Argon2PasswordAlgorithm, PasswordAlgorithm};
use base::db::Db;
use kanau::processor::Processor;
use std::convert::Infallible;
use tonic::codegen::Service;
use tonic::codegen::http;
use tower::Layer;

type TestResult = Result<(), Box<dyn std::error::Error>>;

fn services(db: &Db) -> (SessionService, ApiKeyService) {
    (
        SessionService {
            db: db.clone(),
            hasher: Argon2PasswordAlgorithm::default(),
            config: auth::config::AuthConfig::default(),
        },
        ApiKeyService { db: db.clone() },
    )
}

/// Runs one request through the middleware and reports what a handler calling
/// `from_request` would see: the identity, or the status it would answer with.
async fn verdict(db: &Db, session_id: &str) -> Result<(), tonic::Status> {
    let (sessions, api_keys) = services(db);
    // The handler stand-in: whatever the middleware decided reaches `from_request` exactly
    // as it does in a real gRPC handler.
    let inner = tower::service_fn(|req: http::Request<()>| async move {
        let request = tonic::Request::from_parts(
            tonic::metadata::MetadataMap::default(),
            req.extensions().clone(),
            (),
        );
        Ok::<_, Infallible>(from_request(&request).map(|_| ()))
    });
    let mut service = AuthLayer::new(sessions, api_keys).layer(inner);
    let mut request = http::Request::new(());
    request
        .headers_mut()
        .insert(SESSION_ID_METADATA, session_id.parse().unwrap());
    service.call(request).await.unwrap()
}

/// Logs an account in and returns its session id.
async fn login(db: &Db) -> Result<String, Box<dyn std::error::Error>> {
    let hasher = Argon2PasswordAlgorithm::default();
    db.process(CreateAccount {
        email: "ops@example.com".to_string(),
        password_hash: hasher.hash_password("hunter2hunter2")?,
        role: AccountRole::Maintainer,
    })
    .await?;
    let (sessions, _) = services(db);
    match sessions
        .process(Login {
            email: "ops@example.com".to_string(),
            password: "hunter2hunter2".to_string(),
            user_agent: "test".to_string(),
        })
        .await?
    {
        LoginResult::Success(session_id) => Ok(session_id),
        LoginResult::InvalidCredentials => Err("the login should have succeeded".into()),
    }
}

#[sqlx::test(migrator = "base::db::MIGRATOR")]
async fn a_valid_session_resolves(pool: sqlx::PgPool) -> TestResult {
    let db = Db::new(pool);
    let session_id = login(&db).await?;
    verdict(&db, &session_id)
        .await
        .expect("the session resolves");
    Ok(())
}

/// The one case that may log somebody out: the credential was judged, and rejected.
#[sqlx::test(migrator = "base::db::MIGRATOR")]
async fn an_unknown_session_is_unauthenticated(pool: sqlx::PgPool) -> TestResult {
    let db = Db::new(pool);
    login(&db).await?;
    let status = verdict(&db, "nosuchsession")
        .await
        .expect_err("an unknown session is refused");
    assert_eq!(status.code(), tonic::Code::Unauthenticated);
    Ok(())
}

/// The case that must **not** log anybody out.
///
/// A pool pointed at a database that does not exist fails every lookup before it can say
/// anything about the session, so the answer is `UNAVAILABLE` and the dashboard keeps the
/// cookie. A statement the server cancels at `statement_timeout` takes the same path; that
/// the cancellation is classified as unavailable is proven in `base`'s own tests.
#[sqlx::test(migrator = "base::db::MIGRATOR")]
async fn a_database_that_fails_the_lookup_keeps_the_session(pool: sqlx::PgPool) -> TestResult {
    let db = Db::new(pool);
    let session_id = login(&db).await?;

    let unreachable = sqlx::postgres::PgPoolOptions::new().connect_lazy_with(
        db.db()
            .connect_options()
            .as_ref()
            .clone()
            .database("guru_no_such_database"),
    );
    let status = verdict(&Db::new(unreachable), &session_id)
        .await
        .expect_err("the lookup cannot succeed");
    assert_eq!(
        status.code(),
        tonic::Code::Unavailable,
        "a database failure must not read as an invalid session, or the operator is logged \
         out by a blip: {status:?}"
    );

    // And the session really was fine all along.
    verdict(&db, &session_id)
        .await
        .expect("the session outlived the outage");
    Ok(())
}
