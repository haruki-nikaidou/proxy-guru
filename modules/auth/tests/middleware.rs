//! The auth edge must never turn "the database did not answer" into "you are not logged in".
//!
//! The dashboard's error boundary reacts to `UNAUTHENTICATED` by clearing the session cookie
//! and redirecting to the login page. So the middleware conflating a failed credential lookup
//! with a rejected one does not merely lose a request: it logs the operator out. That
//! happened fifteen times in three days on the live control plane, every time the shared
//! database connection came back from a reconnect without its namespace.

#![allow(clippy::unwrap_used, clippy::panic, clippy::expect_used)]

use auth::entities::surreal::account::{AccountRole, CreateAccount};
use auth::rpc::middleware::{AuthLayer, SESSION_ID_METADATA, from_request};
use auth::services::api_key::ApiKeyService;
use auth::services::session::{Login, LoginResult, SessionService};
use auth::utils::password::{Argon2PasswordAlgorithm, PasswordAlgorithm};
use base::db::Db;
use kanau::processor::Processor;
use std::convert::Infallible;
use std::time::Duration;
use tonic::codegen::Service;
use tonic::codegen::http;
use tower::Layer;

type TestResult = Result<(), Box<dyn std::error::Error>>;

/// A fresh in-memory database with the auth schema applied, bounded as given.
async fn database(limit: Duration) -> Result<Db, Box<dyn std::error::Error>> {
    let db = surrealdb::engine::any::connect("mem://").await?;
    db.use_ns("test").use_db("test").await?;
    let db = Db::new(db).with_timeout(limit);
    let ddl = std::fs::read_to_string(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../database/schema/auth.surql"
    ))?;
    db.raw().query(ddl).await?.check()?;
    Ok(db)
}

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

#[tokio::test]
async fn a_valid_session_resolves() -> TestResult {
    let db = database(Duration::from_secs(10)).await?;
    let session_id = login(&db).await?;
    verdict(&db, &session_id)
        .await
        .expect("the session resolves");
    Ok(())
}

/// The one case that may log somebody out: the credential was judged, and rejected.
#[tokio::test]
async fn an_unknown_session_is_unauthenticated() -> TestResult {
    let db = database(Duration::from_secs(10)).await?;
    login(&db).await?;
    let status = verdict(&db, "nosuchsession")
        .await
        .expect_err("an unknown session is refused");
    assert_eq!(status.code(), tonic::Code::Unauthenticated);
    Ok(())
}

/// The case that must **not** log anybody out.
///
/// A handle whose connection has no namespace selected answers every query with "Specify a
/// namespace to use" — the exact error the live control plane logs when its shared
/// connection comes back from a reconnect, and the one that was costing operators their
/// sessions. It says nothing about whether the session is valid, so the answer is
/// `UNAVAILABLE` and the dashboard keeps the cookie.
///
/// The middleware treats a lookup that times out identically. That branch is not reproduced
/// here — a connection that accepts and then answers nothing cannot be stood up from a test
/// without a fake server — but the bound that produces it is proven in `base`'s own tests
/// against a query that really blocks.
#[tokio::test]
async fn a_database_that_fails_the_lookup_keeps_the_session() -> TestResult {
    let db = database(Duration::from_secs(10)).await?;
    let session_id = login(&db).await?;

    let stateless = Db::new(surrealdb::engine::any::connect("mem://").await?);
    let status = verdict(&stateless, &session_id)
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
