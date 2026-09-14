//! The `auth` configuration document against an in-memory SurrealDB.
//!
//! These tests apply `database/schema/base.surql` (the `app_config` table lives
//! in `base`) to a `mem://` instance and exercise `AuthConfigService` the way
//! the RPC edge does, including the `tonic::Status` a client would see.

#![allow(clippy::unwrap_used, clippy::panic)]

use auth::config::AuthConfig;
use auth::entities::surreal::account::{AccountId, AccountRole};
use auth::services::config::{AuthConfigService, GetModuleConfig, SetModuleConfig};
use auth::services::identity::{Identity, IdentityKind};
use base::services::config::ConfigStore;
use kanau::processor::Processor;
use serde_json::json;
use surrealdb::types::RecordId;
use wakuwaku::surreal::SurrealProcessor;

type TestResult = Result<(), Box<dyn std::error::Error>>;

async fn setup() -> Result<AuthConfigService, Box<dyn std::error::Error>> {
    let db = surrealdb::engine::any::connect("mem://").await?;
    db.use_ns("test").use_db("test").await?;
    let db = SurrealProcessor::new(db);
    let ddl = std::fs::read_to_string(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../database/schema/base.surql"
    ))?;
    // `.check()` surfaces any per-statement error from applying the schema.
    db.db().query(ddl).await?.check()?;
    Ok(AuthConfigService {
        configs: ConfigStore { db },
    })
}

fn actor(role: AccountRole, kind: IdentityKind) -> Identity {
    Identity {
        account_id: AccountId(RecordId::new("auth_account", "someone")),
        role,
        kind,
    }
}

fn admin() -> Identity {
    actor(AccountRole::Admin, IdentityKind::Session)
}

fn code(error: impl Into<tonic::Status>) -> tonic::Code {
    error.into().code()
}

/// An unseeded installation is a normal state: the document says there is no
/// row and hands back exactly what seeding would write.
#[tokio::test]
async fn an_unset_key_reports_its_defaults() -> TestResult {
    let service = setup().await?;
    let document = service.process(GetModuleConfig { actor: admin() }).await?;

    assert!(!document.stored);
    assert_eq!(
        document.defaults,
        serde_json::to_value(AuthConfig::default())?
    );
    assert_eq!(document.json, document.defaults);
    Ok(())
}

/// A write goes to the database, not to a cache: a later read sees it, and the
/// defaults it is offered alongside are still the defaults.
#[tokio::test]
async fn an_admin_write_round_trips_through_the_database() -> TestResult {
    let service = setup().await?;
    let written = service
        .process(SetModuleConfig {
            actor: admin(),
            json: json!({ "session_idle_ttl_secs": 900 }),
        })
        .await?;
    assert!(written.stored);

    let read = service.process(GetModuleConfig { actor: admin() }).await?;
    assert!(read.stored);
    assert_eq!(read.json, json!({ "session_idle_ttl_secs": 900 }));
    assert_eq!(read.defaults, serde_json::to_value(AuthConfig::default())?);
    Ok(())
}

/// A payload of the wrong shape is an operator typo: it is refused with
/// `INVALID_ARGUMENT` naming the key, and the row keeps what it had.
#[tokio::test]
async fn a_wrong_shaped_payload_leaves_the_row_alone() -> TestResult {
    let service = setup().await?;
    service
        .process(SetModuleConfig {
            actor: admin(),
            json: json!({ "session_idle_ttl_secs": 900 }),
        })
        .await?;

    let error = service
        .process(SetModuleConfig {
            actor: admin(),
            json: json!({ "session_idle_ttl_secs": "a while" }),
        })
        .await
        .unwrap_err();
    let status = tonic::Status::from(error);
    assert_eq!(status.code(), tonic::Code::InvalidArgument);
    assert!(status.message().contains("auth"), "{}", status.message());

    let read = service.process(GetModuleConfig { actor: admin() }).await?;
    assert_eq!(read.json, json!({ "session_idle_ttl_secs": 900 }));
    Ok(())
}

/// `ManageConfig` is Admin-only and human-session-only: nobody else may read
/// the installation's settings, let alone replace them.
#[tokio::test]
async fn only_an_admin_session_may_touch_the_document() -> TestResult {
    let service = setup().await?;
    let denied = [
        actor(AccountRole::Maintainer, IdentityKind::Session),
        actor(AccountRole::Observer, IdentityKind::Session),
        // An Admin's API key is a machine credential: configuration is not
        // something a worker token may rewrite.
        actor(AccountRole::Admin, IdentityKind::ApiKey),
    ];
    for identity in denied {
        let read = service
            .process(GetModuleConfig {
                actor: identity.clone(),
            })
            .await
            .unwrap_err();
        assert_eq!(code(read), tonic::Code::PermissionDenied);

        let write = service
            .process(SetModuleConfig {
                actor: identity,
                json: json!({ "session_idle_ttl_secs": 1 }),
            })
            .await
            .unwrap_err();
        assert_eq!(code(write), tonic::Code::PermissionDenied);
    }

    // None of the refused writes reached the row.
    assert!(
        !service
            .process(GetModuleConfig { actor: admin() })
            .await?
            .stored
    );
    Ok(())
}
