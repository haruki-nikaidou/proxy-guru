//! The `orchestration` configuration document against an in-memory SurrealDB.
//!
//! These tests apply `database/schema/base.surql` (the `app_config` table lives
//! in `base`) and exercise `OrchestrationConfigService`
//! the way the RPC edge does, including the `tonic::Status` a client would see.

#![allow(clippy::unwrap_used, clippy::panic)]

use auth::entities::db::account::{AccountId, AccountRole};
use auth::services::identity::{Identity, IdentityKind};
use base::db::Db;
use base::services::config::ConfigStore;
use kanau::processor::Processor;
use orchestration::config::OrchestrationConfig;
use orchestration::services::config::{
    GetModuleConfig, OrchestrationConfigService, SetModuleConfig,
};
use serde_json::json;

type TestResult = Result<(), Box<dyn std::error::Error>>;

fn setup(pool: sqlx::PgPool) -> OrchestrationConfigService {
    OrchestrationConfigService {
        configs: ConfigStore { db: Db::new(pool) },
    }
}

fn actor(role: AccountRole, kind: IdentityKind) -> Identity {
    Identity {
        account_id: AccountId::from_key("someone"),
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

/// The payload a full document carries: the defaults with one knob moved, so a
/// write is distinguishable from the defaults it replaces.
fn edited() -> Result<serde_json::Value, serde_json::Error> {
    serde_json::to_value(OrchestrationConfig {
        acme_renew_before_secs: 1_209_600,
        ..Default::default()
    })
}

/// An unseeded installation is a normal state: the document says there is no
/// row and hands back exactly what seeding would write.
#[sqlx::test(migrator = "base::db::MIGRATOR")]
async fn an_unset_key_reports_its_defaults(pool: sqlx::PgPool) -> TestResult {
    let service = setup(pool);
    let document = service.process(GetModuleConfig { actor: admin() }).await?;

    assert!(!document.stored);
    assert_eq!(
        document.defaults,
        serde_json::to_value(OrchestrationConfig::default())?
    );
    assert_eq!(document.json, document.defaults);
    Ok(())
}

/// A write goes to the database, not to a cache: a later read sees it, and the
/// defaults it is offered alongside are still the defaults.
#[sqlx::test(migrator = "base::db::MIGRATOR")]
async fn an_admin_write_round_trips_through_the_database(pool: sqlx::PgPool) -> TestResult {
    let service = setup(pool);
    let written = service
        .process(SetModuleConfig {
            actor: admin(),
            json: edited()?,
        })
        .await?;
    assert!(written.stored);

    let read = service.process(GetModuleConfig { actor: admin() }).await?;
    assert!(read.stored);
    assert_eq!(read.json, edited()?);
    assert_eq!(
        read.defaults,
        serde_json::to_value(OrchestrationConfig::default())?
    );
    Ok(())
}

/// A payload of the wrong shape is an operator typo: it is refused with
/// `INVALID_ARGUMENT` naming the key, and the row keeps what it had.
#[sqlx::test(migrator = "base::db::MIGRATOR")]
async fn a_wrong_shaped_payload_leaves_the_row_alone(pool: sqlx::PgPool) -> TestResult {
    let service = setup(pool);
    service
        .process(SetModuleConfig {
            actor: admin(),
            json: edited()?,
        })
        .await?;

    let error = service
        .process(SetModuleConfig {
            actor: admin(),
            json: json!({ "acme_renew_before_secs": "a fortnight" }),
        })
        .await
        .unwrap_err();
    let status = tonic::Status::from(error);
    assert_eq!(status.code(), tonic::Code::InvalidArgument);
    assert!(
        status.message().contains("orchestration"),
        "{}",
        status.message()
    );

    let read = service.process(GetModuleConfig { actor: admin() }).await?;
    assert_eq!(read.json, edited()?);
    Ok(())
}

/// `ManageConfig` is Admin-only and human-session-only: nobody else may read
/// the values the whole fleet runs on, let alone replace them.
#[sqlx::test(migrator = "base::db::MIGRATOR")]
async fn only_an_admin_session_may_touch_the_document(pool: sqlx::PgPool) -> TestResult {
    let service = setup(pool);
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
                json: edited()?,
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
