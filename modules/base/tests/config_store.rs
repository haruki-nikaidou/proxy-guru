//! The configuration store against a real PostgreSQL database.
//!
//! `#[sqlx::test]` gives every test a fresh database, migrated with the
//! workspace schema, so `ConfigStore` is exercised the way the binaries use it.

#![allow(clippy::unwrap_used, clippy::panic)]

use base::db::Db;
use base::entities::db::app_config::{ConfigJson, FindRawConfig, UpsertRawConfig};
use base::services::config::{
    ConfigError, ConfigStore, LoadConfig, SeedConfig, StoreConfig, decode,
};
use kanau::processor::Processor;
use serde::{Deserialize, Serialize};

type TestResult = Result<(), Box<dyn std::error::Error>>;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum Strategy {
    RoundRobin,
    IpHash { salt: String },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct Limits {
    max_items: u32,
    labels: Vec<String>,
}

/// A config with the shapes a real module uses: nested struct, enum with a
/// payload, collection.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
struct SampleConfig {
    enabled: bool,
    strategy: Strategy,
    limits: Limits,
}

impl Default for SampleConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            strategy: Strategy::RoundRobin,
            limits: Limits {
                max_items: 42,
                labels: vec!["default".to_string()],
            },
        }
    }
}

impl ConfigJson for SampleConfig {
    const KEY: &'static str = "sample";
}

fn setup(pool: sqlx::PgPool) -> ConfigStore {
    ConfigStore { db: Db::new(pool) }
}

/// An unseeded installation is a normal state, not an error.
#[sqlx::test(migrator = "base::db::MIGRATOR")]
async fn absent_key_loads_defaults(pool: sqlx::PgPool) -> TestResult {
    let store = setup(pool);
    let loaded: SampleConfig = store.process(LoadConfig::new()).await?;
    assert_eq!(loaded, SampleConfig::default());
    Ok(())
}

/// Seeding is idempotent and never clobbers an operator's edit — the whole
/// point of running it after every schema sync.
#[sqlx::test(migrator = "base::db::MIGRATOR")]
async fn seeding_is_idempotent_and_preserves_edits(pool: sqlx::PgPool) -> TestResult {
    let store = setup(pool);
    assert!(store.process(SeedConfig::<SampleConfig>::new()).await?);
    assert!(!store.process(SeedConfig::<SampleConfig>::new()).await?);

    let edited = SampleConfig {
        enabled: false,
        ..SampleConfig::default()
    };
    store.process(StoreConfig(edited.clone())).await?;
    assert!(!store.process(SeedConfig::<SampleConfig>::new()).await?);

    let loaded: SampleConfig = store.process(LoadConfig::new()).await?;
    assert_eq!(loaded, edited);
    Ok(())
}

/// Nested structs, an enum with a payload and a collection survive the trip
/// through `jsonb`, not just `serde_json`.
#[sqlx::test(migrator = "base::db::MIGRATOR")]
async fn nested_config_round_trips_through_the_database(pool: sqlx::PgPool) -> TestResult {
    let store = setup(pool);
    let written = SampleConfig {
        enabled: false,
        strategy: Strategy::IpHash {
            salt: "pepper".to_string(),
        },
        limits: Limits {
            max_items: 7,
            labels: vec!["a".to_string(), "b".to_string()],
        },
    };
    store.process(StoreConfig(written.clone())).await?;

    let loaded: SampleConfig = store.process(LoadConfig::new()).await?;
    assert_eq!(loaded, written);
    assert_eq!(
        store
            .db
            .process(FindRawConfig {
                key: SampleConfig::KEY
            })
            .await?,
        Some(serde_json::to_value(&written)?)
    );
    Ok(())
}

/// A payload that does not match its type fails the read, naming the key.
/// Substituting defaults here would silently replace an operator's whole
/// config.
#[sqlx::test(migrator = "base::db::MIGRATOR")]
async fn undeserializable_payload_fails_the_read(pool: sqlx::PgPool) -> TestResult {
    let store = setup(pool);
    store
        .db
        .process(UpsertRawConfig {
            key: SampleConfig::KEY,
            content: serde_json::json!({ "limits": "unbounded" }),
        })
        .await?;

    let error = store
        .process(LoadConfig::<SampleConfig>::new())
        .await
        .expect_err("a config that does not match its type must not load");
    match error {
        ConfigError::Decode { key, .. } => assert_eq!(key, "sample"),
        other => panic!("expected a decode error, got {other:?}"),
    }
    Ok(())
}

/// A row written before a field was added still loads: the missing field falls
/// back to `Default`, which is what makes additive config changes cheap.
#[sqlx::test(migrator = "base::db::MIGRATOR")]
async fn row_missing_an_added_field_loads_with_its_default(pool: sqlx::PgPool) -> TestResult {
    let store = setup(pool);
    store
        .db
        .process(UpsertRawConfig {
            key: SampleConfig::KEY,
            content: serde_json::json!({ "enabled": false }),
        })
        .await?;

    let loaded: SampleConfig = store.process(LoadConfig::new()).await?;
    assert!(!loaded.enabled);
    assert_eq!(loaded.strategy, SampleConfig::default().strategy);
    assert_eq!(loaded.limits, SampleConfig::default().limits);
    Ok(())
}

/// An operator payload is decoded before it is stored, so a wrong-shaped one
/// cannot land in the row.
#[sqlx::test(migrator = "base::db::MIGRATOR")]
async fn a_payload_that_is_not_the_config_never_reaches_the_row(pool: sqlx::PgPool) -> TestResult {
    let store = setup(pool);
    let error = decode::<SampleConfig>(serde_json::json!({ "limits": 3 }))
        .expect_err("an invalid payload must be rejected");
    assert!(matches!(error, ConfigError::Decode { key: "sample", .. }));
    assert_eq!(
        store
            .db
            .process(FindRawConfig {
                key: SampleConfig::KEY
            })
            .await?,
        None
    );
    Ok(())
}

/// A row an operator has to repair must stay readable: `stored` never decodes,
/// so `manage-tool config get` works on exactly the document that fails the
/// typed startup read.
#[sqlx::test(migrator = "base::db::MIGRATOR")]
async fn a_corrupt_row_is_still_readable_untyped(pool: sqlx::PgPool) -> TestResult {
    let store = setup(pool);
    let corrupt = serde_json::json!({ "limits": "unbounded" });
    store
        .db
        .process(UpsertRawConfig {
            key: SampleConfig::KEY,
            content: corrupt.clone(),
        })
        .await?;

    assert!(
        store
            .process(LoadConfig::<SampleConfig>::new())
            .await
            .is_err()
    );
    assert_eq!(
        store
            .db
            .process(FindRawConfig {
                key: SampleConfig::KEY
            })
            .await?,
        Some(corrupt)
    );
    Ok(())
}
