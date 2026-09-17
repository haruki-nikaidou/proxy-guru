//! The master's country lookup: which addresses it asks about, what it stores,
//! and when the dashboard gets to see an answer.

#![allow(clippy::unwrap_used, clippy::panic, clippy::expect_used)]

mod common;

use base::db::Db;
use chrono::{DateTime, TimeDelta, Utc};
use common::*;
use kanau::processor::Processor;
use orchestration::config::OrchestrationConfig;
use orchestration::entities::db::server::{FindServerById, ServerEntity, ServerId};
use orchestration::services::country::{CountryPass, CountryService, ResolveServerCountries};
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::sync::Mutex;

/// A lookup service on loopback. `GET /<ip>` is answered from `answers`
/// (address, status, body), with a 404 for any other address, and every address
/// asked about is recorded. The URL it returns is a `country_lookup_url` template.
async fn lookup_service(
    answers: Vec<(&'static str, u16, &'static str)>,
) -> (String, Arc<Mutex<Vec<String>>>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let template = format!("http://{}/{{ip}}", listener.local_addr().unwrap());
    let asked = Arc::new(Mutex::new(Vec::new()));
    let log = asked.clone();
    tokio::spawn(async move {
        while let Ok((mut socket, _)) = listener.accept().await {
            let mut head = Vec::new();
            let mut chunk = [0u8; 1024];
            while !head.windows(4).any(|w| w == b"\r\n\r\n") {
                let read = socket.read(&mut chunk).await.unwrap_or(0);
                if read == 0 {
                    break;
                }
                head.extend_from_slice(&chunk[..read]);
            }
            let head = String::from_utf8_lossy(&head).to_string();
            let address = head
                .split(' ')
                .nth(1)
                .unwrap_or_default()
                .trim_start_matches('/')
                .to_string();
            let (status, body) = answers
                .iter()
                .find(|(ip, _, _)| *ip == address)
                .map_or((404, ""), |(_, status, body)| (*status, *body));
            log.lock().await.push(address);
            let response = format!(
                "HTTP/1.1 {status} Answer\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
                body.len()
            );
            let _ = socket.write_all(response.as_bytes()).await;
        }
    });
    (template, asked)
}

fn service(db: &Db, template: &str) -> CountryService {
    CountryService {
        db: db.clone(),
        config: OrchestrationConfig {
            country_lookup_url: template.to_string(),
            ..OrchestrationConfig::default()
        },
        http: reqwest::Client::new(),
    }
}

async fn row(db: &Db, id: &ServerId) -> ServerEntity {
    db.process(FindServerById { id: id.clone() })
        .await
        .unwrap()
        .unwrap()
}

async fn pin_v4(db: &Db, id: &ServerId, address: &str) {
    sqlx::query!(
        "UPDATE orchestration_server SET override_v4 = $2 WHERE id = $1",
        id as _,
        address
    )
    .execute(db.db())
    .await
    .unwrap();
}

async fn pass(service: &CountryService, now: DateTime<Utc>) -> CountryPass {
    service
        .process(ResolveServerCountries { now })
        .await
        .unwrap()
}

#[sqlx::test(migrator = "base::db::MIGRATOR")]
async fn each_public_address_is_looked_up_once(pool: sqlx::PgPool) -> TestResult {
    let db = setup(pool);
    let c = canvas(&db, "prod").await?;
    let hk_a = server_at(&db, &c, "hk-a", "185.14.47.132").await?;
    let hk_b = server_at(&db, &c, "hk-b", "185.14.47.132").await?;
    let cn = server_at(&db, &c, "cn", "120.199.9.67").await?;
    let lan = server_at(&db, &c, "lan", "10.0.0.5").await?;
    let (template, asked) = lookup_service(vec![
        (
            "185.14.47.132",
            200,
            r#"{"ip":"185.14.47.132","country":"HK"}"#,
        ),
        ("120.199.9.67", 200, "CN\n"),
    ])
    .await;
    let service = service(&db, &template);
    let now = Utc::now();

    let first = pass(&service, now).await;
    assert_eq!(
        first,
        CountryPass {
            looked_up: 2,
            resolved: 2,
            cleared: 0
        }
    );
    let mut seen = asked.lock().await.clone();
    seen.sort();
    assert_eq!(seen, ["120.199.9.67", "185.14.47.132"]);

    for (server, country) in [(&hk_a, "HK"), (&hk_b, "HK"), (&cn, "CN")] {
        let stored = row(&db, &server.id).await;
        assert_eq!(stored.country.as_deref(), Some(country));
        assert_eq!(stored.country_of_v4(), Some(country));
        assert!(stored.country_checked_at.is_some());
    }
    let lan = row(&db, &lan.id).await;
    assert_eq!(lan.country_address, None);
    assert_eq!(lan.country_of_v4(), None);

    // Nothing changed, so the next pass asks nobody.
    assert_eq!(pass(&service, now).await, CountryPass::default());
    assert_eq!(asked.lock().await.len(), 2);
    Ok(())
}

#[sqlx::test(migrator = "base::db::MIGRATOR")]
async fn a_moved_address_hides_the_old_answer_until_it_is_looked_up(
    pool: sqlx::PgPool,
) -> TestResult {
    let db = setup(pool);
    let c = canvas(&db, "prod").await?;
    let server = server_at(&db, &c, "edge", "185.14.47.132").await?;
    let (template, asked) = lookup_service(vec![
        ("185.14.47.132", 200, "HK"),
        ("120.199.9.67", 200, "CN"),
    ])
    .await;
    let service = service(&db, &template);
    let now = Utc::now();
    pass(&service, now).await;
    assert_eq!(row(&db, &server.id).await.country_of_v4(), Some("HK"));

    pin_v4(&db, &server.id, "120.199.9.67").await;
    // The stored answer is for the old address: never shown for the new one.
    let moved = row(&db, &server.id).await;
    assert_eq!(moved.country.as_deref(), Some("HK"));
    assert_eq!(moved.country_of_v4(), None);

    let second = pass(&service, now).await;
    assert_eq!(second.looked_up, 1);
    assert_eq!(row(&db, &server.id).await.country_of_v4(), Some("CN"));
    assert_eq!(
        asked.lock().await.as_slice(),
        ["185.14.47.132", "120.199.9.67"]
    );

    // A private address has no country: the stored lookup is dropped.
    pin_v4(&db, &server.id, "10.0.0.5").await;
    let third = pass(&service, now).await;
    assert_eq!(
        third,
        CountryPass {
            looked_up: 0,
            resolved: 0,
            cleared: 1
        }
    );
    let cleared = row(&db, &server.id).await;
    assert_eq!(cleared.country, None);
    assert_eq!(cleared.country_address, None);
    assert_eq!(cleared.country_checked_at, None);
    Ok(())
}

#[sqlx::test(migrator = "base::db::MIGRATOR")]
async fn a_failed_lookup_is_retried_only_after_the_delay(pool: sqlx::PgPool) -> TestResult {
    let db = setup(pool);
    let c = canvas(&db, "prod").await?;
    let server = server_at(&db, &c, "edge", "8.8.8.8").await?;
    let (template, asked) = lookup_service(vec![("8.8.8.8", 500, "upstream down")]).await;
    let service = service(&db, &template);
    let now = Utc::now();

    let failed = pass(&service, now).await;
    assert_eq!(
        failed,
        CountryPass {
            looked_up: 1,
            resolved: 0,
            cleared: 0
        }
    );
    let stored = row(&db, &server.id).await;
    assert_eq!(stored.country, None);
    assert_eq!(stored.country_address.as_deref(), Some("8.8.8.8"));
    assert!(stored.country_checked_at.is_some());

    // The default delay is an hour.
    let soon = now + TimeDelta::minutes(30);
    assert_eq!(pass(&service, soon).await, CountryPass::default());
    assert_eq!(asked.lock().await.len(), 1);

    let later = now + TimeDelta::minutes(61);
    assert_eq!(pass(&service, later).await.looked_up, 1);
    assert_eq!(asked.lock().await.len(), 2);
    Ok(())
}

#[sqlx::test(migrator = "base::db::MIGRATOR")]
async fn an_empty_url_turns_the_lookup_off(pool: sqlx::PgPool) -> TestResult {
    let db = setup(pool);
    let c = canvas(&db, "prod").await?;
    let server = server_at(&db, &c, "edge", "8.8.8.8").await?;
    let service = service(&db, "  ");
    assert_eq!(pass(&service, Utc::now()).await, CountryPass::default());
    assert_eq!(row(&db, &server.id).await.country_checked_at, None);
    Ok(())
}
