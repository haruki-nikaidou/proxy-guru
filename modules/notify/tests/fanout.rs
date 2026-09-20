//! The fan-out's contract: which health facts become notices, and whose.
//!
//! Rows are inserted with raw statements rather than through `orchestration`'s
//! services: what is under test is the transition filter and the recipient
//! resolution, and a canvas with one server and one pod is all either needs.

#![allow(clippy::unwrap_used, clippy::panic, clippy::expect_used)]

use auth::entities::db::account::AccountId;
use kanau::processor::Processor;
use notify::entities::db::setting::{Language, NoticeKind};
use notify::events::{HealthNotifyGroupEvent, HealthNotifyPersonalEvent};
use notify::services::fanout::{
    FanOutHealthFacts, FanoutService, NoticePublisher, PublishedNotice,
};
use orchestration::entities::db::health::ServerHealthStatus;
use orchestration::events::HealthFact;
use std::sync::Arc;
use time::{Duration, OffsetDateTime};

type TestResult = Result<(), Box<dyn std::error::Error>>;

const CANVAS: &str = "cnv00000000000000001";
const CANVAS_NAME: &str = "Edge";
const SERVER: &str = "srv00000000000000001";
const SERVER_NAME: &str = "hkg-1";

/// The service under test plus the sink it published to.
struct World {
    fanout: FanoutService,
    published: Arc<tokio::sync::Mutex<Vec<PublishedNotice>>>,
    db: base::db::Db,
}

impl World {
    async fn new(pool: sqlx::PgPool) -> Result<Self, Box<dyn std::error::Error>> {
        let db = base::db::Db::new(pool);
        sqlx::query!(
            "INSERT INTO orchestration_canvas (id, name, description) VALUES ($1, $2, '')",
            CANVAS,
            CANVAS_NAME
        )
        .execute(db.db())
        .await?;
        sqlx::query!(
            "INSERT INTO orchestration_server
                 (id, canvas, name, icon, comment, position_x, position_y, ipv6_resolve, log_level)
             VALUES ($1, $2, $3, '', '', 0, 0, 'tolerated', 'info')",
            SERVER,
            CANVAS,
            SERVER_NAME
        )
        .execute(db.db())
        .await?;
        let publisher = NoticePublisher::collecting();
        let NoticePublisher::Collect(published) = publisher.clone() else {
            panic!("collecting() is a collecting publisher");
        };
        Ok(Self {
            fanout: FanoutService {
                db: db.clone(),
                publisher,
            },
            published,
            db,
        })
    }

    /// One server fact, as `orchestration` publishes it.
    async fn report(&self, status: ServerHealthStatus) -> TestResult {
        self.fanout
            .process(FanOutHealthFacts {
                facts: vec![HealthFact::Server {
                    server: SERVER.to_string(),
                    server_name: SERVER_NAME.to_string(),
                    canvas: CANVAS.to_string(),
                    canvas_name: CANVAS_NAME.to_string(),
                    status,
                    at_unix_micros: (OffsetDateTime::now_utc().unix_timestamp_nanos() / 1_000)
                        as i64,
                }],
            })
            .await?;
        Ok(())
    }

    /// Everything published since the last drain.
    async fn drain(&self) -> Vec<PublishedNotice> {
        std::mem::take(&mut *self.published.lock().await)
    }

    async fn groups(&self) -> Vec<HealthNotifyGroupEvent> {
        self.drain()
            .await
            .into_iter()
            .filter_map(|notice| match notice {
                PublishedNotice::Group(event) => Some(event),
                PublishedNotice::Personal(_) => None,
            })
            .collect()
    }

    async fn personals(&self) -> Vec<HealthNotifyPersonalEvent> {
        self.drain()
            .await
            .into_iter()
            .filter_map(|notice| match notice {
                PublishedNotice::Personal(event) => Some(event),
                PublishedNotice::Group(_) => None,
            })
            .collect()
    }

    async fn canvas_setting(&self, events: &[&str], emails: &[&str], chats: &[&str]) -> TestResult {
        sqlx::query!(
            "INSERT INTO notify_canvas_setting (canvas, language, events, emails, telegram_chats)
             VALUES ($1, 'ja', $2, $3, $4)",
            CANVAS,
            events as _,
            emails as _,
            chats as _
        )
        .execute(self.db.db())
        .await?;
        Ok(())
    }

    async fn account(&self, email: &str) -> Result<AccountId, Box<dyn std::error::Error>> {
        let id = AccountId::new();
        sqlx::query!(
            "INSERT INTO auth_account (id, email, password_hash, role)
             VALUES ($1, $2, '', 'maintainer')",
            id as _,
            email
        )
        .execute(self.db.db())
        .await?;
        Ok(id)
    }
}

/// A first sighting is recorded and announced to nobody: otherwise deploying
/// this module would notify every recipient about every server in the fleet.
/// The status it recorded is what makes the *next* fact a transition.
#[sqlx::test(migrator = "base::db::MIGRATOR")]
async fn a_first_sighting_is_recorded_and_announced_to_nobody(pool: sqlx::PgPool) -> TestResult {
    let w = World::new(pool).await?;
    w.canvas_setting(
        &["server_online", "server_offline"],
        &["ops@example.com"],
        &[],
    )
    .await?;

    w.report(ServerHealthStatus::Online).await?;
    assert!(
        w.drain().await.is_empty(),
        "the first fact about a server is not news"
    );
    let stored = sqlx::query_scalar!(
        r#"SELECT status AS "status!" FROM notify_server_state WHERE server = $1"#,
        SERVER
    )
    .fetch_one(w.db.db())
    .await?;
    assert_eq!(stored, "online");
    Ok(())
}

/// The transition itself: one group event, carrying the workspace's own
/// destinations and language — and nothing on a repeat of the same status.
#[sqlx::test(migrator = "base::db::MIGRATOR")]
async fn a_transition_reaches_the_workspace_once(pool: sqlx::PgPool) -> TestResult {
    let w = World::new(pool).await?;
    w.canvas_setting(
        &["server_offline"],
        &["ops@example.com", "oncall@example.com"],
        &["12345"],
    )
    .await?;
    w.report(ServerHealthStatus::Online).await?;
    w.drain().await;

    w.report(ServerHealthStatus::Offline).await?;
    let groups = w.groups().await;
    assert_eq!(groups.len(), 1, "{groups:?}");
    let event = &groups[0];
    assert_eq!(event.notice.kind, NoticeKind::ServerOffline);
    assert_eq!(event.notice.subject, SERVER);
    assert_eq!(event.notice.subject_name, SERVER_NAME);
    assert_eq!(event.notice.canvas_name, CANVAS_NAME);
    assert_eq!(event.language, Language::Ja);
    assert_eq!(
        event.emails,
        vec![
            "ops@example.com".to_string(),
            "oncall@example.com".to_string()
        ]
    );
    assert_eq!(event.telegram_chats, vec!["12345".to_string()]);

    w.report(ServerHealthStatus::Offline).await?;
    assert!(
        w.drain().await.is_empty(),
        "the same status reported again is not a transition"
    );
    Ok(())
}

/// A canvas whose `events` is empty — the schema default — hears nothing, and
/// neither does one that subscribed to a different kind.
#[sqlx::test(migrator = "base::db::MIGRATOR")]
async fn a_workspace_that_asked_for_nothing_hears_nothing(pool: sqlx::PgPool) -> TestResult {
    let w = World::new(pool).await?;
    w.canvas_setting(&[], &["ops@example.com"], &["12345"])
        .await?;
    w.report(ServerHealthStatus::Online).await?;
    w.drain().await;

    w.report(ServerHealthStatus::Offline).await?;
    assert!(w.drain().await.is_empty(), "no event was subscribed to");
    Ok(())
}

/// A canvas that subscribed but named no destination is not a recipient either:
/// publishing a notice nobody can receive would only make the notifier log it.
#[sqlx::test(migrator = "base::db::MIGRATOR")]
async fn a_workspace_with_no_destination_publishes_nothing(pool: sqlx::PgPool) -> TestResult {
    let w = World::new(pool).await?;
    w.canvas_setting(&["server_offline"], &[], &[]).await?;
    w.report(ServerHealthStatus::Online).await?;
    w.drain().await;

    w.report(ServerHealthStatus::Offline).await?;
    assert!(w.drain().await.is_empty());
    Ok(())
}

/// Personal resolution: an account with a row for the canvas is served by that
/// row and its default row is ignored; an account with only a default row is
/// served by it; an account whose resolved row did not subscribe hears nothing.
#[sqlx::test(migrator = "base::db::MIGRATOR")]
async fn personal_rows_beat_defaults_and_defaults_serve_the_rest(pool: sqlx::PgPool) -> TestResult {
    let w = World::new(pool).await?;
    let per_canvas = w.account("per-canvas@example.com").await?;
    let by_default = w.account("by-default@example.com").await?;
    let silent = w.account("silent@example.com").await?;

    // Subscribed for this canvas, in Chinese, by mail; its default row says
    // something else entirely and must not be consulted.
    sqlx::query!(
        "INSERT INTO notify_account_setting
             (account, canvas, language, events, email_enabled, telegram_chat)
         VALUES ($1, $2, 'zh_cn', ARRAY['server_offline'], true, NULL)",
        per_canvas as _,
        CANVAS
    )
    .execute(w.db.db())
    .await?;
    sqlx::query!(
        "INSERT INTO notify_account_default
             (account, language, events, email_enabled, telegram_chat)
         VALUES ($1, 'ja', ARRAY['pod_failed'], false, '999')",
        per_canvas as _
    )
    .execute(w.db.db())
    .await?;
    // No row for the canvas: its default row is what serves it.
    sqlx::query!(
        "INSERT INTO notify_account_default
             (account, language, events, email_enabled, telegram_chat)
         VALUES ($1, 'ja', ARRAY['server_offline'], false, '4242')",
        by_default as _
    )
    .execute(w.db.db())
    .await?;
    // Subscribed to a different kind.
    sqlx::query!(
        "INSERT INTO notify_account_default
             (account, language, events, email_enabled, telegram_chat)
         VALUES ($1, 'en', ARRAY['server_online'], true, NULL)",
        silent as _
    )
    .execute(w.db.db())
    .await?;

    w.report(ServerHealthStatus::Online).await?;
    w.drain().await;
    w.report(ServerHealthStatus::Offline).await?;

    let mut events = w.personals().await;
    events.sort_by(|a, b| a.account.cmp(&b.account));
    let mut expected = vec![
        (
            per_canvas.to_string(),
            Language::ZhCn,
            Some("per-canvas@example.com".to_string()),
            None,
        ),
        (
            by_default.to_string(),
            Language::Ja,
            None,
            Some("4242".to_string()),
        ),
    ];
    expected.sort_by(|a, b| a.0.cmp(&b.0));
    assert_eq!(
        events
            .iter()
            .map(|event| (
                event.account.clone(),
                event.language,
                event.email.clone(),
                event.telegram_chat.clone()
            ))
            .collect::<Vec<_>>(),
        expected
    );
    for event in &events {
        assert_eq!(event.notice.kind, NoticeKind::ServerOffline);
    }
    Ok(())
}

/// A server that was deleted between the write and the fan-out records nothing
/// and reads as a first sighting: there is nobody left to announce, and the
/// foreign key must not turn a lost pod into a delivery that requeues forever.
#[sqlx::test(migrator = "base::db::MIGRATOR")]
async fn a_vanished_subject_is_dropped_not_retried(pool: sqlx::PgPool) -> TestResult {
    let w = World::new(pool).await?;
    w.canvas_setting(&["server_offline"], &["ops@example.com"], &[])
        .await?;
    w.report(ServerHealthStatus::Online).await?;
    w.drain().await;
    sqlx::query!("DELETE FROM orchestration_server WHERE id = $1", SERVER)
        .execute(w.db.db())
        .await?;

    w.report(ServerHealthStatus::Offline).await?;
    assert!(w.drain().await.is_empty());
    Ok(())
}

/// A repeated pod fact in one batch is asked about once: the statement would
/// error on a second hit of the same row, and the newest fact is the one that
/// counts.
#[sqlx::test(migrator = "base::db::MIGRATOR")]
async fn one_batch_asks_about_each_pod_once(pool: sqlx::PgPool) -> TestResult {
    let w = World::new(pool).await?;
    w.canvas_setting(&["pod_ready", "pod_failed"], &["ops@example.com"], &[])
        .await?;
    let pod = "pod00000000000000001";
    sqlx::query!(
        "INSERT INTO orchestration_pod (id, canvas, server, name, port, ingress)
         VALUES ($1, $2, $3, 'web', 443, 'client_raw')",
        pod,
        CANVAS,
        SERVER
    )
    .execute(w.db.db())
    .await?;
    let fact = |status: orchestration::entities::db::health::PodHealthStatus, message: &str| {
        HealthFact::Pod {
            pod: pod.to_string(),
            pod_name: "web".to_string(),
            canvas: CANVAS.to_string(),
            canvas_name: CANVAS_NAME.to_string(),
            status,
            message: message.to_string(),
            at_unix_micros: (OffsetDateTime::now_utc().unix_timestamp_nanos() / 1_000) as i64,
        }
    };
    use orchestration::entities::db::health::PodHealthStatus;

    // First sighting of the pod, twice in one batch: recorded once, announced
    // to nobody.
    w.fanout
        .process(FanOutHealthFacts {
            facts: vec![
                fact(PodHealthStatus::Ready, ""),
                fact(PodHealthStatus::Ready, ""),
            ],
        })
        .await?;
    assert!(w.drain().await.is_empty());

    // Two facts about one pod: the last one is the transition that is announced.
    w.fanout
        .process(FanOutHealthFacts {
            facts: vec![
                fact(PodHealthStatus::Ready, ""),
                fact(PodHealthStatus::Failed, "bind: address in use"),
            ],
        })
        .await?;
    let groups = w.groups().await;
    assert_eq!(groups.len(), 1, "{groups:?}");
    assert_eq!(groups[0].notice.kind, NoticeKind::PodFailed);
    assert_eq!(groups[0].notice.message, "bind: address in use");
    Ok(())
}

/// The state row remembers when it last moved, so an operator reading the table
/// can tell a stuck subject from a quiet one.
#[sqlx::test(migrator = "base::db::MIGRATOR")]
async fn an_unchanged_status_leaves_its_timestamp_alone(pool: sqlx::PgPool) -> TestResult {
    let w = World::new(pool).await?;
    w.report(ServerHealthStatus::Online).await?;
    let first = sqlx::query_scalar!(
        r#"SELECT changed_at AS "changed_at!" FROM notify_server_state WHERE server = $1"#,
        SERVER
    )
    .fetch_one(w.db.db())
    .await?;
    sqlx::query!(
        "UPDATE notify_server_state SET changed_at = $2 WHERE server = $1",
        SERVER,
        first - Duration::hours(1)
    )
    .execute(w.db.db())
    .await?;

    w.report(ServerHealthStatus::Online).await?;
    let again = sqlx::query_scalar!(
        r#"SELECT changed_at AS "changed_at!" FROM notify_server_state WHERE server = $1"#,
        SERVER
    )
    .fetch_one(w.db.db())
    .await?;
    assert_eq!(again, first - Duration::hours(1));
    Ok(())
}
