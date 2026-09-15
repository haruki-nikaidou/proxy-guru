//! The Redis hop of the live bus, over a real server.
//!
//! Every other live test uses `LivePublisher::InProcess`, so this is the only
//! place the publish/subscribe wiring — the channel name, the rkyv payload and
//! the cross-replica delivery — is actually exercised. Two buses stand in for
//! two `guru-master` replicas: the services publish through one connection, and
//! a `LiveService` on the *other* bus has to see the change. Needs a reachable
//! Docker daemon; `testcontainers` starts and disposes of the server.

#![allow(clippy::unwrap_used, clippy::panic, clippy::expect_used)]

mod common;

use common::*;
use kanau::processor::Processor;
use orchestration::hooks::live::{LiveBus, LiveEvent, run_redis_subscriber};
use orchestration::services::canvas::CreateCanvas;
use orchestration::services::live::{LiveService, ViewValue};
use orchestration::services::notify::{LivePublisher, Notifier};
use orchestration::services::server::{AddressOverrides, CreateServer, ServerService};
use std::time::Duration;
use testcontainers_modules::redis::Redis;
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use tokio::sync::broadcast;
use tokio_util::sync::CancellationToken;

const WAIT: Duration = Duration::from_secs(10);

/// Waits for the first event on a bus, whatever it is.
async fn first_event(events: &mut broadcast::Receiver<LiveEvent>) -> LiveEvent {
    tokio::time::timeout(WAIT, events.recv())
        .await
        .expect("an event arrived")
        .expect("the bus is alive")
}

#[tokio::test]
async fn a_change_on_one_replica_refreshes_a_view_on_another() -> TestResult {
    let server = Redis::default().start().await?;
    let url = format!(
        "redis://{}:{}/",
        server.get_host().await?,
        server.get_host_port_ipv4(6379).await?
    );
    let client = redis::Client::open(url.as_str())?;

    // Two replicas' buses, both fed from the same Redis.
    let replica_a = LiveBus::new();
    let replica_b = LiveBus::new();
    let mut events_a = replica_a.subscribe();
    let mut events_b = replica_b.subscribe();
    let shutdown = CancellationToken::new();
    let sub_a = tokio::spawn(run_redis_subscriber(
        client.clone(),
        replica_a.clone(),
        shutdown.clone(),
    ));
    let sub_b = tokio::spawn(run_redis_subscriber(
        client.clone(),
        replica_b.clone(),
        shutdown.clone(),
    ));

    // Connecting is what emits the resync, and it is the first thing a watcher
    // sees: it is how a replica that just started knows to read the database.
    assert!(
        matches!(first_event(&mut events_a).await, LiveEvent::Resync),
        "replica A resyncs on connect"
    );
    assert!(
        matches!(first_event(&mut events_b).await, LiveEvent::Resync),
        "replica B resyncs on connect"
    );

    // The world's services publish to Redis, not to either bus directly.
    let w = world().await?;
    let publisher = Notifier {
        amqp: None,
        live: Some(LivePublisher::Redis(
            redis::aio::ConnectionManager::new(client.clone()).await?,
        )),
    };
    let servers = ServerService {
        db: w.db.clone(),
        notifier: publisher,
        config: w.config.clone(),
    };
    let canvas = w
        .canvases
        .process(CreateCanvas {
            actor: operator(),
            name: "prod".to_string(),
            description: String::new(),
        })
        .await?;

    // The watcher lives on replica B and shares its database with the publisher.
    let live = LiveService::new(w.db.clone(), replica_b.clone(), w.config.clone());
    let mut handle = live
        .process(orchestration::services::live::WatchCanvas {
            actor: operator(),
            canvas: canvas.id.clone(),
        })
        .await?;
    let opening = tokio::time::timeout(WAIT, async {
        loop {
            handle.rx.changed().await.expect("the view task is alive");
            let value = handle.rx.borrow_and_update().clone();
            if !matches!(value, ViewValue::Loading) {
                return value;
            }
        }
    })
    .await
    .expect("an opening snapshot");
    match &opening {
        ViewValue::Ready { state, .. } => assert!(state.contents.servers.is_empty()),
        other => panic!("unexpected opening value: {}", describe(other)),
    }

    servers
        .process(CreateServer {
            actor: operator(),
            canvas: canvas.id.clone(),
            name: "tokyo".to_string(),
            icon: String::new(),
            comment: String::new(),
            position: pos0(),
            ipv6_resolve:
                orchestration::entities::surreal::server::ServerIpv6Resolve::Tolerated,
            log_level: "info".to_string(),
            addresses: AddressOverrides {
                override_v4: Some("203.0.113.10".to_string()),
                override_v6: None,
                extra_addresses: Vec::new(),
            },
        })
        .await?;

    // The refresh proves the whole path: publish → Redis → replica B's
    // subscriber → its bus → the shared view → a reload from the database.
    let refreshed = tokio::time::timeout(WAIT, async {
        loop {
            handle.rx.changed().await.expect("the view task is alive");
            if let ViewValue::Ready { state, cause } = handle.rx.borrow_and_update().clone()
                && !state.contents.servers.is_empty()
            {
                return (state, cause);
            }
        }
    })
    .await
    .expect("a refreshed snapshot");
    let (state, cause) = refreshed;
    assert_eq!(state.contents.servers.len(), 1);
    assert_eq!(state.contents.servers[0].name, "tokyo");
    let cause = cause.expect("a cross-replica snapshot names its cause");
    assert!(
        matches!(
            &*cause,
            orchestration::events::live::LiveMessage::CanvasChanged {
                kind: orchestration::events::live::CanvasChangeKind::ServerCreated,
                ..
            }
        ),
        "the rkyv payload survived the round trip: {cause:?}"
    );

    // Replica A, which nobody is watching from, still received the event.
    let seen = tokio::time::timeout(WAIT, async {
        loop {
            if let LiveEvent::Message(message) = events_a.recv().await.expect("the bus is alive") {
                return message;
            }
        }
    })
    .await
    .expect("replica A saw the event too");
    assert!(matches!(
        &*seen,
        orchestration::events::live::LiveMessage::CanvasChanged { .. }
    ));

    drop(handle);
    shutdown.cancel();
    sub_a.await?;
    sub_b.await?;
    Ok(())
}

fn describe<T>(value: &ViewValue<T>) -> &'static str {
    match value {
        ViewValue::Loading => "loading",
        ViewValue::Missing => "missing",
        ViewValue::Ready { .. } => "ready",
    }
}
