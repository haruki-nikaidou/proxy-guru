//! The AMQP path end to end, over a real broker: an edit publishes `CanvasDirty`
//! and the consumer derives the canvas, and a periodic execution signal from the
//! scheduler reaches the same hook.
//!
//! Every other test drives the deriver or a pass directly, so this is the only
//! place the publish/consume wiring — exchange, queue, routing key and payload
//! encoding — is actually exercised. It needs a reachable Docker daemon;
//! `testcontainers` starts and disposes of the broker.

#![allow(clippy::unwrap_used, clippy::panic, clippy::expect_used)]

mod common;

use common::*;
use kanau::message::{MessageDe, MessageSer};
use kanau::processor::Processor;
use orchestration::config::OrchestrationConfig;
use orchestration::entities::db::server::{ServerIpv6Resolve, ServerLogLevel};
use orchestration::entities::db::view::FindServerConfigView;
use orchestration::events::{
    CanvasDirty, DeriveStaleCanvasesSignal, RotateRelayCertificatesSignal,
};
use orchestration::hooks::derive::CanvasDeriver;
use orchestration::services::canvas::{CanvasService, CreateCanvas};
use orchestration::services::graph::{ApplyGraph, GraphChange, GraphService};
use orchestration::services::notify::Notifier;
use orchestration::services::server::{AddressOverrides, CreateServer, ServerService};
use orchestration::utils::secret::SecretKey;
use std::sync::Arc;
use std::time::Duration;
use testcontainers_modules::rabbitmq::RabbitMq;
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use wakuwaku::integration::amqp::{
    AmqpMessageProcessor, AmqpMessageSend, AmqpPool, AmqpRouting, setup_consumer,
};
use wakuwaku::interval_job::IntervalJobExecutionSignal;

#[sqlx::test(migrator = "base::db::MIGRATOR")]
async fn an_edit_reaches_the_deriver_through_the_broker(db_pool: sqlx::PgPool) -> TestResult {
    let broker = RabbitMq::default().start().await?;
    let uri = format!(
        "amqp://guest:guest@{}:{}",
        broker.get_host().await?,
        broker.get_host_port_ipv4(5672).await?
    );

    let args = amqprs::connection::OpenConnectionArguments::try_from(uri.as_str())?;
    let connection = amqprs::connection::Connection::open(&args).await?;
    connection
        .register_callback(amqprs::callbacks::DefaultConnectionCallback)
        .await?;
    let pool = AmqpPool::connect(connection.clone()).await;

    let db = setup(db_pool);
    let channel = <CanvasDeriver as AmqpMessageProcessor<CanvasDirty>>::ensure_queue(&pool).await?;
    channel
        .register_callback(amqprs::callbacks::DefaultChannelCallback)
        .await?;
    setup_consumer::<CanvasDirty, CanvasDeriver>(
        &channel,
        Arc::new(CanvasDeriver {
            db: db.clone(),
            secrets: SecretKey::from_base64(&SecretKey::generate_base64())?,
            config: Default::default(),
            notifier: Notifier::default(),
        }),
    )
    .await?;

    // Services publish; nothing in this test derives anything itself, and no sweep
    // is running, so only the broker can make the view move.
    let notifier = Notifier {
        amqp: Some(pool.clone()),
        live: None,
    };
    let canvases = CanvasService {
        db: db.clone(),
        notifier: notifier.clone(),
    };
    let servers = ServerService {
        db: db.clone(),
        notifier: notifier.clone(),
        config: OrchestrationConfig::default(),
    };
    let graph = GraphService {
        db: db.clone(),
        notifier,
        config: OrchestrationConfig::default(),
    };

    let canvas = canvases
        .process(CreateCanvas {
            actor: operator(),
            name: "prod".to_string(),
            description: String::new(),
            parent: None,
            position: pos0(),
        })
        .await?;
    let server = servers
        .process(CreateServer {
            actor: operator(),
            canvas: canvas.id.clone(),
            name: "tokyo".to_string(),
            icon: String::new(),
            comment: String::new(),
            position: pos0(),
            ipv6_resolve: ServerIpv6Resolve::Tolerated,
            log_level: ServerLogLevel::Info,
            addresses: AddressOverrides {
                override_v4: Some("203.0.113.10".to_string()),
                override_v6: None,
                extra_addresses: Vec::new(),
            },
        })
        .await?;
    let entry = client(&canvas, &server, "edge", 443, None);
    let origin = exit(&canvas, "exit", "10.0.0.5:8080");
    let out = edge_to_exit("out", &entry, &origin);
    let outcome = graph
        .process(ApplyGraph {
            actor: operator(),
            canvas: canvas.id.clone(),
            change: GraphChange {
                put_pods: vec![routed(entry, via(&out))],
                put_exits: vec![origin],
                put_edges: vec![out],
                ..GraphChange::default()
            },
            dry_run: false,
            expected_generation: None,
        })
        .await?;
    assert!(outcome.applied, "{:?}", outcome.diagnostics);

    // Every edit publishes, so the first snapshot to arrive is the empty config the
    // bare server derived to. What proves the wiring is that the *last* edit also
    // gets through, with no sweep running to paper over a lost message.
    let deadline = std::time::Instant::now() + Duration::from_secs(30);
    let desired = loop {
        let view = db
            .process(FindServerConfigView {
                server: server.id.clone(),
            })
            .await?
            .unwrap();
        match view.desired {
            Some(desired) if desired.toml.contains("[::]:443") => break desired,
            other => assert!(
                std::time::Instant::now() < deadline,
                "the broker never drove the last edit: desired={:?} error={:?}",
                other.map(|s| s.revision),
                view.derive_error
            ),
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    };
    assert_eq!(
        desired.forwardings.len(),
        1,
        "the derived snapshot carries the listener it serves"
    );
    assert_eq!(desired.forwardings[0].serves.port, 443);

    drop(channel);
    connection.close().await?;
    Ok(())
}

/// The scheduler's half of the split: `cron` publishes an execution signal and
/// nothing else, and the sweep runs in the consumer. Only the signal queue is
/// bound here and nothing publishes `CanvasDirty`, so the canvas can only be
/// derived by the periodic path.
#[sqlx::test(migrator = "base::db::MIGRATOR")]
async fn a_periodic_signal_reaches_its_hook_through_the_broker(
    db_pool: sqlx::PgPool,
) -> TestResult {
    let broker = RabbitMq::default().start().await?;
    let uri = format!(
        "amqp://guest:guest@{}:{}",
        broker.get_host().await?,
        broker.get_host_port_ipv4(5672).await?
    );
    let args = amqprs::connection::OpenConnectionArguments::try_from(uri.as_str())?;
    let connection = amqprs::connection::Connection::open(&args).await?;
    connection
        .register_callback(amqprs::callbacks::DefaultConnectionCallback)
        .await?;
    let pool = AmqpPool::connect(connection.clone()).await;

    let db = setup(db_pool);
    let deriver = CanvasDeriver {
        db: db.clone(),
        secrets: SecretKey::from_base64(&SecretKey::generate_base64())?,
        config: OrchestrationConfig::default(),
        notifier: Notifier::default(),
    };
    let channel =
        <CanvasDeriver as AmqpMessageProcessor<DeriveStaleCanvasesSignal>>::ensure_queue(&pool)
            .await?;
    channel
        .register_callback(amqprs::callbacks::DefaultChannelCallback)
        .await?;
    setup_consumer::<DeriveStaleCanvasesSignal, CanvasDeriver>(&channel, Arc::new(deriver)).await?;

    // Built with a notifier that publishes nothing: the canvas is left stale, the
    // way it would be after a lost `CanvasDirty` or a broker outage.
    let canvases = CanvasService {
        db: db.clone(),
        notifier: Notifier::default(),
    };
    let servers = ServerService {
        db: db.clone(),
        notifier: Notifier::default(),
        config: OrchestrationConfig::default(),
    };
    let graph = GraphService {
        db: db.clone(),
        notifier: Notifier::default(),
        config: OrchestrationConfig::default(),
    };
    let canvas = canvases
        .process(CreateCanvas {
            actor: operator(),
            name: "prod".to_string(),
            description: String::new(),
            parent: None,
            position: pos0(),
        })
        .await?;
    let server = servers
        .process(CreateServer {
            actor: operator(),
            canvas: canvas.id.clone(),
            name: "tokyo".to_string(),
            icon: String::new(),
            comment: String::new(),
            position: pos0(),
            ipv6_resolve: ServerIpv6Resolve::Tolerated,
            log_level: ServerLogLevel::Info,
            addresses: AddressOverrides {
                override_v4: Some("203.0.113.20".to_string()),
                override_v6: None,
                extra_addresses: Vec::new(),
            },
        })
        .await?;
    let entry = client(&canvas, &server, "edge", 443, None);
    let origin = exit(&canvas, "exit", "10.0.0.6:8080");
    let out = edge_to_exit("out", &entry, &origin);
    let outcome = graph
        .process(ApplyGraph {
            actor: operator(),
            canvas: canvas.id.clone(),
            change: GraphChange {
                put_pods: vec![routed(entry, via(&out))],
                put_exits: vec![origin],
                put_edges: vec![out],
                ..GraphChange::default()
            },
            dry_run: false,
            expected_generation: None,
        })
        .await?;
    assert!(outcome.applied, "{:?}", outcome.diagnostics);

    // Nothing has derived anything yet: no edit was announced.
    let view = db
        .process(FindServerConfigView {
            server: server.id.clone(),
        })
        .await?
        .unwrap();
    assert!(
        view.desired.is_none(),
        "the canvas is stale until the sweep runs: {:?}",
        view.desired.map(|s| s.revision)
    );

    DeriveStaleCanvasesSignal {
        tick_unix_secs: time::OffsetDateTime::now_utc().unix_timestamp(),
    }
    .send(&pool)
    .await?;

    let deadline = std::time::Instant::now() + Duration::from_secs(30);
    loop {
        let view = db
            .process(FindServerConfigView {
                server: server.id.clone(),
            })
            .await?
            .unwrap();
        match view.desired {
            Some(desired) if desired.toml.contains("[::]:443") => break,
            other => assert!(
                std::time::Instant::now() < deadline,
                "the signal never drove the sweep: desired={:?} error={:?}",
                other.map(|s| s.revision),
                view.derive_error
            ),
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    drop(channel);
    connection.close().await?;
    Ok(())
}

/// A tick nobody consumed is dropped by the broker instead of piling up.
///
/// `cron` publishes on each job's own cadence whether a consumer is alive or
/// not, so with the consumer fleet down the signal queues are what grows: the
/// run claim discards a stale delivery, but only after the queue held it and a
/// consumer decoded it. No consumer is bound here — the queues are declared and
/// left alone, the way they are during an outage.
#[tokio::test]
async fn an_unconsumed_periodic_signal_expires_at_the_broker() -> TestResult {
    let broker = RabbitMq::default().start().await?;
    let uri = format!(
        "amqp://guest:guest@{}:{}",
        broker.get_host().await?,
        broker.get_host_port_ipv4(5672).await?
    );
    let args = amqprs::connection::OpenConnectionArguments::try_from(uri.as_str())?;
    let connection = amqprs::connection::Connection::open(&args).await?;
    connection
        .register_callback(amqprs::callbacks::DefaultConnectionCallback)
        .await?;
    let pool = AmqpPool::connect(connection.clone()).await;

    let stale =
        <CanvasDeriver as AmqpMessageProcessor<DeriveStaleCanvasesSignal>>::ensure_queue(&pool)
            .await?;
    let rotate =
        <CanvasDeriver as AmqpMessageProcessor<RotateRelayCertificatesSignal>>::ensure_queue(&pool)
            .await?;

    let now = time::OffsetDateTime::now_utc();
    DeriveStaleCanvasesSignal::tick(now).send(&pool).await?;
    RotateRelayCertificatesSignal::tick(now).send(&pool).await?;

    let stale_queue =
        <CanvasDeriver as AmqpMessageProcessor<DeriveStaleCanvasesSignal>>::QUEUE.to_string();
    let rotate_queue =
        <CanvasDeriver as AmqpMessageProcessor<RotateRelayCertificatesSignal>>::QUEUE.to_string();
    let (_, properties, payload) = get_one(&stale, &stale_queue)
        .await?
        .expect("the 30 s signal reached its queue");
    assert_eq!(
        properties.expiration().map(String::as_str),
        Some("30000"),
        "a tick carries its own cadence as a per-message TTL"
    );
    assert_eq!(
        DeriveStaleCanvasesSignal::from_bytes(&payload)
            .expect("the payload still decodes")
            .tick_unix_secs,
        now.unix_timestamp(),
        "the TTL rides along with the payload, it does not replace it"
    );
    let (_, properties, _) = get_one(&rotate, &rotate_queue)
        .await?
        .expect("the hourly signal reached its queue");
    assert_eq!(
        properties.expiration().map(String::as_str),
        Some("3600000"),
        "each job's TTL is its own cadence, not one constant for the fleet"
    );

    // And the broker acts on it with nothing consuming: the expired tick leaves
    // the queue by itself, which is what bounds a backlog during an outage.
    stale
        .basic_publish(
            amqprs::BasicProperties::default()
                .with_expiration("200")
                .finish(),
            DeriveStaleCanvasesSignal::tick(now).to_bytes()?.into_vec(),
            amqprs::channel::BasicPublishArguments::new(
                DeriveStaleCanvasesSignal::EXCHANGE,
                DeriveStaleCanvasesSignal::ROUTING_KEY,
            )
            .mandatory(true)
            .finish(),
        )
        .await?;
    tokio::time::sleep(Duration::from_secs(2)).await;
    assert!(
        stale
            .basic_get(
                amqprs::channel::BasicGetArguments::new(&stale_queue)
                    .no_ack(true)
                    .finish()
            )
            .await?
            .is_none(),
        "an expired tick is still queued: the backlog is unbounded again"
    );

    drop(stale);
    drop(rotate);
    connection.close().await?;
    Ok(())
}

/// One message off `queue`, waiting for the publish to be routed.
async fn get_one(
    channel: &amqprs::channel::Channel,
    queue: &str,
) -> Result<Option<amqprs::channel::GetMessage>, Box<dyn std::error::Error>> {
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    loop {
        let got = channel
            .basic_get(
                amqprs::channel::BasicGetArguments::new(queue)
                    .no_ack(true)
                    .finish(),
            )
            .await?;
        if got.is_some() || std::time::Instant::now() >= deadline {
            return Ok(got);
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}
