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

use auth::entities::db::account::{AccountId, AccountRole};
use auth::services::identity::{Identity, IdentityKind};
use common::*;
use kanau::processor::Processor;
use orchestration::config::OrchestrationConfig;
use orchestration::entities::db::canvas::CanvasUiPosition;
use orchestration::entities::db::node::{EntryConfig, ExitConfig, NodeSpec, PodConfig};
use orchestration::entities::db::server::ServerIpv6Resolve;
use orchestration::entities::db::view::FindServerConfigView;
use orchestration::events::{CanvasDirty, DeriveStaleCanvasesSignal};
use orchestration::hooks::derive::CanvasDeriver;
use orchestration::services::canvas::{CanvasService, CreateCanvas};
use orchestration::services::edge::{Connect, EdgeService};
use orchestration::services::node::{CreateNode, NodeService};
use orchestration::services::notify::Notifier;
use orchestration::services::server::{AddressOverrides, CreateServer, ServerService};
use orchestration::utils::secret::SecretKey;
use std::sync::Arc;
use std::time::Duration;
use testcontainers_modules::rabbitmq::RabbitMq;
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use wakuwaku::amqp::{AmqpMessageProcessor, AmqpMessageSend, AmqpPool, setup_consumer};

fn operator() -> Identity {
    Identity {
        account_id: AccountId::from_key("admin"),
        role: AccountRole::Admin,
        kind: IdentityKind::Session,
    }
}

fn pos0() -> CanvasUiPosition {
    CanvasUiPosition { x: 0, y: 0 }
}

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
    let nodes = NodeService {
        db: db.clone(),
        notifier: notifier.clone(),
        config: OrchestrationConfig::default(),
    };
    let edges = EdgeService {
        db: db.clone(),
        notifier,
        config: OrchestrationConfig::default(),
    };

    let canvas = canvases
        .process(CreateCanvas {
            actor: operator(),
            name: "prod".to_string(),
            description: String::new(),
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
            log_level: "info".to_string(),
            addresses: AddressOverrides {
                override_v4: Some("203.0.113.10".to_string()),
                override_v6: None,
                extra_addresses: Vec::new(),
            },
        })
        .await?;
    let create = async |name: &str, spec: NodeSpec| {
        nodes
            .process(CreateNode {
                actor: operator(),
                canvas: canvas.id.clone(),
                name: name.to_string(),
                comment: String::new(),
                spec,
                position: pos0(),
                item_count: 0,
            })
            .await
    };
    let pod = create(
        "edge",
        NodeSpec::Pod(PodConfig {
            server: server.id.clone(),
            port: 443,
            bind_ip: None,
            advertise_ip: None,
        }),
    )
    .await?;
    let entry = create(
        "entry",
        NodeSpec::Entry(EntryConfig {
            receive_proxy_protocol: None,
            tls: None,
        }),
    )
    .await?;
    let exit = create(
        "exit",
        NodeSpec::Exit(ExitConfig {
            destination: "10.0.0.5:8080".to_string(),
            pass_proxy_protocol: None,
        }),
    )
    .await?;
    edges
        .process(Connect {
            actor: operator(),
            output_port: port_of(&pod, "listen"),
            input_port: port_of(&entry, "listen"),
        })
        .await?;
    edges
        .process(Connect {
            actor: operator(),
            output_port: port_of(&exit, "destination"),
            input_port: port_of(&pod, "destination"),
        })
        .await?;

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
    let nodes = NodeService {
        db: db.clone(),
        notifier: Notifier::default(),
        config: OrchestrationConfig::default(),
    };
    let edges = EdgeService {
        db: db.clone(),
        notifier: Notifier::default(),
        config: OrchestrationConfig::default(),
    };
    let canvas = canvases
        .process(CreateCanvas {
            actor: operator(),
            name: "prod".to_string(),
            description: String::new(),
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
            log_level: "info".to_string(),
            addresses: AddressOverrides {
                override_v4: Some("203.0.113.20".to_string()),
                override_v6: None,
                extra_addresses: Vec::new(),
            },
        })
        .await?;
    let create = async |name: &str, spec: NodeSpec| {
        nodes
            .process(CreateNode {
                actor: operator(),
                canvas: canvas.id.clone(),
                name: name.to_string(),
                comment: String::new(),
                spec,
                position: pos0(),
                item_count: 0,
            })
            .await
    };
    let pod = create(
        "edge",
        NodeSpec::Pod(PodConfig {
            server: server.id.clone(),
            port: 443,
            bind_ip: None,
            advertise_ip: None,
        }),
    )
    .await?;
    let entry = create(
        "entry",
        NodeSpec::Entry(EntryConfig {
            receive_proxy_protocol: None,
            tls: None,
        }),
    )
    .await?;
    let exit = create(
        "exit",
        NodeSpec::Exit(ExitConfig {
            destination: "10.0.0.6:8080".to_string(),
            pass_proxy_protocol: None,
        }),
    )
    .await?;
    edges
        .process(Connect {
            actor: operator(),
            output_port: port_of(&pod, "listen"),
            input_port: port_of(&entry, "listen"),
        })
        .await?;
    edges
        .process(Connect {
            actor: operator(),
            output_port: port_of(&exit, "destination"),
            input_port: port_of(&pod, "destination"),
        })
        .await?;

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
        tick_unix_secs: chrono::Utc::now().timestamp(),
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
