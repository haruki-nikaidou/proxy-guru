//! The Iggy path end to end, over a real server: an edit publishes
//! `CanvasDirty` and the consumer derives the canvas.
//!
//! Every other test drives the deriver directly, so this is the only place the
//! publish/consume wiring — stream, topic, partition key, type tag and payload
//! encoding — is actually exercised. It needs a reachable Docker daemon;
//! `testcontainers` starts and disposes of the server.

#![allow(clippy::unwrap_used, clippy::panic, clippy::expect_used)]

mod common;

use base::events::{STREAM, all_partitions};
use common::*;
use kanau::processor::Processor;
use orchestration::config::OrchestrationConfig;
use orchestration::entities::db::server::{ServerIpv6Resolve, ServerLogLevel};
use orchestration::entities::db::view::FindServerConfigView;
use orchestration::events::CanvasDirty;
use orchestration::hooks::derive::{CanvasDeriver, CanvasDirtyHandler};
use orchestration::services::canvas::{CanvasService, CreateCanvas};
use orchestration::services::graph::{ApplyGraph, GraphChange, GraphService};
use orchestration::services::notify::Notifier;
use orchestration::services::server::{AddressOverrides, CreateServer, ServerService};
use orchestration::utils::secret::SecretKey;
use std::sync::Arc;
use std::time::{Duration, Instant};
use testcontainers_modules::testcontainers::core::IntoContainerPort;
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use testcontainers_modules::testcontainers::{ContainerAsync, GenericImage, ImageExt};
use wakuwaku::ServiceBuilder;
use wakuwaku::deps::iggy::prelude::IggyClient;
use wakuwaku::iggy::{ConsumerConfig, Publisher};

/// The Iggy release the SDK `wakuwaku` builds against speaks to.
const IGGY_IMAGE: (&str, &str) = ("apache/iggy", "0.9.0");

/// Starts an Iggy server and connects to it once it accepts a login.
async fn iggy()
-> Result<(ContainerAsync<GenericImage>, Arc<IggyClient>), Box<dyn std::error::Error>> {
    let container = GenericImage::new(IGGY_IMAGE.0, IGGY_IMAGE.1)
        .with_exposed_port(8090.tcp())
        .with_env_var("IGGY_ROOT_USERNAME", "iggy")
        .with_env_var("IGGY_ROOT_PASSWORD", "iggy")
        .with_env_var("IGGY_TCP_ADDRESS", "0.0.0.0:8090")
        .with_env_var("IGGY_NODE_ADVERTISED_ADDRESS", "localhost")
        .with_env_var("IGGY_QUIC_ENABLED", "false")
        .with_env_var("IGGY_WEBSOCKET_ENABLED", "false")
        .with_cap_add("SYS_NICE")
        .with_security_opt("seccomp=unconfined")
        .with_ulimit("memlock", -1, Some(-1))
        .start()
        .await?;
    let url = format!(
        "iggy://iggy:iggy@{}:{}",
        container.get_host().await?,
        container.get_host_port_ipv4(8090.tcp()).await?
    );
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        match base::events::connect(&url).await {
            Ok(client) => return Ok((container, client)),
            Err(error) if Instant::now() < deadline => {
                tracing::debug!(%error, "Iggy not ready yet");
                tokio::time::sleep(Duration::from_millis(250)).await;
            }
            Err(error) => return Err(format!("Iggy never accepted a login: {error}").into()),
        }
    }
}

#[sqlx::test(migrator = "base::db::MIGRATOR")]
async fn an_edit_reaches_the_deriver_through_iggy(db_pool: sqlx::PgPool) -> TestResult {
    let (_container, iggy_client) = iggy().await?;
    base::events::ensure_topology(&iggy_client, &orchestration::events::TOPICS).await?;

    let db = setup(db_pool);
    let deriver = CanvasDeriver {
        db: db.clone(),
        secrets: SecretKey::from_base64(&SecretKey::generate_base64())?,
        config: Default::default(),
        notifier: Notifier::default(),
    };
    let (_, consumer) = ServiceBuilder::new(deriver)
        .iggy_consumer::<CanvasDirtyHandler, CanvasDirty, _>()
        .start(
            iggy_client.clone(),
            ConsumerConfig::new(STREAM, "guru_orchestration_canvas_dirty", all_partitions()),
        )
        .await?;

    // Services publish; nothing in this test derives anything itself, and no
    // sweep is running, so only Iggy can make the view move.
    let notifier = Notifier {
        events: Some(Publisher::new(iggy_client, STREAM)?),
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

    // Every edit publishes, so the first snapshot to arrive is the empty config
    // the bare server derived to. What proves the wiring is that the *last* edit
    // also gets through, with no sweep running to paper over a lost message.
    let deadline = Instant::now() + Duration::from_secs(30);
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
                Instant::now() < deadline,
                "Iggy never drove the last edit: desired={:?} error={:?}",
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

    consumer.shutdown().await;
    Ok(())
}
