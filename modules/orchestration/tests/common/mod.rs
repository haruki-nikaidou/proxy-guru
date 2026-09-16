#![allow(dead_code)]

use auth::config::AuthConfig;
use auth::entities::db::account::{AccountId, AccountRole, CreateAccount};
use auth::services::identity::{Identity, IdentityKind};
use auth::services::session::{Login, LoginResult, SessionService};
use auth::utils::password::{Argon2PasswordAlgorithm, PasswordAlgorithm};
use base::db::Db;
use kanau::processor::Processor;
use orchestration::config::OrchestrationConfig;
use orchestration::entities::db::canvas::{CanvasEntity, CanvasId, CanvasUiPosition, CreateCanvas};
use orchestration::entities::db::edge::{EdgeEntity, EdgeId, EdgeTarget};
use orchestration::entities::db::exit::{ExitEntity, ExitId};
use orchestration::entities::db::pod::{PodEntity, PodId, PodIngress, ProxyProtocolVersion};
use orchestration::entities::db::server::{
    CreateServer, ServerEntity, ServerId, ServerIpv6Resolve, ServerLogLevel,
};
use orchestration::entities::db::view::{FindServerConfigView, ServerConfigViewEntity};
use orchestration::hooks::derive::{CanvasDeriver, DeriveCanvas};
use orchestration::hooks::live::LiveBus;
use orchestration::services::acme::{AcmeService, InstantAcmeIssuer};
use orchestration::services::agent::AgentService;
use orchestration::services::ca::CaService;
use orchestration::services::canvas::CanvasService;
use orchestration::services::dns::DnsProviderService;
use orchestration::services::graph::{ApplyGraph, ApplyOutcome, GraphChange, GraphService};
use orchestration::services::health::HealthService;
use orchestration::services::live::LiveService;
use orchestration::services::notify::{LivePublisher, Notifier};
use orchestration::services::rollout::RolloutService;
use orchestration::services::server::ServerService;
use orchestration::utils::secret::SecretKey;
use std::sync::Arc;

pub type TestResult = Result<(), Box<dyn std::error::Error>>;

/// The database `#[sqlx::test]` created for this test, migrated with the
/// workspace schema, wrapped in the handle services hold.
pub fn setup(pool: sqlx::PgPool) -> Db {
    Db::new(pool)
}

pub fn pos(x: i64, y: i64) -> CanvasUiPosition {
    CanvasUiPosition { x, y }
}

pub async fn canvas(sp: &Db, name: &str) -> Result<CanvasEntity, base::db::Error> {
    sp.process(CreateCanvas {
        name: name.to_string(),
        description: String::new(),
        parent: None,
        position: pos(0, 0),
    })
    .await
}

pub async fn subcanvas(
    sp: &Db,
    parent: &CanvasEntity,
    name: &str,
) -> Result<CanvasEntity, base::db::Error> {
    sp.process(CreateCanvas {
        name: name.to_string(),
        description: String::new(),
        parent: Some(parent.id.clone()),
        position: pos(0, 0),
    })
    .await
}

pub async fn server(
    sp: &Db,
    canvas: &CanvasEntity,
    name: &str,
) -> Result<ServerEntity, base::db::Error> {
    server_at(sp, canvas, name, "203.0.113.10").await
}

/// A server with a pinned IPv4 address, so pods placed on it derive a dialable
/// destination without a live worker.
pub async fn server_at(
    sp: &Db,
    canvas: &CanvasEntity,
    name: &str,
    address: &str,
) -> Result<ServerEntity, base::db::Error> {
    sp.process(CreateServer {
        canvas: canvas.id.clone(),
        name: name.to_string(),
        icon: String::new(),
        comment: String::new(),
        position: pos(0, 0),
        ipv6_resolve: ServerIpv6Resolve::Tolerated,
        log_level: ServerLogLevel::Info,
        override_v4: Some(address.to_string()),
        override_v6: None,
        extra_addresses: Vec::new(),
    })
    .await
}

// --- graph rows -----------------------------------------------------------------

/// A record key made from a readable name: lower-cased, anything but `[a-z0-9]`
/// turned into `0`, padded with `x` or cut to 20 characters.
pub fn key(name: &str) -> String {
    let mut out: String = name
        .chars()
        .flat_map(char::to_lowercase)
        .map(|c| if c.is_ascii_lowercase() || c.is_ascii_digit() { c } else { '0' })
        .take(20)
        .collect();
    while out.len() < 20 {
        out.push('x');
    }
    out
}

pub fn pod_id(name: &str) -> PodId {
    PodId::from_key(key(name))
}

pub fn exit_id(name: &str) -> ExitId {
    ExitId::from_key(key(name))
}

pub fn edge_id(name: &str) -> EdgeId {
    EdgeId::from_key(key(name))
}

/// A pod named `name` (its id is [`key`] of it) with no route.
pub fn pod(canvas: &CanvasEntity, server: &ServerEntity, name: &str, port: u16, ingress: PodIngress) -> PodEntity {
    PodEntity {
        id: pod_id(name),
        canvas: canvas.id.clone(),
        server: server.id.clone(),
        name: name.to_string(),
        comment: String::new(),
        port,
        bind_ip: None,
        advertise_ip: None,
        ingress,
        route: None,
    }
}

/// A raw client pod, reading PROXY headers when `proxy` is set.
pub fn client(canvas: &CanvasEntity, server: &ServerEntity, name: &str, port: u16, proxy: Option<ProxyProtocolVersion>) -> PodEntity {
    pod(
        canvas,
        server,
        name,
        port,
        PodIngress::ClientRaw {
            receive_proxy_protocol: proxy,
        },
    )
}

pub fn exit(canvas: &CanvasEntity, name: &str, destination: &str) -> ExitEntity {
    ExitEntity {
        id: exit_id(name),
        canvas: canvas.id.clone(),
        name: name.to_string(),
        comment: String::new(),
        destination: destination.to_string(),
        send_proxy_protocol: None,
        position: pos(0, 0),
    }
}

/// An edge named `name` from a pod to a pod.
pub fn edge_to_pod(name: &str, source: &PodEntity, target: &PodEntity) -> EdgeEntity {
    EdgeEntity {
        id: edge_id(name),
        source: source.id.clone(),
        target: EdgeTarget::Pod(target.id.clone()),
        override_ip: None,
        override_port: None,
    }
}

/// An edge named `name` from a pod to an exit.
pub fn edge_to_exit(name: &str, source: &PodEntity, target: &ExitEntity) -> EdgeEntity {
    EdgeEntity {
        id: edge_id(name),
        source: source.id.clone(),
        target: EdgeTarget::Exit(target.id.clone()),
        override_ip: None,
        override_port: None,
    }
}

/// The route that is just this edge.
pub fn via(edge: &EdgeEntity) -> guru_topology::Route {
    guru_topology::Route::Edge(guru_topology::EdgeId::new(edge.id.as_str()))
}

/// A balance over the given edges, each weighing one.
pub fn balance(edges: &[&EdgeEntity]) -> guru_topology::Route {
    guru_topology::Route::Balance {
        members: edges
            .iter()
            .map(|edge| guru_topology::Weighted {
                weight: 1,
                to: via(edge),
            })
            .collect(),
        sticky: None,
    }
}

/// The pod with its route set to `route`.
pub fn routed(mut pod: PodEntity, route: guru_topology::Route) -> PodEntity {
    pod.route = Some(route);
    pod
}

/// Writes rows straight through the entity layer, unchecked: for tests of what
/// sits behind the graph, not of the graph service.
pub async fn insert_rows(
    sp: &Db,
    canvas: &CanvasEntity,
    pods: Vec<PodEntity>,
    exits: Vec<ExitEntity>,
    edges: Vec<EdgeEntity>,
) -> Result<(), base::db::Error> {
    sp.process(orchestration::entities::db::graph::ApplyGraphBatch {
        canvas: Some(canvas.id.clone()),
        derives: true,
        insert_pods: pods,
        insert_exits: exits,
        insert_edges: edges,
        ..Default::default()
    })
    .await?;
    Ok(())
}

/// Deletes pods (and first the given edges) straight through the entity layer.
pub async fn delete_rows(
    sp: &Db,
    canvas: &CanvasEntity,
    pods: Vec<PodId>,
    edges: Vec<EdgeId>,
) -> Result<(), base::db::Error> {
    sp.process(orchestration::entities::db::graph::ApplyGraphBatch {
        canvas: Some(canvas.id.clone()),
        derives: true,
        delete_pods: pods,
        delete_edges: edges,
        ..Default::default()
    })
    .await?;
    Ok(())
}

/// A TLS client pod asking for `sni` through `provider`.
pub fn tls_client(
    canvas: &CanvasEntity,
    server: &ServerEntity,
    name: &str,
    port: u16,
    provider: &orchestration::entities::db::dns::DnsProviderId,
    sni: &str,
    directory: &str,
) -> PodEntity {
    pod(
        canvas,
        server,
        name,
        port,
        PodIngress::ClientTls {
            receive_proxy_protocol: None,
            tls: orchestration::entities::db::pod::TlsConfig {
                sni: sni.to_string(),
                dns_provider: provider.clone(),
                domain_id: "zone".to_string(),
                acme_directory: directory.to_string(),
            },
        },
    )
}

// --- service-level harness ---------------------------------------------------

pub fn operator() -> Identity {
    Identity {
        account_id: AccountId::from_key("admin"),
        role: AccountRole::Admin,
        kind: IdentityKind::Session,
    }
}

pub fn machine() -> Identity {
    Identity {
        account_id: AccountId::from_key("admin"),
        role: AccountRole::Maintainer,
        kind: IdentityKind::ApiKey,
    }
}

/// A Maintainer holding a human session: it passes the `EditWorkspace` gate, so a
/// refusal can only come from the role check itself.
pub fn maintainer() -> Identity {
    Identity {
        account_id: AccountId::from_key("ops"),
        role: AccountRole::Maintainer,
        kind: IdentityKind::Session,
    }
}

pub fn pos0() -> CanvasUiPosition {
    CanvasUiPosition { x: 0, y: 0 }
}

/// Every service over one database, plus the derivation hook.
pub struct World {
    pub db: Db,
    pub secrets: SecretKey,
    pub config: OrchestrationConfig,
    /// The in-process live bus every service in this world publishes to.
    pub bus: LiveBus,
    pub notifier: Notifier,
    pub canvases: CanvasService,
    pub servers: ServerService,
    pub graph: GraphService,
    pub agents: AgentService,
    pub rollout: RolloutService,
    pub health: HealthService,
    pub ca: CaService,
    pub dns: DnsProviderService,
    pub certificates: AcmeService,
    pub deriver: CanvasDeriver,
    pub live: LiveService,
    pub sessions: SessionService,
}

pub async fn world(pool: sqlx::PgPool) -> Result<World, Box<dyn std::error::Error>> {
    world_with(pool, OrchestrationConfig::default()).await
}

/// A world whose services carry `config`; the gRPC stream tests shorten the
/// keep-alive so a test does not have to wait fifteen seconds for one.
pub async fn world_with(
    pool: sqlx::PgPool,
    config: OrchestrationConfig,
) -> Result<World, Box<dyn std::error::Error>> {
    let db = setup(pool);
    let secrets = SecretKey::from_base64(&SecretKey::generate_base64())?;
    let bus = LiveBus::new();
    // No broker, but a real bus: `amqp: None` keeps the derivation hook driven
    // by the tests themselves, while live events travel as they would in
    // production minus the Redis round trip.
    let notifier = Notifier {
        amqp: None,
        live: Some(LivePublisher::InProcess(bus.clone())),
    };
    Ok(World {
        canvases: CanvasService {
            db: db.clone(),
            notifier: notifier.clone(),
        },
        servers: ServerService {
            db: db.clone(),
            notifier: notifier.clone(),
            config: config.clone(),
        },
        graph: GraphService {
            db: db.clone(),
            notifier: notifier.clone(),
            config: config.clone(),
        },
        agents: AgentService {
            db: db.clone(),
            hub: Default::default(),
            lease: Default::default(),
            notifier: notifier.clone(),
            config: config.clone(),
        },
        rollout: RolloutService {
            db: db.clone(),
            notifier: notifier.clone(),
        },
        health: HealthService {
            db: db.clone(),
            config: config.clone(),
            notifier: notifier.clone(),
        },
        dns: DnsProviderService {
            db: db.clone(),
            secrets: secrets.clone(),
        },
        certificates: AcmeService {
            db: db.clone(),
            secrets: secrets.clone(),
            config: config.clone(),
            notifier: notifier.clone(),
            http: reqwest::Client::new(),
            issuer: Arc::new(InstantAcmeIssuer),
        },
        ca: CaService {
            db: db.clone(),
            secrets: secrets.clone(),
            config: config.clone(),
        },
        deriver: CanvasDeriver {
            db: db.clone(),
            secrets: secrets.clone(),
            config: config.clone(),
            notifier: notifier.clone(),
        },
        live: LiveService::new(db.clone(), bus.clone(), config.clone()),
        sessions: SessionService {
            db: db.clone(),
            hasher: Argon2PasswordAlgorithm::default(),
            config: AuthConfig::default(),
        },
        bus,
        notifier,
        db,
        secrets,
        config,
    })
}

impl World {
    /// Runs a derivation pass the way the consumer or the sweeper would.
    pub async fn derive(&self, canvas: &CanvasId) -> Result<(), Box<dyn std::error::Error>> {
        self.deriver
            .process(DeriveCanvas {
                canvas: canvas.clone(),
            })
            .await?;
        Ok(())
    }

    /// Applies a graph change as an Admin and fails on any error diagnostic.
    pub async fn apply(
        &self,
        canvas: &CanvasEntity,
        change: GraphChange,
    ) -> Result<ApplyOutcome, Box<dyn std::error::Error>> {
        let outcome = self.try_apply(canvas, change).await?;
        if !outcome.applied {
            return Err(format!("change refused: {:#?}", outcome.diagnostics).into());
        }
        Ok(outcome)
    }

    /// Applies a graph change as an Admin and hands back the outcome, refused
    /// or not.
    pub async fn try_apply(
        &self,
        canvas: &CanvasEntity,
        change: GraphChange,
    ) -> Result<ApplyOutcome, Box<dyn std::error::Error>> {
        Ok(self
            .graph
            .process(ApplyGraph {
                actor: operator(),
                canvas: canvas.id.clone(),
                change,
                dry_run: false,
                expected_generation: None,
            })
            .await?)
    }

    pub async fn view(
        &self,
        server: &ServerId,
    ) -> Result<ServerConfigViewEntity, Box<dyn std::error::Error>> {
        Ok(self
            .db
            .process(FindServerConfigView {
                server: server.clone(),
            })
            .await?
            .expect("every server has a config view"))
    }

    /// Creates a Maintainer and logs it in; returns the session token, which is
    /// also the session id the `Watch*` streams re-validate.
    pub async fn login(&self) -> Result<String, Box<dyn std::error::Error>> {
        let hasher = Argon2PasswordAlgorithm::default();
        self.db
            .process(CreateAccount {
                email: "operator@example.com".to_string(),
                password_hash: hasher.hash_password("operator-password")?,
                role: AccountRole::Maintainer,
            })
            .await?;
        match self
            .sessions
            .process(Login {
                email: "operator@example.com".to_string(),
                password: "operator-password".to_string(),
                user_agent: "tests".to_string(),
            })
            .await?
        {
            LoginResult::Success(token) => Ok(token),
            LoginResult::InvalidCredentials => Err("login failed".into()),
        }
    }
}
