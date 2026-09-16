#![allow(dead_code)]

use auth::config::AuthConfig;
use auth::entities::surreal::account::{AccountId, AccountRole, CreateAccount};
use auth::services::identity::{Identity, IdentityKind};
use auth::services::session::{Login, LoginResult, SessionService};
use auth::utils::password::{Argon2PasswordAlgorithm, PasswordAlgorithm};
use base::db::Db;
use kanau::processor::Processor;
use orchestration::config::OrchestrationConfig;
use orchestration::entities::surreal::canvas::{
    CanvasEntity, CanvasId, CanvasUiPosition, CreateCanvas,
};
use orchestration::entities::surreal::node::{
    CreateNodeRow, NewPort, NodeSpec, NodeWithPorts, PodConfig,
};
use orchestration::entities::surreal::port::{PortDirection, PortKind};
use orchestration::entities::surreal::server::{
    CreateServer, ServerEntity, ServerId, ServerIpv6Resolve,
};
use orchestration::entities::surreal::view::{FindServerConfigView, ServerConfigViewEntity};
use orchestration::hooks::derive::{CanvasDeriver, DeriveCanvas};
use orchestration::hooks::live::LiveBus;
use orchestration::services::acme::{AcmeService, InstantAcmeIssuer};
use orchestration::services::agent::AgentService;
use orchestration::services::ca::CaService;
use orchestration::services::canvas::CanvasService;
use orchestration::services::dns::DnsProviderService;
use orchestration::services::edge::EdgeService;
use orchestration::services::health::HealthService;
use orchestration::services::live::LiveService;
use orchestration::services::node::NodeService;
use orchestration::services::notify::{LivePublisher, Notifier};
use orchestration::services::rollout::RolloutService;
use orchestration::services::server::ServerService;
use orchestration::utils::secret::SecretKey;
use std::sync::Arc;
use surrealdb::types::RecordId;

pub type TestResult = Result<(), Box<dyn std::error::Error>>;

/// A fresh in-memory database with the module's real schema applied.
pub async fn setup() -> Result<Db, Box<dyn std::error::Error>> {
    let db = surrealdb::engine::any::connect("mem://").await?;
    db.use_ns("test").use_db("test").await?;
    let sp = Db::new(db);
    let ddl = std::fs::read_to_string(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../database/schema/orchestration.surql"
    ))?;
    sp.raw().query(ddl).await?.check()?;
    // The live streams re-validate their session on every keep-alive tick, so
    // the gRPC-level tests need real `account` and `session` tables.
    let auth_ddl = std::fs::read_to_string(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../database/schema/auth.surql"
    ))?;
    sp.raw().query(auth_ddl).await?.check()?;
    Ok(sp)
}

pub fn pos(x: i64, y: i64) -> CanvasUiPosition {
    CanvasUiPosition { x, y }
}

pub async fn canvas(sp: &Db, name: &str) -> Result<CanvasEntity, surrealdb::Error> {
    sp.process(CreateCanvas {
        name: name.to_string(),
        description: String::new(),
    })
    .await
}

pub async fn server(
    sp: &Db,
    canvas: &CanvasEntity,
    name: &str,
) -> Result<ServerEntity, surrealdb::Error> {
    server_at(sp, canvas, name, "203.0.113.10").await
}

/// A server with a pinned IPv4 address, so pods placed on it derive a dialable
/// destination without a live worker.
pub async fn server_at(
    sp: &Db,
    canvas: &CanvasEntity,
    name: &str,
    address: &str,
) -> Result<ServerEntity, surrealdb::Error> {
    sp.process(CreateServer {
        canvas: canvas.id.clone(),
        name: name.to_string(),
        icon: String::new(),
        comment: String::new(),
        position: pos(0, 0),
        ipv6_resolve: ServerIpv6Resolve::Tolerated,
        log_level: "info".to_string(),
        override_v4: Some(address.to_string()),
        override_v6: None,
        extra_addresses: Vec::new(),
    })
    .await
}

pub fn pod_ports() -> Vec<NewPort> {
    vec![
        NewPort {
            kind: PortKind::DeriveListen,
            direction: PortDirection::Output,
            key: "listen".to_string(),
            position: 0,
        },
        NewPort {
            kind: PortKind::DeriveDestination,
            direction: PortDirection::Input,
            key: "destination".to_string(),
            position: 1,
        },
    ]
}

pub fn exit_ports() -> Vec<NewPort> {
    vec![NewPort {
        kind: PortKind::DeriveDestination,
        direction: PortDirection::Output,
        key: "destination".to_string(),
        position: 0,
    }]
}

pub fn entry_ports() -> Vec<NewPort> {
    vec![NewPort {
        kind: PortKind::DeriveListen,
        direction: PortDirection::Input,
        key: "listen".to_string(),
        position: 0,
    }]
}

pub async fn node(
    sp: &Db,
    canvas: &CanvasEntity,
    name: &str,
    spec: NodeSpec,
    ports: Vec<NewPort>,
) -> Result<NodeWithPorts, surrealdb::Error> {
    sp.process(CreateNodeRow {
        canvas: canvas.id.clone(),
        name: name.to_string(),
        comment: String::new(),
        spec,
        position: pos(0, 0),
        ports,
        import_sync: None,
        fence: None,
        target_fence: None,
    })
    .await
}

pub fn pod_spec(server: &ServerEntity, port: u16) -> NodeSpec {
    NodeSpec::Pod(PodConfig {
        server: server.id.clone(),
        port,
        bind_ip: None,
        advertise_ip: None,
    })
}

/// The id of the port with the given key.
pub fn port_of(node: &NodeWithPorts, key: &str) -> orchestration::entities::surreal::port::PortId {
    node.ports
        .iter()
        .find(|p| p.key == key)
        .map(|p| p.id.clone())
        .unwrap_or_else(|| panic!("node has no port {key}"))
}

// --- service-level harness ---------------------------------------------------

pub fn operator() -> Identity {
    Identity {
        account_id: AccountId(RecordId::new("auth_account", "admin")),
        role: AccountRole::Admin,
        kind: IdentityKind::Session,
    }
}

pub fn machine() -> Identity {
    Identity {
        account_id: AccountId(RecordId::new("auth_account", "admin")),
        role: AccountRole::Maintainer,
        kind: IdentityKind::ApiKey,
    }
}

/// A Maintainer holding a human session: it passes the `EditWorkspace` gate, so a
/// refusal can only come from the role check itself.
pub fn maintainer() -> Identity {
    Identity {
        account_id: AccountId(RecordId::new("auth_account", "ops")),
        role: AccountRole::Maintainer,
        kind: IdentityKind::Session,
    }
}

pub fn pos0() -> CanvasUiPosition {
    CanvasUiPosition { x: 0, y: 0 }
}

/// Every service over one in-memory database, plus the derivation hook.
pub struct World {
    pub db: Db,
    pub secrets: SecretKey,
    pub config: OrchestrationConfig,
    /// The in-process live bus every service in this world publishes to.
    pub bus: LiveBus,
    pub notifier: Notifier,
    pub canvases: CanvasService,
    pub servers: ServerService,
    pub nodes: NodeService,
    pub edges: EdgeService,
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

pub async fn world() -> Result<World, Box<dyn std::error::Error>> {
    world_with(OrchestrationConfig::default()).await
}

/// A world whose services carry `config`; the gRPC stream tests shorten the
/// keep-alive so a test does not have to wait fifteen seconds for one.
pub async fn world_with(config: OrchestrationConfig) -> Result<World, Box<dyn std::error::Error>> {
    let db = setup().await?;
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
        nodes: NodeService {
            db: db.clone(),
            notifier: notifier.clone(),
            config: config.clone(),
        },
        edges: EdgeService {
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
