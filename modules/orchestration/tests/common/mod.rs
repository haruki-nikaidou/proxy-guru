#![allow(dead_code)]

use auth::entities::surreal::account::{AccountId, AccountRole};
use auth::services::identity::{Identity, IdentityKind};
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
    CreateServer, CreateServerIp, ServerEntity, ServerId, ServerIpRecordEntity, ServerIpv6Resolve,
};
use orchestration::entities::surreal::view::{FindServerConfigView, ServerConfigViewEntity};
use orchestration::hooks::derive::{CanvasDeriver, DeriveCanvas};
use orchestration::services::acme::{AcmeService, InstantAcmeIssuer};
use orchestration::services::agent::AgentService;
use orchestration::services::ca::CaService;
use orchestration::services::canvas::CanvasService;
use orchestration::services::dns::DnsProviderService;
use orchestration::services::edge::EdgeService;
use orchestration::services::health::HealthService;
use orchestration::services::node::NodeService;
use orchestration::services::rollout::RolloutService;
use orchestration::services::server::ServerService;
use orchestration::utils::secret::SecretKey;
use std::sync::Arc;
use surrealdb::types::RecordId;
use wakuwaku::surreal::SurrealProcessor;

pub type TestResult = Result<(), Box<dyn std::error::Error>>;

/// A fresh in-memory database with the module's real schema applied.
pub async fn setup() -> Result<SurrealProcessor, Box<dyn std::error::Error>> {
    let db = surrealdb::engine::any::connect("mem://").await?;
    db.use_ns("test").use_db("test").await?;
    let sp = SurrealProcessor::new(db);
    let ddl = std::fs::read_to_string(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../database/schema/orchestration.surql"
    ))?;
    sp.db().query(ddl).await?.check()?;
    Ok(sp)
}

pub fn pos(x: i64, y: i64) -> CanvasUiPosition {
    CanvasUiPosition { x, y }
}

pub async fn canvas(sp: &SurrealProcessor, name: &str) -> Result<CanvasEntity, surrealdb::Error> {
    sp.process(CreateCanvas {
        name: name.to_string(),
        description: String::new(),
    })
    .await
}

pub async fn server(
    sp: &SurrealProcessor,
    canvas: &CanvasEntity,
    name: &str,
) -> Result<ServerEntity, surrealdb::Error> {
    sp.process(CreateServer {
        canvas: canvas.id.clone(),
        name: name.to_string(),
        icon: String::new(),
        comment: String::new(),
        position: pos(0, 0),
        ipv6_resolve: ServerIpv6Resolve::Tolerated,
        log_level: "info".to_string(),
    })
    .await
}

pub async fn server_ip(
    sp: &SurrealProcessor,
    server: &ServerEntity,
    ip: &str,
) -> Result<ServerIpRecordEntity, surrealdb::Error> {
    sp.process(CreateServerIp {
        server: server.id.clone(),
        ip: ip.to_string(),
        country: "jp".to_string(),
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
    sp: &SurrealProcessor,
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
    })
    .await
}

pub fn pod_spec(ip: &ServerIpRecordEntity, port: u16) -> NodeSpec {
    NodeSpec::Pod(PodConfig {
        ip: ip.id.clone(),
        port,
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
    pub db: SurrealProcessor,
    pub secrets: SecretKey,
    pub config: OrchestrationConfig,
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
}

pub async fn world() -> Result<World, Box<dyn std::error::Error>> {
    let db = setup().await?;
    let secrets = SecretKey::from_base64(&SecretKey::generate_base64())?;
    let config = OrchestrationConfig::default();
    Ok(World {
        canvases: CanvasService {
            db: db.clone(),
            notifier: Default::default(),
        },
        servers: ServerService {
            db: db.clone(),
            notifier: Default::default(),
            config: config.clone(),
        },
        nodes: NodeService {
            db: db.clone(),
            notifier: Default::default(),
            config: config.clone(),
        },
        edges: EdgeService {
            db: db.clone(),
            notifier: Default::default(),
            config: config.clone(),
        },
        agents: AgentService {
            db: db.clone(),
            hub: Default::default(),
            lease: Default::default(),
            notifier: Default::default(),
        },
        rollout: RolloutService {
            db: db.clone(),
            notifier: Default::default(),
        },
        health: HealthService {
            db: db.clone(),
            config: Default::default(),
        },
        dns: DnsProviderService {
            db: db.clone(),
            secrets: secrets.clone(),
        },
        certificates: AcmeService {
            db: db.clone(),
            secrets: secrets.clone(),
            config: config.clone(),
            notifier: Default::default(),
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
        },
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
}
