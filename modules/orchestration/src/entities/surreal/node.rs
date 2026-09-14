use crate::entities::surreal::canvas::{CanvasId, CanvasUiPosition};
use crate::entities::surreal::dns::DnsProviderId;
use crate::entities::surreal::port::{PortDirection, PortEntity, PortKind};
use crate::entities::surreal::server::ServerIpRecordId;
use crate::utils::ids::record_key;
use kanau::processor::Processor;
use newtype_record_id::table_record;
use surrealdb_types::SurrealValue;
use wakuwaku::surreal::SurrealProcessor;

table_record!(NodeId, "orchestration_node");

#[derive(Debug, Clone, SurrealValue)]
pub struct NodeEntity {
    pub id: NodeId,
    pub canvas: CanvasId,
    pub name: String,
    pub comment: String,
    pub spec: NodeSpec,
    pub position: CanvasUiPosition,
}

#[derive(Debug, Clone, SurrealValue)]
#[surreal(tag = "type", content = "config", rename_all = "snake_case")]
pub enum NodeSpec {
    CanvasExport(CanvasExportConfig),
    CanvasImport(CanvasImportConfig),
    Pod(PodConfig),
    Entry(EntryConfig),
    Relay(RelayConfig),
    Exit(ExitConfig),
    LoadBalanceDistribute(LoadBalanceDistributeConfig),
    LoadBalanceAggregate(LoadBalanceAggregateConfig),
}

#[derive(Debug, Clone, SurrealValue)]
pub struct CanvasExportConfig {
    pub kind: PortKind,
    pub direction: CanvasExportAs,
}

#[derive(Debug, Clone, SurrealValue, Copy, PartialEq, Eq)]
#[surreal(untagged, rename_all = "snake_case")]
pub enum CanvasExportAs {
    InputIntoCanvas,
    OutputOutOfCanvas,
}

/// An import node embeds another canvas as one node. The target is immutable:
/// re-targeting is "retire and import again". The importing node's ports are
/// derived from the target's export nodes (see
/// [`crate::services::node::import_port_layout`]) and follow every export edit.
#[derive(Debug, Clone, SurrealValue)]
pub struct CanvasImportConfig {
    pub canvas: CanvasId,
}

#[derive(Debug, Clone, SurrealValue)]
pub struct PodConfig {
    /// The ip record this pod listens on; it pins the pod to exactly one server.
    pub ip: ServerIpRecordId,
    pub port: u16,
}

#[derive(Debug, Clone, SurrealValue)]
pub struct EntryConfig {
    pub receive_proxy_protocol: Option<ProxyProtocolVersion>,
    pub tls: Option<TlsConfig>,
}

#[derive(Debug, Clone, SurrealValue)]
/// By setting this, the master node will acquire a TLS certificate and send it to worker nodes.
pub struct TlsConfig {
    /// The SNI of the TLS certificate
    pub sni: String,

    /// Which DNS provider to use for the TLS certificate
    pub dns_provider: DnsProviderId,

    /// The identifier of the domain
    /// - Cloudflare: zone ID
    /// - vercel: domain SLD
    pub domain_id: String,

    /// The URL of the ACME directory.
    ///
    /// eg. <https://acme-staging-v02.api.letsencrypt.org/directory>
    pub acme_directory: String,
}

#[derive(Debug, Clone, SurrealValue, Copy, PartialEq, Eq)]
#[surreal(untagged, rename_all = "snake_case")]
pub enum ProxyProtocolVersion {
    V1,
    V2,
}

impl From<ProxyProtocolVersion> for guru_worker_config::TcpProxyProtocol {
    fn from(value: ProxyProtocolVersion) -> Self {
        match value {
            ProxyProtocolVersion::V1 => guru_worker_config::TcpProxyProtocol::V1,
            ProxyProtocolVersion::V2 => guru_worker_config::TcpProxyProtocol::V2,
        }
    }
}

#[derive(Debug, Clone, SurrealValue)]
pub struct RelayConfig {
    pub protocol: RelayProtocol,
    pub override_ip_address: Option<String>,
    pub override_port: Option<u16>,
}

#[derive(Debug, Clone, Copy, SurrealValue, PartialEq, Eq)]
#[surreal(untagged, rename_all = "snake_case")]
pub enum RelayProtocol {
    TcpRaw,
    TcpTls,
    Quic,
}

#[derive(Debug, Clone, SurrealValue)]
pub struct ExitConfig {
    pub destination: String,
    pub pass_proxy_protocol: Option<ProxyProtocolVersion>,
}

#[derive(Debug, Clone, SurrealValue)]
pub struct LoadBalanceDistributeConfig {
    pub mode: LoadBalanceMode,
}

#[derive(Debug, Clone, SurrealValue)]
pub struct LoadBalanceAggregateConfig {}

#[derive(Debug, Clone, Copy, PartialEq, Eq, SurrealValue)]
#[surreal(untagged, rename_all = "snake_case")]
pub enum LoadBalanceMode {
    RoundRobin,
    Random,
    IpHash,
    Fallback,
}

impl From<LoadBalanceMode> for guru_worker_config::LoadBalanceStrategy {
    fn from(value: LoadBalanceMode) -> Self {
        match value {
            LoadBalanceMode::RoundRobin => guru_worker_config::LoadBalanceStrategy::RoundRobin,
            LoadBalanceMode::Random => guru_worker_config::LoadBalanceStrategy::Random,
            LoadBalanceMode::IpHash => guru_worker_config::LoadBalanceStrategy::IpHash,
            LoadBalanceMode::Fallback => guru_worker_config::LoadBalanceStrategy::Fallback,
        }
    }
}

#[derive(Debug, Clone, SurrealValue)]
pub struct NodeWithPorts {
    pub node: NodeEntity,
    pub ports: Vec<PortEntity>,
}

/// A port to create together with its node.
#[derive(Debug, Clone, SurrealValue)]
pub struct NewPort {
    pub kind: PortKind,
    pub direction: PortDirection,
    pub key: String,
    pub position: i64,
}

/// The importing node whose derived ports must follow this edit, and the port
/// set it must end up with. `None` when the edited node is not an export node
/// of an imported canvas.
///
/// Applied by `fn::orchestration_reshape_ports` in the same transaction as the
/// export edit: a mirrored port whose key survives keeps its row and edges.
#[derive(Debug, Clone, SurrealValue)]
pub struct ImportSync {
    pub node: NodeId,
    pub ports: Vec<NewPort>,
}

/// The key `import_sync.ports` uses for the export node being created, whose
/// record key only exists inside the transaction; `create_node_row.surql`
/// substitutes the real key after the CREATE.
pub const PENDING_EXPORT_KEY: &str = "pending-node";

pub struct CreateNodeRow {
    pub canvas: CanvasId,
    pub name: String,
    pub comment: String,
    pub spec: NodeSpec,
    pub position: CanvasUiPosition,
    pub ports: Vec<NewPort>,
    pub import_sync: Option<ImportSync>,
}

impl Processor<CreateNodeRow> for SurrealProcessor {
    type Output = NodeWithPorts;
    type Error = surrealdb::Error;
    #[tracing::instrument(
        name = "Query-Transaction:CreateNodeRow",
        skip_all,
        err,
        fields(
            canvas = ?input.canvas,
            name = ?input.name,
        )
    )]
    async fn process(&self, input: CreateNodeRow) -> Result<Self::Output, Self::Error> {
        // Statement 0 is BEGIN, 1 the import guard, 2 the CREATE, 3 the port
        // insert, 4 the import target write, 5 the import sync, 6 the touch; the
        // RETURN is statement 7.
        let mut resp = self
            .db()
            .query(include_str!("../../../sql/node/create_node_row.surql"))
            .bind(("canvas", input.canvas))
            .bind(("name", input.name))
            .bind(("comment", input.comment))
            .bind(("spec", input.spec))
            .bind(("position", input.position))
            .bind(("new_ports", input.ports))
            .bind(("import_sync", input.import_sync))
            .await?;
        resp.take::<Option<NodeWithPorts>>(7)?
            .ok_or_else(|| surrealdb::Error::internal("create node returned no row".to_string()))
    }
}

#[derive(Debug)]
pub struct FindNodeById {
    pub id: NodeId,
}

impl Processor<FindNodeById> for SurrealProcessor {
    type Output = Option<NodeEntity>;
    type Error = surrealdb::Error;
    #[tracing::instrument(name = "Query:FindNodeById", skip_all, err)]
    async fn process(&self, input: FindNodeById) -> Result<Self::Output, Self::Error> {
        let mut resp = self
            .db()
            .query("SELECT * FROM $id")
            .bind(("id", input.id))
            .await?;
        resp.take::<Option<NodeEntity>>(0)
    }
}

#[derive(Debug)]
pub struct FindNodeWithPorts {
    pub id: NodeId,
}

impl Processor<FindNodeWithPorts> for SurrealProcessor {
    type Output = Option<NodeWithPorts>;
    type Error = surrealdb::Error;
    #[tracing::instrument(name = "Query:FindNodeWithPorts", skip_all, err)]
    async fn process(&self, input: FindNodeWithPorts) -> Result<Self::Output, Self::Error> {
        let mut resp = self
            .db()
            .query("SELECT * FROM $id")
            .bind(("id", input.id.clone()))
            .query("SELECT * FROM orchestration_port WHERE owner = $id ORDER BY position")
            .bind(("id", input.id))
            .await?;
        let Some(node) = resp.take::<Option<NodeEntity>>(0)? else {
            return Ok(None);
        };
        let ports = resp.take::<Vec<PortEntity>>(1)?;
        Ok(Some(NodeWithPorts { node, ports }))
    }
}

/// The node importing `canvas`, if any.
#[derive(Debug)]
pub struct FindImporterOf {
    pub canvas: CanvasId,
}

impl Processor<FindImporterOf> for SurrealProcessor {
    type Output = Option<NodeEntity>;
    type Error = surrealdb::Error;
    #[tracing::instrument(name = "Query:FindImporterOf", skip_all, err)]
    async fn process(&self, input: FindImporterOf) -> Result<Self::Output, Self::Error> {
        let mut resp = self
            .db()
            .query("SELECT * FROM orchestration_node WHERE spec.config.canvas = $canvas LIMIT 1")
            .bind(("canvas", input.canvas))
            .await?;
        resp.take::<Option<NodeEntity>>(0)
    }
}

/// Updates a node's editable metadata.
///
/// `position` is only written when it is `Some`, so an edit that leaves the
/// position out does not move the node. Whether the edit renamed the node is
/// decided inside the transaction, which is also where the canvas generation is
/// bumped, so a concurrent metadata write cannot land a rename without it.
pub struct UpdateNodeMetaRow {
    pub id: NodeId,
    pub canvas: CanvasId,
    pub name: String,
    pub comment: String,
    pub position: Option<CanvasUiPosition>,
    pub import_sync: Option<ImportSync>,
}

/// A node after a metadata update, plus whether the name actually changed.
#[derive(Debug, Clone, SurrealValue)]
pub struct NodeMetaUpdated {
    pub renamed: bool,
    pub node: NodeEntity,
    pub ports: Vec<PortEntity>,
}

impl Processor<UpdateNodeMetaRow> for SurrealProcessor {
    type Output = NodeMetaUpdated;
    type Error = surrealdb::Error;
    #[tracing::instrument(name = "Query-Transaction:UpdateNodeMetaRow", skip_all, err, fields(id = ?input.id))]
    async fn process(&self, input: UpdateNodeMetaRow) -> Result<Self::Output, Self::Error> {
        // Statement 0 is BEGIN, 1-5 the update steps; the RETURN is statement 6.
        let mut resp = self
            .db()
            .query(include_str!("../../../sql/node/update_node_meta_row.surql"))
            .bind(("id", input.id))
            .bind(("canvas", input.canvas))
            .bind(("name", input.name))
            .bind(("comment", input.comment))
            .bind(("position", input.position))
            .bind(("import_sync", input.import_sync))
            .await?;
        resp.take::<Option<NodeMetaUpdated>>(6)?
            .ok_or_else(|| surrealdb::Error::internal("node not found".to_string()))
    }
}

/// Rewrites a node's spec and reshapes its ports in place.
///
/// A port whose key survives keeps its row id, so the edges attached to it stay
/// attached; ports whose key disappears are deleted with their edges (the service
/// refuses the edit before that can happen silently).
pub struct UpdateNodeSpecRow {
    pub id: NodeId,
    pub canvas: CanvasId,
    pub spec: NodeSpec,
    pub ports: Vec<NewPort>,
    pub import_sync: Option<ImportSync>,
}

impl Processor<UpdateNodeSpecRow> for SurrealProcessor {
    type Output = NodeWithPorts;
    type Error = surrealdb::Error;
    #[tracing::instrument(name = "Query-Transaction:UpdateNodeSpecRow", skip_all, err, fields(id = ?input.id))]
    async fn process(&self, input: UpdateNodeSpecRow) -> Result<Self::Output, Self::Error> {
        // Statement 0 is BEGIN, 1 the spec update, 2 the reshape, 3 the import
        // sync, 4 the touch; the RETURN is statement 5.
        let mut resp = self
            .db()
            .query(include_str!("../../../sql/node/update_node_spec_row.surql"))
            .bind(("id", input.id))
            .bind(("canvas", input.canvas))
            .bind(("spec", input.spec))
            .bind(("new_ports", input.ports))
            .bind(("import_sync", input.import_sync))
            .await?;
        resp.take::<Option<NodeWithPorts>>(5)?
            .ok_or_else(|| surrealdb::Error::internal("update node returned no row".to_string()))
    }
}

#[derive(Debug)]
pub struct DeleteNodeRow {
    pub id: NodeId,
    pub canvas: CanvasId,
    pub import_sync: Option<ImportSync>,
    /// The canvas this node stops importing (set when it is an import node): it
    /// becomes a root again and gets its own generation bump.
    pub frees_canvas: Option<CanvasId>,
}

impl Processor<DeleteNodeRow> for SurrealProcessor {
    type Output = ();
    type Error = surrealdb::Error;
    #[tracing::instrument(name = "Query-Transaction:DeleteNodeRow", skip_all, err)]
    async fn process(&self, input: DeleteNodeRow) -> Result<Self::Output, Self::Error> {
        self.db()
            .query(include_str!("../../../sql/node/delete_node_row.surql"))
            .bind(("id", input.id))
            .bind(("canvas", input.canvas))
            .bind(("import_sync", input.import_sync))
            .bind(("frees", input.frees_canvas))
            .await?
            .check()?;
        Ok(())
    }
}

/// The distinct canvases the given nodes belong to; the relay leaf rotation
/// cron re-derives these after re-issuing.
#[derive(Debug)]
pub struct ListCanvasesOfNodes {
    pub nodes: Vec<NodeId>,
}

impl Processor<ListCanvasesOfNodes> for SurrealProcessor {
    type Output = Vec<CanvasId>;
    type Error = surrealdb::Error;
    #[tracing::instrument(name = "Query:ListCanvasesOfNodes", skip_all, err)]
    async fn process(&self, input: ListCanvasesOfNodes) -> Result<Self::Output, Self::Error> {
        if input.nodes.is_empty() {
            return Ok(Vec::new());
        }
        let mut resp = self
            .db()
            .query("SELECT VALUE canvas FROM orchestration_node WHERE id IN $nodes")
            .bind(("nodes", input.nodes))
            .await?;
        let mut canvases = resp.take::<Vec<CanvasId>>(0)?;
        canvases.sort_by_key(|c| record_key(&c.0));
        canvases.dedup_by_key(|c| record_key(&c.0));
        Ok(canvases)
    }
}

/// The root canvases of every tree holding a TLS or QUIC relay: the trees whose
/// derivation depends on the internal CA. `InitInternalCa` touches them so pods
/// reported invalid for lack of a CA get their first leaves.
#[derive(Debug)]
pub struct ListCanvasesWithRelayTls;

impl Processor<ListCanvasesWithRelayTls> for SurrealProcessor {
    type Output = Vec<CanvasId>;
    type Error = surrealdb::Error;
    #[tracing::instrument(name = "Query:ListCanvasesWithRelayTls", skip_all, err)]
    async fn process(&self, _: ListCanvasesWithRelayTls) -> Result<Self::Output, Self::Error> {
        let mut resp = self
            .db()
            .query(
                "SELECT VALUE fn::orchestration_root(canvas) FROM orchestration_node
                 WHERE spec.type = 'relay' AND spec.config.protocol IN ['tcp_tls', 'quic']",
            )
            .await?;
        let mut canvases = resp.take::<Vec<CanvasId>>(0)?;
        canvases.sort_by_key(|c| record_key(&c.0));
        canvases.dedup_by_key(|c| record_key(&c.0));
        Ok(canvases)
    }
}
