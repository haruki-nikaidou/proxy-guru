//! Topology-related query processors spanning several entity kinds.

use crate::entities::surreal::canvas::{CanvasContents, CanvasEntity, CanvasId};
use crate::entities::surreal::connection::EdgeConnectionEntity;
use crate::entities::surreal::node::{NodeEntity, NodeWithPorts};
use crate::entities::surreal::port::PortEntity;
use crate::entities::surreal::server::{ServerEntity, ServerId};
use kanau::processor::Processor;
use std::collections::HashMap;
use wakuwaku::surreal::SurrealProcessor;

/// A consistent read of everything the topology checker and the config deriver
/// need: one whole canvas tree, from its root.
#[derive(Debug, Clone)]
pub struct CanvasTopology {
    pub root: CanvasId,
    /// Every canvas of the tree, root first.
    pub canvases: Vec<CanvasEntity>,
    pub servers: Vec<ServerEntity>,
    pub nodes: Vec<NodeWithPorts>,
    pub edges: Vec<EdgeConnectionEntity>,
}

impl CanvasTopology {
    pub fn canvas_ids(&self) -> Vec<CanvasId> {
        self.canvases.iter().map(|c| c.id.clone()).collect()
    }
}

/// Loads the tree containing `canvas`. A canvas that does not exist yields an
/// empty topology whose `root` is the given id.
pub struct LoadCanvasTopology {
    pub canvas: CanvasId,
}

impl Processor<LoadCanvasTopology> for SurrealProcessor {
    type Output = CanvasTopology;
    type Error = surrealdb::Error;
    #[tracing::instrument(name = "Query-Transaction:LoadCanvasTopology", skip_all, err)]
    async fn process(&self, input: LoadCanvasTopology) -> Result<Self::Output, Self::Error> {
        let rows = load_canvas(self, &input.canvas).await?;
        Ok(CanvasTopology {
            root: rows.root,
            canvases: rows.canvases,
            servers: rows.servers,
            nodes: rows.nodes,
            edges: rows.edges,
        })
    }
}

/// Everything the dashboard renders for one canvas.
pub struct LoadCanvasContents {
    pub canvas: CanvasId,
}

impl Processor<LoadCanvasContents> for SurrealProcessor {
    type Output = Option<CanvasContents>;
    type Error = surrealdb::Error;
    #[tracing::instrument(name = "Query-Transaction:LoadCanvasContents", skip_all, err)]
    async fn process(&self, input: LoadCanvasContents) -> Result<Self::Output, Self::Error> {
        let mut resp = self
            .db()
            .query("SELECT * FROM $canvas")
            .bind(("canvas", input.canvas.clone()))
            .await?;
        let Some(canvas) = resp.take::<Option<CanvasEntity>>(0)? else {
            return Ok(None);
        };
        // The dashboard shows one canvas, but the tree read is one consistent
        // snapshot and already holds the import targets.
        let rows = load_canvas(self, &input.canvas).await?;
        let key = crate::utils::ids::record_key(&input.canvas.0);
        let mine = |c: &CanvasId| crate::utils::ids::record_key(&c.0) == key;
        // `fn::orchestration_ancestors` is parent-first; the dashboard wants
        // root-first. The rows come back in storage order, so map them onto the
        // reversed chain rather than trusting any read order.
        let mut resp = self
            .db()
            .query(
                "LET $chain = fn::orchestration_ancestors($canvas, 0);
                    RETURN $chain;
                    SELECT * FROM orchestration_canvas WHERE id IN $chain;",
            )
            .bind(("canvas", input.canvas.clone()))
            .await?;
        let chain = resp.take::<Vec<CanvasId>>(1)?;
        let mut ancestor_rows: HashMap<String, CanvasEntity> = resp
            .take::<Vec<CanvasEntity>>(2)?
            .into_iter()
            .map(|c| (crate::utils::ids::record_key(&c.id.0), c))
            .collect();
        let ancestors: Vec<CanvasEntity> = chain
            .iter()
            .rev()
            .filter_map(|id| ancestor_rows.remove(&crate::utils::ids::record_key(&id.0)))
            .collect();
        let nodes: Vec<NodeWithPorts> = rows
            .nodes
            .into_iter()
            .filter(|n| mine(&n.node.canvas))
            .collect();
        let targets: Vec<String> = nodes
            .iter()
            .filter_map(|n| match &n.node.spec {
                crate::entities::surreal::node::NodeSpec::CanvasImport(cfg) => {
                    Some(crate::utils::ids::record_key(&cfg.canvas.0))
                }
                _ => None,
            })
            .collect();
        let import_targets = rows
            .canvases
            .iter()
            .filter(|c| targets.contains(&crate::utils::ids::record_key(&c.id.0)))
            .cloned()
            .collect();

        let servers = rows
            .servers
            .into_iter()
            .filter(|s| mine(&s.canvas))
            .collect();
        let port_owners: std::collections::HashSet<String> = nodes
            .iter()
            .flat_map(|n| {
                n.ports
                    .iter()
                    .map(|p| crate::utils::ids::record_key(&p.id.0))
            })
            .collect();
        let edges = rows
            .edges
            .into_iter()
            .filter(|e| port_owners.contains(&crate::utils::ids::record_key(&e.source.0)))
            .collect();
        Ok(Some(CanvasContents {
            canvas,
            ancestors,
            import_targets,
            servers,
            nodes,
            edges,
        }))
    }
}

pub struct FindCanvasOfServer {
    pub server: ServerId,
}

impl Processor<FindCanvasOfServer> for SurrealProcessor {
    type Output = Option<CanvasId>;
    type Error = surrealdb::Error;
    #[tracing::instrument(name = "Query:FindCanvasOfServer", skip_all, err)]
    async fn process(&self, input: FindCanvasOfServer) -> Result<Self::Output, Self::Error> {
        let mut resp = self
            .db()
            .query("SELECT VALUE canvas FROM $server")
            .bind(("server", input.server))
            .await?;
        resp.take::<Option<CanvasId>>(0)
    }
}

pub(crate) struct CanvasRows {
    pub root: CanvasId,
    pub canvases: Vec<CanvasEntity>,
    pub servers: Vec<ServerEntity>,
    pub nodes: Vec<NodeWithPorts>,
    pub edges: Vec<EdgeConnectionEntity>,
}

/// Reads the canvases, servers, nodes (with their ports) and edges of one canvas
/// tree in one transaction.
async fn load_canvas(
    sp: &SurrealProcessor,
    canvas: &CanvasId,
) -> Result<CanvasRows, surrealdb::Error> {
    let mut resp = sp
        .db()
        .query(include_str!("../../../sql/topology/load_canvas.surql"))
        .bind(("canvas", canvas.clone()))
        .await?;
    // Statement 0 is BEGIN, 1-2 the LETs; the canvases are at 3 and the root at 8.
    let root = resp
        .take::<Option<CanvasId>>(8)?
        .unwrap_or_else(|| canvas.clone());
    group_rows(&mut resp, 3, root)
}

/// Groups the canvas read at `offset` and the four row reads that follow it (the
/// statement shape of `load_canvas.surql`) into ports-per-node shape. Shared with
/// the derivation read, which wraps the same statements in a transaction.
pub(crate) fn group_rows(
    resp: &mut surrealdb::IndexedResults,
    offset: usize,
    root: CanvasId,
) -> Result<CanvasRows, surrealdb::Error> {
    let mut canvases = resp.take::<Vec<CanvasEntity>>(offset)?;
    let root_key = crate::utils::ids::record_key(&root.0);
    canvases.sort_by_cached_key(|c| {
        let key = crate::utils::ids::record_key(&c.id.0);
        (key != root_key, key)
    });
    let servers = resp.take::<Vec<ServerEntity>>(offset.saturating_add(1))?;
    let node_rows = resp.take::<Vec<NodeEntity>>(offset.saturating_add(2))?;
    let port_rows = resp.take::<Vec<PortEntity>>(offset.saturating_add(3))?;
    let edges = resp.take::<Vec<EdgeConnectionEntity>>(offset.saturating_add(4))?;

    let mut ports_by_node: HashMap<String, Vec<PortEntity>> = HashMap::new();
    for port in port_rows {
        ports_by_node
            .entry(crate::utils::ids::record_key(&port.owner.0))
            .or_default()
            .push(port);
    }
    let nodes = node_rows
        .into_iter()
        .map(|node| {
            let ports = ports_by_node
                .remove(crate::utils::ids::record_key(&node.id.0).as_str())
                .unwrap_or_default();
            NodeWithPorts { node, ports }
        })
        .collect();
    Ok(CanvasRows {
        root,
        canvases,
        servers,
        nodes,
        edges,
    })
}
