//! Topology-related query processors spanning several entity kinds.

use crate::entities::db::canvas::{
    CanvasContents, CanvasEntity, CanvasFence, CanvasId, SNAPSHOT_READ,
};
use crate::entities::db::connection::EdgeConnectionEntity;
use crate::entities::db::node::{NodeEntity, NodeRow, NodeSpec, NodeWithPorts};
use crate::entities::db::port::PortEntity;
use crate::entities::db::server::{ServerEntity, ServerId};
use crate::entities::db::tree;
use base::db::{Db, Error};
use kanau::processor::Processor;
use sqlx::PgConnection;
use std::collections::{HashMap, HashSet};

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

    /// The fence a write validated against this snapshot must pass: the tree's
    /// root id and the generation it was read at (see [`CanvasFence`]). `None`
    /// when the snapshot carries no root row — an absent or empty canvas, which
    /// is not a writable state — so a validated caller fails closed rather than
    /// bumping unconditionally.
    pub fn fence(&self) -> Option<CanvasFence> {
        self.canvases
            .iter()
            .find(|c| c.id == self.root)
            .map(|c| CanvasFence {
                root: self.root.clone(),
                generation: c.generation,
            })
    }
}

/// Loads the tree containing `canvas`. A canvas that does not exist yields an
/// empty topology whose `root` is the given id.
pub struct LoadCanvasTopology {
    pub canvas: CanvasId,
}

impl Processor<LoadCanvasTopology> for Db {
    type Output = CanvasTopology;
    type Error = Error;
    #[tracing::instrument(name = "Query-Transaction:LoadCanvasTopology", skip_all, err)]
    async fn process(&self, input: LoadCanvasTopology) -> Result<Self::Output, Self::Error> {
        let mut tx = self.db().begin_with(SNAPSHOT_READ).await?;
        let rows = load_canvas(&mut tx, &input.canvas).await?;
        tx.commit().await?;
        Ok(rows.into_topology())
    }
}

/// Everything the dashboard renders for one canvas.
pub struct LoadCanvasContents {
    pub canvas: CanvasId,
}

impl Processor<LoadCanvasContents> for Db {
    type Output = Option<CanvasContents>;
    type Error = Error;
    #[tracing::instrument(name = "Query-Transaction:LoadCanvasContents", skip_all, err)]
    async fn process(&self, input: LoadCanvasContents) -> Result<Self::Output, Self::Error> {
        let mut tx = self.db().begin_with(SNAPSHOT_READ).await?;
        let canvas: Option<CanvasEntity> =
            sqlx::query_as("SELECT * FROM orchestration_canvas WHERE id = $1")
                .bind(&input.canvas)
                .fetch_optional(&mut *tx)
                .await?;
        let Some(canvas) = canvas else {
            return Ok(None);
        };
        // The dashboard shows one canvas, but the tree read is one consistent
        // snapshot and already holds the import targets.
        let rows = load_canvas(&mut tx, &input.canvas).await?;
        // `ancestors_of` is parent-first; the dashboard wants root-first.
        let chain = tree::ancestors_of(&mut tx, &input.canvas).await?;
        tx.commit().await?;
        let mut ancestor_rows: HashMap<CanvasId, CanvasEntity> = rows
            .canvases
            .iter()
            .filter(|c| chain.contains(&c.id))
            .map(|c| (c.id.clone(), c.clone()))
            .collect();
        let ancestors: Vec<CanvasEntity> = chain
            .iter()
            .rev()
            .filter_map(|id| ancestor_rows.remove(id))
            .collect();
        let nodes: Vec<NodeWithPorts> = rows
            .nodes
            .into_iter()
            .filter(|n| n.node.canvas == input.canvas)
            .collect();
        let targets: HashSet<&CanvasId> = nodes
            .iter()
            .filter_map(|n| match &n.node.spec {
                NodeSpec::CanvasImport(cfg) => Some(&cfg.canvas),
                _ => None,
            })
            .collect();
        let import_targets = rows
            .canvases
            .iter()
            .filter(|c| targets.contains(&c.id))
            .cloned()
            .collect();
        let servers = rows
            .servers
            .into_iter()
            .filter(|s| s.canvas == input.canvas)
            .collect();
        let port_owners: HashSet<&crate::entities::db::port::PortId> = nodes
            .iter()
            .flat_map(|n| n.ports.iter().map(|p| &p.id))
            .collect();
        let edges = rows
            .edges
            .into_iter()
            .filter(|e| port_owners.contains(&e.source))
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

impl Processor<FindCanvasOfServer> for Db {
    type Output = Option<CanvasId>;
    type Error = Error;
    #[tracing::instrument(name = "Query:FindCanvasOfServer", skip_all, err)]
    async fn process(&self, input: FindCanvasOfServer) -> Result<Self::Output, Self::Error> {
        Ok(
            sqlx::query_scalar("SELECT canvas FROM orchestration_server WHERE id = $1")
                .bind(input.server)
                .fetch_optional(self.db())
                .await?,
        )
    }
}

pub(crate) struct CanvasRows {
    pub root: CanvasId,
    pub canvases: Vec<CanvasEntity>,
    pub servers: Vec<ServerEntity>,
    pub nodes: Vec<NodeWithPorts>,
    pub edges: Vec<EdgeConnectionEntity>,
}

impl CanvasRows {
    pub(crate) fn into_topology(self) -> CanvasTopology {
        CanvasTopology {
            root: self.root,
            canvases: self.canvases,
            servers: self.servers,
            nodes: self.nodes,
            edges: self.edges,
        }
    }
}

/// Reads the canvases, servers, nodes (with their ports) and edges of the tree
/// containing `canvas`, inside the caller's transaction. Shared with the
/// derivation read.
pub(crate) async fn load_canvas(
    conn: &mut PgConnection,
    canvas: &CanvasId,
) -> Result<CanvasRows, Error> {
    let (root, tree) = tree::whole_tree_of(&mut *conn, canvas).await?;
    let mut canvases: Vec<CanvasEntity> =
        sqlx::query_as("SELECT * FROM orchestration_canvas WHERE id = ANY($1)")
            .bind(&tree)
            .fetch_all(&mut *conn)
            .await?;
    canvases.sort_by_cached_key(|c| (c.id != root, c.id.clone()));
    let servers: Vec<ServerEntity> =
        sqlx::query_as("SELECT * FROM orchestration_server WHERE canvas = ANY($1) ORDER BY id")
            .bind(&tree)
            .fetch_all(&mut *conn)
            .await?;
    let node_rows: Vec<NodeRow> =
        sqlx::query_as("SELECT * FROM orchestration_node WHERE canvas = ANY($1) ORDER BY id")
            .bind(&tree)
            .fetch_all(&mut *conn)
            .await?;
    let port_rows: Vec<PortEntity> = sqlx::query_as(
        "SELECT p.* FROM orchestration_port p
         JOIN orchestration_node n ON n.id = p.owner
         WHERE n.canvas = ANY($1) ORDER BY p.position, p.id",
    )
    .bind(&tree)
    .fetch_all(&mut *conn)
    .await?;
    let edges: Vec<EdgeConnectionEntity> = sqlx::query_as(
        "SELECT e.* FROM orchestration_edge_connection e
         JOIN orchestration_port p ON p.id = e.source_port
         JOIN orchestration_node n ON n.id = p.owner
         WHERE n.canvas = ANY($1) ORDER BY e.id",
    )
    .bind(&tree)
    .fetch_all(conn)
    .await?;

    let mut ports_by_node: HashMap<crate::entities::db::node::NodeId, Vec<PortEntity>> =
        HashMap::new();
    for port in port_rows {
        ports_by_node
            .entry(port.owner.clone())
            .or_default()
            .push(port);
    }
    let nodes = node_rows
        .into_iter()
        .map(NodeEntity::from)
        .map(|node| {
            let ports = ports_by_node.remove(&node.id).unwrap_or_default();
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
