//! The one reconciling write of the universal-node layer: everything a
//! universal edit and the lane regeneration it implies do to a canvas tree,
//! applied in a single transaction.

use crate::entities::db::canvas::{CanvasFence, CanvasId, CanvasUiPosition};
use crate::entities::db::connection::{EdgeConnectionId, edge_conflict};
use crate::entities::db::fence;
use crate::entities::db::node::{Lane, NewPort, NodeId, NodeSpec, insert_node, write_spec};
use crate::entities::db::port::{PortId, insert_ports, reshape_ports, resolve_port};
use base::db::{Db, Error};
use kanau::processor::Processor;

/// The conflict reported when a batch names an edge endpoint that does not exist.
pub const UNRESOLVED_ENDPOINT: &str = "batch edge endpoint could not be resolved";
/// A batch that names no canvas cannot be fenced or touched; a caller bug.
pub const BATCH_WITHOUT_CANVAS: &str = "batch without a canvas";

/// One end of an edge the batch relates, resolved inside the transaction by
/// [`resolve_port`]: exactly one of the three forms is set.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct PortRef {
    /// An existing port.
    pub port: Option<PortId>,
    /// A port by key on an existing node (possibly created by a reshape in the
    /// same batch).
    pub node: Option<NodeId>,
    /// A port by key on the lane node of this key (possibly created in the
    /// same batch).
    pub lane_key: Option<String>,
    pub key: Option<String>,
}

impl PortRef {
    pub fn existing(port: PortId) -> Self {
        Self {
            port: Some(port),
            ..Self::default()
        }
    }

    pub fn on_node(node: NodeId, key: &str) -> Self {
        Self {
            node: Some(node),
            key: Some(key.to_string()),
            ..Self::default()
        }
    }

    pub fn on_lane(lane_key: &str, key: &str) -> Self {
        Self {
            lane_key: Some(lane_key.to_string()),
            key: Some(key.to_string()),
            ..Self::default()
        }
    }
}

#[derive(Debug, Clone)]
pub struct BatchEdge {
    pub source: PortRef,
    pub target: PortRef,
}

#[derive(Debug, Clone)]
pub struct SpecUpdate {
    pub node: NodeId,
    pub spec: NodeSpec,
}

#[derive(Debug, Clone)]
pub struct PortReshape {
    pub node: NodeId,
    pub ports: Vec<NewPort>,
}

/// A lane node to create with its ports.
#[derive(Debug, Clone)]
pub struct NewLaneNode {
    pub canvas: CanvasId,
    pub name: String,
    pub comment: String,
    pub spec: NodeSpec,
    pub position: CanvasUiPosition,
    pub ports: Vec<NewPort>,
    pub lane: Lane,
}

/// Applied in this order: edges deleted, nodes deleted (with their ports, edges,
/// health history and relay leaf), specs rewritten, ports reshaped, lane nodes
/// created, edges related, tree touched.
#[derive(Debug, Clone, Default)]
pub struct ApplyTopologyBatch {
    pub canvas: Option<CanvasId>,
    /// The snapshot the edit was validated against, fencing the whole batch;
    /// `None` bumps unconditionally (see [`fence::touch_checked`]).
    pub fence: Option<CanvasFence>,
    pub delete_edges: Vec<EdgeConnectionId>,
    pub delete_nodes: Vec<NodeId>,
    pub set_specs: Vec<SpecUpdate>,
    pub reshape: Vec<PortReshape>,
    pub create_nodes: Vec<NewLaneNode>,
    pub add_edges: Vec<BatchEdge>,
}

impl ApplyTopologyBatch {
    /// Whether the batch writes anything besides the generation bump.
    pub fn is_empty(&self) -> bool {
        self.delete_edges.is_empty()
            && self.delete_nodes.is_empty()
            && self.set_specs.is_empty()
            && self.reshape.is_empty()
            && self.create_nodes.is_empty()
            && self.add_edges.is_empty()
    }

    /// Appends `other`'s operations after this batch's own, section by section.
    pub fn extend(&mut self, other: ApplyTopologyBatch) {
        if self.canvas.is_none() {
            self.canvas = other.canvas;
        }
        if self.fence.is_none() {
            self.fence = other.fence;
        }
        self.delete_edges.extend(other.delete_edges);
        self.delete_nodes.extend(other.delete_nodes);
        self.set_specs.extend(other.set_specs);
        self.reshape.extend(other.reshape);
        self.create_nodes.extend(other.create_nodes);
        self.add_edges.extend(other.add_edges);
    }
}

impl Processor<ApplyTopologyBatch> for Db {
    type Output = ();
    type Error = Error;
    #[tracing::instrument(
        name = "Query-Transaction:ApplyTopologyBatch",
        skip_all,
        err,
        fields(
            delete_edges = input.delete_edges.len(),
            delete_nodes = input.delete_nodes.len(),
            set_specs = input.set_specs.len(),
            reshape = input.reshape.len(),
            create_nodes = input.create_nodes.len(),
            add_edges = input.add_edges.len(),
        )
    )]
    async fn process(&self, input: ApplyTopologyBatch) -> Result<Self::Output, Self::Error> {
        let canvas = input.canvas.ok_or(Error::Conflict(BATCH_WITHOUT_CANVAS))?;
        // Statement order is load-bearing: rows are deleted first so a recreated
        // lane never clashes with the one it replaces, specs and ports are
        // reshaped next, new lane nodes are created with their ports, and only
        // then are edges added — an edge may name a port that did not exist when
        // the batch was built, either on a node that was just reshaped
        // (`{ node, key }`) or on a lane node just created (`{ lane_key, key }`),
        // so endpoints are resolved here, inside the transaction.
        let mut tx = self.db().begin().await?;
        sqlx::query("DELETE FROM orchestration_edge_connection WHERE id = ANY($1)")
            .bind(&input.delete_edges)
            .execute(&mut *tx)
            .await?;
        // Ports, edges, health history and relay leaves cascade from the node.
        sqlx::query("DELETE FROM orchestration_node WHERE id = ANY($1)")
            .bind(&input.delete_nodes)
            .execute(&mut *tx)
            .await?;
        for update in &input.set_specs {
            write_spec(&mut tx, &update.node, &update.spec).await?;
        }
        for reshape in &input.reshape {
            reshape_ports(&mut tx, &reshape.node, &reshape.ports).await?;
        }
        for lane in &input.create_nodes {
            let node = insert_node(
                &mut tx,
                &lane.canvas,
                &lane.name,
                &lane.comment,
                &lane.spec,
                lane.position,
                Some(&lane.lane),
            )
            .await?;
            insert_ports(&mut tx, &node.id, &lane.ports).await?;
        }
        for edge in &input.add_edges {
            let source = resolve_port(&mut tx, &canvas, &edge.source).await?;
            let target = resolve_port(&mut tx, &canvas, &edge.target).await?;
            let (Some(source), Some(target)) = (source, target) else {
                return Err(Error::Conflict(UNRESOLVED_ENDPOINT));
            };
            sqlx::query(
                "INSERT INTO orchestration_edge_connection (id, source_port, target_port)
                 VALUES ($1, $2, $3)",
            )
            .bind(EdgeConnectionId::new())
            .bind(&source)
            .bind(&target)
            .execute(&mut *tx)
            .await
            .map_err(|e| edge_conflict(Error::from(e)))?;
        }
        fence::touch_checked(&mut tx, &canvas, input.fence.as_ref()).await?;
        tx.commit().await?;
        Ok(())
    }
}
