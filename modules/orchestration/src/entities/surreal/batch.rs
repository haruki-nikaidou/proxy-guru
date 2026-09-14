//! The one reconciling write of the universal-node layer: everything a
//! universal edit and the lane regeneration it implies do to a canvas tree,
//! applied in a single transaction (`sql/topology/apply_batch.surql`).

use crate::entities::surreal::canvas::{CanvasId, CanvasUiPosition};
use crate::entities::surreal::connection::EdgeConnectionId;
use crate::entities::surreal::node::{Lane, NewPort, NodeId, NodeSpec};
use crate::entities::surreal::port::PortId;
use kanau::processor::Processor;
use surrealdb_types::SurrealValue;
use wakuwaku::surreal::SurrealProcessor;

/// One end of an edge the batch relates, resolved inside the transaction by
/// `fn::orchestration_resolve_port`: exactly one of the three forms is set.
#[derive(Debug, Clone, SurrealValue, PartialEq, Eq)]
pub struct PortRef {
    /// An existing port.
    #[surreal(default)]
    pub port: Option<PortId>,
    /// A port by key on an existing node (possibly created by a reshape in the
    /// same batch).
    #[surreal(default)]
    pub node: Option<NodeId>,
    /// A port by key on the lane node of this key (possibly created in the
    /// same batch).
    #[surreal(default)]
    pub lane_key: Option<String>,
    #[surreal(default)]
    pub key: Option<String>,
}

impl PortRef {
    pub fn existing(port: PortId) -> Self {
        Self {
            port: Some(port),
            node: None,
            lane_key: None,
            key: None,
        }
    }

    pub fn on_node(node: NodeId, key: &str) -> Self {
        Self {
            port: None,
            node: Some(node),
            lane_key: None,
            key: Some(key.to_string()),
        }
    }

    pub fn on_lane(lane_key: &str, key: &str) -> Self {
        Self {
            port: None,
            node: None,
            lane_key: Some(lane_key.to_string()),
            key: Some(key.to_string()),
        }
    }
}

#[derive(Debug, Clone, SurrealValue)]
pub struct BatchEdge {
    pub source: PortRef,
    pub target: PortRef,
}

#[derive(Debug, Clone, SurrealValue)]
pub struct SpecUpdate {
    pub node: NodeId,
    pub spec: NodeSpec,
}

#[derive(Debug, Clone, SurrealValue)]
pub struct PortReshape {
    pub node: NodeId,
    pub ports: Vec<NewPort>,
}

/// A lane node to create with its ports.
#[derive(Debug, Clone, SurrealValue)]
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
        self.delete_edges.extend(other.delete_edges);
        self.delete_nodes.extend(other.delete_nodes);
        self.set_specs.extend(other.set_specs);
        self.reshape.extend(other.reshape);
        self.create_nodes.extend(other.create_nodes);
        self.add_edges.extend(other.add_edges);
    }
}

impl Processor<ApplyTopologyBatch> for SurrealProcessor {
    type Output = ();
    type Error = surrealdb::Error;
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
        let canvas = input
            .canvas
            .ok_or_else(|| surrealdb::Error::internal("batch without a canvas".to_string()))?;
        self.db()
            .query(include_str!("../../../sql/topology/apply_batch.surql"))
            .bind(("canvas", canvas))
            .bind(("delete_edges", input.delete_edges))
            .bind(("delete_nodes", input.delete_nodes))
            .bind(("set_specs", input.set_specs))
            .bind(("reshape", input.reshape))
            .bind(("create_nodes", input.create_nodes))
            .bind(("add_edges", input.add_edges))
            .await?
            .check()?;
        Ok(())
    }
}
