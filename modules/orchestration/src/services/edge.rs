//! Edge operations. All edge legality lives in the topology checker.
//!
//! Edges on universal nodes are special in one way: their ports are created on
//! demand. A connect that names a handle *group* of a universal node instead of
//! a port ([`ConnectUniversal`]) creates the port, relates the edge and
//! regenerates the node's lanes in one write; a disconnect on such a port drops
//! the port and the lanes with it. Edges on the generated side (a `lane:` port,
//! any port of a lane node) are never edited by hand.

use crate::config::OrchestrationConfig;
use crate::entities::surreal::batch::{ApplyTopologyBatch, BatchEdge, PortRef, PortReshape};
use crate::entities::surreal::connection::{
    ConnectPorts, DeleteEdgeRow, EdgeConnectionEntity, EdgeConnectionId, FindEdgeById,
};
use crate::entities::surreal::node::{FindNodeById, NewPort, NodeId, NodeSpec, NodeWithPorts};
use crate::entities::surreal::port::{FindPortById, PortDirection, PortEntity, PortId, PortKind};
use crate::entities::surreal::topology::{CanvasTopology, LoadCanvasTopology};
use crate::entities::surreal::view::ListServerConfigViewsByCanvases;
use crate::services::OrchestrationError;
use crate::services::converge::ensure_switch_safe;
use crate::services::node::port_rows;
use crate::services::rollout::DirtyNotifier;
use crate::services::topology::{TopologyEdit, ensure_valid};
use crate::services::universal;
use crate::utils::ids;
use crate::utils::ids::record_key;
use auth::entities::surreal::account::AccountRole;
use auth::services::identity::Identity;
use auth::utils::rbac::Permission;
use kanau::processor::Processor;
use wakuwaku::surreal::SurrealProcessor;

#[derive(Clone)]
pub struct EdgeService {
    pub db: SurrealProcessor,
    pub notifier: DirtyNotifier,
    pub config: OrchestrationConfig,
}

/// The canvas an edge endpoint belongs to.
async fn canvas_of_port(
    db: &SurrealProcessor,
    port: &PortId,
) -> Result<crate::entities::surreal::canvas::CanvasId, OrchestrationError> {
    let row = db
        .process(FindPortById { id: port.clone() })
        .await?
        .ok_or(OrchestrationError::NotFound)?;
    let node = db
        .process(FindNodeById { id: row.owner })
        .await?
        .ok_or(OrchestrationError::NotFound)?;
    Ok(node.canvas)
}

/// A port and its owner, from a loaded topology.
fn port_in<'a>(
    topology: &'a CanvasTopology,
    port: &PortId,
) -> Result<(&'a PortEntity, &'a NodeWithPorts), OrchestrationError> {
    let key = record_key(&port.0);
    topology
        .nodes
        .iter()
        .find_map(|n| n.ports.iter().find(|p| record_key(&p.id.0) == key).map(|p| (p, n)))
        .ok_or(OrchestrationError::NotFound)
}

/// Edges on the generated side of the graph are not the operator's to draw or
/// cut; bundle ports are only reached through handles.
fn ensure_operator_port(port: &PortEntity, owner: &NodeWithPorts) -> Result<(), OrchestrationError> {
    if universal::is_managed_port(port, &owner.node) {
        return Err(OrchestrationError::Conflict(
            "port is managed by a universal node; edit that node instead".into(),
        ));
    }
    if port.kind == PortKind::Bundle {
        return Err(OrchestrationError::Invalid(
            "bundle ports are connected through the node's bundle handle".into(),
        ));
    }
    Ok(())
}

pub struct Connect {
    pub actor: Identity,
    pub output_port: PortId,
    pub input_port: PortId,
}

impl Processor<Connect> for EdgeService {
    type Output = EdgeConnectionEntity;
    type Error = OrchestrationError;
    #[tracing::instrument(name = "Service:Connect", skip_all, err)]
    async fn process(&self, input: Connect) -> Result<Self::Output, Self::Error> {
        input.actor.ensure(Permission::EditWorkspace)?;
        let canvas = canvas_of_port(&self.db, &input.output_port).await?;
        let topology = self
            .db
            .process(LoadCanvasTopology {
                canvas: canvas.clone(),
            })
            .await?;
        for port in [&input.output_port, &input.input_port] {
            let (port, owner) = port_in(&topology, port)?;
            ensure_operator_port(port, owner)?;
        }
        let projected = topology.project(&[TopologyEdit::AddEdge {
            edge: EdgeConnectionEntity {
                id: ids::edge_id("pending-0"),
                source: input.output_port.clone(),
                target: input.input_port.clone(),
            },
        }]);
        ensure_valid(&projected)?;
        let views = self
            .db
            .process(ListServerConfigViewsByCanvases {
                canvases: projected.canvas_ids(),
            })
            .await?;
        ensure_switch_safe(&projected, &views, &self.config)?;

        let edge = self
            .db
            .process(ConnectPorts {
                source: input.output_port,
                target: input.input_port,
                canvas: canvas.clone(),
            })
            .await?;
        self.notifier.notify(&topology.root).await;
        Ok(edge)
    }
}

/// The handle groups a connect may name on a universal node.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UniversalGroup {
    /// A distributor's channel outputs.
    ChannelOut,
    /// A universal pod's or aggregator's incoming bundles.
    BundleIn,
    /// A distributor's outgoing bundles, or a universal pod's single one.
    BundleOut,
}

/// One end of a [`ConnectUniversal`].
#[derive(Debug, Clone)]
pub enum ConnectEnd {
    Port(PortId),
    Handle { node: NodeId, group: UniversalGroup },
}

impl ConnectEnd {
    fn node_of<'a>(&self, topology: &'a CanvasTopology) -> Result<&'a NodeWithPorts, OrchestrationError> {
        match self {
            ConnectEnd::Port(port) => port_in(topology, port).map(|(_, n)| n),
            ConnectEnd::Handle { node, .. } => {
                let key = record_key(&node.0);
                topology
                    .nodes
                    .iter()
                    .find(|n| record_key(&n.node.id.0) == key)
                    .ok_or(OrchestrationError::NotFound)
            }
        }
    }

    async fn canvas(
        &self,
        db: &SurrealProcessor,
    ) -> Result<crate::entities::surreal::canvas::CanvasId, OrchestrationError> {
        match self {
            ConnectEnd::Port(port) => canvas_of_port(db, port).await,
            ConnectEnd::Handle { node, .. } => Ok(db
                .process(FindNodeById { id: node.clone() })
                .await?
                .ok_or(OrchestrationError::NotFound)?
                .canvas),
        }
    }
}

/// A connect where at least one end is a universal node's handle. The port
/// behind the handle is created in the same write as the edge and the lanes
/// the new edge calls for.
pub struct ConnectUniversal {
    pub actor: Identity,
    pub output: ConnectEnd,
    pub input: ConnectEnd,
}

/// One side's contribution to the write: the port list the node ends up with
/// and the key the edge attaches to.
struct Resolved<'a> {
    node: &'a NodeWithPorts,
    ports: Vec<NewPort>,
    key: String,
}

fn as_new_ports(ports: &[PortEntity]) -> Vec<NewPort> {
    ports
        .iter()
        .map(|p| NewPort {
            kind: p.kind,
            direction: p.direction,
            key: p.key.clone(),
            position: p.position,
        })
        .collect()
}

fn has_edge(topology: &CanvasTopology, port: &PortEntity) -> bool {
    let key = record_key(&port.id.0);
    topology
        .edges
        .iter()
        .any(|e| record_key(&e.source.0) == key || record_key(&e.target.0) == key)
}

impl Processor<ConnectUniversal> for EdgeService {
    type Output = EdgeConnectionEntity;
    type Error = OrchestrationError;
    #[tracing::instrument(name = "Service:ConnectUniversal", skip_all, err)]
    async fn process(&self, input: ConnectUniversal) -> Result<Self::Output, Self::Error> {
        input.actor.ensure(Permission::EditWorkspace)?;
        let canvas = input.output.canvas(&self.db).await?;
        let topology = self
            .db
            .process(LoadCanvasTopology {
                canvas: canvas.clone(),
            })
            .await?;
        let source = input.output.node_of(&topology)?;
        let target = input.input.node_of(&topology)?;

        let (out, inp) = match (&input.output, &input.input) {
            // An entry pod becomes a channel of a distributor.
            (
                ConnectEnd::Handle {
                    group: UniversalGroup::ChannelOut,
                    ..
                },
                ConnectEnd::Port(port),
            ) => {
                if !matches!(
                    source.node.spec,
                    NodeSpec::LoadBalanceDistribute(_) | NodeSpec::UniversalPod(_)
                ) {
                    return Err(OrchestrationError::Invalid(
                        "only a distribute node or a universal pod starts channels".into(),
                    ));
                }
                let (port, owner) = port_in(&topology, port)?;
                ensure_operator_port(port, owner)?;
                if !matches!(owner.node.spec, NodeSpec::Pod(_)) || port.key != "destination" {
                    return Err(OrchestrationError::Invalid(
                        "a channel starts at the destination of a pod".into(),
                    ));
                }
                let pod = record_key(&owner.node.id.0);
                let ordinal = universal::next_ordinal(&topology);
                let mut ports = as_new_ports(&source.ports);
                if ports.iter().any(|p| p.key == universal::chan_key(&pod)) {
                    return Err(OrchestrationError::Conflict(
                        "pod is already a channel of this distributor".into(),
                    ));
                }
                ports.push(NewPort {
                    kind: PortKind::DeriveDestination,
                    direction: PortDirection::Output,
                    key: universal::chan_key(&pod),
                    position: ordinal,
                });
                ports.push(NewPort {
                    kind: PortKind::DeriveDestination,
                    direction: PortDirection::Input,
                    key: universal::lane_key(&pod),
                    position: ordinal,
                });
                (
                    Resolved {
                        node: source,
                        ports,
                        key: universal::chan_key(&pod),
                    },
                    Resolved {
                        node: owner,
                        ports: as_new_ports(&owner.ports),
                        key: port.key.clone(),
                    },
                )
            }
            // A bundle between two universal nodes.
            (
                ConnectEnd::Handle {
                    group: UniversalGroup::BundleOut,
                    ..
                },
                ConnectEnd::Handle {
                    group: UniversalGroup::BundleIn,
                    ..
                },
            ) => {
                let source_key = record_key(&source.node.id.0);
                let target_key = record_key(&target.node.id.0);
                let out_key = match &source.node.spec {
                    NodeSpec::LoadBalanceDistribute(_) => universal::bundle_out_key(&target_key),
                    NodeSpec::UniversalPod(_) => universal::BUNDLE_OUT.to_string(),
                    _ => {
                        return Err(OrchestrationError::Invalid(
                            "only a distribute node or a universal pod bundles out".into(),
                        ));
                    }
                };
                if !target.node.spec.takes_bundles() {
                    return Err(OrchestrationError::Invalid(
                        "only a universal pod, a distribute node or an aggregate node takes bundles in".into(),
                    ));
                }
                if matches!(source.node.spec, NodeSpec::LoadBalanceDistribute(_))
                    && matches!(target.node.spec, NodeSpec::LoadBalanceAggregate(_))
                {
                    return Err(OrchestrationError::Invalid(
                        "a distribute node bundles to universal pods; bundle those to the aggregate node".into(),
                    ));
                }
                let in_key = universal::bundle_in_key(&source_key);
                if let Some(existing) = source.ports.iter().find(|p| p.key == out_key)
                    && has_edge(&topology, existing)
                {
                    return Err(OrchestrationError::Conflict(match &source.node.spec {
                        NodeSpec::UniversalPod(_) => {
                            "a universal pod bundles out to one place; disconnect it first".into()
                        }
                        _ => "these nodes are already bundled".into(),
                    }));
                }
                if target.ports.iter().any(|p| p.key == in_key) {
                    return Err(OrchestrationError::Conflict(
                        "these nodes are already bundled".into(),
                    ));
                }
                let mut out_ports = as_new_ports(&source.ports);
                if !out_ports.iter().any(|p| p.key == out_key) {
                    out_ports.push(NewPort {
                        kind: PortKind::Bundle,
                        direction: PortDirection::Output,
                        key: out_key.clone(),
                        position: 0,
                    });
                }
                let mut in_ports = as_new_ports(&target.ports);
                in_ports.push(NewPort {
                    kind: PortKind::Bundle,
                    direction: PortDirection::Input,
                    key: in_key.clone(),
                    position: 0,
                });
                (
                    Resolved {
                        node: source,
                        ports: out_ports,
                        key: out_key,
                    },
                    Resolved {
                        node: target,
                        ports: in_ports,
                        key: in_key,
                    },
                )
            }
            _ => {
                return Err(OrchestrationError::Invalid(
                    "a universal handle connects a channel output to a pod, or a bundle output to a bundle input"
                        .into(),
                ));
            }
        };
        if record_key(&out.node.node.id.0) == record_key(&inp.node.node.id.0) {
            return Err(OrchestrationError::Invalid(
                "a node cannot be bundled to itself".into(),
            ));
        }

        // The projection: reshaped ports (kept keys keep their ids) plus the
        // edge on the placeholder ports; the batch: the same, by key.
        let mut edits = Vec::new();
        let mut batch = ApplyTopologyBatch {
            canvas: Some(canvas.clone()),
            ..Default::default()
        };
        let mut rows = Vec::new();
        for side in [&out, &inp] {
            let projected = port_rows(&side.node.node.id, &side.node.ports, &side.ports);
            let port = projected
                .iter()
                .find(|p| p.key == side.key)
                .cloned()
                .ok_or(OrchestrationError::NotFound)?;
            if as_new_ports(&side.node.ports) != side.ports {
                edits.push(TopologyEdit::ReshapePorts {
                    node: side.node.node.id.clone(),
                    ports: projected,
                });
                batch.reshape.push(PortReshape {
                    node: side.node.node.id.clone(),
                    ports: side.ports.clone(),
                });
            }
            rows.push(port);
        }
        let (Some(source_port), Some(target_port)) = (rows.first(), rows.get(1)) else {
            return Err(OrchestrationError::NotFound);
        };
        edits.push(TopologyEdit::AddEdge {
            edge: EdgeConnectionEntity {
                id: ids::edge_id("pending-0"),
                source: source_port.id.clone(),
                target: target_port.id.clone(),
            },
        });
        let port_ref = |port: &PortEntity| {
            if record_key(&port.id.0).starts_with(universal::PENDING_PORT_PREFIX) {
                PortRef::on_node(port.owner.clone(), &port.key)
            } else {
                PortRef::existing(port.id.clone())
            }
        };
        batch.add_edges.push(BatchEdge {
            source: port_ref(source_port),
            target: port_ref(target_port),
        });
        let prepared =
            universal::prepare(&self.db, &self.config, &topology, universal::Primary { edits, batch })
                .await?;
        universal::apply(&self.db, &self.notifier, prepared).await?;

        // The edge, by its ends, now that both ports exist.
        let mut resp = self
            .db
            .db()
            .query(
                "SELECT * FROM orchestration_edge_connection
                 WHERE in.owner = $source AND in.key = $source_key
                   AND out.owner = $target AND out.key = $target_key LIMIT 1",
            )
            .bind(("source", out.node.node.id.clone()))
            .bind(("source_key", out.key.clone()))
            .bind(("target", inp.node.node.id.clone()))
            .bind(("target_key", inp.key.clone()))
            .await
            .map_err(OrchestrationError::Db)?;
        resp.take::<Option<EdgeConnectionEntity>>(0)
            .map_err(OrchestrationError::Db)?
            .ok_or(OrchestrationError::NotFound)
    }
}

pub struct Disconnect {
    pub actor: Identity,
    pub edge: EdgeConnectionId,
}

impl Processor<Disconnect> for EdgeService {
    type Output = ();
    type Error = OrchestrationError;
    #[tracing::instrument(name = "Service:Disconnect", skip_all, err)]
    async fn process(&self, input: Disconnect) -> Result<Self::Output, Self::Error> {
        input.actor.ensure(Permission::EditWorkspace)?;
        let edge = self
            .db
            .process(FindEdgeById {
                id: input.edge.clone(),
            })
            .await?
            .ok_or(OrchestrationError::NotFound)?;
        let canvas = canvas_of_port(&self.db, &edge.source).await?;
        let topology = self
            .db
            .process(LoadCanvasTopology {
                canvas: canvas.clone(),
            })
            .await?;
        // An edge on the generated side is not the operator's to cut; an edge on
        // a universal node's own port (a channel, a bundle) takes the port and
        // the lanes behind it away with it.
        let mut universal_side = false;
        for port in [&edge.source, &edge.target] {
            let (port, owner) = port_in(&topology, port)?;
            if universal::is_managed_port(port, &owner.node) {
                return Err(OrchestrationError::Conflict(
                    "edge is managed by a universal node; disconnect its channel or bundle instead"
                        .into(),
                ));
            }
            universal_side |= owner.node.spec.takes_bundles() && universal::is_on_demand(port);
        }
        let edits = vec![TopologyEdit::RetireEdge {
            edge: input.edge.clone(),
        }];
        if universal_side {
            let primary = universal::Primary {
                edits,
                batch: ApplyTopologyBatch {
                    canvas: Some(canvas.clone()),
                    delete_edges: vec![input.edge.clone()],
                    ..Default::default()
                },
            };
            let prepared = universal::prepare(&self.db, &self.config, &topology, primary).await?;
            return universal::apply(&self.db, &self.notifier, prepared).await;
        }
        let projected = topology.project(&edits);
        ensure_valid(&projected)?;
        let views = self
            .db
            .process(ListServerConfigViewsByCanvases {
                canvases: projected.canvas_ids(),
            })
            .await?;
        ensure_switch_safe(&projected, &views, &self.config)?;

        self.db
            .process(DeleteEdgeRow {
                id: input.edge,
                canvas: canvas.clone(),
            })
            .await?;
        self.notifier.notify(&topology.root).await;
        Ok(())
    }
}

/// Deletes an edge without validating the canvas it leaves behind. Admin only.
pub struct ForceDisconnect {
    pub actor: Identity,
    pub edge: EdgeConnectionId,
}

impl Processor<ForceDisconnect> for EdgeService {
    type Output = ();
    type Error = OrchestrationError;
    #[tracing::instrument(name = "Service:ForceDisconnect", skip_all, err)]
    async fn process(&self, input: ForceDisconnect) -> Result<Self::Output, Self::Error> {
        input.actor.ensure(Permission::EditWorkspace)?;
        if input.actor.role != AccountRole::Admin {
            return Err(OrchestrationError::PermissionDenied);
        }
        let edge = self
            .db
            .process(FindEdgeById {
                id: input.edge.clone(),
            })
            .await?
            .ok_or(OrchestrationError::NotFound)?;
        let canvas = canvas_of_port(&self.db, &edge.source).await?;
        tracing::info!(edge = %record_key(&edge.id.0), "force-deleting edge");
        self.db
            .process(DeleteEdgeRow {
                id: input.edge,
                canvas: canvas.clone(),
            })
            .await?;
        self.notifier.notify(&canvas).await;
        Ok(())
    }
}
