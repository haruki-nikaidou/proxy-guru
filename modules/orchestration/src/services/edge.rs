//! Edge operations. All edge legality lives in the topology checker.
//!
//! Edges on universal nodes are special in one way: their ports are created on
//! demand. A connect that names a handle *group* of a universal node instead of
//! a port ([`ConnectUniversal`]) creates the port, relates the edge and
//! regenerates the node's lanes in one write; a disconnect on such a port drops
//! the port and the lanes with it. Edges on the generated side (a `lane:` port,
//! any port of a lane node) are never edited by hand.

use crate::config::OrchestrationConfig;
use crate::entities::db::batch::{ApplyTopologyBatch, BatchEdge, PortRef, PortReshape};
use crate::entities::db::connection::{
    ConnectPorts, DeleteEdgeRow, EdgeConnectionEntity, EdgeConnectionId, FindEdgeByEnds,
    FindEdgeById,
};
use crate::entities::db::node::{FindNodeById, NewPort, NodeId, NodeSpec, NodeWithPorts};
use crate::entities::db::port::{FindPortById, PortDirection, PortEntity, PortId, PortKind};
use crate::entities::db::topology::{CanvasTopology, LoadCanvasTopology};
use crate::entities::db::view::ListServerConfigViewsByCanvases;
use crate::events::live::CanvasChangeKind;
use crate::services::OrchestrationError;
use crate::services::converge::ensure_switch_safe;
use crate::services::node::port_rows;
use crate::services::notify::Notifier;
use crate::services::topology::{TopologyEdit, ensure_valid};
use crate::services::universal;
use crate::utils::ids;
use crate::utils::ids::record_key;
use auth::entities::db::account::AccountRole;
use auth::services::identity::Identity;
use auth::utils::rbac::Permission;
use base::db::Db;
use kanau::processor::Processor;

#[derive(Clone)]
pub struct EdgeService {
    pub db: Db,
    pub notifier: Notifier,
    pub config: OrchestrationConfig,
}

/// The canvas an edge endpoint belongs to.
async fn canvas_of_port(
    db: &Db,
    port: &PortId,
) -> Result<crate::entities::db::canvas::CanvasId, OrchestrationError> {
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
        .find_map(|n| {
            n.ports
                .iter()
                .find(|p| record_key(&p.id.0) == key)
                .map(|p| (p, n))
        })
        .ok_or(OrchestrationError::NotFound)
}

/// Edges on the generated side of the graph are not the operator's to draw or
/// cut; a bundle port only ever joins another bundle port (see
/// [`ConnectUniversal`]), never a thin one.
fn ensure_operator_port(
    port: &PortEntity,
    owner: &NodeWithPorts,
) -> Result<(), OrchestrationError> {
    if universal::is_managed_port(port, &owner.node) {
        return Err(OrchestrationError::Conflict(
            "port is managed by a universal node; edit that node instead".into(),
        ));
    }
    if port.kind == PortKind::Bundle {
        return Err(OrchestrationError::Invalid(
            "a bundle port joins a bundle port or a bundle handle only".into(),
        ));
    }
    Ok(())
}

/// The port a bundle leaves from: a distribute node's member or a universal
/// pod's fixed `bundle_out`.
fn ensure_bundle_source(
    port: &PortEntity,
    owner: &NodeWithPorts,
) -> Result<(), OrchestrationError> {
    if universal::is_managed_port(port, &owner.node) {
        return Err(OrchestrationError::Conflict(
            "port is managed by a universal node; edit that node instead".into(),
        ));
    }
    let ok = port.direction == PortDirection::Output
        && match &owner.node.spec {
            NodeSpec::LoadBalanceDistribute(_) => universal::is_member_port(port),
            NodeSpec::UniversalPod(_) => port.key == universal::BUNDLE_OUT,
            _ => false,
        };
    if !ok {
        return Err(OrchestrationError::Invalid(
            "a bundle leaves through a distribute node's member or a universal pod's bundle output"
                .into(),
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
        // A bundle drawn port to port (a universal pod into an aggregate node's
        // member) regenerates lanes like any bundle: it is the universal path.
        let (out_port, _) = port_in(&topology, &input.output_port)?;
        let (in_port, _) = port_in(&topology, &input.input_port)?;
        if out_port.kind == PortKind::Bundle && in_port.kind == PortKind::Bundle {
            return self
                .process(ConnectUniversal {
                    actor: input.actor,
                    output: ConnectEnd::Port(input.output_port),
                    input: ConnectEnd::Port(input.input_port),
                })
                .await;
        }
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
                fence: Some(topology.fence().ok_or(OrchestrationError::NotFound)?),
            })
            .await?;
        self.notifier.notify(&topology.root).await;
        self.notifier
            .canvas_changed(
                &topology.root,
                CanvasChangeKind::EdgeConnected,
                vec![record_key(&edge.id.0)],
            )
            .await;
        Ok(edge)
    }
}

/// The handle groups a connect may name on a universal node: the ports that
/// are created by the connect itself. Bundles leave through ports that exist
/// already (a distribute node's members, a universal pod's `bundle_out`) and
/// enter an aggregate node through its members, so those ends are ports.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UniversalGroup {
    /// A distributor's or universal pod's channel outputs.
    ChannelOut,
    /// A universal pod's or distributor's incoming bundles.
    BundleIn,
}

/// One end of a [`ConnectUniversal`].
#[derive(Debug, Clone)]
pub enum ConnectEnd {
    Port(PortId),
    Handle { node: NodeId, group: UniversalGroup },
}

impl ConnectEnd {
    fn node_of<'a>(
        &self,
        topology: &'a CanvasTopology,
    ) -> Result<&'a NodeWithPorts, OrchestrationError> {
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
        db: &Db,
    ) -> Result<crate::entities::db::canvas::CanvasId, OrchestrationError> {
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

/// A connect that changes the expansion: a channel or a bundle. The port behind
/// a handle is created in the same write as the edge and the lanes the new
/// edge calls for; a bundle drawn between two existing ports (a universal pod
/// into an aggregate node's member) only regenerates the lanes.
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

/// A bundle port carries one bundle.
fn ensure_bundle_free(
    topology: &CanvasTopology,
    port: &PortEntity,
    owner: &NodeWithPorts,
) -> Result<(), OrchestrationError> {
    if has_edge(topology, port) {
        return Err(OrchestrationError::Conflict(match &owner.node.spec {
            NodeSpec::UniversalPod(_) => {
                "a universal pod bundles out to one place; disconnect it first".into()
            }
            _ => "this member is already bundled; disconnect it first".into(),
        }));
    }
    Ok(())
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
            // A bundle collected where it arrives: a universal pod or a
            // distribute node grows a `bundle_in:` port for it.
            (
                ConnectEnd::Port(out_port),
                ConnectEnd::Handle {
                    group: UniversalGroup::BundleIn,
                    ..
                },
            ) => {
                let (out_port, owner) = port_in(&topology, out_port)?;
                ensure_bundle_source(out_port, owner)?;
                if !matches!(
                    target.node.spec,
                    NodeSpec::UniversalPod(_) | NodeSpec::LoadBalanceDistribute(_)
                ) {
                    return Err(OrchestrationError::Invalid(
                        "a universal pod or a distribute node collects bundles; an aggregate node takes them on its members".into(),
                    ));
                }
                ensure_bundle_free(&topology, out_port, owner)?;
                let source_key = record_key(&source.node.id.0);
                let in_key = universal::bundle_in_key(&source_key);
                if target.ports.iter().any(|p| p.key == in_key) {
                    return Err(OrchestrationError::Conflict(
                        "these nodes are already bundled; one member per far node".into(),
                    ));
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
                        ports: as_new_ports(&source.ports),
                        key: out_port.key.clone(),
                    },
                    Resolved {
                        node: target,
                        ports: in_ports,
                        key: in_key,
                    },
                )
            }
            // A bundle into an aggregate node's member: both ports exist.
            (ConnectEnd::Port(out_port), ConnectEnd::Port(in_port)) => {
                let (out_port, owner) = port_in(&topology, out_port)?;
                ensure_bundle_source(out_port, owner)?;
                let (in_port, taker) = port_in(&topology, in_port)?;
                if !matches!(taker.node.spec, NodeSpec::LoadBalanceAggregate(_))
                    || !universal::is_member_port(in_port)
                    || in_port.direction != PortDirection::Input
                {
                    return Err(OrchestrationError::Invalid(
                        "a bundle drawn onto a port lands on an aggregate node's member; universal pods and distribute nodes take bundles on their bundle handle".into(),
                    ));
                }
                if matches!(source.node.spec, NodeSpec::LoadBalanceDistribute(_)) {
                    return Err(OrchestrationError::Invalid(
                        "a distribute node bundles to universal pods; bundle those to the aggregate node".into(),
                    ));
                }
                ensure_bundle_free(&topology, out_port, owner)?;
                if has_edge(&topology, in_port) {
                    return Err(OrchestrationError::Conflict(
                        "this member already takes a bundle; disconnect it first".into(),
                    ));
                }
                let source_key = record_key(&source.node.id.0);
                let already = topology.edges.iter().any(|e| {
                    let from_source = source
                        .ports
                        .iter()
                        .any(|p| record_key(&p.id.0) == record_key(&e.source.0));
                    let into_target = taker
                        .ports
                        .iter()
                        .any(|p| record_key(&p.id.0) == record_key(&e.target.0));
                    from_source && into_target
                });
                if already {
                    return Err(OrchestrationError::Conflict(format!(
                        "{source_key} is already bundled to this node; one member per far node"
                    )));
                }
                (
                    Resolved {
                        node: source,
                        ports: as_new_ports(&source.ports),
                        key: out_port.key.clone(),
                    },
                    Resolved {
                        node: taker,
                        ports: as_new_ports(&taker.ports),
                        key: in_port.key.clone(),
                    },
                )
            }
            _ => {
                return Err(OrchestrationError::Invalid(
                    "a connect joins a channel handle to a pod, or a bundle port to a bundle handle or an aggregate member"
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
        let prepared = universal::prepare(
            &self.db,
            &self.config,
            &topology,
            universal::Primary { edits, batch },
        )
        .await?;
        // No live event yet: the edge's id only exists after the write, and the
        // batch is what created the ports it hangs off.
        universal::apply(&self.db, &self.notifier, prepared, None).await?;

        // The edge, by its ends, now that both ports exist.
        let edge = self
            .db
            .process(FindEdgeByEnds {
                source: out.node.node.id.clone(),
                source_key: out.key.clone(),
                target: inp.node.node.id.clone(),
                target_key: inp.key.clone(),
            })
            .await?
            .ok_or(OrchestrationError::NotFound)?;
        self.notifier
            .canvas_changed(
                &topology.root,
                CanvasChangeKind::EdgeConnected,
                vec![record_key(&edge.id.0)],
            )
            .await;
        Ok(edge)
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
            universal_side |= owner.node.spec.takes_bundles()
                && (universal::is_on_demand(port) || port.kind == PortKind::Bundle);
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
            return universal::apply(
                &self.db,
                &self.notifier,
                prepared,
                Some((
                    CanvasChangeKind::EdgeRetired,
                    vec![record_key(&input.edge.0)],
                )),
            )
            .await;
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

        let edge = record_key(&input.edge.0);
        self.db
            .process(DeleteEdgeRow {
                id: input.edge,
                canvas: canvas.clone(),
                fence: Some(topology.fence().ok_or(OrchestrationError::NotFound)?),
            })
            .await?;
        self.notifier.notify(&topology.root).await;
        self.notifier
            .canvas_changed(&topology.root, CanvasChangeKind::EdgeRetired, vec![edge])
            .await;
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
        let id = record_key(&input.edge.0);
        self.db
            .process(DeleteEdgeRow {
                id: input.edge,
                canvas: canvas.clone(),
                fence: None,
            })
            .await?;
        self.notifier.notify(&canvas).await;
        self.notifier
            .canvas_changed(&canvas, CanvasChangeKind::EdgeDeleted, vec![id])
            .await;
        Ok(())
    }
}
