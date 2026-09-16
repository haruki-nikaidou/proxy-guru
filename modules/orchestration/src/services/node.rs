//! Node operations. Specs and ports are edited in place; ports keep their identity
//! across a spec change, so the edges attached to them survive it.
//!
//! Import nodes are the one exception: their ports are *derived* from the export
//! nodes of the canvas they import (one port per export, keyed by the export's
//! record key), so every export edit — create, retire, re-kind, move on the y
//! axis — carries an [`ImportSync`] that reshapes the importer's ports in the same
//! transaction.

use crate::config::OrchestrationConfig;
use crate::entities::surreal::canvas::{CanvasFence, CanvasId, CanvasUiPosition, FindCanvasById};
use crate::entities::surreal::certificate::ListCertificatesBySnis;
use crate::entities::surreal::dns::FindDnsProviderById;
use crate::entities::surreal::node::{
    CanvasExportAs, CreateNodeRow, DeleteNodeRow, FindNodeById, FindNodeWithPorts, ImportSync,
    LoadBalanceMember, MEMBER_PREFIX, NewPort, NodeEntity, NodeId, NodeSpec, NodeWithPorts,
    PENDING_EXPORT_KEY, UpdateNodeMetaRow, UpdateNodeSpecRow,
};
use crate::entities::surreal::port::{PortDirection, PortEntity, PortKind};
use crate::entities::surreal::topology::{CanvasTopology, LoadCanvasTopology};
use crate::entities::surreal::view::ListServerConfigViewsByCanvases;
use crate::events::live::CanvasChangeKind;
use crate::services::OrchestrationError;
use crate::services::converge::ensure_switch_safe;
use crate::services::notify::Notifier;
use crate::services::topology::{TopologyEdit, ensure_valid};
use crate::services::universal::{self, PENDING_PORT_PREFIX};
use crate::utils::ids;
use crate::utils::ids::record_key;
use auth::entities::surreal::account::AccountRole;
use auth::services::identity::Identity;
use auth::utils::rbac::Permission;
use base::db::Db;
use kanau::processor::Processor;

#[derive(Clone)]
pub struct NodeService {
    pub db: Db,
    pub notifier: Notifier,
    pub config: OrchestrationConfig,
}

/// The direction of an export node's single port *inside* its canvas. An
/// `InputIntoCanvas` export receives traffic from the parent and therefore
/// *emits* it inside (output port); the importing node shows the mirrored
/// direction.
pub fn export_port_direction(direction: CanvasExportAs) -> PortDirection {
    match direction {
        CanvasExportAs::InputIntoCanvas => PortDirection::Output,
        CanvasExportAs::OutputOutOfCanvas => PortDirection::Input,
    }
}

fn mirror(direction: PortDirection) -> PortDirection {
    match direction {
        PortDirection::Input => PortDirection::Output,
        PortDirection::Output => PortDirection::Input,
    }
}

/// The derived ports of a node importing a canvas with these export nodes: one
/// port per export, keyed by the export's record key, kind copied, direction
/// mirrored, ordered by `(position.y, record key)`. Non-export nodes in
/// `exports` are ignored.
pub fn import_port_layout(exports: &[&NodeEntity]) -> Vec<NewPort> {
    let mut exports: Vec<(&NodeEntity, PortKind, CanvasExportAs)> = exports
        .iter()
        .filter_map(|node| match &node.spec {
            NodeSpec::CanvasExport(cfg) => Some((*node, cfg.kind, cfg.direction)),
            _ => None,
        })
        .collect();
    exports.sort_by_cached_key(|(node, _, _)| (node.position.y, record_key(&node.id.0)));
    exports
        .into_iter()
        .enumerate()
        .map(|(rank, (node, kind, direction))| NewPort {
            kind,
            direction: mirror(export_port_direction(direction)),
            key: record_key(&node.id.0),
            position: rank as i64,
        })
        .collect()
}

/// The import node targeting `canvas` in `topology`, if any.
fn importer_of<'a>(topology: &'a CanvasTopology, canvas: &CanvasId) -> Option<&'a NodeWithPorts> {
    let key = record_key(&canvas.0);
    topology.nodes.iter().find(
        |n| matches!(&n.node.spec, NodeSpec::CanvasImport(cfg) if record_key(&cfg.canvas.0) == key),
    )
}

/// The sync the node importing `canvas` needs so that its ports mirror
/// `projected_exports` (the export nodes `canvas` will have after the edit).
/// `None` when `canvas` is not imported.
pub fn import_sync_for(
    topology: &CanvasTopology,
    canvas: &CanvasId,
    projected_exports: &[&NodeEntity],
) -> Option<ImportSync> {
    let importer = importer_of(topology, canvas)?;
    Some(ImportSync {
        node: importer.node.id.clone(),
        ports: import_port_layout(projected_exports),
    })
}

/// The projection of an [`ImportSync`]: the importer's ports after the sync,
/// keeping the row id of every key that survives so its edges stay attached.
fn reshape_edit(topology: &CanvasTopology, sync: &ImportSync) -> Option<TopologyEdit> {
    let key = record_key(&sync.node.0);
    let importer = topology
        .nodes
        .iter()
        .find(|n| record_key(&n.node.id.0) == key)?;
    Some(TopologyEdit::ReshapePorts {
        node: sync.node.clone(),
        ports: port_rows(&importer.node.id, &importer.ports, &sync.ports),
    })
}

/// The export nodes of `canvas` in `topology`.
fn exports_of<'a>(topology: &'a CanvasTopology, canvas: &CanvasId) -> Vec<&'a NodeEntity> {
    let key = record_key(&canvas.0);
    topology
        .nodes
        .iter()
        .map(|n| &n.node)
        .filter(|n| record_key(&n.canvas.0) == key && matches!(n.spec, NodeSpec::CanvasExport(_)))
        .collect()
}

/// The port layout of a spec. This is the single source of truth for port keys and
/// positions; both creation and replacement generate ports from here. Import
/// nodes have no layout of their own: see [`import_port_layout`].
pub fn port_layout(spec: &NodeSpec, item_count: u32) -> Result<Vec<NewPort>, OrchestrationError> {
    let port = |key: &str, kind: PortKind, direction: PortDirection, position: i64| NewPort {
        kind,
        direction,
        key: key.to_string(),
        position,
    };
    Ok(match spec {
        NodeSpec::Pod(_) => vec![
            port("listen", PortKind::DeriveListen, PortDirection::Output, 0),
            port(
                "destination",
                PortKind::DeriveDestination,
                PortDirection::Input,
                1,
            ),
        ],
        NodeSpec::Entry(_) => vec![port(
            "listen",
            PortKind::DeriveListen,
            PortDirection::Input,
            0,
        )],
        NodeSpec::Relay(_) => vec![
            port("listen", PortKind::DeriveListen, PortDirection::Input, 0),
            port(
                "destination",
                PortKind::DeriveDestination,
                PortDirection::Output,
                1,
            ),
        ],
        NodeSpec::Exit(_) => vec![port(
            "destination",
            PortKind::DeriveDestination,
            PortDirection::Output,
            0,
        )],
        // The operator's members are bundle ports; a lane (no members declared)
        // is laid out thin from its count.
        NodeSpec::LoadBalanceDistribute(cfg) if !cfg.members.is_empty() => {
            ensure_no_count(item_count)?;
            member_ports(&cfg.members, PortDirection::Output)
        }
        NodeSpec::LoadBalanceAggregate(cfg) if !cfg.members.is_empty() => {
            ensure_no_count(item_count)?;
            member_ports(&cfg.members, PortDirection::Input)
        }
        NodeSpec::LoadBalanceDistribute(_) => {
            let count = load_balance_count(item_count)?;
            if count == 0 {
                return Ok(Vec::new());
            }
            let mut ports: Vec<NewPort> = (0..count)
                .map(|i| {
                    port(
                        &format!("{MEMBER_PREFIX}{i}"),
                        PortKind::DeriveDestination,
                        PortDirection::Input,
                        i,
                    )
                })
                .collect();
            ports.push(port(
                "destination",
                PortKind::DeriveDestination,
                PortDirection::Output,
                count,
            ));
            ports
        }
        NodeSpec::LoadBalanceAggregate(_) => {
            let count = load_balance_count(item_count)?;
            if count == 0 {
                return Ok(Vec::new());
            }
            let mut ports = vec![port(
                "source",
                PortKind::DeriveDestination,
                PortDirection::Input,
                0,
            )];
            for i in 0..count {
                ports.push(port(
                    &format!("copy_{i}"),
                    PortKind::DeriveDestination,
                    PortDirection::Output,
                    i.saturating_add(1),
                ));
            }
            ports
        }
        NodeSpec::CanvasExport(cfg) => vec![port(
            "export",
            cfg.kind,
            export_port_direction(cfg.direction),
            0,
        )],
        NodeSpec::CanvasImport(_) => {
            return Err(OrchestrationError::Invalid(
                "import ports are derived from the target canvas".into(),
            ));
        }
        // A universal pod starts with its fixed port only; the rest are created
        // on demand by connects (see `services::universal`).
        spec @ NodeSpec::UniversalPod(_) => universal::initial_ports(spec),
    })
}

/// The largest member count a load-balance node may ask for.
///
/// Every member becomes a port row and an entry in the derived worker config, and
/// the request carries the count as an unbounded `u32`, so the bound is what keeps
/// a single request from asking the master to allocate billions of rows. 256 is far
/// above any realistic fan-out (a load balancer spreading over 256 exits) while
/// staying cheap to build and validate.
pub const MAX_LOAD_BALANCE_MEMBERS: u32 = 256;

/// The longest name a member may have.
pub const MAX_MEMBER_NAME: usize = 64;

/// One bundle port per declared member, keyed by slot and placed in the order
/// given (see [`members_ok`] for what a list must satisfy).
fn member_ports(members: &[LoadBalanceMember], direction: PortDirection) -> Vec<NewPort> {
    members
        .iter()
        .enumerate()
        .map(|(i, member)| NewPort {
            kind: PortKind::Bundle,
            direction,
            key: member.port_key(),
            position: i64::try_from(i).unwrap_or(i64::MAX),
        })
        .collect()
}

/// A node whose members are declared has no counted ports next to them.
fn ensure_no_count(item_count: u32) -> Result<(), OrchestrationError> {
    if item_count != 0 {
        return Err(OrchestrationError::Invalid(
            "a load balance node with members takes no item count".into(),
        ));
    }
    Ok(())
}

/// The operator's member list must be usable as a rule: 1 to 256 members, each
/// with a name (trimmed, at most 64 characters) and a slot unique in the list.
pub fn members_ok(members: &[LoadBalanceMember]) -> Result<(), OrchestrationError> {
    if members.is_empty() {
        return Err(OrchestrationError::Invalid(
            "a load balance node needs at least one member".into(),
        ));
    }
    if members.len() > MAX_LOAD_BALANCE_MEMBERS as usize {
        return Err(OrchestrationError::Invalid(format!(
            "load balance nodes take at most {MAX_LOAD_BALANCE_MEMBERS} members, got {}",
            members.len()
        )));
    }
    let mut slots = std::collections::HashSet::new();
    let mut names = std::collections::HashSet::new();
    for member in members {
        let name = member.name.trim();
        if name.is_empty() {
            return Err(OrchestrationError::Invalid(
                "every member needs a name".into(),
            ));
        }
        if name.chars().count() > MAX_MEMBER_NAME {
            return Err(OrchestrationError::Invalid(format!(
                "member name '{name}' is longer than {MAX_MEMBER_NAME} characters"
            )));
        }
        if !names.insert(name) {
            return Err(OrchestrationError::Invalid(format!(
                "member name '{name}' is used twice"
            )));
        }
        if !slots.insert(member.slot) {
            return Err(OrchestrationError::Invalid(format!(
                "member slot {} is used twice",
                member.slot
            )));
        }
    }
    Ok(())
}

/// An operator's load-balance node declares its members in the spec: the list
/// must be valid, and the counted layout is for lanes only.
fn ensure_operator_members(spec: &NodeSpec, item_count: u32) -> Result<(), OrchestrationError> {
    match spec {
        NodeSpec::LoadBalanceDistribute(cfg) => {
            members_ok(&cfg.members)?;
            ensure_no_count(item_count)
        }
        NodeSpec::LoadBalanceAggregate(cfg) => {
            members_ok(&cfg.members)?;
            ensure_no_count(item_count)
        }
        _ => Ok(()),
    }
}

/// Zero members means no hand-drawn ports at all: a node used through bundles
/// and channels only (see `services::universal`).
fn load_balance_count(item_count: u32) -> Result<i64, OrchestrationError> {
    if item_count == 1 {
        return Err(OrchestrationError::Invalid(
            "load balance nodes need at least 2 members, or none".into(),
        ));
    }
    if item_count > MAX_LOAD_BALANCE_MEMBERS {
        return Err(OrchestrationError::Invalid(format!(
            "load balance nodes take at most {MAX_LOAD_BALANCE_MEMBERS} members, got {item_count}"
        )));
    }
    Ok(i64::from(item_count))
}

/// Port rows for `ports` on `owner`: a key that already exists in `existing`
/// keeps its row id (and with it every edge attached), a new key gets a
/// placeholder named after the owner and the key, so several nodes' pending
/// ports never collide within one projection. Mirrors exactly what
/// `fn::orchestration_reshape_ports` writes.
pub(crate) fn port_rows(
    owner: &NodeId,
    existing: &[PortEntity],
    ports: &[NewPort],
) -> Vec<PortEntity> {
    let owner_key = record_key(&owner.0);
    ports
        .iter()
        .map(|p| PortEntity {
            id: existing
                .iter()
                .find(|e| e.key == p.key)
                .map(|e| e.id.clone())
                .unwrap_or_else(|| {
                    ids::port_id(&format!("{PENDING_PORT_PREFIX}{owner_key}-{}", p.key))
                }),
            owner: owner.clone(),
            kind: p.kind,
            direction: p.direction,
            key: p.key.clone(),
            position: p.position,
        })
        .collect()
}

/// The rows a `CreateNodeRow` will write, with placeholder ids, for validation.
/// The node's placeholder key is [`PENDING_EXPORT_KEY`], which is also what an
/// import sync uses for a pending export node.
fn pending_node(
    canvas: &CanvasId,
    name: &str,
    comment: &str,
    spec: &NodeSpec,
    position: CanvasUiPosition,
    ports: &[NewPort],
) -> (NodeEntity, Vec<PortEntity>) {
    let node_id = ids::node_id(PENDING_EXPORT_KEY);
    let node = NodeEntity {
        id: node_id.clone(),
        canvas: canvas.clone(),
        name: name.to_string(),
        comment: comment.to_string(),
        spec: spec.clone(),
        position,
        lane: None,
    };
    let ports = port_rows(&node_id, &[], ports);
    (node, ports)
}

impl NodeService {
    async fn views_for(
        &self,
        topology: &CanvasTopology,
    ) -> Result<Vec<crate::entities::surreal::view::ServerConfigViewEntity>, OrchestrationError>
    {
        Ok(self
            .db
            .process(ListServerConfigViewsByCanvases {
                canvases: topology.canvas_ids(),
            })
            .await?)
    }
}

/// An Entry's `TlsConfig` must name a hostname the CA can issue for and a DNS
/// provider that exists: the certificate row the cron creates for it references
/// both, and a dangling provider could never answer the challenge. The SNI is
/// stored in its canonical (lower-case) form, which is the certificate row key.
async fn ensure_tls_valid(db: &Db, spec: &mut NodeSpec) -> Result<(), OrchestrationError> {
    let NodeSpec::Entry(entry) = spec else {
        return Ok(());
    };
    let Some(tls) = &mut entry.tls else {
        return Ok(());
    };
    tls.sni = crate::services::acme::validate_sni(&tls.sni)
        .map_err(|e| OrchestrationError::Invalid(format!("tls: {e}")))?;
    db.process(FindDnsProviderById {
        id: tls.dns_provider.clone(),
    })
    .await?
    .ok_or_else(|| OrchestrationError::Invalid("dns provider not found".into()))?;
    // Entries sharing a certificate must agree on how it is issued: the row is
    // keyed by (sni, directory) alone, so a later Entry naming another provider
    // or zone would otherwise be silently ignored. An empty directory is the
    // default one, which is why it matches any row for the sni.
    let rows = db
        .process(ListCertificatesBySnis {
            snis: vec![tls.sni.clone()],
        })
        .await?;
    let conflicting = rows.iter().find(|row| {
        (tls.acme_directory.is_empty() || row.acme_directory == tls.acme_directory)
            && (record_key(&row.dns_provider.0) != record_key(&tls.dns_provider.0)
                || row.domain_id != tls.domain_id)
    });
    if let Some(row) = conflicting {
        return Err(OrchestrationError::Invalid(format!(
            "tls: certificate for {} is already issued through dns provider {} / domain {}; \
             every Entry sharing an sni must use the same provider and domain",
            tls.sni,
            record_key(&row.dns_provider.0),
            row.domain_id
        )));
    }
    Ok(())
}

pub struct CreateNode {
    pub actor: Identity,
    pub canvas: CanvasId,
    pub name: String,
    pub comment: String,
    pub spec: NodeSpec,
    pub position: CanvasUiPosition,
    pub item_count: u32,
}

impl Processor<CreateNode> for NodeService {
    type Output = NodeWithPorts;
    type Error = OrchestrationError;
    /// An import node's ports come from the target canvas's exports; the target
    /// tree is merged into the projection when it is not part of this tree yet,
    /// so the nesting rules (self, ancestor, duplicate) fall out of the checker.
    ///
    /// An export node created in an imported canvas syncs the importer's ports in
    /// the same transaction: its mirrored port is keyed by a record key that only
    /// exists inside the write, so the sync carries [`PENDING_EXPORT_KEY`] and
    /// `create_node_row.surql` substitutes the real key (single round trip).
    #[tracing::instrument(name = "Service:CreateNode", skip_all, err)]
    async fn process(&self, input: CreateNode) -> Result<Self::Output, Self::Error> {
        input.actor.ensure(Permission::EditWorkspace)?;
        self.db
            .process(FindCanvasById {
                id: input.canvas.clone(),
            })
            .await?
            .ok_or(OrchestrationError::NotFound)?;
        if matches!(input.spec, NodeSpec::UniversalPod(_)) {
            return Err(OrchestrationError::Invalid(
                "a universal pod is created with its server".into(),
            ));
        }
        let mut input = input;
        ensure_operator_members(&input.spec, input.item_count)?;
        ensure_tls_valid(&self.db, &mut input.spec).await?;
        let mut topology = self
            .db
            .process(LoadCanvasTopology {
                canvas: input.canvas.clone(),
            })
            .await?;

        // For an import, the target is a separate tree read here and validated
        // together with the importer; its own fence is claimed in the write.
        let mut target_fence: Option<CanvasFence> = None;
        let ports = match &input.spec {
            NodeSpec::CanvasImport(cfg) => {
                let target_key = record_key(&cfg.canvas.0);
                let in_tree = topology
                    .canvases
                    .iter()
                    .any(|c| record_key(&c.id.0) == target_key);
                if !in_tree {
                    let target = self
                        .db
                        .process(LoadCanvasTopology {
                            canvas: cfg.canvas.clone(),
                        })
                        .await?;
                    if target.canvases.is_empty() {
                        return Err(OrchestrationError::NotFound);
                    }
                    target_fence = target.fence();
                    // A target that is not its own root brings its importer along,
                    // which the checker reports as a duplicate import.
                    topology.canvases.extend(target.canvases);
                    topology.servers.extend(target.servers);
                    topology.nodes.extend(target.nodes);
                    topology.edges.extend(target.edges);
                }
                import_port_layout(&exports_of(&topology, &cfg.canvas))
            }
            spec => port_layout(spec, input.item_count)?,
        };

        let (node, port_rows) = pending_node(
            &input.canvas,
            &input.name,
            &input.comment,
            &input.spec,
            input.position,
            &ports,
        );
        let import_sync = match &input.spec {
            NodeSpec::CanvasExport(_) => {
                let mut exports = exports_of(&topology, &input.canvas);
                exports.push(&node);
                import_sync_for(&topology, &input.canvas, &exports)
            }
            _ => None,
        };
        let mut edits = vec![TopologyEdit::AddNode {
            node: Box::new(node.clone()),
            ports: port_rows,
        }];
        if let Some(sync) = &import_sync
            && let Some(edit) = reshape_edit(&topology, sync)
        {
            edits.push(edit);
        }
        let projected = topology.project(&edits);
        ensure_valid(&projected)?;
        let views = self.views_for(&projected).await?;
        ensure_switch_safe(&projected, &views, &self.config)?;

        // Captured before the write moves the spec: an import changes which tree
        // the *target* belongs to, and a watcher already open on that target has
        // a match set containing only itself. Without an event addressed to it,
        // it would never learn it has acquired ancestors.
        let imported = match &input.spec {
            NodeSpec::CanvasImport(cfg) => Some(cfg.canvas.clone()),
            _ => None,
        };
        let created = self
            .db
            .process(CreateNodeRow {
                canvas: input.canvas.clone(),
                name: input.name,
                comment: input.comment,
                spec: input.spec,
                position: input.position,
                ports,
                import_sync,
                fence: Some(topology.fence().ok_or(OrchestrationError::NotFound)?),
                target_fence,
            })
            .await?;
        self.notifier.notify(&topology.root).await;
        let ids = vec![record_key(&created.node.id.0)];
        self.notifier
            .canvas_changed(&topology.root, CanvasChangeKind::NodeCreated, ids.clone())
            .await;
        if let Some(target) = imported {
            self.notifier
                .canvas_changed(&target, CanvasChangeKind::NodeCreated, ids)
                .await;
        }
        Ok(created)
    }
}

pub struct ReplaceNodeSpec {
    pub actor: Identity,
    pub node: NodeId,
    pub spec: NodeSpec,
    pub item_count: u32,
}

impl Processor<ReplaceNodeSpec> for NodeService {
    type Output = NodeWithPorts;
    type Error = OrchestrationError;
    /// An import node has nothing to edit: its target is immutable and its ports
    /// are derived. Re-kinding an export node updates the importer's mirrored port
    /// in place (same key, so an edge on it in the parent survives and the
    /// projection then reports the mismatch, exactly like any kept port).
    #[tracing::instrument(name = "Service:ReplaceNodeSpec", skip_all, err)]
    async fn process(&self, input: ReplaceNodeSpec) -> Result<Self::Output, Self::Error> {
        input.actor.ensure(Permission::EditWorkspace)?;
        let old = self
            .db
            .process(FindNodeWithPorts {
                id: input.node.clone(),
            })
            .await?
            .ok_or(OrchestrationError::NotFound)?;
        if matches!(old.node.spec, NodeSpec::CanvasImport(_))
            || matches!(input.spec, NodeSpec::CanvasImport(_))
        {
            return Err(OrchestrationError::Invalid(
                "an import node has nothing to edit; retire it and import again".into(),
            ));
        }
        if std::mem::discriminant(&old.node.spec) != std::mem::discriminant(&input.spec) {
            return Err(OrchestrationError::Invalid(
                "spec kind cannot change; create a new node".into(),
            ));
        }
        if old.node.spec.is_universal() {
            return Err(OrchestrationError::Invalid(
                "a universal pod has nothing to edit".into(),
            ));
        }
        ensure_lane_edit_allowed(&old, &input.spec)?;
        let mut input = input;
        ensure_operator_members(&input.spec, input.item_count)?;
        ensure_tls_valid(&self.db, &mut input.spec).await?;
        // A load-balance node's on-demand ports (channels, bundles in) are the
        // control plane's: the member layout is replaced, they are kept. A
        // member keeps its port (and the bundle on it) as long as its slot
        // stays in the list, whatever its name or place.
        let mut ports = port_layout(&input.spec, input.item_count)?;
        if input.spec.takes_bundles() {
            ports.extend(
                old.ports
                    .iter()
                    .filter(|p| universal::is_on_demand(p))
                    .map(|p| NewPort {
                        kind: p.kind,
                        direction: p.direction,
                        key: p.key.clone(),
                        position: p.position,
                    }),
            );
        }

        let canvas = old.node.canvas.clone();
        let topology = self
            .db
            .process(LoadCanvasTopology {
                canvas: canvas.clone(),
            })
            .await?;

        // A port whose key survives keeps its row, and with it every edge attached
        // to it. The projection mirrors that: kept keys reuse the existing port id,
        // so the edges below re-attach to exactly the rows the write will keep.
        let port_rows = port_rows(&old.node.id, &old.ports, &ports);
        let kept: Vec<String> = port_rows.iter().map(|p| record_key(&p.id.0)).collect();
        let mut carried = Vec::new();
        for edge in &topology.edges {
            for port in [&edge.source, &edge.target] {
                let Some(old_port) = old
                    .ports
                    .iter()
                    .find(|p| record_key(&p.id.0) == record_key(&port.0))
                else {
                    continue;
                };
                if !kept.contains(&record_key(&old_port.id.0)) {
                    return Err(OrchestrationError::Conflict(
                        "disconnect edges on removed ports first".into(),
                    ));
                }
                carried.push(edge.clone());
            }
        }

        let node = NodeEntity {
            id: old.node.id.clone(),
            canvas: canvas.clone(),
            name: old.node.name.clone(),
            comment: old.node.comment.clone(),
            spec: input.spec.clone(),
            position: old.node.position,
            lane: old.node.lane.clone(),
        };
        let import_sync = match &input.spec {
            NodeSpec::CanvasExport(_) => {
                let own = record_key(&node.id.0);
                let mut exports: Vec<&NodeEntity> = exports_of(&topology, &canvas)
                    .into_iter()
                    .filter(|n| record_key(&n.id.0) != own)
                    .collect();
                exports.push(&node);
                import_sync_for(&topology, &canvas, &exports)
            }
            _ => None,
        };
        let mut edits = vec![
            TopologyEdit::RetireNode {
                node: input.node.clone(),
            },
            TopologyEdit::AddNode {
                node: Box::new(node.clone()),
                ports: port_rows,
            },
        ];
        for edge in carried {
            edits.push(TopologyEdit::AddEdge { edge });
        }
        if let Some(sync) = &import_sync
            && let Some(edit) = reshape_edit(&topology, sync)
        {
            edits.push(edit);
        }
        // A load-balance node's edit may change what its channels expand to (a
        // new relay protocol re-rolls every landing port): the lanes are
        // regenerated in the same write.
        if input.spec.takes_bundles() {
            let primary = universal::Primary {
                edits,
                batch: crate::entities::surreal::batch::ApplyTopologyBatch {
                    canvas: Some(canvas.clone()),
                    set_specs: vec![crate::entities::surreal::batch::SpecUpdate {
                        node: input.node.clone(),
                        spec: input.spec.clone(),
                    }],
                    reshape: vec![crate::entities::surreal::batch::PortReshape {
                        node: input.node.clone(),
                        ports: ports.clone(),
                    }],
                    ..Default::default()
                },
            };
            let prepared = universal::prepare(&self.db, &self.config, &topology, primary).await?;
            universal::apply(
                &self.db,
                &self.notifier,
                prepared,
                Some((
                    CanvasChangeKind::NodeReplaced,
                    vec![record_key(&input.node.0)],
                )),
            )
            .await?;
            return self
                .db
                .process(FindNodeWithPorts { id: input.node })
                .await?
                .ok_or(OrchestrationError::NotFound);
        }
        let projected = topology.project(&edits);
        ensure_valid(&projected)?;
        let views = self.views_for(&projected).await?;
        ensure_switch_safe(&projected, &views, &self.config)?;

        let updated = self
            .db
            .process(UpdateNodeSpecRow {
                id: input.node.clone(),
                canvas: canvas.clone(),
                spec: input.spec,
                ports,
                import_sync,
                fence: Some(topology.fence().ok_or(OrchestrationError::NotFound)?),
            })
            .await?;
        self.notifier.notify(&topology.root).await;
        self.notifier
            .canvas_changed(
                &topology.root,
                CanvasChangeKind::NodeReplaced,
                vec![record_key(&input.node.0)],
            )
            .await;
        Ok(updated)
    }
}

/// A lane node is generated: only a landing pod may be edited, and only its
/// port and addresses — its server is the lane's identity.
fn ensure_lane_edit_allowed(
    old: &NodeWithPorts,
    spec: &NodeSpec,
) -> Result<(), OrchestrationError> {
    let Some(lane) = &old.node.lane else {
        return Ok(());
    };
    match (&old.node.spec, spec) {
        (NodeSpec::Pod(current), NodeSpec::Pod(next))
            if record_key(&current.server.0) == record_key(&next.server.0) =>
        {
            Ok(())
        }
        (NodeSpec::Pod(_), NodeSpec::Pod(_)) => Err(OrchestrationError::Conflict(
            "a landing pod stays on its server; only its port and addresses can change".into(),
        )),
        _ => Err(OrchestrationError::Conflict(format!(
            "node is generated by a universal node ({}); edit that node instead",
            lane.role.name()
        ))),
    }
}

pub struct UpdateNodeMeta {
    pub actor: Identity,
    pub node: NodeId,
    pub name: String,
    pub comment: String,
    /// `None` leaves the node where it is.
    pub position: Option<CanvasUiPosition>,
}

impl Processor<UpdateNodeMeta> for NodeService {
    type Output = NodeWithPorts;
    type Error = OrchestrationError;
    #[tracing::instrument(name = "Service:UpdateNodeMeta", skip_all, err)]
    async fn process(&self, input: UpdateNodeMeta) -> Result<Self::Output, Self::Error> {
        input.actor.ensure(Permission::EditWorkspace)?;
        let old = self
            .db
            .process(FindNodeById {
                id: input.node.clone(),
            })
            .await?
            .ok_or(OrchestrationError::NotFound)?;
        let canvas = old.canvas.clone();
        // An export node moving on the y axis renumbers the mirrored ports on the
        // node importing its canvas; nothing else about a move concerns the tree.
        // That reshape is computed from a snapshot, so it must be fenced.
        let mut fence = None;
        let import_sync = match (&old.spec, input.position) {
            (NodeSpec::CanvasExport(_), Some(position)) if position.y != old.position.y => {
                let topology = self
                    .db
                    .process(LoadCanvasTopology {
                        canvas: canvas.clone(),
                    })
                    .await?;
                let moved = NodeEntity {
                    position,
                    ..old.clone()
                };
                let own = record_key(&old.id.0);
                let mut exports: Vec<&NodeEntity> = exports_of(&topology, &canvas)
                    .into_iter()
                    .filter(|n| record_key(&n.id.0) != own)
                    .collect();
                exports.push(&moved);
                fence = topology.fence();
                import_sync_for(&topology, &canvas, &exports)
            }
            _ => None,
        };
        // Whether this edit renames the node is decided by the query, in the same
        // transaction as the write and the generation bump, so a concurrent
        // metadata write cannot overwrite a rename without scheduling a
        // derivation. The read above only resolves the canvas and NotFound.
        let updated = self
            .db
            .process(UpdateNodeMetaRow {
                id: input.node.clone(),
                canvas: canvas.clone(),
                name: input.name,
                comment: input.comment,
                position: input.position,
                import_sync,
                fence,
            })
            .await?;
        // Only a rename changes a config, so only a rename schedules a
        // derivation — but the dashboard renders the position and the comment
        // too, so the live event goes out either way.
        if updated.renamed {
            self.notifier.notify(&canvas).await;
        }
        self.notifier
            .canvas_changed(
                &canvas,
                CanvasChangeKind::NodeMetaUpdated,
                vec![record_key(&input.node.0)],
            )
            .await;
        Ok(NodeWithPorts {
            node: updated.node,
            ports: updated.ports,
        })
    }
}

/// What retiring a node does to the tree beyond deleting its rows.
struct Retirement {
    import_sync: Option<ImportSync>,
    frees_canvas: Option<CanvasId>,
    edits: Vec<TopologyEdit>,
}

/// Retiring an import node frees its target (a root again, with its own
/// derivation to run); retiring an export node drops the importer's mirrored
/// port and, silently, the parent edge on it.
fn retirement(topology: &CanvasTopology, node: &NodeEntity) -> Retirement {
    let mut edits = vec![TopologyEdit::RetireNode {
        node: node.id.clone(),
    }];
    match &node.spec {
        NodeSpec::CanvasImport(cfg) => Retirement {
            import_sync: None,
            frees_canvas: Some(cfg.canvas.clone()),
            edits,
        },
        NodeSpec::CanvasExport(_) => {
            let own = record_key(&node.id.0);
            let exports: Vec<&NodeEntity> = exports_of(topology, &node.canvas)
                .into_iter()
                .filter(|n| record_key(&n.id.0) != own)
                .collect();
            let import_sync = import_sync_for(topology, &node.canvas, &exports);
            if let Some(sync) = &import_sync
                && let Some(edit) = reshape_edit(topology, sync)
            {
                edits.push(edit);
            }
            Retirement {
                import_sync,
                frees_canvas: None,
                edits,
            }
        }
        _ => Retirement {
            import_sync: None,
            frees_canvas: None,
            edits,
        },
    }
}

impl NodeService {
    /// `kind` separates a checked retire from an Admin's force delete: the two
    /// callers write identical rows but mean different things to a dashboard.
    async fn delete_node(
        &self,
        topology: &CanvasTopology,
        node: NodeEntity,
        retirement: Retirement,
        kind: CanvasChangeKind,
        fence: Option<CanvasFence>,
    ) -> Result<(), OrchestrationError> {
        let frees = retirement.frees_canvas.clone();
        let ids = vec![record_key(&node.id.0)];
        self.db
            .process(DeleteNodeRow {
                id: node.id,
                canvas: node.canvas,
                import_sync: retirement.import_sync,
                frees_canvas: retirement.frees_canvas,
                fence,
            })
            .await?;
        self.notifier.notify(&topology.root).await;
        self.notifier
            .canvas_changed(&topology.root, kind, ids.clone())
            .await;
        // Retiring an import frees the canvas it embedded: that canvas is now a
        // root of its own, and anything watching it has to reload too.
        if let Some(freed) = frees {
            self.notifier.notify(&freed).await;
            self.notifier.canvas_changed(&freed, kind, ids).await;
        }
        Ok(())
    }
}

/// Deletes a node after checking the tree still validates without it.
pub struct RetireNode {
    pub actor: Identity,
    pub node: NodeId,
}

impl Processor<RetireNode> for NodeService {
    type Output = ();
    type Error = OrchestrationError;
    #[tracing::instrument(name = "Service:RetireNode", skip_all, err)]
    async fn process(&self, input: RetireNode) -> Result<Self::Output, Self::Error> {
        input.actor.ensure(Permission::EditWorkspace)?;
        let node = self
            .db
            .process(FindNodeById {
                id: input.node.clone(),
            })
            .await?
            .ok_or(OrchestrationError::NotFound)?;
        let topology = self
            .db
            .process(LoadCanvasTopology {
                canvas: node.canvas.clone(),
            })
            .await?;
        ensure_retire_allowed(&topology, &node)?;
        let retirement = retirement(&topology, &node);
        // Retiring an import splits the tree in two; `PodIpForeign` then rejects
        // a retire that would strand a pod on a server of the other side.
        //
        // Retiring anything but a boundary node may change what the universal
        // nodes expand to (an entry pod that was a channel takes every lane of
        // that channel with it), so the expansion is regenerated in the same
        // write when it changes.
        let boundary = matches!(
            node.spec,
            NodeSpec::CanvasImport(_) | NodeSpec::CanvasExport(_)
        );
        if !boundary {
            let primary = universal::Primary {
                edits: retirement.edits.clone(),
                batch: crate::entities::surreal::batch::ApplyTopologyBatch {
                    canvas: Some(node.canvas.clone()),
                    delete_nodes: vec![node.id.clone()],
                    ..Default::default()
                },
            };
            let prepared = universal::prepare(&self.db, &self.config, &topology, primary).await?;
            if prepared.reconciled {
                return universal::apply(
                    &self.db,
                    &self.notifier,
                    prepared,
                    Some((
                        CanvasChangeKind::NodeRetired,
                        vec![record_key(&input.node.0)],
                    )),
                )
                .await;
            }
        } else {
            let projected = topology.project(&retirement.edits);
            ensure_valid(&projected)?;
            let views = self.views_for(&projected).await?;
            ensure_switch_safe(&projected, &views, &self.config)?;
        }

        let fence = Some(topology.fence().ok_or(OrchestrationError::NotFound)?);
        self.delete_node(
            &topology,
            node,
            retirement,
            CanvasChangeKind::NodeRetired,
            fence,
        )
        .await
    }
}

/// A lane node goes with its universal node, never on its own; a universal pod
/// goes with its server; a distributor or aggregator must be unwired first, so
/// that nothing it generated is left behind.
fn ensure_retire_allowed(
    topology: &CanvasTopology,
    node: &NodeEntity,
) -> Result<(), OrchestrationError> {
    if let Some(lane) = &node.lane {
        return Err(OrchestrationError::Conflict(format!(
            "node is generated by a universal node ({}); disconnect the channel or bundle instead",
            lane.role.name()
        )));
    }
    match node.spec {
        NodeSpec::UniversalPod(_) => Err(OrchestrationError::Conflict(
            "a universal pod goes with its server; delete the server instead".into(),
        )),
        NodeSpec::LoadBalanceDistribute(_) | NodeSpec::LoadBalanceAggregate(_) => {
            let key = record_key(&node.id.0);
            let ports: std::collections::HashSet<String> = topology
                .nodes
                .iter()
                .filter(|n| record_key(&n.node.id.0) == key)
                .flat_map(|n| n.ports.iter().filter(|p| universal::is_on_demand(p)))
                .map(|p| record_key(&p.id.0))
                .collect();
            let wired = topology.edges.iter().any(|e| {
                ports.contains(&record_key(&e.source.0)) || ports.contains(&record_key(&e.target.0))
            });
            if wired {
                return Err(OrchestrationError::Conflict(
                    "disconnect every channel and bundle of the node first".into(),
                ));
            }
            Ok(())
        }
        _ => Ok(()),
    }
}

/// Deletes a node without validating the tree it leaves behind. Admin only.
pub struct ForceDeleteNode {
    pub actor: Identity,
    pub node: NodeId,
}

impl Processor<ForceDeleteNode> for NodeService {
    type Output = ();
    type Error = OrchestrationError;
    #[tracing::instrument(name = "Service:ForceDeleteNode", skip_all, err)]
    async fn process(&self, input: ForceDeleteNode) -> Result<Self::Output, Self::Error> {
        input.actor.ensure(Permission::EditWorkspace)?;
        if input.actor.role != AccountRole::Admin {
            return Err(OrchestrationError::PermissionDenied);
        }
        let node = self
            .db
            .process(FindNodeById {
                id: input.node.clone(),
            })
            .await?
            .ok_or(OrchestrationError::NotFound)?;
        let topology = self
            .db
            .process(LoadCanvasTopology {
                canvas: node.canvas.clone(),
            })
            .await?;
        let retirement = retirement(&topology, &node);
        // Force bypasses topology *rules*, not snapshot *sequencing*: retiring an
        // export computes a mirrored-port reshape from this snapshot, so fence it
        // when present or a concurrent export edit could be erased.
        let fence = if retirement.import_sync.is_some() {
            topology.fence()
        } else {
            None
        };
        self.delete_node(
            &topology,
            node,
            retirement,
            CanvasChangeKind::NodeDeleted,
            fence,
        )
        .await
    }
}
