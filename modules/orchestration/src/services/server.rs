//! Server operations.
//!
//! A server's addresses are mostly learned, not typed: the worker reports its
//! public and interface addresses when it registers, the master records the
//! address the registration came from, and the operator only pins a slot when
//! the learned value is wrong for the fleet (NAT, an overlay network). What other
//! servers dial is computed from these by [`ServerEntity::effective_address`].
//!
//! Every server starts with one pod per transport it can be relayed into
//! (`tcp`, `tls`, `ws`, `quic`), each on a random high port, so a relay hop is
//! drawn by connecting to the target server's pod of the matching protocol.

use crate::config::OrchestrationConfig;
use crate::entities::surreal::canvas::{CanvasId, CanvasUiPosition, FindCanvasById};
use crate::entities::surreal::node::{CreateNodeRow, NodeSpec, PodConfig};
use crate::entities::surreal::server::{
    CreateServer as CreateServerRow, DeleteServerRow, FindServerById, MoveServerPosition,
    ServerEntity, ServerId, ServerIpv6Resolve, UpdateServerSettings,
};
use crate::entities::surreal::topology::LoadCanvasTopology;
use crate::entities::surreal::view::ListServerConfigViewsByCanvases;
use crate::services::converge::ensure_switch_safe;
use crate::services::node::port_layout;
use crate::services::rollout::DirtyNotifier;
use crate::services::topology::{TopologyEdit, ensure_valid};
use crate::services::{OrchestrationError, rollout};
use crate::utils::ids::record_key;
use auth::services::identity::Identity;
use auth::utils::rbac::Permission;
use kanau::processor::Processor;
use rand::Rng;
use std::net::IpAddr;
use std::ops::RangeInclusive;
use wakuwaku::surreal::SurrealProcessor;

/// The transport pods every new server gets, in the order they are created.
/// `ws` has no relay protocol yet and is a placeholder for one.
pub const DEFAULT_POD_NAMES: [&str; 4] = ["tcp", "tls", "ws", "quic"];
/// Where default pods and the dashboard's suggestions draw their ports from:
/// high enough to stay clear of anything an operator types by hand.
pub const DEFAULT_POD_PORTS: RangeInclusive<u16> = 40000..=59999;

#[derive(Clone)]
pub struct ServerService {
    pub db: SurrealProcessor,
    pub notifier: DirtyNotifier,
    pub config: OrchestrationConfig,
}

/// The operator-typed address fields of a create or update, already validated.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct AddressOverrides {
    pub override_v4: Option<String>,
    pub override_v6: Option<String>,
    pub extra_addresses: Vec<String>,
}

impl AddressOverrides {
    /// Normalises and validates raw input: empty strings clear a slot, the IPv4
    /// slot only takes an IPv4 literal (and vice versa), extras are deduplicated.
    pub fn parse(
        override_v4: &str,
        override_v6: &str,
        extra_addresses: &[String],
    ) -> Result<Self, OrchestrationError> {
        let slot = |label: &str, raw: &str, want_v4: bool| -> Result<Option<String>, OrchestrationError> {
            let raw = raw.trim();
            if raw.is_empty() {
                return Ok(None);
            }
            let ip: IpAddr = raw.parse().map_err(|_| {
                OrchestrationError::Invalid(format!("{label}: '{raw}' is not an IP address"))
            })?;
            if ip.is_ipv4() != want_v4 {
                return Err(OrchestrationError::Invalid(format!(
                    "{label}: '{raw}' is not an {} address",
                    if want_v4 { "IPv4" } else { "IPv6" }
                )));
            }
            if ip.is_unspecified() {
                return Err(OrchestrationError::Invalid(format!(
                    "{label}: '{raw}' is not an address anyone can dial"
                )));
            }
            Ok(Some(ip.to_string()))
        };
        let mut extras: Vec<String> = Vec::new();
        for raw in extra_addresses {
            let raw = raw.trim();
            if raw.is_empty() {
                continue;
            }
            let ip: IpAddr = raw.parse().map_err(|_| {
                OrchestrationError::Invalid(format!(
                    "extra_addresses: '{raw}' is not an IP address"
                ))
            })?;
            let normalised = ip.to_string();
            if !extras.contains(&normalised) {
                extras.push(normalised);
            }
        }
        Ok(Self {
            override_v4: slot("override_v4", override_v4, true)?,
            override_v6: slot("override_v6", override_v6, false)?,
            extra_addresses: extras,
        })
    }
}

/// `count` distinct random ports from [`DEFAULT_POD_PORTS`].
pub fn random_default_ports(count: usize) -> Vec<u16> {
    let mut rng = rand::rng();
    let mut ports: Vec<u16> = Vec::with_capacity(count);
    while ports.len() < count {
        let port = rng.random_range(DEFAULT_POD_PORTS);
        if !ports.contains(&port) {
            ports.push(port);
        }
    }
    ports
}

pub struct CreateServer {
    pub actor: Identity,
    pub canvas: CanvasId,
    pub name: String,
    pub icon: String,
    pub comment: String,
    pub position: CanvasUiPosition,
    pub ipv6_resolve: ServerIpv6Resolve,
    pub log_level: String,
    pub addresses: AddressOverrides,
}

impl Processor<CreateServer> for ServerService {
    type Output = ServerEntity;
    type Error = OrchestrationError;
    #[tracing::instrument(name = "Service:CreateServer", skip_all, err)]
    async fn process(&self, input: CreateServer) -> Result<Self::Output, Self::Error> {
        input.actor.ensure(Permission::EditWorkspace)?;
        if input.log_level.trim().is_empty() {
            return Err(OrchestrationError::Invalid(
                "log_level must not be empty".into(),
            ));
        }
        self.db
            .process(FindCanvasById {
                id: input.canvas.clone(),
            })
            .await?
            .ok_or(OrchestrationError::NotFound)?;
        // The row and its config view are created in one transaction, so there is
        // nothing to re-read and nothing to stamp here.
        let server = self
            .db
            .process(CreateServerRow {
                canvas: input.canvas.clone(),
                name: input.name,
                icon: input.icon,
                comment: input.comment,
                position: input.position,
                ipv6_resolve: input.ipv6_resolve,
                log_level: input.log_level,
                override_v4: input.addresses.override_v4,
                override_v6: input.addresses.override_v6,
                extra_addresses: input.addresses.extra_addresses,
            })
            .await?;
        // The transport pods. Unwired pods derive nothing and clash with nothing
        // (distinct ports), so no projection is needed; a failure part-way leaves
        // a server with fewer default pods, which the operator can add by hand.
        for (name, port) in DEFAULT_POD_NAMES
            .iter()
            .zip(random_default_ports(DEFAULT_POD_NAMES.len()))
        {
            let spec = NodeSpec::Pod(PodConfig {
                server: server.id.clone(),
                port,
                bind_ip: None,
                advertise_ip: None,
            });
            let ports = port_layout(&spec, 0)?;
            self.db
                .process(CreateNodeRow {
                    canvas: input.canvas.clone(),
                    name: (*name).to_string(),
                    comment: String::new(),
                    spec,
                    position: server.position,
                    ports,
                    import_sync: None,
                })
                .await?;
        }
        self.notifier.notify(&input.canvas).await;
        Ok(server)
    }
}

pub struct UpdateServer {
    pub actor: Identity,
    pub server: ServerId,
    pub name: String,
    pub icon: String,
    pub comment: String,
    pub ipv6_resolve: ServerIpv6Resolve,
    pub log_level: String,
    pub addresses: AddressOverrides,
}

impl Processor<UpdateServer> for ServerService {
    type Output = ServerEntity;
    type Error = OrchestrationError;
    #[tracing::instrument(name = "Service:UpdateServer", skip_all, err)]
    async fn process(&self, input: UpdateServer) -> Result<Self::Output, Self::Error> {
        input.actor.ensure(Permission::EditWorkspace)?;
        if input.log_level.trim().is_empty() {
            return Err(OrchestrationError::Invalid(
                "log_level must not be empty".into(),
            ));
        }
        let canvas = rollout::canvas_of_server(&self.db, &input.server).await?;
        let topology = self
            .db
            .process(LoadCanvasTopology {
                canvas: canvas.clone(),
            })
            .await?;
        let projected = topology.project(&[TopologyEdit::SetServerSettings {
            server: input.server.clone(),
            ipv6_resolve: input.ipv6_resolve,
            log_level: input.log_level.clone(),
            override_v4: input.addresses.override_v4.clone(),
            override_v6: input.addresses.override_v6.clone(),
            extra_addresses: input.addresses.extra_addresses.clone(),
        }]);
        ensure_valid(&projected)?;
        let views = self
            .db
            .process(ListServerConfigViewsByCanvases {
                canvases: projected.canvas_ids(),
            })
            .await?;
        ensure_switch_safe(&projected, &views, &self.config)?;

        let server = self
            .db
            .process(UpdateServerSettings {
                id: input.server,
                canvas: canvas.clone(),
                name: input.name,
                icon: input.icon,
                comment: input.comment,
                ipv6_resolve: input.ipv6_resolve,
                log_level: input.log_level,
                override_v4: input.addresses.override_v4,
                override_v6: input.addresses.override_v6,
                extra_addresses: input.addresses.extra_addresses,
            })
            .await?;
        self.notifier.notify(&canvas).await;
        Ok(server)
    }
}

pub struct MoveServer {
    pub actor: Identity,
    pub server: ServerId,
    pub position: CanvasUiPosition,
}

impl Processor<MoveServer> for ServerService {
    type Output = ();
    type Error = OrchestrationError;
    #[tracing::instrument(name = "Service:MoveServer", skip_all, err)]
    async fn process(&self, input: MoveServer) -> Result<Self::Output, Self::Error> {
        input.actor.ensure(Permission::EditWorkspace)?;
        self.db
            .process(FindServerById {
                id: input.server.clone(),
            })
            .await?
            .ok_or(OrchestrationError::NotFound)?;
        // Metadata only.
        self.db
            .process(MoveServerPosition {
                id: input.server,
                position: input.position,
            })
            .await?;
        Ok(())
    }
}

pub struct DeleteServer {
    pub actor: Identity,
    pub server: ServerId,
}

impl Processor<DeleteServer> for ServerService {
    type Output = ();
    type Error = OrchestrationError;
    #[tracing::instrument(name = "Service:DeleteServer", skip_all, err)]
    async fn process(&self, input: DeleteServer) -> Result<Self::Output, Self::Error> {
        input.actor.ensure(Permission::EditWorkspace)?;
        let canvas = rollout::canvas_of_server(&self.db, &input.server).await?;
        let topology = self
            .db
            .process(LoadCanvasTopology {
                canvas: canvas.clone(),
            })
            .await?;
        let server_key = record_key(&input.server.0);
        let live_pods = topology
            .nodes
            .iter()
            .filter(|node| match &node.node.spec {
                NodeSpec::Pod(cfg) => record_key(&cfg.server.0) == server_key,
                _ => false,
            })
            .count();
        if live_pods > 0 {
            return Err(OrchestrationError::Conflict(format!(
                "server still has {live_pods} pod(s); delete them first"
            )));
        }

        self.db
            .process(DeleteServerRow {
                id: input.server.clone(),
                canvas: canvas.clone(),
            })
            .await?;
        self.notifier.notify(&canvas).await;
        Ok(())
    }
}
