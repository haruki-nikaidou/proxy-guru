//! Server operations.
//!
//! A server's addresses are mostly learned, not typed: the worker reports its
//! public and interface addresses when it registers, the master records the
//! address the registration came from, and the operator only pins a slot when
//! the learned value is wrong for the fleet (NAT, an overlay network). What other
//! servers dial is computed from these by [`ServerEntity::effective_address`].
//!
//! Every server comes with its universal pod (see `services::universal`): the
//! one node other universal nodes bundle to, which lands every channel it
//! receives on a generated pod of this server.

use crate::config::OrchestrationConfig;
use crate::entities::surreal::canvas::{CanvasId, CanvasUiPosition, FindCanvasById};
use crate::entities::surreal::node::{CreateNodeRow, NodeSpec, UniversalPodConfig};
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
use std::net::IpAddr;
use std::ops::RangeInclusive;
use wakuwaku::surreal::SurrealProcessor;

/// Where generated landing pods and the dashboard's suggestions draw their
/// ports from: high enough to stay clear of anything an operator types by hand.
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
        // The universal pod. Unwired it derives nothing and clashes with
        // nothing, so no projection is needed.
        let spec = NodeSpec::UniversalPod(UniversalPodConfig {
            server: server.id.clone(),
        });
        let ports = port_layout(&spec, 0)?;
        self.db
            .process(CreateNodeRow {
                canvas: input.canvas.clone(),
                name: server.name.clone(),
                comment: String::new(),
                spec,
                position: server.position,
                ports,
                import_sync: None,
            })
            .await?;
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
        let mine: Vec<&crate::entities::surreal::node::NodeWithPorts> = topology
            .nodes
            .iter()
            .filter(|node| match &node.node.spec {
                NodeSpec::Pod(cfg) => record_key(&cfg.server.0) == server_key,
                _ => false,
            })
            .collect();
        // A landing pod is not the operator's to delete: name the channels it
        // serves, so they know which bundle to cut.
        let mut channels: Vec<String> = mine
            .iter()
            .filter_map(|node| node.node.lane.as_ref())
            .map(|lane| {
                let key = record_key(&lane.channel.0);
                topology
                    .nodes
                    .iter()
                    .find(|n| record_key(&n.node.id.0) == key)
                    .map(|n| n.node.name.clone())
                    .unwrap_or(key)
            })
            .collect();
        channels.sort();
        channels.dedup();
        if !channels.is_empty() {
            return Err(OrchestrationError::Conflict(format!(
                "server still lands channel(s) {}; disconnect the bundles into it first",
                channels.join(", ")
            )));
        }
        let live_pods = mine.len();
        if live_pods > 0 {
            return Err(OrchestrationError::Conflict(format!(
                "server still has {live_pods} pod(s); delete them first"
            )));
        }
        let universal_ports: std::collections::HashSet<String> = topology
            .nodes
            .iter()
            .filter(|node| matches!(&node.node.spec, NodeSpec::UniversalPod(cfg) if record_key(&cfg.server.0) == server_key))
            .flat_map(|node| node.ports.iter().map(|p| record_key(&p.id.0)))
            .collect();
        if topology.edges.iter().any(|e| {
            universal_ports.contains(&record_key(&e.source.0))
                || universal_ports.contains(&record_key(&e.target.0))
        }) {
            return Err(OrchestrationError::Conflict(
                "server's universal pod is still bundled; disconnect its bundles first".into(),
            ));
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
