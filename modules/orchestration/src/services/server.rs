//! Server and ip-record operations.

use crate::config::OrchestrationConfig;
use crate::entities::surreal::canvas::{CanvasId, CanvasUiPosition, FindCanvasById};
use crate::entities::surreal::node::NodeSpec;
use crate::entities::surreal::server::{
    CreateServer as CreateServerRow, CreateServerIp, DeleteServerIpRow, DeleteServerRow,
    FindServerById, FindServerIpById, MoveServerPosition, ServerEntity, ServerId,
    ServerIpRecordEntity, ServerIpRecordId, ServerIpv6Resolve, ServerWithIp, UpdateServerSettings,
};
use crate::entities::surreal::topology::LoadCanvasTopology;
use crate::entities::surreal::view::ListServerConfigViewsByCanvases;
use crate::services::converge::ensure_switch_safe;
use crate::services::rollout::DirtyNotifier;
use crate::services::topology::{TopologyEdit, ensure_valid};
use crate::services::{OrchestrationError, rollout};
use crate::utils::ids::record_key;
use auth::services::identity::Identity;
use auth::utils::rbac::Permission;
use kanau::processor::Processor;
use std::net::IpAddr;
use wakuwaku::surreal::SurrealProcessor;

#[derive(Clone)]
pub struct ServerService {
    pub db: SurrealProcessor,
    pub notifier: DirtyNotifier,
    pub config: OrchestrationConfig,
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
}

impl Processor<UpdateServer> for ServerService {
    type Output = ServerWithIp;
    type Error = OrchestrationError;
    #[tracing::instrument(name = "Service:UpdateServer", skip_all, err)]
    async fn process(&self, input: UpdateServer) -> Result<Self::Output, Self::Error> {
        input.actor.ensure(Permission::EditWorkspace)?;
        if input.log_level.trim().is_empty() {
            return Err(OrchestrationError::Invalid(
                "log_level must not be empty".into(),
            ));
        }
        let server_key = record_key(&input.server.0);
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
            })
            .await?;
        self.notifier.notify(&canvas).await;
        // The ip records come from the topology this edit was validated against;
        // the settings write does not touch them.
        let ips = topology
            .ips
            .into_iter()
            .filter(|ip| record_key(&ip.server.0) == server_key)
            .collect();
        Ok(ServerWithIp { server, ips })
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
        let owns_live_pod = topology.nodes.iter().any(|node| match &node.node.spec {
            NodeSpec::Pod(cfg) => topology.ips.iter().any(|ip| {
                record_key(&ip.id.0) == record_key(&cfg.ip.0)
                    && record_key(&ip.server.0) == server_key
            }),
            _ => false,
        });
        if owns_live_pod {
            return Err(OrchestrationError::Conflict(
                "server still has live pods".into(),
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

pub struct AddServerIp {
    pub actor: Identity,
    pub server: ServerId,
    pub ip: String,
    pub country: String,
}

impl Processor<AddServerIp> for ServerService {
    type Output = ServerIpRecordEntity;
    type Error = OrchestrationError;
    #[tracing::instrument(name = "Service:AddServerIp", skip_all, err)]
    async fn process(&self, input: AddServerIp) -> Result<Self::Output, Self::Error> {
        input.actor.ensure(Permission::EditWorkspace)?;
        if input.ip.parse::<IpAddr>().is_err() {
            return Err(OrchestrationError::Invalid(format!(
                "'{}' is not an IP address",
                input.ip
            )));
        }
        self.db
            .process(FindServerById {
                id: input.server.clone(),
            })
            .await?
            .ok_or(OrchestrationError::NotFound)?;
        Ok(self
            .db
            .process(CreateServerIp {
                server: input.server,
                ip: input.ip,
                country: input.country,
            })
            .await?)
    }
}

pub struct RemoveServerIp {
    pub actor: Identity,
    pub ip_record: ServerIpRecordId,
}

impl Processor<RemoveServerIp> for ServerService {
    type Output = ();
    type Error = OrchestrationError;
    #[tracing::instrument(name = "Service:RemoveServerIp", skip_all, err)]
    async fn process(&self, input: RemoveServerIp) -> Result<Self::Output, Self::Error> {
        input.actor.ensure(Permission::EditWorkspace)?;
        let record = self
            .db
            .process(FindServerIpById {
                id: input.ip_record.clone(),
            })
            .await?
            .ok_or(OrchestrationError::NotFound)?;
        let canvas = rollout::canvas_of_server(&self.db, &record.server).await?;
        let topology = self
            .db
            .process(LoadCanvasTopology {
                canvas: canvas.clone(),
            })
            .await?;
        let ip_key = record_key(&input.ip_record.0);
        let used = topology.nodes.iter().any(|node| match &node.node.spec {
            NodeSpec::Pod(cfg) => record_key(&cfg.ip.0) == ip_key,
            _ => false,
        });
        if used {
            return Err(OrchestrationError::Conflict(
                "ip record is used by a live pod".into(),
            ));
        }
        ensure_valid(&topology.project(&[TopologyEdit::RemoveIp {
            ip: input.ip_record.clone(),
        }]))?;

        self.db
            .process(DeleteServerIpRow {
                id: input.ip_record,
                canvas: canvas.clone(),
            })
            .await?;
        self.notifier.notify(&canvas).await;
        Ok(())
    }
}
