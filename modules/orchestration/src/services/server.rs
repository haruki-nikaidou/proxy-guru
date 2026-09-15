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
use crate::entities::surreal::agent_release::{AgentReleaseEntity, FindAgentRelease};
use crate::entities::surreal::canvas::{CanvasId, CanvasUiPosition, FindCanvasById};
use crate::entities::surreal::node::{CreateNodeRow, NodeSpec, UniversalPodConfig};
use crate::entities::surreal::server::{
    CreateServer as CreateServerRow, DeleteServerRow, FindServerById, MoveServerPosition,
    ServerEntity, ServerId, ServerIpv6Resolve, SetAgentUpdateRequested, SetServerAgentKey,
    UpdateServerSettings,
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
use auth::utils::token::{generate_server_agent_key, sha256_hex};
use chrono::Utc;
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
    /// Already validated by [`agent_unit_from`]; `None` clears it.
    pub agent_unit: Option<String>,
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
                agent_unit: input.agent_unit,
            })
            .await?;
        self.notifier.notify(&canvas).await;
        Ok(server)
    }
}

/// Longest systemd instance name the install command accepts.
const AGENT_UNIT_MAX_LEN: usize = 32;

/// Validates an operator-typed systemd instance name: empty means none, else
/// 1–32 lowercase letters, digits or dashes, starting with a letter or digit —
/// what fits unescaped into `guru-worker@<unit>` and its file paths.
pub fn agent_unit_from(raw: &str) -> Result<Option<String>, OrchestrationError> {
    let unit = raw.trim();
    if unit.is_empty() {
        return Ok(None);
    }
    let allowed = |b: u8| b.is_ascii_lowercase() || b.is_ascii_digit();
    let valid = unit.len() <= AGENT_UNIT_MAX_LEN
        && unit.bytes().next().is_some_and(allowed)
        && unit.bytes().all(|b| allowed(b) || b == b'-');
    if !valid {
        return Err(OrchestrationError::Invalid(
            "agent_unit must be 1-32 lowercase letters, digits or dashes, starting with a \
             letter or digit"
                .into(),
        ));
    }
    Ok(Some(unit.to_string()))
}

/// The instance name a server gets when the operator typed none: its name as a
/// slug, or its record key when the name has nothing a slug can keep.
pub fn default_agent_unit(name: &str, key: &str) -> String {
    let mut slug = String::new();
    let mut pending_dash = false;
    for ch in name.chars().flat_map(char::to_lowercase) {
        if ch.is_ascii_lowercase() || ch.is_ascii_digit() {
            if pending_dash && !slug.is_empty() {
                slug.push('-');
            }
            slug.push(ch);
            pending_dash = false;
        } else {
            pending_dash = true;
        }
        if slug.len() >= AGENT_UNIT_MAX_LEN {
            break;
        }
    }
    let slug = slug.trim_end_matches('-');
    if slug.is_empty() {
        key.chars().take(AGENT_UNIT_MAX_LEN).collect()
    } else {
        slug.to_string()
    }
}

/// A rendered install command and what it was rendered for.
#[derive(Debug, Clone)]
pub struct AgentInstall {
    /// The one-liner to paste on the host. Carries the freshly issued agent key,
    /// which is not stored in the clear anywhere else.
    pub command: String,
    pub unit: String,
    pub version: String,
    pub server: ServerEntity,
}

/// Issues the server's agent key and renders the install command carrying it.
///
/// Issuing replaces any previous key: a worker still holding it is refused at
/// its next registration, which is what "regenerate" means to an operator. The
/// instance name is the one given, else the stored one, else derived from the
/// server name.
pub struct IssueServerAgentInstall {
    pub actor: Identity,
    pub server: ServerId,
    pub unit: Option<String>,
}

impl Processor<IssueServerAgentInstall> for ServerService {
    type Output = AgentInstall;
    type Error = OrchestrationError;
    #[tracing::instrument(name = "Service:IssueServerAgentInstall", skip_all, err)]
    async fn process(&self, input: IssueServerAgentInstall) -> Result<Self::Output, Self::Error> {
        input.actor.ensure(Permission::ManageApiKeys)?;
        let base = self.config.agent_download_base().ok_or_else(|| {
            OrchestrationError::Invalid(
                "agent_public_base_url is not configured on the orchestration config".into(),
            )
        })?;
        let release = self
            .db
            .process(FindAgentRelease)
            .await?
            .ok_or_else(|| {
                OrchestrationError::Invalid(
                    "no worker release is published: run `manage-tool agent publish`".into(),
                )
            })?;
        let server = self
            .db
            .process(FindServerById {
                id: input.server.clone(),
            })
            .await?
            .ok_or(OrchestrationError::NotFound)?;
        let unit = input
            .unit
            .or_else(|| server.agent_unit.clone())
            .unwrap_or_else(|| default_agent_unit(&server.name, &record_key(&server.id.0)));

        let secret = generate_server_agent_key();
        let server = self
            .db
            .process(SetServerAgentKey {
                id: server.id.clone(),
                digest: sha256_hex(&secret),
                unit: unit.clone(),
                now: Utc::now(),
            })
            .await?;
        tracing::info!(server = %record_key(&server.id.0), unit, "issued a server agent key");
        let command = render_install_command(&self.config, &base, &release, &server, &unit, &secret);
        Ok(AgentInstall {
            command,
            unit,
            version: release.version,
            server,
        })
    }
}

/// The one-liner the operator pastes on the host: the installer piped into
/// `sudo env … sh`, every setting in the environment and none on the command
/// line. The download base is spelled out only when it is not the installer's
/// default (`<master>/agent`).
fn render_install_command(
    config: &OrchestrationConfig,
    base: &str,
    release: &AgentReleaseEntity,
    server: &ServerEntity,
    unit: &str,
    secret: &str,
) -> String {
    let master = config.agent_public_base_url.trim().trim_end_matches('/');
    let mut env = vec![
        format!("GURU_MASTER={master}"),
        format!("GURU_SERVER_ID={}", record_key(&server.id.0)),
        format!("GURU_UNIT={unit}"),
        format!("GURU_AGENT_VERSION={}", release.version),
        format!("GURU_AGENT_SHA256={}", release.sha256),
    ];
    if base != format!("{master}/agent") {
        env.push(format!("GURU_DOWNLOAD_BASE={base}"));
    }
    env.push(format!("GURU_API_KEY={secret}"));
    let mut lines = vec![format!("curl -fsSL {base}/install.sh \\")];
    for (i, var) in env.iter().enumerate() {
        if i == 0 {
            lines.push(format!("  | sudo env {var} \\"));
        } else {
            lines.push(format!("             {var} \\"));
        }
    }
    lines.push("             sh".to_string());
    lines.join("\n")
}

/// Asks the server's worker to move to the published release. The worker
/// picks the request up at its next poll; what happens then is settled by
/// what it registers as afterwards (see `PollAgentUpdate`).
pub struct RequestAgentUpdate {
    pub actor: Identity,
    pub server: ServerId,
}

impl Processor<RequestAgentUpdate> for ServerService {
    type Output = ServerEntity;
    type Error = OrchestrationError;
    #[tracing::instrument(name = "Service:RequestAgentUpdate", skip_all, err)]
    async fn process(&self, input: RequestAgentUpdate) -> Result<Self::Output, Self::Error> {
        input.actor.ensure(Permission::EditWorkspace)?;
        let release = self
            .db
            .process(FindAgentRelease)
            .await?
            .ok_or_else(|| {
                OrchestrationError::Invalid(
                    "no worker release is published: run `manage-tool agent publish`".into(),
                )
            })?;
        if self.config.agent_download_base().is_none() {
            return Err(OrchestrationError::Invalid(
                "agent_public_base_url is not configured on the orchestration config".into(),
            ));
        }
        let server = self
            .db
            .process(FindServerById {
                id: input.server.clone(),
            })
            .await?
            .ok_or(OrchestrationError::NotFound)?;
        let Some(running) = server.agent_version.as_deref() else {
            return Err(OrchestrationError::Invalid(
                "the worker has not reported a version yet; install or restart it first".into(),
            ));
        };
        if running == release.version {
            return Err(OrchestrationError::Invalid(format!(
                "the worker already runs {running}"
            )));
        }
        if let Some(arch) = server.agent_arch.as_deref()
            && arch != release.arch
        {
            return Err(OrchestrationError::Invalid(format!(
                "the worker runs on {arch} but the published binary is for {}",
                release.arch
            )));
        }
        tracing::info!(
            server = %record_key(&server.id.0),
            from = running,
            to = %release.version,
            "update requested"
        );
        Ok(self
            .db
            .process(SetAgentUpdateRequested {
                id: server.id.clone(),
                version: release.version,
            })
            .await?)
    }
}

/// The published worker release, and whether an install command can be
/// rendered for it.
pub struct GetAgentRelease {
    pub actor: Identity,
}

#[derive(Debug, Clone)]
pub struct AgentReleaseInfo {
    pub release: Option<AgentReleaseEntity>,
    /// `agent_public_base_url` is set, so the install command has an origin.
    pub base_url_configured: bool,
}

impl Processor<GetAgentRelease> for ServerService {
    type Output = AgentReleaseInfo;
    type Error = OrchestrationError;
    #[tracing::instrument(name = "Service:GetAgentRelease", skip_all, err)]
    async fn process(&self, input: GetAgentRelease) -> Result<Self::Output, Self::Error> {
        input.actor.ensure(Permission::ViewWorkspace)?;
        Ok(AgentReleaseInfo {
            release: self.db.process(FindAgentRelease).await?,
            base_url_configured: self.config.agent_download_base().is_some(),
        })
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unit_names_are_slugs_of_the_server_name() {
        assert_eq!(default_agent_unit("HK Edge 1", "k"), "hk-edge-1");
        assert_eq!(default_agent_unit("  tokyo--relay  ", "k"), "tokyo-relay");
        assert_eq!(default_agent_unit("東京", "uz0ih3b30nrekqzs1h1y"), "uz0ih3b30nrekqzs1h1y");
        assert_eq!(default_agent_unit(&"a".repeat(40), "k").len(), AGENT_UNIT_MAX_LEN);
    }

    #[test]
    fn typed_unit_names_are_validated() {
        assert_eq!(agent_unit_from("  ").unwrap_or(None), None);
        assert_eq!(agent_unit_from("hk-1").unwrap_or(None).as_deref(), Some("hk-1"));
        assert!(agent_unit_from("-hk").is_err());
        assert!(agent_unit_from("HK").is_err());
        assert!(agent_unit_from("hk 1").is_err());
        assert!(agent_unit_from(&"a".repeat(33)).is_err());
    }
}
