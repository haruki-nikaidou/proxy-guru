//! Worker registration and acknowledgement.
//!
//! A worker holds its dynamic refresh key in memory only; the master stores just the
//! digest. Every registration rotates the key and bumps the generation, so a worker
//! restart is visible to the master and the previous session's streams die.

use crate::config::OrchestrationConfig;
use crate::entities::surreal::agent_release::FindAgentRelease;
use crate::entities::surreal::health::{
    InsertNodeHealthRecords, NewNodeHealthRecord, NodeHealthStatus, ServerHealthStatus,
    SetServerHealthStatus,
};
use crate::entities::surreal::server::{
    FindServerByAgentKeyDigest, FindServerById, FindServerByRefreshKeyDigest,
    RegisterWorkerSession, ReportedAddresses, ServerEntity, ServerId, SettleAgentUpdate,
};
use crate::entities::surreal::view::{
    AckServerConfig, ConfigSnapshot, FindServerConfigView, ForwardingDeps, PodFailure,
};
use crate::events::live::{CanvasChangeKind, LiveMessage, RolloutScope};
use crate::services::OrchestrationError;
use crate::services::health::{NodeVerdicts, ParsedSnapshot, SnapshotEntry, parse_snapshot};
use crate::services::notify::Notifier;
use crate::services::watch::{SessionLease, WatchHub};
use crate::utils::ids::record_key;
use auth::services::identity::Identity;
use auth::utils::rbac::Permission;
use auth::utils::token::{generate_refresh_key, sha256_hex};
use chrono::{DateTime, Utc};
use kanau::processor::Processor;
use std::collections::HashSet;
use wakuwaku::surreal::SurrealProcessor;

/// An authenticated worker: which server, and which refresh-key generation it holds.
#[derive(Clone, Debug)]
pub struct AgentIdentity {
    pub server: ServerId,
    pub generation: i64,
}

#[derive(Clone)]
pub struct AgentService {
    pub db: SurrealProcessor,
    pub hub: WatchHub,
    pub lease: SessionLease,
    pub notifier: Notifier,
    pub config: OrchestrationConfig,
}

/// What a worker presented to `Register`: an operator API key the auth
/// middleware resolved to an identity, or the raw `x-api-key` it could not — a
/// server's own agent key, checked here against the digest on the server row.
pub enum RegisterCredential {
    Operator(Identity),
    ServerKey(String),
}

pub struct RegisterWorker {
    pub credential: RegisterCredential,
    pub server_id: ServerId,
    pub running_revision: i64,
    /// The peer address the registration arrived from, if the transport knows.
    pub observed: Option<std::net::IpAddr>,
    /// What the worker discovered about its own addresses.
    pub reported: Option<ReportedAddresses>,
    /// The worker's build, when it reports one.
    pub agent_version: Option<String>,
    pub agent_arch: Option<String>,
    /// Why the last self-update on the host failed, when the worker found the
    /// start guard's record of it.
    pub last_update_error: Option<String>,
}

impl Processor<RegisterWorker> for AgentService {
    /// The plaintext refresh key; only its digest is stored.
    type Output = String;
    type Error = OrchestrationError;
    #[tracing::instrument(name = "Service:RegisterWorker", skip_all, err)]
    async fn process(&self, input: RegisterWorker) -> Result<Self::Output, Self::Error> {
        let server = self
            .authenticate_registration(&input.credential, &input.server_id)
            .await?;

        let secret = generate_refresh_key();
        let now = Utc::now();
        // One transaction: rotate the key, reconcile what the worker reports, and
        // clear whatever was in flight — the worker is not running it. Refused
        // while another session still heartbeats, so a second worker pointed at
        // the same server cannot take it over just by reconnecting.
        let rotated = self
            .db
            .process(RegisterWorkerSession {
                server: server.id.clone(),
                canvas: server.canvas.clone(),
                digest: sha256_hex(&secret),
                now,
                lease_until: self.lease.until(now),
                running_revision: input.running_revision,
                observed: input.observed.map(|a| a.to_string()),
                reported: input.reported,
                agent_version: input.agent_version.clone(),
                agent_arch: input.agent_arch,
            })
            .await?
            .ok_or_else(|| {
                OrchestrationError::Conflict(
                    "another worker session is live for this server".into(),
                )
            })?;

        // What the worker registered as decides a pending update: the requested
        // version means it landed, a guard rollback means it did not.
        if let Some(error) = &input.last_update_error {
            tracing::warn!(
                server = %record_key(&server.id.0),
                error,
                "worker reported a rolled-back self-update"
            );
        }
        if input.agent_version.is_some() || input.last_update_error.is_some() {
            self.db
                .process(SettleAgentUpdate {
                    id: server.id.clone(),
                    reported_version: input.agent_version,
                    error: input.last_update_error,
                })
                .await?;
        }

        self.hub
            .supersede(&record_key(&server.id.0), rotated.refresh_key_generation);
        self.notifier.notify(&server.canvas).await;
        // A registration reconciles what the worker runs and clears `in_flight`,
        // and it may have brought a new observed address with it.
        self.notifier
            .canvas_changed(
                &server.canvas,
                CanvasChangeKind::ServerIpChanged,
                vec![record_key(&server.id.0)],
            )
            .await;
        self.notifier
            .rollout_changed(RolloutScope::Server(record_key(&server.id.0)))
            .await;
        Ok(secret)
    }
}

impl AgentService {
    /// The server a registration is for, once its credential checks out.
    ///
    /// An operator key needs `ServerCall` and may register any server it names.
    /// A server key names the server itself — the row is found by the key's
    /// digest — and the `server_id` the worker sent must agree, so a key issued
    /// for one server can never register as another. An unknown key is refused
    /// the same way as a mismatch, without saying which.
    async fn authenticate_registration(
        &self,
        credential: &RegisterCredential,
        server_id: &ServerId,
    ) -> Result<ServerEntity, OrchestrationError> {
        match credential {
            RegisterCredential::Operator(actor) => {
                actor.ensure(Permission::ServerCall)?;
                self.db
                    .process(FindServerById {
                        id: server_id.clone(),
                    })
                    .await?
                    .ok_or(OrchestrationError::NotFound)
            }
            RegisterCredential::ServerKey(secret) => {
                let found = self
                    .db
                    .process(FindServerByAgentKeyDigest {
                        digest: sha256_hex(secret),
                    })
                    .await?;
                match found {
                    Some(server) if server.id.0 == server_id.0 => Ok(server),
                    // Unknown key and a key for another server are refused alike;
                    // the log tells them apart, the caller is not told.
                    found => {
                        tracing::warn!(
                            server = %record_key(&server_id.0),
                            known = found.is_some(),
                            "agent key refused at registration"
                        );
                        Err(OrchestrationError::PermissionDenied)
                    }
                }
            }
        }
    }
}

/// The published binary a worker should move to.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AgentUpdate {
    pub version: String,
    pub url: String,
    pub sha256: String,
}

/// A worker asking whether an update was requested for it, and reporting how
/// the previous attempt went.
pub struct PollAgentUpdate {
    pub agent: AgentIdentity,
    pub last_error: Option<String>,
}

impl Processor<PollAgentUpdate> for AgentService {
    /// The update to install, or nothing to do.
    type Output = Option<AgentUpdate>;
    type Error = OrchestrationError;
    #[tracing::instrument(name = "Service:PollAgentUpdate", skip_all, err)]
    async fn process(&self, input: PollAgentUpdate) -> Result<Self::Output, Self::Error> {
        let server = self
            .db
            .process(FindServerById {
                id: input.agent.server.clone(),
            })
            .await?
            .ok_or(OrchestrationError::NotFound)?;
        if server.refresh_key_generation != input.agent.generation {
            return Err(OrchestrationError::PermissionDenied);
        }
        let key = record_key(&server.id.0);
        // A failure ends the request; the operator reads why and asks again.
        if let Some(error) = input.last_error {
            tracing::warn!(server = %key, %error, "worker reported a failed self-update");
            self.settle_update(&server, None, Some(error)).await?;
            return Ok(None);
        }
        let Some(requested) = server.agent_update_requested.clone() else {
            return Ok(None);
        };
        if server.agent_version.as_deref() == Some(requested.as_str()) {
            self.settle_update(&server, Some(requested), None).await?;
            return Ok(None);
        }
        // The request names the release that was published when it was made;
        // a publish since then withdrew what the worker would have fetched.
        let release = self.db.process(FindAgentRelease).await?;
        let base = self.config.agent_download_base();
        let error = match (&release, &base) {
            (Some(release), Some(base)) if release.version == requested => {
                tracing::info!(
                    server = %key,
                    from = ?server.agent_version,
                    to = %release.version,
                    "offering an update"
                );
                return Ok(Some(AgentUpdate {
                    url: format!("{base}/{}/guru-worker", release.version),
                    version: release.version.clone(),
                    sha256: release.sha256.clone(),
                }));
            }
            (None, _) => {
                "the published release was withdrawn before the worker fetched it".to_string()
            }
            (_, None) => "agent_public_base_url is no longer configured".to_string(),
            (Some(release), _) => format!(
                "the published release changed to {} before the worker fetched {requested}",
                release.version
            ),
        };
        tracing::warn!(server = %key, %error, "dropping an update request");
        self.settle_update(&server, None, Some(error)).await?;
        Ok(None)
    }
}

impl AgentService {
    /// Ends an update request on the row and tells open canvas views the
    /// server's agent state changed. No dirty hint: nothing derived reads it.
    async fn settle_update(
        &self,
        server: &ServerEntity,
        reported_version: Option<String>,
        error: Option<String>,
    ) -> Result<(), OrchestrationError> {
        self.db
            .process(SettleAgentUpdate {
                id: server.id.clone(),
                reported_version,
                error,
            })
            .await?;
        self.notifier
            .canvas_changed(
                &server.canvas,
                CanvasChangeKind::ServerUpdated,
                vec![record_key(&server.id.0)],
            )
            .await;
        Ok(())
    }
}

pub struct AuthenticateRefreshKey {
    pub secret: String,
}

impl Processor<AuthenticateRefreshKey> for AgentService {
    type Output = Option<AgentIdentity>;
    type Error = OrchestrationError;
    #[tracing::instrument(name = "Service:AuthenticateRefreshKey", skip_all, err)]
    async fn process(&self, input: AuthenticateRefreshKey) -> Result<Self::Output, Self::Error> {
        let server = self
            .db
            .process(FindServerByRefreshKeyDigest {
                digest: sha256_hex(&input.secret),
            })
            .await?;
        Ok(server.map(|server| AgentIdentity {
            generation: server.refresh_key_generation,
            server: server.id,
        }))
    }
}

/// A worker's verdict on one `[[forwarding]]`, named by its tag (the pod name).
#[derive(Debug, Clone)]
pub struct PodResult {
    pub tag: String,
    /// Unset when the pod runs the shape the revision asked for.
    pub error: Option<String>,
}

/// `error` set: the revision was not applied at all and `pods` is empty.
/// Otherwise `pods` lists every pod of the revision with its outcome, and the
/// ones that failed keep their previous listener on the worker.
pub struct AckConfig {
    pub agent: AgentIdentity,
    pub revision: i64,
    pub error: Option<String>,
    pub pods: Vec<PodResult>,
}

impl Processor<AckConfig> for AgentService {
    type Output = ();
    type Error = OrchestrationError;
    #[tracing::instrument(name = "Service:AckConfig", skip_all, err)]
    async fn process(&self, input: AckConfig) -> Result<Self::Output, Self::Error> {
        let server = self
            .db
            .process(FindServerById {
                id: input.agent.server.clone(),
            })
            .await?
            .ok_or(OrchestrationError::NotFound)?;
        if server.refresh_key_generation != input.agent.generation {
            return Err(OrchestrationError::PermissionDenied);
        }
        let view = self
            .db
            .process(FindServerConfigView {
                server: server.id.clone(),
            })
            .await?
            .ok_or(OrchestrationError::NotFound)?;
        let in_flight = view
            .in_flight
            .as_ref()
            .filter(|snapshot| snapshot.revision == input.revision)
            .ok_or_else(|| OrchestrationError::Invalid("unknown revision".into()))?;
        let parsed = parse_snapshot(in_flight).map_err(invalid_snapshot)?;
        let now = Utc::now();
        // Failures keyed by tag: `(tag, error)`.
        let (applied, failed_pods, nodes) = match &input.error {
            Some(error) => {
                let failed: Vec<(&str, &str)> = parsed
                    .entries
                    .iter()
                    .map(|entry| (entry.tag(), error.as_str()))
                    .collect();
                (
                    None,
                    Vec::new(),
                    ack_node_records(&parsed.entries, &failed, now),
                )
            }
            None => {
                ensure_pods_match(&parsed.entries, &input.pods)?;
                let failed: Vec<(&str, &str)> = input
                    .pods
                    .iter()
                    .filter_map(|pod| Some((pod.tag.as_str(), pod.error.as_deref()?)))
                    .collect();
                let nodes = ack_node_records(&parsed.entries, &failed, now);
                if failed.is_empty() {
                    (None, Vec::new(), nodes)
                } else {
                    let (mix, failures) =
                        synthesise_applied(in_flight, parsed, view.applied.as_ref(), &failed)?;
                    (Some(mix), failures, nodes)
                }
            }
        };
        let degraded = input.error.is_some() || !failed_pods.is_empty();
        // One conditional update: an ack only lands on the revision the database
        // itself handed this session, so a stale or invented revision changes
        // nothing instead of silently marking the wrong config applied.
        let matched = self
            .db
            .process(AckServerConfig {
                server: server.id.clone(),
                canvas: server.canvas.clone(),
                revision: input.revision,
                error: input.error,
                applied,
                failed_pods,
            })
            .await?;
        if !matched {
            return Err(OrchestrationError::Invalid("unknown revision".into()));
        }
        // The verdict is known now; nobody should have to wait for the next
        // report to see it. The status write is fenced on the session like the
        // ack itself.
        let node_rows = self
            .db
            .process(InsertNodeHealthRecords { records: nodes })
            .await?;
        let health = self
            .db
            .process(SetServerHealthStatus {
                server: server.id.clone(),
                generation: Some(input.agent.generation),
                status: if degraded {
                    ServerHealthStatus::Degraded
                } else {
                    ServerHealthStatus::Online
                },
                now,
            })
            .await?;
        self.notifier.notify(&server.canvas).await;
        let server_key = record_key(&server.id.0);
        self.notifier
            .rollout_changed(RolloutScope::Server(server_key.clone()))
            .await;
        if !node_rows.is_empty() {
            self.notifier
                .live(LiveMessage::NodeHealth {
                    records: node_rows.iter().map(Into::into).collect(),
                })
                .await;
        }
        if let Some(write) = &health {
            self.notifier
                .live(LiveMessage::ServerHealth {
                    server: server_key,
                    canvas: record_key(&write.canvas.0),
                    record: (&write.record).into(),
                    status_changed: write.previous_status != write.record.status,
                })
                .await;
        }
        Ok(())
    }
}

/// The node records an ack settles: `Failed` with the worker's message for a
/// failed pod and every node its forwarding runs through, `Ready` for the rest;
/// one row per node, the worst verdict winning.
fn ack_node_records(
    entries: &[SnapshotEntry<'_>],
    failed: &[(&str, &str)],
    now: DateTime<Utc>,
) -> Vec<NewNodeHealthRecord> {
    let mut verdicts = NodeVerdicts::default();
    for entry in entries {
        let (status, message) = failed
            .iter()
            .find(|(tag, _)| *tag == entry.tag())
            .map_or((NodeHealthStatus::Ready, ""), |(_, error)| {
                (NodeHealthStatus::Failed, *error)
            });
        verdicts.record(entry.deps, status, message);
    }
    verdicts.into_records(now)
}

fn invalid_snapshot(error: guru_worker_config::ConfigError) -> OrchestrationError {
    OrchestrationError::Invalid(format!("stored snapshot is not valid TOML: {error}"))
}

/// A per-pod ack must name every `[[forwarding]]` of the revision exactly once:
/// anything else is a worker that acked a config it was not sent.
fn ensure_pods_match(
    entries: &[SnapshotEntry<'_>],
    pods: &[PodResult],
) -> Result<(), OrchestrationError> {
    let expected: HashSet<&str> = entries.iter().map(SnapshotEntry::tag).collect();
    let reported: HashSet<&str> = pods.iter().map(|pod| pod.tag.as_str()).collect();
    if pods.len() != entries.len() || reported.len() != pods.len() || reported != expected {
        return Err(OrchestrationError::Invalid(
            "pod results do not match the revision".into(),
        ));
    }
    Ok(())
}

/// What the worker runs after a partial apply: the acked revision for the pods
/// that took it, each failed pod's previous shape (matched by pod identity, not
/// socket — the edit that broke a pod may also have moved it) for the rest. A
/// failed pod with no previous shape runs nothing and is omitted.
///
/// The mix carries the acked revision number: it is what the worker reports as
/// running, and it is what registration reconciles against.
fn synthesise_applied(
    revision: &ConfigSnapshot,
    parsed: ParsedSnapshot<'_>,
    previous: Option<&ConfigSnapshot>,
    failed: &[(&str, &str)],
) -> Result<(ConfigSnapshot, Vec<PodFailure>), OrchestrationError> {
    let previous_entries = previous
        .map(parse_snapshot)
        .transpose()
        .map_err(invalid_snapshot)?
        .map(|previous| previous.entries)
        .unwrap_or_default();
    let ParsedSnapshot {
        mut config,
        entries,
    } = parsed;
    let mut deps: Vec<ForwardingDeps> = Vec::with_capacity(entries.len());
    let mut failures = Vec::with_capacity(failed.len());
    for entry in entries {
        let Some((tag, error)) = failed.iter().find(|(tag, _)| *tag == entry.tag()) else {
            config.forwardings.push(entry.forwarding);
            deps.push(entry.deps.clone());
            continue;
        };
        failures.push(PodFailure {
            pod: entry.deps.pod.clone(),
            tag: tag.to_string(),
            error: error.to_string(),
        });
        if let Some(old) = previous_entries
            .iter()
            .find(|old| old.deps.pod.0 == entry.deps.pod.0)
        {
            config.forwardings.push(old.forwarding.clone());
            deps.push(old.deps.clone());
        }
    }
    let mut certificates: Vec<_> = deps
        .iter()
        .flat_map(|deps| deps.certificates.iter().cloned())
        .collect();
    certificates.sort();
    certificates.dedup();
    let toml = config.to_toml_string().map_err(invalid_snapshot)?;
    Ok((
        ConfigSnapshot {
            revision: revision.revision,
            toml,
            created_at: revision.created_at,
            forwardings: deps,
            certificates,
        },
        failures,
    ))
}
