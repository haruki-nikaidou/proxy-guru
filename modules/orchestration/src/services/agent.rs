//! Worker registration and acknowledgement.
//!
//! A worker holds its dynamic refresh key in memory only; the master stores just the
//! digest. Every registration rotates the key and bumps the generation, so a worker
//! restart is visible to the master and the previous session's streams die.

use crate::entities::surreal::health::{
    InsertNodeHealthRecords, NewNodeHealthRecord, NodeHealthStatus, ServerHealthStatus,
    SetServerHealthStatus,
};
use crate::entities::surreal::server::{
    FindServerById, FindServerByRefreshKeyDigest, RegisterWorkerSession, ReportedAddresses,
    ServerId,
};
use crate::entities::surreal::view::{
    AckServerConfig, ConfigSnapshot, FindServerConfigView, ForwardingDeps, PodFailure,
};
use crate::services::OrchestrationError;
use crate::services::health::{NodeVerdicts, ParsedSnapshot, SnapshotEntry, parse_snapshot};
use crate::services::rollout::DirtyNotifier;
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
    pub notifier: DirtyNotifier,
}

pub struct RegisterWorker {
    pub actor: Identity,
    pub server_id: ServerId,
    pub running_revision: i64,
    /// The peer address the registration arrived from, if the transport knows.
    pub observed: Option<std::net::IpAddr>,
    /// What the worker discovered about its own addresses.
    pub reported: Option<ReportedAddresses>,
}

impl Processor<RegisterWorker> for AgentService {
    /// The plaintext refresh key; only its digest is stored.
    type Output = String;
    type Error = OrchestrationError;
    #[tracing::instrument(name = "Service:RegisterWorker", skip_all, err)]
    async fn process(&self, input: RegisterWorker) -> Result<Self::Output, Self::Error> {
        input.actor.ensure(Permission::ServerCall)?;
        let server = self
            .db
            .process(FindServerById {
                id: input.server_id.clone(),
            })
            .await?
            .ok_or(OrchestrationError::NotFound)?;

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
            })
            .await?
            .ok_or_else(|| {
                OrchestrationError::Conflict(
                    "another worker session is live for this server".into(),
                )
            })?;

        self.hub
            .supersede(&record_key(&server.id.0), rotated.refresh_key_generation);
        self.notifier.notify(&server.canvas).await;
        Ok(secret)
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
        self.db
            .process(InsertNodeHealthRecords { records: nodes })
            .await?;
        self.db
            .process(SetServerHealthStatus {
                server: server.id,
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
