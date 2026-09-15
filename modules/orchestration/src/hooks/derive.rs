//! The derivation reactor: turns canvas edits into per-server config snapshots.
//!
//! Every mutating transaction bumps the *root* canvas's `generation`
//! (`fn::orchestration_touch`) and publishes [`CanvasDirty`]. This hook resolves
//! the tree of whatever canvas it is asked about, derives the whole tree at the
//! generation it read and commits only while the root is still at that
//! generation, so a concurrent edit can never be overwritten by a stale pass — it
//! just loses the race and the pass is redone.
//!
//! The message is a latency hint, not the contract: [`sweep_stale_canvases`]
//! re-derives any canvas whose `generation` ran ahead of its
//! `derived_generation`, so a dropped message, a broker outage or a crashed
//! consumer costs at most one sweep interval.
//!
//! Neither periodic pass is an in-process loop any more. `--mode cron` publishes
//! [`DeriveStaleCanvasesSignal`] and [`RotateRelayCertificatesSignal`] and this
//! hook consumes them, so the work is spread over the consumer fleet and runs at
//! most once per tick. The generation counters still carry correctness, so a
//! lost or late signal costs latency, never a wrong config.
//!
//! Certificates are part of the input: a pass first issues the relay leaves its
//! tree needs (when the internal CA exists), then derives against the ACME rows,
//! relay leaves and CA it can see. A pod whose material is missing is reported
//! invalid, never a hard failure. [`rotate_expiring_relay_certificates`]
//! re-issues leaves that are about to expire and re-derives their canvases so
//! the new material ships as a new revision.

use crate::config::OrchestrationConfig;
use crate::entities::surreal::ca::{
    FindInternalCa, ListRelayCertificatesByPods, ListRelayCertificatesExpiringBefore,
};
use crate::entities::surreal::canvas::CanvasId;
use crate::entities::surreal::certificate::{ListCertificatesBySnis, TouchCanvases};
use crate::entities::surreal::health::{
    InsertNodeHealthRecords, NewNodeHealthRecord, NodeHealthStatus,
};
use crate::entities::surreal::job_run::ClaimJobRun;
use crate::entities::surreal::node::{ListCanvasesOfNodes, NodeId};
use crate::entities::surreal::server::ServerId;
use crate::entities::surreal::topology::CanvasTopology;
use crate::entities::surreal::view::{
    CertificateRef, CommitCanvasDerivation, ConfigSnapshot, ListStaleCanvases,
    LoadCanvasDerivationInput, ServerConfigViewEntity, ViewUpdate,
};
use crate::events::live::{LiveMessage, RolloutScope};
use crate::events::{CanvasDirty, DeriveStaleCanvasesSignal, RotateRelayCertificatesSignal};
use crate::services::ca::{CaService, EnsureRelayCertificates, RotateRelayCertificate};
use crate::services::converge::converge;
use crate::services::derive::{
    DerivationCertificates, derive_server_config, relay_tls_pods, tls_snis,
};
use crate::services::notify::Notifier;
use crate::utils::ids::{self, record_key};
use crate::utils::secret::SecretKey;
use chrono::{DateTime, Utc};
use guru_worker_config::{Config, Forwarding};
use kanau::processor::Processor;
use std::collections::HashMap;
use wakuwaku::amqp::AmqpMessageProcessor;
use wakuwaku::surreal::SurrealProcessor;

/// How many times one pass retries after losing the generation race before it
/// leaves the canvas to the next message or sweep tick.
const MAX_ATTEMPTS: usize = 8;

#[derive(Clone)]
pub struct CanvasDeriver {
    pub db: SurrealProcessor,
    pub secrets: SecretKey,
    pub config: OrchestrationConfig,
    /// A derivation pass publishes no AMQP event, but it does move every
    /// server's rollout state, which is what the dashboard's rollout views and
    /// the node-health streams follow.
    pub notifier: Notifier,
}

/// Derives one canvas. The typed input the AMQP consumer and the sweeper share.
pub struct DeriveCanvas {
    pub canvas: CanvasId,
}

impl AmqpMessageProcessor<CanvasDirty> for CanvasDeriver {
    const QUEUE: &'static str = "guru_orchestration_canvas_dirty";
}

impl Processor<CanvasDirty> for CanvasDeriver {
    type Output = ();
    type Error = wakuwaku::Error;
    #[tracing::instrument(name = "Hook:CanvasDirty", skip_all, err, fields(canvas = %input.canvas))]
    async fn process(&self, input: CanvasDirty) -> Result<Self::Output, Self::Error> {
        <Self as Processor<DeriveCanvas>>::process(
            self,
            DeriveCanvas {
                canvas: ids::canvas_id(&input.canvas),
            },
        )
        .await
    }
}

impl AmqpMessageProcessor<DeriveStaleCanvasesSignal> for CanvasDeriver {
    const QUEUE: &'static str = "guru_orchestration_derive_stale_canvases";
}

impl Processor<DeriveStaleCanvasesSignal> for CanvasDeriver {
    type Output = ();
    type Error = wakuwaku::Error;
    #[tracing::instrument(name = "Hook:DeriveStaleCanvasesSignal", skip_all, err)]
    async fn process(&self, input: DeriveStaleCanvasesSignal) -> Result<Self::Output, Self::Error> {
        if !self
            .db
            .process(ClaimJobRun::for_tick(
                "derive_stale_canvases",
                self.config.sweep_interval(),
                input.tick_time(),
            ))
            .await?
        {
            return Ok(());
        }
        sweep_stale_canvases(self).await
    }
}

impl AmqpMessageProcessor<RotateRelayCertificatesSignal> for CanvasDeriver {
    const QUEUE: &'static str = "guru_orchestration_rotate_relay_certificates";
}

impl Processor<RotateRelayCertificatesSignal> for CanvasDeriver {
    type Output = ();
    type Error = wakuwaku::Error;
    #[tracing::instrument(name = "Hook:RotateRelayCertificatesSignal", skip_all, err)]
    async fn process(
        &self,
        input: RotateRelayCertificatesSignal,
    ) -> Result<Self::Output, Self::Error> {
        if !self
            .db
            .process(ClaimJobRun::for_tick(
                "rotate_relay_certificates",
                self.config.relay_rotation_interval(),
                input.tick_time(),
            ))
            .await?
        {
            return Ok(());
        }
        rotate_expiring_relay_certificates(self).await
    }
}

impl Processor<DeriveCanvas> for CanvasDeriver {
    type Output = ();
    type Error = wakuwaku::Error;
    #[tracing::instrument(name = "Hook:DeriveCanvas", skip_all, err, fields(canvas = ?input.canvas))]
    async fn process(&self, input: DeriveCanvas) -> Result<Self::Output, Self::Error> {
        for _ in 0..MAX_ATTEMPTS {
            let Some(state) = self
                .db
                .process(LoadCanvasDerivationInput {
                    canvas: input.canvas.clone(),
                })
                .await?
            else {
                // The canvas was deleted; its servers keep their last config.
                return Ok(());
            };
            if state.generation == state.derived_generation {
                return Ok(());
            }
            let certificates = self.certificates(&state.topology).await?;

            let views_by_server: HashMap<String, &ServerConfigViewEntity> = state
                .views
                .iter()
                .map(|view| (record_key(&view.server.0), view))
                .collect();

            let now = Utc::now();
            let mut updates = Vec::with_capacity(state.topology.servers.len());
            let mut deploying = Vec::new();
            for server in &state.topology.servers {
                let Some(view) = views_by_server.get(&record_key(&server.id.0)) else {
                    tracing::error!(
                        server = %server.name,
                        "server has no config view row; skipping it"
                    );
                    continue;
                };
                let update = derive_one(
                    server.id.clone(),
                    view,
                    &state.topology,
                    &state.views,
                    &certificates,
                    &self.config,
                    now,
                );
                if let Some(desired) = &update.desired {
                    deploying_records(view.desired.as_ref(), desired, now, &mut deploying);
                }
                updates.push(update);
            }

            if self
                .db
                .process(CommitCanvasDerivation {
                    canvas: state.root.clone(),
                    generation: state.generation,
                    updates,
                })
                .await?
            {
                // Status flips the moment a revision is published, not when the
                // worker's next report happens to mention it.
                let rows = self
                    .db
                    .process(InsertNodeHealthRecords { records: deploying })
                    .await?;
                self.notifier
                    .rollout_changed(RolloutScope::Canvas(record_key(&state.root.0)))
                    .await;
                if !rows.is_empty() {
                    self.notifier
                        .live(LiveMessage::NodeHealth {
                            records: rows.iter().map(Into::into).collect(),
                        })
                        .await;
                }
                return Ok(());
            }
            // The tree moved under us: derive the newer state right away rather
            // than waiting for its own message.
        }
        tracing::warn!(canvas = ?input.canvas, "derivation kept losing the generation race");
        Ok(())
    }
}

impl CanvasDeriver {
    fn ca(&self) -> CaService {
        CaService {
            db: self.db.clone(),
            secrets: self.secrets.clone(),
            config: self.config.clone(),
        }
    }

    /// The certificate state one tree derives against, after issuing the relay
    /// leaves it is missing. An issuance failure is logged, not propagated: the
    /// affected pods are reported invalid by the derivation and retried on the
    /// next pass.
    async fn certificates(
        &self,
        topology: &CanvasTopology,
    ) -> Result<DerivationCertificates, wakuwaku::Error> {
        let ca_present = self.db.process(FindInternalCa).await?.is_some();
        let relay_pods = relay_tls_pods(topology);
        if ca_present
            && !relay_pods.is_empty()
            && let Err(e) = self
                .ca()
                .process(EnsureRelayCertificates {
                    pods: relay_pods.clone(),
                })
                .await
        {
            tracing::error!(error = %e, "issuing relay certificates failed; their pods stay invalid");
        }
        let acme = self
            .db
            .process(ListCertificatesBySnis {
                snis: tls_snis(topology),
            })
            .await?;
        let relay = self
            .db
            .process(ListRelayCertificatesByPods { pods: relay_pods })
            .await?;
        Ok(DerivationCertificates {
            acme,
            relay,
            ca_present,
            assume_issued: false,
        })
    }
}

/// One server's slot in a derivation pass.
///
/// A pod that cannot be derived is reported in `invalid_pods` and costs only its
/// own forwarding; `derive_error` is reserved for a failure of the whole server,
/// which now means only a cross-pod one (two pods claiming a socket) or a broken
/// stored snapshot.
#[allow(clippy::too_many_arguments)]
fn derive_one(
    server: ServerId,
    view: &ServerConfigViewEntity,
    topology: &CanvasTopology,
    views: &[ServerConfigViewEntity],
    certificates: &DerivationCertificates,
    config: &OrchestrationConfig,
    now: DateTime<Utc>,
) -> ViewUpdate {
    let failed = |error: String| ViewUpdate {
        server: server.clone(),
        desired: None,
        derive_error: Some(error),
        invalid_pods: Vec::new(),
        waiting_for: Vec::new(),
        clear_failure: false,
    };

    let ideal = match derive_server_config(topology, &server, certificates, config) {
        Ok(ideal) => ideal,
        Err(e) => return failed(e.to_string()),
    };
    let converged = match converge(ideal, view, views) {
        Ok(converged) => converged,
        Err(e) => return failed(e.to_string()),
    };
    if let Err(e) = converged.config.validate() {
        return failed(e.to_string());
    }
    let toml = match converged.config.to_toml_string() {
        Ok(toml) => toml,
        Err(e) => return failed(e.to_string()),
    };

    // Byte-identical TOML pinning the same material: no new revision, so the
    // worker is never restarted for an edit that does not concern it. A renewal
    // changes only the pinned versions, and that alone is a new revision. The
    // pod report still has to land either way: a pod may have broken (or been
    // fixed) without changing the served config.
    let unchanged = view
        .desired
        .as_ref()
        .is_some_and(|s| s.toml == toml && s.certificates == converged.certificates);
    if unchanged {
        return ViewUpdate {
            server,
            desired: None,
            derive_error: None,
            invalid_pods: converged.invalid,
            waiting_for: converged.waiting_for,
            clear_failure: false,
        };
    }
    let revision = view
        .desired
        .as_ref()
        .map(|s| s.revision)
        .unwrap_or(0)
        .saturating_add(1);
    ViewUpdate {
        server,
        desired: Some(ConfigSnapshot {
            revision,
            toml,
            created_at: now,
            forwardings: converged.forwardings,
            certificates: converged.certificates,
        }),
        derive_error: None,
        invalid_pods: converged.invalid,
        waiting_for: converged.waiting_for,
        clear_failure: true,
    }
}

/// `Deploying` records for every pod whose `[[forwarding]]` entry in `next`
/// differs from (or is absent in) `previous`, and for every node that entry was
/// derived through. A renewal of pinned material counts too: the entry's bytes
/// are the same, but the worker still has to swap the files.
fn deploying_records(
    previous: Option<&ConfigSnapshot>,
    next: &ConfigSnapshot,
    now: DateTime<Utc>,
    out: &mut Vec<NewNodeHealthRecord>,
) {
    let Ok(next_config) = Config::from_toml_str(&next.toml) else {
        return;
    };
    let old_entries: HashMap<String, (Forwarding, &[CertificateRef])> = previous
        .and_then(|snapshot| {
            let config = Config::from_toml_str(&snapshot.toml).ok()?;
            Some(
                config
                    .forwardings
                    .into_iter()
                    .zip(&snapshot.forwardings)
                    .map(|(f, deps)| (record_key(&deps.pod.0), (f, deps.certificates.as_slice())))
                    .collect(),
            )
        })
        .unwrap_or_default();
    for (forwarding, deps) in next_config.forwardings.iter().zip(&next.forwardings) {
        let same = old_entries
            .get(&record_key(&deps.pod.0))
            .is_some_and(|(old, refs)| old == forwarding && *refs == deps.certificates);
        if same {
            continue;
        }
        let message = format!("revision {} published", next.revision);
        for node in std::iter::once(&deps.pod).chain(&deps.nodes) {
            out.push(NewNodeHealthRecord {
                node: node.clone(),
                status: NodeHealthStatus::Deploying,
                message: message.clone(),
                report_time: now,
            });
        }
    }
}

/// One sweep pass: re-derives every canvas whose edits outran its derivation.
///
/// This is what makes the AMQP path optional: correctness lives in the
/// generation counters, [`CanvasDirty`] only shortens the delay. A canvas that
/// fails is logged and the pass continues; only a failure to list is returned,
/// because then there is nothing to sweep.
pub async fn sweep_stale_canvases(deriver: &CanvasDeriver) -> Result<(), wakuwaku::Error> {
    let canvases = deriver.db.process(ListStaleCanvases).await?;
    for canvas in canvases {
        if let Err(e) = deriver.process(DeriveCanvas { canvas }).await {
            tracing::error!(error = %e, "sweeping a canvas failed");
        }
    }
    Ok(())
}

/// One rotation pass: re-issues relay leaves expiring within
/// `relay_cert_renew_before` and re-derives the canvases of their pods, so the
/// rotated material ships as a new revision. Callable directly by tests.
///
/// A duplicate [`RotateRelayCertificatesSignal`] delivery cannot double-rotate a
/// leaf: the write is conditional on the `version` the row was read at, so the
/// second pass sees `None` and leaves the pod alone.
pub async fn rotate_expiring_relay_certificates(
    deriver: &CanvasDeriver,
) -> Result<(), wakuwaku::Error> {
    let before = Utc::now()
        .checked_add_signed(
            chrono::Duration::from_std(deriver.config.relay_cert_renew_before())
                .unwrap_or(chrono::TimeDelta::MAX),
        )
        .unwrap_or(DateTime::<Utc>::MAX_UTC);
    let expiring = deriver
        .db
        .process(ListRelayCertificatesExpiringBefore { before })
        .await?;
    if expiring.is_empty() {
        return Ok(());
    }
    let ca = deriver.ca();
    let mut rotated: Vec<NodeId> = Vec::with_capacity(expiring.len());
    for leaf in expiring {
        match ca
            .process(RotateRelayCertificate {
                pod: leaf.pod.clone(),
                expected_version: leaf.version,
            })
            .await
        {
            Ok(Some(_)) => rotated.push(leaf.pod),
            Ok(None) => {
                tracing::debug!(pod = %record_key(&leaf.pod.0), "relay leaf already rotated by another consumer")
            }
            Err(e) => {
                tracing::error!(error = %e, pod = %record_key(&leaf.pod.0), "rotating a relay leaf failed")
            }
        }
    }
    let canvases = deriver
        .db
        .process(ListCanvasesOfNodes { nodes: rotated })
        .await?;
    deriver
        .db
        .process(TouchCanvases {
            canvases: canvases.clone(),
        })
        .await?;
    for canvas in canvases {
        if let Err(e) = deriver.process(DeriveCanvas { canvas }).await {
            tracing::error!(error = %e, "re-deriving a canvas after rotation failed");
        }
    }
    Ok(())
}
