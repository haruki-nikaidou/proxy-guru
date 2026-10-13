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
//! [`CanvasDirtyHandler`] consumes the hint from Iggy (`--mode consumer`); the
//! two periodic passes are `CronJob`s ([`DeriveStaleCanvasesJob`],
//! [`RotateRelayCertificatesJob`]) run by `--mode cron`, each claiming its run
//! so it happens at most once per interval fleet-wide. The generation counters
//! still carry correctness, so a lost hint or a late run costs latency, never a
//! wrong config.
//!
//! Certificates are part of the input: a pass first issues the relay leaves its
//! tree needs (when the internal CA exists), then derives against the ACME rows,
//! relay leaves and CA it can see. A pod whose material is missing is reported
//! invalid, never a hard failure. [`rotate_expiring_relay_certificates`]
//! re-issues leaves that are about to expire and re-derives their canvases so
//! the new material ships as a new revision.

use crate::config::OrchestrationConfig;
use crate::entities::db::ca::{
    FindInternalCa, ListRelayCertificatesByPods, ListRelayCertificatesExpiringBefore,
};
use crate::entities::db::canvas::CanvasId;
use crate::entities::db::certificate::{ListCertificatesBySnis, TouchCanvases, TouchCanvasesFlow};
use crate::entities::db::graph::GraphRows;
use crate::entities::db::health::{InsertPodHealthRecords, NewPodHealthRecord, PodHealthStatus};
use crate::entities::db::job_run::ClaimJobRun;
use crate::entities::db::pod::{ListCanvasesOfPods, PodId};
use crate::entities::db::server::ServerId;
use crate::entities::db::view::{
    CertificateRef, CommitCanvasDerivation, CommitCanvasDerivationFlow, ConfigSnapshot,
    ListStaleCanvases, LoadCanvasDerivationInput, LoadCanvasDerivationInputFlow,
    ServerConfigViewEntity, ViewUpdate,
};
use crate::events::live::{LiveMessage, RolloutScope};
use crate::events::{CanvasDirty, HealthFact};
use crate::services::OrchestrationError;
use crate::services::ca::{CaService, EnsureRelayCertificates, RotateRelayCertificate};
use crate::services::converge::converge;
use crate::services::derive::{
    DerivationCertificates, DeriveError, DerivedConfig, derive_tree, relay_tls_pods, tls_snis,
};
use crate::services::notify::Notifier;
use crate::utils::ids;
use crate::utils::secret::SecretKey;
use base::db::Db;
use guru_worker_config::{Config, Forwarding};
use kanau::processor::Processor;
use std::collections::HashMap;
use std::num::NonZeroUsize;
use std::time::Duration;
use time::{OffsetDateTime, PrimitiveDateTime};
use wakuwaku::entity::sqlx::transaction::{Transaction, TxOutput};
use wakuwaku::iggy::EventHandler;
use wakuwaku::iggy::error::HandleError;
use wakuwaku::{CronJob, CronJobInterval, CronTick, ServiceCreation};

/// How many times one pass retries after losing the generation race before it
/// leaves the canvas to the next message or sweep tick.
const MAX_ATTEMPTS: usize = 8;

#[derive(Clone)]
pub struct CanvasDeriver {
    pub db: Db,
    pub secrets: SecretKey,
    pub config: OrchestrationConfig,
    /// A derivation pass publishes no Iggy event, but it does move every
    /// server's rollout state, which is what the dashboard's rollout views
    /// follow.
    pub notifier: Notifier,
}

/// Derives one canvas. The typed input the Iggy consumer and the sweeper share.
pub struct DeriveCanvas {
    pub canvas: CanvasId,
}

/// Consumes [`CanvasDirty`] from Iggy (`--mode consumer`, consumer
/// `guru_orchestration_canvas_dirty`): derives the canvas each hint names.
pub struct CanvasDirtyHandler {
    pub deriver: CanvasDeriver,
}

impl ServiceCreation for CanvasDirtyHandler {
    type Dep = CanvasDeriver;
    fn create_service(deriver: CanvasDeriver) -> Self {
        Self { deriver }
    }
}

impl EventHandler<CanvasDirty> for CanvasDirtyHandler {
    const MAX_BATCH: NonZeroUsize = NonZeroUsize::MIN;

    #[tracing::instrument(name = "Hook:CanvasDirty", skip_all, err)]
    async fn handle(&self, run: &[CanvasDirty]) -> Result<(), HandleError> {
        for event in run {
            self.deriver
                .process(DeriveCanvas {
                    canvas: ids::canvas_id(&event.canvas),
                })
                .await
                .map_err(OrchestrationError::into_handle_error)?;
        }
        Ok(())
    }
}

impl Processor<CanvasDirty> for CanvasDeriver {
    type Output = ();
    type Error = OrchestrationError;
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

/// Derives every canvas whose edits outran their derivation, at most once per
/// `sweep_interval` fleet-wide. The backstop for a lost [`CanvasDirty`].
pub struct DeriveStaleCanvasesJob {
    pub deriver: CanvasDeriver,
}

impl ServiceCreation for DeriveStaleCanvasesJob {
    type Dep = CanvasDeriver;
    fn create_service(deriver: CanvasDeriver) -> Self {
        Self { deriver }
    }
}

impl CronJob for DeriveStaleCanvasesJob {
    const INTERVAL: CronJobInterval = CronJobInterval::AtLeast(Duration::from_secs(30));
}

impl Processor<CronTick> for DeriveStaleCanvasesJob {
    type Output = ();
    type Error = OrchestrationError;
    #[tracing::instrument(name = "Hook:DeriveStaleCanvasesJob", skip_all, err)]
    async fn process(&self, tick: CronTick) -> Result<Self::Output, Self::Error> {
        if !self
            .deriver
            .db
            .process(ClaimJobRun::for_tick(
                "derive_stale_canvases",
                self.deriver.config.sweep_interval(),
                OffsetDateTime::from(tick.fired_at),
            ))
            .await?
        {
            return Ok(());
        }
        sweep_stale_canvases(&self.deriver).await
    }
}

/// Re-issues relay leaves that are about to expire and re-derives their
/// canvases, at most once per `relay_rotation_interval` fleet-wide.
pub struct RotateRelayCertificatesJob {
    pub deriver: CanvasDeriver,
}

impl ServiceCreation for RotateRelayCertificatesJob {
    type Dep = CanvasDeriver;
    fn create_service(deriver: CanvasDeriver) -> Self {
        Self { deriver }
    }
}

impl CronJob for RotateRelayCertificatesJob {
    const INTERVAL: CronJobInterval = CronJobInterval::AtLeast(Duration::from_secs(3600));
}

impl Processor<CronTick> for RotateRelayCertificatesJob {
    type Output = ();
    type Error = OrchestrationError;
    #[tracing::instrument(name = "Hook:RotateRelayCertificatesJob", skip_all, err)]
    async fn process(&self, tick: CronTick) -> Result<Self::Output, Self::Error> {
        if !self
            .deriver
            .db
            .process(ClaimJobRun::for_tick(
                "rotate_relay_certificates",
                self.deriver.config.relay_rotation_interval(),
                OffsetDateTime::from(tick.fired_at),
            ))
            .await?
        {
            return Ok(());
        }
        rotate_expiring_relay_certificates(&self.deriver).await
    }
}

impl Processor<DeriveCanvas> for CanvasDeriver {
    type Output = ();
    type Error = OrchestrationError;
    #[tracing::instrument(name = "Hook:DeriveCanvas", skip_all, err, fields(canvas = ?input.canvas))]
    async fn process(&self, input: DeriveCanvas) -> Result<Self::Output, Self::Error> {
        for _ in 0..MAX_ATTEMPTS {
            let flow = LoadCanvasDerivationInputFlow::new(LoadCanvasDerivationInput {
                canvas: input.canvas.clone(),
            });
            let state = match self.db.process(Transaction(flow)).await? {
                TxOutput::Committed(state) => state,
                TxOutput::RolledBack(error) => return Err(error.into()),
            };
            let Some(state) = state else {
                // The canvas was deleted; its servers keep their last config.
                return Ok(());
            };
            if state.generation == state.derived_generation
                && state.view_seq == state.derived_view_seq
            {
                return Ok(());
            }
            let certificates = self.certificates(&state.graph).await?;

            let views_by_server: HashMap<&ServerId, &ServerConfigViewEntity> = state
                .views
                .iter()
                .map(|view| (&view.server, view))
                .collect();

            let mut derived = derive_tree(&state.graph, &certificates, &self.config);
            let now = OffsetDateTime::now_utc();
            let mut updates = Vec::with_capacity(state.graph.servers.len());
            let mut deploying = Vec::new();
            for server in &state.graph.servers {
                let Some(view) = views_by_server.get(&server.id) else {
                    tracing::error!(
                        server = %server.name,
                        "server has no config view row; skipping it"
                    );
                    continue;
                };
                // A graph that does not check fails every server of the tree alike.
                let ideal = match &mut derived {
                    Ok(servers) => servers
                        .remove(&server.id)
                        .unwrap_or_else(|| {
                            Err(DeriveError::UnknownServer {
                                server: server.id.to_string(),
                            })
                        })
                        .map_err(|e| e.to_string()),
                    Err(error) => Err(error.to_string()),
                };
                let update = derive_one(server.id.clone(), view, ideal, &state.views, now);
                if let Some(desired) = &update.desired {
                    deploying_records(view.desired.as_ref(), desired, now, &mut deploying);
                }
                updates.push(update);
            }

            let flow = CommitCanvasDerivationFlow::new(CommitCanvasDerivation {
                canvas: state.root.clone(),
                generation: state.generation,
                view_seq: state.view_seq,
                updates,
            });
            let committed = match self.db.process(Transaction(flow)).await? {
                TxOutput::Committed(committed) => committed,
                TxOutput::RolledBack(error) => return Err(error.into()),
            };
            if committed {
                // Status flips the moment a revision is published, not when the
                // worker's next report happens to mention it.
                let rows = self
                    .db
                    .process(InsertPodHealthRecords { records: deploying })
                    .await?;
                self.notifier
                    .rollout_changed(RolloutScope::Canvas(state.root.to_string()))
                    .await;
                if !rows.is_empty() {
                    self.notifier
                        .live(LiveMessage::PodHealth {
                            records: rows.iter().map(|p| (&p.record).into()).collect(),
                        })
                        .await;
                    self.notifier
                        .health_changed(rows.iter().map(HealthFact::pod).collect())
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
        graph: &GraphRows,
    ) -> Result<DerivationCertificates, OrchestrationError> {
        let ca_present = self.db.process(FindInternalCa).await?.is_some();
        let relay_pods = relay_tls_pods(graph);
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
                snis: tls_snis(graph),
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
/// A pod that cannot be compiled is reported in `invalid_pods` and costs only its
/// own forwarding; `derive_error` is reserved for a failure of the whole server:
/// a graph that does not check, or a broken stored snapshot.
fn derive_one(
    server: ServerId,
    view: &ServerConfigViewEntity,
    ideal: Result<DerivedConfig, String>,
    views: &[ServerConfigViewEntity],
    now: OffsetDateTime,
) -> ViewUpdate {
    let failed = |error: String| ViewUpdate {
        server: server.clone(),
        desired: None,
        derive_error: Some(error),
        invalid_pods: Vec::new(),
        waiting_for: Vec::new(),
        clear_failure: false,
    };

    let ideal = match ideal {
        Ok(ideal) => ideal,
        Err(error) => return failed(error),
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
/// differs from (or is absent in) `previous`. A renewal of pinned material
/// counts too: the entry's bytes are the same, but the worker still has to swap
/// the files.
fn deploying_records(
    previous: Option<&ConfigSnapshot>,
    next: &ConfigSnapshot,
    now: OffsetDateTime,
    out: &mut Vec<NewPodHealthRecord>,
) {
    let Ok(next_config) = Config::from_toml_str(&next.toml) else {
        return;
    };
    let old_entries: HashMap<&PodId, (Forwarding, &[CertificateRef])> = previous
        .and_then(|snapshot| {
            let config = Config::from_toml_str(&snapshot.toml).ok()?;
            Some(
                config
                    .forwardings
                    .into_iter()
                    .zip(&snapshot.forwardings)
                    .map(|(f, deps)| (&deps.pod, (f, deps.certificates.as_slice())))
                    .collect(),
            )
        })
        .unwrap_or_default();
    for (forwarding, deps) in next_config.forwardings.iter().zip(&next.forwardings) {
        let same = old_entries
            .get(&deps.pod)
            .is_some_and(|(old, refs)| old == forwarding && *refs == deps.certificates);
        if same {
            continue;
        }
        if out.iter().any(|record| record.pod == deps.pod) {
            continue;
        }
        out.push(NewPodHealthRecord {
            pod: deps.pod.clone(),
            status: PodHealthStatus::Deploying,
            message: format!("revision {} published", next.revision),
            report_time: now,
        });
    }
}

/// One sweep pass: re-derives every canvas whose edits outran its derivation.
///
/// This is what makes the Iggy path optional: correctness lives in the
/// generation counters, [`CanvasDirty`] only shortens the delay. A canvas that
/// fails is logged and the pass continues; only a failure to list is returned,
/// because then there is nothing to sweep.
pub async fn sweep_stale_canvases(deriver: &CanvasDeriver) -> Result<(), OrchestrationError> {
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
/// A duplicate [`RotateRelayCertificatesJob`] run cannot double-rotate a
/// leaf: the write is conditional on the `version` the row was read at, so the
/// second pass sees `None` and leaves the pod alone.
pub async fn rotate_expiring_relay_certificates(
    deriver: &CanvasDeriver,
) -> Result<(), OrchestrationError> {
    let before = OffsetDateTime::now_utc()
        .checked_add(
            time::Duration::try_from(deriver.config.relay_cert_renew_before())
                .unwrap_or(time::Duration::MAX),
        )
        .unwrap_or(PrimitiveDateTime::MAX.assume_utc());
    let expiring = deriver
        .db
        .process(ListRelayCertificatesExpiringBefore { before })
        .await?;
    if expiring.is_empty() {
        return Ok(());
    }
    let ca = deriver.ca();
    let mut rotated: Vec<PodId> = Vec::with_capacity(expiring.len());
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
                tracing::debug!(pod = %leaf.pod.to_string(), "relay leaf already rotated by another consumer")
            }
            Err(e) => {
                tracing::error!(error = %e, pod = %leaf.pod.to_string(), "rotating a relay leaf failed")
            }
        }
    }
    let canvases = deriver
        .db
        .process(ListCanvasesOfPods { pods: rotated })
        .await?;
    if let Some(flow) = TouchCanvasesFlow::new(TouchCanvases {
        canvases: canvases.clone(),
    }) && let TxOutput::RolledBack(error) = deriver.db.process(Transaction(flow)).await?
    {
        return Err(error.into());
    }
    for canvas in canvases {
        if let Err(e) = deriver.process(DeriveCanvas { canvas }).await {
            tracing::error!(error = %e, "re-deriving a canvas after rotation failed");
        }
    }
    Ok(())
}
