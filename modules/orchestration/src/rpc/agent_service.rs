//! The `WorkerAgent` gRPC service: registration, config streaming,
//! acknowledgement and health reporting.

use crate::entities::db::server::{
    ClaimServerWatchSession, FindServerById, ReleaseServerWatchSession, RenewServerWatchSession,
    ReportedAddresses, ServerEntity, ServerId,
};
use crate::entities::db::view::TakeInFlight;
use crate::events::live::RolloutScope;
use crate::rpc::agent_middleware::{agent_from_request, peer_address};
use crate::services::agent::{
    AckConfig, AgentIdentity, AgentService, PodResult, PollAgentUpdate, RegisterCredential,
    RegisterWorker,
};
use crate::services::ca::{BundleCertificates, CaService};
use crate::services::health::{
    HealthReportInput, HealthService, MarkServerOffline, RecordHealthReport,
};
use crate::services::watch::{AgentSignal, SessionLease, WatchFence, WatchHub};
use crate::utils::ids;
use base::db::Db;
use guru_worker_config::Config;
use kanau::processor::Processor;
use rpguru_sdk::orchestration_agent as pb;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status};

const STREAM_CAPACITY: usize = 4;
/// The bound on one unary handler's service call. The database client can leave a
/// request pending forever (its socket reconnected underneath it); a worker
/// waiting on such a call must get an answer it can retry on, not silence.
const UNARY_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(15);

/// One service call, bounded: `UNAVAILABLE` names the call when it does not answer
/// in time, which the worker treats like any failed call.
async fn bounded<T>(
    what: &str,
    call: impl std::future::Future<Output = Result<T, crate::services::OrchestrationError>>,
) -> Result<T, Status> {
    match tokio::time::timeout(UNARY_TIMEOUT, call).await {
        Ok(result) => Ok(result?),
        Err(_) => {
            tracing::warn!(call = what, "service call timed out; answering UNAVAILABLE");
            Err(Status::unavailable(format!("{what} timed out")))
        }
    }
}

#[derive(Clone)]
pub struct WorkerAgentGrpc {
    pub agents: AgentService,
    pub health: HealthService,
    pub ca: CaService,
    pub db: Db,
    pub hub: WatchHub,
    pub lease: SessionLease,
}

/// Empty strings on the wire mean "unknown".
fn non_empty(s: String) -> Option<String> {
    (!s.is_empty()).then_some(s)
}

fn reported_from_proto(reported: pb::ReportedAddresses) -> ReportedAddresses {
    ReportedAddresses {
        public_v4: non_empty(reported.public_v4),
        public_v6: non_empty(reported.public_v6),
        interfaces: reported.interfaces,
        reported_at: chrono::Utc::now(),
    }
}

fn pod_result(pod: pb::PodStatus) -> PodResult {
    PodResult {
        tag: pod.tag,
        error: pod.error,
    }
}

/// What `Register` was presented with: a server's own `gs_` key, which the auth
/// middleware never resolves and which is handed down raw, or an operator
/// credential the middleware did resolve — or refused, exactly as before
/// ("Missing identity"). The prefix only picks the path; the service decides.
fn register_credential<T>(request: &Request<T>) -> Result<RegisterCredential, Status> {
    let raw = request
        .metadata()
        .get(auth::rpc::middleware::API_KEY_METADATA)
        .and_then(|value| value.to_str().ok());
    match raw {
        Some(key) if auth::utils::token::is_server_agent_key(key) => {
            Ok(RegisterCredential::ServerKey(key.to_owned()))
        }
        _ => Ok(RegisterCredential::Operator(
            auth::rpc::middleware::from_request(request)?,
        )),
    }
}

impl WorkerAgentGrpc {
    async fn server_row(&self, server: &ServerId) -> Result<ServerEntity, Status> {
        self.db
            .process(FindServerById { id: server.clone() })
            .await
            .map_err(|e| Status::internal(e.to_string()))?
            .ok_or_else(|| Status::not_found("Not found"))
    }

    /// Sends what the server should run next, if the database hands it to this
    /// session.
    ///
    /// The decision is not made here: [`TakeInFlight`] promotes `desired` to
    /// `in_flight` in one conditional update that also re-checks the fence, so two
    /// streams can never be handed the same revision and a fenced-out stream is
    /// handed nothing. A database failure ends the stream rather than silently
    /// skipping a revision — the worker reconnects and starts over, and the
    /// reclaim clears `in_flight`. The same goes for a revision whose certificate
    /// material cannot be assembled.
    async fn try_send(
        &self,
        server: &ServerId,
        fence: WatchFence,
        tx: &mpsc::Sender<Result<pb::ConfigRevision, Status>>,
    ) -> Result<bool, Status> {
        let taken = self
            .db
            .process(TakeInFlight {
                server: server.clone(),
                generation: fence.generation,
                epoch: fence.epoch,
            })
            .await
            .map_err(|e| Status::internal(e.to_string()))?;
        let Some(snapshot) = taken else {
            return Ok(true);
        };
        // The one publish outside a service: promoting `desired` to `in_flight`
        // happens here, because only the stream knows a worker is listening.
        self.agents
            .notifier
            .rollout_changed(RolloutScope::Server(server.to_string()))
            .await;
        let needs_ca = Config::from_toml_str(&snapshot.toml)
            .map_err(|e| Status::internal(format!("stored revision does not parse: {e}")))?
            .relay_ca
            .is_some();
        let files = self
            .ca
            .process(BundleCertificates {
                refs: &snapshot.certificates,
                ca: needs_ca,
            })
            .await?
            .into_iter()
            .map(|file| pb::CertificateFile {
                path: file.path,
                pem: file.pem,
            })
            .collect();
        Ok(tx
            .send(Ok(pb::ConfigRevision {
                revision: snapshot.revision,
                toml: snapshot.toml,
                files,
                keep_alive: false,
            }))
            .await
            .is_ok())
    }

    /// Extends this session's lease. `false` means the fence moved on.
    async fn renew(&self, server: &ServerId, fence: WatchFence) -> Result<bool, Status> {
        let now = chrono::Utc::now();
        self.db
            .process(RenewServerWatchSession {
                server: server.clone(),
                generation: fence.generation,
                epoch: fence.epoch,
                now,
                lease_until: self.lease.until(now),
            })
            .await
            .map_err(|e| Status::internal(e.to_string()))
    }

    /// Records each report as it arrives and answers it, until the stream ends,
    /// breaks, or carries no report for the offline threshold.
    async fn record_reports(
        &self,
        agent: &AgentIdentity,
        mut reports: tonic::Streaming<pb::HealthReport>,
        tx: &mpsc::Sender<Result<pb::ReportHealthReply, Status>>,
    ) -> HealthEnd {
        let silence = self.health.config.health_offline_after();
        loop {
            let next = match tokio::time::timeout(silence, reports.message()).await {
                Ok(next) => next,
                Err(_) => {
                    return HealthEnd::Silent(Status::deadline_exceeded(format!(
                        "no health report within {}s",
                        silence.as_secs()
                    )));
                }
            };
            let report = match next {
                Ok(Some(report)) => report,
                Ok(None) => return HealthEnd::Closed,
                Err(status) => return HealthEnd::Broken(status),
            };
            // Bounded: nothing cancels this task, so a write that never returns
            // would hold the stream open forever.
            let recorded = bounded(
                "ReportHealth",
                self.health.process(RecordHealthReport {
                    agent: agent.clone(),
                    report: HealthReportInput {
                        running_revision: report.running_revision,
                        upload_bytes: report.upload_bytes,
                        download_bytes: report.download_bytes,
                        current_connections: report.current_connections,
                        max_connections: report.max_connections,
                        pods: report.pods.into_iter().map(pod_result).collect(),
                        reported: report.reported_addresses.map(reported_from_proto),
                    },
                }),
            )
            .await;
            if let Err(status) = recorded {
                return HealthEnd::Unrecorded(status);
            }
            // A full channel holds replies the worker has not read yet; one more
            // is not worth holding up the next report for.
            if let Err(mpsc::error::TrySendError::Closed(_)) =
                tx.try_send(Ok(pb::ReportHealthReply {}))
            {
                return HealthEnd::Closed;
            }
        }
    }

    /// Best effort: a lease that outlives its stream only delays the next
    /// registration until it lapses.
    async fn release(&self, server: &ServerId, fence: WatchFence) {
        if let Err(e) = self
            .db
            .process(ReleaseServerWatchSession {
                server: server.clone(),
                generation: fence.generation,
                epoch: fence.epoch,
            })
            .await
        {
            tracing::warn!(error = %e, "releasing the watch session lease failed");
        }
    }
}

#[tonic::async_trait]
impl pb::worker_agent_server::WorkerAgent for WorkerAgentGrpc {
    async fn register(
        &self,
        request: Request<pb::RegisterRequest>,
    ) -> Result<Response<pb::RegisterReply>, Status> {
        let credential = register_credential(&request)?;
        let observed = peer_address(&request, self.health.config.trust_proxy_address_headers);
        let input = request.into_inner();
        let refresh_key = bounded(
            "Register",
            self.agents.process(RegisterWorker {
                credential,
                server_id: ids::server_id(&input.server_id),
                running_revision: input.running_revision,
                observed,
                reported: input.reported_addresses.map(reported_from_proto),
                agent_version: non_empty(input.agent_version),
                agent_arch: non_empty(input.agent_arch),
                capabilities: input.capabilities,
                last_update_error: non_empty(input.last_update_error),
            }),
        )
        .await?;
        Ok(Response::new(pb::RegisterReply {
            refresh_key,
            health_report_interval_secs: u32::try_from(
                self.health.config.health_report_interval_secs,
            )
            .unwrap_or(u32::MAX),
            agent_update_poll_secs: u32::try_from(self.health.config.agent_update_poll_secs)
                .unwrap_or(u32::MAX),
            stream_keepalive_secs: u32::try_from(self.health.config.stream_keepalive().as_secs())
                .unwrap_or(u32::MAX),
        }))
    }

    type WatchConfigStream = ReceiverStream<Result<pb::ConfigRevision, Status>>;

    async fn watch_config(
        &self,
        request: Request<pb::WatchConfigRequest>,
    ) -> Result<Response<Self::WatchConfigStream>, Status> {
        let agent = agent_from_request(&request)?;
        let keep_alive = request.get_ref().keep_alive;
        // Claim the server's single watch session. The claim is conditional on the
        // generation still being current, so a request that authenticated just
        // before a registration rotated the key cannot open a stream afterwards.
        let now = chrono::Utc::now();
        let server = self
            .db
            .process(ClaimServerWatchSession {
                server: agent.server.clone(),
                generation: agent.generation,
                now,
                lease_until: self.lease.until(now),
            })
            .await
            .map_err(|e| Status::internal(e.to_string()))?
            .ok_or_else(|| Status::unauthenticated("refresh key superseded"))?;
        let fence = WatchFence {
            generation: server.refresh_key_generation,
            epoch: server.watch_epoch,
        };

        let server_key = server.id.to_string();
        let Some(subscription) = self.hub.subscribe(&server_key, fence) else {
            // Our claim already lost to a newer one; the release is a no-op unless we
            // are somehow still the row's owner.
            self.release(&server.id, fence).await;
            return Err(Status::aborted("a newer watch session took over"));
        };
        let (tx, rx) = mpsc::channel(STREAM_CAPACITY);

        let this = self.clone();
        let server_id = server.id.clone();
        // Everything past the claim runs in the task, which always releases the
        // lease on its way out: an early return here would hold the server hostage
        // for a full lease period.
        tokio::spawn(async move {
            let mut subscription = subscription;
            let mut heartbeat = tokio::time::interval(this.lease.heartbeat);
            heartbeat.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            heartbeat.tick().await; // the claim already took the lease
            // Its first tick is not skipped: the keep-alive right after the hand-over
            // attempt is what carries the response headers out, since a proxy may
            // hold bare headers until the stream's first data.
            let mut keepalive = tokio::time::interval(this.health.config.stream_keepalive());
            keepalive.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            // `Err` ends the stream: the worker reconnects with backoff and starts
            // from the row again, so a failed read is never a silently lost revision.
            let ended: Result<(), Status> = async {
                // Ask the database once up front: this session may be taking over a
                // server that already has a revision waiting for it.
                if !this.try_send(&server_id, fence, &tx).await? {
                    return Ok(());
                }
                loop {
                    let signal = tokio::select! {
                        // A worker that vanishes quietly must hand its lease back;
                        // this task is detached, so nothing else would notice.
                        _ = tx.closed() => return Ok(()),
                        _ = heartbeat.tick() => {
                            // Holding the lease is what keeps a second worker from
                            // registering; losing it means we are no longer the owner.
                            if !this.renew(&server_id, fence).await? {
                                return Err(Status::aborted("watch session lease lost"));
                            }
                            continue;
                        }
                        // A proxy cuts a stream that stays silent (Cloudflare after
                        // about two minutes); a revision may not come for days. A full
                        // channel already has data waiting, and a keep-alive must
                        // never hold up the lease renewal above.
                        _ = keepalive.tick(), if keep_alive => {
                            let _ = tx.try_send(Ok(pb::ConfigRevision {
                                keep_alive: true,
                                ..Default::default()
                            }));
                            continue;
                        }
                        signal = subscription.rx.recv() => signal,
                    };
                    match signal {
                        Ok(AgentSignal::Changed) => {
                            if !this.try_send(&server_id, fence, &tx).await? {
                                return Ok(());
                            }
                        }
                        Ok(AgentSignal::Fenced(current)) if current != fence => {
                            return Err(fenced_status(fence, current));
                        }
                        Ok(AgentSignal::Fenced(_)) => {}
                        Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {
                            // The dropped messages may have contained our own fence
                            // signal, so re-check it before trusting the stream.
                            let row = this.server_row(&server_id).await?;
                            let current = WatchFence {
                                generation: row.refresh_key_generation,
                                epoch: row.watch_epoch,
                            };
                            if current != fence {
                                return Err(fenced_status(fence, current));
                            }
                            if !this.try_send(&server_id, fence, &tx).await? {
                                return Ok(());
                            }
                        }
                        Err(tokio::sync::broadcast::error::RecvError::Closed) => return Ok(()),
                    }
                }
            }
            .await;
            // A stream that ends for any reason hands the server back immediately, so
            // a restarting worker does not have to wait the lease out.
            this.release(&server_id, fence).await;
            if let Err(status) = ended {
                let _ = tx.send(Err(status)).await;
            }
        });

        Ok(Response::new(ReceiverStream::new(rx)))
    }

    async fn ack_config(
        &self,
        request: Request<pb::AckConfigRequest>,
    ) -> Result<Response<pb::AckConfigReply>, Status> {
        let agent = agent_from_request(&request)?;
        let input = request.into_inner();
        bounded(
            "AckConfig",
            self.agents.process(AckConfig {
                agent,
                revision: input.revision,
                error: input.error,
                pods: input.pods.into_iter().map(pod_result).collect(),
            }),
        )
        .await?;
        Ok(Response::new(pb::AckConfigReply {}))
    }

    async fn poll_agent_update(
        &self,
        request: Request<pb::PollAgentUpdateRequest>,
    ) -> Result<Response<pb::PollAgentUpdateReply>, Status> {
        let agent = agent_from_request(&request)?;
        let input = request.into_inner();
        let update = bounded(
            "PollAgentUpdate",
            self.agents.process(PollAgentUpdate {
                agent,
                last_error: non_empty(input.last_error),
            }),
        )
        .await?;
        Ok(Response::new(pb::PollAgentUpdateReply {
            update: update.map(|update| pb::AgentUpdate {
                version: update.version,
                url: update.url,
                sha256: update.sha256,
            }),
        }))
    }

    type ReportHealthStream = ReceiverStream<Result<pb::ReportHealthReply, Status>>;

    /// Records every report as it arrives and answers each one, so the reply
    /// stream is never silent to a proxy and the worker can tell its reports
    /// land. The reply stream opens at once, before any report is recorded.
    ///
    /// The stream ending is not the worker going away — a proxy cuts streams for
    /// reasons of its own, and a worker back within the offline threshold never
    /// was offline — so it leaves the verdict to the liveness sweep. Silence is:
    /// a stream that carries no report for `health_offline_after()` is ended here
    /// with `DEADLINE_EXCEEDED` and marks the server `Offline`, fenced on this
    /// session's generation so a stream outlived by a re-registration cannot
    /// clobber the successor's status.
    ///
    /// The loop runs in a task of its own: hyper drops a handler whose stream the
    /// peer reset, and that must not cut a verdict or a write short.
    async fn report_health(
        &self,
        request: Request<tonic::Streaming<pb::HealthReport>>,
    ) -> Result<Response<Self::ReportHealthStream>, Status> {
        let agent = agent_from_request(&request)?;
        let reports = request.into_inner();
        let (tx, rx) = mpsc::channel(STREAM_CAPACITY);
        let this = self.clone();
        tokio::spawn(async move {
            let ended = tokio::select! {
                // The worker's end of the replies is gone: nobody is left to answer.
                _ = tx.closed() => HealthEnd::Closed,
                ended = this.record_reports(&agent, reports, &tx) => ended,
            };
            tracing::info!(
                server = %agent.server,
                generation = agent.generation,
                ended = %ended,
                "health stream ended"
            );
            if let HealthEnd::Silent(_) = ended
                && let Err(e) = this
                    .health
                    .process(MarkServerOffline {
                        server: agent.server.clone(),
                        generation: Some(agent.generation),
                    })
                    .await
            {
                tracing::warn!(error = %e, "marking a silent server offline failed");
            }
            if let Some(status) = ended.into_status() {
                let _ = tx.send(Err(status)).await;
            }
        });
        Ok(Response::new(ReceiverStream::new(rx)))
    }
}

/// How one health stream ended. Only `Silent` marks the server offline.
enum HealthEnd {
    /// The worker half-closed or reset it, or a proxy on the path did.
    Closed,
    /// The request stream broke underneath the master.
    Broken(Status),
    /// No report within the offline threshold: the master's own verdict.
    Silent(Status),
    /// A report arrived and could not be recorded.
    Unrecorded(Status),
}

impl HealthEnd {
    /// What the worker is told, if the stream did not simply close.
    fn into_status(self) -> Option<Status> {
        match self {
            Self::Closed => None,
            Self::Broken(status) | Self::Silent(status) | Self::Unrecorded(status) => Some(status),
        }
    }
}

impl std::fmt::Display for HealthEnd {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Closed => f.write_str("closed"),
            Self::Broken(status) => write!(f, "broken: {}", status.message()),
            Self::Silent(status) => write!(f, "silent: {}", status.message()),
            Self::Unrecorded(status) => write!(f, "unrecorded: {}", status.message()),
        }
    }
}

/// Why a stream lost the fence: a new registration (the worker restarted, or an
/// impostor registered) or a newer stream for the same generation.
fn fenced_status(mine: WatchFence, current: WatchFence) -> Status {
    if current.generation != mine.generation {
        Status::unauthenticated("refresh key superseded")
    } else {
        Status::aborted("a newer watch session took over")
    }
}
