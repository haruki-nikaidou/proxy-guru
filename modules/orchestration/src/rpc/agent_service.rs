//! The `WorkerAgent` gRPC service: registration, config streaming,
//! acknowledgement and health reporting.

use crate::entities::surreal::server::{
    ClaimServerWatchSession, FindServerById, ReleaseServerWatchSession, RenewServerWatchSession,
    ReportedAddresses, ServerEntity, ServerId,
};
use crate::entities::surreal::view::TakeInFlight;
use crate::rpc::agent_middleware::{agent_from_request, peer_address};
use crate::services::agent::{AckConfig, AgentService, PodResult, RegisterWorker};
use crate::services::ca::{BundleCertificates, CaService};
use crate::services::health::{
    HealthReportInput, HealthService, MarkServerOffline, RecordHealthReport,
};
use crate::services::watch::{AgentSignal, SessionLease, WatchFence, WatchHub};
use crate::utils::ids;
use guru_worker_config::Config;
use kanau::processor::Processor;
use rpguru_sdk::orchestration_agent as pb;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status};
use wakuwaku::surreal::SurrealProcessor;

const STREAM_CAPACITY: usize = 4;

#[derive(Clone)]
pub struct WorkerAgentGrpc {
    pub agents: AgentService,
    pub health: HealthService,
    pub ca: CaService,
    pub db: SurrealProcessor,
    pub hub: WatchHub,
    pub lease: SessionLease,
}

/// Empty strings on the wire mean "unknown".
fn reported_from_proto(reported: pb::ReportedAddresses) -> ReportedAddresses {
    let non_empty = |s: String| (!s.is_empty()).then_some(s);
    ReportedAddresses {
        public_v4: non_empty(reported.public_v4),
        public_v6: non_empty(reported.public_v6),
        interfaces: reported.interfaces,
        country: non_empty(reported.country),
        reported_at: chrono::Utc::now(),
    }
}

fn pod_result(pod: pb::PodStatus) -> PodResult {
    PodResult {
        tag: pod.tag,
        error: pod.error,
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
        let actor = auth::rpc::middleware::from_request(&request)?;
        let observed = peer_address(&request, self.health.config.trust_proxy_address_headers);
        let input = request.into_inner();
        let refresh_key = self
            .agents
            .process(RegisterWorker {
                actor,
                server_id: ids::server_id(&input.server_id),
                running_revision: input.running_revision,
                observed,
                reported: input.reported_addresses.map(reported_from_proto),
            })
            .await?;
        Ok(Response::new(pb::RegisterReply {
            refresh_key,
            health_report_interval_secs: u32::try_from(
                self.health.config.health_report_interval_secs,
            )
            .unwrap_or(u32::MAX),
        }))
    }

    type WatchConfigStream = ReceiverStream<Result<pb::ConfigRevision, Status>>;

    async fn watch_config(
        &self,
        request: Request<pb::WatchConfigRequest>,
    ) -> Result<Response<Self::WatchConfigStream>, Status> {
        let agent = agent_from_request(&request)?;
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

        let server_key = ids::record_key(&server.id.0);
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
        self.agents
            .process(AckConfig {
                agent,
                revision: input.revision,
                error: input.error,
                pods: input.pods.into_iter().map(pod_result).collect(),
            })
            .await?;
        Ok(Response::new(pb::AckConfigReply {}))
    }

    /// Records every report as it arrives; the stream ending, however it ends,
    /// is the worker going away. The Offline mark is fenced on this session's
    /// generation, so a stream outlived by a re-registration cannot clobber the
    /// successor's status.
    async fn report_health(
        &self,
        request: Request<tonic::Streaming<pb::HealthReport>>,
    ) -> Result<Response<pb::ReportHealthReply>, Status> {
        let agent = agent_from_request(&request)?;
        let mut reports = request.into_inner();
        let ended = loop {
            match reports.message().await {
                Ok(Some(report)) => {
                    let recorded = self
                        .health
                        .process(RecordHealthReport {
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
                        })
                        .await;
                    if let Err(e) = recorded {
                        break Err(Status::from(e));
                    }
                }
                Ok(None) => break Ok(()),
                Err(status) => break Err(status),
            }
        };
        if let Err(e) = self
            .health
            .process(MarkServerOffline {
                server: agent.server,
                generation: Some(agent.generation),
            })
            .await
        {
            tracing::warn!(error = %e, "marking the server offline after its health stream ended failed");
        }
        ended.map(|()| Response::new(pb::ReportHealthReply {}))
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
