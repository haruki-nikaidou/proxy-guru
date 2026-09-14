//! Control-plane agent: registers with `guru-master`, streams config revisions,
//! acknowledges each one per pod and reports health for as long as the session lives.
//!
//! The dynamic refresh key handed out by `Register` lives in memory only, so a worker
//! restart always produces a fresh registration and the master can tell it was down.

use crate::BoxError;
use crate::certs;
use crate::state::{self, LastKnownGood};
use crate::supervisor::{ApplyOutcome, Supervisor};
use rpguru_sdk::orchestration_agent::{
    AckConfigRequest, HealthReport, PodStatus, RegisterRequest, WatchConfigRequest,
    worker_agent_client::WorkerAgentClient,
};
use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::time::Duration;
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;
use tonic::metadata::Ascii;
use tonic::metadata::MetadataValue;
use tonic::transport::Channel;

pub struct AgentOptions {
    pub master: String,
    pub api_key: String,
    pub server_id: String,
    pub state_dir: PathBuf,
    /// The revision the supervisor is actually serving; `0` until an apply succeeds.
    ///
    /// This — never the state file — is what `Register` reports, so a worker whose
    /// startup replay failed is never recorded by the master as converged.
    pub applied_revision: Arc<AtomicI64>,
    /// Time between two health reports when the master's register reply does not
    /// dictate one.
    pub health_interval: Duration,
}

const BACKOFF_START: Duration = Duration::from_secs(1);
const BACKOFF_CAP: Duration = Duration::from_secs(30);
const CONNECT_TIMEOUT: Duration = Duration::from_secs(5);

/// Keeps a session with the master alive, reconnecting with capped exponential backoff.
pub async fn run(
    opts: AgentOptions,
    sup: Arc<Mutex<Supervisor>>,
    shutdown: CancellationToken,
) -> Result<(), BoxError> {
    let mut backoff = BACKOFF_START;
    loop {
        if shutdown.is_cancelled() {
            return Ok(());
        }
        // Every RPC in a session is unbounded on its own, so the whole session — not
        // just the stream — is raced against shutdown to keep SIGTERM prompt.
        let outcome = tokio::select! {
            _ = shutdown.cancelled() => return Ok(()),
            outcome = session(&opts, &sup, &shutdown, &mut backoff) => outcome,
        };
        match outcome {
            Ok(()) => return Ok(()),
            Err(e) => tracing::error!(error = %e, "agent session ended"),
        }
        tokio::select! {
            _ = shutdown.cancelled() => return Ok(()),
            _ = tokio::time::sleep(jitter(backoff)) => {}
        }
        backoff = std::cmp::min(backoff.saturating_mul(2), BACKOFF_CAP);
    }
}

/// One connection: register, watch, apply, ack, report health. Returns `Ok(())` only
/// on shutdown.
async fn session(
    opts: &AgentOptions,
    sup: &Arc<Mutex<Supervisor>>,
    shutdown: &CancellationToken,
    backoff: &mut Duration,
) -> Result<(), BoxError> {
    let endpoint = tonic::transport::Endpoint::from_shared(opts.master.clone())?
        .connect_timeout(CONNECT_TIMEOUT);
    let mut client = WorkerAgentClient::new(endpoint.connect().await?);

    let running_revision = opts.applied_revision.load(Ordering::Relaxed);
    let mut register = tonic::Request::new(RegisterRequest {
        server_id: opts.server_id.clone(),
        running_revision,
    });
    register
        .metadata_mut()
        .insert("x-api-key", opts.api_key.parse()?);
    let reply = client.register(register).await?.into_inner();
    let refresh_key: MetadataValue<Ascii> = reply.refresh_key.parse()?;
    // The master's interval wins so both sides agree on the offline threshold; the
    // `--health-interval` flag only covers a master that does not send one.
    let health_interval = match reply.health_report_interval_secs {
        0 => opts.health_interval,
        secs => Duration::from_secs(u64::from(secs)),
    };
    tracing::info!(
        server = %opts.server_id,
        running_revision,
        health_interval_secs = health_interval.as_secs(),
        "registered with master"
    );

    let mut watch = tonic::Request::new(WatchConfigRequest {});
    watch
        .metadata_mut()
        .insert("x-refresh-key", refresh_key.clone());
    let mut stream = client.watch_config(watch).await?.into_inner();

    // The health stream lives exactly as long as this session: the guard cancels it on
    // every way out of here, and its ending — the master closing it, or the session
    // key being rotated away — ends the session so the next one reconnects both.
    let health_token = CancellationToken::new();
    let _health_guard = health_token.clone().drop_guard();
    let mut health = tokio::spawn(report_health(
        client.clone(),
        refresh_key.clone(),
        health_interval,
        sup.clone(),
        opts.applied_revision.clone(),
        health_token,
    ));

    loop {
        let message = tokio::select! {
            _ = shutdown.cancelled() => return Ok(()),
            ended = &mut health => {
                return Err(match ended {
                    Ok(Ok(())) => "health stream ended".into(),
                    Ok(Err(e)) => format!("health stream: {e}").into(),
                    Err(e) => format!("health task: {e}").into(),
                });
            }
            message = stream.message() => message?,
        };
        let Some(revision) = message else {
            return Err("config stream ended".into());
        };

        let (error, pods) = match apply_revision(opts, sup, &revision).await {
            Ok(outcome) => {
                opts.applied_revision
                    .store(revision.revision, Ordering::Relaxed);
                (None, outcome.pods)
            }
            Err(error) => {
                tracing::error!(
                    revision = revision.revision,
                    error = %error,
                    "config revision rejected"
                );
                (Some(error), Vec::new())
            }
        };

        let applied = error.is_none();
        let mut ack = tonic::Request::new(AckConfigRequest {
            revision: revision.revision,
            error,
            pods: pods
                .into_iter()
                .map(|p| PodStatus {
                    tag: p.tag,
                    error: p.error,
                })
                .collect(),
        });
        ack.metadata_mut()
            .insert("x-refresh-key", refresh_key.clone());
        client.ack_config(ack).await?;
        if applied {
            // Registering proves nothing: the session is healthy only once a revision
            // has been applied and the master has taken the ack. Resetting any earlier
            // turns a failure loop into a steady 1 Hz re-registration storm, and every
            // pass rotates the refresh key on the master.
            *backoff = BACKOFF_START;
        }
    }
}

/// Puts one revision into service: its certificate files on disk first, then its
/// forwardings one by one. `Err` means nothing changed — the TOML did not parse or a
/// file could not be written; per-forwarding failures are in the outcome.
async fn apply_revision(
    opts: &AgentOptions,
    sup: &Arc<Mutex<Supervisor>>,
    revision: &rpguru_sdk::orchestration_agent::ConfigRevision,
) -> Result<ApplyOutcome, String> {
    let mut cfg = guru_worker_config::Config::from_toml_str(&revision.toml)
        .map_err(|e| format!("config: {e}"))?;
    for warning in cfg.lint() {
        tracing::warn!(revision = revision.revision, warning = %warning, "config lint");
    }
    let written = certs::write_files(&opts.state_dir, &revision.files)
        .map_err(|e| format!("certificate files: {e}"))?;
    cfg.resolve_paths(&opts.state_dir);

    let mut sup = sup.lock().await;
    let outcome = sup.apply(&cfg).await;
    let running = sup.running_config();
    drop(sup);

    let failed = outcome.failed().count();
    if failed == 0 {
        tracing::info!(revision = revision.revision, "applied config revision");
    } else {
        tracing::warn!(
            revision = revision.revision,
            failed,
            "applied config revision partially; failed pods keep their previous listener"
        );
    }
    // A rewritten certificate directory keeps its previous content only for a pod that
    // failed and still runs a listener reading from it — and not when a pod that did
    // apply reads the new content from the same place.
    let dirs_of = |keep: &dyn Fn(&str) -> bool| -> HashSet<PathBuf> {
        running
            .forwardings
            .iter()
            .filter(|f| keep(&f.tag))
            .flat_map(certs::forwarding_paths)
            .filter_map(|p| p.parent().map(Path::to_path_buf))
            .collect()
    };
    let failed_tags: HashSet<&str> = outcome.failed().map(|p| p.tag.as_str()).collect();
    let retained = dirs_of(&|tag| failed_tags.contains(tag));
    let taken = dirs_of(&|tag| !failed_tags.contains(tag));
    written.settle(|dir| retained.contains(dir) && !taken.contains(dir));
    // Persisting last-known-good is a restart optimisation, not part of the apply
    // contract: the revision is already carrying traffic and the master must still be
    // told, so a failure here is logged. What is stored is the mix actually running,
    // not the revision as sent, so a replay brings back exactly what served.
    match running.to_toml_string() {
        Ok(toml) => {
            if let Err(e) = state::store(
                &opts.state_dir,
                &LastKnownGood {
                    revision: revision.revision,
                    toml,
                },
            ) {
                tracing::error!(
                    revision = revision.revision,
                    error = %e,
                    "could not persist last-known-good config"
                );
            }
        }
        Err(e) => tracing::error!(
            revision = revision.revision,
            error = %e,
            "could not render the running config for last-known-good"
        ),
    }
    certs::prune(&opts.state_dir, &running);
    Ok(outcome)
}

/// Streams one `HealthReport` per interval until `token` is cancelled or the master
/// ends the stream. The first report goes out at once.
async fn report_health(
    mut client: WorkerAgentClient<Channel>,
    refresh_key: MetadataValue<Ascii>,
    interval: Duration,
    sup: Arc<Mutex<Supervisor>>,
    applied_revision: Arc<AtomicI64>,
    token: CancellationToken,
) -> Result<(), BoxError> {
    let (tx, rx) = tokio::sync::mpsc::channel::<HealthReport>(1);
    let mut request = tonic::Request::new(tokio_stream::wrappers::ReceiverStream::new(rx));
    request.metadata_mut().insert("x-refresh-key", refresh_key);
    let call = client.report_health(request);
    tokio::pin!(call);

    let mut ticker = tokio::time::interval(interval);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let stats = sup.lock().await.stats();
    loop {
        tokio::select! {
            _ = token.cancelled() => return Ok(()),
            reply = &mut call => {
                reply?;
                return Err("master closed the health stream".into());
            }
            _ = ticker.tick() => {
                let report = {
                    let sup = sup.lock().await;
                    let snapshot = stats.snapshot_and_reset();
                    HealthReport {
                        running_revision: applied_revision.load(Ordering::Relaxed),
                        upload_bytes: clamp(snapshot.upload_bytes),
                        download_bytes: clamp(snapshot.download_bytes),
                        current_connections: clamp(snapshot.current_connections),
                        max_connections: clamp(snapshot.max_connections),
                        pods: sup
                            .pod_statuses()
                            .into_iter()
                            .map(|p| PodStatus { tag: p.tag, error: p.error })
                            .collect(),
                    }
                };
                if tx.send(report).await.is_err() {
                    // The request stream was dropped by the call ending; the next
                    // iteration observes the reply.
                    continue;
                }
            }
        }
    }
}

fn clamp(n: u64) -> i64 {
    i64::try_from(n).unwrap_or(i64::MAX)
}

/// Spreads a reconnect delay over +/-20% so a fleet restarted together does not
/// re-register in lockstep. Cheap process-local xorshift; no dependency needed.
fn jitter(backoff: Duration) -> Duration {
    static STATE: AtomicU64 = AtomicU64::new(0);
    let mut x = STATE.load(Ordering::Relaxed);
    if x == 0 {
        x = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |d| d.as_nanos() as u64)
            | 1;
    }
    x ^= x << 13;
    x ^= x >> 7;
    x ^= x << 17;
    STATE.store(x, Ordering::Relaxed);

    let base = u64::try_from(backoff.as_millis()).unwrap_or(u64::MAX);
    let spread = base / 5;
    let width = spread.saturating_mul(2).saturating_add(1);
    let offset = x.checked_rem(width).unwrap_or(0);
    Duration::from_millis(base.saturating_sub(spread).saturating_add(offset))
}
