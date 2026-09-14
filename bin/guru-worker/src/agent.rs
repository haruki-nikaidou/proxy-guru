//! Control-plane agent: registers with `guru-master`, streams config revisions and
//! acknowledges each one.
//!
//! The dynamic refresh key handed out by `Register` lives in memory only, so a worker
//! restart always produces a fresh registration and the master can tell it was down.

use crate::BoxError;
use crate::state::{self, LastKnownGood};
use crate::supervisor::Supervisor;
use rpguru_sdk::orchestration_agent::{
    AckConfigRequest, RegisterRequest, WatchConfigRequest, worker_agent_client::WorkerAgentClient,
};
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::time::Duration;
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;

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

/// One connection: register, watch, apply, ack. Returns `Ok(())` only on shutdown.
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
    let refresh_key = client.register(register).await?.into_inner().refresh_key;
    tracing::info!(
        server = %opts.server_id,
        running_revision,
        "registered with master"
    );

    let mut watch = tonic::Request::new(WatchConfigRequest {});
    watch
        .metadata_mut()
        .insert("x-refresh-key", refresh_key.parse()?);
    let mut stream = client.watch_config(watch).await?.into_inner();

    loop {
        let message = tokio::select! {
            _ = shutdown.cancelled() => return Ok(()),
            message = stream.message() => message?,
        };
        let Some(revision) = message else {
            return Err("config stream ended".into());
        };

        let error = match guru_worker_config::Config::from_toml_str(&revision.toml) {
            Err(e) => Some(format!("config: {e}")),
            Ok(cfg) => {
                for warning in cfg.lint() {
                    tracing::warn!(revision = revision.revision, warning = %warning, "config lint");
                }
                match sup.lock().await.apply(&cfg).await {
                    Err(e) => Some(e.to_string()),
                    Ok(()) => {
                        opts.applied_revision
                            .store(revision.revision, Ordering::Relaxed);
                        // Persisting last-known-good is a restart optimisation, not part
                        // of the apply contract: the revision is already carrying traffic
                        // and the master must still be told, so a failure here is logged.
                        if let Err(e) = state::store(
                            &opts.state_dir,
                            &LastKnownGood {
                                revision: revision.revision,
                                toml: revision.toml.clone(),
                            },
                        ) {
                            tracing::error!(
                                revision = revision.revision,
                                error = %e,
                                "could not persist last-known-good config"
                            );
                        }
                        tracing::info!(revision = revision.revision, "applied config revision");
                        None
                    }
                }
            }
        };
        if let Some(error) = &error {
            tracing::error!(
                revision = revision.revision,
                error,
                "config revision rejected"
            );
        }

        let applied = error.is_none();
        let mut ack = tonic::Request::new(AckConfigRequest {
            revision: revision.revision,
            error,
        });
        ack.metadata_mut()
            .insert("x-refresh-key", refresh_key.parse()?);
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
