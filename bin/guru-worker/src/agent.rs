//! Control-plane agent: registers with `guru-master`, streams config revisions,
//! acknowledges each one per pod and reports health for as long as the session lives.
//!
//! The dynamic refresh key handed out by `Register` lives in memory only, so a worker
//! restart always produces a fresh registration and the master can tell it was down.

use crate::BoxError;
use crate::addresses::{self, Discovered, Sources};
use crate::certs;
use crate::state::{self, LastKnownGood};
use crate::supervisor::{ApplyOutcome, Supervisor};
use crate::update::{self, UpdateOptions, UpdatePlan};
use rpguru_sdk::orchestration_agent::{
    AckConfigRequest, HealthReport, PodStatus, PollAgentUpdateRequest, RegisterRequest,
    ReportedAddresses, WatchConfigRequest, worker_agent_client::WorkerAgentClient,
};
use std::collections::HashSet;
use std::future::Future;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::time::Duration;
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;
use tonic::metadata::Ascii;
use tonic::metadata::MetadataValue;
use tonic::transport::{Channel, ClientTlsConfig, Endpoint};

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
    /// Where the worker learns its own public addresses and country.
    pub sources: Sources,
    /// Time between two update polls when the master's register reply does not
    /// dictate one.
    pub update_poll: Duration,
    /// Off: an offered update is refused and reported, never installed.
    pub self_update: bool,
    /// Signalled once an update is installed and the process should exit for
    /// systemd to start the new version.
    pub update_done: Arc<tokio::sync::Notify>,
    /// What the start guard recorded about the last update on this host, sent
    /// with the next successful registration and then forgotten.
    pub last_update_error: parking_lot::Mutex<Option<String>>,
    /// How long one unary call (`Register`, `AckConfig`, `PollAgentUpdate`) may
    /// wait for its answer; [`UNARY_TIMEOUT`] outside tests.
    pub unary_timeout: Duration,
}

/// What this build registers as; the master shows it next to the server and
/// compares it against the published release to offer an update.
pub const VERSION: &str = env!("CARGO_PKG_VERSION");

const BACKOFF_START: Duration = Duration::from_secs(1);
const BACKOFF_CAP: Duration = Duration::from_secs(30);
const CONNECT_TIMEOUT: Duration = Duration::from_secs(5);
/// HTTP/2 PING cadence towards whatever answers `--master` — the master itself or a
/// TLS-terminating proxy in front of it. A path that stops answering is torn down after
/// `KEEPALIVE_INTERVAL + KEEPALIVE_TIMEOUT` and the session reconnects, instead of a
/// silent `WatchConfig` stream hiding a dead link for hours.
const KEEPALIVE_INTERVAL: Duration = Duration::from_secs(20);
const KEEPALIVE_TIMEOUT: Duration = Duration::from_secs(10);
/// The bound on one unary call. A master (or a proxy in front of it) that takes a
/// request and never answers it must not park the caller forever: an `AckConfig`
/// that hangs would stop the config stream being read, a `PollAgentUpdate` that
/// hangs used to stop every health report. Well above any honest round trip.
pub const UNARY_TIMEOUT: Duration = Duration::from_secs(30);

/// One unary call, bounded. The error names the call so the session log says which
/// one the master swallowed.
async fn bounded<T>(
    limit: Duration,
    what: &str,
    call: impl Future<Output = Result<tonic::Response<T>, tonic::Status>>,
) -> Result<tonic::Response<T>, BoxError> {
    match tokio::time::timeout(limit, call).await {
        Ok(reply) => Ok(reply?),
        Err(_) => Err(format!("{what}: no answer within {}s", limit.as_secs()).into()),
    }
}

/// Aborts the task when dropped. A session's side tasks must die with the session:
/// a detached task stuck inside an `.await` never sees a cancellation token.
struct AbortOnDrop<T>(tokio::task::JoinHandle<T>);

impl<T> Drop for AbortOnDrop<T> {
    fn drop(&mut self) {
        self.0.abort();
    }
}

/// The channel builder for `--master`. `http://` is plaintext h2c, unchanged. `https://`
/// verifies the peer against the system roots with SNI taken from the URI host, so a
/// TLS-terminating proxy with a public certificate needs no extra flag.
fn endpoint(master: &str) -> Result<Endpoint, BoxError> {
    let endpoint = Endpoint::from_shared(master.to_owned())?
        .connect_timeout(CONNECT_TIMEOUT)
        .http2_keep_alive_interval(KEEPALIVE_INTERVAL)
        .keep_alive_timeout(KEEPALIVE_TIMEOUT)
        .keep_alive_while_idle(true);
    if endpoint.uri().scheme_str() != Some("https") {
        return Ok(endpoint);
    }
    // `with_native_roots` reads the platform store here, so a host without
    // `ca-certificates` fails with a clear error instead of at the handshake.
    Ok(endpoint.tls_config(ClientTlsConfig::new().with_native_roots())?)
}

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
    let mut client = WorkerAgentClient::new(endpoint(&opts.master)?.connect().await?);

    let running_revision = opts.applied_revision.load(Ordering::Relaxed);
    // Best effort and time-bounded: a slow or failed lookup cannot delay the
    // session past `addresses::LOOKUP_TIMEOUT`.
    let discovered = addresses::discover(&opts.sources).await;
    tracing::info!(
        public_v4 = ?discovered.public_v4,
        public_v6 = ?discovered.public_v6,
        country = ?discovered.country,
        interfaces = discovered.interfaces.len(),
        "discovered own addresses"
    );
    let mut register = tonic::Request::new(RegisterRequest {
        server_id: opts.server_id.clone(),
        running_revision,
        reported_addresses: Some(discovered.to_proto()),
        agent_version: VERSION.to_owned(),
        agent_arch: std::env::consts::ARCH.to_owned(),
        last_update_error: opts.last_update_error.lock().clone().unwrap_or_default(),
    });
    register
        .metadata_mut()
        .insert("x-api-key", opts.api_key.parse()?);
    let reply = bounded(opts.unary_timeout, "Register", client.register(register))
        .await?
        .into_inner();
    // Delivered; a registration that failed keeps it for the next attempt.
    opts.last_update_error.lock().take();
    let refresh_key: MetadataValue<Ascii> = reply.refresh_key.parse()?;
    // The master's interval wins so both sides agree on the offline threshold; the
    // `--health-interval` flag only covers a master that does not send one.
    let health_interval = match reply.health_report_interval_secs {
        0 => opts.health_interval,
        secs => Duration::from_secs(u64::from(secs)),
    };
    let update = UpdateOptions {
        poll: match reply.agent_update_poll_secs {
            0 => opts.update_poll,
            secs => Duration::from_secs(u64::from(secs)),
        },
        enabled: opts.self_update,
        master: opts.master.clone(),
        done: opts.update_done.clone(),
    };
    tracing::info!(
        server = %opts.server_id,
        running_revision,
        version = VERSION,
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
    // Cancellation is cooperative, so both side tasks are also aborted on the way
    // out: one stuck inside a call it will never return from must not outlive us.
    let health_token = CancellationToken::new();
    let _health_guard = health_token.clone().drop_guard();
    let mut health = AbortOnDrop(tokio::spawn(report_health(
        client.clone(),
        refresh_key.clone(),
        health_interval,
        sup.clone(),
        opts.applied_revision.clone(),
        opts.sources.clone(),
        discovered,
        health_token.clone(),
    )));
    // Update polls are the health task's business no longer: one that the master
    // never answers used to freeze every report behind it.
    let _updates = AbortOnDrop(tokio::spawn(poll_updates(
        client.clone(),
        refresh_key.clone(),
        update,
        opts.unary_timeout,
        health_token,
    )));

    loop {
        let message = tokio::select! {
            _ = shutdown.cancelled() => return Ok(()),
            ended = &mut health.0 => {
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
        // A swallowed ack ends the session: the next registration reports the running
        // revision, which the master records as applied.
        bounded(opts.unary_timeout, "AckConfig", client.ack_config(ack)).await?;
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
///
/// Nothing in this loop may wait on the master: a report is handed to the stream
/// and the loop moves on, so a master that stops answering is noticed by the stream
/// ending, never by a report that cannot be sent.
#[allow(clippy::too_many_arguments)]
async fn report_health(
    mut client: WorkerAgentClient<Channel>,
    refresh_key: MetadataValue<Ascii>,
    interval: Duration,
    sup: Arc<Mutex<Supervisor>>,
    applied_revision: Arc<AtomicI64>,
    sources: Sources,
    mut last_addresses: Discovered,
    token: CancellationToken,
) -> Result<(), BoxError> {
    let (tx, rx) = tokio::sync::mpsc::channel::<HealthReport>(1);
    let mut request = tonic::Request::new(tokio_stream::wrappers::ReceiverStream::new(rx));
    request
        .metadata_mut()
        .insert("x-refresh-key", refresh_key.clone());
    let call = client.report_health(request);
    tokio::pin!(call);

    let mut ticker = tokio::time::interval(interval);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    // Re-checks own addresses on its own cadence, independent of the report
    // interval; a changed set rides the next report and the master re-derives.
    let mut address_ticker = tokio::time::interval(addresses::REFRESH_INTERVAL);
    address_ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    address_ticker.tick().await; // the immediate first tick was covered by Register
    let mut pending_addresses: Option<ReportedAddresses> = None;
    // The first report that gets through proves this binary: it runs, connects
    // and authenticates, so a swap that installed it is confirmed.
    let mut confirmed = false;
    let stats = sup.lock().await.stats();
    loop {
        tokio::select! {
            _ = token.cancelled() => return Ok(()),
            reply = &mut call => {
                reply?;
                return Err("master closed the health stream".into());
            }
            _ = address_ticker.tick() => {
                let discovered = addresses::discover(&sources).await;
                if discovered != last_addresses {
                    pending_addresses = Some(discovered.to_proto());
                    last_addresses = discovered;
                }
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
                        reported_addresses: pending_addresses.take(),
                    }
                };
                if tx.send(report).await.is_err() {
                    // The request stream was dropped by the call ending; the next
                    // iteration observes the reply.
                    continue;
                }
                if !confirmed {
                    confirmed = true;
                    update::confirm_pending();
                }
            }
        }
    }
}

/// Asks the master for updates on its own, slower cadence and installs the one it
/// is given, until `token` is cancelled. Its own task, so a poll the master sits on
/// delays nothing else; `timeout` bounds each poll so it cannot sit forever either.
async fn poll_updates(
    mut client: WorkerAgentClient<Channel>,
    refresh_key: MetadataValue<Ascii>,
    update: UpdateOptions,
    timeout: Duration,
    token: CancellationToken,
) {
    // Update polls are spread so a fleet restarted together does not ask in
    // lockstep; the immediate first tick is skipped, registration just happened.
    let mut ticker = tokio::time::interval(jitter(update.poll));
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    ticker.tick().await;
    loop {
        tokio::select! {
            _ = token.cancelled() => return,
            _ = ticker.tick() => {}
        }
        let Some(plan) = poll_update(&mut client, &refresh_key, None, timeout).await else {
            continue;
        };
        tracing::info!(from = VERSION, to = %plan.version, "update offered; installing");
        match update::apply(&plan, &update).await {
            Ok(()) => update.done.notify_one(),
            Err(error) => {
                tracing::error!(to = %plan.version, %error, "update failed");
                // Reported at once, so the dashboard shows why without waiting a
                // poll interval.
                poll_update(&mut client, &refresh_key, Some(error), timeout).await;
            }
        }
    }
}

/// One `PollAgentUpdate`: reports `last_error` if there is one, and returns the
/// update the master wants installed — never the running version, and nothing
/// when the call fails or takes longer than `timeout` (the next tick asks again).
async fn poll_update(
    client: &mut WorkerAgentClient<Channel>,
    refresh_key: &MetadataValue<Ascii>,
    last_error: Option<String>,
    timeout: Duration,
) -> Option<UpdatePlan> {
    let mut request = tonic::Request::new(PollAgentUpdateRequest {
        last_error: last_error.unwrap_or_default(),
    });
    request
        .metadata_mut()
        .insert("x-refresh-key", refresh_key.clone());
    match bounded(
        timeout,
        "PollAgentUpdate",
        client.poll_agent_update(request),
    )
    .await
    {
        Ok(reply) => reply
            .into_inner()
            .update
            .filter(|update| update.version != VERSION)
            .map(|update| UpdatePlan {
                version: update.version,
                url: update.url,
                sha256: update.sha256,
            }),
        Err(error) => {
            tracing::warn!(%error, "update poll failed");
            None
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
