//! AMQP event definitions: the edit hint, and the periodic execution signals.
//!
//! Periodic work is not run by the process that schedules it. `--mode cron`
//! publishes one signal per due job and `--mode consumer` runs the pass, so a
//! sweep fails over and scales exactly like an edit does.

pub mod live;

use crate::entities::db::health::{
    HealthWrite, PodHealthStatus, PodHealthWrite, ServerHealthStatus,
};
use amqprs::BasicProperties;
use amqprs::channel::{BasicPublishArguments, Channel, ConfirmSelectArguments};
use kanau::{RkyvMessageDe, RkyvMessageSer};
use std::task::Poll;
use time::OffsetDateTime;
use wakuwaku::amqp::{AmqpExchangeType, AmqpMessageSend, AmqpPool, AmqpRouting};
use wakuwaku::interval_job::IntervalJobExecutionSignal;
use wakuwaku::pool::Pooled;

/// **Public event**
///
/// A canvas has edits that have not been derived yet.
///
/// Published by: every config-affecting mutation (dashboard), plus `Register`,
/// `AckConfig` and `ForgetServerApplied` (workers and dashboard).
/// Consumed by: [`crate::hooks::derive::CanvasDeriver`].
/// Route: exchange `orchestration` (direct), key `canvas_dirty`.
///
/// The payload is only a hint: the canvas generation counter, not this message, is
/// what decides whether a derivation is needed, so a lost message costs latency
/// (until the cron sweep) and never correctness.
#[derive(
    Debug, Clone, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, RkyvMessageSer, RkyvMessageDe,
)]
pub struct CanvasDirty {
    /// The canvas id, as text.
    pub canvas: String,
}

impl AmqpRouting for CanvasDirty {
    const EXCHANGE: &'static str = "orchestration";
    const EXCHANGE_TYPE: AmqpExchangeType = AmqpExchangeType::Direct;
    const ROUTING_KEY: &'static str = "canvas_dirty";
}

impl AmqpMessageSend for CanvasDirty {}

/// **Public event**
///
/// The health facts one write settled: a server whose status flipped, and the
/// pod rows a write produced.
///
/// Published by: [`crate::services::health::HealthService`] (report, stream
/// close and liveness sweep), `AckConfig`
/// ([`crate::services::agent::AgentService`]) and
/// [`crate::hooks::derive::CanvasDeriver`].
/// Consumed by: `notify`'s fan-out hook.
/// Route: exchange `orchestration` (direct), key `health_changed`.
///
/// The facts carry their own labels (server, pod and canvas names) so a
/// consumer can phrase a notification without a database of its own. A lost
/// message costs one notification and never state: what the fleet runs is
/// decided by the rows the publisher already committed.
#[derive(
    Debug, Clone, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, RkyvMessageSer, RkyvMessageDe,
)]
pub struct HealthChanged {
    pub facts: Vec<HealthFact>,
}

/// One health fact, as the publisher observed it.
#[derive(Debug, Clone, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
pub enum HealthFact {
    /// A server's status differs from the one it held before the write.
    Server {
        server: String,
        server_name: String,
        canvas: String,
        canvas_name: String,
        status: ServerHealthStatus,
        at_unix_micros: i64,
    },
    /// A pod row was written; whether it is a change is the consumer's call.
    Pod {
        pod: String,
        pod_name: String,
        canvas: String,
        canvas_name: String,
        status: PodHealthStatus,
        message: String,
        at_unix_micros: i64,
    },
}

impl HealthFact {
    /// The fact a server write settled, or `None` when the write only recorded
    /// the status the server already held: a routine report is not news.
    pub fn server(write: &HealthWrite) -> Option<Self> {
        if write.previous_status == write.record.status {
            return None;
        }
        Some(Self::Server {
            server: write.record.server.to_string(),
            server_name: write.server_name.clone(),
            canvas: write.canvas.to_string(),
            canvas_name: write.canvas_name.clone(),
            status: write.record.status,
            at_unix_micros: (write.record.report_time.unix_timestamp_nanos() / 1_000) as i64,
        })
    }

    /// The fact one written pod row carries. Whether it is a change is the
    /// consumer's call: a report writes one row per pod every interval.
    pub fn pod(write: &PodHealthWrite) -> Self {
        Self::Pod {
            pod: write.record.pod.to_string(),
            pod_name: write.pod_name.clone(),
            canvas: write.canvas.to_string(),
            canvas_name: write.canvas_name.clone(),
            status: write.record.status,
            message: write.record.message.clone(),
            at_unix_micros: (write.record.report_time.unix_timestamp_nanos() / 1_000) as i64,
        }
    }
}

/// Every fact one server write settled: the status flip, if there was one, then
/// the pod rows the write carried.
pub fn health_facts(write: &HealthWrite) -> Vec<HealthFact> {
    HealthFact::server(write)
        .into_iter()
        .chain(write.pods.iter().map(HealthFact::pod))
        .collect()
}

impl AmqpRouting for HealthChanged {
    const EXCHANGE: &'static str = "orchestration";
    const EXCHANGE_TYPE: AmqpExchangeType = AmqpExchangeType::Direct;
    const ROUTING_KEY: &'static str = "health_changed";
}

impl AmqpMessageSend for HealthChanged {}

// --- periodic execution signals ----------------------------------------------

/// Whether `every` seconds have passed between two scheduling ticks.
///
/// Compared in whole seconds, which is also the resolution a signal carries, and
/// saturating so a clock jump cannot overflow into a missed or duplicated tick.
fn elapsed(every: i64, now: OffsetDateTime, last_time: OffsetDateTime) -> bool {
    now.unix_timestamp()
        .saturating_sub(last_time.unix_timestamp())
        >= every
}

/// Publishes `message` with a per-message TTL of `ttl_secs` seconds, so the
/// broker discards a delivery nobody consumed in time.
///
/// This is [`AmqpMessageSend::send`] with one property added, and it exists for
/// the periodic signals only: a tick is worth exactly as much as the next one,
/// so a queue nobody is draining holds about one cadence's worth, not a day of
/// them. The TTL is carried by the message rather than declared on the queue
/// (`x-message-ttl`) because a queue's arguments are immutable once it is
/// declared: a queue argument would need every existing deployment to rename or
/// re-declare its signal queues, while a published property takes effect on the
/// next publish.
///
/// Every message in a signal queue is the same type with the same TTL and they
/// arrive in order, so the head expires first and the queue drains itself —
/// the lazy-expiry caveat of per-message TTLs (an expired message stuck behind
/// a live one) cannot arise here.
#[tracing::instrument(skip_all, err, name = "Publish:IntervalSignal")]
async fn send_expiring<M: AmqpMessageSend>(
    message: M,
    pool: &AmqpPool,
    ttl_secs: i64,
) -> Result<(), wakuwaku::Error> {
    let expiration = ttl_secs.saturating_mul(1_000).to_string();
    let bytes = message.to_bytes().map_err(|e| e.into())?;
    let channel: Result<Pooled<Channel, _>, wakuwaku::Error> = pool.get().await.into();
    let channel = channel?;
    let channel = channel
        .get_ref()
        .ok_or_else(|| wakuwaku::Error::Io(anyhow::anyhow!("the AMQP channel is closed")))?;
    channel
        .confirm_select(ConfirmSelectArguments::new(false))
        .await?;
    channel
        .basic_publish(
            BasicProperties::default()
                .with_expiration(&expiration)
                .finish(),
            bytes.into_vec(),
            BasicPublishArguments::new(M::EXCHANGE, M::ROUTING_KEY)
                .mandatory(true)
                .finish(),
        )
        .await?;
    // The counter the default `send` emits; a signal publish still counts.
    tracing::info!(monotonic_counter.mq_event_push = 1);
    Ok(())
}

/// Implements the AMQP route and [`IntervalJobExecutionSignal`] for one signal.
///
/// The publication cadence is a constant, and it has to be: `time_pool` is a
/// static function over two timestamps, and the scheduler that calls it opens no
/// database, so it cannot read [`crate::config::OrchestrationConfig`]. The
/// constant is therefore a *floor*, not the schedule an operator sees — the hook
/// that consumes the signal loads the configuration and claims a run only once
/// per its configured interval, so a job can be slowed down in the database and
/// never needs to run faster than this.
macro_rules! interval_signal {
    ($ty:ident, $key:literal, $every:expr) => {
        impl $ty {
            /// Seconds between publications of this signal.
            pub const EVERY_SECS: i64 = $every;

            /// The scheduling tick this signal was published for; a hook logs its
            /// age to make a consumer backlog visible.
            pub fn tick_time(&self) -> OffsetDateTime {
                OffsetDateTime::from_unix_timestamp(self.tick_unix_secs)
                    .unwrap_or_else(|_| OffsetDateTime::now_utc())
            }
        }

        impl AmqpRouting for $ty {
            const EXCHANGE: &'static str = "orchestration";
            const EXCHANGE_TYPE: AmqpExchangeType = AmqpExchangeType::Direct;
            const ROUTING_KEY: &'static str = $key;
        }

        // The one place a signal departs from the default `send`: a tick that
        // outlives its own cadence has been superseded by the next one, and the
        // hook's run claim would discard it anyway — so the broker drops it
        // instead of holding a backlog for a fleet that is down.
        impl AmqpMessageSend for $ty {
            async fn send(self, pool: &AmqpPool) -> Result<(), wakuwaku::Error> {
                send_expiring(self, pool, Self::EVERY_SECS).await
            }
        }

        impl IntervalJobExecutionSignal for $ty {
            fn tick(now: OffsetDateTime) -> Self {
                Self {
                    tick_unix_secs: now.unix_timestamp(),
                }
            }

            fn time_pool(now: OffsetDateTime, last_time: OffsetDateTime) -> Poll<Self> {
                if elapsed(Self::EVERY_SECS, now, last_time) {
                    Poll::Ready(Self::tick(now))
                } else {
                    Poll::Pending
                }
            }
        }
    };
}

/// **Periodic signal**
///
/// Derive every canvas whose edits outran their derivation.
///
/// Published by: the `cron` scheduler, every 30 s.
/// Consumed by: [`crate::hooks::derive::CanvasDeriver`].
/// Route: exchange `orchestration` (direct), key `derive_stale_canvases`.
///
/// This is the backstop for a lost [`CanvasDirty`]: correctness lives in the
/// canvas generation counters, so a late or dropped signal costs latency only.
#[derive(
    Debug, Clone, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, RkyvMessageSer, RkyvMessageDe,
)]
pub struct DeriveStaleCanvasesSignal {
    /// The scheduling tick, as a Unix timestamp in seconds.
    pub tick_unix_secs: i64,
}

interval_signal!(DeriveStaleCanvasesSignal, "derive_stale_canvases", 30);

/// **Periodic signal**
///
/// Re-issue relay leaves that are about to expire and re-derive their canvases.
///
/// Published by: the `cron` scheduler, every hour.
/// Consumed by: [`crate::hooks::derive::CanvasDeriver`].
/// Route: exchange `orchestration` (direct), key `rotate_relay_certificates`.
#[derive(
    Debug, Clone, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, RkyvMessageSer, RkyvMessageDe,
)]
pub struct RotateRelayCertificatesSignal {
    /// The scheduling tick, as a Unix timestamp in seconds.
    pub tick_unix_secs: i64,
}

interval_signal!(
    RotateRelayCertificatesSignal,
    "rotate_relay_certificates",
    3600
);

/// **Periodic signal**
///
/// Flip servers that stopped reporting to `Offline`.
///
/// Published by: the `cron` scheduler, every 30 s.
/// Consumed by: [`crate::hooks::health::HealthCronHook`].
/// Route: exchange `orchestration` (direct), key `sweep_liveness`.
#[derive(
    Debug, Clone, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, RkyvMessageSer, RkyvMessageDe,
)]
pub struct SweepLivenessSignal {
    /// The scheduling tick, as a Unix timestamp in seconds.
    pub tick_unix_secs: i64,
}

interval_signal!(SweepLivenessSignal, "sweep_liveness", 30);

/// **Periodic signal**
///
/// Delete health records past their retention TTLs.
///
/// Published by: the `cron` scheduler, every 5 minutes.
/// Consumed by: [`crate::hooks::health::HealthCronHook`].
/// Route: exchange `orchestration` (direct), key `trim_health_history`.
#[derive(
    Debug, Clone, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, RkyvMessageSer, RkyvMessageDe,
)]
pub struct TrimHealthHistorySignal {
    /// The scheduling tick, as a Unix timestamp in seconds.
    pub tick_unix_secs: i64,
}

interval_signal!(TrimHealthHistorySignal, "trim_health_history", 300);

/// **Periodic signal**
///
/// Ensure a `certificate` row exists for every Entry's TLS request, then issue
/// or renew whatever is due.
///
/// Published by: the `cron` scheduler, every 60 s.
/// Consumed by: [`crate::hooks::acme::AcmeCronHook`].
/// Route: exchange `orchestration` (direct), key `renew_certificates`.
#[derive(
    Debug, Clone, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, RkyvMessageSer, RkyvMessageDe,
)]
pub struct RenewCertificatesSignal {
    /// The scheduling tick, as a Unix timestamp in seconds.
    pub tick_unix_secs: i64,
}

interval_signal!(RenewCertificatesSignal, "renew_certificates", 60);

/// **Periodic signal**
///
/// Look up the country of every server IPv4 address that has none: a new or
/// changed address, or one whose last lookup failed long enough ago.
///
/// Published by: the `cron` scheduler, every 60 s.
/// Consumed by: [`crate::hooks::country::CountryCronHook`].
/// Route: exchange `orchestration` (direct), key `resolve_server_countries`.
#[derive(
    Debug, Clone, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, RkyvMessageSer, RkyvMessageDe,
)]
pub struct ResolveServerCountriesSignal {
    /// The scheduling tick, as a Unix timestamp in seconds.
    pub tick_unix_secs: i64,
}

interval_signal!(ResolveServerCountriesSignal, "resolve_server_countries", 60);
