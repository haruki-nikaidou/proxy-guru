//! AMQP event definitions: the edit hint, and the periodic execution signals.
//!
//! Periodic work is not run by the process that schedules it. `--mode cron`
//! publishes one signal per due job and `--mode consumer` runs the pass, so a
//! sweep fails over and scales exactly like an edit does.

pub mod live;

use kanau::{RkyvMessageDe, RkyvMessageSer};
use std::task::Poll;
use time::OffsetDateTime;
use wakuwaku::amqp::{AmqpExchangeType, AmqpMessageSend, AmqpRouting};
use wakuwaku::interval_job::IntervalJobExecutionSignal;

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
            pub fn tick_time(&self) -> chrono::DateTime<chrono::Utc> {
                chrono::DateTime::from_timestamp(self.tick_unix_secs, 0)
                    .unwrap_or_else(chrono::Utc::now)
            }
        }

        impl AmqpRouting for $ty {
            const EXCHANGE: &'static str = "orchestration";
            const EXCHANGE_TYPE: AmqpExchangeType = AmqpExchangeType::Direct;
            const ROUTING_KEY: &'static str = $key;
        }

        impl AmqpMessageSend for $ty {}

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
