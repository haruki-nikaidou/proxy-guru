//! Hysteria's *brutal* congestion control for quinn.
//!
//! Brutal does not probe for bandwidth. It is told the rate the path carries and
//! sends at exactly that rate: the pacer (see `vendor/quinn-proto/PATCH.md`) is
//! given `rate / ack_rate`, and the congestion window is twice the
//! bandwidth-delay product at that rate so that packets the path dropped, which
//! count as in flight until quinn detects the loss, never stall the sender. Loss
//! is answered by sending more, not less: `ack_rate` is the share of bytes the
//! peer acknowledged over the last five seconds, and the send rate is divided by
//! it, floored at 0.8 so that the sender over-sends by a quarter at most however
//! bad the path gets. A port of `core/internal/congestion/brutal/brutal.go`.

use quinn_proto::RttEstimator;
use quinn_proto::congestion::{Controller, ControllerFactory, ControllerMetrics};
use std::any::Any;
use std::sync::Arc;
use std::time::{Duration, Instant};

/// One-second buckets of acknowledged and lost bytes: how far back the ack rate looks.
const SLOTS: usize = 5;
/// Below this many bytes of samples the ack rate is taken as 1: a fresh connection
/// has nothing to compensate for yet.
const MIN_SAMPLE_BYTES: u64 = 50 * 1200;
/// The ack rate is never taken lower than this, so over-sending stops at 1.25x.
const MIN_ACK_RATE: f64 = 0.8;
/// The window is this many bandwidth-delay products.
const WINDOW_GAIN: f64 = 2.0;
/// The window before the first RTT sample: enough for the handshake.
const INITIAL_WINDOW: u64 = 12 * 1200;

/// Builds a [`Brutal`] per connection, all at the same rate.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BrutalConfig {
    pub bytes_per_second: u64,
}

impl ControllerFactory for BrutalConfig {
    fn build(self: Arc<Self>, now: Instant, current_mtu: u16) -> Box<dyn Controller> {
        Box::new(Brutal::new(self.bytes_per_second, now, current_mtu))
    }
}

#[derive(Debug, Clone, Copy, Default)]
struct Slot {
    /// Seconds since the controller was built; a slot is reused once it is
    /// [`SLOTS`] seconds old.
    second: u64,
    acked: u64,
    lost: u64,
}

#[derive(Debug, Clone)]
pub struct Brutal {
    bytes_per_second: u64,
    mtu: u64,
    epoch: Instant,
    /// Smoothed RTT as of the last acknowledgement, `None` until the first one.
    srtt: Option<Duration>,
    slots: [Slot; SLOTS],
    ack_rate: f64,
}

impl Brutal {
    pub fn new(bytes_per_second: u64, now: Instant, mtu: u16) -> Self {
        Self {
            bytes_per_second: bytes_per_second.max(1),
            mtu: u64::from(mtu),
            epoch: now,
            srtt: None,
            slots: [Slot::default(); SLOTS],
            ack_rate: 1.0,
        }
    }

    /// The share of sampled bytes the peer acknowledged, `1` when there is too
    /// little to say, never below [`MIN_ACK_RATE`].
    pub fn ack_rate(&self) -> f64 {
        self.ack_rate
    }

    fn second(&self, now: Instant) -> u64 {
        now.saturating_duration_since(self.epoch).as_secs()
    }

    /// The bucket for `now`, reset if it last held an older second.
    fn slot_mut(&mut self, now: Instant) -> &mut Slot {
        let second = self.second(now);
        let index = usize::try_from(second.rem_euclid(SLOTS as u64)).unwrap_or(0);
        let slot = &mut self.slots[index];
        if slot.second != second {
            *slot = Slot {
                second,
                acked: 0,
                lost: 0,
            };
        }
        slot
    }

    fn update_ack_rate(&mut self, now: Instant) {
        let second = self.second(now);
        let oldest = second.saturating_sub(SLOTS as u64);
        let (mut acked, mut lost) = (0u64, 0u64);
        for slot in &self.slots {
            // A slot older than the window is stale; a zero-second slot on a
            // fresh controller is simply empty and adds nothing either way.
            if slot.second < oldest {
                continue;
            }
            acked = acked.saturating_add(slot.acked);
            lost = lost.saturating_add(slot.lost);
        }
        let total = acked.saturating_add(lost);
        self.ack_rate = if total < MIN_SAMPLE_BYTES {
            1.0
        } else {
            (acked as f64 / total as f64).max(MIN_ACK_RATE)
        };
    }

    fn record(&mut self, now: Instant, acked: u64, lost: u64) {
        let slot = self.slot_mut(now);
        slot.acked = slot.acked.saturating_add(acked);
        slot.lost = slot.lost.saturating_add(lost);
        self.update_ack_rate(now);
    }

    /// An acknowledgement of `bytes` with the smoothed RTT as of then.
    fn acked(&mut self, now: Instant, bytes: u64, srtt: Duration) {
        self.srtt = Some(srtt);
        self.record(now, bytes, 0);
    }

    /// Bytes per second the pacer should release, loss compensation included.
    fn pacing_bytes_per_second(&self) -> u64 {
        (self.bytes_per_second as f64 / self.ack_rate) as u64
    }
}

impl Controller for Brutal {
    fn on_ack(
        &mut self,
        now: Instant,
        _sent: Instant,
        bytes: u64,
        _app_limited: bool,
        rtt: &RttEstimator,
    ) {
        self.acked(now, bytes, rtt.get());
    }

    fn on_congestion_event(
        &mut self,
        now: Instant,
        _sent: Instant,
        _is_persistent_congestion: bool,
        lost_bytes: u64,
    ) {
        // An ECN mark reports zero lost bytes; brutal ignores it either way.
        if lost_bytes > 0 {
            self.record(now, 0, lost_bytes);
        }
    }

    fn on_mtu_update(&mut self, new_mtu: u16) {
        self.mtu = u64::from(new_mtu);
    }

    fn window(&self) -> u64 {
        let Some(srtt) = self.srtt else {
            return self.initial_window();
        };
        let bdp = self.bytes_per_second as f64 * srtt.as_secs_f64();
        let window = (bdp * WINDOW_GAIN / self.ack_rate) as u64;
        window.max(self.mtu.saturating_mul(2))
    }

    fn metrics(&self) -> ControllerMetrics {
        let mut metrics = ControllerMetrics::default();
        metrics.congestion_window = self.window();
        // Bits per second, as the field is documented; the patched pacer converts.
        metrics.pacing_rate = Some(self.pacing_bytes_per_second().saturating_mul(8));
        metrics
    }

    fn clone_box(&self) -> Box<dyn Controller> {
        Box::new(self.clone())
    }

    fn initial_window(&self) -> u64 {
        INITIAL_WINDOW.max(self.mtu.saturating_mul(2))
    }

    fn into_any(self: Box<Self>) -> Box<dyn Any> {
        self
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    const RATE: u64 = 250_000_000; // 2000 Mbit/s

    fn rtt(ms: u64) -> Duration {
        Duration::from_millis(ms)
    }

    #[test]
    fn window_is_twice_the_bandwidth_delay_product() {
        let start = Instant::now();
        let mut brutal = Brutal::new(RATE, start, 1200);
        assert_eq!(brutal.window(), INITIAL_WINDOW, "before any ack");
        brutal.acked(start, 1200, rtt(50));
        // 250 MB/s x 50 ms = 12.5 MB; twice that.
        assert_eq!(brutal.window(), 25_000_000);
        assert_eq!(brutal.metrics().pacing_rate, Some(RATE * 8));
    }

    #[test]
    fn loss_raises_the_rate_and_is_capped_at_a_quarter() {
        let start = Instant::now();
        let mut brutal = Brutal::new(RATE, start, 1200);
        let srtt = rtt(100);
        // Too few samples: no compensation yet.
        brutal.acked(start, 1000, srtt);
        brutal.on_congestion_event(start, start, false, 1000);
        assert_eq!(brutal.ack_rate(), 1.0);

        // 90% delivered over enough bytes: send 1/0.9 as much.
        brutal.acked(start, 900_000, srtt);
        brutal.on_congestion_event(start, start, false, 99_000);
        let rate = brutal.ack_rate();
        assert!((rate - 0.9).abs() < 0.01, "{rate}");
        let expected = (RATE as f64 / rate) as u64 * 8;
        assert_eq!(brutal.metrics().pacing_rate, Some(expected));
        assert_eq!(
            brutal.window(),
            (RATE as f64 * 0.1 * 2.0 / rate) as u64,
            "the window grows with the compensation too"
        );

        // Half lost: clamped to 0.8, so 1.25x and no more.
        brutal.on_congestion_event(start, start, false, 5_000_000);
        assert_eq!(brutal.ack_rate(), MIN_ACK_RATE);
        assert_eq!(
            brutal.metrics().pacing_rate,
            Some((RATE as f64 / MIN_ACK_RATE) as u64 * 8)
        );
    }

    #[test]
    fn samples_older_than_five_seconds_are_forgotten() {
        let start = Instant::now();
        let mut brutal = Brutal::new(RATE, start, 1200);
        let srtt = rtt(20);
        brutal.acked(start, 500_000, srtt);
        brutal.on_congestion_event(start, start, false, 500_000);
        assert_eq!(brutal.ack_rate(), MIN_ACK_RATE);

        let later = start + Duration::from_secs(6);
        brutal.acked(later, 1_000_000, srtt);
        assert_eq!(brutal.ack_rate(), 1.0, "only the clean second remains");
    }
}
