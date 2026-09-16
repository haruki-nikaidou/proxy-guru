//! The transport parameters of one QUIC relay connection, either side, from the
//! `[keepalive]` section and the link's [`QuicTuning`].
//!
//! quinn's defaults suit a web client: Cubic, a 1.25 MB per-stream receive window
//! that is never enlarged, 10 MB unacknowledged per connection. A relay link that
//! is asked for a rate gets brutal (see [`super::brutal`]) and windows sized for
//! it: at 2x the bandwidth-delay product in flight, a window of half a second of
//! the peer's rate covers paths up to 250 ms.
//!
//! The receive window does more than admit bytes. quinn caps how many holes a
//! stream may hold at once in proportion to it (see
//! `vendor/quinn-proto/PATCH.md`), and a fixed-rate sender on a lossy path leaves
//! many: a window too small for the peer's rate does not merely throttle the
//! stream, it ends the connection. So `receive_mbps` is not optional decoration
//! on a link that sets `send_mbps` at the other end.

use crate::quic::brutal::BrutalConfig;
use guru_worker_config::{KeepAlive, QuicCongestion, QuicTuning};
use std::sync::Arc;
use std::time::Duration;

/// Streams a listener lets one connection keep open when the tuning does not say.
pub const DEFAULT_MAX_STREAMS: u32 = 4096;
/// quinn's own per-stream receive window.
const DEFAULT_STREAM_RECEIVE_WINDOW: u64 = 1_250_000;
/// quinn's own per-connection send window.
const DEFAULT_SEND_WINDOW: u64 = 10_000_000;
/// The windows cover this much of the peer's rate.
const WINDOW_SECONDS: f64 = 0.5;

/// Bytes per second for a rate in Mbit/s.
pub fn bytes_per_second(mbps: u32) -> u64 {
    u64::from(mbps).saturating_mul(125_000)
}

fn window_for(mbps: u32, floor: u64) -> u64 {
    (((bytes_per_second(mbps) as f64) * WINDOW_SECONDS) as u64).max(floor)
}

/// The stream limit a listener advertises for `tuning`.
pub fn max_streams(tuning: Option<&QuicTuning>) -> u32 {
    match tuning.map(|t| t.max_streams) {
        Some(n) if n > 0 => n,
        _ => DEFAULT_MAX_STREAMS,
    }
}

/// The per-stream receive window for `tuning`: the override, else derived from
/// the peer's rate, never below quinn's default.
pub fn stream_receive_window(tuning: Option<&QuicTuning>) -> u64 {
    let t = tuning.copied().unwrap_or_default();
    if t.stream_receive_window > 0 {
        return t.stream_receive_window;
    }
    window_for(t.receive_mbps, DEFAULT_STREAM_RECEIVE_WINDOW)
}

/// The per-connection send window for `tuning`: the override, else derived from
/// this side's rate, never below quinn's default.
pub fn send_window(tuning: Option<&QuicTuning>) -> u64 {
    let t = tuning.copied().unwrap_or_default();
    if t.send_window > 0 {
        return t.send_window;
    }
    window_for(t.send_mbps, DEFAULT_SEND_WINDOW)
}

/// The transport parameters for one QUIC connection, either side.
pub fn build(ka: &KeepAlive, tuning: Option<&QuicTuning>) -> Arc<quinn::TransportConfig> {
    let mut transport = quinn::TransportConfig::default();
    transport.keep_alive_interval(Some(Duration::from_secs(u64::from(ka.quic_ping_secs))));
    // `quic_idle_secs` is validated against `u32` seconds, so the millisecond value
    // fits a `VarInt` (a 62-bit integer) with room to spare.
    transport.max_idle_timeout(Some(
        quinn::VarInt::from_u64(u64::from(ka.quic_idle_secs).saturating_mul(1000))
            .unwrap_or(quinn::VarInt::MAX)
            .into(),
    ));
    transport.stream_receive_window(
        quinn::VarInt::from_u64(stream_receive_window(tuning)).unwrap_or(quinn::VarInt::MAX),
    );
    transport.send_window(send_window(tuning));
    if let Some(window) = tuning.map(|t| t.receive_window).filter(|w| *w > 0) {
        transport.receive_window(quinn::VarInt::from_u64(window).unwrap_or(quinn::VarInt::MAX));
    }
    transport.max_concurrent_bidi_streams(quinn::VarInt::from_u32(max_streams(tuning)));
    transport.max_concurrent_uni_streams(quinn::VarInt::from_u32(0));
    if let Some(t) = tuning.filter(|t| t.congestion == QuicCongestion::Brutal && t.send_mbps > 0) {
        transport.congestion_controller_factory(Arc::new(BrutalConfig {
            bytes_per_second: bytes_per_second(t.send_mbps),
        }));
    }
    Arc::new(transport)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn windows_follow_the_rates_and_never_shrink_below_quinn() {
        assert_eq!(stream_receive_window(None), DEFAULT_STREAM_RECEIVE_WINDOW);
        assert_eq!(send_window(None), DEFAULT_SEND_WINDOW);
        assert_eq!(max_streams(None), DEFAULT_MAX_STREAMS);

        let link = QuicTuning {
            send_mbps: 200,
            receive_mbps: 2000,
            ..QuicTuning::default()
        };
        // 2000 Mbit/s = 250 MB/s; half a second of it.
        assert_eq!(stream_receive_window(Some(&link)), 125_000_000);
        // 200 Mbit/s = 25 MB/s; half a second is 12.5 MB, above quinn's 10 MB.
        assert_eq!(send_window(Some(&link)), 12_500_000);

        let slow = QuicTuning {
            send_mbps: 1,
            receive_mbps: 1,
            ..QuicTuning::default()
        };
        assert_eq!(
            stream_receive_window(Some(&slow)),
            DEFAULT_STREAM_RECEIVE_WINDOW
        );
        assert_eq!(send_window(Some(&slow)), DEFAULT_SEND_WINDOW);

        let pinned = QuicTuning {
            receive_mbps: 2000,
            stream_receive_window: 67_108_864,
            send_window: 4_096,
            max_streams: 64,
            ..QuicTuning::default()
        };
        assert_eq!(stream_receive_window(Some(&pinned)), 67_108_864);
        assert_eq!(send_window(Some(&pinned)), 4_096);
        assert_eq!(max_streams(Some(&pinned)), 64);
    }
}
