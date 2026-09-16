# quinn-proto 0.11.17, patched

This is `quinn-proto` 0.11.17 from crates.io with one change: the pacer takes a
pacing rate from the congestion controller.

## Why

quinn's pacer refills its token bucket at `1.25 x cwnd / RTT`, and a
`congestion::Controller` can only influence it through `window()`. The worker's
brutal sender (`bin/guru-worker/src/quic/brutal.rs`) needs a window of twice the
bandwidth-delay product, so that loss-detection lag never stalls it, *and* a pace
of exactly its configured rate. With the stock pacer such a window would be paced
at 2.5x the rate.

The patch makes the pacer read `Controller::metrics().pacing_rate` (bits per
second, a field the trait already has for qlog) on every pacing decision and,
when it is `Some`, shrink the window it paces on to `rate x RTT / 1.25`, so the
refill lands on the rate. Controllers that leave `pacing_rate` at `None` (Cubic,
NewReno, BBR) behave exactly as before.

Upstream `main` (the unreleased 0.12 line) has an equivalent
`rate_limited_window` driven by a static transport setting rather than by the
controller; when a release carries a controller-driven rate this copy goes away.

## Re-applying on a new release

1. Copy `Cargo.toml`, `LICENSE-*` and `src/` of the new release here.
2. Apply the diff below (three files); `cargo check -p guru-worker`.
3. Keep `version` in `Cargo.toml` equal to the release, so the
   `[patch.crates-io]` entry in the workspace `Cargo.toml` still satisfies
   `quinn`'s requirement.

## Diff against 0.11.17

```diff
diff --color -ru a/src/connection/mod.rs b/src/connection/mod.rs
--- a/src/connection/mod.rs	2006-07-23 19:21:28.000000000 -0600
+++ b/src/connection/mod.rs	2026-09-16 07:46:23.549341688 -0600
@@ -619,6 +619,7 @@
                         bytes_to_send,
                         self.path.current_mtu(),
                         self.path.congestion.window(),
+                        paths::pacing_rate_bytes(self.path.congestion.metrics().pacing_rate),
                         now,
                     ) {
                         self.timers.set(Timer::Pacing, delay);
diff --color -ru a/src/connection/pacing.rs b/src/connection/pacing.rs
--- a/src/connection/pacing.rs	2006-07-23 19:21:28.000000000 -0600
+++ b/src/connection/pacing.rs	2026-09-16 07:46:23.545341590 -0600
@@ -1,4 +1,9 @@
 //! Pacing of packet transmissions.
+//!
+//! guru patch (see `vendor/quinn-proto/PATCH.md`): a congestion controller that
+//! reports a `pacing_rate` in its metrics has the pacer refill at that rate
+//! instead of at `1.25 x cwnd / RTT`, so a fixed-rate controller can keep a
+//! large window without sending faster than its rate.
 
 use crate::{Duration, Instant};
 
@@ -22,7 +27,14 @@
 
 impl Pacer {
     /// Obtains a new [`Pacer`].
-    pub(super) fn new(smoothed_rtt: Duration, window: u64, mtu: u16, now: Instant) -> Self {
+    pub(super) fn new(
+        smoothed_rtt: Duration,
+        window: u64,
+        mtu: u16,
+        pacing_rate: Option<u64>,
+        now: Instant,
+    ) -> Self {
+        let window = rate_limited_window(smoothed_rtt, window, pacing_rate);
         let capacity = optimal_capacity(smoothed_rtt, window, mtu);
         Self {
             capacity,
@@ -45,12 +57,16 @@
     ///
     /// The 5/4 ratio used here comes from the suggestion that N = 1.25 in the draft IETF RFC for
     /// QUIC.
+    ///
+    /// `pacing_rate` (bytes per second), when given by the congestion controller, caps the
+    /// refill rate regardless of `window`.
     pub(super) fn delay(
         &mut self,
         smoothed_rtt: Duration,
         bytes_to_send: u64,
         mtu: u16,
         window: u64,
+        pacing_rate: Option<u64>,
         now: Instant,
     ) -> Option<Instant> {
         debug_assert_ne!(
@@ -58,6 +74,8 @@
             "zero-sized congestion control window is nonsense"
         );
 
+        let window = rate_limited_window(smoothed_rtt, window, pacing_rate);
+
         if window != self.last_window || mtu != self.last_mtu {
             self.capacity = optimal_capacity(smoothed_rtt, window, mtu);
 
@@ -113,6 +131,22 @@
     }
 }
 
+/// Shrinks `window` so that refilling at `1.25 x window / RTT` sends no faster than
+/// `bytes_per_second`. `None` leaves the window alone.
+fn rate_limited_window(
+    smoothed_rtt: Duration,
+    window: u64,
+    bytes_per_second: Option<u64>,
+) -> u64 {
+    let Some(bytes_per_second) = bytes_per_second else {
+        return window;
+    };
+    let rate_window = bytes_per_second as f64 * smoothed_rtt.as_secs_f64();
+    // The pacer refills at x1.25, so the window is shrunk to cancel that out.
+    let adjusted = (rate_window / 1.25).round() as u64;
+    window.min(adjusted.max(1))
+}
+
 /// Calculates a pacer capacity for a certain window and RTT
 ///
 /// The goal is to emit a burst (of size `capacity`) in timer intervals
diff --color -ru a/src/connection/paths.rs b/src/connection/paths.rs
--- a/src/connection/paths.rs	2006-07-23 19:21:28.000000000 -0600
+++ b/src/connection/paths.rs	2026-09-16 07:46:38.633710000 -0600
@@ -13,6 +13,12 @@
 use qlog::events::quic::MetricsUpdated;
 
 /// Description of a particular network path
+/// The refill rate for the pacer from a controller's reported pacing rate (bits per
+/// second), in bytes per second. guru patch, see `vendor/quinn-proto/PATCH.md`.
+pub(super) fn pacing_rate_bytes(bits_per_second: Option<u64>) -> Option<u64> {
+    bits_per_second.map(|bits| (bits / 8).max(1))
+}
+
 pub(super) struct PathData {
     pub(super) remote: SocketAddr,
     pub(super) rtt: RttEstimator,
@@ -75,6 +81,7 @@
                 config.initial_rtt,
                 congestion.initial_window(),
                 config.get_initial_mtu(),
+                pacing_rate_bytes(congestion.metrics().pacing_rate),
                 now,
             ),
             congestion,
@@ -118,7 +125,13 @@
         Self {
             remote,
             rtt: prev.rtt,
-            pacing: Pacer::new(smoothed_rtt, congestion.window(), prev.current_mtu(), now),
+            pacing: Pacer::new(
+                smoothed_rtt,
+                congestion.window(),
+                prev.current_mtu(),
+                pacing_rate_bytes(congestion.metrics().pacing_rate),
+                now,
+            ),
             sending_ecn: true,
             congestion,
             challenge: None,
```
