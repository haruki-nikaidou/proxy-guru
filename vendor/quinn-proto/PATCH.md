# quinn-proto 0.11.17, patched

This is `quinn-proto` 0.11.17 from crates.io with two changes, both needed by the
worker's fixed-rate (hysteria "brutal") congestion control in
`bin/guru-worker/src/quic/brutal.rs`.

A `[workspace]` table is appended to `Cargo.toml` so the copy can be tested on its
own (`cd vendor/quinn-proto && cargo test`); it is otherwise the published crate.

## 1. The pacer takes a rate from the congestion controller

quinn's pacer refills its token bucket at `1.25 x cwnd / RTT`, and a
`congestion::Controller` can only influence it through `window()`. Brutal needs a
window of twice the bandwidth-delay product, so that packets the path dropped —
which count as in flight until quinn detects the loss — never stall the sender,
*and* a pace of exactly its configured rate. With the stock pacer such a window
would be paced at 2.5x the rate.

The patch makes the pacer read `Controller::metrics().pacing_rate` (bits per
second, a field the trait already has for qlog) on every pacing decision and,
when it is `Some`, shrink the window it paces on to `rate x RTT / 1.25`, so the
refill lands on the rate. Controllers that leave `pacing_rate` at `None` (Cubic,
NewReno, BBR) behave exactly as before.

It also carries upstream's fix for the refill clock: when the bucket is polled
faster than a whole byte accrues, the truncation to an integer yields nothing, and
advancing `prev` regardless would discard that time for good.

Upstream `main` (the unreleased 0.12 line) has an equivalent `rate_limited_window`
driven by a static transport setting rather than by the controller; when a release
carries a controller-driven rate this half goes away.

## 2. The stream reassembly span cap follows the receive window

Upstream caps every receive stream at 1024 distinct spans and kills the connection
with `INTERNAL_ERROR("too many gaps in stream buffer")` past that. That suits a
congestion controller which slows down when it loses packets. A fixed-rate one
does not: at 1000 Mbit/s over a 150 ms path it keeps ~31,000 packets in flight and
answers loss by sending *more*, so at 1% loss the receiver holds several hundred
holes at once and reaches 1024 within seconds. Measured over a netem link
(150 ms RTT, 1% loss) the stock cap killed the connection after 700 KB, which on
the wire is indistinguishable from the path itself failing.

A hole costs one span, and a peer cannot leave more holes than its receive window
has room for packets, so the cap is now derived from that window — the same number
that already bounds how much the receiver buffers. A peer flooding tiny gapped
frames is still bounded, to `window / 1200` spans, whose bookkeeping is a few
percent of the window itself. quinn's own default window yields 1041, so a
connection nobody tuned behaves as before.

Measured across that netem link, one relay hop carrying 64 MB:

| sender | rate |
| --- | --- |
| Cubic, quinn's defaults | 2.4 Mbit/s |
| Cubic, 64 MiB windows | 4.3 Mbit/s |
| TCP (kernel, same link) | 140 Mbit/s |
| brutal 1000, both patches | 447 Mbit/s |

## Re-applying on a new release

1. Copy `Cargo.toml`, `LICENSE-*` and `src/` of the new release here, then append
   `[workspace]` to `Cargo.toml`.
2. Apply the diff below; `cd vendor/quinn-proto && cargo test`, then
   `cargo check -p guru-worker`.
3. Keep `version` in `Cargo.toml` equal to the release, so the
   `[patch.crates-io]` entry in the workspace `Cargo.toml` still satisfies
   `quinn`'s requirement.

## Diff against 0.11.17

```diff
diff --color -ru a/src/connection/assembler.rs b/src/connection/assembler.rs
--- a/src/connection/assembler.rs	2006-07-23 19:21:28.000000000 -0600
+++ b/src/connection/assembler.rs	2026-09-16 08:43:25.601321034 -0600
@@ -9,7 +9,7 @@
 use crate::range_set::RangeSet;
 
 /// Helper to assemble unordered stream frames into an ordered stream
-#[derive(Debug, Default)]
+#[derive(Debug)]
 pub(super) struct Assembler {
     state: State,
     data: BinaryHeap<Buffer>,
@@ -22,6 +22,23 @@
     /// aka the stream offset.
     bytes_read: u64,
     end: u64,
+    /// Cap on the number of distinct spans, from the stream's receive window.
+    /// guru patch, see `vendor/quinn-proto/PATCH.md`.
+    max_chunks: usize,
+}
+
+impl Default for Assembler {
+    fn default() -> Self {
+        Self {
+            state: State::default(),
+            data: BinaryHeap::new(),
+            buffered: 0,
+            allocated: 0,
+            bytes_read: 0,
+            end: 0,
+            max_chunks: MIN_MAX_CHUNKS,
+        }
+    }
 }
 
 impl Assembler {
@@ -29,10 +46,17 @@
         Self::default()
     }
 
+    /// Sizes the span cap for a stream whose receive window is `window` bytes.
+    pub(super) fn set_window(&mut self, window: u64) {
+        self.max_chunks = chunk_limit(window);
+    }
+
     /// Reset to the initial state
     pub(super) fn reinit(&mut self) {
         let old_data = mem::take(&mut self.data);
+        let max_chunks = self.max_chunks;
         *self = Self::default();
+        self.max_chunks = max_chunks;
         self.data = old_data;
         self.data.clear();
     }
@@ -209,10 +233,10 @@
         // balance between defragmentation overhead and over-allocation.
         let threshold = 32768.max(buffered * 3 / 2);
         // Small gapped frames hold over-allocation below the threshold, so bound the count too.
-        if over_allocation > threshold || self.data.len() > COMPACT_THRESHOLD {
+        if over_allocation > threshold || self.data.len() > self.max_chunks.saturating_mul(2) {
             self.defragment();
             // ngtcp2 uses a threshold of 4000 -- try to be a little more conservative?
-            if self.data.len() > MAX_CHUNKS {
+            if self.data.len() > self.max_chunks {
                 return Err(TooManyChunks);
             }
         }
@@ -351,21 +375,70 @@
 #[derive(Debug)]
 pub(crate) struct TooManyChunks;
 
-/// Bound on the number of distinct spans kept for a stream
+/// Floor on the number of distinct spans kept for a stream.
 ///
-/// Independent of how much memory those spans over-allocate. A frame is rejected only
-/// if compaction cannot get the count back down to this.
-const MAX_CHUNKS: usize = 1024;
-
-/// Chunk count past which `insert` compacts before deciding whether to reject
+/// guru patch (see `vendor/quinn-proto/PATCH.md`): upstream caps every stream at a
+/// flat 1024 spans, which suits a congestion controller that slows down when it
+/// loses packets. A fixed-rate one does not: it keeps twice the bandwidth-delay
+/// product in flight and answers loss by sending more, so on a lossy path the
+/// receiver holds hundreds of holes at once and reaches 1024 within seconds. The
+/// stream then dies with an internal error, which on the wire looks exactly like
+/// the path itself failing.
 ///
-/// Above `MAX_CHUNKS` so a flood of mergeable frames cannot force a defragmentation
-/// per frame.
-const COMPACT_THRESHOLD: usize = 2 * MAX_CHUNKS;
+/// Each hole costs one span, and a peer cannot leave more holes than its receive
+/// window has room for packets, so the cap is derived from that window — the same
+/// number that already bounds how much the receiver buffers. A peer flooding tiny
+/// gapped frames is still bounded, to `window / MIN_SPAN_SPACING` spans, whose
+/// bookkeeping is a few percent of the window itself. quinn's own default window
+/// yields a cap within a rounding error of upstream's 1024, so a connection nobody
+/// tuned behaves exactly as before.
+const MIN_MAX_CHUNKS: usize = 1024;
+
+/// Bytes of receive window that earn one more span: one full-size packet.
+const MIN_SPAN_SPACING: u64 = 1200;
+
+/// Hard ceiling, so a preposterous window cannot ask for unbounded bookkeeping.
+const MAX_MAX_CHUNKS: usize = 1 << 20;
+
+/// The span cap for a stream whose receive window is `window` bytes.
+fn chunk_limit(window: u64) -> usize {
+    usize::try_from(window / MIN_SPAN_SPACING)
+        .unwrap_or(MAX_MAX_CHUNKS)
+        .clamp(MIN_MAX_CHUNKS, MAX_MAX_CHUNKS)
+}
 
 #[cfg(test)]
 mod test {
     use super::*;
+
+    /// What a default-window assembler compacts at.
+    const COMPACT_THRESHOLD: usize = 2 * MIN_MAX_CHUNKS;
+
+    /// guru patch: the span cap tracks the receive window, and an untuned
+    /// connection keeps upstream's limit.
+    #[test]
+    fn span_cap_follows_the_window() {
+        assert_eq!(chunk_limit(0), MIN_MAX_CHUNKS, "no window: the floor");
+        // quinn's own default stream window is within a rounding error of 1024.
+        let default_window = chunk_limit(1_250_000);
+        assert!(
+            (MIN_MAX_CHUNKS..MIN_MAX_CHUNKS + 100).contains(&default_window),
+            "an untuned connection keeps upstream's limit, got {default_window}"
+        );
+        // A window sized for a fixed-rate sender earns one span per packet.
+        assert_eq!(chunk_limit(64 * 1024 * 1024), 55_924);
+        assert_eq!(chunk_limit(u64::MAX), MAX_MAX_CHUNKS, "ceiling holds");
+
+        let mut assembler = Assembler::new();
+        assembler.set_window(64 * 1024 * 1024);
+        // One-byte frames every other byte: far past upstream's flat cap, held
+        // by the window's.
+        for i in 0..(MIN_MAX_CHUNKS as u64 * 4) {
+            assembler
+                .insert(i * 2, Bytes::from_static(b"0"), 1)
+                .expect("a wide window tolerates many holes");
+        }
+    }
     use assert_matches::assert_matches;
 
     #[test]
@@ -684,7 +757,7 @@
         // Withhold offset 0 so an ordered reader can never drain anything.
         let mut offset = 1u64;
         let mut result = Ok(());
-        for _ in 0..(MAX_CHUNKS * 8) {
+        for _ in 0..(MIN_MAX_CHUNKS * 8) {
             result = x.insert(offset, Bytes::from_static(b"gap"), 3);
             if result.is_err() {
                 break;
@@ -708,7 +781,7 @@
         x.ensure_ordering(false).unwrap();
         let top = 1_000_000u64;
         x.insert(top, Bytes::from_static(b"ab"), 2).unwrap();
-        for k in 0..(4 * MAX_CHUNKS as u64) {
+        for k in 0..(4 * MIN_MAX_CHUNKS as u64) {
             x.insert(top - k - 1, Bytes::from_static(b"ab"), 2).unwrap();
             assert!(
                 x.data.len() <= COMPACT_THRESHOLD + 1,
@@ -724,11 +797,11 @@
         // so each one pushes a chunk; they must not be rejected, or compact every frame.
         let mut x = Assembler::new();
         // Withhold offset 0 so nothing can be drained.
-        for i in 0..MAX_CHUNKS as u64 {
+        for i in 0..MIN_MAX_CHUNKS as u64 {
             x.insert(1 + i * 4, Bytes::from_static(b"abc"), 3).unwrap();
         }
         let mut max_len = x.data.len();
-        for _ in 0..(3 * MAX_CHUNKS) {
+        for _ in 0..(3 * MIN_MAX_CHUNKS) {
             x.insert(1, Bytes::from_static(b"abc"), 3)
                 .expect("duplicate flood must not be rejected");
             max_len = max_len.max(x.data.len());
@@ -740,7 +813,7 @@
         }
         // Compacting on every frame would pin the count at `MAX_CHUNKS`.
         assert!(
-            max_len > MAX_CHUNKS,
+            max_len > MIN_MAX_CHUNKS,
             "buffer compacted on every frame (max observed len {max_len})"
         );
     }
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
+++ b/src/connection/pacing.rs	2026-09-16 08:46:14.225497080 -0600
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
 
@@ -89,13 +107,15 @@
         }
 
         let elapsed_rtts = time_elapsed.as_secs_f64() / smoothed_rtt.as_secs_f64();
-        let new_tokens = window as f64 * 1.25 * elapsed_rtts;
-        self.tokens = self
-            .tokens
-            .saturating_add(new_tokens as _)
-            .min(self.capacity);
+        let new_tokens = (window as f64 * 1.25 * elapsed_rtts) as u64;
+        self.tokens = self.tokens.saturating_add(new_tokens).min(self.capacity);
 
-        self.prev = now;
+        // guru patch, from upstream: polled faster than a whole byte accrues, the
+        // truncation above yields nothing — advancing `prev` anyway would throw
+        // that time away and the bucket would never refill.
+        if new_tokens > 0 {
+            self.prev = now;
+        }
 
         // if we can already send a packet, there is no need for delay
         if self.tokens >= bytes_to_send {
@@ -113,6 +133,22 @@
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
@@ -161,18 +197,18 @@
         let rtt = Duration::from_micros(400);
 
         assert!(
-            Pacer::new(rtt, 30000, 1500, new_instant)
-                .delay(Duration::from_micros(0), 0, 1500, 1, old_instant)
+            Pacer::new(rtt, 30000, 1500, None, new_instant)
+                .delay(Duration::from_micros(0), 0, 1500, 1, None, old_instant)
                 .is_none()
         );
         assert!(
-            Pacer::new(rtt, 30000, 1500, new_instant)
-                .delay(Duration::from_micros(0), 1600, 1500, 1, old_instant)
+            Pacer::new(rtt, 30000, 1500, None, new_instant)
+                .delay(Duration::from_micros(0), 1600, 1500, 1, None, old_instant)
                 .is_none()
         );
         assert!(
-            Pacer::new(rtt, 30000, 1500, new_instant)
-                .delay(Duration::from_micros(0), 1500, 1500, 3000, old_instant)
+            Pacer::new(rtt, 30000, 1500, None, new_instant)
+                .delay(Duration::from_micros(0), 1500, 1500, 3000, None, old_instant)
                 .is_none()
         );
     }
@@ -184,18 +220,18 @@
         let rtt = Duration::from_millis(50);
         let now = Instant::now();
 
-        let pacer = Pacer::new(rtt, window, mtu, now);
+        let pacer = Pacer::new(rtt, window, mtu, None, now);
         assert_eq!(
             pacer.capacity,
             (window as u128 * BURST_INTERVAL_NANOS / rtt.as_nanos()) as u64
         );
         assert_eq!(pacer.tokens, pacer.capacity);
 
-        let pacer = Pacer::new(Duration::from_millis(0), window, mtu, now);
+        let pacer = Pacer::new(Duration::from_millis(0), window, mtu, None, now);
         assert_eq!(pacer.capacity, MAX_BURST_SIZE * mtu as u64);
         assert_eq!(pacer.tokens, pacer.capacity);
 
-        let pacer = Pacer::new(rtt, 1, mtu, now);
+        let pacer = Pacer::new(rtt, 1, mtu, None, now);
         assert_eq!(pacer.capacity, MIN_BURST_SIZE * mtu as u64);
         assert_eq!(pacer.tokens, pacer.capacity);
     }
@@ -207,7 +243,7 @@
         let rtt = Duration::from_millis(50);
         let now = Instant::now();
 
-        let mut pacer = Pacer::new(rtt, window, mtu, now);
+        let mut pacer = Pacer::new(rtt, window, mtu, None, now);
         assert_eq!(
             pacer.capacity,
             (window as u128 * BURST_INTERVAL_NANOS / rtt.as_nanos()) as u64
@@ -215,27 +251,27 @@
         assert_eq!(pacer.tokens, pacer.capacity);
         let initial_tokens = pacer.tokens;
 
-        pacer.delay(rtt, mtu as u64, mtu, window * 2, now);
+        pacer.delay(rtt, mtu as u64, mtu, window * 2, None, now);
         assert_eq!(
             pacer.capacity,
             (2 * window as u128 * BURST_INTERVAL_NANOS / rtt.as_nanos()) as u64
         );
         assert_eq!(pacer.tokens, initial_tokens);
 
-        pacer.delay(rtt, mtu as u64, mtu, window / 2, now);
+        pacer.delay(rtt, mtu as u64, mtu, window / 2, None, now);
         assert_eq!(
             pacer.capacity,
             (window as u128 / 2 * BURST_INTERVAL_NANOS / rtt.as_nanos()) as u64
         );
         assert_eq!(pacer.tokens, initial_tokens / 2);
 
-        pacer.delay(rtt, mtu as u64, mtu * 2, window, now);
+        pacer.delay(rtt, mtu as u64, mtu * 2, window, None, now);
         assert_eq!(
             pacer.capacity,
             (window as u128 * BURST_INTERVAL_NANOS / rtt.as_nanos()) as u64
         );
 
-        pacer.delay(rtt, mtu as u64, 20_000, window, now);
+        pacer.delay(rtt, mtu as u64, 20_000, window, None, now);
         assert_eq!(pacer.capacity, 20_000_u64 * MIN_BURST_SIZE);
     }
 
@@ -246,12 +282,12 @@
         let rtt = Duration::from_millis(50);
         let old_instant = Instant::now();
 
-        let mut pacer = Pacer::new(rtt, window, mtu, old_instant);
+        let mut pacer = Pacer::new(rtt, window, mtu, None, old_instant);
         let packet_capacity = pacer.capacity / mtu as u64;
 
         for _ in 0..packet_capacity {
             assert_eq!(
-                pacer.delay(rtt, mtu as u64, mtu, window, old_instant),
+                pacer.delay(rtt, mtu as u64, mtu, window, None, old_instant),
                 None,
                 "When capacity is available packets should be sent immediately"
             );
@@ -263,7 +299,7 @@
 
         assert_eq!(
             pacer
-                .delay(rtt, mtu as u64, mtu, window, old_instant)
+                .delay(rtt, mtu as u64, mtu, window, None, old_instant)
                 .expect("Send must be delayed")
                 .duration_since(old_instant),
             pace_duration
@@ -276,6 +312,7 @@
                 mtu as u64,
                 mtu,
                 window,
+                None,
                 old_instant + pace_duration / 2
             ),
             None
@@ -284,7 +321,7 @@
 
         for _ in 0..packet_capacity / 2 {
             assert_eq!(
-                pacer.delay(rtt, mtu as u64, mtu, window, old_instant),
+                pacer.delay(rtt, mtu as u64, mtu, window, None, old_instant),
                 None,
                 "When capacity is available packets should be sent immediately"
             );
@@ -299,6 +336,7 @@
                 mtu as u64,
                 mtu,
                 window,
+                None,
                 old_instant + pace_duration * 3 / 2
             ),
             None
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
diff --color -ru a/src/connection/streams/recv.rs b/src/connection/streams/recv.rs
--- a/src/connection/streams/recv.rs	2006-07-23 19:21:28.000000000 -0600
+++ b/src/connection/streams/recv.rs	2026-09-16 08:41:28.402418555 -0600
@@ -22,9 +22,12 @@
 
 impl Recv {
     pub(super) fn new(initial_max_data: u64) -> Box<Self> {
+        let mut assembler = Assembler::new();
+        // guru patch: the span cap follows the window, see PATCH.md.
+        assembler.set_window(initial_max_data);
         Box::new(Self {
             state: RecvState::default(),
-            assembler: Assembler::new(),
+            assembler,
             sent_max_stream_data: initial_max_data,
             end: 0,
             stopped: false,
@@ -35,6 +38,7 @@
     pub(super) fn reinit(&mut self, initial_max_data: u64) {
         self.state = RecvState::default();
         self.assembler.reinit();
+        self.assembler.set_window(initial_max_data);
         self.sent_max_stream_data = initial_max_data;
         self.end = 0;
         self.stopped = false;
```
