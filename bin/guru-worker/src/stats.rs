//! Traffic counters behind the health report.
//!
//! Byte counters are keyed by tag and live in a registry the supervisor owns, so a
//! hot-swapped or re-bound listener keeps accumulating into the same counters and a
//! health report never loses the bytes moved during a revision. The connection gauge
//! is server-wide: one current/high-water pair every connection of every tag updates,
//! so `max_connections` is the most connections open at once on the whole worker.

use parking_lot::Mutex;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

/// Server-wide gauge of open connections.
#[derive(Default)]
struct Connections {
    /// Connections open right now.
    current: AtomicU64,
    /// Most connections open at once since the last snapshot.
    max: AtomicU64,
}

/// Counters of one `[[forwarding]]`, shared by every connection it serves.
pub struct TagStats {
    /// Client → target bytes since the last snapshot.
    upload: AtomicU64,
    /// Target → client bytes since the last snapshot.
    download: AtomicU64,
    /// The worker's connection gauge, shared with every other tag.
    connections: Arc<Connections>,
}

impl TagStats {
    pub fn add_upload(&self, n: u64) {
        self.upload.fetch_add(n, Ordering::Relaxed);
    }

    pub fn add_download(&self, n: u64) {
        self.download.fetch_add(n, Ordering::Relaxed);
    }

    /// Counts one connection as open until the returned guard is dropped.
    pub fn open(&self) -> ConnectionGuard {
        let connections = &self.connections;
        let now = connections
            .current
            .fetch_add(1, Ordering::Relaxed)
            .saturating_add(1);
        connections.max.fetch_max(now, Ordering::Relaxed);
        ConnectionGuard {
            connections: connections.clone(),
        }
    }
}

/// Decrements the server-wide connection gauge when dropped.
pub struct ConnectionGuard {
    connections: Arc<Connections>,
}

impl Drop for ConnectionGuard {
    fn drop(&mut self) {
        self.connections.current.fetch_sub(1, Ordering::Relaxed);
    }
}

/// One health report's worth of traffic across every tag.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Snapshot {
    /// Bytes moved client → target since the previous snapshot.
    pub upload_bytes: u64,
    /// Bytes moved target → client since the previous snapshot.
    pub download_bytes: u64,
    /// Connections open at the time of the snapshot.
    pub current_connections: u64,
    /// Most connections open at once, across every tag, since the previous snapshot.
    pub max_connections: u64,
}

/// Registry of per-tag counters plus the server-wide connection gauge.
#[derive(Default)]
pub struct Stats {
    tags: Mutex<HashMap<String, Arc<TagStats>>>,
    connections: Arc<Connections>,
}

impl Stats {
    /// The counters of `tag`, created on first use.
    pub fn tag(&self, tag: &str) -> Arc<TagStats> {
        let mut tags = self.tags.lock();
        match tags.get(tag) {
            Some(stats) => stats.clone(),
            None => {
                let stats = Arc::new(TagStats {
                    upload: AtomicU64::new(0),
                    download: AtomicU64::new(0),
                    connections: self.connections.clone(),
                });
                tags.insert(tag.to_string(), stats.clone());
                stats
            }
        }
    }

    /// Forgets the counters of every tag not in `keep`.
    pub fn retain<F: Fn(&str) -> bool>(&self, keep: F) {
        self.tags.lock().retain(|tag, _| keep(tag));
    }

    /// Reads every counter and starts the next interval: byte counters go back to
    /// zero and the high-water mark restarts at the connections open right now.
    pub fn snapshot_and_reset(&self) -> Snapshot {
        let tags = self.tags.lock();
        let mut snap = Snapshot::default();
        for stats in tags.values() {
            let upload = stats.upload.swap(0, Ordering::Relaxed);
            let download = stats.download.swap(0, Ordering::Relaxed);
            snap.upload_bytes = snap.upload_bytes.saturating_add(upload);
            snap.download_bytes = snap.download_bytes.saturating_add(download);
        }
        let current = self.connections.current.load(Ordering::Relaxed);
        let max = self.connections.max.swap(current, Ordering::Relaxed);
        snap.current_connections = current;
        snap.max_connections = max.max(current);
        snap
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn max_connections_is_the_server_wide_peak_not_the_sum_of_tag_peaks() {
        let stats = Stats::default();
        let (a, b) = (stats.tag("a"), stats.tag("b"));

        // One connection per tag, never open at the same time: peak is 1, not 2.
        drop(a.open());
        drop(b.open());
        let snap = stats.snapshot_and_reset();
        assert_eq!((snap.max_connections, snap.current_connections), (1, 0));

        // Two open across tags at once, one closed again: peak 2, current 1, and the
        // next interval restarts its high-water mark at what is still open.
        let open_a = a.open();
        let open_b = b.open();
        drop(open_b);
        let snap = stats.snapshot_and_reset();
        assert_eq!((snap.max_connections, snap.current_connections), (2, 1));
        let snap = stats.snapshot_and_reset();
        assert_eq!((snap.max_connections, snap.current_connections), (1, 1));
        drop(open_a);
    }
}
