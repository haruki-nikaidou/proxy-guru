//! Self-update, for a worker installed by `deploy/install.sh`.
//!
//! The installer lays out `/opt/guru-worker/<version>/guru-worker` with a
//! `current` symlink the unit execs through, and keeps the version that was
//! current as `previous`. Updating is: fetch the published binary the master
//! points at, verify its SHA-256, install it under its version, note the swap
//! in `pending`, repoint `current`, and let the process exit — systemd's
//! `Restart=always` starts the new version. The start guard
//! (`deploy/guru-worker-guard`) counts the starts of a pending version and
//! rolls `current` back to `previous` when it never proves itself; the new
//! binary proves itself by clearing `pending` once it has registered and sent
//! a health report, and reports a guard rollback (`failed`) with its next
//! registration. Markers are `key=value` lines shared with the guard.

use sha2::{Digest, Sha256};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;
use tokio::io::AsyncWriteExt;
use tokio::sync::Notify;
use tokio_stream::StreamExt;

/// The update a poll returned.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UpdatePlan {
    pub version: String,
    pub url: String,
    pub sha256: String,
}

/// How a session runs updates.
#[derive(Debug, Clone)]
pub struct UpdateOptions {
    /// Time between two `PollAgentUpdate` calls.
    pub poll: Duration,
    /// Off: an offered update is refused and reported, never installed.
    pub enabled: bool,
    /// `--master`: the only origin an update may be fetched from.
    pub master: String,
    /// Signalled once an update is installed and the process should exit for
    /// systemd to start the new version.
    pub done: Arc<Notify>,
}

const BINARY: &str = "guru-worker";
const CURRENT: &str = "current";
const PREVIOUS: &str = "previous";
const PENDING: &str = "pending";
const FAILED: &str = "failed";
/// The whole download; the binary is ~20 MB and a stalled fetch is retried at
/// the next poll anyway.
const DOWNLOAD_TIMEOUT: Duration = Duration::from_secs(600);

/// The install prefix this binary runs under — `/opt/guru-worker` for
/// `/opt/guru-worker/<version>/guru-worker` — or `None` when the layout is not
/// the installer's, in which case there is nothing to update in place.
///
/// The unit execs `current/guru-worker`; the kernel resolves the symlink, so
/// `current_exe` is the versioned path. Its grandparent must hold a `current`
/// link for this to be an installed tree rather than a hand-run binary.
pub fn install_prefix() -> Option<PathBuf> {
    let exe = std::env::current_exe().ok()?;
    if exe.file_name()? != BINARY {
        return None;
    }
    let prefix = exe.parent()?.parent()?;
    let is_link = std::fs::symlink_metadata(prefix.join(CURRENT))
        .ok()?
        .file_type()
        .is_symlink();
    is_link.then(|| prefix.to_path_buf())
}

/// Fetches, verifies and installs `plan`, and repoints `current` at it. The
/// process must exit afterwards; on error nothing was swapped and only the
/// partial download is cleaned up.
pub async fn apply(plan: &UpdatePlan, opts: &UpdateOptions) -> Result<(), String> {
    if !opts.enabled {
        return Err("self-update is disabled on this host (GURU_NO_SELF_UPDATE)".to_string());
    }
    let origin = opts.master.trim_end_matches('/');
    if !plan.url.starts_with(&format!("{origin}/")) {
        return Err(format!(
            "refusing to fetch {} from outside the master origin {origin}",
            plan.url
        ));
    }
    if plan.version.is_empty() || plan.version.contains('/') || plan.version.starts_with('.') {
        return Err(format!("refusing version {:?}", plan.version));
    }
    let prefix = install_prefix().ok_or_else(|| {
        "not installed under a version directory with a `current` link (install.sh lays that out)"
            .to_string()
    })?;

    let version_dir = prefix.join(&plan.version);
    std::fs::create_dir_all(&version_dir)
        .map_err(|e| format!("creating {}: {e}", version_dir.display()))?;
    let tmp = version_dir.join(format!("{BINARY}.tmp"));
    let target = version_dir.join(BINARY);
    if let Err(error) = download(&plan.url, &tmp, &plan.sha256).await {
        let _ = std::fs::remove_file(&tmp);
        return Err(error);
    }
    install_binary(&tmp, &target).map_err(|e| format!("installing {}: {e}", target.display()))?;
    sync_dir(&version_dir).map_err(|e| format!("syncing {}: {e}", version_dir.display()))?;

    // The version that was current stays reachable as `previous` for the guard.
    if let Ok(current) = std::fs::read_link(prefix.join(CURRENT))
        && current != Path::new(&plan.version)
    {
        replace_link(&prefix, PREVIOUS, &current)
            .map_err(|e| format!("recording the previous version: {e}"))?;
    }
    write_marker(
        &prefix.join(PENDING),
        &format!("version={}\nattempts=0\n", plan.version),
    )
    .map_err(|e| format!("writing the pending marker: {e}"))?;
    replace_link(&prefix, CURRENT, Path::new(&plan.version))
        .map_err(|e| format!("repointing current: {e}"))?;
    sync_dir(&prefix).map_err(|e| format!("syncing {}: {e}", prefix.display()))?;
    tracing::info!(
        from = crate::agent::VERSION,
        to = %plan.version,
        "installed update; `current` now points at it"
    );
    Ok(())
}

/// Streams `url` into `tmp`, hashing as it goes, and refuses a body whose
/// digest is not `expected`.
async fn download(url: &str, tmp: &Path, expected: &str) -> Result<(), String> {
    let client = reqwest::Client::builder()
        .timeout(DOWNLOAD_TIMEOUT)
        .build()
        .map_err(|e| format!("http client: {e}"))?;
    let response = client
        .get(url)
        .send()
        .await
        .map_err(|e| format!("fetching {url}: {e}"))?;
    if !response.status().is_success() {
        return Err(format!("fetching {url}: HTTP {}", response.status()));
    }
    let mut file = tokio::fs::File::create(tmp)
        .await
        .map_err(|e| format!("creating {}: {e}", tmp.display()))?;
    let mut hasher = Sha256::new();
    let mut size: u64 = 0;
    let mut body = response.bytes_stream();
    while let Some(chunk) = body.next().await {
        let chunk = chunk.map_err(|e| format!("reading {url}: {e}"))?;
        hasher.update(&chunk);
        size = size.saturating_add(u64::try_from(chunk.len()).unwrap_or(u64::MAX));
        file.write_all(&chunk)
            .await
            .map_err(|e| format!("writing {}: {e}", tmp.display()))?;
    }
    file.sync_all()
        .await
        .map_err(|e| format!("syncing {}: {e}", tmp.display()))?;
    if size == 0 {
        return Err(format!("fetching {url}: empty body"));
    }
    let digest = format!("{:x}", hasher.finalize());
    if digest != expected.trim().to_ascii_lowercase() {
        return Err(format!(
            "checksum mismatch for {url}: downloaded {digest}, published {expected}"
        ));
    }
    Ok(())
}

/// Makes the verified download executable and moves it into place.
fn install_binary(tmp: &Path, target: &Path) -> std::io::Result<()> {
    use std::os::unix::fs::PermissionsExt;
    std::fs::set_permissions(tmp, std::fs::Permissions::from_mode(0o755))?;
    std::fs::rename(tmp, target)
}

/// Repoints `prefix/name` at `target` atomically: `symlink` refuses to replace
/// an existing link, so the new one is created beside it and renamed over.
fn replace_link(prefix: &Path, name: &str, target: &Path) -> std::io::Result<()> {
    let tmp = prefix.join(format!("{name}.tmp"));
    match std::fs::remove_file(&tmp) {
        Ok(()) => {}
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
        Err(e) => return Err(e),
    }
    std::os::unix::fs::symlink(target, &tmp)?;
    std::fs::rename(&tmp, prefix.join(name))
}

/// Writes a marker through a sibling temp file, then renames it into place.
fn write_marker(path: &Path, text: &str) -> std::io::Result<()> {
    use std::io::Write;
    let mut name = path.file_name().unwrap_or_default().to_owned();
    name.push(".tmp");
    let tmp = path.with_file_name(name);
    {
        let mut file = std::fs::File::create(&tmp)?;
        file.write_all(text.as_bytes())?;
        file.sync_all()?;
    }
    std::fs::rename(&tmp, path)
}

fn sync_dir(dir: &Path) -> std::io::Result<()> {
    std::fs::File::open(dir)?.sync_all()
}

/// The start guard's record of a rolled-back update, consumed: it is reported
/// with the next registration, once.
pub fn take_failed() -> Option<String> {
    let path = install_prefix()?.join(FAILED);
    let text = std::fs::read_to_string(&path).ok()?;
    let field = |key: &str| {
        text.lines()
            .find_map(|line| line.strip_prefix(key))
            .unwrap_or("?")
            .to_string()
    };
    let version = field("version=");
    let error = field("error=");
    if let Err(e) = std::fs::remove_file(&path) {
        tracing::warn!(path = %path.display(), error = %e, "could not remove the failed marker");
    }
    tracing::warn!(%version, %error, "the start guard rolled back an update");
    Some(format!("{version}: {error}"))
}

/// This version has registered and reported: the swap that installed it is
/// over, and the guard has nothing left to count.
pub fn confirm_pending() {
    let Some(prefix) = install_prefix() else {
        return;
    };
    let path = prefix.join(PENDING);
    match std::fs::remove_file(&path) {
        Ok(()) => tracing::info!(
            version = crate::agent::VERSION,
            "update confirmed: registered and reporting health"
        ),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
        Err(e) => tracing::warn!(path = %path.display(), error = %e, "could not clear the pending marker"),
    }
}
