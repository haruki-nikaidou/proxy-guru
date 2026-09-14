//! Certificate material delivered alongside a config revision.
//!
//! The master names every file a config references relative to the worker's state
//! directory and ships the current content with the revision. The files are written
//! before the config is applied, so a listener's `key` / `full_chain` pair is always
//! on disk — and always a matching pair — when the supervisor compiles it, including
//! when the last-known-good config is replayed after a restart.

use crate::BoxError;
use guru_worker_config::{Config, Forwarding, ListenAs, RelayHost};
use rpguru_sdk::orchestration_agent::CertificateFile;
use std::collections::{BTreeMap, HashSet};
use std::path::{Component, Path, PathBuf};

/// Where delivered files live under the state directory; nothing else is pruned.
pub const CERTS_DIR: &str = "certs";

const NEW_SUFFIX: &str = ".new";
const OLD_SUFFIX: &str = ".old";
const TMP_SUFFIX: &str = ".tmp";

/// Resolves a delivered path under the state directory, refusing anything that could
/// escape it.
fn resolve(state_dir: &Path, relative: &str) -> Result<PathBuf, BoxError> {
    let path = Path::new(relative);
    if path.as_os_str().is_empty() || !path.components().all(|c| matches!(c, Component::Normal(_)))
    {
        return Err(
            format!("certificate file path {relative:?} is not a plain relative path").into(),
        );
    }
    Ok(state_dir.join(path))
}

fn is_private_key(path: &Path) -> bool {
    path.file_name()
        .and_then(|n| n.to_str())
        .is_some_and(|n| n.ends_with("key.pem"))
}

fn sibling(dir: &Path, suffix: &str) -> PathBuf {
    let mut name = dir.file_name().map(ToOwned::to_owned).unwrap_or_default();
    name.push(suffix);
    dir.with_file_name(name)
}

/// Writes one file with its final permissions and flushes it to disk.
fn write_file(path: &Path, pem: &str, private: bool) -> Result<(), BoxError> {
    use std::io::Write;
    let mut options = std::fs::OpenOptions::new();
    options.write(true).create(true).truncate(true);
    if private {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let mut file = options.open(path)?;
    if private {
        // An existing file keeps the mode it was created with; make sure of it.
        use std::os::unix::fs::PermissionsExt;
        file.set_permissions(std::fs::Permissions::from_mode(0o600))?;
    }
    file.write_all(pem.as_bytes())?;
    file.sync_all()?;
    Ok(())
}

fn sync_dir(dir: &Path) -> Result<(), BoxError> {
    std::fs::File::open(dir)?.sync_all()?;
    Ok(())
}

/// Replaces a single file atomically: written to a sibling temp file, then renamed.
fn replace_file(path: &Path, pem: &str) -> Result<(), BoxError> {
    let dir = path.parent().ok_or("certificate file has no parent")?;
    std::fs::create_dir_all(dir)?;
    let tmp = sibling(path, TMP_SUFFIX);
    write_file(&tmp, pem, is_private_key(path))?;
    std::fs::rename(&tmp, path)?;
    sync_dir(dir)
}

/// Replaces a directory of files atomically: every file is written into `<dir>.new`,
/// which is then swapped in. A crash between the two renames leaves `<dir>.old`
/// beside a missing `<dir>`, which [`recover`] puts back at the next start; no state
/// ever pairs one file's old content with another's new content.
///
/// Returns whether the previous content is still beside the directory as `<dir>.old`;
/// [`Written::settle`] decides what becomes of it.
fn replace_dir(dir: &Path, files: &[(&Path, &str)]) -> Result<bool, BoxError> {
    let parent = dir.parent().ok_or("certificate directory has no parent")?;
    std::fs::create_dir_all(parent)?;
    let new = sibling(dir, NEW_SUFFIX);
    let old = sibling(dir, OLD_SUFFIX);
    if new.exists() {
        std::fs::remove_dir_all(&new)?;
    }
    std::fs::create_dir(&new)?;
    for (path, pem) in files {
        let name = path.file_name().ok_or("certificate file has no name")?;
        write_file(&new.join(name), pem, is_private_key(path))?;
    }
    sync_dir(&new)?;
    if old.exists() {
        std::fs::remove_dir_all(&old)?;
    }
    let had_previous = dir.exists();
    if had_previous {
        std::fs::rename(dir, &old)?;
    }
    std::fs::rename(&new, dir)?;
    sync_dir(parent)?;
    Ok(had_previous)
}

/// Puts a swapped directory's previous content back: the new content is staged
/// aside as `<dir>.new` — which [`recover`] discards — before `<dir>.old` returns.
fn unswap_dir(dir: &Path) -> Result<(), BoxError> {
    let new = sibling(dir, NEW_SUFFIX);
    let old = sibling(dir, OLD_SUFFIX);
    if new.exists() {
        std::fs::remove_dir_all(&new)?;
    }
    std::fs::rename(dir, &new)?;
    std::fs::rename(&old, dir)?;
    std::fs::remove_dir_all(&new)?;
    if let Some(parent) = dir.parent() {
        sync_dir(parent)?;
    }
    Ok(())
}

/// The directories a [`write_files`] call swapped over existing content. Each still
/// has its previous content beside it as `<dir>.old` until [`Written::settle`] runs.
#[derive(Debug, Default)]
#[must_use = "settle the swapped directories once the apply outcome is known"]
pub struct Written {
    swapped: Vec<PathBuf>,
}

impl Written {
    /// The directories whose previous content is still on disk.
    pub fn swapped(&self) -> &[PathBuf] {
        &self.swapped
    }

    /// Decides each swapped directory: those `restore` selects get their previous
    /// content back — a listener that kept running its old shape after failing to
    /// apply must find the material it runs when the last-known-good config is
    /// replayed — and the rest drop it.
    pub fn settle(self, restore: impl Fn(&Path) -> bool) {
        for dir in self.swapped {
            let result = if restore(&dir) {
                tracing::warn!(path = %dir.display(), "keeping previous certificate material for a listener that did not take the new one");
                unswap_dir(&dir)
            } else {
                std::fs::remove_dir_all(sibling(&dir, OLD_SUFFIX)).map_err(Into::into)
            };
            if let Err(e) = result {
                tracing::error!(path = %dir.display(), error = %e, "could not settle certificate directory");
            }
        }
    }
}

/// Writes every delivered file under `state_dir`.
///
/// Files sharing a directory — a certificate's `full_chain.pem` and `key.pem` — are
/// swapped in together so the pair is never observable half-updated; a directory's
/// lone file (the relay CA) is replaced through a temp file and a rename. Key files
/// are created mode `0600`. On error every directory already swapped is put back, so
/// the disk still matches the listeners that keep running.
pub fn write_files(state_dir: &Path, files: &[CertificateFile]) -> Result<Written, BoxError> {
    let mut by_dir: BTreeMap<PathBuf, Vec<(PathBuf, &str)>> = BTreeMap::new();
    for file in files {
        let path = resolve(state_dir, &file.path)?;
        let dir = path
            .parent()
            .ok_or("certificate file has no parent")?
            .to_path_buf();
        by_dir.entry(dir).or_default().push((path, &file.pem));
    }
    let mut written = Written::default();
    for (dir, files) in &by_dir {
        let result = match files.as_slice() {
            [(path, pem)] => {
                replace_file(path, pem).map_err(|e| format!("write {}: {e}", path.display()))
            }
            files => {
                let files: Vec<(&Path, &str)> =
                    files.iter().map(|(p, pem)| (p.as_path(), *pem)).collect();
                replace_dir(dir, &files)
                    .map(|had_previous| {
                        if had_previous {
                            written.swapped.push(dir.clone());
                        }
                    })
                    .map_err(|e| format!("write {}: {e}", dir.display()))
            }
        };
        if let Err(e) = result {
            written.settle(|_| true);
            return Err(e.into());
        }
    }
    Ok(written)
}

fn strip_suffix(path: &Path, suffix: &str) -> Option<PathBuf> {
    let name = path.file_name()?.to_str()?.strip_suffix(suffix)?;
    Some(path.with_file_name(name))
}

/// Finishes or undoes directory swaps a crash interrupted, and removes the temp files
/// and directories of writes that never completed. Run before the last-known-good
/// config is replayed, so every certificate it names is a complete pair.
pub fn recover(state_dir: &Path) {
    fn walk(dir: &Path) {
        let Ok(entries) = std::fs::read_dir(dir) else {
            return;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            let is_dir = entry.file_type().is_ok_and(|t| t.is_dir());
            if is_dir {
                if let Some(target) = strip_suffix(&path, OLD_SUFFIX) {
                    let result = if target.exists() {
                        std::fs::remove_dir_all(&path)
                    } else {
                        tracing::warn!(path = %target.display(), "restoring certificate directory an interrupted swap had moved aside");
                        std::fs::rename(&path, &target)
                    };
                    if let Err(e) = result {
                        tracing::error!(path = %path.display(), error = %e, "could not recover certificate directory");
                    }
                } else if strip_suffix(&path, NEW_SUFFIX).is_some() {
                    if let Err(e) = std::fs::remove_dir_all(&path) {
                        tracing::error!(path = %path.display(), error = %e, "could not remove unfinished certificate directory");
                    }
                } else {
                    walk(&path);
                }
            } else if strip_suffix(&path, TMP_SUFFIX).is_some()
                && let Err(e) = std::fs::remove_file(&path)
            {
                tracing::error!(path = %path.display(), error = %e, "could not remove unfinished certificate file");
            }
        }
    }
    walk(&state_dir.join(CERTS_DIR));
}

/// The certificate files one forwarding's listener reads.
pub fn forwarding_paths(f: &Forwarding) -> impl Iterator<Item = &Path> {
    let host = match &f.listen_as {
        ListenAs::Raw | ListenAs::Relay(RelayHost::Tcp) => None,
        ListenAs::Tls(host)
        | ListenAs::Relay(RelayHost::TlsOverTcp(host) | RelayHost::Quic(host)) => Some(host),
    };
    host.into_iter()
        .flat_map(|h| [h.key.as_path(), h.full_chain.as_path()])
}

/// Every certificate path a config references, as the config states them.
pub fn referenced_paths(cfg: &Config) -> HashSet<PathBuf> {
    let mut paths: HashSet<PathBuf> = cfg
        .forwardings
        .iter()
        .flat_map(forwarding_paths)
        .map(Path::to_path_buf)
        .collect();
    if let Some(ca) = &cfg.relay_ca {
        paths.insert(ca.clone());
    }
    paths
}

/// Removes every file under `<state_dir>/certs` that the running config does not
/// reference, then the directories that emptied. `cfg` must already have its paths
/// resolved against `state_dir`.
pub fn prune(state_dir: &Path, cfg: &Config) {
    /// Returns whether `dir` is empty afterwards.
    fn walk(dir: &Path, keep: &HashSet<PathBuf>) -> bool {
        let Ok(entries) = std::fs::read_dir(dir) else {
            return false;
        };
        let mut empty = true;
        for entry in entries.flatten() {
            let path = entry.path();
            if entry.file_type().is_ok_and(|t| t.is_dir()) {
                if walk(&path, keep) {
                    if let Err(e) = std::fs::remove_dir(&path) {
                        tracing::warn!(path = %path.display(), error = %e, "could not remove empty certificate directory");
                        empty = false;
                    }
                } else {
                    empty = false;
                }
            } else if keep.contains(&path) {
                empty = false;
            } else if let Err(e) = std::fs::remove_file(&path) {
                tracing::warn!(path = %path.display(), error = %e, "could not remove stale certificate file");
                empty = false;
            } else {
                tracing::info!(path = %path.display(), "removed certificate file no longer referenced");
            }
        }
        empty
    }
    let root = state_dir.join(CERTS_DIR);
    if root.is_dir() {
        // The root itself stays: the next revision writes into it.
        walk(&root, &referenced_paths(cfg));
    }
}
