//! Remote shell: an Admin runs commands on this host from the dashboard.
//!
//! Only a host that opted in locally (`--remote-shell`) ever gets here; the master
//! cannot turn it on. A session is one `bash --noprofile --norc` in a process group
//! of its own, fed one command at a time on stdin, with everything it prints kept
//! in a bounded transcript ([`ring`]). The [`ShellTable`] holding the sessions
//! lives as long as the worker process: sessions outlive the agent sessions and
//! `ShellChannel` streams the master reaches them through ([`channel`]), and end
//! only when closed, reaped as idle, exited, or when the worker stops.

pub mod channel;
mod ring;
mod sentinel;
mod session;

use rpguru_sdk::orchestration_agent::{
    ShellCloseReason, ShellDone, ShellError, ShellErrorCode, ShellReply, ShellRequest,
    ShellSessionInfo, ShellSessionList, shell_reply, shell_request,
};
use session::{Refusal, Session, Viewer};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

/// How long [`ShellTable::shutdown`] gives the watches to report the end.
const SHUTDOWN_FLUSH: Duration = Duration::from_secs(1);

/// The worker-local remote-shell settings (`--remote-shell-*`).
#[derive(Debug, Clone)]
pub struct ShellSettings {
    /// The `bash` every session runs, resolved once at startup.
    pub bash: PathBuf,
    /// The transcript budget of one session.
    pub buffer_bytes: usize,
    /// How long a session may sit with no command running and no viewer.
    pub idle_timeout: Duration,
    /// How many sessions may be open at once.
    pub max_sessions: usize,
}

/// The first `bash` on `path` (a `PATH` value): an executable file in an absolute
/// directory. Relative entries are skipped, so the worker's working directory
/// cannot decide what runs.
pub fn find_bash(path: Option<&std::ffi::OsStr>) -> Result<PathBuf, crate::BoxError> {
    path.map(std::env::split_paths)
        .into_iter()
        .flatten()
        .filter(|dir| dir.is_absolute())
        .map(|dir| dir.join("bash"))
        .find(|candidate| executable(candidate))
        .ok_or_else(|| {
            "--remote-shell runs `bash`, and there is none on PATH; install it or put its directory on the worker's PATH"
                .into()
        })
}

fn executable(path: &Path) -> bool {
    use std::os::unix::fs::PermissionsExt;
    std::fs::metadata(path)
        .is_ok_and(|meta| meta.is_file() && meta.permissions().mode() & 0o111 != 0)
}

/// Every remote-shell session of this worker process. Cheap to clone; when the
/// last clone goes, every session still open is killed.
#[derive(Clone)]
pub struct ShellTable(Arc<Table>);

struct Table {
    settings: ShellSettings,
    sessions: parking_lot::Mutex<HashMap<String, Arc<Session>>>,
    /// How many watches hold a session right now, across every channel.
    viewers: tokio::sync::watch::Sender<usize>,
    reaper: tokio::task::AbortHandle,
}

impl Drop for Table {
    fn drop(&mut self) {
        self.reaper.abort();
        for (_, session) in self.sessions.get_mut().drain() {
            session.end(ShellCloseReason::Shutdown);
        }
    }
}

impl ShellTable {
    /// An empty table and its idle reaper. Needs a Tokio runtime.
    pub fn new(settings: ShellSettings) -> Self {
        let tick = (settings.idle_timeout.checked_div(4).unwrap_or_default())
            .clamp(Duration::from_millis(50), Duration::from_secs(5));
        Self(Arc::new_cyclic(|table: &std::sync::Weak<Table>| {
            let weak = table.clone();
            let reaper = tokio::spawn(async move {
                let mut ticker = tokio::time::interval(tick);
                ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
                loop {
                    ticker.tick().await;
                    let Some(table) = weak.upgrade() else {
                        return;
                    };
                    table.reap();
                }
            });
            Table {
                settings,
                sessions: parking_lot::Mutex::default(),
                viewers: tokio::sync::watch::Sender::new(0),
                reaper: reaper.abort_handle(),
            }
        }))
    }

    /// Answers one dashboard request.
    pub async fn request(&self, request: ShellRequest) -> ShellReply {
        let result = match request.op {
            Some(shell_request::Op::Open(_)) => self.open().map(shell_reply::Result::Opened),
            Some(shell_request::Op::Exec(exec)) => self
                .exec(&exec.session_id, &exec.command)
                .await
                .map(|()| shell_reply::Result::Done(ShellDone {})),
            Some(shell_request::Op::Close(close)) => self
                .close(&close.session_id)
                .map(|()| shell_reply::Result::Done(ShellDone {})),
            Some(shell_request::Op::List(_)) => Ok(shell_reply::Result::Sessions(self.list())),
            None => Err(Refusal::new(
                ShellErrorCode::Invalid,
                "the request names no operation",
            )),
        };
        ShellReply {
            request_id: request.request_id,
            result: Some(result.unwrap_or_else(|refusal| {
                shell_reply::Result::Error(ShellError {
                    code: refusal.code.into(),
                    message: refusal.message,
                })
            })),
        }
    }

    /// Kills every session (`SHUTDOWN`), then gives the watches a moment to tell
    /// the master. For the worker's way out: a signal or an installed update.
    pub async fn shutdown(&self) {
        let sessions: Vec<_> = self.0.sessions.lock().drain().map(|(_, s)| s).collect();
        if sessions.is_empty() {
            return;
        }
        for session in &sessions {
            session.end(ShellCloseReason::Shutdown);
        }
        let mut viewers = self.0.viewers.subscribe();
        let _ = tokio::time::timeout(SHUTDOWN_FLUSH, viewers.wait_for(|n| *n == 0)).await;
    }

    /// Counts a watch on session `id` until the returned guard is dropped; `None`
    /// for a session that is not (or no longer) open.
    fn viewer(&self, id: &str) -> Option<Viewer> {
        let session = self.0.sessions.lock().get(id).cloned()?;
        Some(session.viewer(&self.0.viewers))
    }

    fn open(&self) -> Result<ShellSessionInfo, Refusal> {
        let mut sessions = self.0.sessions.lock();
        let cap = self.0.settings.max_sessions;
        if sessions.len() >= cap {
            return Err(Refusal::new(
                ShellErrorCode::Limit,
                format!(
                    "this worker allows {cap} shell sessions at once (--remote-shell-max-sessions)"
                ),
            ));
        }
        let (session, pipes) = Session::spawn(&self.0.settings).map_err(|e| {
            Refusal::new(
                ShellErrorCode::Failed,
                format!("could not start {}: {e}", self.0.settings.bash.display()),
            )
        })?;
        sessions.insert(session.id.clone(), session.clone());
        drop(sessions);
        tracing::info!(session = %session.id, "shell session opened");
        let info = session.info();
        tokio::spawn(session::drive(session, pipes, Arc::downgrade(&self.0)));
        Ok(info)
    }

    async fn exec(&self, id: &str, command: &str) -> Result<(), Refusal> {
        if command.trim().is_empty() {
            return Err(Refusal::new(
                ShellErrorCode::Invalid,
                "the command is empty",
            ));
        }
        if command.contains('\0') {
            return Err(Refusal::new(
                ShellErrorCode::Invalid,
                "the command contains a NUL byte",
            ));
        }
        if command.len() > session::MAX_COMMAND_BYTES {
            return Err(Refusal::new(
                ShellErrorCode::Invalid,
                format!(
                    "the command is longer than {} bytes",
                    session::MAX_COMMAND_BYTES
                ),
            ));
        }
        let session = self
            .0
            .sessions
            .lock()
            .get(id)
            .cloned()
            .ok_or_else(Refusal::not_found)?;
        session.exec(command).await
    }

    fn close(&self, id: &str) -> Result<(), Refusal> {
        let session = self
            .0
            .sessions
            .lock()
            .remove(id)
            .ok_or_else(Refusal::not_found)?;
        session.end(ShellCloseReason::Closed);
        Ok(())
    }

    fn list(&self) -> ShellSessionList {
        let mut sessions: Vec<ShellSessionInfo> = self
            .0
            .sessions
            .lock()
            .values()
            .map(|session| session.info())
            .collect();
        sessions.sort_by(|a, b| (a.opened_at, &a.session_id).cmp(&(b.opened_at, &b.session_id)));
        ShellSessionList { sessions }
    }
}

impl Table {
    /// Ends every session idle for the whole timeout.
    fn reap(&self) {
        let now = tokio::time::Instant::now();
        let timeout = self.settings.idle_timeout;
        let idle: Vec<Arc<Session>> = self
            .sessions
            .lock()
            .extract_if(|_, session| session.idle_for(now).is_some_and(|idle| idle >= timeout))
            .map(|(_, session)| session)
            .collect();
        for session in idle {
            session.end(ShellCloseReason::Idle);
        }
    }

    /// Drops `session` from the table once it ended on its own.
    fn forget(&self, session: &Arc<Session>) {
        let mut sessions = self.sessions.lock();
        if sessions
            .get(&session.id)
            .is_some_and(|held| Arc::ptr_eq(held, session))
        {
            sessions.remove(&session.id);
        }
    }
}

/// 128 random bits as lowercase hex: session ids and sentinel nonces.
fn random_hex() -> String {
    use std::fmt::Write;
    rand::random::<[u8; 16]>()
        .iter()
        .fold(String::with_capacity(32), |mut out, byte| {
            let _ = write!(out, "{byte:02x}");
            out
        })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bash_is_found_on_path_and_relative_entries_are_skipped() {
        let path = std::env::var_os("PATH");
        let bash = find_bash(path.as_deref());
        assert!(
            bash.as_deref()
                .is_ok_and(|bash| bash.is_absolute() && executable(bash)),
            "{bash:?}"
        );
        assert!(find_bash(Some(std::ffi::OsStr::new("relative/bin:bin"))).is_err());
        assert!(find_bash(Some(std::ffi::OsStr::new("/nonexistent-guru"))).is_err());
        assert!(find_bash(None).is_err());
    }

    #[test]
    fn random_hex_is_32_lowercase_hex_digits() {
        let a = random_hex();
        assert_eq!(a.len(), 32);
        assert!(
            a.bytes()
                .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
        );
        assert_ne!(a, random_hex());
    }
}
