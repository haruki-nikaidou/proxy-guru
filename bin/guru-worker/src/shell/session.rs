//! One remote-shell session: a `bash` process in its own process group, the
//! transcript of everything it printed, and the bookkeeping the table and the
//! watches read.

use super::ring::{Entry, Record, Ring, Stream};
use super::sentinel::{Piece, Scanner};
use super::{ShellSettings, Table};
use rpguru_sdk::orchestration_agent::{ShellCloseReason, ShellErrorCode, ShellSessionInfo};
use std::io;
use std::path::PathBuf;
use std::process::{ExitStatus, Stdio};
use std::sync::{Arc, Weak};
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::process::{Child, ChildStderr, ChildStdin, ChildStdout};
use tokio::time::Instant;

/// The longest command accepted.
pub const MAX_COMMAND_BYTES: usize = 64 * 1024;
/// How much one pipe read takes at most.
const READ_CHUNK: usize = 4096;
/// How long the output still in the pipes may take to arrive once the shell is
/// gone. Whatever escaped its process group and still holds a pipe open (a
/// daemon that called `setsid`) cannot keep the session alive past this.
const DRAIN: Duration = Duration::from_secs(2);
/// Writing a command to an idle shell takes no time at all; one that does not
/// read its input for this long is broken.
const WRITE_TIMEOUT: Duration = Duration::from_secs(10);

/// A refused request: the code the reply carries and the message for a human.
#[derive(Debug)]
pub struct Refusal {
    pub code: ShellErrorCode,
    pub message: String,
}

impl Refusal {
    pub fn new(code: ShellErrorCode, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
        }
    }

    pub fn not_found() -> Self {
        Self::new(
            ShellErrorCode::NotFound,
            "no such shell session: closed, reaped, or the worker restarted since",
        )
    }
}

pub struct Session {
    pub id: String,
    /// Unix seconds.
    opened_at: i64,
    /// The shell's pid, which is also its process group's id (`process_group(0)`).
    /// Always above 1, so `kill(-pgid)` can never mean "every process".
    pgid: libc::pid_t,
    /// Printed literally into every sentinel; never a shell variable.
    nonce: String,
    stdin: tokio::sync::Mutex<ChildStdin>,
    state: parking_lot::Mutex<State>,
    /// Bumped on every change a watch may care about: new records, the end.
    changed: tokio::sync::watch::Sender<()>,
}

struct State {
    ring: Ring,
    /// The command running now.
    running: Option<String>,
    /// The running command's sentinels seen so far: stdout's carries the code.
    stdout_code: Option<i32>,
    stderr_done: bool,
    viewers: u32,
    /// Since when the session has had no command running and no viewer.
    idle_since: Instant,
    closed: Option<ShellCloseReason>,
}

/// What one read hands a watch.
pub struct Batch {
    pub entries: Vec<(u64, Entry)>,
    /// The position after the last entry.
    pub next: u64,
    /// More is held past `next`.
    pub more: bool,
    /// The session ended and `next` is the end of its transcript.
    pub closed: Option<ShellCloseReason>,
}

/// The pipes of a running shell, handed to [`drive`].
pub struct Pipes {
    child: Child,
    stdout: ChildStdout,
    stderr: ChildStderr,
}

/// A watch holding a session: counted as a viewer, which keeps the idle timeout
/// away, until dropped.
pub struct Viewer {
    session: Arc<Session>,
    live: tokio::sync::watch::Sender<usize>,
}

impl Viewer {
    pub fn session(&self) -> &Arc<Session> {
        &self.session
    }
}

impl Drop for Viewer {
    fn drop(&mut self) {
        {
            let mut state = self.session.state.lock();
            state.viewers = state.viewers.saturating_sub(1);
            if state.viewers == 0 {
                state.idle_since = Instant::now();
            }
        }
        self.live.send_modify(|n| *n = n.saturating_sub(1));
    }
}

impl Session {
    /// Starts `bash --noprofile --norc` in `$HOME` (`/` when that is not a
    /// directory), in a process group of its own, without the operator API key.
    pub fn spawn(settings: &ShellSettings) -> io::Result<(Arc<Self>, Pipes)> {
        let mut child = tokio::process::Command::new(&settings.bash)
            .args(["--noprofile", "--norc"])
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .env_remove(crate::cli::API_KEY_ENV)
            .current_dir(start_dir())
            .process_group(0)
            .kill_on_drop(true)
            .spawn()?;
        let pgid = child
            .id()
            .and_then(|pid| libc::pid_t::try_from(pid).ok())
            .filter(|pid| *pid > 1)
            .ok_or_else(|| io::Error::other("the shell has no usable process id"))?;
        let missing = || io::Error::other("the shell's pipes were not set up");
        let stdin = child.stdin.take().ok_or_else(missing)?;
        let stdout = child.stdout.take().ok_or_else(missing)?;
        let stderr = child.stderr.take().ok_or_else(missing)?;
        let opened_at = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |d| i64::try_from(d.as_secs()).unwrap_or(i64::MAX));
        let session = Arc::new(Self {
            id: super::random_hex(),
            opened_at,
            pgid,
            nonce: super::random_hex(),
            stdin: tokio::sync::Mutex::new(stdin),
            state: parking_lot::Mutex::new(State {
                ring: Ring::new(settings.buffer_bytes),
                running: None,
                stdout_code: None,
                stderr_done: false,
                viewers: 0,
                idle_since: Instant::now(),
                closed: None,
            }),
            changed: tokio::sync::watch::Sender::new(()),
        });
        Ok((
            session,
            Pipes {
                child,
                stdout,
                stderr,
            },
        ))
    }

    pub fn info(&self) -> ShellSessionInfo {
        let state = self.state.lock();
        ShellSessionInfo {
            session_id: self.id.clone(),
            opened_at: self.opened_at,
            running: state.running.clone(),
            end_offset: state.ring.end(),
            viewers: state.viewers,
        }
    }

    /// Writes `command` to the shell, wrapped so that it reads EOF on stdin and is
    /// followed by both sentinels. Returns once the shell has it.
    pub async fn exec(&self, command: &str) -> Result<(), Refusal> {
        {
            let mut state = self.state.lock();
            if state.closed.is_some() {
                return Err(Refusal::not_found());
            }
            if state.running.is_some() {
                return Err(Refusal::new(
                    ShellErrorCode::Busy,
                    "a command is still running in this session",
                ));
            }
            state.running = Some(command.to_owned());
            state.stdout_code = None;
            state.stderr_done = false;
            state.ring.push(Record::Started {
                command: command.to_owned(),
            });
        }
        self.changed.send_replace(());

        let line = format!(
            "eval -- '{}' </dev/null; printf '\\n%s %d\\n' {nonce} \"$?\"; printf '\\n%s\\n' {nonce} >&2\n",
            command.replace('\'', r"'\''"),
            nonce = self.nonce,
        );
        let mut stdin = self.stdin.lock().await;
        let write = async {
            stdin.write_all(line.as_bytes()).await?;
            stdin.flush().await
        };
        match tokio::time::timeout(WRITE_TIMEOUT, write).await {
            Ok(Ok(())) => Ok(()),
            // The shell is gone; its driver ends the session and reports how.
            Ok(Err(e)) => Err(Refusal::new(
                ShellErrorCode::Failed,
                format!("the shell did not take the command: {e}"),
            )),
            Err(_) => {
                // Half a command may sit in its input: the session cannot be
                // trusted with another one.
                self.end(ShellCloseReason::Closed);
                Err(Refusal::new(
                    ShellErrorCode::Failed,
                    format!(
                        "the shell did not read the command within {}s; the session was closed",
                        WRITE_TIMEOUT.as_secs()
                    ),
                ))
            }
        }
    }

    /// Ends the session for `reason` and kills its process group. Only the first
    /// end counts; returns whether this was it.
    pub fn end(&self, reason: ShellCloseReason) -> bool {
        {
            let mut state = self.state.lock();
            if state.closed.is_some() {
                return false;
            }
            state.closed = Some(reason);
        }
        self.kill_group();
        self.changed.send_replace(());
        tracing::info!(session = %self.id, reason = reason.as_str_name(), "shell session ended");
        true
    }

    fn kill_group(&self) {
        // SAFETY: `kill` takes plain integers and touches no memory of ours. `pgid`
        // is above 1 (so negating it cannot overflow), and the negative pid names
        // this session's process group and nothing wider.
        unsafe {
            libc::kill(self.pgid.wrapping_neg(), libc::SIGKILL);
        }
    }

    /// How long the session has been idle, `None` while it is not (a command
    /// runs, a viewer is attached, or it already ended).
    pub fn idle_for(&self, now: Instant) -> Option<Duration> {
        let state = self.state.lock();
        (state.closed.is_none() && state.running.is_none() && state.viewers == 0)
            .then(|| now.saturating_duration_since(state.idle_since))
    }

    /// Counts a new viewer until the returned guard is dropped.
    pub fn viewer(self: &Arc<Self>, live: &tokio::sync::watch::Sender<usize>) -> Viewer {
        {
            let mut state = self.state.lock();
            state.viewers = state.viewers.saturating_add(1);
        }
        live.send_modify(|n| *n = n.saturating_add(1));
        Viewer {
            session: self.clone(),
            live: live.clone(),
        }
    }

    pub fn subscribe(&self) -> tokio::sync::watch::Receiver<()> {
        self.changed.subscribe()
    }

    /// The transcript from `cursor` on, about `max_bytes` of output at most.
    pub fn read(&self, cursor: u64, max_bytes: usize) -> Batch {
        let state = self.state.lock();
        let mut entries = Vec::new();
        let next = state.ring.read(cursor, max_bytes, &mut entries);
        let more = next < state.ring.end();
        Batch {
            entries,
            next,
            more,
            closed: state.closed.filter(|_| !more),
        }
    }

    /// The end of the transcript.
    pub fn end_offset(&self) -> u64 {
        self.state.lock().ring.end()
    }

    /// Takes one read of `pipe` into the transcript.
    fn take(&self, pipe: &mut Pipe, read: io::Result<usize>, buf: &[u8]) {
        let chunk = match read {
            Ok(n) if n > 0 => buf.get(..n).unwrap_or_default(),
            // EOF, or a pipe that cannot be read any more.
            _ => {
                pipe.open = false;
                &[]
            }
        };
        {
            let mut state = self.state.lock();
            if state.closed.is_some() {
                // The transcript ended with the session.
                return;
            }
            let stream = pipe.stream;
            let mut emit = |piece: Piece<'_>| match piece {
                Piece::Data(data) => state.ring.push_output(stream, data),
                Piece::Sentinel(code) => state.sentinel(code),
            };
            if chunk.is_empty() {
                pipe.scanner.finish(&mut emit);
            } else {
                pipe.scanner.feed(chunk, &mut emit);
            }
        }
        self.changed.send_replace(());
    }

    /// The shell exited: a command it was running finished with the shell's own
    /// status, and the session ends — unless something ended it first.
    fn exited(&self, status: &io::Result<ExitStatus>) {
        {
            let mut state = self.state.lock();
            if state.closed.is_some() {
                return;
            }
            if state.running.take().is_some() {
                state.ring.push(Record::Finished {
                    exit_code: exit_code(status),
                });
            }
            state.closed = Some(ShellCloseReason::Exited);
        }
        self.changed.send_replace(());
        tracing::info!(session = %self.id, "shell exited on its own");
    }
}

impl State {
    fn sentinel(&mut self, code: Option<i32>) {
        if self.running.is_none() {
            return;
        }
        match code {
            Some(code) => self.stdout_code = Some(code),
            None => self.stderr_done = true,
        }
        if let (Some(exit_code), true) = (self.stdout_code, self.stderr_done) {
            self.ring.push(Record::Finished { exit_code });
            self.running = None;
            self.stdout_code = None;
            self.stderr_done = false;
            self.idle_since = Instant::now();
        }
    }
}

struct Pipe {
    stream: Stream,
    scanner: Scanner,
    open: bool,
}

/// Owns the shell process: copies its output into the transcript until it exits,
/// then kills whatever it left behind in its group, collects the last output and
/// ends the session. The table forgets the session on the way out.
pub async fn drive(session: Arc<Session>, pipes: Pipes, table: Weak<Table>) {
    let Pipes {
        mut child,
        mut stdout,
        mut stderr,
    } = pipes;
    let mut out = Pipe {
        stream: Stream::Stdout,
        scanner: Scanner::new(&session.nonce, true),
        open: true,
    };
    let mut err = Pipe {
        stream: Stream::Stderr,
        scanner: Scanner::new(&session.nonce, false),
        open: true,
    };
    let mut out_buf = vec![0u8; READ_CHUNK];
    let mut err_buf = vec![0u8; READ_CHUNK];
    let mut status = None;
    let mut drain_until = None;
    while status.is_none() || out.open || err.open {
        let drained = async move {
            match drain_until {
                Some(deadline) => tokio::time::sleep_until(deadline).await,
                None => std::future::pending().await,
            }
        };
        tokio::select! {
            read = stdout.read(&mut out_buf), if out.open => session.take(&mut out, read, &out_buf),
            read = stderr.read(&mut err_buf), if err.open => session.take(&mut err, read, &err_buf),
            waited = child.wait(), if status.is_none() => {
                // Background jobs die with the shell, so nothing they hold keeps the
                // pipes open.
                session.kill_group();
                status = Some(waited);
                drain_until = Some(Instant::now().checked_add(DRAIN).unwrap_or_else(Instant::now));
            }
            () = drained => break,
        }
    }
    if let Some(status) = &status {
        session.exited(status);
    }
    if let Some(table) = table.upgrade() {
        table.forget(&session);
    }
}

/// `$?` as the shell would report it: 128 + n for a death by signal n.
fn exit_code(status: &io::Result<ExitStatus>) -> i32 {
    use std::os::unix::process::ExitStatusExt;
    match status {
        Ok(status) => status
            .code()
            .or_else(|| status.signal().map(|signal| signal.saturating_add(128)))
            .unwrap_or(-1),
        Err(_) => -1,
    }
}

/// `$HOME` when it names a directory, `/` otherwise (a service user's home is
/// often `/nonexistent`).
fn start_dir() -> PathBuf {
    std::env::var_os("HOME")
        .map(PathBuf::from)
        .filter(|home| home.is_absolute() && home.is_dir())
        .unwrap_or_else(|| PathBuf::from("/"))
}
