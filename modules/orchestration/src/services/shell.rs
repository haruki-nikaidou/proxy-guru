//! Remote shell, the dashboard half (`dashboard_grpc`): an Admin's calls relayed
//! to the worker that runs the shells.
//!
//! The master stores nothing. Each call is checked here — a human Admin
//! ([`Permission::RemoteShell`]), a server that exists, and a worker that
//! advertised [`REMOTE_SHELL`] at `Register` — then handed down the relay
//! ([`ShellDownPublisher`]) to whichever `workers_grpc` replica holds the
//! worker's `ShellChannel`, and answered from the worker's reply, which comes
//! back through [`ShellRouter`]. A worker that does not answer within
//! [`ShellTimings::reply`] makes the call `UNAVAILABLE`.
//!
//! # Watches
//!
//! A watch is a cursor the *worker* keeps into a session's transcript; the
//! relay only keeps it honest ([`WatchShellSession`]):
//!
//! - it renews the watch every [`ShellTimings::renew`], which the worker answers
//!   with a keep-alive and otherwise drops the watch;
//! - it tracks the position the next event must start at, skips anything before
//!   it (a duplicate after a re-attach) and re-attaches from it when an event
//!   starts past it (a lost message), or when nothing at all arrived for
//!   [`ShellTimings::silence`];
//! - it detaches when the client goes away.
//!
//! The relay itself is lossy by design (pub/sub, non-blocking forwards); the
//! offsets are what make the stream exact.

use crate::entities::db::server::{FindServerById, ServerId};
use crate::events::shell::{SHELL_DOWN_CHANNEL, ShellDownMessage};
use crate::hooks::shell::{ShellRouter, WatchInbox, WatchInput};
use crate::services::OrchestrationError;
use crate::services::graph::REMOTE_SHELL;
use crate::services::notify::publish_with_retry;
use crate::services::shell_channel::{ForwardShellDown, ShellChannels};
use auth::services::identity::Identity;
use auth::utils::rbac::Permission;
use base::db::Db;
use kanau::message::MessageSer;
use kanau::processor::Processor;
use prost::Message;
use rpguru_sdk::orchestration_agent as pb;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::time::Instant;

/// How long after a re-attach events past the position are taken for the
/// replaced cursor's stragglers rather than for a new gap.
const REATTACH_GRACE: Duration = Duration::from_secs(2);

/// How many events a watch may have ready for its client.
const WATCH_OUT_CAPACITY: usize = 16;

/// The relay's clock. The defaults are the protocol's; tests shorten them.
#[derive(Debug, Clone, Copy)]
pub struct ShellTimings {
    /// How long a call, or a watch's (re-)attach, waits for the worker.
    pub reply: Duration,
    /// How often a watch renews itself on the worker.
    pub renew: Duration,
    /// How long a watch may hear nothing before it attaches again.
    pub silence: Duration,
}

impl Default for ShellTimings {
    fn default() -> Self {
        Self {
            reply: Duration::from_secs(10),
            renew: Duration::from_secs(10),
            silence: Duration::from_secs(30),
        }
    }
}

/// Why a shell call failed, as far as the caller is concerned.
#[derive(Debug, thiserror::Error)]
pub enum ShellError {
    /// The worker did not answer: not connected to any master, or the relay
    /// lost the message.
    #[error("{0}")]
    Unavailable(String),
    /// The worker never advertised `remote_shell`: not built with it, or its
    /// host did not opt in.
    #[error("this server's worker does not offer remote shell")]
    NotOffered,
    /// No such session: closed, reaped, or the worker restarted since.
    #[error("{0}")]
    NotFound(String),
    /// A command is still running in the session.
    #[error("{0}")]
    Busy(String),
    /// The worker's local session cap is reached.
    #[error("{0}")]
    Limit(String),
    /// The worker refused the request itself (an empty command, a NUL byte).
    #[error("{0}")]
    Invalid(String),
    /// Anything else the worker reported.
    #[error("{0}")]
    Failed(String),
}

impl ShellError {
    fn no_answer() -> Self {
        Self::Unavailable("the worker did not answer".into())
    }

    /// The worker's own refusal, its message kept: it is what the operator
    /// reads.
    fn from_worker(error: pb::ShellError) -> Self {
        let message = |fallback: &str| {
            if error.message.is_empty() {
                fallback.to_string()
            } else {
                error.message.clone()
            }
        };
        match pb::ShellErrorCode::try_from(error.code) {
            Ok(pb::ShellErrorCode::NotFound) => {
                Self::NotFound(message("the shell session no longer exists"))
            }
            Ok(pb::ShellErrorCode::Busy) => {
                Self::Busy(message("a command is still running in this session"))
            }
            Ok(pb::ShellErrorCode::Limit) => Self::Limit(message(
                "the worker's remote shell session limit is reached",
            )),
            Ok(pb::ShellErrorCode::Invalid) => Self::Invalid(message("the command is not valid")),
            Ok(pb::ShellErrorCode::Failed | pb::ShellErrorCode::Unspecified) | Err(_) => {
                Self::Failed(message("the worker's remote shell failed"))
            }
        }
    }
}

impl From<ShellError> for tonic::Status {
    fn from(error: ShellError) -> Self {
        let message = error.to_string();
        match error {
            ShellError::Unavailable(_) => tonic::Status::unavailable(message),
            ShellError::NotOffered | ShellError::Busy(_) => {
                tonic::Status::failed_precondition(message)
            }
            ShellError::NotFound(_) => tonic::Status::not_found(message),
            ShellError::Limit(_) => tonic::Status::resource_exhausted(message),
            ShellError::Invalid(_) => tonic::Status::invalid_argument(message),
            ShellError::Failed(_) => {
                tracing::warn!(%message, "the worker's remote shell failed");
                tonic::Status::internal(message)
            }
        }
    }
}

/// Where down messages go.
#[derive(Clone)]
pub enum ShellDownPublisher {
    /// Production: `PUBLISH` on [`SHELL_DOWN_CHANNEL`], which every
    /// `workers_grpc` replica subscribes to.
    Redis(redis::aio::ConnectionManager),
    /// Tests: forward straight into this process's worker-facing side, skipping
    /// the round trip — the same shortcut `LivePublisher::InProcess` is.
    InProcess(ShellChannels),
}

impl ShellDownPublisher {
    async fn publish(&self, server: &ServerId, down: pb::ShellDown) -> Result<(), ShellError> {
        match self {
            Self::InProcess(channels) => {
                // Whether a stream took it does not matter here, exactly as
                // nobody tells a Redis publisher: no answer is the answer.
                if let Err(error) = channels
                    .process(ForwardShellDown {
                        server: server.clone(),
                        down,
                    })
                    .await
                {
                    tracing::warn!(%error, "forwarding a shell down message failed");
                }
                Ok(())
            }
            Self::Redis(manager) => {
                let bytes = ShellDownMessage {
                    server_id: server.to_string(),
                    down: down.encode_to_vec(),
                }
                .to_bytes()
                .map_err(|error| {
                    ShellError::Failed(format!("encoding a shell message: {error:?}"))
                })?;
                publish_with_retry(manager, SHELL_DOWN_CHANNEL, &bytes)
                    .await
                    .map_err(|error| {
                        tracing::warn!(%error, "publishing a shell down message failed");
                        ShellError::Unavailable("the relay to the worker is unavailable".into())
                    })
            }
        }
    }
}

#[derive(Clone)]
pub struct ShellService {
    pub db: Db,
    pub router: ShellRouter,
    pub down: ShellDownPublisher,
    pub timings: ShellTimings,
}

impl ShellService {
    /// The checks every call makes before anything reaches the worker.
    async fn authorize(
        &self,
        actor: &Identity,
        server: &ServerId,
    ) -> Result<(), OrchestrationError> {
        actor.ensure(Permission::RemoteShell)?;
        let server = self
            .db
            .process(FindServerById { id: server.clone() })
            .await?
            .ok_or(OrchestrationError::NotFound)?;
        if !server.capabilities.iter().any(|c| c == REMOTE_SHELL) {
            return Err(ShellError::NotOffered.into());
        }
        Ok(())
    }

    /// One request, answered by the worker's one reply; its refusal is the
    /// error.
    async fn request(
        &self,
        server: &ServerId,
        op: pb::shell_request::Op,
    ) -> Result<pb::shell_reply::Result, ShellError> {
        let mut wait = self.router.expect_reply(server.as_ref());
        self.down
            .publish(
                server,
                pb::ShellDown {
                    message: Some(pb::shell_down::Message::Request(pb::ShellRequest {
                        request_id: wait.id.clone(),
                        op: Some(op),
                    })),
                },
            )
            .await?;
        let reply = wait
            .recv(self.timings.reply)
            .await
            .ok_or_else(ShellError::no_answer)?;
        match reply.result {
            Some(pb::shell_reply::Result::Error(error)) => Err(ShellError::from_worker(error)),
            Some(result) => Ok(result),
            None => Err(ShellError::Failed("the worker sent an empty reply".into())),
        }
    }
}

fn unexpected() -> ShellError {
    ShellError::Failed("the worker answered with an unexpected reply".into())
}

/// Spawns a shell on the server's worker.
pub struct OpenShellSession {
    pub actor: Identity,
    pub server: ServerId,
}

impl Processor<OpenShellSession> for ShellService {
    type Output = pb::ShellSessionInfo;
    type Error = OrchestrationError;
    #[tracing::instrument(name = "Service:OpenShellSession", skip_all, err)]
    async fn process(&self, input: OpenShellSession) -> Result<Self::Output, Self::Error> {
        self.authorize(&input.actor, &input.server).await?;
        match self
            .request(&input.server, pb::shell_request::Op::Open(pb::ShellOpen {}))
            .await?
        {
            pb::shell_reply::Result::Opened(info) => Ok(info),
            _ => Err(unexpected().into()),
        }
    }
}

/// Writes one command to a session's shell; answered once the worker accepted
/// it, not when it finishes.
pub struct SendShellCommand {
    pub actor: Identity,
    pub server: ServerId,
    pub session_id: String,
    pub command: String,
}

impl Processor<SendShellCommand> for ShellService {
    type Output = ();
    type Error = OrchestrationError;
    #[tracing::instrument(name = "Service:SendShellCommand", skip_all, err)]
    async fn process(&self, input: SendShellCommand) -> Result<Self::Output, Self::Error> {
        self.authorize(&input.actor, &input.server).await?;
        match self
            .request(
                &input.server,
                pb::shell_request::Op::Exec(pb::ShellExec {
                    session_id: input.session_id,
                    command: input.command,
                }),
            )
            .await?
        {
            pb::shell_reply::Result::Done(_) => Ok(()),
            _ => Err(unexpected().into()),
        }
    }
}

/// Kills a session's process group.
pub struct CloseShellSession {
    pub actor: Identity,
    pub server: ServerId,
    pub session_id: String,
}

impl Processor<CloseShellSession> for ShellService {
    type Output = ();
    type Error = OrchestrationError;
    #[tracing::instrument(name = "Service:CloseShellSession", skip_all, err)]
    async fn process(&self, input: CloseShellSession) -> Result<Self::Output, Self::Error> {
        self.authorize(&input.actor, &input.server).await?;
        match self
            .request(
                &input.server,
                pb::shell_request::Op::Close(pb::ShellClose {
                    session_id: input.session_id,
                }),
            )
            .await?
        {
            pb::shell_reply::Result::Done(_) => Ok(()),
            _ => Err(unexpected().into()),
        }
    }
}

/// The sessions the server's worker runs now.
pub struct ListShellSessions {
    pub actor: Identity,
    pub server: ServerId,
}

impl Processor<ListShellSessions> for ShellService {
    type Output = Vec<pb::ShellSessionInfo>;
    type Error = OrchestrationError;
    #[tracing::instrument(name = "Service:ListShellSessions", skip_all, err)]
    async fn process(&self, input: ListShellSessions) -> Result<Self::Output, Self::Error> {
        self.authorize(&input.actor, &input.server).await?;
        match self
            .request(&input.server, pb::shell_request::Op::List(pb::ShellList {}))
            .await?
        {
            pb::shell_reply::Result::Sessions(list) => Ok(list.sessions),
            _ => Err(unexpected().into()),
        }
    }
}

/// Follows a session's transcript from `from_offset`.
///
/// Answers once the worker did: an unknown session is `NOT_FOUND` and a worker
/// that does not answer `UNAVAILABLE` here, before any event. Everything after
/// arrives on [`ShellWatch::rx`]: transcript events in order, without gaps or
/// repeats (a `truncated` where the worker's buffer no longer holds what was
/// asked for), ending with `closed` or with an error.
pub struct WatchShellSession {
    pub actor: Identity,
    pub server: ServerId,
    pub session_id: String,
    pub from_offset: u64,
}

/// An open watch. Dropping the receiver detaches it from the worker.
pub struct ShellWatch {
    pub rx: mpsc::Receiver<Result<pb::ShellEvent, ShellError>>,
}

impl Processor<WatchShellSession> for ShellService {
    type Output = ShellWatch;
    type Error = OrchestrationError;
    #[tracing::instrument(name = "Service:WatchShellSession", skip_all, err)]
    async fn process(&self, input: WatchShellSession) -> Result<Self::Output, Self::Error> {
        self.authorize(&input.actor, &input.server).await?;
        let mut inbox = self.router.watch(input.server.as_ref());
        let mut relay = WatchRelay {
            service: self.clone(),
            server: input.server,
            session_id: input.session_id,
            watch_id: inbox.id.clone(),
            expected: input.from_offset,
            grace_until: None,
        };
        let (tx, rx) = mpsc::channel(WATCH_OUT_CAPACITY);
        // The first answer decides whether there is a watch at all. Before it,
        // a resync only means attaching again.
        relay.attach().await?;
        let deadline = Instant::now()
            .checked_add(self.timings.reply)
            .unwrap_or_else(Instant::now);
        let first = loop {
            match tokio::time::timeout_at(deadline, inbox.rx.recv()).await {
                Ok(Some(WatchInput::Event(event))) => break event,
                Ok(Some(WatchInput::Resync)) => relay.attach().await?,
                Ok(None) | Err(_) => {
                    relay.detach().await;
                    return Err(ShellError::no_answer().into());
                }
            }
        };
        match relay.accept(first, &tx).await {
            Step::Continue => {}
            Step::End(Ended::Failed(error)) => return Err(error.into()),
            Step::End(ended) => {
                // Only a session that closed right away ends here: its last
                // events are ready for the client, and nothing is left to relay.
                tracing::debug!(?ended, "a shell watch ended at its first event");
                return Ok(ShellWatch { rx });
            }
        }
        tokio::spawn(relay.run(inbox, tx));
        Ok(ShellWatch { rx })
    }
}

/// How a watch ended.
#[derive(Debug)]
enum Ended {
    /// The session closed; its `closed` event was the last one sent.
    SessionClosed,
    /// The client went away.
    ClientGone,
    Failed(ShellError),
}

enum Step {
    Continue,
    End(Ended),
}

/// The state of one watch on the dashboard side.
struct WatchRelay {
    service: ShellService,
    server: ServerId,
    session_id: String,
    watch_id: String,
    /// The position the next event must start at.
    expected: u64,
    /// Until when a re-attach is fresh: an event past the position is then the
    /// replaced cursor's straggler, not a new gap.
    grace_until: Option<Instant>,
}

impl WatchRelay {
    async fn send(&self, message: pb::shell_down::Message) -> Result<(), ShellError> {
        self.service
            .down
            .publish(
                &self.server,
                pb::ShellDown {
                    message: Some(message),
                },
            )
            .await
    }

    /// (Re)starts the worker's cursor at the expected position and renews it
    /// right away, so the worker answers even when it has nothing to send.
    async fn attach(&self) -> Result<(), ShellError> {
        self.send(pb::shell_down::Message::Attach(pb::ShellAttach {
            watch_id: self.watch_id.clone(),
            session_id: self.session_id.clone(),
            from_offset: self.expected,
        }))
        .await?;
        self.renew().await
    }

    async fn renew(&self) -> Result<(), ShellError> {
        self.send(pb::shell_down::Message::Renew(pb::ShellRenew {
            watch_id: self.watch_id.clone(),
        }))
        .await
    }

    async fn detach(&self) {
        if let Err(error) = self
            .send(pb::shell_down::Message::Detach(pb::ShellDetach {
                watch_id: self.watch_id.clone(),
            }))
            .await
        {
            // The worker drops a watch nobody renews anyway.
            tracing::debug!(%error, "detaching a shell watch failed");
        }
    }

    /// Re-attaches after a loss; a failed publish leaves recovery to the
    /// silence timer.
    async fn reattach(&mut self) {
        self.grace_until = Instant::now().checked_add(REATTACH_GRACE);
        if let Err(error) = self.attach().await {
            tracing::warn!(%error, "re-attaching a shell watch failed");
        }
    }

    /// An event started past the position: something was lost on the way.
    async fn gap(&mut self) {
        if self.grace_until.is_some_and(|until| Instant::now() < until) {
            return;
        }
        self.reattach().await;
    }

    async fn forward(
        &self,
        event: pb::ShellEvent,
        out: &mpsc::Sender<Result<pb::ShellEvent, ShellError>>,
    ) -> Step {
        if out.send(Ok(event)).await.is_err() {
            Step::End(Ended::ClientGone)
        } else {
            Step::Continue
        }
    }

    /// Takes one event from the worker: forwards what continues the transcript
    /// exactly at the position, drops what came before it, re-attaches over
    /// what starts after it.
    async fn accept(
        &mut self,
        event: pb::ShellEvent,
        out: &mpsc::Sender<Result<pb::ShellEvent, ShellError>>,
    ) -> Step {
        use pb::shell_event::Event;
        let start = event.offset;
        let end = event.end_offset();
        let Some(body) = event.event else {
            return Step::Continue;
        };
        let at = |offset: u64, body: Event| pb::ShellEvent {
            offset,
            event: Some(body),
        };
        match body {
            Event::Error(error) => Step::End(Ended::Failed(ShellError::from_worker(error))),
            Event::KeepAlive(_) => {
                // The worker's cursor: past the position means it sent
                // something that never arrived.
                if start > self.expected {
                    self.gap().await;
                } else {
                    self.grace_until = None;
                }
                Step::Continue
            }
            Event::Truncated(mut truncated) => {
                // The worker no longer holds what the position asks for, so a
                // re-attach would only be told the same. What the client misses
                // is measured from its own position.
                if start <= self.expected {
                    return Step::Continue;
                }
                truncated.dropped = start.saturating_sub(self.expected);
                self.expected = start;
                self.grace_until = None;
                self.forward(at(start, Event::Truncated(truncated)), out)
                    .await
            }
            Event::Closed(closed) => {
                // The session is gone with its buffer: a tail that went missing
                // cannot be fetched again, so it is reported as truncated.
                if start > self.expected {
                    let skipped = pb::ShellTruncated {
                        dropped: start.saturating_sub(self.expected),
                    };
                    if let Step::End(ended) = self
                        .forward(at(start, Event::Truncated(skipped)), out)
                        .await
                    {
                        return Step::End(ended);
                    }
                }
                match self.forward(at(start, Event::Closed(closed)), out).await {
                    Step::Continue => Step::End(Ended::SessionClosed),
                    end => end,
                }
            }
            body @ (Event::Output(_) | Event::Started(_) | Event::Finished(_)) => {
                if end <= self.expected {
                    // Already sent: the replaced cursor, or the restarted one
                    // catching up.
                    self.grace_until = None;
                    return Step::Continue;
                }
                if start > self.expected {
                    self.gap().await;
                    return Step::Continue;
                }
                let body = match body {
                    // Straddles the position: only the unseen tail is new.
                    Event::Output(mut output) if start < self.expected => {
                        let seen = usize::try_from(self.expected.saturating_sub(start))
                            .unwrap_or(usize::MAX)
                            .min(output.data.len());
                        output.data.drain(..seen);
                        Event::Output(output)
                    }
                    body => body,
                };
                let offset = self.expected;
                self.expected = end;
                self.grace_until = None;
                self.forward(at(offset, body), out).await
            }
        }
    }

    /// The watch's life after its first event, until the session closes, the
    /// client leaves or the worker stops answering.
    async fn run(
        mut self,
        mut inbox: WatchInbox,
        out: mpsc::Sender<Result<pb::ShellEvent, ShellError>>,
    ) {
        let ended = self.pump(&mut inbox, &out).await;
        tracing::debug!(watch = %self.watch_id, ?ended, "shell watch ended");
        match ended {
            Ended::SessionClosed => {}
            Ended::ClientGone => self.detach().await,
            Ended::Failed(error) => {
                self.detach().await;
                let _ = out.send(Err(error)).await;
            }
        }
    }

    async fn pump(
        &mut self,
        inbox: &mut WatchInbox,
        out: &mpsc::Sender<Result<pb::ShellEvent, ShellError>>,
    ) -> Ended {
        let timings = self.service.timings;
        let start = Instant::now()
            .checked_add(timings.renew)
            .unwrap_or_else(Instant::now);
        let mut renew = tokio::time::interval_at(start, timings.renew);
        renew.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        let mut heard = Instant::now();
        // Set after a re-attach for silence: the worker must answer by then.
        let mut answer_by: Option<Instant> = None;
        loop {
            let wake = answer_by.unwrap_or_else(|| {
                heard
                    .checked_add(timings.silence)
                    .unwrap_or_else(Instant::now)
            });
            tokio::select! {
                () = out.closed() => return Ended::ClientGone,
                _ = renew.tick() => {
                    if let Err(error) = self.renew().await {
                        tracing::debug!(%error, "renewing a shell watch failed");
                    }
                }
                () = tokio::time::sleep_until(wake) => {
                    if answer_by.is_some() {
                        return Ended::Failed(ShellError::no_answer());
                    }
                    self.reattach().await;
                    answer_by = Some(
                        Instant::now()
                            .checked_add(timings.reply)
                            .unwrap_or_else(Instant::now),
                    );
                }
                input = inbox.rx.recv() => match input {
                    // The inbox's sender lives in the router until the inbox
                    // drops, so this is unreachable short of a bug.
                    None => return Ended::Failed(ShellError::no_answer()),
                    Some(WatchInput::Resync) => self.reattach().await,
                    Some(WatchInput::Event(event)) => {
                        heard = Instant::now();
                        answer_by = None;
                        if let Step::End(ended) = self.accept(event, out).await {
                            return ended;
                        }
                    }
                },
            }
        }
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

    use super::*;
    use pb::shell_event::Event;

    fn relay(expected: u64) -> WatchRelay {
        // Never connected: no channel holds the server, so a (re-)attach goes
        // nowhere and nothing reads the database. These tests only look at
        // what reaches the client.
        let db = Db::new(
            sqlx::postgres::PgPoolOptions::new()
                .connect_lazy("postgres://localhost/unused")
                .unwrap(),
        );
        let router = ShellRouter::new();
        WatchRelay {
            service: ShellService {
                db: db.clone(),
                router: router.clone(),
                down: ShellDownPublisher::InProcess(ShellChannels {
                    db,
                    hub: Default::default(),
                    up: crate::services::shell_channel::ShellUpPublisher::InProcess(router),
                }),
                timings: ShellTimings::default(),
            },
            server: ServerId::from_key("shellserverxxxxxxxxx"),
            session_id: "s".into(),
            watch_id: "w".into(),
            expected,
            grace_until: None,
        }
    }

    fn output(offset: u64, data: &str) -> pb::ShellEvent {
        pb::ShellEvent {
            offset,
            event: Some(Event::Output(pb::ShellOutput {
                stream: pb::ShellStream::Stdout.into(),
                data: data.as_bytes().to_vec(),
            })),
        }
    }

    async fn drain(
        rx: &mut mpsc::Receiver<Result<pb::ShellEvent, ShellError>>,
    ) -> Vec<pb::ShellEvent> {
        let mut events = Vec::new();
        while let Ok(Ok(event)) = rx.try_recv() {
            events.push(event);
        }
        events
    }

    /// Duplicates are skipped, a straddling chunk is trimmed to its unseen
    /// tail, and a chunk past the position is held back for the re-attach.
    #[tokio::test]
    async fn only_the_exact_continuation_reaches_the_client() {
        let mut relay = relay(3);
        let (tx, mut rx) = mpsc::channel(16);
        assert!(matches!(
            relay.accept(output(0, "abc"), &tx).await,
            Step::Continue
        ));
        assert!(matches!(
            relay.accept(output(1, "bcde"), &tx).await,
            Step::Continue
        ));
        assert!(matches!(
            relay.accept(output(9, "zz"), &tx).await,
            Step::Continue
        ));
        assert_eq!(relay.expected, 5);
        assert!(relay.grace_until.is_some(), "the gap re-attached");
        let sent = drain(&mut rx).await;
        assert_eq!(sent, vec![output(3, "de")]);
    }

    /// A truncation reports what the client actually missed, and a session that
    /// closed past the position says so before it says closed.
    #[tokio::test]
    async fn losses_the_worker_cannot_repair_are_reported() {
        let mut relay = relay(2);
        let (tx, mut rx) = mpsc::channel(16);
        let truncated = pb::ShellEvent {
            offset: 10,
            event: Some(Event::Truncated(pb::ShellTruncated { dropped: 10 })),
        };
        assert!(matches!(relay.accept(truncated, &tx).await, Step::Continue));
        let closed = pb::ShellEvent {
            offset: 12,
            event: Some(Event::Closed(pb::ShellSessionClosed {
                reason: pb::ShellCloseReason::Closed.into(),
            })),
        };
        assert!(matches!(
            relay.accept(closed.clone(), &tx).await,
            Step::End(Ended::SessionClosed)
        ));
        let sent = drain(&mut rx).await;
        assert_eq!(
            sent,
            vec![
                pb::ShellEvent {
                    offset: 10,
                    event: Some(Event::Truncated(pb::ShellTruncated { dropped: 8 })),
                },
                pb::ShellEvent {
                    offset: 12,
                    event: Some(Event::Truncated(pb::ShellTruncated { dropped: 2 })),
                },
                closed,
            ]
        );
    }
}
