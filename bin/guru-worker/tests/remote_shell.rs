//! The remote shell end to end on the worker's side: a real agent with a real
//! session table runs real `bash` processes, driven over `ShellChannel` by a fake
//! master that hands the test both ends of every channel the worker opens.

#![allow(clippy::unwrap_used, clippy::panic, clippy::expect_used)]

use guru_worker::agent::{self, AgentOptions, RemoteShell};
use guru_worker::supervisor::Supervisor;
use rpguru_sdk::orchestration_agent::worker_agent_server::{WorkerAgent, WorkerAgentServer};
use rpguru_sdk::orchestration_agent::{
    AckConfigReply, AckConfigRequest, ConfigRevision, HealthReport, PollAgentUpdateReply,
    PollAgentUpdateRequest, RegisterReply, RegisterRequest, ReportHealthReply, ShellDown, ShellUp,
    WatchConfigRequest,
};
use std::sync::Arc;
use std::sync::atomic::{AtomicI64, AtomicUsize, Ordering};
use std::time::Duration;
use tokio::sync::{Mutex, mpsc};
use tokio_stream::wrappers::ReceiverStream;
use tokio_util::sync::CancellationToken;
use tonic::{Request, Response, Status};

/// Nothing here may wait forever: every receive goes through [`within`].
const PATIENCE: Duration = Duration::from_secs(30);

async fn within<F: Future>(what: &str, fut: F) -> F::Output {
    match tokio::time::timeout(PATIENCE, fut).await {
        Ok(value) => value,
        Err(_) => panic!("timed out after {PATIENCE:?} waiting for {what}"),
    }
}

/// Both ends of one `ShellChannel` as the master sees them.
#[cfg_attr(not(feature = "remote-shell"), allow(dead_code))]
struct Opened {
    ups: tonic::Streaming<ShellUp>,
    downs: mpsc::Sender<Result<ShellDown, Status>>,
}

/// The master reduced to what an agent session needs to stay up — a config stream
/// and a health stream that stay open, no revision, no update — plus
/// `ShellChannel`, whose ends go to the test.
struct ShellMaster {
    registrations: mpsc::UnboundedSender<RegisterRequest>,
    channels: mpsc::UnboundedSender<Opened>,
    shell_channels: Arc<AtomicUsize>,
    configs: parking_lot::Mutex<Vec<mpsc::Sender<Result<ConfigRevision, Status>>>>,
    replies: parking_lot::Mutex<Vec<mpsc::Sender<Result<ReportHealthReply, Status>>>>,
}

#[tonic::async_trait]
impl WorkerAgent for ShellMaster {
    async fn register(
        &self,
        request: Request<RegisterRequest>,
    ) -> Result<Response<RegisterReply>, Status> {
        let _ = self.registrations.send(request.into_inner());
        Ok(Response::new(RegisterReply {
            refresh_key: "fake".to_string(),
            health_report_interval_secs: 0,
            agent_update_poll_secs: 0,
            stream_keepalive_secs: 0,
        }))
    }

    async fn poll_agent_update(
        &self,
        _: Request<PollAgentUpdateRequest>,
    ) -> Result<Response<PollAgentUpdateReply>, Status> {
        Ok(Response::new(PollAgentUpdateReply { update: None }))
    }

    type WatchConfigStream = ReceiverStream<Result<ConfigRevision, Status>>;

    async fn watch_config(
        &self,
        _: Request<WatchConfigRequest>,
    ) -> Result<Response<Self::WatchConfigStream>, Status> {
        let (tx, rx) = mpsc::channel(1);
        self.configs.lock().push(tx);
        Ok(Response::new(ReceiverStream::new(rx)))
    }

    async fn ack_config(
        &self,
        _: Request<AckConfigRequest>,
    ) -> Result<Response<AckConfigReply>, Status> {
        Ok(Response::new(AckConfigReply {}))
    }

    type ReportHealthStream = ReceiverStream<Result<ReportHealthReply, Status>>;

    async fn report_health(
        &self,
        request: Request<tonic::Streaming<HealthReport>>,
    ) -> Result<Response<Self::ReportHealthStream>, Status> {
        let mut reports = request.into_inner();
        tokio::spawn(async move { while let Ok(Some(_)) = reports.message().await {} });
        let (tx, rx) = mpsc::channel(1);
        self.replies.lock().push(tx);
        Ok(Response::new(ReceiverStream::new(rx)))
    }

    type ShellChannelStream = ReceiverStream<Result<ShellDown, Status>>;

    async fn shell_channel(
        &self,
        request: Request<tonic::Streaming<ShellUp>>,
    ) -> Result<Response<Self::ShellChannelStream>, Status> {
        self.shell_channels.fetch_add(1, Ordering::SeqCst);
        let (tx, rx) = mpsc::channel(64);
        let _ = self.channels.send(Opened {
            ups: request.into_inner(),
            downs: tx,
        });
        Ok(Response::new(ReceiverStream::new(rx)))
    }
}

/// A worker agent against a [`ShellMaster`].
struct Harness {
    registrations: mpsc::UnboundedReceiver<RegisterRequest>,
    channels: mpsc::UnboundedReceiver<Opened>,
    shell_channels: Arc<AtomicUsize>,
    sup: Arc<Mutex<Supervisor>>,
    shutdown: CancellationToken,
    task: tokio::task::JoinHandle<Result<(), guru_worker::BoxError>>,
    master_shutdown: CancellationToken,
    state_dir: std::path::PathBuf,
}

impl Harness {
    async fn start(remote_shell: Option<RemoteShell>) -> Self {
        let (registrations_tx, registrations) = mpsc::unbounded_channel();
        let (channels_tx, channels) = mpsc::unbounded_channel();
        let shell_channels = Arc::new(AtomicUsize::new(0));
        let master = ShellMaster {
            registrations: registrations_tx,
            channels: channels_tx,
            shell_channels: shell_channels.clone(),
            configs: parking_lot::Mutex::default(),
            replies: parking_lot::Mutex::default(),
        };
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let master_shutdown = CancellationToken::new();
        let token = master_shutdown.clone();
        tokio::spawn(async move {
            let _ = tonic::transport::Server::builder()
                .add_service(WorkerAgentServer::new(master))
                .serve_with_incoming_shutdown(
                    tokio_stream::wrappers::TcpListenerStream::new(listener),
                    async move { token.cancelled().await },
                )
                .await;
        });

        let state_dir = std::env::temp_dir().join(format!(
            "guru-worker-shell-{}-{}",
            std::process::id(),
            addr.port()
        ));
        let _ = std::fs::remove_dir_all(&state_dir);
        let sup = Arc::new(Mutex::new(Supervisor::new()));
        let shutdown = CancellationToken::new();
        let task = tokio::spawn(agent::run(
            AgentOptions {
                master: format!("http://{addr}"),
                api_key: "unused".to_string(),
                server_id: "shell-server".to_string(),
                state_dir: state_dir.clone(),
                applied_revision: Arc::new(AtomicI64::new(0)),
                health_interval: Duration::from_millis(200),
                sources: guru_worker::addresses::Sources::none(),
                update_poll: Duration::from_secs(60),
                self_update: false,
                update_done: Default::default(),
                last_update_error: Default::default(),
                unary_timeout: Duration::from_secs(5),
                remote_shell,
            },
            sup.clone(),
            shutdown.clone(),
        ));
        Self {
            registrations,
            channels,
            shell_channels,
            sup,
            shutdown,
            task,
            master_shutdown,
            state_dir,
        }
    }

    async fn stop(self) {
        self.shutdown.cancel();
        let _ = within("the agent task to stop", self.task).await;
        self.master_shutdown.cancel();
        self.sup.lock().await.shutdown_all();
        let _ = std::fs::remove_dir_all(&self.state_dir);
    }
}

#[tokio::test]
async fn without_the_opt_in_the_worker_neither_advertises_nor_opens_a_shell_channel() {
    let mut harness = Harness::start(None).await;
    let registration = within("the registration", harness.registrations.recv())
        .await
        .expect("the worker registers");
    assert_eq!(
        registration.capabilities,
        vec!["route_table".to_string(), "relay_confirm".to_string()]
    );
    // Well past the moment an opted-in worker opens it, right after registering.
    tokio::time::sleep(Duration::from_millis(1500)).await;
    assert_eq!(harness.shell_channels.load(Ordering::SeqCst), 0);
    assert!(harness.channels.try_recv().is_err());
    harness.stop().await;
}

#[cfg(feature = "remote-shell")]
mod opted_in {
    use super::*;
    use guru_worker::shell::{ShellSettings, ShellTable, find_bash};
    use rpguru_sdk::orchestration_agent::{
        ShellAttach, ShellCloseReason, ShellDetach, ShellErrorCode, ShellEvent, ShellExec,
        ShellList, ShellOpen, ShellRenew, ShellRequest, ShellSessionInfo, ShellStream,
        ShellWatchEvent, shell_down, shell_event, shell_reply, shell_request, shell_up,
    };
    use std::collections::VecDeque;

    fn table(buffer_bytes: usize, idle_timeout: Duration, max_sessions: usize) -> ShellTable {
        let bash = find_bash(std::env::var_os("PATH").as_deref()).expect("bash on PATH");
        ShellTable::new(ShellSettings {
            bash,
            buffer_bytes,
            idle_timeout,
            max_sessions,
        })
    }

    fn roomy() -> ShellTable {
        table(1 << 20, Duration::from_secs(600), 4)
    }

    /// The master's side of one channel: requests out, replies and events in.
    struct Shell {
        ups: tonic::Streaming<ShellUp>,
        downs: mpsc::Sender<Result<ShellDown, Status>>,
        /// Events read while waiting for a reply or another watch.
        events: VecDeque<ShellWatchEvent>,
        requests: u64,
    }

    impl Harness {
        async fn shell(&mut self) -> Shell {
            let opened = within("a shell channel", self.channels.recv())
                .await
                .expect("the worker opens a shell channel");
            Shell {
                ups: opened.ups,
                downs: opened.downs,
                events: VecDeque::new(),
                requests: 0,
            }
        }
    }

    impl Shell {
        async fn send(&self, message: shell_down::Message) {
            self.downs
                .send(Ok(ShellDown {
                    message: Some(message),
                }))
                .await
                .expect("the channel is open");
        }

        /// The next message that is not a keep-alive.
        async fn up(&mut self) -> shell_up::Message {
            loop {
                let up = within("a message from the worker", self.ups.message())
                    .await
                    .expect("the channel is healthy")
                    .expect("the channel is open");
                match up.message {
                    Some(shell_up::Message::KeepAlive(_)) | None => {}
                    Some(message) => return message,
                }
            }
        }

        async fn request(&mut self, op: shell_request::Op) -> shell_reply::Result {
            self.requests += 1;
            let request_id = format!("test:{}", self.requests);
            self.send(shell_down::Message::Request(ShellRequest {
                request_id: request_id.clone(),
                op: Some(op),
            }))
            .await;
            loop {
                match self.up().await {
                    shell_up::Message::Reply(reply) => {
                        assert_eq!(reply.request_id, request_id, "one reply per request");
                        return reply.result.expect("a reply carries a result");
                    }
                    shell_up::Message::Event(event) => self.events.push_back(event),
                    shell_up::Message::KeepAlive(_) => {}
                }
            }
        }

        async fn open(&mut self) -> ShellSessionInfo {
            match self.request(shell_request::Op::Open(ShellOpen {})).await {
                shell_reply::Result::Opened(info) => info,
                other => panic!("open: {other:?}"),
            }
        }

        async fn exec(&mut self, session: &str, command: &str) -> shell_reply::Result {
            self.request(shell_request::Op::Exec(ShellExec {
                session_id: session.to_string(),
                command: command.to_string(),
            }))
            .await
        }

        async fn exec_ok(&mut self, session: &str, command: &str) {
            let reply = self.exec(session, command).await;
            assert!(
                matches!(reply, shell_reply::Result::Done(_)),
                "exec {command:?}: {reply:?}"
            );
        }

        async fn close(&mut self, session: &str) -> shell_reply::Result {
            self.request(shell_request::Op::Close(
                rpguru_sdk::orchestration_agent::ShellClose {
                    session_id: session.to_string(),
                },
            ))
            .await
        }

        async fn list(&mut self) -> Vec<ShellSessionInfo> {
            match self.request(shell_request::Op::List(ShellList {})).await {
                shell_reply::Result::Sessions(list) => list.sessions,
                other => panic!("list: {other:?}"),
            }
        }

        async fn attach(&self, watch: &str, session: &str, from_offset: u64) {
            self.send(shell_down::Message::Attach(ShellAttach {
                watch_id: watch.to_string(),
                session_id: session.to_string(),
                from_offset,
            }))
            .await;
        }

        async fn detach(&self, watch: &str) {
            self.send(shell_down::Message::Detach(ShellDetach {
                watch_id: watch.to_string(),
            }))
            .await;
        }

        async fn renew(&self, watch: &str) {
            self.send(shell_down::Message::Renew(ShellRenew {
                watch_id: watch.to_string(),
            }))
            .await;
        }

        /// The next event of `watch`; other watches' events stay queued.
        async fn event(&mut self, watch: &str) -> ShellEvent {
            if let Some(at) = self.events.iter().position(|e| e.watch_id == watch) {
                let event = self.events.remove(at).expect("queued");
                return event.event.expect("an event");
            }
            loop {
                match self.up().await {
                    shell_up::Message::Event(event) if event.watch_id == watch => {
                        return event.event.expect("an event");
                    }
                    shell_up::Message::Event(event) => self.events.push_back(event),
                    other => panic!("unexpected {other:?}"),
                }
            }
        }

        /// The events of `watch` up to and including the first that `last` accepts.
        async fn until(
            &mut self,
            watch: &str,
            last: impl Fn(&ShellEvent) -> bool,
        ) -> Vec<ShellEvent> {
            let mut events = Vec::new();
            loop {
                let event = self.event(watch).await;
                let done = last(&event);
                events.push(event);
                if done {
                    return events;
                }
            }
        }

        async fn until_finished(&mut self, watch: &str) -> Vec<ShellEvent> {
            self.until(watch, |e| {
                matches!(e.event, Some(shell_event::Event::Finished(_)))
            })
            .await
        }
    }

    fn output(events: &[ShellEvent], stream: ShellStream) -> String {
        let mut text = Vec::new();
        for event in events {
            if let Some(shell_event::Event::Output(out)) = &event.event
                && out.stream == i32::from(stream)
            {
                text.extend_from_slice(&out.data);
            }
        }
        String::from_utf8(text).expect("utf-8 output")
    }

    fn exit_code(events: &[ShellEvent]) -> Option<i32> {
        events.iter().find_map(|e| match e.event {
            Some(shell_event::Event::Finished(f)) => Some(f.exit_code),
            _ => None,
        })
    }

    /// Adjacent output events of one stream merged into one: how a replay reads
    /// what a live watch may have seen arrive in pieces.
    fn merged(events: Vec<ShellEvent>) -> Vec<ShellEvent> {
        let mut out: Vec<ShellEvent> = Vec::with_capacity(events.len());
        for event in events {
            if let (Some(last), Some(shell_event::Event::Output(next))) =
                (out.last_mut(), &event.event)
                && let Some(shell_event::Event::Output(prev)) = &mut last.event
                && prev.stream == next.stream
            {
                prev.data.extend_from_slice(&next.data);
                continue;
            }
            out.push(event);
        }
        out
    }

    /// Positions the event occupies.
    fn len(event: &ShellEvent) -> u64 {
        match &event.event {
            Some(shell_event::Event::Output(out)) => out.data.len() as u64,
            Some(shell_event::Event::Started(_) | shell_event::Event::Finished(_)) => 1,
            _ => 0,
        }
    }

    fn error_code(reply: &shell_reply::Result) -> Option<ShellErrorCode> {
        match reply {
            shell_reply::Result::Error(error) => ShellErrorCode::try_from(error.code).ok(),
            _ => None,
        }
    }

    fn closed(event: &ShellEvent) -> Option<ShellCloseReason> {
        match event.event {
            Some(shell_event::Event::Closed(closed)) => {
                ShellCloseReason::try_from(closed.reason).ok()
            }
            _ => None,
        }
    }

    /// Whether `pid` is still a live process (a zombie is dead).
    fn alive(pid: u32) -> bool {
        std::fs::read_to_string(format!("/proc/{pid}/stat")).is_ok_and(|stat| {
            stat.rsplit_once(')')
                .and_then(|(_, rest)| rest.split_whitespace().next())
                .is_some_and(|state| state != "Z" && state != "X")
        })
    }

    #[tokio::test]
    async fn a_session_keeps_its_state_and_reports_every_exit_code() {
        let mut harness = Harness::start(Some(table(1 << 20, Duration::from_secs(600), 2))).await;
        let registration = within("the registration", harness.registrations.recv())
            .await
            .expect("the worker registers");
        assert!(
            registration
                .capabilities
                .contains(&"remote_shell".to_string()),
            "{:?}",
            registration.capabilities
        );
        let mut shell = harness.shell().await;

        let session = shell.open().await.session_id;
        shell.attach("w", &session, 0).await;

        shell.exec_ok(&session, "cd /tmp").await;
        let cd = shell.until_finished("w").await;
        assert_eq!(cd.len(), 2, "{cd:?}");
        assert_eq!(cd[0].offset, 0);
        assert!(matches!(
            &cd[0].event,
            Some(shell_event::Event::Started(s)) if s.command == "cd /tmp"
        ));
        assert_eq!((cd[1].offset, exit_code(&cd)), (1, Some(0)));

        shell.exec_ok(&session, "pwd").await;
        let pwd = shell.until_finished("w").await;
        assert_eq!(pwd[0].offset, 2);
        assert_eq!(output(&pwd, ShellStream::Stdout), "/tmp\n");
        assert_eq!(exit_code(&pwd), Some(0));
        assert_eq!(pwd.last().map(|e| e.offset), Some(8));

        shell.exec_ok(&session, "false").await;
        assert_eq!(exit_code(&shell.until_finished("w").await), Some(1));

        shell
            .exec_ok(&session, "echo out; echo err >&2; (exit 7)")
            .await;
        let mixed = shell.until_finished("w").await;
        assert_eq!(output(&mixed, ShellStream::Stdout), "out\n");
        assert_eq!(output(&mixed, ShellStream::Stderr), "err\n");
        assert_eq!(exit_code(&mixed), Some(7));

        // Output without a trailing newline keeps every byte, and a command that
        // reads stdin sees EOF instead of the shell's own input.
        shell.exec_ok(&session, "printf 'no newline'; cat").await;
        let partial = shell.until_finished("w").await;
        assert_eq!(output(&partial, ShellStream::Stdout), "no newline");
        assert_eq!(exit_code(&partial), Some(0));

        // One command at a time: a second one is refused, not queued.
        shell.exec_ok(&session, "sleep 2").await;
        let busy = shell.exec(&session, "true").await;
        assert_eq!(error_code(&busy), Some(ShellErrorCode::Busy), "{busy:?}");
        let listed = shell.list().await;
        assert_eq!(listed.len(), 1);
        assert_eq!(listed[0].running.as_deref(), Some("sleep 2"));
        assert_eq!(listed[0].viewers, 1);
        assert_eq!(exit_code(&shell.until_finished("w").await), Some(0));
        shell.exec_ok(&session, "true").await;
        assert_eq!(exit_code(&shell.until_finished("w").await), Some(0));

        for bad in ["", "  ", "a\0b"] {
            let reply = shell.exec(&session, bad).await;
            assert_eq!(error_code(&reply), Some(ShellErrorCode::Invalid), "{bad:?}");
        }
        let long = "x".repeat(64 * 1024 + 1);
        assert_eq!(
            error_code(&shell.exec(&session, &long).await),
            Some(ShellErrorCode::Invalid)
        );
        assert_eq!(
            error_code(&shell.exec("nope", "true").await),
            Some(ShellErrorCode::NotFound)
        );

        // The cap is the worker's own: two sessions here.
        let second = shell.open().await.session_id;
        let third = shell.request(shell_request::Op::Open(ShellOpen {})).await;
        assert_eq!(error_code(&third), Some(ShellErrorCode::Limit), "{third:?}");

        // A shell that exits on its own finishes the command with its status and
        // ends the session.
        shell.exec_ok(&session, "exit 3").await;
        let exit = shell.until("w", |e| closed(e).is_some()).await;
        assert_eq!(exit_code(&exit), Some(3));
        assert_eq!(exit.last().and_then(closed), Some(ShellCloseReason::Exited));
        within("the exited session to leave the table", async {
            loop {
                let sessions = shell.list().await;
                if sessions.iter().all(|s| s.session_id != session) {
                    assert_eq!(sessions.len(), 1);
                    assert_eq!(sessions[0].session_id, second);
                    return;
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        })
        .await;
        assert_eq!(
            error_code(&shell.exec(&session, "true").await),
            Some(ShellErrorCode::NotFound)
        );
        harness.stop().await;
    }

    #[tokio::test]
    async fn closing_a_session_kills_its_whole_process_group() {
        let mut harness = Harness::start(Some(roomy())).await;
        let mut shell = harness.shell().await;
        let session = shell.open().await.session_id;
        shell.attach("w", &session, 0).await;

        shell.exec_ok(&session, "sleep 300 & echo $!").await;
        let events = shell.until_finished("w").await;
        assert_eq!(exit_code(&events), Some(0));
        let pid: u32 = output(&events, ShellStream::Stdout)
            .trim()
            .parse()
            .expect("the background job's pid");
        assert!(alive(pid), "the background job runs");

        assert!(matches!(
            shell.close(&session).await,
            shell_reply::Result::Done(_)
        ));
        let end = shell.until("w", |e| closed(e).is_some()).await;
        assert_eq!(end.last().and_then(closed), Some(ShellCloseReason::Closed));
        within("the background job to die", async {
            while alive(pid) {
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await;
        assert_eq!(
            error_code(&shell.exec(&session, "true").await),
            Some(ShellErrorCode::NotFound)
        );
        assert_eq!(
            error_code(&shell.close(&session).await),
            Some(ShellErrorCode::NotFound)
        );
        harness.stop().await;
    }

    #[tokio::test]
    async fn idle_sessions_are_reaped_unless_watched_or_busy() {
        let mut harness = Harness::start(Some(table(1 << 20, Duration::from_secs(1), 4))).await;
        let mut shell = harness.shell().await;
        let idle = shell.open().await.session_id;
        let watched = shell.open().await.session_id;
        let busy = shell.open().await.session_id;
        shell.attach("w", &watched, 0).await;
        shell.exec_ok(&busy, "sleep 4").await;

        let left = within("the idle session to be reaped", async {
            loop {
                let sessions = shell.list().await;
                if sessions.iter().all(|s| s.session_id != idle) {
                    return sessions;
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        })
        .await;
        let mut left: Vec<_> = left.into_iter().map(|s| s.session_id).collect();
        left.sort();
        let mut expected = vec![watched.clone(), busy.clone()];
        expected.sort();
        assert_eq!(left, expected, "watched and busy sessions stay");
        assert_eq!(
            error_code(&shell.exec(&idle, "true").await),
            Some(ShellErrorCode::NotFound)
        );

        // Unwatched, and later idle, both go.
        shell.detach("w").await;
        within("every session to be reaped", async {
            while !shell.list().await.is_empty() {
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        })
        .await;
        harness.stop().await;
    }

    #[tokio::test]
    async fn a_watch_resumes_from_any_offset_without_repeats() {
        let mut harness = Harness::start(Some(roomy())).await;
        let mut shell = harness.shell().await;
        let session = shell.open().await.session_id;
        shell.attach("first", &session, 0).await;
        shell.exec_ok(&session, "printf 'one\\n'").await;
        let mut transcript = shell.until_finished("first").await;
        shell.exec_ok(&session, "printf 'two\\n'").await;
        transcript.extend(shell.until_finished("first").await);
        shell.detach("first").await;
        // A live watch may have seen an output record grow in two steps; a replay
        // reads it whole. Compared merged, the transcripts must be identical.
        let transcript = merged(transcript);
        assert_eq!(transcript.len(), 6, "{transcript:?}");

        // Contiguous: every event starts where the one before it ended.
        let mut position = 0;
        for event in &transcript {
            assert_eq!(event.offset, position, "{transcript:?}");
            position += len(event);
        }
        let end = position;

        // From the position after the first command: exactly the rest.
        let after_first = transcript[3].offset;
        shell.attach("second", &session, after_first).await;
        let rest = merged(shell.until_finished("second").await);
        assert_eq!(rest, transcript[3..].to_vec());

        // From inside an output record: the rest of it, then everything after.
        let one = transcript[1].offset;
        shell.attach("third", &session, one + 2).await;
        let tail = merged(shell.until_finished("third").await);
        assert_eq!(tail.len(), 2, "{tail:?}");
        assert_eq!(tail[0].offset, one + 2);
        assert_eq!(output(&tail[..1], ShellStream::Stdout), "e\n");
        assert_eq!(tail[1], transcript[2]);
        let next = merged(shell.until_finished("third").await);
        assert_eq!(next, transcript[3..].to_vec());

        // Past the end: attached at the end, which a renewal confirms.
        shell.attach("late", &session, 10_000).await;
        shell.renew("late").await;
        let renewed = shell.event("late").await;
        assert_eq!(renewed.offset, end);
        assert!(matches!(
            renewed.event,
            Some(shell_event::Event::KeepAlive(_))
        ));
        // ... and it sees what comes next.
        shell.exec_ok(&session, "echo three").await;
        let next = shell.until_finished("late").await;
        assert_eq!(next[0].offset, end);
        assert_eq!(output(&next, ShellStream::Stdout), "three\n");

        // Attaching an id again restarts it from the new position.
        shell.attach("late", &session, 0).await;
        let again = merged(shell.until_finished("late").await);
        assert_eq!(again, transcript[..3].to_vec());

        // An unknown session: the watch ends at once with NOT_FOUND.
        shell.attach("ghost", "nope", 0).await;
        let ghost = shell.event("ghost").await;
        assert!(matches!(
            &ghost.event,
            Some(shell_event::Event::Error(e)) if e.code == i32::from(ShellErrorCode::NotFound)
        ));
        harness.stop().await;
    }

    #[tokio::test]
    async fn a_wrapped_buffer_starts_a_late_watch_with_truncated() {
        let mut harness = Harness::start(Some(table(4096, Duration::from_secs(600), 4))).await;
        let mut shell = harness.shell().await;
        let session = shell.open().await.session_id;
        shell.attach("live", &session, 0).await;
        shell
            .exec_ok(&session, "head -c 20000 /dev/zero | tr '\\0' x")
            .await;
        let live = shell.until_finished("live").await;
        assert_eq!(exit_code(&live), Some(0));
        let end = live.last().map(|e| e.offset + 1).expect("finished");
        assert_eq!(end, 20_002, "started, 20000 bytes, finished");
        shell.detach("live").await;

        shell.attach("late", &session, 0).await;
        let late = shell.until_finished("late").await;
        let Some(shell_event::Event::Truncated(truncated)) = late[0].event else {
            panic!("the late watch starts with truncated: {:?}", late[0]);
        };
        let oldest = late[0].offset;
        assert!(oldest > 0);
        assert_eq!(truncated.dropped, oldest, "everything before the oldest");
        let mut position = oldest;
        for event in &late[1..] {
            assert_eq!(event.offset, position);
            position += len(event);
        }
        assert_eq!(position, end);
        let kept = output(&late, ShellStream::Stdout);
        assert!(
            !kept.is_empty() && kept.len() <= 4096,
            "{} bytes kept",
            kept.len()
        );
        assert!(kept.bytes().all(|b| b == b'x'));
        harness.stop().await;
    }

    #[tokio::test]
    async fn a_lost_channel_drops_its_watches_but_not_the_sessions() {
        let mut harness = Harness::start(Some(roomy())).await;
        let mut shell = harness.shell().await;
        let session = shell.open().await.session_id;
        shell.attach("w", &session, 0).await;
        shell.exec_ok(&session, "export GREETING=hello").await;
        let before = shell.until_finished("w").await;
        drop(shell);

        // The worker opens a new channel within the same agent session.
        let mut shell = harness.shell().await;
        let sessions = shell.list().await;
        assert_eq!(sessions.len(), 1);
        assert_eq!(sessions[0].session_id, session);
        assert_eq!(sessions[0].viewers, 0, "the old channel's watch is gone");
        shell.attach("w", &session, 0).await;
        let replay = merged(shell.until_finished("w").await);
        assert_eq!(replay, merged(before));
        shell.exec_ok(&session, "echo $GREETING").await;
        assert_eq!(
            output(&shell.until_finished("w").await, ShellStream::Stdout),
            "hello\n"
        );
        assert!(
            harness.registrations.try_recv().is_ok() && harness.registrations.try_recv().is_err(),
            "one registration: the agent session never ended"
        );
        harness.stop().await;
    }

    #[tokio::test]
    async fn shutdown_ends_every_session_and_tells_the_watches() {
        let shells = roomy();
        let mut harness = Harness::start(Some(shells.clone())).await;
        let mut shell = harness.shell().await;
        let session = shell.open().await.session_id;
        shell.attach("w", &session, 0).await;
        shell.exec_ok(&session, "sleep 300 & echo $!").await;
        let events = shell.until_finished("w").await;
        let pid: u32 = output(&events, ShellStream::Stdout).trim().parse().unwrap();

        within("the shutdown", shells.shutdown()).await;
        let end = shell.until("w", |e| closed(e).is_some()).await;
        assert_eq!(
            end.last().and_then(closed),
            Some(ShellCloseReason::Shutdown)
        );
        assert!(shell.list().await.is_empty());
        within("the background job to die", async {
            while alive(pid) {
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await;
        harness.stop().await;
    }

    #[tokio::test]
    async fn dropping_the_table_kills_every_session() {
        let shells = roomy();
        let mut harness = Harness::start(Some(shells.clone())).await;
        let mut shell = harness.shell().await;
        let session = shell.open().await.session_id;
        shell.attach("w", &session, 0).await;
        shell.exec_ok(&session, "sleep 300 & echo $!").await;
        let events = shell.until_finished("w").await;
        let pid: u32 = output(&events, ShellStream::Stdout).trim().parse().unwrap();
        shell.detach("w").await;
        drop(shell);
        // The agent holds the last other clone; stopping it drops it.
        harness.stop().await;
        drop(shells);
        within("the background job to die", async {
            while alive(pid) {
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await;
    }
}
