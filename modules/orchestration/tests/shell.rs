//! The remote-shell relay end to end: the operator API's shell calls, through
//! the in-process relay, into the real `WorkerAgent` service, over a real
//! `ShellChannel` to a scripted worker — and back.
//!
//! The worker is a fake that speaks the protocol (`Fake`): sessions are
//! transcripts it appends to, watches are cursors it pumps from, and it can be
//! told to lose an event on its way up, which is what the relay's gap repair
//! is for.

#![allow(clippy::unwrap_used, clippy::panic, clippy::expect_used)]

mod common;

use auth::entities::db::account::{
    AccountRole, CreateAccount, FindAccountByEmail, UpdateAccountRole,
};
use auth::services::identity::{Identity, IdentityKind};
use auth::services::session::{Login, LoginResult};
use auth::utils::password::{Argon2PasswordAlgorithm, PasswordAlgorithm};
use base::services::config::ConfigStore;
use common::*;
use kanau::processor::Processor;
use orchestration::entities::db::server::ServerId;
use orchestration::events::shell::{SHELL_DOWN_CHANNEL, shell_up_channel};
use orchestration::hooks::shell::{
    ShellRouter, run_shell_down_subscriber, run_shell_up_subscriber,
};
use orchestration::rpc::agent_middleware::{AgentLayer, REFRESH_KEY_METADATA};
use orchestration::rpc::{OrchestrationGrpc, WorkerAgentGrpc};
use orchestration::services::agent::{AgentService, RegisterCredential, RegisterWorker};
use orchestration::services::config::OrchestrationConfigService;
use orchestration::services::shell::{ShellDownPublisher, ShellService, ShellTimings};
use orchestration::services::shell_channel::{ShellChannels, ShellUpPublisher};
use orchestration::services::watch::{SessionLease, WatchHub};
use parking_lot::Mutex;
use rpguru_sdk::orchestration as api;
use rpguru_sdk::orchestration::orchestration_server::Orchestration;
use rpguru_sdk::orchestration_agent as pb;
use rpguru_sdk::orchestration_agent::worker_agent_client::WorkerAgentClient;
use rpguru_sdk::orchestration_agent::worker_agent_server::WorkerAgentServer;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use testcontainers_modules::redis::Redis;
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio_stream::wrappers::ReceiverStream;
use tokio_util::sync::CancellationToken;
use tonic::{Code, Request, Status};

/// How long a test waits for something that should happen at once.
const WAIT: Duration = Duration::from_secs(10);

/// The protocol's timings, with the reply bound short enough that a call
/// nobody answers fails within the test.
const TIMINGS: ShellTimings = ShellTimings {
    reply: Duration::from_secs(2),
    renew: Duration::from_secs(10),
    silence: Duration::from_secs(30),
};

// --- the fake worker -------------------------------------------------------------

#[derive(Default)]
struct FakeSession {
    transcript: Vec<pb::ShellEvent>,
    end: u64,
    running: Option<String>,
}

/// A worker's shell state machine, minus the shells.
struct Fake {
    sessions: BTreeMap<String, FakeSession>,
    /// watch id → (session, cursor).
    watches: HashMap<String, (String, u64)>,
    opened: u32,
    limit: usize,
    /// Transcript positions whose event is lost once on its way to a watch.
    lose_once: HashSet<u64>,
    /// Everything the master sent but keep-alives, in order.
    log: Vec<pb::shell_down::Message>,
}

impl Fake {
    fn new() -> Arc<Mutex<Self>> {
        Arc::new(Mutex::new(Self {
            sessions: BTreeMap::new(),
            watches: HashMap::new(),
            opened: 0,
            limit: 2,
            lose_once: HashSet::new(),
            log: Vec::new(),
        }))
    }

    fn handle(&mut self, down: pb::ShellDown) -> Vec<pb::ShellUp> {
        use pb::shell_down::Message;
        let Some(message) = down.message else {
            return Vec::new();
        };
        if !matches!(message, Message::KeepAlive(_)) {
            self.log.push(message.clone());
        }
        match message {
            Message::KeepAlive(_) => Vec::new(),
            Message::Request(request) => {
                let mut ups = Vec::new();
                let result = self.answer(request.op, &mut ups);
                ups.insert(
                    0,
                    pb::ShellUp {
                        message: Some(pb::shell_up::Message::Reply(pb::ShellReply {
                            request_id: request.request_id,
                            result: Some(result),
                        })),
                    },
                );
                ups
            }
            Message::Attach(attach) => {
                let Some(session) = self.sessions.get(&attach.session_id) else {
                    return vec![watch_event(
                        &attach.watch_id,
                        pb::ShellEvent {
                            offset: 0,
                            event: Some(pb::shell_event::Event::Error(error(
                                pb::ShellErrorCode::NotFound,
                                "no such shell session",
                            ))),
                        },
                    )];
                };
                let cursor = attach.from_offset.min(session.end);
                self.watches
                    .insert(attach.watch_id.clone(), (attach.session_id, cursor));
                let mut ups = Vec::new();
                self.pump(&attach.watch_id, &mut ups);
                ups
            }
            Message::Renew(renew) => match self.watches.get(&renew.watch_id) {
                Some((_, cursor)) => vec![watch_event(
                    &renew.watch_id,
                    pb::ShellEvent {
                        offset: *cursor,
                        event: Some(pb::shell_event::Event::KeepAlive(pb::ShellKeepAlive {})),
                    },
                )],
                None => Vec::new(),
            },
            Message::Detach(detach) => {
                self.watches.remove(&detach.watch_id);
                Vec::new()
            }
        }
    }

    fn answer(
        &mut self,
        op: Option<pb::shell_request::Op>,
        ups: &mut Vec<pb::ShellUp>,
    ) -> pb::shell_reply::Result {
        use pb::shell_reply::Result as R;
        use pb::shell_request::Op;
        match op {
            Some(Op::Open(_)) => {
                if self.sessions.len() >= self.limit {
                    return R::Error(error(
                        pb::ShellErrorCode::Limit,
                        "the worker's remote shell session limit (2) is reached",
                    ));
                }
                self.opened += 1;
                let id = format!("s{}", self.opened);
                self.sessions.insert(id.clone(), FakeSession::default());
                R::Opened(pb::ShellSessionInfo {
                    session_id: id,
                    opened_at: 1_790_000_096,
                    running: None,
                    end_offset: 0,
                    viewers: 0,
                })
            }
            Some(Op::Exec(exec)) => {
                let Some(session) = self.sessions.get_mut(&exec.session_id) else {
                    return R::Error(error(pb::ShellErrorCode::NotFound, "no such shell session"));
                };
                if exec.command.is_empty() {
                    return R::Error(error(pb::ShellErrorCode::Invalid, "the command is empty"));
                }
                if exec.command == "fail" {
                    return R::Error(error(pb::ShellErrorCode::Failed, "writing to bash failed"));
                }
                if session.running.is_some() {
                    return R::Error(error(
                        pb::ShellErrorCode::Busy,
                        "a command is still running in this session",
                    ));
                }
                use pb::shell_event::Event;
                append(
                    session,
                    Event::Started(pb::ShellCommandStarted {
                        command: exec.command.clone(),
                    }),
                    1,
                );
                if exec.command == "sleep" {
                    session.running = Some(exec.command);
                } else {
                    let line = format!("{}\n", exec.command).into_bytes();
                    let length = line.len() as u64;
                    append(
                        session,
                        Event::Output(pb::ShellOutput {
                            stream: pb::ShellStream::Stdout.into(),
                            data: line,
                        }),
                        length,
                    );
                    append(
                        session,
                        Event::Finished(pb::ShellCommandFinished { exit_code: 0 }),
                        1,
                    );
                }
                let watching: Vec<String> = self
                    .watches
                    .iter()
                    .filter(|(_, (session, _))| *session == exec.session_id)
                    .map(|(watch, _)| watch.clone())
                    .collect();
                for watch in watching {
                    self.pump(&watch, ups);
                }
                R::Done(pb::ShellDone {})
            }
            Some(Op::Close(close)) => {
                let Some(session) = self.sessions.remove(&close.session_id) else {
                    return R::Error(error(pb::ShellErrorCode::NotFound, "no such shell session"));
                };
                let watching: Vec<String> = self
                    .watches
                    .iter()
                    .filter(|(_, (session, _))| *session == close.session_id)
                    .map(|(watch, _)| watch.clone())
                    .collect();
                for watch in watching {
                    self.watches.remove(&watch);
                    ups.push(watch_event(
                        &watch,
                        pb::ShellEvent {
                            offset: session.end,
                            event: Some(pb::shell_event::Event::Closed(pb::ShellSessionClosed {
                                reason: pb::ShellCloseReason::Closed.into(),
                            })),
                        },
                    ));
                }
                R::Done(pb::ShellDone {})
            }
            Some(Op::List(_)) => R::Sessions(pb::ShellSessionList {
                sessions: self
                    .sessions
                    .iter()
                    .map(|(id, session)| pb::ShellSessionInfo {
                        session_id: id.clone(),
                        opened_at: 1_790_000_096,
                        running: session.running.clone(),
                        end_offset: session.end,
                        viewers: self
                            .watches
                            .values()
                            .filter(|(watched, _)| watched == id)
                            .count() as u32,
                    })
                    .collect(),
            }),
            None => R::Error(error(pb::ShellErrorCode::Invalid, "no operation")),
        }
    }

    /// Sends a watch everything from its cursor on, losing what it was told to.
    fn pump(&mut self, watch: &str, ups: &mut Vec<pb::ShellUp>) {
        let Some((session, cursor)) = self.watches.get_mut(watch) else {
            return;
        };
        let Some(session) = self.sessions.get(session.as_str()) else {
            return;
        };
        for event in &session.transcript {
            if event.offset < *cursor {
                continue;
            }
            if self.lose_once.remove(&event.offset) {
                continue;
            }
            ups.push(watch_event(watch, event.clone()));
        }
        *cursor = session.end;
    }

    /// The positions `session` was attached from, in order.
    fn attaches(&self, session: &str) -> Vec<u64> {
        self.log
            .iter()
            .filter_map(|message| match message {
                pb::shell_down::Message::Attach(attach) if attach.session_id == session => {
                    Some(attach.from_offset)
                }
                _ => None,
            })
            .collect()
    }
}

fn append(session: &mut FakeSession, event: pb::shell_event::Event, length: u64) {
    session.transcript.push(pb::ShellEvent {
        offset: session.end,
        event: Some(event),
    });
    session.end += length;
}

fn error(code: pb::ShellErrorCode, message: &str) -> pb::ShellError {
    pb::ShellError {
        code: code.into(),
        message: message.to_string(),
    }
}

fn watch_event(watch: &str, event: pb::ShellEvent) -> pb::ShellUp {
    pb::ShellUp {
        message: Some(pb::shell_up::Message::Event(pb::ShellWatchEvent {
            watch_id: watch.to_string(),
            event: Some(event),
        })),
    }
}

/// One `ShellChannel`, answered by `fake` until the master ends it.
struct FakeWorker {
    fake: Arc<Mutex<Fake>>,
    /// The status the master ended the stream with, if not a plain close.
    ended: JoinHandle<Option<Status>>,
}

async fn connect(
    addr: SocketAddr,
    key: &str,
    fake: Arc<Mutex<Fake>>,
) -> Result<FakeWorker, Box<dyn std::error::Error>> {
    let mut client = WorkerAgentClient::connect(format!("http://{addr}")).await?;
    let (up_tx, up_rx) = mpsc::channel::<pb::ShellUp>(64);
    let mut request = Request::new(ReceiverStream::new(up_rx));
    request
        .metadata_mut()
        .insert(REFRESH_KEY_METADATA, key.parse()?);
    let mut downs = client.shell_channel(request).await?.into_inner();
    let state = fake.clone();
    let ended = tokio::spawn(async move {
        loop {
            let down = match downs.message().await {
                Ok(Some(down)) => down,
                Ok(None) => return None,
                Err(status) => return Some(status),
            };
            let ups = state.lock().handle(down);
            for up in ups {
                if up_tx.send(up).await.is_err() {
                    return None;
                }
            }
        }
    });
    Ok(FakeWorker { fake, ended })
}

// --- the master ------------------------------------------------------------------

struct Harness {
    w: World,
    api: OrchestrationGrpc,
    agents: AgentService,
    addr: SocketAddr,
    server: ServerId,
}

/// A world with one server, the worker API served on a real port, and the
/// operator API in process. Registration leases lapse at once, so a test can
/// register the same server again.
async fn harness(pool: sqlx::PgPool) -> Result<Harness, Box<dyn std::error::Error>> {
    harness_with(pool, None).await
}

/// [`harness`], with the relay running over `redis` instead of in process: the
/// dashboard side and the worker side each with their own publisher and
/// subscriber, exactly as two replicas would be.
async fn harness_with(
    pool: sqlx::PgPool,
    redis: Option<redis::Client>,
) -> Result<Harness, Box<dyn std::error::Error>> {
    let mut w = world(pool).await?;
    w.shell.timings = TIMINGS;
    if let Some(client) = redis {
        let manager = redis::aio::ConnectionManager::new(client.clone()).await?;
        let router = ShellRouter::new();
        w.shells = ShellChannels {
            db: w.db.clone(),
            hub: Default::default(),
            up: ShellUpPublisher::Redis(manager.clone()),
        };
        w.shell = ShellService {
            db: w.db.clone(),
            router: router.clone(),
            down: ShellDownPublisher::Redis(manager.clone()),
            timings: TIMINGS,
        };
        // Both subscribers live as long as the test's runtime.
        let shutdown = CancellationToken::new();
        tokio::spawn(run_shell_up_subscriber(
            client.clone(),
            router.clone(),
            shutdown.clone(),
        ));
        tokio::spawn(run_shell_down_subscriber(
            client,
            w.shells.clone(),
            shutdown,
        ));
        // Pub/sub delivers to whoever is subscribed at the time: wait until
        // both ends are.
        let up = shell_up_channel(router.replica());
        tokio::time::timeout(WAIT, async {
            loop {
                let counts: Vec<(String, u64)> = redis::cmd("PUBSUB")
                    .arg("NUMSUB")
                    .arg(SHELL_DOWN_CHANNEL)
                    .arg(&up)
                    .query_async(&mut manager.clone())
                    .await
                    .expect("PUBSUB NUMSUB answers");
                if counts.iter().all(|(_, subscribers)| *subscribers == 1) {
                    return;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await?;
    }
    let root = canvas(&w.db, "root").await?;
    let server = server(&w.db, &root, "tokyo").await?.id;
    let agents = AgentService {
        db: w.db.clone(),
        hub: WatchHub::default(),
        lease: SessionLease {
            ttl: Duration::ZERO,
            heartbeat: Duration::from_secs(10),
        },
        notifier: w.notifier.clone(),
        config: w.config.clone(),
    };
    let workers = WorkerAgentGrpc {
        agents: agents.clone(),
        health: w.health.clone(),
        ca: w.ca.clone(),
        db: w.db.clone(),
        hub: agents.hub.clone(),
        lease: agents.lease,
        shells: w.shells.clone(),
    };
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;
    let layer = AgentLayer::new(agents.clone());
    tokio::spawn(async move {
        let _ = tonic::transport::Server::builder()
            .layer(layer)
            .add_service(WorkerAgentServer::new(workers))
            .serve_with_incoming(tonic::transport::server::TcpIncoming::from(listener))
            .await;
    });
    let api = OrchestrationGrpc {
        canvases: w.canvases.clone(),
        servers: w.servers.clone(),
        graph: w.graph.clone(),
        rollout: w.rollout.clone(),
        health: w.health.clone(),
        dns: w.dns.clone(),
        certificates: w.certificates.clone(),
        configs: OrchestrationConfigService {
            configs: ConfigStore { db: w.db.clone() },
        },
        live: w.live.clone(),
        sessions: w.sessions.clone(),
        shell: w.shell.clone(),
    };
    Ok(Harness {
        w,
        api,
        agents,
        addr,
        server,
    })
}

impl Harness {
    /// Registers the server's worker, advertising `capabilities`; returns its
    /// refresh key.
    async fn register(&self, capabilities: &[&str]) -> Result<String, Box<dyn std::error::Error>> {
        Ok(self
            .agents
            .process(RegisterWorker {
                credential: RegisterCredential::Operator(machine()),
                server_id: self.server.clone(),
                running_revision: 0,
                observed: None,
                reported: None,
                agent_version: None,
                agent_arch: None,
                capabilities: capabilities.iter().map(|c| c.to_string()).collect(),
                last_update_error: None,
            })
            .await?)
    }

    async fn open(&self, actor: Identity) -> Result<api::ShellSession, Status> {
        Ok(self
            .api
            .open_shell_session(as_actor(
                actor,
                api::OpenShellSessionRequest {
                    server_id: self.server.to_string(),
                },
            ))
            .await?
            .into_inner()
            .session
            .expect("an opened session"))
    }

    async fn exec(&self, session: &str, command: &str) -> Result<(), Status> {
        self.api
            .send_shell_command(as_actor(
                operator(),
                api::SendShellCommandRequest {
                    server_id: self.server.to_string(),
                    session_id: session.to_string(),
                    command: command.to_string(),
                },
            ))
            .await?;
        Ok(())
    }

    async fn close(&self, session: &str) -> Result<(), Status> {
        self.api
            .close_shell_session(as_actor(
                operator(),
                api::CloseShellSessionRequest {
                    server_id: self.server.to_string(),
                    session_id: session.to_string(),
                },
            ))
            .await?;
        Ok(())
    }

    async fn list(&self) -> Result<Vec<api::ShellSession>, Status> {
        Ok(self
            .api
            .list_shell_sessions(as_actor(
                operator(),
                api::ListShellSessionsRequest {
                    server_id: self.server.to_string(),
                },
            ))
            .await?
            .into_inner()
            .sessions)
    }

    async fn watch(
        &self,
        actor: Identity,
        session_token: &str,
        session: &str,
        from_offset: u64,
    ) -> Result<ReceiverStream<Result<api::ShellEvent, Status>>, Status> {
        let mut request = as_actor(
            actor,
            api::WatchShellSessionRequest {
                server_id: self.server.to_string(),
                session_id: session.to_string(),
                from_offset,
            },
        );
        request.metadata_mut().insert(
            auth::rpc::middleware::SESSION_ID_METADATA,
            session_token
                .parse()
                .expect("a session token is valid metadata"),
        );
        Ok(self.api.watch_shell_session(request).await?.into_inner())
    }

    /// A logged-in Admin's session token, which the watch stream re-validates.
    async fn admin_session(&self) -> Result<String, Box<dyn std::error::Error>> {
        let hasher = Argon2PasswordAlgorithm::default();
        self.w
            .db
            .process(CreateAccount {
                email: "admin@example.com".to_string(),
                password_hash: hasher.hash_password("admin-password")?,
                role: AccountRole::Admin,
            })
            .await?;
        match self
            .w
            .sessions
            .process(Login {
                email: "admin@example.com".to_string(),
                password: "admin-password".to_string(),
                user_agent: "tests".to_string(),
            })
            .await?
        {
            LoginResult::Success(token) => Ok(token),
            LoginResult::InvalidCredentials => Err("login failed".into()),
        }
    }
}

fn as_actor<T>(actor: Identity, message: T) -> Request<T> {
    let mut request = Request::new(message);
    request.extensions_mut().insert(actor);
    request
}

fn identity(role: AccountRole, kind: IdentityKind) -> Identity {
    Identity {
        account_id: auth::entities::db::account::AccountId::from_key("someone"),
        role,
        kind,
    }
}

async fn next_event(
    stream: &mut ReceiverStream<Result<api::ShellEvent, Status>>,
) -> Option<Result<api::ShellEvent, Status>> {
    use tokio_stream::StreamExt;
    tokio::time::timeout(WAIT, stream.next())
        .await
        .expect("the watch moved within the wait")
}

fn event(offset: u64, body: api::shell_event::Event) -> api::ShellEvent {
    api::ShellEvent {
        offset,
        event: Some(body),
    }
}

fn output(offset: u64, text: &str) -> api::ShellEvent {
    event(
        offset,
        api::shell_event::Event::Output(api::ShellOutput {
            stream: api::ShellStream::Stdout.into(),
            data: text.as_bytes().to_vec(),
        }),
    )
}

// --- tests -----------------------------------------------------------------------

/// Every shell call needs a human Admin: lesser roles, and machine credentials
/// whatever their role, are refused before anything reaches the worker.
#[sqlx::test(migrator = "base::db::MIGRATOR")]
async fn only_a_human_admin_may_use_the_shell(pool: sqlx::PgPool) -> TestResult {
    let h = harness(pool).await?;
    let key = h.register(&["remote_shell"]).await?;
    let fake = Fake::new();
    let _worker = connect(h.addr, &key, fake.clone()).await?;

    for actor in [
        identity(AccountRole::Maintainer, IdentityKind::Session),
        identity(AccountRole::Observer, IdentityKind::Session),
        identity(AccountRole::Maintainer, IdentityKind::ApiKey),
        identity(AccountRole::Admin, IdentityKind::ApiKey),
    ] {
        let label = format!("{:?} over {:?}", actor.role, actor.kind);
        let server_id = h.server.to_string();
        let codes = [
            h.open(actor.clone()).await.map(|_| ()),
            h.api
                .send_shell_command(as_actor(
                    actor.clone(),
                    api::SendShellCommandRequest {
                        server_id: server_id.clone(),
                        session_id: "s1".into(),
                        command: "id".into(),
                    },
                ))
                .await
                .map(|_| ()),
            h.api
                .close_shell_session(as_actor(
                    actor.clone(),
                    api::CloseShellSessionRequest {
                        server_id: server_id.clone(),
                        session_id: "s1".into(),
                    },
                ))
                .await
                .map(|_| ()),
            h.api
                .list_shell_sessions(as_actor(
                    actor.clone(),
                    api::ListShellSessionsRequest {
                        server_id: server_id.clone(),
                    },
                ))
                .await
                .map(|_| ()),
            h.watch(actor.clone(), "not-a-session", "s1", 0)
                .await
                .map(|_| ()),
        ];
        for result in codes {
            assert_eq!(
                result.unwrap_err().code(),
                Code::PermissionDenied,
                "{label} must be refused"
            );
        }
    }
    assert!(fake.lock().log.is_empty(), "nothing reached the worker");
    Ok(())
}

/// The master refuses a server whose worker did not advertise `remote_shell`
/// — whatever is connected — and names the reason; an unknown server is
/// NOT_FOUND.
#[sqlx::test(migrator = "base::db::MIGRATOR")]
async fn a_worker_that_did_not_advertise_is_refused(pool: sqlx::PgPool) -> TestResult {
    let h = harness(pool).await?;
    let key = h.register(&["route_table", "relay_confirm"]).await?;
    let fake = Fake::new();
    let _worker = connect(h.addr, &key, fake.clone()).await?;

    let refused = h.open(operator()).await.unwrap_err();
    assert_eq!(refused.code(), Code::FailedPrecondition);
    assert_eq!(
        refused.message(),
        "this server's worker does not offer remote shell"
    );
    let token = h.admin_session().await?;
    let refused = h.watch(operator(), &token, "s1", 0).await.unwrap_err();
    assert_eq!(refused.code(), Code::FailedPrecondition);
    assert!(fake.lock().log.is_empty());

    let unknown = h
        .api
        .list_shell_sessions(as_actor(
            operator(),
            api::ListShellSessionsRequest {
                server_id: key_like("nosuchserver"),
            },
        ))
        .await
        .unwrap_err();
    assert_eq!(unknown.code(), Code::NotFound);
    Ok(())
}

fn key_like(name: &str) -> String {
    common::key(name)
}

/// Open, exec, list and close reach the worker and come back, and every refusal
/// the worker can give arrives as its documented status with the worker's own
/// words.
#[sqlx::test(migrator = "base::db::MIGRATOR")]
async fn calls_are_relayed_with_their_status(pool: sqlx::PgPool) -> TestResult {
    let h = harness(pool).await?;
    let key = h.register(&["remote_shell"]).await?;
    let fake = Fake::new();
    let _worker = connect(h.addr, &key, fake.clone()).await?;

    let first = h.open(operator()).await?;
    assert_eq!(first.session_id, "s1");
    assert_eq!(first.opened_at, "2026-09-21T14:14:56Z");
    let second = h.open(operator()).await?;
    let limit = h.open(operator()).await.unwrap_err();
    assert_eq!(limit.code(), Code::ResourceExhausted);
    assert_eq!(
        limit.message(),
        "the worker's remote shell session limit (2) is reached"
    );

    h.exec(&first.session_id, "pwd").await?;
    h.exec(&first.session_id, "sleep").await?;
    let busy = h.exec(&first.session_id, "pwd").await.unwrap_err();
    assert_eq!(busy.code(), Code::FailedPrecondition);
    assert_eq!(busy.message(), "a command is still running in this session");
    let invalid = h.exec(&second.session_id, "").await.unwrap_err();
    assert_eq!(invalid.code(), Code::InvalidArgument);
    let failed = h.exec(&second.session_id, "fail").await.unwrap_err();
    assert_eq!(failed.code(), Code::Internal);

    let sessions = h.list().await?;
    assert_eq!(sessions.len(), 2);
    assert_eq!(sessions[0].session_id, "s1");
    assert_eq!(sessions[0].running.as_deref(), Some("sleep"));
    // started + "pwd\n" + finished, then started of `sleep`.
    assert_eq!(sessions[0].end_offset, 7);
    assert_eq!(sessions[1].running, None);

    h.close(&first.session_id).await?;
    let gone = h.close(&first.session_id).await.unwrap_err();
    assert_eq!(gone.code(), Code::NotFound);
    let gone = h.exec(&first.session_id, "pwd").await.unwrap_err();
    assert_eq!(gone.code(), Code::NotFound);
    assert_eq!(h.list().await?.len(), 1);
    Ok(())
}

/// A server that advertised the shell but whose worker is not connected
/// anywhere answers UNAVAILABLE once the reply bound passes — calls and
/// watches alike.
#[sqlx::test(migrator = "base::db::MIGRATOR")]
async fn no_connected_worker_is_unavailable(pool: sqlx::PgPool) -> TestResult {
    let h = harness(pool).await?;
    h.register(&["remote_shell"]).await?;

    let started = tokio::time::Instant::now();
    let silent = h.open(operator()).await.unwrap_err();
    assert_eq!(silent.code(), Code::Unavailable);
    assert_eq!(silent.message(), "the worker did not answer");
    assert!(started.elapsed() >= TIMINGS.reply);

    let token = h.admin_session().await?;
    let silent = h.watch(operator(), &token, "s1", 0).await.unwrap_err();
    assert_eq!(silent.code(), Code::Unavailable);
    Ok(())
}

/// A watch relays the transcript in order. An event lost on the way is noticed
/// by the next one starting past the position, the watch re-attaches from the
/// position, and the client sees each event once. A reconnect resumes from an
/// event's end, and closing the session ends every watch after `closed`.
#[sqlx::test(migrator = "base::db::MIGRATOR")]
async fn a_watch_relays_the_transcript_and_repairs_a_gap(pool: sqlx::PgPool) -> TestResult {
    let h = harness(pool).await?;
    let key = h.register(&["remote_shell"]).await?;
    let fake = Fake::new();
    let _worker = connect(h.addr, &key, fake.clone()).await?;
    let token = h.admin_session().await?;

    let unknown = h.watch(operator(), &token, "s9", 0).await.unwrap_err();
    assert_eq!(
        unknown.code(),
        Code::NotFound,
        "an unknown session fails the call"
    );

    let session = h.open(operator()).await?.session_id;
    h.exec(&session, "echo hi").await?;
    // started@0, "echo hi\n"@1..9, finished@9: lose the output once.
    fake.lock().lose_once.insert(1);

    let mut watch = h.watch(operator(), &token, &session, 0).await?;
    let started = event(
        0,
        api::shell_event::Event::Started(api::ShellCommandStarted {
            command: "echo hi".into(),
        }),
    );
    let finished = event(
        9,
        api::shell_event::Event::Finished(api::ShellCommandFinished { exit_code: 0 }),
    );
    assert_eq!(next_event(&mut watch).await.unwrap()?, started);
    assert_eq!(
        next_event(&mut watch).await.unwrap()?,
        output(1, "echo hi\n")
    );
    assert_eq!(next_event(&mut watch).await.unwrap()?, finished);
    assert_eq!(
        fake.lock().attaches(&session),
        vec![0, 1],
        "the gap re-attached from the position"
    );

    // A second viewer resuming from the output's end gets only what follows.
    let mut resumed = h.watch(operator(), &token, &session, 9).await?;
    assert_eq!(next_event(&mut resumed).await.unwrap()?, finished);

    // Live output reaches both.
    h.exec(&session, "pwd").await?;
    let pwd_started = event(
        10,
        api::shell_event::Event::Started(api::ShellCommandStarted {
            command: "pwd".into(),
        }),
    );
    for stream in [&mut watch, &mut resumed] {
        assert_eq!(next_event(stream).await.unwrap()?, pwd_started);
        assert_eq!(next_event(stream).await.unwrap()?, output(11, "pwd\n"));
        assert_eq!(
            next_event(stream).await.unwrap()?,
            event(
                15,
                api::shell_event::Event::Finished(api::ShellCommandFinished { exit_code: 0 }),
            )
        );
    }

    h.close(&session).await?;
    let closed = event(
        16,
        api::shell_event::Event::Closed(api::ShellSessionClosed {
            reason: api::ShellCloseReason::Closed.into(),
        }),
    );
    for stream in [&mut watch, &mut resumed] {
        assert_eq!(next_event(stream).await.unwrap()?, closed);
        assert!(
            next_event(stream).await.is_none(),
            "the stream ends after closed"
        );
    }
    Ok(())
}

/// A client that goes away detaches its watch on the worker.
#[sqlx::test(migrator = "base::db::MIGRATOR")]
async fn a_dropped_watch_detaches(pool: sqlx::PgPool) -> TestResult {
    let h = harness(pool).await?;
    let key = h.register(&["remote_shell"]).await?;
    let fake = Fake::new();
    let _worker = connect(h.addr, &key, fake.clone()).await?;
    let token = h.admin_session().await?;
    let session = h.open(operator()).await?.session_id;

    let watch = h.watch(operator(), &token, &session, 0).await?;
    assert_eq!(h.list().await?[0].viewers, 1);
    drop(watch);
    tokio::time::timeout(WAIT, async {
        loop {
            if fake.lock().watches.is_empty() {
                return;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("the watch was detached");
    assert!(
        fake.lock()
            .log
            .iter()
            .any(|message| matches!(message, pb::shell_down::Message::Detach(_)))
    );
    Ok(())
}

/// The watch re-checks its viewer's permission on every keep-alive, not only
/// when it opens: an Admin demoted while attached stops receiving the
/// transcript.
#[sqlx::test(migrator = "base::db::MIGRATOR")]
async fn a_demoted_admin_loses_the_watch(pool: sqlx::PgPool) -> TestResult {
    let mut h = harness(pool).await?;
    h.api.live.config.stream_keepalive_secs = 1;
    let key = h.register(&["remote_shell"]).await?;
    let fake = Fake::new();
    let _worker = connect(h.addr, &key, fake.clone()).await?;
    let token = h.admin_session().await?;
    let session = h.open(operator()).await?.session_id;
    let mut watch = h.watch(operator(), &token, &session, 0).await?;

    let admin =
        h.w.db
            .process(FindAccountByEmail {
                email: "admin@example.com",
            })
            .await?
            .expect("the admin exists");
    h.w.db
        .process(UpdateAccountRole {
            id: admin.id,
            role: AccountRole::Maintainer,
        })
        .await?;

    // Keep-alives keep coming until the tick that sees the new role.
    let ended = tokio::time::timeout(WAIT, async {
        loop {
            match next_event(&mut watch).await {
                Some(Ok(_)) => continue,
                Some(Err(status)) => return status,
                None => panic!("the watch ended without a status"),
            }
        }
    })
    .await
    .expect("the watch ended once the role changed");
    assert_eq!(ended.code(), Code::PermissionDenied);
    Ok(())
}

/// The newest registration owns the shell channel. A registration moves past
/// a held stream, which the next request finds out (it re-reads the
/// generation) and ends instead of trusting; the new key's stream then
/// answers, and a reconnect on the same key replaces it in turn.
#[sqlx::test(migrator = "base::db::MIGRATOR")]
async fn a_newer_registration_supersedes_the_shell_channel(pool: sqlx::PgPool) -> TestResult {
    let h = harness(pool).await?;
    let first_key = h.register(&["remote_shell"]).await?;
    let first = connect(h.addr, &first_key, Fake::new()).await?;
    h.open(operator()).await?;

    let second_key = h.register(&["remote_shell"]).await?;
    let stale = h.open(operator()).await.unwrap_err();
    assert_eq!(
        stale.code(),
        Code::Unavailable,
        "the stale stream was not trusted"
    );
    let ended = tokio::time::timeout(WAIT, first.ended).await??;
    assert_eq!(
        ended.map(|status| status.code()),
        Some(Code::Unauthenticated)
    );
    assert_eq!(
        first.fake.lock().log.len(),
        1,
        "only the request made before the registration reached the old stream"
    );

    let second = connect(h.addr, &second_key, Fake::new()).await?;
    assert_eq!(h.open(operator()).await?.session_id, "s1");
    assert_eq!(second.fake.lock().sessions.len(), 1);

    let third = connect(h.addr, &second_key, Fake::new()).await?;
    let replaced = tokio::time::timeout(WAIT, second.ended).await??;
    assert_eq!(replaced.map(|status| status.code()), Some(Code::Aborted));
    h.open(operator()).await?;
    assert_eq!(third.fake.lock().sessions.len(), 1);
    Ok(())
}

/// The relay over a real Redis: requests published on the shared down channel
/// reach the replica holding the worker's stream, and replies and events come
/// back on the asking replica's own up channel, envelopes and ids intact.
/// Needs a reachable Docker daemon.
#[sqlx::test(migrator = "base::db::MIGRATOR")]
async fn the_relay_crosses_replicas_over_redis(pool: sqlx::PgPool) -> TestResult {
    let redis = Redis::default().start().await?;
    let url = format!(
        "redis://{}:{}/",
        redis.get_host().await?,
        redis.get_host_port_ipv4(6379).await?
    );
    let h = harness_with(pool, Some(redis::Client::open(url.as_str())?)).await?;
    let key = h.register(&["remote_shell"]).await?;
    let fake = Fake::new();
    let _worker = connect(h.addr, &key, fake.clone()).await?;
    let token = h.admin_session().await?;

    let session = h.open(operator()).await?.session_id;
    h.exec(&session, "uname").await?;
    let mut watch = h.watch(operator(), &token, &session, 0).await?;
    assert_eq!(
        next_event(&mut watch).await.unwrap()?,
        event(
            0,
            api::shell_event::Event::Started(api::ShellCommandStarted {
                command: "uname".into(),
            }),
        )
    );
    assert_eq!(next_event(&mut watch).await.unwrap()?, output(1, "uname\n"));
    assert_eq!(
        next_event(&mut watch).await.unwrap()?,
        event(
            7,
            api::shell_event::Event::Finished(api::ShellCommandFinished { exit_code: 0 }),
        )
    );
    assert_eq!(h.list().await?.len(), 1);
    h.close(&session).await?;
    assert!(matches!(
        next_event(&mut watch).await.unwrap()?.event,
        Some(api::shell_event::Event::Closed(_))
    ));
    assert!(next_event(&mut watch).await.is_none());
    Ok(())
}
