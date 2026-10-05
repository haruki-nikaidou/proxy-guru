//! The worker's end of one `ShellChannel` stream: answers the master's requests
//! from the [`ShellTable`], and pumps every watch the master attached from its
//! own cursor into the stream.
//!
//! Watches belong to the stream: when it ends they are dropped (the dashboard
//! attaches again over the next one), while the sessions they watched live on.
//! Nothing here waits on the shells, and the shells never wait on the stream: a
//! slow master only delays the pumps, whose cursors fall behind in the ring.

use super::ShellTable;
use super::ring::{Entry, Record, Stream};
use super::session::Viewer;
use crate::BoxError;
use crate::agent::{AbortOnDrop, WATCHDOG_BEATS};
use rpguru_sdk::orchestration_agent::worker_agent_client::WorkerAgentClient;
use rpguru_sdk::orchestration_agent::{
    ShellCommandFinished, ShellCommandStarted, ShellDown, ShellError, ShellErrorCode, ShellEvent,
    ShellKeepAlive, ShellOutput, ShellSessionClosed, ShellStream, ShellTruncated, ShellUp,
    ShellWatchEvent, shell_down, shell_event, shell_up,
};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{Notify, mpsc};
use tokio::time::Instant;
use tonic::metadata::{Ascii, MetadataValue};
use tonic::transport::Channel;

/// Messages queued towards the master before a pump has to wait.
const OUTBOX: usize = 64;
/// A watch not renewed for this long lost its relay without a `ShellDetach`.
pub const WATCH_LEASE: Duration = Duration::from_secs(30);
/// How often unrenewed and finished watches are swept.
const SWEEP: Duration = Duration::from_secs(1);
/// How much output one read of the transcript copies at most.
const BATCH_BYTES: usize = 64 * 1024;

struct Watch {
    task: AbortOnDrop<()>,
    renew: Arc<Notify>,
    renewed: Instant,
}

/// Opens `ShellChannel` and serves it until it ends: the master closed it, it
/// failed, or it carried nothing at all for three `keepalive` periods. The
/// stream's response headers may be held back by a proxy until data flows, so
/// keep-alives go out before they arrive and nothing waits on them.
pub async fn run(
    mut client: WorkerAgentClient<Channel>,
    refresh_key: MetadataValue<Ascii>,
    keepalive: Duration,
    table: &ShellTable,
) -> Result<(), BoxError> {
    let (tx, rx) = mpsc::channel::<ShellUp>(OUTBOX);
    let mut request = tonic::Request::new(tokio_stream::wrappers::ReceiverStream::new(rx));
    request.metadata_mut().insert("x-refresh-key", refresh_key);
    let call = client.shell_channel(request);
    tokio::pin!(call);
    let mut downs: Option<tonic::Streaming<ShellDown>> = None;
    let silence = keepalive.saturating_mul(WATCHDOG_BEATS);
    let mut heard = Instant::now();
    let mut beat = tokio::time::interval(keepalive);
    beat.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let mut sweep = tokio::time::interval(SWEEP);
    sweep.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let mut watches: HashMap<String, Watch> = HashMap::new();
    loop {
        tokio::select! {
            opened = &mut call, if downs.is_none() => {
                downs = Some(opened?.into_inner());
            }
            down = next_down(&mut downs) => {
                let Some(down) = down? else {
                    return Ok(());
                };
                heard = Instant::now();
                handle(down, table, &tx, &mut watches);
            }
            () = tokio::time::sleep(silence.saturating_sub(heard.elapsed())) => {
                return Err(format!("shell channel silent for {}s", silence.as_secs()).into());
            }
            _ = beat.tick() => {
                // A full outbox is data flowing already; this beat is not missed.
                let _ = tx.try_send(up(shell_up::Message::KeepAlive(ShellKeepAlive {})));
            }
            _ = sweep.tick() => {
                watches.retain(|_, watch| {
                    !watch.task.0.is_finished() && watch.renewed.elapsed() < WATCH_LEASE
                });
            }
        }
    }
}

/// The channel's next message; pending until the master's response opens.
async fn next_down(
    downs: &mut Option<tonic::Streaming<ShellDown>>,
) -> Result<Option<ShellDown>, tonic::Status> {
    match downs {
        Some(downs) => downs.message().await,
        None => std::future::pending().await,
    }
}

fn handle(
    down: ShellDown,
    table: &ShellTable,
    tx: &mpsc::Sender<ShellUp>,
    watches: &mut HashMap<String, Watch>,
) {
    match down.message {
        Some(shell_down::Message::Request(request)) => {
            // Its own task: writing a command may wait on the shell, which must not
            // hold up the rest of the channel.
            let table = table.clone();
            let tx = tx.clone();
            tokio::spawn(async move {
                let reply = table.request(request).await;
                let _ = tx.send(up(shell_up::Message::Reply(reply))).await;
            });
        }
        Some(shell_down::Message::Attach(attach)) => {
            // A restart: the previous pump of this id stops before the new one starts.
            watches.remove(&attach.watch_id);
            let Some(viewer) = table.viewer(&attach.session_id) else {
                let event = ShellEvent {
                    offset: 0,
                    event: Some(shell_event::Event::Error(ShellError {
                        code: ShellErrorCode::NotFound.into(),
                        message: "no such shell session".to_owned(),
                    })),
                };
                let tx = tx.clone();
                let message = watch_event(attach.watch_id, event);
                tokio::spawn(async move {
                    let _ = tx.send(message).await;
                });
                return;
            };
            let renew = Arc::new(Notify::new());
            let task = AbortOnDrop(tokio::spawn(pump(
                attach.watch_id.clone(),
                viewer,
                attach.from_offset,
                renew.clone(),
                tx.clone(),
            )));
            watches.insert(
                attach.watch_id,
                Watch {
                    task,
                    renew,
                    renewed: Instant::now(),
                },
            );
        }
        Some(shell_down::Message::Detach(detach)) => {
            watches.remove(&detach.watch_id);
        }
        Some(shell_down::Message::Renew(renew)) => {
            match watches
                .get_mut(&renew.watch_id)
                .filter(|watch| !watch.task.0.is_finished())
            {
                Some(watch) => {
                    watch.renewed = Instant::now();
                    watch.renew.notify_one();
                }
                // Gone, or ended with its session: nothing to answer for.
                None => {
                    watches.remove(&renew.watch_id);
                }
            }
        }
        Some(shell_down::Message::KeepAlive(_)) | None => {}
    }
}

/// Sends the transcript from `from` on (from the end when `from` is past it),
/// then everything appended after, until the session ends — reported with
/// `closed` once the transcript is through — or the channel goes. A renewal is
/// answered with a `keep_alive` at the position reached.
async fn pump(
    watch_id: String,
    viewer: Viewer,
    from: u64,
    renew: Arc<Notify>,
    tx: mpsc::Sender<ShellUp>,
) {
    let session = viewer.session();
    let mut changes = session.subscribe();
    let mut cursor = from.min(session.end_offset());
    loop {
        changes.mark_unchanged();
        let batch = session.read(cursor, BATCH_BYTES);
        for (offset, entry) in batch.entries {
            let event = transcript_event(offset, entry);
            if tx.send(watch_event(watch_id.clone(), event)).await.is_err() {
                return;
            }
        }
        cursor = batch.next;
        if let Some(reason) = batch.closed {
            let event = ShellEvent {
                offset: cursor,
                event: Some(shell_event::Event::Closed(ShellSessionClosed {
                    reason: reason.into(),
                })),
            };
            let _ = tx.send(watch_event(watch_id, event)).await;
            return;
        }
        if batch.more {
            continue;
        }
        tokio::select! {
            changed = changes.changed() => {
                if changed.is_err() {
                    return;
                }
            }
            () = renew.notified() => {
                let event = ShellEvent {
                    offset: cursor,
                    event: Some(shell_event::Event::KeepAlive(ShellKeepAlive {})),
                };
                if tx.send(watch_event(watch_id.clone(), event)).await.is_err() {
                    return;
                }
            }
        }
    }
}

fn transcript_event(offset: u64, entry: Entry) -> ShellEvent {
    let event = match entry {
        Entry::Truncated { dropped } => shell_event::Event::Truncated(ShellTruncated { dropped }),
        Entry::Record(Record::Output { stream, data }) => {
            let stream = match stream {
                Stream::Stdout => ShellStream::Stdout,
                Stream::Stderr => ShellStream::Stderr,
            };
            shell_event::Event::Output(ShellOutput {
                stream: stream.into(),
                data,
            })
        }
        Entry::Record(Record::Started { command }) => {
            shell_event::Event::Started(ShellCommandStarted { command })
        }
        Entry::Record(Record::Finished { exit_code }) => {
            shell_event::Event::Finished(ShellCommandFinished { exit_code })
        }
    };
    ShellEvent {
        offset,
        event: Some(event),
    }
}

fn watch_event(watch_id: String, event: ShellEvent) -> ShellUp {
    up(shell_up::Message::Event(ShellWatchEvent {
        watch_id,
        event: Some(event),
    }))
}

fn up(message: shell_up::Message) -> ShellUp {
    ShellUp {
        message: Some(message),
    }
}
