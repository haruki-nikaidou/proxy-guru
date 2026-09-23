//! From health facts to notices: who hears about what.
//!
//! This runs wherever the orchestration consumers run, so it may run twice for
//! the same fact in a narrow window (two `consumer` replicas taking two copies
//! of one health write's message). The transition filter
//! ([`crate::entities::db::state`]) makes that cost at most a duplicate
//! notification, never a missing one; a lock per subject is not worth it for
//! notifications.
//!
//! Nothing here fails a delivery over a lost publish: the state row has already
//! advanced, so a requeue would re-run the fan-out against a subject that now
//! looks unchanged and announce nothing. A publish failure is logged and the
//! remaining audiences are still served.

use crate::entities::db::setting::{
    CanvasSetting, FindCanvasLabel, FindCanvasSetting, FindPersonalRecipient,
    ListPersonalRecipients, NoticeKind, PersonalRecipient,
};
use crate::entities::db::state::{ObservePodStatuses, ObserveServerStatus};
use crate::events::{HealthNotice, HealthNotifyGroupEvent, HealthNotifyPersonalEvent};
use crate::services::NotifyError;
use auth::services::identity::Identity;
use base::db::Db;
use kanau::processor::Processor;
use orchestration::entities::db::canvas::CanvasId;
use orchestration::entities::db::health::{PodHealthStatus, ServerHealthStatus};
use orchestration::entities::db::pod::PodId;
use orchestration::entities::db::server::ServerId;
use orchestration::events::HealthFact;
use std::collections::HashMap;
use std::sync::Arc;
use time::{OffsetDateTime, PrimitiveDateTime};
use wakuwaku::integration::amqp::{AmqpMessageSend, AmqpPool};

/// Decides the audience of every health fact and publishes one notice per
/// audience found.
#[derive(Clone)]
pub struct FanoutService {
    pub db: Db,
    pub publisher: NoticePublisher,
}

/// Where notices go.
#[derive(Clone)]
pub enum NoticePublisher {
    /// Production: the `notify` exchange, consumed by the single notifier.
    Amqp(AmqpPool),
    /// Tests: collect what would have been published, skipping the broker.
    Collect(Arc<tokio::sync::Mutex<Vec<PublishedNotice>>>),
}

/// One published notice, as [`NoticePublisher::Collect`] records it.
#[derive(Debug, Clone)]
pub enum PublishedNotice {
    Group(HealthNotifyGroupEvent),
    Personal(HealthNotifyPersonalEvent),
}

impl NoticePublisher {
    /// A test sink that records everything published to it.
    pub fn collecting() -> Self {
        Self::Collect(Arc::new(tokio::sync::Mutex::new(Vec::new())))
    }

    async fn group(&self, event: HealthNotifyGroupEvent) {
        match self {
            Self::Amqp(pool) => {
                if let Err(error) = event.send(pool).await {
                    tracing::warn!(%error, "publishing a group notice failed; it is lost");
                }
            }
            Self::Collect(sink) => sink.lock().await.push(PublishedNotice::Group(event)),
        }
    }

    async fn personal(&self, event: HealthNotifyPersonalEvent) {
        match self {
            Self::Amqp(pool) => {
                if let Err(error) = event.send(pool).await {
                    tracing::warn!(%error, "publishing a personal notice failed; it is lost");
                }
            }
            Self::Collect(sink) => sink.lock().await.push(PublishedNotice::Personal(event)),
        }
    }
}

/// One batch of facts from one health write.
pub struct FanOutHealthFacts {
    pub facts: Vec<HealthFact>,
}

impl Processor<FanOutHealthFacts> for FanoutService {
    type Output = ();
    type Error = NotifyError;
    #[tracing::instrument(name = "Service:FanOutHealthFacts", skip_all, err)]
    async fn process(&self, input: FanOutHealthFacts) -> Result<Self::Output, Self::Error> {
        let mut notices = self.server_notices(&input.facts).await?;
        notices.extend(self.pod_notices(&input.facts).await?);
        if notices.is_empty() {
            return Ok(());
        }
        let mut settings: HashMap<String, Option<CanvasSetting>> = HashMap::new();
        let mut recipients: HashMap<(NoticeKind, String), Vec<PersonalRecipient>> = HashMap::new();
        for notice in notices {
            self.announce(notice, &mut settings, &mut recipients)
                .await?;
        }
        Ok(())
    }
}

impl FanoutService {
    /// The server facts that are a real change, in arrival order.
    ///
    /// A server appears at most once per batch in practice (one write settles
    /// one server); a repeat would ask the filter twice, and the second ask
    /// would find the status already recorded and drop it.
    async fn server_notices(&self, facts: &[HealthFact]) -> Result<Vec<HealthNotice>, NotifyError> {
        let mut notices = Vec::new();
        for fact in facts {
            let HealthFact::Server {
                server,
                server_name,
                canvas,
                canvas_name,
                status,
                at_unix_micros,
            } = fact
            else {
                continue;
            };
            let previous = self
                .db
                .process(ObserveServerStatus {
                    server: ServerId::from_key(server.clone()),
                    status: *status,
                    at: at(*at_unix_micros),
                })
                .await?;
            // `None` is a first sighting: recorded, announced to nobody.
            if previous.is_none_or(|previous| previous == *status) {
                continue;
            }
            notices.push(HealthNotice {
                kind: server_kind(*status),
                subject: server.clone(),
                subject_name: server_name.clone(),
                canvas: canvas.clone(),
                canvas_name: canvas_name.clone(),
                message: String::new(),
                at_unix_micros: *at_unix_micros,
            });
        }
        Ok(notices)
    }

    /// The pod facts that are a real change.
    ///
    /// A report writes one row per pod, so the batch is deduped before it is
    /// asked about — `ON CONFLICT DO UPDATE` errors on a statement that hits one
    /// row twice — keeping the last fact per pod, which is the newest.
    async fn pod_notices(&self, facts: &[HealthFact]) -> Result<Vec<HealthNotice>, NotifyError> {
        let mut latest: Vec<&HealthFact> = Vec::new();
        for fact in facts {
            let HealthFact::Pod { pod, .. } = fact else {
                continue;
            };
            match latest.iter().position(|kept| pod_id(kept) == Some(pod)) {
                Some(index) => latest[index] = fact,
                None => latest.push(fact),
            }
        }
        if latest.is_empty() {
            return Ok(Vec::new());
        }
        let mut pods = Vec::with_capacity(latest.len());
        let mut statuses = Vec::with_capacity(latest.len());
        let mut newest = PrimitiveDateTime::MIN.assume_utc();
        for fact in &latest {
            let HealthFact::Pod {
                pod,
                status,
                at_unix_micros,
                ..
            } = fact
            else {
                continue;
            };
            pods.push(PodId::from_key(pod.clone()));
            statuses.push(*status);
            newest = newest.max(at(*at_unix_micros));
        }
        let observed: HashMap<String, Option<PodHealthStatus>> = self
            .db
            .process(ObservePodStatuses {
                pods,
                statuses,
                at: newest,
            })
            .await?
            .into_iter()
            .map(|observation| (observation.pod.into_string(), observation.previous))
            .collect();
        let mut notices = Vec::new();
        for fact in latest {
            let HealthFact::Pod {
                pod,
                pod_name,
                canvas,
                canvas_name,
                status,
                message,
                at_unix_micros,
            } = fact
            else {
                continue;
            };
            let previous = observed.get(pod).copied().flatten();
            if previous.is_none_or(|previous| previous == *status) {
                continue;
            }
            notices.push(HealthNotice {
                kind: pod_kind(*status),
                subject: pod.clone(),
                subject_name: pod_name.clone(),
                canvas: canvas.clone(),
                canvas_name: canvas_name.clone(),
                message: message.clone(),
                at_unix_micros: *at_unix_micros,
            });
        }
        Ok(notices)
    }

    /// Publishes one notice to the workspace's destinations and to every
    /// account that asked for it. The two caches make a batch touching one
    /// canvas read each setting once.
    async fn announce(
        &self,
        notice: HealthNotice,
        settings: &mut HashMap<String, Option<CanvasSetting>>,
        recipients: &mut HashMap<(NoticeKind, String), Vec<PersonalRecipient>>,
    ) -> Result<(), NotifyError> {
        let canvas = CanvasId::from_key(notice.canvas.clone());
        let setting = match settings.get(&notice.canvas) {
            Some(setting) => setting.clone(),
            None => {
                let setting = self
                    .db
                    .process(FindCanvasSetting {
                        canvas: canvas.clone(),
                    })
                    .await?;
                settings.insert(notice.canvas.clone(), setting.clone());
                setting
            }
        };
        if let Some(setting) = setting.filter(|setting| setting.wants(notice.kind)) {
            self.publisher
                .group(HealthNotifyGroupEvent {
                    notice: notice.clone(),
                    language: setting.language,
                    emails: setting.emails,
                    telegram_chats: setting.telegram_chats,
                })
                .await;
        }
        let key = (notice.kind, notice.canvas.clone());
        let accounts = match recipients.get(&key) {
            Some(accounts) => accounts.clone(),
            None => {
                let accounts = self
                    .db
                    .process(ListPersonalRecipients {
                        kind: notice.kind,
                        canvas,
                    })
                    .await?;
                recipients.insert(key, accounts.clone());
                accounts
            }
        };
        for account in accounts {
            self.publisher
                .personal(HealthNotifyPersonalEvent {
                    notice: notice.clone(),
                    language: account.language,
                    account: account.account.into_string(),
                    email: account.email,
                    telegram_chat: account.telegram_chat,
                })
                .await;
        }
        Ok(())
    }
}

/// One notice to the caller's own channels, so an operator can prove the path
/// works without waiting for a server to fall over.
///
/// It ignores the caller's event subscriptions — the point is the channel, not
/// the subscription — and goes out as a personal notice like any other, so it
/// travels the whole publish → notifier → send path.
pub struct SendTestNotice {
    pub actor: Identity,
    pub canvas: CanvasId,
}

impl Processor<SendTestNotice> for FanoutService {
    type Output = ();
    type Error = NotifyError;
    #[tracing::instrument(name = "Service:SendTestNotice", skip_all, err)]
    async fn process(&self, input: SendTestNotice) -> Result<Self::Output, Self::Error> {
        input.actor.require_human()?;
        let recipient = self
            .db
            .process(FindPersonalRecipient {
                account: input.actor.account_id.clone(),
                canvas: input.canvas.clone(),
            })
            .await?
            // Not `NotFound`: the caller exists, its channels do not — and
            // `FAILED_PRECONDITION` is what carries that sentence to the
            // dashboard verbatim, where `NOT_FOUND` would read as a dead page.
            .ok_or(NotifyError::Disabled)?;
        let canvas_name = self
            .db
            .process(FindCanvasLabel {
                canvas: input.canvas.clone(),
            })
            .await?
            .ok_or(NotifyError::NotFound)?;
        self.publisher
            .personal(HealthNotifyPersonalEvent {
                notice: HealthNotice {
                    kind: NoticeKind::ServerOffline,
                    subject: "test".to_string(),
                    subject_name: "test".to_string(),
                    canvas: input.canvas.into_string(),
                    canvas_name,
                    message: String::new(),
                    at_unix_micros: (OffsetDateTime::now_utc().unix_timestamp_nanos() / 1_000)
                        as i64,
                },
                language: recipient.language,
                account: recipient.account.into_string(),
                email: recipient.email,
                telegram_chat: recipient.telegram_chat,
            })
            .await;
        Ok(())
    }
}

/// The pod a fact is about, for the dedupe pass.
fn pod_id(fact: &HealthFact) -> Option<&String> {
    match fact {
        HealthFact::Pod { pod, .. } => Some(pod),
        HealthFact::Server { .. } => None,
    }
}

fn server_kind(status: ServerHealthStatus) -> NoticeKind {
    match status {
        ServerHealthStatus::Online => NoticeKind::ServerOnline,
        ServerHealthStatus::Degraded => NoticeKind::ServerDegraded,
        ServerHealthStatus::Offline => NoticeKind::ServerOffline,
    }
}

fn pod_kind(status: PodHealthStatus) -> NoticeKind {
    match status {
        PodHealthStatus::Ready => NoticeKind::PodReady,
        PodHealthStatus::Deploying => NoticeKind::PodDeploying,
        PodHealthStatus::Failed => NoticeKind::PodFailed,
    }
}

/// A fact's timestamp. One too large for a calendar is a corrupt event, not a
/// reason to drop the notice: the row's `changed_at` is bookkeeping, and `now`
/// is the closest honest answer.
fn at(unix_micros: i64) -> OffsetDateTime {
    OffsetDateTime::from_unix_timestamp_nanos(i128::from(unix_micros).saturating_mul(1_000))
        .unwrap_or_else(|_| OffsetDateTime::now_utc())
}
