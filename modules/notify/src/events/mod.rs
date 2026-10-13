//! Event definitions: the two notices the fan-out publishes, on Iggy.
//!
//! The fan-out (`consumer` mode, any number of replicas) decides *who* hears
//! about a health change; delivery (`notifier` mode, exactly one instance)
//! decides *how*. These two messages are the seam, and they carry everything a
//! delivery needs — the phrasing inputs, the language, and the destinations —
//! so the notifier opens no database at all. Both share one topic,
//! `notify_health_notice`, told apart by their type tag.

use crate::entities::db::setting::{Language, NoticeKind};
use kanau::{RkyvMessageDe, RkyvMessageSer};
use wakuwaku::iggy::{
    Event, EventAtomicOrdering, EventBody, EventTypeTag, IggySuperPartitionName, NonAssociative,
    PartitionKey,
};

/// The main topics this module publishes to, created at startup.
pub const TOPICS: [&str; 1] = [NoticeCanvasKey::SUPER_PARTITION_NAME.0];

/// Partition key of both notices: the canvas the notice is about.
#[derive(Hash)]
pub struct NoticeCanvasKey(pub String);

impl PartitionKey for NoticeCanvasKey {
    type PartitionMerge = std::hash::DefaultHasher;
    const SUPER_PARTITION_NAME: IggySuperPartitionName =
        IggySuperPartitionName("notify_health_notice");
}

/// What happened, in the words a message is rendered from.
///
/// `subject` is the server's or pod's id and `subject_name` its name;
/// `message` is a pod's failure detail and empty for everything else.
#[derive(Debug, Clone, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
pub struct HealthNotice {
    pub kind: NoticeKind,
    pub subject: String,
    pub subject_name: String,
    pub canvas: String,
    pub canvas_name: String,
    pub message: String,
    pub at_unix_micros: i64,
}

/// **Public event**
///
/// One notice for a workspace's shared destinations.
///
/// Published by: [`crate::services::fanout::FanoutService`].
/// Consumed by: [`crate::hooks::delivery::NoticeDelivery`] (`--mode notifier`).
/// Topic: `notify_health_notice` (keyed by canvas), tag 1.
///
/// The destinations travel with the notice: they were read together with the
/// setting that selected them, so a delivery neither re-reads nor re-authorizes.
#[derive(
    Debug, Clone, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, RkyvMessageSer, RkyvMessageDe,
)]
pub struct HealthNotifyGroupEvent {
    pub notice: HealthNotice,
    pub language: Language,
    pub emails: Vec<String>,
    pub telegram_chats: Vec<String>,
}

impl EventBody for HealthNotifyGroupEvent {}

impl Event for HealthNotifyGroupEvent {
    const TYPE_TAG: EventTypeTag = EventTypeTag::new(1);
    type Key = NoticeCanvasKey;
    type Algebra = NonAssociative;

    fn key(&self) -> NoticeCanvasKey {
        NoticeCanvasKey(self.notice.canvas.clone())
    }

    fn atomic_ordering(&self) -> EventAtomicOrdering {
        EventAtomicOrdering::Relaxed
    }
}

/// **Public event**
///
/// One notice for one account's own channels.
///
/// Published by: [`crate::services::fanout::FanoutService`] (a health change,
/// or an operator's test notification).
/// Consumed by: [`crate::hooks::delivery::NoticeDelivery`] (`--mode notifier`).
/// Topic: `notify_health_notice` (keyed by canvas), tag 2.
///
/// `email` is the account's address and is set only when its setting turns mail
/// on; `telegram_chat` is its chat id when it named one. `account` is carried
/// for the log line, not for a lookup.
#[derive(
    Debug, Clone, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, RkyvMessageSer, RkyvMessageDe,
)]
pub struct HealthNotifyPersonalEvent {
    pub notice: HealthNotice,
    pub language: Language,
    pub account: String,
    pub email: Option<String>,
    pub telegram_chat: Option<String>,
}

impl EventBody for HealthNotifyPersonalEvent {}

impl Event for HealthNotifyPersonalEvent {
    const TYPE_TAG: EventTypeTag = EventTypeTag::new(2);
    type Key = NoticeCanvasKey;
    type Algebra = NonAssociative;

    fn key(&self) -> NoticeCanvasKey {
        NoticeCanvasKey(self.notice.canvas.clone())
    }

    fn atomic_ordering(&self) -> EventAtomicOrdering {
        EventAtomicOrdering::Relaxed
    }
}
