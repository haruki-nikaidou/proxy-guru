//! AMQP event definitions: the two notices the fan-out publishes.
//!
//! The fan-out (`consumer` mode, any number of replicas) decides *who* hears
//! about a health change; delivery (`notifier` mode, exactly one instance)
//! decides *how*. These two messages are the seam, and they carry everything a
//! delivery needs — the phrasing inputs, the language, and the destinations —
//! so the notifier opens no database at all.

use crate::entities::db::setting::{Language, NoticeKind};
use kanau::{RkyvMessageDe, RkyvMessageSer};
use wakuwaku::integration::amqp::{AmqpExchangeType, AmqpMessageSend, AmqpRouting};

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
/// Consumed by: [`crate::hooks::delivery::NoticeDelivery`].
/// Route: exchange `notify` (direct), key `health_notify_group`.
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

impl AmqpRouting for HealthNotifyGroupEvent {
    const EXCHANGE: &'static str = "notify";
    const EXCHANGE_TYPE: AmqpExchangeType = AmqpExchangeType::Direct;
    const ROUTING_KEY: &'static str = "health_notify_group";
}

impl AmqpMessageSend for HealthNotifyGroupEvent {}

/// **Public event**
///
/// One notice for one account's own channels.
///
/// Published by: [`crate::services::fanout::FanoutService`] (a health change,
/// or an operator's test notification).
/// Consumed by: [`crate::hooks::delivery::NoticeDelivery`].
/// Route: exchange `notify` (direct), key `health_notify_personal`.
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

impl AmqpRouting for HealthNotifyPersonalEvent {
    const EXCHANGE: &'static str = "notify";
    const EXCHANGE_TYPE: AmqpExchangeType = AmqpExchangeType::Direct;
    const ROUTING_KEY: &'static str = "health_notify_personal";
}

impl AmqpMessageSend for HealthNotifyPersonalEvent {}
