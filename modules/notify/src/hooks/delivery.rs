//! The delivery hook: notices in, mail and Telegram messages out.
//!
//! One struct with both consumers on it, because both do the same thing with a
//! different envelope: map the event's destinations into targets and hand them
//! to [`DeliverNotice`]. Bound only in `--mode notifier`, which holds a
//! PostgreSQL advisory lock so exactly one instance is consuming
//! ([`crate::utils::lock`]).

use crate::events::{HealthNotifyGroupEvent, HealthNotifyPersonalEvent};
use crate::services::delivery::{DeliverNotice, DeliveryService, Target};
use kanau::processor::Processor;
use wakuwaku::integration::amqp::AmqpMessageProcessor;

/// Consumes [`HealthNotifyGroupEvent`] and [`HealthNotifyPersonalEvent`].
#[derive(Clone)]
pub struct NoticeDelivery {
    pub delivery: DeliveryService,
}

impl AmqpMessageProcessor<HealthNotifyGroupEvent> for NoticeDelivery {
    const QUEUE: &'static str = "guru_notify_health_notify_group";
}

impl Processor<HealthNotifyGroupEvent> for NoticeDelivery {
    type Output = ();
    type Error = wakuwaku::Error;
    #[tracing::instrument(name = "Hook:HealthNotifyGroupEvent", skip_all, err)]
    async fn process(&self, input: HealthNotifyGroupEvent) -> Result<Self::Output, Self::Error> {
        let targets = input
            .emails
            .into_iter()
            .map(Target::Email)
            .chain(input.telegram_chats.into_iter().map(Target::Telegram))
            .collect();
        self.delivery
            .process(DeliverNotice {
                notice: input.notice,
                language: input.language,
                targets,
            })
            .await?;
        Ok(())
    }
}

impl AmqpMessageProcessor<HealthNotifyPersonalEvent> for NoticeDelivery {
    const QUEUE: &'static str = "guru_notify_health_notify_personal";
}

impl Processor<HealthNotifyPersonalEvent> for NoticeDelivery {
    type Output = ();
    type Error = wakuwaku::Error;
    #[tracing::instrument(name = "Hook:HealthNotifyPersonalEvent", skip_all, err)]
    async fn process(&self, input: HealthNotifyPersonalEvent) -> Result<Self::Output, Self::Error> {
        let targets = input
            .email
            .map(Target::Email)
            .into_iter()
            .chain(input.telegram_chat.map(Target::Telegram))
            .collect();
        self.delivery
            .process(DeliverNotice {
                notice: input.notice,
                language: input.language,
                targets,
            })
            .await?;
        Ok(())
    }
}
