//! The delivery hook: notices in, mail and Telegram messages out.
//!
//! One struct with both handlers on it, because both do the same thing with a
//! different envelope: map the event's destinations into targets and hand them
//! to [`DeliverNotice`]. Bound only in `--mode notifier`, which holds a
//! PostgreSQL advisory lock so exactly one instance is consuming
//! ([`crate::utils::lock`]).

use crate::events::{HealthNotifyGroupEvent, HealthNotifyPersonalEvent};
use crate::services::NotifyError;
use crate::services::delivery::{DeliverNotice, DeliveryService, Target};
use kanau::processor::Processor;
use std::num::NonZeroUsize;
use wakuwaku::ServiceCreation;
use wakuwaku::iggy::EventHandler;
use wakuwaku::iggy::error::HandleError;

/// Consumes [`HealthNotifyGroupEvent`] and [`HealthNotifyPersonalEvent`].
pub struct NoticeDelivery {
    pub delivery: DeliveryService,
}

impl ServiceCreation for NoticeDelivery {
    type Dep = DeliveryService;
    fn create_service(delivery: DeliveryService) -> Self {
        Self { delivery }
    }
}

impl EventHandler<HealthNotifyGroupEvent> for NoticeDelivery {
    const MAX_BATCH: NonZeroUsize = NonZeroUsize::MIN;

    #[tracing::instrument(name = "Hook:HealthNotifyGroupEvent", skip_all, err)]
    async fn handle(&self, run: &[HealthNotifyGroupEvent]) -> Result<(), HandleError> {
        for event in run {
            let targets = event
                .emails
                .iter()
                .cloned()
                .map(Target::Email)
                .chain(event.telegram_chats.iter().cloned().map(Target::Telegram))
                .collect();
            self.delivery
                .process(DeliverNotice {
                    notice: event.notice.clone(),
                    language: event.language,
                    targets,
                })
                .await
                .map_err(NotifyError::into_handle_error)?;
        }
        Ok(())
    }
}

impl EventHandler<HealthNotifyPersonalEvent> for NoticeDelivery {
    const MAX_BATCH: NonZeroUsize = NonZeroUsize::MIN;

    #[tracing::instrument(name = "Hook:HealthNotifyPersonalEvent", skip_all, err)]
    async fn handle(&self, run: &[HealthNotifyPersonalEvent]) -> Result<(), HandleError> {
        for event in run {
            let targets = event
                .email
                .clone()
                .map(Target::Email)
                .into_iter()
                .chain(event.telegram_chat.clone().map(Target::Telegram))
                .collect();
            self.delivery
                .process(DeliverNotice {
                    notice: event.notice.clone(),
                    language: event.language,
                    targets,
                })
                .await
                .map_err(NotifyError::into_handle_error)?;
        }
        Ok(())
    }
}
