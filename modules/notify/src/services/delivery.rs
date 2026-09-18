//! Sending one notice to its destinations.
//!
//! **Delivery is best-effort.** The transition filter has already advanced the
//! subject's state row by the time a notice is published, so a notice lost to an
//! SMTP or Telegram outage is not retried past `delivery_attempts`: requeueing
//! the delivery would re-run the fan-out against a subject that now looks
//! unchanged, and the next notice about that subject would be the next real
//! change. A failed send is therefore logged and the remaining targets are
//! still attempted; only a database error propagates (there is none on this
//! path). Notifications are a convenience, not the control plane's record.

use crate::config::NotifyConfig;
use crate::entities::db::setting::Language;
use crate::events::HealthNotice;
use crate::services::NotifyError;
use crate::services::email::EmailSender;
use crate::services::telegram::TelegramSender;
use crate::utils::message::render;
use crate::utils::secret::NotifySecrets;
use kanau::processor::Processor;

/// The delivery side, as the single `notifier` instance runs it. A channel that
/// is not configured is `None`, and every target on it is logged and skipped.
#[derive(Clone)]
pub struct DeliveryService {
    pub email: Option<EmailSender>,
    pub telegram: Option<TelegramSender>,
}

impl DeliveryService {
    /// Builds both channels from the stored config and the environment's
    /// secrets. Fails only on a configuration that cannot produce a transport
    /// at all (an unparseable `smtp_from`, say) — an *absent* channel is not a
    /// failure.
    pub fn new(config: &NotifyConfig, secrets: &NotifySecrets) -> Result<Self, NotifyError> {
        Ok(Self {
            email: EmailSender::from_config(config, secrets.smtp_password.as_deref())?,
            telegram: TelegramSender::from_config(config, secrets.telegram_bot_token.as_deref()),
        })
    }

    /// What the mode logs once at startup, so an operator can see which
    /// channels this notifier can actually use.
    pub fn channels(&self) -> [&'static str; 2] {
        [
            if self.email.is_some() {
                "email: configured"
            } else {
                "email: disabled (no smtp_host, or it is empty)"
            },
            if self.telegram.is_some() {
                "telegram: configured"
            } else {
                "telegram: disabled (no GURU_TELEGRAM_BOT_TOKEN)"
            },
        ]
    }
}

/// One destination of a notice.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Target {
    Email(String),
    Telegram(String),
}

/// One notice, rendered once and sent to every target it names.
pub struct DeliverNotice {
    pub notice: HealthNotice,
    pub language: Language,
    pub targets: Vec<Target>,
}

impl Processor<DeliverNotice> for DeliveryService {
    type Output = ();
    type Error = NotifyError;
    #[tracing::instrument(name = "Service:DeliverNotice", skip_all, err)]
    async fn process(&self, input: DeliverNotice) -> Result<Self::Output, Self::Error> {
        if input.targets.is_empty() {
            return Ok(());
        }
        let rendered = render(&input.notice, input.language);
        for target in &input.targets {
            let sent = match target {
                Target::Email(address) => match &self.email {
                    Some(email) => email.send(address, &rendered.subject, &rendered.body).await,
                    None => Err(NotifyError::Disabled),
                },
                Target::Telegram(chat) => match &self.telegram {
                    Some(telegram) => telegram.send(chat, &rendered.body).await,
                    None => Err(NotifyError::Disabled),
                },
            };
            match sent {
                Ok(()) => tracing::info!(?target, kind = %input.notice.kind, "notice delivered"),
                Err(NotifyError::Disabled) => tracing::warn!(
                    ?target,
                    "the channel this notice names is not configured; skipped"
                ),
                Err(error) => tracing::error!(?target, %error, "delivering a notice failed"),
            }
        }
        Ok(())
    }
}
