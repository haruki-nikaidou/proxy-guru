//! The mail channel: one SMTP relay, one `From`, plain-text messages.
//!
//! The transport is built once and pooled by `lettre`, so a burst of notices
//! shares one connection. The relay is upgraded with STARTTLS by default
//! (`smtp_starttls`, port 587); turning it off speaks plain SMTP, which is for a
//! relay on localhost or a test sink and nothing else.

use crate::config::NotifyConfig;
use crate::services::NotifyError;
use lettre::message::Mailbox;
use lettre::message::header::ContentType;
use lettre::transport::smtp::authentication::Credentials;
use lettre::{AsyncSmtpTransport, AsyncTransport, Message, Tokio1Executor};
use std::time::Duration;

/// A configured mail channel. Cheap to clone; the transport is shared.
#[derive(Clone)]
pub struct EmailSender {
    transport: AsyncSmtpTransport<Tokio1Executor>,
    from: Mailbox,
    attempts: u32,
    retry_delay: Duration,
}

impl EmailSender {
    /// Builds the channel, or `None` when no relay is configured.
    ///
    /// An empty `smtp_username`, or an unset `GURU_SMTP_PASSWORD`, sends
    /// unauthenticated — which is what a relay on localhost expects.
    pub fn from_config(
        config: &NotifyConfig,
        password: Option<&str>,
    ) -> Result<Option<Self>, NotifyError> {
        if !config.email_configured() {
            return Ok(None);
        }
        let host = config.smtp_host.trim();
        let mut builder = if config.smtp_starttls {
            AsyncSmtpTransport::<Tokio1Executor>::starttls_relay(host)
                .map_err(|error| NotifyError::Email(format!("connecting to {host}: {error}")))?
        } else {
            AsyncSmtpTransport::<Tokio1Executor>::builder_dangerous(host)
        }
        .port(config.smtp_port);
        let username = config.smtp_username.trim();
        if let (false, Some(password)) = (username.is_empty(), password) {
            builder =
                builder.credentials(Credentials::new(username.to_string(), password.to_string()));
        }
        let from: Mailbox = config.smtp_from.trim().parse().map_err(|error| {
            NotifyError::Email(format!(
                "smtp_from ({}) is not an address: {error}",
                config.smtp_from
            ))
        })?;
        Ok(Some(Self {
            transport: builder.build(),
            from,
            // Zero attempts would mean "configured, never sends".
            attempts: config.delivery_attempts.max(1),
            retry_delay: config.delivery_retry_delay(),
        }))
    }

    /// Sends one message, retrying up to the configured number of attempts.
    pub async fn send(&self, to: &str, subject: &str, body: &str) -> Result<(), NotifyError> {
        let mailbox: Mailbox = to
            .trim()
            .parse()
            .map_err(|error| NotifyError::Email(format!("{to} is not an address: {error}")))?;
        let message = Message::builder()
            .from(self.from.clone())
            .to(mailbox)
            .subject(subject)
            // Without this header the body defaults to us-ascii, and every
            // Japanese or Chinese notice arrives as mojibake — the subject
            // survives on its own because it is an RFC 2047 encoded word.
            .header(ContentType::TEXT_PLAIN)
            .body(body.to_string())
            .map_err(|error| NotifyError::Email(format!("building the message: {error}")))?;
        let mut last = String::from("no attempt was made");
        for attempt in 1..=self.attempts {
            match self.transport.send(message.clone()).await {
                Ok(_) => return Ok(()),
                Err(error) => {
                    tracing::warn!(attempt, %to, %error, "sending a notification mail failed");
                    last = error.to_string();
                    if attempt < self.attempts {
                        tokio::time::sleep(self.retry_delay).await;
                    }
                }
            }
        }
        Err(NotifyError::Email(last))
    }
}
