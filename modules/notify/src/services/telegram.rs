//! The Telegram channel: `sendMessage` against the Bot API.
//!
//! The bot token comes from the environment and never appears in a log line —
//! it is part of the URL path, so no request URL is logged either; the chat id
//! is what identifies a failure.

use crate::config::NotifyConfig;
use crate::services::NotifyError;
use std::time::Duration;

/// How much of an error body is kept in the error message.
const BODY_EXCERPT: usize = 300;

/// A configured Telegram channel. Cheap to clone; the client is shared.
#[derive(Clone)]
pub struct TelegramSender {
    http: reqwest::Client,
    api_base: String,
    token: String,
    attempts: u32,
    retry_delay: Duration,
}

impl TelegramSender {
    /// Builds the channel, or `None` when no bot token is configured.
    pub fn from_config(config: &NotifyConfig, token: Option<&str>) -> Option<Self> {
        let token = token?.to_string();
        Some(Self {
            http: reqwest::Client::new(),
            api_base: config
                .telegram_api_base
                .trim()
                .trim_end_matches('/')
                .to_string(),
            token,
            attempts: config.delivery_attempts.max(1),
            retry_delay: config.delivery_retry_delay(),
        })
    }

    /// Sends one message, retrying up to the configured number of attempts.
    pub async fn send(&self, chat: &str, text: &str) -> Result<(), NotifyError> {
        let url = format!("{}/bot{}/sendMessage", self.api_base, self.token);
        let body = serde_json::json!({ "chat_id": chat, "text": text });
        let mut last = String::from("no attempt was made");
        for attempt in 1..=self.attempts {
            match self.post(&url, &body).await {
                Ok(()) => return Ok(()),
                Err(error) => {
                    tracing::warn!(attempt, %chat, %error, "sending a telegram notification failed");
                    last = error;
                    if attempt < self.attempts {
                        tokio::time::sleep(self.retry_delay).await;
                    }
                }
            }
        }
        Err(NotifyError::Telegram(last))
    }

    /// One attempt. The error is a string, not a [`NotifyError`]: only the last
    /// attempt's failure becomes one.
    async fn post(&self, url: &str, body: &serde_json::Value) -> Result<(), String> {
        let response = self
            .http
            .post(url)
            .json(body)
            .send()
            .await
            .map_err(|error| error.to_string())?;
        let status = response.status();
        if status.is_success() {
            return Ok(());
        }
        // The API says why in the body ("chat not found", "bot was blocked"),
        // which is what an operator needs to see.
        let body = response.text().await.unwrap_or_default();
        let excerpt: String = body.chars().take(BODY_EXCERPT).collect();
        Err(format!("the API answered {status}: {excerpt}"))
    }
}
