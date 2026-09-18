//! The channel secrets, read from the environment.
//!
//! Neither belongs in [`crate::config::NotifyConfig`]: that document is a
//! database row an Admin reads and replaces from the dashboard, and a bot token
//! is not a setting. An unset value disables its channel rather than failing
//! startup — a fleet with no Telegram bot is a normal deployment — so nothing
//! here returns an error; the `notifier` mode logs what it ended up with.

pub const SMTP_PASSWORD_ENV: &str = "GURU_SMTP_PASSWORD";
pub const TELEGRAM_BOT_TOKEN_ENV: &str = "GURU_TELEGRAM_BOT_TOKEN";

/// The secrets the delivery side runs with. `None` disables the channel.
#[derive(Clone, Default)]
pub struct NotifySecrets {
    pub smtp_password: Option<String>,
    pub telegram_bot_token: Option<String>,
}

/// Never prints either value.
impl std::fmt::Debug for NotifySecrets {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NotifySecrets")
            .field("smtp_password", &self.smtp_password.is_some())
            .field("telegram_bot_token", &self.telegram_bot_token.is_some())
            .finish()
    }
}

impl NotifySecrets {
    /// Reads both variables; an empty value counts as unset, so an
    /// exported-but-empty one disables its channel instead of authenticating
    /// with an empty password.
    pub fn from_env() -> Self {
        Self {
            smtp_password: read(SMTP_PASSWORD_ENV),
            telegram_bot_token: read(TELEGRAM_BOT_TOKEN_ENV),
        }
    }
}

/// An empty value is unset; anything else is taken verbatim. Not trimmed: a
/// password may legitimately begin or end with a space, and silently changing
/// one would authenticate with something the operator never set.
fn read(key: &str) -> Option<String> {
    std::env::var(key).ok().filter(|value| !value.is_empty())
}
