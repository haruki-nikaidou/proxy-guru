//! Reading and replacing the settings rows.
//!
//! Two scopes with two different authorizations: the workspace row is part of
//! the canvas, so it takes `ViewWorkspace` to read and `EditWorkspace` to
//! replace; an account's own row is self-service, so it takes a human session
//! and nothing else — the account is always the caller's own, taken from the
//! identity and never from the request.
//!
//! A read of a row that does not exist answers the defaults (the configured
//! language, no events) rather than `NOT_FOUND`: "nothing is set up yet" is the
//! normal state of a fresh canvas, and the dashboard renders it as a form.

use crate::config::NotifyConfig;
use crate::entities::db::setting::{
    AccountSetting, CanvasSetting, FindAccountDefault, FindAccountSetting, FindCanvasSetting,
    Language, NoticeKind, UpsertAccountDefault, UpsertAccountSetting, UpsertCanvasSetting,
};
use crate::services::NotifyError;
use auth::services::identity::Identity;
use auth::utils::rbac::Permission;
use base::db::Db;
use kanau::processor::Processor;
use orchestration::entities::db::canvas::CanvasId;

/// Reads and replaces notification settings.
#[derive(Clone)]
pub struct SettingService {
    pub db: Db,
    /// Read once at startup; only [`NotifyConfig::default_language`] is used
    /// here, as the language a row that does not exist yet is shown with.
    pub config: NotifyConfig,
}

/// The workspace's shared destinations.
pub struct GetCanvasSetting {
    pub actor: Identity,
    pub canvas: CanvasId,
}

impl Processor<GetCanvasSetting> for SettingService {
    type Output = CanvasSetting;
    type Error = NotifyError;
    #[tracing::instrument(name = "Service:GetCanvasSetting", skip_all, err)]
    async fn process(&self, input: GetCanvasSetting) -> Result<Self::Output, Self::Error> {
        input.actor.ensure(Permission::ViewWorkspace)?;
        Ok(self
            .db
            .process(FindCanvasSetting {
                canvas: input.canvas.clone(),
            })
            .await?
            .unwrap_or_else(|| CanvasSetting {
                canvas: input.canvas,
                language: self.config.default_language,
                events: Vec::new(),
                emails: Vec::new(),
                telegram_chats: Vec::new(),
            }))
    }
}

/// Replaces the workspace's shared destinations.
pub struct SetCanvasSetting {
    pub actor: Identity,
    pub canvas: CanvasId,
    pub language: Language,
    pub events: Vec<NoticeKind>,
    pub emails: Vec<String>,
    pub telegram_chats: Vec<String>,
}

impl Processor<SetCanvasSetting> for SettingService {
    type Output = CanvasSetting;
    type Error = NotifyError;
    #[tracing::instrument(name = "Service:SetCanvasSetting", skip_all, err)]
    async fn process(&self, input: SetCanvasSetting) -> Result<Self::Output, Self::Error> {
        input.actor.ensure(Permission::EditWorkspace)?;
        self.db
            .process(UpsertCanvasSetting {
                setting: CanvasSetting {
                    canvas: input.canvas,
                    language: input.language,
                    events: dedupe(input.events),
                    emails: addresses(input.emails)?,
                    telegram_chats: chats(input.telegram_chats),
                },
            })
            .await
            .map_err(write_error)
    }
}

/// The caller's own setting as it is in force for `canvas`: its row for that
/// canvas, its default row when the canvas has none, and the defaults when it
/// has neither. `canvas: None` asks for the default row itself.
pub struct GetPersonalSetting {
    pub actor: Identity,
    pub canvas: Option<CanvasId>,
}

impl Processor<GetPersonalSetting> for SettingService {
    type Output = AccountSetting;
    type Error = NotifyError;
    #[tracing::instrument(name = "Service:GetPersonalSetting", skip_all, err)]
    async fn process(&self, input: GetPersonalSetting) -> Result<Self::Output, Self::Error> {
        input.actor.require_human()?;
        let account = input.actor.account_id.clone();
        let own = match &input.canvas {
            Some(canvas) => {
                self.db
                    .process(FindAccountSetting {
                        account: account.clone(),
                        canvas: canvas.clone(),
                    })
                    .await?
            }
            None => None,
        };
        let effective = match own {
            Some(setting) => Some(setting),
            None => {
                self.db
                    .process(FindAccountDefault {
                        account: account.clone(),
                    })
                    .await?
            }
        };
        Ok(effective.map_or_else(
            || AccountSetting {
                account: account.clone(),
                canvas: input.canvas.clone(),
                language: self.config.default_language,
                events: Vec::new(),
                email_enabled: false,
                telegram_chat: None,
            },
            // The answer describes the canvas that was asked about, whichever
            // row supplied the values.
            |setting| AccountSetting {
                canvas: input.canvas.clone(),
                ..setting
            },
        ))
    }
}

/// Replaces the caller's own setting for `canvas` (its default row when
/// `canvas` is `None`).
pub struct SetPersonalSetting {
    pub actor: Identity,
    pub canvas: Option<CanvasId>,
    pub language: Language,
    pub events: Vec<NoticeKind>,
    pub email_enabled: bool,
    pub telegram_chat: Option<String>,
}

impl Processor<SetPersonalSetting> for SettingService {
    type Output = AccountSetting;
    type Error = NotifyError;
    #[tracing::instrument(name = "Service:SetPersonalSetting", skip_all, err)]
    async fn process(&self, input: SetPersonalSetting) -> Result<Self::Output, Self::Error> {
        input.actor.require_human()?;
        let account = input.actor.account_id.clone();
        let events = dedupe(input.events);
        let telegram_chat = input
            .telegram_chat
            .map(|chat| chat.trim().to_string())
            .filter(|chat| !chat.is_empty());
        Ok(match input.canvas {
            Some(canvas) => self
                .db
                .process(UpsertAccountSetting {
                    account,
                    canvas,
                    language: input.language,
                    events,
                    email_enabled: input.email_enabled,
                    telegram_chat,
                })
                .await
                .map_err(write_error)?,
            None => self
                .db
                .process(UpsertAccountDefault {
                    account,
                    language: input.language,
                    events,
                    email_enabled: input.email_enabled,
                    telegram_chat,
                })
                .await
                .map_err(write_error)?,
        })
    }
}

/// A write naming a canvas that does not exist is a stale client, not a fault:
/// the foreign key is what refuses it, and `NOT_FOUND` is what the caller can
/// act on.
fn write_error(error: base::db::Error) -> NotifyError {
    if error.fk_violation().is_some() {
        return NotifyError::NotFound;
    }
    NotifyError::Database(error)
}

/// Keeps the first occurrence of each kind: the set is what the column stores,
/// and a duplicate would only make the row longer.
fn dedupe(events: Vec<NoticeKind>) -> Vec<NoticeKind> {
    let mut kept: Vec<NoticeKind> = Vec::with_capacity(events.len());
    for event in events {
        if !kept.contains(&event) {
            kept.push(event);
        }
    }
    kept
}

/// Trims the destinations and drops the empty ones. An address with no `@` is
/// refused here rather than at delivery time: a notice sent to it would fail
/// silently, and this is the one moment an operator is watching.
fn addresses(emails: Vec<String>) -> Result<Vec<String>, NotifyError> {
    let mut kept = Vec::with_capacity(emails.len());
    for email in emails {
        let email = email.trim().to_string();
        if email.is_empty() {
            continue;
        }
        if !email.contains('@') {
            return Err(NotifyError::InvalidInput(format!(
                "{email} is not an email address"
            )));
        }
        if !kept.contains(&email) {
            kept.push(email);
        }
    }
    Ok(kept)
}

/// Trims the chat ids and drops the empty ones.
fn chats(chats: Vec<String>) -> Vec<String> {
    let mut kept = Vec::with_capacity(chats.len());
    for chat in chats {
        let chat = chat.trim().to_string();
        if !chat.is_empty() && !kept.contains(&chat) {
            kept.push(chat);
        }
    }
    kept
}
