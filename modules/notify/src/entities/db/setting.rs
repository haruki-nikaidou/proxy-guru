//! Who wants to hear what, in which language, through which channel.
//!
//! Three tables, two scopes. `notify_canvas_setting` is the workspace's own
//! row: the destinations everyone watching that canvas shares.
//! `notify_account_setting` is one operator's row for one canvas, and
//! `notify_account_default` is the row that serves every canvas the account has
//! no row for — [`ListPersonalRecipients`] resolves that fallback in the
//! statement, so the fan-out never has to.
//!
//! `events` is the set of kinds a row wants. It defaults to empty in the schema
//! on purpose: installing this module notifies nobody until an operator opts in.

use crate::entities::db::tables::{
    NotifyAccountDefault, NotifyAccountSetting, NotifyCanvasSetting,
};
use auth::entities::db::account::AccountId;
use auth::entities::db::tables::AuthAccount;
use base::db::Error;
use db_types::text_enum;
use orchestration::entities::db::canvas::CanvasId;
use orchestration::entities::db::tables::OrchestrationCanvas;
use serde::{Deserialize, Serialize};
use sqlx::PgConnection;
use wakuwaku::entity::sqlx::backend::PgSource;
use wakuwaku::entity::{Execute, Query, Read, Write};

/// The languages a notice can be rendered in.
///
/// The `rkyv` derives put this on the broker inside
/// [`crate::events::HealthNotifyGroupEvent`]; the `text_enum!` spelling is the
/// one the column's `CHECK` names.
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Default,
    Serialize,
    Deserialize,
    rkyv::Archive,
    rkyv::Serialize,
    rkyv::Deserialize,
)]
#[serde(rename_all = "snake_case")]
pub enum Language {
    #[default]
    En,
    Ja,
    ZhCn,
}
text_enum!(Language {
    En => "en",
    Ja => "ja",
    ZhCn => "zh_cn",
});

/// What a notice is about — and, as a set, what a setting subscribes to.
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Hash,
    Serialize,
    Deserialize,
    rkyv::Archive,
    rkyv::Serialize,
    rkyv::Deserialize,
)]
#[serde(rename_all = "snake_case")]
pub enum NoticeKind {
    ServerOnline,
    ServerDegraded,
    ServerOffline,
    PodReady,
    PodDeploying,
    PodFailed,
}
text_enum!(NoticeKind {
    ServerOnline => "server_online",
    ServerDegraded => "server_degraded",
    ServerOffline => "server_offline",
    PodReady => "pod_ready",
    PodDeploying => "pod_deploying",
    PodFailed => "pod_failed",
});

/// One workspace's shared destinations.
#[derive(Debug, Clone)]
pub struct CanvasSetting {
    pub canvas: CanvasId,
    pub language: Language,
    pub events: Vec<NoticeKind>,
    pub emails: Vec<String>,
    pub telegram_chats: Vec<String>,
}

impl CanvasSetting {
    /// Whether this row would receive `kind` anywhere.
    pub fn wants(&self, kind: NoticeKind) -> bool {
        self.events.contains(&kind) && !(self.emails.is_empty() && self.telegram_chats.is_empty())
    }
}

/// One account's own channels, for one canvas or as its default.
#[derive(Debug, Clone)]
pub struct AccountSetting {
    pub account: AccountId,
    /// `None` is the account's default row.
    pub canvas: Option<CanvasId>,
    pub language: Language,
    pub events: Vec<NoticeKind>,
    /// Mail goes to the account's own address, so only the switch is stored.
    pub email_enabled: bool,
    pub telegram_chat: Option<String>,
}

pub struct FindCanvasSetting {
    pub canvas: CanvasId,
}

impl Query for FindCanvasSetting {
    type Effect = Read<NotifyCanvasSetting>;
}

impl Execute<PgSource> for FindCanvasSetting {
    type Output = Option<CanvasSetting>;
    type Error = Error;
    #[tracing::instrument(name = "Query:FindCanvasSetting", skip_all, err)]
    async fn execute(self, conn: &mut PgConnection) -> Result<Self::Output, Self::Error> {
        Ok(sqlx::query_as!(
            CanvasSetting,
            r#"SELECT canvas AS "canvas: CanvasId", language AS "language: Language",
                      events AS "events: Vec<NoticeKind>", emails, telegram_chats
               FROM notify_canvas_setting WHERE canvas = $1"#,
            self.canvas as _
        )
        .fetch_optional(&mut *conn)
        .await?)
    }
}

/// Replaces the whole workspace row, creating it if there is none.
pub struct UpsertCanvasSetting {
    pub setting: CanvasSetting,
}

impl Query for UpsertCanvasSetting {
    type Effect = Write<NotifyCanvasSetting>;
}

impl Execute<PgSource> for UpsertCanvasSetting {
    /// The row as stored.
    type Output = CanvasSetting;
    type Error = Error;
    #[tracing::instrument(name = "Query:UpsertCanvasSetting", skip_all, err)]
    async fn execute(self, conn: &mut PgConnection) -> Result<Self::Output, Self::Error> {
        let setting = self.setting;
        Ok(sqlx::query_file_as!(
            CanvasSetting,
            "sql/upsert_canvas_setting.sql",
            setting.canvas as _,
            setting.language as _,
            &setting.events as _,
            &setting.emails,
            &setting.telegram_chats
        )
        .fetch_one(&mut *conn)
        .await?)
    }
}

/// The flat shape both account tables select; `canvas` is absent in the default
/// row, which is what [`AccountSetting::canvas`] records.
struct AccountSettingRow {
    account: AccountId,
    canvas: Option<CanvasId>,
    language: Language,
    events: Vec<NoticeKind>,
    email_enabled: bool,
    telegram_chat: Option<String>,
}

impl From<AccountSettingRow> for AccountSetting {
    fn from(row: AccountSettingRow) -> Self {
        Self {
            account: row.account,
            canvas: row.canvas,
            language: row.language,
            events: row.events,
            email_enabled: row.email_enabled,
            telegram_chat: row.telegram_chat,
        }
    }
}

/// The account's row for one canvas. Absent means "ask
/// [`FindAccountDefault`]", which is what the fan-out's own statement does.
pub struct FindAccountSetting {
    pub account: AccountId,
    pub canvas: CanvasId,
}

impl Query for FindAccountSetting {
    type Effect = Read<NotifyAccountSetting>;
}

impl Execute<PgSource> for FindAccountSetting {
    type Output = Option<AccountSetting>;
    type Error = Error;
    #[tracing::instrument(name = "Query:FindAccountSetting", skip_all, err)]
    async fn execute(self, conn: &mut PgConnection) -> Result<Self::Output, Self::Error> {
        Ok(sqlx::query_as!(
            AccountSettingRow,
            r#"SELECT account AS "account: AccountId", canvas AS "canvas?: CanvasId",
                      language AS "language: Language", events AS "events: Vec<NoticeKind>",
                      email_enabled, telegram_chat
               FROM notify_account_setting WHERE account = $1 AND canvas = $2"#,
            self.account as _,
            self.canvas as _
        )
        .fetch_optional(&mut *conn)
        .await?
        .map(Into::into))
    }
}

/// The account's default row.
pub struct FindAccountDefault {
    pub account: AccountId,
}

impl Query for FindAccountDefault {
    type Effect = Read<NotifyAccountDefault>;
}

impl Execute<PgSource> for FindAccountDefault {
    type Output = Option<AccountSetting>;
    type Error = Error;
    #[tracing::instrument(name = "Query:FindAccountDefault", skip_all, err)]
    async fn execute(self, conn: &mut PgConnection) -> Result<Self::Output, Self::Error> {
        Ok(sqlx::query_as!(
            AccountSettingRow,
            r#"SELECT account AS "account: AccountId", NULL::text AS "canvas?: CanvasId",
                      language AS "language: Language", events AS "events: Vec<NoticeKind>",
                      email_enabled, telegram_chat
               FROM notify_account_default WHERE account = $1"#,
            self.account as _
        )
        .fetch_optional(&mut *conn)
        .await?
        .map(Into::into))
    }
}

/// Replaces the account's row for one canvas.
pub struct UpsertAccountSetting {
    pub account: AccountId,
    pub canvas: CanvasId,
    pub language: Language,
    pub events: Vec<NoticeKind>,
    pub email_enabled: bool,
    pub telegram_chat: Option<String>,
}

impl Query for UpsertAccountSetting {
    type Effect = Write<NotifyAccountSetting>;
}

impl Execute<PgSource> for UpsertAccountSetting {
    type Output = AccountSetting;
    type Error = Error;
    #[tracing::instrument(name = "Query:UpsertAccountSetting", skip_all, err)]
    async fn execute(self, conn: &mut PgConnection) -> Result<Self::Output, Self::Error> {
        Ok(sqlx::query_file_as!(
            AccountSettingRow,
            "sql/upsert_account_setting.sql",
            self.account as _,
            self.canvas as _,
            self.language as _,
            &self.events as _,
            self.email_enabled,
            self.telegram_chat.as_deref()
        )
        .fetch_one(&mut *conn)
        .await?
        .into())
    }
}

/// Replaces the account's default row.
pub struct UpsertAccountDefault {
    pub account: AccountId,
    pub language: Language,
    pub events: Vec<NoticeKind>,
    pub email_enabled: bool,
    pub telegram_chat: Option<String>,
}

impl Query for UpsertAccountDefault {
    type Effect = Write<NotifyAccountDefault>;
}

impl Execute<PgSource> for UpsertAccountDefault {
    type Output = AccountSetting;
    type Error = Error;
    #[tracing::instrument(name = "Query:UpsertAccountDefault", skip_all, err)]
    async fn execute(self, conn: &mut PgConnection) -> Result<Self::Output, Self::Error> {
        Ok(sqlx::query_file_as!(
            AccountSettingRow,
            "sql/upsert_account_default.sql",
            self.account as _,
            self.language as _,
            &self.events as _,
            self.email_enabled,
            self.telegram_chat.as_deref()
        )
        .fetch_one(&mut *conn)
        .await?
        .into())
    }
}

/// One account the fan-out has to reach, with the channels its setting turned
/// on. `email` is the account's own address, filled in only when its setting
/// enables mail.
#[derive(Debug, Clone)]
pub struct PersonalRecipient {
    pub account: AccountId,
    pub language: Language,
    pub email: Option<String>,
    pub telegram_chat: Option<String>,
}

/// Every account that asked for `kind` on `canvas`, through its own row for
/// that canvas or — when it has none — through its default row.
pub struct ListPersonalRecipients {
    pub kind: NoticeKind,
    pub canvas: CanvasId,
}

impl Query for ListPersonalRecipients {
    type Effect = (
        Read<AuthAccount>,
        Read<NotifyAccountSetting>,
        Read<NotifyAccountDefault>,
    );
}

impl Execute<PgSource> for ListPersonalRecipients {
    type Output = Vec<PersonalRecipient>;
    type Error = Error;
    #[tracing::instrument(name = "Query:ListPersonalRecipients", skip_all, err)]
    async fn execute(self, conn: &mut PgConnection) -> Result<Self::Output, Self::Error> {
        Ok(sqlx::query_file_as!(
            PersonalRecipient,
            "sql/list_personal_recipients.sql",
            self.kind as _,
            self.canvas as _
        )
        .fetch_all(&mut *conn)
        .await?)
    }
}

/// One account's own channels as they are in force for `canvas`, ignoring which
/// kinds it subscribed to: the test notification's recipient. `None` means the
/// account has no channel set up at all.
pub struct FindPersonalRecipient {
    pub account: AccountId,
    pub canvas: CanvasId,
}

impl Query for FindPersonalRecipient {
    type Effect = (
        Read<AuthAccount>,
        Read<NotifyAccountSetting>,
        Read<NotifyAccountDefault>,
    );
}

impl Execute<PgSource> for FindPersonalRecipient {
    type Output = Option<PersonalRecipient>;
    type Error = Error;
    #[tracing::instrument(name = "Query:FindPersonalRecipient", skip_all, err)]
    async fn execute(self, conn: &mut PgConnection) -> Result<Self::Output, Self::Error> {
        Ok(sqlx::query_file_as!(
            PersonalRecipient,
            "sql/find_personal_recipient.sql",
            self.account as _,
            self.canvas as _
        )
        .fetch_optional(&mut *conn)
        .await?)
    }
}

/// The canvas's name, for a notice that names it. The canvas itself belongs to
/// `orchestration`; this module only ever reads its label.
pub struct FindCanvasLabel {
    pub canvas: CanvasId,
}

impl Query for FindCanvasLabel {
    type Effect = Read<OrchestrationCanvas>;
}

impl Execute<PgSource> for FindCanvasLabel {
    type Output = Option<String>;
    type Error = Error;
    #[tracing::instrument(name = "Query:FindCanvasLabel", skip_all, err)]
    async fn execute(self, conn: &mut PgConnection) -> Result<Self::Output, Self::Error> {
        Ok(sqlx::query_scalar!(
            r#"SELECT name AS "name!" FROM orchestration_canvas WHERE id = $1"#,
            self.canvas as _
        )
        .fetch_optional(&mut *conn)
        .await?)
    }
}
