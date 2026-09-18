//! gRPC `Notify` service implementation: a thin adapter over the services.

use crate::entities::db::setting::{AccountSetting, CanvasSetting, Language, NoticeKind};
use crate::services::config::{
    ConfigDocument, GetModuleConfig, NotifyConfigService, SetModuleConfig,
};
use crate::services::fanout::{FanoutService, SendTestNotice};
use crate::services::setting::{
    GetCanvasSetting, GetPersonalSetting, SetCanvasSetting, SetPersonalSetting, SettingService,
};
use auth::rpc::middleware::from_request;
use kanau::processor::Processor;
use orchestration::entities::db::canvas::CanvasId;
use rpguru_sdk::notify as pb;
use tonic::{Request, Response, Status};

/// The concrete gRPC `Notify` service.
#[derive(Clone)]
pub struct NotifyGrpc {
    pub settings: SettingService,
    pub fanout: FanoutService,
    pub configs: NotifyConfigService,
}

fn language_to_proto(language: Language) -> i32 {
    let language = match language {
        Language::En => pb::NotifyLanguage::En,
        Language::Ja => pb::NotifyLanguage::Ja,
        Language::ZhCn => pb::NotifyLanguage::ZhCn,
    };
    language as i32
}

fn language_from_proto(language: i32) -> Result<Language, Status> {
    match pb::NotifyLanguage::try_from(language) {
        Ok(pb::NotifyLanguage::En) => Ok(Language::En),
        Ok(pb::NotifyLanguage::Ja) => Ok(Language::Ja),
        Ok(pb::NotifyLanguage::ZhCn) => Ok(Language::ZhCn),
        _ => Err(Status::invalid_argument("invalid or unspecified language")),
    }
}

fn kind_to_proto(kind: NoticeKind) -> i32 {
    let kind = match kind {
        NoticeKind::ServerOnline => pb::HealthNoticeKind::ServerOnline,
        NoticeKind::ServerDegraded => pb::HealthNoticeKind::ServerDegraded,
        NoticeKind::ServerOffline => pb::HealthNoticeKind::ServerOffline,
        NoticeKind::PodReady => pb::HealthNoticeKind::PodReady,
        NoticeKind::PodDeploying => pb::HealthNoticeKind::PodDeploying,
        NoticeKind::PodFailed => pb::HealthNoticeKind::PodFailed,
    };
    kind as i32
}

fn kinds_from_proto(kinds: &[i32]) -> Result<Vec<NoticeKind>, Status> {
    kinds
        .iter()
        .map(|kind| match pb::HealthNoticeKind::try_from(*kind) {
            Ok(pb::HealthNoticeKind::ServerOnline) => Ok(NoticeKind::ServerOnline),
            Ok(pb::HealthNoticeKind::ServerDegraded) => Ok(NoticeKind::ServerDegraded),
            Ok(pb::HealthNoticeKind::ServerOffline) => Ok(NoticeKind::ServerOffline),
            Ok(pb::HealthNoticeKind::PodReady) => Ok(NoticeKind::PodReady),
            Ok(pb::HealthNoticeKind::PodDeploying) => Ok(NoticeKind::PodDeploying),
            Ok(pb::HealthNoticeKind::PodFailed) => Ok(NoticeKind::PodFailed),
            _ => Err(Status::invalid_argument(
                "invalid or unspecified notice kind",
            )),
        })
        .collect()
}

fn canvas_setting_to_proto(setting: CanvasSetting) -> pb::CanvasNotifySetting {
    pb::CanvasNotifySetting {
        canvas: setting.canvas.into_string(),
        language: language_to_proto(setting.language),
        events: setting.events.into_iter().map(kind_to_proto).collect(),
        emails: setting.emails,
        telegram_chats: setting.telegram_chats,
    }
}

fn account_setting_to_proto(setting: AccountSetting) -> pb::PersonalNotifySetting {
    pb::PersonalNotifySetting {
        canvas: setting
            .canvas
            .map(CanvasId::into_string)
            .unwrap_or_default(),
        language: language_to_proto(setting.language),
        events: setting.events.into_iter().map(kind_to_proto).collect(),
        email_enabled: setting.email_enabled,
        telegram_chat: setting.telegram_chat.unwrap_or_default(),
    }
}

/// A canvas id a request named. Empty is a caller that forgot it.
fn canvas_id(canvas: &str) -> Result<CanvasId, Status> {
    let canvas = canvas.trim();
    if canvas.is_empty() {
        return Err(Status::invalid_argument("canvas is required"));
    }
    Ok(CanvasId::from_key(canvas))
}

/// The canvas of a personal setting: empty names the account's default row.
fn personal_scope(canvas: &str) -> Option<CanvasId> {
    let canvas = canvas.trim();
    if canvas.is_empty() {
        None
    } else {
        Some(CanvasId::from_key(canvas))
    }
}

/// Both document fields are pretty-printed: an operator edits this text.
fn config_to_proto(document: ConfigDocument) -> Result<rpguru_sdk::base::ConfigDocument, Status> {
    let encode = |value: &serde_json::Value| {
        serde_json::to_string_pretty(value).map_err(|error| {
            tracing::error!(error = %error, "serializing notify config document");
            Status::internal("Internal server error")
        })
    };
    Ok(rpguru_sdk::base::ConfigDocument {
        stored: document.stored,
        json: encode(&document.json)?,
        defaults_json: encode(&document.defaults)?,
    })
}

/// The payload an operator typed. Not valid JSON is their typo, not a bug.
fn config_json(json: &str) -> Result<serde_json::Value, Status> {
    serde_json::from_str(json).map_err(|error| {
        Status::invalid_argument(format!("the payload is not valid JSON: {error}"))
    })
}

#[tonic::async_trait]
impl pb::notify_server::Notify for NotifyGrpc {
    async fn get_canvas_notify_setting(
        &self,
        request: Request<pb::GetCanvasNotifySettingRequest>,
    ) -> Result<Response<pb::GetCanvasNotifySettingReply>, Status> {
        let actor = from_request(&request)?;
        let canvas = canvas_id(&request.into_inner().canvas)?;
        let setting = self
            .settings
            .process(GetCanvasSetting { actor, canvas })
            .await?;
        Ok(Response::new(pb::GetCanvasNotifySettingReply {
            setting: Some(canvas_setting_to_proto(setting)),
        }))
    }

    async fn set_canvas_notify_setting(
        &self,
        request: Request<pb::SetCanvasNotifySettingRequest>,
    ) -> Result<Response<pb::SetCanvasNotifySettingReply>, Status> {
        let actor = from_request(&request)?;
        let setting = request
            .into_inner()
            .setting
            .ok_or_else(|| Status::invalid_argument("setting is required"))?;
        let stored = self
            .settings
            .process(SetCanvasSetting {
                actor,
                canvas: canvas_id(&setting.canvas)?,
                language: language_from_proto(setting.language)?,
                events: kinds_from_proto(&setting.events)?,
                emails: setting.emails,
                telegram_chats: setting.telegram_chats,
            })
            .await?;
        Ok(Response::new(pb::SetCanvasNotifySettingReply {
            setting: Some(canvas_setting_to_proto(stored)),
        }))
    }

    async fn get_personal_notify_setting(
        &self,
        request: Request<pb::GetPersonalNotifySettingRequest>,
    ) -> Result<Response<pb::GetPersonalNotifySettingReply>, Status> {
        let actor = from_request(&request)?;
        let canvas = personal_scope(&request.into_inner().canvas);
        let setting = self
            .settings
            .process(GetPersonalSetting { actor, canvas })
            .await?;
        Ok(Response::new(pb::GetPersonalNotifySettingReply {
            setting: Some(account_setting_to_proto(setting)),
        }))
    }

    async fn set_personal_notify_setting(
        &self,
        request: Request<pb::SetPersonalNotifySettingRequest>,
    ) -> Result<Response<pb::SetPersonalNotifySettingReply>, Status> {
        let actor = from_request(&request)?;
        let setting = request
            .into_inner()
            .setting
            .ok_or_else(|| Status::invalid_argument("setting is required"))?;
        let stored = self
            .settings
            .process(SetPersonalSetting {
                actor,
                canvas: personal_scope(&setting.canvas),
                language: language_from_proto(setting.language)?,
                events: kinds_from_proto(&setting.events)?,
                email_enabled: setting.email_enabled,
                telegram_chat: Some(setting.telegram_chat),
            })
            .await?;
        Ok(Response::new(pb::SetPersonalNotifySettingReply {
            setting: Some(account_setting_to_proto(stored)),
        }))
    }

    async fn send_test_notification(
        &self,
        request: Request<pb::SendTestNotificationRequest>,
    ) -> Result<Response<pb::SendTestNotificationReply>, Status> {
        let actor = from_request(&request)?;
        let canvas = canvas_id(&request.into_inner().canvas)?;
        self.fanout
            .process(SendTestNotice { actor, canvas })
            .await?;
        Ok(Response::new(pb::SendTestNotificationReply { sent: true }))
    }

    async fn get_notify_config(
        &self,
        request: Request<pb::GetNotifyConfigRequest>,
    ) -> Result<Response<pb::GetNotifyConfigReply>, Status> {
        let actor = from_request(&request)?;
        let document = self.configs.process(GetModuleConfig { actor }).await?;
        Ok(Response::new(pb::GetNotifyConfigReply {
            config: Some(config_to_proto(document)?),
        }))
    }

    async fn set_notify_config(
        &self,
        request: Request<pb::SetNotifyConfigRequest>,
    ) -> Result<Response<pb::SetNotifyConfigReply>, Status> {
        let actor = from_request(&request)?;
        let json = config_json(&request.into_inner().json)?;
        let document = self
            .configs
            .process(SetModuleConfig { actor, json })
            .await?;
        Ok(Response::new(pb::SetNotifyConfigReply {
            config: Some(config_to_proto(document)?),
        }))
    }
}
