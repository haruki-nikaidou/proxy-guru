//! Rendering one notice into the text a recipient reads.
//!
//! Pure, and deliberately the same text on both channels: a mail's subject is
//! the headline, its body is the headline plus the facts, and a Telegram message
//! is that same body. Every string a recipient can see is here — there is no
//! template file and nothing is formatted at delivery time.

use crate::entities::db::setting::{Language, NoticeKind};
use crate::events::HealthNotice;
use time::OffsetDateTime;
use time::format_description::StaticFormatDescription;

/// How a notice renders its timestamp: UTC wall clock, zero-padded.
const NOTICE_TIME: StaticFormatDescription =
    time::macros::format_description!("[year]-[month]-[day] [hour]:[minute]:[second] UTC");

/// A notice as one recipient will see it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RenderedNotice {
    pub subject: String,
    pub body: String,
}

/// The headline of one notice: the sentence both channels lead with.
fn headline(kind: NoticeKind, name: &str, language: Language) -> String {
    match (language, kind) {
        (Language::En, NoticeKind::ServerOnline) => format!("Server {name} is online"),
        (Language::En, NoticeKind::ServerDegraded) => format!("Server {name} is degraded"),
        (Language::En, NoticeKind::ServerOffline) => format!("Server {name} is offline"),
        (Language::En, NoticeKind::PodReady) => format!("Pod {name} is ready"),
        (Language::En, NoticeKind::PodDeploying) => format!("Pod {name} is deploying"),
        (Language::En, NoticeKind::PodFailed) => format!("Pod {name} failed"),
        (Language::Ja, NoticeKind::ServerOnline) => {
            format!("サーバー {name} がオンラインになりました")
        }
        (Language::Ja, NoticeKind::ServerDegraded) => format!("サーバー {name} が劣化状態です"),
        (Language::Ja, NoticeKind::ServerOffline) => {
            format!("サーバー {name} がオフラインになりました")
        }
        (Language::Ja, NoticeKind::PodReady) => format!("ポッド {name} が稼働しました"),
        (Language::Ja, NoticeKind::PodDeploying) => format!("ポッド {name} を配信中です"),
        (Language::Ja, NoticeKind::PodFailed) => format!("ポッド {name} が失敗しました"),
        (Language::ZhCn, NoticeKind::ServerOnline) => format!("服务器 {name} 已上线"),
        (Language::ZhCn, NoticeKind::ServerDegraded) => format!("服务器 {name} 处于降级状态"),
        (Language::ZhCn, NoticeKind::ServerOffline) => format!("服务器 {name} 已离线"),
        (Language::ZhCn, NoticeKind::PodReady) => format!("节点 {name} 已就绪"),
        (Language::ZhCn, NoticeKind::PodDeploying) => format!("节点 {name} 正在部署"),
        (Language::ZhCn, NoticeKind::PodFailed) => format!("节点 {name} 部署失败"),
    }
}

/// The four field labels of the body, in `(subject, canvas, time, detail)` order.
fn labels(language: Language) -> (&'static str, &'static str, &'static str, &'static str) {
    match language {
        Language::En => ("Subject", "Canvas", "Time", "Detail"),
        Language::Ja => ("対象", "キャンバス", "時刻", "詳細"),
        Language::ZhCn => ("对象", "画布", "时间", "详情"),
    }
}

/// Renders `notice` in `language`.
pub fn render(notice: &HealthNotice, language: Language) -> RenderedNotice {
    let headline = headline(notice.kind, &notice.subject_name, language);
    let (subject_label, canvas_label, time_label, detail_label) = labels(language);
    // A timestamp no calendar can represent is a corrupt event, not a reason to
    // drop the notice: the epoch is obviously wrong to a reader, and the rest of
    // the message still says what happened.
    let time = OffsetDateTime::from_unix_timestamp_nanos(
        i128::from(notice.at_unix_micros).saturating_mul(1_000),
    )
    .unwrap_or(OffsetDateTime::UNIX_EPOCH)
    .format(NOTICE_TIME)
    .unwrap_or_default();
    let mut body = format!(
        "{headline}\n\n{subject_label}: {} ({})\n{canvas_label}: {}\n{time_label}: {time}",
        notice.subject_name, notice.subject, notice.canvas_name
    );
    if !notice.message.is_empty() {
        body.push_str(&format!("\n{detail_label}: {}", notice.message));
    }
    RenderedNotice {
        subject: format!("[guru] {headline}"),
        body,
    }
}
