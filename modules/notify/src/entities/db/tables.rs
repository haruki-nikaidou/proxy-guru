//! One uninhabited marker per SQL table this crate owns, named after the table.
//!
//! A query names the tables it touches in its `Query::Effect` (`Read<_>` /
//! `Write<_>`), and the [`Db`](base::db::Db) capability must cover them.

use wakuwaku::entity::Entity;

/// `notify_canvas_setting`
pub enum NotifyCanvasSetting {}
impl Entity for NotifyCanvasSetting {}

/// `notify_account_default`
pub enum NotifyAccountDefault {}
impl Entity for NotifyAccountDefault {}

/// `notify_account_setting`
pub enum NotifyAccountSetting {}
impl Entity for NotifyAccountSetting {}

/// `notify_server_state`
pub enum NotifyServerState {}
impl Entity for NotifyServerState {}

/// `notify_pod_state`
pub enum NotifyPodState {}
impl Entity for NotifyPodState {}
