//! One uninhabited marker per SQL table this crate owns, named after the table.
//!
//! A query names the tables it touches in its `Query::Effect` (`Read<_>` /
//! `Write<_>`), and the [`Db`](base::db::Db) capability must cover them.

use wakuwaku::entity::Entity;

/// `orchestration_canvas`
pub enum OrchestrationCanvas {}
impl Entity for OrchestrationCanvas {}

/// `orchestration_server`
pub enum OrchestrationServer {}
impl Entity for OrchestrationServer {}

/// `orchestration_server_config_view`
pub enum OrchestrationServerConfigView {}
impl Entity for OrchestrationServerConfigView {}

/// `orchestration_agent_release`
pub enum OrchestrationAgentRelease {}
impl Entity for OrchestrationAgentRelease {}

/// `orchestration_job_run`
pub enum OrchestrationJobRun {}
impl Entity for OrchestrationJobRun {}

/// `orchestration_pod`
pub enum OrchestrationPod {}
impl Entity for OrchestrationPod {}

/// `orchestration_exit`
pub enum OrchestrationExit {}
impl Entity for OrchestrationExit {}

/// `orchestration_edge`
pub enum OrchestrationEdge {}
impl Entity for OrchestrationEdge {}

/// `orchestration_group`
pub enum OrchestrationGroup {}
impl Entity for OrchestrationGroup {}

/// `orchestration_group_member`
pub enum OrchestrationGroupMember {}
impl Entity for OrchestrationGroupMember {}

/// `dns_provider`
pub enum DnsProvider {}
impl Entity for DnsProvider {}

/// `server_health_record`
pub enum ServerHealthRecord {}
impl Entity for ServerHealthRecord {}

/// `pod_health_record`
pub enum PodHealthRecord {}
impl Entity for PodHealthRecord {}

/// `certificate`
pub enum Certificate {}
impl Entity for Certificate {}

/// `internal_ca`
pub enum InternalCa {}
impl Entity for InternalCa {}

/// `relay_certificate`
pub enum RelayCertificate {}
impl Entity for RelayCertificate {}
