//! Transport edge: the gRPC `Notify` service.
//!
//! Handlers are thin adapters: they authenticate with
//! [`auth::rpc::middleware::from_request`], translate the protobuf request into
//! a service input, and encode the reply. Authorization lives in the services —
//! the workspace row takes `ViewWorkspace`/`EditWorkspace`, an account's own row
//! takes a human session, and the config pair takes `ManageConfig`.
//!
//! Mounted by `guru-master` in `--mode dashboard_grpc`, next to `Auth` and
//! `Orchestration`.

mod notify_service;

pub use notify_service::NotifyGrpc;
