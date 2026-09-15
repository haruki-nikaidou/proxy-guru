//! # `rpguru_sdk`
//!
//! Generated gRPC/protobuf types shared across the workspace. `guru-master`,
//! `guru-worker`, and the business modules under `modules/` all take their
//! request/reply types and service traits from here, so there is exactly one
//! generated copy of the API.
//!
//! The workspace `proto/` directory is the single source of truth. This crate's
//! `build.rs` compiles the `.proto` files it lists with `tonic-prost-build`
//! (server and client), and re-runs whenever anything under `proto/` changes.
//! Each generated package is re-exported below with `tonic::include_proto!`.
//! Never hand-edit or duplicate generated code.
//!
//! ## Adding a service
//!
//! 1. Put the `.proto` file under `proto/<module>/` with a package name such as
//!    `guru.<module>`.
//! 2. Add its path to the file list in this crate's `build.rs`.
//! 3. Re-export the generated package here:
//!
//! ```ignore
//! pub mod example {
//!     tonic::include_proto!("guru.example");
//! }
//! ```
//!
//! 4. Regenerate the TypeScript counterpart (`typescript/app-protobuf`) from the
//!    repository root with `bun run generate:proto`.
//!
//! Keep conversions between protobuf types and domain types in this crate too,
//! so every consumer shares one implementation.

#![deny(clippy::unwrap_used)]
#![deny(clippy::expect_used)]
#![deny(clippy::panic)]

/// Generated types for `proto/base/config.proto` (`package guru.base`).
///
/// Shared across services: `guru.auth` and `guru.orchestration` both reference
/// [`base::ConfigDocument`] for their configuration RPCs, and prost resolves
/// that cross-package reference to this module because the re-exports below
/// mirror the proto packages' own names.
pub mod base {
    tonic::include_proto!("guru.base");
}

/// Generated types and service traits for `proto/auth/auth.proto`
/// (`package guru.auth`).
pub mod auth {
    tonic::include_proto!("guru.auth");
}

/// Generated types and service traits for `proto/orchestration/orchestration.proto`
/// (`package guru.orchestration`).
///
/// The `large_enum_variant` allow is for the live-stream `oneof`s: a
/// `CanvasSnapshot` next to an empty `KeepAlive` is a deliberate wire shape,
/// and the generated code is not ours to box.
pub mod orchestration {
    #![allow(clippy::large_enum_variant)]
    tonic::include_proto!("guru.orchestration");
}

/// Generated types and service traits for `proto/orchestration/agent.proto`
/// (`package guru.orchestration.agent`).
pub mod orchestration_agent {
    tonic::include_proto!("guru.orchestration.agent");
}
