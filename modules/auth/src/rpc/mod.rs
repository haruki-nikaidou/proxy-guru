//! Transport edge: gRPC service implementations.
//!
//! This is where the protobuf-generated service traits (from the `app_protobuf`
//! crate) are implemented. An RPC impl is a thin adapter: it decodes the
//! request, calls into [`services`](crate::services) (or
//! [`entities`](crate::entities) directly for trivial reads), and encodes the
//! reply. Keep business logic in services — RPC handlers should only translate
//! between the wire format and domain types.
//!
//! Declare each service in its own submodule and re-export the concrete type so
//! `guru-master` can mount it with
//! `ServiceBuilder::compose_grpc_service(|s| ExampleServer::new(ExampleServiceImpl { .. }))`:
//!
//! ```ignore
//! mod example_service;
//! pub use example_service::ExampleServiceImpl;
//! ```
//!
//! ```ignore
//! use kanau::processor::Processor;
//! use tonic::{Request, Response, Status};
//!
//! use crate::services::{DoSomething, ExampleService};
//!
//! #[derive(Clone)]
//! pub struct ExampleServiceImpl {
//!     pub example: ExampleService,
//! }
//!
//! #[tonic::async_trait]
//! impl app_protobuf::base::example_server::Example for ExampleServiceImpl {
//!     async fn do_something(
//!         &self,
//!         request: Request<app_protobuf::base::DoSomethingRequest>,
//!     ) -> Result<Response<app_protobuf::base::DoSomethingReply>, Status> {
//!         let req = request.into_inner();
//!         self.example.process(DoSomething { id: req.id.parse().unwrap() }).await?;
//!         Ok(Response::new(app_protobuf::base::DoSomethingReply::default()))
//!     }
//! }
//! ```

mod auth_service;
pub mod middleware;

pub use crate::services::{Identity, IdentityKind};
pub use auth_service::AuthGrpc;
pub use middleware::{API_KEY_METADATA, AuthLayer, AuthMiddleware, SESSION_ID_METADATA};

use std::convert::Infallible;
use wakuwaku::proof::ValidateError;

/// A request field that failed its verifier is the caller's mistake: answer
/// `INVALID_ARGUMENT` with the verifier's reason.
pub fn validation_status(error: ValidateError<Infallible>) -> tonic::Status {
    match error {
        ValidateError::Deny(reason) => tonic::Status::invalid_argument(reason.0),
        ValidateError::IoError(never) => match never {},
    }
}
