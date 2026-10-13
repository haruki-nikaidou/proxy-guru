//! Transport edge: gRPC service implementations.
//!
//! This is where the protobuf-generated service traits (from `rpguru_sdk`) are
//! implemented. An RPC impl is a thin adapter: it decodes the request, calls
//! into [`services`](crate::services) (or [`entities`](crate::entities)
//! directly for trivial reads), and encodes the reply. Keep business logic in
//! services — RPC handlers should only translate between the wire format and
//! domain types.
//!
//! A service's error is the module's error enum, which implements
//! `From<ModuleError> for tonic::Status`, so a handler propagates it with `?`;
//! a bare database error goes through [`crate::db::status_of`].
//!
//! Declare each service in its own submodule and re-export the concrete type so
//! `guru-master` can mount it with `ServiceBuilder::compose_grpc_service`:
//!
//! ```ignore
//! mod example_service;
//! pub use example_service::ExampleGrpc;
//! ```
//!
//! ```ignore
//! use kanau::processor::Processor;
//! use tonic::{Request, Response, Status};
//!
//! use crate::services::{DoSomething, ExampleService};
//!
//! #[derive(Clone)]
//! pub struct ExampleGrpc {
//!     pub example: ExampleService,
//! }
//!
//! #[tonic::async_trait]
//! impl rpguru_sdk::base::example_server::Example for ExampleGrpc {
//!     async fn do_something(
//!         &self,
//!         request: Request<rpguru_sdk::base::DoSomethingRequest>,
//!     ) -> Result<Response<rpguru_sdk::base::DoSomethingReply>, Status> {
//!         let req = request.into_inner();
//!         let id = req.id.parse().map_err(|_| Status::invalid_argument("bad id"))?;
//!         self.example.process(DoSomething { id }).await?;
//!         Ok(Response::new(rpguru_sdk::base::DoSomethingReply::default()))
//!     }
//! }
//! ```
