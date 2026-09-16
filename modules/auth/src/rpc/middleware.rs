//! Async identity-injection middleware for the gRPC edge.
//!
//! [`AuthLayer`] wraps a tonic service so that every inbound request is
//! inspected for a session id or API-key secret in its metadata. When one
//! resolves to a valid [`Identity`], it is inserted into the request extensions;
//! handlers then read it back with [`from_request`]. Authentication *failures*
//! never abort the request here — they simply leave no identity, and each
//! handler decides whether that is acceptable.

use std::task::{Context, Poll};

use kanau::processor::Processor;
use tonic::Status;
use tonic::codegen::http;
use tonic::codegen::{BoxFuture, Service};

use crate::services::identity::Identity;
use crate::services::{ApiKeyService, AuthenticateApiKey, AuthenticateSession, SessionService};

/// Metadata key carrying a human session id.
pub const SESSION_ID_METADATA: &str = "x-session-id";
/// Metadata key carrying a machine API-key secret.
pub const API_KEY_METADATA: &str = "x-api-key";
/// How long resolving a credential may take. The database client can leave a
/// lookup pending forever after its socket reconnects; past this the request
/// proceeds without an identity and the handler answers `UNAUTHENTICATED`.
const AUTH_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(10);

/// Tower layer that authenticates requests and injects an [`Identity`] extension.
#[derive(Clone)]
pub struct AuthLayer {
    sessions: SessionService,
    api_keys: ApiKeyService,
}

impl AuthLayer {
    pub fn new(sessions: SessionService, api_keys: ApiKeyService) -> Self {
        Self { sessions, api_keys }
    }
}

impl<S> tower::Layer<S> for AuthLayer {
    type Service = AuthMiddleware<S>;

    fn layer(&self, inner: S) -> Self::Service {
        AuthMiddleware {
            inner,
            sessions: self.sessions.clone(),
            api_keys: self.api_keys.clone(),
        }
    }
}

/// The service produced by [`AuthLayer`].
#[derive(Clone)]
pub struct AuthMiddleware<S> {
    inner: S,
    sessions: SessionService,
    api_keys: ApiKeyService,
}

impl<S, ReqBody> Service<http::Request<ReqBody>> for AuthMiddleware<S>
where
    S: Service<http::Request<ReqBody>> + Clone + Send + 'static,
    S::Future: Send + 'static,
    ReqBody: Send + 'static,
{
    type Response = S::Response;
    type Error = S::Error;
    type Future = BoxFuture<Self::Response, Self::Error>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, mut req: http::Request<ReqBody>) -> Self::Future {
        // Take the ready inner service; the clone left behind is not yet ready.
        let clone = self.inner.clone();
        let mut inner = std::mem::replace(&mut self.inner, clone);
        let sessions = self.sessions.clone();
        let api_keys = self.api_keys.clone();
        Box::pin(async move {
            let session_id = header(&req, SESSION_ID_METADATA);
            let api_key = header(&req, API_KEY_METADATA);
            let lookup = async {
                if let Some(session_id) = session_id {
                    sessions
                        .process(AuthenticateSession { session_id })
                        .await
                        .ok()
                        .flatten()
                } else if let Some(secret) = api_key {
                    api_keys
                        .process(AuthenticateApiKey { secret })
                        .await
                        .ok()
                        .flatten()
                } else {
                    None
                }
            };
            let identity = match tokio::time::timeout(AUTH_TIMEOUT, lookup).await {
                Ok(identity) => identity,
                Err(_) => {
                    tracing::warn!(
                        "resolving a credential timed out; the request proceeds anonymous"
                    );
                    None
                }
            };
            if let Some(identity) = identity {
                req.extensions_mut().insert(identity);
            }
            inner.call(req).await
        })
    }
}

/// Read a header value as an owned `String`, if present and valid UTF-8.
fn header<B>(req: &http::Request<B>, name: &str) -> Option<String> {
    req.headers()
        .get(name)
        .and_then(|value| value.to_str().ok())
        .map(str::to_owned)
}

/// Extract the authenticated [`Identity`] a handler requires, or fail with
/// `UNAUTHENTICATED` when the middleware injected none.
pub fn from_request<T>(req: &tonic::Request<T>) -> Result<Identity, Status> {
    req.extensions()
        .get::<Identity>()
        .cloned()
        .ok_or_else(|| Status::unauthenticated("Missing identity"))
}
