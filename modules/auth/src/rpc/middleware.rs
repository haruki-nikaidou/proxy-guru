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
            let resolve = || {
                let (sessions, api_keys) = (sessions.clone(), api_keys.clone());
                let (session_id, api_key) = (session_id.clone(), api_key.clone());
                async move {
                    if let Some(session_id) = session_id {
                        sessions.process(AuthenticateSession { session_id }).await
                    } else if let Some(secret) = api_key {
                        api_keys.process(AuthenticateApiKey { secret }).await
                    } else {
                        Ok(None)
                    }
                }
            };
            // One retry: these failures are transient (a reconnected database connection
            // that briefly lost its namespace) and the read is idempotent, so retrying is
            // the difference between a blip nobody notices and a visible error.
            let mut outcome = bounded(resolve()).await;
            if matches!(outcome, Resolved::Unavailable) {
                outcome = bounded(resolve()).await;
            }
            match outcome {
                Resolved::Identity(identity) => {
                    req.extensions_mut().insert(identity);
                }
                // Authoritative: no such session, or it expired. The handler answers
                // UNAUTHENTICATED and the caller is correctly sent to log in again.
                Resolved::Anonymous => {}
                // The credential was never judged. Marking the request is the only way to
                // say so from here: this middleware is generic over the inner service, so
                // it cannot build a response of its own.
                Resolved::Unavailable => {
                    req.extensions_mut().insert(AuthUnavailable);
                }
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

/// Marks a request whose credential could not be judged because the database did not
/// answer. Distinct from carrying no identity, which means the credential *was* judged and
/// found wanting.
#[derive(Debug, Clone, Copy)]
pub struct AuthUnavailable;

/// What resolving a credential produced.
enum Resolved {
    Identity(Identity),
    /// The credential was judged: absent, unknown, or expired.
    Anonymous,
    /// The credential was not judged: the database failed or did not answer in time.
    Unavailable,
}

/// Resolves a credential under [`AUTH_TIMEOUT`], keeping "not judged" separate from
/// "judged and rejected".
async fn bounded(
    lookup: impl Future<Output = Result<Option<Identity>, wakuwaku::Error>>,
) -> Resolved {
    match tokio::time::timeout(AUTH_TIMEOUT, lookup).await {
        Ok(Ok(Some(identity))) => Resolved::Identity(identity),
        Ok(Ok(None)) => Resolved::Anonymous,
        Ok(Err(error)) => {
            tracing::warn!(%error, "resolving a credential failed");
            Resolved::Unavailable
        }
        Err(_) => {
            tracing::warn!("resolving a credential timed out");
            Resolved::Unavailable
        }
    }
}

/// Extract the authenticated [`Identity`] a handler requires.
///
/// `UNAUTHENTICATED` only when the credential was actually judged and rejected. A database
/// that failed or timed out answers `UNAVAILABLE` instead: the dashboard's error boundary
/// logs the operator out on `UNAUTHENTICATED`, so blurring the two costs a session every
/// time the database blips.
pub fn from_request<T>(req: &tonic::Request<T>) -> Result<Identity, Status> {
    if let Some(identity) = req.extensions().get::<Identity>() {
        return Ok(identity.clone());
    }
    if req.extensions().get::<AuthUnavailable>().is_some() {
        return Err(Status::unavailable("Authentication is briefly unavailable"));
    }
    Err(Status::unauthenticated("Missing identity"))
}
