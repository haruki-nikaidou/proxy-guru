//! Refresh-key authentication for the worker-facing gRPC port.
//!
//! Mirrors `auth`'s identity middleware: it never rejects a request, it only
//! injects an [`AgentIdentity`] when the presented refresh key resolves. Handlers
//! decide whether an anonymous call is acceptable.

use crate::services::agent::{AgentIdentity, AgentService, AuthenticateRefreshKey};
use kanau::processor::Processor;
use std::net::IpAddr;
use std::task::{Context, Poll};
use tonic::Status;
use tonic::codegen::http;
use tonic::codegen::{BoxFuture, Service};

/// Metadata key carrying a worker's dynamic refresh key.
pub const REFRESH_KEY_METADATA: &str = "x-refresh-key";
/// How long resolving the key may take. The database client can leave a lookup
/// pending forever after its socket reconnects; a request must not hang on that,
/// so past this the call proceeds anonymous and the handler answers
/// `UNAUTHENTICATED`, which the worker retries.
const AUTH_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(10);

#[derive(Clone)]
pub struct AgentLayer {
    agents: AgentService,
}

impl AgentLayer {
    pub fn new(agents: AgentService) -> Self {
        Self { agents }
    }
}

impl<S> tower::Layer<S> for AgentLayer {
    type Service = AgentMiddleware<S>;

    fn layer(&self, inner: S) -> Self::Service {
        AgentMiddleware {
            inner,
            agents: self.agents.clone(),
        }
    }
}

#[derive(Clone)]
pub struct AgentMiddleware<S> {
    inner: S,
    agents: AgentService,
}

impl<S, ReqBody> Service<http::Request<ReqBody>> for AgentMiddleware<S>
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
        let agents = self.agents.clone();
        Box::pin(async move {
            let secret = req
                .headers()
                .get(REFRESH_KEY_METADATA)
                .and_then(|value| value.to_str().ok())
                .map(str::to_owned);
            if let Some(secret) = secret {
                // One retry, then say so: a key that could not be judged must not be
                // reported as a key that was judged and rejected.
                let mut outcome = resolve(&agents, &secret).await;
                if matches!(outcome, Resolved::Unavailable) {
                    outcome = resolve(&agents, &secret).await;
                }
                match outcome {
                    Resolved::Identity(identity) => {
                        req.extensions_mut().insert(identity);
                    }
                    Resolved::Anonymous => {}
                    Resolved::Unavailable => {
                        req.extensions_mut().insert(AgentAuthUnavailable);
                    }
                }
            }
            inner.call(req).await
        })
    }
}

/// Marks a request whose refresh key could not be judged because the database did not
/// answer. Distinct from carrying no identity, which means the key *was* judged and rejected.
#[derive(Debug, Clone, Copy)]
pub struct AgentAuthUnavailable;

/// What resolving a refresh key produced.
enum Resolved {
    Identity(AgentIdentity),
    /// The key was judged: unknown, or superseded by a newer registration.
    Anonymous,
    /// The key was not judged: the database failed or did not answer in time.
    Unavailable,
}

/// Resolves a refresh key under [`AUTH_TIMEOUT`].
async fn resolve(agents: &AgentService, secret: &str) -> Resolved {
    let lookup = agents.process(AuthenticateRefreshKey {
        secret: secret.to_owned(),
    });
    match tokio::time::timeout(AUTH_TIMEOUT, lookup).await {
        Ok(Ok(Some(identity))) => Resolved::Identity(identity),
        Ok(Ok(None)) => Resolved::Anonymous,
        Ok(Err(error)) => {
            tracing::warn!(%error, "resolving a refresh key failed");
            Resolved::Unavailable
        }
        Err(_) => {
            tracing::warn!("resolving a refresh key timed out");
            Resolved::Unavailable
        }
    }
}

/// Extract the authenticated worker.
///
/// `UNAUTHENTICATED` only when the key was actually judged and rejected: that is what tells
/// a worker its session is over and it must register again. A database that did not answer
/// gets `UNAVAILABLE`, which the worker retries without throwing its session away.
pub fn agent_from_request<T>(req: &tonic::Request<T>) -> Result<AgentIdentity, Status> {
    if let Some(identity) = req.extensions().get::<AgentIdentity>() {
        return Ok(identity.clone());
    }
    if req.extensions().get::<AgentAuthUnavailable>().is_some() {
        return Err(Status::unavailable("Authentication is briefly unavailable"));
    }
    Err(Status::unauthenticated("Missing refresh key"))
}

/// The worker's address as the master saw it.
///
/// Behind the documented TLS-terminating proxy the socket peer is the proxy, so
/// `x-real-ip`, else the first hop of `x-forwarded-for`, is preferred while
/// `trust_proxy_headers` is on; otherwise, and when neither header parses, the
/// socket peer is used. IPv4-mapped IPv6 peers are unmapped.
pub fn peer_address<T>(req: &tonic::Request<T>, trust_proxy_headers: bool) -> Option<IpAddr> {
    let header = |name: &str| -> Option<IpAddr> {
        let value = req.metadata().get(name)?.to_str().ok()?;
        value.split(',').next()?.trim().parse::<IpAddr>().ok()
    };
    let forwarded = trust_proxy_headers
        .then(|| header("x-real-ip").or_else(|| header("x-forwarded-for")))
        .flatten();
    forwarded
        .or_else(|| req.remote_addr().map(|addr| addr.ip()))
        .map(|ip| ip.to_canonical())
}
