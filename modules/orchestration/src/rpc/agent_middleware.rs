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
                let lookup = agents.process(AuthenticateRefreshKey { secret });
                match tokio::time::timeout(AUTH_TIMEOUT, lookup).await {
                    Ok(Ok(Some(identity))) => {
                        req.extensions_mut().insert(identity);
                    }
                    Ok(_) => {}
                    Err(_) => tracing::warn!(
                        "resolving a refresh key timed out; the request proceeds anonymous"
                    ),
                }
            }
            inner.call(req).await
        })
    }
}

/// Extract the authenticated worker, or fail with `UNAUTHENTICATED`.
pub fn agent_from_request<T>(req: &tonic::Request<T>) -> Result<AgentIdentity, Status> {
    req.extensions()
        .get::<AgentIdentity>()
        .cloned()
        .ok_or_else(|| Status::unauthenticated("Missing refresh key"))
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
