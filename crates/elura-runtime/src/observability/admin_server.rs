use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;

use async_trait::async_trait;
use axum::Router;
use axum::body::Body;
use axum::extract::State;
use axum::http::{HeaderMap, HeaderValue, StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use elura_core::{Error, Result};
use serde::Serialize;
use serde_json::Value;
use subtle::ConstantTimeEq;
use tokio::sync::watch;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Readiness {
    pub ready: bool,
    pub reason: Option<String>,
}

impl Readiness {
    pub fn ready() -> Self {
        Self {
            ready: true,
            reason: None,
        }
    }

    pub fn unavailable(reason: impl Into<String>) -> Self {
        Self {
            ready: false,
            reason: Some(reason.into()),
        }
    }
}

#[async_trait]
pub trait AdminDiagnostics: Send + Sync + 'static {
    async fn readiness(&self) -> Readiness;
    async fn prometheus(&self) -> String;
    async fn stats(&self) -> Value;
    async fn backend(&self) -> Option<Value> {
        None
    }
    async fn routes(&self) -> Option<Value> {
        None
    }
}

#[async_trait]
pub trait ReadinessProbe: Send + Sync + 'static {
    async fn check(&self) -> Result<()>;
}

#[async_trait]
impl<F, Fut> ReadinessProbe for F
where
    F: Send + Sync + 'static + Fn() -> Fut,
    Fut: Send + std::future::Future<Output = Result<()>>,
{
    async fn check(&self) -> Result<()> {
        self().await
    }
}

#[derive(Debug, Clone)]
pub struct AdminServerConfig {
    pub listen: SocketAddr,
    pub token: Option<String>,
    pub component: String,
    pub instance_id: String,
}

impl AdminServerConfig {
    pub fn validate(&self) -> Result<()> {
        if self.listen.port() == 0
            || self.component.trim().is_empty()
            || self.instance_id.trim().is_empty()
        {
            return Err(Error::InvalidConfig(
                "admin listen, component and instance ID are required".into(),
            ));
        }
        if !self.listen.ip().is_loopback() && self.token.as_deref().is_none_or(str::is_empty) {
            return Err(Error::InvalidConfig(
                "admin token is required for a non-loopback listener".into(),
            ));
        }
        if self.token.as_ref().is_some_and(|token| token.len() < 32) {
            return Err(Error::InvalidConfig(
                "admin token must contain at least 32 bytes".into(),
            ));
        }
        Ok(())
    }

    pub fn loopback(
        port: u16,
        component: impl Into<String>,
        instance_id: impl Into<String>,
    ) -> Self {
        Self {
            listen: SocketAddr::new(IpAddr::V4(std::net::Ipv4Addr::LOCALHOST), port),
            token: None,
            component: component.into(),
            instance_id: instance_id.into(),
        }
    }
}

pub struct AdminServer {
    config: AdminServerConfig,
    diagnostics: Arc<dyn AdminDiagnostics>,
}

#[derive(Clone)]
struct AdminState {
    config: AdminServerConfig,
    diagnostics: Arc<dyn AdminDiagnostics>,
}

impl AdminServer {
    pub fn new(config: AdminServerConfig, diagnostics: Arc<dyn AdminDiagnostics>) -> Result<Self> {
        config.validate()?;
        Ok(Self {
            config,
            diagnostics,
        })
    }

    pub async fn serve(&self, mut shutdown: watch::Receiver<bool>) -> Result<()> {
        let state = AdminState {
            config: self.config.clone(),
            diagnostics: self.diagnostics.clone(),
        };
        let app = Router::new()
            .route("/elura/healthz", get(health))
            .route("/elura/readyz", get(readiness))
            .route("/elura/version", get(version))
            .route("/elura/metrics", get(metrics))
            .route("/elura/debug/stats", get(stats))
            .route("/elura/debug/backend", get(backend))
            .route("/elura/debug/routes", get(routes))
            .with_state(state);
        let listener = tokio::net::TcpListener::bind(self.config.listen).await?;
        axum::serve(listener, app)
            .with_graceful_shutdown(async move {
                if *shutdown.borrow() {
                    return;
                }
                while shutdown.changed().await.is_ok() {
                    if *shutdown.borrow() {
                        return;
                    }
                }
            })
            .await
            .map_err(std::io::Error::other)?;
        Ok(())
    }
}

async fn health() -> StatusCode {
    StatusCode::NO_CONTENT
}

async fn readiness(State(state): State<AdminState>) -> Response {
    let readiness = state.diagnostics.readiness().await;
    if readiness.ready {
        StatusCode::NO_CONTENT.into_response()
    } else {
        (
            StatusCode::SERVICE_UNAVAILABLE,
            readiness.reason.unwrap_or_else(|| "not ready".into()),
        )
            .into_response()
    }
}

#[derive(Serialize)]
struct Version<'a> {
    version: &'static str,
    runtime: &'static str,
    component: &'a str,
    instance_id: &'a str,
}

async fn version(State(state): State<AdminState>) -> Response {
    no_store_json(&Version {
        version: env!("CARGO_PKG_VERSION"),
        runtime: "rust",
        component: &state.config.component,
        instance_id: &state.config.instance_id,
    })
}

async fn metrics(State(state): State<AdminState>, headers: HeaderMap) -> Response {
    if let Some(response) = authorize(&state.config, &headers) {
        return response;
    }
    let mut response = Response::new(Body::from(state.diagnostics.prometheus().await));
    response.headers_mut().insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("text/plain; version=0.0.4; charset=utf-8"),
    );
    response
        .headers_mut()
        .insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
    response
}

async fn stats(State(state): State<AdminState>, headers: HeaderMap) -> Response {
    if let Some(response) = authorize(&state.config, &headers) {
        return response;
    }
    no_store_json(&state.diagnostics.stats().await)
}

async fn backend(State(state): State<AdminState>, headers: HeaderMap) -> Response {
    if let Some(response) = authorize(&state.config, &headers) {
        return response;
    }
    state.diagnostics.backend().await.map_or_else(
        || StatusCode::NOT_FOUND.into_response(),
        |value| no_store_json(&value),
    )
}

async fn routes(State(state): State<AdminState>, headers: HeaderMap) -> Response {
    if let Some(response) = authorize(&state.config, &headers) {
        return response;
    }
    state.diagnostics.routes().await.map_or_else(
        || StatusCode::NOT_FOUND.into_response(),
        |value| no_store_json(&value),
    )
}

fn authorize(config: &AdminServerConfig, headers: &HeaderMap) -> Option<Response> {
    let expected = config.token.as_deref()?;
    let provided = headers
        .get(header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.strip_prefix("Bearer "))
        .unwrap_or_default();
    if provided.as_bytes().ct_eq(expected.as_bytes()).into() {
        return None;
    }
    let mut response = (StatusCode::UNAUTHORIZED, "unauthorized").into_response();
    response
        .headers_mut()
        .insert(header::WWW_AUTHENTICATE, "Bearer".parse().unwrap());
    Some(response)
}

fn no_store_json(value: &impl Serialize) -> Response {
    match serde_json::to_vec(value) {
        Ok(body) => {
            let mut response = Response::new(Body::from(body));
            response.headers_mut().insert(
                header::CONTENT_TYPE,
                HeaderValue::from_static("application/json"),
            );
            response
                .headers_mut()
                .insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
            response
        }
        Err(_) => StatusCode::INTERNAL_SERVER_ERROR.into_response(),
    }
}
