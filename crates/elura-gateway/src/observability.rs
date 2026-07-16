use std::net::IpAddr;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use axum::Router;
use axum::body::Body;
use axum::extract::{Json, Path, State};
use axum::http::{HeaderMap, HeaderValue, StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::routing::{delete, get, post, put};
use elura_core::session::Identity;
use elura_core::{Error, Result};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use subtle::ConstantTimeEq;
use tokio::sync::watch;

use crate::GatewayServer;
use crate::protection::CircuitState;
pub use elura_runtime::observability::{
    AdminServerConfig, OpenTelemetryLayer, PrometheusText, ReadinessProbe, ensure_trace_id,
    new_trace_id, open_telemetry_layer,
};

/// Mutations for a Gateway's admission policy.
///
/// The runtime deliberately owns this small interface rather than depending on
/// a concrete persistence adapter.  For example, the Redis admission adapter
/// implements it while applications can provide their own policy store.
#[async_trait]
pub trait AdmissionAdmin: Send + Sync + 'static {
    async fn ban_ip(&self, ip: IpAddr, ttl: Duration, reason: &str) -> Result<()>;
    async fn unban_ip(&self, ip: IpAddr) -> Result<()>;
    async fn ban_user(&self, identity: &Identity, ttl: Duration, reason: &str) -> Result<()>;
    async fn unban_user(&self, identity: &Identity) -> Result<()>;
    async fn set_maintenance(&self, ttl: Duration, reason: &str) -> Result<()>;
    async fn clear_maintenance(&self) -> Result<()>;
}

/// Gateway controls exposed by the private administration server.
#[derive(Clone)]
pub struct GatewayAdmin {
    gateway: Arc<GatewayServer>,
    admission: Option<Arc<dyn AdmissionAdmin>>,
}

impl GatewayAdmin {
    pub fn new(gateway: Arc<GatewayServer>) -> Self {
        Self {
            gateway,
            admission: None,
        }
    }

    pub fn with_admission(mut self, admission: Arc<dyn AdmissionAdmin>) -> Self {
        self.admission = Some(admission);
        self
    }
}

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

pub struct AdminServer {
    config: AdminServerConfig,
    diagnostics: Arc<dyn AdminDiagnostics>,
    gateway_admin: Option<GatewayAdmin>,
}

#[derive(Clone)]
struct AdminState {
    config: AdminServerConfig,
    diagnostics: Arc<dyn AdminDiagnostics>,
    gateway_admin: Option<GatewayAdmin>,
}

impl AdminServer {
    pub fn new(config: AdminServerConfig, diagnostics: Arc<dyn AdminDiagnostics>) -> Result<Self> {
        config.validate()?;
        Ok(Self {
            config,
            diagnostics,
            gateway_admin: None,
        })
    }

    /// Adds Gateway session and admission-policy controls to this server.
    ///
    /// These routes are intentionally unavailable unless the application
    /// explicitly attaches a Gateway.  The admission-policy routes additionally
    /// require an [`AdmissionAdmin`] implementation.
    pub fn with_gateway_admin(mut self, gateway_admin: GatewayAdmin) -> Self {
        self.gateway_admin = Some(gateway_admin);
        self
    }

    #[doc(hidden)]
    pub fn with_diagnostics(mut self, diagnostics: Arc<dyn AdminDiagnostics>) -> Self {
        self.diagnostics = diagnostics;
        self
    }

    pub async fn serve(&self, mut shutdown: watch::Receiver<bool>) -> Result<()> {
        let state = AdminState {
            config: self.config.clone(),
            diagnostics: self.diagnostics.clone(),
            gateway_admin: self.gateway_admin.clone(),
        };
        let app = Router::new()
            .route("/elura/healthz", get(health))
            .route("/elura/readyz", get(readiness))
            .route("/elura/version", get(version))
            .route("/elura/metrics", get(metrics))
            .route("/elura/debug/stats", get(stats))
            .route("/elura/debug/backend", get(backend))
            .route("/elura/debug/routes", get(routes))
            .route("/elura/admin/sessions/force-logout", post(force_logout))
            .route(
                "/elura/admin/sessions/revoke-account-version",
                post(revoke_account_version),
            )
            .route("/elura/admin/admission/user-bans", put(ban_user))
            .route(
                "/elura/admin/admission/user-bans/{region_id}/{realm_id}/{user_id}",
                delete(unban_user),
            )
            .route(
                "/elura/admin/admission/ip-bans/{ip}",
                put(ban_ip).delete(unban_ip),
            )
            .route(
                "/elura/admin/admission/maintenance",
                put(set_maintenance).delete(clear_maintenance),
            )
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

#[derive(Deserialize)]
struct SessionRequest {
    region_id: u32,
    realm_id: u32,
    user_id: i64,
    reason: String,
}

#[derive(Deserialize)]
struct AccountVersionRequest {
    region_id: u32,
    realm_id: u32,
    user_id: i64,
    minimum_generation: u64,
    reason: String,
}

#[derive(Deserialize)]
struct TimedRequest {
    ttl_ms: u64,
    reason: String,
}

#[derive(Deserialize)]
struct UserBanRequest {
    region_id: u32,
    realm_id: u32,
    user_id: i64,
    ttl_ms: u64,
    reason: String,
}

#[derive(Serialize)]
struct Delivered {
    delivered: usize,
}

async fn force_logout(
    State(state): State<AdminState>,
    headers: HeaderMap,
    Json(request): Json<SessionRequest>,
) -> Response {
    if let Some(response) = authorize(&state.config, &headers) {
        return response;
    }
    let Some(gateway_admin) = &state.gateway_admin else {
        return StatusCode::NOT_FOUND.into_response();
    };
    match gateway_admin
        .gateway
        .force_logout(
            request.region_id,
            request.realm_id,
            request.user_id,
            &request.reason,
        )
        .await
    {
        Ok(delivered) => no_store_json(&Delivered { delivered }),
        Err(error) => admin_error(error),
    }
}

async fn revoke_account_version(
    State(state): State<AdminState>,
    headers: HeaderMap,
    Json(request): Json<AccountVersionRequest>,
) -> Response {
    if let Some(response) = authorize(&state.config, &headers) {
        return response;
    }
    let Some(gateway_admin) = &state.gateway_admin else {
        return StatusCode::NOT_FOUND.into_response();
    };
    match gateway_admin
        .gateway
        .revoke_account_version(
            request.region_id,
            request.realm_id,
            request.user_id,
            request.minimum_generation,
            &request.reason,
        )
        .await
    {
        Ok(delivered) => no_store_json(&Delivered { delivered }),
        Err(error) => admin_error(error),
    }
}

async fn ban_user(
    State(state): State<AdminState>,
    headers: HeaderMap,
    Json(request): Json<UserBanRequest>,
) -> Response {
    if let Some(response) = authorize(&state.config, &headers) {
        return response;
    }
    let Some(admission) = admission_admin(&state) else {
        return StatusCode::NOT_FOUND.into_response();
    };
    let identity = Identity {
        account_id: request.user_id,
        region_id: request.region_id,
        realm_id: request.realm_id,
        user_id: request.user_id,
        generation: 1,
    };
    let result = match duration(request.ttl_ms) {
        Ok(ttl) => admission.ban_user(&identity, ttl, &request.reason).await,
        Err(error) => Err(error),
    };
    match result {
        Ok(()) => StatusCode::NO_CONTENT.into_response(),
        Err(error) => admin_error(error),
    }
}

async fn unban_user(
    State(state): State<AdminState>,
    headers: HeaderMap,
    Path((region_id, realm_id, user_id)): Path<(u32, u32, i64)>,
) -> Response {
    if let Some(response) = authorize(&state.config, &headers) {
        return response;
    }
    let Some(admission) = admission_admin(&state) else {
        return StatusCode::NOT_FOUND.into_response();
    };
    let identity = Identity {
        account_id: user_id,
        region_id,
        realm_id,
        user_id,
        generation: 1,
    };
    match admission.unban_user(&identity).await {
        Ok(()) => StatusCode::NO_CONTENT.into_response(),
        Err(error) => admin_error(error),
    }
}

async fn ban_ip(
    State(state): State<AdminState>,
    headers: HeaderMap,
    Path(ip): Path<IpAddr>,
    Json(request): Json<TimedRequest>,
) -> Response {
    if let Some(response) = authorize(&state.config, &headers) {
        return response;
    }
    let Some(admission) = admission_admin(&state) else {
        return StatusCode::NOT_FOUND.into_response();
    };
    let result = match duration(request.ttl_ms) {
        Ok(ttl) => admission.ban_ip(ip, ttl, &request.reason).await,
        Err(error) => Err(error),
    };
    match result {
        Ok(()) => StatusCode::NO_CONTENT.into_response(),
        Err(error) => admin_error(error),
    }
}

async fn unban_ip(
    State(state): State<AdminState>,
    headers: HeaderMap,
    Path(ip): Path<IpAddr>,
) -> Response {
    if let Some(response) = authorize(&state.config, &headers) {
        return response;
    }
    let Some(admission) = admission_admin(&state) else {
        return StatusCode::NOT_FOUND.into_response();
    };
    match admission.unban_ip(ip).await {
        Ok(()) => StatusCode::NO_CONTENT.into_response(),
        Err(error) => admin_error(error),
    }
}

async fn set_maintenance(
    State(state): State<AdminState>,
    headers: HeaderMap,
    Json(request): Json<TimedRequest>,
) -> Response {
    if let Some(response) = authorize(&state.config, &headers) {
        return response;
    }
    let Some(admission) = admission_admin(&state) else {
        return StatusCode::NOT_FOUND.into_response();
    };
    let result = match duration(request.ttl_ms) {
        Ok(ttl) => admission.set_maintenance(ttl, &request.reason).await,
        Err(error) => Err(error),
    };
    match result {
        Ok(()) => StatusCode::NO_CONTENT.into_response(),
        Err(error) => admin_error(error),
    }
}

async fn clear_maintenance(State(state): State<AdminState>, headers: HeaderMap) -> Response {
    if let Some(response) = authorize(&state.config, &headers) {
        return response;
    }
    let Some(admission) = admission_admin(&state) else {
        return StatusCode::NOT_FOUND.into_response();
    };
    match admission.clear_maintenance().await {
        Ok(()) => StatusCode::NO_CONTENT.into_response(),
        Err(error) => admin_error(error),
    }
}

fn admission_admin(state: &AdminState) -> Option<&Arc<dyn AdmissionAdmin>> {
    state.gateway_admin.as_ref()?.admission.as_ref()
}

fn duration(milliseconds: u64) -> Result<Duration> {
    if milliseconds == 0 {
        return Err(Error::InvalidConfig("ttl_ms must be positive".into()));
    }
    Ok(Duration::from_millis(milliseconds))
}

fn admin_error(error: Error) -> Response {
    let status = match error {
        Error::InvalidConfig(_) | Error::InvalidFrame(_) | Error::Serialization(_) => {
            StatusCode::BAD_REQUEST
        }
        Error::Unavailable | Error::Timeout | Error::QueueFull => StatusCode::SERVICE_UNAVAILABLE,
        _ => StatusCode::INTERNAL_SERVER_ERROR,
    };
    status.into_response()
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

#[async_trait]
impl AdminDiagnostics for GatewayServer {
    async fn readiness(&self) -> Readiness {
        self.readiness().await
    }

    async fn prometheus(&self) -> String {
        let stats = self.stats();
        let mut metrics = PrometheusText::default();
        metrics
            .gauge_float(
                "elura_gateway_uptime_seconds",
                "Gateway process uptime in seconds.",
                stats.uptime_millis as f64 / 1000.0,
            )
            .counter(
                "elura_gateway_connections_total",
                "Client connections accepted.",
                stats.connections,
            )
            .gauge(
                "elura_gateway_connections_active",
                "Client connections currently active.",
                stats.active_connections,
            )
            .gauge(
                "elura_gateway_sessions_authenticated",
                "Authenticated Sessions currently active.",
                stats.authenticated_sessions,
            )
            .counter(
                "elura_gateway_requests_total",
                "Client requests received.",
                stats.requests,
            )
            .counter(
                "elura_gateway_requests_rejected_total",
                "Client requests or connections rejected.",
                stats.rejected,
            )
            .counter(
                "elura_gateway_failures_total",
                "Gateway request failures.",
                stats.failures,
            )
            .counter(
                "elura_gateway_pushes_total",
                "Push delivery attempts.",
                stats.pushes,
            )
            .counter(
                "elura_gateway_push_failures_total",
                "Failed Push delivery attempts.",
                stats.push_failures,
            );
        if let Some(backend) = self.protection_stats().await {
            metrics
                .gauge(
                    "elura_gateway_world_commands_active",
                    "Active Gateway-to-World commands.",
                    backend.active,
                )
                .gauge(
                    "elura_gateway_world_circuit_open",
                    "Whether the World circuit is open.",
                    i64::from(backend.circuit == CircuitState::Open),
                )
                .gauge(
                    "elura_gateway_world_circuit_half_open",
                    "Whether the World circuit is half open.",
                    i64::from(backend.circuit == CircuitState::HalfOpen),
                )
                .counter(
                    "elura_gateway_world_commands_accepted_total",
                    "Gateway-to-World commands admitted.",
                    backend.accepted,
                )
                .counter(
                    "elura_gateway_world_commands_overloaded_total",
                    "Commands rejected by concurrency protection.",
                    backend.rejected_concurrency,
                )
                .counter(
                    "elura_gateway_world_commands_circuit_rejected_total",
                    "Commands rejected by the World circuit.",
                    backend.rejected_circuit,
                )
                .counter(
                    "elura_gateway_world_transient_failures_total",
                    "Transient Gateway-to-World failures.",
                    backend.transient_failures,
                )
                .counter(
                    "elura_gateway_world_circuit_opened_total",
                    "World circuit breaker openings.",
                    backend.opened,
                );
        }
        metrics.finish()
    }

    async fn stats(&self) -> Value {
        serde_json::to_value(self.stats()).unwrap_or(Value::Null)
    }

    async fn backend(&self) -> Option<Value> {
        let stats = self.protection_stats().await?;
        Some(serde_json::json!({
            "active": stats.active,
            "accepted": stats.accepted,
            "rejected_concurrency": stats.rejected_concurrency,
            "rejected_circuit": stats.rejected_circuit,
            "transient_failures": stats.transient_failures,
            "opened": stats.opened,
            "circuit": match stats.circuit { CircuitState::Closed => "closed", CircuitState::Open => "open", CircuitState::HalfOpen => "half_open" },
        }))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use super::*;
    use bytes::Bytes;
    use elura_core::ticket::{MemoryReplayStore, TicketService};

    use crate::{GatewayConfig, WorldClient, WorldRequest};

    struct NeverWorld;

    #[async_trait]
    impl WorldClient for NeverWorld {
        async fn command(&self, _request: WorldRequest) -> Result<Bytes> {
            Err(Error::Unavailable)
        }
    }

    #[derive(Default)]
    struct RecordingAdmission {
        calls: Mutex<Vec<String>>,
    }

    #[async_trait]
    impl AdmissionAdmin for RecordingAdmission {
        async fn ban_ip(&self, ip: IpAddr, _ttl: Duration, _reason: &str) -> Result<()> {
            self.calls.lock().unwrap().push(format!("ban_ip:{ip}"));
            Ok(())
        }

        async fn unban_ip(&self, ip: IpAddr) -> Result<()> {
            self.calls.lock().unwrap().push(format!("unban_ip:{ip}"));
            Ok(())
        }

        async fn ban_user(&self, identity: &Identity, _ttl: Duration, _reason: &str) -> Result<()> {
            self.calls.lock().unwrap().push(format!(
                "ban_user:{}:{}:{}",
                identity.region_id, identity.realm_id, identity.user_id
            ));
            Ok(())
        }

        async fn unban_user(&self, identity: &Identity) -> Result<()> {
            self.calls.lock().unwrap().push(format!(
                "unban_user:{}:{}:{}",
                identity.region_id, identity.realm_id, identity.user_id
            ));
            Ok(())
        }

        async fn set_maintenance(&self, _ttl: Duration, _reason: &str) -> Result<()> {
            self.calls.lock().unwrap().push("maintenance".into());
            Ok(())
        }

        async fn clear_maintenance(&self) -> Result<()> {
            self.calls.lock().unwrap().push("clear_maintenance".into());
            Ok(())
        }
    }

    fn state(admission: Arc<RecordingAdmission>) -> AdminState {
        let tickets = Arc::new(
            TicketService::new(
                [7_u8; 32],
                "admin-test-auth",
                "admin-test-gateway",
                Duration::from_secs(60),
            )
            .unwrap(),
        );
        let gateway = Arc::new(
            GatewayServer::new(
                GatewayConfig::default(),
                tickets,
                Arc::new(MemoryReplayStore::default()),
                Arc::new(NeverWorld),
            )
            .unwrap(),
        );
        let mut config =
            AdminServerConfig::new("127.0.0.1:9000".parse().unwrap(), "gateway", "test");
        config.token = Some("a-32-byte-administration-token-xx".into());
        AdminState {
            config,
            diagnostics: gateway.clone(),
            gateway_admin: Some(GatewayAdmin::new(gateway).with_admission(admission)),
        }
    }

    fn authorized_headers() -> HeaderMap {
        let mut headers = HeaderMap::new();
        headers.insert(
            header::AUTHORIZATION,
            "Bearer a-32-byte-administration-token-xx".parse().unwrap(),
        );
        headers
    }

    #[test]
    fn requires_token_on_public_listener() {
        let config = AdminServerConfig::new("0.0.0.0:9000".parse().unwrap(), "gateway", "a");
        assert!(config.validate().is_err());
    }

    #[tokio::test]
    async fn user_ban_route_requires_authentication_and_calls_policy() {
        let admission = Arc::new(RecordingAdmission::default());
        let request = UserBanRequest {
            region_id: 1,
            realm_id: 2,
            user_id: 3,
            ttl_ms: 60_000,
            reason: "abuse".into(),
        };
        let unauthorized = ban_user(
            State(state(admission.clone())),
            HeaderMap::new(),
            Json(request),
        )
        .await;
        assert_eq!(unauthorized.status(), StatusCode::UNAUTHORIZED);
        assert!(admission.calls.lock().unwrap().is_empty());

        let response = ban_user(
            State(state(admission.clone())),
            authorized_headers(),
            Json(UserBanRequest {
                region_id: 1,
                realm_id: 2,
                user_id: 3,
                ttl_ms: 60_000,
                reason: "abuse".into(),
            }),
        )
        .await;
        assert_eq!(response.status(), StatusCode::NO_CONTENT);
        assert_eq!(
            admission.calls.lock().unwrap().as_slice(),
            ["ban_user:1:2:3"]
        );
    }

    #[tokio::test]
    async fn force_logout_route_returns_delivery_count() {
        let response = force_logout(
            State(state(Arc::new(RecordingAdmission::default()))),
            authorized_headers(),
            Json(SessionRequest {
                region_id: 1,
                realm_id: 2,
                user_id: 3,
                reason: "operator request".into(),
            }),
        )
        .await;
        assert_eq!(response.status(), StatusCode::OK);
    }
}
