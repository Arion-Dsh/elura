//! HTTP login, refresh, bearer authentication, and Gateway-ticket exchange.
//!
//! This module complements the stateful ELR2 protocol. HTTP access tokens are
//! validated independently on every request, while the one-time Gateway ticket
//! returned here continues through the existing ELR2 authentication route.

use std::sync::Arc;

use async_trait::async_trait;
use axum::Json;
use axum::Router;
use axum::extract::{DefaultBodyLimit, FromRequestParts, Request, State};
use axum::http::request::Parts;
use axum::http::{HeaderMap, StatusCode, header};
use axum::middleware::{self, Next};
use axum::response::{IntoResponse, Response};
use axum::routing::post;
use elura_core::http_auth::{HttpTokenClaims, HttpTokenPair, HttpTokenService};
use elura_core::identity::Principal;
use elura_core::session::Identity;
use elura_core::ticket::{ReplayStore, TicketService};
use elura_core::{Error, Result};
use serde::{Deserialize, Serialize};
use serde_json::Value;

/// [`IdentityService`](elura_providers::identity::IdentityService) integration.
#[cfg(feature = "identity-http")]
pub mod identity;

/// Scope required to exchange an HTTP login for a Gateway Session ticket.
pub const GAME_CONNECT_SCOPE: &str = "game:connect";

/// Successful application login before framework tokens are issued.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HttpLoginGrant {
    /// Authenticated application account.
    pub principal: Principal,
    /// HTTP permissions granted to this login.
    pub scopes: Vec<String>,
}

impl HttpLoginGrant {
    /// Creates a login grant.
    pub fn new(principal: Principal, scopes: impl IntoIterator<Item = impl Into<String>>) -> Self {
        Self {
            principal,
            scopes: scopes.into_iter().map(Into::into).collect(),
        }
    }
}

/// Application-owned login and game-identity resolution boundary.
#[async_trait]
pub trait HttpLoginBackend: Send + Sync + 'static {
    /// Authenticates a provider-specific JSON credential.
    async fn login(&self, provider: &str, credential: Value) -> Result<HttpLoginGrant>;

    /// Resolves and authorizes a selected player for an authenticated account.
    ///
    /// Implementations must verify that the requested player belongs to
    /// `principal` and may enter the requested region and realm.
    async fn game_identity(
        &self,
        principal: Principal,
        request: &GameSessionTicketRequest,
    ) -> Result<Identity>;
}

/// Provider credential submitted to the common HTTP login endpoint.
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HttpLoginRequest {
    /// Registered provider name, such as `password`, `phone`, or `wechat`.
    pub provider: String,
    /// Provider-specific credential object.
    pub credential: Value,
    /// Optional player selection for issuing the first Gateway ticket inline.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub game: Option<GameSessionTicketRequest>,
}

/// Player selection used to request a one-time Gateway login ticket.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GameSessionTicketRequest {
    /// Selected player ID.
    pub user_id: i64,
    /// Selected region.
    pub region_id: u32,
    /// Selected realm.
    pub realm_id: u32,
}

impl GameSessionTicketRequest {
    fn validate(&self) -> Result<()> {
        if self.user_id <= 0 || self.region_id == 0 || self.realm_id == 0 {
            return Err(Error::Authentication);
        }
        Ok(())
    }
}

/// One-time ticket accepted by the ELR2 authentication route.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GatewaySessionTicket {
    /// Signed one-time Gateway login ticket.
    pub ticket: String,
    /// Ticket lifetime in seconds.
    pub expires_in_seconds: u64,
}

/// Response returned after one successful user login.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HttpLoginResponse {
    /// Authenticated account ID.
    pub account_id: i64,
    /// Account generation captured by the login.
    pub generation: u64,
    /// HTTP access and refresh credentials.
    #[serde(flatten)]
    pub tokens: HttpTokenPair,
    /// Initial Gateway ticket when the login included a player selection.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub gateway: Option<GatewaySessionTicket>,
}

/// Single-use refresh token submitted for rotation.
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HttpRefreshRequest {
    /// Refresh token returned by login or the previous refresh.
    pub refresh_token: String,
}

/// Authenticated HTTP identity inserted by [`require_bearer`].
#[derive(Debug, Clone)]
pub struct AuthenticatedHttp {
    claims: HttpTokenClaims,
}

impl AuthenticatedHttp {
    /// Returns the verified token claims.
    pub const fn claims(&self) -> &HttpTokenClaims {
        &self.claims
    }

    /// Returns the authenticated account.
    pub const fn principal(&self) -> Principal {
        self.claims.principal
    }

    /// Returns whether the access token grants `scope`.
    pub fn has_scope(&self, scope: &str) -> bool {
        self.claims.has_scope(scope)
    }

    /// Requires one scope for a business handler.
    pub fn require_scope(&self, scope: &str) -> std::result::Result<(), HttpAuthRejection> {
        if self.has_scope(scope) {
            Ok(())
        } else {
            Err(HttpAuthRejection::forbidden())
        }
    }
}

impl<S> FromRequestParts<S> for AuthenticatedHttp
where
    S: Send + Sync,
{
    type Rejection = HttpAuthRejection;

    async fn from_request_parts(
        parts: &mut Parts,
        _state: &S,
    ) -> std::result::Result<Self, Self::Rejection> {
        parts
            .extensions
            .get::<HttpTokenClaims>()
            .cloned()
            .map(|claims| Self { claims })
            .ok_or_else(HttpAuthRejection::unauthenticated)
    }
}

/// Rejection returned by HTTP bearer authentication and scope checks.
#[derive(Debug)]
pub struct HttpAuthRejection {
    status: StatusCode,
    code: &'static str,
    message: &'static str,
}

impl HttpAuthRejection {
    fn unauthenticated() -> Self {
        Self {
            status: StatusCode::UNAUTHORIZED,
            code: "UNAUTHENTICATED",
            message: "authentication failed",
        }
    }

    fn forbidden() -> Self {
        Self {
            status: StatusCode::FORBIDDEN,
            code: "FORBIDDEN",
            message: "permission denied",
        }
    }
}

impl IntoResponse for HttpAuthRejection {
    fn into_response(self) -> Response {
        auth_error_response(self.status, self.code, self.message)
    }
}

/// State passed to [`require_bearer`] for protecting arbitrary Axum routes.
#[derive(Clone)]
pub struct HttpBearerAuth {
    tokens: Arc<HttpTokenService>,
}

impl HttpBearerAuth {
    /// Creates bearer authentication from an HTTP token verifier.
    pub fn new(tokens: Arc<HttpTokenService>) -> Self {
        Self { tokens }
    }

    /// Verifies the bearer credential in `headers`.
    pub fn authenticate(
        &self,
        headers: &HeaderMap,
    ) -> std::result::Result<HttpTokenClaims, HttpAuthRejection> {
        let value = headers
            .get(header::AUTHORIZATION)
            .and_then(|value| value.to_str().ok())
            .ok_or_else(HttpAuthRejection::unauthenticated)?;
        let (scheme, token) = value
            .split_once(' ')
            .ok_or_else(HttpAuthRejection::unauthenticated)?;
        if !scheme.eq_ignore_ascii_case("bearer")
            || token.is_empty()
            || token.bytes().any(|byte| byte.is_ascii_whitespace())
        {
            return Err(HttpAuthRejection::unauthenticated());
        }
        self.tokens
            .verify_access(token)
            .map_err(|_| HttpAuthRejection::unauthenticated())
    }
}

/// Axum middleware that verifies an HTTP access token and inserts its claims.
pub async fn require_bearer(
    State(auth): State<HttpBearerAuth>,
    mut request: Request,
    next: Next,
) -> Response {
    match auth.authenticate(request.headers()) {
        Ok(claims) => {
            request.extensions_mut().insert(claims);
            next.run(request).await
        }
        Err(rejection) => rejection.into_response(),
    }
}

struct HttpAuthApiInner {
    tokens: Arc<HttpTokenService>,
    tickets: Arc<TicketService>,
    replay: Arc<dyn ReplayStore>,
    backend: Arc<dyn HttpLoginBackend>,
}

/// Ready-to-mount HTTP authentication and Gateway-ticket API.
///
/// Routes:
///
/// - `POST /elura/auth/login`
/// - `POST /elura/auth/refresh`
/// - `POST /elura/auth/session-ticket`
#[derive(Clone)]
pub struct HttpAuthApi {
    inner: Arc<HttpAuthApiInner>,
}

impl HttpAuthApi {
    /// Creates the API from application login logic and shared token services.
    pub fn new(
        tokens: Arc<HttpTokenService>,
        tickets: Arc<TicketService>,
        replay: Arc<dyn ReplayStore>,
        backend: Arc<dyn HttpLoginBackend>,
    ) -> Self {
        Self {
            inner: Arc::new(HttpAuthApiInner {
                tokens,
                tickets,
                replay,
                backend,
            }),
        }
    }

    /// Returns middleware state for protecting application-owned HTTP routes.
    pub fn bearer_auth(&self) -> HttpBearerAuth {
        HttpBearerAuth::new(self.inner.tokens.clone())
    }

    /// Builds an Axum router containing the authentication endpoints.
    pub fn router(&self) -> Router {
        let protected = Router::new()
            .route("/elura/auth/session-ticket", post(issue_session_ticket))
            .route_layer(middleware::from_fn_with_state(
                self.bearer_auth(),
                require_bearer,
            ));
        Router::new()
            .route("/elura/auth/login", post(login))
            .route("/elura/auth/refresh", post(refresh))
            .merge(protected)
            .layer(DefaultBodyLimit::max(64 * 1024))
            .with_state(self.inner.clone())
    }
}

async fn login(
    State(state): State<Arc<HttpAuthApiInner>>,
    Json(request): Json<HttpLoginRequest>,
) -> Response {
    let provider = request.provider.trim();
    if provider.is_empty() || provider.len() > 32 {
        return api_error(&Error::Authentication);
    }
    let grant = match state.backend.login(provider, request.credential).await {
        Ok(grant) => grant,
        Err(error) => return api_error(&error),
    };
    if grant.principal.validate().is_err() {
        return api_error(&Error::Authentication);
    }
    let tokens = match state
        .tokens
        .issue(grant.principal, grant.scopes.iter().cloned())
    {
        Ok(tokens) => tokens,
        Err(error) => return api_error(&error),
    };
    let gateway = match request.game {
        Some(selection) => {
            let claims = match state.tokens.verify_access(&tokens.access_token) {
                Ok(claims) => claims,
                Err(error) => return api_error(&error),
            };
            match issue_gateway_ticket(&state, &claims, &selection).await {
                Ok(ticket) => Some(ticket),
                Err(error) => return api_error(&error),
            }
        }
        None => None,
    };
    (
        StatusCode::OK,
        Json(HttpLoginResponse {
            account_id: grant.principal.account_id,
            generation: grant.principal.generation,
            tokens,
            gateway,
        }),
    )
        .into_response()
}

async fn refresh(
    State(state): State<Arc<HttpAuthApiInner>>,
    Json(request): Json<HttpRefreshRequest>,
) -> Response {
    if request.refresh_token.is_empty() {
        return api_error(&Error::Authentication);
    }
    match state
        .tokens
        .rotate_refresh(&request.refresh_token, state.replay.as_ref())
        .await
    {
        Ok(tokens) => (StatusCode::OK, Json(tokens)).into_response(),
        Err(error) => api_error(&error),
    }
}

async fn issue_session_ticket(
    State(state): State<Arc<HttpAuthApiInner>>,
    authenticated: AuthenticatedHttp,
    Json(request): Json<GameSessionTicketRequest>,
) -> Response {
    match issue_gateway_ticket(&state, authenticated.claims(), &request).await {
        Ok(ticket) => (StatusCode::OK, Json(ticket)).into_response(),
        Err(error) => api_error(&error),
    }
}

async fn issue_gateway_ticket(
    state: &HttpAuthApiInner,
    claims: &HttpTokenClaims,
    request: &GameSessionTicketRequest,
) -> Result<GatewaySessionTicket> {
    request.validate()?;
    if !claims.has_scope(GAME_CONNECT_SCOPE) {
        return Err(Error::Business {
            code: "forbidden".into(),
            message: "permission denied".into(),
            retryable: false,
        });
    }
    let identity = state
        .backend
        .game_identity(claims.principal, request)
        .await?;
    identity.validate()?;
    if identity.account_id != claims.principal.account_id
        || identity.generation != claims.principal.generation
        || identity.user_id != request.user_id
        || identity.region_id != request.region_id
        || identity.realm_id != request.realm_id
    {
        return Err(Error::Authentication);
    }
    Ok(GatewaySessionTicket {
        ticket: state.tickets.issue_login(identity)?,
        expires_in_seconds: state.tickets.login_ttl().as_secs(),
    })
}

/// Stable JSON error returned by the built-in HTTP authentication endpoints.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HttpAuthErrorResponse {
    /// Stable machine-readable error code.
    pub code: String,
    /// Safe client-facing description.
    pub message: String,
    /// Whether retrying later may succeed without changing credentials.
    pub retryable: bool,
}

fn api_error(error: &Error) -> Response {
    match error {
        Error::Authentication
        | Error::TicketExpired
        | Error::TicketReplayed
        | Error::SessionRevoked => auth_error_response(
            StatusCode::UNAUTHORIZED,
            "UNAUTHENTICATED",
            "authentication failed",
        ),
        Error::RateLimited => (
            StatusCode::TOO_MANY_REQUESTS,
            Json(HttpAuthErrorResponse {
                code: "RATE_LIMITED".into(),
                message: "request rate exceeded".into(),
                retryable: true,
            }),
        )
            .into_response(),
        Error::Business {
            code,
            message,
            retryable,
        } => (
            StatusCode::FORBIDDEN,
            Json(HttpAuthErrorResponse {
                code: code.clone(),
                message: message.clone(),
                retryable: *retryable,
            }),
        )
            .into_response(),
        Error::Unavailable | Error::Timeout | Error::Io(_) => (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(HttpAuthErrorResponse {
                code: "UNAVAILABLE".into(),
                message: "service is unavailable".into(),
                retryable: true,
            }),
        )
            .into_response(),
        _ => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(HttpAuthErrorResponse {
                code: "INTERNAL".into(),
                message: "internal error".into(),
                retryable: false,
            }),
        )
            .into_response(),
    }
}

fn auth_error_response(status: StatusCode, code: &'static str, message: &'static str) -> Response {
    let mut response = (
        status,
        Json(HttpAuthErrorResponse {
            code: code.into(),
            message: message.into(),
            retryable: false,
        }),
    )
        .into_response();
    if status == StatusCode::UNAUTHORIZED {
        response.headers_mut().insert(
            header::WWW_AUTHENTICATE,
            "Bearer".parse().expect("static bearer challenge"),
        );
    }
    response
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use axum::body::{Body, to_bytes};
    use axum::http::Request;
    use elura_core::ticket::{MemoryReplayStore, TicketPurpose};
    use tower::ServiceExt;

    use super::*;

    struct DemoLogin;

    #[async_trait]
    impl HttpLoginBackend for DemoLogin {
        async fn login(&self, provider: &str, credential: Value) -> Result<HttpLoginGrant> {
            if provider != "demo" || credential["secret"] != "valid" {
                return Err(Error::Authentication);
            }
            Ok(HttpLoginGrant::new(
                Principal {
                    account_id: 17,
                    generation: 4,
                },
                [GAME_CONNECT_SCOPE, "payments:write"],
            ))
        }

        async fn game_identity(
            &self,
            principal: Principal,
            request: &GameSessionTicketRequest,
        ) -> Result<Identity> {
            if request.user_id != 23 {
                return Err(Error::Authentication);
            }
            Ok(Identity {
                account_id: principal.account_id,
                user_id: request.user_id,
                region_id: request.region_id,
                realm_id: request.realm_id,
                generation: principal.generation,
            })
        }
    }

    fn services() -> (
        Arc<HttpTokenService>,
        Arc<TicketService>,
        Arc<MemoryReplayStore>,
    ) {
        (
            Arc::new(
                HttpTokenService::new(
                    [8_u8; 32],
                    "game-login",
                    "game-http-api",
                    Duration::from_secs(900),
                    Duration::from_secs(30 * 24 * 60 * 60),
                )
                .unwrap(),
            ),
            Arc::new(
                TicketService::new(
                    [9_u8; 32],
                    "game-login",
                    "game-gateway",
                    Duration::from_secs(60),
                    Duration::from_secs(30 * 60),
                )
                .unwrap(),
            ),
            Arc::new(MemoryReplayStore::default()),
        )
    }

    fn json_request(uri: &str, value: Value) -> Request<Body> {
        Request::builder()
            .method("POST")
            .uri(uri)
            .header(header::CONTENT_TYPE, "application/json")
            .body(Body::from(serde_json::to_vec(&value).unwrap()))
            .unwrap()
    }

    async fn response_json<T: for<'de> Deserialize<'de>>(response: Response) -> T {
        let body = to_bytes(response.into_body(), 64 * 1024).await.unwrap();
        serde_json::from_slice(&body).unwrap()
    }

    #[tokio::test]
    async fn one_login_can_issue_http_tokens_and_the_first_gateway_ticket() {
        let (tokens, tickets, replay) = services();
        let api = HttpAuthApi::new(tokens, tickets.clone(), replay, Arc::new(DemoLogin));
        let response = api
            .router()
            .oneshot(json_request(
                "/elura/auth/login",
                serde_json::json!({
                    "provider": "demo",
                    "credential": {"secret": "valid"},
                    "game": {"user_id": 23, "region_id": 1, "realm_id": 2}
                }),
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let login: HttpLoginResponse = response_json(response).await;
        assert_eq!(login.account_id, 17);
        assert_eq!(login.generation, 4);
        let gateway = login.gateway.unwrap();
        let verified = tickets.validate(&gateway.ticket).unwrap();
        assert_eq!(verified.claims().purpose, TicketPurpose::Login);
        assert_eq!(
            verified.claims().identity,
            Identity {
                account_id: 17,
                user_id: 23,
                region_id: 1,
                realm_id: 2,
                generation: 4,
            }
        );
    }

    #[tokio::test]
    async fn access_token_exchanges_for_gateway_ticket_on_any_http_instance() {
        let (tokens, tickets, replay) = services();
        let first = HttpAuthApi::new(
            tokens.clone(),
            tickets.clone(),
            replay.clone(),
            Arc::new(DemoLogin),
        );
        let second = HttpAuthApi::new(tokens, tickets, replay, Arc::new(DemoLogin));
        let login_response = first
            .router()
            .oneshot(json_request(
                "/elura/auth/login",
                serde_json::json!({
                    "provider": "demo",
                    "credential": {"secret": "valid"}
                }),
            ))
            .await
            .unwrap();
        let login: HttpLoginResponse = response_json(login_response).await;

        let unauthorized = second
            .router()
            .oneshot(json_request(
                "/elura/auth/session-ticket",
                serde_json::json!({"user_id": 23, "region_id": 1, "realm_id": 2}),
            ))
            .await
            .unwrap();
        assert_eq!(unauthorized.status(), StatusCode::UNAUTHORIZED);

        let mut request = json_request(
            "/elura/auth/session-ticket",
            serde_json::json!({"user_id": 23, "region_id": 1, "realm_id": 2}),
        );
        request.headers_mut().insert(
            header::AUTHORIZATION,
            format!("Bearer {}", login.tokens.access_token)
                .parse()
                .unwrap(),
        );
        let response = second.router().oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let ticket: GatewaySessionTicket = response_json(response).await;
        assert_eq!(ticket.expires_in_seconds, 60);

        let removed = first
            .router()
            .oneshot(json_request(
                "/elura/game/session-ticket",
                serde_json::json!({"user_id": 23, "region_id": 1, "realm_id": 2}),
            ))
            .await
            .unwrap();
        assert_eq!(removed.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn refresh_endpoint_rotates_each_refresh_token_once() {
        let (tokens, tickets, replay) = services();
        let api = HttpAuthApi::new(tokens, tickets, replay, Arc::new(DemoLogin));
        let login_response = api
            .router()
            .oneshot(json_request(
                "/elura/auth/login",
                serde_json::json!({
                    "provider": "demo",
                    "credential": {"secret": "valid"}
                }),
            ))
            .await
            .unwrap();
        let login: HttpLoginResponse = response_json(login_response).await;
        let refresh_body = serde_json::json!({
            "refresh_token": login.tokens.refresh_token
        });

        let first = api
            .router()
            .oneshot(json_request("/elura/auth/refresh", refresh_body.clone()))
            .await
            .unwrap();
        assert_eq!(first.status(), StatusCode::OK);
        let rotated: HttpTokenPair = response_json(first).await;
        assert_ne!(rotated.access_token, login.tokens.access_token);

        let replayed = api
            .router()
            .oneshot(json_request("/elura/auth/refresh", refresh_body))
            .await
            .unwrap();
        assert_eq!(replayed.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn bearer_middleware_protects_application_http_routes() {
        async fn payment(identity: AuthenticatedHttp) -> Response {
            match identity.require_scope("payments:write") {
                Ok(()) => StatusCode::NO_CONTENT.into_response(),
                Err(error) => error.into_response(),
            }
        }

        let (tokens, _, _) = services();
        let pair = tokens
            .issue(
                Principal {
                    account_id: 17,
                    generation: 4,
                },
                ["payments:write"],
            )
            .unwrap();
        let app = Router::new().route("/payments", post(payment)).route_layer(
            middleware::from_fn_with_state(HttpBearerAuth::new(tokens), require_bearer),
        );
        let mut request = Request::builder()
            .method("POST")
            .uri("/payments")
            .body(Body::empty())
            .unwrap();
        request.headers_mut().insert(
            header::AUTHORIZATION,
            format!("Bearer {}", pair.access_token).parse().unwrap(),
        );
        let response = app.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::NO_CONTENT);
    }
}
