use std::collections::{HashMap, HashSet};
use std::net::{IpAddr, SocketAddr};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, RwLock};
use std::time::{Duration, SystemTime};

use async_trait::async_trait;
use bytes::Bytes;
use elura_core::gateway_world::WorldRequest;
use elura_core::ownership::{OwnershipResolver, shard_for};
use elura_core::protocol::{
    Frame, FrameKind, HEADER_LEN, ROUTE_AUTHENTICATE, ROUTE_HEARTBEAT, ROUTE_RECONNECT,
    SessionControlAction,
};
use elura_core::push::{PushRequest, PushTarget, PushTransport};
use elura_core::rate_limit::TokenBucket;
use elura_core::replay_protection::ReplayProtectionStore;
use elura_core::session::{
    Identity, Session, SessionControlEvent, SessionControlKind, SessionControlTransport,
};
use elura_core::ticket::{TicketPurpose, TicketService};
use elura_core::{Error, ErrorEnvelope, Result};
use tokio::sync::watch;
use tokio::time::timeout;
use tracing::{Instrument, debug};
use uuid::Uuid;

use crate::client_protocol::{
    AuthenticateRequest, AuthenticateResponse, ClientFrameAction, ReconnectTicketRequest,
    ReconnectTicketResponse, error_response, validate_client_frame,
};
use crate::config::GatewayConfig;
use crate::discovery::WorldClient;
use crate::interceptor::{
    self, GatewayInterceptContext, GatewayInterceptor, GatewayRequest, GatewayResponse,
};
use crate::observability;
use crate::presence::{DuplicateLoginMode, OnlineAdmission, OnlineAdmissionPolicy, SessionLease};
use crate::protection::BackendProtector;
use crate::server::OnlineConfig;
use crate::session_state::{
    SessionHandle, SessionSenders, SharedSessionIndex, disconnect_handle, enqueue_session_control,
};
use crate::stats::GatewayStats;
use crate::transport::{
    AccountVersionPolicy, AdmissionPolicy, AdmissionRequest, AdmissionStage, KeyedRateLimiter,
    SessionConnection, SessionEventKind, SessionObserver, notify_session_observers,
};

pub(crate) struct ConnectionContext {
    pub(crate) config: GatewayConfig,
    pub(crate) tickets: Arc<TicketService>,
    pub(crate) replay: Arc<dyn ReplayProtectionStore>,
    pub(crate) world: Arc<dyn WorldClient>,
    pub(crate) sessions: SessionSenders,
    pub(crate) ownership: Option<(u32, Arc<dyn OwnershipResolver>)>,
    pub(crate) protector: Option<Arc<BackendProtector>>,
    pub(crate) ip_requests: Option<Arc<KeyedRateLimiter<IpAddr>>>,
    pub(crate) session_index: SharedSessionIndex,
    pub(crate) topics: Arc<RwLock<HashMap<String, HashSet<Uuid>>>>,
    pub(crate) online: Option<OnlineConfig>,
    pub(crate) push: Option<Arc<dyn PushTransport>>,
    pub(crate) session_control: Option<Arc<dyn SessionControlTransport>>,
    pub(crate) admission: Option<AdmissionPolicy>,
    pub(crate) observers: Vec<Arc<dyn SessionObserver>>,
    pub(crate) account_versions: Option<AccountVersionPolicy>,
    pub(crate) interceptors: Vec<Arc<dyn GatewayInterceptor>>,
    pub(crate) stats: Arc<GatewayStats>,
}

impl ConnectionContext {
    pub(crate) async fn serve(self, mut connection: SessionConnection) -> Result<()> {
        let _active_connection = self.stats.connection_started();
        let peer = connection.peer;
        let session = Session::new(client_ip(peer));
        notify_session_observers(&self.observers, SessionEventKind::Connected, &session);
        if let Err(error) = self
            .check_admission(AdmissionRequest {
                stage: AdmissionStage::Connected,
                remote_ip: session.remote_ip(),
                identity: None,
            })
            .await
        {
            self.stats.record_rejection();
            session.close();
            notify_session_observers(&self.observers, SessionEventKind::Closed, &session);
            return Err(error);
        }
        let response_tx = connection.responses;
        let push_tx = connection.pushes;
        let (disconnect_tx, mut disconnect_rx) = watch::channel(false);
        let authenticated = Arc::new(AtomicBool::new(false));
        self.sessions
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .insert(
                session.id(),
                SessionHandle {
                    pushes: push_tx.clone(),
                    disconnect: disconnect_tx,
                    authenticated: authenticated.clone(),
                },
            );
        let session_id = session.id();
        let result = async {
            let mut limiter =
                TokenBucket::new(self.config.request_rate, self.config.request_burst);
            let mut byte_limiter = TokenBucket::new(
                self.config.inbound_byte_rate,
                self.config.inbound_byte_burst,
            );
            let mut route_limiters = self
                .config
                .route_rate_limits
                .iter()
                .map(|(route, limit)| {
                    (
                        *route,
                        TokenBucket::new(limit.requests_per_second, limit.burst),
                    )
                })
                .collect::<HashMap<_, _>>();
            let mut rate_limit_violations = 0_u32;
            let mut rate_limit_notified = false;
            let mut protocol_violations = 0_u32;
            let renew_interval = self
                .online
                .as_ref()
                .map_or(Duration::from_secs(3600), |online| {
                    online.config.renew_interval
                });
            let mut renewal = tokio::time::interval_at(
                tokio::time::Instant::now() + renew_interval,
                renew_interval,
            );
            renewal.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            let now = tokio::time::Instant::now();
            let mut heartbeat = tokio::time::interval_at(
                now + self.config.heartbeat_interval,
                self.config.heartbeat_interval,
            );
            heartbeat.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            let authentication_deadline = now + self.config.authentication_timeout;
            let mut idle_deadline = now + self.config.idle_timeout;
            let mut heartbeat_request_id = u64::MAX;
            let mut pending_heartbeat = None;
            let mut version_checked_at = None;
            let mut lease_valid_until = None;

            loop {
            let next = tokio::select! {
                changed = disconnect_rx.changed() => {
                    if changed.is_err() || *disconnect_rx.borrow() { break Ok(()); }
                    continue;
                }
                _ = renewal.tick(), if self.online.is_some() => {
                    if let Some(identity) = session.identity() {
                        match self.renew_lease(session.id(), identity).await {
                            Ok(()) => lease_valid_until = self.lease_safety_deadline(),
                            Err(error) => {
                                debug!(session_id = %session.id(), %error, "renew online session lease");
                                if lease_valid_until.is_some_and(|deadline| tokio::time::Instant::now() >= deadline) {
                                    break Err(Error::Unavailable);
                                }
                            }
                        }
                    }
                    continue;
                }
                _ = tokio::time::sleep_until(authentication_deadline), if session.identity().is_none() => {
                    break Err(Error::Authentication);
                }
                _ = heartbeat.tick() => {
                    if pending_heartbeat.is_none() {
                        let request_id = heartbeat_request_id;
                        heartbeat_request_id = heartbeat_request_id.saturating_sub(1).max(1);
                        response_tx
                            .try_send(Frame::request(ROUTE_HEARTBEAT, request_id, Bytes::new())?)
                            .map_err(|_| Error::QueueFull)?;
                        pending_heartbeat = Some(request_id);
                    }
                    continue;
                }
                _ = tokio::time::sleep_until(idle_deadline) => break Err(Error::Timeout),
                next = connection.inbound.recv() => next,
            };
            let Some(frame) = next else {
                break Ok(());
            };
            let frame = frame?;
            let authenticated_now = session.identity().is_some();
            let action = validate_client_frame(&frame, authenticated_now, pending_heartbeat);
            let action = match action {
                Ok(action) => {
                    protocol_violations = 0;
                    action
                }
                Err(error) => {
                    self.stats.record_rejection();
                    protocol_violations = protocol_violations.saturating_add(1);
                    if frame.kind == FrameKind::Request {
                        response_tx
                            .try_send(Frame::error(
                                &frame,
                                ErrorEnvelope::from(&error).to_bytes(),
                            ))
                            .map_err(|_| Error::QueueFull)?;
                    }
                    if protocol_violations >= self.config.max_protocol_violations {
                        break Err(error);
                    }
                    continue;
                }
            };
            idle_deadline = tokio::time::Instant::now() + self.config.idle_timeout;
            session.touch();
            if action == ClientFrameAction::HeartbeatResponse {
                pending_heartbeat = None;
                continue;
            }
            self.stats.record_request();
            let frame_bytes = u32::try_from(HEADER_LEN.saturating_add(frame.payload.len()))
                .unwrap_or(u32::MAX);
            let route_allowed = route_limiters
                .get_mut(&frame.route)
                .is_none_or(TokenBucket::allow);
            let byte_allowed = byte_limiter.allow_n(frame_bytes);
            let ip_allowed = self
                .ip_requests
                .as_ref()
                .is_none_or(|limiter| limiter.allow(session.remote_ip()));
            if !route_allowed
                || !limiter.allow()
                || !byte_allowed
                || !ip_allowed
            {
                self.stats.record_rejection();
                rate_limit_violations = rate_limit_violations.saturating_add(1);
                if !rate_limit_notified {
                    response_tx
                        .try_send(Frame::error(
                            &frame,
                            ErrorEnvelope::from(&Error::RateLimited).to_bytes(),
                        ))
                        .map_err(|_| Error::QueueFull)?;
                    rate_limit_notified = true;
                }
                if rate_limit_violations >= self.config.max_rate_limit_violations {
                    break Err(Error::RateLimited);
                }
                continue;
            }
            rate_limit_violations = rate_limit_violations.saturating_sub(1);
            if rate_limit_violations == 0 {
                rate_limit_notified = false;
            }
            // `request_id` correlates one transport attempt. Application retries always reach
            // World; durable idempotency belongs to the application's operation ID and storage.
            if let Some(identity) = session.identity()
                && let Some(policy) = &self.account_versions
            {
                let now = tokio::time::Instant::now();
                let due = version_checked_at
                    .is_none_or(|checked| now.duration_since(checked) >= policy.check_interval());
                if due {
                    version_checked_at = Some(now);
                    if let Err(error) = policy.check(&identity).await {
                        if let Err(queue_error) = enqueue_session_control(
                            &push_tx,
                            SessionControlAction::AccountVersionChanged,
                            "account version changed or unavailable",
                        ) {
                            break Err(queue_error);
                        }
                        break Err(error);
                    }
                }
            }
            let was_authenticated = session.identity().is_some();
            let request_deadline = tokio::time::Instant::now() + self.config.handler_timeout;
            let response = tokio::select! {
                changed = disconnect_rx.changed() => {
                    if changed.is_err() || *disconnect_rx.borrow() {
                        break Ok(());
                    }
                    continue;
                }
                response = tokio::time::timeout_at(
                    request_deadline,
                    self.handle(&session, &frame, authenticated.as_ref(), request_deadline),
                ) => response,
            };
            let response = match response {
                Ok(Ok(payload)) => Frame::response(&frame, payload),
                Ok(Err(error)) => {
                    self.stats.record_error(&error);
                    error_response(&frame, &error)
                }
                Err(_) => {
                    self.stats.record_failure();
                    Frame::error(&frame, ErrorEnvelope::from(&Error::Timeout).to_bytes())
                }
            };
            response_tx
                .try_send(response)
                .map_err(|_| Error::QueueFull)?;
            if !was_authenticated && session.identity().is_some() {
                version_checked_at = Some(tokio::time::Instant::now());
                lease_valid_until = self.lease_safety_deadline();
            }
            }
        }
        .await;

        let identity = session.identity();
        if identity.is_some() {
            self.stats.authenticated_session_ended();
        }
        if let Some(identity) = &identity
            && timeout(
                Duration::from_millis(200),
                self.remove_online(session.id(), identity.clone()),
            )
            .await
            .is_err()
        {
            debug!(session_id = %session.id(), "online Session cleanup timed out");
        }
        session.close();
        notify_session_observers(&self.observers, SessionEventKind::Closed, &session);
        self.sessions
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .remove(&session_id);
        self.session_index
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .remove(session_id);
        self.topics
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .retain(|_, sessions| {
                sessions.remove(&session_id);
                !sessions.is_empty()
            });
        drop(response_tx);
        drop(push_tx);
        result
    }

    async fn handle(
        &self,
        session: &Session,
        frame: &Frame,
        authenticated: &AtomicBool,
        deadline: tokio::time::Instant,
    ) -> Result<Bytes> {
        match frame.route {
            ROUTE_AUTHENTICATE => {
                if session.identity().is_some() {
                    return Err(Error::Authentication);
                }
                let request: AuthenticateRequest = serde_json::from_slice(&frame.payload)?;
                let verified = self.tickets.validate(&request.ticket)?;
                let pending_identity = verified.claims().identity.clone();
                self.check_admission(AdmissionRequest {
                    stage: AdmissionStage::Authenticated,
                    remote_ip: session.remote_ip(),
                    identity: Some(pending_identity.clone()),
                })
                .await?;
                if let Some(policy) = &self.account_versions {
                    policy.check(&pending_identity).await?;
                }
                let previous = self
                    .admit_online(session.id(), pending_identity.clone())
                    .await?;
                let claims = match verified.consume(self.replay.as_ref()).await {
                    Ok(claims) => claims,
                    Err(error) => {
                        self.remove_online(session.id(), pending_identity).await;
                        return Err(error);
                    }
                };
                session.authenticate(claims.identity.clone())?;
                authenticated.store(true, Ordering::Release);
                self.stats.authenticated_session_started();
                notify_session_observers(&self.observers, SessionEventKind::Authenticated, session);
                self.session_index
                    .write()
                    .unwrap_or_else(|poisoned| poisoned.into_inner())
                    .insert(session.id(), claims.identity.clone());
                if let Some(transport) = &self.session_control {
                    let event = SessionControlEvent {
                        kind: SessionControlKind::Login,
                        region_id: claims.identity.region_id,
                        realm_id: claims.identity.realm_id,
                        user_id: claims.identity.user_id,
                        generation: claims.identity.generation,
                        session_id: Some(session.id()),
                        keep_session_id: Some(session.id()),
                        reason: "new login".into(),
                    };
                    if let Err(error) = transport.publish(&event).await {
                        debug!(session_id = %session.id(), %error, "publish Session login event");
                    }
                }
                self.kick_previous(previous, &claims.identity).await;
                let reconnect = ReconnectTicketResponse {
                    ticket: self.tickets.issue_reconnect(claims.identity.clone())?,
                    expires_in_seconds: self.tickets.reconnect_ttl().as_secs(),
                };
                Ok(Bytes::from(serde_json::to_vec(&AuthenticateResponse {
                    session_id: session.id().to_string(),
                    identity: claims.identity,
                    reconnect,
                })?))
            }
            ROUTE_HEARTBEAT => Ok(Bytes::new()),
            ROUTE_RECONNECT => {
                let request: ReconnectTicketRequest = serde_json::from_slice(&frame.payload)?;
                if request.ticket.is_empty() {
                    return Err(Error::Authentication);
                }
                let identity = session.identity().ok_or(Error::Authentication)?;
                let verified = self.tickets.validate(&request.ticket)?;
                if verified.claims().purpose != TicketPurpose::Reconnect
                    || verified.claims().identity != identity
                {
                    return Err(Error::Authentication);
                }
                verified.consume(self.replay.as_ref()).await?;
                let ticket = self.tickets.issue_reconnect(identity)?;
                Ok(Bytes::from(serde_json::to_vec(&ReconnectTicketResponse {
                    ticket,
                    expires_in_seconds: self.tickets.reconnect_ttl().as_secs(),
                })?))
            }
            route => {
                let identity = session.identity().ok_or(Error::Authentication)?;
                let trace_id = observability::new_trace_id();
                let ownership = match &self.ownership {
                    Some((shard_count, resolver)) => {
                        let shard = shard_for(identity.user_id, *shard_count)?;
                        let assignment = resolver
                            .resolve(identity.region_id, identity.realm_id, shard)
                            .await?;
                        if assignment.region_id != identity.region_id
                            || assignment.realm_id != identity.realm_id
                            || assignment.shard_id != shard
                        {
                            return Err(Error::Unavailable);
                        }
                        Some(assignment)
                    }
                    None => None,
                };
                let span = tracing::info_span!(
                    "gateway.command",
                    trace_id = %trace_id,
                    route,
                    request_id = frame.request_id,
                    user_id = identity.user_id,
                    region_id = identity.region_id,
                    realm_id = identity.realm_id,
                );
                let context = GatewayInterceptContext::new(
                    identity,
                    session.id(),
                    session.remote_ip(),
                    trace_id,
                    ownership,
                );
                let request = GatewayRequest::new(route, frame.request_id, frame.payload.clone());
                let dispatch = WorldDispatch {
                    world: self.world.as_ref(),
                    protector: self.protector.as_deref(),
                    deadline,
                };
                async move {
                    interceptor::run_interceptors(&self.interceptors, &dispatch, &context, &request)
                        .await
                        .map(GatewayResponse::into_payload)
                }
                .instrument(span)
                .await
            }
        }
    }

    async fn check_admission(&self, request: AdmissionRequest) -> Result<()> {
        match &self.admission {
            Some(admission) => admission.check(request).await,
            None => Ok(()),
        }
    }

    fn lease(&self, session_id: Uuid, identity: Identity) -> Option<SessionLease> {
        self.online.as_ref().map(|online| SessionLease {
            session_id,
            gateway_id: online.config.gateway_id.clone(),
            identity,
            expires_at: SystemTime::now() + online.config.lease_ttl,
        })
    }

    async fn admit_online(&self, session_id: Uuid, identity: Identity) -> Result<Vec<Uuid>> {
        let Some(online) = &self.online else {
            return Ok(Vec::new());
        };
        let maximum = online
            .config
            .max_sessions(identity.region_id, identity.realm_id);
        let policy = OnlineAdmissionPolicy::new(online.config.duplicate_login, maximum)?;
        let lease = self.lease(session_id, identity).ok_or(Error::Unavailable)?;
        match online.directory.acquire(lease, policy).await? {
            OnlineAdmission::Accepted { previous_session } => {
                Ok(previous_session.into_iter().collect())
            }
            OnlineAdmission::Duplicate => Err(Error::DuplicateSession),
            OnlineAdmission::RealmFull => Err(Error::AdmissionDenied {
                code: "realm_full".into(),
                reason: "the selected realm is at capacity".into(),
                retry_after_ms: u64::try_from(online.config.full_retry_after.as_millis())
                    .unwrap_or(u64::MAX),
            }),
        }
    }

    async fn renew_lease(&self, session_id: Uuid, identity: Identity) -> Result<()> {
        let Some(online) = &self.online else {
            return Ok(());
        };
        online
            .directory
            .renew(self.lease(session_id, identity).ok_or(Error::Unavailable)?)
            .await
    }

    fn lease_safety_deadline(&self) -> Option<tokio::time::Instant> {
        self.online.as_ref().map(|online| {
            tokio::time::Instant::now()
                + online
                    .config
                    .lease_ttl
                    .saturating_sub(online.config.renew_interval)
        })
    }

    async fn remove_online(&self, session_id: Uuid, identity: Identity) {
        let Some(online) = &self.online else {
            return;
        };
        let Some(lease) = self.lease(session_id, identity) else {
            return;
        };
        if let Err(error) = online.directory.unregister(&lease).await {
            debug!(%session_id, %error, "unregister online session");
        }
    }

    async fn kick_previous(&self, sessions: Vec<Uuid>, identity: &Identity) {
        if sessions.is_empty()
            || self.online.as_ref().is_none_or(|online| {
                online.config.duplicate_login != DuplicateLoginMode::KickExisting
            })
        {
            return;
        }
        for session_id in sessions {
            let request = PushRequest {
                region_id: identity.region_id,
                realm_id: identity.realm_id,
                target: PushTarget::Disconnect(session_id),
                route: 0,
                sequence: 0,
                trace_id: observability::new_trace_id(),
                payload: Bytes::from_static(b"duplicate_login"),
            };
            if let Some(handle) = self
                .sessions
                .read()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .get(&session_id)
                .cloned()
            {
                let _ = disconnect_handle(handle, "duplicate_login");
            } else if let Some(push) = &self.push {
                let _ = push.publish(&request).await;
            }
        }
    }
}

pub(crate) fn client_ip(peer: SocketAddr) -> IpAddr {
    peer.ip()
}

struct WorldDispatch<'a> {
    world: &'a dyn WorldClient,
    protector: Option<&'a BackendProtector>,
    deadline: tokio::time::Instant,
}

#[async_trait]
impl interceptor::GatewayDispatch for WorldDispatch<'_> {
    async fn dispatch(
        &self,
        context: &GatewayInterceptContext,
        request: &GatewayRequest,
    ) -> Result<GatewayResponse> {
        let remaining = self
            .deadline
            .saturating_duration_since(tokio::time::Instant::now())
            .max(Duration::from_millis(1));
        let command = || {
            self.world.command(WorldRequest {
                identity: context.identity().clone(),
                session_id: context.session_id(),
                trace_id: context.trace_id().to_owned(),
                route: request.route(),
                request_id: request.request_id(),
                payload: request.payload().clone(),
                ownership: context.ownership().cloned(),
                timeout: remaining,
            })
        };
        let payload = match self.protector {
            Some(protector) => {
                protector
                    .execute(command, |error| {
                        matches!(error, Error::Unavailable | Error::Timeout | Error::Io(_))
                    })
                    .await?
            }
            None => command().await?,
        };
        Ok(GatewayResponse::new(payload))
    }
}
