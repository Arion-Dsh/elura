use std::collections::HashMap;
use std::fmt;
use std::hash::{DefaultHasher, Hash, Hasher};
use std::io;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use futures_util::{SinkExt, StreamExt};
use prost::Message;
use serde::Serialize;
use tokio::net::TcpStream;
use tokio::sync::{broadcast, mpsc, oneshot, watch};
use tokio::time::Instant;
use tokio_util::codec::Framed;

pub use elura_protocol::*;

#[derive(Debug, Clone)]
pub struct ClientConfig {
    pub connect_timeout: Duration,
    pub request_timeout: Duration,
    pub reconnect_renewal_margin: Duration,
    pub automatic_reconnect: bool,
    pub reconnect_initial_delay: Duration,
    pub reconnect_max_delay: Duration,
    pub reconnect_max_attempts: Option<u32>,
    /// Randomizes reconnect deadlines by up to this percentage to avoid a reconnect storm.
    pub reconnect_jitter_percent: u8,
    pub max_payload: usize,
    pub max_in_flight_requests: usize,
    pub command_capacity: usize,
    pub event_capacity: usize,
    pub tcp_nodelay: bool,
}

impl Default for ClientConfig {
    fn default() -> Self {
        Self {
            connect_timeout: Duration::from_secs(5),
            request_timeout: Duration::from_secs(10),
            reconnect_renewal_margin: Duration::from_secs(30),
            automatic_reconnect: true,
            reconnect_initial_delay: Duration::from_millis(250),
            reconnect_max_delay: Duration::from_secs(10),
            reconnect_max_attempts: Some(8),
            reconnect_jitter_percent: 20,
            max_payload: DEFAULT_MAX_PAYLOAD,
            max_in_flight_requests: 1024,
            command_capacity: 64,
            event_capacity: 64,
            tcp_nodelay: true,
        }
    }
}

impl ClientConfig {
    fn validate(&self) -> ClientResult<()> {
        if self.connect_timeout.is_zero()
            || self.request_timeout.is_zero()
            || self.reconnect_renewal_margin.is_zero()
            || self.reconnect_initial_delay.is_zero()
            || self.reconnect_max_delay < self.reconnect_initial_delay
            || self.reconnect_max_attempts == Some(0)
            || self.reconnect_jitter_percent > 100
            || self.max_in_flight_requests == 0
            || self.command_capacity == 0
            || self.event_capacity == 0
        {
            return Err(ClientError::Configuration(
                "client timeouts, reconnect limits, and capacities must be positive".into(),
            ));
        }
        Elr2Codec::new(self.max_payload).map_err(ClientError::Protocol)?;
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ClientEvent {
    Push(Elr2Frame),
    SessionControl(SessionControl),
    Disconnected,
    Reconnecting { attempt: u32, delay: Duration },
    Reconnected,
    ReauthenticationRequired,
    ReconnectExhausted,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConnectionState {
    Connected,
    Reconnecting,
    ReauthenticationRequired,
    Disconnected,
    Closed,
}

#[derive(Debug)]
pub enum ClientError {
    Configuration(String),
    Transport(io::Error),
    Protocol(Elr2ProtocolError),
    Server(ErrorEnvelope),
    ConnectTimeout,
    RequestTimeout,
    RequestInterrupted,
    ReauthenticationRequired,
    ReconnectExhausted,
    TooManyInFlight,
    NotConnected(ConnectionState),
    Closed,
    UnexpectedFrame(String),
}

impl fmt::Display for ClientError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Configuration(message) => {
                write!(formatter, "invalid client configuration: {message}")
            }
            Self::Transport(error) => write!(formatter, "transport error: {error}"),
            Self::Protocol(error) => write!(formatter, "protocol error: {error}"),
            Self::Server(error) => {
                write!(formatter, "server error {}: {}", error.code, error.message)
            }
            Self::ConnectTimeout => formatter.write_str("connection timed out"),
            Self::RequestTimeout => formatter.write_str("request timed out"),
            Self::RequestInterrupted => {
                formatter.write_str("request was interrupted by a connection loss")
            }
            Self::ReauthenticationRequired => {
                formatter.write_str("a fresh login ticket is required")
            }
            Self::ReconnectExhausted => {
                formatter.write_str("automatic reconnect attempts were exhausted")
            }
            Self::TooManyInFlight => formatter.write_str("too many requests are in flight"),
            Self::NotConnected(state) => write!(formatter, "client is not connected: {state:?}"),
            Self::Closed => formatter.write_str("the Gateway connection is closed"),
            Self::UnexpectedFrame(message) => write!(formatter, "unexpected frame: {message}"),
        }
    }
}

impl std::error::Error for ClientError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Transport(error) => Some(error),
            Self::Protocol(error) => Some(error),
            _ => None,
        }
    }
}

impl From<io::Error> for ClientError {
    fn from(error: io::Error) -> Self {
        Self::Transport(error)
    }
}

impl From<Elr2ProtocolError> for ClientError {
    fn from(error: Elr2ProtocolError) -> Self {
        Self::Protocol(error)
    }
}

pub type ClientResult<T> = Result<T, ClientError>;

#[derive(Clone)]
pub struct EluraClient {
    shared: Arc<ClientShared>,
}

struct ClientShared {
    commands: mpsc::Sender<Command>,
    events: broadcast::Sender<ClientEvent>,
    state: watch::Receiver<ConnectionState>,
    session: watch::Receiver<Option<SessionSnapshot>>,
}

#[derive(Debug, Clone)]
struct SessionSnapshot {
    authentication: AuthenticateResponse,
    reconnect_expires_at: Instant,
}

impl EluraClient {
    pub async fn connect(
        address: impl Into<String>,
        ticket: impl Into<String>,
    ) -> ClientResult<Self> {
        Self::connect_with_config(address, ticket, ClientConfig::default()).await
    }

    pub async fn connect_with_config(
        address: impl Into<String>,
        ticket: impl Into<String>,
        config: ClientConfig,
    ) -> ClientResult<Self> {
        config.validate()?;
        let address = address.into();
        let (events, _) = broadcast::channel(config.event_capacity);
        let mut next_request_id = 1;
        let (connection, authentication) = open_and_authenticate(
            &address,
            ticket.into(),
            &config,
            &events,
            &mut next_request_id,
        )
        .await?;
        let snapshot = session_snapshot(authentication);
        let (state_tx, state) = watch::channel(ConnectionState::Connected);
        let (session_tx, session) = watch::channel(Some(snapshot.clone()));
        let (commands, command_rx) = mpsc::channel(config.command_capacity);
        let reconnect_renew_at = renewal_time(&snapshot, config.reconnect_renewal_margin);
        let reconnect_seed = reconnect_seed(&address, &snapshot.authentication.session_id);
        tokio::spawn(
            ClientDriver {
                address,
                config,
                commands: command_rx,
                events: events.clone(),
                state: state_tx,
                session: session_tx,
                connection: Some(connection),
                authentication: snapshot.authentication,
                reconnect_renew_at,
                reconnect_at: None,
                reconnect_attempt: 0,
                reconnect_seed,
                next_request_id,
                pending: HashMap::new(),
            }
            .run(),
        );
        Ok(Self {
            shared: Arc::new(ClientShared {
                commands,
                events,
                state,
                session,
            }),
        })
    }

    pub fn subscribe(&self) -> broadcast::Receiver<ClientEvent> {
        self.shared.events.subscribe()
    }

    pub fn subscribe_state(&self) -> watch::Receiver<ConnectionState> {
        self.shared.state.clone()
    }

    pub fn state(&self) -> ConnectionState {
        *self.shared.state.borrow()
    }

    pub fn authentication(&self) -> Option<AuthenticateResponse> {
        self.shared
            .session
            .borrow()
            .as_ref()
            .map(|snapshot| snapshot.authentication.clone())
    }

    pub fn reconnect_ticket_valid_for(&self) -> Option<Duration> {
        self.shared.session.borrow().as_ref().map(|snapshot| {
            snapshot
                .reconnect_expires_at
                .saturating_duration_since(Instant::now())
        })
    }

    pub async fn request(&self, route: u32, payload: impl Into<Bytes>) -> ClientResult<Elr2Frame> {
        if route < EluraRoutes::FIRST_APPLICATION {
            return Err(ClientError::Configuration(format!(
                "application route must be at least {}",
                EluraRoutes::FIRST_APPLICATION
            )));
        }
        let (response, receiver) = oneshot::channel();
        self.send(Command::Request {
            route,
            payload: payload.into(),
            response,
        })
        .await?;
        receive(receiver).await
    }

    pub async fn request_protobuf<Request, Response>(
        &self,
        route: u32,
        request: &Request,
    ) -> ClientResult<Response>
    where
        Request: Message,
        Response: Message + Default,
    {
        let response = self.request(route, request.encode_to_vec()).await?;
        Response::decode(response.payload).map_err(|error| {
            ClientError::Protocol(Elr2ProtocolError::new(format!(
                "invalid application protobuf response: {error}"
            )))
        })
    }

    pub async fn request_json<Request>(
        &self,
        route: u32,
        request: &Request,
    ) -> ClientResult<Elr2Frame>
    where
        Request: Serialize,
    {
        let payload = serde_json::to_vec(request).map_err(|error| {
            ClientError::Protocol(Elr2ProtocolError::new(format!(
                "invalid application JSON request: {error}"
            )))
        })?;
        self.request(route, payload).await
    }

    pub async fn renew_reconnect_ticket(&self) -> ClientResult<ReconnectTicketResponse> {
        let (response, receiver) = oneshot::channel();
        self.send(Command::Renew { response }).await?;
        receive(receiver).await
    }

    pub async fn reconnect(&self) -> ClientResult<AuthenticateResponse> {
        let (response, receiver) = oneshot::channel();
        self.send(Command::Reconnect { response }).await?;
        receive(receiver).await
    }

    pub async fn reauthenticate(
        &self,
        login_ticket: impl Into<String>,
    ) -> ClientResult<AuthenticateResponse> {
        let (response, receiver) = oneshot::channel();
        self.send(Command::Reauthenticate {
            ticket: login_ticket.into(),
            response,
        })
        .await?;
        receive(receiver).await
    }

    pub async fn close(&self) -> ClientResult<()> {
        let (response, receiver) = oneshot::channel();
        self.send(Command::Close { response }).await?;
        receive(receiver).await
    }

    async fn send(&self, command: Command) -> ClientResult<()> {
        self.shared
            .commands
            .send(command)
            .await
            .map_err(|_| ClientError::Closed)
    }
}

async fn receive<T>(receiver: oneshot::Receiver<ClientResult<T>>) -> ClientResult<T> {
    receiver.await.map_err(|_| ClientError::Closed)?
}

enum Command {
    Request {
        route: u32,
        payload: Bytes,
        response: oneshot::Sender<ClientResult<Elr2Frame>>,
    },
    Renew {
        response: oneshot::Sender<ClientResult<ReconnectTicketResponse>>,
    },
    Reconnect {
        response: oneshot::Sender<ClientResult<AuthenticateResponse>>,
    },
    Reauthenticate {
        ticket: String,
        response: oneshot::Sender<ClientResult<AuthenticateResponse>>,
    },
    Close {
        response: oneshot::Sender<ClientResult<()>>,
    },
}

struct ClientDriver {
    address: String,
    config: ClientConfig,
    commands: mpsc::Receiver<Command>,
    events: broadcast::Sender<ClientEvent>,
    state: watch::Sender<ConnectionState>,
    session: watch::Sender<Option<SessionSnapshot>>,
    connection: Option<Framed<TcpStream, Elr2Codec>>,
    authentication: AuthenticateResponse,
    reconnect_renew_at: Instant,
    reconnect_at: Option<Instant>,
    reconnect_attempt: u32,
    reconnect_seed: u64,
    next_request_id: u64,
    pending: HashMap<u64, Pending>,
}

struct Pending {
    route: u32,
    deadline: Instant,
    kind: PendingKind,
}

enum PendingKind {
    Application(oneshot::Sender<ClientResult<Elr2Frame>>),
    Renewal(Option<oneshot::Sender<ClientResult<ReconnectTicketResponse>>>),
}

enum DriverAction {
    Command(Option<Command>),
    Frame(Option<io::Result<Elr2Frame>>),
    Timer,
}

impl ClientDriver {
    async fn run(mut self) {
        loop {
            let timer_at = self.next_timer();
            let action = if let Some(connection) = self.connection.as_mut() {
                tokio::select! {
                    command = self.commands.recv() => DriverAction::Command(command),
                    frame = connection.next() => DriverAction::Frame(frame),
                    () = tokio::time::sleep_until(timer_at) => DriverAction::Timer,
                }
            } else {
                tokio::select! {
                    command = self.commands.recv() => DriverAction::Command(command),
                    () = tokio::time::sleep_until(timer_at) => DriverAction::Timer,
                }
            };
            let keep_running = match action {
                DriverAction::Command(Some(command)) => self.handle_command(command).await,
                DriverAction::Command(None) => false,
                DriverAction::Frame(frame) => {
                    self.handle_frame(frame).await;
                    true
                }
                DriverAction::Timer => {
                    self.handle_timer().await;
                    true
                }
            };
            if !keep_running {
                self.finish();
                return;
            }
        }
    }

    fn next_timer(&self) -> Instant {
        if self.connection.is_none() {
            return self
                .reconnect_at
                .unwrap_or_else(|| Instant::now() + Duration::from_secs(86_400));
        }
        self.pending
            .values()
            .map(|pending| pending.deadline)
            .fold(self.reconnect_renew_at, Instant::min)
    }

    async fn handle_command(&mut self, command: Command) -> bool {
        match command {
            Command::Request {
                route,
                payload,
                response,
            } => {
                if self.current_state() != ConnectionState::Connected {
                    let _ = response.send(Err(self.not_connected_error()));
                } else if self.pending.len() >= self.config.max_in_flight_requests {
                    let _ = response.send(Err(ClientError::TooManyInFlight));
                } else {
                    let request_id = self.take_request_id();
                    match Elr2Frame::request(route, request_id, payload) {
                        Ok(frame) => {
                            if self.send_frame(frame).await.is_ok() {
                                self.pending.insert(
                                    request_id,
                                    Pending {
                                        route,
                                        deadline: Instant::now() + self.config.request_timeout,
                                        kind: PendingKind::Application(response),
                                    },
                                );
                            } else {
                                let _ = response.send(Err(ClientError::RequestInterrupted));
                                self.connection_lost();
                            }
                        }
                        Err(error) => {
                            let _ = response.send(Err(ClientError::Protocol(error)));
                        }
                    }
                }
            }
            Command::Renew { response } => {
                if self.current_state() != ConnectionState::Connected {
                    let _ = response.send(Err(self.not_connected_error()));
                } else if self.has_pending_renewal() {
                    let _ = response.send(Err(ClientError::Configuration(
                        "reconnect-ticket renewal is already in flight".into(),
                    )));
                } else if let Err(error) = self.start_renewal(Some(response)).await {
                    self.connection_lost();
                    if !is_connection_loss(&error) {
                        self.require_reauthentication();
                    }
                }
            }
            Command::Reconnect { response } => {
                self.disconnect_for_reconnect();
                let result = self
                    .connect_with_ticket(self.authentication.reconnect.ticket.clone())
                    .await;
                if result.is_err()
                    && self.current_state() != ConnectionState::ReauthenticationRequired
                {
                    if self.config.automatic_reconnect {
                        self.schedule_reconnect(1);
                    } else {
                        self.set_state(ConnectionState::Disconnected);
                    }
                }
                let _ = response.send(result);
            }
            Command::Reauthenticate { ticket, response } => {
                self.disconnect_for_reconnect();
                let result = self.connect_with_ticket(ticket).await;
                if result.is_err()
                    && self.current_state() != ConnectionState::ReauthenticationRequired
                {
                    self.set_state(ConnectionState::Disconnected);
                }
                let _ = response.send(result);
            }
            Command::Close { response } => {
                let result = match self.connection.as_mut() {
                    Some(connection) => {
                        tokio::time::timeout(self.config.request_timeout, connection.close())
                            .await
                            .map_err(|_| ClientError::RequestTimeout)
                            .and_then(|result| result.map_err(ClientError::Transport))
                    }
                    None => Ok(()),
                };
                let _ = response.send(result);
                return false;
            }
        }
        true
    }

    async fn handle_frame(&mut self, received: Option<io::Result<Elr2Frame>>) {
        let frame = match received {
            Some(Ok(frame)) => frame,
            Some(Err(_)) | None => {
                self.connection_lost();
                return;
            }
        };
        if frame.kind == FrameKind::Request && frame.route == EluraRoutes::HEARTBEAT {
            match EluraProtocol::heartbeat_response(&frame) {
                Ok(response) => {
                    if self.send_frame(response).await.is_err() {
                        self.connection_lost();
                    }
                }
                _ => self.connection_lost(),
            }
            return;
        }
        if frame.kind == FrameKind::Push {
            match decode_event(frame) {
                Ok(event) => {
                    self.emit(event.clone());
                    self.apply_event_state(&event);
                }
                Err(_) => self.connection_lost(),
            }
            return;
        }
        let Some(pending) = self.pending.remove(&frame.request_id) else {
            self.connection_lost();
            return;
        };
        if frame.route != pending.route {
            match pending.kind {
                PendingKind::Application(response) => {
                    let _ = response.send(Err(ClientError::UnexpectedFrame(
                        "response route does not match its request".into(),
                    )));
                }
                PendingKind::Renewal(Some(response)) => {
                    let _ = response.send(Err(ClientError::RequestInterrupted));
                }
                PendingKind::Renewal(None) => {}
            }
            self.connection_lost();
            return;
        }
        match pending.kind {
            PendingKind::Application(response) => {
                let result = match frame.kind {
                    FrameKind::Response => Ok(frame),
                    FrameKind::Error => EluraProtocol::decode_error(&frame)
                        .map_err(ClientError::Protocol)
                        .and_then(|error| {
                            if requires_reauthentication(&error) {
                                self.require_reauthentication();
                                Err(ClientError::ReauthenticationRequired)
                            } else {
                                Err(ClientError::Server(error))
                            }
                        }),
                    _ => Err(ClientError::UnexpectedFrame(
                        "expected response or error frame".into(),
                    )),
                };
                let _ = response.send(result);
            }
            PendingKind::Renewal(response) => {
                let result = self.finish_renewal(frame);
                if result.is_err()
                    && self.current_state() != ConnectionState::ReauthenticationRequired
                {
                    self.reconnect_renew_at = Instant::now() + self.config.reconnect_initial_delay;
                }
                if let Some(response) = response {
                    let _ = response.send(result);
                }
            }
        }
    }

    async fn handle_timer(&mut self) {
        let now = Instant::now();
        if self.connection.is_none() {
            if self.reconnect_at.is_some_and(|deadline| now >= deadline) {
                self.try_automatic_reconnect().await;
            }
            return;
        }
        if let Some((&request_id, _)) = self
            .pending
            .iter()
            .filter(|(_, pending)| pending.deadline <= now)
            .min_by_key(|(_, pending)| pending.deadline)
        {
            if let Some(pending) = self.pending.remove(&request_id) {
                Self::complete_pending(pending, PendingFailure::Timeout);
            }
            self.connection_lost();
            return;
        }
        if now >= self.reconnect_renew_at
            && !self.has_pending_renewal()
            && self.start_renewal(None).await.is_err()
        {
            self.connection_lost();
        }
    }

    async fn start_renewal(
        &mut self,
        response: Option<oneshot::Sender<ClientResult<ReconnectTicketResponse>>>,
    ) -> ClientResult<()> {
        let request_id = self.take_request_id();
        let frame = EluraProtocol::renew_reconnect_ticket(
            request_id,
            self.authentication.reconnect.ticket.clone(),
        )?;
        self.pending.insert(
            request_id,
            Pending {
                route: EluraRoutes::RENEW_RECONNECT_TICKET,
                deadline: Instant::now() + self.config.request_timeout,
                kind: PendingKind::Renewal(response),
            },
        );
        if let Err(error) = self.send_frame(frame).await {
            if let Some(pending) = self.pending.remove(&request_id) {
                Self::complete_pending(pending, PendingFailure::Interrupted);
            }
            return Err(error);
        }
        Ok(())
    }

    fn finish_renewal(&mut self, frame: Elr2Frame) -> ClientResult<ReconnectTicketResponse> {
        if frame.kind == FrameKind::Error {
            let error = EluraProtocol::decode_error(&frame)?;
            if requires_reauthentication(&error) {
                self.require_reauthentication();
                return Err(ClientError::ReauthenticationRequired);
            }
            return Err(ClientError::Server(error));
        }
        let reconnect = EluraProtocol::decode_reconnect_ticket(&frame)?;
        self.authentication.reconnect = reconnect.clone();
        self.publish_session();
        Ok(reconnect)
    }

    async fn try_automatic_reconnect(&mut self) {
        let result = self
            .connect_with_ticket(self.authentication.reconnect.ticket.clone())
            .await;
        if result.is_ok() || self.current_state() == ConnectionState::ReauthenticationRequired {
            return;
        }
        self.reconnect_attempt = self.reconnect_attempt.saturating_add(1);
        if self
            .config
            .reconnect_max_attempts
            .is_some_and(|maximum| self.reconnect_attempt > maximum)
        {
            self.reconnect_at = None;
            self.set_state(ConnectionState::Disconnected);
            self.emit(ClientEvent::ReconnectExhausted);
            return;
        }
        let delay = reconnect_delay(&self.config, self.reconnect_attempt, self.reconnect_seed);
        self.reconnect_at = Some(Instant::now() + delay);
        self.emit(ClientEvent::Reconnecting {
            attempt: self.reconnect_attempt,
            delay,
        });
    }

    async fn connect_with_ticket(&mut self, ticket: String) -> ClientResult<AuthenticateResponse> {
        match open_and_authenticate(
            &self.address,
            ticket,
            &self.config,
            &self.events,
            &mut self.next_request_id,
        )
        .await
        {
            Ok((connection, authentication)) => {
                self.connection = Some(connection);
                self.authentication = authentication.clone();
                self.reconnect_seed = reconnect_seed(&self.address, &authentication.session_id);
                self.reconnect_attempt = 0;
                self.reconnect_at = None;
                self.publish_session();
                self.set_state(ConnectionState::Connected);
                self.emit(ClientEvent::Reconnected);
                Ok(authentication)
            }
            Err(ClientError::Server(error)) if requires_reauthentication(&error) => {
                self.require_reauthentication();
                Err(ClientError::ReauthenticationRequired)
            }
            Err(error) => Err(error),
        }
    }

    fn connection_lost(&mut self) {
        self.connection = None;
        self.fail_pending(ClientError::RequestInterrupted);
        if self.current_state() == ConnectionState::Connected {
            self.emit(ClientEvent::Disconnected);
        }
        if self.config.automatic_reconnect {
            self.schedule_reconnect(1);
        } else {
            self.set_state(ConnectionState::Disconnected);
            self.reconnect_at = None;
        }
    }

    fn disconnect_for_reconnect(&mut self) {
        self.connection = None;
        self.fail_pending(ClientError::RequestInterrupted);
        self.reconnect_at = None;
        self.set_state(ConnectionState::Reconnecting);
    }

    fn schedule_reconnect(&mut self, attempt: u32) {
        self.set_state(ConnectionState::Reconnecting);
        self.reconnect_attempt = attempt;
        let delay = reconnect_delay(&self.config, attempt, self.reconnect_seed);
        self.reconnect_at = Some(Instant::now() + delay);
        self.emit(ClientEvent::Reconnecting { attempt, delay });
    }

    fn fail_pending(&mut self, error: ClientError) {
        let failure = match error {
            ClientError::RequestInterrupted => PendingFailure::Interrupted,
            ClientError::RequestTimeout => PendingFailure::Timeout,
            ClientError::ReauthenticationRequired => PendingFailure::ReauthenticationRequired,
            ClientError::ReconnectExhausted => PendingFailure::ReconnectExhausted,
            _ => PendingFailure::Closed,
        };
        for (_, pending) in self.pending.drain() {
            Self::complete_pending(pending, failure);
        }
    }

    fn complete_pending(pending: Pending, failure: PendingFailure) {
        match pending.kind {
            PendingKind::Application(response) => {
                let _ = response.send(Err(failure.into_error()));
            }
            PendingKind::Renewal(response) => {
                if let Some(response) = response {
                    let _ = response.send(Err(failure.into_error()));
                }
            }
        }
    }

    fn has_pending_renewal(&self) -> bool {
        self.pending
            .values()
            .any(|pending| matches!(pending.kind, PendingKind::Renewal(_)))
    }

    async fn send_frame(&mut self, frame: Elr2Frame) -> ClientResult<()> {
        let state = self.current_state();
        self.connection
            .as_mut()
            .ok_or(ClientError::NotConnected(state))?
            .send(frame)
            .await
            .map_err(ClientError::Transport)
    }

    fn publish_session(&mut self) {
        let snapshot = session_snapshot(self.authentication.clone());
        self.reconnect_renew_at = renewal_time(&snapshot, self.config.reconnect_renewal_margin);
        self.session.send_replace(Some(snapshot));
    }

    fn require_reauthentication(&mut self) {
        self.connection = None;
        self.reconnect_at = None;
        self.fail_pending(ClientError::ReauthenticationRequired);
        self.session.send_replace(None);
        self.set_state(ConnectionState::ReauthenticationRequired);
        self.emit(ClientEvent::ReauthenticationRequired);
    }

    fn apply_event_state(&mut self, event: &ClientEvent) {
        if let ClientEvent::SessionControl(control) = event
            && control.action != SessionControlAction::ServerDraining
        {
            self.require_reauthentication();
        }
    }

    fn set_state(&self, state: ConnectionState) {
        self.state.send_replace(state);
    }

    fn current_state(&self) -> ConnectionState {
        *self.state.borrow()
    }

    fn not_connected_error(&self) -> ClientError {
        match self.current_state() {
            ConnectionState::ReauthenticationRequired => ClientError::ReauthenticationRequired,
            ConnectionState::Closed => ClientError::Closed,
            state => ClientError::NotConnected(state),
        }
    }

    fn emit(&self, event: ClientEvent) {
        let _ = self.events.send(event);
    }

    fn take_request_id(&mut self) -> u64 {
        let request_id = self.next_request_id;
        self.next_request_id = self.next_request_id.checked_add(1).unwrap_or(1);
        request_id
    }

    fn finish(&mut self) {
        self.connection = None;
        self.fail_pending(ClientError::Closed);
        self.set_state(ConnectionState::Closed);
        self.session.send_replace(None);
    }
}

async fn open_and_authenticate(
    address: &str,
    ticket: String,
    config: &ClientConfig,
    events: &broadcast::Sender<ClientEvent>,
    next_request_id: &mut u64,
) -> ClientResult<(Framed<TcpStream, Elr2Codec>, AuthenticateResponse)> {
    let stream = tokio::time::timeout(config.connect_timeout, TcpStream::connect(address))
        .await
        .map_err(|_| ClientError::ConnectTimeout)??;
    stream.set_nodelay(config.tcp_nodelay)?;
    let mut connection = Framed::new(stream, Elr2Codec::new(config.max_payload)?);
    let request_id = *next_request_id;
    *next_request_id = (*next_request_id).checked_add(1).unwrap_or(1);
    let request = EluraProtocol::authenticate(request_id, ticket)?;
    tokio::time::timeout(
        config.request_timeout,
        authenticate_connection(&mut connection, request, events),
    )
    .await
    .map_err(|_| ClientError::RequestTimeout)?
    .map(|authentication| (connection, authentication))
}

async fn authenticate_connection(
    connection: &mut Framed<TcpStream, Elr2Codec>,
    request: Elr2Frame,
    events: &broadcast::Sender<ClientEvent>,
) -> ClientResult<AuthenticateResponse> {
    let request_id = request.request_id;
    connection.send(request).await?;
    loop {
        let frame = connection.next().await.ok_or(ClientError::Closed)??;
        if frame.kind == FrameKind::Request && frame.route == EluraRoutes::HEARTBEAT {
            connection
                .send(EluraProtocol::heartbeat_response(&frame)?)
                .await?;
            continue;
        }
        if frame.kind == FrameKind::Push {
            let event = decode_event(frame)?;
            let _ = events.send(event);
            continue;
        }
        if frame.request_id != request_id || frame.route != EluraRoutes::AUTHENTICATE {
            return Err(ClientError::UnexpectedFrame(
                "authentication response does not match its request".into(),
            ));
        }
        return match frame.kind {
            FrameKind::Response => EluraProtocol::decode_authenticate(&frame).map_err(Into::into),
            FrameKind::Error => Err(ClientError::Server(EluraProtocol::decode_error(&frame)?)),
            _ => Err(ClientError::UnexpectedFrame(
                "expected authentication response or error".into(),
            )),
        };
    }
}

fn session_snapshot(authentication: AuthenticateResponse) -> SessionSnapshot {
    let reconnect_expires_at =
        Instant::now() + Duration::from_secs(authentication.reconnect.expires_in_seconds);
    SessionSnapshot {
        authentication,
        reconnect_expires_at,
    }
}

#[derive(Clone, Copy)]
enum PendingFailure {
    Interrupted,
    Timeout,
    ReauthenticationRequired,
    ReconnectExhausted,
    Closed,
}

impl PendingFailure {
    fn into_error(self) -> ClientError {
        match self {
            Self::Interrupted => ClientError::RequestInterrupted,
            Self::Timeout => ClientError::RequestTimeout,
            Self::ReauthenticationRequired => ClientError::ReauthenticationRequired,
            Self::ReconnectExhausted => ClientError::ReconnectExhausted,
            Self::Closed => ClientError::Closed,
        }
    }
}

fn renewal_time(snapshot: &SessionSnapshot, configured_margin: Duration) -> Instant {
    let ttl = Duration::from_secs(snapshot.authentication.reconnect.expires_in_seconds);
    let margin = configured_margin.min(ttl / 2);
    snapshot.reconnect_expires_at - margin
}

fn reconnect_seed(address: &str, session_id: &str) -> u64 {
    let mut hasher = DefaultHasher::new();
    address.hash(&mut hasher);
    session_id.hash(&mut hasher);
    hasher.finish()
}

fn reconnect_delay(config: &ClientConfig, attempt: u32, seed: u64) -> Duration {
    let exponent = attempt.saturating_sub(1).min(31);
    let base = config
        .reconnect_initial_delay
        .saturating_mul(1_u32 << exponent)
        .min(config.reconnect_max_delay);
    if config.reconnect_jitter_percent == 0 {
        return base;
    }

    // SplitMix64 gives each client and attempt a stable, well-distributed offset without
    // adding an RNG or synchronization to the request path.
    let mut mixed = seed.wrapping_add(u64::from(attempt).wrapping_mul(0x9e37_79b9_7f4a_7c15));
    mixed = (mixed ^ (mixed >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    mixed = (mixed ^ (mixed >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    mixed ^= mixed >> 31;

    let jitter = u64::from(config.reconnect_jitter_percent);
    let factor = 100 - jitter + mixed % (jitter.saturating_mul(2) + 1);
    base.mul_f64(factor as f64 / 100.0)
        .min(config.reconnect_max_delay)
}

fn is_connection_loss(error: &ClientError) -> bool {
    matches!(
        error,
        ClientError::Transport(_)
            | ClientError::ConnectTimeout
            | ClientError::RequestTimeout
            | ClientError::Closed
    )
}

fn requires_reauthentication(error: &ErrorEnvelope) -> bool {
    matches!(error.code.as_str(), "UNAUTHENTICATED" | "SESSION_REVOKED")
}

fn decode_event(frame: Elr2Frame) -> ClientResult<ClientEvent> {
    if frame.route == EluraRoutes::SESSION_CONTROL {
        return Ok(ClientEvent::SessionControl(SessionControlCodec::decode(
            &frame.payload,
        )?));
    }
    Ok(ClientEvent::Push(frame))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reconnect_jitter_is_bounded_and_distributed() {
        let config = ClientConfig {
            reconnect_initial_delay: Duration::from_millis(1_000),
            reconnect_max_delay: Duration::from_secs(30),
            reconnect_jitter_percent: 20,
            ..ClientConfig::default()
        };
        let delays = (0..1_000)
            .map(|seed| reconnect_delay(&config, 1, seed))
            .collect::<Vec<_>>();

        assert!(
            delays
                .iter()
                .all(|delay| *delay >= Duration::from_millis(800))
        );
        assert!(
            delays
                .iter()
                .all(|delay| *delay <= Duration::from_millis(1_200))
        );
        assert!(delays.windows(2).any(|pair| pair[0] != pair[1]));
    }

    #[test]
    fn reconnect_jitter_can_be_disabled() {
        let config = ClientConfig {
            reconnect_initial_delay: Duration::from_millis(250),
            reconnect_max_delay: Duration::from_secs(10),
            reconnect_jitter_percent: 0,
            ..ClientConfig::default()
        };

        assert_eq!(reconnect_delay(&config, 4, 123), Duration::from_secs(2));
    }
}
