//! WebSocket endpoint backed by the shared Gateway Session engine.

use std::collections::HashSet;
use std::io;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use axum::extract::connect_info::Connected;
use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::extract::{ConnectInfo, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::{Router, serve::IncomingStream};
use bytes::{Bytes, BytesMut};
use elura_core::protocol::{Frame, FrameCodec, HEADER_LEN, PROTOCOL_IDENTIFIER};
use elura_core::{Error, Result};
use futures_util::{SinkExt, StreamExt};
use tokio::net::TcpListener;
use tokio::sync::{Semaphore, mpsc, watch};
use tokio::task::JoinHandle;
use tokio_util::codec::Encoder;
use tracing::warn;

use elura_runtime::internal::{BoxedInternalStream, ServerTlsConfig};

use super::session::next_outbound;
use super::{SessionConnection, SessionService, TrustedProxies};

#[derive(Clone)]
pub struct WebSocketConfig {
    pub listen: SocketAddr,
    pub path: String,
    pub subprotocol: String,
    pub allowed_origins: Vec<String>,
    pub allow_missing_origin: bool,
    pub trusted_proxies: TrustedProxies,
    pub max_payload: usize,
    pub inbound_capacity: usize,
    pub response_capacity: usize,
    pub push_capacity: usize,
    pub write_timeout: Duration,
    pub tcp_keepalive: Duration,
    pub tls_handshake_timeout: Duration,
    pub max_pending_handshakes: usize,
    pub tls: Option<ServerTlsConfig>,
}

impl Default for WebSocketConfig {
    fn default() -> Self {
        Self {
            listen: "127.0.0.1:17001".parse().expect("static address"),
            path: "/elura/game".into(),
            subprotocol: PROTOCOL_IDENTIFIER.into(),
            allowed_origins: Vec::new(),
            allow_missing_origin: false,
            trusted_proxies: TrustedProxies::default(),
            max_payload: 1 << 20,
            inbound_capacity: 64,
            response_capacity: 64,
            push_capacity: 64,
            write_timeout: Duration::from_secs(10),
            tcp_keepalive: Duration::from_secs(30),
            tls_handshake_timeout: Duration::from_secs(5),
            max_pending_handshakes: 1024,
            tls: None,
        }
    }
}

impl WebSocketConfig {
    pub fn validate(&self) -> Result<()> {
        if !self.path.starts_with('/') || self.path.contains(['?', '#']) {
            return Err(Error::InvalidConfig("invalid WebSocket path".into()));
        }
        if self.subprotocol.trim().is_empty()
            || self.max_payload == 0
            || self.inbound_capacity == 0
            || self.response_capacity == 0
            || self.push_capacity == 0
            || self.write_timeout.is_zero()
            || self.tcp_keepalive.is_zero()
            || self.tls_handshake_timeout.is_zero()
            || self.max_pending_handshakes == 0
        {
            return Err(Error::InvalidConfig(
                "invalid WebSocket limits or subprotocol".into(),
            ));
        }
        Ok(())
    }
}

#[derive(Clone)]
struct WebSocketState {
    service: Arc<dyn SessionService>,
    config: Arc<WebSocketConfig>,
    origins: Arc<HashSet<String>>,
}

pub(crate) async fn serve_websocket(
    config: WebSocketConfig,
    service: Arc<dyn SessionService>,
    mut shutdown: watch::Receiver<bool>,
) -> Result<()> {
    config.validate()?;
    let path = config.path.clone();
    let origins = config
        .allowed_origins
        .iter()
        .map(|origin| origin.trim_end_matches('/').to_ascii_lowercase())
        .collect();
    let state = WebSocketState {
        service,
        config: Arc::new(config.clone()),
        origins: Arc::new(origins),
    };
    let app = Router::new().route(&path, get(upgrade)).with_state(state);
    let listener = WebSocketListener::bind(&config).await?;
    axum::serve(
        listener,
        app.into_make_service_with_connect_info::<PeerAddress>(),
    )
    .with_graceful_shutdown(async move {
        if *shutdown.borrow() {
            return;
        }
        while shutdown.changed().await.is_ok() {
            if *shutdown.borrow() {
                break;
            }
        }
    })
    .await
    .map_err(io::Error::other)?;
    Ok(())
}

async fn upgrade(
    State(state): State<WebSocketState>,
    ConnectInfo(peer): ConnectInfo<PeerAddress>,
    headers: HeaderMap,
    upgrade: WebSocketUpgrade,
) -> Response {
    if !origin_allowed(&state, &headers) {
        return (StatusCode::FORBIDDEN, "WebSocket origin rejected").into_response();
    }
    let offered = headers
        .get(axum::http::header::SEC_WEBSOCKET_PROTOCOL)
        .and_then(|value| value.to_str().ok())
        .is_some_and(|value| {
            value
                .split(',')
                .any(|item| item.trim() == state.config.subprotocol)
        });
    if !offered {
        return (
            StatusCode::BAD_REQUEST,
            "required WebSocket subprotocol was not offered",
        )
            .into_response();
    }
    let protocol = state.config.subprotocol.clone();
    let client_peer = headers
        .get("x-forwarded-for")
        .and_then(|value| value.to_str().ok())
        .map_or(peer.0, |value| {
            state
                .config
                .trusted_proxies
                .forwarded_address(peer.0, value)
        });
    upgrade
        .max_message_size(state.config.max_payload.saturating_add(HEADER_LEN))
        .max_frame_size(state.config.max_payload.saturating_add(HEADER_LEN))
        .protocols([protocol])
        .on_upgrade(move |socket| connection(socket, client_peer, state))
}

fn origin_allowed(state: &WebSocketState, headers: &HeaderMap) -> bool {
    let Some(origin) = headers
        .get(axum::http::header::ORIGIN)
        .and_then(|value| value.to_str().ok())
    else {
        return state.config.allow_missing_origin;
    };
    if state.origins.is_empty() {
        let Some(host) = headers
            .get(axum::http::header::HOST)
            .and_then(|value| value.to_str().ok())
        else {
            return false;
        };
        return origin
            .split_once("://")
            .is_some_and(|(_, origin_host)| origin_host.eq_ignore_ascii_case(host));
    }
    state
        .origins
        .contains(&origin.trim_end_matches('/').to_ascii_lowercase())
}

async fn connection(socket: WebSocket, peer: SocketAddr, state: WebSocketState) {
    let (mut socket_tx, mut socket_rx) = socket.split();
    let (inbound_tx, inbound) = mpsc::channel(state.config.inbound_capacity);
    let (responses, mut response_rx) = mpsc::channel::<Frame>(state.config.response_capacity);
    let (pushes, mut push_rx) = mpsc::channel::<Frame>(state.config.push_capacity);
    let max_payload = state.config.max_payload;
    let write_timeout = state.config.write_timeout;

    let reader = tokio::spawn(async move {
        while let Some(result) = socket_rx.next().await {
            let result = match result {
                Ok(Message::Binary(binary)) => decode_message(binary, max_payload),
                Ok(Message::Close(_)) => break,
                Ok(Message::Ping(_) | Message::Pong(_)) => continue,
                Ok(_) => Err(Error::InvalidFrame(
                    "WebSocket game messages must be binary".into(),
                )),
                Err(error) => Err(Error::Io(io::Error::other(error))),
            };
            let failed = result.is_err();
            if inbound_tx.send(result).await.is_err() || failed {
                break;
            }
        }
    });
    let writer = tokio::spawn(async move {
        let mut codec = FrameCodec::new(max_payload)?;
        while let Some(frame) = next_outbound(&mut response_rx, &mut push_rx).await {
            let mut output = BytesMut::new();
            codec.encode(frame, &mut output)?;
            tokio::time::timeout(
                write_timeout,
                socket_tx.send(Message::Binary(output.freeze())),
            )
            .await
            .map_err(|_| Error::Timeout)?
            .map_err(|error| Error::Io(io::Error::other(error)))?;
        }
        Result::<()>::Ok(())
    });
    if let Err(error) = state
        .service
        .serve_session(SessionConnection {
            peer,
            inbound,
            responses,
            pushes,
        })
        .await
    {
        warn!(%peer, %error, "WebSocket session closed");
    }
    reader.abort();
    let mut writer = writer;
    if tokio::time::timeout(Duration::from_millis(250), &mut writer)
        .await
        .is_err()
    {
        writer.abort();
    }
}

fn decode_message(binary: Bytes, max_payload: usize) -> Result<Frame> {
    // Axum owns WebSocket binary messages as `Bytes`. Pass that allocation
    // straight through so `FrameCodec` can slice off the header in place.
    Ok(FrameCodec::new(max_payload)?.decode_message(binary)?)
}

struct WebSocketListener {
    incoming: mpsc::Receiver<(BoxedInternalStream, SocketAddr)>,
    task: JoinHandle<()>,
    local_addr: SocketAddr,
}

impl WebSocketListener {
    async fn bind(config: &WebSocketConfig) -> Result<Self> {
        let listener = TcpListener::bind(config.listen).await?;
        let local_addr = listener.local_addr()?;
        let (sender, incoming) = mpsc::channel(config.max_pending_handshakes);
        let permits = Arc::new(Semaphore::new(config.max_pending_handshakes));
        let tls = config.tls.clone();
        let handshake_timeout = config.tls_handshake_timeout;
        let tcp_keepalive = config.tcp_keepalive;
        let task = tokio::spawn(async move {
            loop {
                let (stream, peer) = match listener.accept().await {
                    Ok(connection) => connection,
                    Err(error) => {
                        warn!(%error, "WebSocket accept failed");
                        tokio::time::sleep(Duration::from_millis(50)).await;
                        continue;
                    }
                };
                if let Err(error) = super::tcp::configure_stream(&stream, tcp_keepalive) {
                    warn!(%peer, %error, "WebSocket TCP configuration rejected");
                    continue;
                }
                let Ok(permit) = permits.clone().try_acquire_owned() else {
                    warn!(%peer, "WebSocket handshake limit reached");
                    continue;
                };
                let sender = sender.clone();
                let tls = tls.clone();
                tokio::spawn(async move {
                    let _permit = permit;
                    let stream: Result<BoxedInternalStream> = match tls {
                        Some(tls) => tokio::time::timeout(handshake_timeout, tls.accept(stream))
                            .await
                            .map_err(|_| Error::Timeout)
                            .and_then(|result| result),
                        None => Ok(Box::new(stream)),
                    };
                    match stream {
                        Ok(stream) => {
                            let _ = sender.send((stream, peer)).await;
                        }
                        Err(error) => warn!(%peer, %error, "WSS handshake rejected"),
                    }
                });
            }
        });
        Ok(Self {
            incoming,
            task,
            local_addr,
        })
    }
}

impl Drop for WebSocketListener {
    fn drop(&mut self) {
        self.task.abort();
    }
}

impl axum::serve::Listener for WebSocketListener {
    type Io = BoxedInternalStream;
    type Addr = SocketAddr;

    async fn accept(&mut self) -> (Self::Io, Self::Addr) {
        match self.incoming.recv().await {
            Some(connection) => connection,
            None => std::future::pending().await,
        }
    }

    fn local_addr(&self) -> io::Result<Self::Addr> {
        Ok(self.local_addr)
    }
}

#[derive(Clone, Copy)]
struct PeerAddress(SocketAddr);

impl Connected<IncomingStream<'_, WebSocketListener>> for PeerAddress {
    fn connect_info(stream: IncomingStream<'_, WebSocketListener>) -> Self {
        Self(*stream.remote_addr())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn validates_paths() {
        let config = WebSocketConfig {
            path: "game".into(),
            ..WebSocketConfig::default()
        };
        assert!(config.validate().is_err());
    }

    #[test]
    fn forwarded_ip_requires_a_trusted_peer() {
        let trusted = TrustedProxies::parse(["10.0.0.0/8"]).unwrap();
        let proxy = "10.1.2.3:443".parse().unwrap();
        assert_eq!(
            trusted.forwarded_address(proxy, "192.0.2.4, 10.0.0.8").ip(),
            "192.0.2.4".parse::<std::net::IpAddr>().unwrap()
        );
        let direct = "203.0.113.9:443".parse().unwrap();
        assert_eq!(
            trusted.forwarded_address(direct, "192.0.2.4").ip(),
            direct.ip()
        );
    }

    #[test]
    fn binary_message_decode_reuses_websocket_bytes() {
        let frame = Frame::request(100, 7, Bytes::from_static(b"hello")).unwrap();
        let mut codec = FrameCodec::default();
        let mut encoded = BytesMut::new();
        codec.encode(frame, &mut encoded).unwrap();
        let binary = encoded.freeze();
        let expected_payload = binary.slice(elura_core::protocol::HEADER_LEN..);

        let decoded = decode_message(binary, 1024).unwrap();

        assert_eq!(decoded.payload, expected_payload);
        assert_eq!(decoded.payload.as_ptr(), expected_payload.as_ptr());
    }
}
