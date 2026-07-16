//! WebSocket endpoint backed by the shared Gateway Session engine.

use std::collections::HashSet;
use std::io;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll, ready};
use std::time::Duration;

use async_trait::async_trait;
use axum::extract::connect_info::Connected;
use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::extract::{ConnectInfo, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::{Router, serve::IncomingStream};
use bytes::Bytes;
use elura_core::protocol::{HEADER_LEN, PROTOCOL_IDENTIFIER};
use elura_core::{Error, Result};
use futures_util::{Sink, Stream};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::net::TcpListener;
use tokio::sync::{Semaphore, mpsc};
use tokio::task::JoinHandle;
use tracing::warn;

use elura_runtime::security::{BoxedServiceStream, ServerTlsConfig};

use super::{GatewayTransportListener, TrustedProxies};

#[derive(Clone)]
#[non_exhaustive]
pub struct WebSocketConfig {
    pub listen: SocketAddr,
    pub path: String,
    pub subprotocol: String,
    pub allowed_origins: Vec<String>,
    pub allow_missing_origin: bool,
    pub trusted_proxies: TrustedProxies,
    pub tcp_keepalive: Duration,
    pub tls_handshake_timeout: Duration,
    pub max_pending_handshakes: usize,
    pub tls: Option<ServerTlsConfig>,
}

impl Default for WebSocketConfig {
    fn default() -> Self {
        Self {
            listen: "127.0.0.1:17002".parse().expect("static address"),
            path: "/elura/game".into(),
            subprotocol: PROTOCOL_IDENTIFIER.into(),
            allowed_origins: Vec::new(),
            allow_missing_origin: false,
            trusted_proxies: TrustedProxies::default(),
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
    incoming: mpsc::Sender<Result<(SocketAddr, WebSocketIo)>>,
    config: Arc<WebSocketConfig>,
    origins: Arc<HashSet<String>>,
}

#[doc(hidden)]
pub struct WebSocketGatewayListener {
    incoming: mpsc::Receiver<Result<(SocketAddr, WebSocketIo)>>,
    task: JoinHandle<()>,
}

pub(crate) async fn bind(config: WebSocketConfig) -> Result<WebSocketGatewayListener> {
    config.validate()?;
    let path = config.path.clone();
    let origins = config
        .allowed_origins
        .iter()
        .map(|origin| origin.trim_end_matches('/').to_ascii_lowercase())
        .collect();
    let (sender, incoming) = mpsc::channel(config.max_pending_handshakes);
    let state = WebSocketState {
        incoming: sender.clone(),
        config: Arc::new(config.clone()),
        origins: Arc::new(origins),
    };
    let app = Router::new().route(&path, get(upgrade)).with_state(state);
    let listener = WebSocketListener::bind(&config).await?;
    let task = tokio::spawn(async move {
        let result = axum::serve(
            listener,
            app.into_make_service_with_connect_info::<PeerAddress>(),
        )
        .await
        .map_err(|error| Error::Io(io::Error::other(error)));
        if let Err(error) = result {
            let _ = sender.send(Err(error)).await;
        }
    });
    Ok(WebSocketGatewayListener { incoming, task })
}

#[async_trait]
impl GatewayTransportListener for WebSocketGatewayListener {
    type Io = WebSocketIo;

    async fn accept(&mut self) -> Result<(SocketAddr, Self::Io)> {
        self.incoming.recv().await.ok_or(Error::Unavailable)?
    }
}

impl Drop for WebSocketGatewayListener {
    fn drop(&mut self) {
        self.task.abort();
    }
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
    let max_frame_bytes = (64 << 20) + HEADER_LEN;
    upgrade
        .max_message_size(max_frame_bytes)
        .max_frame_size(max_frame_bytes)
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
    let stream = WebSocketIo {
        socket,
        buffered: Bytes::new(),
    };
    let _ = state.incoming.send(Ok((peer, stream))).await;
}

#[doc(hidden)]
pub struct WebSocketIo {
    socket: WebSocket,
    buffered: Bytes,
}

impl AsyncRead for WebSocketIo {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        output: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        loop {
            if !self.buffered.is_empty() {
                let length = output.remaining().min(self.buffered.len());
                output.put_slice(&self.buffered.split_to(length));
                return Poll::Ready(Ok(()));
            }
            match ready!(Pin::new(&mut self.socket).poll_next(cx)) {
                Some(Ok(Message::Binary(binary))) => self.buffered = binary,
                Some(Ok(Message::Close(_))) | None => return Poll::Ready(Ok(())),
                Some(Ok(Message::Ping(_) | Message::Pong(_))) => {}
                Some(Ok(_)) => {
                    return Poll::Ready(Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "WebSocket game messages must be binary",
                    )));
                }
                Some(Err(error)) => return Poll::Ready(Err(io::Error::other(error))),
            }
        }
    }
}

impl AsyncWrite for WebSocketIo {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        input: &[u8],
    ) -> Poll<io::Result<usize>> {
        ready!(Pin::new(&mut self.socket).poll_ready(cx)).map_err(io::Error::other)?;
        Pin::new(&mut self.socket)
            .start_send(Message::Binary(Bytes::copy_from_slice(input)))
            .map_err(io::Error::other)?;
        Poll::Ready(Ok(input.len()))
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.socket)
            .poll_flush(cx)
            .map_err(io::Error::other)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.socket)
            .poll_close(cx)
            .map_err(io::Error::other)
    }
}

struct WebSocketListener {
    incoming: mpsc::Receiver<(BoxedServiceStream, SocketAddr)>,
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
                    let stream: Result<BoxedServiceStream> = match tls {
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
    type Io = BoxedServiceStream;
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
}
