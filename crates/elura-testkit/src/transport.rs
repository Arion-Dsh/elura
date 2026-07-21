use std::io;
use std::net::{IpAddr, Ipv4Addr, SocketAddr, TcpListener};

use async_trait::async_trait;
use bytes::BytesMut;
use elura_core::protocol::{Frame, FrameCodec};
use elura_core::{Error, Result};
use elura_gateway::transport::{GatewayTransport, TcpConfig, TcpTransport, WebSocketConfig};
use futures_util::{SinkExt, StreamExt};
use tokio::net::TcpStream;
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::http::HeaderValue;
use tokio_util::codec::{Decoder, Encoder, Framed};

/// One client-side connection for a selectable Gateway transport.
#[async_trait]
pub trait TestConnection: Send + 'static {
    /// Sends one ELR2 frame through the real client transport.
    async fn send(&mut self, frame: Frame) -> Result<()>;
    /// Receives one ELR2 frame through the real client transport.
    async fn receive(&mut self) -> Result<Frame>;
}

/// A paired server transport and client connector used by the full-stack harness.
#[async_trait]
pub trait TestTransport: Clone + Send + Sync + 'static {
    /// Server-side transport registered on the real Gateway.
    type Server: GatewayTransport;

    /// Stable name included in load reports.
    fn name(&self) -> &'static str;
    /// Builds the server endpoint registered with Gateway.
    fn server(&self) -> Result<Self::Server>;
    /// Opens one real client connection to that endpoint.
    async fn connect(&self) -> Result<Box<dyn TestConnection>>;
}

/// Returns an unused loopback TCP address for local test servers.
///
/// The socket is released before return, so callers should bind it promptly.
pub fn loopback_address() -> Result<SocketAddr> {
    let listener = TcpListener::bind((IpAddr::V4(Ipv4Addr::LOCALHOST), 0))?;
    Ok(listener.local_addr()?)
}

/// Built-in raw TCP test transport.
#[derive(Clone)]
pub struct TcpTestTransport {
    config: TcpConfig,
}

impl TcpTestTransport {
    /// Creates a TCP transport on an automatically selected loopback address.
    pub fn loopback() -> Result<Self> {
        let mut config = TcpConfig::default();
        config.listen = loopback_address()?;
        Ok(Self { config })
    }

    /// Wraps an explicit Gateway TCP configuration.
    pub fn new(config: TcpConfig) -> Self {
        Self { config }
    }

    /// Returns the configured server address.
    pub fn address(&self) -> SocketAddr {
        self.config.listen
    }
}

#[async_trait]
impl TestTransport for TcpTestTransport {
    type Server = TcpTransport;

    fn name(&self) -> &'static str {
        "tcp"
    }

    fn server(&self) -> Result<Self::Server> {
        TcpTransport::new(self.config.clone())
    }

    async fn connect(&self) -> Result<Box<dyn TestConnection>> {
        let stream = TcpStream::connect(self.config.listen).await?;
        stream.set_nodelay(true)?;
        Ok(Box::new(TcpConnection(Framed::new(
            stream,
            FrameCodec::default(),
        ))))
    }
}

struct TcpConnection(Framed<TcpStream, FrameCodec>);

#[async_trait]
impl TestConnection for TcpConnection {
    async fn send(&mut self, frame: Frame) -> Result<()> {
        self.0.send(frame).await.map_err(Error::from)
    }

    async fn receive(&mut self) -> Result<Frame> {
        self.0
            .next()
            .await
            .ok_or(Error::Unavailable)?
            .map_err(Error::from)
    }
}

/// Built-in WebSocket test transport using binary ELR2 messages.
#[derive(Clone)]
pub struct WebSocketTestTransport {
    config: WebSocketConfig,
}

impl WebSocketTestTransport {
    /// Creates a WebSocket transport on an automatically selected loopback address.
    pub fn loopback() -> Result<Self> {
        let mut config = WebSocketConfig::default();
        config.listen = loopback_address()?;
        config.allow_missing_origin = true;
        Ok(Self { config })
    }

    /// Wraps an explicit Gateway WebSocket configuration.
    pub fn new(config: WebSocketConfig) -> Self {
        Self { config }
    }

    /// Returns the configured server address.
    pub fn address(&self) -> SocketAddr {
        self.config.listen
    }
}

#[async_trait]
impl TestTransport for WebSocketTestTransport {
    type Server = WebSocketConfig;

    fn name(&self) -> &'static str {
        "websocket"
    }

    fn server(&self) -> Result<Self::Server> {
        Ok(self.config.clone())
    }

    async fn connect(&self) -> Result<Box<dyn TestConnection>> {
        let url = format!("ws://{}{}", self.config.listen, self.config.path);
        let mut request = url
            .into_client_request()
            .map_err(|error| Error::Io(io::Error::other(error)))?;
        request.headers_mut().insert(
            "sec-websocket-protocol",
            HeaderValue::from_str(&self.config.subprotocol)
                .map_err(|error| Error::Io(io::Error::other(error)))?,
        );
        let (socket, _) = connect_async(request)
            .await
            .map_err(|error| Error::Io(io::Error::other(error)))?;
        Ok(Box::new(WebSocketConnection {
            socket,
            codec: FrameCodec::default(),
            buffered: BytesMut::new(),
        }))
    }
}

type ClientWebSocket =
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<TcpStream>>;

struct WebSocketConnection {
    socket: ClientWebSocket,
    codec: FrameCodec,
    buffered: BytesMut,
}

#[async_trait]
impl TestConnection for WebSocketConnection {
    async fn send(&mut self, frame: Frame) -> Result<()> {
        let mut encoded = BytesMut::new();
        self.codec.encode(frame, &mut encoded)?;
        self.socket
            .send(Message::Binary(encoded.freeze()))
            .await
            .map_err(|error| Error::Io(io::Error::other(error)))
    }

    async fn receive(&mut self) -> Result<Frame> {
        loop {
            if let Some(frame) = self.codec.decode(&mut self.buffered)? {
                return Ok(frame);
            }
            match self.socket.next().await {
                Some(Ok(Message::Binary(bytes))) => self.buffered.extend_from_slice(&bytes),
                Some(Ok(Message::Ping(payload))) => {
                    self.socket
                        .send(Message::Pong(payload))
                        .await
                        .map_err(|error| Error::Io(io::Error::other(error)))?;
                }
                Some(Ok(Message::Close(_))) | None => return Err(Error::Unavailable),
                Some(Ok(_)) => {}
                Some(Err(error)) => return Err(Error::Io(io::Error::other(error))),
            }
        }
    }
}
