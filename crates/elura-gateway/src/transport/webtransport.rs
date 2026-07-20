//! WebTransport-over-HTTP/3 endpoint backed by the shared Gateway Session engine.

use std::collections::HashSet;
use std::io;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use elura_core::protocol::{FrameCodec, HEADER_LEN};
use elura_core::{Error, Result};
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::sync::{Semaphore, mpsc};
use tokio::task::JoinHandle;
use tracing::debug;
use wtransport::endpoint::IncomingSession;
use wtransport::stream::BiStream;
use wtransport::{Connection, Endpoint, Identity, ServerConfig};

use super::GatewayTransportListener;

const MAX_WEBTRANSPORT_DATAGRAM_BYTES: usize = 65_507;

/// ELR2 channel used inside one WebTransport Session.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum WebTransportMode {
    /// The client opens one bidirectional stream carrying the ELR2 byte stream.
    #[default]
    ReliableStream,
    /// Every WebTransport Datagram carries exactly one complete ELR2 frame.
    Datagram,
}

/// Configuration for a browser-compatible WebTransport endpoint.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
#[non_exhaustive]
pub struct WebTransportConfig {
    /// UDP address on which HTTP/3 connections are accepted.
    pub listen: SocketAddr,
    /// HTTP path accepted by the WebTransport CONNECT request.
    pub path: String,
    /// Explicitly allowed browser origins.
    ///
    /// If empty, the request Origin must match the request authority.
    pub allowed_origins: Vec<String>,
    /// Allows non-browser clients that omit the Origin header.
    pub allow_missing_origin: bool,
    /// PEM-encoded TLS certificate chain.
    pub certificate_file: PathBuf,
    /// PEM-encoded TLS private key.
    pub key_file: PathBuf,
    /// ELR2 channel selected for each accepted Session.
    pub mode: WebTransportMode,
    /// Maximum time allowed for the HTTP/3 handshake and Session request.
    pub handshake_timeout: Duration,
    /// Maximum time allowed for a reliable client stream to be opened.
    pub stream_open_timeout: Duration,
    /// QUIC connection idle timeout.
    pub idle_timeout: Duration,
    /// Optional QUIC keep-alive interval.
    pub keep_alive_interval: Option<Duration>,
    /// Maximum number of handshakes prepared concurrently.
    pub max_pending_handshakes: usize,
    /// Largest ELR2 frame accepted in Datagram mode.
    pub max_datagram_bytes: usize,
    /// Number of validated Datagrams buffered for one Session.
    pub datagram_queue: usize,
}

impl Default for WebTransportConfig {
    fn default() -> Self {
        Self {
            listen: "127.0.0.1:17005".parse().expect("static address"),
            path: "/elura/game".into(),
            allowed_origins: Vec::new(),
            allow_missing_origin: false,
            certificate_file: PathBuf::new(),
            key_file: PathBuf::new(),
            mode: WebTransportMode::ReliableStream,
            handshake_timeout: Duration::from_secs(5),
            stream_open_timeout: Duration::from_secs(5),
            idle_timeout: Duration::from_secs(90),
            keep_alive_interval: Some(Duration::from_secs(30)),
            max_pending_handshakes: 1024,
            max_datagram_bytes: 1100,
            datagram_queue: 64,
        }
    }
}

impl WebTransportConfig {
    /// Creates an endpoint configuration backed by PEM identity files.
    pub fn from_pem_files(
        listen: SocketAddr,
        certificate_file: impl Into<PathBuf>,
        key_file: impl Into<PathBuf>,
    ) -> Self {
        Self {
            listen,
            certificate_file: certificate_file.into(),
            key_file: key_file.into(),
            ..Self::default()
        }
    }

    /// Validates settings without reading identity files or binding the socket.
    pub fn validate(&self) -> Result<()> {
        if !self.path.starts_with('/')
            || self.path.contains(['?', '#'])
            || self.certificate_file.as_os_str().is_empty()
            || self.key_file.as_os_str().is_empty()
            || self.handshake_timeout.is_zero()
            || self.stream_open_timeout.is_zero()
            || self.idle_timeout.is_zero()
            || self
                .keep_alive_interval
                .is_some_and(|value| value.is_zero())
            || self.max_pending_handshakes == 0
            || !(HEADER_LEN + 1..=MAX_WEBTRANSPORT_DATAGRAM_BYTES)
                .contains(&self.max_datagram_bytes)
            || self.datagram_queue == 0
        {
            return Err(Error::InvalidConfig(
                "invalid WebTransport configuration".into(),
            ));
        }
        Ok(())
    }

    async fn server_config(&self) -> Result<ServerConfig> {
        self.validate()?;
        let identity = Identity::load_pemfiles(&self.certificate_file, &self.key_file)
            .await
            .map_err(|error| {
                Error::InvalidConfig(format!("invalid WebTransport identity: {error}"))
            })?;
        let builder = ServerConfig::builder()
            .with_bind_address(self.listen)
            .with_identity(identity)
            .max_idle_timeout(Some(self.idle_timeout))
            .map_err(|error| {
                Error::InvalidConfig(format!("invalid WebTransport idle timeout: {error}"))
            })?
            .keep_alive_interval(self.keep_alive_interval);
        Ok(builder.build())
    }
}

#[doc(hidden)]
pub struct WebTransportGatewayListener {
    incoming: mpsc::Receiver<(SocketAddr, WebTransportIo)>,
    task: JoinHandle<()>,
}

pub(crate) async fn bind(config: WebTransportConfig) -> Result<WebTransportGatewayListener> {
    let endpoint = Endpoint::server(config.server_config().await?)?;
    let config = std::sync::Arc::new(config);
    let origins = std::sync::Arc::new(
        config
            .allowed_origins
            .iter()
            .map(|origin| normalize_origin(origin))
            .collect::<HashSet<_>>(),
    );
    let pending = std::sync::Arc::new(Semaphore::new(config.max_pending_handshakes));
    let (sender, incoming) = mpsc::channel(config.max_pending_handshakes);
    let task = tokio::spawn(async move {
        loop {
            let session = endpoint.accept().await;
            if !session.remote_address_validated() {
                session.retry();
                continue;
            }
            let Ok(permit) = pending.clone().try_acquire_owned() else {
                session.refuse();
                continue;
            };
            let sender = sender.clone();
            let config = config.clone();
            let origins = origins.clone();
            tokio::spawn(async move {
                let _permit = permit;
                match prepare(session, &config, &origins).await {
                    Ok(Some(accepted)) => {
                        let _ = sender.send(accepted).await;
                    }
                    Ok(None) => {}
                    Err(error) => debug!(%error, "WebTransport handshake rejected"),
                }
            });
        }
    });
    Ok(WebTransportGatewayListener { incoming, task })
}

async fn prepare(
    incoming: IncomingSession,
    config: &WebTransportConfig,
    origins: &HashSet<String>,
) -> Result<Option<(SocketAddr, WebTransportIo)>> {
    let request = tokio::time::timeout(config.handshake_timeout, incoming)
        .await
        .map_err(|_| Error::Timeout)?
        .map_err(webtransport_error)?;
    if request.path() != config.path {
        request.not_found().await;
        return Ok(None);
    }
    if !origin_allowed(
        request.origin(),
        request.authority(),
        origins,
        config.allow_missing_origin,
    ) {
        request.forbidden().await;
        return Ok(None);
    }
    let peer = request.remote_address();
    let connection = tokio::time::timeout(config.handshake_timeout, request.accept())
        .await
        .map_err(|_| Error::Timeout)?
        .map_err(webtransport_error)?;
    let io = match config.mode {
        WebTransportMode::ReliableStream => {
            let stream = tokio::time::timeout(config.stream_open_timeout, connection.accept_bi())
                .await
                .map_err(|_| Error::Timeout)?
                .map_err(webtransport_error)?;
            WebTransportIo::Reliable(ReliableWebTransportIo {
                stream: BiStream::join(stream),
                _connection: connection,
            })
        }
        WebTransportMode::Datagram => {
            if connection.max_datagram_size().is_none() {
                return Err(Error::InvalidConfig(
                    "WebTransport peer does not support Datagrams".into(),
                ));
            }
            WebTransportIo::Datagram(DatagramWebTransportIo::new(
                connection,
                config.max_datagram_bytes,
                config.datagram_queue,
            )?)
        }
    };
    Ok(Some((peer, io)))
}

fn normalize_origin(origin: &str) -> String {
    origin.trim_end_matches('/').to_ascii_lowercase()
}

fn origin_allowed(
    origin: Option<&str>,
    authority: &str,
    allowed_origins: &HashSet<String>,
    allow_missing_origin: bool,
) -> bool {
    let Some(origin) = origin else {
        return allow_missing_origin;
    };
    let origin = normalize_origin(origin);
    if !allowed_origins.is_empty() {
        return allowed_origins.contains(&origin);
    }
    origin
        .split_once("://")
        .is_some_and(|(_, origin_authority)| origin_authority.eq_ignore_ascii_case(authority))
}

fn webtransport_error(error: impl std::fmt::Display) -> Error {
    Error::Io(io::Error::other(error.to_string()))
}

#[async_trait]
impl GatewayTransportListener for WebTransportGatewayListener {
    type Io = WebTransportIo;

    async fn accept(&mut self) -> Result<(SocketAddr, Self::Io)> {
        self.incoming.recv().await.ok_or(Error::Unavailable)
    }
}

impl Drop for WebTransportGatewayListener {
    fn drop(&mut self) {
        self.task.abort();
    }
}

#[doc(hidden)]
pub enum WebTransportIo {
    Reliable(ReliableWebTransportIo),
    Datagram(DatagramWebTransportIo),
}

#[doc(hidden)]
pub struct ReliableWebTransportIo {
    stream: BiStream,
    _connection: Connection,
}

#[doc(hidden)]
pub struct DatagramWebTransportIo {
    connection: Connection,
    incoming: mpsc::Receiver<Bytes>,
    buffered: Bytes,
    codec: FrameCodec,
    task: JoinHandle<()>,
}

impl DatagramWebTransportIo {
    fn new(connection: Connection, max_datagram_bytes: usize, capacity: usize) -> Result<Self> {
        let codec = FrameCodec::new(max_datagram_bytes - HEADER_LEN)?;
        let receive_codec = codec.clone();
        let receive_connection = connection.clone();
        let (sender, incoming) = mpsc::channel(capacity);
        let task = tokio::spawn(async move {
            loop {
                let datagram = match receive_connection.receive_datagram().await {
                    Ok(datagram) => datagram.payload(),
                    Err(_) => break,
                };
                if datagram.len() > max_datagram_bytes
                    || receive_codec.decode_message(datagram.clone()).is_err()
                {
                    continue;
                }
                match sender.try_send(datagram) {
                    Ok(()) | Err(mpsc::error::TrySendError::Full(_)) => {}
                    Err(mpsc::error::TrySendError::Closed(_)) => break,
                }
            }
        });
        Ok(Self {
            connection,
            incoming,
            buffered: Bytes::new(),
            codec,
            task,
        })
    }
}

impl Drop for DatagramWebTransportIo {
    fn drop(&mut self) {
        self.task.abort();
    }
}

impl AsyncRead for WebTransportIo {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        destination: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        match &mut *self {
            Self::Reliable(io) => Pin::new(&mut io.stream).poll_read(cx, destination),
            Self::Datagram(io) => {
                if io.buffered.is_empty() {
                    io.buffered = match io.incoming.poll_recv(cx) {
                        Poll::Ready(Some(datagram)) => datagram,
                        Poll::Ready(None) => return Poll::Ready(Ok(())),
                        Poll::Pending => return Poll::Pending,
                    };
                }
                let length = destination.remaining().min(io.buffered.len());
                destination.put_slice(&io.buffered.split_to(length));
                Poll::Ready(Ok(()))
            }
        }
    }
}

impl AsyncWrite for WebTransportIo {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        source: &[u8],
    ) -> Poll<io::Result<usize>> {
        match &mut *self {
            Self::Reliable(io) => Pin::new(&mut io.stream).poll_write(cx, source),
            Self::Datagram(io) => {
                if io
                    .codec
                    .decode_message(Bytes::copy_from_slice(source))
                    .is_err()
                {
                    return Poll::Ready(Err(io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "WebTransport Datagram must contain exactly one ELR2 frame",
                    )));
                }
                match io.connection.send_datagram(source) {
                    Ok(()) => Poll::Ready(Ok(source.len())),
                    Err(error) => Poll::Ready(Err(io::Error::other(error.to_string()))),
                }
            }
        }
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match &mut *self {
            Self::Reliable(io) => Pin::new(&mut io.stream).poll_flush(cx),
            Self::Datagram(_) => Poll::Ready(Ok(())),
        }
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match &mut *self {
            Self::Reliable(io) => Pin::new(&mut io.stream).poll_shutdown(cx),
            Self::Datagram(_) => Poll::Ready(Ok(())),
        }
    }
}

#[cfg(test)]
mod tests {
    use bytes::{Bytes, BytesMut};
    use elura_core::protocol::{Frame, FrameCodec};
    use futures_util::{SinkExt, StreamExt};
    use tokio_util::codec::{Encoder, Framed};
    use wtransport::{ClientConfig, Endpoint};

    use super::*;

    fn identity_files() -> (PathBuf, PathBuf) {
        let directory = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("src/transport/testdata");
        (
            directory.join("quic-cert.pem"),
            directory.join("quic-key.pem"),
        )
    }

    fn unused_address() -> SocketAddr {
        let socket = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
        socket.local_addr().unwrap()
    }

    fn config(mode: WebTransportMode) -> WebTransportConfig {
        let (certificate, key) = identity_files();
        WebTransportConfig {
            mode,
            allow_missing_origin: true,
            ..WebTransportConfig::from_pem_files(unused_address(), certificate, key)
        }
    }

    fn client() -> Endpoint<wtransport::endpoint::endpoint_side::Client> {
        let config = ClientConfig::builder()
            .with_bind_default()
            .with_no_cert_validation()
            .build();
        Endpoint::client(config).unwrap()
    }

    #[test]
    fn validates_identity_path_and_limits() {
        assert!(WebTransportConfig::default().validate().is_err());
        let mut config = config(WebTransportMode::ReliableStream);
        assert!(config.validate().is_ok());
        config.path = "missing-leading-slash".into();
        assert!(config.validate().is_err());
    }

    #[test]
    fn origin_policy_defaults_to_same_origin() {
        let origins = HashSet::new();
        assert!(origin_allowed(
            Some("https://game.example:443"),
            "game.example:443",
            &origins,
            false,
        ));
        assert!(!origin_allowed(
            Some("https://evil.example"),
            "game.example",
            &origins,
            false,
        ));
        assert!(!origin_allowed(None, "game.example", &origins, false));
    }

    #[tokio::test]
    async fn reliable_stream_carries_elr2_frames() {
        let config = config(WebTransportMode::ReliableStream);
        let address = config.listen;
        let path = config.path.clone();
        let mut listener = bind(config).await.unwrap();
        let client_endpoint = client();
        let connection = client_endpoint
            .connect(format!("https://localhost:{}{path}", address.port()))
            .await
            .unwrap();
        let streams = connection.open_bi().await.unwrap().await.unwrap();
        let mut client = Framed::new(BiStream::join(streams), FrameCodec::default());
        let request = Frame::request(100, 7, Bytes::from_static(b"webtransport")).unwrap();
        client.send(request.clone()).await.unwrap();

        let (_, stream) = listener.accept().await.unwrap();
        let mut server = Framed::new(stream, FrameCodec::default());
        assert_eq!(server.next().await.unwrap().unwrap(), request);
        server
            .send(Frame::response(&request, Bytes::from_static(b"ok")))
            .await
            .unwrap();
        assert_eq!(
            client.next().await.unwrap().unwrap().payload,
            Bytes::from_static(b"ok")
        );
    }

    #[tokio::test]
    async fn datagram_mode_preserves_message_boundaries() {
        let config = config(WebTransportMode::Datagram);
        let address = config.listen;
        let path = config.path.clone();
        let mut listener = bind(config).await.unwrap();
        let client_endpoint = client();
        let connection = client_endpoint
            .connect(format!("https://localhost:{}{path}", address.port()))
            .await
            .unwrap();
        let request = Frame::request(100, 9, Bytes::from_static(b"datagram")).unwrap();
        let mut encoded = BytesMut::new();
        FrameCodec::default()
            .encode(request.clone(), &mut encoded)
            .unwrap();
        connection.send_datagram(&encoded).unwrap();

        let (_, stream) = listener.accept().await.unwrap();
        let mut server = Framed::new(stream, FrameCodec::default());
        assert_eq!(server.next().await.unwrap().unwrap(), request);
        server
            .send(Frame::response(&request, Bytes::from_static(b"ok")))
            .await
            .unwrap();
        let response = connection.receive_datagram().await.unwrap().payload();
        assert_eq!(
            FrameCodec::default()
                .decode_message(response)
                .unwrap()
                .payload,
            Bytes::from_static(b"ok")
        );
    }
}
