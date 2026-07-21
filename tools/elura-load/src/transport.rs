use std::error::Error;
use std::fmt;
use std::io;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::sync::Arc;

use async_trait::async_trait;
use bytes::{Bytes, BytesMut};
use elura_core::protocol::{Frame, FrameCodec, HEADER_LEN, PROTOCOL_IDENTIFIER};
use futures_util::{SinkExt, StreamExt};
use quinn::crypto::rustls::QuicClientConfig;
use rustls::pki_types::CertificateDer;
use rustls::pki_types::pem::PemObject;
use tokio::net::{TcpStream, UdpSocket, lookup_host};
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::http::HeaderValue;
use tokio_util::codec::{Decoder, Encoder, Framed};
use wtransport::stream::BiStream;

pub(crate) type TransportError = Box<dyn Error + Send + Sync>;
pub(crate) type TransportResult<T> = std::result::Result<T, TransportError>;

/// Client transport selected by one load run.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) enum TransportKind {
    #[default]
    Tcp,
    Udp,
    WebSocket,
    Quic,
    WebTransport,
}

impl TransportKind {
    pub(crate) const fn name(self) -> &'static str {
        match self {
            Self::Tcp => "tcp",
            Self::Udp => "udp",
            Self::WebSocket => "websocket",
            Self::Quic => "quic",
            Self::WebTransport => "webtransport",
        }
    }

    const fn uses_tls(self) -> bool {
        matches!(self, Self::Quic | Self::WebTransport)
    }
}

impl FromStr for TransportKind {
    type Err = TransportError;

    fn from_str(value: &str) -> TransportResult<Self> {
        match value {
            "tcp" => Ok(Self::Tcp),
            "udp" => Ok(Self::Udp),
            "websocket" | "ws" => Ok(Self::WebSocket),
            "quic" => Ok(Self::Quic),
            "webtransport" | "wt" => Ok(Self::WebTransport),
            _ => Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("unsupported transport {value}"),
            )
            .into()),
        }
    }
}

impl fmt::Display for TransportKind {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.name())
    }
}

/// Parameters shared by all worker connections in one load run.
pub(crate) struct ConnectorConfig {
    pub(crate) transport: TransportKind,
    pub(crate) address: String,
    pub(crate) server_name: Option<String>,
    pub(crate) path: String,
    pub(crate) max_payload: usize,
    pub(crate) max_datagram_bytes: usize,
    pub(crate) ca_certificate: Option<PathBuf>,
}

/// Factory for client-side connections to an already running Gateway.
pub(crate) struct Connector {
    kind: TransportKind,
    address: SocketAddr,
    authority: String,
    server_name: String,
    path: String,
    max_payload: usize,
    max_datagram_bytes: usize,
    roots: Option<rustls::RootCertStore>,
}

impl Connector {
    pub(crate) async fn new(config: ConnectorConfig) -> TransportResult<Self> {
        if !config.path.starts_with('/') || config.path.contains(['?', '#']) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "transport path must start with '/' and contain no query or fragment",
            )
            .into());
        }
        FrameCodec::new(config.max_payload)?;
        if !(HEADER_LEN + 1..=65_507).contains(&config.max_datagram_bytes) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "max datagram bytes must hold an ELR2 payload and not exceed 65507",
            )
            .into());
        }
        let address = lookup_host(config.address.as_str())
            .await?
            .next()
            .ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::AddrNotAvailable,
                    "load target did not resolve",
                )
            })?;
        let server_name = config
            .server_name
            .unwrap_or_else(|| host_from_authority(&config.address));
        let roots = config
            .transport
            .uses_tls()
            .then(|| root_store(config.ca_certificate.as_deref()))
            .transpose()?;
        Ok(Self {
            kind: config.transport,
            address,
            authority: config.address,
            server_name,
            path: config.path,
            max_payload: config.max_payload,
            max_datagram_bytes: config.max_datagram_bytes,
            roots,
        })
    }

    pub(crate) async fn connect(&self) -> TransportResult<Box<dyn LoadConnection>> {
        match self.kind {
            TransportKind::Tcp => self.connect_tcp().await,
            TransportKind::Udp => self.connect_udp().await,
            TransportKind::WebSocket => self.connect_websocket().await,
            TransportKind::Quic => self.connect_quic().await,
            TransportKind::WebTransport => self.connect_webtransport().await,
        }
    }

    async fn connect_tcp(&self) -> TransportResult<Box<dyn LoadConnection>> {
        let stream = TcpStream::connect(self.address).await?;
        stream.set_nodelay(true)?;
        Ok(Box::new(StreamConnection(Framed::new(
            stream,
            FrameCodec::new(self.max_payload)?,
        ))))
    }

    async fn connect_udp(&self) -> TransportResult<Box<dyn LoadConnection>> {
        let bind = match self.address.ip() {
            IpAddr::V4(_) => SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), 0),
            IpAddr::V6(_) => SocketAddr::new(IpAddr::V6(Ipv6Addr::UNSPECIFIED), 0),
        };
        let socket = UdpSocket::bind(bind).await?;
        socket.connect(self.address).await?;
        Ok(Box::new(UdpConnection {
            socket,
            codec: FrameCodec::new(self.max_payload.min(self.max_datagram_bytes - HEADER_LEN))?,
            receive_buffer: vec![0_u8; self.max_datagram_bytes + 1],
            max_datagram_bytes: self.max_datagram_bytes,
        }))
    }

    async fn connect_websocket(&self) -> TransportResult<Box<dyn LoadConnection>> {
        let url = format!("ws://{}{}", self.authority, self.path);
        let mut request = url.into_client_request()?;
        request.headers_mut().insert(
            "sec-websocket-protocol",
            HeaderValue::from_static(PROTOCOL_IDENTIFIER),
        );
        let (socket, _) = connect_async(request).await?;
        Ok(Box::new(WebSocketConnection {
            socket,
            codec: FrameCodec::new(self.max_payload)?,
            buffered: BytesMut::new(),
        }))
    }

    async fn connect_quic(&self) -> TransportResult<Box<dyn LoadConnection>> {
        let mut tls = rustls::ClientConfig::builder()
            .with_root_certificates(self.roots().clone())
            .with_no_client_auth();
        tls.alpn_protocols = vec![PROTOCOL_IDENTIFIER.as_bytes().to_vec()];
        let client = quinn::ClientConfig::new(Arc::new(QuicClientConfig::try_from(tls)?));
        let bind = match self.address.ip() {
            IpAddr::V4(_) => SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), 0),
            IpAddr::V6(_) => SocketAddr::new(IpAddr::V6(Ipv6Addr::UNSPECIFIED), 0),
        };
        let mut endpoint = quinn::Endpoint::client(bind)?;
        endpoint.set_default_client_config(client);
        let connection = endpoint.connect(self.address, &self.server_name)?.await?;
        let (send, receive) = connection.open_bi().await?;
        Ok(Box::new(QuicConnection {
            _endpoint: endpoint,
            _connection: connection,
            framed: Framed::new(
                tokio::io::join(receive, send),
                FrameCodec::new(self.max_payload)?,
            ),
        }))
    }

    async fn connect_webtransport(&self) -> TransportResult<Box<dyn LoadConnection>> {
        let mut tls = rustls::ClientConfig::builder()
            .with_root_certificates(self.roots().clone())
            .with_no_client_auth();
        tls.alpn_protocols = vec![wtransport::tls::WEBTRANSPORT_ALPN.to_vec()];
        let mut client = wtransport::ClientConfig::builder()
            .with_bind_default()
            .with_custom_tls(tls)
            .build();
        client.set_dns_resolver(FixedResolver(self.address));
        let endpoint = wtransport::Endpoint::client(client)?;
        let authority = authority_for_host(&self.server_name, self.address.port());
        let connection = endpoint
            .connect(format!("https://{authority}{}", self.path))
            .await?;
        let streams = connection.open_bi().await?.await?;
        Ok(Box::new(WebTransportConnection {
            _endpoint: endpoint,
            _connection: connection,
            framed: Framed::new(BiStream::join(streams), FrameCodec::new(self.max_payload)?),
        }))
    }

    fn roots(&self) -> &rustls::RootCertStore {
        self.roots
            .as_ref()
            .expect("TLS transports initialize a root store")
    }
}

#[async_trait]
pub(crate) trait LoadConnection: Send {
    async fn send(&mut self, frame: Frame) -> TransportResult<()>;
    async fn receive(&mut self) -> TransportResult<Frame>;
}

struct StreamConnection(Framed<TcpStream, FrameCodec>);

#[async_trait]
impl LoadConnection for StreamConnection {
    async fn send(&mut self, frame: Frame) -> TransportResult<()> {
        self.0.send(frame).await?;
        Ok(())
    }

    async fn receive(&mut self) -> TransportResult<Frame> {
        let frame = self.0.next().await.ok_or_else(closed)??;
        Ok(frame)
    }
}

struct UdpConnection {
    socket: UdpSocket,
    codec: FrameCodec,
    receive_buffer: Vec<u8>,
    max_datagram_bytes: usize,
}

#[async_trait]
impl LoadConnection for UdpConnection {
    async fn send(&mut self, frame: Frame) -> TransportResult<()> {
        let mut encoded = BytesMut::new();
        self.codec.encode(frame, &mut encoded)?;
        if encoded.len() > self.max_datagram_bytes {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "ELR2 frame exceeds the configured UDP datagram size",
            )
            .into());
        }
        self.socket.send(&encoded).await?;
        Ok(())
    }

    async fn receive(&mut self) -> TransportResult<Frame> {
        let length = self.socket.recv(&mut self.receive_buffer).await?;
        if length > self.max_datagram_bytes {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "received UDP datagram exceeds the configured limit",
            )
            .into());
        }
        Ok(self
            .codec
            .decode_message(Bytes::copy_from_slice(&self.receive_buffer[..length]))?)
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
impl LoadConnection for WebSocketConnection {
    async fn send(&mut self, frame: Frame) -> TransportResult<()> {
        let mut encoded = BytesMut::new();
        self.codec.encode(frame, &mut encoded)?;
        self.socket.send(Message::Binary(encoded.freeze())).await?;
        Ok(())
    }

    async fn receive(&mut self) -> TransportResult<Frame> {
        loop {
            if let Some(frame) = self.codec.decode(&mut self.buffered)? {
                return Ok(frame);
            }
            match self.socket.next().await {
                Some(Ok(Message::Binary(bytes))) => self.buffered.extend_from_slice(&bytes),
                Some(Ok(Message::Ping(payload))) => {
                    self.socket.send(Message::Pong(payload)).await?
                }
                Some(Ok(Message::Close(_))) | None => return Err(closed()),
                Some(Ok(_)) => {}
                Some(Err(error)) => return Err(error.into()),
            }
        }
    }
}

type QuicFramed = Framed<tokio::io::Join<quinn::RecvStream, quinn::SendStream>, FrameCodec>;

struct QuicConnection {
    _endpoint: quinn::Endpoint,
    _connection: quinn::Connection,
    framed: QuicFramed,
}

#[async_trait]
impl LoadConnection for QuicConnection {
    async fn send(&mut self, frame: Frame) -> TransportResult<()> {
        self.framed.send(frame).await?;
        Ok(())
    }

    async fn receive(&mut self) -> TransportResult<Frame> {
        let frame = self.framed.next().await.ok_or_else(closed)??;
        Ok(frame)
    }
}

struct WebTransportConnection {
    _endpoint: wtransport::Endpoint<wtransport::endpoint::endpoint_side::Client>,
    _connection: wtransport::Connection,
    framed: Framed<BiStream, FrameCodec>,
}

#[async_trait]
impl LoadConnection for WebTransportConnection {
    async fn send(&mut self, frame: Frame) -> TransportResult<()> {
        self.framed.send(frame).await?;
        Ok(())
    }

    async fn receive(&mut self) -> TransportResult<Frame> {
        let frame = self.framed.next().await.ok_or_else(closed)??;
        Ok(frame)
    }
}

fn root_store(ca_certificate: Option<&Path>) -> TransportResult<rustls::RootCertStore> {
    let mut roots = rustls::RootCertStore::empty();
    roots.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
    if let Some(path) = ca_certificate {
        for certificate in CertificateDer::pem_file_iter(path)? {
            roots.add(certificate?)?;
        }
    }
    Ok(roots)
}

fn host_from_authority(authority: &str) -> String {
    if let Ok(address) = authority.parse::<SocketAddr>() {
        return address.ip().to_string();
    }
    authority
        .rsplit_once(':')
        .map_or(authority, |(host, _)| host)
        .trim_matches(['[', ']'])
        .to_owned()
}

fn authority_for_host(host: &str, port: u16) -> String {
    if host.parse::<Ipv6Addr>().is_ok() {
        format!("[{host}]:{port}")
    } else {
        format!("{host}:{port}")
    }
}

#[derive(Debug)]
struct FixedResolver(SocketAddr);

impl wtransport::config::DnsResolver for FixedResolver {
    fn resolve(&self, _host: &str) -> std::pin::Pin<Box<dyn wtransport::config::DnsLookupFuture>> {
        let address = self.0;
        Box::pin(async move { Ok(Some(address)) })
    }
}

fn closed() -> TransportError {
    io::Error::new(io::ErrorKind::UnexpectedEof, "transport connection closed").into()
}

#[cfg(test)]
mod tests {
    use super::*;
    use elura_gateway::transport::{
        GatewayTransport, GatewayTransportListener, QuicConfig, TcpConfig, TcpTransport, UdpConfig,
        WebSocketConfig, WebTransportConfig,
    };

    fn unused_stream_address() -> SocketAddr {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        listener.local_addr().unwrap()
    }

    fn unused_datagram_address() -> SocketAddr {
        let socket = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
        socket.local_addr().unwrap()
    }

    fn identity_files() -> (PathBuf, PathBuf) {
        let directory = Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../crates/elura-gateway/src/transport/testdata");
        (
            directory.join("quic-cert.pem"),
            directory.join("quic-key.pem"),
        )
    }

    async fn assert_echo<T: GatewayTransport>(server: T, connector: ConnectorConfig) {
        let mut listener = server.bind().await.unwrap();
        let task = tokio::spawn(async move {
            let (_, stream) = listener.accept().await.unwrap();
            let mut framed = Framed::new(stream, FrameCodec::default());
            let request = framed.next().await.unwrap().unwrap();
            framed
                .send(Frame::response(&request, request.payload.clone()))
                .await
                .unwrap();
        });
        let connector = Connector::new(connector).await.unwrap();
        let mut connection = connector.connect().await.unwrap();
        let request = Frame::request(100, 7, Bytes::from_static(b"transport")).unwrap();
        connection.send(request.clone()).await.unwrap();
        let response = connection.receive().await.unwrap();
        assert_eq!(response.request_id, request.request_id);
        assert_eq!(response.payload, request.payload);
        task.await.unwrap();
    }

    fn connector(
        transport: TransportKind,
        address: SocketAddr,
        ca_certificate: Option<PathBuf>,
    ) -> ConnectorConfig {
        ConnectorConfig {
            transport,
            address: address.to_string(),
            server_name: Some("localhost".into()),
            path: "/elura/game".into(),
            max_payload: 1 << 20,
            max_datagram_bytes: 1200,
            ca_certificate,
        }
    }

    #[test]
    fn parses_transport_aliases() {
        assert_eq!("tcp".parse::<TransportKind>().unwrap(), TransportKind::Tcp);
        assert_eq!(
            "ws".parse::<TransportKind>().unwrap(),
            TransportKind::WebSocket
        );
        assert_eq!(
            "wt".parse::<TransportKind>().unwrap(),
            TransportKind::WebTransport
        );
        assert!("unknown".parse::<TransportKind>().is_err());
    }

    #[test]
    fn derives_server_names_from_authorities() {
        assert_eq!(host_from_authority("game.example:17003"), "game.example");
        assert_eq!(host_from_authority("127.0.0.1:17003"), "127.0.0.1");
        assert_eq!(host_from_authority("[::1]:17003"), "::1");
    }

    #[tokio::test]
    async fn tcp_connector_preserves_elr2_frames() {
        let address = unused_stream_address();
        let mut config = TcpConfig::default();
        config.listen = address;
        assert_echo(
            TcpTransport::new(config).unwrap(),
            connector(TransportKind::Tcp, address, None),
        )
        .await;
    }

    #[tokio::test]
    async fn udp_connector_preserves_elr2_datagrams() {
        let address = unused_datagram_address();
        let mut config = UdpConfig::default();
        config.listen = address;
        assert_echo(config, connector(TransportKind::Udp, address, None)).await;
    }

    #[tokio::test]
    async fn websocket_connector_preserves_elr2_messages() {
        let address = unused_stream_address();
        let mut config = WebSocketConfig::default();
        config.listen = address;
        config.allow_missing_origin = true;
        assert_echo(config, connector(TransportKind::WebSocket, address, None)).await;
    }

    #[tokio::test]
    async fn quic_connector_validates_tls_and_opens_one_stream() {
        let address = unused_datagram_address();
        let (certificate, key) = identity_files();
        assert_echo(
            QuicConfig::from_pem_files(address, &certificate, key),
            connector(TransportKind::Quic, address, Some(certificate)),
        )
        .await;
    }

    #[tokio::test]
    async fn webtransport_connector_validates_tls_and_opens_one_stream() {
        let address = unused_datagram_address();
        let (certificate, key) = identity_files();
        let mut config = WebTransportConfig::from_pem_files(address, &certificate, key);
        config.allow_missing_origin = true;
        assert_echo(
            config,
            connector(TransportKind::WebTransport, address, Some(certificate)),
        )
        .await;
    }
}
