//! QUIC endpoint backed by the shared Gateway Session engine.

use std::collections::HashSet;
use std::io;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;

use async_trait::async_trait;
use bytes::{Bytes, BytesMut};
use elura_core::protocol::{FIRST_APPLICATION_ROUTE, FrameCodec, HEADER_LEN, PROTOCOL_IDENTIFIER};
use elura_core::{Error, Result};
use futures_util::StreamExt;
use quinn::crypto::rustls::QuicServerConfig;
use quinn::{Endpoint, IdleTimeout, Incoming, TransportConfig, VarInt};
use rustls::pki_types::pem::PemObject;
use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::sync::{Semaphore, mpsc};
use tokio::task::JoinHandle;
use tokio_util::codec::{Encoder, FramedRead};

use super::GatewayTransportListener;

const MAX_QUIC_DATAGRAM_BYTES: usize = 65_507;
const MAX_ELR2_PAYLOAD: usize = 64 << 20;

/// ELR2 channels available inside one QUIC connection.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum QuicMode {
    /// Every ELR2 frame uses the first client-initiated bidirectional stream.
    #[default]
    ReliableStream,
    /// Configured application routes use QUIC Datagrams while every other
    /// route remains on the reliable stream.
    Hybrid,
}

/// Configuration for a public QUIC endpoint.
///
/// A QUIC connection carries exactly one ELR2 Session on the first
/// client-initiated bidirectional stream. In [`QuicMode::Hybrid`], selected
/// application routes share that Session through QUIC Datagrams. QUIC always
/// uses TLS 1.3, so a certificate chain and private key are mandatory.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
#[non_exhaustive]
pub struct QuicConfig {
    pub listen: SocketAddr,
    pub certificate_file: PathBuf,
    pub key_file: PathBuf,
    pub alpn_protocol: String,
    pub handshake_timeout: Duration,
    pub idle_timeout: Duration,
    pub keep_alive_interval: Option<Duration>,
    pub max_pending_connections: usize,
    /// ELR2 channel selection for each accepted connection.
    pub mode: QuicMode,
    /// Application route IDs carried by QUIC Datagrams in hybrid mode.
    ///
    /// Built-in protocol routes are always reliable. The client must use the
    /// same route policy when selecting its outbound channel.
    pub datagram_routes: Vec<u32>,
    /// Largest ELR2 frame accepted on the Datagram channel.
    pub max_datagram_bytes: usize,
    /// Number of validated stream frames and Datagrams buffered while merging
    /// the two inbound channels.
    pub datagram_queue: usize,
}

impl Default for QuicConfig {
    fn default() -> Self {
        Self {
            listen: "127.0.0.1:17003".parse().expect("static address"),
            certificate_file: PathBuf::new(),
            key_file: PathBuf::new(),
            alpn_protocol: PROTOCOL_IDENTIFIER.into(),
            handshake_timeout: Duration::from_secs(5),
            idle_timeout: Duration::from_secs(90),
            keep_alive_interval: Some(Duration::from_secs(30)),
            max_pending_connections: 1024,
            mode: QuicMode::ReliableStream,
            datagram_routes: Vec::new(),
            max_datagram_bytes: 1100,
            datagram_queue: 64,
        }
    }
}

impl QuicConfig {
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

    pub fn validate(&self) -> Result<()> {
        let datagram_routes = self.datagram_routes.iter().copied().collect::<HashSet<_>>();
        let hybrid_routes_valid = match self.mode {
            QuicMode::ReliableStream => self.datagram_routes.is_empty(),
            QuicMode::Hybrid => {
                !self.datagram_routes.is_empty()
                    && datagram_routes.len() == self.datagram_routes.len()
                    && self
                        .datagram_routes
                        .iter()
                        .all(|route| *route >= FIRST_APPLICATION_ROUTE)
            }
        };
        if self.certificate_file.as_os_str().is_empty()
            || self.key_file.as_os_str().is_empty()
            || self.alpn_protocol.is_empty()
            || self.alpn_protocol.len() > u8::MAX as usize
            || self.handshake_timeout.is_zero()
            || self.idle_timeout.is_zero()
            || self
                .keep_alive_interval
                .is_some_and(|value| value.is_zero())
            || self.max_pending_connections == 0
            || !hybrid_routes_valid
            || !(HEADER_LEN + 1..=MAX_QUIC_DATAGRAM_BYTES).contains(&self.max_datagram_bytes)
            || self.datagram_queue == 0
            || self
                .max_datagram_bytes
                .checked_mul(self.datagram_queue)
                .is_none()
        {
            return Err(Error::InvalidConfig("invalid QUIC configuration".into()));
        }
        IdleTimeout::try_from(self.idle_timeout)
            .map_err(|_| Error::InvalidConfig("QUIC idle timeout is too large".into()))?;
        Ok(())
    }

    fn server_config(&self) -> Result<quinn::ServerConfig> {
        self.validate()?;
        let certificates = load_certificates(&self.certificate_file)?;
        let key = PrivateKeyDer::from_pem_file(&self.key_file)
            .map_err(|error| Error::InvalidConfig(format!("invalid QUIC private key: {error}")))?;
        let mut tls = rustls::ServerConfig::builder()
            .with_no_client_auth()
            .with_single_cert(certificates, key)
            .map_err(|error| {
                Error::InvalidConfig(format!("invalid QUIC server identity: {error}"))
            })?;
        tls.alpn_protocols = vec![self.alpn_protocol.as_bytes().to_vec()];
        let crypto = QuicServerConfig::try_from(tls).map_err(|error| {
            Error::InvalidConfig(format!("invalid QUIC TLS configuration: {error}"))
        })?;
        let datagram_buffer_bytes = self
            .max_datagram_bytes
            .checked_mul(self.datagram_queue)
            .expect("validated QUIC Datagram buffer size");
        let mut transport = TransportConfig::default();
        transport
            .max_concurrent_bidi_streams(VarInt::from_u32(1))
            .max_concurrent_uni_streams(VarInt::from_u32(0))
            .max_idle_timeout(Some(IdleTimeout::try_from(self.idle_timeout).map_err(
                |_| Error::InvalidConfig("QUIC idle timeout is too large".into()),
            )?))
            .keep_alive_interval(self.keep_alive_interval);
        match self.mode {
            QuicMode::ReliableStream => {
                transport
                    .datagram_receive_buffer_size(None)
                    .datagram_send_buffer_size(0);
            }
            QuicMode::Hybrid => {
                transport
                    .datagram_receive_buffer_size(Some(datagram_buffer_bytes))
                    .datagram_send_buffer_size(datagram_buffer_bytes);
            }
        }
        let mut server = quinn::ServerConfig::with_crypto(Arc::new(crypto));
        server.transport_config(Arc::new(transport));
        server.max_incoming(self.max_pending_connections);
        Ok(server)
    }
}

#[doc(hidden)]
pub struct QuicGatewayListener {
    incoming: mpsc::Receiver<Result<(SocketAddr, QuicIo)>>,
    task: JoinHandle<()>,
}

pub(crate) async fn bind(config: QuicConfig) -> Result<QuicGatewayListener> {
    let server_config = config.server_config()?;
    let endpoint = Endpoint::server(server_config, config.listen)?;
    let pending = Arc::new(Semaphore::new(config.max_pending_connections));
    let (sender, incoming) = mpsc::channel(config.max_pending_connections);
    let config = Arc::new(config);
    let task = tokio::spawn(async move {
        loop {
            let Some(incoming) = endpoint.accept().await else {
                break;
            };
            let Ok(permit) = pending.clone().try_acquire_owned() else {
                incoming.refuse();
                continue;
            };
            let sender = sender.clone();
            let config = config.clone();
            tokio::spawn(async move {
                let _permit = permit;
                let peer = incoming.remote_address();
                let result = connection(incoming, peer, &config).await;
                let _ = sender.send(result).await;
            });
        }
    });
    Ok(QuicGatewayListener { incoming, task })
}

#[async_trait]
impl GatewayTransportListener for QuicGatewayListener {
    type Io = QuicIo;

    async fn accept(&mut self) -> Result<(SocketAddr, Self::Io)> {
        self.incoming.recv().await.ok_or(Error::Unavailable)?
    }
}

impl Drop for QuicGatewayListener {
    fn drop(&mut self) {
        self.task.abort();
    }
}

async fn connection(
    incoming: Incoming,
    peer: SocketAddr,
    config: &QuicConfig,
) -> Result<(SocketAddr, QuicIo)> {
    let connecting = incoming
        .accept()
        .map_err(|error| Error::Io(io::Error::other(error)))?;
    let connection = tokio::time::timeout(config.handshake_timeout, connecting)
        .await
        .map_err(|_| Error::Timeout)?
        .map_err(|error| Error::Io(io::Error::other(error)))?;
    let (send, receive) = tokio::time::timeout(config.handshake_timeout, connection.accept_bi())
        .await
        .map_err(|_| Error::Timeout)?
        .map_err(|error| Error::Io(io::Error::other(error)))?;
    let keepalive = connection.clone();
    tokio::spawn(async move {
        keepalive.closed().await;
    });
    let io = match config.mode {
        QuicMode::ReliableStream => QuicIo::Reliable(ReliableQuicIo {
            stream: tokio::io::join(receive, send),
            _connection: connection,
        }),
        QuicMode::Hybrid => {
            if connection.max_datagram_size().is_none() {
                return Err(Error::InvalidConfig(
                    "QUIC peer does not support Datagrams".into(),
                ));
            }
            QuicIo::Hybrid(HybridQuicIo::new(
                connection,
                send,
                receive,
                config.datagram_routes.iter().copied().collect(),
                config.max_datagram_bytes,
                config.datagram_queue,
            )?)
        }
    };
    Ok((peer, io))
}

#[doc(hidden)]
pub enum QuicIo {
    Reliable(ReliableQuicIo),
    Hybrid(HybridQuicIo),
}

#[doc(hidden)]
pub struct ReliableQuicIo {
    stream: tokio::io::Join<quinn::RecvStream, quinn::SendStream>,
    _connection: quinn::Connection,
}

#[doc(hidden)]
pub struct HybridQuicIo {
    connection: quinn::Connection,
    reliable_send: quinn::SendStream,
    incoming: mpsc::Receiver<Bytes>,
    buffered: Bytes,
    datagram_routes: Arc<HashSet<u32>>,
    max_datagram_bytes: usize,
    reliable_write_remaining: usize,
    task: JoinHandle<()>,
}

impl HybridQuicIo {
    fn new(
        connection: quinn::Connection,
        reliable_send: quinn::SendStream,
        reliable_receive: quinn::RecvStream,
        datagram_routes: HashSet<u32>,
        max_datagram_bytes: usize,
        capacity: usize,
    ) -> Result<Self> {
        let datagram_routes = Arc::new(datagram_routes);
        let datagram_codec = FrameCodec::new(max_datagram_bytes - HEADER_LEN)?;
        let task = spawn_hybrid_reader(
            connection.clone(),
            reliable_receive,
            datagram_routes.clone(),
            datagram_codec.clone(),
            capacity,
            max_datagram_bytes,
        )?;
        Ok(Self {
            connection,
            reliable_send,
            incoming: task.incoming,
            buffered: Bytes::new(),
            datagram_routes,
            max_datagram_bytes,
            reliable_write_remaining: 0,
            task: task.handle,
        })
    }
}

struct HybridReader {
    incoming: mpsc::Receiver<Bytes>,
    handle: JoinHandle<()>,
}

fn spawn_hybrid_reader(
    connection: quinn::Connection,
    reliable_receive: quinn::RecvStream,
    datagram_routes: Arc<HashSet<u32>>,
    datagram_codec: FrameCodec,
    capacity: usize,
    max_datagram_bytes: usize,
) -> Result<HybridReader> {
    let mut stream_codec = FrameCodec::new(MAX_ELR2_PAYLOAD)?;
    let mut reliable = FramedRead::new(reliable_receive, stream_codec.clone());
    let (sender, incoming) = mpsc::channel(capacity);
    let handle = tokio::spawn(async move {
        loop {
            tokio::select! {
                next = reliable.next() => {
                    let Some(Ok(frame)) = next else {
                        break;
                    };
                    let mut encoded = BytesMut::new();
                    if stream_codec.encode(frame, &mut encoded).is_err()
                        || sender.send(encoded.freeze()).await.is_err()
                    {
                        break;
                    }
                }
                received = connection.read_datagram() => {
                    let datagram = match received {
                        Ok(datagram) => datagram,
                        Err(_) => break,
                    };
                    if datagram.len() > max_datagram_bytes {
                        continue;
                    }
                    let Ok(frame) = datagram_codec.decode_message(datagram.clone()) else {
                        continue;
                    };
                    if !datagram_routes.contains(&frame.route) {
                        continue;
                    }
                    match sender.try_send(datagram) {
                        Ok(()) | Err(mpsc::error::TrySendError::Full(_)) => {}
                        Err(mpsc::error::TrySendError::Closed(_)) => break,
                    }
                }
            }
        }
    });
    Ok(HybridReader { incoming, handle })
}

impl Drop for HybridQuicIo {
    fn drop(&mut self) {
        self.task.abort();
    }
}

impl AsyncRead for QuicIo {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buffer: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        match &mut *self {
            Self::Reliable(io) => Pin::new(&mut io.stream).poll_read(cx, buffer),
            Self::Hybrid(io) => {
                if io.buffered.is_empty() {
                    io.buffered = match io.incoming.poll_recv(cx) {
                        Poll::Ready(Some(frame)) => frame,
                        Poll::Ready(None) => return Poll::Ready(Ok(())),
                        Poll::Pending => return Poll::Pending,
                    };
                }
                let length = buffer.remaining().min(io.buffered.len());
                buffer.put_slice(&io.buffered.split_to(length));
                Poll::Ready(Ok(()))
            }
        }
    }
}

impl AsyncWrite for QuicIo {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buffer: &[u8],
    ) -> Poll<io::Result<usize>> {
        match &mut *self {
            Self::Reliable(io) => Pin::new(&mut io.stream).poll_write(cx, buffer),
            Self::Hybrid(io) => {
                if io.reliable_write_remaining != 0 {
                    let result =
                        AsyncWrite::poll_write(Pin::new(&mut io.reliable_send), cx, buffer);
                    if let Poll::Ready(Ok(written)) = result {
                        io.reliable_write_remaining =
                            io.reliable_write_remaining.saturating_sub(written);
                    }
                    return result;
                }
                let frame = FrameCodec::new(MAX_ELR2_PAYLOAD)
                    .expect("static maximum ELR2 payload")
                    .decode_message(Bytes::copy_from_slice(buffer))
                    .map_err(|_| {
                        io::Error::new(
                            io::ErrorKind::InvalidInput,
                            "hybrid QUIC writes must contain exactly one ELR2 frame",
                        )
                    });
                let frame = match frame {
                    Ok(frame) => frame,
                    Err(error) => return Poll::Ready(Err(error)),
                };
                if io.datagram_routes.contains(&frame.route) {
                    if buffer.len() > io.max_datagram_bytes {
                        return Poll::Ready(Err(io::Error::new(
                            io::ErrorKind::InvalidInput,
                            "ELR2 frame exceeds the configured QUIC Datagram size",
                        )));
                    }
                    return match io.connection.send_datagram(Bytes::copy_from_slice(buffer)) {
                        Ok(()) => Poll::Ready(Ok(buffer.len())),
                        Err(error) => Poll::Ready(Err(io::Error::other(error.to_string()))),
                    };
                }
                let result = AsyncWrite::poll_write(Pin::new(&mut io.reliable_send), cx, buffer);
                if let Poll::Ready(Ok(written)) = result
                    && written < buffer.len()
                {
                    io.reliable_write_remaining = buffer.len() - written;
                }
                result
            }
        }
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match &mut *self {
            Self::Reliable(io) => Pin::new(&mut io.stream).poll_flush(cx),
            Self::Hybrid(io) => Pin::new(&mut io.reliable_send).poll_flush(cx),
        }
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match &mut *self {
            Self::Reliable(io) => Pin::new(&mut io.stream).poll_shutdown(cx),
            Self::Hybrid(io) => Pin::new(&mut io.reliable_send).poll_shutdown(cx),
        }
    }
}

fn load_certificates(path: &Path) -> Result<Vec<CertificateDer<'static>>> {
    let certificates = CertificateDer::pem_file_iter(path)
        .map_err(|error| Error::InvalidConfig(format!("invalid QUIC certificate file: {error}")))?
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(|error| Error::InvalidConfig(format!("invalid QUIC certificate: {error}")))?;
    if certificates.is_empty() {
        return Err(Error::InvalidConfig(
            "QUIC certificate file contains no certificates".into(),
        ));
    }
    Ok(certificates)
}

#[cfg(test)]
mod tests {
    use bytes::{Bytes, BytesMut};
    use elura_core::protocol::{Frame, FrameCodec, PROTOCOL_IDENTIFIER};
    use futures_util::{SinkExt, StreamExt};
    use quinn::crypto::rustls::QuicClientConfig;
    use tokio_util::codec::{Encoder, Framed};

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

    fn client_config(certificate: &Path, datagrams: bool) -> quinn::ClientConfig {
        let mut roots = rustls::RootCertStore::empty();
        roots
            .add(load_certificates(certificate).unwrap().remove(0))
            .unwrap();
        let mut crypto = rustls::ClientConfig::builder()
            .with_root_certificates(roots)
            .with_no_client_auth();
        crypto.alpn_protocols = vec![PROTOCOL_IDENTIFIER.as_bytes().to_vec()];
        let mut config =
            quinn::ClientConfig::new(Arc::new(QuicClientConfig::try_from(crypto).unwrap()));
        if datagrams {
            let mut transport = TransportConfig::default();
            transport
                .datagram_receive_buffer_size(Some(64 * 1100))
                .datagram_send_buffer_size(64 * 1100);
            config.transport_config(Arc::new(transport));
        }
        config
    }

    #[test]
    fn requires_an_identity() {
        assert!(QuicConfig::default().validate().is_err());
    }

    #[test]
    fn validates_operational_limits_without_reading_files() {
        let mut config = QuicConfig::from_pem_files(
            "127.0.0.1:17002".parse().unwrap(),
            "certificate.pem",
            "key.pem",
        );
        assert!(config.validate().is_ok());
        config.max_pending_connections = 0;
        assert!(config.validate().is_err());
    }

    #[test]
    fn hybrid_mode_requires_unique_application_routes() {
        let mut config = QuicConfig::from_pem_files(
            "127.0.0.1:17002".parse().unwrap(),
            "certificate.pem",
            "key.pem",
        );
        config.mode = QuicMode::Hybrid;
        assert!(config.validate().is_err());
        config.datagram_routes = vec![FIRST_APPLICATION_ROUTE - 1];
        assert!(config.validate().is_err());
        config.datagram_routes = vec![FIRST_APPLICATION_ROUTE, FIRST_APPLICATION_ROUTE];
        assert!(config.validate().is_err());
        config.datagram_routes = vec![FIRST_APPLICATION_ROUTE];
        assert!(config.validate().is_ok());
    }

    #[tokio::test]
    async fn listener_yields_an_elr2_byte_stream() {
        let (certificate, key) = identity_files();
        let address = unused_address();
        let mut listener = bind(QuicConfig::from_pem_files(address, &certificate, key))
            .await
            .unwrap();
        let server = tokio::spawn(async move {
            let (_, stream) = listener.accept().await.unwrap();
            let mut framed = Framed::new(stream, FrameCodec::default());
            let request = framed.next().await.unwrap().unwrap();
            framed
                .send(Frame::response(&request, request.payload.clone()))
                .await
                .unwrap();
            framed.close().await.unwrap();
            listener
        });

        let mut endpoint = Endpoint::client("127.0.0.1:0".parse().unwrap()).unwrap();
        endpoint.set_default_client_config(client_config(&certificate, false));
        let connection = endpoint
            .connect(address, "localhost")
            .unwrap()
            .await
            .unwrap();
        let (send, receive) = connection.open_bi().await.unwrap();
        let mut framed = Framed::new(tokio::io::join(receive, send), FrameCodec::default());
        framed
            .send(Frame::request(100, 7, Bytes::from_static(b"quic")).unwrap())
            .await
            .unwrap();
        let response = framed.next().await.unwrap().unwrap();
        assert_eq!(response.payload, Bytes::from_static(b"quic"));
        drop(server.await.unwrap());
    }

    #[tokio::test]
    async fn hybrid_mode_routes_selected_frames_over_datagrams() {
        let (certificate, key) = identity_files();
        let address = unused_address();
        let realtime_route = FIRST_APPLICATION_ROUTE + 1;
        let mut config = QuicConfig::from_pem_files(address, &certificate, key);
        config.mode = QuicMode::Hybrid;
        config.datagram_routes = vec![realtime_route];
        let mut listener = bind(config).await.unwrap();
        let server = tokio::spawn(async move {
            let (_, stream) = listener.accept().await.unwrap();
            let mut framed = Framed::new(stream, FrameCodec::default());
            for _ in 0..2 {
                let request = framed.next().await.unwrap().unwrap();
                framed
                    .send(Frame::response(&request, request.payload.clone()))
                    .await
                    .unwrap();
            }
            listener
        });

        let mut endpoint = Endpoint::client("127.0.0.1:0".parse().unwrap()).unwrap();
        endpoint.set_default_client_config(client_config(&certificate, true));
        let connection = endpoint
            .connect(address, "localhost")
            .unwrap()
            .await
            .unwrap();
        let (send, receive) = connection.open_bi().await.unwrap();
        let mut reliable = Framed::new(tokio::io::join(receive, send), FrameCodec::default());
        let reliable_request =
            Frame::request(FIRST_APPLICATION_ROUTE, 7, Bytes::from_static(b"reliable")).unwrap();
        reliable.send(reliable_request.clone()).await.unwrap();
        let datagram_request =
            Frame::request(realtime_route, 8, Bytes::from_static(b"realtime")).unwrap();
        let mut encoded = BytesMut::new();
        FrameCodec::default()
            .encode(datagram_request.clone(), &mut encoded)
            .unwrap();
        connection.send_datagram(encoded.freeze()).unwrap();

        let reliable_response = reliable.next().await.unwrap().unwrap();
        assert_eq!(reliable_response.route, reliable_request.route);
        assert_eq!(reliable_response.payload, reliable_request.payload);
        let datagram = connection.read_datagram().await.unwrap();
        let datagram_response = FrameCodec::default().decode_message(datagram).unwrap();
        assert_eq!(datagram_response.route, datagram_request.route);
        assert_eq!(datagram_response.payload, datagram_request.payload);
        drop(server.await.unwrap());
    }
}
