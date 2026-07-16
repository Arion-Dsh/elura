//! QUIC endpoint backed by the shared Gateway Session engine.

use std::io;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;

use async_trait::async_trait;
use elura_core::protocol::PROTOCOL_IDENTIFIER;
use elura_core::{Error, Result};
use quinn::crypto::rustls::QuicServerConfig;
use quinn::{Endpoint, IdleTimeout, Incoming, TransportConfig, VarInt};
use rustls::pki_types::pem::PemObject;
use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::sync::{Semaphore, mpsc};
use tokio::task::JoinHandle;

use super::GatewayTransportListener;

/// Configuration for a public QUIC endpoint.
///
/// A QUIC connection carries exactly one ELR2 Session on the first
/// client-initiated bidirectional stream. QUIC always uses TLS 1.3, so a
/// certificate chain and private key are mandatory.
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
        let mut transport = TransportConfig::default();
        transport
            .max_concurrent_bidi_streams(VarInt::from_u32(1))
            .max_concurrent_uni_streams(VarInt::from_u32(0))
            .max_idle_timeout(Some(IdleTimeout::try_from(self.idle_timeout).map_err(
                |_| Error::InvalidConfig("QUIC idle timeout is too large".into()),
            )?))
            .keep_alive_interval(self.keep_alive_interval);
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
            let handshake_timeout = config.handshake_timeout;
            tokio::spawn(async move {
                let _permit = permit;
                let peer = incoming.remote_address();
                let result = connection(incoming, peer, handshake_timeout).await;
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
    handshake_timeout: Duration,
) -> Result<(SocketAddr, QuicIo)> {
    let connecting = incoming
        .accept()
        .map_err(|error| Error::Io(io::Error::other(error)))?;
    let connection = tokio::time::timeout(handshake_timeout, connecting)
        .await
        .map_err(|_| Error::Timeout)?
        .map_err(|error| Error::Io(io::Error::other(error)))?;
    let (send, receive) = tokio::time::timeout(handshake_timeout, connection.accept_bi())
        .await
        .map_err(|_| Error::Timeout)?
        .map_err(|error| Error::Io(io::Error::other(error)))?;
    let keepalive = connection.clone();
    tokio::spawn(async move {
        keepalive.closed().await;
    });
    let stream = QuicIo {
        stream: tokio::io::join(receive, send),
    };
    Ok((peer, stream))
}

#[doc(hidden)]
pub struct QuicIo {
    stream: tokio::io::Join<quinn::RecvStream, quinn::SendStream>,
}

impl AsyncRead for QuicIo {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buffer: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut self.stream).poll_read(cx, buffer)
    }
}

impl AsyncWrite for QuicIo {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buffer: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.stream).poll_write(cx, buffer)
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.stream).poll_flush(cx)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.stream).poll_shutdown(cx)
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
    use bytes::Bytes;
    use elura_core::protocol::{Frame, FrameCodec, PROTOCOL_IDENTIFIER};
    use futures_util::{SinkExt, StreamExt};
    use quinn::crypto::rustls::QuicClientConfig;
    use tokio_util::codec::Framed;

    use super::*;

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

    #[tokio::test]
    async fn listener_yields_an_elr2_byte_stream() {
        let certificate =
            Path::new(env!("CARGO_MANIFEST_DIR")).join("src/transport/testdata/quic-cert.pem");
        let key = Path::new(env!("CARGO_MANIFEST_DIR")).join("src/transport/testdata/quic-key.pem");
        let socket = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
        let address = socket.local_addr().unwrap();
        drop(socket);
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

        let mut roots = rustls::RootCertStore::empty();
        roots
            .add(load_certificates(&certificate).unwrap().remove(0))
            .unwrap();
        let mut crypto = rustls::ClientConfig::builder()
            .with_root_certificates(roots)
            .with_no_client_auth();
        crypto.alpn_protocols = vec![PROTOCOL_IDENTIFIER.as_bytes().to_vec()];
        let client_config =
            quinn::ClientConfig::new(Arc::new(QuicClientConfig::try_from(crypto).unwrap()));
        let mut endpoint = Endpoint::client("127.0.0.1:0".parse().unwrap()).unwrap();
        endpoint.set_default_client_config(client_config);
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
}
