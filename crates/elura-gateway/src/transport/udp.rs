//! Best-effort UDP endpoint backed by the shared Gateway Session engine.

use std::collections::HashMap;
use std::io;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use async_trait::async_trait;
use bytes::Bytes;
use elura_core::protocol::{FrameCodec, HEADER_LEN};
use elura_core::{Error, Result};
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::net::UdpSocket;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

use super::GatewayTransportListener;

const MAX_UDP_DATAGRAM_BYTES: usize = 65_507;

/// Configuration for the built-in UDP endpoint.
///
/// Every UDP datagram must contain exactly one complete ELR2 frame. A source
/// address is treated as one best-effort Gateway Session until that Session is
/// closed by the shared authentication, heartbeat, or idle-timeout policies.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
#[non_exhaustive]
pub struct UdpConfig {
    /// Address on which UDP datagrams are received.
    pub listen: SocketAddr,
    /// Largest accepted UDP payload, including the ELR2 header.
    ///
    /// The conservative default avoids IP fragmentation on common IPv6 paths.
    pub max_datagram_bytes: usize,
    /// Maximum number of source-address Sessions tracked by this endpoint.
    pub max_sessions: usize,
    /// Number of received datagrams buffered for one Session.
    pub per_session_queue: usize,
}

impl Default for UdpConfig {
    fn default() -> Self {
        Self {
            listen: "127.0.0.1:17004".parse().expect("static address"),
            max_datagram_bytes: 1200,
            max_sessions: 4096,
            per_session_queue: 64,
        }
    }
}

impl UdpConfig {
    /// Validates the endpoint limits without binding the socket.
    pub fn validate(&self) -> Result<()> {
        if !(HEADER_LEN + 1..=MAX_UDP_DATAGRAM_BYTES).contains(&self.max_datagram_bytes)
            || self.max_sessions == 0
            || self.per_session_queue == 0
        {
            return Err(Error::InvalidConfig("invalid UDP configuration".into()));
        }
        Ok(())
    }
}

struct PeerSession {
    generation: u64,
    incoming: mpsc::Sender<Bytes>,
}

#[doc(hidden)]
pub struct UdpGatewayListener {
    incoming: mpsc::Receiver<Result<(SocketAddr, UdpIo)>>,
    task: JoinHandle<()>,
}

pub(crate) async fn bind(config: UdpConfig) -> Result<UdpGatewayListener> {
    config.validate()?;
    let socket = Arc::new(UdpSocket::bind(config.listen).await?);
    let (sender, incoming) = mpsc::channel(config.max_sessions);
    let (closed, mut closed_rx) = mpsc::unbounded_channel();
    let task_socket = socket.clone();
    let task = tokio::spawn(async move {
        let codec = FrameCodec::new(config.max_datagram_bytes - HEADER_LEN)
            .expect("validated UDP frame size");
        let mut peers = HashMap::<SocketAddr, PeerSession>::new();
        let mut generation = 0_u64;
        let mut buffer = vec![0_u8; config.max_datagram_bytes + 1];

        loop {
            tokio::select! {
                Some((peer, closed_generation)) = closed_rx.recv() => {
                    if peers
                        .get(&peer)
                        .is_some_and(|session| session.generation == closed_generation)
                    {
                        peers.remove(&peer);
                    }
                }
                received = task_socket.recv_from(&mut buffer) => {
                    let (length, peer) = match received {
                        Ok(received) => received,
                        Err(error) => {
                            let _ = sender.send(Err(error.into())).await;
                            break;
                        }
                    };
                    if length > config.max_datagram_bytes {
                        continue;
                    }
                    let datagram = Bytes::copy_from_slice(&buffer[..length]);
                    if codec.decode_message(datagram.clone()).is_err() {
                        continue;
                    }

                    if let Some(session) = peers.get(&peer) {
                        match session.incoming.try_send(datagram) {
                            Ok(()) | Err(mpsc::error::TrySendError::Full(_)) => continue,
                            Err(mpsc::error::TrySendError::Closed(datagram)) => {
                                peers.remove(&peer);
                                if !open_peer(
                                    peer,
                                    datagram,
                                    &config,
                                    &socket,
                                    &closed,
                                    &sender,
                                    &mut peers,
                                    &mut generation,
                                ) {
                                    break;
                                }
                                continue;
                            }
                        }
                    }
                    if !open_peer(
                        peer,
                        datagram,
                        &config,
                        &socket,
                        &closed,
                        &sender,
                        &mut peers,
                        &mut generation,
                    ) {
                        break;
                    }
                }
            }
        }
    });
    Ok(UdpGatewayListener { incoming, task })
}

#[allow(clippy::too_many_arguments)]
fn open_peer(
    peer: SocketAddr,
    first_datagram: Bytes,
    config: &UdpConfig,
    socket: &Arc<UdpSocket>,
    closed: &mpsc::UnboundedSender<(SocketAddr, u64)>,
    accepted: &mpsc::Sender<Result<(SocketAddr, UdpIo)>>,
    peers: &mut HashMap<SocketAddr, PeerSession>,
    generation: &mut u64,
) -> bool {
    if peers.len() >= config.max_sessions {
        return true;
    }
    *generation = generation.wrapping_add(1).max(1);
    let current_generation = *generation;
    let (incoming, datagrams) = mpsc::channel(config.per_session_queue);
    let io = UdpIo {
        socket: socket.clone(),
        peer,
        generation: current_generation,
        closed: closed.clone(),
        datagrams,
        buffered: Bytes::new(),
        max_datagram_bytes: config.max_datagram_bytes,
    };
    match accepted.try_send(Ok((peer, io))) {
        Ok(()) => {
            incoming
                .try_send(first_datagram)
                .expect("new UDP Session queue has capacity");
            peers.insert(
                peer,
                PeerSession {
                    generation: current_generation,
                    incoming,
                },
            );
            true
        }
        Err(mpsc::error::TrySendError::Full(_)) => true,
        Err(mpsc::error::TrySendError::Closed(_)) => false,
    }
}

#[async_trait]
impl GatewayTransportListener for UdpGatewayListener {
    type Io = UdpIo;

    async fn accept(&mut self) -> Result<(SocketAddr, Self::Io)> {
        self.incoming.recv().await.ok_or(Error::Unavailable)?
    }
}

impl Drop for UdpGatewayListener {
    fn drop(&mut self) {
        self.task.abort();
    }
}

/// A source-address-scoped UDP byte stream used by the shared Session engine.
#[doc(hidden)]
pub struct UdpIo {
    socket: Arc<UdpSocket>,
    peer: SocketAddr,
    generation: u64,
    closed: mpsc::UnboundedSender<(SocketAddr, u64)>,
    datagrams: mpsc::Receiver<Bytes>,
    buffered: Bytes,
    max_datagram_bytes: usize,
}

impl AsyncRead for UdpIo {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        destination: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        if self.buffered.is_empty() {
            self.buffered = match self.datagrams.poll_recv(cx) {
                Poll::Ready(Some(datagram)) => datagram,
                Poll::Ready(None) => return Poll::Ready(Ok(())),
                Poll::Pending => return Poll::Pending,
            };
        }
        let length = destination.remaining().min(self.buffered.len());
        destination.put_slice(&self.buffered.split_to(length));
        Poll::Ready(Ok(()))
    }
}

impl AsyncWrite for UdpIo {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        source: &[u8],
    ) -> Poll<io::Result<usize>> {
        if source.len() > self.max_datagram_bytes {
            return Poll::Ready(Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "ELR2 frame exceeds the configured UDP datagram size",
            )));
        }
        match self.socket.poll_send_to(cx, source, self.peer) {
            Poll::Ready(Ok(written)) if written != source.len() => Poll::Ready(Err(
                io::Error::new(io::ErrorKind::WriteZero, "partial UDP datagram write"),
            )),
            result => result,
        }
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }
}

impl Drop for UdpIo {
    fn drop(&mut self) {
        let _ = self.closed.send((self.peer, self.generation));
    }
}

#[cfg(test)]
mod tests {
    use bytes::{Bytes, BytesMut};
    use elura_core::protocol::{Frame, FrameCodec};
    use futures_util::{SinkExt, StreamExt};
    use tokio_util::codec::{Encoder, Framed};

    use super::*;

    fn unused_address() -> SocketAddr {
        let socket = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
        socket.local_addr().unwrap()
    }

    #[test]
    fn validates_datagram_and_session_limits() {
        assert!(UdpConfig::default().validate().is_ok());
        let config = UdpConfig {
            max_datagram_bytes: HEADER_LEN,
            ..UdpConfig::default()
        };
        assert!(config.validate().is_err());
        let config = UdpConfig {
            max_datagram_bytes: MAX_UDP_DATAGRAM_BYTES + 1,
            ..UdpConfig::default()
        };
        assert!(config.validate().is_err());
    }

    #[tokio::test]
    async fn listener_maps_one_peer_to_an_elr2_session() {
        let address = unused_address();
        let config = UdpConfig {
            listen: address,
            ..UdpConfig::default()
        };
        let mut listener = bind(config).await.unwrap();
        let client = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let request = Frame::request(100, 7, Bytes::from_static(b"udp")).unwrap();
        let mut encoded = BytesMut::new();
        FrameCodec::default()
            .encode(request.clone(), &mut encoded)
            .unwrap();
        client.send_to(&encoded, address).await.unwrap();

        let (peer, stream) = listener.accept().await.unwrap();
        assert_eq!(peer, client.local_addr().unwrap());
        let mut framed = Framed::new(stream, FrameCodec::default());
        assert_eq!(framed.next().await.unwrap().unwrap(), request);
        framed
            .send(Frame::response(&request, Bytes::from_static(b"ok")))
            .await
            .unwrap();

        let mut response = [0_u8; 1200];
        let length = client.recv(&mut response).await.unwrap();
        let response = FrameCodec::default()
            .decode_message(Bytes::copy_from_slice(&response[..length]))
            .unwrap();
        assert_eq!(response.payload, Bytes::from_static(b"ok"));
    }

    #[tokio::test]
    async fn malformed_datagrams_do_not_create_sessions() {
        let address = unused_address();
        let config = UdpConfig {
            listen: address,
            ..UdpConfig::default()
        };
        let mut listener = bind(config).await.unwrap();
        let client = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        client.send_to(b"not an ELR2 frame", address).await.unwrap();
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(50), listener.accept())
                .await
                .is_err()
        );
    }
}
