use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use elura_core::{Error, Result};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{Semaphore, mpsc};
use tokio::task::JoinHandle;

use elura_runtime::security::BoxedServiceStream;

use super::{GatewayTransportListener, TcpTransport, proxy_client_address};

#[doc(hidden)]
pub struct TcpGatewayListener {
    incoming: mpsc::Receiver<Result<(std::net::SocketAddr, BoxedServiceStream)>>,
    task: JoinHandle<()>,
}

pub(crate) async fn bind(transport: TcpTransport) -> Result<TcpGatewayListener> {
    let config = transport.config().clone();
    let listener = TcpListener::bind(config.listen).await?;
    let tls = transport.tls();
    let proxy_protocol = transport.proxy_protocol();
    let (sender, incoming) = mpsc::channel(config.max_pending_handshakes);
    let permits = Arc::new(Semaphore::new(config.max_pending_handshakes));
    let task = tokio::spawn(async move {
        loop {
            let (stream, peer) = match listener.accept().await {
                Ok(connection) => connection,
                Err(error) => {
                    let _ = sender.send(Err(error.into())).await;
                    break;
                }
            };
            let Ok(permit) = permits.clone().try_acquire_owned() else {
                continue;
            };
            let sender = sender.clone();
            let tls = tls.clone();
            let proxy_protocol = proxy_protocol.clone();
            let config = config.clone();
            tokio::spawn(async move {
                let _permit = permit;
                let result = prepare(stream, peer, config, tls, proxy_protocol).await;
                let _ = sender.send(result).await;
            });
        }
    });
    Ok(TcpGatewayListener { incoming, task })
}

async fn prepare(
    mut stream: TcpStream,
    peer: std::net::SocketAddr,
    config: super::TcpConfig,
    tls: Option<elura_runtime::security::ServerTlsConfig>,
    proxy_protocol: Option<super::ProxyProtocolConfig>,
) -> Result<(std::net::SocketAddr, BoxedServiceStream)> {
    configure_stream(&stream, config.keepalive)?;
    let peer = match &proxy_protocol {
        Some(config) => proxy_client_address(&mut stream, peer, config).await?,
        None => peer,
    };
    let stream: BoxedServiceStream = match tls {
        Some(tls) => tokio::time::timeout(config.tls_handshake_timeout, tls.accept(stream))
            .await
            .map_err(|_| Error::Timeout)??,
        None => Box::new(stream),
    };
    Ok((peer, stream))
}

#[async_trait]
impl GatewayTransportListener for TcpGatewayListener {
    type Io = BoxedServiceStream;

    async fn accept(&mut self) -> Result<(std::net::SocketAddr, Self::Io)> {
        self.incoming.recv().await.ok_or(Error::Unavailable)?
    }
}

impl Drop for TcpGatewayListener {
    fn drop(&mut self) {
        self.task.abort();
    }
}

pub(crate) fn configure_stream(stream: &TcpStream, keepalive: Duration) -> Result<()> {
    stream.set_nodelay(true)?;
    socket2::SockRef::from(stream)
        .set_tcp_keepalive(
            &socket2::TcpKeepalive::new()
                .with_time(keepalive)
                .with_interval(keepalive),
        )
        .map_err(Error::from)
}
