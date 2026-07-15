use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use elura_core::protocol::{Frame, FrameCodec};
use elura_core::{Error, Result};
use futures_util::{SinkExt, StreamExt};
use tokio::net::TcpStream;
use tokio::sync::mpsc;
use tokio_util::codec::Framed;

use elura_runtime::internal::BoxedInternalStream;

use super::session::next_outbound;
use super::{SessionConnection, SessionService};

pub(crate) struct StreamConfig {
    pub(crate) max_payload: usize,
    pub(crate) inbound_capacity: usize,
    pub(crate) response_capacity: usize,
    pub(crate) push_capacity: usize,
    pub(crate) write_timeout: Duration,
}

pub(crate) async fn serve_stream(
    stream: BoxedInternalStream,
    peer: SocketAddr,
    config: StreamConfig,
    service: Arc<dyn SessionService>,
) -> Result<()> {
    let framed = Framed::new(stream, FrameCodec::new(config.max_payload)?);
    let (mut sink, mut source) = framed.split();
    let (inbound_tx, inbound) = mpsc::channel(config.inbound_capacity);
    let (responses, mut response_rx) = mpsc::channel::<Frame>(config.response_capacity);
    let (pushes, mut push_rx) = mpsc::channel::<Frame>(config.push_capacity);

    let reader = tokio::spawn(async move {
        while let Some(result) = source.next().await {
            let result = result.map_err(Error::from);
            let failed = result.is_err();
            if inbound_tx.send(result).await.is_err() || failed {
                break;
            }
        }
    });
    let writer = tokio::spawn(async move {
        while let Some(frame) = next_outbound(&mut response_rx, &mut push_rx).await {
            tokio::time::timeout(config.write_timeout, sink.send(frame))
                .await
                .map_err(|_| Error::Timeout)??;
        }
        Result::<()>::Ok(())
    });

    let result = service
        .serve_session(SessionConnection {
            peer,
            inbound,
            responses,
            pushes,
        })
        .await;
    reader.abort();
    let mut writer = writer;
    if tokio::time::timeout(std::time::Duration::from_millis(250), &mut writer)
        .await
        .is_err()
    {
        writer.abort();
    }
    result
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
