use std::net::SocketAddr;

use async_trait::async_trait;
use elura_core::protocol::{Frame, FrameCodec};
use elura_core::{Error, Result};
use futures_util::{SinkExt, StreamExt};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::sync::mpsc;
use tokio_util::codec::Framed;

/// Internal, decoded connection consumed by the shared Session engine.
pub(crate) struct SessionConnection {
    pub peer: SocketAddr,
    pub inbound: mpsc::Receiver<Result<Frame>>,
    pub responses: mpsc::Sender<Frame>,
    pub pushes: mpsc::Sender<Frame>,
}

pub(crate) struct SessionIoConfig {
    pub max_payload: usize,
    pub inbound_capacity: usize,
    pub response_capacity: usize,
    pub push_capacity: usize,
    pub write_timeout: std::time::Duration,
}

pub(crate) async fn serve_stream<S>(
    stream: S,
    peer: SocketAddr,
    config: SessionIoConfig,
    service: std::sync::Arc<dyn SessionService>,
) -> Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    let framed = Framed::new(stream, FrameCodec::new(config.max_payload)?);
    let (mut sink, mut source) = framed.split();
    let (inbound_tx, inbound) = mpsc::channel(config.inbound_capacity);
    let (responses, mut response_rx) = mpsc::channel::<Frame>(config.response_capacity);
    let (pushes, mut push_rx) = mpsc::channel::<Frame>(config.push_capacity);

    let read_task = tokio::spawn(async move {
        while let Some(result) = source.next().await {
            let result = result.map_err(Error::from);
            let failed = result.is_err();
            if inbound_tx.send(result).await.is_err() || failed {
                break;
            }
        }
    });
    let write_timeout = config.write_timeout;
    let write_task = tokio::spawn(async move {
        while let Some(frame) = next_outbound(&mut response_rx, &mut push_rx).await {
            tokio::time::timeout(write_timeout, sink.send(frame))
                .await
                .map_err(|_| Error::Timeout)??;
        }
        sink.close().await?;
        Result::<()>::Ok(())
    });

    let session = service
        .serve_session(SessionConnection {
            peer,
            inbound,
            responses,
            pushes,
        })
        .await;
    read_task.abort();
    finish_writer(write_task).await;
    session
}

async fn finish_writer(mut task: tokio::task::JoinHandle<Result<()>>) {
    if tokio::time::timeout(std::time::Duration::from_millis(250), &mut task)
        .await
        .is_err()
    {
        task.abort();
    }
}

pub(crate) async fn next_outbound(
    responses: &mut mpsc::Receiver<Frame>,
    pushes: &mut mpsc::Receiver<Frame>,
) -> Option<Frame> {
    tokio::select! {
        biased;
        frame = responses.recv() => match frame {
            Some(frame) => Some(frame),
            None => pushes.recv().await,
        },
        frame = pushes.recv() => match frame {
            Some(frame) => Some(frame),
            None => responses.recv().await,
        },
    }
}

#[async_trait]
pub(crate) trait SessionService: Send + Sync + 'static {
    async fn serve_session(&self, connection: SessionConnection) -> Result<()>;
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::Duration;

    use async_trait::async_trait;
    use bytes::Bytes;
    use elura_core::protocol::{Frame, FrameKind};
    use futures_util::{SinkExt, StreamExt};
    use tokio_util::codec::Framed;

    use super::*;

    #[tokio::test]
    async fn responses_have_priority_over_an_independent_push_queue() {
        let (response_tx, mut responses) = mpsc::channel(1);
        let (push_tx, mut pushes) = mpsc::channel(1);
        push_tx
            .try_send(Frame {
                kind: FrameKind::Push,
                flags: 0,
                route: 101,
                request_id: 0,
                sequence: 0,
                payload: Bytes::new(),
            })
            .unwrap();
        response_tx
            .try_send(Frame::response(
                &Frame::request(100, 1, Bytes::new()).unwrap(),
                Bytes::new(),
            ))
            .unwrap();
        assert_eq!(
            next_outbound(&mut responses, &mut pushes)
                .await
                .unwrap()
                .kind,
            FrameKind::Response
        );
        assert_eq!(
            next_outbound(&mut responses, &mut pushes)
                .await
                .unwrap()
                .kind,
            FrameKind::Push
        );
    }

    struct EchoSession;

    #[async_trait]
    impl SessionService for EchoSession {
        async fn serve_session(&self, mut connection: SessionConnection) -> Result<()> {
            let request = connection
                .inbound
                .recv()
                .await
                .ok_or(Error::Unavailable)??;
            connection
                .responses
                .send(Frame::response(&request, request.payload.clone()))
                .await
                .map_err(|_| Error::Unavailable)
        }
    }

    fn test_config() -> SessionIoConfig {
        SessionIoConfig {
            max_payload: 1024,
            inbound_capacity: 4,
            response_capacity: 4,
            push_capacity: 4,
            write_timeout: Duration::from_secs(1),
        }
    }

    #[tokio::test]
    async fn stream_connections_use_the_gateway_codec() {
        let (client, server) = tokio::io::duplex(1024);
        let task = tokio::spawn(serve_stream(
            server,
            "127.0.0.1:17000".parse().unwrap(),
            test_config(),
            Arc::new(EchoSession),
        ));
        let mut client = Framed::new(client, FrameCodec::new(1024).unwrap());
        let request = Frame::request(100, 7, Bytes::from_static(b"fixed-elr2")).unwrap();
        client.send(request).await.unwrap();
        let response = client.next().await.unwrap().unwrap();
        assert_eq!(response.payload, Bytes::from_static(b"fixed-elr2"));
        task.await.unwrap().unwrap();
    }
}
