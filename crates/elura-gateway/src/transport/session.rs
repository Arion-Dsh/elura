use std::net::SocketAddr;

use async_trait::async_trait;
use elura_core::Result;
use elura_core::protocol::Frame;
use tokio::sync::mpsc;

/// A protocol-neutral, ordered stream of Elura frames.
pub(crate) struct SessionConnection {
    pub peer: SocketAddr,
    pub inbound: mpsc::Receiver<Result<Frame>>,
    pub responses: mpsc::Sender<Frame>,
    pub pushes: mpsc::Sender<Frame>,
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
    use bytes::Bytes;
    use elura_core::protocol::{Frame, FrameKind};

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
}
