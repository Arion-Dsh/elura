use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use elura_core::gateway_world::{GatewayWorldCommand, WorldCommand, WorldRequest};
use elura_core::protocol::{Frame, FrameCodec, FrameKind};
use elura_core::{Error, ErrorEnvelope, Result};
use elura_runtime::security::{BoxedServiceStream, ClientTlsConfig, InternalToken};
use futures_util::{SinkExt, StreamExt};
use tokio::net::TcpStream;
use tokio::sync::{mpsc, oneshot};
use tokio::time::timeout;
use tokio_util::codec::Framed;

use crate::discovery::WorldClient;

pub struct TcpWorldClient {
    address: SocketAddr,
    max_payload: usize,
    connect_timeout: Duration,
    max_in_flight_per_connection: usize,
    pool: Vec<Mutex<Option<mpsc::Sender<PendingWorldRequest>>>>,
    next: AtomicUsize,
    transport_request_id: AtomicU64,
    authorization: Option<InternalToken>,
    tls: Option<ClientTlsConfig>,
}

pub(crate) const WORLD_CONNECTION_IN_FLIGHT: usize = 64;

struct PendingWorldRequest {
    frame: Frame,
    response: oneshot::Sender<Result<Bytes>>,
    deadline: tokio::time::Instant,
}

struct PendingWorldResponse {
    response: Option<oneshot::Sender<Result<Bytes>>>,
    deadline: tokio::time::Instant,
}

#[derive(Clone)]
struct WorldConnectionConfig {
    address: SocketAddr,
    max_payload: usize,
    connect_timeout: Duration,
    max_in_flight: usize,
    tls: Option<ClientTlsConfig>,
}

impl TcpWorldClient {
    pub fn new(address: SocketAddr, max_payload: usize) -> Self {
        Self::with_pool_size(address, max_payload, 16).expect("non-zero static pool size")
    }

    pub fn with_pool_size(
        address: SocketAddr,
        max_payload: usize,
        pool_size: usize,
    ) -> Result<Self> {
        if pool_size == 0 || pool_size > 1024 {
            return Err(Error::InvalidConfig(
                "world connection pool must be in 1..=1024".into(),
            ));
        }
        Ok(Self {
            address,
            max_payload,
            connect_timeout: Duration::from_secs(2),
            max_in_flight_per_connection: WORLD_CONNECTION_IN_FLIGHT,
            pool: (0..pool_size).map(|_| Mutex::new(None)).collect(),
            next: AtomicUsize::new(0),
            transport_request_id: AtomicU64::new(1),
            authorization: None,
            tls: None,
        })
    }

    pub fn with_internal_token(mut self, token: InternalToken) -> Self {
        self.authorization = Some(token);
        self
    }

    pub fn with_tls(mut self, tls: ClientTlsConfig) -> Self {
        self.tls = Some(tls);
        self
    }

    pub fn with_max_in_flight_per_connection(mut self, limit: usize) -> Result<Self> {
        validate_world_connection_in_flight(limit)?;
        self.max_in_flight_per_connection = limit;
        Ok(self)
    }

    fn connection_sender(&self, slot: usize) -> mpsc::Sender<PendingWorldRequest> {
        let mut state = self.pool[slot]
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if let Some(sender) = state.as_ref().filter(|sender| !sender.is_closed()) {
            return sender.clone();
        }
        let (sender, receiver) = mpsc::channel(self.max_in_flight_per_connection);
        tokio::spawn(world_connection_worker(
            WorldConnectionConfig {
                address: self.address,
                max_payload: self.max_payload,
                connect_timeout: self.connect_timeout,
                max_in_flight: self.max_in_flight_per_connection,
                tls: self.tls.clone(),
            },
            receiver,
        ));
        *state = Some(sender.clone());
        sender
    }

    fn invalidate_connection_sender(
        &self,
        slot: usize,
        sender: &mpsc::Sender<PendingWorldRequest>,
    ) {
        let mut state = self.pool[slot]
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if state
            .as_ref()
            .is_some_and(|current| current.same_channel(sender))
        {
            *state = None;
        }
    }

    fn next_transport_request_id(&self) -> u64 {
        loop {
            let current = self.transport_request_id.load(Ordering::Relaxed);
            let next = if current == u64::MAX { 1 } else { current + 1 };
            if self
                .transport_request_id
                .compare_exchange_weak(current, next, Ordering::Relaxed, Ordering::Relaxed)
                .is_ok()
            {
                return current;
            }
        }
    }
}

#[async_trait]
impl WorldClient for TcpWorldClient {
    async fn command(&self, request: WorldRequest) -> Result<Bytes> {
        let slot = self.next.fetch_add(1, Ordering::Relaxed) % self.pool.len();
        let transport_request_id = self.next_transport_request_id();
        let command = WorldCommand {
            authorization: self
                .authorization
                .as_ref()
                .map(|token| token.expose().to_owned()),
            identity: request.identity,
            session_id: request.session_id.to_string(),
            trace_id: request.trace_id,
            request_id: request.request_id,
            payload: request.payload,
            shard_id: request
                .ownership
                .as_ref()
                .map(|assignment| assignment.shard_id),
            owner_id: request
                .ownership
                .as_ref()
                .map(|assignment| assignment.world_id.clone()),
            owner_epoch: request
                .ownership
                .as_ref()
                .map(|assignment| assignment.epoch),
            timeout: request.timeout,
        };
        let protobuf = GatewayWorldCommand::from(command).encode_frame_payload();
        let frame = Frame::request(request.route, transport_request_id, protobuf)?;
        let sender = self.connection_sender(slot);
        let (response, receiver) = oneshot::channel();
        let deadline = tokio::time::Instant::now()
            .checked_add(request.timeout)
            .ok_or_else(|| Error::InvalidConfig("World request timeout is too large".into()))?;
        let enqueue = sender.send(PendingWorldRequest {
            frame,
            response,
            deadline,
        });
        match tokio::time::timeout_at(deadline, enqueue).await {
            Ok(Ok(())) => {}
            Ok(Err(_)) => return Err(Error::Unavailable),
            Err(_) => return Err(Error::Timeout),
        }
        match tokio::time::timeout_at(deadline, receiver).await {
            Ok(Ok(result)) => result,
            Ok(Err(_)) => Err(Error::Unavailable),
            Err(_) => {
                self.invalidate_connection_sender(slot, &sender);
                Err(Error::Timeout)
            }
        }
    }

    async fn readiness(&self) -> Result<()> {
        let config = WorldConnectionConfig {
            address: self.address,
            max_payload: self.max_payload,
            connect_timeout: self.connect_timeout,
            max_in_flight: self.max_in_flight_per_connection,
            tls: self.tls.clone(),
        };
        let _connection = connect_world(&config).await?;
        Ok(())
    }
}

async fn world_connection_worker(
    config: WorldConnectionConfig,
    mut receiver: mpsc::Receiver<PendingWorldRequest>,
) {
    while let Some(first) = receiver.recv().await {
        let connect_deadline = first
            .deadline
            .saturating_duration_since(tokio::time::Instant::now());
        if connect_deadline.is_zero() {
            let _ = first.response.send(Err(Error::Timeout));
            continue;
        }
        let framed = match timeout(connect_deadline, connect_world(&config)).await {
            Ok(Ok(framed)) => framed,
            Ok(Err(error)) => {
                let _ = first.response.send(Err(error));
                continue;
            }
            Err(_) => {
                let _ = first.response.send(Err(Error::Timeout));
                continue;
            }
        };
        let (mut sink, mut source) = framed.split();
        let mut pending = HashMap::with_capacity(config.max_in_flight);
        let mut input_open = true;
        let mut next = Some(first);
        loop {
            if let Some(request) = next.take() {
                let remaining = request
                    .deadline
                    .saturating_duration_since(tokio::time::Instant::now());
                if remaining.is_zero() {
                    let _ = request.response.send(Err(Error::Timeout));
                    continue;
                }
                let request_id = request.frame.request_id;
                match timeout(
                    remaining.min(config.connect_timeout),
                    sink.send(request.frame),
                )
                .await
                {
                    Ok(Ok(())) => {
                        pending.insert(
                            request_id,
                            PendingWorldResponse {
                                response: Some(request.response),
                                deadline: request.deadline,
                            },
                        );
                    }
                    Ok(Err(_)) | Err(_) => {
                        let _ = request.response.send(Err(Error::Unavailable));
                        fail_world_requests(&mut pending);
                        break;
                    }
                }
            }
            if !input_open && !has_active_world_requests(&pending) {
                return;
            }
            let next_deadline = pending
                .values()
                .filter(|request| request.response.is_some())
                .map(|request| request.deadline)
                .min();
            tokio::select! {
                request = receiver.recv(), if input_open && pending.len() < config.max_in_flight => {
                    match request {
                        Some(request) => next = Some(request),
                        None => input_open = false,
                    }
                }
                response = source.next() => {
                    let response = match response {
                        Some(Ok(response)) => response,
                        Some(Err(_)) | None => {
                            fail_world_requests(&mut pending);
                            break;
                        }
                    };
                    let Some(completion) = pending.remove(&response.request_id) else {
                        fail_world_requests(&mut pending);
                        break;
                    };
                    if let Some(completion) = completion.response {
                        let result = match response.kind {
                            FrameKind::Response => Ok(response.payload),
                            FrameKind::Error => match ErrorEnvelope::from_slice(&response.payload) {
                                Ok(envelope) => Err(envelope.into_error()),
                                Err(error) => Err(error),
                            },
                            _ => Err(Error::InvalidFrame("unexpected World response".into())),
                        };
                        let _ = completion.send(result);
                    }
                    if !pending.is_empty() && !has_active_world_requests(&pending) {
                        break;
                    }
                }
                _ = async {
                    match next_deadline {
                        Some(deadline) => tokio::time::sleep_until(deadline).await,
                        None => std::future::pending::<()>().await,
                    }
                }, if next_deadline.is_some() => {
                    let now = tokio::time::Instant::now();
                    for request in pending.values_mut() {
                        if request.deadline <= now
                            && let Some(response) = request.response.take()
                        {
                            let _ = response.send(Err(Error::Timeout));
                        }
                    }
                    if !pending.is_empty() && !has_active_world_requests(&pending) {
                        break;
                    }
                }
            }
        }
    }
}

pub(crate) fn validate_world_connection_in_flight(limit: usize) -> Result<()> {
    if !(1..=4096).contains(&limit) {
        return Err(Error::InvalidConfig(
            "World connection in-flight limit must be in 1..=4096".into(),
        ));
    }
    Ok(())
}

async fn connect_world(
    config: &WorldConnectionConfig,
) -> Result<Framed<BoxedServiceStream, FrameCodec>> {
    let stream = timeout(config.connect_timeout, TcpStream::connect(config.address))
        .await
        .map_err(|_| Error::Timeout)??;
    stream.set_nodelay(true)?;
    let stream: BoxedServiceStream = match &config.tls {
        Some(tls) => timeout(config.connect_timeout, tls.connect(stream))
            .await
            .map_err(|_| Error::Timeout)??,
        None => Box::new(stream),
    };
    Ok(Framed::new(stream, FrameCodec::new(config.max_payload)?))
}

fn fail_world_requests(pending: &mut HashMap<u64, PendingWorldResponse>) {
    for (_, completion) in pending.drain() {
        if let Some(response) = completion.response {
            let _ = response.send(Err(Error::Unavailable));
        }
    }
}

fn has_active_world_requests(pending: &HashMap<u64, PendingWorldResponse>) -> bool {
    pending.values().any(|request| request.response.is_some())
}

#[cfg(test)]
mod tests {
    use elura_core::protocol::FrameCodec;
    use elura_core::session::Identity;
    use futures_util::{SinkExt, StreamExt};
    use tokio::net::TcpListener;
    use uuid::Uuid;

    use super::*;

    fn world_request(timeout: Duration) -> WorldRequest {
        WorldRequest {
            identity: Identity {
                account_id: 1,
                user_id: 1,
                region_id: 1,
                realm_id: 1,
                generation: 1,
            },
            session_id: Uuid::new_v4(),
            trace_id: "test".into(),
            route: 100,
            request_id: 1,
            payload: Bytes::new(),
            ownership: None,
            timeout,
        }
    }

    #[tokio::test]
    async fn request_timeout_replaces_a_half_open_connection() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (first, _) = listener.accept().await.unwrap();
            let mut first = Framed::new(first, FrameCodec::default());
            first.next().await.unwrap().unwrap();
            assert!(first.next().await.is_none());

            let (second, _) = listener.accept().await.unwrap();
            let mut second = Framed::new(second, FrameCodec::default());
            let request = second.next().await.unwrap().unwrap();
            second
                .send(Frame::response(&request, Bytes::from_static(b"ok")))
                .await
                .unwrap();
        });
        let client = TcpWorldClient::with_pool_size(address, 1024, 1).unwrap();
        assert!(matches!(
            client
                .command(world_request(Duration::from_millis(20)))
                .await,
            Err(Error::Timeout)
        ));
        assert_eq!(
            client
                .command(world_request(Duration::from_secs(1)))
                .await
                .unwrap(),
            Bytes::from_static(b"ok")
        );
        server.await.unwrap();
    }

    #[tokio::test]
    async fn request_timeout_does_not_cancel_another_in_flight_request() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let mut stream = Framed::new(stream, FrameCodec::default());
            let first = stream.next().await.unwrap().unwrap();
            let second = stream.next().await.unwrap().unwrap();

            tokio::time::sleep(Duration::from_millis(50)).await;
            stream
                .send(Frame::response(
                    &first,
                    Bytes::from_static(b"late-response"),
                ))
                .await
                .unwrap();
            stream
                .send(Frame::response(&second, Bytes::from_static(b"ok")))
                .await
                .unwrap();
        });
        let client = TcpWorldClient::with_pool_size(address, 1024, 1).unwrap();

        let (first, second) = tokio::join!(
            client.command(world_request(Duration::from_millis(20))),
            client.command(world_request(Duration::from_secs(1))),
        );

        assert!(matches!(first, Err(Error::Timeout)));
        assert_eq!(second.unwrap(), Bytes::from_static(b"ok"));
        server.await.unwrap();
    }
}
