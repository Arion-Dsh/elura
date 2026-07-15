use std::net::{IpAddr, SocketAddr, TcpListener};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use bytes::{Bytes, BytesMut};
use elura_core::ErrorEnvelope;
use elura_core::account_version::{AccountVersionKey, AccountVersionStore};
use elura_core::online::{DuplicateLoginMode, MemoryOnlineDirectory};
use elura_core::protocol::{
    Frame, FrameCodec, FrameKind, PROTOCOL_IDENTIFIER, ROUTE_AUTHENTICATE, ROUTE_HEARTBEAT,
    ROUTE_SESSION_CONTROL, SessionControl, SessionControlAction,
};
use elura_core::session::{
    Identity, SessionControlEvent, SessionControlHandler, SessionControlKind,
    SessionControlTransport, SessionState,
};
use elura_core::ticket::{MemoryReplayStore, TicketService};
use elura_gateway::transport::{
    AccountVersionSettings, AdmissionController, AdmissionDecision, AdmissionRejection,
    AdmissionRequest, AdmissionSettings, AdmissionStage, ProxyProtocolConfig, SessionEvent,
    SessionEventKind, TrustedProxies, WebSocketConfig,
};
use elura_gateway::{
    Gateway, GatewayConfig, RouteRateLimit, TcpWorldClient, WorldClient, WorldRequest,
};
use elura_monolith::{MonolithLaunchConfig, MonolithLauncher};
use elura_runtime::security::InternalToken;
use elura_world::{WorldConfig, WorldServer};
use futures_util::{SinkExt, StreamExt};
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;
use tokio::sync::{Notify, watch};
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::http::HeaderValue;
use tokio_util::codec::Framed;
use tokio_util::codec::{Decoder, Encoder};

fn free_address() -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    listener.local_addr().unwrap()
}

struct NeverWorld;

#[async_trait::async_trait]
impl WorldClient for NeverWorld {
    async fn command(&self, _request: WorldRequest) -> elura_core::Result<Bytes> {
        panic!("admission denial must happen before World forwarding")
    }
}

#[tokio::test]
async fn monolith_routes_gateway_requests_without_world_tcp() {
    let gateway_address = free_address();
    let admin_address = free_address();
    let key = "m".repeat(32);
    let mut config = MonolithLaunchConfig::default();
    config.gateway.listen = gateway_address;
    config.gateway.shutdown_timeout = Duration::from_millis(200);
    config.admin.listen = admin_address;
    config.ticket.key = key.clone();
    let tickets = TicketService::new(
        key,
        config.ticket.issuer.clone(),
        config.ticket.audience.clone(),
        config.ticket.ttl,
    )
    .unwrap();
    let ticket = tickets
        .issue(Identity {
            account_id: 1,
            user_id: 2,
            region_id: 1,
            realm_id: 1,
            generation: 1,
        })
        .unwrap();
    let launcher = MonolithLauncher::new(config)
        .unwrap()
        .configure_world(|world| {
            world.register(100, |_context, payload: Bytes| async move { Ok(payload) })?;
            Ok(())
        })
        .unwrap();
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let task = tokio::spawn(launcher.run_until(shutdown_rx));
    let stream = tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            match TcpStream::connect(gateway_address).await {
                Ok(stream) => break stream,
                Err(_) => tokio::task::yield_now().await,
            }
        }
    })
    .await
    .unwrap();
    let mut protocol = Framed::new(stream, FrameCodec::default());
    protocol
        .send(
            Frame::request(
                ROUTE_AUTHENTICATE,
                1,
                serde_json::to_vec(&serde_json::json!({ "ticket": ticket })).unwrap(),
            )
            .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(
        protocol.next().await.unwrap().unwrap().kind,
        FrameKind::Response
    );
    protocol
        .send(Frame::request(100, 2, Bytes::from_static(b"in-process")).unwrap())
        .await
        .unwrap();
    let response = protocol.next().await.unwrap().unwrap();
    assert_eq!(response.kind, FrameKind::Response);
    assert_eq!(response.payload, Bytes::from_static(b"in-process"));
    drop(protocol);
    shutdown_tx.send(true).unwrap();
    task.await.unwrap().unwrap();
}

#[tokio::test]
async fn gateway_readiness_includes_required_dependencies() {
    let tickets = Arc::new(
        TicketService::new(
            [41_u8; 32],
            "test-auth",
            "test-gateway",
            Duration::from_secs(60),
        )
        .unwrap(),
    );
    let gateway = Gateway::new(
        GatewayConfig::default(),
        tickets,
        Arc::new(MemoryReplayStore::default()),
        Arc::new(NeverWorld),
    )
    .unwrap()
    .with_readiness_probe(
        "redis",
        Arc::new(|| async { Err(elura_core::Error::Unavailable) }),
    )
    .unwrap();
    let readiness = gateway.readiness().await;
    assert!(!readiness.ready);
    assert_eq!(
        readiness.reason.as_deref(),
        Some("redis dependency is unavailable")
    );
}

#[derive(Default)]
struct CountingWorld(AtomicUsize);

#[async_trait::async_trait]
impl WorldClient for CountingWorld {
    async fn command(&self, request: WorldRequest) -> elura_core::Result<Bytes> {
        self.0.fetch_add(1, Ordering::Relaxed);
        Ok(request.payload)
    }
}

#[derive(Default)]
struct RecordingSessionControl(Mutex<Vec<SessionControlEvent>>);

#[async_trait::async_trait]
impl SessionControlTransport for RecordingSessionControl {
    async fn publish(&self, event: &SessionControlEvent) -> elura_core::Result<()> {
        self.0
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .push(event.clone());
        Ok(())
    }

    async fn subscribe(
        &self,
        _handler: Arc<dyn SessionControlHandler>,
        _shutdown: tokio::sync::watch::Receiver<bool>,
    ) -> elura_core::Result<()> {
        Err(elura_core::Error::Unavailable)
    }
}

struct AtomicVersionStore(AtomicU64);

impl AtomicVersionStore {
    fn new(version: u64) -> Self {
        Self(AtomicU64::new(version))
    }

    fn set(&self, version: u64) {
        self.0.store(version, Ordering::Release);
    }
}

#[derive(Default)]
struct BlockingWorld {
    started: Notify,
    release: Notify,
}

#[async_trait::async_trait]
impl WorldClient for BlockingWorld {
    async fn command(&self, request: WorldRequest) -> elura_core::Result<Bytes> {
        self.started.notify_one();
        self.release.notified().await;
        Ok(request.payload)
    }
}

#[tokio::test]
async fn gateway_drain_allows_an_inflight_request_to_finish() {
    let gateway_address = free_address();
    let tickets = Arc::new(
        TicketService::new(
            [11_u8; 32],
            "test-auth",
            "test-gateway",
            Duration::from_secs(60),
        )
        .unwrap(),
    );
    let ticket = tickets
        .issue(Identity {
            account_id: 1,
            user_id: 2,
            region_id: 1,
            realm_id: 1,
            generation: 1,
        })
        .unwrap();
    let world = Arc::new(BlockingWorld::default());
    let gateway = Arc::new(
        Gateway::new(
            GatewayConfig {
                listen: gateway_address,
                shutdown_timeout: Duration::from_millis(500),
                ..GatewayConfig::default()
            },
            tickets,
            Arc::new(MemoryReplayStore::default()),
            world.clone(),
        )
        .unwrap(),
    );

    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let gateway_task = tokio::spawn(gateway.clone().serve_tcp(shutdown_rx));
    tokio::time::sleep(Duration::from_millis(20)).await;
    let mut protocol = Framed::new(
        TcpStream::connect(gateway_address).await.unwrap(),
        FrameCodec::default(),
    );
    protocol
        .send(
            Frame::request(
                ROUTE_AUTHENTICATE,
                1,
                serde_json::to_vec(&serde_json::json!({ "ticket": ticket })).unwrap(),
            )
            .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(
        protocol.next().await.unwrap().unwrap().kind,
        FrameKind::Response
    );
    protocol
        .send(Frame::request(100, 2, Bytes::from_static(b"finish-me")).unwrap())
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(1), world.started.notified())
        .await
        .unwrap();
    shutdown_tx.send(true).unwrap();
    for _ in 0..20 {
        if gateway.is_draining() {
            break;
        }
        tokio::task::yield_now().await;
    }
    assert!(gateway.is_draining());
    assert_eq!(gateway.active_session_count(), 1);
    assert!(!gateway_task.is_finished());

    world.release.notify_one();
    let response = protocol.next().await.unwrap().unwrap();
    assert_eq!(response.kind, FrameKind::Response);
    assert_eq!(response.payload, Bytes::from_static(b"finish-me"));
    drop(protocol);
    gateway_task.await.unwrap().unwrap();
    assert_eq!(gateway.active_session_count(), 0);
}

#[tokio::test]
async fn gateway_drain_deadline_forces_remaining_sessions() {
    let gateway_address = free_address();
    let tickets = Arc::new(
        TicketService::new(
            [12_u8; 32],
            "test-auth",
            "test-gateway",
            Duration::from_secs(60),
        )
        .unwrap(),
    );
    let ticket = tickets
        .issue(Identity {
            account_id: 1,
            user_id: 2,
            region_id: 1,
            realm_id: 1,
            generation: 1,
        })
        .unwrap();
    let gateway = Arc::new(
        Gateway::new(
            GatewayConfig {
                listen: gateway_address,
                shutdown_timeout: Duration::from_millis(40),
                ..GatewayConfig::default()
            },
            tickets,
            Arc::new(MemoryReplayStore::default()),
            Arc::new(NeverWorld),
        )
        .unwrap(),
    );

    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let gateway_task = tokio::spawn(gateway.clone().serve_tcp(shutdown_rx));
    tokio::time::sleep(Duration::from_millis(20)).await;
    let mut protocol = Framed::new(
        TcpStream::connect(gateway_address).await.unwrap(),
        FrameCodec::default(),
    );
    protocol
        .send(
            Frame::request(
                ROUTE_AUTHENTICATE,
                1,
                serde_json::to_vec(&serde_json::json!({ "ticket": ticket })).unwrap(),
            )
            .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(
        protocol.next().await.unwrap().unwrap().kind,
        FrameKind::Response
    );
    shutdown_tx.send(true).unwrap();
    let control = tokio::time::timeout(Duration::from_secs(1), protocol.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(control.kind, FrameKind::Push);
    assert_eq!(control.route, ROUTE_SESSION_CONTROL);
    assert_eq!(
        SessionControl::decode_frame_payload(control.payload)
            .unwrap()
            .action_kind()
            .unwrap(),
        SessionControlAction::ServerDraining
    );
    assert!(matches!(
        gateway_task.await.unwrap(),
        Err(elura_core::Error::Timeout)
    ));
    assert_eq!(gateway.active_session_count(), 0);
}

#[async_trait::async_trait]
impl AccountVersionStore for AtomicVersionStore {
    async fn current(&self, _key: AccountVersionKey) -> elura_core::Result<Option<u64>> {
        Ok(Some(self.0.load(Ordering::Acquire)))
    }
}

#[tokio::test]
async fn stale_ticket_is_rejected_by_authoritative_account_version() {
    let gateway_address = free_address();
    let tickets = Arc::new(
        TicketService::new(
            [6_u8; 32],
            "test-auth",
            "test-gateway",
            Duration::from_secs(60),
        )
        .unwrap(),
    );
    let ticket = tickets
        .issue(Identity {
            account_id: 1,
            user_id: 2,
            region_id: 3,
            realm_id: 4,
            generation: 1,
        })
        .unwrap();
    let gateway = Gateway::new(
        GatewayConfig {
            listen: gateway_address,
            ..GatewayConfig::default()
        },
        tickets,
        Arc::new(MemoryReplayStore::default()),
        Arc::new(NeverWorld),
    )
    .unwrap()
    .with_account_version_store(
        Arc::new(AtomicVersionStore::new(2)),
        AccountVersionSettings::default(),
    )
    .unwrap();

    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let gateway_task = tokio::spawn(gateway.serve(shutdown_rx));
    tokio::time::sleep(Duration::from_millis(20)).await;
    let mut protocol = Framed::new(
        TcpStream::connect(gateway_address).await.unwrap(),
        FrameCodec::default(),
    );
    protocol
        .send(
            Frame::request(
                ROUTE_AUTHENTICATE,
                1,
                serde_json::to_vec(&serde_json::json!({ "ticket": ticket })).unwrap(),
            )
            .unwrap(),
        )
        .await
        .unwrap();
    let response = protocol.next().await.unwrap().unwrap();
    assert_eq!(response.kind, FrameKind::Error);
    assert_eq!(
        ErrorEnvelope::from_slice(&response.payload).unwrap().code,
        "SESSION_REVOKED"
    );

    drop(protocol);
    shutdown_tx.send(true).unwrap();
    gateway_task.await.unwrap().unwrap();
}

#[tokio::test]
async fn periodic_account_version_check_disconnects_a_stale_session() {
    let gateway_address = free_address();
    let tickets = Arc::new(
        TicketService::new(
            [8_u8; 32],
            "test-auth",
            "test-gateway",
            Duration::from_secs(60),
        )
        .unwrap(),
    );
    let identity = Identity {
        account_id: 1,
        user_id: 20,
        region_id: 1,
        realm_id: 2,
        generation: 1,
    };
    let ticket = tickets.issue(identity).unwrap();
    let versions = Arc::new(AtomicVersionStore::new(1));
    let gateway = Gateway::new(
        GatewayConfig {
            listen: gateway_address,
            ..GatewayConfig::default()
        },
        tickets,
        Arc::new(MemoryReplayStore::default()),
        Arc::new(NeverWorld),
    )
    .unwrap()
    .with_account_version_store(
        versions.clone(),
        AccountVersionSettings {
            check_interval: Duration::from_millis(20),
            ..AccountVersionSettings::default()
        },
    )
    .unwrap();

    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let gateway_task = tokio::spawn(gateway.serve(shutdown_rx));
    tokio::time::sleep(Duration::from_millis(20)).await;
    let mut protocol = Framed::new(
        TcpStream::connect(gateway_address).await.unwrap(),
        FrameCodec::default(),
    );
    protocol
        .send(
            Frame::request(
                ROUTE_AUTHENTICATE,
                1,
                serde_json::to_vec(&serde_json::json!({ "ticket": ticket })).unwrap(),
            )
            .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(
        protocol.next().await.unwrap().unwrap().kind,
        FrameKind::Response
    );
    versions.set(2);
    tokio::time::sleep(Duration::from_millis(25)).await;
    protocol
        .send(Frame::request(ROUTE_HEARTBEAT, 2, Bytes::new()).unwrap())
        .await
        .unwrap();
    let control = protocol.next().await.unwrap().unwrap();
    assert_eq!(control.kind, FrameKind::Push);
    assert_eq!(control.route, ROUTE_SESSION_CONTROL);
    assert_eq!(
        SessionControl::decode_frame_payload(control.payload)
            .unwrap()
            .action_kind()
            .unwrap(),
        SessionControlAction::AccountVersionChanged
    );

    drop(protocol);
    shutdown_tx.send(true).unwrap();
    gateway_task.await.unwrap().unwrap();
}

#[tokio::test]
async fn local_account_version_revocation_targets_only_older_sessions() {
    let gateway_address = free_address();
    let tickets = Arc::new(
        TicketService::new(
            [10_u8; 32],
            "test-auth",
            "test-gateway",
            Duration::from_secs(60),
        )
        .unwrap(),
    );
    let identity = Identity {
        account_id: 1,
        user_id: 30,
        region_id: 1,
        realm_id: 2,
        generation: 1,
    };
    let ticket = tickets.issue(identity.clone()).unwrap();
    let newer_ticket = tickets
        .issue(Identity {
            generation: 2,
            ..identity.clone()
        })
        .unwrap();
    let gateway = Arc::new(
        Gateway::new(
            GatewayConfig {
                listen: gateway_address,
                ..GatewayConfig::default()
            },
            tickets,
            Arc::new(MemoryReplayStore::default()),
            Arc::new(NeverWorld),
        )
        .unwrap(),
    );

    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let gateway_task = tokio::spawn(gateway.clone().serve_tcp(shutdown_rx));
    tokio::time::sleep(Duration::from_millis(20)).await;
    let mut protocol = Framed::new(
        TcpStream::connect(gateway_address).await.unwrap(),
        FrameCodec::default(),
    );
    protocol
        .send(
            Frame::request(
                ROUTE_AUTHENTICATE,
                1,
                serde_json::to_vec(&serde_json::json!({ "ticket": ticket })).unwrap(),
            )
            .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(
        protocol.next().await.unwrap().unwrap().kind,
        FrameKind::Response
    );
    let mut newer = Framed::new(
        TcpStream::connect(gateway_address).await.unwrap(),
        FrameCodec::default(),
    );
    newer
        .send(
            Frame::request(
                ROUTE_AUTHENTICATE,
                1,
                serde_json::to_vec(&serde_json::json!({ "ticket": newer_ticket })).unwrap(),
            )
            .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(
        newer.next().await.unwrap().unwrap().kind,
        FrameKind::Response
    );
    assert_eq!(
        gateway
            .revoke_account_version(
                identity.region_id,
                identity.realm_id,
                identity.user_id,
                2,
                "credentials rotated",
            )
            .await
            .unwrap(),
        1
    );
    let control = protocol.next().await.unwrap().unwrap();
    assert_eq!(control.kind, FrameKind::Push);
    assert_eq!(control.route, ROUTE_SESSION_CONTROL);
    let control = SessionControl::decode_frame_payload(control.payload).unwrap();
    assert_eq!(
        control.action_kind().unwrap(),
        SessionControlAction::AccountVersionChanged
    );
    assert_eq!(control.reason, "credentials rotated");
    newer
        .send(Frame::request(ROUTE_HEARTBEAT, 2, Bytes::new()).unwrap())
        .await
        .unwrap();
    assert_eq!(
        newer.next().await.unwrap().unwrap().kind,
        FrameKind::Response
    );

    drop(protocol);
    drop(newer);
    shutdown_tx.send(true).unwrap();
    gateway_task.await.unwrap().unwrap();
}

#[derive(Default)]
struct RecordingAdmission {
    stages: Mutex<Vec<AdmissionStage>>,
    remote_ips: Mutex<Vec<IpAddr>>,
}

#[async_trait::async_trait]
impl AdmissionController for RecordingAdmission {
    async fn admit(&self, request: &AdmissionRequest) -> elura_core::Result<AdmissionDecision> {
        self.stages.lock().unwrap().push(request.stage);
        self.remote_ips.lock().unwrap().push(request.remote_ip);
        if request.stage == AdmissionStage::Authenticated {
            return Ok(AdmissionDecision::Deny(AdmissionRejection::new(
                "user_banned",
                "account temporarily suspended",
                Some(Duration::from_secs(30)),
            )?));
        }
        Ok(AdmissionDecision::Allow)
    }
}

#[tokio::test]
async fn session_observers_receive_isolated_ordered_lifecycle_events() {
    let gateway_address = free_address();
    let tickets = Arc::new(
        TicketService::new(
            [5_u8; 32],
            "test-auth",
            "test-gateway",
            Duration::from_secs(60),
        )
        .unwrap(),
    );
    let identity = Identity {
        account_id: 10,
        user_id: 20,
        region_id: 1,
        realm_id: 2,
        generation: 3,
    };
    let ticket = tickets.issue(identity.clone()).unwrap();
    let observed = Arc::new(Mutex::new(Vec::<SessionEvent>::new()));
    let sink = observed.clone();
    let gateway = Gateway::new(
        GatewayConfig {
            listen: gateway_address,
            ..GatewayConfig::default()
        },
        tickets,
        Arc::new(MemoryReplayStore::default()),
        Arc::new(NeverWorld),
    )
    .unwrap()
    .with_session_observer(Arc::new(|_event: SessionEvent| {
        Err(elura_core::Error::Internal("observer unavailable".into()))
    }))
    .with_session_observer(Arc::new(move |event: SessionEvent| {
        sink.lock().unwrap().push(event);
        Ok(())
    }));

    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let gateway_task = tokio::spawn(gateway.serve(shutdown_rx));
    tokio::time::sleep(Duration::from_millis(20)).await;
    let stream = TcpStream::connect(gateway_address).await.unwrap();
    let mut protocol = Framed::new(stream, FrameCodec::default());
    protocol
        .send(
            Frame::request(
                ROUTE_AUTHENTICATE,
                1,
                serde_json::to_vec(&serde_json::json!({ "ticket": ticket })).unwrap(),
            )
            .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(
        protocol.next().await.unwrap().unwrap().kind,
        FrameKind::Response
    );
    drop(protocol);

    for _ in 0..50 {
        if observed.lock().unwrap().len() == 3 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    let events = observed.lock().unwrap().clone();
    assert_eq!(
        events.iter().map(|event| event.kind).collect::<Vec<_>>(),
        vec![
            SessionEventKind::Connected,
            SessionEventKind::Authenticated,
            SessionEventKind::Closed,
        ]
    );
    assert_eq!(events[0].session.state, SessionState::Anonymous);
    assert_eq!(events[1].session.state, SessionState::Authenticated);
    assert_eq!(events[2].session.state, SessionState::Closed);
    assert_eq!(events[2].session.identity, Some(identity));
    assert!(events.windows(2).all(|pair| {
        pair[0].session.id == pair[1].session.id
            && pair[0].session.last_activity_at <= pair[1].session.last_activity_at
    }));

    shutdown_tx.send(true).unwrap();
    gateway_task.await.unwrap().unwrap();
}

#[tokio::test]
async fn proxy_protocol_source_reaches_gateway_admission() {
    let gateway_address = free_address();
    let tickets = Arc::new(
        TicketService::new(
            [4_u8; 32],
            "test-auth",
            "test-gateway",
            Duration::from_secs(60),
        )
        .unwrap(),
    );
    let ticket = tickets
        .issue(Identity {
            account_id: 1,
            user_id: 2,
            region_id: 3,
            realm_id: 4,
            generation: 1,
        })
        .unwrap();
    let admission = Arc::new(RecordingAdmission::default());
    let proxy =
        ProxyProtocolConfig::new(TrustedProxies::parse(["127.0.0.0/8", "::1/128"]).unwrap())
            .unwrap();
    let gateway = Gateway::new(
        GatewayConfig {
            listen: gateway_address,
            ..GatewayConfig::default()
        },
        tickets,
        Arc::new(MemoryReplayStore::default()),
        Arc::new(NeverWorld),
    )
    .unwrap()
    .with_admission(admission.clone(), AdmissionSettings::default())
    .unwrap()
    .with_proxy_protocol(proxy)
    .unwrap();

    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let gateway_task = tokio::spawn(gateway.serve(shutdown_rx));
    tokio::time::sleep(Duration::from_millis(20)).await;
    let mut stream = TcpStream::connect(gateway_address).await.unwrap();
    stream
        .write_all(b"PROXY TCP4 192.0.2.77 127.0.0.1 32100 17000\r\n")
        .await
        .unwrap();
    let mut protocol = Framed::new(stream, FrameCodec::default());
    protocol
        .send(
            Frame::request(
                ROUTE_AUTHENTICATE,
                1,
                serde_json::to_vec(&serde_json::json!({ "ticket": ticket })).unwrap(),
            )
            .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(
        protocol.next().await.unwrap().unwrap().kind,
        FrameKind::Error
    );
    assert_eq!(
        *admission.remote_ips.lock().unwrap(),
        vec!["192.0.2.77".parse::<IpAddr>().unwrap(); 2]
    );

    drop(protocol);
    shutdown_tx.send(true).unwrap();
    gateway_task.await.unwrap().unwrap();
}

#[tokio::test]
async fn admission_checks_connection_and_authenticated_identity() {
    let gateway_address = free_address();
    let tickets = Arc::new(
        TicketService::new(
            [3_u8; 32],
            "test-auth",
            "test-gateway",
            Duration::from_secs(60),
        )
        .unwrap(),
    );
    let ticket = tickets
        .issue(Identity {
            account_id: 1,
            user_id: 2,
            region_id: 3,
            realm_id: 4,
            generation: 1,
        })
        .unwrap();
    let admission = Arc::new(RecordingAdmission::default());
    let gateway = Gateway::new(
        GatewayConfig {
            listen: gateway_address,
            ..GatewayConfig::default()
        },
        tickets,
        Arc::new(MemoryReplayStore::default()),
        Arc::new(NeverWorld),
    )
    .unwrap()
    .with_admission(admission.clone(), AdmissionSettings::default())
    .unwrap();

    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let gateway_task = tokio::spawn(gateway.serve(shutdown_rx));
    tokio::time::sleep(Duration::from_millis(20)).await;
    let stream = TcpStream::connect(gateway_address).await.unwrap();
    let mut protocol = Framed::new(stream, FrameCodec::default());
    protocol
        .send(
            Frame::request(
                ROUTE_AUTHENTICATE,
                1,
                serde_json::to_vec(&serde_json::json!({ "ticket": ticket })).unwrap(),
            )
            .unwrap(),
        )
        .await
        .unwrap();
    let response = protocol.next().await.unwrap().unwrap();
    assert_eq!(response.kind, FrameKind::Error);
    assert_eq!(
        ErrorEnvelope::from_slice(&response.payload).unwrap().code,
        "USER_BANNED"
    );
    assert_eq!(
        *admission.stages.lock().unwrap(),
        vec![AdmissionStage::Connected, AdmissionStage::Authenticated]
    );

    drop(protocol);
    shutdown_tx.send(true).unwrap();
    gateway_task.await.unwrap().unwrap();
}

#[tokio::test]
async fn websocket_uses_the_gateway_session_engine() {
    let world_address = free_address();
    let gateway_address = free_address();
    let websocket_address = free_address();
    let internal_token = InternalToken::new("fedcba9876543210fedcba9876543210").unwrap();

    let mut world = WorldServer::builder(WorldConfig {
        listen: world_address,
        ..WorldConfig::default()
    })
    .unwrap();
    world
        .register(100, |_context, payload| async move { Ok(payload) })
        .unwrap();
    let world = world
        .build()
        .unwrap()
        .with_internal_token(internal_token.clone());

    let tickets = Arc::new(
        TicketService::new(
            [7_u8; 32],
            "test-auth",
            "test-gateway",
            Duration::from_secs(60),
        )
        .unwrap(),
    );
    let identity = Identity {
        account_id: 7,
        user_id: 8,
        region_id: 1,
        realm_id: 1,
        generation: 1,
    };
    let ticket = tickets.issue(identity.clone()).unwrap();
    let gateway = Arc::new(
        Gateway::new(
            GatewayConfig {
                listen: gateway_address,
                ..GatewayConfig::default()
            },
            tickets,
            Arc::new(MemoryReplayStore::default()),
            Arc::new(
                TcpWorldClient::new(world_address, 1 << 20).with_internal_token(internal_token),
            ),
        )
        .unwrap(),
    );
    let websocket_config = WebSocketConfig {
        listen: websocket_address,
        allowed_origins: vec!["https://game.example.com".into()],
        ..WebSocketConfig::default()
    };

    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let world_task = tokio::spawn(world.serve(shutdown_rx.clone()));
    tokio::time::sleep(Duration::from_millis(20)).await;
    let tcp_task = tokio::spawn(gateway.clone().serve_tcp(shutdown_rx.clone()));
    let websocket_task = tokio::spawn(
        gateway
            .clone()
            .serve_websocket(websocket_config, shutdown_rx.clone()),
    );
    tokio::time::sleep(Duration::from_millis(20)).await;

    let uri = format!("ws://{websocket_address}/elura/game");
    let mut rejected = uri.clone().into_client_request().unwrap();
    rejected.headers_mut().insert(
        "sec-websocket-protocol",
        HeaderValue::from_static(PROTOCOL_IDENTIFIER),
    );
    assert!(tokio_tungstenite::connect_async(rejected).await.is_err());

    let mut request = uri.into_client_request().unwrap();
    request.headers_mut().insert(
        "origin",
        HeaderValue::from_static("https://game.example.com"),
    );
    request.headers_mut().insert(
        "sec-websocket-protocol",
        HeaderValue::from_static(PROTOCOL_IDENTIFIER),
    );
    let (mut socket, response) = tokio_tungstenite::connect_async(request).await.unwrap();
    assert_eq!(
        response.headers()["sec-websocket-protocol"],
        PROTOCOL_IDENTIFIER
    );

    let authentication = Frame::request(
        ROUTE_AUTHENTICATE,
        1,
        serde_json::to_vec(&serde_json::json!({ "ticket": ticket })).unwrap(),
    )
    .unwrap();
    socket
        .send(Message::Binary(encode_websocket(authentication)))
        .await
        .unwrap();
    assert_eq!(
        receive_websocket(&mut socket).await.kind,
        FrameKind::Response
    );

    socket
        .send(Message::Binary(encode_websocket(
            Frame::request(100, 2, Bytes::from_static(b"websocket")).unwrap(),
        )))
        .await
        .unwrap();
    let response = receive_websocket(&mut socket).await;
    assert_eq!(response.kind, FrameKind::Response);
    assert_eq!(response.payload, Bytes::from_static(b"websocket"));

    shutdown_tx.send(true).unwrap();
    for _ in 0..20 {
        if gateway.is_draining() {
            break;
        }
        tokio::task::yield_now().await;
    }
    assert!(gateway.is_draining());
    socket.close(None).await.unwrap();
    drop(socket);
    world_task.await.unwrap().unwrap();
    tcp_task.await.unwrap().unwrap();
    websocket_task.await.unwrap().unwrap();
}

fn encode_websocket(frame: Frame) -> Bytes {
    let mut output = BytesMut::new();
    FrameCodec::default().encode(frame, &mut output).unwrap();
    output.freeze()
}

async fn receive_websocket(
    socket: &mut tokio_tungstenite::WebSocketStream<
        tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
    >,
) -> Frame {
    let Message::Binary(binary) = socket.next().await.unwrap().unwrap() else {
        panic!("expected binary WebSocket message");
    };
    let mut input = BytesMut::from(binary.as_ref());
    FrameCodec::default().decode(&mut input).unwrap().unwrap()
}

#[tokio::test]
async fn ticket_gateway_world_round_trip() {
    let world_address = free_address();
    let gateway_address = free_address();

    let mut world = WorldServer::builder(WorldConfig {
        listen: world_address,
        ..WorldConfig::default()
    })
    .unwrap();
    world
        .register(100, |_context, payload| async move { Ok(payload) })
        .unwrap();
    world
        .register(101, |_context, _payload| async move {
            Err(elura_core::Error::business(
                "NOT_ENOUGH_GOLD",
                "not enough gold",
            ))
        })
        .unwrap();
    let internal_token = InternalToken::new("0123456789abcdef0123456789abcdef").unwrap();
    let world = world
        .build()
        .unwrap()
        .with_internal_token(internal_token.clone());

    let tickets = Arc::new(
        TicketService::new(
            [9_u8; 32],
            "test-auth",
            "test-gateway",
            Duration::from_secs(60),
        )
        .unwrap(),
    );
    let identity = Identity {
        account_id: 11,
        user_id: 22,
        region_id: 1,
        realm_id: 2,
        generation: 1,
    };
    let ticket = tickets.issue(identity.clone()).unwrap();
    let second_ticket = tickets.issue(identity.clone()).unwrap();
    let config = GatewayConfig {
        listen: gateway_address,
        ..GatewayConfig::default()
    };
    let gateway = Gateway::new(
        config.clone(),
        tickets,
        Arc::new(MemoryReplayStore::default()),
        Arc::new(
            TcpWorldClient::new(world_address, config.max_payload)
                .with_internal_token(internal_token),
        ),
    )
    .unwrap()
    .with_online_directory(
        "gateway-test",
        Arc::new(MemoryOnlineDirectory::default()),
        Duration::from_secs(30),
        Duration::from_secs(10),
        DuplicateLoginMode::KickExisting,
    )
    .unwrap();

    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let world_shutdown = shutdown_rx.clone();
    let gateway_shutdown = shutdown_rx.clone();
    let world_task = tokio::spawn(world.serve(world_shutdown));
    tokio::time::sleep(Duration::from_millis(20)).await;
    let gateway_task = tokio::spawn(gateway.serve(gateway_shutdown));
    tokio::time::sleep(Duration::from_millis(20)).await;

    let stream = TcpStream::connect(gateway_address).await.unwrap();
    let mut protocol = Framed::new(stream, FrameCodec::new(1 << 20).unwrap());
    protocol
        .send(
            Frame::request(
                ROUTE_AUTHENTICATE,
                1,
                serde_json::to_vec(&serde_json::json!({ "ticket": ticket })).unwrap(),
            )
            .unwrap(),
        )
        .await
        .unwrap();
    let authentication = protocol.next().await.unwrap().unwrap();
    assert_eq!(authentication.kind, FrameKind::Response);
    let authenticated: serde_json::Value = serde_json::from_slice(&authentication.payload).unwrap();
    assert_eq!(authenticated["identity"]["user_id"], identity.user_id);

    protocol
        .send(Frame::request(100, 2, Bytes::from_static(b"hello")).unwrap())
        .await
        .unwrap();
    let response = protocol.next().await.unwrap().unwrap();
    assert_eq!(response.kind, FrameKind::Response);
    assert_eq!(response.payload, Bytes::from_static(b"hello"));

    protocol
        .send(Frame::request(101, 3, Bytes::new()).unwrap())
        .await
        .unwrap();
    let response = protocol.next().await.unwrap().unwrap();
    assert_eq!(response.kind, FrameKind::Error);
    let error = ErrorEnvelope::from_slice(&response.payload).unwrap();
    assert_eq!(error.code, "NOT_ENOUGH_GOLD");
    assert_eq!(error.message, "not enough gold");
    assert!(!error.retryable);

    let unauthorized = TcpWorldClient::new(world_address, config.max_payload);
    let error = unauthorized
        .command(WorldRequest {
            identity,
            session_id: uuid::Uuid::new_v4(),
            trace_id: "untrusted".into(),
            route: 100,
            request_id: 1,
            payload: Bytes::new(),
            ownership: None,
            timeout: Duration::from_secs(5),
        })
        .await
        .unwrap_err();
    assert!(error.to_string().contains("authentication failed"));

    let second_stream = TcpStream::connect(gateway_address).await.unwrap();
    let mut second = Framed::new(second_stream, FrameCodec::new(1 << 20).unwrap());
    second
        .send(
            Frame::request(
                ROUTE_AUTHENTICATE,
                1,
                serde_json::to_vec(&serde_json::json!({ "ticket": second_ticket })).unwrap(),
            )
            .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(
        second.next().await.unwrap().unwrap().kind,
        FrameKind::Response
    );
    let control = protocol.next().await.unwrap().unwrap();
    assert_eq!(control.kind, FrameKind::Push);
    assert_eq!(control.route, elura_core::protocol::ROUTE_SESSION_CONTROL);
    assert_eq!(
        SessionControl::decode_frame_payload(control.payload)
            .unwrap()
            .action_kind()
            .unwrap(),
        SessionControlAction::Kick
    );

    drop(protocol);
    drop(second);
    shutdown_tx.send(true).unwrap();
    world_task.await.unwrap().unwrap();
    gateway_task.await.unwrap().unwrap();
}

#[tokio::test]
async fn unauthenticated_connection_is_closed_at_authentication_deadline() {
    let address = free_address();
    let tickets = Arc::new(
        TicketService::new(
            [31_u8; 32],
            "test-auth",
            "test-gateway",
            Duration::from_secs(60),
        )
        .unwrap(),
    );
    let gateway = Arc::new(
        Gateway::new(
            GatewayConfig {
                listen: address,
                authentication_timeout: Duration::from_millis(30),
                heartbeat_interval: Duration::from_secs(1),
                ..GatewayConfig::default()
            },
            tickets,
            Arc::new(MemoryReplayStore::default()),
            Arc::new(NeverWorld),
        )
        .unwrap(),
    );
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let task = tokio::spawn(gateway.serve_tcp(shutdown_rx));
    tokio::time::sleep(Duration::from_millis(20)).await;
    let mut client = Framed::new(
        TcpStream::connect(address).await.unwrap(),
        FrameCodec::default(),
    );
    let closed = tokio::time::timeout(Duration::from_millis(500), client.next())
        .await
        .unwrap();
    assert!(closed.is_none());
    shutdown_tx.send(true).unwrap();
    task.await.unwrap().unwrap();
}

#[tokio::test]
async fn duplicate_request_returns_cached_response_without_reexecution() {
    let address = free_address();
    let tickets = Arc::new(
        TicketService::new(
            [32_u8; 32],
            "test-auth",
            "test-gateway",
            Duration::from_secs(60),
        )
        .unwrap(),
    );
    let ticket = tickets
        .issue(Identity {
            account_id: 1,
            user_id: 2,
            region_id: 1,
            realm_id: 1,
            generation: 1,
        })
        .unwrap();
    let world = Arc::new(CountingWorld::default());
    let gateway = Arc::new(
        Gateway::new(
            GatewayConfig {
                listen: address,
                heartbeat_interval: Duration::from_secs(1),
                ..GatewayConfig::default()
            },
            tickets,
            Arc::new(MemoryReplayStore::default()),
            world.clone(),
        )
        .unwrap(),
    );
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let task = tokio::spawn(gateway.serve_tcp(shutdown_rx));
    tokio::time::sleep(Duration::from_millis(20)).await;
    let mut client = Framed::new(
        TcpStream::connect(address).await.unwrap(),
        FrameCodec::default(),
    );
    client
        .send(
            Frame::request(
                ROUTE_AUTHENTICATE,
                1,
                serde_json::to_vec(&serde_json::json!({"ticket": ticket})).unwrap(),
            )
            .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(
        client.next().await.unwrap().unwrap().kind,
        FrameKind::Response
    );
    client
        .send(Frame::request(100, 2, Bytes::from_static(b"first")).unwrap())
        .await
        .unwrap();
    assert_eq!(
        client.next().await.unwrap().unwrap().payload,
        Bytes::from_static(b"first")
    );
    client
        .send(Frame::request(100, 2, Bytes::from_static(b"first")).unwrap())
        .await
        .unwrap();
    assert_eq!(
        client.next().await.unwrap().unwrap().payload,
        Bytes::from_static(b"first")
    );
    assert_eq!(world.0.load(Ordering::Relaxed), 1);
    drop(client);
    shutdown_tx.send(true).unwrap();
    task.await.unwrap().unwrap();
}

#[tokio::test]
async fn route_rate_limit_disconnects_after_repeated_violations() {
    let address = free_address();
    let tickets = Arc::new(
        TicketService::new(
            [33_u8; 32],
            "test-auth",
            "test-gateway",
            Duration::from_secs(60),
        )
        .unwrap(),
    );
    let ticket = tickets
        .issue(Identity {
            account_id: 1,
            user_id: 2,
            region_id: 1,
            realm_id: 1,
            generation: 1,
        })
        .unwrap();
    let gateway = Arc::new(
        Gateway::new(
            GatewayConfig {
                listen: address,
                route_rate_limits: std::collections::HashMap::from([
                    (
                        ROUTE_AUTHENTICATE,
                        RouteRateLimit {
                            requests_per_second: 5,
                            burst: 5,
                        },
                    ),
                    (
                        100,
                        RouteRateLimit {
                            requests_per_second: 1,
                            burst: 1,
                        },
                    ),
                ]),
                max_rate_limit_violations: 2,
                heartbeat_interval: Duration::from_secs(1),
                ..GatewayConfig::default()
            },
            tickets,
            Arc::new(MemoryReplayStore::default()),
            Arc::new(CountingWorld::default()),
        )
        .unwrap(),
    );
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let task = tokio::spawn(gateway.serve_tcp(shutdown_rx));
    tokio::time::sleep(Duration::from_millis(20)).await;
    let mut client = Framed::new(
        TcpStream::connect(address).await.unwrap(),
        FrameCodec::default(),
    );
    client
        .send(
            Frame::request(
                ROUTE_AUTHENTICATE,
                1,
                serde_json::to_vec(&serde_json::json!({"ticket": ticket})).unwrap(),
            )
            .unwrap(),
        )
        .await
        .unwrap();
    client.next().await.unwrap().unwrap();
    client
        .send(Frame::request(100, 2, Bytes::new()).unwrap())
        .await
        .unwrap();
    assert_eq!(
        client.next().await.unwrap().unwrap().kind,
        FrameKind::Response
    );
    client
        .send(Frame::request(100, 3, Bytes::new()).unwrap())
        .await
        .unwrap();
    assert_eq!(client.next().await.unwrap().unwrap().kind, FrameKind::Error);
    client
        .send(Frame::request(100, 4, Bytes::new()).unwrap())
        .await
        .unwrap();
    assert!(
        tokio::time::timeout(Duration::from_millis(500), client.next())
            .await
            .unwrap()
            .is_none()
    );
    shutdown_tx.send(true).unwrap();
    task.await.unwrap().unwrap();
}

#[tokio::test]
async fn reserved_client_routes_disconnect_after_repeated_protocol_violations() {
    let address = free_address();
    let tickets = Arc::new(
        TicketService::new(
            [36_u8; 32],
            "test-auth",
            "test-gateway",
            Duration::from_secs(60),
        )
        .unwrap(),
    );
    let ticket = tickets
        .issue(Identity {
            account_id: 1,
            user_id: 2,
            region_id: 1,
            realm_id: 1,
            generation: 1,
        })
        .unwrap();
    let gateway = Arc::new(
        Gateway::new(
            GatewayConfig {
                listen: address,
                max_protocol_violations: 2,
                ..GatewayConfig::default()
            },
            tickets,
            Arc::new(MemoryReplayStore::default()),
            Arc::new(CountingWorld::default()),
        )
        .unwrap(),
    );
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let task = tokio::spawn(gateway.serve_tcp(shutdown_rx));
    tokio::time::sleep(Duration::from_millis(20)).await;
    let mut client = Framed::new(
        TcpStream::connect(address).await.unwrap(),
        FrameCodec::default(),
    );
    client
        .send(
            Frame::request(
                ROUTE_AUTHENTICATE,
                1,
                serde_json::to_vec(&serde_json::json!({"ticket": ticket})).unwrap(),
            )
            .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(
        client.next().await.unwrap().unwrap().kind,
        FrameKind::Response
    );

    for request_id in [2, 3] {
        client
            .send(Frame::request(ROUTE_SESSION_CONTROL, request_id, Bytes::new()).unwrap())
            .await
            .unwrap();
        assert_eq!(client.next().await.unwrap().unwrap().kind, FrameKind::Error);
    }
    assert!(
        tokio::time::timeout(Duration::from_millis(500), client.next())
            .await
            .unwrap()
            .is_none()
    );
    shutdown_tx.send(true).unwrap();
    task.await.unwrap().unwrap();
}

#[tokio::test]
async fn server_heartbeat_accepts_response_and_keeps_session_usable() {
    let address = free_address();
    let tickets = Arc::new(
        TicketService::new(
            [34_u8; 32],
            "test-auth",
            "test-gateway",
            Duration::from_secs(60),
        )
        .unwrap(),
    );
    let ticket = tickets
        .issue(Identity {
            account_id: 1,
            user_id: 2,
            region_id: 1,
            realm_id: 1,
            generation: 1,
        })
        .unwrap();
    let gateway = Arc::new(
        Gateway::new(
            GatewayConfig {
                listen: address,
                heartbeat_interval: Duration::from_millis(30),
                ..GatewayConfig::default()
            },
            tickets,
            Arc::new(MemoryReplayStore::default()),
            Arc::new(CountingWorld::default()),
        )
        .unwrap(),
    );
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let task = tokio::spawn(gateway.serve_tcp(shutdown_rx));
    tokio::time::sleep(Duration::from_millis(20)).await;
    let mut client = Framed::new(
        TcpStream::connect(address).await.unwrap(),
        FrameCodec::default(),
    );
    client
        .send(
            Frame::request(
                ROUTE_AUTHENTICATE,
                1,
                serde_json::to_vec(&serde_json::json!({"ticket": ticket})).unwrap(),
            )
            .unwrap(),
        )
        .await
        .unwrap();
    client.next().await.unwrap().unwrap();
    let heartbeat = tokio::time::timeout(Duration::from_millis(300), client.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(heartbeat.kind, FrameKind::Request);
    assert_eq!(heartbeat.route, ROUTE_HEARTBEAT);
    let mut forged_response = Frame::response(&heartbeat, Bytes::new());
    forged_response.request_id = forged_response.request_id.saturating_sub(1);
    client.send(forged_response).await.unwrap();
    client
        .send(Frame::response(&heartbeat, Bytes::new()))
        .await
        .unwrap();
    client
        .send(Frame::request(100, 2, Bytes::from_static(b"alive")).unwrap())
        .await
        .unwrap();
    let response = client.next().await.unwrap().unwrap();
    assert_eq!(response.kind, FrameKind::Response);
    assert_eq!(response.payload, Bytes::from_static(b"alive"));
    drop(client);
    shutdown_tx.send(true).unwrap();
    task.await.unwrap().unwrap();
}

#[tokio::test]
async fn gateway_publishes_login_and_force_logout_session_control_events() {
    let address = free_address();
    let tickets = Arc::new(
        TicketService::new(
            [35_u8; 32],
            "test-auth",
            "test-gateway",
            Duration::from_secs(60),
        )
        .unwrap(),
    );
    let identity = Identity {
        account_id: 1,
        user_id: 42,
        region_id: 1,
        realm_id: 2,
        generation: 3,
    };
    let ticket = tickets.issue(identity.clone()).unwrap();
    let control = Arc::new(RecordingSessionControl::default());
    let gateway = Arc::new(
        Gateway::new(
            GatewayConfig {
                listen: address,
                heartbeat_interval: Duration::from_secs(1),
                ..GatewayConfig::default()
            },
            tickets,
            Arc::new(MemoryReplayStore::default()),
            Arc::new(CountingWorld::default()),
        )
        .unwrap()
        .with_session_control_transport(control.clone()),
    );
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let task = tokio::spawn(gateway.clone().serve_tcp(shutdown_rx));
    tokio::time::sleep(Duration::from_millis(20)).await;
    let mut client = Framed::new(
        TcpStream::connect(address).await.unwrap(),
        FrameCodec::default(),
    );
    client
        .send(
            Frame::request(
                ROUTE_AUTHENTICATE,
                1,
                serde_json::to_vec(&serde_json::json!({"ticket": ticket})).unwrap(),
            )
            .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(
        client.next().await.unwrap().unwrap().kind,
        FrameKind::Response
    );
    assert_eq!(
        control
            .0
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())[0]
            .kind,
        SessionControlKind::Login
    );
    assert_eq!(
        gateway
            .force_logout(
                identity.region_id,
                identity.realm_id,
                identity.user_id,
                "operator request",
            )
            .await
            .unwrap(),
        1
    );
    let notification = client.next().await.unwrap().unwrap();
    assert_eq!(notification.kind, FrameKind::Push);
    assert_eq!(notification.route, ROUTE_SESSION_CONTROL);
    assert_eq!(
        SessionControl::decode_frame_payload(notification.payload)
            .unwrap()
            .action_kind()
            .unwrap(),
        SessionControlAction::ForceLogout
    );
    {
        let events = control
            .0
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        assert_eq!(events.len(), 2);
        assert_eq!(events[1].kind, SessionControlKind::ForceLogout);
    }
    drop(client);
    shutdown_tx.send(true).unwrap();
    task.await.unwrap().unwrap();
}
