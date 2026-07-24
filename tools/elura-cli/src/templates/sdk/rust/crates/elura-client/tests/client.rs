use std::time::Duration;

use bytes::Bytes;
use elura_client::{
    ClientConfig, ClientError, ClientEvent, ConnectionState, Elr2Codec, Elr2Frame, EluraClient,
    EluraRoutes, FrameKind,
};
use futures_util::{SinkExt, StreamExt};
use tokio::net::TcpListener;
use tokio_util::codec::Framed;

#[tokio::test]
async fn tcp_client_authenticates_handles_heartbeat_and_queues_pushes() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let mut connection = Framed::new(stream, Elr2Codec::default());

        let authentication = connection.next().await.unwrap().unwrap();
        assert_eq!(authentication.route, EluraRoutes::AUTHENTICATE);

        let heartbeat = Elr2Frame::request(EluraRoutes::HEARTBEAT, 91, Bytes::new()).unwrap();
        connection.send(heartbeat.clone()).await.unwrap();
        let heartbeat_response = connection.next().await.unwrap().unwrap();
        assert_eq!(heartbeat_response.kind, FrameKind::Response);
        assert_eq!(heartbeat_response.request_id, heartbeat.request_id);

        connection
            .send(
                Elr2Frame::response(
                    &authentication,
                    serde_json::to_vec(&serde_json::json!({
                        "session_id": "session-1",
                        "identity": {
                            "account_id": 1,
                            "user_id": 2,
                            "region_id": 3,
                            "realm_id": 4,
                            "generation": 5
                        },
                        "reconnect": {
                            "ticket": "reconnect-1",
                            "expires_in_seconds": 60
                        }
                    }))
                    .unwrap(),
                )
                .unwrap(),
            )
            .await
            .unwrap();

        let application = connection.next().await.unwrap().unwrap();
        assert_eq!(application.route, 100);
        connection
            .send(Elr2Frame::push(100, "concurrent-push").unwrap())
            .await
            .unwrap();
        connection
            .send(Elr2Frame::response(&application, "response").unwrap())
            .await
            .unwrap();
    });

    let config = ClientConfig {
        connect_timeout: Duration::from_secs(1),
        request_timeout: Duration::from_secs(1),
        ..ClientConfig::default()
    };
    let client = EluraClient::connect_with_config(address.to_string(), "login", config)
        .await
        .unwrap();
    let mut events = client.subscribe();
    assert_eq!(client.authentication().unwrap().session_id, "session-1");
    assert_eq!(
        client.authentication().unwrap().reconnect.ticket,
        "reconnect-1"
    );
    assert!(matches!(
        client
            .request(EluraRoutes::SESSION_CONTROL, Bytes::new())
            .await,
        Err(ClientError::Configuration(_))
    ));

    let response = client.request(100, "request").await.unwrap();
    assert_eq!(response.payload, Bytes::from_static(b"response"));
    assert_eq!(
        events.recv().await.unwrap(),
        ClientEvent::Push(Elr2Frame::push(100, "concurrent-push").unwrap())
    );

    server.await.unwrap();
}

#[tokio::test]
async fn tcp_client_rotates_and_uses_the_latest_reconnect_ticket() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let mut first = Framed::new(stream, Elr2Codec::default());
        let authentication = first.next().await.unwrap().unwrap();
        assert_eq!(
            serde_json::from_slice::<serde_json::Value>(&authentication.payload).unwrap()["ticket"],
            "login"
        );
        first
            .send(authentication_response(
                &authentication,
                "session-1",
                "reconnect-1",
            ))
            .await
            .unwrap();

        let renewal = first.next().await.unwrap().unwrap();
        assert_eq!(renewal.route, EluraRoutes::RENEW_RECONNECT_TICKET);
        assert_eq!(
            serde_json::from_slice::<serde_json::Value>(&renewal.payload).unwrap()["ticket"],
            "reconnect-1"
        );
        first
            .send(
                Elr2Frame::response(
                    &renewal,
                    serde_json::to_vec(&serde_json::json!({
                        "ticket": "reconnect-2",
                        "expires_in_seconds": 120
                    }))
                    .unwrap(),
                )
                .unwrap(),
            )
            .await
            .unwrap();

        let (stream, _) = listener.accept().await.unwrap();
        let mut second = Framed::new(stream, Elr2Codec::default());
        let authentication = second.next().await.unwrap().unwrap();
        assert_eq!(
            serde_json::from_slice::<serde_json::Value>(&authentication.payload).unwrap()["ticket"],
            "reconnect-2"
        );
        second
            .send(authentication_response(
                &authentication,
                "session-2",
                "reconnect-3",
            ))
            .await
            .unwrap();
    });

    let client = EluraClient::connect(address.to_string(), "login")
        .await
        .unwrap();
    let renewed = client.renew_reconnect_ticket().await.unwrap();
    assert_eq!(renewed.ticket, "reconnect-2");
    assert_eq!(renewed.expires_in_seconds, 120);

    let reconnected = client.reconnect().await.unwrap();
    assert_eq!(reconnected.session_id, "session-2");
    assert_eq!(reconnected.reconnect.ticket, "reconnect-3");
    server.await.unwrap();
}

#[tokio::test]
async fn idle_client_renews_the_reconnect_ticket_while_waiting_for_events() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let mut connection = Framed::new(stream, Elr2Codec::default());
        let authentication = connection.next().await.unwrap().unwrap();
        connection
            .send(authentication_response_with_ttl(
                &authentication,
                "session-1",
                "reconnect-short",
                1,
            ))
            .await
            .unwrap();

        let renewal = tokio::time::timeout(Duration::from_secs(2), connection.next())
            .await
            .expect("client did not renew before ticket expiry")
            .unwrap()
            .unwrap();
        assert_eq!(renewal.route, EluraRoutes::RENEW_RECONNECT_TICKET);
        assert_eq!(
            serde_json::from_slice::<serde_json::Value>(&renewal.payload).unwrap()["ticket"],
            "reconnect-short"
        );
        connection
            .send(
                Elr2Frame::response(
                    &renewal,
                    serde_json::to_vec(&serde_json::json!({
                        "ticket": "reconnect-automatic",
                        "expires_in_seconds": 60
                    }))
                    .unwrap(),
                )
                .unwrap(),
            )
            .await
            .unwrap();
        connection
            .send(Elr2Frame::push(100, "after-renewal").unwrap())
            .await
            .unwrap();
    });

    let config = ClientConfig {
        connect_timeout: Duration::from_secs(1),
        request_timeout: Duration::from_secs(1),
        reconnect_renewal_margin: Duration::from_secs(1),
        ..ClientConfig::default()
    };
    let client = EluraClient::connect_with_config(address.to_string(), "login", config)
        .await
        .unwrap();
    let mut events = client.subscribe();
    assert_eq!(
        events.recv().await.unwrap(),
        ClientEvent::Push(Elr2Frame::push(100, "after-renewal").unwrap())
    );
    assert_eq!(
        client.authentication().unwrap().reconnect.ticket,
        "reconnect-automatic"
    );
    server.await.unwrap();
}

#[tokio::test]
async fn connection_loss_reconnects_without_replaying_the_interrupted_request() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let mut first = Framed::new(stream, Elr2Codec::default());
        let authentication = first.next().await.unwrap().unwrap();
        first
            .send(authentication_response(
                &authentication,
                "session-1",
                "reconnect-1",
            ))
            .await
            .unwrap();
        let interrupted = first.next().await.unwrap().unwrap();
        assert_eq!(interrupted.payload, Bytes::from_static(b"do-not-replay"));
        drop(first);

        let (stream, _) = listener.accept().await.unwrap();
        let mut second = Framed::new(stream, Elr2Codec::default());
        let authentication = second.next().await.unwrap().unwrap();
        assert_eq!(
            serde_json::from_slice::<serde_json::Value>(&authentication.payload).unwrap()["ticket"],
            "reconnect-1"
        );
        second
            .send(authentication_response(
                &authentication,
                "session-2",
                "reconnect-2",
            ))
            .await
            .unwrap();
        let next_request = second.next().await.unwrap().unwrap();
        assert_eq!(next_request.payload, Bytes::from_static(b"new-request"));
        second
            .send(Elr2Frame::response(&next_request, "ok").unwrap())
            .await
            .unwrap();
    });

    let config = ClientConfig {
        connect_timeout: Duration::from_secs(1),
        request_timeout: Duration::from_secs(1),
        reconnect_initial_delay: Duration::from_millis(10),
        reconnect_max_delay: Duration::from_millis(20),
        reconnect_max_attempts: Some(3),
        ..ClientConfig::default()
    };
    let client = EluraClient::connect_with_config(address.to_string(), "login", config)
        .await
        .unwrap();
    let mut events = client.subscribe();
    assert!(matches!(
        client.request(100, "do-not-replay").await,
        Err(ClientError::RequestInterrupted)
    ));
    assert_eq!(events.recv().await.unwrap(), ClientEvent::Disconnected);
    assert!(matches!(
        events.recv().await.unwrap(),
        ClientEvent::Reconnecting { attempt: 1, .. }
    ));
    assert_eq!(events.recv().await.unwrap(), ClientEvent::Reconnected);
    assert_eq!(client.state(), ConnectionState::Connected);

    let response = client.request(100, "new-request").await.unwrap();
    assert_eq!(response.payload, Bytes::from_static(b"ok"));
    server.await.unwrap();
}

#[tokio::test]
async fn reconnect_state_machine_survives_repeated_transport_failures() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let mut first = Framed::new(stream, Elr2Codec::default());
        let authentication = first.next().await.unwrap().unwrap();
        first
            .send(authentication_response(
                &authentication,
                "session-1",
                "reconnect-retry",
            ))
            .await
            .unwrap();
        let trigger = first.next().await.unwrap().unwrap();
        assert_eq!(trigger.payload, Bytes::from_static(b"trigger-retries"));
        drop(first);

        for _ in 0..2 {
            let (stream, _) = listener.accept().await.unwrap();
            let mut failed = Framed::new(stream, Elr2Codec::default());
            let authentication = failed.next().await.unwrap().unwrap();
            assert_eq!(
                serde_json::from_slice::<serde_json::Value>(&authentication.payload).unwrap()["ticket"],
                "reconnect-retry"
            );
        }

        let (stream, _) = listener.accept().await.unwrap();
        let mut recovered = Framed::new(stream, Elr2Codec::default());
        let authentication = recovered.next().await.unwrap().unwrap();
        recovered
            .send(authentication_response(
                &authentication,
                "session-recovered",
                "reconnect-recovered",
            ))
            .await
            .unwrap();
        let request = recovered.next().await.unwrap().unwrap();
        recovered
            .send(Elr2Frame::response(&request, "stable").unwrap())
            .await
            .unwrap();
    });

    let client = EluraClient::connect_with_config(
        address.to_string(),
        "login",
        ClientConfig {
            connect_timeout: Duration::from_secs(1),
            request_timeout: Duration::from_secs(1),
            reconnect_initial_delay: Duration::from_millis(5),
            reconnect_max_delay: Duration::from_millis(20),
            reconnect_max_attempts: Some(4),
            ..ClientConfig::default()
        },
    )
    .await
    .unwrap();
    let mut events = client.subscribe();
    assert!(matches!(
        client.request(100, "trigger-retries").await,
        Err(ClientError::RequestInterrupted)
    ));
    assert_eq!(events.recv().await.unwrap(), ClientEvent::Disconnected);
    for expected_attempt in 1..=3 {
        assert!(matches!(
            events.recv().await.unwrap(),
            ClientEvent::Reconnecting { attempt, .. } if attempt == expected_attempt
        ));
    }
    assert_eq!(events.recv().await.unwrap(), ClientEvent::Reconnected);
    assert_eq!(
        client.request(100, "after-retries").await.unwrap().payload,
        Bytes::from_static(b"stable")
    );
    server.await.unwrap();
}

#[tokio::test]
async fn rejected_reconnect_requires_a_fresh_login_ticket() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let mut first = Framed::new(stream, Elr2Codec::default());
        let authentication = first.next().await.unwrap().unwrap();
        first
            .send(authentication_response(
                &authentication,
                "session-1",
                "expired-ticket",
            ))
            .await
            .unwrap();
        let trigger = first.next().await.unwrap().unwrap();
        assert_eq!(trigger.payload, Bytes::from_static(b"trigger-disconnect"));
        drop(first);

        let (stream, _) = listener.accept().await.unwrap();
        let mut rejected = Framed::new(stream, Elr2Codec::default());
        let authentication = rejected.next().await.unwrap().unwrap();
        rejected
            .send(
                Elr2Frame::error(
                    &authentication,
                    serde_json::to_vec(&serde_json::json!({
                        "code": "UNAUTHENTICATED",
                        "message": "authentication failed",
                        "retryable": false
                    }))
                    .unwrap(),
                )
                .unwrap(),
            )
            .await
            .unwrap();

        let (stream, _) = listener.accept().await.unwrap();
        let mut recovered = Framed::new(stream, Elr2Codec::default());
        let authentication = recovered.next().await.unwrap().unwrap();
        assert_eq!(
            serde_json::from_slice::<serde_json::Value>(&authentication.payload).unwrap()["ticket"],
            "fresh-login"
        );
        recovered
            .send(authentication_response(
                &authentication,
                "session-2",
                "reconnect-fresh",
            ))
            .await
            .unwrap();
        let request = recovered.next().await.unwrap().unwrap();
        recovered
            .send(Elr2Frame::response(&request, "recovered").unwrap())
            .await
            .unwrap();
    });

    let config = ClientConfig {
        connect_timeout: Duration::from_secs(1),
        request_timeout: Duration::from_secs(1),
        reconnect_initial_delay: Duration::from_millis(10),
        reconnect_max_delay: Duration::from_millis(20),
        reconnect_max_attempts: Some(3),
        ..ClientConfig::default()
    };
    let client = EluraClient::connect_with_config(address.to_string(), "login", config)
        .await
        .unwrap();
    let mut events = client.subscribe();
    assert!(matches!(
        client.request(100, "trigger-disconnect").await,
        Err(ClientError::RequestInterrupted)
    ));
    assert_eq!(events.recv().await.unwrap(), ClientEvent::Disconnected);
    assert!(matches!(
        events.recv().await.unwrap(),
        ClientEvent::Reconnecting { attempt: 1, .. }
    ));
    assert_eq!(
        events.recv().await.unwrap(),
        ClientEvent::ReauthenticationRequired
    );
    assert_eq!(client.state(), ConnectionState::ReauthenticationRequired);
    assert!(client.authentication().is_none());
    assert!(matches!(
        client.request(100, Bytes::new()).await,
        Err(ClientError::ReauthenticationRequired)
    ));

    let authentication = client.reauthenticate("fresh-login").await.unwrap();
    assert_eq!(authentication.session_id, "session-2");
    assert_eq!(client.state(), ConnectionState::Connected);
    assert_eq!(client.authentication().unwrap().session_id, "session-2");
    assert_eq!(
        client.request(100, "after-login").await.unwrap().payload,
        Bytes::from_static(b"recovered")
    );
    server.await.unwrap();
}

#[tokio::test]
async fn cloned_handles_correlate_concurrent_out_of_order_responses() {
    const REQUESTS: usize = 1024;

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let mut connection = Framed::new(stream, Elr2Codec::default());
        let authentication = connection.next().await.unwrap().unwrap();
        connection
            .send(authentication_response(
                &authentication,
                "session-concurrent",
                "reconnect-concurrent",
            ))
            .await
            .unwrap();

        let mut requests = Vec::with_capacity(REQUESTS);
        for _ in 0..REQUESTS {
            requests.push(connection.next().await.unwrap().unwrap());
        }
        connection
            .send(Elr2Frame::push(101, "while-busy").unwrap())
            .await
            .unwrap();
        for request in requests.into_iter().rev() {
            let payload = request.payload.clone();
            connection
                .send(Elr2Frame::response(&request, payload).unwrap())
                .await
                .unwrap();
        }
    });

    let client = EluraClient::connect(address.to_string(), "login")
        .await
        .unwrap();
    let mut events = client.subscribe();
    let mut tasks = Vec::with_capacity(REQUESTS);
    for index in 0..REQUESTS {
        let client = client.clone();
        tasks.push(tokio::spawn(async move {
            let payload = index.to_le_bytes().to_vec();
            let response = client.request(100, payload.clone()).await.unwrap();
            assert_eq!(response.payload.as_ref(), payload);
        }));
    }
    for task in tasks {
        task.await.unwrap();
    }
    assert_eq!(
        events.recv().await.unwrap(),
        ClientEvent::Push(Elr2Frame::push(101, "while-busy").unwrap())
    );
    server.await.unwrap();
}

fn authentication_response(
    request: &Elr2Frame,
    session_id: &str,
    reconnect_ticket: &str,
) -> Elr2Frame {
    authentication_response_with_ttl(request, session_id, reconnect_ticket, 60)
}

fn authentication_response_with_ttl(
    request: &Elr2Frame,
    session_id: &str,
    reconnect_ticket: &str,
    expires_in_seconds: u64,
) -> Elr2Frame {
    Elr2Frame::response(
        request,
        serde_json::to_vec(&serde_json::json!({
            "session_id": session_id,
            "identity": {
                "account_id": 1,
                "user_id": 2,
                "region_id": 3,
                "realm_id": 4,
                "generation": 5
            },
            "reconnect": {
                "ticket": reconnect_ticket,
                "expires_in_seconds": expires_in_seconds
            }
        }))
        .unwrap(),
    )
    .unwrap()
}
