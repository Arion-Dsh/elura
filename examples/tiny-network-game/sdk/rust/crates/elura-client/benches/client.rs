use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use bytes::Bytes;
use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use elura_client::{ClientConfig, ClientEvent, Elr2Codec, Elr2Frame, EluraClient, EluraRoutes};
use futures_util::{SinkExt, StreamExt, future::join_all};
use tokio::net::TcpListener;
use tokio::runtime::Runtime;
use tokio_util::codec::Framed;

fn client_benchmarks(criterion: &mut Criterion) {
    let runtime = Runtime::new().unwrap();
    let (client, _) = runtime.block_on(start_echo_client(false));
    let (mixed_client, mixed_pushes) = runtime.block_on(start_echo_client(true));
    let mut group = criterion.benchmark_group("client_loopback");
    group.sample_size(30);

    for concurrency in [1_usize, 32, 128, 1024] {
        group.throughput(Throughput::Elements(concurrency as u64));
        group.bench_with_input(
            BenchmarkId::new("round_trip_batch", concurrency),
            &concurrency,
            |benchmark, &concurrency| {
                benchmark.to_async(&runtime).iter(|| {
                    let client = client.clone();
                    async move {
                        let requests = (0..concurrency).map(|index| {
                            let client = client.clone();
                            async move {
                                let payload = Bytes::copy_from_slice(&index.to_le_bytes());
                                let response = client.request(100, payload.clone()).await.unwrap();
                                assert_eq!(response.payload, payload);
                            }
                        });
                        join_all(requests).await
                    }
                });
            },
        );
    }

    let concurrency = 128;
    let mixed_expected = Arc::new(AtomicUsize::new(0));
    group.throughput(Throughput::Elements(concurrency as u64));
    group.bench_function(
        BenchmarkId::new("round_trip_with_push", concurrency),
        |benchmark| {
            benchmark.to_async(&runtime).iter(|| {
                let client = mixed_client.clone();
                let pushes = mixed_pushes.clone();
                let expected = mixed_expected.clone();
                async move {
                    let expected_pushes = expected
                        .fetch_add(concurrency, Ordering::AcqRel)
                        .saturating_add(concurrency);
                    let requests = (0..concurrency).map(|index| {
                        let client = client.clone();
                        async move {
                            let payload = Bytes::copy_from_slice(&index.to_le_bytes());
                            let response = client.request(100, payload.clone()).await.unwrap();
                            assert_eq!(response.payload, payload);
                        }
                    });
                    join_all(requests).await;
                    while pushes.load(Ordering::Acquire) < expected_pushes {
                        tokio::task::yield_now().await;
                    }
                    std::hint::black_box(expected_pushes);
                }
            });
        },
    );

    group.finish();
}

async fn start_echo_client(send_pushes: bool) -> (EluraClient, Arc<AtomicUsize>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let mut connection = Framed::new(stream, Elr2Codec::default());
        let authentication = connection.next().await.unwrap().unwrap();
        assert_eq!(authentication.route, EluraRoutes::AUTHENTICATE);
        connection
            .send(authentication_response(&authentication))
            .await
            .unwrap();

        while let Some(Ok(request)) = connection.next().await {
            if send_pushes {
                connection
                    .send(Elr2Frame::push(101, Bytes::new()).unwrap())
                    .await
                    .unwrap();
            }
            let payload = request.payload.clone();
            connection
                .send(Elr2Frame::response(&request, payload).unwrap())
                .await
                .unwrap();
        }
    });

    let client = EluraClient::connect_with_config(
        address.to_string(),
        "benchmark-login-ticket",
        ClientConfig {
            connect_timeout: Duration::from_secs(1),
            request_timeout: Duration::from_secs(5),
            reconnect_renewal_margin: Duration::from_secs(60),
            max_in_flight_requests: 1024,
            command_capacity: 1024,
            ..ClientConfig::default()
        },
    )
    .await
    .unwrap();
    let received_pushes = Arc::new(AtomicUsize::new(0));
    let pushes = received_pushes.clone();
    let mut events = client.subscribe();
    tokio::spawn(async move {
        while let Ok(event) = events.recv().await {
            if matches!(event, ClientEvent::Push(_)) {
                pushes.fetch_add(1, Ordering::Release);
            }
        }
    });
    (client, received_pushes)
}

fn authentication_response(request: &Elr2Frame) -> Elr2Frame {
    Elr2Frame::response(
        request,
        serde_json::to_vec(&serde_json::json!({
            "session_id": "benchmark-session",
            "identity": {
                "account_id": 1,
                "user_id": 2,
                "region_id": 3,
                "realm_id": 4,
                "generation": 5
            },
            "reconnect": {
                "ticket": "benchmark-reconnect-ticket",
                "expires_in_seconds": 3600
            }
        }))
        .unwrap(),
    )
    .unwrap()
}

criterion_group!(benches, client_benchmarks);
criterion_main!(benches);
