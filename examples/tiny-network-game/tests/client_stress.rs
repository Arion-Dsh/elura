use std::collections::BTreeMap;
use std::io;
use std::net::{Ipv4Addr, SocketAddr, TcpListener};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use elura::prelude::*;
use elura_client::{ClientConfig, ClientError, ConnectionState, EluraClient};
use elura_spot_demo::{Arena, DEMO_TICKET_KEY, Move, MoveRequest, ROUTE_MOVE, Snapshot};
use prost::bytes::Bytes;
use tokio::task::JoinSet;
use tokio::time::timeout;

const ROUTE_ECHO: u32 = 1000;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "configurable sustained performance test"]
async fn sustained_sdk_load_against_real_gateway() {
    let duration = Duration::from_secs(environment("ELURA_CLIENT_STRESS_SECONDS", 30));
    let concurrency = usize::try_from(environment("ELURA_CLIENT_STRESS_CONCURRENCY", 1024))
        .expect("stress concurrency does not fit usize");
    assert!(concurrency > 0, "stress concurrency must be positive");

    let gateway_port = unused_loopback_port();
    let admin_port = unused_loopback_port();
    let gateway_address = SocketAddr::from((Ipv4Addr::LOCALHOST, gateway_port));
    let mut gateway = GatewayConfig::default();
    gateway.ticket.key = DEMO_TICKET_KEY.to_owned();
    gateway.request_rate = 1_000_000;
    gateway.request_burst = 1_000_000;
    gateway.inbound_byte_rate = u32::MAX;
    gateway.inbound_byte_burst = u32::MAX;
    gateway.inbound_queue = concurrency;
    gateway.response_queue = concurrency;
    gateway.ip_request_rate = 0;
    gateway.ip_request_burst = 0;
    let mut tcp = TcpConfig::default();
    tcp.listen = gateway_address;
    let arena = Arc::new(Mutex::new(Arena::default()));
    let server = Monolith::new(gateway, WorldConfig::default())
        .transport(TcpTransport::new(tcp).unwrap())
        .route(Move, move |context: WorldContext, request| {
            let arena = arena.clone();
            async move {
                Ok(arena
                    .lock()
                    .map_err(|_| elura::Error::Internal("arena lock poisoned".into()))?
                    .apply_move(context.identity.user_id, request))
            }
        })
        .build()
        .unwrap();
    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
    let server_task = tokio::spawn(server.serve(
        AdminServerConfig::loopback(admin_port, "tiny-arena-client-stress", "stress"),
        shutdown_rx,
    ));

    let client = connect_when_ready(gateway_address, concurrency)
        .await
        .unwrap();
    let started = Instant::now();
    let deadline = started + duration;
    let mut completed = 0_u64;
    let mut errors = 0_u64;
    let mut error_kinds = BTreeMap::<String, u64>::new();
    let mut latency = LatencyHistogram::new(Duration::from_secs(30), Duration::from_micros(100));

    while Instant::now() < deadline {
        let mut requests = JoinSet::new();
        for index in 0..concurrency {
            let client = client.clone();
            requests.spawn(async move {
                let started = Instant::now();
                let result = client
                    .request_protobuf::<_, Snapshot>(
                        ROUTE_MOVE,
                        &MoveRequest {
                            dx: (index & 1) as i32,
                            dy: 0,
                        },
                    )
                    .await;
                (started.elapsed(), result)
            });
        }
        while let Some(result) = requests.join_next().await {
            match result {
                Ok((elapsed, Ok(_))) => {
                    completed += 1;
                    latency.record(elapsed);
                }
                Ok((_, Err(error))) => {
                    errors += 1;
                    *error_kinds.entry(error.to_string()).or_default() += 1;
                }
                Err(error) => {
                    errors += 1;
                    *error_kinds
                        .entry(format!("request task failed: {error}"))
                        .or_default() += 1;
                }
            }
        }
    }

    let elapsed = started.elapsed();
    println!("stress.duration_seconds={:.3}", elapsed.as_secs_f64());
    println!("stress.concurrency={concurrency}");
    println!("stress.requests.completed={completed}");
    println!("stress.requests.errors={errors}");
    for (error, count) in error_kinds {
        println!("stress.errors.{error}={count}");
    }
    println!(
        "stress.throughput_requests_per_second={:.2}",
        completed as f64 / elapsed.as_secs_f64()
    );
    println!("stress.latency.p50_us_le={}", latency.percentile(0.50));
    println!("stress.latency.p95_us_le={}", latency.percentile(0.95));
    println!("stress.latency.p99_us_le={}", latency.percentile(0.99));
    println!("stress.latency.max_us={}", latency.maximum);

    assert!(completed > 0, "stress run completed no requests");
    assert_eq!(errors, 0, "stress run encountered request errors");

    drop(client);
    shutdown_tx.send(true).unwrap();
    timeout(Duration::from_secs(5), server_task)
        .await
        .expect("server shutdown timed out")
        .expect("server task panicked")
        .expect("server returned an error");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "configurable many-connection performance test"]
async fn many_sdk_clients_against_real_gateway() {
    let connections = usize::try_from(environment("ELURA_CLIENT_CONNECTIONS", 1000))
        .expect("connection count does not fit usize");
    let requests_per_connection = environment("ELURA_CLIENT_REQUESTS_PER_CONNECTION", 100);
    let batch_size = usize::try_from(environment("ELURA_CLIENT_CONNECT_BATCH", 100))
        .expect("connection batch size does not fit usize");
    let ramp = Duration::from_millis(environment("ELURA_CLIENT_CONNECT_RAMP_MS", 10));
    let idle = Duration::from_secs(environment("ELURA_CLIENT_IDLE_SECONDS", 0));
    let request_interval =
        Duration::from_millis(environment("ELURA_CLIENT_REQUEST_INTERVAL_MS", 0));
    let spread_request_phases = environment("ELURA_CLIENT_SPREAD_REQUEST_PHASES", 1) != 0;
    let channel_capacity = usize::try_from(environment("ELURA_CLIENT_CHANNEL_CAPACITY", 8))
        .expect("channel capacity does not fit usize");
    assert!(connections > 0, "connection count must be positive");
    assert!(
        requests_per_connection > 0,
        "request count must be positive"
    );
    assert!(batch_size > 0, "connection batch size must be positive");
    assert!(channel_capacity > 0, "channel capacity must be positive");

    let gateway_port = unused_loopback_port();
    let admin_port = unused_loopback_port();
    let gateway_address = SocketAddr::from((Ipv4Addr::LOCALHOST, gateway_port));
    let server = build_echo_server(gateway_address, connections.saturating_add(batch_size));
    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
    let server_task = tokio::spawn(server.serve(
        AdminServerConfig::loopback(admin_port, "many-rust-sdk-clients", "stress"),
        shutdown_rx,
    ));
    let tickets = Arc::new(
        TicketService::new(
            DEMO_TICKET_KEY,
            "game-login",
            "game-gateway",
            Duration::from_secs(60),
            Duration::from_secs(30 * 60),
        )
        .unwrap(),
    );

    let connect_started = Instant::now();
    let mut clients = Vec::with_capacity(connections);
    let mut connect_latency =
        LatencyHistogram::new(Duration::from_secs(30), Duration::from_micros(100));
    let mut connect_errors = BTreeMap::<String, u64>::new();
    for batch_start in (0..connections).step_by(batch_size) {
        let batch_end = batch_start.saturating_add(batch_size).min(connections);
        let mut connecting = JoinSet::new();
        for index in batch_start..batch_end {
            let user_id = i64::try_from(index)
                .ok()
                .and_then(|index| index.checked_add(1))
                .expect("connection index does not fit a user ID");
            let ticket = tickets
                .issue_login(Identity {
                    account_id: user_id,
                    user_id,
                    region_id: 1,
                    realm_id: 1,
                    generation: 1,
                })
                .unwrap();
            connecting.spawn(connect_ticket_when_ready(
                gateway_address,
                ticket,
                ClientConfig {
                    max_in_flight_requests: 4,
                    command_capacity: channel_capacity,
                    event_capacity: channel_capacity,
                    request_timeout: Duration::from_secs(10),
                    ..ClientConfig::default()
                },
            ));
        }
        while let Some(result) = connecting.join_next().await {
            match result {
                Ok(Ok((client, elapsed))) => {
                    clients.push(client);
                    connect_latency.record(elapsed);
                }
                Ok(Err(error)) => {
                    *connect_errors.entry(error.to_string()).or_default() += 1;
                }
                Err(error) => {
                    *connect_errors
                        .entry(format!("connect task failed: {error}"))
                        .or_default() += 1;
                }
            }
        }
        if batch_end < connections && !ramp.is_zero() {
            tokio::time::sleep(ramp).await;
        }
    }
    let connect_elapsed = connect_started.elapsed();
    println!("connections.requested={connections}");
    println!("connections.authenticated={}", clients.len());
    println!(
        "connections.failed={}",
        connections.saturating_sub(clients.len())
    );
    for (error, count) in &connect_errors {
        println!("connections.errors.{error}={count}");
    }
    println!(
        "connections.elapsed_seconds={:.3}",
        connect_elapsed.as_secs_f64()
    );
    println!(
        "connections.per_second={:.2}",
        clients.len() as f64 / connect_elapsed.as_secs_f64()
    );
    println!(
        "connections.latency.p50_us_le={}",
        connect_latency.percentile(0.50)
    );
    println!(
        "connections.latency.p95_us_le={}",
        connect_latency.percentile(0.95)
    );
    println!(
        "connections.latency.p99_us_le={}",
        connect_latency.percentile(0.99)
    );
    assert_eq!(clients.len(), connections, "not every Client authenticated");

    println!("connections.idle_seconds={}", idle.as_secs());
    println!("connections.client_channel_capacity={channel_capacity}");
    if !idle.is_zero() {
        tokio::time::sleep(idle).await;
    }

    let barrier = Arc::new(tokio::sync::Barrier::new(clients.len() + 1));
    let mut workers = JoinSet::new();
    let latency_capacity =
        usize::try_from(requests_per_connection).expect("request count does not fit usize");
    for (client_index, client) in clients.into_iter().enumerate() {
        let barrier = barrier.clone();
        workers.spawn(async move {
            let mut result = WorkerMetrics::new(latency_capacity);
            barrier.wait().await;
            if spread_request_phases && !request_interval.is_zero() {
                let phase = request_interval.mul_f64(client_index as f64 / connections as f64);
                tokio::time::sleep(phase).await;
            }
            let mut cadence = if request_interval.is_zero() {
                None
            } else {
                let mut interval = tokio::time::interval(request_interval);
                interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
                Some(interval)
            };
            for request_index in 0..requests_per_connection {
                if let Some(cadence) = cadence.as_mut() {
                    cadence.tick().await;
                }
                let payload = Bytes::copy_from_slice(&request_index.to_le_bytes());
                let started = Instant::now();
                match client.request(ROUTE_ECHO, payload.clone()).await {
                    Ok(response) if response.payload == payload => {
                        result.completed += 1;
                        result.record_latency(started.elapsed());
                    }
                    Ok(_) => result.record_error("response payload mismatch".into()),
                    Err(error) => result.record_error(error.to_string()),
                }
            }
            result
        });
    }
    barrier.wait().await;
    let request_started = Instant::now();
    let mut summary = RequestSummary::new();
    while let Some(result) = workers.join_next().await {
        match result {
            Ok(result) => summary.merge(result),
            Err(error) => summary.record_error(format!("request task failed: {error}")),
        }
    }
    let request_elapsed = request_started.elapsed();
    println!("requests.completed={}", summary.completed);
    println!("requests.errors={}", summary.errors);
    println!(
        "requests.interval_milliseconds={}",
        request_interval.as_millis()
    );
    println!("requests.spread_phases={spread_request_phases}");
    for (error, count) in &summary.error_kinds {
        println!("requests.errors.{error}={count}");
    }
    println!(
        "requests.elapsed_seconds={:.3}",
        request_elapsed.as_secs_f64()
    );
    println!(
        "requests.throughput_per_second={:.2}",
        summary.completed as f64 / request_elapsed.as_secs_f64()
    );
    println!(
        "requests.latency.p50_us_le={}",
        summary.latency.percentile(0.50)
    );
    println!(
        "requests.latency.p95_us_le={}",
        summary.latency.percentile(0.95)
    );
    println!(
        "requests.latency.p99_us_le={}",
        summary.latency.percentile(0.99)
    );
    println!("requests.latency.max_us={}", summary.latency.maximum);
    assert_eq!(summary.errors, 0, "many-client run encountered errors");

    shutdown_tx.send(true).unwrap();
    timeout(Duration::from_secs(5), server_task)
        .await
        .expect("server shutdown timed out")
        .expect("server task panicked")
        .expect("server returned an error");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "configurable reconnect-storm performance test"]
async fn sdk_clients_reconnect_after_gateway_restart() {
    let connections = usize::try_from(environment("ELURA_CLIENT_RECONNECT_CONNECTIONS", 1000))
        .expect("reconnect connection count does not fit usize");
    let batch_size = usize::try_from(environment("ELURA_CLIENT_CONNECT_BATCH", 100))
        .expect("connection batch size does not fit usize");
    let restart_delay =
        Duration::from_millis(environment("ELURA_CLIENT_GATEWAY_RESTART_DELAY_MS", 500));
    assert!(connections > 0, "connection count must be positive");
    assert!(batch_size > 0, "connection batch size must be positive");

    let gateway_port = unused_loopback_port();
    let first_admin_port = unused_loopback_port();
    let gateway_address = SocketAddr::from((Ipv4Addr::LOCALHOST, gateway_port));
    let first_server = build_echo_server(gateway_address, connections.saturating_add(batch_size));
    let (first_shutdown_tx, first_shutdown_rx) = tokio::sync::watch::channel(false);
    let first_server_task = tokio::spawn(first_server.serve(
        AdminServerConfig::loopback(first_admin_port, "reconnect-storm-before", "stress"),
        first_shutdown_rx,
    ));
    let tickets = Arc::new(
        TicketService::new(
            DEMO_TICKET_KEY,
            "game-login",
            "game-gateway",
            Duration::from_secs(60),
            Duration::from_secs(30 * 60),
        )
        .unwrap(),
    );

    let mut clients = Vec::with_capacity(connections);
    for batch_start in (0..connections).step_by(batch_size) {
        let batch_end = batch_start.saturating_add(batch_size).min(connections);
        let mut connecting = JoinSet::new();
        for index in batch_start..batch_end {
            let user_id = i64::try_from(index)
                .ok()
                .and_then(|index| index.checked_add(1))
                .expect("connection index does not fit a user ID");
            let ticket = tickets
                .issue_login(Identity {
                    account_id: user_id,
                    user_id,
                    region_id: 1,
                    realm_id: 1,
                    generation: 1,
                })
                .unwrap();
            connecting.spawn(connect_ticket_when_ready(
                gateway_address,
                ticket,
                ClientConfig {
                    reconnect_initial_delay: Duration::from_millis(50),
                    reconnect_max_delay: Duration::from_millis(500),
                    reconnect_max_attempts: Some(20),
                    command_capacity: 8,
                    event_capacity: 8,
                    ..ClientConfig::default()
                },
            ));
        }
        while let Some(result) = connecting.join_next().await {
            clients.push(result.unwrap().unwrap().0);
        }
    }
    assert_eq!(clients.len(), connections);
    let states = clients
        .iter()
        .map(EluraClient::subscribe_state)
        .collect::<Vec<_>>();

    first_shutdown_tx.send(true).unwrap();
    let first_shutdown = timeout(Duration::from_secs(5), first_server_task)
        .await
        .expect("first Gateway shutdown timed out")
        .expect("first Gateway task panicked");
    assert!(
        matches!(first_shutdown, Ok(()) | Err(elura::Error::Timeout)),
        "unexpected first Gateway shutdown result: {first_shutdown:?}"
    );
    let disconnect_deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let disconnected = states
            .iter()
            .filter(|state| *state.borrow() != ConnectionState::Connected)
            .count();
        if disconnected == connections {
            break;
        }
        assert!(
            Instant::now() < disconnect_deadline,
            "not every Client observed the Gateway shutdown"
        );
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    tokio::time::sleep(restart_delay).await;

    let second_admin_port = unused_loopback_port();
    let second_server = build_echo_server(gateway_address, connections.saturating_add(batch_size));
    let (second_shutdown_tx, second_shutdown_rx) = tokio::sync::watch::channel(false);
    let second_server_task = tokio::spawn(second_server.serve(
        AdminServerConfig::loopback(second_admin_port, "reconnect-storm-after", "stress"),
        second_shutdown_rx,
    ));
    let recovery_started = Instant::now();
    let recovery_deadline = recovery_started + Duration::from_secs(15);
    let mut recovered_at = vec![None; connections];
    loop {
        for (index, state) in states.iter().enumerate() {
            if recovered_at[index].is_none() && *state.borrow() == ConnectionState::Connected {
                recovered_at[index] = Some(recovery_started.elapsed());
            }
        }
        if recovered_at.iter().all(Option::is_some) {
            break;
        }
        assert!(
            Instant::now() < recovery_deadline,
            "not every Client reconnected after the Gateway restart"
        );
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    let mut recovery_latency =
        LatencyHistogram::new(Duration::from_secs(15), Duration::from_millis(1));
    for elapsed in recovered_at.into_iter().flatten() {
        recovery_latency.record(elapsed);
    }
    println!("reconnect.connections={connections}");
    println!(
        "reconnect.restart_delay_milliseconds={}",
        restart_delay.as_millis()
    );
    println!(
        "reconnect.latency.p50_ms_le={}",
        recovery_latency.percentile(0.50) / 1000
    );
    println!(
        "reconnect.latency.p95_ms_le={}",
        recovery_latency.percentile(0.95) / 1000
    );
    println!(
        "reconnect.latency.p99_ms_le={}",
        recovery_latency.percentile(0.99) / 1000
    );
    println!(
        "reconnect.latency.max_ms={}",
        recovery_latency.maximum / 1000
    );

    let mut verification = JoinSet::new();
    for client in clients {
        verification.spawn(async move {
            client
                .request(ROUTE_ECHO, Bytes::from_static(b"reconnected"))
                .await
        });
    }
    let mut verified = 0;
    while let Some(result) = verification.join_next().await {
        let response = result.unwrap().unwrap();
        assert_eq!(response.payload, Bytes::from_static(b"reconnected"));
        verified += 1;
    }
    assert_eq!(verified, connections);

    second_shutdown_tx.send(true).unwrap();
    timeout(Duration::from_secs(5), second_server_task)
        .await
        .expect("second Gateway shutdown timed out")
        .expect("second Gateway task panicked")
        .expect("second Gateway returned an error");
}

fn build_echo_server(address: SocketAddr, connection_capacity: usize) -> MonolithServer {
    let mut gateway = GatewayConfig::default();
    gateway.ticket.key = DEMO_TICKET_KEY.to_owned();
    gateway.max_connections = connection_capacity;
    gateway.max_connections_per_ip = connection_capacity;
    gateway.request_rate = 100_000;
    gateway.request_burst = 100_000;
    gateway.inbound_byte_rate = u32::MAX;
    gateway.inbound_byte_burst = u32::MAX;
    gateway.ip_request_rate = 0;
    gateway.ip_request_burst = 0;
    gateway.shutdown_timeout = Duration::from_millis(500);
    let mut tcp = TcpConfig::default();
    tcp.listen = address;
    Monolith::new(gateway, WorldConfig::default())
        .transport(TcpTransport::new(tcp).unwrap())
        .route_raw(
            ROUTE_ECHO,
            |_context, payload: Bytes| async move { Ok(payload) },
        )
        .build()
        .unwrap()
}

async fn connect_when_ready(address: SocketAddr, concurrency: usize) -> io::Result<EluraClient> {
    let tickets = TicketService::new(
        DEMO_TICKET_KEY,
        "game-login",
        "game-gateway",
        Duration::from_secs(60),
        Duration::from_secs(30 * 60),
    )
    .map_err(io::Error::other)?;
    let ticket = tickets
        .issue_login(Identity {
            account_id: 1,
            user_id: 1,
            region_id: 1,
            realm_id: 1,
            generation: 1,
        })
        .map_err(io::Error::other)?;
    let mut last_error = None;
    for _ in 0..50 {
        match EluraClient::connect_with_config(
            address.to_string(),
            ticket.clone(),
            ClientConfig {
                max_in_flight_requests: concurrency,
                command_capacity: concurrency,
                request_timeout: Duration::from_secs(30),
                ..ClientConfig::default()
            },
        )
        .await
        {
            Ok(client) => return Ok(client),
            Err(error) => last_error = Some(error),
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    Err(last_error
        .map(io::Error::other)
        .unwrap_or_else(|| io::Error::other("server did not start")))
}

async fn connect_ticket_when_ready(
    address: SocketAddr,
    ticket: String,
    config: ClientConfig,
) -> io::Result<(EluraClient, Duration)> {
    let started = Instant::now();
    let mut last_error = None;
    for _ in 0..50 {
        match EluraClient::connect_with_config(address.to_string(), ticket.clone(), config.clone())
            .await
        {
            Ok(client) => return Ok((client, started.elapsed())),
            Err(ClientError::Transport(error))
                if error.kind() == io::ErrorKind::ConnectionRefused =>
            {
                last_error = Some(ClientError::Transport(error));
            }
            Err(ClientError::ConnectTimeout) => {
                last_error = Some(ClientError::ConnectTimeout);
            }
            Err(error) => return Err(io::Error::other(error)),
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    Err(last_error
        .map(io::Error::other)
        .unwrap_or_else(|| io::Error::other("server did not start")))
}

fn unused_loopback_port() -> u16 {
    TcpListener::bind((Ipv4Addr::LOCALHOST, 0))
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

fn environment(name: &str, default: u64) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}

struct LatencyHistogram {
    buckets: Vec<u64>,
    bucket_width_micros: u64,
    samples: u64,
    maximum: u64,
}

impl LatencyHistogram {
    fn new(maximum: Duration, bucket_width: Duration) -> Self {
        let maximum_micros = maximum.as_micros().min(u64::MAX as u128) as u64;
        let bucket_width_micros = bucket_width.as_micros().max(1).min(u64::MAX as u128) as u64;
        let bucket_count = maximum_micros
            .checked_div(bucket_width_micros)
            .and_then(|count| count.checked_add(1))
            .and_then(|count| usize::try_from(count).ok())
            .expect("latency histogram is too large");
        Self {
            buckets: vec![0; bucket_count],
            bucket_width_micros,
            samples: 0,
            maximum: 0,
        }
    }

    fn record(&mut self, latency: Duration) {
        let micros = latency.as_micros().min(u64::MAX as u128) as u64;
        let bucket = usize::try_from(micros / self.bucket_width_micros)
            .unwrap_or(usize::MAX)
            .min(self.buckets.len() - 1);
        self.buckets[bucket] += 1;
        self.samples += 1;
        self.maximum = self.maximum.max(micros);
    }

    fn percentile(&self, quantile: f64) -> u64 {
        if self.samples == 0 {
            return 0;
        }
        let target = (self.samples as f64 * quantile).ceil() as u64;
        let mut seen = 0;
        for (index, count) in self.buckets.iter().enumerate() {
            seen += count;
            if seen >= target {
                return (index as u64)
                    .saturating_add(1)
                    .saturating_mul(self.bucket_width_micros)
                    .saturating_sub(1);
            }
        }
        self.maximum
    }
}

struct WorkerMetrics {
    completed: u64,
    errors: u64,
    error_kinds: BTreeMap<String, u64>,
    latency_micros: Vec<u64>,
}

impl WorkerMetrics {
    fn new(latency_capacity: usize) -> Self {
        Self {
            completed: 0,
            errors: 0,
            error_kinds: BTreeMap::new(),
            latency_micros: Vec::with_capacity(latency_capacity),
        }
    }

    fn record_latency(&mut self, latency: Duration) {
        self.latency_micros
            .push(latency.as_micros().min(u64::MAX as u128) as u64);
    }

    fn record_error(&mut self, error: String) {
        self.errors += 1;
        *self.error_kinds.entry(error).or_default() += 1;
    }
}

struct RequestSummary {
    completed: u64,
    errors: u64,
    error_kinds: BTreeMap<String, u64>,
    latency: LatencyHistogram,
}

impl RequestSummary {
    fn new() -> Self {
        Self {
            completed: 0,
            errors: 0,
            error_kinds: BTreeMap::new(),
            latency: LatencyHistogram::new(Duration::from_secs(30), Duration::from_micros(100)),
        }
    }

    fn record_error(&mut self, error: String) {
        self.errors += 1;
        *self.error_kinds.entry(error).or_default() += 1;
    }

    fn merge(&mut self, worker: WorkerMetrics) {
        self.completed += worker.completed;
        self.errors += worker.errors;
        for (error, count) in worker.error_kinds {
            *self.error_kinds.entry(error).or_default() += count;
        }
        for micros in worker.latency_micros {
            self.latency.record(Duration::from_micros(micros));
        }
    }
}
