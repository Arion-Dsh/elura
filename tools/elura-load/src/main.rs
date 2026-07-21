use std::collections::BTreeMap;
use std::error::Error;
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;
use elura_core::ErrorEnvelope;
use elura_core::protocol::{Frame, FrameCodec, FrameKind, ROUTE_AUTHENTICATE};
use elura_core::session::Identity;
use elura_core::ticket::TicketService;
use futures_util::{SinkExt, StreamExt};
use tokio::net::{TcpStream, lookup_host};
use tokio::sync::Barrier;
use tokio::task::JoinSet;
use tokio::time::timeout;
use tokio_util::codec::Framed;

type AnyError = Box<dyn Error + Send + Sync>;
type AnyResult<T> = std::result::Result<T, AnyError>;

fn invalid_input(message: impl Into<String>) -> AnyError {
    std::io::Error::new(std::io::ErrorKind::InvalidInput, message.into()).into()
}

#[derive(Clone)]
struct Config {
    address: String,
    connections: usize,
    requests_per_connection: usize,
    route: u32,
    payload_bytes: usize,
    max_payload: usize,
    timeout: Duration,
    batch_size: usize,
    ramp_interval: Duration,
    ticket_key: Arc<[u8]>,
    issuer: Arc<str>,
    audience: Arc<str>,
    region_id: u32,
    realm_id: u32,
    first_user_id: i64,
}

impl Config {
    fn parse() -> AnyResult<Option<Self>> {
        let mut config = Self {
            address: "127.0.0.1:17000".into(),
            connections: 1_000,
            requests_per_connection: 100,
            route: 100,
            payload_bytes: 128,
            max_payload: 1 << 20,
            timeout: Duration::from_secs(5),
            batch_size: 100,
            ramp_interval: Duration::from_millis(100),
            ticket_key: std::env::var("ELURA_LOAD_TICKET_KEY")
                .unwrap_or_default()
                .into_bytes()
                .into(),
            issuer: Arc::from("auth"),
            audience: Arc::from("gateway"),
            region_id: 1,
            realm_id: 1,
            first_user_id: 1,
        };
        let mut args = std::env::args().skip(1);
        while let Some(argument) = args.next() {
            if argument == "--help" || argument == "-h" {
                print_help();
                return Ok(None);
            }
            let value = args
                .next()
                .ok_or_else(|| invalid_input(format!("missing value for {argument}")))?;
            match argument.as_str() {
                "--address" => config.address = value,
                "--connections" => config.connections = value.parse()?,
                "--requests" => config.requests_per_connection = value.parse()?,
                "--route" => config.route = value.parse()?,
                "--payload-bytes" => config.payload_bytes = value.parse()?,
                "--max-payload" => config.max_payload = value.parse()?,
                "--timeout-ms" => config.timeout = Duration::from_millis(value.parse()?),
                "--batch-size" => config.batch_size = value.parse()?,
                "--ramp-ms" => config.ramp_interval = Duration::from_millis(value.parse()?),
                "--ticket-key" => config.ticket_key = Arc::from(value.into_bytes()),
                "--issuer" => config.issuer = Arc::from(value),
                "--audience" => config.audience = Arc::from(value),
                "--region" => config.region_id = value.parse()?,
                "--realm" => config.realm_id = value.parse()?,
                "--first-user-id" => config.first_user_id = value.parse()?,
                _ => return Err(invalid_input(format!("unknown argument {argument}"))),
            }
        }
        if config.connections == 0
            || config.requests_per_connection == 0
            || config.route < 100
            || config.payload_bytes > config.max_payload
            || config.batch_size == 0
            || config.timeout.is_zero()
            || config.region_id == 0
            || config.realm_id == 0
            || config.first_user_id <= 0
            || config.ticket_key.len() < 32
        {
            return Err(invalid_input(
                "invalid load configuration; use --help for constraints",
            ));
        }
        Ok(Some(config))
    }
}

#[derive(Default)]
struct WorkerResult {
    connected: bool,
    authenticated: bool,
    connect_micros: Option<u64>,
    authentication_micros: Option<u64>,
    authentication_error: Option<String>,
    request_micros: Vec<u64>,
    request_errors: u64,
}

#[derive(Default)]
struct Summary {
    connected: usize,
    authenticated: usize,
    connect_micros: Vec<u64>,
    authentication_micros: Vec<u64>,
    authentication_errors: BTreeMap<String, u64>,
    request_micros: Vec<u64>,
    request_errors: u64,
    task_errors: u64,
}

impl Summary {
    fn record(&mut self, result: WorkerResult) {
        self.connected += usize::from(result.connected);
        self.authenticated += usize::from(result.authenticated);
        self.connect_micros.extend(result.connect_micros);
        self.authentication_micros
            .extend(result.authentication_micros);
        if let Some(error) = result.authentication_error {
            *self.authentication_errors.entry(error).or_default() += 1;
        }
        self.request_micros.extend(result.request_micros);
        self.request_errors += result.request_errors;
    }

    fn print(mut self, config: &Config, total_elapsed: Duration, request_elapsed: Duration) {
        self.connect_micros.sort_unstable();
        self.authentication_micros.sort_unstable();
        self.request_micros.sort_unstable();
        let completed = self.request_micros.len() as f64;
        println!("connections.requested={}", config.connections);
        println!("connections.connected={}", self.connected);
        println!("connections.authenticated={}", self.authenticated);
        println!(
            "connections.failed={}",
            config.connections.saturating_sub(self.authenticated)
        );
        for (error, count) in &self.authentication_errors {
            println!("authentication.errors.{error}={count}");
        }
        println!("requests.completed={}", self.request_micros.len());
        println!("requests.errors={}", self.request_errors);
        println!("tasks.errors={}", self.task_errors);
        println!("elapsed.total_seconds={:.3}", total_elapsed.as_secs_f64());
        println!(
            "elapsed.request_phase_seconds={:.3}",
            request_elapsed.as_secs_f64()
        );
        println!(
            "throughput.requests_per_second={:.2}",
            completed / request_elapsed.as_secs_f64().max(f64::EPSILON)
        );
        print_distribution("connect", &self.connect_micros);
        print_distribution("authentication", &self.authentication_micros);
        print_distribution("request", &self.request_micros);
    }
}

#[tokio::main]
async fn main() -> AnyResult<()> {
    let Some(mut config) = Config::parse()? else {
        return Ok(());
    };
    let target = config.address.clone();
    config.address = lookup_host(target.as_str())
        .await?
        .next()
        .ok_or_else(|| invalid_input("load target did not resolve"))?
        .to_string();
    let tickets = Arc::new(TicketService::new(
        config.ticket_key.as_ref(),
        config.issuer.as_ref(),
        config.audience.as_ref(),
        Duration::from_secs(300),
        Duration::from_secs(1_800),
    )?);
    let started = Instant::now();
    let start_barrier = Arc::new(Barrier::new(config.connections + 1));
    let mut workers = JoinSet::new();
    for batch_start in (0..config.connections).step_by(config.batch_size) {
        let batch_end = (batch_start + config.batch_size).min(config.connections);
        for index in batch_start..batch_end {
            let config = config.clone();
            let tickets = tickets.clone();
            let start_barrier = start_barrier.clone();
            workers.spawn(async move { run_worker(index, config, tickets, start_barrier).await });
        }
        if batch_end < config.connections && !config.ramp_interval.is_zero() {
            tokio::time::sleep(config.ramp_interval).await;
        }
    }
    start_barrier.wait().await;
    let request_phase_started = Instant::now();
    let mut summary = Summary::default();
    while let Some(result) = workers.join_next().await {
        match result {
            Ok(result) => summary.record(result),
            Err(_) => summary.task_errors += 1,
        }
    }
    summary.print(&config, started.elapsed(), request_phase_started.elapsed());
    Ok(())
}

async fn run_worker(
    index: usize,
    config: Config,
    tickets: Arc<TicketService>,
    start_barrier: Arc<Barrier>,
) -> WorkerResult {
    let (mut result, framed) = prepare_worker(index, &config, &tickets).await;
    start_barrier.wait().await;
    let Some(mut framed) = framed else {
        return result;
    };

    let payload = Bytes::from(vec![7; config.payload_bytes]);
    result.request_micros = Vec::with_capacity(config.requests_per_connection);
    for request_index in 0..config.requests_per_connection {
        let request_id = request_index as u64 + 2;
        let request = match Frame::request(config.route, request_id, payload.clone()) {
            Ok(request) => request,
            Err(_) => {
                result.request_errors += 1;
                continue;
            }
        };
        let request_started = Instant::now();
        match exchange(&mut framed, request, config.timeout).await {
            Ok(response)
                if response.request_id == request_id && response.kind == FrameKind::Response =>
            {
                result
                    .request_micros
                    .push(micros(request_started.elapsed()));
            }
            _ => result.request_errors += 1,
        }
    }
    result
}

async fn prepare_worker(
    index: usize,
    config: &Config,
    tickets: &TicketService,
) -> (WorkerResult, Option<Framed<TcpStream, FrameCodec>>) {
    let mut result = WorkerResult::default();
    let connected_at = Instant::now();
    let stream = match timeout(config.timeout, TcpStream::connect(config.address.as_str())).await {
        Ok(Ok(stream)) => stream,
        _ => return (result, None),
    };
    result.connected = true;
    result.connect_micros = Some(micros(connected_at.elapsed()));
    if stream.set_nodelay(true).is_err() {
        return (result, None);
    }
    let codec = match FrameCodec::new(config.max_payload) {
        Ok(codec) => codec,
        Err(_) => return (result, None),
    };
    let mut framed = Framed::new(stream, codec);
    let user_id = match config.first_user_id.checked_add(index as i64) {
        Some(user_id) => user_id,
        None => return (result, None),
    };
    let ticket = match tickets.issue_login(Identity {
        account_id: user_id,
        user_id,
        region_id: config.region_id,
        realm_id: config.realm_id,
        generation: 1,
    }) {
        Ok(ticket) => ticket,
        Err(_) => return (result, None),
    };
    let authentication_payload = match serde_json::to_vec(&serde_json::json!({ "ticket": ticket }))
    {
        Ok(payload) => payload,
        Err(_) => return (result, None),
    };
    let authentication = match Frame::request(ROUTE_AUTHENTICATE, 1, authentication_payload) {
        Ok(frame) => frame,
        Err(_) => return (result, None),
    };
    let authentication_started = Instant::now();
    let response = match exchange(&mut framed, authentication, config.timeout).await {
        Ok(response) => response,
        Err(()) => {
            result.authentication_error = Some("CLIENT_IO_OR_TIMEOUT".into());
            return (result, None);
        }
    };
    if response.kind != FrameKind::Response {
        result.authentication_error = Some(
            ErrorEnvelope::from_slice(&response.payload)
                .map(|error| error.code)
                .unwrap_or_else(|_| "INVALID_ERROR_RESPONSE".into()),
        );
        return (result, None);
    }
    result.authenticated = true;
    result.authentication_micros = Some(micros(authentication_started.elapsed()));
    (result, Some(framed))
}

async fn exchange(
    framed: &mut Framed<TcpStream, FrameCodec>,
    request: Frame,
    deadline: Duration,
) -> std::result::Result<Frame, ()> {
    timeout(deadline, framed.send(request))
        .await
        .map_err(|_| ())?
        .map_err(|_| ())?;
    timeout(deadline, framed.next())
        .await
        .map_err(|_| ())?
        .ok_or(())?
        .map_err(|_| ())
}

fn micros(duration: Duration) -> u64 {
    duration.as_micros().min(u64::MAX as u128) as u64
}

fn print_distribution(name: &str, values: &[u64]) {
    if values.is_empty() {
        println!("latency.{name}.samples=0");
        return;
    }
    println!("latency.{name}.samples={}", values.len());
    println!("latency.{name}.p50_us={}", percentile(values, 0.50));
    println!("latency.{name}.p95_us={}", percentile(values, 0.95));
    println!("latency.{name}.p99_us={}", percentile(values, 0.99));
    println!("latency.{name}.max_us={}", values[values.len() - 1]);
}

fn percentile(sorted: &[u64], quantile: f64) -> u64 {
    let index = ((sorted.len() - 1) as f64 * quantile).ceil() as usize;
    sorted[index]
}

fn print_help() {
    println!(
        "elura-load options:\n\
         --address HOST:PORT          Gateway TCP address (default 127.0.0.1:17000)\n\
         --connections N              Concurrent connections, e.g. 1000 or 10000\n\
         --requests N                 Requests per authenticated connection (default 100)\n\
         --route N                    Application route >= 100 (default 100)\n\
         --payload-bytes N            Request payload size (default 128)\n\
         --max-payload N              Frame payload limit (default 1048576)\n\
         --timeout-ms N               Per operation timeout (default 5000)\n\
         --batch-size N               Connections opened per ramp batch (default 100)\n\
         --ramp-ms N                  Delay between batches (default 100)\n\
         --ticket-key KEY             Gateway HMAC key, or ELURA_LOAD_TICKET_KEY\n\
         --issuer NAME                Ticket issuer (default auth)\n\
         --audience NAME              Ticket audience (default gateway)\n\
         --region N --realm N         Identity region/realm (default 1/1)\n\
         --first-user-id N            First generated user ID (default 1)"
    );
}
