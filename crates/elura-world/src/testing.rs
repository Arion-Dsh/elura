use std::collections::BTreeMap;
use std::future::Future;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use bytes::Bytes;
use elura_core::session::Identity;
use elura_core::{Error, ErrorEnvelope, Result};
use futures_util::FutureExt;
use prost::Message;
use tokio::sync::Barrier;
use tokio::task::JoinSet;
use uuid::Uuid;

use super::runtime::WorldRuntime;
use super::{Route, RouteInfo, WorldCommand, WorldStatsSnapshot};

/// Creates a minimal identity for business tests.
///
/// The account and user IDs are both set to `user_id`; region, realm and generation default to
/// `1`. Callers can overwrite fields when a test needs a different scope or account mapping.
/// `user_id` must be positive when the identity is passed to [`WorldHarness::client`].
pub fn test_identity(user_id: i64) -> Identity {
    Identity {
        account_id: user_id,
        user_id,
        region_id: 1,
        realm_id: 1,
        generation: 1,
    }
}

/// Workload settings for an in-process World load run.
///
/// Each worker owns one stable session and sends its requests sequentially. Workers run
/// concurrently. Commands for the same player are still serialized by the World runtime, exactly
/// as they are in production.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WorldLoadConfig {
    /// Number of concurrent workers.
    pub concurrency: usize,
    /// Number of business-scenario iterations run by each worker.
    pub iterations_per_worker: usize,
}

impl WorldLoadConfig {
    /// Creates workload settings.
    pub const fn new(concurrency: usize, iterations_per_worker: usize) -> Self {
        Self {
            concurrency,
            iterations_per_worker,
        }
    }

    fn validate(self) -> Result<()> {
        if self.concurrency == 0 {
            return Err(Error::InvalidConfig(
                "World load concurrency must be greater than zero".into(),
            ));
        }
        if self.iterations_per_worker == 0 {
            return Err(Error::InvalidConfig(
                "World load iterations per worker must be greater than zero".into(),
            ));
        }
        self.concurrency
            .checked_mul(self.iterations_per_worker)
            .ok_or_else(|| {
                Error::InvalidConfig("World load operation count overflows usize".into())
            })?;
        Ok(())
    }
}

/// Business-operation latency distribution from an in-process World load run.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct WorldLoadLatency {
    /// Fastest observed operation.
    pub min: Duration,
    /// Arithmetic mean across all attempted operations.
    pub mean: Duration,
    /// 50th-percentile operation latency.
    pub p50: Duration,
    /// 95th-percentile operation latency.
    pub p95: Duration,
    /// 99th-percentile operation latency.
    pub p99: Duration,
    /// Slowest observed operation.
    pub max: Duration,
}

impl WorldLoadLatency {
    fn from_samples(mut samples: Vec<u64>) -> Self {
        if samples.is_empty() {
            return Self::default();
        }
        samples.sort_unstable();
        let total = samples
            .iter()
            .fold(0_u128, |sum, sample| sum + u128::from(*sample));
        let mean = (total / samples.len() as u128).min(u128::from(u64::MAX)) as u64;
        Self {
            min: Duration::from_nanos(samples[0]),
            mean: Duration::from_nanos(mean),
            p50: Duration::from_nanos(percentile(&samples, 50)),
            p95: Duration::from_nanos(percentile(&samples, 95)),
            p99: Duration::from_nanos(percentile(&samples, 99)),
            max: Duration::from_nanos(samples[samples.len() - 1]),
        }
    }
}

/// Aggregate result of an in-process World load run.
#[derive(Debug, Clone, PartialEq)]
pub struct WorldLoadReport {
    /// Total business operations attempted by all workers.
    pub attempted: u64,
    /// Business operations which completed successfully.
    pub succeeded: u64,
    /// Business operations which returned an error or panicked.
    pub failed: u64,
    /// Wall-clock duration of the complete load run.
    pub elapsed: Duration,
    /// Latency distribution across successful and failed operations.
    pub latency: WorldLoadLatency,
    /// Stable public error-code counts for failed operations.
    pub errors: BTreeMap<String, u64>,
}

impl WorldLoadReport {
    /// Returns true when every attempted operation succeeded.
    pub fn is_success(&self) -> bool {
        self.failed == 0
    }

    /// Returns successful operations divided by attempted operations.
    pub fn success_ratio(&self) -> f64 {
        if self.attempted == 0 {
            return 0.0;
        }
        self.succeeded as f64 / self.attempted as f64
    }

    /// Returns attempted business operations per wall-clock second.
    pub fn operations_per_second(&self) -> f64 {
        self.attempted as f64 / self.elapsed.as_secs_f64().max(f64::EPSILON)
    }

    /// Returns attempted operations per second for single-route request workloads.
    pub fn requests_per_second(&self) -> f64 {
        self.operations_per_second()
    }
}

/// In-process entry point for unit tests and business-path load scenarios.
#[derive(Clone)]
pub struct WorldHarness {
    runtime: Arc<WorldRuntime>,
    next_request_id: Arc<AtomicU64>,
}

/// A virtual authenticated World client for business tests.
///
/// Clones share the same identity, session and request-ID source, so a test can express a
/// multi-route business flow without manually threading protocol metadata through every call.
#[derive(Clone)]
pub struct WorldTestClient {
    harness: WorldHarness,
    identity: Identity,
    session_id: Uuid,
}

impl WorldTestClient {
    /// Invokes a typed route in this client's stable session.
    pub async fn call<E: Route>(&self, _route: E, request: E::Request) -> Result<E::Response> {
        self.call_typed::<E>(request).await
    }

    /// Invokes a raw route in this client's stable session.
    pub async fn command_raw(&self, route: u32, payload: impl Into<Bytes>) -> Result<Bytes> {
        self.harness
            .command_raw(
                self.identity.clone(),
                self.session_id,
                route,
                self.harness.next_request_id(),
                payload,
            )
            .await
    }

    /// Returns this virtual client's authenticated identity.
    pub fn identity(&self) -> &Identity {
        &self.identity
    }

    /// Returns the session shared by all calls made through this client.
    pub fn session_id(&self) -> Uuid {
        self.session_id
    }

    async fn call_typed<E: Route>(&self, request: E::Request) -> Result<E::Response> {
        self.harness
            .call_typed::<E>(self.identity.clone(), self.session_id, request)
            .await
    }
}

impl WorldHarness {
    pub(crate) fn new(runtime: Arc<WorldRuntime>) -> Self {
        Self {
            runtime,
            next_request_id: Arc::new(AtomicU64::new(1)),
        }
    }

    /// Creates a virtual client with a fresh stable session for business tests.
    pub fn client(&self, identity: Identity) -> Result<WorldTestClient> {
        self.client_in_session(identity, Uuid::new_v4())
    }

    /// Creates a virtual client with a caller-provided stable session.
    pub fn client_in_session(
        &self,
        identity: Identity,
        session_id: Uuid,
    ) -> Result<WorldTestClient> {
        identity.validate()?;
        Ok(WorldTestClient {
            harness: self.clone(),
            identity,
            session_id,
        })
    }

    /// Invokes a typed route with an isolated test session.
    pub async fn call<E: Route>(
        &self,
        route: E,
        identity: Identity,
        request: E::Request,
    ) -> Result<E::Response> {
        self.call_in_session(route, identity, Uuid::new_v4(), request)
            .await
    }

    /// Invokes a typed route in a caller-provided test session.
    pub async fn call_in_session<E: Route>(
        &self,
        _route: E,
        identity: Identity,
        session_id: Uuid,
        request: E::Request,
    ) -> Result<E::Response> {
        self.call_typed::<E>(identity, session_id, request).await
    }

    /// Runs a single typed route workload without opening network listeners.
    ///
    /// Identity and request factories receive worker and iteration indices. Each worker owns one
    /// stable virtual client and session. Give workers distinct player identities to exercise
    /// parallel handler execution; World deliberately serializes commands for the same player.
    ///
    /// This exercises handlers, middleware, timeouts, player serialization and response codecs.
    /// It does not measure Gateway, transport, TLS or network overhead.
    pub async fn load_route<E, I, R>(
        &self,
        _route: E,
        config: WorldLoadConfig,
        make_identity: I,
        make_request: R,
    ) -> Result<WorldLoadReport>
    where
        E: Route,
        I: Fn(usize) -> Identity + Send + Sync + 'static,
        R: Fn(usize, usize) -> E::Request + Send + Sync + 'static,
    {
        let make_request = Arc::new(make_request);
        self.load_scenario(config, make_identity, move |client, worker, iteration| {
            let request = make_request(worker, iteration);
            async move {
                client.call_typed::<E>(request).await?;
                Ok(())
            }
        })
        .await
    }

    /// Runs a reusable multi-route business scenario concurrently without network listeners.
    ///
    /// The identity factory runs once per worker. The scenario receives that worker's stable
    /// virtual client plus `(worker_index, iteration_index)`, and may call any number of typed or
    /// raw routes. One reported operation is one complete scenario iteration.
    pub async fn load_scenario<I, S, Fut>(
        &self,
        config: WorldLoadConfig,
        make_identity: I,
        scenario: S,
    ) -> Result<WorldLoadReport>
    where
        I: Fn(usize) -> Identity + Send + Sync + 'static,
        S: Fn(WorldTestClient, usize, usize) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<()>> + Send + 'static,
    {
        config.validate()?;
        let clients = (0..config.concurrency)
            .map(|worker| self.client(make_identity(worker)))
            .collect::<Result<Vec<_>>>()?;
        let scenario = Arc::new(scenario);
        let start = Arc::new(Barrier::new(config.concurrency + 1));
        let mut workers = JoinSet::new();
        for (worker_index, client) in clients.into_iter().enumerate() {
            let scenario = scenario.clone();
            let start = start.clone();
            workers.spawn(async move {
                start.wait().await;
                let mut result = LoadWorkerResult::with_capacity(config.iterations_per_worker);
                for iteration in 0..config.iterations_per_worker {
                    let operation_started = Instant::now();
                    let response = match catch_unwind(AssertUnwindSafe(|| {
                        scenario(client.clone(), worker_index, iteration)
                    })) {
                        Ok(operation) => match AssertUnwindSafe(operation).catch_unwind().await {
                            Ok(response) => response,
                            Err(_) => Err(Error::Internal(
                                "World load scenario operation panicked".into(),
                            )),
                        },
                        Err(_) => Err(Error::Internal(
                            "World load scenario factory panicked".into(),
                        )),
                    };
                    result.latency_nanos.push(elapsed_nanos(operation_started));
                    match response {
                        Ok(_) => result.succeeded += 1,
                        Err(error) => {
                            let code = ErrorEnvelope::from(&error).code;
                            *result.errors.entry(code).or_default() += 1;
                        }
                    }
                }
                result
            });
        }
        let started = Instant::now();
        start.wait().await;

        let attempted = config
            .concurrency
            .checked_mul(config.iterations_per_worker)
            .expect("validated World load operation count") as u64;
        let mut succeeded = 0_u64;
        let mut errors = BTreeMap::new();
        let mut latency_nanos = Vec::with_capacity(attempted as usize);
        while let Some(worker) = workers.join_next().await {
            let worker = worker
                .map_err(|error| Error::Internal(format!("World load worker failed: {error}")))?;
            succeeded += worker.succeeded;
            latency_nanos.extend(worker.latency_nanos);
            for (code, count) in worker.errors {
                *errors.entry(code).or_default() += count;
            }
        }
        Ok(WorldLoadReport {
            attempted,
            succeeded,
            failed: attempted - succeeded,
            elapsed: started.elapsed(),
            latency: WorldLoadLatency::from_samples(latency_nanos),
            errors,
        })
    }

    async fn call_typed<E: Route>(
        &self,
        identity: Identity,
        session_id: Uuid,
        request: E::Request,
    ) -> Result<E::Response> {
        let response = self
            .command_raw(
                identity,
                session_id,
                E::ID,
                self.next_request_id(),
                Bytes::from(request.encode_to_vec()),
            )
            .await?;
        E::Response::decode(response)
            .map_err(|_| Error::InvalidFrame("invalid typed World harness response".into()))
    }

    /// Invokes a route using a raw route ID and payload.
    pub async fn command_raw(
        &self,
        identity: Identity,
        session_id: Uuid,
        route: u32,
        request_id: u64,
        payload: impl Into<Bytes>,
    ) -> Result<Bytes> {
        identity.validate()?;
        if request_id == 0 {
            return Err(Error::InvalidFrame(
                "World harness request ID is zero".into(),
            ));
        }
        self.runtime
            .execute(
                route,
                request_id,
                WorldCommand {
                    authorization: None,
                    identity,
                    session_id: session_id.to_string(),
                    trace_id: elura_runtime::observability::new_trace_id(),
                    request_id,
                    payload: payload.into(),
                    shard_id: None,
                    owner_id: None,
                    owner_epoch: None,
                    timeout: std::time::Duration::from_secs(5),
                },
            )
            .await
    }

    fn next_request_id(&self) -> u64 {
        loop {
            let request_id = self.next_request_id.fetch_add(1, Ordering::Relaxed);
            if request_id != 0 {
                return request_id;
            }
        }
    }

    pub fn routes(&self) -> Vec<RouteInfo> {
        self.runtime.route_info()
    }
    pub fn stats(&self) -> WorldStatsSnapshot {
        self.runtime.stats()
    }
}

struct LoadWorkerResult {
    succeeded: u64,
    latency_nanos: Vec<u64>,
    errors: BTreeMap<String, u64>,
}

impl LoadWorkerResult {
    fn with_capacity(requests: usize) -> Self {
        Self {
            succeeded: 0,
            latency_nanos: Vec::with_capacity(requests),
            errors: BTreeMap::new(),
        }
    }
}

fn elapsed_nanos(started: Instant) -> u64 {
    started.elapsed().as_nanos().min(u128::from(u64::MAX)) as u64
}

fn percentile(sorted: &[u64], percentile: usize) -> u64 {
    let rank = sorted.len().saturating_mul(percentile).saturating_add(99) / 100;
    sorted[rank.saturating_sub(1).min(sorted.len() - 1)]
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime::WorldBuilder;
    use crate::{Route, WorldConfig};

    #[derive(Clone, PartialEq, Message)]
    struct Echo {
        #[prost(string, tag = "1")]
        value: String,
    }

    struct EchoRoute;

    impl Route for EchoRoute {
        const ID: u32 = 100;
        const NAME: &'static str = "test.echo";

        type Request = Echo;
        type Response = Echo;
    }

    #[tokio::test]
    async fn invokes_business_handler_without_network_runtime() {
        let mut builder = WorldBuilder::new(WorldConfig::default()).unwrap();
        builder
            .register_raw(100, |_context, payload: Bytes| async move { Ok(payload) })
            .unwrap();
        let harness = builder.build().unwrap().harness();
        let identity = Identity {
            account_id: 1,
            user_id: 1,
            region_id: 1,
            realm_id: 1,
            generation: 1,
        };
        let response = harness
            .command_raw(
                identity,
                Uuid::new_v4(),
                100,
                1,
                Bytes::from_static(b"hello"),
            )
            .await
            .unwrap();
        assert_eq!(response, Bytes::from_static(b"hello"));
        assert_eq!(harness.stats().succeeded, 1);
    }

    #[tokio::test]
    async fn invokes_and_decodes_a_typed_route() {
        let mut builder = WorldBuilder::new(WorldConfig::default()).unwrap();
        builder
            .register(EchoRoute, |_context, mut request| async move {
                request.value.make_ascii_uppercase();
                Ok(request)
            })
            .unwrap();
        let harness = builder.build().unwrap().harness();
        let response = harness
            .call(
                EchoRoute,
                Identity {
                    account_id: 1,
                    user_id: 1,
                    region_id: 1,
                    realm_id: 1,
                    generation: 1,
                },
                Echo {
                    value: "hello".into(),
                },
            )
            .await
            .unwrap();
        assert_eq!(response.value, "HELLO");
    }

    #[tokio::test]
    async fn runs_concurrent_typed_workload_and_aggregates_failures() {
        let mut builder = WorldBuilder::new(WorldConfig::default()).unwrap();
        builder
            .register(EchoRoute, |_context, request| async move {
                if request.value == "reject" {
                    return Err(Error::business("REJECTED", "rejected by test"));
                }
                Ok(request)
            })
            .unwrap();
        let harness = builder.build().unwrap().harness();
        let report = harness
            .load_route(
                EchoRoute,
                WorldLoadConfig::new(4, 5),
                |worker| Identity {
                    account_id: worker as i64 + 1,
                    user_id: worker as i64 + 1,
                    region_id: 1,
                    realm_id: 1,
                    generation: 1,
                },
                |_worker, iteration| Echo {
                    value: if iteration == 0 {
                        "reject".into()
                    } else {
                        "ok".into()
                    },
                },
            )
            .await
            .unwrap();

        assert_eq!(report.attempted, 20);
        assert_eq!(report.succeeded, 16);
        assert_eq!(report.failed, 4);
        assert_eq!(report.errors.get("REJECTED"), Some(&4));
        assert_eq!(report.success_ratio(), 0.8);
        assert!(!report.is_success());
        assert!(report.requests_per_second().is_finite());
        assert!(report.latency.min <= report.latency.p50);
        assert!(report.latency.p50 <= report.latency.p95);
        assert!(report.latency.p95 <= report.latency.p99);
        assert!(report.latency.p99 <= report.latency.max);
        assert_eq!(harness.stats().commands, 20);
    }

    #[tokio::test]
    async fn virtual_client_keeps_identity_and_session_across_business_calls() {
        let mut builder = WorldBuilder::new(WorldConfig::default()).unwrap();
        builder
            .register(EchoRoute, |context, _request| async move {
                Ok(Echo {
                    value: format!("{}:{}", context.identity.user_id, context.session_id),
                })
            })
            .unwrap();
        let harness = builder.build().unwrap().harness();
        let client = harness
            .client(Identity {
                account_id: 7,
                user_id: 9,
                region_id: 1,
                realm_id: 1,
                generation: 1,
            })
            .unwrap();

        let first = client.call(EchoRoute, Echo::default()).await.unwrap();
        let second = client.call(EchoRoute, Echo::default()).await.unwrap();

        assert_eq!(client.identity().user_id, 9);
        assert_eq!(first, second);
        assert_eq!(first.value, format!("9:{}", client.session_id()));
    }

    #[tokio::test]
    async fn runs_multi_route_business_scenarios_as_load_operations() {
        let mut builder = WorldBuilder::new(WorldConfig::default()).unwrap();
        builder
            .register(EchoRoute, |_context, request| async move { Ok(request) })
            .unwrap();
        let harness = builder.build().unwrap().harness();
        let report = harness
            .load_scenario(
                WorldLoadConfig::new(2, 3),
                |worker| Identity {
                    account_id: worker as i64 + 1,
                    user_id: worker as i64 + 1,
                    region_id: 1,
                    realm_id: 1,
                    generation: 1,
                },
                |client, worker, iteration| async move {
                    let first = client
                        .call(
                            EchoRoute,
                            Echo {
                                value: format!("{worker}:{iteration}"),
                            },
                        )
                        .await?;
                    let second = client.call(EchoRoute, first.clone()).await?;
                    if first != second {
                        return Err(Error::Internal("scenario response changed".into()));
                    }
                    Ok(())
                },
            )
            .await
            .unwrap();

        assert_eq!(report.attempted, 6);
        assert_eq!(report.succeeded, 6);
        assert_eq!(harness.stats().commands, 12);
    }

    #[tokio::test]
    async fn isolates_panicked_scenario_iterations_and_continues_the_worker() {
        let mut builder = WorldBuilder::new(WorldConfig::default()).unwrap();
        builder
            .register(EchoRoute, |_context, request| async move { Ok(request) })
            .unwrap();
        let harness = builder.build().unwrap().harness();
        let report = harness
            .load_scenario(
                WorldLoadConfig::new(1, 3),
                |_| Identity {
                    account_id: 1,
                    user_id: 1,
                    region_id: 1,
                    realm_id: 1,
                    generation: 1,
                },
                |client, _worker, iteration| async move {
                    assert_ne!(iteration, 1, "intentional scenario panic");
                    client.call(EchoRoute, Echo::default()).await?;
                    Ok(())
                },
            )
            .await
            .unwrap();

        assert_eq!(report.attempted, 3);
        assert_eq!(report.succeeded, 2);
        assert_eq!(report.failed, 1);
        assert_eq!(report.errors.get("INTERNAL"), Some(&1));
        assert_eq!(harness.stats().commands, 2);
    }

    #[tokio::test]
    async fn rejects_empty_load_configuration() {
        let mut builder = WorldBuilder::new(WorldConfig::default()).unwrap();
        builder
            .register(EchoRoute, |_context, request| async move { Ok(request) })
            .unwrap();
        let harness = builder.build().unwrap().harness();
        let result = harness
            .load_route(
                EchoRoute,
                WorldLoadConfig::new(0, 1),
                |_| unreachable!("invalid workload must fail before making identities"),
                |_, _| unreachable!("invalid workload must fail before making requests"),
            )
            .await;
        assert!(matches!(result, Err(Error::InvalidConfig(_))));
    }

    #[test]
    fn calculates_nearest_rank_percentiles() {
        let latency = WorldLoadLatency::from_samples((1..=100).collect());
        assert_eq!(latency.min, Duration::from_nanos(1));
        assert_eq!(latency.mean, Duration::from_nanos(50));
        assert_eq!(latency.p50, Duration::from_nanos(50));
        assert_eq!(latency.p95, Duration::from_nanos(95));
        assert_eq!(latency.p99, Duration::from_nanos(99));
        assert_eq!(latency.max, Duration::from_nanos(100));
    }
}
