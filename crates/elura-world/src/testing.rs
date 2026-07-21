use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use bytes::Bytes;
use elura_core::session::Identity;
use elura_core::{Error, Result};
use prost::Message;
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

/// In-process entry point for fast World business unit tests.
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

    /// Returns registered route diagnostics.
    pub fn routes(&self) -> Vec<RouteInfo> {
        self.runtime.route_info()
    }

    /// Returns a snapshot of World runtime counters.
    pub fn stats(&self) -> WorldStatsSnapshot {
        self.runtime.stats()
    }
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
        let response = harness
            .command_raw(
                test_identity(1),
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
                test_identity(1),
                Echo {
                    value: "hello".into(),
                },
            )
            .await
            .unwrap();
        assert_eq!(response.value, "HELLO");
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
        let client = harness.client(test_identity(9)).unwrap();

        let first = client.call(EchoRoute, Echo::default()).await.unwrap();
        let second = client.call(EchoRoute, Echo::default()).await.unwrap();

        assert_eq!(client.identity().user_id, 9);
        assert_eq!(first, second);
        assert_eq!(first.value, format!("9:{}", client.session_id()));
    }
}
