use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use bytes::Bytes;
use elura_core::session::Identity;
use elura_core::{Error, Result};
use prost::Message;
use uuid::Uuid;

use super::runtime::WorldRuntime;
use super::{Route, RouteInfo, WorldCommand, WorldStatsSnapshot};

#[derive(Clone)]
pub struct WorldHarness {
    runtime: Arc<WorldRuntime>,
    next_request_id: Arc<AtomicU64>,
}

impl WorldHarness {
    pub(crate) fn new(runtime: Arc<WorldRuntime>) -> Self {
        Self {
            runtime,
            next_request_id: Arc::new(AtomicU64::new(1)),
        }
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Route, WorldBuilder, WorldConfig};

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
}
