use std::sync::Arc;

use bytes::Bytes;
use elura_core::session::Identity;
use elura_core::{Error, Result};
use uuid::Uuid;

use super::runtime::WorldRuntime;
use super::{RouteManifest, WorldCommand, WorldStatsSnapshot};

#[derive(Clone)]
pub struct WorldHarness {
    runtime: Arc<WorldRuntime>,
}

impl WorldHarness {
    pub(crate) fn new(runtime: Arc<WorldRuntime>) -> Self {
        Self { runtime }
    }

    pub async fn command(
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

    pub fn routes(&self) -> RouteManifest {
        self.runtime.route_manifest()
    }
    pub fn stats(&self) -> WorldStatsSnapshot {
        self.runtime.stats()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{WorldBuilder, WorldConfig};

    #[tokio::test]
    async fn invokes_business_handler_without_network_runtime() {
        let mut builder = WorldBuilder::new(WorldConfig::default()).unwrap();
        builder
            .register(100, |_context, payload: Bytes| async move { Ok(payload) })
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
            .command(
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
}
