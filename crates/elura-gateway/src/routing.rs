use std::collections::{HashMap, HashSet};
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};

pub use crate::discovery::{WorldRouteTarget, WorldRouteUpdater};
use async_trait::async_trait;
use elura_core::gateway_world::WorldRequest;
use elura_core::ownership::Assignment;
use elura_core::{Error, Result};
use sha2::{Digest, Sha256};

use elura_runtime::security::{ClientTlsConfig, InternalToken};

use super::{TcpWorldClient, WORLD_CONNECTION_IN_FLIGHT, validate_world_connection_in_flight};
use crate::discovery::WorldClient;

#[async_trait]
pub(crate) trait WorldRouteDirectory: Send + Sync + 'static {
    async fn resolve(
        &self,
        region_id: u32,
        realm_id: u32,
        route: u32,
    ) -> Result<Vec<WorldRouteTarget>>;

    async fn readiness(&self) -> Result<()> {
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
struct RouteKey {
    region_id: u32,
    realm_id: u32,
    route: u32,
}

#[derive(Default)]
pub(crate) struct MemoryWorldRouteDirectory {
    targets: RwLock<HashMap<RouteKey, Vec<WorldRouteTarget>>>,
}

impl MemoryWorldRouteDirectory {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn set_targets(
        &self,
        region_id: u32,
        realm_id: u32,
        route: u32,
        targets: Vec<WorldRouteTarget>,
    ) -> Result<()> {
        if region_id == 0 || realm_id == 0 {
            return Err(Error::InvalidConfig(
                "World route region and realm must be positive".into(),
            ));
        }
        if targets.is_empty() {
            self.targets
                .write()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .remove(&RouteKey {
                    region_id,
                    realm_id,
                    route,
                });
            return Ok(());
        }
        let mut identities = HashSet::new();
        let mut addresses = HashSet::new();
        for target in &targets {
            target.validate()?;
            if !identities.insert(target.world_id.clone()) || !addresses.insert(target.address) {
                return Err(Error::InvalidConfig("duplicate World route target".into()));
            }
        }
        self.targets
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .insert(
                RouteKey {
                    region_id,
                    realm_id,
                    route,
                },
                targets,
            );
        Ok(())
    }
}

#[async_trait]
impl WorldRouteUpdater for MemoryWorldRouteDirectory {
    async fn replace_targets(
        &self,
        region_id: u32,
        realm_id: u32,
        route: u32,
        targets: Vec<WorldRouteTarget>,
    ) -> Result<()> {
        self.set_targets(region_id, realm_id, route, targets)
    }
}

#[async_trait]
impl WorldRouteDirectory for MemoryWorldRouteDirectory {
    async fn resolve(
        &self,
        region_id: u32,
        realm_id: u32,
        route: u32,
    ) -> Result<Vec<WorldRouteTarget>> {
        let targets = self
            .targets
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        targets
            .get(&RouteKey {
                region_id,
                realm_id,
                route,
            })
            .or_else(|| {
                targets.get(&RouteKey {
                    region_id,
                    realm_id,
                    route: 0,
                })
            })
            .filter(|targets| !targets.is_empty())
            .cloned()
            .ok_or(Error::Unavailable)
    }

    async fn readiness(&self) -> Result<()> {
        let ready = self
            .targets
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .values()
            .any(|targets| !targets.is_empty());
        if ready {
            Ok(())
        } else {
            Err(Error::Unavailable)
        }
    }
}

pub(crate) struct RouteWorldClient {
    directory: Arc<dyn WorldRouteDirectory>,
    max_payload: usize,
    pool_size: usize,
    max_in_flight_per_connection: usize,
    clients: RwLock<HashMap<(String, SocketAddr), CachedClient>>,
    client_clock: AtomicU64,
    authorization: Option<InternalToken>,
    tls: Option<ClientTlsConfig>,
}

struct CachedClient {
    client: Arc<TcpWorldClient>,
    access: u64,
}

const ROUTE_CLIENT_CACHE_CAPACITY: usize = 1024;

impl RouteWorldClient {
    pub fn new(
        directory: Arc<dyn WorldRouteDirectory>,
        max_payload: usize,
        pool_size: usize,
    ) -> Result<Self> {
        if max_payload == 0 || pool_size == 0 || pool_size > 1024 {
            return Err(Error::InvalidConfig("invalid Route World client".into()));
        }
        Ok(Self {
            directory,
            max_payload,
            pool_size,
            max_in_flight_per_connection: WORLD_CONNECTION_IN_FLIGHT,
            clients: RwLock::new(HashMap::new()),
            client_clock: AtomicU64::new(0),
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

    async fn client(&self, target: &WorldRouteTarget) -> Result<Arc<TcpWorldClient>> {
        let key = (target.world_id.clone(), target.address);
        let access = self.client_clock.fetch_add(1, Ordering::Relaxed);
        {
            let mut clients = self
                .clients
                .write()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            if let Some(cached) = clients.get_mut(&key) {
                cached.access = access;
                return Ok(cached.client.clone());
            }
        }
        let mut client =
            TcpWorldClient::with_pool_size(target.address, self.max_payload, self.pool_size)?
                .with_max_in_flight_per_connection(self.max_in_flight_per_connection)?;
        if let Some(token) = &self.authorization {
            client = client.with_internal_token(token.clone());
        }
        if let Some(tls) = &self.tls {
            client = client.with_tls(tls.clone());
        }
        let client = Arc::new(client);
        let mut clients = self
            .clients
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if let Some(cached) = clients.get_mut(&key) {
            cached.access = access;
            return Ok(cached.client.clone());
        }
        if clients.len() >= ROUTE_CLIENT_CACHE_CAPACITY
            && let Some(oldest) = clients
                .iter()
                .min_by_key(|(_, cached)| cached.access)
                .map(|(key, _)| key.clone())
        {
            clients.remove(&oldest);
        }
        clients.insert(
            key,
            CachedClient {
                client: client.clone(),
                access,
            },
        );
        Ok(client)
    }
}

fn select_target<'a>(
    targets: &'a [WorldRouteTarget],
    ownership: Option<&Assignment>,
    user_id: i64,
) -> Result<&'a WorldRouteTarget> {
    if let Some(ownership) = ownership {
        return targets
            .iter()
            .find(|target| target.world_id == ownership.world_id)
            .ok_or(Error::Unavailable);
    }
    targets
        .iter()
        .max_by_key(|target| {
            let mut hash = Sha256::new();
            hash.update(user_id.to_be_bytes());
            hash.update(target.world_id.as_bytes());
            let digest = hash.finalize();
            u64::from_be_bytes(digest[..8].try_into().expect("SHA-256 prefix"))
        })
        .ok_or(Error::Unavailable)
}

#[async_trait]
impl WorldClient for RouteWorldClient {
    async fn command(&self, request: WorldRequest) -> Result<bytes::Bytes> {
        let targets = self
            .directory
            .resolve(
                request.identity.region_id,
                request.identity.realm_id,
                request.route,
            )
            .await?;
        let target = select_target(
            &targets,
            request.ownership.as_ref(),
            request.identity.user_id,
        )?;
        self.client(target).await?.command(request).await
    }

    async fn readiness(&self) -> Result<()> {
        self.directory.readiness().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn exact_route_overrides_realm_default() {
        let directory = MemoryWorldRouteDirectory::new();
        directory
            .replace_targets(
                1,
                2,
                0,
                vec![WorldRouteTarget {
                    world_id: "default".into(),
                    address: "127.0.0.1:18000".parse().unwrap(),
                }],
            )
            .await
            .unwrap();
        directory
            .replace_targets(
                1,
                2,
                100,
                vec![WorldRouteTarget {
                    world_id: "battle".into(),
                    address: "127.0.0.1:18001".parse().unwrap(),
                }],
            )
            .await
            .unwrap();
        assert_eq!(
            directory.resolve(1, 2, 99).await.unwrap()[0].world_id,
            "default"
        );
        assert_eq!(
            directory.resolve(1, 2, 100).await.unwrap()[0].world_id,
            "battle"
        );
        directory
            .replace_targets(1, 2, 100, Vec::new())
            .await
            .unwrap();
        assert_eq!(
            directory.resolve(1, 2, 100).await.unwrap()[0].world_id,
            "default"
        );
    }

    #[tokio::test]
    async fn route_client_reuses_cached_connection_pool() {
        let directory = Arc::new(MemoryWorldRouteDirectory::new());
        let client = RouteWorldClient::new(directory, 1024, 1).unwrap();
        let target = WorldRouteTarget {
            world_id: "world-1".into(),
            address: "127.0.0.1:18000".parse().unwrap(),
        };
        let first = client.client(&target).await.unwrap();
        let second = client.client(&target).await.unwrap();
        assert!(Arc::ptr_eq(&first, &second));
    }

    #[test]
    fn route_group_keeps_a_player_on_one_world() {
        let targets = vec![
            WorldRouteTarget {
                world_id: "world-1".into(),
                address: "127.0.0.1:18000".parse().unwrap(),
            },
            WorldRouteTarget {
                world_id: "world-2".into(),
                address: "127.0.0.1:18001".parse().unwrap(),
            },
        ];
        let first = select_target(&targets, None, 42).unwrap();
        let second = select_target(&targets, None, 42).unwrap();
        assert_eq!(first, second);

        let ownership = Assignment {
            region_id: 1,
            realm_id: 1,
            shard_id: 0,
            world_id: "world-2".into(),
            epoch: 1,
        };
        assert_eq!(
            select_target(&targets, Some(&ownership), 42)
                .unwrap()
                .world_id,
            "world-2"
        );
    }

    #[test]
    fn adding_a_world_only_moves_players_to_the_new_world() {
        let two = vec![
            WorldRouteTarget {
                world_id: "world-1".into(),
                address: "127.0.0.1:18000".parse().unwrap(),
            },
            WorldRouteTarget {
                world_id: "world-2".into(),
                address: "127.0.0.1:18001".parse().unwrap(),
            },
        ];
        let mut three = two.clone();
        three.push(WorldRouteTarget {
            world_id: "world-3".into(),
            address: "127.0.0.1:18002".parse().unwrap(),
        });
        for user_id in 1..=1_000 {
            let before = select_target(&two, None, user_id).unwrap();
            let after = select_target(&three, None, user_id).unwrap();
            if before != after {
                assert_eq!(after.world_id, "world-3");
            }
        }
    }
}
