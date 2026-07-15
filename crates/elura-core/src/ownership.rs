use crate::{Error, Result};
use async_trait::async_trait;
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, RwLock};
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Assignment {
    pub region_id: u32,
    pub realm_id: u32,
    pub shard_id: u32,
    pub world_id: String,
    pub epoch: u64,
}
impl Assignment {
    pub fn validate(&self, count: u32) -> Result<()> {
        if count == 0
            || self.region_id == 0
            || self.realm_id == 0
            || self.shard_id >= count
            || self.world_id.is_empty()
            || self.epoch == 0
        {
            Err(Error::InvalidConfig("invalid shard assignment".into()))
        } else {
            Ok(())
        }
    }
}
pub fn shard_for(player: i64, count: u32) -> Result<u32> {
    if player <= 0 || count == 0 {
        return Err(Error::InvalidConfig("invalid shard input".into()));
    }
    let mut v = player as u64;
    v ^= v >> 33;
    v = v.wrapping_mul(0xff51afd7ed558ccd);
    v ^= v >> 33;
    v = v.wrapping_mul(0xc4ceb9fe1a85ec53);
    v ^= v >> 33;
    Ok((v % u64::from(count)) as u32)
}
pub fn preferred_world(shard: u32, worlds: &[String]) -> Result<String> {
    if worlds.is_empty() {
        return Err(Error::Unavailable);
    }
    let mut seen = HashSet::new();
    let mut best: Option<(&str, u64)> = None;
    for id in worlds {
        if id.is_empty() || !seen.insert(id) {
            return Err(Error::InvalidConfig("invalid world membership".into()));
        }
        let score = score(shard, id);
        if best.is_none_or(|(old, s)| score > s || score == s && id.as_str() < old) {
            best = Some((id, score));
        }
    }
    best.map(|(id, _)| id.into()).ok_or(Error::Unavailable)
}
fn score(shard: u32, id: &str) -> u64 {
    let mut h = 14695981039346656037u64;
    for b in shard.to_string().bytes().chain([0]).chain(id.bytes()) {
        h ^= u64::from(b);
        h = h.wrapping_mul(1099511628211)
    }
    h ^= h >> 33;
    h = h.wrapping_mul(0xff51afd7ed558ccd);
    h ^= h >> 33;
    h = h.wrapping_mul(0xc4ceb9fe1a85ec53);
    h ^ (h >> 33)
}
#[async_trait]
pub trait OwnershipResolver: Send + Sync {
    async fn resolve(&self, region_id: u32, realm_id: u32, shard: u32) -> Result<Assignment>;
}

#[derive(Default)]
pub struct OwnershipResolverDirectory {
    resolvers: RwLock<HashMap<(u32, u32), Arc<dyn OwnershipResolver>>>,
}

impl OwnershipResolverDirectory {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn replace(
        &self,
        resolvers: impl IntoIterator<Item = ((u32, u32), Arc<dyn OwnershipResolver>)>,
    ) -> Result<()> {
        let mut next = HashMap::new();
        for ((region_id, realm_id), resolver) in resolvers {
            if region_id == 0 || realm_id == 0 {
                return Err(Error::InvalidConfig("invalid ownership scope".into()));
            }
            if next.insert((region_id, realm_id), resolver).is_some() {
                return Err(Error::InvalidConfig("duplicate ownership scope".into()));
            }
        }
        if next.is_empty() {
            return Err(Error::InvalidConfig(
                "ownership resolver directory cannot be empty".into(),
            ));
        }
        *self
            .resolvers
            .write()
            .map_err(|_| Error::Internal("ownership directory poisoned".into()))? = next;
        Ok(())
    }
}

#[async_trait]
impl OwnershipResolver for OwnershipResolverDirectory {
    async fn resolve(&self, region_id: u32, realm_id: u32, shard: u32) -> Result<Assignment> {
        let resolver = self
            .resolvers
            .read()
            .map_err(|_| Error::Internal("ownership directory poisoned".into()))?
            .get(&(region_id, realm_id))
            .cloned()
            .ok_or(Error::Unavailable)?;
        resolver.resolve(region_id, realm_id, shard).await
    }
}
pub struct OwnershipTable {
    count: u32,
    owners: RwLock<HashMap<(u32, u32, u32), Assignment>>,
}
impl OwnershipTable {
    pub fn new(count: u32) -> Result<Self> {
        if count == 0 {
            return Err(Error::InvalidConfig("zero shards".into()));
        }
        Ok(Self {
            count,
            owners: RwLock::new(HashMap::new()),
        })
    }
    pub fn replace(&self, values: impl IntoIterator<Item = Assignment>) -> Result<()> {
        let mut next = HashMap::new();
        for a in values {
            a.validate(self.count)?;
            let key = (a.region_id, a.realm_id, a.shard_id);
            if next.insert(key, a).is_some() {
                return Err(Error::InvalidConfig("duplicate shard".into()));
            }
        }
        *self
            .owners
            .write()
            .map_err(|_| Error::Internal("ownership poisoned".into()))? = next;
        Ok(())
    }
    pub fn snapshot(&self) -> Result<Vec<Assignment>> {
        Ok(self
            .owners
            .read()
            .map_err(|_| Error::Internal("ownership poisoned".into()))?
            .values()
            .cloned()
            .collect())
    }
}
#[async_trait]
impl OwnershipResolver for OwnershipTable {
    async fn resolve(&self, region_id: u32, realm_id: u32, shard: u32) -> Result<Assignment> {
        if region_id == 0 || realm_id == 0 || shard >= self.count {
            return Err(Error::InvalidConfig("shard range".into()));
        }
        self.owners
            .read()
            .map_err(|_| Error::Internal("ownership poisoned".into()))?
            .get(&(region_id, realm_id, shard))
            .cloned()
            .ok_or(Error::Unavailable)
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn replacement_is_atomic() {
        let t = OwnershipTable::new(8).unwrap();
        t.replace([
            Assignment {
                region_id: 1,
                realm_id: 2,
                shard_id: 2,
                world_id: "a".into(),
                epoch: 3,
            },
            Assignment {
                region_id: 1,
                realm_id: 3,
                shard_id: 2,
                world_id: "b".into(),
                epoch: 4,
            },
        ])
        .unwrap();
        assert_eq!(t.resolve(1, 2, 2).await.unwrap().epoch, 3);
        assert_eq!(t.resolve(1, 3, 2).await.unwrap().world_id, "b");
        assert!(t.resolve(2, 3, 2).await.is_err());
    }

    #[tokio::test]
    async fn resolver_directory_dispatches_by_region_and_realm() {
        let table = Arc::new(OwnershipTable::new(8).unwrap());
        table
            .replace([Assignment {
                region_id: 2,
                realm_id: 3,
                shard_id: 4,
                world_id: "realm-3-world".into(),
                epoch: 5,
            }])
            .unwrap();
        let directory = OwnershipResolverDirectory::new();
        directory
            .replace([((2, 3), table as Arc<dyn OwnershipResolver>)])
            .unwrap();
        assert_eq!(
            directory.resolve(2, 3, 4).await.unwrap().world_id,
            "realm-3-world"
        );
        assert!(directory.resolve(2, 4, 4).await.is_err());
    }
    #[test]
    fn hashing_is_stable() {
        assert_eq!(shard_for(42, 256).unwrap(), shard_for(42, 256).unwrap());
        assert_eq!(
            preferred_world(3, &["a".into(), "b".into()]).unwrap(),
            preferred_world(3, &["a".into(), "b".into()]).unwrap()
        )
    }
}
