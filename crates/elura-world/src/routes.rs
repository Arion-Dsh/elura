use std::collections::{BTreeMap, HashSet};

use elura_core::protocol::FIRST_APPLICATION_ROUTE;
use elura_core::{Error, Result};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorldRoute {
    pub id: u32,
    pub name: String,
}

#[derive(Debug, Clone, Default)]
pub struct RouteCatalog {
    routes: BTreeMap<u32, WorldRoute>,
}

pub type RouteManifest = Vec<WorldRoute>;

impl RouteCatalog {
    pub fn new(routes: impl IntoIterator<Item = WorldRoute>) -> Result<Self> {
        let mut catalog = Self::default();
        let mut names = HashSet::new();
        for route in routes {
            if route.id < FIRST_APPLICATION_ROUTE || route.name.trim().is_empty() {
                return Err(Error::InvalidConfig(
                    "invalid World route catalog entry".into(),
                ));
            }
            if !names.insert(route.name.clone()) || catalog.routes.insert(route.id, route).is_some()
            {
                return Err(Error::InvalidConfig(
                    "duplicate World route ID or name".into(),
                ));
            }
        }
        if catalog.routes.is_empty() {
            return Err(Error::InvalidConfig("World route catalog is empty".into()));
        }
        Ok(catalog)
    }

    pub fn contains(&self, route: u32) -> bool {
        self.routes.contains_key(&route)
    }

    pub fn routes(&self) -> RouteManifest {
        self.routes.values().cloned().collect()
    }
}
