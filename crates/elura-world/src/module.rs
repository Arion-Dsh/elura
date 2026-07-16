use async_trait::async_trait;
use elura_core::Result;

use super::runtime::WorldBuilder;
use super::{Route, WorldContext, WorldHandler, WorldMiddleware};

/// Route and middleware registration surface available to a [`WorldModule`].
///
/// The runtime builder remains internal; modules only receive the operations
/// needed to mount their business functionality.
pub struct WorldModuleRegistry<'a> {
    pub(crate) builder: &'a mut WorldBuilder,
}

impl WorldModuleRegistry<'_> {
    pub fn route_raw(&mut self, route: u32, handler: impl WorldHandler) -> Result<&mut Self> {
        self.builder.register_raw(route, handler)?;
        Ok(self)
    }

    pub fn route<E, F, Fut>(&mut self, route: E, handler: F) -> Result<&mut Self>
    where
        E: Route,
        F: Fn(WorldContext, E::Request) -> Fut + Send + Sync + 'static,
        Fut: std::future::Future<Output = Result<E::Response>> + Send + 'static,
    {
        self.builder.register(route, handler)?;
        Ok(self)
    }

    pub fn middleware<M>(&mut self, middleware: M) -> Result<&mut Self>
    where
        M: WorldMiddleware,
    {
        self.builder
            .use_middleware(std::sync::Arc::new(middleware))?;
        Ok(self)
    }

    pub fn route_middleware<E, M>(&mut self, route: E, middleware: M) -> Result<&mut Self>
    where
        E: Route,
        M: WorldMiddleware,
    {
        self.builder
            .use_route_middleware(route, std::sync::Arc::new(middleware))?;
        Ok(self)
    }
}

#[async_trait]
pub trait WorldModule: Send + Sync + 'static {
    fn name(&self) -> &str;
    fn register(&self, world: &mut WorldModuleRegistry<'_>) -> Result<()>;
    async fn start(&self) -> Result<()> {
        Ok(())
    }
    async fn stop(&self) -> Result<()> {
        Ok(())
    }
}
