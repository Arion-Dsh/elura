use async_trait::async_trait;
use bytes::Bytes;
use elura_core::Result;

use super::WorldContext;

#[async_trait]
pub trait WorldHandler: Send + Sync + 'static {
    async fn handle(&self, context: WorldContext, payload: Bytes) -> Result<Bytes>;
}

#[async_trait]
impl<F, Fut> WorldHandler for F
where
    F: Send + Sync + 'static + Fn(WorldContext, Bytes) -> Fut,
    Fut: Send + 'static + std::future::Future<Output = Result<Bytes>>,
{
    async fn handle(&self, context: WorldContext, payload: Bytes) -> Result<Bytes> {
        self(context, payload).await
    }
}
