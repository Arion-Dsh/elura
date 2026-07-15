use async_trait::async_trait;
use elura_core::Result;

use super::WorldBuilder;

#[async_trait]
pub trait WorldModule: Send + Sync + 'static {
    fn name(&self) -> &str;
    fn register(&self, builder: &mut WorldBuilder) -> Result<()>;
    async fn start(&self) -> Result<()> {
        Ok(())
    }
    async fn stop(&self) -> Result<()> {
        Ok(())
    }
}
