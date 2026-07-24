//! Composable World command runtime.

#![deny(rustdoc::broken_intra_doc_links)]

mod config;
mod context;
mod handler;
mod keyed;
mod middleware;
mod module;
mod observability;
pub mod player;
pub mod registration;
mod routes;
mod runtime;
pub mod scene;
mod server;
mod stats;
mod testing;
mod world;

/// Typed business routes and client-facing events.
pub mod route {
    pub use crate::{Event, Route, RouteInfo};
}

pub use config::WorldConfig;
pub use context::{ContextKey, TransactionGuard, WorldContext};
use elura_core::gateway_world::WorldCommand;
pub use handler::WorldHandler;
pub use middleware::{
    LoggingMiddleware, Next, TransactionFactory, UnitOfWorkMiddleware, WorldMiddleware,
    WorldTransaction,
};
pub use module::{WorldModule, WorldModuleRegistry};
pub use routes::{Event, Route, RouteInfo};
pub use server::{InProcessWorldClient, WorldDiagnostics, WorldServer};
pub use stats::WorldStatsSnapshot;
pub use testing::{WorldHarness, WorldTestClient, test_identity};
pub use world::World;
