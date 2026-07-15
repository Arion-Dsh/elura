//! Kubernetes discovery and distributed shard ownership.

mod endpoints;
mod leader;
mod ownership;

pub use endpoints::{
    EndpointDiscovery, EndpointWatcher, EndpointWatcherConfig, EndpointWatcherStats,
    targets_from_slices,
};
pub use leader::{LeaderElectionConfig, LeadershipError, run_leader_elected};
pub use ownership::{
    OwnershipCoordinator, OwnershipCoordinatorConfig, OwnershipObserver, OwnershipObserverConfig,
    assignments_from_leases,
};

fn kube_error(error: kube::Error) -> elura_core::Error {
    elura_core::Error::Internal(format!("kubernetes: {error}"))
}
