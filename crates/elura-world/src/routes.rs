use prost::Message;
use serde::{Deserialize, Serialize};

/// Compile-time contract for an application route.
pub trait Route: Send + Sync + 'static {
    const ID: u32;
    const NAME: &'static str;

    type Request: Message + Default + Send + 'static;
    type Response: Message + Send + 'static;
}

/// Runtime route metadata exposed only through diagnostics and test tooling.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RouteInfo {
    pub id: u32,
    pub name: String,
}
