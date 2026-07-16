use prost::Message;
use serde::{Deserialize, Serialize};

/// Compile-time contract for an application route.
pub trait Route: Send + Sync + 'static {
    /// Stable application route ID. Values below 100 are reserved by ELR2.
    const ID: u32;
    /// Unique, non-empty name used by diagnostics and test tooling.
    const NAME: &'static str;

    /// Protobuf message decoded from the client request payload.
    type Request: Message + Default + Send + 'static;
    /// Protobuf message encoded into the successful response payload.
    type Response: Message + Default + Send + 'static;
}

/// Compile-time contract for an application Push event.
pub trait Event: Send + Sync + 'static {
    /// Stable application route ID. Values below 100 are reserved by ELR2.
    const ID: u32;

    /// Protobuf message encoded into the Push payload.
    type Message: Message + Send + 'static;
}

/// Runtime route metadata exposed only through diagnostics and test tooling.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RouteInfo {
    /// Numeric ELR2 application route ID.
    pub id: u32,
    /// Diagnostic route name, or an empty string for a raw route.
    pub name: String,
}
