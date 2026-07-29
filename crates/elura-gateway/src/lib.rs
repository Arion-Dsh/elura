//! Client-facing Gateway runtime.

#![deny(rustdoc::broken_intra_doc_links)]

mod builder;
mod client_protocol;
mod config;
mod connection;
pub mod discovery;
mod gateway;
mod http_auth;
mod interceptor;
pub mod observability;
pub mod presence;
pub mod protection;
mod routing;
mod server;
mod session_state;
mod stats;
pub mod transport;
mod world_client;

/// HTTP login, bearer-token, and game-session authentication contracts.
pub mod auth {
    pub use crate::http_auth::{
        AuthenticatedHttp, GAME_CONNECT_SCOPE, GameSessionTicketRequest, GatewaySessionTicket,
        HttpAuthApi, HttpAuthErrorResponse, HttpAuthRejection, HttpBearerAuth, HttpLoginBackend,
        HttpLoginGrant, HttpLoginRequest, HttpLoginResponse, HttpRefreshRequest, require_bearer,
    };
    pub use elura_core::http_auth::{
        HttpTokenClaims, HttpTokenPair, HttpTokenPurpose, HttpTokenService,
    };

    /// Identity-provider bridge for the HTTP authentication API.
    #[cfg(feature = "identity-http")]
    pub mod identity {
        pub use crate::http_auth::identity::{IdentityHttpBackend, IdentityHttpPolicy};
    }
}

/// Connection and authenticated-session admission contracts.
pub mod admission {
    pub use crate::observability::AdmissionAdmin;
    pub use crate::transport::{
        AdmissionController, AdmissionDecision, AdmissionRejection, AdmissionRequest,
        AdmissionSettings, AdmissionStage, RealmAdmission,
    };
}

/// Account-version and cross-Gateway session-control contracts.
pub mod session {
    pub use crate::transport::{
        AccountVersionSettings, SessionEvent, SessionEventKind, SessionObserver,
    };
    pub use elura_core::account_version::{
        AccountVersionKey, AccountVersionStore, MutableAccountVersionStore,
    };
    pub use elura_core::session::{
        SessionControlEvent, SessionControlHandler, SessionControlKind, SessionControlTransport,
    };
}

/// Session-ticket issuance contracts.
pub mod ticket {
    pub use elura_core::ticket::{TicketPurpose, TicketService};
}

pub use gateway::Gateway;
pub use interceptor::{
    GatewayInterceptContext, GatewayInterceptor, GatewayNext, GatewayRequest, GatewayResponse,
};

pub use builder::{
    GatewayInfrastructure, GatewayOnlineConfig, GatewayRealmAdmissionConfig, GatewayTicketConfig,
    GatewayWorldTlsConfig, RealmCapacityLimit,
};
pub use client_protocol::{ReconnectTicketRequest, ReconnectTicketResponse};
pub use config::{GatewayConfig, RouteRateLimit};
pub(crate) use routing::{MemoryWorldRouteDirectory, RouteWorldClient};
pub use server::GatewayServer;
pub use stats::GatewayStatsSnapshot;
pub use world_client::TcpWorldClient;
pub(crate) use world_client::{WORLD_CONNECTION_IN_FLIGHT, validate_world_connection_in_flight};
