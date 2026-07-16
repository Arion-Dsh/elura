//! Application-facing API for Elura.
//!
//! The crate root contains only the framework error types. Runtime components,
//! extension contracts and infrastructure integrations live in their domain
//! modules.

#![deny(rustdoc::broken_intra_doc_links)]
#![deny(missing_docs)]

#[cfg(feature = "core")]
pub use elura_core::{Error, Result};

/// Low-level protocol and domain primitives.
#[cfg(feature = "core")]
pub mod core {
    pub use elura_core::{
        account_version, gateway_world, identity, online, otp, outbox, ownership, protocol, push,
        rate_limit, realm_gateway, realtime, replay, replication, session, state_hash, ticket,
    };
}

/// Common application types and extension traits for opt-in glob imports.
///
/// Concrete adapters and provider implementations remain in their domain
/// modules so their infrastructure dependencies stay visible at call sites.
pub mod prelude {
    #[cfg(feature = "core")]
    pub use crate::{Error, Result};
    #[cfg(feature = "core")]
    pub use elura_core::account_version::{
        AccountVersionKey, AccountVersionStore, MutableAccountVersionStore,
    };
    #[cfg(feature = "core")]
    pub use elura_core::online::{
        DuplicateLoginMode, OnlineBackend, OnlineDirectory, OnlineStats, OnlineStatsReader,
    };
    #[cfg(feature = "core")]
    pub use elura_core::otp::OtpStore;
    #[cfg(feature = "core")]
    pub use elura_core::outbox::{DeadLetter, OutboxDelivery, OutboxEvent, OutboxStore};
    #[cfg(feature = "core")]
    pub use elura_core::ownership::OwnershipResolver;
    #[cfg(feature = "core")]
    pub use elura_core::push::{
        PushHandler, PushReceipt, PushRequest, PushTarget, PushTargetResolver, PushTransport,
    };
    #[cfg(feature = "core")]
    pub use elura_core::session::{
        Identity, PlayerKey, SessionControlHandler, SessionControlTransport,
    };
    #[cfg(feature = "core")]
    pub use elura_core::ticket::{ReplayStore, TicketPurpose, TicketService};

    #[cfg(feature = "core")]
    pub use crate::discovery::{
        GatewayWorldRoutingConfig, WorldDiscovery, WorldRegistrar, WorldRegistration,
    };
    #[cfg(feature = "gateway")]
    pub use crate::gateway::observability::{AdmissionAdmin, GatewayAdmin};
    #[cfg(feature = "gateway")]
    pub use crate::gateway::{
        Gateway, GatewayConfig, GatewayInterceptContext, GatewayInterceptor, GatewayNext,
        GatewayRealmAdmissionConfig, GatewayRequest, GatewayResponse, GatewayServer,
        GatewayStatsSnapshot, GatewayTicketConfig, GatewayWorldTlsConfig, ReconnectTicketRequest,
        ReconnectTicketResponse, RouteRateLimit, TcpWorldClient, WorldClient, WorldRequest,
        WorldRouteTarget, WorldRouteUpdater,
    };
    #[cfg(feature = "runtime")]
    pub use crate::launch::ServerTlsFilesConfig;
    #[cfg(feature = "monolith")]
    pub use crate::monolith::{Monolith, MonolithServer};
    #[cfg(feature = "runtime")]
    pub use crate::observability::{
        AdminDiagnostics, AdminServer, AdminServerConfig, PrometheusText, Readiness, ReadinessProbe,
    };
    #[cfg(feature = "gateway")]
    pub use crate::protection::{
        BackendProtector, CircuitState, ProtectionConfig, ProtectionStats,
    };
    #[cfg(feature = "runtime")]
    pub use crate::security::{
        ClientTlsConfig, InternalToken, ServerTlsConfig, TlsCertificateReloader,
    };
    #[cfg(feature = "gateway")]
    pub use crate::transport::{
        AccountVersionSettings, AdmissionController, AdmissionDecision, AdmissionRejection,
        AdmissionRequest, AdmissionSettings, AdmissionStage, GatewayTransport,
        GatewayTransportListener, ProxyProtocolConfig, QuicConfig, RealmAdmission, SessionEvent,
        SessionEventKind, SessionObserver, TcpConfig, TcpProxyProtocolConfig, TcpTransport,
        TrustedProxies, WebSocketConfig,
    };
    #[cfg(feature = "world")]
    pub use crate::world::player::{InvalidationBus, InvalidationHandler, PlayerLoader};
    #[cfg(feature = "world")]
    pub use crate::world::{
        Event, InProcessWorldClient, Next, Route, World, WorldConfig, WorldContext,
        WorldDiagnostics, WorldHandler, WorldMiddleware, WorldModule, WorldServer,
        WorldStatsSnapshot,
    };

    #[cfg(feature = "runtime")]
    pub use elura_runtime::outbox::{EventHandler, IdempotencyStore};

    #[cfg(any(feature = "identity", feature = "notification-alisms", feature = "otp"))]
    pub use crate::providers::identity::{
        IdentityBindingStore, IdentityProvider, OtpSender, OtpVerifier, PasswordCredentialStore,
    };
    #[cfg(feature = "providers")]
    pub use crate::providers::payment::PaymentProvider;
    #[cfg(feature = "providers")]
    pub use crate::providers::{ProviderError, ProviderName, ProviderResult};
}

/// World runtime APIs grouped by responsibility.
#[cfg(feature = "world")]
pub mod world {
    pub use elura_world::{
        Event, InProcessWorldClient, Next, Route, World, WorldConfig, WorldContext,
        WorldDiagnostics, WorldHandler, WorldMiddleware, WorldModule, WorldModuleRegistry,
        WorldServer, WorldStatsSnapshot,
    };

    /// Middleware contracts and built-in middleware.
    pub mod middleware {
        pub use elura_world::LoggingMiddleware;

        /// Transaction support for unit-of-work middleware.
        pub mod transaction {
            pub use elura_world::{
                TransactionFactory, TransactionGuard, UnitOfWorkMiddleware, WorldTransaction,
            };
        }
    }

    /// Player-state loading, caching and invalidation middleware.
    pub mod player {
        pub use elura_world::player::*;
    }

    /// Data returned by World diagnostics.
    pub mod diagnostics {
        pub use elura_world::RouteInfo;
    }

    /// In-process World test harness.
    pub mod testing {
        pub use elura_world::WorldHarness;
    }
}

/// Advanced runtime APIs grouped by responsibility. These are intentionally
/// not flattened into the crate root used by World applications.
#[cfg(feature = "core")]
pub use elura_core::gateway_world as discovery;

/// Transactional outbox contracts and dispatch runtime.
#[cfg(feature = "runtime")]
pub mod outbox {
    pub use elura_core::outbox::*;
    pub use elura_runtime::outbox::*;
}

#[cfg(feature = "runtime")]
pub use elura_runtime::{launch, lifecycle, observability, security};

#[cfg(feature = "gateway")]
pub use elura_gateway::{self as gateway, protection, transport};

#[cfg(feature = "monolith")]
pub use elura_monolith as monolith;

/// Redis, SQL, Kubernetes and other infrastructure implementations.
#[cfg(feature = "adapters")]
pub use elura_adapters as adapters;

/// Identity, notification, OTP and payment integrations for upper applications.
#[cfg(feature = "providers")]
pub mod providers {
    pub use elura_providers::{ProviderError, ProviderName, ProviderResult, payment};

    #[cfg(any(feature = "identity", feature = "notification-alisms", feature = "otp"))]
    pub use elura_providers::identity;
    #[cfg(feature = "notification-alisms")]
    pub use elura_providers::notification;
    #[cfg(feature = "otp")]
    pub use elura_providers::otp;
}
