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
        account_version, gateway_world, http_auth, identity, otp, outbox, ownership, protocol,
        push, rate_limit, realm_gateway, realtime, replay, replay_protection, session,
        snapshot_replication, state_hash, ticket,
    };
}

/// High-frequency application types and game-facing extension points.
///
/// Capability-specific contracts remain in their owning domain modules. This
/// keeps glob imports focused on APIs used while writing game behavior.
pub mod prelude {
    #[cfg(feature = "gateway")]
    pub use crate::gateway::presence::{
        DuplicateLoginMode, OnlineAdmission, OnlineAdmissionPolicy, OnlineStats,
    };
    #[cfg(feature = "gateway")]
    pub use crate::gateway::ticket::{TicketPurpose, TicketService};
    #[cfg(feature = "core")]
    pub use crate::{Error, Result};
    #[cfg(feature = "core")]
    pub use elura_core::outbox::OutboxEvent;
    #[cfg(feature = "core")]
    pub use elura_core::push::{PushReceipt, PushRequest, PushTarget};
    #[cfg(feature = "core")]
    pub use elura_core::realtime::AuthoritativeSimulation;
    #[cfg(feature = "core")]
    pub use elura_core::session::{Identity, PlayerKey};

    #[cfg(feature = "gateway")]
    pub use crate::gateway::admission::{
        AdmissionController, AdmissionDecision, AdmissionRejection, AdmissionRequest,
        AdmissionSettings, AdmissionStage, RealmAdmission,
    };
    #[cfg(all(feature = "gateway", feature = "identity"))]
    pub use crate::gateway::auth::identity::{IdentityHttpBackend, IdentityHttpPolicy};
    #[cfg(feature = "gateway")]
    pub use crate::gateway::auth::{
        AuthenticatedHttp, GAME_CONNECT_SCOPE, GameSessionTicketRequest, GatewaySessionTicket,
        HttpAuthApi, HttpAuthErrorResponse, HttpAuthRejection, HttpBearerAuth, HttpLoginBackend,
        HttpLoginGrant, HttpLoginRequest, HttpLoginResponse, HttpRefreshRequest, HttpTokenClaims,
        HttpTokenPair, HttpTokenPurpose, HttpTokenService, require_bearer,
    };
    #[cfg(feature = "gateway")]
    pub use crate::gateway::protection::{
        BackendProtector, CircuitState, ProtectionConfig, ProtectionStats,
    };
    #[cfg(feature = "gateway")]
    pub use crate::gateway::session::{
        AccountVersionSettings, SessionEvent, SessionEventKind, SessionObserver,
    };
    #[cfg(feature = "gateway")]
    pub use crate::gateway::transport::{
        ProxyProtocolConfig, QuicConfig, QuicMode, TcpConfig, TcpProxyProtocolConfig, TcpTransport,
        TransportSocketKind, TrustedProxies, UdpConfig, WebSocketConfig, WebTransportConfig,
        WebTransportMode,
    };
    #[cfg(feature = "gateway")]
    pub use crate::gateway::{
        Gateway, GatewayConfig, GatewayInterceptContext, GatewayInterceptor, GatewayNext,
        GatewayOnlineConfig, GatewayRealmAdmissionConfig, GatewayRequest, GatewayResponse,
        GatewayServer, GatewayStatsSnapshot, GatewayTicketConfig, GatewayWorldTlsConfig,
        RealmCapacityLimit, ReconnectTicketRequest, ReconnectTicketResponse, RouteRateLimit,
        TcpWorldClient,
    };
    #[cfg(feature = "runtime")]
    pub use crate::launch::ServerTlsFilesConfig;
    #[cfg(feature = "monolith")]
    pub use crate::monolith::{Monolith, MonolithServer};
    #[cfg(feature = "runtime")]
    pub use crate::observability::{AdminServer, AdminServerConfig, PrometheusText, Readiness};
    #[cfg(feature = "runtime")]
    pub use crate::security::{
        ClientTlsConfig, InternalToken, ServerTlsConfig, TlsCertificateReloader,
    };
    #[cfg(feature = "world")]
    pub use crate::world::player::PlayerLoader;
    #[cfg(feature = "world")]
    pub use crate::world::{
        Event, InProcessWorldClient, Next, Route, World, WorldConfig, WorldContext,
        WorldDiagnostics, WorldMiddleware, WorldModule, WorldServer, WorldStatsSnapshot,
    };

    #[cfg(feature = "identity")]
    pub use crate::providers::identity::IdentityProvider;
    #[cfg(feature = "providers")]
    pub use crate::providers::payment::PaymentProvider;
    #[cfg(feature = "providers")]
    pub use crate::providers::{ProviderError, ProviderName, ProviderResult};

    #[cfg(feature = "simulation")]
    pub use crate::gameplay::simulation::FixedStepSimulation;
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
        pub use elura_world::player::{
            CachedPlayerLoader, InvalidationBus, InvalidationHandler, PlayerCache,
            PlayerCacheConfig, PlayerCacheStats, PlayerCacheSynchronizer, PlayerInvalidation,
            PlayerLoader, PlayerSnapshot, PlayerStateMiddleware,
        };
    }

    /// Actor-style lifecycle and serial command execution for stateful game scenes.
    pub mod scene {
        pub use elura_world::scene::{
            Scene, SceneCommand, SceneError, SceneResult, SceneRuntime, SceneRuntimeConfig,
        };
    }

    /// Data returned by World diagnostics.
    pub mod diagnostics {
        pub use elura_world::RouteInfo;
    }

    /// Typed business routes and client-facing events.
    pub mod route {
        pub use elura_world::route::{Event, Route, RouteInfo};
    }

    /// In-process World test harness.
    pub mod testing {
        pub use elura_world::{WorldHarness, WorldTestClient, test_identity};
    }

    /// World-side service registration contracts.
    pub mod registration {
        pub use elura_world::registration::{WorldRegistrar, WorldRegistration};
    }
}

/// Transactional outbox contracts and dispatch runtime.
#[cfg(feature = "core")]
pub mod outbox {
    pub use elura_core::outbox::{
        DeadLetter, MemoryOutbox, OutboxDelivery, OutboxEvent, OutboxStore, validate_failure_reason,
    };
    #[cfg(feature = "runtime")]
    pub use elura_runtime::outbox::{
        Dispatcher, DispatcherConfig, DispatcherStats, EventHandler, IdempotencyStore,
        MemoryIdempotencyStore,
    };
}

/// Cross-domain single-use replay protection.
#[cfg(feature = "core")]
pub mod replay_protection {
    pub use elura_core::replay_protection::{MemoryReplayProtectionStore, ReplayProtectionStore};
}

#[cfg(feature = "runtime")]
pub use elura_runtime::{launch, lifecycle, observability, security};

#[cfg(feature = "gateway")]
pub use elura_gateway as gateway;

#[cfg(feature = "monolith")]
pub use elura_monolith as monolith;

/// Cross-process push delivery contracts.
#[cfg(feature = "core")]
pub use elura_core::push;

/// Shard-ownership resolution contracts.
#[cfg(feature = "core")]
pub use elura_core::ownership;

/// Gameplay state and networking primitives.
pub mod gameplay {
    /// Application-owned room roster and lifecycle primitives.
    #[cfg(feature = "room")]
    pub use elura_room as room;

    /// Generic two-dimensional area-of-interest indexing.
    #[cfg(feature = "aoi")]
    pub use elura_aoi as aoi;

    /// Deterministic fixed-step simulation timing primitives.
    #[cfg(feature = "simulation")]
    pub use elura_simulation as simulation;

    /// Tick synchronization and redundant input delivery primitives.
    #[cfg(feature = "netcode")]
    pub use elura_netcode as netcode;

    /// Per-observer entity visibility and state replication primitives.
    #[cfg(feature = "replication")]
    pub use elura_replication as replication;

    /// Server-side bounded historical state and lag-compensated query primitives.
    #[cfg(feature = "lag-compensation")]
    pub use elura_lag_compensation as lag_compensation;

    /// Deterministic adverse network simulation for tests and local development.
    #[cfg(feature = "net-sim")]
    pub use elura_net_sim as net_sim;
}

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
