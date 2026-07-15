//! Shared process infrastructure for Elura server runtimes.

#![deny(rustdoc::broken_intra_doc_links)]

#[doc(hidden)]
pub mod internal;
pub mod launch;
pub mod lifecycle;
pub mod observability;

/// Security and TLS types for trusted service-to-service traffic.
pub mod security {
    pub use crate::internal::{
        ClientTlsConfig, InternalToken, ServerTlsConfig, TlsCertificateReloader,
    };
}
