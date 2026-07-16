//! Security and stream primitives for trusted service-to-service traffic.

#![deny(missing_docs)]

mod auth;
mod tls;

use tokio::io::{AsyncRead, AsyncWrite};

pub use auth::InternalToken;
pub use tls::{ClientTlsConfig, ServerTlsConfig, TlsCertificateReloader};

/// Async byte stream used for trusted service-to-service connections.
pub trait ServiceStream: AsyncRead + AsyncWrite + Unpin + Send {}

impl<T> ServiceStream for T where T: AsyncRead + AsyncWrite + Unpin + Send {}

/// Type-erased trusted service-to-service stream.
pub type BoxedServiceStream = Box<dyn ServiceStream>;
