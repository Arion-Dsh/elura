//! Security and stream primitives for trusted service-to-service traffic.

mod auth;
mod tls;

use tokio::io::{AsyncRead, AsyncWrite};

pub use auth::InternalToken;
pub use tls::{ClientTlsConfig, ServerTlsConfig, TlsCertificateReloader};

#[doc(hidden)]
pub trait InternalStream: AsyncRead + AsyncWrite + Unpin + Send {}

impl<T> InternalStream for T where T: AsyncRead + AsyncWrite + Unpin + Send {}

#[doc(hidden)]
pub type BoxedInternalStream = Box<dyn InternalStream>;
