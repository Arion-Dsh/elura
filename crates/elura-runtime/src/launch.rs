//! Serializable configuration for process administration and TLS listeners.

use std::path::PathBuf;

use crate::security::ServerTlsConfig;
use elura_core::Result;
use serde::{Deserialize, Serialize};

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[non_exhaustive]
/// PEM file paths used to construct an inbound TLS listener.
pub struct ServerTlsFilesConfig {
    /// PEM certificate chain presented to clients.
    pub certificate_file: PathBuf,
    /// PEM private key corresponding to the server certificate.
    pub key_file: PathBuf,
    /// Optional client CA bundle that enables mutual TLS.
    pub client_ca_file: Option<PathBuf>,
}

impl ServerTlsFilesConfig {
    /// Creates file-backed server TLS configuration without client authentication.
    pub fn new(certificate_file: impl Into<PathBuf>, key_file: impl Into<PathBuf>) -> Self {
        Self {
            certificate_file: certificate_file.into(),
            key_file: key_file.into(),
            client_ca_file: None,
        }
    }

    #[doc(hidden)]
    pub fn build(self) -> Result<ServerTlsConfig> {
        ServerTlsConfig::from_pem_files(self.certificate_file, self.key_file, self.client_ca_file)
    }
}
