use std::net::SocketAddr;
use std::path::PathBuf;

use crate::internal::ServerTlsConfig;
use crate::observability::AdminServerConfig;
use elura_core::Result;
use serde::{Deserialize, Serialize};

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LaunchAdminConfig {
    pub listen: SocketAddr,
    #[serde(skip)]
    pub token: Option<String>,
    pub component: String,
    pub instance_id: String,
}

impl From<LaunchAdminConfig> for AdminServerConfig {
    fn from(config: LaunchAdminConfig) -> Self {
        Self {
            listen: config.listen,
            token: config.token,
            component: config.component,
            instance_id: config.instance_id,
        }
    }
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ServerTlsFilesConfig {
    pub certificate_file: PathBuf,
    pub key_file: PathBuf,
    pub client_ca_file: Option<PathBuf>,
}

impl ServerTlsFilesConfig {
    #[doc(hidden)]
    pub fn build(self) -> Result<ServerTlsConfig> {
        ServerTlsConfig::from_pem_files(self.certificate_file, self.key_file, self.client_ca_file)
    }
}
