use std::fs::File;
use std::io::{self, BufReader};
use std::path::Path;
use std::path::PathBuf;
use std::sync::{Arc, RwLock};

use elura_core::{Error, Result};
use rustls::pki_types::{CertificateDer, PrivateKeyDer, ServerName};
use rustls::server::{ClientHello, ResolvesServerCert};
use rustls::{ClientConfig, RootCertStore, ServerConfig, SignatureScheme};
use tokio::net::TcpStream;
use tokio_rustls::{TlsAcceptor, TlsConnector};

use super::BoxedInternalStream;

#[derive(Clone)]
pub struct ClientTlsConfig {
    connector: TlsConnector,
    server_name: ServerName<'static>,
}

impl ClientTlsConfig {
    /// Builds a TLS 1.2+ client. `ca_file=None` uses the WebPKI root set.
    /// A client certificate and key enable mutual TLS and must be supplied together.
    pub fn from_pem_files(
        ca_file: Option<impl AsRef<Path>>,
        client_certificate: Option<impl AsRef<Path>>,
        client_key: Option<impl AsRef<Path>>,
        server_name: impl Into<String>,
    ) -> Result<Self> {
        if client_certificate.is_some() != client_key.is_some() {
            return Err(Error::InvalidConfig(
                "internal TLS client certificate and key must be provided together".into(),
            ));
        }
        let mut roots = RootCertStore::empty();
        if let Some(path) = ca_file {
            for certificate in load_certificates(path.as_ref())? {
                roots.add(certificate).map_err(|error| {
                    Error::InvalidConfig(format!("invalid internal TLS CA: {error}"))
                })?;
            }
        } else {
            roots.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
        }
        if roots.is_empty() {
            return Err(Error::InvalidConfig(
                "internal TLS trust store contains no certificates".into(),
            ));
        }

        let builder = ClientConfig::builder().with_root_certificates(roots);
        let config = match (client_certificate, client_key) {
            (Some(certificate), Some(key)) => builder
                .with_client_auth_cert(
                    load_certificates(certificate.as_ref())?,
                    load_private_key(key.as_ref())?,
                )
                .map_err(|error| {
                    Error::InvalidConfig(format!("invalid internal TLS client identity: {error}"))
                })?,
            (None, None) => builder.with_no_client_auth(),
            _ => unreachable!("pair validated above"),
        };
        let server_name = ServerName::try_from(server_name.into())
            .map_err(|_| Error::InvalidConfig("invalid internal TLS server name".into()))?;
        Ok(Self {
            connector: TlsConnector::from(Arc::new(config)),
            server_name,
        })
    }

    pub fn from_reloader(
        ca_file: Option<impl AsRef<Path>>,
        reloader: Arc<TlsCertificateReloader>,
        server_name: impl Into<String>,
    ) -> Result<Self> {
        let roots = load_roots(ca_file)?;
        let config = ClientConfig::builder()
            .with_root_certificates(roots)
            .with_client_cert_resolver(reloader);
        let server_name = ServerName::try_from(server_name.into())
            .map_err(|_| Error::InvalidConfig("invalid internal TLS server name".into()))?;
        Ok(Self {
            connector: TlsConnector::from(Arc::new(config)),
            server_name,
        })
    }

    #[doc(hidden)]
    pub async fn connect(&self, stream: TcpStream) -> Result<BoxedInternalStream> {
        let stream = self
            .connector
            .connect(self.server_name.clone(), stream)
            .await
            .map_err(|error| Error::Io(io::Error::other(error)))?;
        Ok(Box::new(stream))
    }
}

#[derive(Clone)]
pub struct ServerTlsConfig {
    acceptor: TlsAcceptor,
}

impl ServerTlsConfig {
    /// Builds a TLS 1.2+ server. Supplying `client_ca_file` requires a valid
    /// client certificate and therefore enables mutual TLS.
    pub fn from_pem_files(
        certificate_file: impl AsRef<Path>,
        key_file: impl AsRef<Path>,
        client_ca_file: Option<impl AsRef<Path>>,
    ) -> Result<Self> {
        let certificates = load_certificates(certificate_file.as_ref())?;
        let key = load_private_key(key_file.as_ref())?;
        let builder = ServerConfig::builder();
        let config = if let Some(path) = client_ca_file {
            let mut roots = RootCertStore::empty();
            for certificate in load_certificates(path.as_ref())? {
                roots.add(certificate).map_err(|error| {
                    Error::InvalidConfig(format!("invalid internal TLS client CA: {error}"))
                })?;
            }
            let verifier = rustls::server::WebPkiClientVerifier::builder(Arc::new(roots))
                .build()
                .map_err(|error| {
                    Error::InvalidConfig(format!("invalid internal TLS client CA: {error}"))
                })?;
            builder
                .with_client_cert_verifier(verifier)
                .with_single_cert(certificates, key)
        } else {
            builder
                .with_no_client_auth()
                .with_single_cert(certificates, key)
        }
        .map_err(|error| {
            Error::InvalidConfig(format!("invalid internal TLS server identity: {error}"))
        })?;
        Ok(Self {
            acceptor: TlsAcceptor::from(Arc::new(config)),
        })
    }

    pub fn from_reloader(
        reloader: Arc<TlsCertificateReloader>,
        client_ca_file: Option<impl AsRef<Path>>,
    ) -> Result<Self> {
        let builder = ServerConfig::builder();
        let config = if let Some(path) = client_ca_file {
            let roots = load_roots(Some(path))?;
            let verifier = rustls::server::WebPkiClientVerifier::builder(Arc::new(roots))
                .build()
                .map_err(|error| {
                    Error::InvalidConfig(format!("invalid internal TLS client CA: {error}"))
                })?;
            builder
                .with_client_cert_verifier(verifier)
                .with_cert_resolver(reloader)
        } else {
            builder.with_no_client_auth().with_cert_resolver(reloader)
        };
        Ok(Self {
            acceptor: TlsAcceptor::from(Arc::new(config)),
        })
    }

    #[doc(hidden)]
    pub async fn accept(&self, stream: TcpStream) -> Result<BoxedInternalStream> {
        let stream = self
            .acceptor
            .accept(stream)
            .await
            .map_err(|error| Error::Io(io::Error::other(error)))?;
        Ok(Box::new(stream))
    }
}

#[derive(Debug)]
pub struct TlsCertificateReloader {
    certificate_file: PathBuf,
    key_file: PathBuf,
    current: RwLock<Arc<rustls::sign::CertifiedKey>>,
}

impl TlsCertificateReloader {
    pub fn new(certificate_file: impl Into<PathBuf>, key_file: impl Into<PathBuf>) -> Result<Self> {
        let certificate_file = certificate_file.into();
        let key_file = key_file.into();
        let current = load_certified_key(&certificate_file, &key_file)?;
        Ok(Self {
            certificate_file,
            key_file,
            current: RwLock::new(current),
        })
    }

    pub fn reload(&self) -> Result<()> {
        let next = load_certified_key(&self.certificate_file, &self.key_file)?;
        *self
            .current
            .write()
            .map_err(|_| Error::Internal("TLS certificate reload lock poisoned".into()))? = next;
        Ok(())
    }

    fn certificate(&self) -> Option<Arc<rustls::sign::CertifiedKey>> {
        self.current.read().ok().map(|current| current.clone())
    }
}

impl ResolvesServerCert for TlsCertificateReloader {
    fn resolve(&self, _client_hello: ClientHello<'_>) -> Option<Arc<rustls::sign::CertifiedKey>> {
        self.certificate()
    }
}

impl rustls::client::ResolvesClientCert for TlsCertificateReloader {
    fn resolve(
        &self,
        _root_hint_subjects: &[&[u8]],
        _sigschemes: &[SignatureScheme],
    ) -> Option<Arc<rustls::sign::CertifiedKey>> {
        self.certificate()
    }

    fn has_certs(&self) -> bool {
        self.certificate().is_some()
    }
}

fn load_certified_key(
    certificate_file: &Path,
    key_file: &Path,
) -> Result<Arc<rustls::sign::CertifiedKey>> {
    let certificates = load_certificates(certificate_file)?;
    let key = load_private_key(key_file)?;
    let provider = rustls::crypto::ring::default_provider();
    rustls::sign::CertifiedKey::from_der(certificates, key, &provider)
        .map(Arc::new)
        .map_err(|error| Error::InvalidConfig(format!("invalid reloadable TLS identity: {error}")))
}

fn load_roots(path: Option<impl AsRef<Path>>) -> Result<RootCertStore> {
    let mut roots = RootCertStore::empty();
    if let Some(path) = path {
        for certificate in load_certificates(path.as_ref())? {
            roots.add(certificate).map_err(|error| {
                Error::InvalidConfig(format!("invalid internal TLS CA: {error}"))
            })?;
        }
    } else {
        roots.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
    }
    if roots.is_empty() {
        return Err(Error::InvalidConfig(
            "internal TLS trust store contains no certificates".into(),
        ));
    }
    Ok(roots)
}

fn load_certificates(path: &Path) -> Result<Vec<CertificateDer<'static>>> {
    let file = File::open(path)?;
    let certificates =
        rustls_pemfile::certs(&mut BufReader::new(file)).collect::<io::Result<Vec<_>>>()?;
    if certificates.is_empty() {
        return Err(Error::InvalidConfig(format!(
            "PEM file {} contains no certificates",
            path.display()
        )));
    }
    Ok(certificates)
}

fn load_private_key(path: &Path) -> Result<PrivateKeyDer<'static>> {
    let file = File::open(path)?;
    rustls_pemfile::private_key(&mut BufReader::new(file))?
        .ok_or_else(|| Error::InvalidConfig(format!("PEM file {} contains no key", path.display())))
}
