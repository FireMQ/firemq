//! TLS for the AMQP and FireMQ listeners, using rustls.

use std::io;
use std::path::PathBuf;
use std::sync::Arc;

use rustls::pki_types::pem::PemObject;
use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio_rustls::TlsAcceptor;

/// Certificate and private key files, PEM-encoded.
#[derive(Debug, Clone)]
pub struct TlsConfig {
    /// Certificate chain, leaf first.
    pub cert: PathBuf,
    pub key: PathBuf,
}

fn invalid(what: &str, e: impl std::fmt::Display) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, format!("{what}: {e}"))
}

/// Builds a TLS acceptor from PEM files.
pub fn acceptor(cfg: &TlsConfig) -> io::Result<TlsAcceptor> {
    let certs: Vec<CertificateDer<'static>> = CertificateDer::pem_file_iter(&cfg.cert)
        .map_err(|e| {
            invalid(
                &format!("cannot read certificate {}", cfg.cert.display()),
                e,
            )
        })?
        .collect::<Result<_, _>>()
        .map_err(|e| invalid(&format!("invalid certificate {}", cfg.cert.display()), e))?;
    if certs.is_empty() {
        return Err(invalid(
            "no certificate found",
            cfg.cert.display().to_string(),
        ));
    }
    let key = PrivateKeyDer::from_pem_file(&cfg.key)
        .map_err(|e| invalid(&format!("cannot read private key {}", cfg.key.display()), e))?;
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let config = rustls::ServerConfig::builder_with_provider(provider)
        .with_safe_default_protocol_versions()
        .map_err(|e| invalid("TLS setup failed", e))?
        .with_no_client_auth()
        .with_single_cert(certs, key)
        .map_err(|e| invalid("certificate and key do not match", e))?;
    Ok(TlsAcceptor::from(Arc::new(config)))
}

/// A byte stream: plain TCP or TLS.
pub trait IoStream: AsyncRead + AsyncWrite + Send + Unpin {}

impl<T: AsyncRead + AsyncWrite + Send + Unpin> IoStream for T {}
