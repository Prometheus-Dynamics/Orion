use std::{
    hash::{Hash, Hasher},
    path::PathBuf,
    sync::Arc,
};

use orion_transport_common::{
    build_client_verifier, fingerprint_file_state, install_rustls_crypto_provider,
    parse_cert_chain, parse_private_key,
};
use rustls::ServerConfig;
use tokio_rustls::TlsAcceptor;

use crate::HttpTransportError;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct HttpClientTlsConfig {
    pub root_cert_pem: Vec<u8>,
    pub client_cert_pem: Option<Vec<u8>>,
    pub client_key_pem: Option<Vec<u8>>,
}

pub trait HttpTlsTrustProvider: Send + Sync {
    fn trusted_client_roots_pem(&self) -> Vec<Vec<u8>>;
}

#[derive(Clone)]
pub enum HttpServerClientAuth {
    Disabled,
    OptionalStatic {
        trusted_client_roots_pem: Vec<Vec<u8>>,
    },
    RequiredStatic {
        trusted_client_roots_pem: Vec<Vec<u8>>,
    },
    OptionalDynamic {
        trust_provider: Arc<dyn HttpTlsTrustProvider>,
    },
    RequiredDynamic {
        trust_provider: Arc<dyn HttpTlsTrustProvider>,
    },
}

#[derive(Clone)]
pub struct HttpServerTlsConfig {
    pub cert_pem: Vec<u8>,
    pub key_pem: Vec<u8>,
    pub cert_path: Option<PathBuf>,
    pub key_path: Option<PathBuf>,
    pub client_auth: HttpServerClientAuth,
}

#[derive(Clone)]
pub(crate) struct CachedTlsAcceptor {
    pub(crate) fingerprint: u64,
    pub(crate) acceptor: TlsAcceptor,
}

pub(crate) fn cached_tls_acceptor(
    tls: &HttpServerTlsConfig,
    cached: &mut Option<CachedTlsAcceptor>,
) -> Result<TlsAcceptor, HttpTransportError> {
    let fingerprint = fingerprint_server_tls_config(tls)?;
    if let Some(cached) = cached.as_ref()
        && cached.fingerprint == fingerprint
    {
        return Ok(cached.acceptor.clone());
    }

    let acceptor = TlsAcceptor::from(Arc::new(build_server_tls_config(tls)?));
    *cached = Some(CachedTlsAcceptor {
        fingerprint,
        acceptor: acceptor.clone(),
    });
    Ok(acceptor)
}

pub(crate) fn build_server_tls_config(
    tls: &HttpServerTlsConfig,
) -> Result<ServerConfig, HttpTransportError> {
    install_crypto_provider();
    let cert_pem = match (&tls.cert_path, &tls.key_path) {
        (Some(cert_path), Some(_)) => {
            // Certificate/key file loading is a startup or reload-time path. It remains
            // synchronous so callers do not pay async complexity in the steady-state hot path.
            std::fs::read(cert_path).map_err(|err| HttpTransportError::Tls(err.to_string()))?
        }
        (None, None) => tls.cert_pem.clone(),
        _ => {
            return Err(HttpTransportError::Tls(
                "server TLS requires both certificate and key paths".into(),
            ));
        }
    };
    let key_pem = match (&tls.cert_path, &tls.key_path) {
        (Some(_), Some(key_path)) => {
            std::fs::read(key_path).map_err(|err| HttpTransportError::Tls(err.to_string()))?
        }
        (None, None) => tls.key_pem.clone(),
        _ => {
            return Err(HttpTransportError::Tls(
                "server TLS requires both certificate and key paths".into(),
            ));
        }
    };
    let cert_chain =
        parse_cert_chain(&cert_pem, "HTTP TLS certificate PEM").map_err(HttpTransportError::Tls)?;
    let private_key =
        parse_private_key(&key_pem, "HTTP TLS key PEM").map_err(HttpTransportError::Tls)?;

    let builder = ServerConfig::builder();
    let builder = match &tls.client_auth {
        HttpServerClientAuth::Disabled => builder
            .with_client_cert_verifier(rustls::server::WebPkiClientVerifier::no_client_auth()),
        HttpServerClientAuth::OptionalStatic {
            trusted_client_roots_pem,
        } => builder.with_client_cert_verifier(
            build_client_verifier(
                trusted_client_roots_pem.iter().cloned(),
                true,
                "HTTP TLS trusted client roots",
            )
            .map_err(HttpTransportError::Tls)?,
        ),
        HttpServerClientAuth::RequiredStatic {
            trusted_client_roots_pem,
        } => builder.with_client_cert_verifier(
            build_client_verifier(
                trusted_client_roots_pem.iter().cloned(),
                false,
                "HTTP TLS trusted client roots",
            )
            .map_err(HttpTransportError::Tls)?,
        ),
        HttpServerClientAuth::OptionalDynamic { trust_provider } => builder
            .with_client_cert_verifier(
                build_client_verifier(
                    trust_provider.trusted_client_roots_pem(),
                    true,
                    "HTTP TLS trusted client roots",
                )
                .map_err(HttpTransportError::Tls)?,
            ),
        HttpServerClientAuth::RequiredDynamic { trust_provider } => builder
            .with_client_cert_verifier(
                build_client_verifier(
                    trust_provider.trusted_client_roots_pem(),
                    false,
                    "HTTP TLS trusted client roots",
                )
                .map_err(HttpTransportError::Tls)?,
            ),
    };

    builder
        .with_single_cert(cert_chain, private_key)
        .map_err(|err| HttpTransportError::Tls(err.to_string()))
}

pub(crate) fn install_crypto_provider() {
    install_rustls_crypto_provider();
}

fn fingerprint_server_tls_config(tls: &HttpServerTlsConfig) -> Result<u64, HttpTransportError> {
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    match (&tls.cert_path, &tls.key_path) {
        (Some(cert_path), Some(key_path)) => {
            fingerprint_file_state(cert_path, &mut hasher, "HTTP TLS certificate path")
                .map_err(HttpTransportError::Tls)?;
            fingerprint_file_state(key_path, &mut hasher, "HTTP TLS key path")
                .map_err(HttpTransportError::Tls)?;
        }
        (None, None) => {
            tls.cert_pem.hash(&mut hasher);
            tls.key_pem.hash(&mut hasher);
        }
        _ => {
            return Err(HttpTransportError::Tls(
                "server TLS requires both certificate and key paths".into(),
            ));
        }
    }

    match &tls.client_auth {
        HttpServerClientAuth::Disabled => {
            0u8.hash(&mut hasher);
        }
        HttpServerClientAuth::OptionalStatic {
            trusted_client_roots_pem,
        } => {
            1u8.hash(&mut hasher);
            trusted_client_roots_pem.hash(&mut hasher);
        }
        HttpServerClientAuth::RequiredStatic {
            trusted_client_roots_pem,
        } => {
            2u8.hash(&mut hasher);
            trusted_client_roots_pem.hash(&mut hasher);
        }
        HttpServerClientAuth::OptionalDynamic { trust_provider } => {
            3u8.hash(&mut hasher);
            trust_provider.trusted_client_roots_pem().hash(&mut hasher);
        }
        HttpServerClientAuth::RequiredDynamic { trust_provider } => {
            4u8.hash(&mut hasher);
            trust_provider.trusted_client_roots_pem().hash(&mut hasher);
        }
    }

    Ok(hasher.finish())
}
