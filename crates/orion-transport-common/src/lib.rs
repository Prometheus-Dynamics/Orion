use rustls::{RootCertStore, pki_types::CertificateDer, pki_types::PrivateKeyDer};
use rustls_pemfile::{certs, pkcs8_private_keys, rsa_private_keys};
use std::{
    hash::{Hash, Hasher},
    net::{IpAddr, Ipv4Addr, SocketAddr},
    path::Path,
    sync::Arc,
    time::Duration,
};
use tokio::task::JoinSet;

pub const DEFAULT_TRANSPORT_SERVER_NAME: &str = "localhost";
pub const DEFAULT_HTTP_CLIENT_CONNECT_TIMEOUT: Duration = Duration::from_millis(500);
pub const DEFAULT_HTTP_CLIENT_REQUEST_TIMEOUT: Duration = Duration::from_secs(1);
pub const DEFAULT_QUIC_INSECURE_SELF_SIGNED_SAN_HOSTS: &[&str] =
    &[DEFAULT_TRANSPORT_SERVER_NAME, "node-b.local"];

pub fn loopback_ephemeral_socket_addr() -> SocketAddr {
    SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0)
}

pub fn install_rustls_crypto_provider() {
    let _ = rustls::crypto::ring::default_provider().install_default();
}

pub fn parse_cert_chain(
    cert_pem: &[u8],
    context: &str,
) -> Result<Vec<CertificateDer<'static>>, String> {
    let mut reader = std::io::BufReader::new(cert_pem);
    let cert_chain = certs(&mut reader)
        .collect::<Result<Vec<_>, _>>()
        .map_err(|err| format!("{context}: {err}"))?;
    if cert_chain.is_empty() {
        return Err(format!("{context}: PEM did not contain any certificates"));
    }
    Ok(cert_chain)
}

pub fn parse_private_key(key_pem: &[u8], context: &str) -> Result<PrivateKeyDer<'static>, String> {
    let mut key_reader = std::io::BufReader::new(key_pem);
    let mut pkcs8_keys = pkcs8_private_keys(&mut key_reader)
        .collect::<Result<Vec<_>, _>>()
        .map_err(|err| format!("{context}: {err}"))?;
    if let Some(key) = pkcs8_keys.pop() {
        return Ok(key.into());
    }

    let mut key_reader = std::io::BufReader::new(key_pem);
    let mut rsa_keys = rsa_private_keys(&mut key_reader)
        .collect::<Result<Vec<_>, _>>()
        .map_err(|err| format!("{context}: {err}"))?;
    rsa_keys
        .pop()
        .map(Into::into)
        .ok_or_else(|| format!("{context}: PEM did not contain a supported private key"))
}

pub fn root_store_from_pem(cert_pem: &[u8], context: &str) -> Result<RootCertStore, String> {
    let mut roots = RootCertStore::empty();
    let mut reader = std::io::BufReader::new(cert_pem);
    for cert in certs(&mut reader) {
        let cert = cert.map_err(|err| format!("{context}: {err}"))?;
        roots.add(cert).map_err(|err| format!("{context}: {err}"))?;
    }
    Ok(roots)
}

pub fn root_store_from_pem_iter(
    trusted_roots_pem: impl IntoIterator<Item = Vec<u8>>,
    context: &str,
) -> Result<RootCertStore, String> {
    let mut roots = RootCertStore::empty();
    for pem in trusted_roots_pem {
        let mut reader = std::io::BufReader::new(pem.as_slice());
        for cert in certs(&mut reader) {
            let cert = cert.map_err(|err| format!("{context}: {err}"))?;
            roots.add(cert).map_err(|err| format!("{context}: {err}"))?;
        }
    }
    Ok(roots)
}

pub fn build_client_verifier(
    trusted_roots_pem: impl IntoIterator<Item = Vec<u8>>,
    allow_unauthenticated: bool,
    context: &str,
) -> Result<Arc<dyn rustls::server::danger::ClientCertVerifier>, String> {
    let roots = root_store_from_pem_iter(trusted_roots_pem, context)?;
    if allow_unauthenticated && roots.is_empty() {
        return Ok(rustls::server::WebPkiClientVerifier::no_client_auth());
    }
    let mut verifier = rustls::server::WebPkiClientVerifier::builder(roots.into());
    if allow_unauthenticated {
        verifier = verifier.allow_unauthenticated();
    }
    verifier.build().map_err(|err| format!("{context}: {err}"))
}

pub fn stable_fingerprint(bytes: &[u8]) -> u64 {
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    bytes.hash(&mut hasher);
    hasher.finish()
}

pub fn fingerprint_file_state(
    path: &Path,
    hasher: &mut std::collections::hash_map::DefaultHasher,
    context: &str,
) -> Result<(), String> {
    path.hash(hasher);
    let metadata = std::fs::metadata(path).map_err(|err| format!("{context}: {err}"))?;
    metadata.len().hash(hasher);
    let modified = metadata
        .modified()
        .map_err(|err| format!("{context}: {err}"))?;
    modified
        .duration_since(std::time::UNIX_EPOCH)
        .map_err(|err| format!("{context}: {err}"))?
        .as_nanos()
        .hash(hasher);
    Ok(())
}

pub struct ConnectionTasks {
    tasks: JoinSet<()>,
}

impl ConnectionTasks {
    pub fn new() -> Self {
        Self {
            tasks: JoinSet::new(),
        }
    }

    pub fn spawn<F, E>(&mut self, future: F)
    where
        F: std::future::Future<Output = Result<(), E>> + Send + 'static,
        E: Send + 'static,
    {
        self.tasks.spawn(async move {
            let _ = future.await;
        });
    }

    pub fn spawn_unit<F>(&mut self, future: F)
    where
        F: std::future::Future<Output = ()> + Send + 'static,
    {
        self.tasks.spawn(future);
    }

    pub async fn abort_all(&mut self) {
        self.tasks.abort_all();
        while self.tasks.join_next().await.is_some() {}
    }
}

impl Default for ConnectionTasks {
    fn default() -> Self {
        Self::new()
    }
}
