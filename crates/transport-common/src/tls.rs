//! rustls helpers shared by the TLS-capable transports (HTTP, TCP, QUIC).
//!
//! Gated behind the `tls` feature so IPC-only builds do not link rustls.

use rustls::{RootCertStore, pki_types::CertificateDer, pki_types::PrivateKeyDer};
use rustls_pemfile::{certs, pkcs8_private_keys, rsa_private_keys};
use std::sync::Arc;

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
