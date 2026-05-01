use super::{AuditEventKind, NodeApp, NodeError};
use crate::blocking::run_possibly_blocking;
use crate::storage::NodeStorage;
use crate::storage_io::storage_path_error;
use orion::NodeId;
use std::{
    collections::BTreeSet,
    net::SocketAddr,
    path::{Path, PathBuf},
};

pub(super) use orion_transport_common::stable_fingerprint;

const AUTO_HTTP_TLS_DIR: &str = "http-tls";
const AUTO_HTTP_TLS_CERT_FILE: &str = "cert.pem";
const AUTO_HTTP_TLS_KEY_FILE: &str = "key.pem";

impl NodeApp {
    pub fn rotate_http_tls_identity(&self) -> Result<(), NodeError> {
        if !self.auto_http_tls {
            return Err(NodeError::Storage(
                "HTTP TLS rotation is only supported for auto-generated TLS identities".into(),
            ));
        }
        let cert_path = self.http_tls_cert_path.as_deref().ok_or_else(|| {
            NodeError::Storage("HTTP TLS certificate path is not configured".into())
        })?;
        let key_path = self
            .http_tls_key_path
            .as_deref()
            .ok_or_else(|| NodeError::Storage("HTTP TLS key path is not configured".into()))?;
        rewrite_auto_http_tls_files(
            cert_path,
            key_path,
            &self.config.node_id,
            self.config.http_bind_addr,
        )?;
        self.evict_all_peer_clients();
        self.record_audit_event(
            AuditEventKind::HttpTlsRotated,
            None,
            "rotated local auto-generated HTTP TLS identity".into(),
        );
        Ok(())
    }

    pub(crate) fn evict_all_peer_clients(&self) {
        self.with_peer_clients_mut(|peer_clients| {
            peer_clients.clear();
        });
    }
}

pub(super) fn build_peer_http_client(
    base_url: &str,
    tls: Option<orion::transport::http::HttpClientTlsConfig>,
) -> Result<orion::transport::http::HttpClient, NodeError> {
    match tls {
        Some(tls) => orion::transport::http::HttpClient::with_tls(base_url.to_owned(), tls)
            .map_err(NodeError::from),
        None => orion::transport::http::HttpClient::try_new(base_url.to_owned())
            .map_err(NodeError::from),
    }
}

pub(super) fn build_bootstrap_peer_http_client(
    base_url: &str,
) -> Result<orion::transport::http::HttpClient, NodeError> {
    orion::transport::http::HttpClient::with_dangerous_tls(base_url.to_owned())
        .map_err(NodeError::from)
}

pub(super) fn resolve_http_server_tls_paths(
    cert_path: Option<PathBuf>,
    key_path: Option<PathBuf>,
    auto_http_tls: bool,
    storage: Option<&NodeStorage>,
    node_id: &NodeId,
    bind_addr: SocketAddr,
) -> Result<(Option<PathBuf>, Option<PathBuf>), NodeError> {
    match (cert_path, key_path) {
        (Some(cert_path), Some(key_path)) => Ok((Some(cert_path), Some(key_path))),
        (None, None) if auto_http_tls => {
            let storage = storage.ok_or_else(|| {
                NodeError::Storage(
                    "automatic HTTP TLS requires ORION_NODE_STATE_DIR or NodeConfig.state_dir"
                        .into(),
                )
            })?;
            let (cert_path, key_path) =
                ensure_auto_http_tls_files(storage.root(), node_id, bind_addr)?;
            Ok((Some(cert_path), Some(key_path)))
        }
        (None, None) => Ok((None, None)),
        _ => Err(NodeError::Storage(
            "HTTP TLS requires both certificate and key paths".into(),
        )),
    }
}

fn ensure_auto_http_tls_files(
    state_dir: &Path,
    node_id: &NodeId,
    bind_addr: SocketAddr,
) -> Result<(PathBuf, PathBuf), NodeError> {
    let tls_dir = state_dir.join(AUTO_HTTP_TLS_DIR);
    let cert_path = tls_dir.join(AUTO_HTTP_TLS_CERT_FILE);
    let key_path = tls_dir.join(AUTO_HTTP_TLS_KEY_FILE);
    if cert_path.exists() && key_path.exists() {
        return Ok((cert_path, key_path));
    }

    // Startup/admin TLS provisioning still uses synchronous filesystem work. This stays off the
    // async hot path and is intentionally isolated behind `run_possibly_blocking(...)`.
    run_possibly_blocking(|| {
        std::fs::create_dir_all(&tls_dir).map_err(|err| {
            storage_path_error("failed to create HTTP TLS directory", &tls_dir, &err)
        })
    })?;
    rewrite_auto_http_tls_files(&cert_path, &key_path, node_id, bind_addr)?;
    Ok((cert_path, key_path))
}

pub(super) fn rewrite_auto_http_tls_files(
    cert_path: &Path,
    key_path: &Path,
    node_id: &NodeId,
    bind_addr: SocketAddr,
) -> Result<(), NodeError> {
    // Key generation plus atomic replacement is intentionally synchronous today because it is a
    // rare rotation/bootstrap path rather than a request-serving code path.
    run_possibly_blocking(|| {
        let rcgen::CertifiedKey { cert, signing_key } =
            rcgen::generate_simple_self_signed(auto_http_tls_subject_names(node_id, bind_addr))
                .map_err(|err| {
                    NodeError::Storage(format!("failed to generate HTTP TLS identity: {err}"))
                })?;
        let cert_tmp = cert_path.with_extension("pem.tmp");
        let key_tmp = key_path.with_extension("pem.tmp");
        std::fs::write(&cert_tmp, cert.pem()).map_err(|err| {
            storage_path_error(
                "failed to write temporary HTTP TLS certificate",
                &cert_tmp,
                &err,
            )
        })?;
        std::fs::write(&key_tmp, signing_key.serialize_pem()).map_err(|err| {
            storage_path_error("failed to write temporary HTTP TLS key", &key_tmp, &err)
        })?;
        std::fs::rename(&cert_tmp, cert_path).map_err(|err| {
            storage_path_error("failed to install HTTP TLS certificate", cert_path, &err)
        })?;
        std::fs::rename(&key_tmp, key_path)
            .map_err(|err| storage_path_error("failed to install HTTP TLS key", key_path, &err))?;
        Ok(())
    })
}

fn auto_http_tls_subject_names(node_id: &NodeId, bind_addr: SocketAddr) -> Vec<String> {
    let mut names = BTreeSet::from([
        "localhost".to_owned(),
        "127.0.0.1".to_owned(),
        "::1".to_owned(),
        node_id.as_str().to_owned(),
    ]);
    let bind_ip = bind_addr.ip();
    if !bind_ip.is_unspecified() {
        names.insert(bind_ip.to_string());
    }
    if let Some(hostname) = std::env::var("HOSTNAME")
        .ok()
        .or_else(|| std::env::var("COMPUTERNAME").ok())
        .map(|value| value.trim().to_owned())
        .filter(|value| {
            !value.is_empty()
                && value
                    .chars()
                    .all(|ch| ch.is_ascii_alphanumeric() || ch == '-' || ch == '.')
        })
    {
        names.insert(hostname);
    }
    names.into_iter().collect()
}
