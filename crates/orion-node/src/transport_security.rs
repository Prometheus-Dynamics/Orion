use crate::auth::NodeSecurity;
use crate::blocking::run_possibly_blocking;
use crate::{NodeError, app::HttpMutualTlsMode};
#[cfg(feature = "transport-quic")]
use orion::transport::quic::{QuicClientTlsConfig, QuicServerClientAuth, QuicServerTlsConfig};
#[cfg(feature = "transport-tcp")]
use orion::transport::tcp::{TcpClientTlsConfig, TcpServerClientAuth, TcpServerTlsConfig};
use orion::{
    NodeId,
    transport::http::{
        HttpClientTlsConfig, HttpServerClientAuth, HttpServerTlsConfig, HttpTlsTrustProvider,
    },
};
#[cfg(feature = "transport-tcp")]
use orion_transport_common::DEFAULT_TRANSPORT_SERVER_NAME;
use std::{
    path::{Path, PathBuf},
    sync::Arc,
};

type IdentityPemPair = (Vec<u8>, Vec<u8>);

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ManagedTransportProtocol {
    Http,
    #[cfg(feature = "transport-tcp")]
    Tcp,
    #[cfg(feature = "transport-quic")]
    Quic,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ManagedNodeTransportSurface {
    PeerHttpControl,
    HttpProbe,
    #[cfg(feature = "transport-tcp")]
    PeerTcpData,
    #[cfg(feature = "transport-quic")]
    PeerQuicData,
}

impl ManagedNodeTransportSurface {
    pub fn protocol(self) -> ManagedTransportProtocol {
        match self {
            Self::PeerHttpControl | Self::HttpProbe => ManagedTransportProtocol::Http,
            #[cfg(feature = "transport-tcp")]
            Self::PeerTcpData => ManagedTransportProtocol::Tcp,
            #[cfg(feature = "transport-quic")]
            Self::PeerQuicData => ManagedTransportProtocol::Quic,
        }
    }

    pub fn uses_transport_security(self) -> bool {
        !matches!(self, Self::HttpProbe)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PeerTransportSecurityMode {
    Disabled,
    Optional,
    Required,
}

impl From<HttpMutualTlsMode> for PeerTransportSecurityMode {
    fn from(value: HttpMutualTlsMode) -> Self {
        match value {
            HttpMutualTlsMode::Disabled => Self::Disabled,
            HttpMutualTlsMode::Optional => Self::Optional,
            HttpMutualTlsMode::Required => Self::Required,
        }
    }
}

#[derive(Clone)]
pub struct NodeTransportSecurityManager {
    security: Arc<NodeSecurity>,
    cert_path: Option<PathBuf>,
    key_path: Option<PathBuf>,
    peer_mode: PeerTransportSecurityMode,
}

impl NodeTransportSecurityManager {
    pub fn new(
        security: Arc<NodeSecurity>,
        cert_path: Option<PathBuf>,
        key_path: Option<PathBuf>,
        peer_mode: PeerTransportSecurityMode,
    ) -> Self {
        Self {
            security,
            cert_path,
            key_path,
            peer_mode,
        }
    }

    pub fn peer_mode(&self) -> PeerTransportSecurityMode {
        self.peer_mode
    }

    pub fn server_security(
        &self,
        protocol: ManagedTransportProtocol,
    ) -> Result<Option<ManagedServerTransportSecurity>, NodeError> {
        match protocol {
            ManagedTransportProtocol::Http => self
                .http_server_config()
                .map(|config| config.map(ManagedServerTransportSecurity::Http)),
            #[cfg(feature = "transport-tcp")]
            ManagedTransportProtocol::Tcp => self
                .tcp_server_config()
                .map(|config| config.map(ManagedServerTransportSecurity::Tcp)),
            #[cfg(feature = "transport-quic")]
            ManagedTransportProtocol::Quic => self
                .quic_server_config()
                .map(|config| config.map(ManagedServerTransportSecurity::Quic)),
        }
    }

    pub fn client_security(
        &self,
        protocol: ManagedTransportProtocol,
        node_id: &NodeId,
        configured_path: Option<&str>,
        server_name: Option<&str>,
        endpoint_hint: Option<&str>,
    ) -> Result<Option<ManagedClientTransportSecurity>, NodeError> {
        #[cfg(not(feature = "transport-tcp"))]
        let _ = server_name;
        match protocol {
            ManagedTransportProtocol::Http => self
                .http_client_config(node_id, endpoint_hint.unwrap_or("http://"), configured_path)
                .map(|config| config.map(ManagedClientTransportSecurity::Http)),
            #[cfg(feature = "transport-tcp")]
            ManagedTransportProtocol::Tcp => self
                .tcp_client_config(
                    node_id,
                    configured_path,
                    server_name.unwrap_or(DEFAULT_TRANSPORT_SERVER_NAME),
                )
                .map(|config| config.map(ManagedClientTransportSecurity::Tcp)),
            #[cfg(feature = "transport-quic")]
            ManagedTransportProtocol::Quic => self
                .quic_client_config(node_id, configured_path)
                .map(|config| config.map(ManagedClientTransportSecurity::Quic)),
        }
    }

    pub fn surface_server_security(
        &self,
        surface: ManagedNodeTransportSurface,
    ) -> Result<Option<ManagedServerTransportSecurity>, NodeError> {
        if !surface.uses_transport_security() {
            return Ok(None);
        }
        self.server_security(surface.protocol())
    }

    pub fn surface_client_security(
        &self,
        surface: ManagedNodeTransportSurface,
        node_id: &NodeId,
        configured_path: Option<&str>,
        server_name: Option<&str>,
        endpoint_hint: Option<&str>,
    ) -> Result<Option<ManagedClientTransportSecurity>, NodeError> {
        if !surface.uses_transport_security() {
            return Ok(None);
        }
        self.client_security(
            surface.protocol(),
            node_id,
            configured_path,
            server_name,
            endpoint_hint,
        )
    }

    pub fn local_server_identity_pem(&self) -> Result<Option<IdentityPemPair>, NodeError> {
        self.load_local_identity_pem()
    }

    pub fn local_client_identity_pem(&self) -> Result<Option<IdentityPemPair>, NodeError> {
        if self.peer_mode == PeerTransportSecurityMode::Disabled {
            return Ok(None);
        }
        self.load_local_identity_pem()
    }

    pub fn trusted_peer_root_cert_pem(
        &self,
        node_id: &NodeId,
        configured_path: Option<&str>,
    ) -> Result<Option<Vec<u8>>, NodeError> {
        match configured_path {
            Some(path) => Ok(Some(read_file(Path::new(path))?)),
            None => self.security.trusted_peer_tls_root_cert_pem(node_id),
        }
    }

    pub fn http_server_config(&self) -> Result<Option<HttpServerTlsConfig>, NodeError> {
        let Some((cert_pem, key_pem)) = self.local_server_identity_pem()? else {
            return Ok(None);
        };
        Ok(Some(HttpServerTlsConfig {
            cert_pem,
            key_pem,
            cert_path: self.cert_path.clone(),
            key_path: self.key_path.clone(),
            client_auth: match self.peer_mode {
                PeerTransportSecurityMode::Disabled => HttpServerClientAuth::Disabled,
                PeerTransportSecurityMode::Optional => HttpServerClientAuth::OptionalDynamic {
                    trust_provider: Arc::new(SecurityTrustProvider {
                        security: self.security.clone(),
                    }),
                },
                PeerTransportSecurityMode::Required => HttpServerClientAuth::RequiredDynamic {
                    trust_provider: Arc::new(SecurityTrustProvider {
                        security: self.security.clone(),
                    }),
                },
            },
        }))
    }

    pub fn http_client_config(
        &self,
        node_id: &NodeId,
        base_url: &str,
        configured_path: Option<&str>,
    ) -> Result<Option<HttpClientTlsConfig>, NodeError> {
        let tls_root_cert_pem = self.trusted_peer_root_cert_pem(node_id, configured_path)?;
        if base_url.starts_with("https://") && tls_root_cert_pem.is_none() {
            return Err(NodeError::Authentication(format!(
                "HTTPS peer {} does not have a trusted TLS certificate; bootstrap is required",
                node_id
            )));
        }
        let Some(root_cert_pem) = tls_root_cert_pem else {
            return Ok(None);
        };
        let client_identity = self.local_client_identity_pem()?;
        Ok(Some(HttpClientTlsConfig {
            root_cert_pem,
            client_cert_pem: client_identity.as_ref().map(|(cert, _)| cert.clone()),
            client_key_pem: client_identity.map(|(_, key)| key),
        }))
    }

    #[cfg(feature = "transport-tcp")]
    pub fn tcp_server_config(&self) -> Result<Option<TcpServerTlsConfig>, NodeError> {
        let Some((cert_pem, key_pem)) = self.local_server_identity_pem()? else {
            return Ok(None);
        };
        Ok(Some(
            TcpServerTlsConfig::new(cert_pem, key_pem).with_client_auth(match self.peer_mode {
                PeerTransportSecurityMode::Disabled => TcpServerClientAuth::Disabled,
                PeerTransportSecurityMode::Optional => TcpServerClientAuth::Optional {
                    trusted_client_roots_pem: self
                        .security
                        .trusted_peer_tls_root_certs()?
                        .into_values()
                        .collect(),
                },
                PeerTransportSecurityMode::Required => TcpServerClientAuth::Required {
                    trusted_client_roots_pem: self
                        .security
                        .trusted_peer_tls_root_certs()?
                        .into_values()
                        .collect(),
                },
            }),
        ))
    }

    #[cfg(feature = "transport-tcp")]
    pub fn tcp_client_config(
        &self,
        node_id: &NodeId,
        configured_path: Option<&str>,
        server_name: &str,
    ) -> Result<Option<TcpClientTlsConfig>, NodeError> {
        let Some(root_cert_pem) = self.trusted_peer_root_cert_pem(node_id, configured_path)? else {
            return Ok(None);
        };
        let client_identity = self.local_client_identity_pem()?;
        let mut config = TcpClientTlsConfig::new(root_cert_pem, server_name);
        if let Some((cert, key)) = client_identity {
            config = config.with_client_identity(cert, key);
        }
        Ok(Some(config))
    }

    #[cfg(feature = "transport-quic")]
    pub fn quic_server_config(&self) -> Result<Option<QuicServerTlsConfig>, NodeError> {
        let Some((cert_pem, key_pem)) = self.local_server_identity_pem()? else {
            return Ok(None);
        };
        Ok(Some(
            QuicServerTlsConfig::new(cert_pem, key_pem).with_client_auth(match self.peer_mode {
                PeerTransportSecurityMode::Disabled => QuicServerClientAuth::Disabled,
                PeerTransportSecurityMode::Optional => QuicServerClientAuth::Optional {
                    trusted_client_roots_pem: self
                        .security
                        .trusted_peer_tls_root_certs()?
                        .into_values()
                        .collect(),
                },
                PeerTransportSecurityMode::Required => QuicServerClientAuth::Required {
                    trusted_client_roots_pem: self
                        .security
                        .trusted_peer_tls_root_certs()?
                        .into_values()
                        .collect(),
                },
            }),
        ))
    }

    #[cfg(feature = "transport-quic")]
    pub fn quic_client_config(
        &self,
        node_id: &NodeId,
        configured_path: Option<&str>,
    ) -> Result<Option<QuicClientTlsConfig>, NodeError> {
        let Some(root_cert_pem) = self.trusted_peer_root_cert_pem(node_id, configured_path)? else {
            return Ok(None);
        };
        let client_identity = self.local_client_identity_pem()?;
        let mut config = QuicClientTlsConfig::new(root_cert_pem);
        if let Some((cert, key)) = client_identity {
            config = config.with_client_identity(cert, key);
        }
        Ok(Some(config))
    }

    fn load_local_identity_pem(&self) -> Result<Option<IdentityPemPair>, NodeError> {
        match (&self.cert_path, &self.key_path) {
            (Some(cert_path), Some(key_path)) => {
                Ok(Some((read_file(cert_path)?, read_file(key_path)?)))
            }
            (None, None) => Ok(None),
            _ => Err(NodeError::Storage(
                "transport security requires both certificate and key paths".into(),
            )),
        }
    }
}

#[derive(Clone)]
pub enum ManagedServerTransportSecurity {
    Http(HttpServerTlsConfig),
    #[cfg(feature = "transport-tcp")]
    Tcp(TcpServerTlsConfig),
    #[cfg(feature = "transport-quic")]
    Quic(QuicServerTlsConfig),
}

#[derive(Clone)]
pub enum ManagedClientTransportSecurity {
    Http(HttpClientTlsConfig),
    #[cfg(feature = "transport-tcp")]
    Tcp(TcpClientTlsConfig),
    #[cfg(feature = "transport-quic")]
    Quic(QuicClientTlsConfig),
}

#[derive(Clone)]
struct SecurityTrustProvider {
    security: Arc<NodeSecurity>,
}

impl HttpTlsTrustProvider for SecurityTrustProvider {
    fn trusted_client_roots_pem(&self) -> Vec<Vec<u8>> {
        self.security
            .trusted_peer_tls_root_certs()
            .unwrap_or_default()
            .into_values()
            .collect()
    }
}

fn read_file(path: &Path) -> Result<Vec<u8>, NodeError> {
    run_possibly_blocking(|| {
        std::fs::read(path).map_err(|err| {
            NodeError::Storage(format!("failed to read file `{}`: {err}", path.display()))
        })
    })
}
