use std::{net::SocketAddr, sync::Arc};

use orion_transport_common::{
    ConnectionTasks, DEFAULT_QUIC_INSECURE_SELF_SIGNED_SAN_HOSTS, DEFAULT_TRANSPORT_SERVER_NAME,
    build_client_verifier, install_rustls_crypto_provider, loopback_ephemeral_socket_addr,
    parse_cert_chain, parse_private_key, root_store_from_pem,
};
use quinn::{ClientConfig, Connection, Endpoint, ServerConfig, TransportConfig};
use rcgen::generate_simple_self_signed;
use rustls::RootCertStore;
use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::pki_types::UnixTime;
use rustls_pki_types::{CertificateDer, ServerName};

use crate::{QuicCodec, QuicEndpoint, QuicFrame, QuicTransportError};

pub trait QuicFrameHandler: Send + Sync + 'static {
    fn handle_frame(&self, frame: QuicFrame) -> Result<QuicFrame, QuicTransportError>;
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct QuicClientTlsConfig {
    pub root_cert_pem: Vec<u8>,
    pub client_cert_pem: Option<Vec<u8>>,
    pub client_key_pem: Option<Vec<u8>>,
}

impl QuicClientTlsConfig {
    pub fn insecure() -> Self {
        Self {
            root_cert_pem: Vec::new(),
            client_cert_pem: None,
            client_key_pem: None,
        }
    }

    pub fn new(root_cert_pem: Vec<u8>) -> Self {
        Self {
            root_cert_pem,
            client_cert_pem: None,
            client_key_pem: None,
        }
    }

    pub fn with_client_identity(
        mut self,
        client_cert_pem: Vec<u8>,
        client_key_pem: Vec<u8>,
    ) -> Self {
        self.client_cert_pem = Some(client_cert_pem);
        self.client_key_pem = Some(client_key_pem);
        self
    }
}

#[derive(Clone)]
pub enum QuicServerClientAuth {
    Disabled,
    Optional {
        trusted_client_roots_pem: Vec<Vec<u8>>,
    },
    Required {
        trusted_client_roots_pem: Vec<Vec<u8>>,
    },
}

#[derive(Clone)]
pub struct QuicServerTlsConfig {
    pub cert_pem: Vec<u8>,
    pub key_pem: Vec<u8>,
    pub client_auth: QuicServerClientAuth,
}

impl QuicServerTlsConfig {
    pub fn new(cert_pem: Vec<u8>, key_pem: Vec<u8>) -> Self {
        Self {
            cert_pem,
            key_pem,
            client_auth: QuicServerClientAuth::Disabled,
        }
    }

    pub fn with_client_auth(mut self, client_auth: QuicServerClientAuth) -> Self {
        self.client_auth = client_auth;
        self
    }
}

pub struct QuicFrameServer {
    endpoint: Endpoint,
    local_endpoint: QuicEndpoint,
    handler: Arc<dyn QuicFrameHandler>,
}

impl QuicFrameServer {
    pub async fn bind(
        endpoint: QuicEndpoint,
        handler: Arc<dyn QuicFrameHandler>,
    ) -> Result<(Self, QuicEndpoint), QuicTransportError> {
        let insecure_tls = generate_insecure_self_signed_tls()?;
        Self::bind_secure(endpoint, handler, insecure_tls).await
    }

    pub async fn bind_secure(
        endpoint: QuicEndpoint,
        handler: Arc<dyn QuicFrameHandler>,
        tls: QuicServerTlsConfig,
    ) -> Result<(Self, QuicEndpoint), QuicTransportError> {
        install_rustls_crypto_provider();
        let server_config = build_server_config(&tls)?;
        let bind_addr: SocketAddr = format!("{}:{}", endpoint.host, endpoint.port)
            .parse()
            .map_err(|err: std::net::AddrParseError| {
                QuicTransportError::BindFailed(err.to_string())
            })?;
        let server_endpoint = Endpoint::server(server_config, bind_addr)
            .map_err(|err| QuicTransportError::BindFailed(err.to_string()))?;
        let addr = server_endpoint
            .local_addr()
            .map_err(|err| QuicTransportError::BindFailed(err.to_string()))?;
        let local_endpoint = QuicEndpoint::new(addr.ip().to_string(), addr.port())
            .with_server_name(
                endpoint
                    .server_name
                    .clone()
                    .unwrap_or_else(|| DEFAULT_TRANSPORT_SERVER_NAME.to_owned()),
            );

        Ok((
            Self {
                endpoint: server_endpoint,
                local_endpoint: local_endpoint.clone(),
                handler,
            },
            local_endpoint,
        ))
    }

    pub fn local_endpoint(&self) -> &QuicEndpoint {
        &self.local_endpoint
    }

    pub async fn serve(self) -> Result<(), QuicTransportError> {
        let handler = self.handler.clone();
        serve_quic_accept_loop(self.endpoint, move |connecting| {
            let handler = handler.clone();
            async move {
                let _ = handle_connection(connecting.await, handler).await;
            }
        })
        .await
    }

    pub async fn serve_with_shutdown<F>(self, shutdown: F) -> Result<(), QuicTransportError>
    where
        F: std::future::Future<Output = ()>,
    {
        let handler = self.handler.clone();
        serve_quic_accept_loop_with_shutdown(self.endpoint, shutdown, move |connecting| {
            let handler = handler.clone();
            async move {
                let _ = handle_connection(connecting.await, handler).await;
            }
        })
        .await
    }
}

async fn serve_quic_accept_loop<SpawnFn, Fut>(
    endpoint: Endpoint,
    mut spawn_connection: SpawnFn,
) -> Result<(), QuicTransportError>
where
    SpawnFn: FnMut(quinn::Incoming) -> Fut,
    Fut: std::future::Future<Output = ()> + Send + 'static,
{
    while let Some(connecting) = endpoint.accept().await {
        tokio::spawn(spawn_connection(connecting));
    }
    Ok(())
}

async fn serve_quic_accept_loop_with_shutdown<Shutdown, SpawnFn, Fut>(
    endpoint: Endpoint,
    shutdown: Shutdown,
    mut spawn_connection: SpawnFn,
) -> Result<(), QuicTransportError>
where
    Shutdown: std::future::Future<Output = ()>,
    SpawnFn: FnMut(quinn::Incoming) -> Fut,
    Fut: std::future::Future<Output = ()> + Send + 'static,
{
    let mut shutdown = std::pin::pin!(shutdown);
    let mut connection_tasks = ConnectionTasks::new();
    loop {
        tokio::select! {
            accepted = endpoint.accept() => {
                let Some(connecting) = accepted else {
                    break Ok(());
                };
                connection_tasks.spawn_unit(spawn_connection(connecting));
            }
            _ = &mut shutdown => {
                connection_tasks.abort_all().await;
                break Ok(());
            }
        }
    }
}

#[derive(Clone)]
pub struct QuicFrameClient {
    endpoint: Endpoint,
    remote: QuicEndpoint,
}

impl QuicFrameClient {
    pub fn new(remote: QuicEndpoint) -> Result<Self, QuicTransportError> {
        let insecure_tls = QuicClientTlsConfig::insecure();
        Self::with_tls(remote, &insecure_tls)
    }

    pub fn with_tls(
        remote: QuicEndpoint,
        tls: &QuicClientTlsConfig,
    ) -> Result<Self, QuicTransportError> {
        install_rustls_crypto_provider();
        let mut endpoint = Endpoint::client(loopback_ephemeral_socket_addr())
            .map_err(|err| QuicTransportError::BindFailed(err.to_string()))?;
        endpoint.set_default_client_config(build_client_config(tls)?);
        Ok(Self { endpoint, remote })
    }

    pub async fn send(&self, frame: QuicFrame) -> Result<QuicFrame, QuicTransportError> {
        let server_name = self
            .remote
            .server_name
            .clone()
            .unwrap_or_else(|| DEFAULT_TRANSPORT_SERVER_NAME.to_owned());
        let remote_addr: SocketAddr = format!("{}:{}", self.remote.host, self.remote.port)
            .parse()
            .map_err(|err: std::net::AddrParseError| {
                QuicTransportError::ConnectFailed(err.to_string())
            })?;
        let connection = self
            .endpoint
            .connect(remote_addr, &server_name)
            .map_err(|err| QuicTransportError::ConnectFailed(err.to_string()))?
            .await
            .map_err(|err| QuicTransportError::ConnectFailed(err.to_string()))?;

        send_frame_over_connection(&connection, frame).await
    }
}

fn generate_insecure_self_signed_tls() -> Result<QuicServerTlsConfig, QuicTransportError> {
    let cert = generate_simple_self_signed(
        DEFAULT_QUIC_INSECURE_SELF_SIGNED_SAN_HOSTS
            .iter()
            .map(|host| (*host).to_owned())
            .collect::<Vec<_>>(),
    )
    .map_err(|err| QuicTransportError::Certificate(err.to_string()))?;
    Ok(QuicServerTlsConfig::new(
        cert.cert.pem().into_bytes(),
        cert.signing_key.serialize_pem().into_bytes(),
    ))
}

fn build_server_config(tls: &QuicServerTlsConfig) -> Result<ServerConfig, QuicTransportError> {
    let cert_chain = parse_cert_chain(&tls.cert_pem, "QUIC TLS certificate PEM")
        .map_err(QuicTransportError::Certificate)?;
    let key_der = parse_private_key(&tls.key_pem, "QUIC TLS key PEM")
        .map_err(QuicTransportError::Certificate)?;
    let crypto = rustls::ServerConfig::builder();
    let crypto = match &tls.client_auth {
        QuicServerClientAuth::Disabled => {
            crypto.with_client_cert_verifier(rustls::server::WebPkiClientVerifier::no_client_auth())
        }
        QuicServerClientAuth::Optional {
            trusted_client_roots_pem,
        } => crypto.with_client_cert_verifier(
            build_client_verifier(
                trusted_client_roots_pem.iter().cloned(),
                true,
                "QUIC TLS trusted client roots",
            )
            .map_err(QuicTransportError::Certificate)?,
        ),
        QuicServerClientAuth::Required {
            trusted_client_roots_pem,
        } => crypto.with_client_cert_verifier(
            build_client_verifier(
                trusted_client_roots_pem.iter().cloned(),
                false,
                "QUIC TLS trusted client roots",
            )
            .map_err(QuicTransportError::Certificate)?,
        ),
    };
    let crypto = crypto
        .with_single_cert(cert_chain, key_der)
        .map_err(|err| QuicTransportError::Certificate(err.to_string()))?;
    let mut server_config = ServerConfig::with_crypto(Arc::new(
        quinn::crypto::rustls::QuicServerConfig::try_from(crypto)
            .map_err(|err| QuicTransportError::Certificate(err.to_string()))?,
    ));
    server_config.transport = Arc::new(TransportConfig::default());
    Ok(server_config)
}

fn build_client_config(tls: &QuicClientTlsConfig) -> Result<ClientConfig, QuicTransportError> {
    let rustls_config = if tls.root_cert_pem.is_empty() {
        rustls::ClientConfig::builder()
            .dangerous()
            .with_custom_certificate_verifier(Arc::new(InsecureServerVerifier))
            .with_no_client_auth()
    } else {
        let roots: RootCertStore =
            root_store_from_pem(&tls.root_cert_pem, "QUIC TLS root certificate PEM")
                .map_err(QuicTransportError::Certificate)?;
        let builder = rustls::ClientConfig::builder().with_root_certificates(roots);
        match (&tls.client_cert_pem, &tls.client_key_pem) {
            (Some(cert_pem), Some(key_pem)) => builder
                .with_client_auth_cert(
                    parse_cert_chain(cert_pem, "QUIC TLS client certificate PEM")
                        .map_err(QuicTransportError::Certificate)?,
                    parse_private_key(key_pem, "QUIC TLS client key PEM")
                        .map_err(QuicTransportError::Certificate)?,
                )
                .map_err(|err| QuicTransportError::Certificate(err.to_string()))?,
            (None, None) => builder.with_no_client_auth(),
            _ => {
                return Err(QuicTransportError::Certificate(
                    "QUIC client identity requires both certificate and key".into(),
                ));
            }
        }
    };
    Ok(ClientConfig::new(Arc::new(
        quinn::crypto::rustls::QuicClientConfig::try_from(rustls_config)
            .map_err(|err| QuicTransportError::Certificate(err.to_string()))?,
    )))
}

async fn handle_connection(
    connection: Result<Connection, quinn::ConnectionError>,
    handler: Arc<dyn QuicFrameHandler>,
) -> Result<(), QuicTransportError> {
    let connection = connection.map_err(|err| QuicTransportError::AcceptFailed(err.to_string()))?;
    let (mut send, mut recv) = connection
        .accept_bi()
        .await
        .map_err(|err| QuicTransportError::AcceptFailed(err.to_string()))?;
    let request = recv
        .read_to_end(usize::MAX)
        .await
        .map_err(|err| QuicTransportError::ReadFailed(err.to_string()))?;
    let codec = QuicCodec;
    let frame = codec.decode_frame(&request)?;
    let response = handler.handle_frame(frame)?;
    let bytes = codec.encode_frame(&response)?;
    send.write_all(&bytes)
        .await
        .map_err(|err| QuicTransportError::WriteFailed(err.to_string()))?;
    send.finish()
        .map_err(|err| QuicTransportError::WriteFailed(err.to_string()))?;
    let _ = send.stopped().await;
    Ok(())
}

async fn send_frame_over_connection(
    connection: &Connection,
    frame: QuicFrame,
) -> Result<QuicFrame, QuicTransportError> {
    let (mut send, mut recv) = connection
        .open_bi()
        .await
        .map_err(|err| QuicTransportError::OpenStreamFailed(err.to_string()))?;
    let codec = QuicCodec;
    let bytes = codec.encode_frame(&frame)?;
    send.write_all(&bytes)
        .await
        .map_err(|err| QuicTransportError::WriteFailed(err.to_string()))?;
    send.finish()
        .map_err(|err| QuicTransportError::WriteFailed(err.to_string()))?;
    let _ = send.stopped().await;
    let response = recv
        .read_to_end(usize::MAX)
        .await
        .map_err(|err| QuicTransportError::ReadFailed(err.to_string()))?;
    codec.decode_frame(&response).map_err(Into::into)
}

#[derive(Debug)]
struct InsecureServerVerifier;

impl ServerCertVerifier for InsecureServerVerifier {
    fn verify_server_cert(
        &self,
        _end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _server_name: &ServerName<'_>,
        _ocsp_response: &[u8],
        _now: UnixTime,
    ) -> Result<ServerCertVerified, rustls::Error> {
        Ok(ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        _message: &[u8],
        _cert: &CertificateDer<'_>,
        _dss: &rustls::DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        Ok(HandshakeSignatureValid::assertion())
    }

    fn verify_tls13_signature(
        &self,
        _message: &[u8],
        _cert: &CertificateDer<'_>,
        _dss: &rustls::DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        Ok(HandshakeSignatureValid::assertion())
    }

    fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
        vec![
            rustls::SignatureScheme::ECDSA_NISTP256_SHA256,
            rustls::SignatureScheme::RSA_PSS_SHA256,
            rustls::SignatureScheme::RSA_PKCS1_SHA256,
            rustls::SignatureScheme::ED25519,
        ]
    }
}

#[cfg(test)]
mod tests;
