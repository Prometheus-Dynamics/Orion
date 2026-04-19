use std::{net::SocketAddr, sync::Arc};

use orion_transport_common::{
    ConnectionTasks, build_client_verifier, install_rustls_crypto_provider, parse_cert_chain,
    parse_private_key, root_store_from_pem,
};
use rustls::{RootCertStore, ServerConfig};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
};
use tokio_rustls::{TlsAcceptor, TlsConnector};

use crate::{TcpCodec, TcpFrame, TcpTransportError};

pub trait TcpFrameHandler: Send + Sync + 'static {
    fn handle_frame(&self, frame: TcpFrame) -> Result<TcpFrame, TcpTransportError>;
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TcpClientTlsConfig {
    pub root_cert_pem: Vec<u8>,
    pub client_cert_pem: Option<Vec<u8>>,
    pub client_key_pem: Option<Vec<u8>>,
    pub server_name: String,
}

impl TcpClientTlsConfig {
    pub fn new(root_cert_pem: Vec<u8>, server_name: impl Into<String>) -> Self {
        Self {
            root_cert_pem,
            client_cert_pem: None,
            client_key_pem: None,
            server_name: server_name.into(),
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
pub enum TcpServerClientAuth {
    Disabled,
    Optional {
        trusted_client_roots_pem: Vec<Vec<u8>>,
    },
    Required {
        trusted_client_roots_pem: Vec<Vec<u8>>,
    },
}

#[derive(Clone)]
pub struct TcpServerTlsConfig {
    pub cert_pem: Vec<u8>,
    pub key_pem: Vec<u8>,
    pub client_auth: TcpServerClientAuth,
}

impl TcpServerTlsConfig {
    pub fn new(cert_pem: Vec<u8>, key_pem: Vec<u8>) -> Self {
        Self {
            cert_pem,
            key_pem,
            client_auth: TcpServerClientAuth::Disabled,
        }
    }

    pub fn with_client_auth(mut self, client_auth: TcpServerClientAuth) -> Self {
        self.client_auth = client_auth;
        self
    }
}

pub struct TcpFrameServer {
    handler: Arc<dyn TcpFrameHandler>,
}

impl TcpFrameServer {
    pub fn new(handler: Arc<dyn TcpFrameHandler>) -> Self {
        Self { handler }
    }

    pub async fn bind(
        addr: impl tokio::net::ToSocketAddrs,
        handler: Arc<dyn TcpFrameHandler>,
    ) -> Result<(SocketAddr, Self, TcpListener), TcpTransportError> {
        let listener = TcpListener::bind(addr)
            .await
            .map_err(|err| TcpTransportError::BindFailed(err.to_string()))?;
        let local_addr = listener
            .local_addr()
            .map_err(|err| TcpTransportError::BindFailed(err.to_string()))?;
        Ok((local_addr, Self::new(handler), listener))
    }

    pub async fn serve(self, listener: TcpListener) -> Result<(), TcpTransportError> {
        let handler = self.handler.clone();
        serve_accept_loop(listener, move |stream| {
            let handler = handler.clone();
            async move {
                let _ = handle_stream(stream, handler).await;
            }
        })
        .await
    }

    pub async fn serve_with_shutdown<F>(
        self,
        listener: TcpListener,
        shutdown: F,
    ) -> Result<(), TcpTransportError>
    where
        F: std::future::Future<Output = ()>,
    {
        let handler = self.handler.clone();
        serve_accept_loop_with_shutdown(listener, shutdown, move |stream| {
            let handler = handler.clone();
            async move {
                let _ = handle_stream(stream, handler).await;
            }
        })
        .await
    }

    pub async fn serve_tls(
        self,
        listener: TcpListener,
        tls: TcpServerTlsConfig,
    ) -> Result<(), TcpTransportError> {
        let acceptor = TlsAcceptor::from(Arc::new(build_server_tls_config(&tls)?));
        let handler = self.handler.clone();
        serve_accept_loop(listener, move |stream| {
            let handler = handler.clone();
            let acceptor = acceptor.clone();
            async move {
                let stream = match acceptor.accept(stream).await {
                    Ok(stream) => stream,
                    Err(_) => return,
                };
                let _ = handle_tls_stream(stream, handler).await;
            }
        })
        .await
    }

    pub async fn serve_tls_with_shutdown<F>(
        self,
        listener: TcpListener,
        tls: TcpServerTlsConfig,
        shutdown: F,
    ) -> Result<(), TcpTransportError>
    where
        F: std::future::Future<Output = ()>,
    {
        let acceptor = TlsAcceptor::from(Arc::new(build_server_tls_config(&tls)?));
        let handler = self.handler.clone();
        serve_accept_loop_with_shutdown(listener, shutdown, move |stream| {
            let handler = handler.clone();
            let acceptor = acceptor.clone();
            async move {
                let stream = match acceptor.accept(stream).await {
                    Ok(stream) => stream,
                    Err(_) => return,
                };
                let _ = handle_tls_stream(stream, handler).await;
            }
        })
        .await
    }
}

async fn serve_accept_loop<SpawnFn, Fut>(
    listener: TcpListener,
    mut spawn_connection: SpawnFn,
) -> Result<(), TcpTransportError>
where
    SpawnFn: FnMut(TcpStream) -> Fut,
    Fut: std::future::Future<Output = ()> + Send + 'static,
{
    loop {
        let (stream, _) = listener
            .accept()
            .await
            .map_err(|err| TcpTransportError::AcceptFailed(err.to_string()))?;
        tokio::spawn(spawn_connection(stream));
    }
}

async fn serve_accept_loop_with_shutdown<Shutdown, SpawnFn, Fut>(
    listener: TcpListener,
    shutdown: Shutdown,
    mut spawn_connection: SpawnFn,
) -> Result<(), TcpTransportError>
where
    Shutdown: std::future::Future<Output = ()>,
    SpawnFn: FnMut(TcpStream) -> Fut,
    Fut: std::future::Future<Output = ()> + Send + 'static,
{
    let mut shutdown = std::pin::pin!(shutdown);
    let mut connection_tasks = ConnectionTasks::new();
    loop {
        tokio::select! {
            accepted = listener.accept() => {
                let (stream, _) = accepted
                    .map_err(|err| TcpTransportError::AcceptFailed(err.to_string()))?;
                connection_tasks.spawn_unit(spawn_connection(stream));
            }
            _ = &mut shutdown => {
                connection_tasks.abort_all().await;
                break Ok(());
            }
        }
    }
}

#[derive(Clone, Debug)]
pub struct TcpFrameClient {
    addr: SocketAddr,
    tls: Option<TcpClientTlsConfig>,
}

impl TcpFrameClient {
    pub fn new(addr: SocketAddr) -> Self {
        Self { addr, tls: None }
    }

    pub fn with_tls(addr: SocketAddr, tls: TcpClientTlsConfig) -> Self {
        Self {
            addr,
            tls: Some(tls),
        }
    }

    pub async fn send(&self, frame: TcpFrame) -> Result<TcpFrame, TcpTransportError> {
        match &self.tls {
            Some(tls) => self.send_tls(frame, tls).await,
            None => self.send_plain(frame).await,
        }
    }

    async fn send_plain(&self, frame: TcpFrame) -> Result<TcpFrame, TcpTransportError> {
        let mut stream = TcpStream::connect(self.addr)
            .await
            .map_err(|err| TcpTransportError::ConnectFailed(err.to_string()))?;
        let codec = TcpCodec;
        let bytes = codec.encode_frame(&frame)?;
        stream
            .write_all(&bytes)
            .await
            .map_err(|err| TcpTransportError::WriteFailed(err.to_string()))?;
        stream
            .shutdown()
            .await
            .map_err(|err| TcpTransportError::WriteFailed(err.to_string()))?;

        let mut response = Vec::new();
        stream
            .read_to_end(&mut response)
            .await
            .map_err(|err| TcpTransportError::ReadFailed(err.to_string()))?;
        codec.decode_frame(&response).map_err(Into::into)
    }

    async fn send_tls(
        &self,
        frame: TcpFrame,
        tls: &TcpClientTlsConfig,
    ) -> Result<TcpFrame, TcpTransportError> {
        let stream = TcpStream::connect(self.addr)
            .await
            .map_err(|err| TcpTransportError::ConnectFailed(err.to_string()))?;
        let connector = TlsConnector::from(Arc::new(build_client_tls_config(tls)?));
        let server_name = rustls::pki_types::ServerName::try_from(tls.server_name.clone())
            .map_err(|err| TcpTransportError::Tls(err.to_string()))?;
        let mut stream = connector
            .connect(server_name, stream)
            .await
            .map_err(|err| TcpTransportError::ConnectFailed(err.to_string()))?;
        let codec = TcpCodec;
        let bytes = codec.encode_frame(&frame)?;
        stream
            .write_all(&bytes)
            .await
            .map_err(|err| TcpTransportError::WriteFailed(err.to_string()))?;
        stream
            .shutdown()
            .await
            .map_err(|err| TcpTransportError::WriteFailed(err.to_string()))?;

        let mut response = Vec::new();
        stream
            .read_to_end(&mut response)
            .await
            .map_err(|err| TcpTransportError::ReadFailed(err.to_string()))?;
        codec.decode_frame(&response).map_err(Into::into)
    }
}

async fn handle_stream(
    mut stream: TcpStream,
    handler: Arc<dyn TcpFrameHandler>,
) -> Result<(), TcpTransportError> {
    let mut request = Vec::new();
    stream
        .read_to_end(&mut request)
        .await
        .map_err(|err| TcpTransportError::ReadFailed(err.to_string()))?;

    let codec = TcpCodec;
    let frame = codec.decode_frame(&request)?;
    let response = handler.handle_frame(frame)?;
    let bytes = codec.encode_frame(&response)?;
    stream
        .write_all(&bytes)
        .await
        .map_err(|err| TcpTransportError::WriteFailed(err.to_string()))?;
    stream
        .shutdown()
        .await
        .map_err(|err| TcpTransportError::WriteFailed(err.to_string()))?;
    Ok(())
}

async fn handle_tls_stream(
    mut stream: tokio_rustls::server::TlsStream<TcpStream>,
    handler: Arc<dyn TcpFrameHandler>,
) -> Result<(), TcpTransportError> {
    let mut request = Vec::new();
    stream
        .read_to_end(&mut request)
        .await
        .map_err(|err| TcpTransportError::ReadFailed(err.to_string()))?;

    let codec = TcpCodec;
    let frame = codec.decode_frame(&request)?;
    let response = handler.handle_frame(frame)?;
    let bytes = codec.encode_frame(&response)?;
    stream
        .write_all(&bytes)
        .await
        .map_err(|err| TcpTransportError::WriteFailed(err.to_string()))?;
    stream
        .shutdown()
        .await
        .map_err(|err| TcpTransportError::WriteFailed(err.to_string()))?;
    Ok(())
}

fn build_server_tls_config(tls: &TcpServerTlsConfig) -> Result<ServerConfig, TcpTransportError> {
    install_rustls_crypto_provider();
    let cert_chain = parse_cert_chain(&tls.cert_pem, "TCP TLS certificate PEM")
        .map_err(TcpTransportError::Tls)?;
    let private_key =
        parse_private_key(&tls.key_pem, "TCP TLS key PEM").map_err(TcpTransportError::Tls)?;
    let builder = ServerConfig::builder();
    let builder = match &tls.client_auth {
        TcpServerClientAuth::Disabled => builder
            .with_client_cert_verifier(rustls::server::WebPkiClientVerifier::no_client_auth()),
        TcpServerClientAuth::Optional {
            trusted_client_roots_pem,
        } => builder.with_client_cert_verifier(
            build_client_verifier(
                trusted_client_roots_pem.iter().cloned(),
                true,
                "TCP TLS trusted client roots",
            )
            .map_err(TcpTransportError::Tls)?,
        ),
        TcpServerClientAuth::Required {
            trusted_client_roots_pem,
        } => builder.with_client_cert_verifier(
            build_client_verifier(
                trusted_client_roots_pem.iter().cloned(),
                false,
                "TCP TLS trusted client roots",
            )
            .map_err(TcpTransportError::Tls)?,
        ),
    };
    builder
        .with_single_cert(cert_chain, private_key)
        .map_err(|err| TcpTransportError::Tls(err.to_string()))
}

fn build_client_tls_config(
    tls: &TcpClientTlsConfig,
) -> Result<rustls::ClientConfig, TcpTransportError> {
    install_rustls_crypto_provider();
    let roots: RootCertStore =
        root_store_from_pem(&tls.root_cert_pem, "TCP TLS root certificate PEM")
            .map_err(TcpTransportError::Tls)?;

    let builder = rustls::ClientConfig::builder().with_root_certificates(roots);
    match (&tls.client_cert_pem, &tls.client_key_pem) {
        (Some(cert_pem), Some(key_pem)) => {
            let cert_chain = parse_cert_chain(cert_pem, "TCP TLS client certificate PEM")
                .map_err(TcpTransportError::Tls)?;
            let private_key = parse_private_key(key_pem, "TCP TLS client key PEM")
                .map_err(TcpTransportError::Tls)?;
            builder
                .with_client_auth_cert(cert_chain, private_key)
                .map_err(|err| TcpTransportError::Tls(err.to_string()))
        }
        (None, None) => Ok(builder.with_no_client_auth()),
        _ => Err(TcpTransportError::Tls(
            "TCP TLS client identity requires both certificate and key".into(),
        )),
    }
}
