use std::{net::SocketAddr, sync::Arc, time::Duration};

use orion_transport_common::{
    ConnectionTasks, DEFAULT_MAX_TRANSPORT_PAYLOAD_BYTES, DEFAULT_TRANSPORT_IO_TIMEOUT,
    DEFAULT_TRANSPORT_MAX_CONCURRENT_CONNECTIONS, build_client_verifier,
    install_rustls_crypto_provider, parse_cert_chain, parse_private_key, root_store_from_pem,
};
use rustls::{RootCertStore, ServerConfig};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::Semaphore,
};
use tokio_rustls::{TlsAcceptor, TlsConnector};

use crate::{TcpCodec, TcpCodecError, TcpFrame, TcpTransportError};

/// Synchronous TCP frame handler boundary.
///
/// Implementations are invoked from async socket tasks. Keep direct work bounded and offload
/// persistence, filesystem, network fanout, or long CPU work through explicit worker APIs.
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
    max_payload_bytes: usize,
    io_timeout: Duration,
    max_connections: usize,
}

impl TcpFrameServer {
    pub fn new(handler: Arc<dyn TcpFrameHandler>) -> Self {
        Self {
            handler,
            max_payload_bytes: DEFAULT_MAX_TRANSPORT_PAYLOAD_BYTES,
            io_timeout: DEFAULT_TRANSPORT_IO_TIMEOUT,
            max_connections: DEFAULT_TRANSPORT_MAX_CONCURRENT_CONNECTIONS,
        }
    }

    pub fn with_max_payload_bytes(mut self, max_payload_bytes: usize) -> Self {
        self.max_payload_bytes = max_payload_bytes.max(1);
        self
    }

    pub fn with_io_timeout(mut self, io_timeout: Duration) -> Self {
        self.io_timeout = io_timeout.max(Duration::from_millis(1));
        self
    }

    pub fn with_max_connections(mut self, max_connections: usize) -> Self {
        self.max_connections = max_connections.max(1);
        self
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
        let max_payload_bytes = self.max_payload_bytes;
        let io_timeout = self.io_timeout;
        let max_connections = self.max_connections;
        serve_accept_loop(
            listener,
            move |stream| {
                let handler = handler.clone();
                let max_payload_bytes = max_payload_bytes;
                let io_timeout = io_timeout;
                async move {
                    let _ = handle_stream(stream, handler, max_payload_bytes, io_timeout).await;
                }
            },
            max_connections,
        )
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
        let max_payload_bytes = self.max_payload_bytes;
        let io_timeout = self.io_timeout;
        let max_connections = self.max_connections;
        serve_accept_loop_with_shutdown(
            listener,
            shutdown,
            move |stream| {
                let handler = handler.clone();
                let max_payload_bytes = max_payload_bytes;
                let io_timeout = io_timeout;
                async move {
                    let _ = handle_stream(stream, handler, max_payload_bytes, io_timeout).await;
                }
            },
            max_connections,
        )
        .await
    }

    pub async fn serve_tls(
        self,
        listener: TcpListener,
        tls: TcpServerTlsConfig,
    ) -> Result<(), TcpTransportError> {
        let acceptor = TlsAcceptor::from(Arc::new(build_server_tls_config(&tls)?));
        let handler = self.handler.clone();
        let max_payload_bytes = self.max_payload_bytes;
        let io_timeout = self.io_timeout;
        let max_connections = self.max_connections;
        serve_accept_loop(
            listener,
            move |stream| {
                let handler = handler.clone();
                let acceptor = acceptor.clone();
                let max_payload_bytes = max_payload_bytes;
                let io_timeout = io_timeout;
                async move {
                    let stream =
                        match timed(io_timeout, acceptor.accept(stream), "TCP TLS handshake").await
                        {
                            Ok(stream) => stream,
                            Err(_) => return,
                        };
                    let _ = handle_tls_stream(stream, handler, max_payload_bytes, io_timeout).await;
                }
            },
            max_connections,
        )
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
        let max_payload_bytes = self.max_payload_bytes;
        let io_timeout = self.io_timeout;
        let max_connections = self.max_connections;
        serve_accept_loop_with_shutdown(
            listener,
            shutdown,
            move |stream| {
                let handler = handler.clone();
                let acceptor = acceptor.clone();
                let max_payload_bytes = max_payload_bytes;
                let io_timeout = io_timeout;
                async move {
                    let stream =
                        match timed(io_timeout, acceptor.accept(stream), "TCP TLS handshake").await
                        {
                            Ok(stream) => stream,
                            Err(_) => return,
                        };
                    let _ = handle_tls_stream(stream, handler, max_payload_bytes, io_timeout).await;
                }
            },
            max_connections,
        )
        .await
    }
}

async fn serve_accept_loop<SpawnFn, Fut>(
    listener: TcpListener,
    spawn_connection: SpawnFn,
    max_connections: usize,
) -> Result<(), TcpTransportError>
where
    SpawnFn: Fn(TcpStream) -> Fut + Clone + Send + 'static,
    Fut: std::future::Future<Output = ()> + Send + 'static,
{
    let semaphore = Arc::new(Semaphore::new(max_connections.max(1)));
    loop {
        let permit = semaphore
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| TcpTransportError::AcceptFailed("connection limiter closed".into()))?;
        let (stream, _) = listener
            .accept()
            .await
            .map_err(|err| TcpTransportError::AcceptFailed(err.to_string()))?;
        let spawn_connection = spawn_connection.clone();
        tokio::spawn(async move {
            let _permit = permit;
            spawn_connection(stream).await;
        });
    }
}

async fn serve_accept_loop_with_shutdown<Shutdown, SpawnFn, Fut>(
    listener: TcpListener,
    shutdown: Shutdown,
    spawn_connection: SpawnFn,
    max_connections: usize,
) -> Result<(), TcpTransportError>
where
    Shutdown: std::future::Future<Output = ()>,
    SpawnFn: Fn(TcpStream) -> Fut + Clone + Send + 'static,
    Fut: std::future::Future<Output = ()> + Send + 'static,
{
    let mut shutdown = std::pin::pin!(shutdown);
    let mut connection_tasks = ConnectionTasks::new();
    let semaphore = Arc::new(Semaphore::new(max_connections.max(1)));
    loop {
        connection_tasks.reap_finished();
        let permit = tokio::select! {
            permit = semaphore.clone().acquire_owned() => {
                permit.map_err(|_| TcpTransportError::AcceptFailed("connection limiter closed".into()))?
            }
            _ = &mut shutdown => {
                connection_tasks.abort_all().await;
                break Ok(());
            }
        };
        tokio::select! {
            accepted = listener.accept() => {
                let (stream, _) = accepted
                    .map_err(|err| TcpTransportError::AcceptFailed(err.to_string()))?;
                let spawn_connection = spawn_connection.clone();
                connection_tasks.spawn_unit(async move {
                    let _permit = permit;
                    spawn_connection(stream).await;
                });
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
    max_payload_bytes: usize,
    io_timeout: Duration,
}

impl TcpFrameClient {
    pub fn new(addr: SocketAddr) -> Self {
        Self {
            addr,
            tls: None,
            max_payload_bytes: DEFAULT_MAX_TRANSPORT_PAYLOAD_BYTES,
            io_timeout: DEFAULT_TRANSPORT_IO_TIMEOUT,
        }
    }

    pub fn with_tls(addr: SocketAddr, tls: TcpClientTlsConfig) -> Self {
        Self {
            addr,
            tls: Some(tls),
            max_payload_bytes: DEFAULT_MAX_TRANSPORT_PAYLOAD_BYTES,
            io_timeout: DEFAULT_TRANSPORT_IO_TIMEOUT,
        }
    }

    pub fn with_max_payload_bytes(mut self, max_payload_bytes: usize) -> Self {
        self.max_payload_bytes = max_payload_bytes.max(1);
        self
    }

    pub fn with_io_timeout(mut self, io_timeout: Duration) -> Self {
        self.io_timeout = io_timeout.max(Duration::from_millis(1));
        self
    }

    pub async fn send(&self, frame: TcpFrame) -> Result<TcpFrame, TcpTransportError> {
        match &self.tls {
            Some(tls) => self.send_tls(frame, tls).await,
            None => self.send_plain(frame).await,
        }
    }

    async fn send_plain(&self, frame: TcpFrame) -> Result<TcpFrame, TcpTransportError> {
        let mut stream = timed(
            self.io_timeout,
            TcpStream::connect(self.addr),
            "TCP connect",
        )
        .await
        .map_err(|err| TcpTransportError::ConnectFailed(err.to_string()))?;
        let codec = TcpCodec;
        let bytes = codec.encode_frame(&frame)?;
        timed(self.io_timeout, stream.write_all(&bytes), "TCP write")
            .await
            .map_err(|err| TcpTransportError::WriteFailed(err.to_string()))?;
        timed(self.io_timeout, stream.shutdown(), "TCP shutdown")
            .await
            .map_err(|err| TcpTransportError::WriteFailed(err.to_string()))?;

        let response =
            read_bounded_frame_bytes(&mut stream, self.max_payload_bytes, self.io_timeout).await?;
        codec.decode_frame(&response).map_err(Into::into)
    }

    async fn send_tls(
        &self,
        frame: TcpFrame,
        tls: &TcpClientTlsConfig,
    ) -> Result<TcpFrame, TcpTransportError> {
        let stream = timed(
            self.io_timeout,
            TcpStream::connect(self.addr),
            "TCP connect",
        )
        .await
        .map_err(|err| TcpTransportError::ConnectFailed(err.to_string()))?;
        let connector = TlsConnector::from(Arc::new(build_client_tls_config(tls)?));
        let server_name = rustls::pki_types::ServerName::try_from(tls.server_name.clone())
            .map_err(|err| TcpTransportError::Tls(err.to_string()))?;
        let mut stream = timed(
            self.io_timeout,
            connector.connect(server_name, stream),
            "TCP TLS handshake",
        )
        .await
        .map_err(|err| TcpTransportError::ConnectFailed(err.to_string()))?;
        let codec = TcpCodec;
        let bytes = codec.encode_frame(&frame)?;
        timed(self.io_timeout, stream.write_all(&bytes), "TCP write")
            .await
            .map_err(|err| TcpTransportError::WriteFailed(err.to_string()))?;
        timed(self.io_timeout, stream.shutdown(), "TCP shutdown")
            .await
            .map_err(|err| TcpTransportError::WriteFailed(err.to_string()))?;

        let response =
            read_bounded_frame_bytes(&mut stream, self.max_payload_bytes, self.io_timeout).await?;
        codec.decode_frame(&response).map_err(Into::into)
    }
}

async fn read_bounded_frame_bytes<R>(
    reader: &mut R,
    max_payload_bytes: usize,
    io_timeout: Duration,
) -> Result<Vec<u8>, TcpTransportError>
where
    R: AsyncReadExt + Unpin,
{
    let max_payload_bytes = max_payload_bytes.max(1);
    let mut bytes = Vec::new();
    timed(
        io_timeout,
        reader
            .take(max_payload_bytes as u64 + 1)
            .read_to_end(&mut bytes),
        "TCP read",
    )
    .await
    .map_err(|err| TcpTransportError::ReadFailed(err.to_string()))?;
    if bytes.len() > max_payload_bytes {
        return Err(TcpTransportError::Codec(TcpCodecError::FrameTooLarge));
    }
    Ok(bytes)
}

async fn handle_stream(
    mut stream: TcpStream,
    handler: Arc<dyn TcpFrameHandler>,
    max_payload_bytes: usize,
    io_timeout: Duration,
) -> Result<(), TcpTransportError> {
    let request = read_bounded_frame_bytes(&mut stream, max_payload_bytes, io_timeout).await?;

    let codec = TcpCodec;
    let frame = codec.decode_frame(&request)?;
    let response = handler.handle_frame(frame)?;
    let bytes = codec.encode_frame(&response)?;
    timed(io_timeout, stream.write_all(&bytes), "TCP write")
        .await
        .map_err(|err| TcpTransportError::WriteFailed(err.to_string()))?;
    timed(io_timeout, stream.shutdown(), "TCP shutdown")
        .await
        .map_err(|err| TcpTransportError::WriteFailed(err.to_string()))?;
    Ok(())
}

async fn handle_tls_stream(
    mut stream: tokio_rustls::server::TlsStream<TcpStream>,
    handler: Arc<dyn TcpFrameHandler>,
    max_payload_bytes: usize,
    io_timeout: Duration,
) -> Result<(), TcpTransportError> {
    let request = read_bounded_frame_bytes(&mut stream, max_payload_bytes, io_timeout).await?;

    let codec = TcpCodec;
    let frame = codec.decode_frame(&request)?;
    let response = handler.handle_frame(frame)?;
    let bytes = codec.encode_frame(&response)?;
    timed(io_timeout, stream.write_all(&bytes), "TCP write")
        .await
        .map_err(|err| TcpTransportError::WriteFailed(err.to_string()))?;
    timed(io_timeout, stream.shutdown(), "TCP shutdown")
        .await
        .map_err(|err| TcpTransportError::WriteFailed(err.to_string()))?;
    Ok(())
}

async fn timed<T>(
    timeout: Duration,
    future: impl std::future::Future<Output = Result<T, impl std::fmt::Display>>,
    context: &str,
) -> Result<T, String> {
    match tokio::time::timeout(timeout.max(Duration::from_millis(1)), future).await {
        Ok(Ok(value)) => Ok(value),
        Ok(Err(err)) => Err(err.to_string()),
        Err(_) => Err(format!(
            "{context} timed out after {} ms",
            timeout.as_millis()
        )),
    }
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

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::AsyncReadExt;

    #[tokio::test]
    async fn bounded_frame_read_rejects_oversized_payload() {
        let mut reader = tokio::io::repeat(0).take(DEFAULT_MAX_TRANSPORT_PAYLOAD_BYTES as u64 + 1);
        let err = read_bounded_frame_bytes(
            &mut reader,
            DEFAULT_MAX_TRANSPORT_PAYLOAD_BYTES,
            DEFAULT_TRANSPORT_IO_TIMEOUT,
        )
        .await
        .expect_err("oversized TCP frame bytes should be rejected");
        assert!(matches!(
            err,
            TcpTransportError::Codec(TcpCodecError::FrameTooLarge)
        ));
    }

    #[tokio::test]
    async fn bounded_frame_read_times_out_stalled_reader() {
        let (_writer, mut reader) = tokio::io::duplex(8);
        let err = read_bounded_frame_bytes(
            &mut reader,
            DEFAULT_MAX_TRANSPORT_PAYLOAD_BYTES,
            std::time::Duration::from_millis(10),
        )
        .await
        .expect_err("stalled TCP reader should time out");
        assert!(
            matches!(err, TcpTransportError::ReadFailed(message) if message.contains("timed out"))
        );
    }
}
