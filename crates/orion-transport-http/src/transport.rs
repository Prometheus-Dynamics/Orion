use std::{future::Future, net::SocketAddr, pin::Pin, sync::Arc};

use axum::{
    Router,
    body::{Body, to_bytes},
    extract::{Request, State},
    http::{Method, StatusCode, header},
    response::{IntoResponse, Response},
    routing::{get, post},
};
use hyper::server::conn::http1;
use hyper_util::rt::TokioIo;
use hyper_util::service::TowerToHyperService;
use orion_transport_common::{
    ConnectionTasks, DEFAULT_HTTP_CLIENT_CONNECT_TIMEOUT, DEFAULT_HTTP_CLIENT_REQUEST_TIMEOUT,
    DEFAULT_MAX_TRANSPORT_PAYLOAD_BYTES, DEFAULT_TRANSPORT_IO_TIMEOUT,
    DEFAULT_TRANSPORT_MAX_CONCURRENT_CONNECTIONS,
};
use tokio::sync::Semaphore;
use tokio_rustls::TlsAcceptor;

use crate::{
    ControlRoute, HttpCodec, HttpRequest, HttpRequestPayload, HttpResponse, HttpResponsePayload,
    HttpTransportError,
    tls::{
        CachedTlsAcceptor, HttpClientTlsConfig, HttpServerTlsConfig, cached_tls_acceptor,
        install_crypto_provider,
    },
};

pub const METRICS_PATH: &str = "/metrics";

pub trait HttpService {
    fn handle(&self, request: HttpRequest) -> HttpResponse;
}

/// Synchronous HTTP control handler boundary.
///
/// Implementations are called from async server tasks, so request handling should stay CPU-cheap
/// and route persistence, filesystem, or other blocking work through an explicit worker boundary.
pub trait HttpControlHandler: Send + Sync + 'static {
    fn handle_payload(
        &self,
        payload: HttpRequestPayload,
    ) -> Result<HttpResponsePayload, HttpTransportError>;

    fn handle_payload_async(
        &self,
        payload: HttpRequestPayload,
    ) -> Pin<Box<dyn Future<Output = Result<HttpResponsePayload, HttpTransportError>> + Send + '_>>
    {
        Box::pin(async move { self.handle_payload(payload) })
    }

    fn handle_payload_metered_async(
        &self,
        payload: HttpRequestPayload,
        _bytes_received: u64,
    ) -> Pin<Box<dyn Future<Output = Result<HttpResponsePayload, HttpTransportError>> + Send + '_>>
    {
        self.handle_payload_async(payload)
    }

    fn handle_health(&self) -> Result<HttpResponsePayload, HttpTransportError> {
        Err(HttpTransportError::UnsupportedPath(
            ControlRoute::Health.path().to_owned(),
        ))
    }

    fn handle_health_async(
        &self,
    ) -> Pin<Box<dyn Future<Output = Result<HttpResponsePayload, HttpTransportError>> + Send + '_>>
    {
        Box::pin(async move { self.handle_health() })
    }

    fn handle_readiness(&self) -> Result<HttpResponsePayload, HttpTransportError> {
        Err(HttpTransportError::UnsupportedPath(
            ControlRoute::Readiness.path().to_owned(),
        ))
    }

    fn handle_readiness_async(
        &self,
    ) -> Pin<Box<dyn Future<Output = Result<HttpResponsePayload, HttpTransportError>> + Send + '_>>
    {
        Box::pin(async move { self.handle_readiness() })
    }

    fn handle_metrics(&self) -> Result<String, HttpTransportError> {
        Err(HttpTransportError::UnsupportedPath(METRICS_PATH.to_owned()))
    }

    fn handle_metrics_async(
        &self,
    ) -> Pin<Box<dyn Future<Output = Result<String, HttpTransportError>> + Send + '_>> {
        Box::pin(async move { self.handle_metrics() })
    }

    fn record_transport_error(&self, _error: &HttpTransportError) {}

    fn record_control_exchange(
        &self,
        _id: &str,
        _scope: &str,
        _bytes_received: u64,
        _bytes_sent: u64,
        _duration: std::time::Duration,
    ) {
    }
}

#[derive(Clone, Debug, Default)]
pub struct HttpTransport {
    codec: HttpCodec,
}

impl HttpTransport {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn codec(&self) -> &HttpCodec {
        &self.codec
    }

    pub fn send<S: HttpService>(
        &self,
        service: &S,
        payload: &HttpRequestPayload,
    ) -> Result<HttpResponsePayload, HttpTransportError> {
        let request = self.codec.encode_request(payload)?;
        let response = service.handle(request);
        self.codec.decode_response(&response)
    }
}

#[derive(Clone, Debug)]
pub struct HttpClient {
    base_url: String,
    client: reqwest::Client,
    codec: HttpCodec,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum BaseUrlScheme {
    Http,
    Https,
}

impl HttpClient {
    /// Builds an HTTP client without panicking on TLS/client-builder failures.
    pub fn try_new(base_url: impl Into<String>) -> Result<Self, HttpTransportError> {
        Self::with_optional_tls(base_url, None)
    }

    pub fn with_tls(
        base_url: impl Into<String>,
        tls: HttpClientTlsConfig,
    ) -> Result<Self, HttpTransportError> {
        Self::with_optional_tls(base_url, Some(tls))
    }

    pub fn with_dangerous_tls(base_url: impl Into<String>) -> Result<Self, HttpTransportError> {
        install_crypto_provider();
        let client = default_http_client_builder()
            .danger_accept_invalid_certs(true)
            .build()
            .map_err(|err| HttpTransportError::Tls(err.to_string()))?;
        Ok(Self {
            base_url: base_url.into().trim_end_matches('/').to_owned(),
            client,
            codec: HttpCodec,
        })
    }

    fn with_optional_tls(
        base_url: impl Into<String>,
        tls: Option<HttpClientTlsConfig>,
    ) -> Result<Self, HttpTransportError> {
        install_crypto_provider();
        let (base_url, scheme) = normalize_base_url(base_url.into())?;
        let mut builder = default_http_client_builder();
        if scheme == BaseUrlScheme::Http {
            if tls.is_some() {
                return Err(HttpTransportError::Tls(
                    "plain HTTP targets do not use TLS configuration; remove TLS material or switch to https://"
                        .into(),
                ));
            }
            // Reqwest initializes the rustls platform verifier at client-build time even for
            // plain-HTTP clients unless the client is configured to use only explicit roots.
            // Give the builder an empty explicit root set so stripped rootfs images do not need
            // a system trust store just to construct an http:// client.
            builder = builder.tls_certs_only(Vec::<reqwest::Certificate>::new());
        }
        if let Some(tls) = tls {
            let cert = reqwest::Certificate::from_pem(&tls.root_cert_pem)
                .map_err(|err| HttpTransportError::Tls(err.to_string()))?;
            // Use only the caller-provided roots instead of treating them as "extra" roots.
            // On minimal images this avoids reqwest's platform-verifier initialization path.
            builder = builder.tls_certs_only([cert]);
            if let (Some(client_cert_pem), Some(client_key_pem)) =
                (tls.client_cert_pem, tls.client_key_pem)
            {
                let mut identity_pem = client_cert_pem;
                if !identity_pem.ends_with(b"\n") {
                    identity_pem.push(b'\n');
                }
                identity_pem.extend_from_slice(&client_key_pem);
                let identity = reqwest::Identity::from_pem(&identity_pem)
                    .map_err(|err| HttpTransportError::Tls(err.to_string()))?;
                builder = builder.identity(identity);
            }
        }

        Ok(Self {
            base_url,
            client: builder
                .build()
                .map_err(|err| HttpTransportError::Tls(err.to_string()))?,
            codec: HttpCodec,
        })
    }

    pub async fn send(
        &self,
        payload: &HttpRequestPayload,
    ) -> Result<HttpResponsePayload, HttpTransportError> {
        let request = self.codec.encode_request(payload)?;
        let method = match request.method {
            crate::HttpMethod::Post => Method::POST,
            crate::HttpMethod::Get => Method::GET,
        };

        let response = self
            .client
            .request(method, format!("{}{}", self.base_url, request.path))
            .body(request.body)
            .send()
            .await
            .map_err(HttpTransportError::request_failed_from_reqwest)?;

        let status = response.status().as_u16();
        let body = response
            .bytes()
            .await
            .map_err(HttpTransportError::request_failed_from_reqwest)?;

        self.codec.decode_response(&HttpResponse {
            status,
            body: body.to_vec(),
        })
    }

    pub async fn get_route(
        &self,
        route: ControlRoute,
    ) -> Result<HttpResponsePayload, HttpTransportError> {
        if route.method() != crate::HttpMethod::Get {
            return Err(HttpTransportError::UnsupportedMethod(
                route.method().as_str().to_owned(),
            ));
        }

        let response = self
            .client
            .request(Method::GET, format!("{}{}", self.base_url, route.path()))
            .send()
            .await
            .map_err(HttpTransportError::request_failed_from_reqwest)?;

        let status = response.status().as_u16();
        let body = response
            .bytes()
            .await
            .map_err(HttpTransportError::request_failed_from_reqwest)?;

        self.codec.decode_response(&HttpResponse {
            status,
            body: body.to_vec(),
        })
    }
}

fn default_http_client_builder() -> reqwest::ClientBuilder {
    reqwest::Client::builder()
        .connect_timeout(DEFAULT_HTTP_CLIENT_CONNECT_TIMEOUT)
        .timeout(DEFAULT_HTTP_CLIENT_REQUEST_TIMEOUT)
}

fn normalize_base_url(base_url: String) -> Result<(String, BaseUrlScheme), HttpTransportError> {
    let parsed = reqwest::Url::parse(&base_url)
        .map_err(|err| HttpTransportError::InvalidBaseUrl(err.to_string()))?;
    let scheme = match parsed.scheme() {
        "http" => BaseUrlScheme::Http,
        "https" => BaseUrlScheme::Https,
        other => {
            return Err(HttpTransportError::InvalidBaseUrl(format!(
                "unsupported scheme `{other}`; expected http:// or https://"
            )));
        }
    };
    Ok((parsed.as_str().trim_end_matches('/').to_owned(), scheme))
}

#[derive(Clone)]
pub struct HttpServer {
    service: Arc<dyn HttpControlHandler>,
    surface: HttpServerSurface,
    max_body_bytes: usize,
    io_timeout: std::time::Duration,
    max_connections: usize,
}

#[derive(Clone, Copy)]
enum HttpServerSurface {
    Control,
    Probe,
}

#[derive(Clone)]
struct HttpServerState {
    service: Arc<dyn HttpControlHandler>,
    max_body_bytes: usize,
    io_timeout: std::time::Duration,
}

#[derive(Clone, Copy)]
struct HttpTlsServeConfig {
    io_timeout: std::time::Duration,
    max_connections: usize,
}

impl HttpServer {
    pub fn new(service: Arc<dyn HttpControlHandler>) -> Self {
        Self {
            service,
            surface: HttpServerSurface::Control,
            max_body_bytes: DEFAULT_MAX_TRANSPORT_PAYLOAD_BYTES,
            io_timeout: DEFAULT_TRANSPORT_IO_TIMEOUT,
            max_connections: DEFAULT_TRANSPORT_MAX_CONCURRENT_CONNECTIONS,
        }
    }

    pub fn probe(service: Arc<dyn HttpControlHandler>) -> Self {
        Self {
            service,
            surface: HttpServerSurface::Probe,
            max_body_bytes: DEFAULT_MAX_TRANSPORT_PAYLOAD_BYTES,
            io_timeout: DEFAULT_TRANSPORT_IO_TIMEOUT,
            max_connections: DEFAULT_TRANSPORT_MAX_CONCURRENT_CONNECTIONS,
        }
    }

    pub fn with_max_body_bytes(mut self, max_body_bytes: usize) -> Self {
        self.max_body_bytes = max_body_bytes.max(1);
        self
    }

    pub fn with_io_timeout(mut self, io_timeout: std::time::Duration) -> Self {
        self.io_timeout = io_timeout.max(std::time::Duration::from_millis(1));
        self
    }

    pub fn with_max_connections(mut self, max_connections: usize) -> Self {
        self.max_connections = max_connections.max(1);
        self
    }

    pub fn router(&self) -> Router {
        let router = match self.surface {
            HttpServerSurface::Control => Router::new()
                .route(ControlRoute::Hello.path(), post(handle_http_request))
                .route(ControlRoute::Sync.path(), post(handle_http_request))
                .route(ControlRoute::Snapshot.path(), post(handle_http_request))
                .route(ControlRoute::Mutations.path(), post(handle_http_request))
                .route(
                    ControlRoute::Observability.path(),
                    post(handle_http_request),
                )
                .route(
                    ControlRoute::ObservedUpdate.path(),
                    post(handle_http_request),
                ),
            HttpServerSurface::Probe => {
                Router::new().route(METRICS_PATH, get(handle_metrics_request))
            }
        };

        router
            .route(ControlRoute::Health.path(), get(handle_health_request))
            .route(
                ControlRoute::Readiness.path(),
                get(handle_readiness_request),
            )
            .with_state(HttpServerState {
                service: self.service.clone(),
                max_body_bytes: self.max_body_bytes,
                io_timeout: self.io_timeout,
            })
    }

    pub async fn serve(self, listener: tokio::net::TcpListener) -> Result<(), HttpTransportError> {
        axum::serve(listener, self.router())
            .await
            .map_err(|err| HttpTransportError::ServeFailed(err.to_string()))
    }

    pub async fn serve_with_shutdown<F>(
        self,
        listener: tokio::net::TcpListener,
        shutdown: F,
    ) -> Result<(), HttpTransportError>
    where
        F: std::future::Future<Output = ()> + Send + 'static,
    {
        axum::serve(listener, self.router())
            .with_graceful_shutdown(shutdown)
            .into_future()
            .await
            .map_err(|err| HttpTransportError::ServeFailed(err.to_string()))
    }

    pub async fn serve_tls(
        self,
        listener: tokio::net::TcpListener,
        tls: HttpServerTlsConfig,
    ) -> Result<(), HttpTransportError> {
        let app = self.router();
        let handler = self.service.clone();
        let config = HttpTlsServeConfig {
            io_timeout: self.io_timeout,
            max_connections: self.max_connections,
        };
        let mut cached_acceptor: Option<CachedTlsAcceptor> = None;
        serve_tls_accept_loop(listener, &tls, &mut cached_acceptor, handler, app, config).await
    }

    pub async fn serve_tls_with_shutdown<F>(
        self,
        listener: tokio::net::TcpListener,
        tls: HttpServerTlsConfig,
        shutdown: F,
    ) -> Result<(), HttpTransportError>
    where
        F: std::future::Future<Output = ()>,
    {
        let app = self.router();
        let handler = self.service.clone();
        let config = HttpTlsServeConfig {
            io_timeout: self.io_timeout,
            max_connections: self.max_connections,
        };
        let mut cached_acceptor: Option<CachedTlsAcceptor> = None;
        serve_tls_accept_loop_with_shutdown(
            listener,
            &tls,
            &mut cached_acceptor,
            handler,
            app,
            config,
            shutdown,
        )
        .await
    }

    pub async fn bind(
        addr: SocketAddr,
        service: Arc<dyn HttpControlHandler>,
    ) -> Result<(SocketAddr, Self, tokio::net::TcpListener), HttpTransportError> {
        let listener = tokio::net::TcpListener::bind(addr)
            .await
            .map_err(|err| HttpTransportError::BindFailed(err.to_string()))?;
        let local_addr = listener
            .local_addr()
            .map_err(|err| HttpTransportError::BindFailed(err.to_string()))?;
        Ok((local_addr, Self::new(service), listener))
    }

    pub async fn bind_probe(
        addr: SocketAddr,
        service: Arc<dyn HttpControlHandler>,
    ) -> Result<(SocketAddr, Self, tokio::net::TcpListener), HttpTransportError> {
        let listener = tokio::net::TcpListener::bind(addr)
            .await
            .map_err(|err| HttpTransportError::BindFailed(err.to_string()))?;
        let local_addr = listener
            .local_addr()
            .map_err(|err| HttpTransportError::BindFailed(err.to_string()))?;
        Ok((local_addr, Self::probe(service), listener))
    }
}

async fn serve_tls_accept_loop(
    listener: tokio::net::TcpListener,
    tls: &HttpServerTlsConfig,
    cached_acceptor: &mut Option<CachedTlsAcceptor>,
    handler: Arc<dyn HttpControlHandler>,
    app: Router,
    config: HttpTlsServeConfig,
) -> Result<(), HttpTransportError> {
    let semaphore = Arc::new(Semaphore::new(config.max_connections.max(1)));
    loop {
        let permit = semaphore
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| HttpTransportError::ServeFailed("connection limiter closed".into()))?;
        let (stream, _) = listener
            .accept()
            .await
            .map_err(|err| HttpTransportError::ServeFailed(err.to_string()))?;
        let acceptor = cached_tls_acceptor(tls, cached_acceptor)?;
        let service = app.clone();
        let handler = handler.clone();
        tokio::spawn(async move {
            let _permit = permit;
            serve_tls_connection(stream, acceptor, handler, service, config.io_timeout).await;
        });
    }
}

async fn serve_tls_accept_loop_with_shutdown<F>(
    listener: tokio::net::TcpListener,
    tls: &HttpServerTlsConfig,
    cached_acceptor: &mut Option<CachedTlsAcceptor>,
    handler: Arc<dyn HttpControlHandler>,
    app: Router,
    config: HttpTlsServeConfig,
    shutdown: F,
) -> Result<(), HttpTransportError>
where
    F: std::future::Future<Output = ()>,
{
    let mut shutdown = std::pin::pin!(shutdown);
    let mut connection_tasks = ConnectionTasks::new();
    let semaphore = Arc::new(Semaphore::new(config.max_connections.max(1)));
    loop {
        let permit = tokio::select! {
            permit = semaphore.clone().acquire_owned() => {
                permit.map_err(|_| HttpTransportError::ServeFailed("connection limiter closed".into()))?
            }
            _ = &mut shutdown => {
                connection_tasks.abort_all().await;
                break Ok(());
            }
        };
        tokio::select! {
            accepted = listener.accept() => {
                let (stream, _) = accepted
                    .map_err(|err| HttpTransportError::ServeFailed(err.to_string()))?;
                let acceptor = cached_tls_acceptor(tls, cached_acceptor)?;
                let service = app.clone();
                let handler = handler.clone();
                connection_tasks.spawn_unit(async move {
                    let _permit = permit;
                    serve_tls_connection(stream, acceptor, handler, service, config.io_timeout).await;
                });
            }
            _ = &mut shutdown => {
                connection_tasks.abort_all().await;
                break Ok(());
            }
        }
    }
}

async fn serve_tls_connection(
    stream: tokio::net::TcpStream,
    acceptor: TlsAcceptor,
    handler: Arc<dyn HttpControlHandler>,
    service: Router,
    io_timeout: std::time::Duration,
) {
    let tls_stream = match tokio::time::timeout(
        io_timeout.max(std::time::Duration::from_millis(1)),
        acceptor.accept(stream),
    )
    .await
    {
        Ok(Ok(stream)) => stream,
        Ok(Err(err)) => {
            handler.record_transport_error(&HttpTransportError::Tls(format!(
                "tls handshake failed: {err}"
            )));
            return;
        }
        Err(_) => {
            handler.record_transport_error(&HttpTransportError::Tls(format!(
                "tls handshake timed out after {} ms",
                io_timeout.as_millis()
            )));
            return;
        }
    };
    let io = TokioIo::new(tls_stream);
    let service = TowerToHyperService::new(service);
    let _ = http1::Builder::new()
        .serve_connection(io, service)
        .with_upgrades()
        .await;
}

async fn handle_http_request(
    State(state): State<HttpServerState>,
    request: Request<Body>,
) -> Response {
    let started = std::time::Instant::now();
    let (parts, body) = request.into_parts();
    let body =
        match tokio::time::timeout(state.io_timeout, to_bytes(body, state.max_body_bytes)).await {
            Ok(Ok(body)) => body.to_vec(),
            Ok(Err(err)) => return (StatusCode::BAD_REQUEST, err.to_string()).into_response(),
            Err(_) => {
                return (
                    StatusCode::REQUEST_TIMEOUT,
                    format!(
                        "HTTP request body timed out after {} ms",
                        state.io_timeout.as_millis()
                    ),
                )
                    .into_response();
            }
        };
    let bytes_received = body.len().min(u64::MAX as usize) as u64;
    let service = state.service;

    let codec = HttpCodec;
    let method = match crate::HttpMethod::from_http_name(parts.method.as_str()) {
        Some(method) => method,
        None => {
            let err = HttpTransportError::UnsupportedMethod(parts.method.as_str().to_owned());
            service.record_transport_error(&err);
            return (StatusCode::BAD_REQUEST, err.to_string()).into_response();
        }
    };
    let payload = match codec.decode_request(&HttpRequest {
        method,
        path: parts.uri.path().to_owned(),
        body,
    }) {
        Ok(payload) => payload,
        Err(err) => {
            service.record_transport_error(&err);
            return (StatusCode::BAD_REQUEST, err.to_string()).into_response();
        }
    };

    let response_payload = match service
        .handle_payload_metered_async(payload, bytes_received)
        .await
    {
        Ok(response) => response,
        Err(err) => {
            service.record_transport_error(&err);
            return (StatusCode::INTERNAL_SERVER_ERROR, err.to_string()).into_response();
        }
    };

    let response = match codec.encode_response(&response_payload) {
        Ok(response) => response,
        Err(err) => {
            service.record_transport_error(&err);
            return (StatusCode::INTERNAL_SERVER_ERROR, err.to_string()).into_response();
        }
    };
    let bytes_sent = response.body.len().min(u64::MAX as usize) as u64;
    service.record_control_exchange(
        "http/control",
        "control",
        bytes_received,
        bytes_sent,
        started.elapsed(),
    );

    (
        StatusCode::from_u16(response.status).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR),
        response.body,
    )
        .into_response()
}

async fn handle_health_request(State(state): State<HttpServerState>) -> Response {
    let started = std::time::Instant::now();
    let service = state.service;
    let codec = HttpCodec;
    let response_payload = match service.handle_health_async().await {
        Ok(response) => response,
        Err(err) => return (StatusCode::INTERNAL_SERVER_ERROR, err.to_string()).into_response(),
    };

    let response = match codec.encode_response(&response_payload) {
        Ok(response) => response,
        Err(err) => return (StatusCode::INTERNAL_SERVER_ERROR, err.to_string()).into_response(),
    };
    service.record_control_exchange(
        "http/health",
        "health",
        0,
        response.body.len().min(u64::MAX as usize) as u64,
        started.elapsed(),
    );

    (
        StatusCode::from_u16(response.status).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR),
        response.body,
    )
        .into_response()
}

async fn handle_readiness_request(State(state): State<HttpServerState>) -> Response {
    let started = std::time::Instant::now();
    let service = state.service;
    let codec = HttpCodec;
    let response_payload = match service.handle_readiness_async().await {
        Ok(response) => response,
        Err(err) => return (StatusCode::INTERNAL_SERVER_ERROR, err.to_string()).into_response(),
    };

    let response = match codec.encode_response(&response_payload) {
        Ok(response) => response,
        Err(err) => return (StatusCode::INTERNAL_SERVER_ERROR, err.to_string()).into_response(),
    };
    service.record_control_exchange(
        "http/readiness",
        "readiness",
        0,
        response.body.len().min(u64::MAX as usize) as u64,
        started.elapsed(),
    );

    (
        StatusCode::from_u16(response.status).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR),
        response.body,
    )
        .into_response()
}

async fn handle_metrics_request(State(state): State<HttpServerState>) -> Response {
    let service = state.service;
    match service.handle_metrics_async().await {
        Ok(metrics) => (
            StatusCode::OK,
            [(
                header::CONTENT_TYPE,
                "text/plain; version=0.0.4; charset=utf-8",
            )],
            metrics,
        )
            .into_response(),
        Err(err) => {
            service.record_transport_error(&err);
            (StatusCode::INTERNAL_SERVER_ERROR, err.to_string()).into_response()
        }
    }
}
