use std::{net::SocketAddr, sync::Arc};

use axum::{
    Router,
    body::{Body, to_bytes},
    extract::{Request, State},
    http::{Method, StatusCode},
    response::{IntoResponse, Response},
    routing::{get, post},
};
use hyper::server::conn::http1;
use hyper_util::rt::TokioIo;
use hyper_util::service::TowerToHyperService;
use orion_transport_common::{
    ConnectionTasks, DEFAULT_HTTP_CLIENT_CONNECT_TIMEOUT, DEFAULT_HTTP_CLIENT_REQUEST_TIMEOUT,
};
use tokio_rustls::TlsAcceptor;

use crate::{
    ControlRoute, HttpCodec, HttpRequest, HttpRequestPayload, HttpResponse, HttpResponsePayload,
    HttpTransportError,
    tls::{
        CachedTlsAcceptor, HttpClientTlsConfig, HttpServerTlsConfig, cached_tls_acceptor,
        install_crypto_provider,
    },
};

pub trait HttpService {
    fn handle(&self, request: HttpRequest) -> HttpResponse;
}

pub trait HttpControlHandler: Send + Sync + 'static {
    fn handle_payload(
        &self,
        payload: HttpRequestPayload,
    ) -> Result<HttpResponsePayload, HttpTransportError>;

    fn handle_health(&self) -> Result<HttpResponsePayload, HttpTransportError> {
        Err(HttpTransportError::UnsupportedPath(
            ControlRoute::Health.path().to_owned(),
        ))
    }

    fn handle_readiness(&self) -> Result<HttpResponsePayload, HttpTransportError> {
        Err(HttpTransportError::UnsupportedPath(
            ControlRoute::Readiness.path().to_owned(),
        ))
    }

    fn record_transport_error(&self, _error: &HttpTransportError) {}
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
        let mut builder = default_http_client_builder();
        if let Some(tls) = tls {
            let cert = reqwest::Certificate::from_pem(&tls.root_cert_pem)
                .map_err(|err| HttpTransportError::Tls(err.to_string()))?;
            builder = builder.add_root_certificate(cert);
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
            base_url: base_url.into().trim_end_matches('/').to_owned(),
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

#[derive(Clone)]
pub struct HttpServer {
    service: Arc<dyn HttpControlHandler>,
    surface: HttpServerSurface,
}

#[derive(Clone, Copy)]
enum HttpServerSurface {
    Control,
    Probe,
}

impl HttpServer {
    pub fn new(service: Arc<dyn HttpControlHandler>) -> Self {
        Self {
            service,
            surface: HttpServerSurface::Control,
        }
    }

    pub fn probe(service: Arc<dyn HttpControlHandler>) -> Self {
        Self {
            service,
            surface: HttpServerSurface::Probe,
        }
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
            HttpServerSurface::Probe => Router::new(),
        };

        router
            .route(ControlRoute::Health.path(), get(handle_health_request))
            .route(
                ControlRoute::Readiness.path(),
                get(handle_readiness_request),
            )
            .with_state(self.service.clone())
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
        let mut cached_acceptor: Option<CachedTlsAcceptor> = None;
        serve_tls_accept_loop(listener, &tls, &mut cached_acceptor, handler, app).await
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
        let mut cached_acceptor: Option<CachedTlsAcceptor> = None;
        serve_tls_accept_loop_with_shutdown(
            listener,
            &tls,
            &mut cached_acceptor,
            handler,
            app,
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
) -> Result<(), HttpTransportError> {
    loop {
        let (stream, _) = listener
            .accept()
            .await
            .map_err(|err| HttpTransportError::ServeFailed(err.to_string()))?;
        let acceptor = cached_tls_acceptor(tls, cached_acceptor)?;
        let service = app.clone();
        let handler = handler.clone();
        tokio::spawn(async move {
            serve_tls_connection(stream, acceptor, handler, service).await;
        });
    }
}

async fn serve_tls_accept_loop_with_shutdown<F>(
    listener: tokio::net::TcpListener,
    tls: &HttpServerTlsConfig,
    cached_acceptor: &mut Option<CachedTlsAcceptor>,
    handler: Arc<dyn HttpControlHandler>,
    app: Router,
    shutdown: F,
) -> Result<(), HttpTransportError>
where
    F: std::future::Future<Output = ()>,
{
    let mut shutdown = std::pin::pin!(shutdown);
    let mut connection_tasks = ConnectionTasks::new();
    loop {
        tokio::select! {
            accepted = listener.accept() => {
                let (stream, _) = accepted
                    .map_err(|err| HttpTransportError::ServeFailed(err.to_string()))?;
                let acceptor = cached_tls_acceptor(tls, cached_acceptor)?;
                let service = app.clone();
                let handler = handler.clone();
                connection_tasks.spawn_unit(async move {
                    serve_tls_connection(stream, acceptor, handler, service).await;
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
) {
    let tls_stream = match acceptor.accept(stream).await {
        Ok(stream) => stream,
        Err(err) => {
            handler.record_transport_error(&HttpTransportError::Tls(format!(
                "tls handshake failed: {err}"
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
    State(service): State<Arc<dyn HttpControlHandler>>,
    request: Request<Body>,
) -> Response {
    let (parts, body) = request.into_parts();
    let body = match to_bytes(body, usize::MAX).await {
        Ok(body) => body.to_vec(),
        Err(err) => return (StatusCode::BAD_REQUEST, err.to_string()).into_response(),
    };

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

    let response_payload = match service.handle_payload(payload) {
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

    (
        StatusCode::from_u16(response.status).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR),
        response.body,
    )
        .into_response()
}

async fn handle_health_request(State(service): State<Arc<dyn HttpControlHandler>>) -> Response {
    let codec = HttpCodec;
    let response_payload = match service.handle_health() {
        Ok(response) => response,
        Err(err) => return (StatusCode::INTERNAL_SERVER_ERROR, err.to_string()).into_response(),
    };

    let response = match codec.encode_response(&response_payload) {
        Ok(response) => response,
        Err(err) => return (StatusCode::INTERNAL_SERVER_ERROR, err.to_string()).into_response(),
    };

    (
        StatusCode::from_u16(response.status).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR),
        response.body,
    )
        .into_response()
}

async fn handle_readiness_request(State(service): State<Arc<dyn HttpControlHandler>>) -> Response {
    let codec = HttpCodec;
    let response_payload = match service.handle_readiness() {
        Ok(response) => response,
        Err(err) => return (StatusCode::INTERNAL_SERVER_ERROR, err.to_string()).into_response(),
    };

    let response = match codec.encode_response(&response_payload) {
        Ok(response) => response,
        Err(err) => return (StatusCode::INTERNAL_SERVER_ERROR, err.to_string()).into_response(),
    };

    (
        StatusCode::from_u16(response.status).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR),
        response.body,
    )
        .into_response()
}
