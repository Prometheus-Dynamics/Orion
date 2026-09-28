use std::{future::Future, pin::Pin};

use crate::{
    ControlRoute, HttpCodec, HttpRequest, HttpRequestPayload, HttpResponse, HttpResponsePayload,
    HttpTransportError,
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
