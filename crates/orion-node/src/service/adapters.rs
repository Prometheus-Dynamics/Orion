use super::{ControlRequest, ControlSurface};
use crate::NodeApp;
use orion::{
    ArchiveEncode,
    control_plane::{MetricsExportConfig, render_observability_metrics_with_config},
    encode_to_vec,
    transport::{
        http::{
            HttpCodec, HttpControlHandler, HttpRequestPayload, HttpResponsePayload,
            HttpTransportError,
        },
        ipc::{
            ControlEnvelope, IpcTransportError, LocalAddress, UnixControlHandler, UnixPeerIdentity,
        },
    },
};
use std::{future::Future, pin::Pin, time::Instant};

#[derive(Clone)]
pub(crate) struct HttpControlServiceAdapter {
    app: NodeApp,
}

impl HttpControlServiceAdapter {
    pub(crate) fn new(app: NodeApp) -> Self {
        Self { app }
    }
}

impl HttpControlHandler for HttpControlServiceAdapter {
    fn handle_payload(
        &self,
        payload: HttpRequestPayload,
    ) -> Result<HttpResponsePayload, HttpTransportError> {
        self.handle_payload_sync(payload)
    }

    fn handle_payload_async(
        &self,
        payload: HttpRequestPayload,
    ) -> Pin<Box<dyn Future<Output = Result<HttpResponsePayload, HttpTransportError>> + Send + '_>>
    {
        self.handle_payload_metered_async(payload, 0)
    }

    fn handle_payload_metered_async(
        &self,
        payload: HttpRequestPayload,
        _bytes_received: u64,
    ) -> Pin<Box<dyn Future<Output = Result<HttpResponsePayload, HttpTransportError>> + Send + '_>>
    {
        Box::pin(async move { self.handle_payload_metered_sync(payload) })
    }

    fn handle_health(&self) -> Result<HttpResponsePayload, HttpTransportError> {
        self.handle_health_sync()
    }

    fn handle_health_async(
        &self,
    ) -> Pin<Box<dyn Future<Output = Result<HttpResponsePayload, HttpTransportError>> + Send + '_>>
    {
        Box::pin(async move { self.handle_health_sync() })
    }

    fn handle_readiness(&self) -> Result<HttpResponsePayload, HttpTransportError> {
        self.handle_readiness_sync()
    }

    fn handle_readiness_async(
        &self,
    ) -> Pin<Box<dyn Future<Output = Result<HttpResponsePayload, HttpTransportError>> + Send + '_>>
    {
        Box::pin(async move { self.handle_readiness_sync() })
    }

    fn record_transport_error(&self, error: &HttpTransportError) {
        self.app.record_http_transport_error(error);
    }

    fn record_control_exchange(
        &self,
        id: &str,
        scope: &str,
        bytes_received: u64,
        bytes_sent: u64,
        duration: std::time::Duration,
    ) {
        self.app
            .record_http_control_exchange(id, scope, bytes_received, bytes_sent, duration);
    }
}

impl HttpControlServiceAdapter {
    fn handle_payload_sync(
        &self,
        payload: HttpRequestPayload,
    ) -> Result<HttpResponsePayload, HttpTransportError> {
        let bytes_received = fallback_http_request_len(&payload);
        let started = Instant::now();
        let response = self.handle_payload_metered_sync(payload);
        if let Ok(payload) = &response {
            self.app.record_http_control_exchange(
                "http/control",
                "control",
                bytes_received,
                fallback_http_response_len(payload),
                started.elapsed(),
            );
        }
        response
    }

    fn handle_payload_metered_sync(
        &self,
        payload: HttpRequestPayload,
    ) -> Result<HttpResponsePayload, HttpTransportError> {
        let started = Instant::now();
        let request = ControlRequest::from_http_payload(payload);
        let response = self.app.execute_http_control_request(request);
        if let Err(err) = &response {
            self.app.record_http_control_failure(
                "http/control",
                "control",
                Some(started.elapsed()),
                err,
            );
        }
        response
    }

    fn handle_health_sync(&self) -> Result<HttpResponsePayload, HttpTransportError> {
        let started = Instant::now();
        let response = self
            .app
            .execute_http_control_request(ControlRequest::health());
        if let Ok(payload) = &response {
            self.app.record_http_control_exchange(
                "http/health",
                "health",
                0,
                fallback_http_response_len(payload),
                started.elapsed(),
            );
        }
        response
    }

    fn handle_readiness_sync(&self) -> Result<HttpResponsePayload, HttpTransportError> {
        let started = Instant::now();
        let response = self
            .app
            .execute_http_control_request(ControlRequest::readiness());
        if let Ok(payload) = &response {
            self.app.record_http_control_exchange(
                "http/readiness",
                "readiness",
                0,
                fallback_http_response_len(payload),
                started.elapsed(),
            );
        }
        response
    }
}

#[derive(Clone)]
pub(crate) struct HttpProbeServiceAdapter {
    app: NodeApp,
}

impl HttpProbeServiceAdapter {
    pub(crate) fn new(app: NodeApp) -> Self {
        Self { app }
    }
}

impl HttpControlHandler for HttpProbeServiceAdapter {
    fn handle_payload(
        &self,
        _payload: HttpRequestPayload,
    ) -> Result<HttpResponsePayload, HttpTransportError> {
        Err(HttpTransportError::UnsupportedPath(
            "probe server only exposes health and readiness".into(),
        ))
    }

    fn handle_health(&self) -> Result<HttpResponsePayload, HttpTransportError> {
        self.handle_health_sync()
    }

    fn handle_health_async(
        &self,
    ) -> Pin<Box<dyn Future<Output = Result<HttpResponsePayload, HttpTransportError>> + Send + '_>>
    {
        Box::pin(async move { self.handle_health_sync() })
    }

    fn handle_readiness(&self) -> Result<HttpResponsePayload, HttpTransportError> {
        self.handle_readiness_sync()
    }

    fn handle_readiness_async(
        &self,
    ) -> Pin<Box<dyn Future<Output = Result<HttpResponsePayload, HttpTransportError>> + Send + '_>>
    {
        Box::pin(async move { self.handle_readiness_sync() })
    }

    fn handle_metrics(&self) -> Result<String, HttpTransportError> {
        self.handle_metrics_sync()
    }

    fn handle_metrics_async(
        &self,
    ) -> Pin<Box<dyn Future<Output = Result<String, HttpTransportError>> + Send + '_>> {
        Box::pin(async move { self.handle_metrics_sync() })
    }

    fn record_transport_error(&self, error: &HttpTransportError) {
        self.app.record_http_transport_error(error);
    }

    fn record_control_exchange(
        &self,
        id: &str,
        scope: &str,
        bytes_received: u64,
        bytes_sent: u64,
        duration: std::time::Duration,
    ) {
        self.app
            .record_http_control_exchange(id, scope, bytes_received, bytes_sent, duration);
    }
}

impl HttpProbeServiceAdapter {
    fn handle_health_sync(&self) -> Result<HttpResponsePayload, HttpTransportError> {
        self.app
            .execute_http_control_request(ControlRequest::health())
    }

    fn handle_readiness_sync(&self) -> Result<HttpResponsePayload, HttpTransportError> {
        self.app
            .execute_http_control_request(ControlRequest::readiness())
    }

    fn handle_metrics_sync(&self) -> Result<String, HttpTransportError> {
        let started = Instant::now();
        let snapshot = self.app.observability_snapshot();
        let config =
            MetricsExportConfig::try_from_env().map_err(HttpTransportError::request_failed)?;
        let rendered = render_observability_metrics_with_config(&snapshot, &config);
        self.app.record_http_control_exchange(
            "http/metrics",
            "metrics",
            0,
            rendered.len().min(u64::MAX as usize) as u64,
            started.elapsed(),
        );
        Ok(rendered)
    }
}

#[derive(Clone)]
pub(crate) struct UnixControlServiceAdapter {
    app: NodeApp,
}

impl UnixControlServiceAdapter {
    pub(crate) fn new(app: NodeApp) -> Self {
        Self { app }
    }
}

impl UnixControlHandler for UnixControlServiceAdapter {
    fn handle_control(
        &self,
        envelope: ControlEnvelope,
    ) -> Result<ControlEnvelope, IpcTransportError> {
        self.handle_control_with_identity(envelope, None)
    }

    fn handle_control_with_identity(
        &self,
        envelope: ControlEnvelope,
        identity: Option<UnixPeerIdentity>,
    ) -> Result<ControlEnvelope, IpcTransportError> {
        self.handle_control_with_identity_sync(envelope, identity)
    }

    fn handle_control_with_identity_async(
        &self,
        envelope: ControlEnvelope,
        identity: Option<UnixPeerIdentity>,
    ) -> Pin<Box<dyn Future<Output = Result<ControlEnvelope, IpcTransportError>> + Send + '_>> {
        let adapter = self.clone();
        Box::pin(async move {
            tokio::task::spawn_blocking(move || {
                adapter.execute_control_with_identity(envelope, identity)
            })
            .await
            .map_err(|err| IpcTransportError::WriteFailed(err.to_string()))?
        })
    }

    fn record_transport_error(&self, error: &IpcTransportError) {
        self.app.record_ipc_transport_error(error);
    }

    fn record_control_exchange(
        &self,
        source: &LocalAddress,
        bytes_received: u64,
        bytes_sent: u64,
        duration: std::time::Duration,
    ) {
        self.app.record_local_unary_communication(
            source,
            bytes_received,
            bytes_sent,
            duration,
            crate::app::CommunicationStageDurations {
                socket_read: Some(duration),
                socket_write: Some(duration),
                ..Default::default()
            },
        );
    }
}

impl UnixControlServiceAdapter {
    fn handle_control_with_identity_sync(
        &self,
        envelope: ControlEnvelope,
        identity: Option<UnixPeerIdentity>,
    ) -> Result<ControlEnvelope, IpcTransportError> {
        let started = Instant::now();
        let source = envelope.source.clone();
        let bytes_received = fallback_encoded_len_u64(&envelope);
        let response = self.execute_control_with_identity(envelope, identity)?;
        let bytes_sent = fallback_encoded_len_u64(&response);
        self.app.record_local_unary_communication(
            &source,
            bytes_received,
            bytes_sent,
            started.elapsed(),
            crate::app::CommunicationStageDurations {
                encode: Some(started.elapsed()),
                decode: Some(started.elapsed()),
                socket_read: Some(started.elapsed()),
                socket_write: Some(started.elapsed()),
                ..Default::default()
            },
        );
        Ok(response)
    }

    fn execute_control_with_identity(
        &self,
        envelope: ControlEnvelope,
        identity: Option<UnixPeerIdentity>,
    ) -> Result<ControlEnvelope, IpcTransportError> {
        let message = self.app.execute_local_control_request(
            ControlRequest::from_local_envelope_with_identity(
                ControlSurface::LocalIpc,
                &envelope,
                identity,
            ),
        )?;
        Ok(ControlEnvelope {
            source: envelope.destination,
            destination: envelope.source,
            message,
        })
    }
}

fn fallback_encoded_len_u64<T: ArchiveEncode>(value: &T) -> u64 {
    encode_to_vec(value)
        .map(|bytes| bytes.len().min(u64::MAX as usize) as u64)
        .unwrap_or(0)
}

fn fallback_http_request_len(payload: &HttpRequestPayload) -> u64 {
    HttpCodec
        .encode_request(payload)
        .map(|request| request.body.len().min(u64::MAX as usize) as u64)
        .unwrap_or(0)
}

fn fallback_http_response_len(payload: &HttpResponsePayload) -> u64 {
    HttpCodec
        .encode_response(payload)
        .map(|response| response.body.len().min(u64::MAX as usize) as u64)
        .unwrap_or(0)
}

impl NodeApp {
    fn record_http_control_exchange(
        &self,
        id: &str,
        scope: &str,
        bytes_received: u64,
        bytes_sent: u64,
        duration: std::time::Duration,
    ) {
        self.record_communication_endpoint_exchange_with_stages(
            http_control_endpoint(id, scope, self.config.node_id.as_ref()),
            bytes_received,
            bytes_sent,
            duration,
            crate::app::CommunicationStageDurations {
                socket_write: Some(duration),
                ..Default::default()
            },
        );
    }

    fn record_http_control_failure(
        &self,
        id: &str,
        scope: &str,
        duration: Option<std::time::Duration>,
        error: &HttpTransportError,
    ) {
        self.record_communication_endpoint_failure_kind(
            http_control_endpoint(id, scope, self.config.node_id.as_ref()),
            duration,
            crate::app::classify_http_communication_failure(error),
            error.to_string(),
        );
    }
}

fn http_control_endpoint(
    id: &str,
    scope: &str,
    node_id: &str,
) -> crate::app::CommunicationEndpointRuntime {
    let mut endpoint = crate::app::CommunicationEndpointRuntime::new(id, "http", scope);
    endpoint.local = Some(node_id.to_owned());
    endpoint
        .labels
        .insert("node_id".to_owned(), node_id.to_owned());
    endpoint
}
