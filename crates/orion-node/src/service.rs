use crate::{AuthenticatedPeer, NodeApp, NodeError};
use orion::{
    auth::{AuthenticatedPeerRequest, PeerRequestAuth, PeerRequestPayload},
    control_plane::{ControlMessage, ObservedStateUpdate},
    transport::{
        http::{HttpControlHandler, HttpRequestPayload, HttpResponsePayload, HttpTransportError},
        ipc::{
            ControlEnvelope, IpcTransportError, LocalAddress, UnixControlHandler, UnixPeerIdentity,
        },
    },
};
use orion_service::{MiddlewareNext, MiddlewareStack, RequestMiddleware, RequestService};
use std::sync::Arc;
use tracing::error;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ControlPrincipal {
    Anonymous,
    Peer(AuthenticatedPeer),
    Local {
        source: LocalAddress,
        destination: LocalAddress,
    },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ControlSurface {
    PeerHttp,
    LocalIpc,
    LocalIpcStream,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ControlSource {
    PeerHttp,
    Local {
        source: LocalAddress,
        destination: LocalAddress,
    },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ControlRequestContext {
    pub surface: ControlSurface,
    pub source: ControlSource,
    pub principal: ControlPrincipal,
    pub peer_auth: Option<PeerRequestAuth>,
    pub authenticated_peer: Option<AuthenticatedPeer>,
    pub local_identity: Option<UnixPeerIdentity>,
}

impl ControlRequestContext {
    pub fn peer_http() -> Self {
        Self {
            surface: ControlSurface::PeerHttp,
            source: ControlSource::PeerHttp,
            principal: ControlPrincipal::Anonymous,
            peer_auth: None,
            authenticated_peer: None,
            local_identity: None,
        }
    }

    pub fn local(surface: ControlSurface, source: LocalAddress, destination: LocalAddress) -> Self {
        Self::local_with_identity(surface, source, destination, None)
    }

    pub fn local_with_identity(
        surface: ControlSurface,
        source: LocalAddress,
        destination: LocalAddress,
        local_identity: Option<UnixPeerIdentity>,
    ) -> Self {
        Self {
            surface,
            source: ControlSource::Local {
                source: source.clone(),
                destination: destination.clone(),
            },
            principal: ControlPrincipal::Local {
                source,
                destination,
            },
            peer_auth: None,
            authenticated_peer: None,
            local_identity,
        }
    }

    pub fn local_source(&self) -> Option<&LocalAddress> {
        match &self.source {
            ControlSource::PeerHttp => None,
            ControlSource::Local { source, .. } => Some(source),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ControlRequestBody {
    Control(Box<ControlMessage>),
    ObservedUpdate(ObservedStateUpdate),
    Health,
    Readiness,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ControlOperation {
    Hello,
    SyncRequest,
    SyncSummaryRequest,
    SyncDiffRequest,
    QueryStateSnapshot,
    Snapshot,
    Mutations,
    QueryObservability,
    ObservedUpdate,
    ClientHello,
    ClientWelcome,
    ProviderState,
    ExecutorState,
    QueryExecutorWorkloads,
    WatchExecutorWorkloads,
    ExecutorWorkloads,
    QueryProviderLeases,
    WatchProviderLeases,
    ProviderLeases,
    EnrollPeer,
    QueryPeerTrust,
    PeerTrust,
    RevokePeer,
    ReplacePeerIdentity,
    RotateHttpTlsIdentity,
    QueryMaintenance,
    UpdateMaintenance,
    MaintenanceStatus,
    Observability,
    WatchState,
    PollClientEvents,
    ClientEvents,
    Ping,
    Pong,
    Accepted,
    Rejected,
    Health,
    Readiness,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ControlRequest {
    pub context: ControlRequestContext,
    pub body: ControlRequestBody,
}

impl ControlRequest {
    pub fn from_http_payload(payload: HttpRequestPayload) -> Self {
        let (body, peer_auth) = match payload {
            HttpRequestPayload::Control(message) => (ControlRequestBody::Control(message), None),
            HttpRequestPayload::ObservedUpdate(update) => {
                (ControlRequestBody::ObservedUpdate(update), None)
            }
            HttpRequestPayload::AuthenticatedPeer(AuthenticatedPeerRequest { auth, payload }) => {
                let body = match payload {
                    PeerRequestPayload::Control(message) => ControlRequestBody::Control(message),
                    PeerRequestPayload::ObservedUpdate(update) => {
                        ControlRequestBody::ObservedUpdate(update)
                    }
                };
                (body, Some(auth))
            }
        };

        Self {
            context: ControlRequestContext {
                peer_auth,
                ..ControlRequestContext::peer_http()
            },
            body,
        }
    }

    pub fn from_local_envelope(surface: ControlSurface, envelope: &ControlEnvelope) -> Self {
        Self::from_local_envelope_with_identity(surface, envelope, None)
    }

    pub fn from_local_envelope_with_identity(
        surface: ControlSurface,
        envelope: &ControlEnvelope,
        local_identity: Option<UnixPeerIdentity>,
    ) -> Self {
        Self::from_local_message(
            surface,
            envelope.source.clone(),
            envelope.destination.clone(),
            envelope.message.clone(),
            local_identity,
        )
    }

    pub fn from_local_message(
        surface: ControlSurface,
        source: LocalAddress,
        destination: LocalAddress,
        message: ControlMessage,
        local_identity: Option<UnixPeerIdentity>,
    ) -> Self {
        Self {
            context: ControlRequestContext::local_with_identity(
                surface,
                source,
                destination,
                local_identity,
            ),
            body: ControlRequestBody::Control(Box::new(message)),
        }
    }

    pub fn health() -> Self {
        Self {
            context: ControlRequestContext::peer_http(),
            body: ControlRequestBody::Health,
        }
    }

    pub fn readiness() -> Self {
        Self {
            context: ControlRequestContext::peer_http(),
            body: ControlRequestBody::Readiness,
        }
    }

    pub fn operation(&self) -> ControlOperation {
        match &self.body {
            ControlRequestBody::Control(message) => match message.as_ref() {
                ControlMessage::Hello(_) => ControlOperation::Hello,
                ControlMessage::SyncRequest(_) => ControlOperation::SyncRequest,
                ControlMessage::SyncSummaryRequest(_) => ControlOperation::SyncSummaryRequest,
                ControlMessage::SyncDiffRequest(_) => ControlOperation::SyncDiffRequest,
                ControlMessage::QueryStateSnapshot => ControlOperation::QueryStateSnapshot,
                ControlMessage::Snapshot(_) => ControlOperation::Snapshot,
                ControlMessage::Mutations(_) => ControlOperation::Mutations,
                ControlMessage::QueryObservability => ControlOperation::QueryObservability,
                ControlMessage::ClientHello(_) => ControlOperation::ClientHello,
                ControlMessage::ClientWelcome(_) => ControlOperation::ClientWelcome,
                ControlMessage::ProviderState(_) => ControlOperation::ProviderState,
                ControlMessage::ExecutorState(_) => ControlOperation::ExecutorState,
                ControlMessage::QueryExecutorWorkloads(_) => {
                    ControlOperation::QueryExecutorWorkloads
                }
                ControlMessage::WatchExecutorWorkloads(_) => {
                    ControlOperation::WatchExecutorWorkloads
                }
                ControlMessage::ExecutorWorkloads(_) => ControlOperation::ExecutorWorkloads,
                ControlMessage::QueryProviderLeases(_) => ControlOperation::QueryProviderLeases,
                ControlMessage::WatchProviderLeases(_) => ControlOperation::WatchProviderLeases,
                ControlMessage::ProviderLeases(_) => ControlOperation::ProviderLeases,
                ControlMessage::EnrollPeer(_) => ControlOperation::EnrollPeer,
                ControlMessage::QueryPeerTrust => ControlOperation::QueryPeerTrust,
                ControlMessage::PeerTrust(_) => ControlOperation::PeerTrust,
                ControlMessage::RevokePeer(_) => ControlOperation::RevokePeer,
                ControlMessage::ReplacePeerIdentity(_) => ControlOperation::ReplacePeerIdentity,
                ControlMessage::RotateHttpTlsIdentity => ControlOperation::RotateHttpTlsIdentity,
                ControlMessage::QueryMaintenance => ControlOperation::QueryMaintenance,
                ControlMessage::UpdateMaintenance(_) => ControlOperation::UpdateMaintenance,
                ControlMessage::MaintenanceStatus(_) => ControlOperation::MaintenanceStatus,
                ControlMessage::Observability(_) => ControlOperation::Observability,
                ControlMessage::WatchState(_) => ControlOperation::WatchState,
                ControlMessage::PollClientEvents(_) => ControlOperation::PollClientEvents,
                ControlMessage::ClientEvents(_) => ControlOperation::ClientEvents,
                ControlMessage::Ping => ControlOperation::Ping,
                ControlMessage::Pong => ControlOperation::Pong,
                ControlMessage::Accepted => ControlOperation::Accepted,
                ControlMessage::Rejected(_) => ControlOperation::Rejected,
            },
            ControlRequestBody::ObservedUpdate(_) => ControlOperation::ObservedUpdate,
            ControlRequestBody::Health => ControlOperation::Health,
            ControlRequestBody::Readiness => ControlOperation::Readiness,
        }
    }

    pub fn peer_request_payload(&self) -> Option<PeerRequestPayload> {
        if !matches!(self.context.surface, ControlSurface::PeerHttp) {
            return None;
        }
        match &self.body {
            ControlRequestBody::Control(message) => {
                Some(PeerRequestPayload::Control(message.clone()))
            }
            ControlRequestBody::ObservedUpdate(update) => {
                Some(PeerRequestPayload::ObservedUpdate(update.clone()))
            }
            ControlRequestBody::Health | ControlRequestBody::Readiness => None,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ControlResponse {
    Http(Box<HttpResponsePayload>),
    Local(Box<ControlMessage>),
}

impl ControlResponse {
    fn into_http(self) -> Result<HttpResponsePayload, HttpTransportError> {
        match self {
            Self::Http(response) => Ok(*response),
            Self::Local(_) => Err(HttpTransportError::request_failed(
                "local control response returned on HTTP boundary",
            )),
        }
    }

    fn into_local(self) -> Result<ControlMessage, IpcTransportError> {
        match self {
            Self::Local(message) => Ok(*message),
            Self::Http(_) => Err(IpcTransportError::WriteFailed(
                "HTTP control response returned on local IPC boundary".into(),
            )),
        }
    }
}

pub trait ControlMiddleware:
    RequestMiddleware<ControlRequest, Response = ControlResponse, Error = NodeError>
{
}

impl<T> ControlMiddleware for T where
    T: RequestMiddleware<ControlRequest, Response = ControlResponse, Error = NodeError>
{
}

pub(crate) type ControlMiddlewareHandle = Arc<dyn ControlMiddleware>;
// `orion-service` is intentionally used only at the control-plane ingress boundary. Keep
// reconcile loops, transport inner loops, and other hot paths on direct typed calls rather than
// extending this dynamic-dispatch pipeline into performance-sensitive code.
type ControlPipeline = MiddlewareStack<ControlRequest, ControlResponse, NodeError>;

pub trait Authenticator: Send + Sync {
    fn authenticate(&self, request: &mut ControlRequest) -> Result<(), NodeError>;
}

pub trait Authorizer: Send + Sync {
    fn authorize(&self, request: &ControlRequest) -> Result<(), NodeError>;
}

#[derive(Clone)]
pub struct AuthorizationMiddleware {
    authenticator: Arc<dyn Authenticator>,
    authorizer: Arc<dyn Authorizer>,
}

impl AuthorizationMiddleware {
    pub fn new(authenticator: Arc<dyn Authenticator>, authorizer: Arc<dyn Authorizer>) -> Self {
        Self {
            authenticator,
            authorizer,
        }
    }
}

impl RequestMiddleware<ControlRequest> for AuthorizationMiddleware {
    type Response = ControlResponse;
    type Error = NodeError;

    fn handle(
        &self,
        mut request: ControlRequest,
        next: MiddlewareNext<'_, ControlRequest, Self::Response, Self::Error>,
    ) -> Result<Self::Response, Self::Error> {
        self.authenticator.authenticate(&mut request)?;
        self.authorizer.authorize(&request)?;
        next.run(request)
    }
}

#[derive(Clone)]
struct NodeControlService {
    app: NodeApp,
}

impl NodeControlService {
    fn new(app: NodeApp) -> Self {
        Self { app }
    }
}

impl RequestService<ControlRequest> for NodeControlService {
    type Response = ControlResponse;
    type Error = NodeError;

    fn serve(&self, request: ControlRequest) -> Result<Self::Response, Self::Error> {
        let ControlRequest { context, body } = request;

        match body {
            ControlRequestBody::Control(message) => match context.surface {
                ControlSurface::PeerHttp => self
                    .app
                    .apply_control_message(*message)
                    .map(|response| ControlResponse::Http(Box::new(response))),
                ControlSurface::LocalIpc | ControlSurface::LocalIpcStream => {
                    let source = context.local_source().ok_or_else(|| {
                        NodeError::Storage(
                            "local control request missing local source address".into(),
                        )
                    })?;
                    self.app
                        .apply_local_control_message(source, *message)
                        .map(|message| ControlResponse::Local(Box::new(message)))
                }
            },
            ControlRequestBody::ObservedUpdate(update) => self
                .app
                .apply_observed_update(update)
                .map(|response| ControlResponse::Http(Box::new(response))),
            ControlRequestBody::Health => Ok(ControlResponse::Http(Box::new(
                HttpResponsePayload::Health(self.app.health_snapshot()),
            ))),
            ControlRequestBody::Readiness => Ok(ControlResponse::Http(Box::new(
                HttpResponsePayload::Readiness(self.app.readiness_snapshot()),
            ))),
        }
    }
}

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
        let request = ControlRequest::from_http_payload(payload);
        self.app.execute_http_control_request(request)
    }

    fn handle_health(&self) -> Result<HttpResponsePayload, HttpTransportError> {
        self.app
            .execute_http_control_request(ControlRequest::health())
    }

    fn handle_readiness(&self) -> Result<HttpResponsePayload, HttpTransportError> {
        self.app
            .execute_http_control_request(ControlRequest::readiness())
    }

    fn record_transport_error(&self, error: &HttpTransportError) {
        self.app.record_http_transport_error(error);
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
        self.app
            .execute_http_control_request(ControlRequest::health())
    }

    fn handle_readiness(&self) -> Result<HttpResponsePayload, HttpTransportError> {
        self.app
            .execute_http_control_request(ControlRequest::readiness())
    }

    fn record_transport_error(&self, error: &HttpTransportError) {
        self.app.record_http_transport_error(error);
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

    fn record_transport_error(&self, error: &IpcTransportError) {
        self.app.record_ipc_transport_error(error);
    }
}

impl NodeApp {
    fn execute_control_request(
        &self,
        request: ControlRequest,
    ) -> Result<ControlResponse, NodeError> {
        self.serve_control_request(request)
    }

    fn execute_http_control_request(
        &self,
        request: ControlRequest,
    ) -> Result<HttpResponsePayload, HttpTransportError> {
        let operation = request.operation();
        self.execute_control_request(request)
            .map_err(|err| {
                error!(
                    surface = "peer_http",
                    ?operation,
                    error = %err,
                    "control request failed"
                );
                HttpTransportError::request_failed(err.to_string())
            })?
            .into_http()
    }

    fn execute_local_control_request(
        &self,
        request: ControlRequest,
    ) -> Result<ControlMessage, IpcTransportError> {
        match self.execute_control_request(request) {
            Ok(response) => response.into_local(),
            Err(err) => Ok(ControlMessage::Rejected(err.to_string())),
        }
    }

    fn control_pipeline(&self) -> ControlPipeline {
        let mut pipeline = ControlPipeline::new(NodeControlService::new(self.clone()));
        for middleware in self.control_middlewares.iter().cloned() {
            pipeline = pipeline.with_arc_middleware(middleware);
        }
        pipeline
    }

    pub(crate) fn serve_control_request(
        &self,
        request: ControlRequest,
    ) -> Result<ControlResponse, NodeError> {
        // Control middleware and request handlers are expected to stay synchronous and cheap.
        // Any unavoidable blocking work should be isolated to the specific persistence,
        // filesystem, or TLS helper that needs it rather than offloading the whole request.
        self.control_pipeline().serve(request)
    }

    pub(crate) fn serve_local_control_message(
        &self,
        surface: ControlSurface,
        source: LocalAddress,
        destination: LocalAddress,
        message: ControlMessage,
    ) -> Result<ControlMessage, NodeError> {
        self.serve_control_request(ControlRequest::from_local_message(
            surface,
            source,
            destination,
            message,
            None,
        ))?
        .into_local()
        .map_err(NodeError::IpcTransport)
    }

    pub(crate) fn http_control_handler(&self) -> HttpControlServiceAdapter {
        HttpControlServiceAdapter::new(self.clone())
    }

    pub(crate) fn http_probe_handler(&self) -> HttpProbeServiceAdapter {
        HttpProbeServiceAdapter::new(self.clone())
    }

    pub(crate) fn unix_control_handler(&self) -> UnixControlServiceAdapter {
        UnixControlServiceAdapter::new(self.clone())
    }
}

#[cfg(test)]
mod tests;
