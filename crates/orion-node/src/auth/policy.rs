use super::{
    AuthenticatedPeer, AuthorizationLookup, LocalAuthenticationMode, NodeSecurity,
    PeerAuthenticationMode, PeerObservedScope,
    crypto::{current_effective_gid, current_effective_uid},
    transport_binding_from_hello,
};
use crate::NodeError;
use crate::service::{
    Authenticator, AuthorizationMiddleware, Authorizer, ControlOperation, ControlPrincipal,
    ControlRequest, ControlSurface,
};
use orion::{
    control_plane::{ClientRole, ControlMessage, ObservedStateUpdate},
    transport::ipc::LocalAddress,
};
use std::sync::Arc;

#[derive(Clone)]
struct NodeSecurityAuthenticator {
    security: Arc<NodeSecurity>,
}

impl NodeSecurityAuthenticator {
    fn new(security: Arc<NodeSecurity>) -> Self {
        Self { security }
    }
}

impl Authenticator for NodeSecurityAuthenticator {
    fn authenticate(&self, request: &mut ControlRequest) -> Result<(), NodeError> {
        if !matches!(request.context.surface, ControlSurface::PeerHttp) {
            return Ok(());
        }

        let Some(payload) = request.peer_request_payload() else {
            return Ok(());
        };

        match request.context.peer_auth.as_ref() {
            Some(auth) => {
                let authenticated = self.security.authenticate_request(auth, &payload)?;
                request.context.principal = ControlPrincipal::Peer(authenticated.clone());
                request.context.authenticated_peer = Some(authenticated);
                Ok(())
            }
            None if self.security.mode() == PeerAuthenticationMode::Required => {
                Err(NodeError::Authorization(format!(
                    "peer authentication required for {:?}",
                    request.operation()
                )))
            }
            None => Ok(()),
        }
    }
}

#[derive(Clone)]
struct NodeSecurityAuthorizer {
    security: Arc<NodeSecurity>,
    peer_authentication: PeerAuthenticationMode,
    local_authentication: LocalAuthenticationMode,
    local_uid: u32,
    local_gid: u32,
    lookup: Arc<dyn AuthorizationLookup>,
}

impl NodeSecurityAuthorizer {
    fn new(
        security: &Arc<NodeSecurity>,
        peer_authentication: PeerAuthenticationMode,
        local_authentication: LocalAuthenticationMode,
        local_uid: u32,
        local_gid: u32,
        lookup: Arc<dyn AuthorizationLookup>,
    ) -> Self {
        Self {
            security: security.clone(),
            peer_authentication,
            local_authentication,
            local_uid,
            local_gid,
            lookup,
        }
    }

    fn authorize_local_identity(&self, request: &ControlRequest) -> Result<(), NodeError> {
        if self.local_authentication == LocalAuthenticationMode::Disabled {
            return Ok(());
        }
        let Some(identity) = request.context.local_identity.as_ref() else {
            return Ok(());
        };
        let allowed = match self.local_authentication {
            LocalAuthenticationMode::Disabled => true,
            LocalAuthenticationMode::SameUser => identity.uid == self.local_uid,
            LocalAuthenticationMode::SameUserOrGroup => {
                identity.uid == self.local_uid || identity.gid == self.local_gid
            }
        };
        if !allowed {
            return Err(NodeError::Authorization(format!(
                "local caller uid {} gid {} does not satisfy {:?} policy for node uid {} gid {}",
                identity.uid,
                identity.gid,
                self.local_authentication,
                self.local_uid,
                self.local_gid
            )));
        }
        Ok(())
    }

    fn authorize_local_role(
        &self,
        source: &LocalAddress,
        expected: ClientRole,
    ) -> Result<(), NodeError> {
        let session = self
            .lookup
            .session_for(source)
            .ok_or_else(|| NodeError::UnknownClient(source.clone()))?;
        if session.role != expected {
            return Err(NodeError::ClientRoleMismatch {
                client: source.clone(),
                expected,
                found: session.role,
            });
        }
        Ok(())
    }

    fn authorize_authenticated_peer_write(
        &self,
        request: &ControlRequest,
    ) -> Result<(), NodeError> {
        if self.peer_authentication == PeerAuthenticationMode::Disabled {
            return Ok(());
        }
        let peer = match &request.context.principal {
            ControlPrincipal::Peer(peer) => peer,
            _ => {
                return Err(NodeError::AuthenticatedPeerRequired {
                    operation: request.operation(),
                });
            }
        };
        if !self.lookup.is_configured_peer(&peer.node_id) {
            return Err(NodeError::ConfiguredPeerRequired {
                operation: request.operation(),
            });
        }
        Ok(())
    }

    fn authorize_peer_message_consistency(
        &self,
        peer: &AuthenticatedPeer,
        message: &ControlMessage,
    ) -> Result<(), NodeError> {
        let payload_node_id = match message {
            ControlMessage::Hello(hello) => Some(&hello.node_id),
            ControlMessage::SyncRequest(request) => Some(&request.node_id),
            ControlMessage::SyncSummaryRequest(request) => Some(&request.node_id),
            ControlMessage::SyncDiffRequest(request) => Some(&request.node_id),
            _ => None,
        };
        if let Some(payload_node_id) = payload_node_id
            && payload_node_id != &peer.node_id
        {
            return Err(NodeError::Authorization(format!(
                "authenticated peer {} cannot send payload for {}",
                peer.node_id, payload_node_id
            )));
        }
        if let ControlMessage::Hello(hello) = message
            && let Some(binding) = transport_binding_from_hello(hello)?
        {
            self.security
                .validate_or_update_transport_binding(&peer.node_id, &binding)?;
        }
        Ok(())
    }

    fn authorize_observed_update_scope(
        &self,
        peer: &AuthenticatedPeer,
        update: &ObservedStateUpdate,
    ) -> Result<(), NodeError> {
        let scope: PeerObservedScope = self.lookup.observed_scope_for(&peer.node_id);
        for node_id in update.observed.nodes.keys() {
            if node_id != &peer.node_id {
                return Err(NodeError::Authorization(format!(
                    "peer {} cannot publish observed node record for {}",
                    peer.node_id, node_id
                )));
            }
        }
        for workload in update.observed.workloads.values() {
            match &workload.assigned_node_id {
                Some(node_id) if node_id == &peer.node_id => {}
                Some(node_id) => {
                    return Err(NodeError::Authorization(format!(
                        "peer {} cannot publish workload {} assigned to {}",
                        peer.node_id, workload.workload_id, node_id
                    )));
                }
                None => {
                    return Err(NodeError::Authorization(format!(
                        "peer {} cannot publish unassigned workload {}",
                        peer.node_id, workload.workload_id
                    )));
                }
            }
        }
        for resource_id in update.observed.resources.keys() {
            if !scope.resource_ids.contains(resource_id) {
                return Err(NodeError::Authorization(format!(
                    "peer {} cannot publish observed resource {}",
                    peer.node_id, resource_id
                )));
            }
        }
        for lease in update.observed.leases.values() {
            if !scope.resource_ids.contains(&lease.resource_id) {
                return Err(NodeError::Authorization(format!(
                    "peer {} cannot publish observed lease for resource {}",
                    peer.node_id, lease.resource_id
                )));
            }
        }
        Ok(())
    }
}

impl Authorizer for NodeSecurityAuthorizer {
    fn authorize(&self, request: &ControlRequest) -> Result<(), NodeError> {
        if matches!(request.context.principal, ControlPrincipal::Local { .. }) {
            self.authorize_local_identity(request)?;
        }
        if let ControlPrincipal::Peer(peer) = &request.context.principal
            && let crate::service::ControlRequestBody::Control(message) = &request.body
        {
            self.authorize_peer_message_consistency(peer, message)?;
        }
        match (&request.context.principal, request.operation()) {
            (
                ControlPrincipal::Anonymous | ControlPrincipal::Peer(_),
                ControlOperation::Hello
                | ControlOperation::SyncRequest
                | ControlOperation::SyncSummaryRequest
                | ControlOperation::SyncDiffRequest
                | ControlOperation::QueryStateSnapshot
                | ControlOperation::QueryObservability
                | ControlOperation::Health
                | ControlOperation::Readiness,
            ) => Ok(()),
            (
                ControlPrincipal::Anonymous | ControlPrincipal::Peer(_),
                ControlOperation::Snapshot | ControlOperation::Mutations,
            ) => self.authorize_authenticated_peer_write(request),
            (ControlPrincipal::Peer(peer), ControlOperation::ObservedUpdate) => {
                self.authorize_authenticated_peer_write(request)?;
                let crate::service::ControlRequestBody::ObservedUpdate(update) = &request.body
                else {
                    return Err(NodeError::Authorization(
                        "observed update operation missing observed update body".into(),
                    ));
                };
                self.authorize_observed_update_scope(peer, update)
            }
            (_, ControlOperation::ObservedUpdate) => {
                self.authorize_authenticated_peer_write(request)
            }
            (
                ControlPrincipal::Local { .. },
                ControlOperation::ClientHello
                | ControlOperation::QueryStateSnapshot
                | ControlOperation::QueryObservability
                | ControlOperation::QueryPeerTrust
                | ControlOperation::Ping
                | ControlOperation::Pong
                | ControlOperation::Hello
                | ControlOperation::SyncRequest,
            ) => Ok(()),
            (
                ControlPrincipal::Local { source, .. },
                ControlOperation::ProviderState
                | ControlOperation::QueryProviderLeases
                | ControlOperation::WatchProviderLeases,
            ) => self.authorize_local_role(source, ClientRole::Provider),
            (
                ControlPrincipal::Local { source, .. },
                ControlOperation::ExecutorState
                | ControlOperation::QueryExecutorWorkloads
                | ControlOperation::WatchExecutorWorkloads,
            ) => self.authorize_local_role(source, ClientRole::Executor),
            (
                ControlPrincipal::Local { source, .. },
                ControlOperation::Mutations
                | ControlOperation::WatchState
                | ControlOperation::PollClientEvents
                | ControlOperation::EnrollPeer
                | ControlOperation::RevokePeer
                | ControlOperation::ReplacePeerIdentity
                | ControlOperation::RotateHttpTlsIdentity,
            ) => self.authorize_local_role(source, ClientRole::ControlPlane),
            (ControlPrincipal::Local { source, .. }, ControlOperation::PeerTrust) => {
                self.authorize_local_role(source, ClientRole::ControlPlane)
            }
            (principal, operation) => Err(NodeError::Authorization(format!(
                "principal {:?} is not allowed to perform {:?}",
                principal, operation
            ))),
        }
    }
}

#[derive(Clone)]
pub struct PeerSecurityMiddleware {
    inner: AuthorizationMiddleware,
}

impl PeerSecurityMiddleware {
    pub fn new(
        security: &Arc<NodeSecurity>,
        local_authentication: LocalAuthenticationMode,
        lookup: Arc<dyn AuthorizationLookup>,
    ) -> Self {
        let local_uid = current_effective_uid();
        let local_gid = current_effective_gid();
        Self {
            inner: AuthorizationMiddleware::new(
                Arc::new(NodeSecurityAuthenticator::new(security.clone())),
                Arc::new(NodeSecurityAuthorizer::new(
                    security,
                    security.mode(),
                    local_authentication,
                    local_uid,
                    local_gid,
                    lookup,
                )),
            ),
        }
    }
}

impl orion_service::RequestMiddleware<ControlRequest> for PeerSecurityMiddleware {
    type Response = crate::service::ControlResponse;
    type Error = NodeError;

    fn handle(
        &self,
        request: ControlRequest,
        next: orion_service::MiddlewareNext<'_, ControlRequest, Self::Response, Self::Error>,
    ) -> Result<Self::Response, Self::Error> {
        self.inner.handle(request, next)
    }
}
