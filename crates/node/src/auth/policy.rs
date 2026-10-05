use super::{
    AuthenticatedOperator, AuthenticatedPeer, AuthorizationLookup, LocalAuthenticationMode,
    NodeSecurity, OperatorAuthentication, PeerAuthenticationMode, PeerObservedScope,
    crypto::{current_effective_gid, current_effective_uid},
    transport_binding_from_hello,
};
use crate::NodeError;
use crate::service::{
    Authenticator, AuthorizationMiddleware, Authorizer, ControlOperation, ControlPrincipal,
    ControlRequest,
};
use orion::{
    control_plane::{ClientRole, ControlMessage, ObservedStateUpdate, OperatorId},
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
        if !request.context.surface.is_peer() {
            return Ok(());
        }

        let Some(payload) = request.peer_request_payload() else {
            return Ok(());
        };

        match request.context.peer_auth.as_ref() {
            // Remote operators are a principal kind of their own: their keys live in the operator
            // trust store, never in the peer trust store, and they never become peers.
            Some(auth) if OperatorId::is_operator_principal(auth.node_id.as_str()) => {
                let is_hello = request.operation() == ControlOperation::OperatorHello;
                request.context.principal = match self
                    .security
                    .authenticate_operator(auth, &payload, is_hello)?
                {
                    OperatorAuthentication::Enrolled(operator) => {
                        ControlPrincipal::Operator(operator)
                    }
                    OperatorAuthentication::Unenrolled {
                        operator_id,
                        public_key,
                    } => ControlPrincipal::UnenrolledOperator {
                        operator_id,
                        public_key,
                    },
                };
                Ok(())
            }
            Some(auth) => {
                let authenticated = self.security.authenticate_request(auth, &payload)?;
                request.context.principal = ControlPrincipal::Peer(authenticated.clone());
                request.context.authenticated_peer = Some(authenticated);
                Ok(())
            }
            // The enrollment handshake is how an unknown peer becomes trusted; it carries its
            // own proofs and is answered only when an enrollment key is configured.
            None if request.operation().is_enrollment_handshake() => Ok(()),
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

    /// What an enrolled operator may do: read (with `read`), run the actions its policy allows,
    /// and query actions (all with `read`, else only its own). Nothing else: operators are not
    /// cluster members, so sync, desired-state writes and observed updates are refused.
    fn authorize_operator(
        &self,
        operator: &AuthenticatedOperator,
        request: &ControlRequest,
    ) -> Result<(), NodeError> {
        let operation = request.operation();
        let refuse = |why: &str| {
            Err(NodeError::Authorization(format!(
                "operator {} may not perform {operation:?}: {why}",
                operator.operator_id
            )))
        };
        match operation {
            ControlOperation::OperatorHello => Ok(()),
            ControlOperation::QueryStateSnapshot
            | ControlOperation::QueryObservability
            | ControlOperation::QueryStatus => {
                if operator.policy.read {
                    Ok(())
                } else {
                    refuse("its policy grants no read access")
                }
            }
            ControlOperation::QueryActions => {
                if operator.policy.read || !operator.allowed_actions.is_empty() {
                    Ok(())
                } else {
                    refuse("its policy grants neither read access nor actions")
                }
            }
            ControlOperation::RunAction => {
                let crate::service::ControlRequestBody::Control(message) = &request.body else {
                    return refuse("malformed request");
                };
                let ControlMessage::RunAction(action) = message.as_ref() else {
                    return refuse("malformed request");
                };
                if operator.allows_action(&action.name) {
                    Ok(())
                } else {
                    Err(NodeError::Authorization(format!(
                        "operator {} may not run action `{}` (allowed: {})",
                        operator.operator_id,
                        action.name,
                        if operator.allowed_actions.is_empty() {
                            "none".to_owned()
                        } else {
                            operator.allowed_actions.join(", ")
                        }
                    )))
                }
            }
            _ => refuse("operators are not cluster members"),
        }
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
        for (resource_id, resource) in &update.observed.resources {
            if !scope.resource_ids.contains(resource_id) && !scope.owns(resource) {
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
            (ControlPrincipal::Operator(operator), _) => self.authorize_operator(operator, request),
            (ControlPrincipal::UnenrolledOperator { .. }, ControlOperation::OperatorHello) => {
                Ok(())
            }
            (ControlPrincipal::UnenrolledOperator { operator_id, .. }, operation) => {
                Err(NodeError::Authorization(format!(
                    "operator {operator_id} is not enrolled and may not perform {operation:?}"
                )))
            }
            (_, ControlOperation::OperatorHello) => Err(NodeError::Authorization(
                "OperatorHello needs a request signed by an `operator:<name>` principal".into(),
            )),
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
            (
                ControlPrincipal::Anonymous | ControlPrincipal::Peer(_),
                ControlOperation::EnrollmentHello | ControlOperation::EnrollmentConfirm,
            ) => Ok(()),
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
            // Actions forwarded by peers: always an authenticated, enrolled peer, whatever the
            // peer authentication mode (`docs/actions.md`).
            (
                ControlPrincipal::Peer(peer),
                ControlOperation::RunAction
                | ControlOperation::QueryActions
                | ControlOperation::QueryStatus,
            ) if self.lookup.is_configured_peer(&peer.node_id) => Ok(()),
            (
                ControlPrincipal::Anonymous | ControlPrincipal::Peer(_),
                ControlOperation::RunAction
                | ControlOperation::QueryActions
                | ControlOperation::QueryStatus,
            ) => Err(NodeError::Authorization(format!(
                "{:?} needs an authenticated, enrolled peer",
                request.operation()
            ))),
            (
                ControlPrincipal::Local { .. },
                ControlOperation::ClientHello
                | ControlOperation::QueryStateSnapshot
                | ControlOperation::QueryObservability
                | ControlOperation::QueryPeerTrust
                | ControlOperation::QueryDiscovery
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
                ControlPrincipal::Local { .. },
                ControlOperation::QueryStatus | ControlOperation::WatchStatus,
            ) => Ok(()),
            (
                ControlPrincipal::Local { source, .. },
                ControlOperation::PublishStatus
                | ControlOperation::WatchActionRequests
                | ControlOperation::ClaimNodeActions
                | ControlOperation::ReportActionResult,
            ) => self
                .authorize_local_role(source, ClientRole::Provider)
                .or_else(|_| self.authorize_local_role(source, ClientRole::Executor)),
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
                | ControlOperation::EnrollDiscoveredPeer
                | ControlOperation::RemovePeer
                | ControlOperation::RevokePeer
                | ControlOperation::ReplacePeerIdentity
                | ControlOperation::RotateHttpTlsIdentity
                | ControlOperation::QueryMaintenance
                | ControlOperation::UpdateMaintenance
                | ControlOperation::RunAction
                | ControlOperation::QueryActions
                | ControlOperation::WatchActions
                | ControlOperation::QueryOperators
                | ControlOperation::EnrollOperator
                | ControlOperation::RemoveOperator,
            ) => self.authorize_local_role(source, ClientRole::ControlPlane),
            (
                ControlPrincipal::Local { source, .. },
                ControlOperation::PeerTrust | ControlOperation::MaintenanceStatus,
            ) => self.authorize_local_role(source, ClientRole::ControlPlane),
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
