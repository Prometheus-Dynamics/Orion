//! Remote operators on the node side (`docs/remote-operator.md`): the requests an operator may
//! send over the signed peer transports, and the local administration messages
//! (`orionctl operators ...`).
//!
//! Operators never take part in the cluster: their requests do not count as peer liveness, they
//! are not registered for sync, they hold no desired-state replica, and the only state they can
//! change is by submitting actions their policy allows.

use super::{ActionOrigin, AuditEventKind, NodeApp, NodeError};
use crate::{AuthenticatedOperator, ControlPrincipal};
use orion::control_plane::{
    ActionQuery, ControlMessage, OperatorEnrollment, OperatorEnrollmentMethod, OperatorId,
    OperatorPolicy, OperatorTrustState, OperatorWelcome, OperatorsSnapshot,
};
use orion::transport::http::HttpResponsePayload;
use orion_auth::{crypto::key_fingerprint, hex::parse_key_hex};
use tracing::info;

impl NodeApp {
    /// Serves a request of a remote operator (already authenticated and authorized by
    /// `PeerSecurityMiddleware`).
    pub(crate) fn apply_operator_control_message(
        &self,
        principal: &ControlPrincipal,
        message: ControlMessage,
    ) -> Result<HttpResponsePayload, NodeError> {
        let operator = match principal {
            ControlPrincipal::Operator(operator) => operator,
            ControlPrincipal::UnenrolledOperator {
                operator_id,
                public_key,
            } => {
                return match message {
                    ControlMessage::OperatorHello => Ok(HttpResponsePayload::OperatorWelcome(
                        Box::new(self.operator_welcome(operator_id, public_key, None)),
                    )),
                    _ => Err(NodeError::Authorization(format!(
                        "operator {operator_id} is not enrolled"
                    ))),
                };
            }
            _ => {
                return Err(NodeError::Authorization(
                    "not a remote operator request".into(),
                ));
            }
        };
        match message {
            ControlMessage::OperatorHello => {
                let key = parse_key_hex(operator.public_key_hex.as_str())
                    .map_err(NodeError::Authentication)?;
                Ok(HttpResponsePayload::OperatorWelcome(Box::new(
                    self.operator_welcome(&operator.operator_id, &key, Some(operator)),
                )))
            }
            ControlMessage::QueryStateSnapshot => {
                Ok(HttpResponsePayload::Snapshot(self.state_snapshot()))
            }
            ControlMessage::QueryObservability => Ok(HttpResponsePayload::Observability(Box::new(
                self.observability_snapshot(),
            ))),
            ControlMessage::QueryStatus(query) => Ok(HttpResponsePayload::Status(
                self.query_status_routed(&query)?,
            )),
            ControlMessage::RunAction(request) => {
                Ok(HttpResponsePayload::Actions(vec![self.submit_action(
                    *request,
                    ActionOrigin::Operator(operator.operator_id.clone()),
                )?]))
            }
            ControlMessage::QueryActions(query) => Ok(HttpResponsePayload::Actions(
                self.query_actions_for_operator(&query, operator),
            )),
            other => Err(NodeError::Authorization(format!(
                "operator {} may not send {other:?}",
                operator.operator_id
            ))),
        }
    }

    /// Every tracked action for operators with read access, else only the operator's own.
    fn query_actions_for_operator(
        &self,
        query: &ActionQuery,
        operator: &AuthenticatedOperator,
    ) -> Vec<orion::control_plane::ActionResult> {
        let mut results = self.query_actions(query);
        if !operator.policy.read {
            let own = operator.operator_id.as_str();
            results.retain(|result| result.requested_by == own);
        }
        results
    }

    fn operator_welcome(
        &self,
        operator_id: &OperatorId,
        operator_key: &[u8; 32],
        enrolled: Option<&AuthenticatedOperator>,
    ) -> OperatorWelcome {
        let node_key = self.security.public_key_bytes();
        let (cluster, enrollment_key_configured) = self.operator_enrollment_context();
        OperatorWelcome {
            node_id: self.config.node_id.clone(),
            node_public_key: node_key.to_vec(),
            node_key_fingerprint: key_fingerprint(&node_key),
            cluster,
            operator_id: operator_id.clone(),
            operator_key_fingerprint: key_fingerprint(operator_key),
            state: match enrolled {
                Some(_) => OperatorTrustState::Enrolled,
                None => OperatorTrustState::Pending,
            },
            enrollment_key_configured,
            read: enrolled.is_some_and(|operator| operator.policy.read),
            allowed_actions: enrolled
                .map(|operator| operator.allowed_actions.clone())
                .unwrap_or_default(),
        }
    }

    /// The discovery cluster name and whether a shared enrollment key is configured.
    fn operator_enrollment_context(&self) -> (String, bool) {
        #[cfg(feature = "discovery-mdns")]
        if let Some(state) = self.discovery_state() {
            return (
                state.config.cluster.clone(),
                state.config.enrollment_key.is_some(),
            );
        }
        (String::new(), false)
    }

    /// `QueryOperators`, `EnrollOperator` and `RemoveOperator` from a local control client.
    pub(crate) fn apply_local_operator_message(
        &self,
        message: ControlMessage,
    ) -> Result<ControlMessage, NodeError> {
        match message {
            ControlMessage::QueryOperators => {
                Ok(ControlMessage::Operators(Box::new(self.query_operators())))
            }
            ControlMessage::EnrollOperator(request) => {
                self.approve_operator(*request)?;
                Ok(ControlMessage::Accepted)
            }
            ControlMessage::RemoveOperator(operator_id) => {
                self.remove_operator(&operator_id)?;
                Ok(ControlMessage::Accepted)
            }
            other => Err(NodeError::Authorization(format!(
                "{other:?} is not an operator administration message"
            ))),
        }
    }

    /// Enrolled, pending and revoked operators.
    pub fn query_operators(&self) -> OperatorsSnapshot {
        let node_key = self.security.public_key_bytes();
        OperatorsSnapshot {
            local_key_fingerprint: key_fingerprint(&node_key),
            default_actions: self.security.operator_default_actions(),
            enrollment_key_configured: self.operator_enrollment_context().1,
            operators: self.security.operator_records(),
        }
    }

    /// Administrator approval of an operator (`orionctl operators enroll`): pins its key (given,
    /// or the key of its pending request) with `request.policy`. Lifts a revocation.
    pub fn approve_operator(&self, request: OperatorEnrollment) -> Result<(), NodeError> {
        let operator_id = request.operator_id;
        let key = match &request.public_key_hex {
            Some(hex) => parse_key_hex(hex.as_str()).map_err(NodeError::Config)?,
            None => self
                .security
                .pending_operator_key(&operator_id)
                .ok_or_else(|| {
                    NodeError::Config(format!(
                        "operator {operator_id} has no pending request on node {}; let it \
                         connect first, or pass its public key",
                        self.config.node_id
                    ))
                })?,
        };
        let fingerprint = key_fingerprint(&key);
        if let Some(expected) = &request.expected_key_fingerprint
            && !expected.eq_ignore_ascii_case(&fingerprint)
        {
            return Err(NodeError::Authentication(format!(
                "the key of operator {operator_id} has fingerprint {fingerprint}, not {expected}"
            )));
        }
        self.enroll_operator_key(
            &operator_id,
            key,
            OperatorEnrollmentMethod::Approval,
            request.policy,
        )
    }

    /// Pins `key` for `operator_id` and records the enrollment in the audit log.
    pub(crate) fn enroll_operator_key(
        &self,
        operator_id: &OperatorId,
        key: [u8; 32],
        method: OperatorEnrollmentMethod,
        policy: OperatorPolicy,
    ) -> Result<(), NodeError> {
        self.security
            .enroll_operator(operator_id, key, method, policy)?;
        let fingerprint = key_fingerprint(&key);
        info!(node = %self.config.node_id, operator = %operator_id, %fingerprint, ?method, "operator enrolled");
        self.record_audit_event(
            AuditEventKind::OperatorEnrolled,
            Some(operator_id.to_string()),
            format!("enrolled operator `{operator_id}` with key {fingerprint} ({method:?})"),
        );
        Ok(())
    }

    /// Revokes an operator (`orionctl operators remove`), persisted; returns whether anything
    /// changed. Enrolling it again is an explicit administrator decision.
    pub fn remove_operator(&self, operator_id: &OperatorId) -> Result<bool, NodeError> {
        let changed = self.security.remove_operator(operator_id)?;
        if changed {
            info!(node = %self.config.node_id, operator = %operator_id, "operator removed");
            self.record_audit_event(
                AuditEventKind::OperatorRemoved,
                Some(operator_id.to_string()),
                format!("removed operator `{operator_id}` (key revoked)"),
            );
        }
        Ok(changed)
    }
}
