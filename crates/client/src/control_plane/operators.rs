//! Remote operator administration on the local socket (`docs/remote-operator.md`).

use super::LocalControlPlaneClient;
use crate::{error::ClientError, response::expect_accepted};
use orion_control_plane::{ControlMessage, OperatorEnrollment, OperatorId, OperatorsSnapshot};

impl LocalControlPlaneClient {
    /// Enrolled, pending and revoked remote operators, and the node's default action patterns.
    pub async fn query_operators(&self) -> Result<OperatorsSnapshot, ClientError> {
        self.send_request_with(ControlMessage::QueryOperators, |message| match message {
            ControlMessage::Operators(snapshot) => Ok(*snapshot),
            ControlMessage::Rejected(reason) => Err(ClientError::Rejected(reason)),
            _ => Err(ClientError::NoMessageAvailable),
        })
        .await
    }

    /// Approves a remote operator: pins its key (given, or the key of its pending request,
    /// optionally only if it has `expected_key_fingerprint`) with a policy. Lifts a revocation.
    pub async fn enroll_operator(&self, enrollment: OperatorEnrollment) -> Result<(), ClientError> {
        self.send_request_with(
            ControlMessage::EnrollOperator(Box::new(enrollment)),
            expect_accepted,
        )
        .await
    }

    /// Revokes a remote operator (persisted; it is never enrolled again automatically).
    pub async fn remove_operator(&self, operator_id: OperatorId) -> Result<(), ClientError> {
        self.send_request_with(ControlMessage::RemoveOperator(operator_id), expect_accepted)
            .await
    }
}
