//! Read, action and enrollment calls of [`RemoteOperator`].

use super::{RemoteError, RemoteOperator, operator::unexpected};
use orion_auth::enrollment::{
    ENROLLMENT_NONCE_LEN, EnrollmentSide, EnrollmentTranscript, enrollment_proof,
    sign_enrollment, verify_enrollment_proof, verify_enrollment_signature,
};
use orion_control_plane::{
    ActionQuery, ActionRequest, ActionResult, ControlMessage, ENROLLMENT_PROTOCOL_VERSION,
    EnrollmentConfirm, EnrollmentHello, EnrollmentRole, NodeObservabilitySnapshot, NodeRecord,
    OperatorTrustState, OperatorWelcome, StateSnapshot, StatusEntry, StatusQuery,
};
use orion_core::NodeId;
use orion_transport_http::HttpResponsePayload;
use rand_core::{OsRng, RngCore};
use std::{collections::BTreeMap, time::Duration};
use tokio::time::Instant;

impl RemoteOperator {
    /// Runs the shared-key enrollment handshake (`docs/discovery.md`) with the node as an
    /// operator. `key` is the cluster's enrollment key (`ORION_NODE_ENROLLMENT_KEY`).
    ///
    /// The handshake also authenticates the node: its proof covers its key, so a session opened
    /// with [`super::NodeTrust::FirstUse`] is bound to a node that knows the enrollment key once
    /// this returns. Operators enrolled this way get read access and the node's default action
    /// patterns (`ORION_NODE_OPERATOR_ACTIONS`).
    pub async fn enroll_with_key(&self, key: &[u8]) -> Result<OperatorWelcome, RemoteError> {
        let welcome = self.welcome();
        let identity = self.identity();
        let initiator = identity.principal();
        let initiator_key = identity.public_key();
        let mut initiator_nonce = [0u8; ENROLLMENT_NONCE_LEN];
        OsRng.fill_bytes(&mut initiator_nonce);
        let responder = self.node_id().clone();
        let responder_key = self.node_public_key();
        let hello = EnrollmentHello {
            version: ENROLLMENT_PROTOCOL_VERSION,
            cluster: welcome.cluster.clone(),
            initiator: initiator.clone(),
            initiator_public_key: initiator_key.to_vec(),
            initiator_nonce: initiator_nonce.to_vec(),
            initiator_url: None,
            responder: responder.clone(),
            role: EnrollmentRole::Operator,
        };
        let challenge = match self
            .request_unsigned(ControlMessage::EnrollmentHello(Box::new(hello)))
            .await
            .map_err(enrollment_error)?
        {
            HttpResponsePayload::EnrollmentChallenge(challenge) => *challenge,
            other => return Err(unexpected("EnrollmentHello", &other)),
        };
        if challenge.responder != responder
            || challenge.responder_public_key != responder_key
            || challenge.responder_nonce.len() != ENROLLMENT_NONCE_LEN
        {
            return Err(RemoteError::Enrollment(
                "the node's challenge does not match its identity".into(),
            ));
        }
        let transcript = EnrollmentTranscript {
            role: EnrollmentRole::Operator,
            cluster: &welcome.cluster,
            initiator: &initiator,
            initiator_key: &initiator_key,
            initiator_nonce: &initiator_nonce,
            initiator_url: "",
            responder: &responder,
            responder_key: &responder_key,
            responder_nonce: &challenge.responder_nonce,
        }
        .bytes();
        verify_enrollment_proof(
            key,
            EnrollmentSide::Responder,
            &transcript,
            &challenge.proof,
        )
        .map_err(|err| RemoteError::Enrollment(err.to_string()))?;
        verify_enrollment_signature(
            &responder_key,
            EnrollmentSide::Responder,
            &transcript,
            &challenge.signature,
        )
        .map_err(|err| RemoteError::Enrollment(err.to_string()))?;
        let confirm = EnrollmentConfirm {
            initiator,
            responder,
            responder_nonce: challenge.responder_nonce,
            proof: enrollment_proof(key, EnrollmentSide::Initiator, &transcript),
            signature: sign_enrollment(
                identity.signing_key(),
                EnrollmentSide::Initiator,
                &transcript,
            ),
        };
        match self
            .request_unsigned(ControlMessage::EnrollmentConfirm(Box::new(confirm)))
            .await
            .map_err(enrollment_error)?
        {
            HttpResponsePayload::Accepted => {}
            other => return Err(unexpected("EnrollmentConfirm", &other)),
        }
        let welcome = self.hello().await?;
        if welcome.state != OperatorTrustState::Enrolled {
            return Err(RemoteError::Enrollment(format!(
                "the node accepted the handshake but reports the operator as {}",
                welcome.state
            )));
        }
        Ok(welcome)
    }

    /// The node's state snapshot: desired state and the converged observed state of every node
    /// it syncs with (needs read access).
    pub async fn state_snapshot(&self) -> Result<StateSnapshot, RemoteError> {
        match self.request(ControlMessage::QueryStateSnapshot).await? {
            HttpResponsePayload::Snapshot(snapshot) => Ok(snapshot),
            other => Err(unexpected("QueryStateSnapshot", &other)),
        }
    }

    /// Every node record the node knows, cluster-wide, sorted by id, merged like `orionctl get
    /// nodes`: desired records with the clock and host facts each node reports about itself
    /// (replicated with its observed slice), plus nodes that are only in observed state.
    pub async fn nodes(&self) -> Result<Vec<NodeRecord>, RemoteError> {
        let snapshot = self.state_snapshot().await?;
        Ok(effective_nodes(snapshot).into_values().collect())
    }

    /// One node record (see [`Self::nodes`]).
    pub async fn node(&self, node_id: &NodeId) -> Result<Option<NodeRecord>, RemoteError> {
        let snapshot = self.state_snapshot().await?;
        Ok(effective_nodes(snapshot).remove(node_id))
    }

    /// The node's volatile status lane (its own entries: host metrics, provider and action
    /// status; the lane is not replicated between nodes).
    pub async fn status(&self, query: StatusQuery) -> Result<Vec<StatusEntry>, RemoteError> {
        match self.request(ControlMessage::QueryStatus(query)).await? {
            HttpResponsePayload::Status(entries) => Ok(entries),
            other => Err(unexpected("QueryStatus", &other)),
        }
    }

    /// The node's observability snapshot (needs read access).
    pub async fn observability(&self) -> Result<NodeObservabilitySnapshot, RemoteError> {
        match self.request(ControlMessage::QueryObservability).await? {
            HttpResponsePayload::Observability(snapshot) => Ok(*snapshot),
            other => Err(unexpected("QueryObservability", &other)),
        }
    }

    /// Submits an action. The node checks the operator's policy for the action name, stamps
    /// `requested_by` with the operator id, and routes it (forwarding it once to the node that
    /// owns the target). Returns its current result; resubmitting the same request is safe.
    pub async fn run_action(&self, request: ActionRequest) -> Result<ActionResult, RemoteError> {
        match self
            .request(ControlMessage::RunAction(Box::new(request)))
            .await?
        {
            HttpResponsePayload::Actions(mut results) if !results.is_empty() => {
                Ok(results.remove(0))
            }
            other => Err(unexpected("RunAction", &other)),
        }
    }

    /// Tracked actions matching `query` (every action with read access, else the operator's own).
    pub async fn query_actions(&self, query: ActionQuery) -> Result<Vec<ActionResult>, RemoteError> {
        match self.request(ControlMessage::QueryActions(query)).await? {
            HttpResponsePayload::Actions(results) => Ok(results),
            other => Err(unexpected("QueryActions", &other)),
        }
    }

    /// The current result of one action, if the node still tracks it.
    pub async fn action(&self, action_id: &str) -> Result<Option<ActionResult>, RemoteError> {
        let results = self
            .query_actions(ActionQuery {
                action_id: Some(action_id.to_owned()),
                target: None,
            })
            .await?;
        Ok(results
            .into_iter()
            .find(|result| result.action_id == action_id))
    }

    /// Polls an action every [`super::RemoteOperatorConfig::poll_interval`] until it is final
    /// (succeeded, failed, rejected or timed out) or `timeout` passes.
    pub async fn wait_for_action(
        &self,
        action_id: &str,
        timeout: Duration,
    ) -> Result<ActionResult, RemoteError> {
        let deadline = Instant::now() + timeout;
        loop {
            let result = self
                .action(action_id)
                .await?
                .ok_or_else(|| RemoteError::UnknownAction(action_id.to_owned()))?;
            if result.state.is_terminal() {
                return Ok(result);
            }
            let now = Instant::now();
            if now >= deadline {
                return Err(RemoteError::ActionTimeout {
                    action_id: action_id.to_owned(),
                    timeout_ms: u64::try_from(timeout.as_millis()).unwrap_or(u64::MAX),
                });
            }
            tokio::time::sleep(self.config().poll_interval.min(deadline - now)).await;
        }
    }
}

fn effective_nodes(snapshot: StateSnapshot) -> BTreeMap<NodeId, NodeRecord> {
    let mut nodes = snapshot.state.desired.nodes;
    for (node_id, observed) in snapshot.state.observed.nodes {
        match nodes.get_mut(&node_id) {
            Some(node) => {
                node.clock = observed.clock;
                node.host = observed.host;
            }
            None => {
                nodes.insert(node_id, observed);
            }
        }
    }
    nodes
}

fn enrollment_error(err: RemoteError) -> RemoteError {
    match err {
        RemoteError::Rejected(message) => RemoteError::Enrollment(message),
        other => other,
    }
}
