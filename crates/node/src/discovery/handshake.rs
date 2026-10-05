//! Both sides of the shared-key enrollment handshake (protocol in [`super::enrollment`]).

use super::{
    config::EnrollmentKey,
    enrollment::{
        NONCE_LEN, PendingChallenge, Role, Transcript, key_array, proof, random_nonce,
        signed_message, transcript_url, verify_proof, verify_signature,
    },
    registry::DiscoveredPeer,
    runtime::DiscoveryState,
    store::EnrollmentMethod,
};
use crate::peer::peer_tcp_authority;
use crate::peer_tcp::{PeerTcpError, ResponseFrame, STATUS_OK};
use crate::{NodeApp, NodeError, PEER_TCP_SCHEME, PeerTransportKind};
use orion::{
    decode_from_slice,
    transport::http::{HttpCodec, HttpRequestPayload, HttpResponsePayload},
};
use orion_control_plane::{
    ControlMessage, ENROLLMENT_PROTOCOL_VERSION, EnrollmentChallenge, EnrollmentConfirm,
    EnrollmentHello, EnrollmentRole, OperatorEnrollmentMethod, OperatorId, OperatorPolicy,
};
use orion_core::PeerBaseUrl;
use orion_transport_ipc::{ControlFrameReadState, write_control_payload_frame};
use std::{future::Future, net::IpAddr, sync::Arc, time::Duration};
use tokio::net::TcpStream;

fn not_enabled() -> NodeError {
    NodeError::Authorization(
        "shared-key enrollment is not enabled on this node (no enrollment key)".into(),
    )
}

impl NodeApp {
    fn enrollment_key(&self) -> Result<(Arc<DiscoveryState>, EnrollmentKey), NodeError> {
        let state = self.discovery_state().ok_or_else(not_enabled)?;
        let key = state
            .config
            .enrollment_key
            .clone()
            .ok_or_else(not_enabled)?;
        Ok((state, key))
    }

    /// Responder, step 2: validates an `EnrollmentHello` and answers with a challenge.
    pub(crate) fn serve_enrollment_hello(
        &self,
        hello: EnrollmentHello,
    ) -> Result<EnrollmentChallenge, NodeError> {
        let (state, key) = self.enrollment_key()?;
        let result = self.issue_enrollment_challenge(&state, &key, hello);
        if result.is_err() {
            state.record_attempt();
            state.record_outcome(false);
        }
        result
    }

    fn issue_enrollment_challenge(
        &self,
        state: &DiscoveryState,
        key: &EnrollmentKey,
        hello: EnrollmentHello,
    ) -> Result<EnrollmentChallenge, NodeError> {
        let reject = |reason: String| Err(NodeError::Authorization(reason));
        if hello.version != ENROLLMENT_PROTOCOL_VERSION {
            return reject(format!(
                "unsupported enrollment protocol version {}",
                hello.version
            ));
        }
        if hello.cluster != state.config.cluster {
            return reject(format!(
                "enrollment for cluster `{}` received by a node of cluster `{}`",
                hello.cluster, state.config.cluster
            ));
        }
        if hello.responder != self.config.node_id || hello.initiator == self.config.node_id {
            return reject(format!(
                "enrollment addressed to {} from {} does not match this node",
                hello.responder, hello.initiator
            ));
        }
        let initiator_key = key_array(&hello.initiator_public_key, "initiator key")?;
        if hello.initiator_nonce.len() != NONCE_LEN {
            return reject("enrollment nonce must be 32 bytes".into());
        }
        match hello.role {
            EnrollmentRole::Node => {
                if OperatorId::is_operator_principal(hello.initiator.as_str()) {
                    return reject(format!(
                        "{} is an operator id; operators enroll with the operator role",
                        hello.initiator
                    ));
                }
                let initiator_url = hello.initiator_url.as_ref().ok_or_else(|| {
                    NodeError::Authorization("enrollment hello lacks a peer URL".into())
                })?;
                PeerTransportKind::check_supported(initiator_url.as_str())
                    .map_err(NodeError::Config)?;
                self.check_auto_enrollable(&hello.initiator, &initiator_key)?;
            }
            EnrollmentRole::Operator => {
                let operator_id = OperatorId::from_principal(&hello.initiator).ok_or_else(|| {
                    NodeError::Authorization(format!(
                        "operator enrollment from `{}`, which is not an operator id",
                        hello.initiator
                    ))
                })?;
                self.security
                    .check_operator_auto_enrollable(&operator_id, &initiator_key)?;
            }
        }

        let responder_key = self.security.public_key_bytes();
        let responder_nonce = random_nonce();
        let transcript = Transcript {
            role: hello.role,
            cluster: &hello.cluster,
            initiator: &hello.initiator,
            initiator_key: &initiator_key,
            initiator_nonce: &hello.initiator_nonce,
            initiator_url: transcript_url(&hello),
            responder: &self.config.node_id,
            responder_key: &responder_key,
            responder_nonce: &responder_nonce,
        }
        .bytes();
        let challenge = EnrollmentChallenge {
            responder: self.config.node_id.clone(),
            responder_public_key: responder_key.to_vec(),
            responder_nonce: responder_nonce.to_vec(),
            proof: proof(key, Role::Responder, &transcript),
            signature: self
                .security
                .sign_bytes(&signed_message(Role::Responder, &transcript)),
        };
        state.pending().insert(PendingChallenge {
            hello,
            responder_nonce,
            transcript,
            issued_at_ms: Self::current_time_ms(),
        });
        Ok(challenge)
    }

    /// Responder, step 4: checks the initiator's proofs and enrolls it.
    pub(crate) fn serve_enrollment_confirm(
        &self,
        confirm: EnrollmentConfirm,
    ) -> Result<(), NodeError> {
        let (state, key) = self.enrollment_key()?;
        state.record_attempt();
        let result = (|| {
            let pending = state
                .pending()
                .take(&confirm.responder_nonce, Self::current_time_ms())
                .ok_or_else(|| {
                    NodeError::Authentication(
                        "unknown, expired or already used enrollment challenge".into(),
                    )
                })?;
            if confirm.initiator != pending.hello.initiator
                || confirm.responder != self.config.node_id
            {
                return Err(NodeError::Authentication(
                    "enrollment confirmation does not match its challenge".into(),
                ));
            }
            verify_proof(&key, Role::Initiator, &pending.transcript, &confirm.proof)?;
            let initiator_key = key_array(&pending.hello.initiator_public_key, "initiator key")?;
            verify_signature(
                &initiator_key,
                Role::Initiator,
                &pending.transcript,
                &confirm.signature,
            )?;
            if pending.hello.role == EnrollmentRole::Operator {
                let operator_id =
                    OperatorId::from_principal(&confirm.initiator).ok_or_else(|| {
                        NodeError::Authorization("operator enrollment without an operator id".into())
                    })?;
                self.security
                    .check_operator_auto_enrollable(&operator_id, &initiator_key)?;
                return self.enroll_operator_key(
                    &operator_id,
                    initiator_key,
                    OperatorEnrollmentMethod::EnrollmentKey,
                    OperatorPolicy::default(),
                );
            }
            let url = pending.hello.initiator_url.clone().ok_or_else(|| {
                NodeError::Authorization("enrollment hello lacks a peer URL".into())
            })?;
            self.enroll_trusted_peer(
                &confirm.initiator,
                initiator_key,
                url,
                EnrollmentMethod::EnrollmentKey,
            )
        })();
        state.record_outcome(result.is_ok());
        result
    }

    /// Shared-key enrollment must not override operator decisions.
    fn check_auto_enrollable(
        &self,
        node_id: &orion::NodeId,
        key: &[u8; 32],
    ) -> Result<(), NodeError> {
        if self.security.is_peer_revoked(node_id)? {
            return Err(NodeError::Authorization(format!(
                "peer {node_id} was removed by an operator"
            )));
        }
        if let Some(trusted) = self.trusted_key_hex(node_id)
            && !trusted.eq_ignore_ascii_case(&super::advert::hex(key))
        {
            return Err(NodeError::Authentication(format!(
                "peer {node_id} is already trusted with a different key"
            )));
        }
        Ok(())
    }

    /// Initiator: runs the handshake with a discovered peer over `orion+tcp` and enrolls it.
    pub(crate) async fn enroll_with_shared_key(
        &self,
        state: &DiscoveryState,
        peer: &DiscoveredPeer,
    ) -> Result<(), NodeError> {
        let key = state
            .config
            .enrollment_key
            .clone()
            .ok_or_else(not_enabled)?;
        state.record_attempt();
        let result = self.run_enrollment_handshake(state, &key, peer).await;
        state.record_outcome(result.is_ok());
        result
    }

    async fn run_enrollment_handshake(
        &self,
        state: &DiscoveryState,
        key: &EnrollmentKey,
        peer: &DiscoveredPeer,
    ) -> Result<(), NodeError> {
        let url = peer.peer_tcp_url().cloned().ok_or_else(|| {
            NodeError::Config(format!(
                "peer {} advertises no orion+tcp URL",
                peer.node_id()
            ))
        })?;
        let local_port = state.advertisement.peer_tcp_port.ok_or_else(|| {
            NodeError::Config("shared-key enrollment needs ORION_NODE_PEER_ADDR".into())
        })?;
        let tuning = &self.config.runtime_tuning;
        let mut connection = EnrollmentConnection::connect(
            peer_tcp_authority(url.as_str()).map_err(PeerTcpError::InvalidAddress)?,
            tuning.transport_io_timeout,
            tuning.transport_max_payload_bytes,
        )
        .await?;
        // The responder reaches us at the address it sees this connection come from.
        let initiator_url = tcp_url(connection.local_ip, local_port);
        let initiator_key = self.security.public_key_bytes();
        let initiator_nonce = random_nonce();
        let hello = EnrollmentHello {
            version: ENROLLMENT_PROTOCOL_VERSION,
            cluster: state.config.cluster.clone(),
            initiator: self.config.node_id.clone(),
            initiator_public_key: initiator_key.to_vec(),
            initiator_nonce: initiator_nonce.to_vec(),
            initiator_url: Some(initiator_url.clone()),
            responder: peer.node_id().clone(),
            role: EnrollmentRole::Node,
        };
        let challenge = match connection
            .exchange(ControlMessage::EnrollmentHello(Box::new(hello)))
            .await?
        {
            HttpResponsePayload::EnrollmentChallenge(challenge) => *challenge,
            _ => {
                return Err(NodeError::Authentication(
                    "peer answered the enrollment hello without a challenge".into(),
                ));
            }
        };
        if &challenge.responder != peer.node_id() {
            return Err(NodeError::Authentication(format!(
                "enrollment challenge came from {}, expected {}",
                challenge.responder,
                peer.node_id()
            )));
        }
        if challenge.responder_public_key != peer.advertisement.public_key {
            return Err(NodeError::Authentication(format!(
                "peer {} answered with a key other than the one it advertises",
                peer.node_id()
            )));
        }
        if challenge.responder_nonce.len() != NONCE_LEN {
            return Err(NodeError::Authentication(
                "enrollment nonce must be 32 bytes".into(),
            ));
        }
        let transcript = Transcript {
            role: EnrollmentRole::Node,
            cluster: &state.config.cluster,
            initiator: &self.config.node_id,
            initiator_key: &initiator_key,
            initiator_nonce: &initiator_nonce,
            initiator_url: initiator_url.as_str(),
            responder: peer.node_id(),
            responder_key: &peer.advertisement.public_key,
            responder_nonce: &challenge.responder_nonce,
        }
        .bytes();
        verify_proof(key, Role::Responder, &transcript, &challenge.proof)?;
        verify_signature(
            &peer.advertisement.public_key,
            Role::Responder,
            &transcript,
            &challenge.signature,
        )?;
        let confirm = EnrollmentConfirm {
            initiator: self.config.node_id.clone(),
            responder: peer.node_id().clone(),
            responder_nonce: challenge.responder_nonce,
            proof: proof(key, Role::Initiator, &transcript),
            signature: self
                .security
                .sign_bytes(&signed_message(Role::Initiator, &transcript)),
        };
        match connection
            .exchange(ControlMessage::EnrollmentConfirm(Box::new(confirm)))
            .await?
        {
            HttpResponsePayload::Accepted => {}
            _ => {
                return Err(NodeError::Authentication(
                    "peer did not accept the enrollment confirmation".into(),
                ));
            }
        }
        let app = self.clone();
        let node_id = peer.node_id().clone();
        let public_key = peer.advertisement.public_key;
        tokio::task::spawn_blocking(move || {
            app.enroll_trusted_peer(&node_id, public_key, url, EnrollmentMethod::EnrollmentKey)
        })
        .await
        .map_err(|err| NodeError::Storage(format!("enrollment task failed: {err}")))?
    }
}

fn tcp_url(ip: IpAddr, port: u16) -> PeerBaseUrl {
    match ip {
        IpAddr::V4(v4) => PeerBaseUrl::new(format!("{PEER_TCP_SCHEME}://{v4}:{port}")),
        IpAddr::V6(v6) => PeerBaseUrl::new(format!("{PEER_TCP_SCHEME}://[{v6}]:{port}")),
    }
}

/// One `orion+tcp` connection used for the two requests of a handshake. Enrollment requests
/// are not signed (the peer does not know our key yet); the handshake carries its own proofs.
struct EnrollmentConnection {
    stream: TcpStream,
    read_state: ControlFrameReadState,
    local_ip: IpAddr,
    addr: String,
    io_timeout: Duration,
    max_payload_bytes: usize,
}

impl EnrollmentConnection {
    async fn connect(
        authority: &str,
        io_timeout: Duration,
        max_payload_bytes: usize,
    ) -> Result<Self, NodeError> {
        let addr = authority.to_owned();
        let stream = timed(&addr, "connect", io_timeout, TcpStream::connect(authority))
            .await?
            .map_err(|err| PeerTcpError::Connect {
                addr: addr.clone(),
                message: err.to_string(),
            })?;
        let local_ip = stream
            .local_addr()
            .map_err(|err| PeerTcpError::Connection {
                addr: addr.clone(),
                message: err.to_string(),
            })?
            .ip();
        let _ = stream.set_nodelay(true);
        Ok(Self {
            stream,
            read_state: ControlFrameReadState::new(),
            local_ip,
            addr,
            io_timeout,
            max_payload_bytes,
        })
    }

    async fn exchange(
        &mut self,
        message: ControlMessage,
    ) -> Result<HttpResponsePayload, NodeError> {
        let request = HttpCodec
            .encode_request(&HttpRequestPayload::Control(Box::new(message)))?
            .body;
        timed(
            &self.addr,
            "write",
            self.io_timeout,
            write_control_payload_frame(&mut self.stream, &request, self.max_payload_bytes),
        )
        .await?
        .map_err(PeerTcpError::Frame)?;
        let response = timed(
            &self.addr,
            "read",
            self.io_timeout,
            self.read_state
                .read_payload(&mut self.stream, self.max_payload_bytes),
        )
        .await?
        .map_err(PeerTcpError::Frame)?
        .ok_or_else(|| PeerTcpError::Closed {
            addr: self.addr.clone(),
        })?;
        let frame = ResponseFrame::decode(&response).map_err(PeerTcpError::from)?;
        if frame.status != STATUS_OK {
            return Err(
                PeerTcpError::Remote(String::from_utf8_lossy(&frame.body).into_owned()).into(),
            );
        }
        Ok(decode_from_slice::<HttpResponsePayload>(&frame.body)
            .map_err(|err| PeerTcpError::Decode(err.to_string()))?)
    }
}

async fn timed<T>(
    addr: &str,
    operation: &'static str,
    timeout: Duration,
    future: impl Future<Output = T>,
) -> Result<T, PeerTcpError> {
    tokio::time::timeout(timeout, future)
        .await
        .map_err(|_| PeerTcpError::Timeout {
            addr: addr.to_owned(),
            operation,
            timeout_ms: timeout.as_millis().min(u128::from(u64::MAX)) as u64,
        })
}
