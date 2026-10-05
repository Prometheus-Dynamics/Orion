//! [`RemoteOperator`]: one signed `orion+tcp` session with one node.

use super::{OperatorIdentity, RemoteError};
use orion_auth::{
    PeerRequestPayload,
    crypto::{key_fingerprint, public_key_array, sign_peer_request, verify_peer_response},
    peer_tcp::{PEER_TCP_STATUS_OK, PeerTcpResponseFrame, peer_tcp_authority},
};
use orion_control_plane::{ControlMessage, OperatorTrustState, OperatorWelcome};
use orion_core::{NodeId, decode_from_slice};
use orion_transport_http::{HttpCodec, HttpRequestPayload, HttpResponsePayload};
use orion_transport_ipc::{ControlTcpClient, ControlTcpError, IpcTransportError};
use rand_core::{OsRng, RngCore};
use std::{
    sync::{Arc, RwLock},
    time::Duration,
};

/// How the client decides that it talks to the right node. Every response is signed by the
/// node's ed25519 key and verified against the key chosen here.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum NodeTrust {
    /// Accept the key the node presents on connect and pin it for the session. Read it with
    /// [`RemoteOperator::node_public_key`], compare its fingerprint out of band (or let
    /// [`RemoteOperator::enroll_with_key`] authenticate it), and store it for later sessions.
    FirstUse,
    /// Only a node with this public key (for example stored from an earlier session, or taken
    /// from a discovery advertisement whose fingerprint was checked).
    Key([u8; 32]),
    /// Only this node id with this public key.
    Node { node_id: NodeId, public_key: [u8; 32] },
}

/// Client settings.
#[derive(Clone, Debug)]
pub struct RemoteOperatorConfig {
    /// Bound on every connect, write and read.
    pub io_timeout: Duration,
    /// Largest frame accepted in either direction.
    pub max_payload_bytes: usize,
    /// Poll interval of [`RemoteOperator::wait_for_action`] and the default of the watches.
    pub poll_interval: Duration,
}

impl Default for RemoteOperatorConfig {
    fn default() -> Self {
        Self {
            io_timeout: Duration::from_secs(5),
            max_payload_bytes: 8 * 1024 * 1024,
            poll_interval: Duration::from_millis(500),
        }
    }
}

#[derive(Clone, Debug)]
struct BoundNode {
    node_id: NodeId,
    public_key: [u8; 32],
}

struct Inner {
    url: String,
    identity: OperatorIdentity,
    config: RemoteOperatorConfig,
    client: ControlTcpClient,
    node: BoundNode,
    welcome: RwLock<OperatorWelcome>,
}

/// A remote operator session with one node over the signed `orion+tcp` transport.
///
/// Cheap to clone; clones share the connection (requests on it are sequential). Every request is
/// signed with the operator's key and a fresh random nonce; every response must carry a valid
/// signature of the node's key, bound to the request it answers.
#[derive(Clone)]
pub struct RemoteOperator {
    inner: Arc<Inner>,
}

impl std::fmt::Debug for RemoteOperator {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RemoteOperator")
            .field("url", &self.inner.url)
            .field("node_id", &self.inner.node.node_id)
            .field("operator_id", self.inner.identity.operator_id())
            .finish()
    }
}

impl RemoteOperator {
    /// Connects to the node at `url` (`orion+tcp://host:port`), checks its identity against
    /// `trust`, and introduces the operator (`OperatorHello`). Succeeds for operators that are
    /// not enrolled yet: check [`Self::is_enrolled`], then enroll with
    /// [`Self::enroll_with_key`] or let an administrator approve the fingerprint.
    pub async fn connect(
        url: &str,
        identity: OperatorIdentity,
        trust: NodeTrust,
    ) -> Result<Self, RemoteError> {
        Self::connect_with(url, identity, trust, RemoteOperatorConfig::default()).await
    }

    /// [`Self::connect`] with explicit settings.
    pub async fn connect_with(
        url: &str,
        identity: OperatorIdentity,
        trust: NodeTrust,
        config: RemoteOperatorConfig,
    ) -> Result<Self, RemoteError> {
        let authority = peer_tcp_authority(url).map_err(RemoteError::InvalidUrl)?;
        let client = ControlTcpClient::new(authority, config.io_timeout, config.max_payload_bytes);
        let request = encode_signed(&identity, ControlMessage::OperatorHello)?;
        let response = exchange(&client, &request).await?;
        let frame = PeerTcpResponseFrame::decode(&response)
            .map_err(|err| RemoteError::Decode(err.to_string()))?;
        let signature = frame.signature.as_ref().ok_or_else(|| {
            RemoteError::NodeAuthentication(
                "the node sent an unsigned response (is ORION_NODE_PEER_AUTH disabled?)".into(),
            )
        })?;
        if frame.status != PEER_TCP_STATUS_OK {
            // The node id is unknown before a welcome, so the signature over an error cannot be
            // checked unless the caller pinned the node.
            let message = String::from_utf8_lossy(&frame.body).into_owned();
            if let NodeTrust::Node {
                node_id,
                public_key,
            } = &trust
            {
                verify_peer_response(
                    node_id,
                    public_key,
                    &request,
                    frame.status,
                    &frame.body,
                    signature,
                )
                .map_err(|err| RemoteError::NodeAuthentication(err.to_string()))?;
                return Err(RemoteError::Rejected(message));
            }
            return Err(RemoteError::Rejected(format!(
                "{message} (unverified: the node is not pinned)"
            )));
        }
        let welcome = match decode_payload(&frame.body)? {
            HttpResponsePayload::OperatorWelcome(welcome) => *welcome,
            other => return Err(unexpected("OperatorHello", &other)),
        };
        let public_key = public_key_array(&welcome.node_public_key)
            .map_err(|err| RemoteError::NodeAuthentication(err.to_string()))?;
        match &trust {
            NodeTrust::FirstUse => {}
            NodeTrust::Key(expected) if expected == &public_key => {}
            NodeTrust::Node {
                node_id,
                public_key: expected,
            } if node_id == &welcome.node_id && expected == &public_key => {}
            _ => {
                return Err(RemoteError::NodeAuthentication(format!(
                    "node {} presents key {}, which is not the trusted key",
                    welcome.node_id,
                    key_fingerprint(&public_key)
                )));
            }
        }
        verify_peer_response(
            &welcome.node_id,
            &public_key,
            &request,
            frame.status,
            &frame.body,
            signature,
        )
        .map_err(|err| RemoteError::NodeAuthentication(err.to_string()))?;
        if welcome.operator_id != *identity.operator_id() {
            return Err(RemoteError::NodeAuthentication(format!(
                "the node welcomed {} instead of {}",
                welcome.operator_id,
                identity.operator_id()
            )));
        }
        Ok(Self {
            inner: Arc::new(Inner {
                url: url.to_owned(),
                identity,
                config,
                client,
                node: BoundNode {
                    node_id: welcome.node_id.clone(),
                    public_key,
                },
                welcome: RwLock::new(welcome),
            }),
        })
    }

    /// The node this session is bound to.
    pub fn node_id(&self) -> &NodeId {
        &self.inner.node.node_id
    }

    /// The node's ed25519 public key (store it and connect with [`NodeTrust::Node`] next time).
    pub fn node_public_key(&self) -> [u8; 32] {
        self.inner.node.public_key
    }

    /// `sha256:<32 hex>` of the node's key, as `orionctl get operators` prints it on the node.
    pub fn node_fingerprint(&self) -> String {
        key_fingerprint(&self.inner.node.public_key)
    }

    pub fn url(&self) -> &str {
        &self.inner.url
    }

    pub fn identity(&self) -> &OperatorIdentity {
        &self.inner.identity
    }

    pub fn config(&self) -> &RemoteOperatorConfig {
        &self.inner.config
    }

    /// The node's last answer to `OperatorHello` (see [`Self::hello`]).
    pub fn welcome(&self) -> OperatorWelcome {
        self.inner
            .welcome
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }

    /// Whether the operator was enrolled on the node at the last hello.
    pub fn is_enrolled(&self) -> bool {
        self.welcome().state == OperatorTrustState::Enrolled
    }

    /// Introduces the operator again and refreshes [`Self::welcome`] (for example after an
    /// administrator approved it or changed its policy).
    pub async fn hello(&self) -> Result<OperatorWelcome, RemoteError> {
        let welcome = match self.request(ControlMessage::OperatorHello).await? {
            HttpResponsePayload::OperatorWelcome(welcome) => *welcome,
            other => return Err(unexpected("OperatorHello", &other)),
        };
        *self
            .inner
            .welcome
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = welcome.clone();
        Ok(welcome)
    }

    /// Sends one signed control message and returns the node's verified answer.
    pub(crate) async fn request(
        &self,
        message: ControlMessage,
    ) -> Result<HttpResponsePayload, RemoteError> {
        let request = encode_signed(&self.inner.identity, message)?;
        self.exchange_verified(&request).await
    }

    /// Sends one unsigned control message (only the enrollment handshake) and returns the node's
    /// verified answer.
    pub(crate) async fn request_unsigned(
        &self,
        message: ControlMessage,
    ) -> Result<HttpResponsePayload, RemoteError> {
        let request = HttpCodec
            .encode_request(&HttpRequestPayload::Control(Box::new(message)))
            .map_err(|err| RemoteError::Decode(err.to_string()))?
            .body;
        self.exchange_verified(&request).await
    }

    async fn exchange_verified(&self, request: &[u8]) -> Result<HttpResponsePayload, RemoteError> {
        let response = exchange(&self.inner.client, request).await?;
        let frame = PeerTcpResponseFrame::decode(&response)
            .map_err(|err| RemoteError::Decode(err.to_string()))?;
        let signature = frame.signature.as_ref().ok_or_else(|| {
            RemoteError::NodeAuthentication(format!(
                "node {} sent an unsigned response",
                self.inner.node.node_id
            ))
        })?;
        verify_peer_response(
            &self.inner.node.node_id,
            &self.inner.node.public_key,
            request,
            frame.status,
            &frame.body,
            signature,
        )
        .map_err(|err| RemoteError::NodeAuthentication(err.to_string()))?;
        if frame.status != PEER_TCP_STATUS_OK {
            return Err(RemoteError::Rejected(
                String::from_utf8_lossy(&frame.body).into_owned(),
            ));
        }
        decode_payload(&frame.body)
    }
}

fn encode_signed(
    identity: &OperatorIdentity,
    message: ControlMessage,
) -> Result<Vec<u8>, RemoteError> {
    // Nodes remember a window of recent nonces per principal; random 64-bit nonces need no
    // client-side state and do not repeat in practice.
    let nonce = OsRng.next_u64();
    let request = sign_peer_request(
        identity.signing_key(),
        &identity.principal(),
        nonce,
        PeerRequestPayload::Control(Box::new(message)),
    )
    .map_err(|err| RemoteError::InvalidIdentity(err.to_string()))?;
    Ok(HttpCodec
        .encode_request(&HttpRequestPayload::AuthenticatedPeer(request))
        .map_err(|err| RemoteError::Decode(err.to_string()))?
        .body)
}

async fn exchange(client: &ControlTcpClient, request: &[u8]) -> Result<Vec<u8>, RemoteError> {
    match client.exchange(request).await {
        Ok((response, _)) => Ok(response),
        Err(ControlTcpError::Frame(IpcTransportError::ProtocolMismatch { local, remote })) => {
            Err(RemoteError::ProtocolMismatch { local, remote })
        }
        Err(err) => Err(RemoteError::Transport(err.to_string())),
    }
}

fn decode_payload(body: &[u8]) -> Result<HttpResponsePayload, RemoteError> {
    decode_from_slice::<HttpResponsePayload>(body).map_err(|err| RemoteError::Decode(err.to_string()))
}

pub(crate) fn unexpected(request: &str, response: &HttpResponsePayload) -> RemoteError {
    let kind = match response {
        HttpResponsePayload::Accepted => "accepted",
        HttpResponsePayload::Hello(_) => "hello",
        HttpResponsePayload::Summary(_) => "summary",
        HttpResponsePayload::Snapshot(_) => "snapshot",
        HttpResponsePayload::Mutations(_) => "mutations",
        HttpResponsePayload::Observability(_) => "observability",
        HttpResponsePayload::Health(_) => "health",
        HttpResponsePayload::Readiness(_) => "readiness",
        HttpResponsePayload::EnrollmentChallenge(_) => "enrollment challenge",
        HttpResponsePayload::Actions(_) => "actions",
        HttpResponsePayload::OperatorWelcome(_) => "operator welcome",
        HttpResponsePayload::Status(_) => "status",
    };
    RemoteError::UnexpectedResponse(format!("{kind} response to {request}"))
}
