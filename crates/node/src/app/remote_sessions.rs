//! Remote push sessions on the node side (`docs/remote-operator.md`, "Push sessions").
//!
//! A session is a local client under a reserved address (`REMOTE_SESSION_PREFIX`), so the watch
//! machinery local stream clients use (bootstrap, coalesced changes, bounded queues) serves it
//! unchanged. The `orion+tcp` server attaches the session's connection as the client's stream;
//! the operator's watches arrive as signed requests on that connection and are applied to the
//! client after the operator's policy allowed them.

use super::{NodeApp, NodeError};
use orion::{
    control_plane::{
        ClientHello, ClientRole, ControlMessage, OperatorId, REMOTE_SESSION_PREFIX, SessionOpen,
        SessionWelcome,
    },
    transport::{http::HttpResponsePayload, ipc::LocalAddress},
};
use orion_core::{ClientName, NodeId};
use std::sync::atomic::{AtomicU64, Ordering};

/// A session that sends nothing (not even a ping) for this long is closed.
pub(crate) const REMOTE_SESSION_IDLE_TIMEOUT: std::time::Duration =
    std::time::Duration::from_secs(30);

static NEXT_SESSION: AtomicU64 = AtomicU64::new(1);

/// The address prefix of `operator`'s sessions.
fn session_prefix(operator: &OperatorId) -> String {
    format!("{REMOTE_SESSION_PREFIX}{operator}/")
}

/// The address prefix of a peer's sessions (`peer:<node>/`, never an operator id).
fn peer_session_prefix(peer: &NodeId) -> String {
    format!("{REMOTE_SESSION_PREFIX}peer:{peer}/")
}

impl NodeApp {
    /// `OpenSession` from an enrolled operator: registers the session's client.
    pub(super) fn open_remote_session(
        &self,
        operator: &OperatorId,
        open: SessionOpen,
    ) -> Result<HttpResponsePayload, NodeError> {
        self.open_session_at(&session_prefix(operator), operator.to_string(), open)
    }

    /// `OpenSession` from a configured peer: its push session for the actions it forwarded.
    pub(crate) fn open_peer_session(
        &self,
        peer: &NodeId,
        open: SessionOpen,
    ) -> Result<HttpResponsePayload, NodeError> {
        self.open_session_at(&peer_session_prefix(peer), format!("peer:{peer}"), open)
    }

    fn open_session_at(
        &self,
        prefix: &str,
        default_name: String,
        open: SessionOpen,
    ) -> Result<HttpResponsePayload, NodeError> {
        let n = NEXT_SESSION.fetch_add(1, Ordering::Relaxed);
        let source = LocalAddress::new(format!("{prefix}{n}"));
        let client_name = if open.client_name.trim().is_empty() {
            default_name
        } else {
            open.client_name
        };
        match self.apply_local_control_message(
            &source,
            ControlMessage::ClientHello(ClientHello {
                client_name: ClientName::new(client_name),
                // Never seen by the local role checks: nothing on the local socket may use the
                // reserved address, and the operator's policy decides what the session may do.
                role: ClientRole::ControlPlane,
            }),
        )? {
            ControlMessage::ClientWelcome(_) => {}
            ControlMessage::Rejected(why) => return Err(NodeError::Authorization(why)),
            other => {
                return Err(NodeError::Storage(format!(
                    "unexpected answer to a session's hello: {other:?}"
                )));
            }
        }
        Ok(HttpResponsePayload::Session(SessionWelcome {
            session_id: source.as_str().to_owned(),
            idle_timeout_ms: u64::try_from(REMOTE_SESSION_IDLE_TIMEOUT.as_millis())
                .unwrap_or(u64::MAX),
        }))
    }

    /// A watch or ping on `operator`'s session `session` (already allowed by its policy).
    pub(super) fn apply_remote_session_message(
        &self,
        operator: &OperatorId,
        session: Option<&LocalAddress>,
        message: ControlMessage,
    ) -> Result<HttpResponsePayload, NodeError> {
        let Some(session) = session.filter(|s| s.as_str().starts_with(&session_prefix(operator)))
        else {
            return Err(NodeError::Authorization(format!(
                "operator {operator}: watches need a push session (OpenSession on its own \
                 orion+tcp connection)"
            )));
        };
        self.serve_session_message(session, message)
    }

    /// A peer's watch or ping on its own session. The only watch a peer may hold is over the
    /// actions it forwarded: its `forwarded_by` is forced to `peer`, and a query naming another
    /// peer is refused (`docs/actions.md`, "Peer watches").
    pub(crate) fn apply_peer_session_message(
        &self,
        peer: &NodeId,
        session: Option<&LocalAddress>,
        message: ControlMessage,
    ) -> Result<HttpResponsePayload, NodeError> {
        let Some(session) = session.filter(|s| s.as_str().starts_with(&peer_session_prefix(peer)))
        else {
            return Err(NodeError::Authorization(format!(
                "peer {peer}: watches need its push session (OpenSession on its own orion+tcp \
                 connection)"
            )));
        };
        let message = match message {
            ControlMessage::WatchActions(query) => {
                if query
                    .forwarded_by
                    .as_ref()
                    .is_some_and(|other| other != peer)
                {
                    return Err(NodeError::Authorization(format!(
                        "peer {peer} may watch only the actions it forwarded"
                    )));
                }
                ControlMessage::WatchActions(query.forwarded_by(peer.clone()))
            }
            ControlMessage::Ping => ControlMessage::Ping,
            _ => {
                return Err(NodeError::Authorization(format!(
                    "peer {peer}: a push session takes only WatchActions and Ping"
                )));
            }
        };
        self.serve_session_message(session, message)
    }

    /// Applies one message on a session's client and flushes what it queued.
    fn serve_session_message(
        &self,
        session: &LocalAddress,
        message: ControlMessage,
    ) -> Result<HttpResponsePayload, NodeError> {
        let answer = self.apply_local_control_message(session, message)?;
        // A new watch queues its bootstrap: send it now, as the local stream server does after
        // every message.
        self.flush_client_stream_for_source(session);
        match answer {
            ControlMessage::Accepted | ControlMessage::Pong => Ok(HttpResponsePayload::Accepted),
            ControlMessage::Rejected(why) => Err(NodeError::Authorization(why)),
            other => Err(NodeError::Storage(format!(
                "unexpected answer on a push session: {other:?}"
            ))),
        }
    }

    /// Attaches a session's connection as its client stream; pushed events go to `sender`.
    #[cfg(feature = "peer-tcp")]
    pub(crate) fn attach_remote_session(
        &self,
        session: &LocalAddress,
        sender: tokio::sync::mpsc::Sender<orion::transport::ipc::ControlEnvelope>,
    ) -> Result<(), NodeError> {
        self.attach_local_client_stream(session, sender)?;
        self.flush_client_stream_for_source(session);
        Ok(())
    }

    /// The session's connection closed: forgets the session and its watches.
    #[cfg(feature = "peer-tcp")]
    pub(crate) fn close_remote_session(
        &self,
        session: &LocalAddress,
        sender: &tokio::sync::mpsc::Sender<orion::transport::ipc::ControlEnvelope>,
    ) {
        self.detach_local_client_stream(session, sender);
        self.with_client_registry_txn(|txn| {
            txn.remove(session);
        });
    }
}

/// Whether `source` is a remote session's reserved address (refused on the local socket).
pub(crate) fn is_remote_session_address(source: &LocalAddress) -> bool {
    source.as_str().starts_with(REMOTE_SESSION_PREFIX)
}
