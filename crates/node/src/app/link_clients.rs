//! Clients of link sessions (interface revision 1 on Unix links): each session that takes the
//! client role is a local client of the node, registered under `link:<link>/<device name>` with
//! the caller's peer identity. Requests go through the control pipeline as the control sockets'
//! do, so the same authorization and rate limit apply; the link drains the client's event queue
//! itself (`link_wake`) instead of a stream.

use super::{NodeApp, NodeError};
use crate::service::{ControlRequest, ControlSurface};
use orion::{
    control_plane::{
        ClientEvent, ClientEventKind, ClientHello, ClientRole, ClientSession, ControlMessage,
        SampleChunk, SeriesDecl, StatusSubject, StreamDecl, StreamRef,
    },
    transport::ipc::{LocalAddress, UnixPeerIdentity},
};
use orion_core::SessionId;
use std::sync::Arc;
use tokio::sync::Notify;

/// Who a link session's client acts for.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum LinkCaller {
    /// The caller on a Unix link (`SO_PEERCRED`), authorized by the local policy.
    Local(UnixPeerIdentity),
    /// An enrolled operator on a `tcp:` link, authenticated by the link's handshake and
    /// authorized by its policy, looked up again for every request.
    Operator { principal: String, key: [u8; 32] },
}

/// Which of a link client's watches to end.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum LinkWatch {
    Status,
    Actions,
}

impl NodeApp {
    /// Serves one control message for a link client, through the control pipeline. A refusal is
    /// an error, as `Rejected` answers are turned into [`NodeError::Action`].
    pub(crate) fn serve_link_client_message(
        &self,
        source: &LocalAddress,
        caller: &LinkCaller,
        message: ControlMessage,
    ) -> Result<ControlMessage, NodeError> {
        self.serve_link_request(self.link_request(source, caller, message)?)
    }

    /// Whether `caller` may read the node's records (a link session's record query or watch):
    /// authorized as a state snapshot query, which they are a filtered view of.
    pub(crate) fn authorize_link_read(
        &self,
        source: &LocalAddress,
        caller: &LinkCaller,
    ) -> Result<(), NodeError> {
        self.serve_link_request(
            self.link_request(source, caller, ControlMessage::QueryStateSnapshot)?
                .check_only(),
        )
        .map(drop)
    }

    fn link_request(
        &self,
        source: &LocalAddress,
        caller: &LinkCaller,
        message: ControlMessage,
    ) -> Result<ControlRequest, NodeError> {
        Ok(match caller {
            LinkCaller::Local(identity) => ControlRequest::from_local_message(
                ControlSurface::LocalIpc,
                source.clone(),
                LocalAddress::new("orion"),
                message,
                Some(identity.clone()),
            ),
            LinkCaller::Operator { principal, key } => ControlRequest::remote_link(
                source.clone(),
                self.security.enrolled_operator(principal, *key, false)?,
                message,
            ),
        })
    }

    /// Serves one control message for a Unix link caller (the executor role's requests).
    pub(crate) fn serve_link_local_message(
        &self,
        source: &LocalAddress,
        identity: &UnixPeerIdentity,
        message: ControlMessage,
    ) -> Result<ControlMessage, NodeError> {
        self.serve_link_client_message(source, &LinkCaller::Local(identity.clone()), message)
    }

    fn serve_link_request(&self, request: ControlRequest) -> Result<ControlMessage, NodeError> {
        let response = self
            .serve_control_request(request)?
            .into_local()
            .map_err(NodeError::IpcTransport)?;
        match response {
            ControlMessage::Rejected(reason) => Err(NodeError::Action(reason)),
            other => Ok(other),
        }
    }

    /// Registers the client of a link session (a control-plane client named `client_name`), and
    /// has `wake` notified whenever an event is queued for it.
    pub(crate) fn register_link_client(
        &self,
        source: &LocalAddress,
        client_name: &str,
        caller: &LinkCaller,
        wake: Arc<Notify>,
    ) -> Result<(), NodeError> {
        let hello = ControlMessage::ClientHello(ClientHello {
            client_name: orion_core::ClientName::new(client_name.to_owned()),
            role: ClientRole::ControlPlane,
        });
        match caller {
            LinkCaller::Local(_) => {
                self.serve_link_client_message(source, caller, hello)?;
            }
            // Operators are never local control-plane clients: the session is allowed as a push
            // session is, then registered directly, as `open_remote_session` does.
            LinkCaller::Operator { .. } => {
                self.serve_link_request(
                    self.link_request(
                        source,
                        caller,
                        ControlMessage::OpenSession(orion::control_plane::SessionOpen {
                            client_name: client_name.to_owned(),
                        }),
                    )?
                    .check_only(),
                )?;
                match self.apply_local_control_message(source, hello)? {
                    ControlMessage::ClientWelcome(_) => {}
                    ControlMessage::Rejected(why) => return Err(NodeError::Authorization(why)),
                    other => {
                        return Err(NodeError::Storage(format!(
                            "unexpected answer to a link session's hello: {other:?}"
                        )));
                    }
                }
            }
        }
        self.with_client_mut(source, |client| client.link_wake = Some(wake))
    }

    /// Registers the publisher of a link session's streams (a provider session that declares one)
    /// unless the session's client is registered already. The provider role was admitted when
    /// the session opened, so this does not go through the control pipeline.
    pub(crate) fn register_link_publisher(
        &self,
        source: &LocalAddress,
        client_name: &str,
        wake: Arc<Notify>,
    ) {
        let now_ms = Self::current_time_ms();
        let session = ClientSession {
            session_id: SessionId::new(format!("{}:{}", self.config.node_id, source.as_str())),
            client_name: orion_core::ClientName::new(client_name.to_owned()),
            role: ClientRole::Provider,
            node_id: self.config.node_id.clone(),
            source: source.as_str().to_owned(),
        };
        let tuning = &self.config.runtime_tuning;
        self.with_client_registry_txn(|txn| {
            if txn.client_mut_if_present(source).is_none() {
                txn.upsert_session(
                    source,
                    session,
                    now_ms,
                    tuning.local_client_event_queue_limit,
                    tuning.local_client_event_queue_bytes,
                );
            }
            if let Ok(client) = txn.client_mut(source) {
                client.link_wake = Some(wake);
            }
        });
    }

    /// Declares stream `id` of the provider of `device` on `link`, published by `source`.
    pub(crate) fn declare_link_stream(
        &self,
        source: &LocalAddress,
        link: &str,
        device: &str,
        id: String,
        series: Vec<SeriesDecl>,
    ) -> Result<StreamRef, NodeError> {
        let provider = self.link_device_provider(link, device).ok_or_else(|| {
            NodeError::Authorization(format!(
                "device `{device}` has no accepted provider snapshot on {link}; send ProviderState first"
            ))
        })?;
        let owner = StatusSubject::Provider(provider);
        self.declare_owned_stream(
            source,
            StreamDecl {
                owner: owner.clone(),
                id: id.clone(),
                series,
            },
        )?;
        Ok(StreamRef { owner, id })
    }

    /// Publishes a batch of a stream `source` declared.
    pub(crate) fn publish_link_stream(
        &self,
        source: &LocalAddress,
        stream: &StreamRef,
        chunks: &mut Vec<SampleChunk>,
    ) -> Result<(), NodeError> {
        self.publish_stream_batch(source, stream, chunks)
    }

    /// Takes up to `max` queued events of a link client, oldest first.
    pub(crate) fn take_link_client_events(
        &self,
        source: &LocalAddress,
        max: usize,
    ) -> Vec<ClientEvent> {
        let now_ms = Self::current_time_ms();
        self.with_client_mut_if_present(source, |client| {
            client.last_activity_ms = now_ms;
            let mut events = Vec::new();
            while events.len() < max {
                let Some(event) = client.queued_events.pop_front() else {
                    break;
                };
                events.push(event);
            }
            events
        })
        .unwrap_or_default()
    }

    /// Ends a link client's status or action watch, with the events it still had queued.
    pub(crate) fn end_link_client_watch(&self, source: &LocalAddress, watch: LinkWatch) {
        let _ = self.with_client_mut_if_present(source, |client| match watch {
            LinkWatch::Status => {
                client.status_watch = None;
                client
                    .queued_events
                    .retain(|event| !matches!(event.event, ClientEventKind::Status(_)));
            }
            // Results of actions the session runs keep coming (`action_calls`); the link sorts
            // them by request.
            LinkWatch::Actions => client.action_watch = None,
        });
    }

    /// Removes a link client when its session ends.
    pub(crate) fn remove_link_client(&self, source: &LocalAddress) {
        self.with_client_registry_txn(|txn| {
            txn.remove(source);
        });
        self.release_action_handler(source);
        // Its subscriptions end, and the streams it published lose their publisher.
        self.forget_stream_client(source);
    }
}

#[cfg(test)]
impl NodeApp {
    /// Whether a local client is registered at `source`.
    pub(crate) fn has_local_client(&self, source: &LocalAddress) -> bool {
        self.clients_read().contains_key(source)
    }

    /// Replaces the local admission policy at accept and in the control pipeline (the first
    /// middleware, as the builder installs it), for tests that need a caller the policy refuses.
    pub(crate) fn replace_local_admission(&mut self, policy: crate::auth::LocalAdmissionPolicy) {
        let lookup = Arc::new(super::state_access::AppLocalSessionLookup {
            state: self.state.clone(),
        });
        let mut middlewares: Vec<_> = self.control_middlewares.iter().cloned().collect();
        if let Some(first) = middlewares.first_mut() {
            *first = Arc::new(crate::auth::PeerSecurityMiddleware::with_local_admission(
                &self.security,
                policy.clone(),
                lookup,
            ));
        }
        self.control_middlewares = Arc::from(middlewares);
        self.local_admission = Arc::new(policy);
    }
}
