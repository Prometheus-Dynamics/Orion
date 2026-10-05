//! Transport abstraction of the peer sync engine.
//!
//! The engine in `peer_sync.rs` only needs to send one control request to a peer and get one
//! response back. [`PeerSyncTransport`] is that contract; HTTP(S) peers and `orion+tcp` peers
//! implement it, and tests can implement it in memory. Signing requests (and, over TCP,
//! verifying signed responses) is part of the transport.

use super::{NodeApp, NodeError};
use crate::peer::{PeerState, PeerTransportKind};
use orion::{
    NodeId,
    control_plane::{CommunicationFailureKind, ControlMessage, PeerHello},
    transport::http::{HttpRequestPayload, HttpResponsePayload},
};
use std::{future::Future, time::Duration};

/// One request/response channel to a peer, used by the transport-independent sync engine.
pub(crate) trait PeerSyncTransport: Send + Sync {
    /// Transport label for logs and errors (`http`, `tcp`, ...).
    fn label(&self) -> &'static str;

    /// Sends one request (a control message or an observed-state update, not yet signed) to the
    /// peer and returns its response.
    fn exchange(
        &self,
        app: &NodeApp,
        node_id: &NodeId,
        request: HttpRequestPayload,
    ) -> impl Future<Output = Result<HttpResponsePayload, NodeError>> + Send;

    /// Sends one control message.
    fn send_control(
        &self,
        app: &NodeApp,
        node_id: &NodeId,
        message: ControlMessage,
    ) -> impl Future<Output = Result<HttpResponsePayload, NodeError>> + Send {
        self.exchange(app, node_id, HttpRequestPayload::Control(Box::new(message)))
    }

    /// Exchanges `Hello` messages. Transports override this to add transport-level trust steps
    /// (HTTPS enrolls and checks the peer's TLS binding here).
    fn hello(
        &self,
        app: &NodeApp,
        node_id: &NodeId,
        _peer: &PeerState,
        hello: PeerHello,
    ) -> impl Future<Output = Result<PeerHello, NodeError>> + Send {
        async move {
            match self
                .send_control(app, node_id, ControlMessage::Hello(hello))
                .await?
            {
                HttpResponsePayload::Hello(hello) => Ok(hello),
                other => Err(unexpected_response(self.label(), "hello", &other)),
            }
        }
    }
}

pub(super) fn unexpected_response(
    transport: &str,
    request: &str,
    response: &HttpResponsePayload,
) -> NodeError {
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
    NodeError::Storage(format!(
        "unexpected {kind} response to peer {request} request over {transport}"
    ))
}

/// The transport selected for one peer, by the scheme of its base URL.
pub(crate) enum PeerChannel {
    #[cfg(feature = "transport-http")]
    Http(super::peer_sync_client::HttpPeerChannel),
    #[cfg(feature = "peer-tcp")]
    Tcp(super::peer_sync_tcp::TcpPeerChannel),
}

impl PeerSyncTransport for PeerChannel {
    fn label(&self) -> &'static str {
        match self {
            #[cfg(feature = "transport-http")]
            Self::Http(channel) => channel.label(),
            #[cfg(feature = "peer-tcp")]
            Self::Tcp(channel) => channel.label(),
        }
    }

    async fn exchange(
        &self,
        app: &NodeApp,
        node_id: &NodeId,
        request: HttpRequestPayload,
    ) -> Result<HttpResponsePayload, NodeError> {
        match self {
            #[cfg(feature = "transport-http")]
            Self::Http(channel) => channel.exchange(app, node_id, request).await,
            #[cfg(feature = "peer-tcp")]
            Self::Tcp(channel) => channel.exchange(app, node_id, request).await,
        }
    }

    async fn hello(
        &self,
        app: &NodeApp,
        node_id: &NodeId,
        peer: &PeerState,
        hello: PeerHello,
    ) -> Result<PeerHello, NodeError> {
        match self {
            #[cfg(feature = "transport-http")]
            Self::Http(channel) => channel.hello(app, node_id, peer, hello).await,
            #[cfg(feature = "peer-tcp")]
            Self::Tcp(channel) => channel.hello(app, node_id, peer, hello).await,
        }
    }
}

impl NodeApp {
    /// Opens the channel for `peer` according to its base URL.
    pub(super) fn open_peer_channel(
        &self,
        node_id: &NodeId,
        peer: &PeerState,
    ) -> Result<PeerChannel, NodeError> {
        let kind = PeerTransportKind::check_supported(peer.base_url.as_str())
            .map_err(NodeError::Config)?;
        match kind {
            #[cfg(feature = "transport-http")]
            PeerTransportKind::Http | PeerTransportKind::Https => Ok(PeerChannel::Http(
                super::peer_sync_client::HttpPeerChannel::open(self, node_id, peer)?,
            )),
            #[cfg(feature = "peer-tcp")]
            PeerTransportKind::Tcp => Ok(PeerChannel::Tcp(self.peer_tcp_channel(node_id, peer)?)),
            #[allow(unreachable_patterns)]
            other => Err(NodeError::Config(format!(
                "peer {node_id} needs the `{}` feature",
                other.cargo_feature()
            ))),
        }
    }

    /// Per-peer communication metrics (`<transport>/peer-sync/<node>` endpoints).
    pub(crate) fn record_peer_exchange_sent(&self, node_id: &NodeId, bytes_sent: u64) {
        self.with_observability_txn(|txn| {
            txn.state_mut()
                .peer_http_communication
                .entry(node_id.clone())
                .or_default()
                .record_sent(bytes_sent);
        });
    }

    pub(crate) fn record_peer_exchange_success(
        &self,
        node_id: &NodeId,
        bytes_received: u64,
        duration: Duration,
    ) {
        let now_ms = Self::current_time_ms();
        self.with_observability_txn(|txn| {
            let metrics = txn
                .state_mut()
                .peer_http_communication
                .entry(node_id.clone())
                .or_default();
            metrics.record_received(bytes_received);
            metrics.record_success_exchange(now_ms, duration, 0, bytes_received);
        });
    }

    pub(crate) fn record_peer_exchange_failure(
        &self,
        node_id: &NodeId,
        duration: Duration,
        kind: CommunicationFailureKind,
        error: String,
    ) {
        let now_ms = Self::current_time_ms();
        self.with_observability_txn(|txn| {
            txn.state_mut()
                .peer_http_communication
                .entry(node_id.clone())
                .or_default()
                .record_failure_kind(now_ms, Some(duration), kind, error);
        });
    }
}
