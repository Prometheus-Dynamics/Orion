//! `orion+tcp` implementation of [`PeerSyncTransport`].

use super::{NodeApp, NodeError, peer_transport::PeerSyncTransport};
use crate::peer::{PeerState, peer_tcp_authority};
use crate::peer_tcp::{PeerTcpClient, PeerTcpError, ResponseFrame, STATUS_OK};
use orion::{
    NodeId, decode_from_slice,
    transport::http::{HttpCodec, HttpRequestPayload, HttpResponsePayload},
};
use std::{sync::Arc, time::Instant};

/// A channel to one `orion+tcp` peer. Clients (and their connection) are cached per peer.
pub(crate) struct TcpPeerChannel {
    client: Arc<PeerTcpClient>,
}

impl NodeApp {
    pub(super) fn peer_tcp_channel(
        &self,
        node_id: &NodeId,
        peer: &PeerState,
    ) -> Result<TcpPeerChannel, NodeError> {
        let authority =
            peer_tcp_authority(peer.base_url.as_str()).map_err(PeerTcpError::InvalidAddress)?;
        let mut clients = self.peer_tcp_clients_lock();
        let client = match clients.get(node_id) {
            Some(client) if client.authority() == authority => client.clone(),
            _ => {
                let client = Arc::new(PeerTcpClient::new(
                    authority,
                    self.config.runtime_tuning.transport_io_timeout,
                    self.config.runtime_tuning.transport_max_payload_bytes,
                ));
                clients.insert(node_id.clone(), client.clone());
                client
            }
        };
        Ok(TcpPeerChannel { client })
    }
}

impl TcpPeerChannel {
    pub(crate) fn label(&self) -> &'static str {
        "tcp"
    }

    async fn send(
        &self,
        app: &NodeApp,
        node_id: &NodeId,
        request: HttpRequestPayload,
        started: Instant,
    ) -> Result<HttpResponsePayload, NodeError> {
        let payload = app.security.wrap_http_payload_async(request).await?;
        let request = HttpCodec.encode_request(&payload)?.body;
        let (response, bytes) = self
            .client
            .exchange(&request)
            .await
            .map_err(PeerTcpError::from)?;
        app.record_peer_exchange_sent(node_id, bytes.sent);
        let frame = ResponseFrame::decode(&response).map_err(PeerTcpError::from)?;
        app.security.verify_peer_response(
            node_id,
            &request,
            frame.status,
            &frame.body,
            frame.signature.as_ref(),
        )?;
        if frame.status != STATUS_OK {
            return Err(
                PeerTcpError::Remote(String::from_utf8_lossy(&frame.body).into_owned()).into(),
            );
        }
        let response = decode_from_slice::<HttpResponsePayload>(&frame.body)
            .map_err(|err| PeerTcpError::Decode(err.to_string()))?;
        app.record_peer_exchange_success(node_id, bytes.received, started.elapsed());
        Ok(response)
    }
}

impl PeerSyncTransport for TcpPeerChannel {
    fn label(&self) -> &'static str {
        "tcp"
    }

    async fn exchange(
        &self,
        app: &NodeApp,
        node_id: &NodeId,
        request: HttpRequestPayload,
    ) -> Result<HttpResponsePayload, NodeError> {
        let started = Instant::now();
        let result = self.send(app, node_id, request, started).await;
        if let Err(err) = &result {
            let kind = match err {
                NodeError::PeerTcp(err) => err.communication_failure_kind(),
                NodeError::Authentication(_) | NodeError::Authorization(_) => {
                    orion::control_plane::CommunicationFailureKind::Protocol
                }
                _ => orion::control_plane::CommunicationFailureKind::Unknown,
            };
            app.record_peer_exchange_failure(node_id, started.elapsed(), kind, err.to_string());
        }
        result
    }
}
