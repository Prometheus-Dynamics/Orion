//! Server side of the `orion+tcp` transport (`ORION_NODE_PEER_ADDR`).

use super::{
    PEER_TCP_IDLE_TIMEOUT, PeerTcpError,
    RESPONSE_HEADER_MAX_BYTES, ResponseFrame, STATUS_ERROR,
};
use crate::{
    ControlRequest, NodeApp, NodeError,
    app::{CommunicationEndpointRuntime, CommunicationStageDurations, GracefulTaskHandle},
    peer_tcp::STATUS_OK,
};
use orion::{
    encode_to_vec,
    transport::{
        http::{HttpCodec, HttpResponsePayload},
        ipc::IpcTransportError,
    },
};
use orion_transport_ipc::{
    ControlFrameReadState, write_control_payload_frame, write_control_protocol_mismatch_frame,
};
use std::{net::SocketAddr, sync::Arc, time::Instant};
use tokio::{
    net::{TcpListener, TcpStream},
    sync::{Semaphore, oneshot},
    task::JoinSet,
};
use tracing::{debug, warn};

impl NodeApp {
    /// Starts the `orion+tcp` peer listener. Returns the bound address and a handle that stops
    /// the listener and closes its connections.
    pub async fn start_peer_tcp_server(
        &self,
        addr: SocketAddr,
    ) -> Result<(SocketAddr, GracefulTaskHandle<NodeError>), NodeError> {
        let listener = TcpListener::bind(addr)
            .await
            .map_err(|err| PeerTcpError::Bind {
                addr: addr.to_string(),
                message: err.to_string(),
            })?;
        let local_addr = listener.local_addr().map_err(|err| PeerTcpError::Bind {
            addr: addr.to_string(),
            message: err.to_string(),
        })?;
        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        let app = self.clone();
        let handle = tokio::spawn(async move {
            serve(app, listener, shutdown_rx).await;
            Ok(())
        });
        Ok((local_addr, GracefulTaskHandle::new(shutdown_tx, handle)))
    }

    /// Executes one decoded request frame and builds the response frame.
    async fn serve_peer_tcp_request(&self, request: &[u8]) -> ResponseFrame {
        let (status, body) = match self.execute_peer_tcp_request(request).await {
            Ok(response) => match encode_to_vec(&response) {
                Ok(body) => (STATUS_OK, body),
                Err(err) => (STATUS_ERROR, err.to_string().into_bytes()),
            },
            Err(err) => (STATUS_ERROR, err.to_string().into_bytes()),
        };
        // The peer reads frames up to the same limit; leave room for the signature header.
        let max_body = self
            .config
            .runtime_tuning
            .transport_max_payload_bytes
            .saturating_sub(RESPONSE_HEADER_MAX_BYTES);
        let (status, body) = if body.len() > max_body {
            (
                STATUS_ERROR,
                b"response exceeds the maximum transport payload size".to_vec(),
            )
        } else {
            (status, body)
        };
        let signature = match self.security.sign_peer_response(request, status, &body) {
            Ok(signature) => signature,
            Err(err) => {
                warn!(node = %self.config.node_id, error = %err, "failed to sign peer TCP response");
                None
            }
        };
        ResponseFrame {
            status,
            signature,
            body,
        }
    }

    async fn execute_peer_tcp_request(
        &self,
        request: &[u8],
    ) -> Result<HttpResponsePayload, NodeError> {
        let payload = HttpCodec.decode_request_body(request)?;
        let app = self.clone();
        let response = tokio::task::spawn_blocking(move || {
            app.serve_control_request(ControlRequest::from_peer_tcp_payload(payload))
        })
        .await
        .map_err(|err| NodeError::Storage(format!("peer TCP request task failed: {err}")))??;
        match response {
            crate::ControlResponse::Http(response) => Ok(*response),
            crate::ControlResponse::Local(_) => Err(NodeError::Storage(
                "local control response returned on the peer TCP surface".into(),
            )),
        }
    }

    fn record_peer_tcp_server_exchange(
        &self,
        remote: SocketAddr,
        bytes_received: u64,
        bytes_sent: u64,
        duration: std::time::Duration,
    ) {
        self.record_communication_endpoint_exchange_with_stages(
            peer_tcp_server_endpoint(self, remote),
            bytes_received,
            bytes_sent,
            duration,
            CommunicationStageDurations {
                socket_write: Some(duration),
                ..Default::default()
            },
        );
    }

    fn record_peer_tcp_server_failure(&self, remote: SocketAddr, error: &PeerTcpError) {
        self.record_communication_endpoint_failure_kind(
            peer_tcp_server_endpoint(self, remote),
            None,
            error.communication_failure_kind(),
            error.to_string(),
        );
    }
}

fn peer_tcp_server_endpoint(app: &NodeApp, remote: SocketAddr) -> CommunicationEndpointRuntime {
    let node_id = app.config.node_id.as_str();
    let mut endpoint = CommunicationEndpointRuntime::new("tcp/peer-control", "tcp", "control");
    endpoint.local = Some(node_id.to_owned());
    endpoint.remote = Some(remote.ip().to_string());
    endpoint
        .labels
        .insert("node_id".to_owned(), node_id.to_owned());
    endpoint
}

async fn serve(app: NodeApp, listener: TcpListener, mut shutdown: oneshot::Receiver<()>) {
    let limit = app
        .config
        .runtime_tuning
        .transport_max_concurrent_connections
        .max(1);
    let permits = Arc::new(Semaphore::new(limit));
    let mut connections = JoinSet::new();
    loop {
        tokio::select! {
            _ = &mut shutdown => break,
            Some(_) = connections.join_next(), if !connections.is_empty() => {}
            accepted = listener.accept() => {
                let (stream, remote) = match accepted {
                    Ok(accepted) => accepted,
                    Err(err) => {
                        warn!(node = %app.config.node_id, error = %err, "peer TCP accept failed");
                        continue;
                    }
                };
                let Ok(permit) = permits.clone().try_acquire_owned() else {
                    warn!(
                        node = %app.config.node_id,
                        remote = %remote,
                        limit,
                        "peer TCP connection limit reached; closing new connection"
                    );
                    continue;
                };
                let app = app.clone();
                connections.spawn(async move {
                    let _permit = permit;
                    serve_connection(app, stream, remote).await;
                });
            }
        }
    }
    connections.abort_all();
    while connections.join_next().await.is_some() {}
}

async fn serve_connection(app: NodeApp, mut stream: TcpStream, remote: SocketAddr) {
    let _ = stream.set_nodelay(true);
    let tuning = app.config.runtime_tuning.clone();
    let mut read_state = ControlFrameReadState::new();
    loop {
        let read = tokio::time::timeout(
            PEER_TCP_IDLE_TIMEOUT,
            read_state.read_payload(&mut stream, tuning.transport_max_payload_bytes),
        )
        .await;
        let request = match read {
            Err(_) | Ok(Ok(None)) => return,
            Ok(Ok(Some(request))) => request,
            Ok(Err(err @ IpcTransportError::ProtocolMismatch { .. })) => {
                let _ = tokio::time::timeout(
                    tuning.transport_io_timeout,
                    write_control_protocol_mismatch_frame(&mut stream),
                )
                .await;
                warn!(
                    node = %app.config.node_id,
                    remote = %remote,
                    error = %err,
                    "rejected peer TCP connection speaking a different control protocol version; \
                     upgrade orion-node peers together"
                );
                app.record_peer_tcp_server_failure(remote, &PeerTcpError::Frame(err));
                return;
            }
            Ok(Err(err)) => {
                debug!(node = %app.config.node_id, remote = %remote, error = %err, "peer TCP read failed");
                app.record_peer_tcp_server_failure(remote, &PeerTcpError::Frame(err));
                return;
            }
        };
        let started = Instant::now();
        let response = app.serve_peer_tcp_request(&request).await;
        let encoded = match response.encode() {
            Ok(encoded) => encoded,
            Err(err) => {
                app.record_peer_tcp_server_failure(remote, &PeerTcpError::from(err));
                return;
            }
        };
        let written = tokio::time::timeout(
            tuning.transport_io_timeout,
            write_control_payload_frame(&mut stream, &encoded, usize::MAX),
        )
        .await;
        match written {
            Ok(Ok(sent)) => app.record_peer_tcp_server_exchange(
                remote,
                request.len() as u64,
                sent as u64,
                started.elapsed(),
            ),
            Ok(Err(err)) => {
                app.record_peer_tcp_server_failure(remote, &PeerTcpError::Frame(err));
                return;
            }
            Err(_) => {
                app.record_peer_tcp_server_failure(
                    remote,
                    &PeerTcpError::Timeout {
                        addr: remote.to_string(),
                        operation: "write",
                        timeout_ms: tuning
                            .transport_io_timeout
                            .as_millis()
                            .min(u128::from(u64::MAX)) as u64,
                    },
                );
                return;
            }
        }
    }
}
