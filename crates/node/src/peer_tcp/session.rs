//! Remote push sessions on an `orion+tcp` connection (`docs/remote-operator.md`, "Push
//! sessions").
//!
//! Once a connection's `OpenSession` was answered, it stays open: the operator's signed requests
//! (watches, pings) keep arriving on it and are answered in order, and the events of its watches
//! are pushed in between, each signed by the node as a response to the `OpenSession` request
//! with a sequence number that only moves on (`PEER_TCP_STATUS_PUSH`). One writer task owns the
//! socket's write half, so responses and pushes never interleave mid-frame.

use super::{PEER_TCP_IDLE_TIMEOUT, PeerTcpError, ResponseFrame};
use crate::{NodeApp, app::REMOTE_SESSION_IDLE_TIMEOUT};
use orion::{
    encode_to_vec,
    transport::ipc::{ControlEnvelope, IpcTransportError, LocalAddress},
};
use orion_auth::peer_tcp::PEER_TCP_STATUS_PUSH;
use orion_transport_ipc::{ControlFrameReadState, write_control_payload_frame};
use std::net::SocketAddr;
use tokio::{net::TcpStream, sync::mpsc};
use tracing::{debug, info, warn};

/// Serves the session `session` on `stream` until either side closes it or it goes idle.
/// `open_request` is the exact `OpenSession` request payload pushes are bound to.
pub(super) async fn serve_session(
    app: NodeApp,
    stream: TcpStream,
    mut read_state: ControlFrameReadState,
    remote: SocketAddr,
    session: LocalAddress,
    open_request: Vec<u8>,
) {
    info!(node = %app.config.node_id, remote = %remote, session = session.as_str(), "remote push session opened");
    let tuning = app.config.runtime_tuning.clone();
    let (mut reader, mut writer) = stream.into_split();
    let (push_tx, mut push_rx) =
        mpsc::channel::<ControlEnvelope>(tuning.local_stream_send_queue_capacity.max(1));
    let (response_tx, mut response_rx) = mpsc::channel::<Vec<u8>>(8);
    if let Err(err) = app.attach_remote_session(&session, push_tx.clone()) {
        warn!(node = %app.config.node_id, session = session.as_str(), error = %err, "failed to attach a remote session");
        app.close_remote_session(&session, &push_tx);
        return;
    }

    // The writer: responses as they are ready, pushes as the watches produce them.
    let writer_app = app.clone();
    let io_timeout = tuning.transport_io_timeout;
    let writer_task = tokio::spawn(async move {
        let mut seq: u64 = 0;
        loop {
            let frame = tokio::select! {
                biased;
                response = response_rx.recv() => match response {
                    Some(frame) => frame,
                    None => break,
                },
                pushed = push_rx.recv() => match pushed {
                    Some(envelope) => {
                        seq = seq.saturating_add(1);
                        match push_frame(&writer_app, &open_request, seq, &envelope) {
                            Ok(frame) => frame,
                            Err(err) => {
                                warn!(error = %err, "failed to encode a push event; closing the session");
                                break;
                            }
                        }
                    }
                    None => break,
                },
            };
            match tokio::time::timeout(
                io_timeout,
                write_control_payload_frame(&mut writer, &frame, usize::MAX),
            )
            .await
            {
                Ok(Ok(_)) => {}
                Ok(Err(_)) | Err(_) => break,
            }
        }
    });

    // The reader: one signed request at a time, answered in order.
    let idle = REMOTE_SESSION_IDLE_TIMEOUT.max(PEER_TCP_IDLE_TIMEOUT / 2);
    loop {
        let read = tokio::time::timeout(
            idle,
            read_state.read_payload(&mut reader, tuning.transport_max_payload_bytes),
        )
        .await;
        let request = match read {
            Err(_) => {
                debug!(session = session.as_str(), "remote push session went idle");
                break;
            }
            Ok(Ok(None)) => break,
            Ok(Ok(Some(request))) => request,
            Ok(Err(err @ IpcTransportError::ProtocolMismatch { .. })) | Ok(Err(err)) => {
                app.record_peer_tcp_server_failure(remote, &PeerTcpError::Frame(err));
                break;
            }
        };
        let response = app
            .serve_peer_tcp_request(&request, Some(session.clone()))
            .await
            .0;
        let encoded = match response.encode() {
            Ok(encoded) => encoded,
            Err(err) => {
                app.record_peer_tcp_server_failure(remote, &PeerTcpError::from(err));
                break;
            }
        };
        if response_tx.send(encoded).await.is_err() {
            break;
        }
    }
    drop(response_tx);
    app.close_remote_session(&session, &push_tx);
    drop(push_tx);
    let _ = writer_task.await;
    info!(node = %app.config.node_id, session = session.as_str(), "remote push session closed");
}

/// One pushed envelope as a signed `PEER_TCP_STATUS_PUSH` response frame.
fn push_frame(
    app: &NodeApp,
    open_request: &[u8],
    seq: u64,
    envelope: &ControlEnvelope,
) -> Result<Vec<u8>, PeerTcpError> {
    let message =
        encode_to_vec(&envelope.message).map_err(|err| PeerTcpError::Decode(err.to_string()))?;
    let mut body = Vec::with_capacity(8 + message.len());
    body.extend_from_slice(&seq.to_le_bytes());
    body.extend_from_slice(&message);
    let signature = app
        .security
        .sign_peer_response(open_request, PEER_TCP_STATUS_PUSH, &body)
        .map_err(|err| PeerTcpError::Remote(err.to_string()))?;
    ResponseFrame {
        status: PEER_TCP_STATUS_PUSH,
        signature,
        body,
    }
    .encode()
    .map_err(PeerTcpError::from)
}
