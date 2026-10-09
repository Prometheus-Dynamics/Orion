use super::{ControlRequest, ControlSurface};
use crate::NodeApp;
use orion::{
    ArchiveEncode, encode_to_vec,
    transport::ipc::{
        ControlEnvelope, IpcTransportError, LocalAddress, UnixControlHandler, UnixPeerIdentity,
    },
};
use std::{future::Future, pin::Pin, time::Instant};

#[cfg(feature = "transport-http")]
mod http;
#[cfg(feature = "transport-http")]
pub(crate) use http::{HttpControlServiceAdapter, HttpProbeServiceAdapter};

#[derive(Clone)]
pub(crate) struct UnixControlServiceAdapter {
    app: NodeApp,
}

impl UnixControlServiceAdapter {
    pub(crate) fn new(app: NodeApp) -> Self {
        Self { app }
    }
}

impl UnixControlHandler for UnixControlServiceAdapter {
    fn handle_control(
        &self,
        envelope: ControlEnvelope,
    ) -> Result<ControlEnvelope, IpcTransportError> {
        self.handle_control_with_identity(envelope, None)
    }

    fn handle_control_with_identity(
        &self,
        envelope: ControlEnvelope,
        identity: Option<UnixPeerIdentity>,
    ) -> Result<ControlEnvelope, IpcTransportError> {
        self.handle_control_with_identity_sync(envelope, identity)
    }

    fn handle_control_with_identity_async(
        &self,
        envelope: ControlEnvelope,
        identity: Option<UnixPeerIdentity>,
    ) -> Pin<Box<dyn Future<Output = Result<ControlEnvelope, IpcTransportError>> + Send + '_>> {
        let adapter = self.clone();
        Box::pin(async move {
            let wait = NodeApp::run_action_wait(&envelope.message);
            let mut response = tokio::task::spawn_blocking(move || {
                adapter.execute_control_with_identity(envelope, identity)
            })
            .await
            .map_err(|err| IpcTransportError::WriteFailed(err.to_string()))??;
            if wait.is_some() {
                response.message = self
                    .app
                    .complete_local_action_wait(wait, response.message)
                    .await;
            }
            Ok(response)
        })
    }

    fn record_transport_error(&self, error: &IpcTransportError) {
        self.app.record_ipc_transport_error(error);
    }

    fn record_control_exchange(
        &self,
        source: &LocalAddress,
        bytes_received: u64,
        bytes_sent: u64,
        duration: std::time::Duration,
    ) {
        self.app.record_local_unary_communication(
            source,
            bytes_received,
            bytes_sent,
            duration,
            crate::app::CommunicationStageDurations {
                socket_read: Some(duration),
                socket_write: Some(duration),
                ..Default::default()
            },
        );
    }
}

impl UnixControlServiceAdapter {
    fn handle_control_with_identity_sync(
        &self,
        envelope: ControlEnvelope,
        identity: Option<UnixPeerIdentity>,
    ) -> Result<ControlEnvelope, IpcTransportError> {
        let started = Instant::now();
        let source = envelope.source.clone();
        let bytes_received = fallback_encoded_len_u64(&envelope);
        let response = self.execute_control_with_identity(envelope, identity)?;
        let bytes_sent = fallback_encoded_len_u64(&response);
        self.app.record_local_unary_communication(
            &source,
            bytes_received,
            bytes_sent,
            started.elapsed(),
            crate::app::CommunicationStageDurations {
                encode: Some(started.elapsed()),
                decode: Some(started.elapsed()),
                socket_read: Some(started.elapsed()),
                socket_write: Some(started.elapsed()),
                ..Default::default()
            },
        );
        Ok(response)
    }

    fn execute_control_with_identity(
        &self,
        envelope: ControlEnvelope,
        identity: Option<UnixPeerIdentity>,
    ) -> Result<ControlEnvelope, IpcTransportError> {
        let message = self.app.execute_local_control_request(
            ControlRequest::from_local_envelope_with_identity(
                ControlSurface::LocalIpc,
                &envelope,
                identity,
            ),
        )?;
        Ok(ControlEnvelope {
            source: envelope.destination,
            destination: envelope.source,
            message,
        })
    }
}

fn fallback_encoded_len_u64<T: ArchiveEncode>(value: &T) -> u64 {
    encode_to_vec(value)
        .map(|bytes| bytes.len().min(u64::MAX as usize) as u64)
        .unwrap_or(0)
}
