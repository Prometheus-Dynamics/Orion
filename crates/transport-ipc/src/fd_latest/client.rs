use std::{path::Path, time::Duration};

use orion_transport_common::DEFAULT_TRANSPORT_IO_TIMEOUT;
use tokio::net::UnixStream;

use super::{
    UnixFdLatestFrame, UnixFdLatestReply,
    protocol::{
        LatestRequest, REQUEST_BYTES, RESPONSE_HEADER_BYTES, ResponseHeader, ResponseStatus,
    },
};
use crate::{
    DEFAULT_UNIX_FD_FRAME_MAX_FDS, DEFAULT_UNIX_FD_FRAME_MAX_PAYLOAD_BYTES, IpcTransportError,
    UnixFdFrame, recv_unix_fd_frame_async, send_unix_fd_frame_async, unix::timed_connect,
};

/// Persistent async client for a [`UnixFdLatestServer`](super::UnixFdLatestServer).
///
/// Requests are issued sequentially over one connection. If the server is at its client limit the
/// first request fails with [`IpcTransportError::ConnectionRefused`]. After any failed, timed-out,
/// or cancelled request the client refuses further requests and must be reconnected.
#[derive(Debug)]
pub struct UnixFdLatestClient {
    stream: UnixStream,
    max_payload_bytes: usize,
    max_fds: usize,
    io_timeout: Duration,
    /// Set while an exchange is in flight; stays set if it failed, timed out, or was cancelled,
    /// since a late reply would otherwise be read as the answer to the next request.
    desynced: bool,
}

impl UnixFdLatestClient {
    pub async fn connect(socket_path: impl AsRef<Path>) -> Result<Self, IpcTransportError> {
        let stream = timed_connect(
            DEFAULT_TRANSPORT_IO_TIMEOUT,
            UnixStream::connect(socket_path.as_ref()),
            "fd latest connect",
        )
        .await?;
        Ok(Self {
            stream,
            max_payload_bytes: DEFAULT_UNIX_FD_FRAME_MAX_PAYLOAD_BYTES,
            max_fds: DEFAULT_UNIX_FD_FRAME_MAX_FDS,
            io_timeout: DEFAULT_TRANSPORT_IO_TIMEOUT,
            desynced: false,
        })
    }

    /// Maximum opaque payload accepted from the server (excluding the protocol header).
    pub fn with_max_payload_bytes(mut self, max_payload_bytes: usize) -> Self {
        self.max_payload_bytes = max_payload_bytes.max(1);
        self
    }

    pub fn with_max_fds(mut self, max_fds: usize) -> Self {
        self.max_fds = max_fds;
        self
    }

    /// Timeout for a single request/reply exchange, added on top of any `next_after` wait.
    pub fn with_io_timeout(mut self, io_timeout: Duration) -> Self {
        self.io_timeout = io_timeout.max(Duration::from_millis(1));
        self
    }

    /// Returns the currently published frame without waiting.
    pub async fn latest(&mut self) -> Result<UnixFdLatestReply, IpcTransportError> {
        self.request(LatestRequest::Latest, Duration::ZERO).await
    }

    /// Waits up to `timeout` for a frame with a sequence greater than `sequence`.
    ///
    /// Returns immediately if such a frame is already published. The server may clamp `timeout` to
    /// its configured maximum wait. Pass `0` to wait for the first frame ever published.
    pub async fn next_after(
        &mut self,
        sequence: u64,
        timeout: Duration,
    ) -> Result<UnixFdLatestReply, IpcTransportError> {
        self.request(
            LatestRequest::NextAfter {
                sequence,
                wait: timeout,
            },
            timeout,
        )
        .await
    }

    async fn request(
        &mut self,
        request: LatestRequest,
        wait: Duration,
    ) -> Result<UnixFdLatestReply, IpcTransportError> {
        if self.desynced {
            return Err(IpcTransportError::ConnectFailed(
                "fd latest connection is unusable after an interrupted request; reconnect".into(),
            ));
        }
        self.desynced = true;
        let deadline = self.io_timeout.saturating_add(wait);
        let reply = match tokio::time::timeout(deadline, self.exchange(request)).await {
            Ok(reply) => reply?,
            Err(_) => {
                return Err(IpcTransportError::ReadFailed(format!(
                    "fd latest request timed out after {} ms",
                    deadline.as_millis()
                )));
            }
        };
        self.desynced = false;
        Ok(reply)
    }

    async fn exchange(
        &mut self,
        request: LatestRequest,
    ) -> Result<UnixFdLatestReply, IpcTransportError> {
        let request = UnixFdFrame::new(request.encode(), Vec::new());
        if let Err(send_err) =
            send_unix_fd_frame_async(&self.stream, &request, REQUEST_BYTES, 0).await
        {
            // A server at its client limit sends a busy notice and closes before reading, which
            // can surface here as a broken pipe; prefer the queued notice when present.
            return match self.recv_reply().await {
                Ok(Some(reply)) => decode_reply(reply),
                _ => Err(send_err),
            };
        }
        match self.recv_reply().await? {
            Some(reply) => decode_reply(reply),
            None => Err(IpcTransportError::ReadFailed(
                "fd latest server closed the connection".into(),
            )),
        }
    }

    async fn recv_reply(&self) -> Result<Option<UnixFdFrame>, IpcTransportError> {
        recv_unix_fd_frame_async(
            &self.stream,
            self.max_payload_bytes.saturating_add(RESPONSE_HEADER_BYTES),
            self.max_fds,
        )
        .await
    }
}

fn decode_reply(reply: UnixFdFrame) -> Result<UnixFdLatestReply, IpcTransportError> {
    let (header, payload) = ResponseHeader::decode_from(reply.payload)?;
    if header.status != ResponseStatus::Frame && (!payload.is_empty() || !reply.fds.is_empty()) {
        return Err(IpcTransportError::DecodeFailed(
            "fd latest status reply carried unexpected payload or descriptors".into(),
        ));
    }
    let published_at = super::protocol::unix_nanos_to_system_time(header.published_at_unix_nanos);
    Ok(match header.status {
        ResponseStatus::Frame => UnixFdLatestReply::Frame(UnixFdLatestFrame {
            sequence: header.sequence,
            published_at,
            frame: UnixFdFrame::new(payload, reply.fds),
        }),
        ResponseStatus::Empty => UnixFdLatestReply::Empty,
        ResponseStatus::Stale => UnixFdLatestReply::Stale {
            sequence: header.sequence,
            published_at,
        },
        ResponseStatus::Timeout => UnixFdLatestReply::Timeout {
            latest_sequence: (header.sequence != 0).then_some(header.sequence),
        },
        ResponseStatus::Busy => {
            return Err(IpcTransportError::ConnectionRefused(
                "fd latest server is at its client limit".into(),
            ));
        }
    })
}
