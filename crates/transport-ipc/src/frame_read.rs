//! Cancel-safe reading of length-prefixed control frames.
//!
//! `read_u32_le` followed by `read_exact` loses whatever bytes it already consumed when the future
//! is dropped, which desynchronises the stream if the read is raced in `tokio::select!` or wrapped
//! in a timeout. [`ControlFrameReadState`] keeps the partially read header and payload between
//! calls, and only ever awaits `AsyncReadExt::read` (itself cancel-safe), so a cancelled read
//! resumes exactly where it stopped on the next call. It never reads past the current frame, so no
//! bytes are held back from the underlying stream between frames.

use orion_core::decode_from_slice_with;
use tokio::io::{AsyncRead, AsyncReadExt};

use crate::{ControlEnvelope, IpcTransportError};

const HEADER_BYTES: usize = std::mem::size_of::<u32>();

/// Partial-frame state for reading control frames from one stream. Keep one per stream and reuse
/// it for every read on that stream.
#[derive(Debug, Default)]
pub struct ControlFrameReadState {
    header: [u8; HEADER_BYTES],
    header_filled: usize,
    payload: Option<Vec<u8>>,
    payload_filled: usize,
}

impl ControlFrameReadState {
    pub fn new() -> Self {
        Self::default()
    }

    /// Returns `true` when a frame has been partially read and the next call will resume it.
    pub fn has_partial_frame(&self) -> bool {
        self.header_filled > 0
    }

    /// Reads the next control frame, returning it with the number of wire bytes it occupied.
    ///
    /// Returns `Ok(None)` on end of stream before a complete length prefix. Cancel-safe: dropping
    /// the returned future loses no data as long as the same state is used for the next read.
    pub async fn read_metered<R>(
        &mut self,
        reader: &mut R,
        max_payload_bytes: usize,
    ) -> Result<Option<(ControlEnvelope, usize)>, IpcTransportError>
    where
        R: AsyncRead + Unpin,
    {
        while self.header_filled < HEADER_BYTES {
            let read = reader
                .read(&mut self.header[self.header_filled..])
                .await
                .map_err(|err| IpcTransportError::ReadFailed(err.to_string()))?;
            if read == 0 {
                self.reset();
                return Ok(None);
            }
            self.header_filled += read;
        }

        if self.payload.is_none() {
            let len = u32::from_le_bytes(self.header) as usize;
            if len > max_payload_bytes.max(1) {
                self.reset();
                return Err(IpcTransportError::DecodeFailed(
                    "control frame exceeds maximum transport payload size".into(),
                ));
            }
            self.payload = Some(vec![0_u8; len]);
            self.payload_filled = 0;
        }

        let payload = self
            .payload
            .as_mut()
            .expect("payload buffer allocated above");
        while self.payload_filled < payload.len() {
            let read = reader
                .read(&mut payload[self.payload_filled..])
                .await
                .map_err(|err| IpcTransportError::ReadFailed(err.to_string()))?;
            if read == 0 {
                self.reset();
                return Err(IpcTransportError::ReadFailed("early eof".into()));
            }
            self.payload_filled += read;
        }

        let payload = self.payload.take().expect("payload buffer allocated above");
        self.reset();
        let bytes_received = payload.len() + HEADER_BYTES;
        decode_from_slice_with(&payload, IpcTransportError::DecodeFailed)
            .map(|envelope| Some((envelope, bytes_received)))
    }

    fn reset(&mut self) {
        self.header_filled = 0;
        self.payload = None;
        self.payload_filled = 0;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{LocalAddress, write_control_frame};
    use orion_control_plane::ControlMessage;
    use std::time::Duration;
    use tokio::io::AsyncWriteExt;

    fn envelope(message: ControlMessage) -> ControlEnvelope {
        ControlEnvelope {
            source: LocalAddress::new("a"),
            destination: LocalAddress::new("b"),
            message,
        }
    }

    async fn encoded(envelope: &ControlEnvelope) -> Vec<u8> {
        let mut bytes = Vec::new();
        write_control_frame(&mut bytes, envelope)
            .await
            .expect("frame should encode");
        bytes
    }

    #[tokio::test]
    async fn cancelled_reads_resume_mid_header_and_mid_payload() {
        let first = envelope(ControlMessage::Rejected("first frame".repeat(8)));
        let second = envelope(ControlMessage::Ping);
        let mut wire = encoded(&first).await;
        wire.extend(encoded(&second).await);

        let (mut client, mut server) = tokio::io::duplex(1024);
        let mut state = ControlFrameReadState::new();
        let mut frames = Vec::new();
        let mut cancelled_mid_frame = 0;
        // Deliver the stream three bytes at a time (splitting headers and payloads) and cancel the
        // read via a short timeout whenever it cannot complete yet.
        for chunk in wire.chunks(3) {
            client.write_all(chunk).await.expect("chunk should write");
            match tokio::time::timeout(
                Duration::from_millis(1),
                state.read_metered(&mut server, 4096),
            )
            .await
            {
                Ok(frame) => frames.push(frame.expect("frame should decode").expect("frame").0),
                Err(_) => cancelled_mid_frame += usize::from(state.has_partial_frame()),
            }
        }
        drop(client);
        assert!(
            state
                .read_metered(&mut server, 4096)
                .await
                .expect("eof should be clean")
                .is_none()
        );
        assert!(cancelled_mid_frame > 2, "test must cancel reads mid-frame");
        assert_eq!(frames, vec![first, second]);
    }

    #[tokio::test]
    async fn eof_mid_payload_and_oversized_frames_are_errors() {
        let (mut client, mut server) = tokio::io::duplex(64);
        client
            .write_all(&8_u32.to_le_bytes())
            .await
            .expect("header should write");
        client
            .write_all(&[1, 2])
            .await
            .expect("payload should write");
        drop(client);
        let mut state = ControlFrameReadState::new();
        assert!(matches!(
            state.read_metered(&mut server, 64).await,
            Err(IpcTransportError::ReadFailed(_))
        ));

        let (mut client, mut server) = tokio::io::duplex(64);
        client
            .write_all(&65_u32.to_le_bytes())
            .await
            .expect("header should write");
        assert!(matches!(
            state.read_metered(&mut server, 64).await,
            Err(IpcTransportError::DecodeFailed(_))
        ));
    }
}
