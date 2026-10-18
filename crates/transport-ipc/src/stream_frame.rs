//! Control stream frames: `[preamble][payload_len u32 LE][payload]`.
//!
//! Platform-neutral (any `AsyncRead`/`AsyncWrite`): used over Unix sockets by the local IPC
//! transport and over TCP by the `orion+tcp` peer and remote operator transport, which is why it
//! also builds on non-Unix targets.

use std::io::{self, IoSlice};

use orion_core::encode_to_vec;
use orion_transport_common::DEFAULT_MAX_TRANSPORT_PAYLOAD_BYTES;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

use crate::{
    ControlEnvelope, ControlFrameReadState, IpcTransportError,
    frame_read::STREAM_FRAME_HEADER_BYTES,
    preamble::{CONTROL_PREAMBLE_BYTES, control_preamble},
};

pub async fn write_control_frame<W>(
    writer: &mut W,
    envelope: &ControlEnvelope,
) -> Result<(), IpcTransportError>
where
    W: AsyncWriteExt + Unpin,
{
    write_control_frame_with_limit(writer, envelope, DEFAULT_MAX_TRANSPORT_PAYLOAD_BYTES).await
}

pub async fn write_control_frame_with_limit<W>(
    writer: &mut W,
    envelope: &ControlEnvelope,
    max_payload_bytes: usize,
) -> Result<(), IpcTransportError>
where
    W: AsyncWriteExt + Unpin,
{
    write_control_frame_with_limit_metered(writer, envelope, max_payload_bytes)
        .await
        .map(|_| ())
}

pub async fn write_control_frame_with_limit_metered<W>(
    writer: &mut W,
    envelope: &ControlEnvelope,
    max_payload_bytes: usize,
) -> Result<usize, IpcTransportError>
where
    W: AsyncWriteExt + Unpin,
{
    let bytes =
        encode_to_vec(envelope).map_err(|err| IpcTransportError::EncodeFailed(err.to_string()))?;
    write_control_payload_frame(writer, &bytes, max_payload_bytes).await
}

/// Writes one control frame whose payload is already encoded:
/// `[preamble][payload_len u32 LE][payload]`. Returns the wire bytes written.
///
/// Used by protocols that carry their own payload format inside the control frame (the
/// `orion+tcp` peer transport); read the frames back with
/// [`ControlFrameReadState::read_payload`].
pub async fn write_control_payload_frame<W>(
    writer: &mut W,
    payload: &[u8],
    max_payload_bytes: usize,
) -> Result<usize, IpcTransportError>
where
    W: AsyncWriteExt + Unpin,
{
    if payload.len() > max_payload_bytes.max(1) {
        return Err(IpcTransportError::EncodeFailed(
            "control frame exceeds maximum transport payload size".into(),
        ));
    }
    let len = u32::try_from(payload.len())
        .map_err(|_| IpcTransportError::EncodeFailed("control frame too large".into()))?;
    write_header_and_payload(writer, &stream_frame_header(len), payload)
        .await
        .map_err(|err| IpcTransportError::WriteFailed(err.to_string()))?;
    writer
        .flush()
        .await
        .map_err(|err| IpcTransportError::WriteFailed(err.to_string()))?;
    Ok(payload.len() + STREAM_FRAME_HEADER_BYTES)
}

/// Writes `header` then `payload` with vectored writes, so the frame normally leaves in one
/// syscall (no separate small write for the header) and the payload is never copied. Partial
/// writes are resumed until both slices are fully written.
async fn write_header_and_payload<W>(
    writer: &mut W,
    header: &[u8],
    payload: &[u8],
) -> io::Result<()>
where
    W: AsyncWriteExt + Unpin,
{
    let mut header = header;
    let mut payload = payload;
    while !header.is_empty() || !payload.is_empty() {
        let written = if header.is_empty() {
            writer.write(payload).await?
        } else {
            writer
                .write_vectored(&[IoSlice::new(header), IoSlice::new(payload)])
                .await?
        };
        if written == 0 {
            return Err(io::Error::new(
                io::ErrorKind::WriteZero,
                "control frame write returned zero bytes",
            ));
        }
        if written < header.len() {
            header = &header[written..];
        } else {
            payload = &payload[written - header.len()..];
            header = &[];
        }
    }
    Ok(())
}

/// Writes a payload-free stream frame carrying only this build's protocol preamble.
///
/// Servers send it after a read failed with [`IpcTransportError::ProtocolMismatch`], so the
/// remote side reports the same typed mismatch (with the server's version) instead of seeing the
/// connection drop.
pub async fn write_control_protocol_mismatch_frame<W>(
    writer: &mut W,
) -> Result<(), IpcTransportError>
where
    W: AsyncWriteExt + Unpin,
{
    writer
        .write_all(&stream_frame_header(0))
        .await
        .map_err(|err| IpcTransportError::WriteFailed(err.to_string()))?;
    writer
        .flush()
        .await
        .map_err(|err| IpcTransportError::WriteFailed(err.to_string()))
}

fn stream_frame_header(len: u32) -> [u8; STREAM_FRAME_HEADER_BYTES] {
    let mut header = [0_u8; STREAM_FRAME_HEADER_BYTES];
    header[..CONTROL_PREAMBLE_BYTES].copy_from_slice(&control_preamble());
    header[CONTROL_PREAMBLE_BYTES..].copy_from_slice(&len.to_le_bytes());
    header
}

pub async fn read_control_frame<R>(
    reader: &mut R,
) -> Result<Option<ControlEnvelope>, IpcTransportError>
where
    R: AsyncReadExt + Unpin,
{
    read_control_frame_with_limit(reader, DEFAULT_MAX_TRANSPORT_PAYLOAD_BYTES).await
}

pub async fn read_control_frame_with_limit<R>(
    reader: &mut R,
    max_payload_bytes: usize,
) -> Result<Option<ControlEnvelope>, IpcTransportError>
where
    R: AsyncReadExt + Unpin,
{
    read_control_frame_with_limit_metered(reader, max_payload_bytes)
        .await
        .map(|frame| frame.map(|(envelope, _)| envelope))
}

/// Reads one control frame.
///
/// Not cancel-safe: dropping the future mid-frame discards the bytes already consumed. Use a
/// persistent [`ControlFrameReadState`] when the read may be raced in `tokio::select!` or wrapped in
/// a timeout that does not also abandon the stream.
pub async fn read_control_frame_with_limit_metered<R>(
    reader: &mut R,
    max_payload_bytes: usize,
) -> Result<Option<(ControlEnvelope, usize)>, IpcTransportError>
where
    R: AsyncReadExt + Unpin,
{
    ControlFrameReadState::new()
        .read_metered(reader, max_payload_bytes)
        .await
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{
        pin::Pin,
        task::{Context, Poll},
    };
    use tokio::io::AsyncWrite;

    /// Accepts at most `chunk` bytes per call, so every write comes back partial.
    struct Trickle {
        out: Vec<u8>,
        chunk: usize,
    }

    impl AsyncWrite for Trickle {
        fn poll_write(
            mut self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
            buf: &[u8],
        ) -> Poll<io::Result<usize>> {
            let written = buf.len().min(self.chunk);
            self.out.extend_from_slice(&buf[..written]);
            Poll::Ready(Ok(written))
        }

        fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
            Poll::Ready(Ok(()))
        }

        fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
            Poll::Ready(Ok(()))
        }
    }

    #[tokio::test]
    async fn partial_vectored_writes_resume_until_header_and_payload_are_out() {
        let payload: Vec<u8> = (0..=40).collect();
        for chunk in [1, 3, 7, 64] {
            let mut writer = Trickle {
                out: Vec::new(),
                chunk,
            };
            let header = stream_frame_header(payload.len() as u32);
            write_header_and_payload(&mut writer, &header, &payload)
                .await
                .expect("partial writes should resume");
            let mut expected = header.to_vec();
            expected.extend_from_slice(&payload);
            assert_eq!(writer.out, expected, "chunk size {chunk}");
        }
    }
}
