//! Control stream frames: `[preamble][payload_len u32 LE][payload]`.
//!
//! Platform-neutral (any `AsyncRead`/`AsyncWrite`): used over Unix sockets by the local IPC
//! transport and over TCP by the `orion+tcp` peer and remote operator transport, which is why it
//! also builds on non-Unix targets.

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
    writer
        .write_all(&stream_frame_header(len))
        .await
        .map_err(|err| IpcTransportError::WriteFailed(err.to_string()))?;
    writer
        .write_all(payload)
        .await
        .map_err(|err| IpcTransportError::WriteFailed(err.to_string()))?;
    writer
        .flush()
        .await
        .map_err(|err| IpcTransportError::WriteFailed(err.to_string()))?;
    Ok(payload.len() + STREAM_FRAME_HEADER_BYTES)
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
