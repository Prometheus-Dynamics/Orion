//! Async adapters over [`embedded_io_async`] byte streams (feature `embedded-io-async`).

use embedded_io_async::{BufRead, Read, Write};

use crate::frame::FrameView;
use crate::read_error::ReadFrameError;
use crate::stream::{Status, StreamDecoder, StreamEncoder};

/// Stack chunk used to batch encoder output into `write_all` calls.
const CHUNK: usize = 32;

/// Writes every remaining byte of `encoder` (one COBS-framed message) to `writer`.
///
/// # Errors
///
/// The writer's error.
pub async fn write_stream<W: Write>(
    writer: &mut W,
    mut encoder: StreamEncoder<'_>,
) -> Result<(), W::Error> {
    let mut chunk = [0u8; CHUNK];
    loop {
        let n = encoder.fill(&mut chunk);
        if n == 0 {
            return Ok(());
        }
        writer.write_all(chunk.get(..n).unwrap_or_default()).await?;
    }
}

/// Reads one byte at a time until `decoder` completes a valid frame, skipping (and counting)
/// corrupt packets. With a buffered reader prefer [`read_frame_buffered`].
///
/// # Errors
///
/// [`ReadFrameError::Io`] or [`ReadFrameError::Eof`].
pub async fn read_frame<'d, R: Read, const N: usize>(
    reader: &mut R,
    decoder: &'d mut StreamDecoder<N>,
) -> Result<FrameView<'d>, ReadFrameError<R::Error>> {
    let mut byte = [0u8; 1];
    loop {
        if reader.read(&mut byte).await.map_err(ReadFrameError::Io)? == 0 {
            return Err(ReadFrameError::Eof);
        }
        if decoder.push_status(byte[0]) == Status::Frame {
            break;
        }
    }
    decoder.frame().ok_or(ReadFrameError::Eof)
}

/// Like [`read_frame`] but consumes whole buffered chunks, leaving bytes after the frame in the
/// reader.
///
/// # Errors
///
/// [`ReadFrameError::Io`] or [`ReadFrameError::Eof`].
pub async fn read_frame_buffered<'d, R: BufRead, const N: usize>(
    reader: &mut R,
    decoder: &'d mut StreamDecoder<N>,
) -> Result<FrameView<'d>, ReadFrameError<R::Error>> {
    loop {
        let buf = reader.fill_buf().await.map_err(ReadFrameError::Io)?;
        if buf.is_empty() {
            return Err(ReadFrameError::Eof);
        }
        let (used, status) = decoder.push_slice_status(buf);
        reader.consume(used);
        if status == Status::Frame {
            break;
        }
    }
    decoder.frame().ok_or(ReadFrameError::Eof)
}
