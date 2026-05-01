use crate::QuicFrame;
use orion_core::{decode_length_prefixed, encode_length_prefixed};
use thiserror::Error;

#[derive(Debug, Error, PartialEq, Eq)]
pub enum QuicCodecError {
    #[error("frame length header is incomplete")]
    IncompleteHeader,
    #[error("frame payload is incomplete")]
    IncompletePayload,
    #[error("frame payload exceeds supported size")]
    FrameTooLarge,
    #[error("failed to encode frame: {0}")]
    Encode(String),
    #[error("failed to decode frame: {0}")]
    Decode(String),
}

#[derive(Clone, Debug, Default)]
pub struct QuicCodec;

impl QuicCodec {
    pub fn encode_frame(&self, frame: &QuicFrame) -> Result<Vec<u8>, QuicCodecError> {
        encode_length_prefixed(frame, QuicCodecError::Encode, || {
            QuicCodecError::FrameTooLarge
        })
    }

    pub fn decode_frame(&self, bytes: &[u8]) -> Result<QuicFrame, QuicCodecError> {
        let (frame, remaining) = self.decode_stream(bytes)?;
        if !remaining.is_empty() {
            return Err(QuicCodecError::Decode(
                "extra bytes remained after frame decode".to_owned(),
            ));
        }
        Ok(frame)
    }

    pub fn decode_stream<'a>(
        &self,
        bytes: &'a [u8],
    ) -> Result<(QuicFrame, &'a [u8]), QuicCodecError> {
        decode_length_prefixed(
            bytes,
            QuicCodecError::Decode,
            || QuicCodecError::IncompleteHeader,
            || QuicCodecError::IncompletePayload,
            || QuicCodecError::FrameTooLarge,
        )
    }
}
