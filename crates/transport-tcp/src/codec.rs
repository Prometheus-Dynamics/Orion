use crate::TcpFrame;
use orion_core::{decode_length_prefixed, encode_length_prefixed};
use thiserror::Error;

#[derive(Debug, Error, PartialEq, Eq)]
pub enum TcpCodecError {
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
pub struct TcpCodec;

impl TcpCodec {
    pub fn encode_frame(&self, frame: &TcpFrame) -> Result<Vec<u8>, TcpCodecError> {
        encode_length_prefixed(frame, TcpCodecError::Encode, || {
            TcpCodecError::FrameTooLarge
        })
    }

    pub fn decode_frame(&self, bytes: &[u8]) -> Result<TcpFrame, TcpCodecError> {
        let (frame, remaining) = self.decode_stream(bytes)?;
        if !remaining.is_empty() {
            return Err(TcpCodecError::Decode(
                "extra bytes remained after frame decode".to_owned(),
            ));
        }
        Ok(frame)
    }

    pub fn decode_stream<'a>(
        &self,
        bytes: &'a [u8],
    ) -> Result<(TcpFrame, &'a [u8]), TcpCodecError> {
        decode_length_prefixed(
            bytes,
            TcpCodecError::Decode,
            || TcpCodecError::IncompleteHeader,
            || TcpCodecError::IncompletePayload,
            || TcpCodecError::FrameTooLarge,
        )
    }
}
