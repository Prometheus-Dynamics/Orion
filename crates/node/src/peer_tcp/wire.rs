//! Payload layout of `orion+tcp` frames.
//!
//! ```text
//! request  = [kind u8][rkyv archive]   (HttpCodec request body)
//! response = [status u8][sig_len u8][signature][key_len u8][public key][body]
//!            status 0: body is an rkyv HttpResponsePayload; status 1: body is a UTF-8 error
//! ```

use super::PeerTcpError;
use crate::auth::PeerResponseSignature;

pub(crate) const STATUS_OK: u8 = 0;
pub(crate) const STATUS_ERROR: u8 = 1;
/// Largest response header: status, signature length, signature, key length, key.
pub(crate) const RESPONSE_HEADER_MAX_BYTES: usize = 3 + 64 + 32;

/// One decoded response frame.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ResponseFrame {
    pub(crate) status: u8,
    pub(crate) signature: Option<PeerResponseSignature>,
    pub(crate) body: Vec<u8>,
}

impl ResponseFrame {
    pub(crate) fn encode(&self) -> Result<Vec<u8>, PeerTcpError> {
        let (signature, public_key) = match &self.signature {
            Some(signature) => (
                signature.signature.as_slice(),
                signature.public_key.as_slice(),
            ),
            None => (&[][..], &[][..]),
        };
        let signature_len = u8::try_from(signature.len())
            .map_err(|_| PeerTcpError::Decode("response signature is too long".into()))?;
        let key_len = u8::try_from(public_key.len())
            .map_err(|_| PeerTcpError::Decode("response public key is too long".into()))?;
        let mut bytes =
            Vec::with_capacity(3 + signature.len() + public_key.len() + self.body.len());
        bytes.push(self.status);
        bytes.push(signature_len);
        bytes.extend_from_slice(signature);
        bytes.push(key_len);
        bytes.extend_from_slice(public_key);
        bytes.extend_from_slice(&self.body);
        Ok(bytes)
    }

    pub(crate) fn decode(bytes: &[u8]) -> Result<Self, PeerTcpError> {
        let malformed = || PeerTcpError::Decode("truncated response frame".into());
        let (&status, rest) = bytes.split_first().ok_or_else(malformed)?;
        if status != STATUS_OK && status != STATUS_ERROR {
            return Err(PeerTcpError::Decode(format!(
                "unknown response status {status}"
            )));
        }
        let (&signature_len, rest) = rest.split_first().ok_or_else(malformed)?;
        let (signature, rest) = rest
            .split_at_checked(usize::from(signature_len))
            .ok_or_else(malformed)?;
        let (&key_len, rest) = rest.split_first().ok_or_else(malformed)?;
        let (public_key, body) = rest
            .split_at_checked(usize::from(key_len))
            .ok_or_else(malformed)?;
        let signature = match (signature.is_empty(), public_key.is_empty()) {
            (true, true) => None,
            (false, false) => Some(PeerResponseSignature {
                signature: signature.to_vec(),
                public_key: public_key.to_vec(),
            }),
            _ => {
                return Err(PeerTcpError::Decode(
                    "response carries a signature without a key or a key without a signature"
                        .into(),
                ));
            }
        };
        Ok(Self {
            status,
            signature,
            body: body.to_vec(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn response_frames_roundtrip_and_reject_truncation() {
        let signed = ResponseFrame {
            status: STATUS_OK,
            signature: Some(PeerResponseSignature {
                signature: vec![1; 64],
                public_key: vec![2; 32],
            }),
            body: b"body".to_vec(),
        };
        let bytes = signed.encode().expect("frame should encode");
        assert_eq!(ResponseFrame::decode(&bytes), Ok(signed));

        let unsigned = ResponseFrame {
            status: STATUS_ERROR,
            signature: None,
            body: b"boom".to_vec(),
        };
        let bytes = unsigned.encode().expect("frame should encode");
        assert_eq!(bytes[..3], [STATUS_ERROR, 0, 0]);
        assert_eq!(ResponseFrame::decode(&bytes), Ok(unsigned));

        assert!(ResponseFrame::decode(&[]).is_err());
        assert!(ResponseFrame::decode(&[STATUS_OK, 64, 1, 2]).is_err());
        assert!(ResponseFrame::decode(&[7, 0, 0]).is_err());
        assert!(ResponseFrame::decode(&[STATUS_OK, 1, 9, 0]).is_err());
    }
}
