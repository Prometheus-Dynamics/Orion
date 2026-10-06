//! The operator's ed25519 identity.

use super::RemoteError;
use ed25519_dalek::SigningKey;
use getrandom::{SysRng, rand_core::UnwrapErr};
use orion_auth::{crypto::key_fingerprint, hex::encode_hex};
use orion_control_plane::OperatorId;
use orion_core::NodeId;
use std::fmt;

/// An operator's identity: its id (`operator:<name>`) and ed25519 signing key.
///
/// The caller owns storage: persist [`Self::secret_key_bytes`] (for example in the OS keyring
/// or a file readable only by the user) and restore it with [`Self::from_secret_key_bytes`]. A
/// node pins the public key at enrollment, so a lost key means enrolling again.
#[derive(Clone)]
pub struct OperatorIdentity {
    operator_id: OperatorId,
    signing_key: SigningKey,
}

impl OperatorIdentity {
    /// A fresh random key for `name` (`alice` or `operator:alice`).
    pub fn generate(name: &str) -> Result<Self, RemoteError> {
        Ok(Self {
            operator_id: parse_id(name)?,
            signing_key: SigningKey::generate(&mut UnwrapErr(SysRng)),
        })
    }

    /// Restores an identity from its 32-byte secret key.
    pub fn from_secret_key_bytes(name: &str, secret: &[u8]) -> Result<Self, RemoteError> {
        let secret: [u8; 32] = secret.try_into().map_err(|_| {
            RemoteError::InvalidIdentity(format!(
                "an operator secret key is 32 bytes, got {}",
                secret.len()
            ))
        })?;
        Ok(Self {
            operator_id: parse_id(name)?,
            signing_key: SigningKey::from_bytes(&secret),
        })
    }

    /// The secret key, for the caller to store. Treat it like a password.
    pub fn secret_key_bytes(&self) -> [u8; 32] {
        self.signing_key.to_bytes()
    }

    /// `operator:<name>`.
    pub fn operator_id(&self) -> &OperatorId {
        &self.operator_id
    }

    /// The principal id the operator signs requests as.
    pub fn principal(&self) -> NodeId {
        self.operator_id.to_principal()
    }

    pub fn public_key(&self) -> [u8; 32] {
        self.signing_key.verifying_key().to_bytes()
    }

    /// The public key as 64 hex characters (`orionctl operators enroll --public-key`).
    pub fn public_key_hex(&self) -> String {
        encode_hex(&self.public_key())
    }

    /// `sha256:<32 hex>`, the same format `orionctl get operators` and discovery print.
    pub fn fingerprint(&self) -> String {
        key_fingerprint(&self.public_key())
    }

    pub(crate) fn signing_key(&self) -> &SigningKey {
        &self.signing_key
    }
}

impl fmt::Debug for OperatorIdentity {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("OperatorIdentity")
            .field("operator_id", &self.operator_id)
            .field("fingerprint", &self.fingerprint())
            .finish_non_exhaustive()
    }
}

fn parse_id(name: &str) -> Result<OperatorId, RemoteError> {
    OperatorId::try_new(name).map_err(|err| RemoteError::InvalidIdentity(err.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn identities_roundtrip_through_their_secret() {
        let identity = OperatorIdentity::generate("alice").expect("valid name");
        assert_eq!(identity.operator_id().as_str(), "operator:alice");
        let restored =
            OperatorIdentity::from_secret_key_bytes("operator:alice", &identity.secret_key_bytes())
                .expect("restores");
        assert_eq!(restored.public_key(), identity.public_key());
        assert_eq!(restored.fingerprint(), identity.fingerprint());
        assert!(identity.fingerprint().starts_with("sha256:"));
        assert!(!format!("{identity:?}").contains(&encode_hex(&identity.secret_key_bytes())));
        assert!(OperatorIdentity::generate("not valid").is_err());
        assert!(OperatorIdentity::from_secret_key_bytes("alice", &[0; 31]).is_err());
    }
}
