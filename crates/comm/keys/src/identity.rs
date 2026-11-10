//! The node's key, and the two things done with keys: signing and checking.

use std::io::{self, Write};
use std::os::unix::fs::OpenOptionsExt;
use std::path::Path;

use ed25519_dalek::{Signature, Signer, SigningKey, VerifyingKey};
use orion_types::KeyId;
use sha2::{Digest, Sha256};

/// Fills `out` from the system's random source.
///
/// # Panics
///
/// If the system has none. Nothing here can work without it.
pub fn random<const N: usize>() -> [u8; N] {
    let mut out = [0_u8; N];
    getrandom::fill(&mut out).expect("the system's random source");
    out
}

/// An ed25519 identity.
pub struct Identity(SigningKey);

impl std::fmt::Debug for Identity {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Identity({})", fingerprint(&self.public()))
    }
}

impl Identity {
    /// A new identity.
    #[must_use]
    pub fn generate() -> Self {
        Self(SigningKey::from_bytes(&random()))
    }

    /// The identity with this secret.
    #[must_use]
    pub fn from_secret(secret: &[u8; 32]) -> Self {
        Self(SigningKey::from_bytes(secret))
    }

    /// The identity kept at `path`, made and saved there if there is none. The file holds the
    /// 32 secret bytes and is readable by its owner only.
    ///
    /// # Errors
    ///
    /// If the file can't be read or made, or is not 32 bytes. A key that is there but can't be
    /// used is never replaced: that would change who the board is.
    pub fn load_or_create(path: &Path) -> io::Result<Self> {
        match std::fs::read(path) {
            Ok(bytes) => {
                let secret: [u8; 32] = bytes.as_slice().try_into().map_err(|_| {
                    io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!("{} is not a 32-byte key", path.display()),
                    )
                })?;
                Ok(Self::from_secret(&secret))
            }
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                let identity = Self::generate();
                if let Some(parent) = path.parent() {
                    std::fs::create_dir_all(parent)?;
                }
                // `create_new`: if another start got there first, use its key, not ours.
                let made = std::fs::OpenOptions::new()
                    .write(true)
                    .create_new(true)
                    .mode(0o600)
                    .open(path);
                match made {
                    Ok(mut file) => {
                        file.write_all(&identity.0.to_bytes())?;
                        file.sync_all()?;
                        Ok(identity)
                    }
                    Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {
                        Self::load_or_create(path)
                    }
                    Err(error) => Err(error),
                }
            }
            Err(error) => Err(error),
        }
    }

    /// The secret, for a caller that keeps it somewhere of its own.
    #[must_use]
    pub fn secret(&self) -> [u8; 32] {
        self.0.to_bytes()
    }

    /// The public key.
    #[must_use]
    pub fn public(&self) -> [u8; 32] {
        self.0.verifying_key().to_bytes()
    }

    /// The public key, as the identity of this key.
    #[must_use]
    pub fn key_id(&self) -> KeyId {
        KeyId(self.public())
    }

    /// Signs `message`.
    #[must_use]
    pub fn sign(&self, message: &[u8]) -> [u8; 64] {
        self.0.sign(message).to_bytes()
    }
}

/// Whether `signature` is `key`'s signature of `message`.
#[must_use]
pub fn verify(key: &[u8; 32], message: &[u8], signature: &[u8]) -> bool {
    let (Ok(key), Ok(signature)) = (
        VerifyingKey::from_bytes(key),
        <[u8; 64]>::try_from(signature),
    ) else {
        return false;
    };
    key.verify_strict(message, &Signature::from_bytes(&signature))
        .is_ok()
}

/// A key's fingerprint, for people to compare: `sha256:` and 32 hex digits.
#[must_use]
pub fn fingerprint(key: &[u8; 32]) -> String {
    let digest = Sha256::digest(key);
    let mut out = String::from("sha256:");
    for byte in &digest[..16] {
        out.push_str(&format!("{byte:02x}"));
    }
    out
}
