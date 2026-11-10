//! The signed handshake, as bytes. No I/O here.
//!
//! ```text
//! C -> S  "ORTC" mode version                      mode 0: plain, 1: signed
//! S -> C  status                                   0: go on, 1: this node needs a signed session
//!   (plain ends here; frames follow)
//! C -> S  eph_C nonce_C key_C                      32 bytes each
//! S -> C  eph_S nonce_S key_S sig_S                32, 32, 32, 64
//! C -> S  sig_C                                    64
//! S -> C  status                                   0: accepted, 2: this key is not welcome
//!
//! T     = "orion+tcp-v1" eph_C nonce_C key_C eph_S nonce_S key_S
//! sig_S = Sign(key_S, "server\0" T)       sig_C = Sign(key_C, "client\0" T)
//! dh    = X25519(eph secret, the other side's eph)
//! k_c2s = HMAC(dh, T "c2s")               k_s2c = HMAC(dh, T "s2c")
//! ```
//!
//! Each side proves it holds its identity key by signing both sides' fresh values, so a
//! handshake cannot be played back. The ephemeral exchange gives the two a secret nobody
//! watching has, and every frame after carries a MAC under it: nothing can be changed, added,
//! dropped or reordered in flight without the next frame failing. The session is not
//! encrypted, as on `dev`: what goes over it can be read, and cannot be forged.

use hmac::{Hmac, KeyInit, Mac};
use orion_identity::{Identity, random, verify};
use sha2::Sha256;
use x25519_dalek::{X25519_BASEPOINT_BYTES, x25519};

pub const MAGIC: [u8; 4] = *b"ORTC";
pub const VERSION: u8 = 1;
pub const PREAMBLE_LEN: usize = 6;
pub const MODE_PLAIN: u8 = 0;
pub const MODE_SIGNED: u8 = 1;

pub const STATUS_OK: u8 = 0;
pub const STATUS_NEEDS_SIGNED: u8 = 1;
pub const STATUS_REFUSED: u8 = 2;

pub const CLIENT_HELLO_LEN: usize = 96;
pub const SERVER_HELLO_LEN: usize = 160;
pub const SIGNATURE_LEN: usize = 64;
/// Bytes of the MAC on every frame of a signed session.
pub const TAG_LEN: usize = 16;

const DOMAIN: &[u8] = b"orion+tcp-v1";

pub fn preamble(mode: u8) -> [u8; PREAMBLE_LEN] {
    [MAGIC[0], MAGIC[1], MAGIC[2], MAGIC[3], mode, VERSION]
}

/// One side's fresh values: an ephemeral secret, its public half and a nonce.
pub struct Fresh {
    secret: [u8; 32],
    pub public: [u8; 32],
    pub nonce: [u8; 32],
}

impl Fresh {
    pub fn new() -> Self {
        let secret: [u8; 32] = random();
        Self {
            public: x25519(secret, X25519_BASEPOINT_BYTES),
            secret,
            nonce: random(),
        }
    }
}

/// The values both sides sign: the client's three, then the server's three.
#[derive(Clone, Copy)]
pub struct Transcript {
    pub client: [u8; CLIENT_HELLO_LEN],
    pub server: [u8; 96],
}

impl Transcript {
    fn message(&self, label: &[u8]) -> Vec<u8> {
        let mut out = Vec::with_capacity(label.len() + DOMAIN.len() + 192);
        out.extend_from_slice(label);
        out.extend_from_slice(DOMAIN);
        out.extend_from_slice(&self.client);
        out.extend_from_slice(&self.server);
        out
    }

    pub fn client_key(&self) -> [u8; 32] {
        self.client[64..].try_into().expect("32 bytes")
    }

    pub fn server_key(&self) -> [u8; 32] {
        self.server[64..].try_into().expect("32 bytes")
    }

    pub fn sign_server(&self, identity: &Identity) -> [u8; 64] {
        identity.sign(&self.message(b"server\0"))
    }

    pub fn sign_client(&self, identity: &Identity) -> [u8; 64] {
        identity.sign(&self.message(b"client\0"))
    }

    pub fn server_signed(&self, signature: &[u8]) -> bool {
        verify(&self.server_key(), &self.message(b"server\0"), signature)
    }

    pub fn client_signed(&self, signature: &[u8]) -> bool {
        verify(&self.client_key(), &self.message(b"client\0"), signature)
    }

    /// The two frame keys, client-to-server then server-to-client, from this side's secret
    /// and the other side's ephemeral public key. `None` if the other side sent a point that
    /// gives no secret at all.
    pub fn keys(&self, own: &Fresh, client: bool) -> Option<([u8; 32], [u8; 32])> {
        let theirs: [u8; 32] = if client {
            self.server[..32].try_into().expect("32 bytes")
        } else {
            self.client[..32].try_into().expect("32 bytes")
        };
        let shared = x25519(own.secret, theirs);
        if shared == [0; 32] {
            return None;
        }
        let key = |label: &[u8]| -> [u8; 32] {
            let mut mac =
                <Hmac<Sha256> as KeyInit>::new_from_slice(&shared).expect("any key length");
            mac.update(DOMAIN);
            mac.update(&self.client);
            mac.update(&self.server);
            mac.update(label);
            mac.finalize().into_bytes().into()
        };
        Some((key(b"c2s"), key(b"s2c")))
    }
}

pub fn hello(fresh: &Fresh, key: &[u8; 32]) -> [u8; 96] {
    let mut out = [0_u8; 96];
    out[..32].copy_from_slice(&fresh.public);
    out[32..64].copy_from_slice(&fresh.nonce);
    out[64..].copy_from_slice(key);
    out
}

/// One direction's frame MAC: a key and a count of frames, so a frame only verifies in its
/// place in the order.
pub struct Seal {
    key: [u8; 32],
    count: u64,
}

impl Seal {
    pub const fn new(key: [u8; 32]) -> Self {
        Self { key, count: 0 }
    }

    fn tag(&self, frame: &[u8]) -> [u8; TAG_LEN] {
        let mut mac = <Hmac<Sha256> as KeyInit>::new_from_slice(&self.key).expect("any key length");
        mac.update(&self.count.to_le_bytes());
        mac.update(&(frame.len() as u32).to_le_bytes());
        mac.update(frame);
        let full = mac.finalize().into_bytes();
        full[..TAG_LEN].try_into().expect("16 bytes")
    }

    /// The MAC of the next frame to send.
    pub fn seal(&mut self, frame: &[u8]) -> [u8; TAG_LEN] {
        let tag = self.tag(frame);
        self.count += 1;
        tag
    }

    /// Whether `tag` is the MAC of the next frame expected. Compared in constant time.
    pub fn open(&mut self, frame: &[u8], tag: &[u8]) -> bool {
        let expected = self.tag(frame);
        let same = tag.len() == TAG_LEN
            && expected
                .iter()
                .zip(tag)
                .fold(0_u8, |difference, (a, b)| difference | (a ^ b))
                == 0;
        if same {
            self.count += 1;
        }
        same
    }
}

impl std::fmt::Debug for Seal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("Seal")
    }
}

impl Default for Fresh {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Debug for Fresh {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("Fresh")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn pair() -> (Transcript, Fresh, Fresh, Identity, Identity) {
        let (client, server) = (Identity::generate(), Identity::generate());
        let (fresh_c, fresh_s) = (Fresh::new(), Fresh::new());
        let transcript = Transcript {
            client: hello(&fresh_c, &client.public()),
            server: hello(&fresh_s, &server.public()),
        };
        (transcript, fresh_c, fresh_s, client, server)
    }

    #[test]
    fn both_sides_derive_the_same_keys_and_each_signature_only_fits_its_side() {
        let (transcript, fresh_c, fresh_s, client, server) = pair();
        let ours = transcript.keys(&fresh_c, true).expect("keys");
        let theirs = transcript.keys(&fresh_s, false).expect("keys");
        assert_eq!(ours, theirs);
        assert_ne!(ours.0, ours.1, "one key per direction");

        let (sig_s, sig_c) = (
            transcript.sign_server(&server),
            transcript.sign_client(&client),
        );
        assert!(transcript.server_signed(&sig_s) && transcript.client_signed(&sig_c));
        // A signature is for one side of one handshake.
        assert!(!transcript.server_signed(&transcript.sign_server(&client)));
        assert!(!transcript.client_signed(&sig_s));
        let (other, ..) = pair();
        assert!(!other.server_signed(&sig_s));
        // A point that gives no shared secret is refused.
        let mut zero = transcript;
        zero.server[..32].fill(0);
        assert!(zero.keys(&fresh_c, true).is_none());
    }

    #[test]
    fn a_frame_only_verifies_whole_and_in_its_place() {
        let key = [7; 32];
        let (mut sender, mut receiver) = (Seal::new(key), Seal::new(key));
        let first = sender.seal(b"first");
        let second = sender.seal(b"second");
        // Out of order, changed, cut short, or under another key: refused.
        assert!(!receiver.open(b"second", &second));
        assert!(!receiver.open(b"first!", &first));
        assert!(!receiver.open(b"first", &first[..15]));
        assert!(!Seal::new([8; 32]).open(b"first", &first));
        // In order: accepted, once.
        assert!(receiver.open(b"first", &first));
        assert!(!receiver.open(b"first", &first));
        assert!(receiver.open(b"second", &second));
    }
}
