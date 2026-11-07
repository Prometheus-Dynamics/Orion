//! The remote link channel (`tcp:` links, docs/design/stable-interface.md, "Step 6 wire"): a
//! handshake signed by the node's and the operator's ed25519 keys, then link bytes in sealed
//! records (ChaCha20-Poly1305, one key per direction, counter nonces, key rotation).
//!
//! Sans-IO: the caller moves bytes and supplies the randomness; this module only builds and
//! checks messages.
//!
//! Handshake, each message as `[u16 LE length][body]`:
//!
//! 1. client → node `ClientHello`: `b"OLC1"`, the client's ephemeral X25519 key, 32 random bytes.
//! 2. node → client `ServerHello`: `b"OLS1"`, the node's ephemeral X25519 key, 32 random bytes,
//!    the node id (`u8` length, UTF-8), the node's ed25519 key, and its signature over
//!    [`SERVER_DOMAIN`] and SHA-256 of `ClientHello` and `ServerHello` up to the signature.
//! 3. client → node `ClientAuth`: `b"OLA1"`, the operator principal (`u8` length, UTF-8,
//!    `operator:<name>`), the operator's ed25519 key, and its signature over [`CLIENT_DOMAIN`] and
//!    SHA-256 of the three messages up to the signature.
//!
//! Both keys come from HKDF-SHA256 of the X25519 secret, salted with that last hash, so they
//! belong to this handshake alone. The node answers `ClientAuth` with a sealed [`KIND_ACCEPT`]
//! record, or [`KIND_REFUSE`] with the reason, and closes.
//!
//! A record is `[u16 LE length][ciphertext][16-byte tag]`; the plaintext is a kind byte and the
//! payload, the length is the associated data, and the nonce is the record's number in its
//! direction (`u64 LE`, then four zero bytes). TCP keeps records in order, so a record replayed,
//! dropped or reordered fails its tag and ends the connection. A sender rotates its key with a
//! [`KIND_ROTATE`] record: the records after it use the next key, HKDF of the current one, and
//! numbering starts again at 0. [`Sealer`] rotates by itself every [`ROTATE_RECORDS`] records;
//! callers rotate on time as well ([`ROTATE_AFTER_SECS`]).

use alloc::{string::String, vec::Vec};
use chacha20poly1305::{
    AeadInPlace, ChaCha20Poly1305, KeyInit as _, Nonce, Tag, aead::generic_array::GenericArray,
};
use curve25519_dalek::montgomery::MontgomeryPoint;
use ed25519_dalek::{Signature, Signer, SigningKey, VerifyingKey};
use hmac::{Hmac, KeyInit, Mac};
use sha2::{Digest, Sha256};
use zeroize::Zeroize;

/// Signature domain of the node's `ServerHello`.
pub const SERVER_DOMAIN: &[u8] = b"orion.link.remote.server.v1";
/// Signature domain of the operator's `ClientAuth`.
pub const CLIENT_DOMAIN: &[u8] = b"orion.link.remote.client.v1";
/// Largest handshake message body.
pub const MAX_HANDSHAKE: usize = 1024;
/// Largest payload of one record; longer data goes in several.
pub const MAX_PAYLOAD: usize = 16 * 1024;
/// Largest record body (kind, payload, tag).
pub const MAX_RECORD: usize = 1 + MAX_PAYLOAD + TAG;
/// Records a sender seals with one key before it rotates by itself.
pub const ROTATE_RECORDS: u64 = 1 << 20;
/// Seconds after which callers should rotate a direction's key ([`Sealer::rotate`]).
pub const ROTATE_AFTER_SECS: u64 = 600;

/// Link bytes.
pub const KIND_DATA: u8 = 0;
/// The sender's later records use its next key.
pub const KIND_ROTATE: u8 = 1;
/// The node accepted the operator (the first record from the node).
pub const KIND_ACCEPT: u8 = 2;
/// The node refused the operator; the payload is the reason (UTF-8), then the node closes.
pub const KIND_REFUSE: u8 = 3;

const TAG: usize = 16;
const KEY: usize = 32;
const CLIENT_MAGIC: &[u8; 4] = b"OLC1";
const SERVER_MAGIC: &[u8; 4] = b"OLS1";
const AUTH_MAGIC: &[u8; 4] = b"OLA1";

/// Why a handshake message or record was refused. Every error ends the connection.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ChannelError {
    /// A message that does not parse, or is longer than its limit.
    Malformed(&'static str),
    /// A signature or key that does not verify.
    BadSignature(&'static str),
    /// The node is not the one the client expected (its key differs).
    UnexpectedNode,
    /// A record whose tag does not verify: altered, replayed, dropped or reordered.
    BadRecord,
}

impl core::fmt::Display for ChannelError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::Malformed(what) => write!(f, "malformed {what}"),
            Self::BadSignature(what) => write!(f, "{what} does not verify"),
            Self::UnexpectedNode => f.write_str("the node's key is not the expected one"),
            Self::BadRecord => f.write_str("a record failed authentication"),
        }
    }
}

/// `[u16 LE length][body]`.
#[must_use]
pub fn frame(body: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(2 + body.len());
    out.extend_from_slice(&u16::try_from(body.len()).unwrap_or(u16::MAX).to_le_bytes());
    out.extend_from_slice(body);
    out
}

/// Takes one handshake message off the front of `buf`, if it is all there.
///
/// # Errors
/// A message longer than [`MAX_HANDSHAKE`].
pub fn take_message(buf: &mut Vec<u8>) -> Result<Option<Vec<u8>>, ChannelError> {
    let Some(len) = buf
        .get(..2)
        .map(|b| usize::from(u16::from_le_bytes([b[0], b[1]])))
    else {
        return Ok(None);
    };
    if len > MAX_HANDSHAKE {
        return Err(ChannelError::Malformed("handshake message"));
    }
    if buf.len() < 2 + len {
        return Ok(None);
    }
    let body = buf.get(2..2 + len).unwrap_or_default().to_vec();
    buf.drain(..2 + len);
    Ok(Some(body))
}

/// The client's side of the handshake.
pub struct ClientHandshake {
    secret: [u8; 32],
    hello: Vec<u8>,
}

impl Drop for ClientHandshake {
    fn drop(&mut self) {
        self.secret.zeroize();
    }
}

/// The client's handshake, done: `auth` goes to the node, then records.
pub struct ClientDone {
    /// The `ClientAuth` body, to send framed.
    pub auth: Vec<u8>,
    /// The node's id, as it signed it.
    pub node_id: String,
    /// The node's ed25519 key.
    pub node_key: [u8; 32],
    /// Seals the client's records.
    pub sealer: Sealer,
    /// Opens the node's records.
    pub opener: Opener,
}

impl ClientHandshake {
    /// Starts a handshake with an ephemeral secret and random bytes from the OS.
    #[must_use]
    pub fn new(secret: [u8; 32], random: [u8; 32]) -> Self {
        let public = MontgomeryPoint::mul_base_clamped(secret).to_bytes();
        let mut hello = Vec::with_capacity(4 + 64);
        hello.extend_from_slice(CLIENT_MAGIC);
        hello.extend_from_slice(&public);
        hello.extend_from_slice(&random);
        Self { secret, hello }
    }

    /// The `ClientHello` body, to send framed.
    #[must_use]
    pub fn hello(&self) -> &[u8] {
        &self.hello
    }

    /// Checks the node's `ServerHello` (against `expected_node_key` when the caller pinned one)
    /// and signs `ClientAuth` as `operator` (`operator:<name>`).
    ///
    /// # Errors
    /// A malformed `ServerHello`, a signature that does not verify, or another node's key.
    pub fn finish(
        self,
        server_hello: &[u8],
        expected_node_key: Option<&[u8; 32]>,
        operator: &str,
        operator_key: &SigningKey,
    ) -> Result<ClientDone, ChannelError> {
        let server = ServerHello::parse(server_hello)?;
        if expected_node_key.is_some_and(|key| key != &server.node_key) {
            return Err(ChannelError::UnexpectedNode);
        }
        let mut hash = Sha256::new();
        hash.update(&self.hello);
        hash.update(server.signed);
        verify(
            &server.node_key,
            SERVER_DOMAIN,
            &hash.finalize(),
            server.signature,
            "the node's signature",
        )?;
        if operator.len() > usize::from(u8::MAX) || operator.is_empty() {
            return Err(ChannelError::Malformed("operator principal"));
        }
        let mut auth = Vec::with_capacity(4 + 1 + operator.len() + 32 + 64);
        auth.extend_from_slice(AUTH_MAGIC);
        auth.push(operator.len() as u8);
        auth.extend_from_slice(operator.as_bytes());
        auth.extend_from_slice(operator_key.verifying_key().as_bytes());
        let mut hash = Sha256::new();
        hash.update(&self.hello);
        hash.update(server_hello);
        hash.update(&auth);
        let transcript: [u8; 32] = hash.finalize().into();
        let signature = operator_key.sign(&signed(CLIENT_DOMAIN, &transcript));
        auth.extend_from_slice(&signature.to_bytes());
        let (c2s, s2c) = derive(&self.secret, &server.ephemeral, &transcript)?;
        Ok(ClientDone {
            auth,
            node_id: String::from(server.node_id),
            node_key: server.node_key,
            sealer: Sealer::new(c2s),
            opener: Opener::new(s2c),
        })
    }
}

/// The node's side of the handshake, after its `ServerHello`.
pub struct ServerHandshake {
    secret: [u8; 32],
    client_ephemeral: [u8; 32],
    hash: Sha256,
}

impl Drop for ServerHandshake {
    fn drop(&mut self) {
        self.secret.zeroize();
    }
}

/// The node's handshake, done: who signed, and the keys. Whether the operator may connect is
/// the caller's decision; it answers with [`KIND_ACCEPT`] or [`KIND_REFUSE`].
pub struct ServerDone {
    /// The operator principal the client signed as (`operator:<name>`, not checked here).
    pub operator: String,
    /// The operator's ed25519 key.
    pub operator_key: [u8; 32],
    /// Seals the node's records.
    pub sealer: Sealer,
    /// Opens the client's records.
    pub opener: Opener,
}

impl ServerHandshake {
    /// Answers `client_hello`: returns the `ServerHello` body, to send framed.
    ///
    /// # Errors
    /// A malformed `ClientHello`, or a node id longer than 255 bytes.
    pub fn respond(
        client_hello: &[u8],
        secret: [u8; 32],
        random: [u8; 32],
        node_id: &str,
        node_key: &SigningKey,
    ) -> Result<(Vec<u8>, Self), ChannelError> {
        let (Some(magic), Some(client_ephemeral), true) = (
            client_hello.get(..4),
            client_hello.get(4..36),
            client_hello.len() == 68,
        ) else {
            return Err(ChannelError::Malformed("client hello"));
        };
        if magic != CLIENT_MAGIC {
            return Err(ChannelError::Malformed("client hello"));
        }
        if node_id.is_empty() || node_id.len() > usize::from(u8::MAX) {
            return Err(ChannelError::Malformed("node id"));
        }
        let mut client_ephemeral_key = [0u8; 32];
        client_ephemeral_key.copy_from_slice(client_ephemeral);
        let public = MontgomeryPoint::mul_base_clamped(secret).to_bytes();
        let mut hello = Vec::with_capacity(4 + 64 + 1 + node_id.len() + 32 + 64);
        hello.extend_from_slice(SERVER_MAGIC);
        hello.extend_from_slice(&public);
        hello.extend_from_slice(&random);
        hello.push(node_id.len() as u8);
        hello.extend_from_slice(node_id.as_bytes());
        hello.extend_from_slice(node_key.verifying_key().as_bytes());
        let mut hash = Sha256::new();
        hash.update(client_hello);
        hash.update(&hello);
        let signature = node_key.sign(&signed(SERVER_DOMAIN, &hash.clone().finalize()));
        hello.extend_from_slice(&signature.to_bytes());
        // The client's transcript hash covers the whole `ServerHello`, signature included.
        let mut hash = Sha256::new();
        hash.update(client_hello);
        hash.update(&hello);
        Ok((
            hello,
            Self {
                secret,
                client_ephemeral: client_ephemeral_key,
                hash,
            },
        ))
    }

    /// Checks `ClientAuth`: the operator's signature over the handshake.
    ///
    /// # Errors
    /// A malformed `ClientAuth` or a signature that does not verify.
    pub fn finish(self, client_auth: &[u8]) -> Result<ServerDone, ChannelError> {
        let malformed = ChannelError::Malformed("client auth");
        if client_auth.get(..4) != Some(AUTH_MAGIC.as_slice()) {
            return Err(malformed);
        }
        let len = usize::from(*client_auth.get(4).ok_or(malformed.clone())?);
        let operator = client_auth
            .get(5..5 + len)
            .and_then(|bytes| core::str::from_utf8(bytes).ok())
            .filter(|operator| !operator.is_empty())
            .ok_or(malformed.clone())?;
        let key_at = 5 + len;
        let (Some(key), Some(signature), true) = (
            client_auth.get(key_at..key_at + 32),
            client_auth.get(key_at + 32..key_at + 96),
            client_auth.len() == key_at + 96,
        ) else {
            return Err(malformed);
        };
        let mut operator_key = [0u8; 32];
        operator_key.copy_from_slice(key);
        let mut hash = self.hash.clone();
        hash.update(client_auth.get(..key_at + 32).unwrap_or_default());
        let transcript: [u8; 32] = hash.finalize().into();
        verify(
            &operator_key,
            CLIENT_DOMAIN,
            &transcript,
            signature,
            "the operator's signature",
        )?;
        let (c2s, s2c) = derive(&self.secret, &self.client_ephemeral, &transcript)?;
        Ok(ServerDone {
            operator: String::from(operator),
            operator_key,
            sealer: Sealer::new(s2c),
            opener: Opener::new(c2s),
        })
    }
}

struct ServerHello<'a> {
    ephemeral: [u8; 32],
    node_id: &'a str,
    node_key: [u8; 32],
    /// Everything up to the signature.
    signed: &'a [u8],
    signature: &'a [u8],
}

impl<'a> ServerHello<'a> {
    fn parse(body: &'a [u8]) -> Result<Self, ChannelError> {
        let malformed = ChannelError::Malformed("server hello");
        if body.get(..4) != Some(SERVER_MAGIC.as_slice()) {
            return Err(malformed);
        }
        let mut ephemeral = [0u8; 32];
        ephemeral.copy_from_slice(body.get(4..36).ok_or(malformed.clone())?);
        let len = usize::from(*body.get(68).ok_or(malformed.clone())?);
        let node_id = body
            .get(69..69 + len)
            .and_then(|bytes| core::str::from_utf8(bytes).ok())
            .filter(|id| !id.is_empty())
            .ok_or(malformed.clone())?;
        let key_at = 69 + len;
        let (Some(key), Some(signed), Some(signature), true) = (
            body.get(key_at..key_at + 32),
            body.get(..key_at + 32),
            body.get(key_at + 32..key_at + 96),
            body.len() == key_at + 96,
        ) else {
            return Err(malformed);
        };
        let mut node_key = [0u8; 32];
        node_key.copy_from_slice(key);
        Ok(Self {
            ephemeral,
            node_id,
            node_key,
            signed,
            signature,
        })
    }
}

fn signed(domain: &[u8], transcript: &[u8]) -> Vec<u8> {
    let mut message = Vec::with_capacity(domain.len() + transcript.len());
    message.extend_from_slice(domain);
    message.extend_from_slice(transcript);
    message
}

fn verify(
    key: &[u8; 32],
    domain: &[u8],
    transcript: &[u8],
    signature: &[u8],
    what: &'static str,
) -> Result<(), ChannelError> {
    let key = VerifyingKey::from_bytes(key).map_err(|_| ChannelError::BadSignature(what))?;
    let signature =
        Signature::from_slice(signature).map_err(|_| ChannelError::BadSignature(what))?;
    key.verify_strict(&signed(domain, transcript), &signature)
        .map_err(|_| ChannelError::BadSignature(what))
}

fn hmac(key: &[u8], parts: &[&[u8]]) -> [u8; KEY] {
    // HMAC takes keys of any length.
    let Ok(mut mac) = <Hmac<Sha256> as KeyInit>::new_from_slice(key) else {
        unreachable!("HMAC takes keys of any length")
    };
    for part in parts {
        mac.update(part);
    }
    mac.finalize().into_bytes().into()
}

/// HKDF-SHA256 of the X25519 secret, salted with the transcript: (client → node, node → client).
fn derive(
    secret: &[u8; 32],
    peer_ephemeral: &[u8; 32],
    transcript: &[u8; 32],
) -> Result<([u8; KEY], [u8; KEY]), ChannelError> {
    let mut shared = MontgomeryPoint(*peer_ephemeral)
        .mul_clamped(*secret)
        .to_bytes();
    // A low-order peer key gives the all-zero secret: refuse it.
    if shared.iter().all(|b| *b == 0) {
        return Err(ChannelError::Malformed("ephemeral key"));
    }
    let mut prk = hmac(transcript, &[&shared]);
    shared.zeroize();
    let c2s = hmac(&prk, &[b"orion.link.remote c2s", &[1]]);
    let s2c = hmac(&prk, &[b"orion.link.remote s2c", &[1]]);
    prk.zeroize();
    Ok((c2s, s2c))
}

fn next_key(key: &[u8; KEY]) -> [u8; KEY] {
    hmac(key, &[b"orion.link.remote rotate", &[1]])
}

fn nonce(counter: u64) -> Nonce {
    let mut nonce = [0u8; 12];
    nonce[..8].copy_from_slice(&counter.to_le_bytes());
    *Nonce::from_slice(&nonce)
}

/// Seals one direction's records.
pub struct Sealer {
    key: [u8; KEY],
    cipher: ChaCha20Poly1305,
    counter: u64,
}

impl Drop for Sealer {
    fn drop(&mut self) {
        self.key.zeroize();
    }
}

impl Sealer {
    fn new(key: [u8; KEY]) -> Self {
        Self {
            cipher: ChaCha20Poly1305::new(GenericArray::from_slice(&key)),
            key,
            counter: 0,
        }
    }

    /// Appends one record of `kind` with `payload` (at most [`MAX_PAYLOAD`] bytes) to `out`.
    pub fn seal(&mut self, kind: u8, payload: &[u8], out: &mut Vec<u8>) {
        debug_assert!(payload.len() <= MAX_PAYLOAD);
        if self.counter >= ROTATE_RECORDS && kind != KIND_ROTATE {
            self.rotate(out);
        }
        let len = 1 + payload.len() + TAG;
        let aad = (len as u16).to_le_bytes();
        out.extend_from_slice(&aad);
        let start = out.len();
        out.push(kind);
        out.extend_from_slice(payload);
        let tag = self
            .cipher
            .encrypt_in_place_detached(&nonce(self.counter), &aad, &mut out[start..])
            .unwrap_or_else(|_| unreachable!("a record is far below ChaCha20's limit"));
        out.extend_from_slice(&tag);
        self.counter += 1;
    }

    /// Appends `data` as link-byte records, split at [`MAX_PAYLOAD`].
    pub fn seal_data(&mut self, data: &[u8], out: &mut Vec<u8>) {
        for chunk in data.chunks(MAX_PAYLOAD) {
            self.seal(KIND_DATA, chunk, out);
        }
    }

    /// Appends a [`KIND_ROTATE`] record and moves to the next key.
    pub fn rotate(&mut self, out: &mut Vec<u8>) {
        self.seal(KIND_ROTATE, &[], out);
        let next = next_key(&self.key);
        self.key.zeroize();
        *self = Self::new(next);
    }

    /// Records sealed with the current key.
    #[must_use]
    pub fn records(&self) -> u64 {
        self.counter
    }
}

/// Opens the other direction's records.
pub struct Opener {
    key: [u8; KEY],
    cipher: ChaCha20Poly1305,
    counter: u64,
}

impl Drop for Opener {
    fn drop(&mut self) {
        self.key.zeroize();
    }
}

impl Opener {
    fn new(key: [u8; KEY]) -> Self {
        Self {
            cipher: ChaCha20Poly1305::new(GenericArray::from_slice(&key)),
            key,
            counter: 0,
        }
    }

    /// Opens the record at the front of `buf`, if it is all there, and removes it. Rotations
    /// are applied here and not returned. Returns the record's kind; its payload is appended to
    /// `payload`.
    ///
    /// # Errors
    /// A record longer than [`MAX_RECORD`] or one that fails its tag.
    pub fn open(
        &mut self,
        buf: &mut Vec<u8>,
        payload: &mut Vec<u8>,
    ) -> Result<Option<u8>, ChannelError> {
        loop {
            match self.open_slice(buf, payload)? {
                Some((kind, used)) => {
                    buf.drain(..used);
                    if kind != KIND_ROTATE {
                        return Ok(Some(kind));
                    }
                }
                None => return Ok(None),
            }
        }
    }

    /// As [`Self::open`], without removing anything: decrypts the record at the front of `buf`
    /// in place and returns its kind and the bytes it (and any rotation before it) took, which
    /// the caller skips. [`KIND_ROTATE`] means only rotations were there: skip the bytes and
    /// call again. For a reader that drains its buffer once per read instead of once per record.
    ///
    /// # Errors
    /// As [`Self::open`]. A failed record leaves the opener unusable: the connection ends.
    pub fn open_slice(
        &mut self,
        buf: &mut [u8],
        payload: &mut Vec<u8>,
    ) -> Result<Option<(u8, usize)>, ChannelError> {
        let mut at = 0;
        loop {
            let rest = &mut buf[at..];
            // Rotations already applied: the caller must skip their records.
            let applied = (at > 0).then_some((KIND_ROTATE, at));
            let Some(len) = rest
                .get(..2)
                .map(|b| usize::from(u16::from_le_bytes([b[0], b[1]])))
            else {
                return Ok(applied);
            };
            if !(1 + TAG..=MAX_RECORD).contains(&len) {
                return Err(ChannelError::Malformed("record"));
            }
            if rest.len() < 2 + len {
                return Ok(applied);
            }
            let aad = [rest[0], rest[1]];
            let (text, tag) = rest[2..2 + len].split_at_mut(len - TAG);
            self.cipher
                .decrypt_in_place_detached(&nonce(self.counter), &aad, text, Tag::from_slice(tag))
                .map_err(|_| ChannelError::BadRecord)?;
            self.counter += 1;
            at += 2 + len;
            let kind = text[0];
            if kind == KIND_ROTATE {
                let next = next_key(&self.key);
                self.key.zeroize();
                *self = Self::new(next);
                continue;
            }
            payload.extend_from_slice(&text[1..]);
            return Ok(Some((kind, at)));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloc::vec;

    fn key(seed: u8) -> SigningKey {
        SigningKey::from_bytes(&[seed; 32])
    }

    fn pair() -> (ClientDone, ServerDone) {
        let client = ClientHandshake::new([1; 32], [2; 32]);
        let node = key(9);
        let (hello, server) =
            ServerHandshake::respond(client.hello(), [3; 32], [4; 32], "node-a", &node).unwrap();
        let done = client
            .finish(
                &hello,
                Some(node.verifying_key().as_bytes()),
                "operator:alice",
                &key(7),
            )
            .unwrap();
        let server = server.finish(&done.auth).unwrap();
        (done, server)
    }

    #[test]
    fn both_sides_agree_and_records_go_both_ways() {
        let (mut client, mut server) = pair();
        assert_eq!(client.node_id, "node-a");
        assert_eq!(server.operator, "operator:alice");
        assert_eq!(&server.operator_key, key(7).verifying_key().as_bytes());
        let mut wire = Vec::new();
        client.sealer.seal_data(b"hello", &mut wire);
        let mut got = Vec::new();
        assert_eq!(server.opener.open(&mut wire, &mut got), Ok(Some(KIND_DATA)));
        assert_eq!(got, b"hello");
        assert!(wire.is_empty());
        server.sealer.seal(KIND_ACCEPT, &[], &mut wire);
        got.clear();
        assert_eq!(
            client.opener.open(&mut wire, &mut got),
            Ok(Some(KIND_ACCEPT))
        );
    }

    #[test]
    fn a_partial_record_waits_for_the_rest() {
        let (mut client, mut server) = pair();
        let mut sealed = Vec::new();
        client.sealer.seal_data(&[5; 100], &mut sealed);
        let mut wire = sealed[..50].to_vec();
        let mut got = Vec::new();
        assert_eq!(server.opener.open(&mut wire, &mut got), Ok(None));
        wire.extend_from_slice(&sealed[50..]);
        assert_eq!(server.opener.open(&mut wire, &mut got), Ok(Some(KIND_DATA)));
        assert_eq!(got, vec![5; 100]);
    }

    #[test]
    fn altered_replayed_and_reordered_records_fail() {
        let (mut client, mut server) = pair();
        let (mut first, mut second) = (Vec::new(), Vec::new());
        client.sealer.seal_data(b"one", &mut first);
        client.sealer.seal_data(b"two", &mut second);
        let mut got = Vec::new();
        // Reordered: the second record under the first one's nonce.
        assert_eq!(
            server.opener.open(&mut second.clone(), &mut got),
            Err(ChannelError::BadRecord)
        );
        let (mut client, mut server) = pair();
        let mut first = Vec::new();
        client.sealer.seal_data(b"one", &mut first);
        let replay = first.clone();
        assert_eq!(
            server.opener.open(&mut first, &mut got),
            Ok(Some(KIND_DATA))
        );
        assert_eq!(
            server.opener.open(&mut replay.clone(), &mut got),
            Err(ChannelError::BadRecord)
        );
        let (mut client, mut server) = pair();
        let mut altered = Vec::new();
        client.sealer.seal_data(b"one", &mut altered);
        altered[3] ^= 1;
        assert_eq!(
            server.opener.open(&mut altered, &mut got),
            Err(ChannelError::BadRecord)
        );
    }

    #[test]
    fn records_after_a_rotation_open_with_the_next_key() {
        let (mut client, mut server) = pair();
        let mut wire = Vec::new();
        client.sealer.seal_data(b"before", &mut wire);
        client.sealer.rotate(&mut wire);
        assert_eq!(client.sealer.records(), 0);
        client.sealer.seal_data(b"after", &mut wire);
        let mut got = Vec::new();
        assert_eq!(server.opener.open(&mut wire, &mut got), Ok(Some(KIND_DATA)));
        assert_eq!(server.opener.open(&mut wire, &mut got), Ok(Some(KIND_DATA)));
        assert_eq!(got, b"beforeafter");
        // The old key no longer opens anything.
        let (mut client2, _) = pair();
        let mut stale = Vec::new();
        client2.sealer.seal_data(b"x", &mut stale);
        assert_eq!(
            server.opener.open(&mut stale, &mut got),
            Err(ChannelError::BadRecord)
        );
    }

    #[test]
    fn open_slice_skips_rotations_and_reports_what_it_took() {
        let (mut client, mut server) = pair();
        let mut wire = Vec::new();
        client.sealer.rotate(&mut wire);
        let rotation = wire.len();
        client.sealer.seal_data(b"ab", &mut wire);
        client.sealer.rotate(&mut wire);
        let mut got = Vec::new();
        // The data record comes back with the rotation before it counted in.
        let (kind, used) = server
            .opener
            .open_slice(&mut wire, &mut got)
            .unwrap()
            .unwrap();
        assert_eq!((kind, got.as_slice()), (KIND_DATA, b"ab".as_slice()));
        assert!(used > rotation);
        // Only a rotation left: reported as such, to be skipped.
        let rest = &mut wire[used..];
        let (kind, left) = server.opener.open_slice(rest, &mut got).unwrap().unwrap();
        assert_eq!((kind, left), (KIND_ROTATE, rest.len()));
        let mut after = Vec::new();
        client.sealer.seal_data(b"c", &mut after);
        assert_eq!(
            server.opener.open(&mut after, &mut got),
            Ok(Some(KIND_DATA))
        );
    }

    #[test]
    fn a_tampered_handshake_or_another_node_is_refused() {
        let client = ClientHandshake::new([1; 32], [2; 32]);
        let node = key(9);
        let (mut hello, _) =
            ServerHandshake::respond(client.hello(), [3; 32], [4; 32], "node-a", &node).unwrap();
        let other = key(8);
        let pinned = other.verifying_key();
        let result = ClientHandshake::new([1; 32], [2; 32]).finish(
            &hello,
            Some(pinned.as_bytes()),
            "operator:alice",
            &key(7),
        );
        assert!(matches!(result, Err(ChannelError::UnexpectedNode)));
        hello[40] ^= 1;
        let result = client.finish(&hello, None, "operator:alice", &key(7));
        assert!(matches!(result, Err(ChannelError::BadSignature(_))));

        // The operator's signature covers the node's hello: an auth for another handshake fails.
        let (mut done, _) = pair();
        let client = ClientHandshake::new([5; 32], [6; 32]);
        let (_, server) =
            ServerHandshake::respond(client.hello(), [3; 32], [4; 32], "node-a", &node).unwrap();
        assert!(matches!(
            server.finish(&done.auth),
            Err(ChannelError::BadSignature(_))
        ));
        done.auth.truncate(10);
        let client = ClientHandshake::new([5; 32], [6; 32]);
        let (_, server) =
            ServerHandshake::respond(client.hello(), [3; 32], [4; 32], "node-a", &node).unwrap();
        assert!(matches!(
            server.finish(&done.auth),
            Err(ChannelError::Malformed(_))
        ));
    }

    #[test]
    fn a_low_order_ephemeral_key_is_refused() {
        assert!(matches!(
            derive(&[1; 32], &[0; 32], &[0; 32]),
            Err(ChannelError::Malformed(_))
        ));
    }

    #[test]
    fn handshake_messages_are_framed_and_bounded() {
        let mut buf = frame(b"abc");
        buf.extend_from_slice(&frame(b"de")[..3]);
        assert_eq!(take_message(&mut buf), Ok(Some(b"abc".to_vec())));
        assert_eq!(take_message(&mut buf), Ok(None));
        let mut big = ((MAX_HANDSHAKE + 1) as u16).to_le_bytes().to_vec();
        assert!(take_message(&mut big).is_err());
    }
}
