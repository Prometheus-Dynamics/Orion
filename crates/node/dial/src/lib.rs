//! Node logic connecting to another node as this node.
//!
//! The comm core makes the node's key and keeps the trust file, which pins each peer to one
//! key; node logic reads both in `keys.dir`. A session to another node is made with this
//! node's key, so the other node sees a peer, and asks the other node to prove it holds the
//! key pinned for it. The trust file is read on each connect, so a peer trusted or revoked
//! since is seen.

use std::io;
use std::path::{Path, PathBuf};
use std::time::Duration;

use orion_connect::{Address, ClientError, Identity, Options, Roles, Session};
use orion_types::NodeName;

/// This node's key, which the comm core makes and keeps.
const IDENTITY_FILE: &str = "node-identity.ed25519";

/// The comm core's trust file, which names the peers and their keys.
const TRUST_FILE: &str = "trust";

/// How to reach other nodes as this one.
#[derive(Clone, Debug)]
pub struct Dialer {
    /// This node.
    pub me: NodeName,
    /// Where the node's key and the trust file are.
    pub keys_dir: PathBuf,
    /// Whether a node must be pinned: with security on, a node the trust file does not name
    /// is not connected to.
    pub check: bool,
    /// How long connecting may take.
    pub timeout: Duration,
}

impl Dialer {
    /// A session to `node`, reached at `address` (`host:port`), with `roles`. Its name on the
    /// other node is `<what>:<this node>`.
    ///
    /// # Errors
    ///
    /// [`ClientError::Denied`] if the trust file pins no key for `node` and security is on,
    /// or `node` does not prove it holds the pinned key; otherwise what connecting gave.
    pub fn dial(
        &self,
        node: &NodeName,
        address: &str,
        what: &str,
        roles: Roles,
    ) -> Result<Session, ClientError> {
        let key = pinned(&self.keys_dir.join(TRUST_FILE), node)?;
        if self.check && key.is_none() {
            return Err(ClientError::Denied(
                "the trust file pins no key for that node",
            ));
        }
        let identity = Identity::load_or_create(&self.keys_dir.join(IDENTITY_FILE))?;
        let options = Options {
            roles,
            identity: Some(identity),
            timeout: self.timeout,
            ..Options::default()
        };
        let address = Address::Network {
            host: address.to_owned(),
            key,
        };
        Session::connect(&address, &format!("{what}:{}", self.me), options)
    }
}

/// The key the trust file at `path` pins for `node`: a line `peer NODE HEX`, unless the key is
/// also on a `revoked HEX` line. No file pins nothing.
///
/// # Errors
///
/// The file is there and cannot be read.
pub fn pinned(path: &Path, node: &NodeName) -> Result<Option<[u8; 32]>, ClientError> {
    let text = match std::fs::read_to_string(path) {
        Ok(text) => text,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(ClientError::Io(error)),
    };
    let mut key = None;
    let mut revoked = Vec::new();
    for line in text.lines() {
        let mut words = line.split_whitespace();
        match (words.next(), words.next(), words.next()) {
            (Some("peer"), Some(name), Some(hex)) if name == node.as_str() => key = unhex(hex),
            (Some("revoked"), Some(hex), _) => revoked.extend(unhex(hex)),
            _ => {}
        }
    }
    Ok(key.filter(|key| !revoked.contains(key)))
}

fn unhex(text: &str) -> Option<[u8; 32]> {
    let mut key = [0_u8; 32];
    if text.len() != 64 {
        return None;
    }
    for (byte, pair) in key.iter_mut().zip(text.as_bytes().chunks(2)) {
        *byte = u8::from_str_radix(std::str::from_utf8(pair).ok()?, 16).ok()?;
    }
    Some(key)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_pin_is_read_and_a_revoked_one_is_not() {
        let dir = std::env::temp_dir().join(format!("orion-dial-{}", std::process::id()));
        std::fs::create_dir_all(&dir).expect("dir");
        let path = dir.join(TRUST_FILE);
        let (right, other) = ("ab".repeat(32), "cd".repeat(32));
        let node: NodeName = "right".parse().expect("name");
        assert_eq!(pinned(&path, &node).expect("no file"), None);
        std::fs::write(
            &path,
            format!("# a comment\npeer right {right}\npeer other {other}\n"),
        )
        .expect("write");
        assert_eq!(pinned(&path, &node).expect("read"), Some([0xab; 32]));
        std::fs::write(&path, format!("peer right {right}\nrevoked {right}\n")).expect("write");
        assert_eq!(pinned(&path, &node).expect("read"), None);
        std::fs::write(&path, "peer right not-hex\n").expect("write");
        assert_eq!(pinned(&path, &node).expect("read"), None);
        let _ = std::fs::remove_dir_all(&dir);
    }
}
