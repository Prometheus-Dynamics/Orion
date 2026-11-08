//! SSH host key fingerprints, from a directory of OpenSSH `*.pub` files
//! (`ORION_NODE_SSH_HOST_KEYS`). Only the public halves are read. A fleet tool checks them over
//! Orion's authenticated channel before it trusts a board's SSH.

use sha2::{Digest, Sha256};
use std::path::{Path, PathBuf};

/// One host key.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SshHostKey {
    /// `ed25519`, `ecdsa` or `rsa`.
    pub kind: String,
    /// As `ssh-keygen -l` prints it: `SHA256:` and unpadded base64.
    pub sha256: String,
}

/// The directory to read, when `ORION_NODE_SSH_HOST_KEYS` names one.
pub fn ssh_host_keys_dir_from_env() -> Option<PathBuf> {
    std::env::var_os("ORION_NODE_SSH_HOST_KEYS")
        .filter(|value| !value.is_empty())
        .map(PathBuf::from)
}

/// The fingerprints of the `*.pub` files in `dir`, sorted by kind. Empty when the directory
/// cannot be read; a file that is not a public key is skipped.
pub fn read_ssh_host_keys(dir: &Path) -> Vec<SshHostKey> {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return Vec::new();
    };
    let mut keys: Vec<SshHostKey> = entries
        .flatten()
        .map(|entry| entry.path())
        .filter(|path| path.extension().is_some_and(|ext| ext == "pub"))
        .filter_map(|path| std::fs::read_to_string(path).ok())
        .filter_map(|text| parse_public_key(&text))
        .collect();
    keys.sort_by(|a, b| a.kind.cmp(&b.kind));
    keys
}

fn parse_public_key(text: &str) -> Option<SshHostKey> {
    let mut fields = text.split_whitespace();
    let (kind, blob) = (fields.next()?, fields.next()?);
    let kind = match kind {
        "ssh-ed25519" => "ed25519",
        "ssh-rsa" => "rsa",
        other if other.starts_with("ecdsa-sha2-") => "ecdsa",
        _ => return None,
    };
    let digest = Sha256::digest(decode_base64(blob)?);
    Some(SshHostKey {
        kind: kind.to_owned(),
        sha256: format!("SHA256:{}", encode_base64(digest.as_slice())),
    })
}

const ALPHABET: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

fn decode_base64(text: &str) -> Option<Vec<u8>> {
    let mut out = Vec::with_capacity(text.len() / 4 * 3);
    let (mut bits, mut pending) = (0u32, 0u32);
    for byte in text.bytes().take_while(|byte| *byte != b'=') {
        let value = ALPHABET.iter().position(|c| *c == byte)? as u32;
        bits = (bits << 6) | value;
        pending += 6;
        if pending >= 8 {
            pending -= 8;
            out.push((bits >> pending) as u8);
            bits &= (1 << pending) - 1;
        }
    }
    (!out.is_empty()).then_some(out)
}

/// Unpadded, as OpenSSH prints fingerprints.
fn encode_base64(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len().div_ceil(3) * 4);
    let (mut bits, mut pending) = (0u32, 0u32);
    for byte in bytes {
        bits = (bits << 8) | u32::from(*byte);
        pending += 8;
        while pending >= 6 {
            pending -= 6;
            out.push(ALPHABET[((bits >> pending) & 63) as usize] as char);
            bits &= (1 << pending) - 1;
        }
    }
    if pending > 0 {
        out.push(ALPHABET[((bits << (6 - pending)) & 63) as usize] as char);
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    // `ssh-keygen -l -E sha256` over this key prints the fingerprint below.
    const ED25519: &str =
        "ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIJdzxcqK2+fOF2ZZ3aNCGuSURr0hSszSIT5VchcpkUvp test@host
";
    const KNOWN_FINGERPRINT: &str = "SHA256:f6ArUxzhz664MkH7xCFKvF/vB29iSKplA6EHdc/KOuo";

    #[test]
    fn a_public_key_gets_its_openssh_fingerprint() {
        let key = parse_public_key(ED25519).expect("a public key");
        assert_eq!(key.kind, "ed25519");
        assert_eq!(key.sha256, KNOWN_FINGERPRINT);
    }

    #[test]
    fn text_that_is_not_a_public_key_is_skipped() {
        assert_eq!(parse_public_key("not a key"), None);
        assert_eq!(parse_public_key("ssh-ed25519 !!!!"), None);
        assert_eq!(parse_public_key(""), None);
    }

    #[test]
    fn only_pub_files_are_read_and_they_sort_by_kind() {
        let dir = std::env::temp_dir().join(format!("orion-hostkeys-{}", std::process::id()));
        std::fs::create_dir_all(&dir).expect("dir");
        std::fs::write(dir.join("ssh_host_ed25519_key.pub"), ED25519).expect("pub");
        std::fs::write(dir.join("ssh_host_ed25519_key"), "PRIVATE").expect("private");
        std::fs::write(
            dir.join("ssh_host_rsa_key.pub"),
            "ssh-rsa AAAAB3NzaC1yc2E= x\n",
        )
        .expect("rsa");
        let keys = read_ssh_host_keys(&dir);
        assert_eq!(
            keys.iter().map(|key| key.kind.as_str()).collect::<Vec<_>>(),
            ["ed25519", "rsa"]
        );
        assert!(read_ssh_host_keys(&dir.join("missing")).is_empty());
        let _ = std::fs::remove_dir_all(dir);
    }
}
