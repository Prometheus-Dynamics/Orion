//! Lowercase hex for ed25519 keys and fingerprints.

use alloc::{borrow::ToOwned, string::String};

const DIGITS: &[u8; 16] = b"0123456789abcdef";

/// Lowercase hex of `bytes`.
pub fn encode_hex(bytes: &[u8]) -> String {
    let mut output = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        output.push(char::from(DIGITS[usize::from(byte >> 4)]));
        output.push(char::from(DIGITS[usize::from(byte & 0x0f)]));
    }
    output
}

/// Parses a 32-byte ed25519 public key written as 64 hex characters (surrounding whitespace is
/// ignored, either case is accepted).
pub fn parse_key_hex(value: &str) -> Result<[u8; 32], String> {
    let value = value.trim();
    let invalid = || "public key must be 64 hex characters".to_owned();
    if value.len() != 64 || !value.is_ascii() {
        return Err(invalid());
    }
    let mut key = [0u8; 32];
    for (index, byte) in key.iter_mut().enumerate() {
        *byte = u8::from_str_radix(&value[index * 2..index * 2 + 2], 16).map_err(|_| invalid())?;
    }
    Ok(key)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn keys_roundtrip_through_hex() {
        let key = [0xab; 32];
        let hex = encode_hex(&key);
        assert_eq!(hex.len(), 64);
        assert!(hex.starts_with("abab"));
        assert_eq!(parse_key_hex(&hex), Ok(key));
        assert_eq!(parse_key_hex(&hex.to_uppercase()), Ok(key));
        assert!(parse_key_hex("abcd").is_err());
        assert!(parse_key_hex(&"zz".repeat(32)).is_err());
    }
}
