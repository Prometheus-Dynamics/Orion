//! Where a comm core is.

use std::fmt;
use std::path::PathBuf;
use std::str::FromStr;

/// The board's socket, unless told otherwise.
pub const DEFAULT_SOCKET: &str = "/run/orion/orion.sock";
/// The port of `orion+tcp`, unless told otherwise.
pub const DEFAULT_PORT: u16 = 9200;

/// Where a comm core is. The same calls work on all three.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Address {
    /// The socket of a comm core on this board: `/run/orion/orion.sock`, or `unix:<path>`.
    Local(PathBuf),
    /// A comm core on the network: `orion+tcp://host[:port]`, with `#<key in hex>` at the end
    /// to accept only the node that holds that key.
    Network {
        /// `host:port`.
        host: String,
        /// The key the node must prove it holds.
        key: Option<[u8; 32]>,
    },
    /// A comm core at the other end of a serial line: `serial:<path>[@baud][#key]`.
    Serial {
        /// The port.
        path: PathBuf,
        /// Bits per second.
        baud: u32,
        /// The key the board must prove it holds, on a signed session.
        key: Option<[u8; 32]>,
    },
}

impl Default for Address {
    fn default() -> Self {
        Self::Local(PathBuf::from(DEFAULT_SOCKET))
    }
}

fn key_from_hex(text: &str) -> Option<[u8; 32]> {
    if text.len() != 64 || !text.is_ascii() {
        return None;
    }
    let mut key = [0_u8; 32];
    for (byte, pair) in key.iter_mut().zip(text.as_bytes().chunks(2)) {
        *byte = u8::from_str_radix(std::str::from_utf8(pair).ok()?, 16).ok()?;
    }
    Some(key)
}

impl FromStr for Address {
    type Err = String;

    fn from_str(text: &str) -> Result<Self, String> {
        let text = text.trim();
        if let Some(rest) = text.strip_prefix("orion+tcp://") {
            let (host, key) = match rest.split_once('#') {
                Some((host, key)) => (
                    host,
                    Some(key_from_hex(key).ok_or("the key after # is not 64 hex digits")?),
                ),
                None => (rest, None),
            };
            if host.is_empty() {
                return Err("orion+tcp:// needs a host".into());
            }
            // A port is the digits after the last colon, unless that colon is inside an IPv6
            // address in brackets.
            let has_port = host.rsplit_once(':').is_some_and(|(name, port)| {
                !name.ends_with(':') && !port.contains(']') && port.parse::<u16>().is_ok()
            });
            let host = if has_port {
                host.to_owned()
            } else {
                format!("{host}:{DEFAULT_PORT}")
            };
            return Ok(Self::Network { host, key });
        }
        if let Some(rest) = text.strip_prefix("serial:") {
            let (rest, key) = match rest.rsplit_once('#') {
                Some((rest, key)) => (
                    rest,
                    Some(key_from_hex(key).ok_or("the key after # is not 64 hex digits")?),
                ),
                None => (rest, None),
            };
            let (path, baud) = match rest.rsplit_once('@') {
                Some((path, baud)) => (
                    path,
                    baud.parse()
                        .map_err(|_| format!("{baud:?} is not a baud rate"))?,
                ),
                None => (rest, 115_200),
            };
            if path.is_empty() {
                return Err("serial: needs a port".into());
            }
            return Ok(Self::Serial {
                path: PathBuf::from(path),
                baud,
                key,
            });
        }
        let path = text.strip_prefix("unix:").unwrap_or(text);
        if path.starts_with('/') {
            return Ok(Self::Local(PathBuf::from(path)));
        }
        Err(format!(
            "{text:?} is not an address: a socket path, unix:<path>, orion+tcp://host[:port] or serial:<path>[@baud]"
        ))
    }
}

impl fmt::Display for Address {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Local(path) => write!(f, "unix:{}", path.display()),
            Self::Network { host, key: None } => write!(f, "orion+tcp://{host}"),
            Self::Network {
                host,
                key: Some(key),
            } => {
                write!(f, "orion+tcp://{host}#")?;
                key.iter().try_for_each(|byte| write!(f, "{byte:02x}"))
            }
            Self::Serial { path, baud, key } => {
                write!(f, "serial:{}@{baud}", path.display())?;
                if let Some(key) = key {
                    f.write_str("#")?;
                    key.iter().try_for_each(|byte| write!(f, "{byte:02x}"))?;
                }
                Ok(())
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_address_reads_back_as_it_is_written() {
        let key = "ab".repeat(32);
        for (written, shown) in [
            ("/run/orion/orion.sock", "unix:/run/orion/orion.sock"),
            ("unix:/tmp/o.sock", "unix:/tmp/o.sock"),
            ("orion+tcp://board.local", "orion+tcp://board.local:9200"),
            ("orion+tcp://10.0.0.2:9300", "orion+tcp://10.0.0.2:9300"),
            ("orion+tcp://[fe80::1]", "orion+tcp://[fe80::1]:9200"),
            (
                "orion+tcp://[2001:db8::2]:77",
                "orion+tcp://[2001:db8::2]:77",
            ),
            ("serial:/dev/ttyACM0", "serial:/dev/ttyACM0@115200"),
            ("serial:/dev/ttyUSB1@921600", "serial:/dev/ttyUSB1@921600"),
        ] {
            let address: Address = written.parse().expect(written);
            assert_eq!(address.to_string(), shown);
            assert_eq!(shown.parse::<Address>(), Ok(address));
        }
        let pinned: Address = format!("orion+tcp://board:1#{key}")
            .parse()
            .expect("pinned");
        assert_eq!(
            pinned,
            Address::Network {
                host: "board:1".into(),
                key: Some([0xAB; 32])
            }
        );
        assert_eq!(pinned.to_string(), format!("orion+tcp://board:1#{key}"));
        assert_eq!(Address::default().to_string(), "unix:/run/orion/orion.sock");
    }

    #[test]
    fn what_is_not_an_address_says_what_one_is() {
        for wrong in [
            "",
            "board.local",
            "orion+tcp://",
            "orion+tcp://board#abcd",
            "serial:",
            "serial:/dev/x@fast",
            "http://x",
        ] {
            assert!(wrong.parse::<Address>().is_err(), "{wrong:?}");
        }
        assert!(
            "relative/path"
                .parse::<Address>()
                .expect_err("refused")
                .contains("orion+tcp://")
        );
    }
}
