//! What a node announces about itself: the `_orion._tcp` service and its TXT record.
//!
//! ```text
//! v=2            TXT layout version
//! id=<node>      the node's name (the instance name is only a label)
//! pk=<hex>       ed25519 public key, 64 hex characters
//! wv=<u8>        wire protocol version
//! rev=<u16>      highest interface revision
//! cl=<name>      cluster name
//! tcp=<port>     where the node takes `orion+tcp` connections
//! ```
//!
//! Addresses come from the announcement's address records.

use std::net::{IpAddr, SocketAddr};

use orion_types::NodeName;

/// The DNS-SD service type nodes announce and browse.
pub const SERVICE_TYPE: &str = "_orion._tcp.local.";
const TXT_VERSION: &str = "2";
/// The longest name that fits a TXT entry.
const MAX_NAME_LEN: usize = 200;

/// What a node announces about itself.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Advertisement {
    /// The node's name.
    pub node: NodeName,
    /// The cluster it belongs to.
    pub cluster: String,
    /// Its ed25519 public key.
    pub public_key: [u8; 32],
    /// The wire protocol version it speaks.
    pub wire: u8,
    /// The highest interface revision it speaks.
    pub revision: u16,
    /// Where it takes `orion+tcp` connections.
    pub port: u16,
}

/// `bytes` as lowercase hex.
#[must_use]
pub fn hex(bytes: &[u8]) -> String {
    use std::fmt::Write as _;
    bytes
        .iter()
        .fold(String::with_capacity(bytes.len() * 2), |mut out, byte| {
            let _ = write!(out, "{byte:02x}");
            out
        })
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

impl Advertisement {
    /// The TXT properties, in announcement order.
    #[must_use]
    pub fn txt(&self) -> Vec<(String, String)> {
        vec![
            ("v".to_owned(), TXT_VERSION.to_owned()),
            ("id".to_owned(), self.node.to_string()),
            ("pk".to_owned(), hex(&self.public_key)),
            ("wv".to_owned(), self.wire.to_string()),
            ("rev".to_owned(), self.revision.to_string()),
            ("cl".to_owned(), self.cluster.clone()),
            ("tcp".to_owned(), self.port.to_string()),
        ]
    }

    /// Parses TXT properties. Keys are case-insensitive and the first occurrence wins.
    ///
    /// # Errors
    ///
    /// What is missing or wrong, for the log.
    pub fn from_txt(txt: &[(String, String)]) -> Result<Self, String> {
        let get = |key: &str| {
            txt.iter()
                .find(|(name, _)| name.eq_ignore_ascii_case(key))
                .map(|(_, value)| value.as_str())
                .ok_or_else(|| format!("TXT record lacks `{key}`"))
        };
        fn number<T: std::str::FromStr>(key: &str, value: &str) -> Result<T, String> {
            value
                .parse()
                .map_err(|_| format!("TXT `{key}` is not a number: {value}"))
        }
        if get("v")? != TXT_VERSION {
            return Err(format!("unsupported TXT layout version {}", get("v")?));
        }
        let name = get("id")?;
        if name.len() > MAX_NAME_LEN {
            return Err("TXT `id` is too long".into());
        }
        let port: u16 = number("tcp", get("tcp")?)?;
        if port == 0 {
            return Err("TXT `tcp` is not a port".into());
        }
        Ok(Self {
            node: name.parse().map_err(|error| format!("TXT `id`: {error}"))?,
            cluster: get("cl")?.to_owned(),
            public_key: key_from_hex(get("pk")?).ok_or("TXT `pk` is not a key")?,
            wire: number("wv", get("wv")?)?,
            revision: number("rev", get("rev")?)?,
            port,
        })
    }

    /// Where to connect, given the announcement's addresses: IPv4 before IPv6. Unspecified,
    /// multicast and IPv6 link-local addresses (which need a scope) are left out.
    #[must_use]
    pub fn addrs(&self, addresses: &[IpAddr]) -> Vec<SocketAddr> {
        let mut usable: Vec<IpAddr> = addresses
            .iter()
            .copied()
            .filter(|ip| !ip.is_unspecified() && !ip.is_multicast())
            .filter(|ip| match ip {
                IpAddr::V6(v6) => !v6.is_unicast_link_local(),
                IpAddr::V4(_) => true,
            })
            .collect();
        usable.sort_by_key(|ip| (ip.is_ipv6(), *ip));
        usable.dedup();
        usable
            .into_iter()
            .map(|ip| SocketAddr::new(ip, self.port))
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn advert() -> Advertisement {
        Advertisement {
            node: "node-a".parse().expect("name"),
            cluster: "lab".into(),
            public_key: [0xAB; 32],
            wire: 1,
            revision: 3,
            port: 9200,
        }
    }

    #[test]
    fn a_txt_record_reads_back_as_it_was_written() {
        let advert = advert();
        let txt = advert.txt();
        assert_eq!(Advertisement::from_txt(&txt), Ok(advert.clone()));
        // Keys in any case, and entries this build does not know, change nothing.
        let mut shouted: Vec<(String, String)> = txt
            .iter()
            .map(|(k, v)| (k.to_uppercase(), v.clone()))
            .collect();
        shouted.push(("future".into(), "thing".into()));
        assert_eq!(Advertisement::from_txt(&shouted), Ok(advert));
    }

    #[test]
    fn a_record_that_is_not_one_is_refused_with_the_reason() {
        let change = |key: &str, value: Option<&str>| {
            let mut txt = advert().txt();
            txt.retain(|(name, _)| name != key);
            if let Some(value) = value {
                txt.push((key.to_owned(), value.to_owned()));
            }
            Advertisement::from_txt(&txt).expect_err("refused")
        };
        assert!(change("pk", None).contains("lacks `pk`"));
        assert!(change("pk", Some("abcd")).contains("not a key"));
        assert!(change("pk", Some(&"zz".repeat(32))).contains("not a key"));
        assert!(change("v", Some("1")).contains("layout version 1"));
        assert!(change("tcp", Some("0")).contains("not a port"));
        assert!(change("tcp", Some("70000")).contains("not a number"));
        assert!(change("id", Some("not a name!")).contains("TXT `id`"));
        assert!(change("id", Some(&"a".repeat(300))).contains("too long"));
    }

    #[test]
    fn addresses_that_cannot_be_dialed_are_left_out() {
        let addresses: Vec<IpAddr> = [
            "fe80::1",
            "2001:db8::2",
            "192.0.2.9",
            "0.0.0.0",
            "224.0.0.251",
            "192.0.2.9",
            "10.0.0.1",
        ]
        .iter()
        .map(|ip| ip.parse().expect("ip"))
        .collect();
        let addrs: Vec<String> = advert()
            .addrs(&addresses)
            .iter()
            .map(ToString::to_string)
            .collect();
        assert_eq!(
            addrs,
            ["10.0.0.1:9200", "192.0.2.9:9200", "[2001:db8::2]:9200"]
        );
    }
}
