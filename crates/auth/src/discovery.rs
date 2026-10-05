//! The `_orion._tcp` DNS-SD advertisement and its TXT record (`docs/discovery.md`), shared by
//! `orion-node` (which advertises and browses) and the remote operator client (which browses).
//!
//! ```text
//! v=1            TXT layout version
//! id=<node id>   authoritative node id (the instance name is only a label)
//! pk=<hex>       ed25519 public key, 64 hex characters
//! cp=<u16>       control protocol version
//! cl=<name>      cluster name
//! tcp=<port>     orion+tcp peer listener port (optional)
//! http=<port> | https=<port>   HTTP peer listener port (optional)
//! ```
//!
//! Addresses come from the mDNS A/AAAA records; peer URLs are built from them and the ports.

use crate::{hex::encode_hex, hex::parse_key_hex, peer_tcp::PEER_TCP_SCHEME};
use alloc::{
    borrow::ToOwned,
    format,
    string::{String, ToString},
    vec,
    vec::Vec,
};
use core::net::IpAddr;
use orion_core::{NodeId, PeerBaseUrl};

/// DNS-SD service type advertised and browsed by Orion nodes.
pub const SERVICE_TYPE: &str = "_orion._tcp.local.";
const TXT_VERSION: &str = "1";
/// Longest node id that fits a TXT entry (`id=` plus the id must stay under 255 bytes).
const MAX_NODE_ID_LEN: usize = 200;

/// What a node announces about itself.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Advertisement {
    pub node_id: NodeId,
    pub cluster: String,
    pub public_key: [u8; 32],
    pub control_protocol_version: u16,
    pub peer_tcp_port: Option<u16>,
    pub http_port: Option<u16>,
    pub https: bool,
}

impl Advertisement {
    /// TXT properties in announcement order.
    pub fn txt(&self) -> Vec<(String, String)> {
        let mut txt = vec![
            ("v".to_owned(), TXT_VERSION.to_owned()),
            ("id".to_owned(), self.node_id.to_string()),
            ("pk".to_owned(), encode_hex(&self.public_key)),
            ("cp".to_owned(), self.control_protocol_version.to_string()),
            ("cl".to_owned(), self.cluster.clone()),
        ];
        if let Some(port) = self.peer_tcp_port {
            txt.push(("tcp".to_owned(), port.to_string()));
        }
        if let Some(port) = self.http_port {
            let key = if self.https { "https" } else { "http" };
            txt.push((key.to_owned(), port.to_string()));
        }
        txt
    }

    /// The SRV port: the `orion+tcp` listener, else the HTTP listener.
    pub fn srv_port(&self) -> u16 {
        self.peer_tcp_port.or(self.http_port).unwrap_or(0)
    }

    /// Parses TXT properties. Keys are case-insensitive; the first occurrence wins (RFC 6763).
    pub fn from_txt(txt: &[(String, String)]) -> Result<Self, String> {
        let get = |key: &str| {
            txt.iter()
                .find(|(name, _)| name.eq_ignore_ascii_case(key))
                .map(|(_, value)| value.as_str())
        };
        let required = |key: &str| get(key).ok_or_else(|| format!("TXT record lacks `{key}`"));
        let port = |key: &str| {
            get(key)
                .map(|value| {
                    value
                        .parse::<u16>()
                        .ok()
                        .filter(|port| *port != 0)
                        .ok_or_else(|| format!("TXT `{key}` is not a port: {value}"))
                })
                .transpose()
        };
        if required("v")? != TXT_VERSION {
            return Err(format!("unsupported TXT layout version {}", required("v")?));
        }
        let node_id = required("id")?;
        if node_id.len() > MAX_NODE_ID_LEN {
            return Err("TXT `id` is too long".into());
        }
        let node_id = NodeId::try_new(node_id).map_err(|err| err.to_string())?;
        let public_key = parse_key_hex(required("pk")?)?;
        let control_protocol_version = required("cp")?
            .parse::<u16>()
            .map_err(|_| "TXT `cp` is not a protocol version".to_owned())?;
        let cluster = required("cl")?.to_owned();
        let (http_port, https) = match (port("https")?, port("http")?) {
            (Some(port), _) => (Some(port), true),
            (None, Some(port)) => (Some(port), false),
            (None, None) => (None, false),
        };
        Ok(Self {
            node_id,
            cluster,
            public_key,
            control_protocol_version,
            peer_tcp_port: port("tcp")?,
            http_port,
            https,
        })
    }

    /// Peer URLs for the given addresses: `orion+tcp://` first, then `http(s)://`; IPv4 before
    /// IPv6. Unspecified, multicast and IPv6 link-local addresses (which need a scope) are skipped.
    pub fn peer_urls(&self, addresses: &[IpAddr]) -> Vec<PeerBaseUrl> {
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
        let http_scheme = if self.https { "https" } else { "http" };
        let schemes = [
            self.peer_tcp_port.map(|port| (PEER_TCP_SCHEME, port)),
            self.http_port.map(|port| (http_scheme, port)),
        ];
        schemes
            .into_iter()
            .flatten()
            .flat_map(|(scheme, port)| {
                usable.iter().map(move |ip| match ip {
                    IpAddr::V4(v4) => PeerBaseUrl::new(format!("{scheme}://{v4}:{port}")),
                    IpAddr::V6(v6) => PeerBaseUrl::new(format!("{scheme}://[{v6}]:{port}")),
                })
            })
            .collect()
    }
}
