//! Robot-link peer trust (`docs/discovery.md`, "Robot links: trust without a shared key").
//!
//! In open mode (`ORION_NODE_PEER_AUTH=disabled`) a peer is trusted without a shared key only when
//! its connection comes from a subnet the robot declares as its own link, and its key is pinned on
//! first use. Everything else, including every `optional` and `required` setting, is decided by
//! the rules in `auth.rs`, unchanged.
//!
//! The hello exchange does not call [`NodeSecurity::trust_robot_link_peer`] yet (the next step in
//! `docs/discovery.md`), so outside tests the decision and pinning code is unused.
#![cfg_attr(not(test), allow(dead_code))]

use super::NodeSecurity;
use super::modes::PeerAuthenticationMode;
use crate::NodeError;
use crate::lock::{read_rwlock, write_rwlock};
use orion::NodeId;
use std::net::IpAddr;

/// `ORION_NODE_PEER_AUTO_TRUST`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PeerAutoTrust {
    Off,
    RobotLink,
}

/// A network prefix (`192.0.2.0/24`, `fd00::/64`), or one address (`192.0.2.7`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Subnet {
    addr: IpAddr,
    prefix: u8,
}

impl Subnet {
    pub fn parse(text: &str) -> Result<Self, NodeError> {
        let invalid = |why: &str| {
            NodeError::Config(format!(
                "ORION_NODE_ROBOT_LINKS entry `{text}` is not a valid subnet: {why}"
            ))
        };
        let (addr_text, prefix_text) = match text.split_once('/') {
            Some((addr, prefix)) => (addr, Some(prefix)),
            None => (text, None),
        };
        let addr: IpAddr = addr_text
            .parse()
            .map_err(|err: std::net::AddrParseError| invalid(&err.to_string()))?;
        let max = if addr.is_ipv4() { 32 } else { 128 };
        let prefix = match prefix_text {
            None => max,
            Some(prefix) => prefix
                .parse::<u8>()
                .ok()
                .filter(|prefix| *prefix <= max)
                .ok_or_else(|| invalid(&format!("prefix must be 0 to {max}")))?,
        };
        Ok(Self { addr, prefix })
    }

    /// Whether `ip` is inside this subnet. An IPv4-mapped IPv6 address matches its IPv4 subnet.
    pub fn contains(&self, ip: IpAddr) -> bool {
        let ip = ip.to_canonical();
        if self.addr.is_ipv4() != ip.is_ipv4() {
            return false;
        }
        let (net, ip) = (octets(self.addr), octets(ip));
        let full = usize::from(self.prefix / 8);
        if net[..full] != ip[..full] {
            return false;
        }
        let rest = self.prefix % 8;
        rest == 0 || {
            let mask = 0xff_u8 << (8 - rest);
            (net[full] & mask) == (ip[full] & mask)
        }
    }
}

fn octets(ip: IpAddr) -> Vec<u8> {
    match ip {
        IpAddr::V4(addr) => addr.octets().to_vec(),
        IpAddr::V6(addr) => addr.octets().to_vec(),
    }
}

/// The robot's declared links and whether robot-link trust is on.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RobotLinkTrust {
    pub mode: PeerAutoTrust,
    pub links: Vec<Subnet>,
}

impl Default for RobotLinkTrust {
    fn default() -> Self {
        Self {
            mode: PeerAutoTrust::Off,
            links: Vec::new(),
        }
    }
}

impl RobotLinkTrust {
    /// Reads `ORION_NODE_ROBOT_LINKS` and `ORION_NODE_PEER_AUTO_TRUST` (see
    /// [`Self::from_settings`]).
    pub fn try_from_env(peer_authentication: PeerAuthenticationMode) -> Result<Self, NodeError> {
        let read = |key: &str| match std::env::var(key) {
            Ok(value) => Ok(Some(value)),
            Err(std::env::VarError::NotPresent) => Ok(None),
            Err(std::env::VarError::NotUnicode(_)) => {
                Err(NodeError::Config(format!("{key} must be valid unicode")))
            }
        };
        Self::from_settings(
            peer_authentication,
            read("ORION_NODE_PEER_AUTO_TRUST")?.as_deref(),
            read("ORION_NODE_ROBOT_LINKS")?.as_deref(),
        )
    }

    /// Builds the policy from the raw settings. Auto-trust defaults to `robot-link` in open mode
    /// and to `off` otherwise. `robot-link` without any link is off when it is the default, and
    /// an error when it was asked for explicitly.
    pub fn from_settings(
        peer_authentication: PeerAuthenticationMode,
        auto_trust: Option<&str>,
        links: Option<&str>,
    ) -> Result<Self, NodeError> {
        let links = links
            .unwrap_or("")
            .split(',')
            .map(str::trim)
            .filter(|entry| !entry.is_empty())
            .map(Subnet::parse)
            .collect::<Result<Vec<_>, _>>()?;
        let default = if peer_authentication == PeerAuthenticationMode::Disabled {
            "robot-link"
        } else {
            "off"
        };
        let choices = [
            ("off", PeerAutoTrust::Off),
            ("robot-link", PeerAutoTrust::RobotLink),
        ];
        let mode = match auto_trust {
            Some(value) => parse_choice_value(value, &choices)?,
            None => parse_choice_value(default, &choices)?,
        };
        if mode == PeerAutoTrust::RobotLink && links.is_empty() {
            if auto_trust.is_some() {
                return Err(NodeError::Config(
                    "ORION_NODE_PEER_AUTO_TRUST=robot-link needs ORION_NODE_ROBOT_LINKS".into(),
                ));
            }
            return Ok(Self::default());
        }
        Ok(Self { mode, links })
    }

    fn on_a_link(&self, remote: IpAddr) -> bool {
        self.links.iter().any(|link| link.contains(remote))
    }
}

fn parse_choice_value<T: Copy>(value: &str, choices: &[(&str, T)]) -> Result<T, NodeError> {
    let normalized = value.trim().to_ascii_lowercase();
    choices
        .iter()
        .find_map(|(name, choice)| (*name == normalized).then_some(*choice))
        .ok_or_else(|| {
            NodeError::Config(format!(
                "ORION_NODE_PEER_AUTO_TRUST must be off or robot-link: {value}"
            ))
        })
}

/// Why a presented key is or is not trusted through the robot link.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum RobotLinkDecision {
    Trust,
    RefusedSecurityOn,
    RefusedAutoTrustOff,
    RefusedOffLink,
    RefusedKeyMismatch,
}

/// The rule of `docs/discovery.md`, "Robot links": open mode, auto-trust on, a connection from a
/// robot link, and the presented key is the pinned one or none is pinned yet.
pub(crate) fn decide(
    mode: PeerAuthenticationMode,
    policy: &RobotLinkTrust,
    remote: Option<IpAddr>,
    pinned: Option<&[u8; 32]>,
    presented: &[u8; 32],
) -> RobotLinkDecision {
    if mode != PeerAuthenticationMode::Disabled {
        return RobotLinkDecision::RefusedSecurityOn;
    }
    if policy.mode == PeerAutoTrust::Off {
        return RobotLinkDecision::RefusedAutoTrustOff;
    }
    match remote {
        Some(remote) if policy.on_a_link(remote) => {}
        _ => return RobotLinkDecision::RefusedOffLink,
    }
    match pinned {
        Some(pinned) if pinned != presented => RobotLinkDecision::RefusedKeyMismatch,
        _ => RobotLinkDecision::Trust,
    }
}

impl NodeSecurity {
    /// Sets the robot-link policy (`ORION_NODE_ROBOT_LINKS`, `ORION_NODE_PEER_AUTO_TRUST`).
    pub fn set_robot_link_trust(&self, trust: RobotLinkTrust) {
        *write_rwlock(self.robot_link.write(), "robot_link") = trust;
    }

    /// Trusts `node_id` with `public_key` if the robot-link rule allows it, pinning the key on first
    /// use. `remote` is the address the connection comes from. Errors refuse the peer.
    pub(crate) fn trust_robot_link_peer(
        &self,
        node_id: &NodeId,
        remote: Option<IpAddr>,
        public_key: [u8; 32],
    ) -> Result<(), NodeError> {
        self.ensure_peer_is_not_revoked(node_id)?;
        let policy = read_rwlock(self.robot_link.read(), "robot_link").clone();
        let pinned = read_rwlock(self.trusted_peer_state.read(), "trusted_peer_state")
            .peers
            .get(node_id)
            .copied();
        match decide(self.mode, &policy, remote, pinned.as_ref(), &public_key) {
            RobotLinkDecision::Trust => {
                if pinned.is_none() {
                    self.pin_or_validate_trusted_peer(node_id, public_key)?;
                }
                Ok(())
            }
            RobotLinkDecision::RefusedSecurityOn => Err(NodeError::Authorization(format!(
                "peer {node_id}: robot-link trust applies only in open mode (ORION_NODE_PEER_AUTH=disabled)"
            ))),
            RobotLinkDecision::RefusedAutoTrustOff => Err(NodeError::Authorization(format!(
                "peer {node_id}: robot-link trust is off (ORION_NODE_PEER_AUTO_TRUST)"
            ))),
            RobotLinkDecision::RefusedOffLink => Err(NodeError::Authorization(format!(
                "peer {node_id}: not on a robot link (ORION_NODE_ROBOT_LINKS)"
            ))),
            RobotLinkDecision::RefusedKeyMismatch => Err(NodeError::Authentication(format!(
                "peer {node_id} presented a key other than the one pinned on its robot link"
            ))),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::auth::PeerAuthenticationMode::{Disabled, Optional, Required};

    const KEY_A: [u8; 32] = [1; 32];
    const KEY_B: [u8; 32] = [2; 32];

    fn open_node_with_link(link: &str) -> NodeSecurity {
        let security = NodeSecurity::load_or_create(NodeId::new("node-a"), Disabled, &[], None, 16)
            .expect("security should build without storage");
        security.set_robot_link_trust(
            RobotLinkTrust::from_settings(Disabled, Some("robot-link"), Some(link))
                .expect("robot link should parse"),
        );
        security
    }

    fn ip(text: &str) -> Option<IpAddr> {
        Some(text.parse().expect("address should parse"))
    }

    fn pinned_key(security: &NodeSecurity, node_id: &str) -> Option<[u8; 32]> {
        read_rwlock(security.trusted_peer_state.read(), "trusted_peer_state")
            .peers
            .get(&NodeId::new(node_id))
            .copied()
    }

    #[test]
    fn subnets_match_their_prefix_and_families() {
        let v4 = Subnet::parse("192.0.2.0/24").expect("subnet should parse");
        assert!(v4.contains(ip("192.0.2.77").unwrap()));
        assert!(!v4.contains(ip("192.0.3.1").unwrap()));
        assert!(!v4.contains(ip("2001:db8::1").unwrap()));
        assert!(
            v4.contains(ip("::ffff:192.0.2.9").unwrap()),
            "mapped addresses match IPv4"
        );

        let host = Subnet::parse("198.51.100.7").expect("single address should parse");
        assert!(host.contains(ip("198.51.100.7").unwrap()));
        assert!(!host.contains(ip("198.51.100.8").unwrap()));

        let v6 = Subnet::parse("fd00:7::/64").expect("v6 subnet should parse");
        assert!(v6.contains(ip("fd00:7::42").unwrap()));
        assert!(!v6.contains(ip("fd00:8::42").unwrap()));

        let odd = Subnet::parse("10.0.0.0/12").expect("odd prefix should parse");
        assert!(odd.contains(ip("10.15.1.1").unwrap()));
        assert!(!odd.contains(ip("10.16.0.1").unwrap()));

        assert!(Subnet::parse("192.0.2.0/33").is_err());
        assert!(Subnet::parse("not-an-address").is_err());
    }

    #[test]
    fn settings_default_on_only_in_open_mode_and_need_a_link() {
        let open = RobotLinkTrust::from_settings(Disabled, None, Some("192.0.2.0/24"))
            .expect("defaults should parse");
        assert_eq!(open.mode, PeerAutoTrust::RobotLink);
        assert_eq!(open.links.len(), 1);

        let secured = RobotLinkTrust::from_settings(Optional, None, Some("192.0.2.0/24"))
            .expect("defaults should parse");
        assert_eq!(secured.mode, PeerAutoTrust::Off, "security on keeps it off");

        let no_link = RobotLinkTrust::from_settings(Disabled, None, None)
            .expect("default without links is off, not an error");
        assert_eq!(no_link, RobotLinkTrust::default());

        assert!(
            RobotLinkTrust::from_settings(Disabled, Some("robot-link"), None).is_err(),
            "asking for robot-link without a link fails startup"
        );
        assert!(RobotLinkTrust::from_settings(Disabled, Some("sometimes"), None).is_err());
    }

    #[test]
    fn auto_trust_pins_the_first_key_on_a_robot_link() {
        let security = open_node_with_link("192.0.2.0/24");
        security
            .trust_robot_link_peer(&NodeId::new("node-b"), ip("192.0.2.20"), KEY_A)
            .expect("first use on the robot link should be trusted");
        assert_eq!(pinned_key(&security, "node-b"), Some(KEY_A));
        security
            .trust_robot_link_peer(&NodeId::new("node-b"), ip("192.0.2.20"), KEY_A)
            .expect("the same key again is still trusted");
    }

    #[test]
    fn auto_trust_refuses_a_peer_off_the_link() {
        let security = open_node_with_link("192.0.2.0/24");
        let err = security
            .trust_robot_link_peer(&NodeId::new("node-b"), ip("203.0.113.5"), KEY_A)
            .expect_err("a peer off the link is never auto-trusted");
        assert!(err.to_string().contains("not on a robot link"), "{err}");
        assert_eq!(pinned_key(&security, "node-b"), None);

        let unknown_source = security
            .trust_robot_link_peer(&NodeId::new("node-b"), None, KEY_A)
            .expect_err("a connection without a source address is off the link");
        assert!(unknown_source.to_string().contains("not on a robot link"));
    }

    #[test]
    fn auto_trust_is_refused_with_security_on() {
        for mode in [Optional, Required] {
            let security = NodeSecurity::load_or_create(NodeId::new("node-a"), mode, &[], None, 16)
                .expect("security should build without storage");
            security.set_robot_link_trust(
                RobotLinkTrust::from_settings(Disabled, Some("robot-link"), Some("192.0.2.0/24"))
                    .expect("robot link should parse"),
            );
            let err = security
                .trust_robot_link_peer(&NodeId::new("node-b"), ip("192.0.2.20"), KEY_A)
                .expect_err("security on: robot-link trust never applies");
            assert!(err.to_string().contains("open mode"), "{err}");
            assert_eq!(pinned_key(&security, "node-b"), None);
        }
    }

    #[test]
    fn auto_trust_refuses_a_changed_key_for_a_pinned_peer() {
        let security = open_node_with_link("192.0.2.0/24");
        security
            .trust_robot_link_peer(&NodeId::new("node-b"), ip("192.0.2.20"), KEY_A)
            .expect("first use should be trusted");
        let err = security
            .trust_robot_link_peer(&NodeId::new("node-b"), ip("192.0.2.20"), KEY_B)
            .expect_err("a different key for a pinned peer is refused, even on the link");
        assert!(err.to_string().contains("pinned"), "{err}");
        assert_eq!(
            pinned_key(&security, "node-b"),
            Some(KEY_A),
            "the pin is kept"
        );
    }

    #[test]
    fn auto_trust_off_by_default_refuses_everything() {
        let security = NodeSecurity::load_or_create(NodeId::new("node-a"), Disabled, &[], None, 16)
            .expect("security should build without storage");
        let err = security
            .trust_robot_link_peer(&NodeId::new("node-b"), ip("192.0.2.20"), KEY_A)
            .expect_err("without a robot link configured nothing is auto-trusted");
        assert!(err.to_string().contains("off"), "{err}");
    }
}
