//! `ORION_NODE_LINKS` parsing.
//!
//! Syntax: a `;`-separated list of `<kind>:<target>[?key=value&key=value...]` entries, for example
//! `serial:/dev/ttyAMA0?baud=115200&allow=imu-board` or
//! `can:can0?device_base=0x600&host_base=0x680&addresses=1-16&fd=false`. Every key is documented
//! in docs/node-env.md ("Link gateway").

use crate::NodeError;
use orion_link::CanLinkIds;
use orion_link::host::{HOST_MAX_FRAME, HostConfig};
use orion_link::message::NodeId;
use std::collections::BTreeSet;
use std::ops::RangeInclusive;
use std::path::PathBuf;

/// Environment variable holding the link list.
pub const LINKS_ENV: &str = "ORION_NODE_LINKS";

/// Default serial baud rate.
pub const DEFAULT_BAUD: u32 = 115_200;
/// Default heartbeat interval announced to devices.
pub const DEFAULT_HEARTBEAT_MS: u32 = 1_000;
/// Default number of missed heartbeats before a device is lost.
pub const DEFAULT_MISSED_HEARTBEATS: u32 = 3;
/// Default CAN device→host base identifier.
pub const DEFAULT_CAN_DEVICE_BASE: u32 = 0x600;
/// Default CAN host→device base identifier.
pub const DEFAULT_CAN_HOST_BASE: u32 = 0x680;
/// Default CAN device address range.
pub const DEFAULT_CAN_ADDRESSES: RangeInclusive<u32> = 1..=16;
/// Most device addresses one CAN link may serve (one kernel receive filter per address).
pub const MAX_CAN_ADDRESSES: u32 = 512;

const MIN_HEARTBEAT_MS: u32 = 10;
const MAX_HEARTBEAT_MS: u32 = 600_000;
const MIN_MAX_FRAME: u32 = 32;

/// Serial baud rates the gateway can program (termios `B*` constants).
pub const SUPPORTED_BAUD_RATES: &[u32] = &[
    1_200, 2_400, 4_800, 9_600, 19_200, 38_400, 57_600, 115_200, 230_400, 460_800, 500_000,
    576_000, 921_600, 1_000_000, 1_152_000, 1_500_000, 2_000_000, 2_500_000, 3_000_000, 3_500_000,
    4_000_000,
];

/// One configured link.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LinkConfig {
    /// `<kind>:<target>`, used in logs and status.
    pub name: String,
    /// Physical link.
    pub transport: LinkTransport,
    /// If set, only these device names are accepted (`allow=`).
    pub allowed_devices: Option<BTreeSet<String>>,
    /// Heartbeat interval announced in `Welcome` (`heartbeat_ms=`).
    pub heartbeat_ms: u32,
    /// Heartbeats without traffic before a device is lost (`missed_heartbeats=`).
    pub missed_heartbeats: u32,
    /// Host frame limit (`max_frame=`), at most [`HOST_MAX_FRAME`].
    pub max_frame: u32,
}

/// The physical side of a [`LinkConfig`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum LinkTransport {
    /// A serial port (UART, RS-485 point-to-point, USB-CDC), COBS framed.
    Serial(SerialLinkConfig),
    /// A SocketCAN interface shared by several devices.
    Can(CanLinkConfig),
}

/// `serial:<path>` settings.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SerialLinkConfig {
    /// Device path, e.g. `/dev/ttyAMA0`.
    pub path: PathBuf,
    /// Baud rate (`baud=`), one of [`SUPPORTED_BAUD_RATES`]. Always 8N1, no flow control.
    pub baud: u32,
}

/// `can:<interface>` settings.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CanLinkConfig {
    /// Interface name, e.g. `can0`.
    pub interface: String,
    /// Device→host base identifier (`device_base=`); a device uses `device_base + address`.
    pub device_base: u32,
    /// Host→device base identifier (`host_base=`).
    pub host_base: u32,
    /// Device addresses served (`addresses=N` or `A-B`).
    pub addresses: RangeInclusive<u32>,
    /// CAN FD (`fd=`): 64-byte frames instead of classic 8-byte frames.
    pub fd: bool,
    /// 29-bit identifiers (`extended=`).
    pub extended: bool,
}

impl CanLinkConfig {
    /// Base identifier pair.
    #[must_use]
    pub fn base_ids(&self) -> CanLinkIds {
        CanLinkIds::new(self.device_base, self.host_base, self.extended)
    }

    /// Every device→host identifier this link listens to.
    pub fn device_ids(&self) -> impl Iterator<Item = u32> + '_ {
        self.addresses
            .clone()
            .filter_map(|address| CanLinkIds::for_address(self.base_ids(), address))
            .map(|ids| ids.device_to_host)
    }
}

impl LinkConfig {
    /// Reads [`LINKS_ENV`]. Unset or blank means no links.
    pub fn try_from_env() -> Result<Vec<Self>, NodeError> {
        match std::env::var(LINKS_ENV) {
            Ok(raw) => Self::parse_list(&raw),
            Err(std::env::VarError::NotPresent) => Ok(Vec::new()),
            Err(std::env::VarError::NotUnicode(_)) => Err(NodeError::Config(format!(
                "{LINKS_ENV} must be valid unicode"
            ))),
        }
    }

    /// Parses a `;`-separated link list. Empty entries are ignored.
    pub fn parse_list(raw: &str) -> Result<Vec<Self>, NodeError> {
        let mut links: Vec<Self> = Vec::new();
        for entry in raw.split(';').map(str::trim).filter(|e| !e.is_empty()) {
            let link = Self::parse(entry)?;
            if links.iter().any(|other| other.name == link.name) {
                return Err(link_error(entry, "the same link is configured twice"));
            }
            links.push(link);
        }
        Ok(links)
    }

    /// Parses one `<kind>:<target>[?query]` entry.
    pub fn parse(entry: &str) -> Result<Self, NodeError> {
        let (kind, rest) = entry
            .split_once(':')
            .ok_or_else(|| link_error(entry, "expected `serial:<path>` or `can:<interface>`"))?;
        let (target, query) = rest.split_once('?').unwrap_or((rest, ""));
        let target = target.trim();
        if target.is_empty() {
            return Err(link_error(entry, "the link target is empty"));
        }
        let mut params = Params::parse(entry, query)?;
        let allowed_devices = params.take("allow").map(parse_allow).transpose();
        let allowed_devices = allowed_devices.map_err(|msg| link_error(entry, msg))?;
        let heartbeat_ms = params.number("heartbeat_ms", DEFAULT_HEARTBEAT_MS)?;
        if !(MIN_HEARTBEAT_MS..=MAX_HEARTBEAT_MS).contains(&heartbeat_ms) {
            return Err(link_error(
                entry,
                format!("heartbeat_ms must be between {MIN_HEARTBEAT_MS} and {MAX_HEARTBEAT_MS}"),
            ));
        }
        let missed_heartbeats = params.number("missed_heartbeats", DEFAULT_MISSED_HEARTBEATS)?;
        if missed_heartbeats == 0 {
            return Err(link_error(entry, "missed_heartbeats must be at least 1"));
        }
        let max_frame = params.number("max_frame", HOST_MAX_FRAME as u32)?;
        if !(MIN_MAX_FRAME..=HOST_MAX_FRAME as u32).contains(&max_frame) {
            return Err(link_error(
                entry,
                format!("max_frame must be between {MIN_MAX_FRAME} and {HOST_MAX_FRAME}"),
            ));
        }
        let transport = match kind.trim().to_ascii_lowercase().as_str() {
            "serial" => {
                let baud = params.number("baud", DEFAULT_BAUD)?;
                if !SUPPORTED_BAUD_RATES.contains(&baud) {
                    return Err(link_error(
                        entry,
                        format!("unsupported baud {baud}; supported: {SUPPORTED_BAUD_RATES:?}"),
                    ));
                }
                LinkTransport::Serial(SerialLinkConfig {
                    path: PathBuf::from(target),
                    baud,
                })
            }
            "can" => LinkTransport::Can(parse_can(entry, target, &mut params)?),
            other => {
                return Err(link_error(
                    entry,
                    format!("unknown link kind `{other}`; expected `serial` or `can`"),
                ));
            }
        };
        params.finish()?;
        Ok(Self {
            name: format!("{}:{target}", kind.trim().to_ascii_lowercase()),
            transport,
            allowed_devices,
            heartbeat_ms,
            missed_heartbeats,
            max_frame,
        })
    }

    /// The host session settings for this link.
    #[must_use]
    pub fn host_config(&self, node_id: NodeId, session_seed: u32) -> HostConfig {
        let mut config = HostConfig::new(node_id);
        config.heartbeat_ms = self.heartbeat_ms;
        config.missed_heartbeats = self.missed_heartbeats;
        config.max_frame = self.max_frame;
        config.allowed_devices = self.allowed_devices.clone();
        config.session_seed = session_seed;
        config
    }
}

fn parse_can(
    entry: &str,
    interface: &str,
    params: &mut Params<'_>,
) -> Result<CanLinkConfig, NodeError> {
    if interface.len() >= 16 || interface.contains('/') {
        return Err(link_error(entry, "invalid CAN interface name"));
    }
    let device_base = params.number("device_base", DEFAULT_CAN_DEVICE_BASE)?;
    let host_base = params.number("host_base", DEFAULT_CAN_HOST_BASE)?;
    let addresses = match params.take("addresses") {
        Some(raw) => parse_range(raw).map_err(|msg| link_error(entry, msg))?,
        None => DEFAULT_CAN_ADDRESSES,
    };
    let fd = params.flag("fd")?;
    let extended = params.flag("extended")?;
    let config = CanLinkConfig {
        interface: interface.to_owned(),
        device_base,
        host_base,
        addresses,
        fd,
        extended,
    };
    let (start, end) = (*config.addresses.start(), *config.addresses.end());
    if end - start >= MAX_CAN_ADDRESSES {
        return Err(link_error(
            entry,
            format!("addresses may span at most {MAX_CAN_ADDRESSES} devices"),
        ));
    }
    let base = config.base_ids();
    for address in [start, end] {
        if CanLinkIds::for_address(base, address).is_none() {
            return Err(link_error(
                entry,
                format!(
                    "address {address} leaves the {} identifier range (max {:#x}) or makes the device and host identifiers equal",
                    if extended { "29-bit" } else { "11-bit" },
                    base.max_id()
                ),
            ));
        }
    }
    let device = (device_base + start)..=(device_base + end);
    let host = (host_base + start)..=(host_base + end);
    if device.start() <= host.end() && host.start() <= device.end() {
        return Err(link_error(
            entry,
            format!(
                "device identifiers {:#x}-{:#x} overlap host identifiers {:#x}-{:#x}",
                device.start(),
                device.end(),
                host.start(),
                host.end()
            ),
        ));
    }
    Ok(config)
}

fn parse_allow(raw: &str) -> Result<BTreeSet<String>, String> {
    let names: BTreeSet<String> = raw
        .split(',')
        .map(str::trim)
        .filter(|name| !name.is_empty())
        .map(str::to_owned)
        .collect();
    if names.is_empty() {
        return Err("allow must list at least one device name".into());
    }
    Ok(names)
}

fn parse_range(raw: &str) -> Result<RangeInclusive<u32>, String> {
    let (start, end) = match raw.split_once('-') {
        Some((start, end)) => (parse_u32(start)?, parse_u32(end)?),
        None => {
            let value = parse_u32(raw)?;
            (value, value)
        }
    };
    if start > end {
        return Err(format!("addresses `{raw}` is an empty range"));
    }
    Ok(start..=end)
}

fn parse_u32(raw: &str) -> Result<u32, String> {
    let raw = raw.trim();
    let parsed = match raw.strip_prefix("0x").or_else(|| raw.strip_prefix("0X")) {
        Some(hex) => u32::from_str_radix(hex, 16),
        None => raw.parse::<u32>(),
    };
    parsed.map_err(|_| format!("`{raw}` is not a valid unsigned integer"))
}

fn link_error(entry: &str, message: impl std::fmt::Display) -> NodeError {
    NodeError::Config(format!("invalid {LINKS_ENV} entry `{entry}`: {message}"))
}

/// `key=value` pairs of one entry; every key must be consumed.
struct Params<'a> {
    entry: &'a str,
    pairs: Vec<(&'a str, &'a str)>,
}

impl<'a> Params<'a> {
    fn parse(entry: &'a str, query: &'a str) -> Result<Self, NodeError> {
        let mut pairs: Vec<(&str, &str)> = Vec::new();
        for pair in query.split('&').map(str::trim).filter(|p| !p.is_empty()) {
            let (key, value) = pair
                .split_once('=')
                .ok_or_else(|| link_error(entry, format!("`{pair}` is not key=value")))?;
            let key = key.trim();
            if pairs.iter().any(|(k, _)| *k == key) {
                return Err(link_error(entry, format!("`{key}` is given twice")));
            }
            pairs.push((key, value.trim()));
        }
        Ok(Self { entry, pairs })
    }

    fn take(&mut self, key: &str) -> Option<&'a str> {
        let index = self.pairs.iter().position(|(k, _)| *k == key)?;
        Some(self.pairs.remove(index).1)
    }

    fn number(&mut self, key: &str, default: u32) -> Result<u32, NodeError> {
        match self.take(key) {
            Some(raw) => {
                parse_u32(raw).map_err(|msg| link_error(self.entry, format!("{key}: {msg}")))
            }
            None => Ok(default),
        }
    }

    fn flag(&mut self, key: &str) -> Result<bool, NodeError> {
        match self.take(key).map(str::to_ascii_lowercase).as_deref() {
            None | Some("false" | "0" | "no" | "off") => Ok(false),
            Some("true" | "1" | "yes" | "on") => Ok(true),
            Some(other) => Err(link_error(
                self.entry,
                format!("{key} must be true or false, not `{other}`"),
            )),
        }
    }

    fn finish(self) -> Result<(), NodeError> {
        match self.pairs.first() {
            None => Ok(()),
            Some((key, _)) => Err(link_error(
                self.entry,
                format!("unknown or inapplicable key `{key}`"),
            )),
        }
    }
}
