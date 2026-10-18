//! Host facts and action settings (`docs/host-facts.md`, `docs/actions.md`,
//! `docs/node-env.md`).

use super::runtime_tuning::{duration_ms_env_or, normalize_runtime_tuning_duration, parse_env_or};
use crate::NodeError;
use std::{env, path::PathBuf, time::Duration};

const DEFAULT_HOST_FACTS_REFRESH_MS: u64 = 10_000;
const DEFAULT_ACTION_DEFAULT_DEADLINE_MS: u64 = 30_000;
const DEFAULT_ACTION_MAX_DEADLINE_MS: u64 = 600_000;
const DEFAULT_ACTION_RESULT_TTL_MS: u64 = 600_000;
const DEFAULT_ACTION_MAX_TRACKED: usize = 256;
const DEFAULT_ACTION_MAX_WAIT_MS: u64 = 2_000;

/// How the node samples host facts.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct HostFactsTuning {
    /// How often the host-facts source is sampled (`ORION_NODE_HOST_FACTS_REFRESH_MS`);
    /// `Duration::ZERO` turns host facts off.
    pub refresh_interval: Duration,
    /// Files that declare the system image name/version, tried in order
    /// (`ORION_NODE_IMAGE_VERSION_FILE`, `:`-separated). Empty by default.
    pub image_files: Vec<PathBuf>,
}

impl Default for HostFactsTuning {
    fn default() -> Self {
        Self {
            refresh_interval: Duration::from_millis(DEFAULT_HOST_FACTS_REFRESH_MS),
            image_files: Vec::new(),
        }
    }
}

impl HostFactsTuning {
    pub(crate) fn from_env() -> Result<Self, NodeError> {
        let image_files = match env::var("ORION_NODE_IMAGE_VERSION_FILE") {
            Ok(raw) => raw
                .split(':')
                .map(str::trim)
                .filter(|path| !path.is_empty())
                .map(PathBuf::from)
                .collect(),
            Err(env::VarError::NotPresent) => Vec::new(),
            Err(env::VarError::NotUnicode(_)) => {
                return Err(NodeError::Config(
                    "ORION_NODE_IMAGE_VERSION_FILE must be valid unicode".into(),
                ));
            }
        };
        Ok(Self {
            refresh_interval: duration_ms_env_or(
                "ORION_NODE_HOST_FACTS_REFRESH_MS",
                DEFAULT_HOST_FACTS_REFRESH_MS,
            )?,
            image_files,
        })
    }

    pub fn with_refresh_interval(mut self, interval: Duration) -> Self {
        self.refresh_interval = interval;
        self
    }

    pub fn with_image_files(mut self, files: impl IntoIterator<Item = PathBuf>) -> Self {
        self.image_files = files.into_iter().collect();
        self
    }

    /// Whether the host-facts loop runs.
    pub fn enabled(&self) -> bool {
        !self.refresh_interval.is_zero()
    }
}

/// Action deadlines and the bounded result registry.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ActionTuning {
    /// Deadline of a request with `deadline_ms = 0` (`ORION_NODE_ACTION_DEFAULT_DEADLINE_MS`).
    pub default_deadline: Duration,
    /// Longest accepted deadline; longer ones are capped (`ORION_NODE_ACTION_MAX_DEADLINE_MS`).
    pub max_deadline: Duration,
    /// How long final results stay queryable (`ORION_NODE_ACTION_RESULT_TTL_MS`).
    pub result_ttl: Duration,
    /// Most actions tracked at once (`ORION_NODE_ACTION_MAX_TRACKED`). The oldest final results
    /// are evicted first; requests are rejected while every tracked action is still running.
    pub max_tracked: usize,
    /// Longest time the node holds one unary answer to a `RunAction` with `wait_ms`
    /// (`ORION_NODE_ACTION_MAX_WAIT_MS`). It stays below the transport I/O timeouts; callers that
    /// need longer resend the request.
    pub max_wait: Duration,
    /// Action-name patterns enrolled remote operators may run when their policy names none
    /// (`ORION_NODE_OPERATOR_ACTIONS`, comma-separated `*`, `prefix*` or exact names). Empty by
    /// default: operators are read-only unless granted actions (`docs/remote-operator.md`).
    pub operator_actions: Vec<String>,
    /// What operators the node has not enrolled may read (`ORION_NODE_OPERATOR_OPEN_READ`).
    pub operator_open_read: OperatorOpenRead,
    /// Networks whose operators are enrolled on their first hello
    /// (`ORION_NODE_OPERATOR_AUTO_ENROLL`).
    pub operator_auto_enroll: Vec<OperatorAutoEnroll>,
}

/// What operators the node has not enrolled may read, without actions
/// (`ORION_NODE_OPERATOR_OPEN_READ`: `status`, `streams` and `state`, comma-separated). Their
/// requests are still signed (replay-protected); they can open push sessions for what is listed.
/// Nothing by default.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct OperatorOpenRead {
    /// `QueryStatus` and status watches.
    pub status: bool,
    /// Stream subscriptions (`docs/streams.md`).
    pub streams: bool,
    /// `QueryStateSnapshot` and state watches (node records, host facts).
    pub state: bool,
}

impl OperatorOpenRead {
    pub fn any(&self) -> bool {
        self.status || self.streams || self.state
    }

    pub fn parse(raw: &str) -> Result<Self, NodeError> {
        let mut open = Self::default();
        for item in raw
            .split(',')
            .map(str::trim)
            .filter(|item| !item.is_empty())
        {
            match item {
                "status" => open.status = true,
                "streams" => open.streams = true,
                "state" => open.state = true,
                other => {
                    return Err(NodeError::Config(format!(
                        "ORION_NODE_OPERATOR_OPEN_READ: `{other}` is not one of status, streams, \
                         state"
                    )));
                }
            }
        }
        Ok(open)
    }
}

/// Operators whose `OperatorHello` comes from `network` are enrolled with `actions` (and read
/// access) without an administrator (`ORION_NODE_OPERATOR_AUTO_ENROLL`), unless an administrator
/// removed them or they are enrolled with another key. The enrollment is persisted and audited
/// like any other.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OperatorAutoEnroll {
    pub network: std::net::IpAddr,
    pub prefix: u8,
    pub actions: Vec<String>,
}

impl OperatorAutoEnroll {
    /// Whether `addr` is on the rule's network (IPv4-mapped IPv6 counts as IPv4).
    pub fn contains(&self, addr: std::net::IpAddr) -> bool {
        use std::net::IpAddr;
        let addr = match addr {
            IpAddr::V6(v6) => v6.to_ipv4_mapped().map_or(addr, IpAddr::V4),
            v4 => v4,
        };
        match (self.network, addr) {
            (IpAddr::V4(net), IpAddr::V4(ip)) => {
                let bits = u32::from(self.prefix.min(32));
                let mask = if bits == 0 {
                    0
                } else {
                    u32::MAX << (32 - bits)
                };
                u32::from(net) & mask == u32::from(ip) & mask
            }
            (IpAddr::V6(net), IpAddr::V6(ip)) => {
                let bits = u32::from(self.prefix.min(128));
                let mask = if bits == 0 {
                    0
                } else {
                    u128::MAX << (128 - bits)
                };
                u128::from(net) & mask == u128::from(ip) & mask
            }
            _ => false,
        }
    }

    /// `<network>/<prefix>=<pattern>,<pattern>`, rules separated by `;`
    /// (`172.31.0.0/16=reboot,usb-boot`). An empty pattern list grants read access only.
    pub fn parse_all(raw: &str) -> Result<Vec<Self>, NodeError> {
        let invalid = |rule: &str, why: &str| {
            NodeError::Config(format!("ORION_NODE_OPERATOR_AUTO_ENROLL: `{rule}`: {why}"))
        };
        let mut rules = Vec::new();
        for rule in raw
            .split(';')
            .map(str::trim)
            .filter(|rule| !rule.is_empty())
        {
            let (network, actions) = rule.split_once('=').unwrap_or((rule, ""));
            let (address, prefix) = network
                .trim()
                .split_once('/')
                .ok_or_else(|| invalid(rule, "the network needs a /prefix"))?;
            let network: std::net::IpAddr = address
                .parse()
                .map_err(|_| invalid(rule, "not an IP address"))?;
            let prefix: u8 = prefix.parse().map_err(|_| invalid(rule, "bad prefix"))?;
            if prefix > if network.is_ipv4() { 32 } else { 128 } {
                return Err(invalid(rule, "prefix too long"));
            }
            let actions: Vec<String> = actions
                .split(',')
                .map(str::trim)
                .filter(|pattern| !pattern.is_empty())
                .map(str::to_owned)
                .collect();
            orion::control_plane::validate_action_patterns(&actions)
                .map_err(|err| invalid(rule, &err.to_string()))?;
            rules.push(Self {
                network,
                prefix,
                actions,
            });
        }
        Ok(rules)
    }
}

impl Default for ActionTuning {
    fn default() -> Self {
        Self {
            default_deadline: Duration::from_millis(DEFAULT_ACTION_DEFAULT_DEADLINE_MS),
            max_deadline: Duration::from_millis(DEFAULT_ACTION_MAX_DEADLINE_MS),
            result_ttl: Duration::from_millis(DEFAULT_ACTION_RESULT_TTL_MS),
            max_tracked: DEFAULT_ACTION_MAX_TRACKED,
            max_wait: Duration::from_millis(DEFAULT_ACTION_MAX_WAIT_MS),
            operator_actions: Vec::new(),
            operator_open_read: OperatorOpenRead::default(),
            operator_auto_enroll: Vec::new(),
        }
    }
}

impl ActionTuning {
    pub(crate) fn from_env() -> Result<Self, NodeError> {
        let mut tuning = Self {
            default_deadline: duration_ms_env_or(
                "ORION_NODE_ACTION_DEFAULT_DEADLINE_MS",
                DEFAULT_ACTION_DEFAULT_DEADLINE_MS,
            )?,
            max_deadline: duration_ms_env_or(
                "ORION_NODE_ACTION_MAX_DEADLINE_MS",
                DEFAULT_ACTION_MAX_DEADLINE_MS,
            )?,
            result_ttl: duration_ms_env_or(
                "ORION_NODE_ACTION_RESULT_TTL_MS",
                DEFAULT_ACTION_RESULT_TTL_MS,
            )?,
            max_tracked: parse_env_or("ORION_NODE_ACTION_MAX_TRACKED", DEFAULT_ACTION_MAX_TRACKED)?,
            max_wait: duration_ms_env_or(
                "ORION_NODE_ACTION_MAX_WAIT_MS",
                DEFAULT_ACTION_MAX_WAIT_MS,
            )?,
            operator_actions: operator_actions_from_env()?,
            operator_open_read: match env::var("ORION_NODE_OPERATOR_OPEN_READ") {
                Ok(raw) => OperatorOpenRead::parse(&raw)?,
                Err(env::VarError::NotPresent) => OperatorOpenRead::default(),
                Err(env::VarError::NotUnicode(_)) => {
                    return Err(NodeError::Config(
                        "ORION_NODE_OPERATOR_OPEN_READ must be valid unicode".into(),
                    ));
                }
            },
            operator_auto_enroll: match env::var("ORION_NODE_OPERATOR_AUTO_ENROLL") {
                Ok(raw) => OperatorAutoEnroll::parse_all(&raw)?,
                Err(env::VarError::NotPresent) => Vec::new(),
                Err(env::VarError::NotUnicode(_)) => {
                    return Err(NodeError::Config(
                        "ORION_NODE_OPERATOR_AUTO_ENROLL must be valid unicode".into(),
                    ));
                }
            },
        };
        tuning.normalize();
        Ok(tuning)
    }

    pub(crate) fn normalize(&mut self) {
        self.max_deadline = normalize_runtime_tuning_duration(self.max_deadline);
        self.default_deadline =
            normalize_runtime_tuning_duration(self.default_deadline).min(self.max_deadline);
        self.result_ttl = normalize_runtime_tuning_duration(self.result_ttl);
        self.max_tracked = self.max_tracked.max(1);
        self.max_wait = normalize_runtime_tuning_duration(self.max_wait);
    }

    pub fn with_default_deadline(mut self, deadline: Duration) -> Self {
        self.default_deadline = deadline;
        self.normalize();
        self
    }

    pub fn with_max_deadline(mut self, deadline: Duration) -> Self {
        self.max_deadline = deadline;
        self.normalize();
        self
    }

    pub fn with_result_ttl(mut self, ttl: Duration) -> Self {
        self.result_ttl = ttl;
        self.normalize();
        self
    }

    pub fn with_max_tracked(mut self, max_tracked: usize) -> Self {
        self.max_tracked = max_tracked;
        self.normalize();
        self
    }

    pub fn with_max_wait(mut self, max_wait: Duration) -> Self {
        self.max_wait = max_wait;
        self.normalize();
        self
    }

    /// Default action patterns of remote operators (see [`Self::operator_actions`]).
    pub fn with_operator_actions(mut self, patterns: impl IntoIterator<Item = String>) -> Self {
        self.operator_actions = patterns.into_iter().collect();
        self
    }

    /// See [`Self::operator_open_read`].
    pub fn with_operator_open_read(mut self, open: OperatorOpenRead) -> Self {
        self.operator_open_read = open;
        self
    }

    /// See [`Self::operator_auto_enroll`].
    pub fn with_operator_auto_enroll(mut self, rules: Vec<OperatorAutoEnroll>) -> Self {
        self.operator_auto_enroll = rules;
        self
    }
}

fn operator_actions_from_env() -> Result<Vec<String>, NodeError> {
    let raw = match env::var("ORION_NODE_OPERATOR_ACTIONS") {
        Ok(raw) => raw,
        Err(env::VarError::NotPresent) => return Ok(Vec::new()),
        Err(env::VarError::NotUnicode(_)) => {
            return Err(NodeError::Config(
                "ORION_NODE_OPERATOR_ACTIONS must be valid unicode".into(),
            ));
        }
    };
    let patterns: Vec<String> = raw
        .split(',')
        .map(str::trim)
        .filter(|pattern| !pattern.is_empty())
        .map(str::to_owned)
        .collect();
    orion::control_plane::validate_action_patterns(&patterns)
        .map_err(|err| NodeError::Config(format!("ORION_NODE_OPERATOR_ACTIONS: {err}")))?;
    Ok(patterns)
}

#[cfg(test)]
pub(crate) fn host_actions_doc_defaults(
    host: &HostFactsTuning,
    actions: &ActionTuning,
) -> Vec<(&'static str, String)> {
    vec![
        (
            "ORION_NODE_HOST_FACTS_REFRESH_MS",
            host.refresh_interval.as_millis().to_string(),
        ),
        (
            "ORION_NODE_ACTION_DEFAULT_DEADLINE_MS",
            actions.default_deadline.as_millis().to_string(),
        ),
        (
            "ORION_NODE_ACTION_MAX_DEADLINE_MS",
            actions.max_deadline.as_millis().to_string(),
        ),
        (
            "ORION_NODE_ACTION_RESULT_TTL_MS",
            actions.result_ttl.as_millis().to_string(),
        ),
        (
            "ORION_NODE_ACTION_MAX_TRACKED",
            actions.max_tracked.to_string(),
        ),
    ]
}
