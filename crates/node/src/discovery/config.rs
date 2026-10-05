//! Discovery configuration (`ORION_NODE_DISCOVERY`, `ORION_NODE_CLUSTER`, ... in
//! `docs/node-env.md`).

use crate::NodeError;
use std::{env, fmt, path::Path, time::Duration};

/// Cluster name used when `ORION_NODE_CLUSTER` is unset.
pub const DEFAULT_CLUSTER: &str = "default";
const DEFAULT_TTL_MS: u64 = 120_000;
const DEFAULT_TICK_MS: u64 = 1_000;
const DEFAULT_ENROLLMENT_RETRY_MS: u64 = 5_000;
const MAX_CLUSTER_LEN: usize = 63;
/// Shortest accepted enrollment key, in bytes. The key must resist offline guessing (see the
/// threat model in `docs/discovery.md`), so `openssl rand -hex 32` is the recommended source.
pub(crate) const MIN_ENROLLMENT_KEY_LEN: usize = 32;

/// A shared enrollment key. Holders of the key can enroll with every node that has it.
#[derive(Clone, PartialEq, Eq)]
pub struct EnrollmentKey(Vec<u8>);

impl EnrollmentKey {
    /// Accepts keys of at least 32 bytes (surrounding whitespace is ignored).
    pub fn try_new(bytes: impl AsRef<[u8]>) -> Result<Self, NodeError> {
        let bytes = bytes.as_ref().trim_ascii();
        if bytes.len() < MIN_ENROLLMENT_KEY_LEN {
            return Err(NodeError::Config(format!(
                "the enrollment key must be at least {MIN_ENROLLMENT_KEY_LEN} bytes (for example \
                 `openssl rand -hex 32`)"
            )));
        }
        Ok(Self(bytes.to_vec()))
    }

    pub(crate) fn as_bytes(&self) -> &[u8] {
        &self.0
    }
}

impl fmt::Debug for EnrollmentKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("EnrollmentKey(<redacted>)")
    }
}

/// Settings of the discovery runtime.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DiscoveryConfig {
    /// Only peers that advertise the same cluster name are considered.
    pub cluster: String,
    /// How long a discovered peer stays in the set without a new announcement.
    pub ttl: Duration,
    /// Period of expiry checks and enrollment retries.
    pub tick: Duration,
    /// Base delay before retrying a failed automatic enrollment (doubles per failure, at most
    /// five minutes).
    pub enrollment_retry: Duration,
    /// Shared enrollment key; `None` disables automatic enrollment.
    pub enrollment_key: Option<EnrollmentKey>,
    /// Network interfaces (names or addresses) to use for mDNS; empty means all.
    pub interfaces: Vec<String>,
}

impl DiscoveryConfig {
    pub fn new(cluster: impl Into<String>) -> Result<Self, NodeError> {
        let cluster = cluster.into();
        validate_cluster_name(&cluster)?;
        Ok(Self {
            cluster,
            ttl: Duration::from_millis(DEFAULT_TTL_MS),
            tick: Duration::from_millis(DEFAULT_TICK_MS),
            enrollment_retry: Duration::from_millis(DEFAULT_ENROLLMENT_RETRY_MS),
            enrollment_key: None,
            interfaces: Vec::new(),
        })
    }

    pub fn with_enrollment_key(mut self, key: EnrollmentKey) -> Self {
        self.enrollment_key = Some(key);
        self
    }

    pub fn with_ttl(mut self, ttl: Duration) -> Self {
        self.ttl = ttl;
        self
    }

    pub fn with_tick(mut self, tick: Duration) -> Self {
        self.tick = tick;
        self
    }

    pub fn with_enrollment_retry(mut self, retry: Duration) -> Self {
        self.enrollment_retry = retry;
        self
    }

    /// Reads `ORION_NODE_DISCOVERY` and the related variables. `Ok(None)` when discovery is off.
    pub fn try_from_env() -> Result<Option<Self>, NodeError> {
        let mode = env::var("ORION_NODE_DISCOVERY").unwrap_or_default();
        match mode.trim().to_ascii_lowercase().as_str() {
            "" | "off" | "none" | "disabled" => {
                if enrollment_key_from_env()?.is_some() {
                    return Err(NodeError::Config(
                        "ORION_NODE_ENROLLMENT_KEY(_FILE) needs ORION_NODE_DISCOVERY=mdns".into(),
                    ));
                }
                return Ok(None);
            }
            "mdns" => {}
            other => {
                return Err(NodeError::Config(format!(
                    "invalid ORION_NODE_DISCOVERY `{other}`; expected `mdns` or `off`"
                )));
            }
        }
        let cluster = env::var("ORION_NODE_CLUSTER").unwrap_or_else(|_| DEFAULT_CLUSTER.into());
        let mut config = Self::new(cluster.trim())?;
        if let Some(ttl) = positive_ms_env("ORION_NODE_DISCOVERY_TTL_MS")? {
            config.ttl = ttl;
        }
        config.enrollment_key = enrollment_key_from_env()?;
        config.interfaces = env::var("ORION_NODE_DISCOVERY_INTERFACES")
            .unwrap_or_default()
            .split(',')
            .map(str::trim)
            .filter(|name| !name.is_empty())
            .map(str::to_owned)
            .collect();
        Ok(Some(config))
    }
}

fn positive_ms_env(key: &str) -> Result<Option<Duration>, NodeError> {
    let Ok(raw) = env::var(key) else {
        return Ok(None);
    };
    match raw.trim().parse::<u64>() {
        Ok(value) if value > 0 => Ok(Some(Duration::from_millis(value))),
        _ => Err(NodeError::Config(format!(
            "{key} must be a positive number of milliseconds: {raw}"
        ))),
    }
}

fn enrollment_key_from_env() -> Result<Option<EnrollmentKey>, NodeError> {
    let inline = env::var("ORION_NODE_ENROLLMENT_KEY").ok();
    let file = env::var("ORION_NODE_ENROLLMENT_KEY_FILE").ok();
    match (inline, file) {
        (Some(_), Some(_)) => Err(NodeError::Config(
            "set only one of ORION_NODE_ENROLLMENT_KEY and ORION_NODE_ENROLLMENT_KEY_FILE".into(),
        )),
        (Some(key), None) => EnrollmentKey::try_new(key)
            .map(Some)
            .map_err(|err| NodeError::Config(format!("ORION_NODE_ENROLLMENT_KEY: {err}"))),
        (None, Some(path)) => {
            let bytes = std::fs::read(Path::new(&path)).map_err(|err| {
                NodeError::Config(format!(
                    "failed to read ORION_NODE_ENROLLMENT_KEY_FILE `{path}`: {err}"
                ))
            })?;
            EnrollmentKey::try_new(bytes)
                .map(Some)
                .map_err(|err| NodeError::Config(format!("ORION_NODE_ENROLLMENT_KEY_FILE: {err}")))
        }
        (None, None) => Ok(None),
    }
}

/// Cluster names are 1-63 characters of `[A-Za-z0-9._-]`.
pub(crate) fn validate_cluster_name(cluster: &str) -> Result<(), NodeError> {
    let valid = !cluster.is_empty()
        && cluster.len() <= MAX_CLUSTER_LEN
        && cluster
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, '.' | '_' | '-'));
    if valid {
        Ok(())
    } else {
        Err(NodeError::Config(format!(
            "invalid cluster name `{cluster}`: use 1-{MAX_CLUSTER_LEN} characters of \
             [A-Za-z0-9._-]"
        )))
    }
}
