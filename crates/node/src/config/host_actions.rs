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
}

impl Default for ActionTuning {
    fn default() -> Self {
        Self {
            default_deadline: Duration::from_millis(DEFAULT_ACTION_DEFAULT_DEADLINE_MS),
            max_deadline: Duration::from_millis(DEFAULT_ACTION_MAX_DEADLINE_MS),
            result_ttl: Duration::from_millis(DEFAULT_ACTION_RESULT_TTL_MS),
            max_tracked: DEFAULT_ACTION_MAX_TRACKED,
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
