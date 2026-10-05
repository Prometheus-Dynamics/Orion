//! Placement and cross-node binding settings (`docs/placement.md`, `docs/node-env.md`).

use super::runtime_tuning::{duration_ms_env_or, normalize_runtime_tuning_duration};
use crate::NodeError;
use std::{env, time::Duration};

const DEFAULT_LIVENESS_TIMEOUT_MS: u64 = 5_000;
const DEFAULT_PLACEMENT_GRACE_MS: u64 = 10_000;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PlacementTuning {
    /// This node's labels (`ORION_NODE_LABELS=k=v,k2=v2,flag`), published in its observed node
    /// record and matched by workload node selectors. Normalized `key=value` / `key` strings.
    pub labels: Vec<String>,
    /// A peer is considered gone when nothing was heard from it (successful sync round, signed
    /// request, observed push) for this long (`ORION_NODE_LIVENESS_TIMEOUT_MS`).
    pub liveness_timeout: Duration,
    /// How long a workload's assignee (or a cross-node binding's owner) must stay gone or
    /// ineligible before the workload or binding moves (`ORION_NODE_PLACEMENT_GRACE_MS`).
    pub grace: Duration,
}

impl Default for PlacementTuning {
    fn default() -> Self {
        Self {
            labels: Vec::new(),
            liveness_timeout: Duration::from_millis(DEFAULT_LIVENESS_TIMEOUT_MS),
            grace: Duration::from_millis(DEFAULT_PLACEMENT_GRACE_MS),
        }
    }
}

impl PlacementTuning {
    pub(crate) fn from_env() -> Result<Self, NodeError> {
        let labels = match env::var("ORION_NODE_LABELS") {
            Ok(raw) => orion::control_plane::parse_node_labels(&raw),
            Err(env::VarError::NotPresent) => Vec::new(),
            Err(env::VarError::NotUnicode(_)) => {
                return Err(NodeError::Config(
                    "ORION_NODE_LABELS must be valid unicode".into(),
                ));
            }
        };
        let mut tuning = Self {
            labels,
            liveness_timeout: duration_ms_env_or(
                "ORION_NODE_LIVENESS_TIMEOUT_MS",
                DEFAULT_LIVENESS_TIMEOUT_MS,
            )?,
            grace: duration_ms_env_or("ORION_NODE_PLACEMENT_GRACE_MS", DEFAULT_PLACEMENT_GRACE_MS)?,
        };
        tuning.normalize();
        Ok(tuning)
    }

    pub(crate) fn normalize(&mut self) {
        self.liveness_timeout = normalize_runtime_tuning_duration(self.liveness_timeout);
    }

    /// Sets the labels from `ORION_NODE_LABELS` syntax.
    pub fn with_labels(mut self, raw: &str) -> Self {
        self.labels = orion::control_plane::parse_node_labels(raw);
        self
    }

    pub fn with_liveness_timeout(mut self, timeout: Duration) -> Self {
        self.liveness_timeout = timeout;
        self.normalize();
        self
    }

    pub fn with_grace(mut self, grace: Duration) -> Self {
        self.grace = grace;
        self
    }
}

#[cfg(test)]
pub(crate) fn placement_doc_defaults(tuning: &PlacementTuning) -> Vec<(&'static str, String)> {
    vec![
        (
            "ORION_NODE_LIVENESS_TIMEOUT_MS",
            tuning.liveness_timeout.as_millis().to_string(),
        ),
        (
            "ORION_NODE_PLACEMENT_GRACE_MS",
            tuning.grace.as_millis().to_string(),
        ),
    ]
}
