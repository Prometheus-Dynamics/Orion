//! Workload placement constraints, node labels, and cross-node binding records.
//!
//! See `docs/placement.md`. These are plain data types; the placement algorithm lives in
//! `orion-cluster`.

use alloc::{
    string::{String, ToString},
    vec::Vec,
};
use orion_core::{NodeId, ResourceId, WorkloadId};
use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use serde::{Deserialize, Serialize};

/// One node-selector term: the node must carry label `key`, and when `value` is set the label's
/// value must equal it.
#[derive(
    Clone,
    Debug,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Serialize,
    Deserialize,
    Archive,
    RkyvSerialize,
    RkyvDeserialize,
)]
pub struct LabelRequirement {
    pub key: String,
    pub value: Option<String>,
}

impl LabelRequirement {
    /// The node must have label `key` with exactly `value`.
    pub fn equals(key: impl Into<String>, value: impl Into<String>) -> Self {
        Self {
            key: key.into(),
            value: Some(value.into()),
        }
    }

    /// The node must have label `key`, with any value.
    pub fn exists(key: impl Into<String>) -> Self {
        Self {
            key: key.into(),
            value: None,
        }
    }

    /// Parses `key=value` (equality) or `key` (existence).
    pub fn parse(term: &str) -> Option<Self> {
        let (key, value) = split_label(term)?;
        Some(match value {
            Some(value) => Self::equals(key, value),
            None => Self::exists(key),
        })
    }

    /// Whether `labels` (node label strings, `key=value` or `key`) satisfy this term.
    pub fn matches<'a>(&self, labels: impl IntoIterator<Item = &'a str>) -> bool {
        labels
            .into_iter()
            .filter_map(split_label)
            .any(|(key, value)| {
                key == self.key
                    && self
                        .value
                        .as_deref()
                        .is_none_or(|expected| value.unwrap_or("") == expected)
            })
    }
}

impl core::fmt::Display for LabelRequirement {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match &self.value {
            Some(value) => write!(f, "{}={}", self.key, value),
            None => f.write_str(&self.key),
        }
    }
}

/// Splits a node label (`key=value` or `key`) into its key and optional value. Surrounding
/// whitespace is trimmed; an empty key is rejected.
pub fn split_label(label: &str) -> Option<(&str, Option<&str>)> {
    let label = label.trim();
    let (key, value) = match label.split_once('=') {
        Some((key, value)) => (key.trim(), Some(value.trim())),
        None => (label, None),
    };
    (!key.is_empty()).then_some((key, value))
}

/// Parses a comma-separated label list (`ORION_NODE_LABELS` syntax: `k=v,k2=v2,flag`) into
/// normalized `key=value` / `key` strings, sorted and deduplicated by key (the last value for a
/// repeated key wins).
pub fn parse_node_labels(raw: &str) -> Vec<String> {
    let mut by_key = alloc::collections::BTreeMap::<String, Option<String>>::new();
    for term in raw.split(',') {
        if let Some((key, value)) = split_label(term) {
            by_key.insert(key.to_string(), value.map(ToString::to_string));
        }
    }
    by_key
        .into_iter()
        .map(|(key, value)| match value {
            Some(value) => alloc::format!("{key}={value}"),
            None => key,
        })
        .collect()
}

/// Why the placement engine assigned a workload to its node.
#[derive(
    Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub enum PlacementReason {
    /// First placement of an unassigned workload.
    Placed,
    /// Moved off `from` after it was gone or ineligible for longer than the grace period.
    Failover { from: NodeId },
}

/// The assignment the placement engine wrote. `node_id` equals the workload's
/// `assigned_node_id` while the assignment is managed by placement.
#[derive(
    Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct PlacementDecision {
    pub node_id: NodeId,
    pub reason: PlacementReason,
}

/// Placement constraints of a workload. A workload with `placement: None` is placed manually
/// (only an explicit `assigned_node_id` runs it). With `Some`, any node that satisfies every
/// constraint is eligible; empty constraints mean "any eligible node".
#[derive(
    Clone,
    Debug,
    Default,
    PartialEq,
    Eq,
    Serialize,
    Deserialize,
    Archive,
    RkyvSerialize,
    RkyvDeserialize,
)]
pub struct WorkloadPlacement {
    /// Every term must match one of the node's labels.
    pub node_selector: Vec<LabelRequirement>,
    /// Run on the node that hosts this resource (its provider's node, or the node of the
    /// executor that realizes it).
    pub colocate_with_resource: Option<ResourceId>,
    /// Set by the placement engine when it writes `assigned_node_id`. An `assigned_node_id`
    /// that differs from `decision.node_id` (or any assignment without a decision) is an
    /// explicit user assignment and is never moved.
    pub decision: Option<PlacementDecision>,
}

impl WorkloadPlacement {
    /// Any eligible node.
    pub fn any() -> Self {
        Self::default()
    }

    pub fn require_label(mut self, requirement: LabelRequirement) -> Self {
        self.node_selector.push(requirement);
        self
    }

    pub fn colocate_with(mut self, resource_id: impl Into<ResourceId>) -> Self {
        self.colocate_with_resource = Some(resource_id.into());
        self
    }
}

/// How a remote binding reaches the executor: the resource's own endpoints (Orion does not proxy
/// data) and whether the owning node is currently reachable.
#[derive(
    Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct RemoteBinding {
    pub endpoints: Vec<String>,
    /// `false` while the owning node is unreachable or reports the resource unavailable.
    pub available: bool,
}

/// One holder of a cross-node lease.
#[derive(
    Clone,
    Debug,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Serialize,
    Deserialize,
    Archive,
    RkyvSerialize,
    RkyvDeserialize,
)]
pub struct LeaseHolder {
    pub node_id: NodeId,
    pub workload_id: WorkloadId,
}

impl LeaseHolder {
    pub fn new(node_id: impl Into<NodeId>, workload_id: impl Into<WorkloadId>) -> Self {
        Self {
            node_id: node_id.into(),
            workload_id: workload_id.into(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn label_requirements_match_equality_and_existence() {
        let labels = ["zone=north", "gpu", "camera = front "];
        assert!(LabelRequirement::equals("zone", "north").matches(labels));
        assert!(!LabelRequirement::equals("zone", "south").matches(labels));
        assert!(LabelRequirement::exists("gpu").matches(labels));
        assert!(LabelRequirement::exists("zone").matches(labels));
        assert!(LabelRequirement::equals("camera", "front").matches(labels));
        assert!(LabelRequirement::equals("gpu", "").matches(labels));
        assert!(!LabelRequirement::exists("missing").matches(labels));
    }

    #[test]
    fn node_labels_parse_normalized_and_deduplicated() {
        assert_eq!(
            parse_node_labels(" zone=north, gpu ,,zone=south,=bad"),
            ["gpu", "zone=south"]
        );
        assert_eq!(
            LabelRequirement::parse("rack=7"),
            Some(LabelRequirement::equals("rack", "7"))
        );
        assert_eq!(LabelRequirement::parse(" "), None);
    }
}
