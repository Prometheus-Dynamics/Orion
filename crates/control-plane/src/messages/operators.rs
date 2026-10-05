//! Remote operators (`docs/remote-operator.md`).
//!
//! An operator is a desktop or fleet tool that talks to nodes over the signed `orion+tcp`
//! transport without running `orion-node`. It authenticates like a peer (an ed25519 key and a
//! signed [`AuthenticatedPeerRequest`](https://docs.rs/orion-auth)), but its principal id carries
//! the [`OPERATOR_ID_PREFIX`], and nodes treat it as a separate principal kind: it is never a
//! cluster member (no desired-state replica, no sync, no liveness or placement role) and may only
//! perform what its [`OperatorPolicy`] allows.

use alloc::{borrow::ToOwned, format, string::String, vec::Vec};
use core::{fmt, str::FromStr};
use orion_core::{NodeId, PublicKeyHex};
use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use serde::{Deserialize, Serialize};

/// Prefix of every operator principal id (`operator:<name>`).
pub const OPERATOR_ID_PREFIX: &str = "operator:";
/// Longest operator name (the part after the prefix).
pub const MAX_OPERATOR_NAME_LEN: usize = 64;
/// Most action-name patterns one policy may hold.
pub const MAX_OPERATOR_ACTION_PATTERNS: usize = 64;

/// An operator principal id: `operator:<name>`, where the name is 1-64 characters of
/// `[A-Za-z0-9._@-]`.
#[derive(
    Clone,
    Debug,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    Serialize,
    Deserialize,
    Archive,
    RkyvSerialize,
    RkyvDeserialize,
)]
#[serde(try_from = "String", into = "String")]
#[rkyv(derive(Debug, PartialEq, Eq, PartialOrd, Ord, Hash))]
pub struct OperatorId(String);

/// Why an operator id was refused.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct InvalidOperatorId(pub String);

impl fmt::Display for InvalidOperatorId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "invalid operator id `{}`: use `operator:<name>` (or just `<name>`) with 1-{} \
             characters of [A-Za-z0-9._@-]",
            self.0, MAX_OPERATOR_NAME_LEN
        )
    }
}

impl core::error::Error for InvalidOperatorId {}

impl OperatorId {
    /// Accepts `operator:<name>` or a bare `<name>` (the prefix is added).
    pub fn try_new(value: impl AsRef<str>) -> Result<Self, InvalidOperatorId> {
        let value = value.as_ref().trim();
        let name = value.strip_prefix(OPERATOR_ID_PREFIX).unwrap_or(value);
        let valid = !name.is_empty()
            && name.len() <= MAX_OPERATOR_NAME_LEN
            && name
                .chars()
                .all(|c| c.is_ascii_alphanumeric() || matches!(c, '.' | '_' | '@' | '-'));
        if !valid {
            return Err(InvalidOperatorId(value.to_owned()));
        }
        Ok(Self(format!("{OPERATOR_ID_PREFIX}{name}")))
    }

    /// The full id, `operator:<name>`.
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// The name after the prefix.
    pub fn name(&self) -> &str {
        &self.0[OPERATOR_ID_PREFIX.len()..]
    }

    /// The principal id carried in signed requests (`PeerRequestAuth::node_id`).
    pub fn to_principal(&self) -> NodeId {
        NodeId::new(self.0.clone())
    }

    /// Whether a request principal names an operator (rather than a node).
    pub fn is_operator_principal(principal: &str) -> bool {
        principal.starts_with(OPERATOR_ID_PREFIX)
    }

    /// The operator a request principal names, if it is a valid operator id.
    pub fn from_principal(principal: &NodeId) -> Option<Self> {
        if !Self::is_operator_principal(principal.as_str()) {
            return None;
        }
        Self::try_new(principal.as_str()).ok()
    }
}

impl fmt::Display for OperatorId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl FromStr for OperatorId {
    type Err = InvalidOperatorId;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Self::try_new(value)
    }
}

impl TryFrom<String> for OperatorId {
    type Error = InvalidOperatorId;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::try_new(value)
    }
}

impl From<OperatorId> for String {
    fn from(value: OperatorId) -> Self {
        value.0
    }
}

/// `true` when an action-name pattern matches `name`: `*` matches every name, `prefix*` names
/// starting with `prefix`, anything else exactly that name.
pub fn action_pattern_matches(pattern: &str, name: &str) -> bool {
    match pattern.strip_suffix('*') {
        Some(prefix) => name.starts_with(prefix),
        None => pattern == name,
    }
}

/// What an enrolled operator may do on a node.
#[derive(
    Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct OperatorPolicy {
    /// Read access: node records and the state snapshot, the status lane, every tracked action,
    /// and the observability snapshot.
    pub read: bool,
    /// Action-name patterns the operator may run (`*`, `prefix*`, or an exact name). `None`
    /// follows the node default (`ORION_NODE_OPERATOR_ACTIONS`); `Some(vec![])` allows none.
    pub actions: Option<Vec<String>>,
}

impl Default for OperatorPolicy {
    /// Read access and the node's default action patterns.
    fn default() -> Self {
        Self {
            read: true,
            actions: None,
        }
    }
}

impl OperatorPolicy {
    /// The action patterns in force, given the node default.
    pub fn effective_actions<'a>(&'a self, node_default: &'a [String]) -> &'a [String] {
        self.actions.as_deref().unwrap_or(node_default)
    }

    /// Whether the operator may run action `name`.
    pub fn allows_action(&self, name: &str, node_default: &[String]) -> bool {
        self.effective_actions(node_default)
            .iter()
            .any(|pattern| action_pattern_matches(pattern, name))
    }

    /// Checks the patterns (non-empty, at most 128 bytes, `*` only as the last character).
    pub fn validate(&self) -> Result<(), String> {
        let Some(patterns) = &self.actions else {
            return Ok(());
        };
        validate_action_patterns(patterns)
    }
}

/// Checks a list of action-name patterns.
pub fn validate_action_patterns(patterns: &[String]) -> Result<(), String> {
    if patterns.len() > MAX_OPERATOR_ACTION_PATTERNS {
        return Err(format!(
            "at most {MAX_OPERATOR_ACTION_PATTERNS} action patterns are allowed"
        ));
    }
    for pattern in patterns {
        let body = pattern.strip_suffix('*').unwrap_or(pattern);
        if pattern.is_empty() || pattern.len() > 128 || body.contains('*') {
            return Err(format!(
                "invalid action pattern `{pattern}`: use `*`, `prefix*`, or an exact action name"
            ));
        }
    }
    Ok(())
}

/// How an operator was enrolled.
#[derive(
    Clone,
    Copy,
    Debug,
    PartialEq,
    Eq,
    Serialize,
    Deserialize,
    Archive,
    RkyvSerialize,
    RkyvDeserialize,
)]
#[serde(rename_all = "snake_case")]
pub enum OperatorEnrollmentMethod {
    /// `orionctl operators enroll` on the node.
    Approval,
    /// The shared enrollment key handshake.
    EnrollmentKey,
}

/// Trust state of an operator on one node.
#[derive(
    Clone,
    Copy,
    Debug,
    PartialEq,
    Eq,
    Serialize,
    Deserialize,
    Archive,
    RkyvSerialize,
    RkyvDeserialize,
)]
#[serde(rename_all = "snake_case")]
pub enum OperatorTrustState {
    /// Enrolled: requests signed with the pinned key are served under the operator's policy.
    Enrolled,
    /// Sent a validly signed request but is not enrolled; waits for `orionctl operators enroll`.
    Pending,
    /// Removed by an administrator; never enrolled again automatically.
    Revoked,
}

impl fmt::Display for OperatorTrustState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Enrolled => "enrolled",
            Self::Pending => "pending",
            Self::Revoked => "revoked",
        })
    }
}

/// One operator known to a node.
#[derive(
    Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct OperatorRecord {
    pub operator_id: OperatorId,
    pub state: OperatorTrustState,
    /// The pinned key (enrolled) or the key the operator presented (pending); `None` for a
    /// revoked operator.
    pub public_key_hex: Option<PublicKeyHex>,
    /// `sha256:` + the first 16 bytes of SHA-256 over the key, in hex.
    pub key_fingerprint: Option<String>,
    pub method: Option<OperatorEnrollmentMethod>,
    pub policy: OperatorPolicy,
    /// The action patterns in force (the policy's, or the node default).
    pub effective_actions: Vec<String>,
    /// Enrollment time (enrolled) or first request (pending), Unix milliseconds.
    pub since_ms: u64,
    /// Last authenticated request, Unix milliseconds (0 when never seen).
    pub last_seen_ms: u64,
}

/// Answer to [`crate::ControlMessage::QueryOperators`].
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
pub struct OperatorsSnapshot {
    /// Fingerprint of the node's own key, for the operator to compare.
    pub local_key_fingerprint: String,
    /// Default action patterns for operators without their own (`ORION_NODE_OPERATOR_ACTIONS`).
    pub default_actions: Vec<String>,
    /// Whether operators can enroll with the shared enrollment key.
    pub enrollment_key_configured: bool,
    pub operators: Vec<OperatorRecord>,
}

/// Administrator approval of an operator (`orionctl operators enroll`).
#[derive(
    Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct OperatorEnrollment {
    pub operator_id: OperatorId,
    /// The operator's key. When `None`, the key of the operator's pending request is used.
    pub public_key_hex: Option<PublicKeyHex>,
    /// When set, the enrollment fails unless the key has this fingerprint.
    pub expected_key_fingerprint: Option<String>,
    pub policy: OperatorPolicy,
}

/// Answer to [`crate::ControlMessage::OperatorHello`]: who the node is and what the operator may
/// do there.
#[derive(
    Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct OperatorWelcome {
    pub node_id: NodeId,
    /// The node's ed25519 public key (it signs every response with it).
    pub node_public_key: Vec<u8>,
    pub node_key_fingerprint: String,
    /// Discovery cluster name; empty when discovery is off.
    pub cluster: String,
    pub operator_id: OperatorId,
    /// Fingerprint of the key the operator signed the hello with.
    pub operator_key_fingerprint: String,
    pub state: OperatorTrustState,
    /// Whether the operator can enroll with the shared enrollment key here.
    pub enrollment_key_configured: bool,
    pub read: bool,
    /// Action patterns the operator may run (empty unless enrolled).
    pub allowed_actions: Vec<String>,
}

impl OperatorWelcome {
    /// Whether the operator may run action `name` on this node.
    pub fn allows_action(&self, name: &str) -> bool {
        self.state == OperatorTrustState::Enrolled
            && self
                .allowed_actions
                .iter()
                .any(|pattern| action_pattern_matches(pattern, name))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloc::vec;

    #[test]
    fn operator_ids_are_prefixed_and_validated() {
        let id = OperatorId::try_new("alice").expect("bare names get the prefix");
        assert_eq!(id.as_str(), "operator:alice");
        assert_eq!(id.name(), "alice");
        assert_eq!(OperatorId::try_new("operator:alice"), Ok(id.clone()));
        assert_eq!(
            OperatorId::from_principal(&NodeId::new("operator:alice")),
            Some(id)
        );
        assert_eq!(OperatorId::from_principal(&NodeId::new("node-a")), None);
        for bad in ["", "operator:", "a b", "operator:x/y", "operator:a:b"] {
            assert!(OperatorId::try_new(bad).is_err(), "{bad} must be refused");
        }
    }

    #[test]
    fn action_patterns_match_exact_prefix_and_wildcard() {
        assert!(action_pattern_matches("*", "reboot"));
        assert!(action_pattern_matches("self-*", "self-test"));
        assert!(!action_pattern_matches("self-*", "reboot"));
        assert!(action_pattern_matches("locate", "locate"));
        assert!(!action_pattern_matches("locate", "locate-all"));

        let defaults = vec!["locate".to_owned()];
        let inherit = OperatorPolicy::default();
        assert!(inherit.allows_action("locate", &defaults));
        assert!(!inherit.allows_action("reboot", &defaults));
        let none = OperatorPolicy {
            read: true,
            actions: Some(vec![]),
        };
        assert!(!none.allows_action("locate", &defaults));
        assert!(
            OperatorPolicy {
                read: true,
                actions: Some(vec!["re*boot".into()]),
            }
            .validate()
            .is_err()
        );
    }
}
