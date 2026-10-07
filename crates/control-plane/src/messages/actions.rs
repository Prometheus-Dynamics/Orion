//! Generic actions: one-shot, named operations on a node, provider, resource, or executor.
//!
//! An [`ActionRequest`] names a target and a free-form action name with typed arguments. The node
//! that owns the target routes it to a handler (a node-side handler registered by the embedder,
//! or the local provider or executor client that owns the target) and tracks its lifecycle as an
//! [`ActionResult`]. Orion defines no action itself; [`action_names`] lists a few well-known names
//! and their argument conventions so independent handlers agree. See `docs/actions.md`.

use super::status::{StatusEntry, StatusSubject};
use crate::{ResourceActionResult, ResourceActionStatus, TypedConfigValue};
use alloc::{collections::BTreeMap, string::String, vec::Vec};
use core::{fmt, str::FromStr};
use orion_core::{ExecutorId, NodeId, ProviderId, ResourceId};
use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use serde::{Deserialize, Serialize};

/// Well-known action names and their argument conventions. Orion implements none of them; a
/// handler that implements one should follow its convention.
pub mod action_names {
    /// Reboot the target (a node, or a device behind a provider). Optional args: `delay_ms`
    /// (`UInt`), `reason` (`String`).
    pub const REBOOT: &str = "reboot";
    /// Restart a service unit on a node. Args: `unit` (`String`, required).
    pub const RESTART_UNIT: &str = "restart-unit";
    /// Make the target physically identifiable (blink an LED, beep). Optional args:
    /// `duration_ms` (`UInt`), `enabled` (`Bool`, `false` stops it).
    pub const LOCATE: &str = "locate";
    /// Run a self-test. Optional args: `level` (`String`, handler-defined).
    pub const SELF_TEST: &str = "self-test";
    /// Install a software update, asynchronously (typically handled by a device agent that
    /// claimed the node action). Args ([`update_action`](super::update_action)): `image_url`
    /// (`String`, the handler downloads it; image bytes never travel over Orion) or `transfer_id`
    /// (`String`, reserved for in-band transfers), `sha256` (`String`, hex digest of the file as
    /// served), `size` (`UInt`, bytes). The action reports `Succeeded` with output
    /// `phase = "staging"` once the download and stage have **started**; the outcome comes only
    /// from the `update.*` status keys and the node's host facts. See `docs/device-agent.md`.
    pub const UPDATE: &str = "update";
    /// Abort an `update` download or stage in progress, or forget a staged update. No args.
    /// `Succeeded` with `phase = "cancelled"`, or `phase = "idle"` when there was nothing to
    /// cancel. Claimed by the handler of `update`.
    pub const UPDATE_CANCEL: &str = "update.cancel";
    /// Boot back to the previous confirmed slot. No args. `Succeeded` with
    /// `phase = "rebooting"` before the reboot; `Rejected` when there is no previous slot.
    pub const UPDATE_ROLLBACK: &str = "update.rollback";
}

/// Argument, output and status-key names of the well-known `update` action
/// (`docs/device-agent.md`). Orion does not interpret them.
pub mod update_action {
    /// Arg: URL the handler downloads the image from (`String`).
    pub const ARG_IMAGE_URL: &str = "image_url";
    /// Arg: an in-band transfer naming the image (`String`; reserved).
    pub const ARG_TRANSFER_ID: &str = "transfer_id";
    /// Arg: hex SHA-256 of the file as served (`String`).
    pub const ARG_SHA256: &str = "sha256";
    /// Arg: size of the file as served, in bytes (`UInt`).
    pub const ARG_SIZE: &str = "size";

    /// Output: where the handler is when the action ends (`PHASE_*`).
    pub const OUTPUT_PHASE: &str = "phase";
    /// `update`: the download and stage have started; follow [`KEY_STATE`].
    pub const PHASE_STAGING: &str = "staging";
    /// `update.rollback`, `reboot`: reported just before the handler reboots.
    pub const PHASE_REBOOTING: &str = "rebooting";
    /// `update.cancel`: a download, stage, or staged update was cancelled.
    pub const PHASE_CANCELLED: &str = "cancelled";
    /// `update.cancel`: there was nothing to cancel.
    pub const PHASE_IDLE: &str = "idle";

    /// Status key under `node/<id>`: the updater's state (`String`, one of the `STATE_*`
    /// values).
    pub const KEY_STATE: &str = "update.state";
    /// [`KEY_STATE`]: nothing in progress.
    pub const STATE_IDLE: &str = "idle";
    /// [`KEY_STATE`]: downloading, verifying and writing the inactive slot.
    pub const STATE_STAGING: &str = "staging";
    /// [`KEY_STATE`]: written and verified, not switched to yet.
    pub const STATE_STAGED: &str = "staged";
    /// [`KEY_STATE`]: the switch was issued and the device is rebooting.
    pub const STATE_REBOOTING: &str = "rebooting";
    /// [`KEY_STATE`]: booted the new slot on trial, not confirmed yet.
    pub const STATE_TRYING: &str = "trying";
    /// [`KEY_STATE`]: the new slot passed its health check and is kept (success).
    pub const STATE_CONFIRMED: &str = "confirmed";
    /// [`KEY_STATE`]: the trial was not confirmed (or `update.rollback` ran) and the previous
    /// slot runs again.
    pub const STATE_ROLLED_BACK: &str = "rolled-back";
    /// [`KEY_STATE`]: `update.cancel` stopped a download or stage, or forgot a staged update.
    pub const STATE_CANCELLED: &str = "cancelled";
    /// [`KEY_STATE`]: the update failed before the switch; see [`KEY_ERROR`].
    pub const STATE_ERROR: &str = "error";
    /// Status key: version of the running image (`String`).
    pub const KEY_VERSION_ACTIVE: &str = "update.version_active";
    /// Status key: version of the staged image (`String`).
    pub const KEY_VERSION_STAGED: &str = "update.version_staged";
    /// Status key: the running slot (`String`, for example `A`).
    pub const KEY_SLOT_ACTIVE: &str = "update.slot_active";
    /// Status key: the staged slot (`String`).
    pub const KEY_SLOT_STAGED: &str = "update.slot_staged";
    /// Status key: progress of the current step, per mille (`UInt`, 0 to 1000).
    pub const KEY_PROGRESS: &str = "update.progress";
    /// Status key: the last error (`String`, empty when none).
    pub const KEY_ERROR: &str = "update.error";
    /// Status key: the kernel boot id the keys were published in (`String`), so readers can
    /// tell keys of this boot from leftovers.
    pub const KEY_BOOT_ID: &str = "update.boot_id";
}

/// Status-lane key convention for action progress (`docs/actions.md`).
///
/// Handlers may mirror an action's lifecycle into the volatile status lane, under the action's
/// target as subject ([`status_subject`](crate::ActionTarget::status_subject)):
/// `action.<action_id>.state` (`String`, [`as_str`](crate::ActionState::as_str)),
/// `action.<action_id>.progress` (`UInt`, per-mille 0 to 1000), and
/// `action.<action_id>.error` (`String`). Unlike the in-memory action record, a handler can
/// republish these keys after a restart (for example after the node rebooted to finish an
/// update), so watchers learn the final outcome.
pub mod action_status_keys {
    use alloc::{format, string::String};

    pub const STATE: &str = "state";
    pub const PROGRESS: &str = "progress";
    pub const ERROR: &str = "error";

    /// `action.<action_id>.<field>`.
    pub fn key(action_id: &str, field: &str) -> String {
        format!("action.{action_id}.{field}")
    }
}

/// What an action acts on.
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
pub enum ActionTarget {
    /// A node; handled by a node-side action handler registered on that node.
    Node(NodeId),
    /// A provider; handled by the local client that registered as its action handler.
    Provider(ProviderId),
    /// A resource; handled by the action handler of the resource's provider.
    Resource(ResourceId),
    /// An executor; handled by the local client that registered as its action handler.
    Executor(ExecutorId),
}

impl ActionTarget {
    /// `node`, `provider`, `resource`, or `executor`.
    pub fn kind_name(&self) -> &'static str {
        match self {
            Self::Node(_) => "node",
            Self::Provider(_) => "provider",
            Self::Resource(_) => "resource",
            Self::Executor(_) => "executor",
        }
    }

    pub fn id(&self) -> &str {
        match self {
            Self::Node(id) => id.as_str(),
            Self::Provider(id) => id.as_str(),
            Self::Resource(id) => id.as_str(),
            Self::Executor(id) => id.as_str(),
        }
    }
}

impl ActionTarget {
    /// The status-lane subject for this target (see [`action_status_keys`]).
    pub fn status_subject(&self) -> StatusSubject {
        match self {
            Self::Node(id) => StatusSubject::Node(id.clone()),
            Self::Provider(id) => StatusSubject::Provider(id.clone()),
            Self::Resource(id) => StatusSubject::Resource(id.clone()),
            Self::Executor(id) => StatusSubject::Executor(id.clone()),
        }
    }
}

impl fmt::Display for ActionTarget {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}/{}", self.kind_name(), self.id())
    }
}

/// Error for a malformed `kind/id` action target.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ActionTargetParseError(pub String);

impl fmt::Display for ActionTargetParseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "invalid action target `{}`; expected node/<id>, provider/<id>, resource/<id>, or executor/<id>",
            self.0
        )
    }
}

impl FromStr for ActionTarget {
    type Err = ActionTargetParseError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let error = || ActionTargetParseError(value.into());
        let (kind, id) = value.split_once('/').ok_or_else(error)?;
        if id.trim().is_empty() {
            return Err(error());
        }
        Ok(match kind {
            "node" => Self::Node(NodeId::new(id)),
            "provider" => Self::Provider(ProviderId::new(id)),
            "resource" => Self::Resource(ResourceId::new(id)),
            "executor" => Self::Executor(ExecutorId::new(id)),
            _ => return Err(error()),
        })
    }
}

/// A request to run an action.
///
/// `action_id` is chosen by the client and must be unique among the actions a node remembers; a
/// request repeated with the same id and content returns the existing result instead of running
/// twice. `deadline_ms` is a relative time budget in milliseconds, counted from when the node
/// accepts the request (`0` asks for the node default; the node caps it at its maximum). The node
/// replaces `requested_by` with the authenticated requester (`local:<client name>` or
/// `peer:<node id>/<original>`).
#[derive(
    Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct ActionRequest {
    pub action_id: String,
    pub target: ActionTarget,
    pub name: String,
    #[serde(default)]
    pub args: BTreeMap<String, TypedConfigValue>,
    #[serde(default)]
    pub deadline_ms: u64,
    #[serde(default)]
    pub requested_by: String,
}

impl ActionRequest {
    pub fn new(
        action_id: impl Into<String>,
        target: ActionTarget,
        name: impl Into<String>,
    ) -> Self {
        Self {
            action_id: action_id.into(),
            target,
            name: name.into(),
            args: BTreeMap::new(),
            deadline_ms: 0,
            requested_by: String::new(),
        }
    }

    pub fn with_arg(mut self, key: impl Into<String>, value: TypedConfigValue) -> Self {
        self.args.insert(key.into(), value);
        self
    }

    pub fn with_deadline_ms(mut self, deadline_ms: u64) -> Self {
        self.deadline_ms = deadline_ms;
        self
    }

    pub fn with_requested_by(mut self, requested_by: impl Into<String>) -> Self {
        self.requested_by = requested_by.into();
        self
    }
}

/// Lifecycle state of an action.
#[derive(
    Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub enum ActionState {
    /// Accepted by the owning node and handed to its handler.
    Accepted,
    /// The handler reported progress (`progress` in per-mille, 0 to 1000; `None` when unknown).
    Running {
        progress: Option<u16>,
    },
    Succeeded,
    Failed {
        reason: String,
    },
    /// Refused before it ran (no handler, unknown target, not authorized, invalid arguments).
    Rejected {
        reason: String,
    },
    /// The deadline passed before the handler reported a final state.
    TimedOut,
}

impl ActionState {
    /// `accepted`, `running`, `succeeded`, `failed`, `rejected`, or `timed_out`.
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Accepted => "accepted",
            Self::Running { .. } => "running",
            Self::Succeeded => "succeeded",
            Self::Failed { .. } => "failed",
            Self::Rejected { .. } => "rejected",
            Self::TimedOut => "timed_out",
        }
    }

    /// Whether the state is final (no further updates follow).
    pub fn is_terminal(&self) -> bool {
        !matches!(self, Self::Accepted | Self::Running { .. })
    }

    /// The failure or rejection reason, if any.
    pub fn reason(&self) -> Option<&str> {
        match self {
            Self::Failed { reason } | Self::Rejected { reason } => Some(reason),
            _ => None,
        }
    }
}

impl fmt::Display for ActionState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Current state of an action, as tracked by the node it was submitted to.
///
/// `output` is a small result map set by the handler (the node bounds its size). Timestamps are
/// Unix milliseconds. `handled_by` is the node that owns the target.
#[derive(
    Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct ActionResult {
    pub action_id: String,
    pub target: ActionTarget,
    pub name: String,
    pub state: ActionState,
    #[serde(default)]
    pub output: BTreeMap<String, TypedConfigValue>,
    pub handled_by: NodeId,
    pub requested_by: String,
    pub created_at_ms: u64,
    pub updated_at_ms: u64,
}

impl ActionResult {
    /// A result with empty output, no requester, and zero timestamps.
    pub fn new(
        action_id: impl Into<String>,
        target: ActionTarget,
        name: impl Into<String>,
        handled_by: NodeId,
        state: ActionState,
    ) -> Self {
        Self {
            action_id: action_id.into(),
            target,
            name: name.into(),
            state,
            output: BTreeMap::new(),
            handled_by,
            requested_by: String::new(),
            created_at_ms: 0,
            updated_at_ms: 0,
        }
    }

    pub fn with_output(mut self, key: impl Into<String>, value: TypedConfigValue) -> Self {
        self.output.insert(key.into(), value);
        self
    }

    /// Status-lane entries for this result under the [`action_status_keys`] convention:
    /// `state`, plus `progress` while running with a known progress and `error` when failed or
    /// rejected.
    pub fn status_entries(&self) -> Vec<StatusEntry> {
        let subject = self.target.status_subject();
        let key = |field| action_status_keys::key(&self.action_id, field);
        let mut entries = alloc::vec![StatusEntry::new(
            subject.clone(),
            key(action_status_keys::STATE),
            TypedConfigValue::String(self.state.as_str().into()),
        )];
        if let ActionState::Running {
            progress: Some(progress),
        } = &self.state
        {
            entries.push(StatusEntry::new(
                subject.clone(),
                key(action_status_keys::PROGRESS),
                TypedConfigValue::UInt(u64::from(*progress)),
            ));
        }
        if let Some(reason) = self.state.reason() {
            entries.push(StatusEntry::new(
                subject,
                key(action_status_keys::ERROR),
                TypedConfigValue::String(reason.into()),
            ));
        }
        entries
    }

    /// The outcome as the per-resource [`ResourceActionResult`] carried in `ResourceState` (and
    /// on the MCU link wire), for handlers that also record the last action on the resource.
    /// `None` while the action is not final.
    pub fn as_resource_action_result(&self) -> Option<ResourceActionResult> {
        let status = match &self.state {
            ActionState::Succeeded => ResourceActionStatus::Applied,
            ActionState::Failed { .. } | ActionState::Rejected { .. } | ActionState::TimedOut => {
                ResourceActionStatus::Failed
            }
            ActionState::Accepted | ActionState::Running { .. } => return None,
        };
        Some(ResourceActionResult {
            action_kind: self.name.clone(),
            status,
            data: None,
            error: self.state.reason().map(Into::into).or_else(|| {
                matches!(self.state, ActionState::TimedOut).then(|| String::from("timed out"))
            }),
        })
    }
}

/// A handler's progress or outcome report for an action it received (`ReportActionResult`).
///
/// `state` is `Running`, `Succeeded`, `Failed`, or `Rejected`; `output` is merged into the
/// action's result.
#[derive(
    Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct ActionReport {
    pub action_id: String,
    pub state: ActionState,
    #[serde(default)]
    pub output: BTreeMap<String, TypedConfigValue>,
}

impl ActionReport {
    pub fn new(action_id: impl Into<String>, state: ActionState) -> Self {
        Self {
            action_id: action_id.into(),
            state,
            output: BTreeMap::new(),
        }
    }

    pub fn with_output(mut self, key: impl Into<String>, value: TypedConfigValue) -> Self {
        self.output.insert(key.into(), value);
        self
    }
}

/// Selects tracked actions. Empty filters match everything.
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
pub struct ActionQuery {
    pub action_id: Option<String>,
    pub target: Option<ActionTarget>,
}

impl ActionQuery {
    pub fn all() -> Self {
        Self::default()
    }

    pub fn action(action_id: impl Into<String>) -> Self {
        Self {
            action_id: Some(action_id.into()),
            target: None,
        }
    }

    pub fn target(target: ActionTarget) -> Self {
        Self {
            action_id: None,
            target: Some(target),
        }
    }

    pub fn matches(&self, result: &ActionResult) -> bool {
        self.action_id
            .as_deref()
            .is_none_or(|id| id == result.action_id)
            && self
                .target
                .as_ref()
                .is_none_or(|target| target == &result.target)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn targets_round_trip_through_text() {
        for text in [
            "node/node-a",
            "provider/provider.camera",
            "resource/camera.front",
            "executor/executor.engine",
        ] {
            let target: ActionTarget = text.parse().expect("target parses");
            assert_eq!(alloc::format!("{target}"), text);
        }
        assert!("workload/w".parse::<ActionTarget>().is_err());
        assert!("node/".parse::<ActionTarget>().is_err());
    }

    #[test]
    fn terminal_states_and_resource_results() {
        assert!(!ActionState::Accepted.is_terminal());
        assert!(!ActionState::Running { progress: Some(5) }.is_terminal());
        assert!(ActionState::TimedOut.is_terminal());
        let mut result = ActionResult::new(
            "a1",
            ActionTarget::Node(NodeId::new("node-a")),
            "locate",
            NodeId::new("node-a"),
            ActionState::Accepted,
        );
        assert_eq!(result.as_resource_action_result(), None);
        result.state = ActionState::Failed {
            reason: "led broken".into(),
        };
        let resource = result.as_resource_action_result().expect("final");
        assert_eq!(resource.status, ResourceActionStatus::Failed);
        assert_eq!(resource.error.as_deref(), Some("led broken"));
        assert_eq!(resource.action_kind, "locate");
    }

    #[test]
    fn status_entries_follow_the_key_convention() {
        let mut result = ActionResult::new(
            "u1",
            ActionTarget::Node(NodeId::new("node-a")),
            "update",
            NodeId::new("node-a"),
            ActionState::Running {
                progress: Some(250),
            },
        );
        let entries = result.status_entries();
        assert_eq!(entries.len(), 2);
        assert_eq!(
            entries[0].subject,
            StatusSubject::Node(NodeId::new("node-a"))
        );
        assert_eq!(entries[0].key, "action.u1.state");
        assert_eq!(entries[0].value, TypedConfigValue::String("running".into()));
        assert_eq!(entries[1].key, "action.u1.progress");
        assert_eq!(entries[1].value, TypedConfigValue::UInt(250));
        result.state = ActionState::Failed {
            reason: "bad digest".into(),
        };
        let entries = result.status_entries();
        assert_eq!(entries[1].key, "action.u1.error");
    }
}
