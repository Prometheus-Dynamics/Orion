//! Update intents and their observed status (`docs/update-recovery.md`, "Data model").
//!
//! An [`UpdateIntentRecord`] is desired state: it says which version of which component a node
//! should run. Its single writer is the requester (an operator, a fleet tool, a rollout
//! controller). The target node never writes it. The target reports how the update is going in an
//! [`UpdateStatusRecord`], which lives in its own observed slice and travels to peers like any
//! other observed record.
//!
//! Orion never orders version strings: `target_version` and `installed_version` are compared for
//! equality only. Everything here is opaque to Orion; the device manager gives the strings meaning.

use super::workloads::TypedConfigValue;
use super::{DesiredClusterState, ObservedClusterState};
use crate::ActionTarget;
use alloc::{format, string::String, vec::Vec};
use orion_core::{ArtifactId, NodeId, UpdateIntentId};
use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use serde::{Deserialize, Serialize};

/// Longest component name (`os`, `mcu/motor-a`, ...).
pub const MAX_UPDATE_COMPONENT_LEN: usize = 128;
/// Longest version string, in intents and in status records.
pub const MAX_UPDATE_VERSION_LEN: usize = 128;
/// Longest optional digest hint (a SHA-256 in hex is 64 characters).
pub const MAX_UPDATE_DIGEST_LEN: usize = 128;
/// Longest handler action name.
pub const MAX_UPDATE_HANDLER_NAME_LEN: usize = 64;
/// Most free-form arguments one intent may carry.
pub const MAX_UPDATE_ARGS: usize = 32;
/// Longest argument name.
pub const MAX_UPDATE_ARG_NAME_LEN: usize = 64;
/// Longest error text kept in a status record.
pub const MAX_UPDATE_ERROR_LEN: usize = 512;
/// Longest drain timeout an intent may ask for (24 hours).
pub const MAX_UPDATE_DRAIN_TIMEOUT_MS: u64 = 24 * 60 * 60 * 1_000;

/// The intent id of `component` on `node_id`: `<node_id>/<component>`.
pub fn update_intent_id(node_id: &NodeId, component: &str) -> UpdateIntentId {
    UpdateIntentId::new(format!("{node_id}/{component}"))
}

/// Who performs an update and how the request reaches it.
#[derive(
    Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub enum UpdateHandler {
    /// Delivered as an action (`name`, with the intent's arguments) to `target`.
    Action { target: ActionTarget, name: String },
}

/// Whether and how a node drains its workloads before it applies an update.
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
pub enum DrainPolicy {
    /// Do not change maintenance mode.
    None,
    /// Stop new placements (`schedulable = false`) and keep running workloads.
    Cordon,
    /// Cordon, then drain placement-managed workloads, waiting at most `timeout_ms`.
    Drain { timeout_ms: u64 },
}

/// Desired state: install `target_version` of `component` on `node_id`.
///
/// Keyed by [`update_intent_id`]. Changing `generation` asks for a retry of the same version
/// without a version change; the target acts on `(target_version, generation)`.
#[derive(
    Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct UpdateIntentRecord {
    pub intent_id: UpdateIntentId,
    /// The target node.
    pub node_id: NodeId,
    /// Opaque component name, for example `os` or `mcu/motor-a`.
    pub component: String,
    /// Opaque version, compared for equality only.
    pub target_version: String,
    /// The artifact to install, when the requester names one.
    pub artifact: Option<ArtifactId>,
    /// Optional digest hint; the device manager verifies its own way.
    pub expected_digest: Option<String>,
    pub handler: UpdateHandler,
    /// Free-form arguments, passed to the handler unchanged.
    pub args: Vec<(String, TypedConfigValue)>,
    pub generation: u64,
    pub drain: DrainPolicy,
    /// A paused intent is kept but not delivered.
    pub paused: bool,
}

impl UpdateIntentRecord {
    /// An intent for `component` on `node_id` at `target_version`, generation 1, delivered to the
    /// node's own `update` action, not drained and not paused.
    pub fn new(
        node_id: impl Into<NodeId>,
        component: impl Into<String>,
        target_version: impl Into<String>,
    ) -> Self {
        let node_id = node_id.into();
        let component = component.into();
        Self {
            intent_id: update_intent_id(&node_id, &component),
            handler: UpdateHandler::Action {
                target: ActionTarget::Node(node_id.clone()),
                name: "update".into(),
            },
            node_id,
            component,
            target_version: target_version.into(),
            artifact: None,
            expected_digest: None,
            args: Vec::new(),
            generation: 1,
            drain: DrainPolicy::None,
            paused: false,
        }
    }

    /// Checks the record's shape and bounds. Called before an intent is written, so a remote
    /// requester cannot make the state or the observed slices grow without limit.
    pub fn validate(&self) -> Result<(), String> {
        if self.intent_id != update_intent_id(&self.node_id, &self.component) {
            return Err(format!(
                "update intent id `{}` must be `<node>/<component>` (`{}/{}`)",
                self.intent_id, self.node_id, self.component
            ));
        }
        check_text("component", &self.component, MAX_UPDATE_COMPONENT_LEN, true)?;
        check_text(
            "target version",
            &self.target_version,
            MAX_UPDATE_VERSION_LEN,
            true,
        )?;
        if let Some(digest) = &self.expected_digest {
            check_text("expected digest", digest, MAX_UPDATE_DIGEST_LEN, true)?;
        }
        let UpdateHandler::Action { name, .. } = &self.handler;
        check_text("handler action", name, MAX_UPDATE_HANDLER_NAME_LEN, true)?;
        if self.args.len() > MAX_UPDATE_ARGS {
            return Err(format!(
                "an update intent carries at most {MAX_UPDATE_ARGS} arguments"
            ));
        }
        let mut names: Vec<&str> = Vec::with_capacity(self.args.len());
        for (name, _) in &self.args {
            check_text("argument name", name, MAX_UPDATE_ARG_NAME_LEN, true)?;
            if names.contains(&name.as_str()) {
                return Err(format!("duplicate update argument `{name}`"));
            }
            names.push(name);
        }
        if let DrainPolicy::Drain { timeout_ms } = self.drain
            && (timeout_ms == 0 || timeout_ms > MAX_UPDATE_DRAIN_TIMEOUT_MS)
        {
            return Err(format!(
                "drain timeout must be 1..={MAX_UPDATE_DRAIN_TIMEOUT_MS} ms"
            ));
        }
        Ok(())
    }
}

/// The version and generation a node is currently acting on.
#[derive(
    Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct UpdateActing {
    pub target_version: String,
    pub generation: u64,
}

/// Coarse phase of one update, as the target node reports it.
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
pub enum UpdatePhase {
    /// Installed version equals the target: nothing to do.
    Idle,
    /// The intent is known but not delivered yet (paused, gated, draining, or no handler yet).
    Pending,
    /// The handler accepted the request.
    Delivered,
    /// Downloading, verifying or writing the inactive slot.
    Applying,
    /// Written; waiting for the switch and reboot.
    AwaitingReboot,
    /// Booted the new version in trial mode; health checks are running.
    Verifying,
    /// Marked good; the installed version is the target.
    Committed,
    /// The handler gave up; nothing changed, or the change was undone.
    Failed,
    /// The old version is active again.
    RolledBack,
}

/// Observed state: how far `node_id` got with the intent `intent_id`. Single writer: the node.
#[derive(
    Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct UpdateStatusRecord {
    pub intent_id: UpdateIntentId,
    pub node_id: NodeId,
    pub component: String,
    /// The version and generation the node acts on, when it acts on one.
    pub acting_on: Option<UpdateActing>,
    pub phase: UpdatePhase,
    /// The version that runs now.
    pub installed_version: Option<String>,
    /// The version a rollback would return to.
    pub previous_version: Option<String>,
    pub last_error: Option<String>,
    /// When this status was last written (the node's wall clock, milliseconds since the epoch).
    pub updated_at_ms: u64,
}

impl UpdateStatusRecord {
    /// A status for `intent` in `phase`, with nothing else known yet.
    pub fn new(intent: &UpdateIntentRecord, phase: UpdatePhase, updated_at_ms: u64) -> Self {
        Self {
            intent_id: intent.intent_id.clone(),
            node_id: intent.node_id.clone(),
            component: intent.component.clone(),
            acting_on: None,
            phase,
            installed_version: None,
            previous_version: None,
            last_error: None,
            updated_at_ms,
        }
    }

    /// Checks bounds; the node validates its own statuses before it publishes them.
    pub fn validate(&self) -> Result<(), String> {
        check_text("component", &self.component, MAX_UPDATE_COMPONENT_LEN, true)?;
        if let Some(acting) = &self.acting_on {
            check_text(
                "acting version",
                &acting.target_version,
                MAX_UPDATE_VERSION_LEN,
                true,
            )?;
        }
        for (label, value) in [
            ("installed version", &self.installed_version),
            ("previous version", &self.previous_version),
        ] {
            if let Some(value) = value {
                check_text(label, value, MAX_UPDATE_VERSION_LEN, true)?;
            }
        }
        if let Some(error) = &self.last_error {
            check_text("last error", error, MAX_UPDATE_ERROR_LEN, false)?;
        }
        Ok(())
    }
}

/// An update intent with the status its node last reported (what `orionctl get updates` shows).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct UpdateIntentView {
    pub intent: UpdateIntentRecord,
    /// `None` until the target node has reported on the intent.
    pub status: Option<UpdateStatusRecord>,
}

/// Every update intent of `desired`, each with the status its node reported in `observed`.
pub fn update_intent_views(
    desired: &DesiredClusterState,
    observed: &ObservedClusterState,
) -> Vec<UpdateIntentView> {
    desired
        .update_intents
        .values()
        .map(|intent| UpdateIntentView {
            intent: intent.clone(),
            status: observed.update_statuses.get(&intent.intent_id).cloned(),
        })
        .collect()
}

/// Non-empty (when `required`), at most `max` bytes, and free of control characters.
fn check_text(label: &str, value: &str, max: usize, required: bool) -> Result<(), String> {
    if required && value.trim().is_empty() {
        return Err(format!("update {label} must not be empty"));
    }
    if value.len() > max {
        return Err(format!("update {label} is longer than {max} bytes"));
    }
    if value.chars().any(char::is_control) {
        return Err(format!("update {label} contains a control character"));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloc::vec;
    use orion_core::{decode_from_slice, encode_to_vec};

    fn intent() -> UpdateIntentRecord {
        let mut record = UpdateIntentRecord::new("node-a", "os", "2.0");
        record.artifact = Some(ArtifactId::new("os-2.0"));
        record.expected_digest = Some("ab".repeat(32));
        record.args = vec![
            (
                "image_url".into(),
                TypedConfigValue::String("http://x/y.img".into()),
            ),
            ("size".into(), TypedConfigValue::UInt(7)),
        ];
        record.generation = 3;
        record.drain = DrainPolicy::Drain { timeout_ms: 5_000 };
        record.paused = true;
        record
    }

    #[test]
    fn intent_id_is_node_and_component() {
        let record = UpdateIntentRecord::new("node-a", "mcu/motor-a", "1");
        assert_eq!(record.intent_id.as_str(), "node-a/mcu/motor-a");
        assert!(record.validate().is_ok());
    }

    #[test]
    fn intent_round_trips_through_rkyv_and_serde() {
        let record = intent();
        let bytes = encode_to_vec(&record).expect("encode");
        let decoded: UpdateIntentRecord = decode_from_slice(&bytes).expect("decode");
        assert_eq!(decoded, record);
        let json = serde_json::to_string(&record).expect("json");
        let back: UpdateIntentRecord = serde_json::from_str(&json).expect("json decode");
        assert_eq!(back, record);
    }

    #[test]
    fn status_round_trips() {
        let mut status = UpdateStatusRecord::new(&intent(), UpdatePhase::Failed, 42);
        status.acting_on = Some(UpdateActing {
            target_version: "2.0".into(),
            generation: 3,
        });
        status.installed_version = Some("1.0".into());
        status.previous_version = Some("1.0".into());
        status.last_error = Some("digest mismatch".into());
        assert!(status.validate().is_ok());
        let bytes = encode_to_vec(&status).expect("encode");
        let decoded: UpdateStatusRecord = decode_from_slice(&bytes).expect("decode");
        assert_eq!(decoded, status);
    }

    #[test]
    fn validation_refuses_out_of_bounds_and_inconsistent_intents() {
        let mut bad_id = intent();
        bad_id.intent_id = UpdateIntentId::new("node-b/os");
        assert!(
            bad_id
                .validate()
                .unwrap_err()
                .contains("<node>/<component>")
        );

        let mut empty_version = intent();
        empty_version.target_version = "  ".into();
        assert!(empty_version.validate().is_err());

        let mut long_version = intent();
        long_version.target_version = "v".repeat(MAX_UPDATE_VERSION_LEN + 1);
        assert!(long_version.validate().unwrap_err().contains("longer than"));

        let mut too_many = intent();
        too_many.args = (0..=MAX_UPDATE_ARGS)
            .map(|index| (format!("a{index}"), TypedConfigValue::Bool(true)))
            .collect();
        assert!(too_many.validate().unwrap_err().contains("at most"));

        let mut duplicate = intent();
        duplicate
            .args
            .push(("size".into(), TypedConfigValue::UInt(9)));
        assert!(duplicate.validate().unwrap_err().contains("duplicate"));

        let mut no_timeout = intent();
        no_timeout.drain = DrainPolicy::Drain { timeout_ms: 0 };
        assert!(no_timeout.validate().is_err());

        let mut control = intent();
        control.component = "os\n".into();
        control.intent_id = update_intent_id(&control.node_id, &control.component);
        assert!(
            control
                .validate()
                .unwrap_err()
                .contains("control character")
        );
    }
}
