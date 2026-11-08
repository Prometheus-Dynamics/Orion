//! Safe mode (`docs/update-recovery.md`, "Safe mode"): the node starts so that it can be inspected
//! and repaired, and does no harm meanwhile.
//!
//! In safe mode the node runs in `Maintenance` with no allowed workloads (so it is not
//! schedulable and starts nothing), outbound peer sync is off (peers may still push and be pulled
//! from, see `peer_sync_receive_only`), and health reports `safe_mode: <cause>`. Local IPC, health,
//! and observability keep working. The cause is persisted in `safe-mode.txt` in the state
//! directory, so a restart stays in safe mode until the operator runs `orionctl maintenance exit`
//! and restarts the node.

use super::{NodeApp, NodeError};
use crate::storage::NodeStorage;
use orion::control_plane::{
    MaintenanceMode, MaintenanceState, StatusEntry, StatusSubject, TypedConfigValue,
};
use std::path::PathBuf;
use tracing::warn;

/// Why the node is in safe mode.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SafeModeCause {
    /// The node identity key could not be read. Peer sync is off until the key is repaired.
    IdentityUnreadable { error: String },
    /// Persisted state could not be decoded. It was moved to `quarantine` and the node started with
    /// an empty desired state. (A state directory in another snapshot format is not safe mode: see
    /// `unsupported-format-<unix ms>/` in `docs/node-env.md`.)
    StateQuarantined { reason: String, quarantine: PathBuf },
    /// `ORION_NODE_SAFE_MODE` is set.
    Requested,
    /// An earlier start entered safe mode and no operator has cleared it yet. Holds the cause.
    Persisted(String),
}

impl SafeModeCause {
    /// A short, fixed name of the cause, for the `cause` label of `orion_safe_mode`.
    pub fn kind(&self) -> &'static str {
        match self {
            Self::IdentityUnreadable { .. } => "identity_unreadable",
            Self::StateQuarantined { .. } => "state_quarantined",
            Self::Requested => "requested",
            Self::Persisted(_) => "persisted",
        }
    }

    /// The cause as shown after `safe_mode: ` in health reasons, and as persisted in the marker.
    pub fn describe(&self) -> String {
        match self {
            Self::StateQuarantined { reason, quarantine } => {
                format!("state quarantined to {}: {reason}", quarantine.display())
            }
            Self::IdentityUnreadable { error } => format!("node identity unreadable: {error}"),
            Self::Requested => "requested by ORION_NODE_SAFE_MODE".to_owned(),
            Self::Persisted(cause) => cause.clone(),
        }
    }
}

/// Moves the named state files aside after they failed to load and describes why. Nothing is
/// deleted.
pub(super) fn quarantine_files(
    storage: &NodeStorage,
    names: &[&str],
    reason: String,
) -> Result<SafeModeCause, NodeError> {
    let quarantine = storage
        .quarantine_files(names)?
        .unwrap_or_else(|| storage.root().to_path_buf());
    Ok(SafeModeCause::StateQuarantined { reason, quarantine })
}

/// A marker left by a build that treated a newer snapshot format as damage. No build does now,
/// so the marker no longer means anything and must not keep the node in safe mode.
pub(super) fn is_stale_format_marker(marker: &str) -> bool {
    marker.contains("is newer than this build reads")
}

impl NodeApp {
    /// Says in status which stores a different build left that this one runs without.
    pub(super) fn publish_unreadable_stores(&self, state_root: bool, trust_store: bool) {
        let subject = StatusSubject::Node(self.config.node_id.clone());
        let entries: Vec<StatusEntry> = [
            ("state.root_unreadable", state_root),
            ("auth.trust_store_unreadable", trust_store),
        ]
        .into_iter()
        .filter(|(_, unreadable)| *unreadable)
        .map(|(key, _)| StatusEntry::new(subject.clone(), key, TypedConfigValue::Bool(true)))
        .collect();
        if !entries.is_empty()
            && let Err(error) = self.publish_status_as("node:state", entries)
        {
            warn!(%error, "failed to publish which stores could not be read");
        }
    }

    /// The cause of safe mode, when this process runs in it.
    pub fn safe_mode(&self) -> Option<&SafeModeCause> {
        self.safe_mode.as_ref()
    }

    /// The `orion_safe_mode` family for the metrics endpoint (`docs/observability.md`).
    #[cfg_attr(not(feature = "transport-http"), allow(dead_code))]
    pub(crate) fn safe_mode_metrics(&self) -> String {
        orion::control_plane::render_safe_mode_metrics(
            &self.config.node_id,
            self.safe_mode.as_ref().map(SafeModeCause::kind),
        )
    }

    /// In safe mode peers may still push to the node and it pulls from them, but it never pushes
    /// its own state or observed slice, so a stale copy cannot overwrite peers' newer state.
    #[cfg_attr(not(peer_sync), allow(dead_code))]
    pub(crate) fn peer_sync_receive_only(&self) -> bool {
        self.safe_mode.is_some()
    }

    /// Outbound peer sync is off when the node identity could not be read.
    #[cfg_attr(not(peer_sync), allow(dead_code))]
    pub(crate) fn peer_sync_disabled(&self) -> bool {
        self.security.identity_error().is_some()
    }

    /// Enters safe mode: forces `Maintenance` with no allowed workloads in memory (not persisted),
    /// and writes the marker so the next start stays in safe mode until cleared.
    pub(super) fn activate_safe_mode(&mut self, cause: SafeModeCause) {
        warn!(
            node = %self.config.node_id,
            cause = %cause.describe(),
            "starting orion-node in safe mode"
        );
        if let Some(storage) = self.storage.as_ref()
            && let Err(err) = storage.save_safe_mode_marker(&cause.describe())
        {
            warn!(node = %self.config.node_id, error = %err, "could not persist the safe mode marker");
        }
        let forced = MaintenanceState {
            mode: MaintenanceMode::Maintenance,
            allow_runtime_types: Vec::new(),
            allow_workload_ids: Vec::new(),
        };
        *self.maintenance_state_lock() = forced.clone();
        self.with_store_mut(|store| store.set_maintenance(forced));
        self.safe_mode = Some(cause);
    }
}
