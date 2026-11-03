//! Update intents on the node (`docs/update-recovery.md`, "Data model"; `docs/device-agent.md`).
//!
//! Requesters write `UpdateIntentRecord`s as ordinary desired-state batches; this module adds the
//! two things that are specific to intents: validating the records before they are committed, and
//! an audit record for every intent written or removed. It also keeps this node's
//! `UpdateStatusRecord`s (its observed slice) in step with the intents it acts on. Delivering an
//! intent to the device manager is the next milestone (U4); until then a status only says whether
//! the node already runs the target version or still has to.

use super::{AuditEventKind, NodeApp, NodeError, desired_writes::WriteOrigin};
use orion::control_plane::{
    DesiredStateMutation, MutationBatch, NodeRecord, UpdateIntentRecord, UpdatePhase,
    UpdateStatusRecord,
};
use orion_core::UpdateIntentId;

/// The component whose installed version the node reports from its host facts: the image it booted.
const OS_COMPONENT: &str = "os";

impl NodeApp {
    /// Commits a batch of desired-state writes from a client (a local control-plane client or a
    /// remote operator allowed to request updates). Update intents in the batch are validated first
    /// and audited once committed; other mutations are committed as before.
    pub(crate) fn apply_client_mutation_batch(
        &self,
        batch: &MutationBatch,
        actor: &str,
    ) -> Result<(), NodeError> {
        for mutation in &batch.mutations {
            if let DesiredStateMutation::PutUpdateIntent(record) = mutation {
                record.validate().map_err(|why| {
                    NodeError::Config(format!("update intent `{}`: {why}", record.intent_id))
                })?;
            }
        }
        self.apply_mutation_batch(batch, WriteOrigin::Local)?;
        for mutation in &batch.mutations {
            match mutation {
                DesiredStateMutation::PutUpdateIntent(record) => self.record_audit_event(
                    AuditEventKind::UpdateIntentWritten,
                    Some(record.intent_id.to_string()),
                    format!(
                        "{actor} requested `{}` at version `{}` (generation {}, paused: {})",
                        record.component, record.target_version, record.generation, record.paused
                    ),
                ),
                DesiredStateMutation::RemoveUpdateIntent(intent_id) => self.record_audit_event(
                    AuditEventKind::UpdateIntentRemoved,
                    Some(intent_id.to_string()),
                    format!("{actor} removed the update intent (the node keeps what it runs)"),
                ),
                _ => {}
            }
        }
        Ok(())
    }

    /// Brings the update statuses of this node's intents up to date. Runs in the placement pass
    /// of every reconcile tick. Returns whether the observed slice changed.
    ///
    /// The status records are the node's own facts, so only this node writes them. Known facts
    /// only: an intent whose component is `os` is `Idle` when the image the node booted has the
    /// target version, else `Pending`. Other components have no installed-version source yet, so
    /// they stay `Pending` until the delivery loop (U4) reports progress. A status the delivery loop
    /// already owns (acting on the intent's version and generation) is left as it is.
    pub(crate) fn publish_local_update_statuses(&self) -> bool {
        let now_ms = Self::current_time_ms();
        self.with_store_mut(|store| {
            let local = store.local_node_id.clone();
            let installed_os = installed_os_version(store.observed.nodes.get(&local));
            let desired = &store.desired.update_intents;
            let observed = &mut store.observed;
            let is_local = |intent: &UpdateIntentRecord| intent.node_id == local;
            if !desired.values().any(is_local) && observed.update_statuses.is_empty() {
                return false;
            }

            let mut changed = false;
            let stale: Vec<UpdateIntentId> = observed
                .update_statuses
                .iter()
                .filter(|(id, status)| {
                    status.node_id == local && !desired.get(*id).is_some_and(is_local)
                })
                .map(|(id, _)| id.clone())
                .collect();
            for id in stale {
                observed.update_statuses.remove(&id);
                changed = true;
            }

            for intent in desired.values().filter(|intent| is_local(intent)) {
                let installed = if intent.component == OS_COMPONENT {
                    installed_os.clone()
                } else {
                    None
                };
                let candidate = status_for(
                    intent,
                    installed,
                    observed.update_statuses.get(&intent.intent_id),
                );
                if observed
                    .update_statuses
                    .get(&intent.intent_id)
                    .is_some_and(|current| same_content(current, &candidate))
                {
                    continue;
                }
                let mut next = candidate;
                next.updated_at_ms = now_ms;
                observed.put_update_status(next);
                changed = true;
            }
            changed
        })
    }
}

/// The version of the image this node booted, from its published host facts.
fn installed_os_version(record: Option<&NodeRecord>) -> Option<String> {
    let host = record?.host.as_ref()?;
    host.image_version
        .clone()
        .or_else(|| host.os_version.clone())
}

/// The status this node should report for `intent`, given what it has installed and what it
/// reported before. `updated_at_ms` is filled in by the caller when the content changed.
fn status_for(
    intent: &UpdateIntentRecord,
    installed: Option<String>,
    existing: Option<&UpdateStatusRecord>,
) -> UpdateStatusRecord {
    let mut status = UpdateStatusRecord::new(intent, UpdatePhase::Pending, 0);
    status.previous_version = existing.and_then(|current| current.previous_version.clone());
    if let Some(current) = existing
        && let Some(acting) = &current.acting_on
        && acting.target_version == intent.target_version
        && acting.generation == intent.generation
        && current.phase != UpdatePhase::Idle
    {
        // Owned by the delivery loop: keep its phase and error, refresh what is installed.
        status.acting_on = current.acting_on.clone();
        status.phase = current.phase;
        status.last_error = current.last_error.clone();
    }
    if installed.as_deref() == Some(intent.target_version.as_str()) {
        status.phase = UpdatePhase::Idle;
        status.acting_on = None;
        status.last_error = None;
    }
    status.installed_version = installed;
    status
}

/// Equal content, ignoring the timestamp.
fn same_content(left: &UpdateStatusRecord, right: &UpdateStatusRecord) -> bool {
    let mut left = left.clone();
    left.updated_at_ms = right.updated_at_ms;
    left == *right
}
