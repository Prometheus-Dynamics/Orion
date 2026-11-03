//! Warn-once bookkeeping for local workloads that no local executor can start or stop.
//!
//! `Runtime::reconcile` skips such a workload rather than failing the pass, so one missing
//! executor cannot hold back the rest. Its log follows one rule per `(workload, runtime type)`
//! pair: the first sighting logs at warn, later sightings at debug on an exponential back-off.
//! A pair that stops being unsupported is forgotten, so a later failure warns again.
//!
//! Each pass that needs this scan borrows from the store and reuses the map's entries, so the
//! steady state allocates nothing; only a new pair clones its ids.

use super::NodeApp;
use orion::runtime::{LocalRuntimeStore, ReconcileReport};
use orion::{NodeId, RuntimeType, WorkloadId};
use std::collections::BTreeMap;
use std::time::{Duration, Instant};
use tracing::{debug, warn};

const DEBUG_BACKOFF_FIRST: Duration = Duration::from_secs(30);
const DEBUG_BACKOFF_MAX: Duration = Duration::from_secs(600);

#[derive(Default)]
pub(in crate::app) struct UnsupportedWarnings {
    /// Bumped by every scan. An entry not touched by the latest scan is no longer unsupported.
    generation: u64,
    entries: BTreeMap<WorkloadId, WarnedWorkload>,
}

struct WarnedWorkload {
    runtime_type: RuntimeType,
    generation: u64,
    next_debug_at: Instant,
    backoff: Duration,
}

impl UnsupportedWarnings {
    pub(in crate::app) fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    fn observe<'a>(
        &mut self,
        node_id: &NodeId,
        now: Instant,
        found: impl Iterator<Item = (&'a WorkloadId, &'a RuntimeType, bool)>,
    ) {
        self.generation += 1;
        let generation = self.generation;
        for (workload_id, runtime_type, executor_on_other_node) in found {
            if let Some(entry) = self.entries.get_mut(workload_id)
                && entry.runtime_type == *runtime_type
            {
                if entry.generation != generation {
                    entry.generation = generation;
                    if now >= entry.next_debug_at {
                        entry.backoff = (entry.backoff * 2).min(DEBUG_BACKOFF_MAX);
                        entry.next_debug_at = now + entry.backoff;
                        debug!(
                            node = %node_id,
                            workload = %workload_id,
                            runtime_type = %runtime_type,
                            next_in_secs = entry.backoff.as_secs(),
                            "workload still has no local executor for its runtime type"
                        );
                    }
                }
                continue;
            }

            warn!(
                node = %node_id,
                workload = %workload_id,
                runtime_type = %runtime_type,
                executor_on_other_node,
                "no local executor realizes this workload's runtime type; skipping it until one registers"
            );
            self.entries.insert(
                workload_id.clone(),
                WarnedWorkload {
                    runtime_type: runtime_type.clone(),
                    generation,
                    next_debug_at: now + DEBUG_BACKOFF_FIRST,
                    backoff: DEBUG_BACKOFF_FIRST,
                },
            );
        }
        self.entries
            .retain(|_, entry| entry.generation == generation);
    }
}

impl NodeApp {
    /// Logs this pass's workloads that no local executor can run, under the warn-once rule.
    /// Called with the store read guard held, right after `reconcile`. A pass with nothing
    /// unsupported and nothing remembered does no work.
    pub(in crate::app) fn note_unsupported_workloads(
        &self,
        store: &LocalRuntimeStore,
        report: &ReconcileReport,
    ) {
        let mut warnings = self
            .state
            .reconcile
            .unsupported
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if report.unsupported_runtime_count == 0 && warnings.is_empty() {
            return;
        }
        warnings.observe(
            &self.config.node_id,
            Instant::now(),
            self.runtime.unsupported_workloads(store).map(|workload| {
                (
                    workload.workload_id,
                    workload.runtime_type,
                    workload.executor_on_other_node,
                )
            }),
        );
    }
}
