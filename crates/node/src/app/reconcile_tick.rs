use super::{NodeApp, NodeError, NodeTickReport};
use orion::runtime::{
    ExecutorCommand, ExecutorIntegration, ExecutorSnapshot, ProviderSnapshot, ReconcileReport,
};
use std::{collections::BTreeMap, sync::Arc};
use tracing::info_span;

struct CollectedRuntimeState {
    executors: Vec<Arc<dyn ExecutorIntegration + Send + Sync>>,
    provider_snapshots: Vec<ProviderSnapshot>,
    executor_snapshots: Vec<ExecutorSnapshot>,
}

/// What one reconcile pass changed, used to decide whether the pass is worth an observability
/// event and whether the loop should follow up with another pass.
#[derive(Clone, Copy, Debug, Default)]
pub(super) struct ReconcileOutcome {
    /// In-process provider/executor snapshots changed the runtime store.
    pub(super) runtime_changed: bool,
    /// The applied revision advanced to the desired revision.
    pub(super) applied_revision_changed: bool,
    /// Commands delivered to in-process executor integrations.
    pub(super) dispatched_commands: usize,
}

impl ReconcileOutcome {
    pub(super) fn changed(&self) -> bool {
        self.runtime_changed || self.applied_revision_changed || self.dispatched_commands > 0
    }

    /// In-process integrations do not publish their changes, so after they changed or received
    /// commands the next pass is what observes the result. IPC-published state wakes the loop on
    /// its own, and commands for IPC executors are only re-derived, never dispatched, here.
    fn needs_follow_up(&self) -> bool {
        self.runtime_changed || self.dispatched_commands > 0
    }
}

impl NodeApp {
    pub async fn tick_async(&self) -> Result<NodeTickReport, NodeError> {
        let started = std::time::Instant::now();
        self.state.reconcile.begin_pass();
        let collected = {
            let _span = info_span!("reconcile", node = %self.config.node_id).entered();
            self.collect_runtime_state()?
        };
        let runtime_changed = self.apply_runtime_snapshots(&collected)?;
        let reconcile = {
            let store = self.store_read();
            self.runtime.reconcile(&store)?
        };

        let result = self
            .apply_reconcile_report_async(&collected.executors, reconcile, runtime_changed)
            .await;
        self.finish_reconcile_pass(started, result)
    }

    pub fn tick(&self) -> Result<NodeTickReport, NodeError> {
        let _span = info_span!("reconcile", node = %self.config.node_id).entered();
        let started = std::time::Instant::now();
        self.state.reconcile.begin_pass();
        let collected = self.collect_runtime_state()?;
        let runtime_changed = self.apply_runtime_snapshots(&collected)?;

        let reconcile = {
            let store = self.store_read();
            self.runtime.reconcile(&store)?
        };

        let result = self.apply_reconcile_report(&collected.executors, reconcile, runtime_changed);
        self.finish_reconcile_pass(started, result)
    }

    fn finish_reconcile_pass(
        &self,
        started: std::time::Instant,
        result: Result<(NodeTickReport, ReconcileOutcome), NodeError>,
    ) -> Result<NodeTickReport, NodeError> {
        match result {
            Ok((report, outcome)) => {
                self.record_reconcile_success(started.elapsed(), &outcome, report.commands.len());
                if outcome.needs_follow_up() {
                    self.request_reconcile();
                }
                Ok(report)
            }
            Err(err) => {
                self.record_reconcile_failure(started.elapsed(), &err);
                Err(err)
            }
        }
    }

    /// Delivers commands to in-process executors and advances the applied revision. Returns the
    /// tick report plus what the pass changed.
    fn dispatch_reconcile_report(
        &self,
        executors: &[Arc<dyn ExecutorIntegration + Send + Sync>],
        report: ReconcileReport,
        runtime_changed: bool,
    ) -> Result<(NodeTickReport, ReconcileOutcome), NodeError> {
        let executors_by_id = executor_index(executors);
        let mut outcome = ReconcileOutcome {
            runtime_changed,
            ..ReconcileOutcome::default()
        };
        for command in &report.commands {
            if let Some(executor) = executor_for_command(&executors_by_id, command) {
                executor.apply_command(command)?;
                outcome.dispatched_commands += 1;
            }
        }

        self.with_store_mut(|store| {
            if store.applied.revision != report.desired_revision {
                store.mark_applied_revision(report.desired_revision);
                outcome.applied_revision_changed = true;
            }
        });

        Ok((
            NodeTickReport {
                local_node_id: report.local_node_id,
                desired_revision: report.desired_revision,
                applied_revision: self.store_read().applied.revision,
                commands: report.commands,
            },
            outcome,
        ))
    }

    async fn apply_reconcile_report_async(
        &self,
        executors: &[Arc<dyn ExecutorIntegration + Send + Sync>],
        report: ReconcileReport,
        runtime_changed: bool,
    ) -> Result<(NodeTickReport, ReconcileOutcome), NodeError> {
        // Executor integrations are still synchronous. Async reconcile only makes persistence
        // asynchronous after command application; executor callbacks themselves must stay cheap.
        // Command application is intentionally serialized by reconcile order for this release.
        let (report, outcome) =
            self.dispatch_reconcile_report(executors, report, runtime_changed)?;
        if outcome.runtime_changed || outcome.applied_revision_changed {
            self.persist_observed_state_async().await?;
        }
        Ok((report, outcome))
    }

    fn apply_reconcile_report(
        &self,
        executors: &[Arc<dyn ExecutorIntegration + Send + Sync>],
        report: ReconcileReport,
        runtime_changed: bool,
    ) -> Result<(NodeTickReport, ReconcileOutcome), NodeError> {
        let (report, outcome) =
            self.dispatch_reconcile_report(executors, report, runtime_changed)?;
        if outcome.runtime_changed || outcome.applied_revision_changed {
            self.persist_observed_state()?;
        }
        Ok((report, outcome))
    }

    fn collect_runtime_state(&self) -> Result<CollectedRuntimeState, NodeError> {
        // Provider/executor snapshot collection is intentionally synchronous today. Integration
        // implementations are expected to treat these callbacks as cheap local reads. The work is
        // serialized, but it happens before the runtime-store mutation phase so expensive
        // integrations do not stall while holding shared-state write access.
        let providers: Vec<_> = self.providers_read().values().cloned().collect();
        let executors: Vec<_> = self.executors_read().values().cloned().collect();
        let provider_snapshots = providers
            .iter()
            .map(|provider| provider.snapshot())
            .collect();
        let executor_snapshots = executors
            .iter()
            .map(|executor| executor.snapshot())
            .collect();

        Ok(CollectedRuntimeState {
            executors,
            provider_snapshots,
            executor_snapshots,
        })
    }

    fn apply_runtime_snapshots(
        &self,
        collected: &CollectedRuntimeState,
    ) -> Result<bool, NodeError> {
        self.with_store_mut(|store| -> Result<bool, NodeError> {
            let mut changed = false;
            for provider_snapshot in &collected.provider_snapshots {
                changed |= store.apply_provider_snapshot(provider_snapshot.clone())?;
            }
            for executor_snapshot in &collected.executor_snapshots {
                changed |= store.apply_executor_snapshot(executor_snapshot.clone())?;
            }
            Ok(changed)
        })
    }
}

fn executor_index(
    executors: &[Arc<dyn ExecutorIntegration + Send + Sync>],
) -> BTreeMap<orion::ExecutorId, &Arc<dyn ExecutorIntegration + Send + Sync>> {
    executors
        .iter()
        .map(|executor| (executor.executor_record().executor_id, executor))
        .collect()
}

fn executor_for_command<'a>(
    executors_by_id: &'a BTreeMap<
        orion::ExecutorId,
        &'a Arc<dyn ExecutorIntegration + Send + Sync>,
    >,
    command: &ExecutorCommand,
) -> Option<&'a Arc<dyn ExecutorIntegration + Send + Sync>> {
    let executor_id = match command {
        ExecutorCommand::Start(plan) => &plan.executor_id,
        ExecutorCommand::Stop { executor_id, .. } => executor_id,
    };
    executors_by_id.get(executor_id).copied()
}
