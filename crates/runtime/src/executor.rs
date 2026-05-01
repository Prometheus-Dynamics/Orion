use orion_control_plane::{ExecutorRecord, ResourceBinding, ResourceRecord, WorkloadRecord};
use orion_core::{ExecutorId, WorkloadId};

use crate::RuntimeError;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WorkloadPlan {
    pub executor_id: ExecutorId,
    pub workload: WorkloadRecord,
    pub resource_bindings: Vec<ResourceBinding>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ExecutorSnapshot {
    pub executor: ExecutorRecord,
    pub workloads: Vec<WorkloadRecord>,
    pub resources: Vec<ResourceRecord>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ExecutorCommand {
    Start(WorkloadPlan),
    Stop {
        executor_id: ExecutorId,
        workload_id: WorkloadId,
    },
}

pub trait ExecutorIntegration {
    /// Returns the executor's control-plane identity record.
    ///
    /// This method is called synchronously from node runtime code and should not perform blocking
    /// I/O or expensive computation.
    fn executor_record(&self) -> ExecutorRecord;

    /// Captures the executor's current local snapshot.
    ///
    /// This method runs inline during reconcile state collection. Implementations should treat it
    /// as a cheap, synchronous snapshot read and avoid filesystem, network, or long-running work.
    fn snapshot(&self) -> ExecutorSnapshot;

    /// Validates whether a workload is acceptable for this executor.
    ///
    /// This method is invoked synchronously while validating desired state and should remain
    /// CPU-bounded and side-effect free.
    fn validate_workload(&self, _workload: &WorkloadRecord) -> Result<(), RuntimeError> {
        Ok(())
    }

    /// Applies a reconcile command to the executor.
    ///
    /// This method is currently called synchronously from the node reconcile loop. Implementations
    /// must keep it fast and non-blocking until the execution contract becomes async-aware.
    fn apply_command(&self, _command: &ExecutorCommand) -> Result<(), RuntimeError> {
        Ok(())
    }
}

pub trait ExecutorDescriptor {
    /// Returns the executor's control-plane identity record.
    fn executor_record(&self) -> ExecutorRecord;

    /// Returns workloads currently observed by the executor.
    ///
    /// This method is used to build [`ExecutorIntegration::snapshot`] synchronously and should
    /// avoid blocking I/O.
    fn workloads(&self) -> Vec<WorkloadRecord> {
        Vec::new()
    }

    /// Returns resources currently observed by the executor.
    ///
    /// This method is used to build [`ExecutorIntegration::snapshot`] synchronously and should
    /// avoid blocking I/O.
    fn resources(&self) -> Vec<ResourceRecord> {
        Vec::new()
    }

    /// Validates whether a workload is acceptable for this executor.
    ///
    /// Descriptor-based implementations should keep this logic deterministic and side-effect free.
    fn validate_workload(&self, _workload: &WorkloadRecord) -> Result<(), RuntimeError> {
        Ok(())
    }
}

impl<T> ExecutorIntegration for T
where
    T: ExecutorDescriptor,
{
    fn executor_record(&self) -> ExecutorRecord {
        ExecutorDescriptor::executor_record(self)
    }

    fn snapshot(&self) -> ExecutorSnapshot {
        ExecutorSnapshot {
            executor: self.executor_record(),
            workloads: self.workloads(),
            resources: self.resources(),
        }
    }

    fn validate_workload(&self, workload: &WorkloadRecord) -> Result<(), RuntimeError> {
        ExecutorDescriptor::validate_workload(self, workload)
    }
}
