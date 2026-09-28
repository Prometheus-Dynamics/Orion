use crate::{
    AssignedWorkload, ClientError, LocalExecutorEvent, LocalExecutorService,
    LocalExecutorSubscription, assigned_workloads_from_records,
};

/// One update of the full set of workloads assigned to a local executor.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AssignedWorkloadsUpdate {
    /// Watch event sequence; `None` for the initial bootstrap fetch.
    pub sequence: Option<u64>,
    /// Complete assigned-workload set at this point, not a delta.
    pub workloads: Vec<AssignedWorkload>,
}

impl AssignedWorkloadsUpdate {
    pub fn is_bootstrap(&self) -> bool {
        self.sequence.is_none()
    }
}

/// Assigned-workload watch built on [`LocalExecutorSubscription`].
///
/// Connection, bootstrap, and stream reconnects follow the service's
/// [`LocalServiceRetryPolicy`](crate::LocalServiceRetryPolicy). Consecutive updates carrying an
/// identical workload set (such as the daemon's initial watch event repeating the bootstrap
/// fetch, or a replay after reconnect) are suppressed.
pub struct AssignedWorkloadWatch {
    subscription: LocalExecutorSubscription,
    last: Option<Vec<AssignedWorkload>>,
}

impl AssignedWorkloadWatch {
    pub(crate) fn new(subscription: LocalExecutorSubscription) -> Self {
        Self {
            subscription,
            last: None,
        }
    }

    /// Waits for the next change to the assigned-workload set.
    ///
    /// The first call returns the bootstrap set. Errors are returned once the retry policy gives
    /// up reconnecting.
    pub async fn next(&mut self) -> Result<AssignedWorkloadsUpdate, ClientError> {
        loop {
            let (sequence, records) = match self.subscription.next_event().await? {
                LocalExecutorEvent::Bootstrap(workloads) => (None, workloads),
                LocalExecutorEvent::WorkloadsChanged {
                    sequence,
                    workloads,
                } => (Some(sequence), workloads),
            };
            let workloads = assigned_workloads_from_records(records);
            if self.last.as_ref() == Some(&workloads) {
                continue;
            }
            self.last = Some(workloads.clone());
            return Ok(AssignedWorkloadsUpdate {
                sequence,
                workloads,
            });
        }
    }

    /// Last assigned-workload set returned by [`Self::next`], if any.
    pub fn current(&self) -> Option<&[AssignedWorkload]> {
        self.last.as_deref()
    }
}

impl LocalExecutorService {
    /// Subscribes to this executor's assigned workloads as consumer-facing views.
    pub async fn watch_assigned_workloads(&self) -> Result<AssignedWorkloadWatch, ClientError> {
        Ok(AssignedWorkloadWatch::new(
            self.subscribe_workloads().await?,
        ))
    }

    /// Fetches this executor's assigned workloads once, as consumer-facing views.
    pub async fn fetch_assigned_workload_views(
        &self,
    ) -> Result<Vec<AssignedWorkload>, ClientError> {
        Ok(assigned_workloads_from_records(
            self.fetch_assigned_workloads().await?,
        ))
    }
}
