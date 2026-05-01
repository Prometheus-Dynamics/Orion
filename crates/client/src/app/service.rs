use std::{collections::VecDeque, future::Future, time::Duration};

use orion_control_plane::{
    ClientEventKind, ExecutorRecord, LeaseRecord, ProviderRecord, StateSnapshot, WorkloadRecord,
};
use orion_core::Revision;
use tokio::time::sleep;

use crate::{
    ClientError, ControlPlaneEventStream, ExecutorEventStream, LocalNodeRuntime,
    ProviderEventStream,
};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct LocalServiceRetryPolicy {
    delay: Duration,
    max_attempts: Option<usize>,
}

impl LocalServiceRetryPolicy {
    pub const fn no_retry() -> Self {
        Self {
            delay: Duration::from_millis(0),
            max_attempts: Some(1),
        }
    }

    pub const fn fixed_delay(delay: Duration) -> Self {
        Self {
            delay,
            max_attempts: None,
        }
    }

    pub const fn with_max_attempts(mut self, max_attempts: usize) -> Self {
        self.max_attempts = Some(max_attempts);
        self
    }

    async fn retry<F, Fut, T>(&self, mut operation: F) -> Result<T, ClientError>
    where
        F: FnMut() -> Fut,
        Fut: Future<Output = Result<T, ClientError>>,
    {
        let mut attempts = 0usize;
        loop {
            match operation().await {
                Ok(value) => return Ok(value),
                Err(error) => {
                    attempts = attempts.saturating_add(1);
                    if matches!(self.max_attempts, Some(max) if attempts >= max) {
                        return Err(error);
                    }
                    if !self.delay.is_zero() {
                        sleep(self.delay).await;
                    }
                }
            }
        }
    }
}

impl Default for LocalServiceRetryPolicy {
    fn default() -> Self {
        Self::no_retry()
    }
}

#[derive(Clone, Debug)]
pub struct LocalExecutorService {
    runtime: LocalNodeRuntime,
    client_name: String,
    executor: ExecutorRecord,
    retry_policy: LocalServiceRetryPolicy,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum LocalExecutorEvent {
    Bootstrap(Vec<WorkloadRecord>),
    WorkloadsChanged {
        sequence: u64,
        workloads: Vec<WorkloadRecord>,
    },
}

pub struct LocalExecutorSubscription {
    service: LocalExecutorService,
    stream: ExecutorEventStream,
    pending: VecDeque<LocalExecutorEvent>,
}

impl LocalExecutorService {
    pub fn new(
        runtime: LocalNodeRuntime,
        client_name: impl Into<String>,
        executor: ExecutorRecord,
    ) -> Self {
        Self {
            runtime,
            client_name: client_name.into(),
            executor,
            retry_policy: LocalServiceRetryPolicy::default(),
        }
    }

    pub fn with_retry_policy(mut self, retry_policy: LocalServiceRetryPolicy) -> Self {
        self.retry_policy = retry_policy;
        self
    }

    pub fn runtime(&self) -> &LocalNodeRuntime {
        &self.runtime
    }

    pub fn client_name(&self) -> &str {
        self.client_name.as_str()
    }

    pub fn executor(&self) -> &ExecutorRecord {
        &self.executor
    }

    pub async fn register(&self) -> Result<(), ClientError> {
        self.retry_policy
            .retry(|| async {
                self.runtime
                    .executor(self.client_name.clone(), self.executor.clone())?
                    .register()
                    .await
            })
            .await
    }

    pub async fn fetch_assigned_workloads(&self) -> Result<Vec<WorkloadRecord>, ClientError> {
        self.retry_policy
            .retry(|| async {
                self.runtime
                    .executor(self.client_name.clone(), self.executor.clone())?
                    .fetch_assigned_workloads()
                    .await
            })
            .await
    }

    pub async fn subscribe_workloads(&self) -> Result<LocalExecutorSubscription, ClientError> {
        let bootstrap = self.fetch_assigned_workloads().await?;
        let stream = self.connect_workload_stream().await?;
        Ok(LocalExecutorSubscription {
            service: self.clone(),
            stream,
            pending: VecDeque::from([LocalExecutorEvent::Bootstrap(bootstrap)]),
        })
    }

    async fn connect_workload_stream(&self) -> Result<ExecutorEventStream, ClientError> {
        let stream_name = format!("{}-watch", self.client_name);
        self.retry_policy
            .retry(|| async {
                let mut stream = self.runtime.executor_stream(stream_name.clone()).await?;
                stream.subscribe_workloads(&self.executor).await?;
                Ok(stream)
            })
            .await
    }
}

impl LocalExecutorSubscription {
    pub async fn next_event(&mut self) -> Result<LocalExecutorEvent, ClientError> {
        if let Some(event) = self.pending.pop_front() {
            return Ok(event);
        }

        loop {
            match self.stream.next_events().await {
                Ok(events) => {
                    for event in events {
                        if let ClientEventKind::ExecutorWorkloads {
                            executor_id,
                            workloads,
                        } = event.event
                            && executor_id == self.service.executor.executor_id
                        {
                            return Ok(LocalExecutorEvent::WorkloadsChanged {
                                sequence: event.sequence,
                                workloads,
                            });
                        }
                    }
                }
                Err(_) => {
                    self.stream = self.service.connect_workload_stream().await?;
                }
            }
        }
    }
}

#[derive(Clone, Debug)]
pub struct LocalProviderService {
    runtime: LocalNodeRuntime,
    client_name: String,
    provider: ProviderRecord,
    retry_policy: LocalServiceRetryPolicy,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum LocalProviderEvent {
    BootstrapLeases(Vec<LeaseRecord>),
    BootstrapStateSnapshot(StateSnapshot),
    LeasesChanged {
        sequence: u64,
        leases: Vec<LeaseRecord>,
    },
    StateSnapshot {
        sequence: u64,
        snapshot: StateSnapshot,
    },
}

pub struct LocalProviderSubscription {
    service: LocalProviderService,
    desired_revision: Revision,
    lease_stream: ProviderEventStream,
    state_stream: ControlPlaneEventStream,
    pending: VecDeque<LocalProviderEvent>,
}

impl LocalProviderService {
    pub fn new(
        runtime: LocalNodeRuntime,
        client_name: impl Into<String>,
        provider: ProviderRecord,
    ) -> Self {
        Self {
            runtime,
            client_name: client_name.into(),
            provider,
            retry_policy: LocalServiceRetryPolicy::default(),
        }
    }

    pub fn with_retry_policy(mut self, retry_policy: LocalServiceRetryPolicy) -> Self {
        self.retry_policy = retry_policy;
        self
    }

    pub fn runtime(&self) -> &LocalNodeRuntime {
        &self.runtime
    }

    pub fn client_name(&self) -> &str {
        self.client_name.as_str()
    }

    pub fn provider(&self) -> &ProviderRecord {
        &self.provider
    }

    pub async fn register(&self) -> Result<(), ClientError> {
        self.retry_policy
            .retry(|| async {
                self.runtime
                    .provider(self.client_name.clone(), self.provider.clone())?
                    .register()
                    .await
            })
            .await
    }

    pub async fn fetch_leases(&self) -> Result<Vec<LeaseRecord>, ClientError> {
        self.retry_policy
            .retry(|| async {
                self.runtime
                    .provider(self.client_name.clone(), self.provider.clone())?
                    .fetch_leases()
                    .await
            })
            .await
    }

    pub async fn fetch_state_snapshot(&self) -> Result<StateSnapshot, ClientError> {
        let control_client_name = format!("{}-control", self.client_name);
        self.retry_policy
            .retry(|| async {
                self.runtime
                    .control_plane(control_client_name.clone())?
                    .fetch_state_snapshot()
                    .await
            })
            .await
    }

    pub async fn subscribe(
        &self,
        desired_revision: Revision,
    ) -> Result<LocalProviderSubscription, ClientError> {
        let leases = self.fetch_leases().await?;
        let snapshot = self.fetch_state_snapshot().await?;
        let current_revision = snapshot.state.desired.revision;
        let lease_stream = self.connect_lease_stream().await?;
        let state_stream = self.connect_state_stream(desired_revision).await?;
        Ok(LocalProviderSubscription {
            service: self.clone(),
            desired_revision: current_revision.max(desired_revision),
            lease_stream,
            state_stream,
            pending: VecDeque::from([
                LocalProviderEvent::BootstrapLeases(leases),
                LocalProviderEvent::BootstrapStateSnapshot(snapshot),
            ]),
        })
    }

    async fn connect_lease_stream(&self) -> Result<ProviderEventStream, ClientError> {
        let stream_name = format!("{}-leases", self.client_name);
        self.retry_policy
            .retry(|| async {
                let mut stream = self.runtime.provider_stream(stream_name.clone()).await?;
                stream.subscribe_leases(&self.provider).await?;
                Ok(stream)
            })
            .await
    }

    async fn connect_state_stream(
        &self,
        desired_revision: Revision,
    ) -> Result<ControlPlaneEventStream, ClientError> {
        let stream_name = format!("{}-control", self.client_name);
        self.retry_policy
            .retry(|| async {
                let mut stream = self.runtime.control_stream(stream_name.clone()).await?;
                stream.subscribe_state(desired_revision).await?;
                Ok(stream)
            })
            .await
    }
}

impl LocalProviderSubscription {
    pub async fn next_event(&mut self) -> Result<LocalProviderEvent, ClientError> {
        if let Some(event) = self.pending.pop_front() {
            return Ok(event);
        }

        loop {
            tokio::select! {
                lease_result = self.lease_stream.next_events() => {
                    match lease_result {
                        Ok(events) => {
                            for event in events {
                                if let ClientEventKind::ProviderLeases { provider_id, leases } = event.event
                                    && provider_id == self.service.provider.provider_id
                                {
                                    return Ok(LocalProviderEvent::LeasesChanged {
                                        sequence: event.sequence,
                                        leases,
                                    });
                                }
                            }
                        }
                        Err(_) => {
                            self.lease_stream = self.service.connect_lease_stream().await?;
                        }
                    }
                }
                state_result = self.state_stream.next_events() => {
                    match state_result {
                        Ok(events) => {
                            for event in events {
                                if let ClientEventKind::StateSnapshot(snapshot) = event.event {
                                    self.desired_revision = snapshot.state.desired.revision;
                                    return Ok(LocalProviderEvent::StateSnapshot {
                                        sequence: event.sequence,
                                        snapshot: *snapshot,
                                    });
                                }
                            }
                        }
                        Err(_) => {
                            self.state_stream = self.service.connect_state_stream(self.desired_revision).await?;
                        }
                    }
                }
            }
        }
    }
}
