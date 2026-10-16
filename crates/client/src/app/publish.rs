use orion_control_plane::{ProviderRecord, ResourceRecord, WorkloadRecord};
use orion_runtime::ExecutorSnapshot;

use crate::{ClientError, LocalExecutorApp, LocalNodeRuntime, LocalProviderApp};

#[derive(Clone, Debug)]
pub struct LocalRuntimePublisherBuilder {
    runtime: LocalNodeRuntime,
    client_name: String,
    provider: Option<ProviderRecord>,
    executor: Option<orion_control_plane::ExecutorRecord>,
}

/// Publishes a provider's resources and an executor's snapshot. Each role
/// keeps one app on the node's stream socket for the publisher's life (its
/// clones share it): every publish reuses that session, and a session that
/// drops reconnects on the next request, instead of a new connection and
/// handshake per publish.
#[derive(Clone, Debug)]
pub struct LocalRuntimePublisher {
    runtime: LocalNodeRuntime,
    client_name: String,
    provider: Option<ProviderRecord>,
    executor: Option<orion_control_plane::ExecutorRecord>,
    provider_app: Option<LocalProviderApp>,
    executor_app: Option<LocalExecutorApp>,
}

impl LocalRuntimePublisherBuilder {
    pub fn new(runtime: LocalNodeRuntime, client_name: impl Into<String>) -> Self {
        Self {
            runtime,
            client_name: client_name.into(),
            provider: None,
            executor: None,
        }
    }

    pub fn provider(mut self, provider: ProviderRecord) -> Self {
        self.provider = Some(provider);
        self
    }

    pub fn executor(mut self, executor: orion_control_plane::ExecutorRecord) -> Self {
        self.executor = Some(executor);
        self
    }

    pub fn build(self) -> LocalRuntimePublisher {
        // A session app connects lazily, on its first request; building one
        // only checks the role, so these don't fail in practice (and a
        // publisher without one falls back to an app per call).
        let provider_app = self.provider.clone().and_then(|provider| {
            self.runtime
                .provider_session(format!("{}-provider", self.client_name), provider)
                .ok()
        });
        let executor_app = self.executor.clone().and_then(|executor| {
            self.runtime
                .executor_session(format!("{}-executor", self.client_name), executor)
                .ok()
        });
        LocalRuntimePublisher {
            runtime: self.runtime,
            client_name: self.client_name,
            provider: self.provider,
            executor: self.executor,
            provider_app,
            executor_app,
        }
    }
}

impl LocalRuntimePublisher {
    pub fn builder(
        runtime: LocalNodeRuntime,
        client_name: impl Into<String>,
    ) -> LocalRuntimePublisherBuilder {
        LocalRuntimePublisherBuilder::new(runtime, client_name)
    }

    pub fn provider(&self) -> Option<&ProviderRecord> {
        self.provider.as_ref()
    }

    pub fn executor(&self) -> Option<&orion_control_plane::ExecutorRecord> {
        self.executor.as_ref()
    }

    pub async fn register_all(&self) -> Result<(), ClientError> {
        if let Some(provider) = self.provider.as_ref() {
            self.provider_app(provider.clone())?.register().await?;
        }
        if let Some(executor) = self.executor.as_ref() {
            self.executor_app(executor.clone())?.register().await?;
        }
        Ok(())
    }

    pub async fn publish_provider_resources<I>(&self, resources: I) -> Result<(), ClientError>
    where
        I: IntoIterator<Item = ResourceRecord>,
    {
        let provider = self
            .provider
            .clone()
            .ok_or_else(|| ClientError::Rejected("runtime publisher has no provider".into()))?;
        self.provider_app(provider)?
            .publish_resources(resources)
            .await
    }

    pub async fn publish_executor_snapshot<I, J>(
        &self,
        workloads: I,
        resources: J,
    ) -> Result<(), ClientError>
    where
        I: IntoIterator<Item = WorkloadRecord>,
        J: IntoIterator<Item = ResourceRecord>,
    {
        let executor = self
            .executor
            .clone()
            .ok_or_else(|| ClientError::Rejected("runtime publisher has no executor".into()))?;
        self.executor_app(executor)?
            .publish_snapshot(workloads, resources)
            .await
    }

    pub async fn publish_executor_state(
        &self,
        snapshot: ExecutorSnapshot,
    ) -> Result<(), ClientError> {
        if let Some(executor) = self.executor.as_ref()
            && snapshot.executor != *executor
        {
            return Err(ClientError::Rejected(
                "executor snapshot does not match runtime publisher executor".into(),
            ));
        }
        self.publish_executor_snapshot(snapshot.workloads, snapshot.resources)
            .await
    }

    pub async fn publish_all<I>(
        &self,
        provider_resources: I,
        executor_snapshot: ExecutorSnapshot,
    ) -> Result<(), ClientError>
    where
        I: IntoIterator<Item = ResourceRecord>,
    {
        self.publish_provider_resources(provider_resources).await?;
        self.publish_executor_state(executor_snapshot).await
    }

    fn provider_app(&self, provider: ProviderRecord) -> Result<LocalProviderApp, ClientError> {
        match &self.provider_app {
            Some(app) => Ok(app.clone()),
            None => self
                .runtime
                .provider(format!("{}-provider", self.client_name), provider),
        }
    }

    fn executor_app(
        &self,
        executor: orion_control_plane::ExecutorRecord,
    ) -> Result<LocalExecutorApp, ClientError> {
        match &self.executor_app {
            Some(app) => Ok(app.clone()),
            None => self
                .runtime
                .executor(format!("{}-executor", self.client_name), executor),
        }
    }
}
