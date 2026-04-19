use std::path::{Path, PathBuf};

use orion_control_plane::{
    ExecutorRecord, LeaseRecord, ProviderRecord, ResourceRecord, WorkloadRecord,
};
use orion_runtime::ExecutorSnapshot;
use orion_transport_ipc::LocalControlTransport;

use super::local_unary::{
    LocalExecutorApp, LocalExecutorClient, LocalProviderApp, LocalProviderClient,
};
use crate::{
    ClientError, ControlPlaneEventStream, ExecutorClient, ExecutorEventStream,
    LocalControlPlaneClient, ProviderClient, ProviderEventStream, default_ipc_socket_path,
    default_ipc_stream_socket_path,
};

#[derive(Clone)]
pub struct ProviderApp<T> {
    client: ProviderClient<T>,
    provider: ProviderRecord,
}

impl<T> ProviderApp<T>
where
    T: LocalControlTransport,
{
    pub fn new(client: ProviderClient<T>, provider: ProviderRecord) -> Self {
        Self { client, provider }
    }

    pub fn client(&self) -> &ProviderClient<T> {
        &self.client
    }

    pub fn provider(&self) -> &ProviderRecord {
        &self.provider
    }

    pub fn register(&self) -> Result<(), ClientError> {
        self.client.register_provider(self.provider.clone())
    }

    pub fn publish_resource(&self, resource: ResourceRecord) -> Result<(), ClientError> {
        self.publish_resources(vec![resource])
    }

    pub fn publish_resources<I>(&self, resources: I) -> Result<(), ClientError>
    where
        I: IntoIterator<Item = ResourceRecord>,
    {
        self.client
            .publish_resources(self.provider.clone(), resources)
    }

    pub fn fetch_leases(&self) -> Result<Vec<LeaseRecord>, ClientError> {
        self.client.fetch_leases(&self.provider)
    }
}

#[derive(Clone)]
pub struct ExecutorApp<T> {
    client: ExecutorClient<T>,
    executor: ExecutorRecord,
}

impl<T> ExecutorApp<T>
where
    T: LocalControlTransport,
{
    pub fn new(client: ExecutorClient<T>, executor: ExecutorRecord) -> Self {
        Self { client, executor }
    }

    pub fn client(&self) -> &ExecutorClient<T> {
        &self.client
    }

    pub fn executor(&self) -> &ExecutorRecord {
        &self.executor
    }

    pub fn register(&self) -> Result<(), ClientError> {
        self.client.register_executor(self.executor.clone())
    }

    pub fn publish_workloads<I>(&self, workloads: I) -> Result<(), ClientError>
    where
        I: IntoIterator<Item = WorkloadRecord>,
    {
        self.client
            .publish_workloads(self.executor.clone(), workloads)
    }

    pub fn publish_resource(&self, resource: ResourceRecord) -> Result<(), ClientError> {
        self.publish_resources(vec![resource])
    }

    pub fn publish_resources<I>(&self, resources: I) -> Result<(), ClientError>
    where
        I: IntoIterator<Item = ResourceRecord>,
    {
        self.client
            .publish_resources(self.executor.clone(), resources)
    }

    pub fn publish_snapshot<I, J>(&self, workloads: I, resources: J) -> Result<(), ClientError>
    where
        I: IntoIterator<Item = WorkloadRecord>,
        J: IntoIterator<Item = ResourceRecord>,
    {
        self.client.publish_snapshot(ExecutorSnapshot {
            executor: self.executor.clone(),
            workloads: workloads.into_iter().collect(),
            resources: resources.into_iter().collect(),
        })
    }

    pub fn fetch_assigned_workloads(&self) -> Result<Vec<WorkloadRecord>, ClientError> {
        self.client.fetch_assigned_workloads(&self.executor)
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LocalNodeRuntime {
    ipc_socket_path: PathBuf,
    ipc_stream_socket_path: PathBuf,
}

impl LocalNodeRuntime {
    pub fn new(
        ipc_socket_path: impl Into<PathBuf>,
        ipc_stream_socket_path: impl Into<PathBuf>,
    ) -> Self {
        Self {
            ipc_socket_path: ipc_socket_path.into(),
            ipc_stream_socket_path: ipc_stream_socket_path.into(),
        }
    }

    pub fn connect_default() -> Self {
        Self::new(default_ipc_socket_path(), default_ipc_stream_socket_path())
    }

    pub fn ipc_socket_path(&self) -> &Path {
        &self.ipc_socket_path
    }

    pub fn ipc_stream_socket_path(&self) -> &Path {
        &self.ipc_stream_socket_path
    }

    pub fn control_plane(
        &self,
        name: impl Into<String>,
    ) -> Result<LocalControlPlaneClient, ClientError> {
        LocalControlPlaneClient::connect_at(&self.ipc_socket_path, name)
    }

    pub fn provider(
        &self,
        name: impl Into<String>,
        provider: ProviderRecord,
    ) -> Result<LocalProviderApp, ClientError> {
        LocalProviderApp::connect_at(&self.ipc_socket_path, name, provider)
    }

    pub fn provider_client(
        &self,
        name: impl Into<String>,
    ) -> Result<LocalProviderClient, ClientError> {
        LocalProviderClient::connect_at(&self.ipc_socket_path, name)
    }

    pub fn executor(
        &self,
        name: impl Into<String>,
        executor: ExecutorRecord,
    ) -> Result<LocalExecutorApp, ClientError> {
        LocalExecutorApp::connect_at(&self.ipc_socket_path, name, executor)
    }

    pub fn executor_client(
        &self,
        name: impl Into<String>,
    ) -> Result<LocalExecutorClient, ClientError> {
        LocalExecutorClient::connect_at(&self.ipc_socket_path, name)
    }

    pub async fn control_stream(
        &self,
        name: impl Into<String>,
    ) -> Result<ControlPlaneEventStream, ClientError> {
        ControlPlaneEventStream::connect_at(&self.ipc_stream_socket_path, name).await
    }

    pub async fn provider_stream(
        &self,
        name: impl Into<String>,
    ) -> Result<ProviderEventStream, ClientError> {
        ProviderEventStream::connect_at(&self.ipc_stream_socket_path, name).await
    }

    pub async fn executor_stream(
        &self,
        name: impl Into<String>,
    ) -> Result<ExecutorEventStream, ClientError> {
        ExecutorEventStream::connect_at(&self.ipc_stream_socket_path, name).await
    }
}
