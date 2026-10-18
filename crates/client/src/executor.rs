use orion_control_plane::{
    ClientEvent, ClientRole, ControlMessage, ExecutorRecord, ExecutorStateUpdate,
    ExecutorWorkloadQuery, ResourceRecord, WorkloadRecord,
};
use std::{sync::Arc, time::Duration};

use orion_runtime::ExecutorSnapshot;
use orion_transport_ipc::LocalControlTransport;

use crate::{
    default_ipc_stream_socket_path,
    error::ClientError,
    publish::{PublishDedupe, send_state_publish},
    response::{executor_workload_query, expect_executor_workloads},
    session::{
        ClientSession, ensure_client_role, local_identity_for_role, local_session_config_for_role,
        local_session_config_with_address,
    },
    stream::ClientEventStreamSession,
};

#[derive(Clone)]
pub struct ExecutorClient<T> {
    session: ClientSession<T>,
    dedupe: Option<Arc<PublishDedupe>>,
}

pub struct ExecutorEventStream {
    session: ClientEventStreamSession,
}

impl<T> ExecutorClient<T>
where
    T: LocalControlTransport,
{
    pub fn new(session: ClientSession<T>) -> Result<Self, ClientError> {
        ensure_client_role(session.identity(), ClientRole::Executor)?;
        Ok(Self {
            session,
            dedupe: None,
        })
    }

    pub fn session(&self) -> &ClientSession<T> {
        &self.session
    }

    /// Skips a snapshot publish whose encoded message is identical to the last one the node
    /// accepted, sending it again once `refresh` has elapsed. Keep `refresh` well below the
    /// node's local session TTL (300 s by default). Off unless this is called; see
    /// [`crate::publish`] for the revision-sync latency it trades for fewer publishes.
    pub fn with_publish_refresh(mut self, refresh: Duration) -> Self {
        self.dedupe = Some(Arc::new(PublishDedupe::new(refresh)));
        self
    }

    pub fn register_executor(&self, executor: ExecutorRecord) -> Result<(), ClientError> {
        self.publish_snapshot(ExecutorSnapshot {
            executor,
            workloads: Vec::new(),
            resources: Vec::new(),
        })
    }

    pub fn publish_workloads<I>(
        &self,
        executor: ExecutorRecord,
        workloads: I,
    ) -> Result<(), ClientError>
    where
        I: IntoIterator<Item = WorkloadRecord>,
    {
        self.publish_snapshot(ExecutorSnapshot {
            executor,
            workloads: workloads.into_iter().collect(),
            resources: Vec::new(),
        })
    }

    pub fn publish_snapshot(&self, snapshot: ExecutorSnapshot) -> Result<(), ClientError> {
        send_state_publish(
            &self.session,
            self.dedupe.as_deref(),
            ControlMessage::ExecutorState(ExecutorStateUpdate {
                executor: snapshot.executor,
                workloads: snapshot.workloads,
                resources: snapshot.resources,
            }),
        )
    }

    pub fn publish_resources<I>(
        &self,
        executor: ExecutorRecord,
        resources: I,
    ) -> Result<(), ClientError>
    where
        I: IntoIterator<Item = ResourceRecord>,
    {
        self.publish_snapshot(ExecutorSnapshot {
            executor,
            workloads: Vec::new(),
            resources: resources.into_iter().collect(),
        })
    }

    pub fn fetch_assigned_workloads(
        &self,
        executor: &ExecutorRecord,
    ) -> Result<Vec<WorkloadRecord>, ClientError> {
        self.session.request_control_with(
            executor_workload_query(executor.executor_id.clone()),
            expect_executor_workloads,
        )
    }
}

impl ExecutorEventStream {
    pub async fn connect_at_with_local_address(
        socket_path: impl AsRef<std::path::Path>,
        name: impl Into<String>,
        local_address: impl Into<String>,
    ) -> Result<Self, ClientError> {
        let identity = local_identity_for_role(name, ClientRole::Executor);
        Self::connect(
            socket_path,
            identity.clone(),
            local_session_config_with_address(orion_transport_ipc::LocalAddress::new(
                local_address,
            )),
        )
        .await
    }

    pub async fn connect_at(
        socket_path: impl AsRef<std::path::Path>,
        name: impl Into<String>,
    ) -> Result<Self, ClientError> {
        let identity = local_identity_for_role(name, ClientRole::Executor);
        Self::connect(
            socket_path,
            identity.clone(),
            local_session_config_for_role(&identity),
        )
        .await
    }

    pub async fn connect_default(name: impl Into<String>) -> Result<Self, ClientError> {
        Self::connect_at(default_ipc_stream_socket_path(), name).await
    }

    pub async fn connect(
        socket_path: impl AsRef<std::path::Path>,
        identity: crate::session::ClientIdentity,
        config: crate::session::SessionConfig,
    ) -> Result<Self, ClientError> {
        ensure_client_role(&identity, ClientRole::Executor)?;

        Ok(Self {
            session: ClientEventStreamSession::connect(
                socket_path,
                identity,
                config,
                ClientRole::Executor,
            )
            .await?,
        })
    }

    pub async fn subscribe_workloads(
        &mut self,
        executor: &ExecutorRecord,
    ) -> Result<(), ClientError> {
        self.session
            .subscribe_and_expect_accepted(ControlMessage::WatchExecutorWorkloads(
                ExecutorWorkloadQuery {
                    executor_id: executor.executor_id.clone(),
                },
            ))
            .await
    }

    pub async fn next_events(&mut self) -> Result<Vec<ClientEvent>, ClientError> {
        self.session.next_client_events().await
    }
}
