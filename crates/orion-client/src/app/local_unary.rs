use std::{
    fmt,
    marker::PhantomData,
    path::{Path, PathBuf},
    sync::Arc,
};

use orion_control_plane::{
    ClientHello, ClientRole, ControlMessage, ExecutorRecord, ExecutorStateUpdate,
    ExecutorWorkloadQuery, LeaseRecord, ProviderLeaseQuery, ProviderRecord, ProviderStateUpdate,
    ResourceRecord, WorkloadRecord,
};
use orion_transport_ipc::{
    ControlEnvelope, LocalAddress, UnixControlClient, UnixControlStreamClient,
};
use tokio::sync::Mutex;

use crate::{
    ClientError, ClientIdentity, SessionConfig, default_ipc_stream_socket_path,
    session::{
        ensure_client_role, local_identity_for_role, local_session_config_for_role,
        local_session_config_with_address,
    },
};

trait LocalUnaryRole {
    const ROLE: ClientRole;
}

#[derive(Clone, Copy, Debug)]
struct ProviderRole;
#[derive(Clone, Copy, Debug)]
struct ExecutorRole;

impl LocalUnaryRole for ProviderRole {
    const ROLE: ClientRole = ClientRole::Provider;
}

impl LocalUnaryRole for ExecutorRole {
    const ROLE: ClientRole = ClientRole::Executor;
}

#[derive(Default)]
struct StreamSession {
    client: Option<UnixControlStreamClient>,
    hello_completed: bool,
}

#[derive(Clone)]
enum LocalTransport {
    Unary {
        socket_path: PathBuf,
    },
    Stream {
        socket_path: PathBuf,
        session: Arc<Mutex<StreamSession>>,
    },
}

impl fmt::Debug for LocalTransport {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Unary { socket_path } => f
                .debug_struct("Unary")
                .field("socket_path", socket_path)
                .finish(),
            Self::Stream { socket_path, .. } => f
                .debug_struct("Stream")
                .field("socket_path", socket_path)
                .finish(),
        }
    }
}

#[derive(Clone, Debug)]
struct LocalUnaryClient<Role> {
    transport: LocalTransport,
    identity: ClientIdentity,
    config: SessionConfig,
    role: PhantomData<Role>,
}

impl<Role> LocalUnaryClient<Role>
where
    Role: LocalUnaryRole,
{
    fn connect_at_with_local_address(
        socket_path: impl AsRef<Path>,
        name: impl Into<String>,
        local_address: impl Into<String>,
    ) -> Result<Self, ClientError> {
        let identity = local_identity_for_role(name, Role::ROLE);
        Self::connect(
            socket_path,
            identity.clone(),
            local_session_config_with_address(LocalAddress::new(local_address)),
        )
    }

    fn connect_at(
        socket_path: impl AsRef<Path>,
        name: impl Into<String>,
    ) -> Result<Self, ClientError> {
        let identity = local_identity_for_role(name, Role::ROLE);
        Self::connect(
            socket_path,
            identity.clone(),
            local_session_config_for_role(&identity),
        )
    }

    fn connect_default(name: impl Into<String>) -> Result<Self, ClientError> {
        let identity = local_identity_for_role(name, Role::ROLE);
        Self::connect_stream(
            default_ipc_stream_socket_path(),
            identity.clone(),
            local_session_config_for_role(&identity),
        )
    }

    fn connect(
        socket_path: impl AsRef<Path>,
        identity: ClientIdentity,
        config: SessionConfig,
    ) -> Result<Self, ClientError> {
        ensure_client_role(&identity, Role::ROLE)?;
        Ok(Self {
            transport: LocalTransport::Unary {
                socket_path: socket_path.as_ref().to_path_buf(),
            },
            identity,
            config,
            role: PhantomData,
        })
    }

    fn connect_stream(
        socket_path: impl AsRef<Path>,
        identity: ClientIdentity,
        config: SessionConfig,
    ) -> Result<Self, ClientError> {
        ensure_client_role(&identity, Role::ROLE)?;
        Ok(Self {
            transport: LocalTransport::Stream {
                socket_path: socket_path.as_ref().to_path_buf(),
                session: Arc::new(Mutex::new(StreamSession::default())),
            },
            identity,
            config,
            role: PhantomData,
        })
    }

    fn identity(&self) -> &ClientIdentity {
        &self.identity
    }

    async fn send_and_expect_accepted(&self, message: ControlMessage) -> Result<(), ClientError> {
        match self.request(message).await? {
            ControlMessage::Accepted => Ok(()),
            ControlMessage::Rejected(reason) => Err(ClientError::Rejected(reason)),
            _ => Err(ClientError::NoMessageAvailable),
        }
    }

    async fn request(&self, message: ControlMessage) -> Result<ControlMessage, ClientError> {
        match &self.transport {
            LocalTransport::Unary { socket_path } => self.request_unary(socket_path, message).await,
            LocalTransport::Stream {
                socket_path,
                session,
            } => self.request_stream(socket_path, session, message).await,
        }
    }

    async fn request_unary(
        &self,
        socket_path: &Path,
        message: ControlMessage,
    ) -> Result<ControlMessage, ClientError> {
        let client = UnixControlClient::new(socket_path);
        let response = client
            .send(ControlEnvelope {
                source: self.config.local_address.clone(),
                destination: self.config.daemon_address.clone(),
                message: ControlMessage::ClientHello(ClientHello {
                    client_name: self.identity.name.clone().into(),
                    role: Role::ROLE,
                }),
            })
            .await?;
        match response.message {
            ControlMessage::ClientWelcome(_) => {}
            ControlMessage::Rejected(reason) => return Err(ClientError::Rejected(reason)),
            _ => return Err(ClientError::NoMessageAvailable),
        }

        let response = client
            .send(ControlEnvelope {
                source: self.config.local_address.clone(),
                destination: self.config.daemon_address.clone(),
                message,
            })
            .await?;
        Ok(response.message)
    }

    async fn request_stream(
        &self,
        socket_path: &Path,
        session: &Arc<Mutex<StreamSession>>,
        message: ControlMessage,
    ) -> Result<ControlMessage, ClientError> {
        self.request_stream_inner(socket_path, session, message, true)
            .await
    }

    async fn request_stream_inner(
        &self,
        socket_path: &Path,
        session: &Arc<Mutex<StreamSession>>,
        message: ControlMessage,
        allow_retry: bool,
    ) -> Result<ControlMessage, ClientError> {
        let mut stream_session = session.lock().await;
        if stream_session.client.is_none() {
            stream_session.client = Some(UnixControlStreamClient::connect(socket_path).await?);
            stream_session.hello_completed = false;
        }
        if !stream_session.hello_completed {
            self.complete_stream_hello(&mut stream_session).await?;
        }

        let response = match self
            .send_stream_request(&mut stream_session, message.clone())
            .await
        {
            Ok(response) => response,
            Err(_error) if allow_retry => {
                stream_session.client = None;
                stream_session.hello_completed = false;
                drop(stream_session);
                return Box::pin(self.request_stream_inner(socket_path, session, message, false))
                    .await;
            }
            Err(error) => return Err(error),
        };

        Ok(response)
    }

    async fn complete_stream_hello(&self, session: &mut StreamSession) -> Result<(), ClientError> {
        let client = session.client.as_mut().ok_or(ClientError::SendFailed)?;
        client
            .send(&ControlEnvelope {
                source: self.config.local_address.clone(),
                destination: self.config.daemon_address.clone(),
                message: ControlMessage::ClientHello(ClientHello {
                    client_name: self.identity.name.clone().into(),
                    role: Role::ROLE,
                }),
            })
            .await?;
        match self.recv_stream_message(client).await? {
            ControlMessage::ClientWelcome(_) => {
                session.hello_completed = true;
                Ok(())
            }
            ControlMessage::Rejected(reason) => Err(ClientError::Rejected(reason)),
            _ => Err(ClientError::NoMessageAvailable),
        }
    }

    async fn send_stream_request(
        &self,
        session: &mut StreamSession,
        message: ControlMessage,
    ) -> Result<ControlMessage, ClientError> {
        let client = session.client.as_mut().ok_or(ClientError::SendFailed)?;
        client
            .send(&ControlEnvelope {
                source: self.config.local_address.clone(),
                destination: self.config.daemon_address.clone(),
                message,
            })
            .await?;
        self.recv_stream_message(client).await
    }

    async fn recv_stream_message(
        &self,
        client: &mut UnixControlStreamClient,
    ) -> Result<ControlMessage, ClientError> {
        loop {
            let Some(envelope) = client.recv().await? else {
                return Err(ClientError::NoMessageAvailable);
            };
            match envelope.message {
                ControlMessage::Ping => {
                    client
                        .send(&ControlEnvelope {
                            source: self.config.local_address.clone(),
                            destination: self.config.daemon_address.clone(),
                            message: ControlMessage::Pong,
                        })
                        .await?;
                }
                message => return Ok(message),
            }
        }
    }
}

#[derive(Clone, Debug)]
pub struct LocalProviderClient {
    inner: LocalUnaryClient<ProviderRole>,
}

#[derive(Clone, Debug)]
pub struct LocalProviderApp {
    client: LocalProviderClient,
    provider: ProviderRecord,
}

impl LocalProviderClient {
    /// Build a provider client that uses unary IPC.
    ///
    /// Each request on this client establishes a fresh unary connection and performs the local
    /// `ClientHello` handshake before sending the payload. Prefer [`Self::connect_default`] for
    /// repeated calls on the default daemon layout.
    pub fn connect_at_with_local_address(
        socket_path: impl AsRef<Path>,
        name: impl Into<String>,
        local_address: impl Into<String>,
    ) -> Result<Self, ClientError> {
        Ok(Self {
            inner: LocalUnaryClient::connect_at_with_local_address(
                socket_path,
                name,
                local_address,
            )?,
        })
    }

    /// Build a provider client that uses unary IPC.
    ///
    /// Each request on this client establishes a fresh unary connection and performs the local
    /// `ClientHello` handshake before sending the payload. Prefer [`Self::connect_default`] for
    /// repeated calls on the default daemon layout.
    pub fn connect_at(
        socket_path: impl AsRef<Path>,
        name: impl Into<String>,
    ) -> Result<Self, ClientError> {
        Ok(Self {
            inner: LocalUnaryClient::connect_at(socket_path, name)?,
        })
    }

    /// Connect using the default IPC stream socket and reuse the negotiated session across
    /// repeated local requests from this client instance.
    pub fn connect_default(name: impl Into<String>) -> Result<Self, ClientError> {
        Ok(Self {
            inner: LocalUnaryClient::connect_default(name)?,
        })
    }

    pub fn connect(
        socket_path: impl AsRef<Path>,
        identity: ClientIdentity,
        config: SessionConfig,
    ) -> Result<Self, ClientError> {
        Ok(Self {
            inner: LocalUnaryClient::connect(socket_path, identity, config)?,
        })
    }

    pub fn identity(&self) -> &ClientIdentity {
        self.inner.identity()
    }

    pub async fn register_provider(&self, provider: ProviderRecord) -> Result<(), ClientError> {
        self.publish_resources(provider, std::iter::empty()).await
    }

    pub async fn publish_resources<I>(
        &self,
        provider: ProviderRecord,
        resources: I,
    ) -> Result<(), ClientError>
    where
        I: IntoIterator<Item = ResourceRecord>,
    {
        self.inner
            .send_and_expect_accepted(ControlMessage::ProviderState(ProviderStateUpdate {
                provider,
                resources: resources.into_iter().collect(),
            }))
            .await
    }

    pub async fn fetch_leases(
        &self,
        provider: &ProviderRecord,
    ) -> Result<Vec<LeaseRecord>, ClientError> {
        match self
            .inner
            .request(ControlMessage::QueryProviderLeases(ProviderLeaseQuery {
                provider_id: provider.provider_id.clone(),
            }))
            .await?
        {
            ControlMessage::ProviderLeases(leases) => Ok(leases),
            ControlMessage::Rejected(reason) => Err(ClientError::Rejected(reason)),
            _ => Err(ClientError::NoMessageAvailable),
        }
    }
}

impl LocalProviderApp {
    /// Build a provider app that uses unary IPC.
    ///
    /// Each request from this app establishes a fresh unary connection and performs the local
    /// `ClientHello` handshake before sending the payload. Prefer [`Self::connect_default`] for
    /// repeated calls on the default daemon layout.
    pub fn connect_at_with_local_address(
        socket_path: impl AsRef<Path>,
        name: impl Into<String>,
        local_address: impl Into<String>,
        provider: ProviderRecord,
    ) -> Result<Self, ClientError> {
        Ok(Self {
            client: LocalProviderClient::connect_at_with_local_address(
                socket_path,
                name,
                local_address,
            )?,
            provider,
        })
    }

    /// Build a provider app that uses unary IPC.
    ///
    /// Each request from this app establishes a fresh unary connection and performs the local
    /// `ClientHello` handshake before sending the payload. Prefer [`Self::connect_default`] for
    /// repeated calls on the default daemon layout.
    pub fn connect_at(
        socket_path: impl AsRef<Path>,
        name: impl Into<String>,
        provider: ProviderRecord,
    ) -> Result<Self, ClientError> {
        Ok(Self {
            client: LocalProviderClient::connect_at(socket_path, name)?,
            provider,
        })
    }

    /// Connect using the default IPC stream socket and reuse the negotiated session across
    /// repeated local requests from this app instance.
    pub fn connect_default(
        name: impl Into<String>,
        provider: ProviderRecord,
    ) -> Result<Self, ClientError> {
        Ok(Self {
            client: LocalProviderClient::connect_default(name)?,
            provider,
        })
    }

    pub fn connect(
        socket_path: impl AsRef<Path>,
        identity: ClientIdentity,
        config: SessionConfig,
        provider: ProviderRecord,
    ) -> Result<Self, ClientError> {
        Ok(Self {
            client: LocalProviderClient::connect(socket_path, identity, config)?,
            provider,
        })
    }

    pub fn identity(&self) -> &ClientIdentity {
        self.client.identity()
    }

    pub fn provider(&self) -> &ProviderRecord {
        &self.provider
    }

    pub async fn register(&self) -> Result<(), ClientError> {
        self.client.register_provider(self.provider.clone()).await
    }

    pub async fn publish_resource(&self, resource: ResourceRecord) -> Result<(), ClientError> {
        self.publish_resources(vec![resource]).await
    }

    pub async fn publish_resources<I>(&self, resources: I) -> Result<(), ClientError>
    where
        I: IntoIterator<Item = ResourceRecord>,
    {
        self.client
            .publish_resources(self.provider.clone(), resources)
            .await
    }

    pub async fn fetch_leases(&self) -> Result<Vec<LeaseRecord>, ClientError> {
        self.client.fetch_leases(&self.provider).await
    }
}

#[derive(Clone, Debug)]
pub struct LocalExecutorClient {
    inner: LocalUnaryClient<ExecutorRole>,
}

#[derive(Clone, Debug)]
pub struct LocalExecutorApp {
    client: LocalExecutorClient,
    executor: ExecutorRecord,
}

impl LocalExecutorClient {
    /// Build an executor client that uses unary IPC.
    ///
    /// Each request on this client establishes a fresh unary connection and performs the local
    /// `ClientHello` handshake before sending the payload. Prefer [`Self::connect_default`] for
    /// repeated calls on the default daemon layout.
    pub fn connect_at_with_local_address(
        socket_path: impl AsRef<Path>,
        name: impl Into<String>,
        local_address: impl Into<String>,
    ) -> Result<Self, ClientError> {
        Ok(Self {
            inner: LocalUnaryClient::connect_at_with_local_address(
                socket_path,
                name,
                local_address,
            )?,
        })
    }

    /// Build an executor client that uses unary IPC.
    ///
    /// Each request on this client establishes a fresh unary connection and performs the local
    /// `ClientHello` handshake before sending the payload. Prefer [`Self::connect_default`] for
    /// repeated calls on the default daemon layout.
    pub fn connect_at(
        socket_path: impl AsRef<Path>,
        name: impl Into<String>,
    ) -> Result<Self, ClientError> {
        Ok(Self {
            inner: LocalUnaryClient::connect_at(socket_path, name)?,
        })
    }

    /// Connect using the default IPC stream socket and reuse the negotiated session across
    /// repeated local requests from this client instance.
    pub fn connect_default(name: impl Into<String>) -> Result<Self, ClientError> {
        Ok(Self {
            inner: LocalUnaryClient::connect_default(name)?,
        })
    }

    pub fn connect(
        socket_path: impl AsRef<Path>,
        identity: ClientIdentity,
        config: SessionConfig,
    ) -> Result<Self, ClientError> {
        Ok(Self {
            inner: LocalUnaryClient::connect(socket_path, identity, config)?,
        })
    }

    pub fn identity(&self) -> &ClientIdentity {
        self.inner.identity()
    }

    pub async fn register_executor(&self, executor: ExecutorRecord) -> Result<(), ClientError> {
        self.publish_snapshot(executor, std::iter::empty(), std::iter::empty())
            .await
    }

    pub async fn publish_workloads<I>(
        &self,
        executor: ExecutorRecord,
        workloads: I,
    ) -> Result<(), ClientError>
    where
        I: IntoIterator<Item = WorkloadRecord>,
    {
        self.publish_snapshot(executor, workloads, std::iter::empty())
            .await
    }

    pub async fn publish_resources<I>(
        &self,
        executor: ExecutorRecord,
        resources: I,
    ) -> Result<(), ClientError>
    where
        I: IntoIterator<Item = ResourceRecord>,
    {
        self.publish_snapshot(executor, std::iter::empty(), resources)
            .await
    }

    pub async fn publish_snapshot<I, J>(
        &self,
        executor: ExecutorRecord,
        workloads: I,
        resources: J,
    ) -> Result<(), ClientError>
    where
        I: IntoIterator<Item = WorkloadRecord>,
        J: IntoIterator<Item = ResourceRecord>,
    {
        self.inner
            .send_and_expect_accepted(ControlMessage::ExecutorState(ExecutorStateUpdate {
                executor,
                workloads: workloads.into_iter().collect(),
                resources: resources.into_iter().collect(),
            }))
            .await
    }

    pub async fn fetch_assigned_workloads(
        &self,
        executor: &ExecutorRecord,
    ) -> Result<Vec<WorkloadRecord>, ClientError> {
        match self
            .inner
            .request(ControlMessage::QueryExecutorWorkloads(
                ExecutorWorkloadQuery {
                    executor_id: executor.executor_id.clone(),
                },
            ))
            .await?
        {
            ControlMessage::ExecutorWorkloads(workloads) => Ok(workloads),
            ControlMessage::Rejected(reason) => Err(ClientError::Rejected(reason)),
            _ => Err(ClientError::NoMessageAvailable),
        }
    }
}

impl LocalExecutorApp {
    /// Build an executor app that uses unary IPC.
    ///
    /// Each request from this app establishes a fresh unary connection and performs the local
    /// `ClientHello` handshake before sending the payload. Prefer [`Self::connect_default`] for
    /// repeated calls on the default daemon layout.
    pub fn connect_at_with_local_address(
        socket_path: impl AsRef<Path>,
        name: impl Into<String>,
        local_address: impl Into<String>,
        executor: ExecutorRecord,
    ) -> Result<Self, ClientError> {
        Ok(Self {
            client: LocalExecutorClient::connect_at_with_local_address(
                socket_path,
                name,
                local_address,
            )?,
            executor,
        })
    }

    /// Build an executor app that uses unary IPC.
    ///
    /// Each request from this app establishes a fresh unary connection and performs the local
    /// `ClientHello` handshake before sending the payload. Prefer [`Self::connect_default`] for
    /// repeated calls on the default daemon layout.
    pub fn connect_at(
        socket_path: impl AsRef<Path>,
        name: impl Into<String>,
        executor: ExecutorRecord,
    ) -> Result<Self, ClientError> {
        Ok(Self {
            client: LocalExecutorClient::connect_at(socket_path, name)?,
            executor,
        })
    }

    /// Connect using the default IPC stream socket and reuse the negotiated session across
    /// repeated local requests from this app instance.
    pub fn connect_default(
        name: impl Into<String>,
        executor: ExecutorRecord,
    ) -> Result<Self, ClientError> {
        Ok(Self {
            client: LocalExecutorClient::connect_default(name)?,
            executor,
        })
    }

    pub fn connect(
        socket_path: impl AsRef<Path>,
        identity: ClientIdentity,
        config: SessionConfig,
        executor: ExecutorRecord,
    ) -> Result<Self, ClientError> {
        Ok(Self {
            client: LocalExecutorClient::connect(socket_path, identity, config)?,
            executor,
        })
    }

    pub fn identity(&self) -> &ClientIdentity {
        self.client.identity()
    }

    pub fn executor(&self) -> &ExecutorRecord {
        &self.executor
    }

    pub async fn register(&self) -> Result<(), ClientError> {
        self.client.register_executor(self.executor.clone()).await
    }

    pub async fn publish_workloads<I>(&self, workloads: I) -> Result<(), ClientError>
    where
        I: IntoIterator<Item = WorkloadRecord>,
    {
        self.client
            .publish_workloads(self.executor.clone(), workloads)
            .await
    }

    pub async fn publish_resource(&self, resource: ResourceRecord) -> Result<(), ClientError> {
        self.publish_resources(vec![resource]).await
    }

    pub async fn publish_resources<I>(&self, resources: I) -> Result<(), ClientError>
    where
        I: IntoIterator<Item = ResourceRecord>,
    {
        self.client
            .publish_resources(self.executor.clone(), resources)
            .await
    }

    pub async fn publish_snapshot<I, J>(
        &self,
        workloads: I,
        resources: J,
    ) -> Result<(), ClientError>
    where
        I: IntoIterator<Item = WorkloadRecord>,
        J: IntoIterator<Item = ResourceRecord>,
    {
        self.client
            .publish_snapshot(self.executor.clone(), workloads, resources)
            .await
    }

    pub async fn fetch_assigned_workloads(&self) -> Result<Vec<WorkloadRecord>, ClientError> {
        self.client.fetch_assigned_workloads(&self.executor).await
    }
}
