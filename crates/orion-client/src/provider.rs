use orion_control_plane::{
    ClientEvent, ClientRole, ControlMessage, LeaseRecord, ProviderLeaseQuery, ProviderRecord,
    ProviderStateUpdate, ResourceRecord,
};
use orion_transport_ipc::LocalControlTransport;

use crate::{
    default_ipc_stream_socket_path,
    error::ClientError,
    session::{
        ClientSession, ensure_client_role, local_identity_for_role, local_session_config_for_role,
        local_session_config_with_address,
    },
    stream::ClientEventStreamSession,
};

#[derive(Clone)]
pub struct ProviderClient<T> {
    session: ClientSession<T>,
}

pub struct ProviderEventStream {
    session: ClientEventStreamSession,
}

impl<T> ProviderClient<T>
where
    T: LocalControlTransport,
{
    pub fn new(session: ClientSession<T>) -> Result<Self, ClientError> {
        ensure_client_role(session.identity(), ClientRole::Provider)?;
        Ok(Self { session })
    }

    pub fn session(&self) -> &ClientSession<T> {
        &self.session
    }

    pub fn register_provider(&self, provider: ProviderRecord) -> Result<(), ClientError> {
        self.publish_resources(provider, std::iter::empty())
    }

    pub fn publish_resources<I>(
        &self,
        provider: ProviderRecord,
        resources: I,
    ) -> Result<(), ClientError>
    where
        I: IntoIterator<Item = ResourceRecord>,
    {
        self.session
            .send_control_and_expect_accepted(ControlMessage::ProviderState(ProviderStateUpdate {
                provider,
                resources: resources.into_iter().collect(),
            }))
    }

    pub fn fetch_leases(&self, provider: &ProviderRecord) -> Result<Vec<LeaseRecord>, ClientError> {
        self.session.request_control_with(
            ControlMessage::QueryProviderLeases(ProviderLeaseQuery {
                provider_id: provider.provider_id.clone(),
            }),
            |message| match message {
                ControlMessage::ProviderLeases(leases) => Ok(leases),
                ControlMessage::Rejected(reason) => Err(ClientError::Rejected(reason)),
                _ => Err(ClientError::NoMessageAvailable),
            },
        )
    }
}

impl ProviderEventStream {
    pub async fn connect_at_with_local_address(
        socket_path: impl AsRef<std::path::Path>,
        name: impl Into<String>,
        local_address: impl Into<String>,
    ) -> Result<Self, ClientError> {
        let identity = local_identity_for_role(name, ClientRole::Provider);
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
        let identity = local_identity_for_role(name, ClientRole::Provider);
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
        ensure_client_role(&identity, ClientRole::Provider)?;

        Ok(Self {
            session: ClientEventStreamSession::connect(
                socket_path,
                identity,
                config,
                ClientRole::Provider,
            )
            .await?,
        })
    }

    pub async fn subscribe_leases(&mut self, provider: &ProviderRecord) -> Result<(), ClientError> {
        self.session
            .subscribe_and_expect_accepted(ControlMessage::WatchProviderLeases(
                ProviderLeaseQuery {
                    provider_id: provider.provider_id.clone(),
                },
            ))
            .await
    }

    pub async fn next_events(&mut self) -> Result<Vec<ClientEvent>, ClientError> {
        self.session.next_client_events().await
    }
}
