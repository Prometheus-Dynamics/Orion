use crate::{
    error::ClientError,
    session::{ClientIdentity, SessionConfig, ensure_client_role},
};
use orion_control_plane::{ClientEvent, ClientHello, ClientRole, ControlMessage};
use orion_transport_ipc::{ControlEnvelope, LocalAddress, UnixControlStreamClient};
use std::path::Path;

pub(crate) struct ClientEventStreamSession {
    client: UnixControlStreamClient,
    local_address: LocalAddress,
    daemon_address: LocalAddress,
}

impl ClientEventStreamSession {
    pub(crate) async fn connect(
        socket_path: impl AsRef<Path>,
        identity: ClientIdentity,
        config: SessionConfig,
        expected_role: ClientRole,
    ) -> Result<Self, ClientError> {
        ensure_client_role(&identity, expected_role.clone())?;

        let mut client = UnixControlStreamClient::connect(socket_path).await?;
        client
            .send(&ControlEnvelope {
                source: config.local_address.clone(),
                destination: config.daemon_address.clone(),
                message: ControlMessage::ClientHello(ClientHello {
                    client_name: identity.name.into(),
                    role: expected_role,
                }),
            })
            .await?;
        let Some(response) = client.recv().await? else {
            return Err(ClientError::NoMessageAvailable);
        };
        match response.message {
            ControlMessage::ClientWelcome(_) => Ok(Self {
                client,
                local_address: config.local_address,
                daemon_address: config.daemon_address,
            }),
            ControlMessage::Rejected(reason) => Err(ClientError::Rejected(reason)),
            _ => Err(ClientError::NoMessageAvailable),
        }
    }

    pub(crate) async fn send(&mut self, message: ControlMessage) -> Result<(), ClientError> {
        self.client
            .send(&ControlEnvelope {
                source: self.local_address.clone(),
                destination: self.daemon_address.clone(),
                message,
            })
            .await?;
        Ok(())
    }

    pub(crate) async fn subscribe_and_expect_accepted(
        &mut self,
        message: ControlMessage,
    ) -> Result<(), ClientError> {
        self.send(message).await?;
        match self.recv_message().await? {
            ControlMessage::Accepted => Ok(()),
            ControlMessage::Rejected(reason) => Err(ClientError::Rejected(reason)),
            _ => Err(ClientError::NoMessageAvailable),
        }
    }

    pub(crate) async fn recv_message(&mut self) -> Result<ControlMessage, ClientError> {
        loop {
            let Some(response) = self.client.recv().await? else {
                return Err(ClientError::NoMessageAvailable);
            };
            match response.message {
                ControlMessage::Ping => {
                    self.send(ControlMessage::Pong).await?;
                }
                message => return Ok(message),
            }
        }
    }

    pub(crate) async fn next_client_events(&mut self) -> Result<Vec<ClientEvent>, ClientError> {
        match self.recv_message().await? {
            ControlMessage::ClientEvents(events) => Ok(events),
            ControlMessage::Rejected(reason) => Err(ClientError::Rejected(reason)),
            _ => Err(ClientError::NoMessageAvailable),
        }
    }
}
