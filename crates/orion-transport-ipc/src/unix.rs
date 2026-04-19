use std::{
    path::{Path, PathBuf},
    sync::Arc,
};

use orion_core::{decode_from_slice_with, encode_to_vec};
use orion_transport_common::ConnectionTasks;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{
        UnixListener, UnixStream,
        unix::{OwnedReadHalf, OwnedWriteHalf},
    },
};

use crate::{ControlEnvelope, IpcTransportError, UnixPeerIdentity};

pub trait UnixControlHandler: Send + Sync + 'static {
    fn handle_control(
        &self,
        envelope: ControlEnvelope,
    ) -> Result<ControlEnvelope, IpcTransportError>;

    fn handle_control_with_identity(
        &self,
        envelope: ControlEnvelope,
        _identity: Option<UnixPeerIdentity>,
    ) -> Result<ControlEnvelope, IpcTransportError> {
        self.handle_control(envelope)
    }

    fn record_transport_error(&self, _error: &IpcTransportError) {}
}

pub struct UnixControlServer {
    socket_path: PathBuf,
    listener: UnixListener,
    handler: Arc<dyn UnixControlHandler>,
}

impl UnixControlServer {
    pub async fn bind(
        socket_path: impl AsRef<Path>,
        handler: Arc<dyn UnixControlHandler>,
    ) -> Result<Self, IpcTransportError> {
        let socket_path = socket_path.as_ref().to_path_buf();
        if socket_path.exists() {
            let _ = tokio::fs::remove_file(&socket_path).await;
        }

        let listener = UnixListener::bind(&socket_path)
            .map_err(|err| IpcTransportError::BindFailed(err.to_string()))?;

        Ok(Self {
            socket_path,
            listener,
            handler,
        })
    }

    pub fn socket_path(&self) -> &Path {
        &self.socket_path
    }

    pub async fn serve(self) -> Result<(), IpcTransportError> {
        loop {
            let (stream, _) = self
                .listener
                .accept()
                .await
                .map_err(|err| IpcTransportError::AcceptFailed(err.to_string()))?;
            let handler = self.handler.clone();
            tokio::spawn(async move {
                let _ = handle_stream(stream, handler).await;
            });
        }
    }

    pub async fn serve_with_shutdown<F>(self, shutdown: F) -> Result<(), IpcTransportError>
    where
        F: std::future::Future<Output = ()>,
    {
        let mut shutdown = std::pin::pin!(shutdown);
        let mut connection_tasks = ConnectionTasks::new();
        loop {
            tokio::select! {
                accepted = self.listener.accept() => {
                    let (stream, _) = accepted
                        .map_err(|err| IpcTransportError::AcceptFailed(err.to_string()))?;
                    let handler = self.handler.clone();
                    connection_tasks.spawn(handle_stream(stream, handler));
                }
                _ = &mut shutdown => {
                    connection_tasks.abort_all().await;
                    break Ok(());
                }
            }
        }
    }
}

#[derive(Clone, Debug)]
pub struct UnixControlClient {
    socket_path: PathBuf,
}

pub struct UnixControlStreamClient {
    stream: UnixStream,
}

impl UnixControlClient {
    pub fn new(socket_path: impl AsRef<Path>) -> Self {
        Self {
            socket_path: socket_path.as_ref().to_path_buf(),
        }
    }

    pub async fn send(
        &self,
        envelope: ControlEnvelope,
    ) -> Result<ControlEnvelope, IpcTransportError> {
        let mut stream = UnixStream::connect(&self.socket_path)
            .await
            .map_err(|err| IpcTransportError::ConnectFailed(err.to_string()))?;
        let bytes = encode_to_vec(&envelope)
            .map_err(|err| IpcTransportError::EncodeFailed(err.to_string()))?;
        stream
            .write_all(&bytes)
            .await
            .map_err(|err| IpcTransportError::WriteFailed(err.to_string()))?;
        stream
            .shutdown()
            .await
            .map_err(|err| IpcTransportError::WriteFailed(err.to_string()))?;

        let mut response = Vec::new();
        stream
            .read_to_end(&mut response)
            .await
            .map_err(|err| IpcTransportError::ReadFailed(err.to_string()))?;

        decode_from_slice_with(&response, IpcTransportError::DecodeFailed)
    }
}

impl UnixControlStreamClient {
    pub async fn connect(socket_path: impl AsRef<Path>) -> Result<Self, IpcTransportError> {
        let stream = UnixStream::connect(socket_path.as_ref())
            .await
            .map_err(|err| IpcTransportError::ConnectFailed(err.to_string()))?;
        Ok(Self { stream })
    }

    pub async fn send(&mut self, envelope: &ControlEnvelope) -> Result<(), IpcTransportError> {
        write_control_frame(&mut self.stream, envelope).await
    }

    pub async fn recv(&mut self) -> Result<Option<ControlEnvelope>, IpcTransportError> {
        read_control_frame(&mut self.stream).await
    }

    pub fn into_split(self) -> (OwnedReadHalf, OwnedWriteHalf) {
        self.stream.into_split()
    }
}

pub async fn write_control_frame<W>(
    writer: &mut W,
    envelope: &ControlEnvelope,
) -> Result<(), IpcTransportError>
where
    W: AsyncWriteExt + Unpin,
{
    let bytes =
        encode_to_vec(envelope).map_err(|err| IpcTransportError::EncodeFailed(err.to_string()))?;
    let len = u32::try_from(bytes.len())
        .map_err(|_| IpcTransportError::EncodeFailed("control frame too large".into()))?;
    writer
        .write_u32_le(len)
        .await
        .map_err(|err| IpcTransportError::WriteFailed(err.to_string()))?;
    writer
        .write_all(&bytes)
        .await
        .map_err(|err| IpcTransportError::WriteFailed(err.to_string()))?;
    writer
        .flush()
        .await
        .map_err(|err| IpcTransportError::WriteFailed(err.to_string()))?;
    Ok(())
}

pub async fn read_control_frame<R>(
    reader: &mut R,
) -> Result<Option<ControlEnvelope>, IpcTransportError>
where
    R: AsyncReadExt + Unpin,
{
    let len = match reader.read_u32_le().await {
        Ok(len) => len,
        Err(err) if err.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(None),
        Err(err) => return Err(IpcTransportError::ReadFailed(err.to_string())),
    };
    let mut bytes = vec![0_u8; len as usize];
    reader
        .read_exact(&mut bytes)
        .await
        .map_err(|err| IpcTransportError::ReadFailed(err.to_string()))?;
    decode_from_slice_with(&bytes, IpcTransportError::DecodeFailed).map(Some)
}

async fn handle_stream(
    mut stream: UnixStream,
    handler: Arc<dyn UnixControlHandler>,
) -> Result<(), IpcTransportError> {
    let identity = stream
        .peer_cred()
        .map(|cred| UnixPeerIdentity {
            pid: cred.pid().and_then(|pid| u32::try_from(pid).ok()),
            uid: cred.uid(),
            gid: cred.gid(),
        })
        .ok();
    let mut request = Vec::new();
    if let Err(err) = stream
        .read_to_end(&mut request)
        .await
        .map_err(|err| IpcTransportError::ReadFailed(err.to_string()))
    {
        handler.record_transport_error(&err);
        return Err(err);
    }

    let envelope: ControlEnvelope =
        match decode_from_slice_with(&request, IpcTransportError::DecodeFailed) {
            Ok(envelope) => envelope,
            Err(err) => {
                handler.record_transport_error(&err);
                return Err(err);
            }
        };
    let response = match handler.handle_control_with_identity(envelope, identity) {
        Ok(response) => response,
        Err(err) => {
            handler.record_transport_error(&err);
            return Err(err);
        }
    };
    let bytes = match encode_to_vec(&response)
        .map_err(|err| IpcTransportError::EncodeFailed(err.to_string()))
    {
        Ok(bytes) => bytes,
        Err(err) => {
            handler.record_transport_error(&err);
            return Err(err);
        }
    };

    if let Err(err) = stream
        .write_all(&bytes)
        .await
        .map_err(|err| IpcTransportError::WriteFailed(err.to_string()))
    {
        handler.record_transport_error(&err);
        return Err(err);
    }
    if let Err(err) = stream
        .shutdown()
        .await
        .map_err(|err| IpcTransportError::WriteFailed(err.to_string()))
    {
        handler.record_transport_error(&err);
        return Err(err);
    }

    Ok(())
}
