use std::{
    future::Future,
    path::{Path, PathBuf},
    pin::Pin,
    sync::Arc,
    time::{Duration, Instant},
};

use orion_core::{decode_from_slice_with, encode_to_vec};
use orion_transport_common::{
    ConnectionTasks, DEFAULT_MAX_TRANSPORT_PAYLOAD_BYTES, DEFAULT_TRANSPORT_IO_TIMEOUT,
    DEFAULT_TRANSPORT_MAX_CONCURRENT_CONNECTIONS,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{
        UnixListener, UnixStream,
        unix::{OwnedReadHalf, OwnedWriteHalf},
    },
    sync::Semaphore,
};

use crate::{ControlEnvelope, IpcTransportError, LocalAddress, UnixPeerIdentity};

/// Synchronous Unix control handler boundary.
///
/// Implementations run inside async connection tasks. Keep direct work short and move durable
/// persistence, filesystem access, or other blocking operations behind explicit worker handoff.
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

    fn handle_control_with_identity_async(
        &self,
        envelope: ControlEnvelope,
        identity: Option<UnixPeerIdentity>,
    ) -> Pin<Box<dyn Future<Output = Result<ControlEnvelope, IpcTransportError>> + Send + '_>> {
        Box::pin(async move { self.handle_control_with_identity(envelope, identity) })
    }

    fn record_transport_error(&self, _error: &IpcTransportError) {}

    fn record_control_exchange(
        &self,
        _source: &LocalAddress,
        _bytes_received: u64,
        _bytes_sent: u64,
        _duration: Duration,
    ) {
    }
}

pub struct UnixControlServer {
    socket_path: PathBuf,
    listener: UnixListener,
    handler: Arc<dyn UnixControlHandler>,
    max_payload_bytes: usize,
    io_timeout: Duration,
    max_connections: usize,
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
            max_payload_bytes: DEFAULT_MAX_TRANSPORT_PAYLOAD_BYTES,
            io_timeout: DEFAULT_TRANSPORT_IO_TIMEOUT,
            max_connections: DEFAULT_TRANSPORT_MAX_CONCURRENT_CONNECTIONS,
        })
    }

    pub fn with_max_payload_bytes(mut self, max_payload_bytes: usize) -> Self {
        self.max_payload_bytes = max_payload_bytes.max(1);
        self
    }

    pub fn with_io_timeout(mut self, io_timeout: Duration) -> Self {
        self.io_timeout = io_timeout.max(Duration::from_millis(1));
        self
    }

    pub fn with_max_connections(mut self, max_connections: usize) -> Self {
        self.max_connections = max_connections.max(1);
        self
    }

    pub fn socket_path(&self) -> &Path {
        &self.socket_path
    }

    pub async fn serve(self) -> Result<(), IpcTransportError> {
        let semaphore = Arc::new(Semaphore::new(self.max_connections.max(1)));
        loop {
            let permit =
                semaphore.clone().acquire_owned().await.map_err(|_| {
                    IpcTransportError::AcceptFailed("connection limiter closed".into())
                })?;
            let (stream, _) = self
                .listener
                .accept()
                .await
                .map_err(|err| IpcTransportError::AcceptFailed(err.to_string()))?;
            let handler = self.handler.clone();
            let max_payload_bytes = self.max_payload_bytes;
            let io_timeout = self.io_timeout;
            tokio::spawn(async move {
                let _permit = permit;
                let _ = handle_stream(stream, handler, max_payload_bytes, io_timeout).await;
            });
        }
    }

    pub async fn serve_with_shutdown<F>(self, shutdown: F) -> Result<(), IpcTransportError>
    where
        F: std::future::Future<Output = ()>,
    {
        let mut shutdown = std::pin::pin!(shutdown);
        let mut connection_tasks = ConnectionTasks::new();
        let semaphore = Arc::new(Semaphore::new(self.max_connections.max(1)));
        loop {
            let permit =
                semaphore.clone().acquire_owned().await.map_err(|_| {
                    IpcTransportError::AcceptFailed("connection limiter closed".into())
                })?;
            tokio::select! {
                accepted = self.listener.accept() => {
                    let (stream, _) = accepted
                        .map_err(|err| IpcTransportError::AcceptFailed(err.to_string()))?;
                    let handler = self.handler.clone();
                    let max_payload_bytes = self.max_payload_bytes;
                    let io_timeout = self.io_timeout;
                    connection_tasks.spawn(async move {
                        let _permit = permit;
                        handle_stream(stream, handler, max_payload_bytes, io_timeout).await
                    });
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
    max_payload_bytes: usize,
    io_timeout: Duration,
}

pub struct UnixControlStreamClient {
    stream: UnixStream,
    max_payload_bytes: usize,
    io_timeout: Duration,
}

pub struct UnixControlExchange {
    pub envelope: ControlEnvelope,
    pub bytes_sent: usize,
    pub bytes_received: usize,
}

impl UnixControlClient {
    pub fn new(socket_path: impl AsRef<Path>) -> Self {
        Self {
            socket_path: socket_path.as_ref().to_path_buf(),
            max_payload_bytes: DEFAULT_MAX_TRANSPORT_PAYLOAD_BYTES,
            io_timeout: DEFAULT_TRANSPORT_IO_TIMEOUT,
        }
    }

    pub fn with_max_payload_bytes(mut self, max_payload_bytes: usize) -> Self {
        self.max_payload_bytes = max_payload_bytes.max(1);
        self
    }

    pub fn with_io_timeout(mut self, io_timeout: Duration) -> Self {
        self.io_timeout = io_timeout.max(Duration::from_millis(1));
        self
    }

    pub async fn send(
        &self,
        envelope: ControlEnvelope,
    ) -> Result<ControlEnvelope, IpcTransportError> {
        self.send_metered(envelope)
            .await
            .map(|exchange| exchange.envelope)
    }

    pub async fn send_metered(
        &self,
        envelope: ControlEnvelope,
    ) -> Result<UnixControlExchange, IpcTransportError> {
        let mut stream = timed_connect(
            self.io_timeout,
            UnixStream::connect(&self.socket_path),
            "IPC connect",
        )
        .await?;
        let bytes = encode_to_vec(&envelope)
            .map_err(|err| IpcTransportError::EncodeFailed(err.to_string()))?;
        let bytes_sent = bytes.len();
        timed(self.io_timeout, stream.write_all(&bytes), "IPC write")
            .await
            .map_err(|err| IpcTransportError::WriteFailed(err.to_string()))?;
        timed(self.io_timeout, stream.shutdown(), "IPC shutdown")
            .await
            .map_err(|err| IpcTransportError::WriteFailed(err.to_string()))?;

        let mut response = Vec::new();
        timed(
            self.io_timeout,
            (&mut stream)
                .take(self.max_payload_bytes as u64 + 1)
                .read_to_end(&mut response),
            "IPC read",
        )
        .await
        .map_err(|err| IpcTransportError::ReadFailed(err.to_string()))?;
        if response.len() > self.max_payload_bytes {
            return Err(IpcTransportError::DecodeFailed(
                "control response exceeds maximum transport payload size".into(),
            ));
        }
        let bytes_received = response.len();

        Ok(UnixControlExchange {
            envelope: decode_from_slice_with(&response, IpcTransportError::DecodeFailed)?,
            bytes_sent,
            bytes_received,
        })
    }
}

impl UnixControlStreamClient {
    pub async fn connect(socket_path: impl AsRef<Path>) -> Result<Self, IpcTransportError> {
        let stream = timed_connect(
            DEFAULT_TRANSPORT_IO_TIMEOUT,
            UnixStream::connect(socket_path.as_ref()),
            "IPC stream connect",
        )
        .await?;
        Ok(Self {
            stream,
            max_payload_bytes: DEFAULT_MAX_TRANSPORT_PAYLOAD_BYTES,
            io_timeout: DEFAULT_TRANSPORT_IO_TIMEOUT,
        })
    }

    pub fn with_max_payload_bytes(mut self, max_payload_bytes: usize) -> Self {
        self.max_payload_bytes = max_payload_bytes.max(1);
        self
    }

    pub fn with_io_timeout(mut self, io_timeout: Duration) -> Self {
        self.io_timeout = io_timeout.max(Duration::from_millis(1));
        self
    }

    pub async fn send(&mut self, envelope: &ControlEnvelope) -> Result<(), IpcTransportError> {
        self.send_metered(envelope).await.map(|_| ())
    }

    pub async fn send_metered(
        &mut self,
        envelope: &ControlEnvelope,
    ) -> Result<usize, IpcTransportError> {
        timed(
            self.io_timeout,
            write_control_frame_with_limit_metered(
                &mut self.stream,
                envelope,
                self.max_payload_bytes,
            ),
            "IPC stream write",
        )
        .await
        .map_err(IpcTransportError::WriteFailed)
    }

    pub async fn recv(&mut self) -> Result<Option<ControlEnvelope>, IpcTransportError> {
        self.recv_metered()
            .await
            .map(|envelope| envelope.map(|(envelope, _)| envelope))
    }

    pub async fn recv_metered(
        &mut self,
    ) -> Result<Option<(ControlEnvelope, usize)>, IpcTransportError> {
        timed(
            self.io_timeout,
            read_control_frame_with_limit_metered(&mut self.stream, self.max_payload_bytes),
            "IPC stream read",
        )
        .await
        .map_err(IpcTransportError::ReadFailed)
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
    write_control_frame_with_limit(writer, envelope, DEFAULT_MAX_TRANSPORT_PAYLOAD_BYTES).await
}

pub async fn write_control_frame_with_limit<W>(
    writer: &mut W,
    envelope: &ControlEnvelope,
    max_payload_bytes: usize,
) -> Result<(), IpcTransportError>
where
    W: AsyncWriteExt + Unpin,
{
    write_control_frame_with_limit_metered(writer, envelope, max_payload_bytes)
        .await
        .map(|_| ())
}

pub async fn write_control_frame_with_limit_metered<W>(
    writer: &mut W,
    envelope: &ControlEnvelope,
    max_payload_bytes: usize,
) -> Result<usize, IpcTransportError>
where
    W: AsyncWriteExt + Unpin,
{
    let max_payload_bytes = max_payload_bytes.max(1);
    let bytes =
        encode_to_vec(envelope).map_err(|err| IpcTransportError::EncodeFailed(err.to_string()))?;
    if bytes.len() > max_payload_bytes {
        return Err(IpcTransportError::EncodeFailed(
            "control frame exceeds maximum transport payload size".into(),
        ));
    }
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
    Ok(bytes.len() + std::mem::size_of::<u32>())
}

pub async fn read_control_frame<R>(
    reader: &mut R,
) -> Result<Option<ControlEnvelope>, IpcTransportError>
where
    R: AsyncReadExt + Unpin,
{
    read_control_frame_with_limit(reader, DEFAULT_MAX_TRANSPORT_PAYLOAD_BYTES).await
}

pub async fn read_control_frame_with_limit<R>(
    reader: &mut R,
    max_payload_bytes: usize,
) -> Result<Option<ControlEnvelope>, IpcTransportError>
where
    R: AsyncReadExt + Unpin,
{
    read_control_frame_with_limit_metered(reader, max_payload_bytes)
        .await
        .map(|frame| frame.map(|(envelope, _)| envelope))
}

pub async fn read_control_frame_with_limit_metered<R>(
    reader: &mut R,
    max_payload_bytes: usize,
) -> Result<Option<(ControlEnvelope, usize)>, IpcTransportError>
where
    R: AsyncReadExt + Unpin,
{
    let max_payload_bytes = max_payload_bytes.max(1);
    let len = match reader.read_u32_le().await {
        Ok(len) => len,
        Err(err) if err.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(None),
        Err(err) => return Err(IpcTransportError::ReadFailed(err.to_string())),
    };
    if len as usize > max_payload_bytes {
        return Err(IpcTransportError::DecodeFailed(
            "control frame exceeds maximum transport payload size".into(),
        ));
    }
    let mut bytes = vec![0_u8; len as usize];
    reader
        .read_exact(&mut bytes)
        .await
        .map_err(|err| IpcTransportError::ReadFailed(err.to_string()))?;
    let bytes_received = bytes.len() + std::mem::size_of::<u32>();
    decode_from_slice_with(&bytes, IpcTransportError::DecodeFailed)
        .map(|envelope| Some((envelope, bytes_received)))
}

async fn handle_stream(
    mut stream: UnixStream,
    handler: Arc<dyn UnixControlHandler>,
    max_payload_bytes: usize,
    io_timeout: Duration,
) -> Result<(), IpcTransportError> {
    let max_payload_bytes = max_payload_bytes.max(1);
    let identity = stream
        .peer_cred()
        .map(|cred| UnixPeerIdentity {
            pid: cred.pid().and_then(|pid| u32::try_from(pid).ok()),
            uid: cred.uid(),
            gid: cred.gid(),
        })
        .ok();
    let mut request = Vec::new();
    if let Err(err) = timed(
        io_timeout,
        (&mut stream)
            .take(max_payload_bytes as u64 + 1)
            .read_to_end(&mut request),
        "IPC read",
    )
    .await
    .map_err(IpcTransportError::ReadFailed)
    {
        handler.record_transport_error(&err);
        return Err(err);
    }
    if request.len() > max_payload_bytes {
        let err = IpcTransportError::DecodeFailed(
            "control envelope exceeds maximum transport payload size".into(),
        );
        handler.record_transport_error(&err);
        return Err(err);
    }

    let bytes_received = request.len().min(u64::MAX as usize) as u64;
    let started = Instant::now();
    let envelope: ControlEnvelope =
        match decode_from_slice_with(&request, IpcTransportError::DecodeFailed) {
            Ok(envelope) => envelope,
            Err(err) => {
                handler.record_transport_error(&err);
                return Err(err);
            }
        };
    let source = envelope.source.clone();
    let response = match handler
        .handle_control_with_identity_async(envelope, identity)
        .await
    {
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
    let bytes_sent = bytes.len().min(u64::MAX as usize) as u64;

    if let Err(err) = timed(io_timeout, stream.write_all(&bytes), "IPC write")
        .await
        .map_err(IpcTransportError::WriteFailed)
    {
        handler.record_transport_error(&err);
        return Err(err);
    }
    if let Err(err) = timed(io_timeout, stream.shutdown(), "IPC shutdown")
        .await
        .map_err(IpcTransportError::WriteFailed)
    {
        handler.record_transport_error(&err);
        return Err(err);
    }
    handler.record_control_exchange(&source, bytes_received, bytes_sent, started.elapsed());

    Ok(())
}

async fn timed<T>(
    timeout: Duration,
    future: impl Future<Output = Result<T, impl std::fmt::Display>>,
    context: &str,
) -> Result<T, String> {
    match tokio::time::timeout(timeout.max(Duration::from_millis(1)), future).await {
        Ok(Ok(value)) => Ok(value),
        Ok(Err(err)) => Err(err.to_string()),
        Err(_) => Err(format!(
            "{context} timed out after {} ms",
            timeout.as_millis()
        )),
    }
}

async fn timed_connect<T>(
    timeout: Duration,
    future: impl Future<Output = Result<T, std::io::Error>>,
    context: &str,
) -> Result<T, IpcTransportError> {
    match tokio::time::timeout(timeout.max(Duration::from_millis(1)), future).await {
        Ok(Ok(value)) => Ok(value),
        Ok(Err(err)) => Err(ipc_connect_error(err)),
        Err(_) => Err(IpcTransportError::ConnectFailed(format!(
            "{context} timed out after {} ms",
            timeout.as_millis()
        ))),
    }
}

fn ipc_connect_error(err: std::io::Error) -> IpcTransportError {
    let message = err.to_string();
    if err.kind() == std::io::ErrorKind::ConnectionRefused {
        IpcTransportError::ConnectionRefused(message)
    } else {
        IpcTransportError::ConnectFailed(message)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn unary_read_times_out_stalled_reader() {
        let (_writer, mut reader) = tokio::io::duplex(8);
        let mut request = Vec::new();
        let err = timed(
            Duration::from_millis(10),
            (&mut reader)
                .take(DEFAULT_MAX_TRANSPORT_PAYLOAD_BYTES as u64 + 1)
                .read_to_end(&mut request),
            "IPC read",
        )
        .await
        .expect_err("stalled IPC reader should time out");
        assert!(err.contains("timed out"));
    }
}
