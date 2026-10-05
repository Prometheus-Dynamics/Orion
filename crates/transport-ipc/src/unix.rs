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

use crate::{
    ControlEnvelope, ControlFrameReadState, IpcTransportError, LocalAddress, UnixPeerIdentity,
    preamble::{CONTROL_PREAMBLE_BYTES, check_control_preamble, control_preamble},
    stream_frame::write_control_frame_with_limit_metered,
};

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
            connection_tasks.reap_finished();
            // Wait for a permit inside `select!` so shutdown is honoured while the limiter is
            // saturated. `acquire_owned` is cancel-safe (the waiter is simply dequeued).
            let permit = tokio::select! {
                permit = semaphore.clone().acquire_owned() => {
                    permit.map_err(|_| {
                        IpcTransportError::AcceptFailed("connection limiter closed".into())
                    })?
                }
                _ = &mut shutdown => {
                    connection_tasks.abort_all().await;
                    break Ok(());
                }
            };
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
    read_state: ControlFrameReadState,
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
        let bytes_sent = bytes.len() + CONTROL_PREAMBLE_BYTES;
        timed(
            self.io_timeout,
            write_unary_message(&mut stream, &bytes),
            "IPC write",
        )
        .await
        .map_err(IpcTransportError::WriteFailed)?;
        timed(self.io_timeout, stream.shutdown(), "IPC shutdown")
            .await
            .map_err(IpcTransportError::WriteFailed)?;

        let response = timeout_ipc(
            self.io_timeout,
            read_unary_message(&mut stream, self.max_payload_bytes, "control response"),
            "IPC read",
        )
        .await?;
        let bytes_received = response.len() + CONTROL_PREAMBLE_BYTES;

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
            read_state: ControlFrameReadState::new(),
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
        timed_stream(
            self.io_timeout,
            write_control_frame_with_limit_metered(
                &mut self.stream,
                envelope,
                self.max_payload_bytes,
            ),
            "IPC stream write",
            IpcTransportError::WriteFailed,
        )
        .await
    }

    /// Receives the next frame, failing after the configured I/O timeout.
    ///
    /// Frame reads are cancel-safe: a partially received frame is kept and resumed by the next
    /// `recv*` call, so these futures may be raced in `tokio::select!` or wrapped in timeouts.
    pub async fn recv(&mut self) -> Result<Option<ControlEnvelope>, IpcTransportError> {
        self.recv_metered()
            .await
            .map(|envelope| envelope.map(|(envelope, _)| envelope))
    }

    pub async fn recv_metered(
        &mut self,
    ) -> Result<Option<(ControlEnvelope, usize)>, IpcTransportError> {
        timed_stream(
            self.io_timeout,
            self.read_state
                .read_metered(&mut self.stream, self.max_payload_bytes),
            "IPC stream read",
            IpcTransportError::ReadFailed,
        )
        .await
    }

    pub async fn recv_wait(&mut self) -> Result<Option<ControlEnvelope>, IpcTransportError> {
        self.recv_wait_metered()
            .await
            .map(|envelope| envelope.map(|(envelope, _)| envelope))
    }

    pub async fn recv_wait_metered(
        &mut self,
    ) -> Result<Option<(ControlEnvelope, usize)>, IpcTransportError> {
        self.read_state
            .read_metered(&mut self.stream, self.max_payload_bytes)
            .await
    }

    /// Splits the underlying socket. Bytes of a partially received frame (only present if a
    /// `recv*` call was cancelled mid-frame) are discarded.
    pub fn into_split(self) -> (OwnedReadHalf, OwnedWriteHalf) {
        self.stream.into_split()
    }
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
    let request = match timeout_ipc(
        io_timeout,
        read_unary_message(&mut stream, max_payload_bytes, "control envelope"),
        "IPC read",
    )
    .await
    {
        Ok(request) => request,
        Err(err) => {
            handler.record_transport_error(&err);
            if matches!(err, IpcTransportError::ProtocolMismatch { .. }) {
                // Answer with our preamble only, so the client reports a typed mismatch too.
                let _ = timed(
                    io_timeout,
                    write_unary_message(&mut stream, &[]),
                    "IPC write",
                )
                .await;
                let _ = timed(io_timeout, stream.shutdown(), "IPC shutdown").await;
            }
            return Err(err);
        }
    };

    let bytes_received = (request.len() + CONTROL_PREAMBLE_BYTES).min(u64::MAX as usize) as u64;
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
    let bytes_sent = (bytes.len() + CONTROL_PREAMBLE_BYTES).min(u64::MAX as usize) as u64;

    if let Err(err) = timed(
        io_timeout,
        write_unary_message(&mut stream, &bytes),
        "IPC write",
    )
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

/// Like [`timed`], but keeps typed inner errors and only maps the timeout.
async fn timeout_ipc<T>(
    timeout: Duration,
    future: impl Future<Output = Result<T, IpcTransportError>>,
    context: &str,
) -> Result<T, IpcTransportError> {
    tokio::time::timeout(timeout.max(Duration::from_millis(1)), future)
        .await
        .unwrap_or_else(|_| {
            Err(IpcTransportError::ReadFailed(format!(
                "{context} timed out after {} ms",
                timeout.as_millis()
            )))
        })
}

/// Stream I/O timeout wrapper: inner errors are folded into `wrap` as before, except protocol
/// mismatches, which stay typed so callers can report them clearly.
async fn timed_stream<T>(
    timeout: Duration,
    future: impl Future<Output = Result<T, IpcTransportError>>,
    context: &str,
    wrap: fn(String) -> IpcTransportError,
) -> Result<T, IpcTransportError> {
    match tokio::time::timeout(timeout.max(Duration::from_millis(1)), future).await {
        Ok(Ok(value)) => Ok(value),
        Ok(Err(err @ IpcTransportError::ProtocolMismatch { .. })) => Err(err),
        Ok(Err(err)) => Err(wrap(err.to_string())),
        Err(_) => Err(wrap(format!(
            "{context} timed out after {} ms",
            timeout.as_millis()
        ))),
    }
}

async fn write_unary_message<W>(writer: &mut W, archive: &[u8]) -> std::io::Result<()>
where
    W: AsyncWriteExt + Unpin,
{
    writer.write_all(&control_preamble()).await?;
    writer.write_all(archive).await
}

/// Reads `[preamble][archive]` until EOF. The whole (bounded) message is drained before the
/// preamble is checked so a mismatched peer is never left with unread bytes; the archive is only
/// returned, never decoded, when the preamble matches.
async fn read_unary_message<R>(
    reader: &mut R,
    max_payload_bytes: usize,
    what: &str,
) -> Result<Vec<u8>, IpcTransportError>
where
    R: AsyncReadExt + Unpin,
{
    let max_payload_bytes = max_payload_bytes.max(1);
    let mut preamble = [0_u8; CONTROL_PREAMBLE_BYTES];
    let mut filled = 0;
    while filled < CONTROL_PREAMBLE_BYTES {
        let read = reader
            .read(&mut preamble[filled..])
            .await
            .map_err(|err| IpcTransportError::ReadFailed(err.to_string()))?;
        if read == 0 {
            break;
        }
        filled += read;
    }
    // A separate buffer keeps the archive at the start of its allocation for aligned decode.
    let mut archive = Vec::new();
    (&mut *reader)
        .take(max_payload_bytes as u64 + 1)
        .read_to_end(&mut archive)
        .await
        .map_err(|err| IpcTransportError::ReadFailed(err.to_string()))?;
    check_control_preamble(&preamble[..filled])?;
    if archive.len() > max_payload_bytes {
        return Err(IpcTransportError::DecodeFailed(format!(
            "{what} exceeds maximum transport payload size"
        )));
    }
    Ok(archive)
}

pub(crate) async fn timed_connect<T>(
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
    use crate::write_control_frame;
    use orion_control_plane::ControlMessage;
    use std::time::{SystemTime, UNIX_EPOCH};
    use tokio::net::UnixListener;

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

    #[tokio::test]
    async fn stream_recv_wait_does_not_treat_idle_as_timeout() {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("time should advance")
            .as_nanos();
        let socket_path = std::env::temp_dir().join(format!("orion-ipc-stream-idle-{nanos}.sock"));
        let _ = tokio::fs::remove_file(&socket_path).await;
        let listener = UnixListener::bind(&socket_path).expect("listener should bind");
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.expect("accept should work");
            tokio::time::sleep(Duration::from_millis(50)).await;
            write_control_frame(
                &mut stream,
                &ControlEnvelope {
                    source: LocalAddress::new("orion"),
                    destination: LocalAddress::new("client"),
                    message: ControlMessage::Ping,
                },
            )
            .await
            .expect("ping should write");
        });

        let mut client = UnixControlStreamClient::connect(&socket_path)
            .await
            .expect("client should connect")
            .with_io_timeout(Duration::from_millis(10));
        let envelope = client
            .recv_wait()
            .await
            .expect("idle stream recv should not timeout")
            .expect("server should send a frame");

        assert_eq!(envelope.message, ControlMessage::Ping);
        server.await.expect("server should complete");
        let _ = tokio::fs::remove_file(&socket_path).await;
    }
}
