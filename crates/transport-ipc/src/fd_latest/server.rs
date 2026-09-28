use std::{
    future::Future,
    io,
    os::{
        fd::{AsRawFd, OwnedFd, RawFd},
        unix::fs::{FileTypeExt, MetadataExt},
    },
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::{Instant, SystemTime},
};

use orion_transport_common::ConnectionTasks;
use tokio::{
    io::Interest,
    net::{UnixListener, UnixStream},
    sync::{Semaphore, watch},
};

use super::{
    UnixFdLatestConfig,
    protocol::{
        LatestRequest, REQUEST_BYTES, RESPONSE_HEADER_BYTES, ResponseHeader, ResponseStatus,
        system_time_to_unix_nanos,
    },
};
use crate::{IpcTransportError, UnixFdFrame, recv_unix_fd_frame_async, send_unix_fd_frame_async};

/// The retained latest frame. Descriptors are dup'd per reply; the originals close once the frame
/// is replaced and no in-flight reply still references it.
struct StoredFrame {
    sequence: u64,
    published_at: SystemTime,
    published_instant: Instant,
    payload: Vec<u8>,
    fds: Vec<OwnedFd>,
}

type LatestSlot = Option<Arc<StoredFrame>>;

struct LatestState {
    slot: watch::Sender<LatestSlot>,
    last_sequence: AtomicU64,
    max_payload_bytes: usize,
    max_fds: usize,
}

/// Producer handle for a [`UnixFdLatestServer`]. Cheap to clone and usable while the server is
/// serving.
#[derive(Clone)]
pub struct UnixFdLatestPublisher {
    state: Arc<LatestState>,
}

impl std::fmt::Debug for UnixFdLatestPublisher {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("UnixFdLatestPublisher")
            .field("latest_sequence", &self.latest_sequence())
            .finish_non_exhaustive()
    }
}

impl UnixFdLatestPublisher {
    /// Replaces the latest frame and wakes waiting clients. Returns the new sequence number.
    pub fn publish(&self, payload: Vec<u8>, fds: Vec<OwnedFd>) -> Result<u64, IpcTransportError> {
        if payload.len() > self.state.max_payload_bytes {
            return Err(IpcTransportError::EncodeFailed(
                "fd latest payload exceeds maximum transport payload size".into(),
            ));
        }
        if fds.len() > self.state.max_fds {
            return Err(IpcTransportError::EncodeFailed(
                "fd latest descriptor count exceeds maximum transport descriptor count".into(),
            ));
        }
        let mut stored = Some((payload, fds));
        let mut sequence = 0;
        // The watch lock serialises concurrent publishers, so sequence order matches slot order.
        self.state.slot.send_modify(|slot| {
            let (payload, fds) = stored.take().expect("publish closure runs once");
            sequence = self.state.last_sequence.fetch_add(1, Ordering::Relaxed) + 1;
            *slot = Some(Arc::new(StoredFrame {
                sequence,
                published_at: SystemTime::now(),
                published_instant: Instant::now(),
                payload,
                fds,
            }));
        });
        Ok(sequence)
    }

    pub fn publish_frame(&self, frame: UnixFdFrame) -> Result<u64, IpcTransportError> {
        self.publish(frame.payload, frame.fds)
    }

    /// Drops the retained frame (closing its descriptors once in-flight replies finish). Sequence
    /// numbering continues from the last published value.
    pub fn clear(&self) {
        self.state.slot.send_modify(|slot| *slot = None);
    }

    /// Sequence of the most recently published frame, if any has been published.
    pub fn latest_sequence(&self) -> Option<u64> {
        match self.state.last_sequence.load(Ordering::Relaxed) {
            0 => None,
            sequence => Some(sequence),
        }
    }
}

/// Unix socket server that hands out the latest published fd frame to any number of clients.
///
/// The socket file is removed on bind if it is a stale socket (no live listener), and removed again
/// when the server is dropped or `serve*` returns, provided it is still the socket this server
/// created.
pub struct UnixFdLatestServer {
    socket_path: PathBuf,
    listener: UnixListener,
    publisher: UnixFdLatestPublisher,
    config: UnixFdLatestConfig,
    _socket_file: SocketFileGuard,
}

impl UnixFdLatestServer {
    pub async fn bind(socket_path: impl AsRef<Path>) -> Result<Self, IpcTransportError> {
        Self::bind_with_config(socket_path, UnixFdLatestConfig::default()).await
    }

    pub async fn bind_with_config(
        socket_path: impl AsRef<Path>,
        config: UnixFdLatestConfig,
    ) -> Result<Self, IpcTransportError> {
        let socket_path = socket_path.as_ref().to_path_buf();
        remove_stale_socket(&socket_path).await?;
        let listener = UnixListener::bind(&socket_path)
            .map_err(|err| IpcTransportError::BindFailed(err.to_string()))?;
        let socket_file = SocketFileGuard::new(socket_path.clone());
        let (slot, _) = watch::channel(None);
        let publisher = UnixFdLatestPublisher {
            state: Arc::new(LatestState {
                slot,
                last_sequence: AtomicU64::new(0),
                max_payload_bytes: config.max_payload_bytes,
                max_fds: config.max_fds,
            }),
        };
        Ok(Self {
            socket_path,
            listener,
            publisher,
            config,
            _socket_file: socket_file,
        })
    }

    pub fn socket_path(&self) -> &Path {
        &self.socket_path
    }

    pub fn config(&self) -> &UnixFdLatestConfig {
        &self.config
    }

    pub fn publisher(&self) -> UnixFdLatestPublisher {
        self.publisher.clone()
    }

    pub fn publish(&self, payload: Vec<u8>, fds: Vec<OwnedFd>) -> Result<u64, IpcTransportError> {
        self.publisher.publish(payload, fds)
    }

    pub async fn serve(self) -> Result<(), IpcTransportError> {
        self.serve_with_shutdown(std::future::pending()).await
    }

    /// Serves clients until `shutdown` resolves, then aborts open connections and removes the
    /// socket file.
    pub async fn serve_with_shutdown<F>(self, shutdown: F) -> Result<(), IpcTransportError>
    where
        F: Future<Output = ()>,
    {
        let mut shutdown = std::pin::pin!(shutdown);
        let mut connection_tasks = ConnectionTasks::new();
        let limiter = Arc::new(Semaphore::new(self.config.max_clients.max(1)));
        let result = loop {
            connection_tasks.reap_finished();
            tokio::select! {
                accepted = self.listener.accept() => {
                    let stream = match accepted {
                        Ok((stream, _)) => stream,
                        Err(err) => break Err(IpcTransportError::AcceptFailed(err.to_string())),
                    };
                    let Ok(permit) = limiter.clone().try_acquire_owned() else {
                        reject_busy(&stream, &self.config);
                        continue;
                    };
                    let state = self.publisher.state.clone();
                    let config = self.config.clone();
                    connection_tasks.spawn(async move {
                        let _permit = permit;
                        handle_connection(stream, state, config).await
                    });
                }
                _ = &mut shutdown => break Ok(()),
            }
        };
        connection_tasks.abort_all().await;
        result
    }
}

async fn handle_connection(
    stream: UnixStream,
    state: Arc<LatestState>,
    config: UnixFdLatestConfig,
) -> Result<(), IpcTransportError> {
    let mut slot = state.slot.subscribe();
    loop {
        let received = recv_unix_fd_frame_async(&stream, REQUEST_BYTES, 0);
        let received = match config.idle_timeout {
            Some(idle_timeout) => match tokio::time::timeout(idle_timeout, received).await {
                Ok(received) => received?,
                Err(_) => return Ok(()),
            },
            None => received.await?,
        };
        let Some(request) = received else {
            return Ok(());
        };
        let reply = match LatestRequest::decode(&request.payload)? {
            LatestRequest::Latest => {
                let current = slot.borrow_and_update().clone();
                match current {
                    Some(stored) => frame_reply(&stored, &config)?,
                    None => status_frame(ResponseStatus::Empty, 0),
                }
            }
            LatestRequest::NextAfter { sequence, wait } => {
                let Some(reply) = wait_for_newer(
                    &stream,
                    &mut slot,
                    sequence,
                    wait.min(config.max_wait),
                    &config,
                )
                .await?
                else {
                    return Ok(());
                };
                reply
            }
        };
        send_reply(&stream, &reply, &config).await?;
    }
}

/// Waits until a frame newer than `after` exists. Returns `None` if the peer disconnected.
async fn wait_for_newer(
    stream: &UnixStream,
    slot: &mut watch::Receiver<LatestSlot>,
    after: u64,
    wait: std::time::Duration,
    config: &UnixFdLatestConfig,
) -> Result<Option<UnixFdFrame>, IpcTransportError> {
    let deadline = tokio::time::Instant::now() + wait;
    let mut watch_peer = true;
    loop {
        let current = slot.borrow_and_update().clone();
        let latest_sequence = current.as_ref().map_or(0, |stored| stored.sequence);
        if let Some(stored) = current
            && stored.sequence > after
        {
            return frame_reply(&stored, config).map(Some);
        }
        let timeout_reply = || status_frame(ResponseStatus::Timeout, latest_sequence);
        tokio::select! {
            changed = slot.changed() => {
                if changed.is_err() {
                    return Ok(Some(timeout_reply()));
                }
            }
            _ = tokio::time::sleep_until(deadline) => return Ok(Some(timeout_reply())),
            closed = peer_closed(stream), if watch_peer => match closed {
                Ok(true) | Err(_) => return Ok(None),
                // A pipelined request is waiting; stop polling readiness until this reply is sent.
                Ok(false) => watch_peer = false,
            },
        }
    }
}

fn frame_reply(
    stored: &StoredFrame,
    config: &UnixFdLatestConfig,
) -> Result<UnixFdFrame, IpcTransportError> {
    let header = ResponseHeader {
        status: ResponseStatus::Frame,
        sequence: stored.sequence,
        published_at_unix_nanos: system_time_to_unix_nanos(stored.published_at),
    };
    if let Some(max_age) = config.max_age
        && stored.published_instant.elapsed() > max_age
    {
        return Ok(UnixFdFrame::new(
            ResponseHeader {
                status: ResponseStatus::Stale,
                ..header
            }
            .encode_with_payload(&[]),
            Vec::new(),
        ));
    }
    let fds = stored
        .fds
        .iter()
        .map(OwnedFd::try_clone)
        .collect::<io::Result<Vec<_>>>()
        .map_err(|err| {
            IpcTransportError::EncodeFailed(format!("failed to duplicate fd latest frame: {err}"))
        })?;
    Ok(UnixFdFrame::new(
        header.encode_with_payload(&stored.payload),
        fds,
    ))
}

fn status_frame(status: ResponseStatus, sequence: u64) -> UnixFdFrame {
    UnixFdFrame::new(
        ResponseHeader::status_only(status, sequence).encode_with_payload(&[]),
        Vec::new(),
    )
}

async fn send_reply(
    stream: &UnixStream,
    reply: &UnixFdFrame,
    config: &UnixFdLatestConfig,
) -> Result<(), IpcTransportError> {
    let send = send_unix_fd_frame_async(
        stream,
        reply,
        config.max_payload_bytes + RESPONSE_HEADER_BYTES,
        config.max_fds,
    );
    match tokio::time::timeout(config.io_timeout, send).await {
        Ok(sent) => sent.map(|_| ()),
        Err(_) => Err(IpcTransportError::WriteFailed(format!(
            "fd latest reply timed out after {} ms",
            config.io_timeout.as_millis()
        ))),
    }
}

/// Best-effort, non-blocking busy notice before closing an over-limit connection.
fn reject_busy(stream: &UnixStream, config: &UnixFdLatestConfig) {
    let _ = crate::send_unix_fd_frame(
        stream,
        &status_frame(ResponseStatus::Busy, 0),
        config.max_payload_bytes + RESPONSE_HEADER_BYTES,
        0,
    );
}

/// Resolves `true` once the peer has closed its end, `false` if request bytes are pending.
async fn peer_closed(stream: &UnixStream) -> io::Result<bool> {
    loop {
        stream.readable().await?;
        match stream.try_io(Interest::READABLE, || peek_one(stream.as_raw_fd())) {
            Ok(received) => return Ok(received == 0),
            Err(err) if err.kind() == io::ErrorKind::WouldBlock => continue,
            Err(err) => return Err(err),
        }
    }
}

fn peek_one(raw_fd: RawFd) -> io::Result<usize> {
    let mut byte = 0_u8;
    let received = unsafe {
        libc::recv(
            raw_fd,
            (&mut byte as *mut u8).cast(),
            1,
            libc::MSG_PEEK | libc::MSG_DONTWAIT,
        )
    };
    if received < 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(received as usize)
}

async fn remove_stale_socket(path: &Path) -> Result<(), IpcTransportError> {
    let metadata = match tokio::fs::symlink_metadata(path).await {
        Ok(metadata) => metadata,
        Err(err) if err.kind() == io::ErrorKind::NotFound => return Ok(()),
        Err(err) => return Err(IpcTransportError::BindFailed(err.to_string())),
    };
    if !metadata.file_type().is_socket() {
        return Err(IpcTransportError::BindFailed(format!(
            "{} exists and is not a unix socket",
            path.display()
        )));
    }
    if UnixStream::connect(path).await.is_ok() {
        return Err(IpcTransportError::BindFailed(format!(
            "{} is already served by a live listener",
            path.display()
        )));
    }
    match tokio::fs::remove_file(path).await {
        Ok(()) => Ok(()),
        Err(err) if err.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(err) => Err(IpcTransportError::BindFailed(err.to_string())),
    }
}

/// Removes the bound socket file on drop, unless it has since been replaced by another socket.
struct SocketFileGuard {
    path: PathBuf,
    identity: Option<(u64, u64)>,
}

impl SocketFileGuard {
    fn new(path: PathBuf) -> Self {
        let identity = std::fs::symlink_metadata(&path)
            .ok()
            .map(|metadata| (metadata.dev(), metadata.ino()));
        Self { path, identity }
    }
}

impl Drop for SocketFileGuard {
    fn drop(&mut self) {
        let current = std::fs::symlink_metadata(&self.path)
            .ok()
            .map(|metadata| (metadata.dev(), metadata.ino()));
        if current.is_some() && current == self.identity {
            let _ = std::fs::remove_file(&self.path);
        }
    }
}
