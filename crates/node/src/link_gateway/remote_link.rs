//! `tcp:` links: link sessions of remote operators (docs/link-protocol.md, "Remote links").
//!
//! A connection opens with the link channel's handshake (`orion_auth::link_channel`): the node
//! signs with its identity key, the operator with its enrolled key. The node then accepts an
//! enrolled operator, or refuses with the reason and closes. After that the connection carries
//! the same COBS link frames as a Unix link, inside sealed records, and is served by the same
//! link task: a session per connection, client role only.

use crate::NodeApp;
use crate::app::LinkCaller;
use orion_auth::link_channel::{
    self, ChannelError, KIND_ACCEPT, KIND_DATA, KIND_REFUSE, KIND_ROTATE, MAX_PAYLOAD, Opener,
    ROTATE_AFTER_SECS, Sealer, ServerHandshake,
};
use std::io;
use std::net::SocketAddr;
use std::time::{Duration, Instant};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};

/// A handshake that has not finished in this long is closed.
pub(super) const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(5);

/// One direction of a connection's link bytes: the read side of [`super::unix_link`]'s
/// connection task. Reads must be cancel safe (the task selects on them).
pub(super) trait ConnRead: Send + 'static {
    fn read_bytes(&mut self, buf: &mut [u8]) -> impl Future<Output = io::Result<usize>> + Send;
}

/// The write side of a connection's link bytes.
pub(super) trait ConnWrite: Send + 'static {
    fn write_bytes(&mut self, bytes: &[u8]) -> impl Future<Output = io::Result<()>> + Send;
}

impl ConnRead for tokio::net::unix::OwnedReadHalf {
    async fn read_bytes(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        self.read(buf).await
    }
}

impl ConnWrite for tokio::net::unix::OwnedWriteHalf {
    async fn write_bytes(&mut self, bytes: &[u8]) -> io::Result<()> {
        self.write_all(bytes).await
    }
}

fn invalid(error: ChannelError) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, error.to_string())
}

/// Link bytes out of the peer's sealed records.
pub(super) struct SealedReader {
    inner: OwnedReadHalf,
    opener: Opener,
    /// Ciphertext read; `sealed[..skip]` is opened already (dropped at the next read).
    sealed: Vec<u8>,
    skip: usize,
    /// Opened bytes not handed out yet.
    open: Vec<u8>,
    at: usize,
}

impl ConnRead for SealedReader {
    async fn read_bytes(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        // Cancel safe: every await is the socket's read, and state changes only after it.
        let mut chunk = [0u8; 4096];
        loop {
            if self.at < self.open.len() {
                let n = buf.len().min(self.open.len() - self.at);
                buf[..n].copy_from_slice(&self.open[self.at..self.at + n]);
                self.at += n;
                if self.at == self.open.len() {
                    self.open.clear();
                    self.at = 0;
                }
                return Ok(n);
            }
            match self
                .opener
                .open_slice(&mut self.sealed[self.skip..], &mut self.open)
                .map_err(invalid)?
            {
                Some((KIND_DATA | KIND_ROTATE, used)) => {
                    self.skip += used;
                    continue;
                }
                Some(_) => return Err(invalid(ChannelError::Malformed("record kind"))),
                None => {}
            }
            let n = self.inner.read(&mut chunk).await?;
            if n == 0 {
                return Ok(0);
            }
            // One move per read, not per record.
            self.sealed.drain(..self.skip);
            self.skip = 0;
            self.sealed.extend_from_slice(&chunk[..n]);
        }
    }
}

/// Link bytes into sealed records; rotates its key every [`ROTATE_AFTER_SECS`].
pub(super) struct SealedWriter {
    inner: OwnedWriteHalf,
    sealer: Sealer,
    rotated: Instant,
    out: Vec<u8>,
}

impl ConnWrite for SealedWriter {
    async fn write_bytes(&mut self, bytes: &[u8]) -> io::Result<()> {
        self.out.clear();
        if self.rotated.elapsed() >= Duration::from_secs(ROTATE_AFTER_SECS) {
            self.sealer.rotate(&mut self.out);
            self.rotated = Instant::now();
        }
        self.sealer.seal_data(bytes, &mut self.out);
        self.inner.write_all(&self.out).await
    }
}

/// A connection whose handshake finished: its operator was accepted.
pub(super) struct Accepted {
    pub(super) reader: SealedReader,
    pub(super) writer: SealedWriter,
    pub(super) caller: LinkCaller,
}

/// Runs the node's side of the handshake on a new connection and accepts or refuses its
/// operator. `Err` closes the connection (a refusal was sent first when the handshake got that
/// far).
pub(super) async fn handshake(
    app: &NodeApp,
    stream: TcpStream,
    peer: SocketAddr,
) -> io::Result<Accepted> {
    let _ = stream.set_nodelay(true);
    let (mut inner_read, mut inner_write) = stream.into_split();
    let mut buf = Vec::new();
    let hello = read_message(&mut inner_read, &mut buf).await?;
    let (secret, random) = (random_bytes()?, random_bytes()?);
    let key = app.security.identity_key();
    let (server_hello, pending) =
        ServerHandshake::respond(&hello, secret, random, app.config.node_id.as_str(), &key)
            .map_err(invalid)?;
    inner_write
        .write_all(&link_channel::frame(&server_hello))
        .await?;
    let auth = read_message(&mut inner_read, &mut buf).await?;
    let done = pending.finish(&auth).map_err(invalid)?;
    let mut sealer = done.sealer;
    let mut out = Vec::new();
    let caller = match app
        .security
        .enrolled_operator(&done.operator, done.operator_key, true)
    {
        Ok(_) => {
            sealer.seal(KIND_ACCEPT, &[], &mut out);
            LinkCaller::Operator {
                principal: done.operator.clone(),
                key: done.operator_key,
            }
        }
        Err(error) => {
            tracing::info!(%peer, operator = %done.operator, %error, "tcp link: operator refused");
            let reason = error.to_string();
            let reason = reason.as_bytes();
            sealer.seal(
                KIND_REFUSE,
                reason
                    .get(..MAX_PAYLOAD.min(reason.len()))
                    .unwrap_or_default(),
                &mut out,
            );
            inner_write.write_all(&out).await?;
            let _ = inner_write.shutdown().await;
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                error.to_string(),
            ));
        }
    };
    inner_write.write_all(&out).await?;
    tracing::debug!(%peer, operator = %done.operator, "tcp link: operator accepted");
    Ok(Accepted {
        reader: SealedReader {
            inner: inner_read,
            opener: done.opener,
            // Records the client sent right after its auth are already here.
            sealed: buf,
            skip: 0,
            open: Vec::new(),
            at: 0,
        },
        writer: SealedWriter {
            inner: inner_write,
            sealer,
            rotated: Instant::now(),
            out: Vec::new(),
        },
        caller,
    })
}

async fn read_message(read: &mut OwnedReadHalf, buf: &mut Vec<u8>) -> io::Result<Vec<u8>> {
    let mut chunk = [0u8; 1024];
    loop {
        if let Some(message) = link_channel::take_message(buf).map_err(invalid)? {
            return Ok(message);
        }
        let n = read.read(&mut chunk).await?;
        if n == 0 {
            return Err(io::ErrorKind::UnexpectedEof.into());
        }
        buf.extend_from_slice(&chunk[..n]);
    }
}

fn random_bytes() -> io::Result<[u8; 32]> {
    let mut bytes = [0u8; 32];
    getrandom::fill(&mut bytes).map_err(|error| io::Error::other(error.to_string()))?;
    Ok(bytes)
}
