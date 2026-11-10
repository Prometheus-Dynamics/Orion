//! The network carrier: `orion+tcp`.
//!
//! This is how nodes reach each other and how people's tools reach a node. It carries the same
//! frames as every other carrier, with a plain length in front.
//!
//! **A signed session.** With security on, a connection starts with a handshake in which each
//! side proves it holds its identity key (the [handshake](self) is in `handshake.rs`), and
//! every frame after carries a MAC. The server knows the other end by its key: a pinned peer,
//! an operator, or a revoked key, which is turned away. A client that knows which key to
//! expect refuses any other, so a node cannot be impersonated to its peers.
//!
//! On `dev` every request was signed and every answer signed back. A session here carries
//! streams as well as requests, so the keys are proved once and each frame is covered by a
//! MAC instead: the same guarantee for what crosses the wire, at a fraction of the cost per
//! frame.
//!
//! **With security off** a plain session is accepted: no keys, no MAC, and the other end is
//! nobody in particular. A node with security on refuses a plain session and says why.
//!
//! **One connection each way.** A node dials each peer it is told about and keeps that
//! connection for its own requests; the peer dials back for its own. [`dial`] is that: it
//! connects, hands the connection over, and when it ends connects again, at a short fixed
//! interval for as long as the program runs. The network is a way in, and ways in are never
//! backed off.
//!
//! **Always push.** Nothing polls. A session stays open and the node sends what the session
//! asked to be sent, when it happens.

#![cfg(target_os = "linux")]

mod handshake;

use std::io;
use std::net::SocketAddr;
use std::os::fd::OwnedFd;
use std::time::Duration;

use orion_keys::SharedKeys;
use orion_server::{Carrier, Connection, FdSet, FrameRead, FrameWrite, Principal, Roles, Security};
use orion_supervise::{Path, State, Status, Supervisor};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;
use tokio::time::{Instant, sleep, timeout};

use handshake::{
    CLIENT_HELLO_LEN, Fresh, MODE_PLAIN, MODE_SIGNED, PREAMBLE_LEN, SERVER_HELLO_LEN,
    SIGNATURE_LEN, STATUS_NEEDS_SIGNED, STATUS_OK, STATUS_REFUSED, Seal, TAG_LEN, Transcript,
    hello, preamble,
};

/// The roles a connection over the network may ask for.
const NETWORK_ROLES: Roles = Roles::CLIENT.and(Roles::PEER);
/// Connections that have finished their handshake and wait to be accepted.
const READY: usize = 16;

/// What both ends of the carrier need to know.
#[derive(Clone, Debug)]
pub struct TcpOptions {
    /// Whether sessions are signed. A server with this on refuses plain sessions; a client
    /// with it on asks for a signed one.
    pub security: Security,
    /// The node's identity and whom it trusts.
    pub keys: SharedKeys,
    /// The largest frame, in bytes.
    pub max_frame: u32,
    /// How long a handshake may take.
    pub handshake: Duration,
}

fn refused(why: &str) -> io::Error {
    io::Error::new(io::ErrorKind::PermissionDenied, why.to_owned())
}

/// The reading half of a [`TcpConnection`].
#[derive(Debug)]
pub struct TcpRead {
    stream: OwnedReadHalf,
    seal: Option<Seal>,
}

/// The writing half of a [`TcpConnection`].
#[derive(Debug)]
pub struct TcpWrite {
    stream: OwnedWriteHalf,
    seal: Option<Seal>,
}

impl std::fmt::Debug for Seal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("Seal")
    }
}

impl FrameRead for TcpRead {
    async fn recv(&mut self, buf: &mut [u8], _fds: &mut FdSet) -> io::Result<Option<usize>> {
        let mut prefix = [0_u8; 4];
        match self.stream.read_exact(&mut prefix).await {
            Ok(_) => {}
            Err(error) if error.kind() == io::ErrorKind::UnexpectedEof => return Ok(None),
            Err(error) => return Err(error),
        }
        let len = u32::from_le_bytes(prefix) as usize;
        // Refused before a byte of it is read: the other end cannot make this one buffer more
        // than it agreed to.
        let Some(frame) = buf.get_mut(..len) else {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "frame too large",
            ));
        };
        self.stream.read_exact(frame).await?;
        if let Some(seal) = &mut self.seal {
            let mut tag = [0_u8; TAG_LEN];
            self.stream.read_exact(&mut tag).await?;
            if !seal.open(frame, &tag) {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "a frame failed its check",
                ));
            }
        }
        Ok(Some(len))
    }
}

impl FrameWrite for TcpWrite {
    async fn send(&mut self, frame: &[u8], fds: &[OwnedFd]) -> io::Result<()> {
        if !fds.is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "descriptors do not cross a network",
            ));
        }
        self.stream
            .write_all(&(frame.len() as u32).to_le_bytes())
            .await?;
        self.stream.write_all(frame).await?;
        if let Some(seal) = &mut self.seal {
            let tag = seal.seal(frame);
            self.stream.write_all(&tag).await?;
        }
        Ok(())
    }
}

/// One connection: the server's end of it, or a client's.
#[derive(Debug)]
pub struct TcpConnection {
    read: TcpRead,
    write: TcpWrite,
    principal: Principal,
    peer_key: Option<[u8; 32]>,
    peer_addr: SocketAddr,
}

impl TcpConnection {
    /// The identity key the other end proved it holds. `None` on a plain session.
    #[must_use]
    pub const fn peer_key(&self) -> Option<&[u8; 32]> {
        self.peer_key.as_ref()
    }

    /// The other end's address.
    #[must_use]
    pub const fn peer_addr(&self) -> SocketAddr {
        self.peer_addr
    }

    /// Sends one frame from this end.
    ///
    /// # Errors
    ///
    /// If the connection is broken.
    pub async fn send(&mut self, frame: &[u8]) -> io::Result<()> {
        self.write.send(frame, &[]).await
    }

    /// Receives one frame at this end. `None` when the other side has closed.
    ///
    /// # Errors
    ///
    /// If the connection is broken, the frame doesn't fit `buf`, or it fails its check.
    pub async fn recv(&mut self, buf: &mut [u8]) -> io::Result<Option<usize>> {
        self.read.recv(buf, &mut FdSet::default()).await
    }
}

impl Connection for TcpConnection {
    type Read = TcpRead;
    type Write = TcpWrite;

    fn principal(&self) -> Principal {
        self.principal.clone()
    }

    fn offers(&self) -> Roles {
        NETWORK_ROLES
    }

    fn passes_descriptors(&self) -> bool {
        false
    }

    fn split(self) -> (TcpRead, TcpWrite) {
        (self.read, self.write)
    }
}

fn connection(
    stream: TcpStream,
    seals: Option<(Seal, Seal)>,
    principal: Principal,
    peer_key: Option<[u8; 32]>,
) -> io::Result<TcpConnection> {
    // Requests and wake-ups are small and should not wait to be batched.
    stream.set_nodelay(true)?;
    let peer_addr = stream.peer_addr()?;
    let (read, write) = stream.into_split();
    let (reading, writing) = match seals {
        Some((reading, writing)) => (Some(reading), Some(writing)),
        None => (None, None),
    };
    Ok(TcpConnection {
        read: TcpRead {
            stream: read,
            seal: reading,
        },
        write: TcpWrite {
            stream: write,
            seal: writing,
        },
        principal,
        peer_key,
        peer_addr,
    })
}

/// The server's side of the handshake.
async fn welcome(mut stream: TcpStream, options: &TcpOptions) -> io::Result<TcpConnection> {
    let mut start = [0_u8; PREAMBLE_LEN];
    stream.read_exact(&mut start).await?;
    if start[..4] != handshake::MAGIC || start[5] != handshake::VERSION {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "not an orion+tcp connection",
        ));
    }
    match start[4] {
        MODE_PLAIN if options.security == Security::On => {
            // Told why, so a tool can say "this node needs a key" instead of "it hung up".
            stream.write_all(&[STATUS_NEEDS_SIGNED]).await?;
            Err(refused("a plain session was refused: security is on"))
        }
        MODE_PLAIN => {
            stream.write_all(&[STATUS_OK]).await?;
            connection(stream, None, Principal::Anonymous, None)
        }
        MODE_SIGNED => {
            stream.write_all(&[STATUS_OK]).await?;
            let mut client = [0_u8; CLIENT_HELLO_LEN];
            stream.read_exact(&mut client).await?;
            let fresh = Fresh::new();
            let (transcript, signature) = {
                let keys = options.keys.lock();
                let transcript = Transcript {
                    client,
                    server: hello(&fresh, &keys.identity().public()),
                };
                (transcript, transcript.sign_server(keys.identity()))
            };
            let mut answer = [0_u8; SERVER_HELLO_LEN];
            answer[..96].copy_from_slice(&transcript.server);
            answer[96..].copy_from_slice(&signature);
            stream.write_all(&answer).await?;

            let mut confirm = [0_u8; SIGNATURE_LEN];
            stream.read_exact(&mut confirm).await?;
            let key = transcript.client_key();
            let principal = options.keys.lock().principal(&key);
            let frame_keys = transcript.keys(&fresh, false);
            let genuine = transcript.client_signed(&confirm);
            let (true, Some((to_server, to_client)), false) =
                (genuine, frame_keys, principal == Principal::Anonymous)
            else {
                // A signature that does not verify, or a key that was revoked.
                stream.write_all(&[STATUS_REFUSED]).await?;
                return Err(refused("the other end's key was refused"));
            };
            stream.write_all(&[STATUS_OK]).await?;
            connection(
                stream,
                Some((Seal::new(to_server), Seal::new(to_client))),
                principal,
                Some(key),
            )
        }
        _ => Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "an unknown session mode",
        )),
    }
}

/// Connects to the node at `addr`. With security on the session is signed, and if `expect` is
/// a key, the node must prove it holds exactly that one.
///
/// # Errors
///
/// - `PermissionDenied`: the node refused this end's key, needs a signed session, or is not
///   the node that was expected.
/// - `TimedOut`: the handshake took too long.
/// - Anything the connection fails with.
pub async fn connect(
    addr: SocketAddr,
    options: &TcpOptions,
    expect: Option<[u8; 32]>,
) -> io::Result<TcpConnection> {
    let handshake = async {
        let mut stream = TcpStream::connect(addr).await?;
        let signed = options.security == Security::On;
        stream
            .write_all(&preamble(if signed { MODE_SIGNED } else { MODE_PLAIN }))
            .await?;
        let mut status = [0_u8; 1];
        stream.read_exact(&mut status).await?;
        if status[0] != STATUS_OK {
            return Err(refused("the node needs a signed session"));
        }
        if !signed {
            return connection(stream, None, Principal::Anonymous, None);
        }
        let fresh = Fresh::new();
        let client = hello(&fresh, &options.keys.lock().identity().public());
        stream.write_all(&client).await?;
        let mut answer = [0_u8; SERVER_HELLO_LEN];
        stream.read_exact(&mut answer).await?;
        let transcript = Transcript {
            client,
            server: answer[..96].try_into().expect("96 bytes"),
        };
        let node_key = transcript.server_key();
        if !transcript.server_signed(&answer[96..]) {
            return Err(refused("the node did not prove it holds its key"));
        }
        if expect.is_some_and(|expected| expected != node_key) {
            return Err(refused("the node is not the one that was expected"));
        }
        if options.keys.lock().is_revoked(&node_key) {
            return Err(refused("the node's key has been revoked"));
        }
        let Some((to_server, to_client)) = transcript.keys(&fresh, true) else {
            return Err(refused("the node sent a key exchange that gives no secret"));
        };
        let signature = transcript.sign_client(options.keys.lock().identity());
        stream.write_all(&signature).await?;
        stream.read_exact(&mut status).await?;
        if status[0] != STATUS_OK {
            return Err(refused("the node refused this end's key"));
        }
        let principal = options.keys.lock().principal(&node_key);
        connection(
            stream,
            Some((Seal::new(to_client), Seal::new(to_server))),
            principal,
            Some(node_key),
        )
    };
    timeout(options.handshake, handshake)
        .await
        .unwrap_or_else(|_| Err(io::ErrorKind::TimedOut.into()))
}

/// The carrier: a listening TCP socket.
#[derive(Debug)]
pub struct TcpCarrier {
    listener: TcpListener,
    options: TcpOptions,
    ready: mpsc::Receiver<TcpConnection>,
    done: mpsc::Sender<TcpConnection>,
}

impl TcpCarrier {
    /// Listens at `addr`. Must be called inside the runtime the carrier will be served on.
    ///
    /// # Errors
    ///
    /// Anything `bind` fails with.
    pub async fn bind(addr: SocketAddr, options: TcpOptions) -> io::Result<Self> {
        let (done, ready) = mpsc::channel(READY);
        Ok(Self {
            listener: TcpListener::bind(addr).await?,
            options,
            ready,
            done,
        })
    }

    /// The address it listens at, with the port it was given if it asked for any.
    ///
    /// # Errors
    ///
    /// If the socket has no address.
    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        self.listener.local_addr()
    }
}

impl Carrier for TcpCarrier {
    type Connection = TcpConnection;

    async fn accept(&mut self) -> io::Result<TcpConnection> {
        loop {
            tokio::select! {
                Some(connection) = self.ready.recv() => return Ok(connection),
                accepted = self.listener.accept() => {
                    let (stream, from) = accepted?;
                    // Each handshake is its own task with its own deadline, so a connection
                    // that says nothing holds up nobody else's.
                    let (options, done) = (self.options.clone(), self.done.clone());
                    tokio::task::spawn_local(async move {
                        match timeout(options.handshake, welcome(stream, &options)).await {
                            Ok(Ok(connection)) => {
                                let _ = done.send(connection).await;
                            }
                            Ok(Err(error)) => tracing::debug!(%from, %error, "a connection did not get through the handshake"),
                            Err(_) => tracing::debug!(%from, "a connection took too long over the handshake"),
                        }
                    });
                }
            }
        }
    }
}

/// Keeps a connection to `addr` for as long as the program runs: connects, gives the
/// connection to `serve`, and when `serve` returns (the connection ended) connects again.
///
/// The wait between tries is short and fixed. It never grows, however long the peer has been
/// gone: the network is a way in. `report` is told the state after every change, so it can be
/// shown.
pub async fn dial<Serve, Served>(
    addr: SocketAddr,
    options: TcpOptions,
    expect: Option<[u8; 32]>,
    mut report: impl FnMut(&Status),
    mut serve: Serve,
) where
    Serve: FnMut(TcpConnection) -> Served,
    Served: Future<Output = ()>,
{
    let started = Instant::now();
    let now_ms = || started.elapsed().as_millis() as u64;
    let mut supervisor = Supervisor::new(Path::Access);
    loop {
        match connect(addr, &options, expect).await {
            Ok(connection) => {
                supervisor.on_success();
                report(&supervisor.status());
                serve(connection).await;
            }
            Err(error) => {
                if let Some(repeats) = supervisor.log_failure(now_ms()) {
                    tracing::warn!(%addr, %error, repeats, "could not reach a peer");
                }
            }
        }
        let now = now_ms();
        let retry_at = match supervisor.on_failure(now) {
            orion_supervise::Decision::RetryAt(at) => at,
            // An access path cannot be switched off by policy; only by hand.
            orion_supervise::Decision::StayOff => now,
        };
        report(&supervisor.status());
        debug_assert_eq!(supervisor.status().state, State::Retrying);
        sleep(Duration::from_millis(retry_at.saturating_sub(now))).await;
    }
}
