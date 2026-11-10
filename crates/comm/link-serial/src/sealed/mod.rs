//! A link that crosses a network: the same byte stream, inside a sealed channel.
//!
//! A device is not always plugged into the board. With this, a person's computer that has the
//! device on one of its ports carries the port's bytes to the board over TCP. The board accepts
//! one such connection at a time, checks who is bringing it, and from then on treats the bytes
//! exactly as a serial port's.
//!
//! Who is bringing it is an operator: the handshake is signed by the board's key and by the
//! operator's, and the operator must be enrolled and granted the action `link.attach`. Every
//! byte after the handshake is in a sealed record, so nobody between the two can read the
//! device's traffic or put a frame into it. [`channel`] has the messages.

pub mod channel;

use std::cell::RefCell;
use std::io;
use std::net::SocketAddr;
use std::rc::Rc;
use std::time::{Duration, Instant};

use orion_keys::{Identity, SharedKeys, random};
use orion_server::{Grants, What};
use orion_types::KeyId;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::task::JoinSet;
use tracing::{info, warn};

use crate::carrier::{ByteStream, Open};
use channel::{
    ChannelError, ClientHandshake, KIND_ACCEPT, KIND_DATA, KIND_REFUSE, MAX_HANDSHAKE, Opener,
    ROTATE_AFTER_SECS, Sealer, ServerHandshake,
};

/// The action an operator must be granted to bring a device to the board.
pub const LINK_ATTACH: &str = "link.attach";
/// How long a handshake may take.
const HANDSHAKE: Duration = Duration::from_secs(5);
/// How many handshakes may be under way at once.
const MAX_HANDSHAKES: usize = 8;
/// How many bytes are read from the socket at a time.
const READ: usize = 4096;

fn other(error: ChannelError) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, error.to_string())
}

async fn send_message(stream: &mut TcpStream, body: &[u8]) -> io::Result<()> {
    stream.write_all(&channel::frame(body)).await
}

async fn read_message(stream: &mut TcpStream) -> io::Result<Vec<u8>> {
    let len = usize::from(stream.read_u16_le().await?);
    if len > MAX_HANDSHAKE {
        return Err(other(ChannelError::Malformed("handshake message")));
    }
    let mut body = vec![0; len];
    stream.read_exact(&mut body).await?;
    Ok(body)
}

struct Sending {
    sealer: Sealer,
    rotated: Instant,
    out: Vec<u8>,
}

struct Receiving {
    opener: Opener,
    /// Sealed bytes not yet opened.
    sealed: Vec<u8>,
    /// Opened bytes not yet read, from `at`.
    plain: Vec<u8>,
    at: usize,
}

/// Link bytes over TCP, sealed.
pub struct SealedStream {
    stream: TcpStream,
    sending: RefCell<Sending>,
    receiving: RefCell<Receiving>,
}

impl std::fmt::Debug for SealedStream {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("SealedStream")
    }
}

impl SealedStream {
    fn new(stream: TcpStream, sealer: Sealer, opener: Opener, sealed: Vec<u8>) -> Self {
        Self {
            stream,
            sending: RefCell::new(Sending {
                sealer,
                rotated: Instant::now(),
                out: Vec::with_capacity(READ),
            }),
            receiving: RefCell::new(Receiving {
                opener,
                sealed,
                plain: Vec::with_capacity(READ),
                at: 0,
            }),
        }
    }

    /// The operator's side: connects to a board's sealed link at `addr`, as `name` with `key`.
    /// With `expect`, the board must hold that key.
    ///
    /// # Errors
    ///
    /// If the board can't be reached, is not the one expected, or refuses the operator; the
    /// refusal carries the board's reason.
    pub async fn connect(
        addr: SocketAddr,
        name: &str,
        key: &Identity,
        expect: Option<&[u8; 32]>,
    ) -> io::Result<Self> {
        let work = async {
            let mut stream = TcpStream::connect(addr).await?;
            stream.set_nodelay(true)?;
            let handshake = ClientHandshake::new(random(), random());
            send_message(&mut stream, handshake.hello()).await?;
            let hello = read_message(&mut stream).await?;
            let done = handshake.finish(&hello, expect, name, key).map_err(other)?;
            send_message(&mut stream, &done.auth).await?;
            let mut link = Self::new(stream, done.sealer, done.opener, Vec::new());
            // The board's first record says whether it takes this operator.
            let mut reason = Vec::new();
            match link.next_record(&mut reason).await? {
                KIND_ACCEPT => Ok(link),
                KIND_REFUSE => Err(io::Error::new(
                    io::ErrorKind::PermissionDenied,
                    String::from_utf8_lossy(&reason).into_owned(),
                )),
                _ => Err(other(ChannelError::Malformed("first record"))),
            }
        };
        tokio::time::timeout(HANDSHAKE, work)
            .await
            .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "the handshake took too long"))?
    }

    /// Reads until one whole record is open, and returns its kind with its payload in `payload`.
    async fn next_record(&mut self, payload: &mut Vec<u8>) -> io::Result<u8> {
        let receiving = self.receiving.get_mut();
        loop {
            if let Some(kind) = receiving
                .opener
                .open(&mut receiving.sealed, payload)
                .map_err(other)?
            {
                return Ok(kind);
            }
            if self.stream.read_buf(&mut receiving.sealed).await? == 0 {
                return Err(io::ErrorKind::UnexpectedEof.into());
            }
        }
    }
}

impl ByteStream for SealedStream {
    async fn read(&self, buf: &mut [u8]) -> io::Result<usize> {
        loop {
            {
                let mut receiving = self.receiving.borrow_mut();
                let receiving = &mut *receiving;
                if receiving.at == receiving.plain.len() {
                    receiving.plain.clear();
                    receiving.at = 0;
                    // Everything whole that has arrived is opened; a record cut short waits.
                    while let Some(kind) = receiving
                        .opener
                        .open(&mut receiving.sealed, &mut receiving.plain)
                        .map_err(other)?
                    {
                        if kind != KIND_DATA {
                            return Err(other(ChannelError::Malformed("record kind")));
                        }
                    }
                }
                if receiving.at < receiving.plain.len() {
                    let len = buf.len().min(receiving.plain.len() - receiving.at);
                    buf[..len].copy_from_slice(&receiving.plain[receiving.at..receiving.at + len]);
                    receiving.at += len;
                    return Ok(len);
                }
            }
            self.stream.readable().await?;
            let mut receiving = self.receiving.borrow_mut();
            let start = receiving.sealed.len();
            receiving.sealed.resize(start + READ, 0);
            let read = self.stream.try_read(&mut receiving.sealed[start..]);
            receiving
                .sealed
                .truncate(start + *read.as_ref().unwrap_or(&0));
            match read {
                Ok(0) => return Ok(0),
                Ok(_) => {}
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => {}
                Err(error) => return Err(error),
            }
        }
    }

    async fn write(&self, bytes: &[u8]) -> io::Result<usize> {
        // A record is sent whole or the channel is lost, so the sealed bytes are kept until
        // the socket has all of them.
        let out = {
            let mut sending = self.sending.borrow_mut();
            let sending = &mut *sending;
            let mut out = std::mem::take(&mut sending.out);
            out.clear();
            if sending.rotated.elapsed() >= Duration::from_secs(ROTATE_AFTER_SECS) {
                sending.sealer.rotate(&mut out);
                sending.rotated = Instant::now();
            }
            sending.sealer.seal_data(bytes, &mut out);
            out
        };
        let mut sent = 0;
        let result = loop {
            if sent == out.len() {
                break Ok(bytes.len());
            }
            if let Err(error) = self.stream.writable().await {
                break Err(error);
            }
            match self.stream.try_write(&out[sent..]) {
                Ok(0) => break Err(io::ErrorKind::WriteZero.into()),
                Ok(wrote) => sent += wrote,
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => {}
                Err(error) => break Err(error),
            }
        };
        // The buffer goes back for the next write, so steady traffic allocates nothing.
        self.sending.borrow_mut().out = out;
        result
    }
}

/// Where a board takes a device brought over the network.
pub struct SealedListener {
    listener: TcpListener,
    node: Rc<str>,
    keys: SharedKeys,
    /// Handshakes under way. They run side by side, so a caller that says nothing holds
    /// nobody else up.
    handshakes: JoinSet<Option<SealedStream>>,
}

impl std::fmt::Debug for SealedListener {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "SealedListener({:?})", self.listener.local_addr())
    }
}

impl SealedListener {
    /// Listens on `addr` as the node `node`, whose key and operators are in `keys`.
    ///
    /// # Errors
    ///
    /// If the address can't be bound.
    pub fn bind(addr: SocketAddr, node: &str, keys: SharedKeys) -> io::Result<Self> {
        let listener = std::net::TcpListener::bind(addr)?;
        listener.set_nonblocking(true)?;
        Ok(Self {
            listener: TcpListener::from_std(listener)?,
            node: node.into(),
            keys,
            handshakes: JoinSet::new(),
        })
    }

    /// The address it listens on.
    ///
    /// # Errors
    ///
    /// The socket's.
    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        self.listener.local_addr()
    }
}

/// The handshake with one caller. `Ok(None)` when the caller was refused and told so.
async fn admit(
    mut stream: TcpStream,
    node: &str,
    keys: &SharedKeys,
) -> io::Result<Option<SealedStream>> {
    stream.set_nodelay(true)?;
    let hello = read_message(&mut stream).await?;
    let (answer, handshake) = {
        let keys = keys.lock();
        ServerHandshake::respond(&hello, random(), random(), node, keys.identity())
            .map_err(other)?
    };
    send_message(&mut stream, &answer).await?;
    let auth = read_message(&mut stream).await?;
    let mut done = handshake.finish(&auth).map_err(other)?;
    let allowed = keys.operator_may(&KeyId(done.operator_key), What::Run(LINK_ATTACH));
    let mut first = Vec::new();
    if !allowed {
        warn!(operator = %done.operator, "an operator without the grant tried to attach a link");
        done.sealer.seal(
            KIND_REFUSE,
            b"this operator may not attach a link",
            &mut first,
        );
        stream.write_all(&first).await?;
        return Ok(None);
    }
    done.sealer.seal(KIND_ACCEPT, &[], &mut first);
    stream.write_all(&first).await?;
    info!(operator = %done.operator, "link attached over the network");
    Ok(Some(SealedStream::new(
        stream,
        done.sealer,
        done.opener,
        Vec::new(),
    )))
}

impl Open for SealedListener {
    type Stream = SealedStream;

    async fn open(&mut self) -> io::Result<SealedStream> {
        loop {
            tokio::select! {
                accepted = self.listener.accept() => {
                    let (stream, from) = accepted?;
                    if self.handshakes.len() >= MAX_HANDSHAKES {
                        // Dropped: the callers already here get their turn first.
                        continue;
                    }
                    let (node, keys) = (self.node.clone(), self.keys.clone());
                    self.handshakes.spawn_local(async move {
                        match tokio::time::timeout(HANDSHAKE, admit(stream, &node, &keys)).await {
                            Ok(Ok(link)) => link,
                            Ok(Err(error)) => {
                                warn!(%from, %error, "a link handshake failed");
                                None
                            }
                            Err(_) => {
                                warn!(%from, "a link handshake took too long");
                                None
                            }
                        }
                    });
                }
                Some(finished) = self.handshakes.join_next() => {
                    if let Ok(Some(link)) = finished {
                        return Ok(link);
                    }
                }
            }
        }
    }

    fn describe(&self) -> String {
        match self.listener.local_addr() {
            Ok(addr) => format!("sealed tcp {addr}"),
            Err(_) => "sealed tcp".into(),
        }
    }
}
