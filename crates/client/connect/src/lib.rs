//! The client's connection to a comm core.
//!
//! One [`Session`], with the same calls wherever the comm core is: on this board
//! ([`Address::Local`]), on the network ([`Address::Network`]), at the other end of a serial
//! port ([`Address::Serial`]), or at the other end of a link the program opened itself
//! ([`Session::over`]). Every other client crate is built on it.
//!
//! **On every system.** The network and a link the program opened work on Linux, Windows and
//! macOS. The board's own socket, descriptors with frames, and serial ports by path are
//! Linux's, as the board is.
//!
//! The session is blocking and owns no thread and no timer. [`Session::next`] waits for the
//! next frame for as long as it is told to, and while it waits it does the session's
//! housekeeping: it answers the comm core's pings, sends its own, and notices when the comm
//! core has gone quiet. A program that has nothing to wait for calls it with a zero wait from
//! its own loop. On Linux, `Session::as_fd` is the descriptor to give `poll` when the program
//! waits on other things too.
//!
//! **There is one way back.** When a call fails with an error that [`ClientError::is_lost`],
//! [`Session::reconnect`] connects again to the same address, as the same program. It waits a
//! short, fixed time between tries and never gives up: a client's connection to its comm core
//! is a way in, and those are not backed off.
//!
//! With feature `tokio`, on Linux, `Session::next_async` waits inside a runtime instead of
//! blocking a thread.

mod address;
mod alive;
mod client;
mod error;
mod fd;
mod hello;
mod level;
mod link_sign;
mod piped;
mod transport;
mod welcomed;

use std::time::{Duration, Instant};

use orion_msg_session::{Hello, Ping, Pong, Refusal};
use orion_supervise::{Decision, Path, Supervisor};
use orion_wire::{
    Body, Decode, FrameView, INTERFACE_LEVEL, INTERFACE_REVISION, encode_body, frame, kind,
};

pub use address::{Address, DEFAULT_PORT, DEFAULT_SOCKET};
pub use alive::Reply;
pub use client::{Client, Handler};
pub use error::{ClientError, SOCKET_PATH_MAX, socket_path};
pub use fd::{BorrowedFd, OwnedFd};
pub use hello::{FIRST_AGAIN, MOST_BETWEEN};
pub use level::Feature;
pub use orion_identity::Identity;
pub use orion_msg_session::Roles;
pub use orion_supervise::Status;
pub use piped::{LinkRead, LinkWrite};
pub use welcomed::Welcomed;

use fd::FdSet;
use transport::{Step, Transport};

/// How a session is made.
#[derive(Debug)]
pub struct Options {
    /// The roles to ask for.
    pub roles: Roles,
    /// This client's key. With one, a network session is signed: the node knows who is
    /// calling and nothing can be changed on the way. Without one it is plain, which a node
    /// with security on refuses.
    pub identity: Option<Identity>,
    /// The largest frame this client takes.
    pub max_frame: u32,
    /// How long connecting and saying hello may take, and, with [`Reply::Timeout`], each
    /// answer.
    pub timeout: Duration,
    /// How a wait for an answer is bounded. [`Reply::Timeout`] by default.
    pub reply: Reply,
}

impl Default for Options {
    fn default() -> Self {
        Self {
            roles: Roles::CLIENT,
            identity: None,
            max_frame: 64 * 1024,
            timeout: Duration::from_secs(5),
            reply: Reply::Timeout,
        }
    }
}

/// One frame from the comm core. Its descriptors, if any, are taken with
/// [`Session::take_fd`].
#[derive(Clone, Copy, Debug)]
pub struct Incoming<'a> {
    view: FrameView<'a>,
}

impl<'a> Incoming<'a> {
    /// The frame's kind.
    #[must_use]
    pub const fn kind(&self) -> u8 {
        self.view.kind()
    }

    /// The frame's sequence number.
    #[must_use]
    pub const fn seq(&self) -> u16 {
        self.view.seq()
    }

    /// The frame's payload.
    #[must_use]
    pub const fn payload(&self) -> &'a [u8] {
        self.view.payload()
    }

    /// The body, as a `B`.
    ///
    /// # Errors
    ///
    /// If the frame is another kind or does not decode.
    pub fn body<B: Body + Decode<'a>>(&self) -> Result<B, ClientError> {
        orion_wire::decode_body(&self.view)
            .map_err(|_| ClientError::Protocol("a body that does not decode"))
    }
}

/// What a handler may do with the session while it holds a frame: answer, and take the
/// frame's descriptors.
pub struct Cx<'a> {
    transport: &'a mut Transport,
    outbox: &'a mut Vec<u8>,
    seq: &'a mut u16,
    fds: &'a mut FdSet,
    limit: usize,
    level: u16,
}

impl std::fmt::Debug for Cx<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("Cx")
    }
}

impl Cx<'_> {
    /// The node's additive level, as it said at hello.
    #[must_use]
    pub const fn level(&self) -> u16 {
        self.level
    }

    /// Sends `body`. As [`Session::send`].
    ///
    /// # Errors
    ///
    /// As [`Session::send`].
    pub fn send<B: Body>(&mut self, body: &B) -> Result<u16, ClientError> {
        self.send_with_fds(body, &[])
    }

    /// Sends `body` with descriptors attached. As [`Session::send_with_fds`].
    ///
    /// # Errors
    ///
    /// As [`Session::send_with_fds`].
    pub fn send_with_fds<B: Body>(
        &mut self,
        body: &B,
        fds: &[BorrowedFd<'_>],
    ) -> Result<u16, ClientError> {
        *self.seq = self.seq.wrapping_add(1);
        let limit = self.limit.min(self.outbox.len());
        let len = encode_body(body, *self.seq, &mut self.outbox[..limit])
            .map_err(|_| ClientError::TooLarge)?;
        self.transport.send(&self.outbox[..len], fds)?;
        Ok(*self.seq)
    }

    /// Takes descriptor `index` of those that came with the frame.
    pub fn take_fd(&mut self, index: usize) -> Option<OwnedFd> {
        self.fds.take(index)
    }

    /// How many descriptors came with the frame.
    #[must_use]
    pub const fn fds(&self) -> usize {
        self.fds.len()
    }
}

/// A session with a comm core.
pub struct Session {
    transport: Transport,
    address: Option<Address>,
    /// The key the board at the other end of a link the program opened must prove.
    link_key: Option<[u8; 32]>,
    name: String,
    options: Options,
    welcomed: Welcomed,
    seq: u16,
    /// The last frame received.
    inbox: Vec<u8>,
    inbox_len: usize,
    fds: FdSet,
    /// Where frames are put together to send.
    outbox: Vec<u8>,
    next_ping: Instant,
    last_heard: Instant,
    started: Instant,
    reconnects: u64,
}

impl std::fmt::Debug for Session {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Session({} at {})", self.name, self.welcomed.node)
    }
}

impl Session {
    /// Connects to the comm core at `address` as the program `name`, and says hello.
    ///
    /// # Errors
    ///
    /// If nothing answers there, the comm core turns the session away, or, on the network,
    /// the node is not who was expected or refuses this client.
    pub fn connect(address: &Address, name: &str, options: Options) -> Result<Self, ClientError> {
        let transport = open(address, &options)?;
        Self::hello(transport, Some(address.clone()), name, options)
    }

    /// A session over a link someone else opened: a descriptor that carries bytes both ways,
    /// such as a serial port. There is no address to connect to again, so
    /// [`Session::reconnect`] fails.
    ///
    /// # Errors
    ///
    /// As [`Session::connect`].
    #[cfg(target_os = "linux")]
    pub fn over_link(link: OwnedFd, name: &str, options: Options) -> Result<Self, ClientError> {
        let transport = signed(
            Transport::link(transport::nonblocking(link)?),
            None,
            &options,
        )?;
        Self::hello(transport, None, name, options)
    }

    /// A session over a link the program opened itself, as two halves: a USB interface it
    /// claimed, say. The same frames go over it as over a serial port, and with a key in
    /// `options` the session is signed; `expect` is then the key the board must prove.
    ///
    /// `read` goes to a thread of its own; the session writes with `write` on the caller's
    /// thread. The session never waits for that thread: dropping the session drops `write`,
    /// and the program then makes `read` return an error, which ends the thread. See
    /// [`LinkRead`].
    ///
    /// # Errors
    ///
    /// As [`Session::connect`]; [`ClientError::Denied`] for an `expect` without a key.
    pub fn over(
        read: impl LinkRead,
        write: impl LinkWrite,
        expect: Option<[u8; 32]>,
        name: &str,
        options: Options,
    ) -> Result<Self, ClientError> {
        let transport = signed(Transport::over(read, write)?, expect.as_ref(), &options)?;
        let mut session = Self::hello(transport, None, name, options)?;
        session.link_key = expect;
        Ok(session)
    }

    /// Says hello again, as the same program, over new halves of a link the program opened:
    /// the cable was plugged in again, say. The old halves are let go first. A link that is not
    /// signed loses frames without a word, so the hello is said again, on a capped backoff
    /// ([`FIRST_AGAIN`] doubling to [`MOST_BETWEEN`]), until the welcome comes or the session's
    /// timeout passes. To keep trying past that, with new halves each time, use
    /// [`Session::reconnect_over_with`].
    ///
    /// # Errors
    ///
    /// As [`Session::over`].
    pub fn reconnect_over(
        &mut self,
        read: impl LinkRead,
        write: impl LinkWrite,
    ) -> Result<(), ClientError> {
        let options = self.same_options();
        self.transport.let_go();
        let transport = signed(
            Transport::over(read, write)?,
            self.link_key.as_ref(),
            &options,
        )?;
        let fresh = Self::hello(transport, None, &self.name, options)?;
        let (started, reconnects, link_key) = (self.started, self.reconnects, self.link_key);
        *self = Self {
            started,
            reconnects: reconnects + 1,
            link_key,
            ..fresh
        };
        Ok(())
    }

    /// Says hello again, as the same program, over halves `open` gives, for as long as
    /// `keep_trying` says: the way back for a program whose link comes and goes. Each try
    /// opens new halves and waits for the welcome, saying hello again while there is none
    /// (see [`Session::reconnect_over`]); a try that finds nobody is followed by the next at
    /// the same pace as [`Session::reconnect`]. `keep_trying` is asked before each wait and
    /// told the state: return `false` to stop.
    ///
    /// # Errors
    ///
    /// The last failure, if `keep_trying` said to stop or `open` failed; or if the comm core now
    /// says no: trying again does not change an answer.
    pub fn reconnect_over_with<R: LinkRead, W: LinkWrite>(
        &mut self,
        mut open: impl FnMut() -> std::io::Result<(R, W)>,
        mut keep_trying: impl FnMut(&Status) -> bool,
    ) -> Result<(), ClientError> {
        let mut supervisor = Supervisor::new(Path::Access);
        loop {
            let (read, write) = open().map_err(ClientError::Io)?;
            let error = match self.reconnect_over(read, write) {
                Ok(()) => return Ok(()),
                Err(error) if error.is_lost() => error,
                Err(error) => return Err(error),
            };
            let now_ms = self.started.elapsed().as_millis() as u64;
            let wait_ms = match supervisor.on_failure(now_ms) {
                Decision::RetryAt(at_ms) => at_ms.saturating_sub(now_ms),
                Decision::StayOff => u64::from(orion_supervise::ACCESS_RETRY_MS),
            };
            if !keep_trying(&supervisor.status()) {
                return Err(error);
            }
            std::thread::sleep(Duration::from_millis(wait_ms));
        }
    }

    /// The options this session was made with, again.
    fn same_options(&self) -> Options {
        Options {
            roles: self.options.roles,
            identity: self
                .options
                .identity
                .as_ref()
                .map(|identity| Identity::from_secret(&identity.secret())),
            max_frame: self.options.max_frame,
            timeout: self.options.timeout,
            reply: self.options.reply,
        }
    }

    fn hello(
        mut transport: Transport,
        address: Option<Address>,
        name: &str,
        options: Options,
    ) -> Result<Self, ClientError> {
        let max_frame = (options.max_frame as usize).min(transport.frame_limit());
        let mut inbox = vec![0_u8; max_frame];
        let mut outbox = vec![0_u8; max_frame];
        let hello = Hello {
            name,
            roles: options.roles,
            max_frame: max_frame as u32,
            revision: INTERFACE_REVISION,
            level: INTERFACE_LEVEL,
        };
        let len = encode_body(&hello, 1, &mut outbox).map_err(|_| ClientError::TooLarge)?;
        let mut fds = FdSet::default();
        let welcomed = hello::welcomed(
            &mut transport,
            &outbox[..len],
            &mut inbox,
            &mut fds,
            options.timeout,
        )?;
        let now = Instant::now();
        Ok(Self {
            transport,
            address,
            link_key: None,
            name: name.to_owned(),
            options,
            next_ping: now + welcomed.heartbeat / 2,
            last_heard: now,
            welcomed,
            seq: 1,
            inbox,
            inbox_len: 0,
            fds,
            outbox,
            started: now,
            reconnects: 0,
        })
    }

    /// What the comm core said at hello.
    #[must_use]
    pub const fn welcomed(&self) -> &Welcomed {
        &self.welcomed
    }

    /// Whether descriptors travel on this session: only on the board's own socket.
    #[must_use]
    pub const fn passes_descriptors(&self) -> bool {
        self.transport.passes_descriptors()
    }

    /// How many times the session has been made again.
    #[must_use]
    pub const fn reconnects(&self) -> u64 {
        self.reconnects
    }

    /// Sends `body` and returns the sequence number it went with, which is what an answer
    /// names.
    ///
    /// # Errors
    ///
    /// If the connection is broken, or the body does not fit a frame.
    pub fn send<B: Body>(&mut self, body: &B) -> Result<u16, ClientError> {
        self.send_with_fds(body, &[])
    }

    /// Sends `body` with descriptors attached.
    ///
    /// # Errors
    ///
    /// As [`Session::send`], and [`ClientError::NoDescriptors`] anywhere but on the board's
    /// own socket.
    pub fn send_with_fds<B: Body>(
        &mut self,
        body: &B,
        fds: &[BorrowedFd<'_>],
    ) -> Result<u16, ClientError> {
        self.seq = self.seq.wrapping_add(1);
        let limit = (self.welcomed.max_frame as usize).min(self.outbox.len());
        let len = encode_body(body, self.seq, &mut self.outbox[..limit])
            .map_err(|_| ClientError::TooLarge)?;
        self.transport.send(&self.outbox[..len], fds)?;
        Ok(self.seq)
    }

    /// Sends a frame of `kind` whose payload is already encoded, with descriptors attached.
    ///
    /// # Errors
    ///
    /// As [`Session::send_with_fds`].
    pub fn send_frame(
        &mut self,
        kind: u8,
        payload: &[u8],
        fds: &[BorrowedFd<'_>],
    ) -> Result<u16, ClientError> {
        self.seq = self.seq.wrapping_add(1);
        let limit = (self.welcomed.max_frame as usize).min(self.outbox.len());
        let header = frame::FrameHeader::new(kind, self.seq);
        let len = frame::encode(header, payload, &mut self.outbox[..limit])
            .map_err(|_| ClientError::TooLarge)?;
        self.transport.send(&self.outbox[..len], fds)?;
        Ok(self.seq)
    }

    /// Waits up to `wait` for the next frame that is the program's to look at. `None` when
    /// the time passed without one. Pings are answered and sent here and never returned.
    ///
    /// # Errors
    ///
    /// [`ClientError::Lost`] if the comm core closed the session or said nothing for three
    /// heartbeats, and whatever the connection fails with. A refusal is not an error here: it
    /// is a frame, of kind `REFUSAL`, for whoever sent the request.
    pub fn next(&mut self, wait: Duration) -> Result<Option<Incoming<'_>>, ClientError> {
        let deadline = Instant::now() + wait;
        loop {
            if self.take()? {
                return Ok(Some(self.incoming()));
            }
            let now = Instant::now();
            self.keep_alive(now)?;
            let until = self.wake_at(deadline);
            if now >= deadline {
                return Ok(None);
            }
            self.transport.wait(until.saturating_duration_since(now))?;
        }
    }

    /// As [`Session::next`], with the means to answer the frame while it is held.
    ///
    /// # Errors
    ///
    /// As [`Session::next`].
    pub fn next_with(
        &mut self,
        wait: Duration,
    ) -> Result<Option<(Incoming<'_>, Cx<'_>)>, ClientError> {
        if self.next(wait)?.is_none() {
            return Ok(None);
        }
        let Self {
            transport,
            welcomed,
            seq,
            inbox,
            inbox_len,
            fds,
            outbox,
            ..
        } = self;
        let view = frame::decode(&inbox[..*inbox_len])
            .map_err(|_| ClientError::Protocol("a damaged frame"))?;
        Ok(Some((
            Incoming { view },
            Cx {
                transport,
                outbox,
                seq,
                fds,
                limit: welcomed.max_frame as usize,
                level: welcomed.level,
            },
        )))
    }

    /// How long connecting may take for its hello, and, with [`Reply::Timeout`], a request
    /// for its answer.
    #[must_use]
    pub const fn timeout(&self) -> Duration {
        self.options.timeout
    }

    /// Takes descriptor `index` of those that came with the last frame.
    pub fn take_fd(&mut self, index: usize) -> Option<OwnedFd> {
        self.fds.take(index)
    }

    /// Waits for the answer to the request sent as `seq`: the next frame of kind `want`.
    /// Other frames that arrive first are dropped, so this is for a session that is doing one
    /// thing at a time.
    ///
    /// # Errors
    ///
    /// [`ClientError::Refused`] if the comm core refused the request, as
    /// [`Session::reply_left`] when the wait is over, and as [`Session::next`].
    pub fn answer(&mut self, seq: u16, want: u8) -> Result<Incoming<'_>, ClientError> {
        let since = Instant::now();
        loop {
            let left = self.reply_left(since)?;
            let Some(incoming) = self.next(left)? else {
                continue;
            };
            if incoming.kind() == kind::REFUSAL {
                let refusal: Refusal<'_> = incoming.body()?;
                if refusal.request_seq == seq {
                    return Err(ClientError::refused(
                        refusal.error().unwrap_or(orion_types::Error::Internal),
                        refusal.detail,
                    ));
                }
            } else if incoming.kind() == want {
                break;
            }
        }
        Ok(self.incoming())
    }

    /// The descriptor to wait on, with `poll` or its like, when the program waits on other
    /// things too. When it is readable, call [`Session::next`].
    #[cfg(target_os = "linux")]
    #[must_use]
    pub fn as_fd(&self) -> BorrowedFd<'_> {
        self.transport.fd()
    }

    /// Connects again to the same address as the same program, for as long as it takes. The
    /// wait between tries is short and fixed. `keep_trying` is asked before each wait and
    /// told the state: return `false` to stop.
    ///
    /// # Errors
    ///
    /// The last failure, if `keep_trying` said to stop, or if the comm core now says no:
    /// trying again does not change an answer. A session made with [`Session::over_link`] has
    /// no address and fails at once.
    pub fn reconnect(
        &mut self,
        mut keep_trying: impl FnMut(&Status) -> bool,
    ) -> Result<(), ClientError> {
        let address = self.address.clone().ok_or(ClientError::Denied(
            "this session has no address to connect to again",
        ))?;
        let mut supervisor = Supervisor::new(Path::Access);
        loop {
            let options = self.same_options();
            let error = match open(&address, &options)
                .and_then(|transport| Self::hello(transport, None, &self.name, options))
            {
                Ok(fresh) => {
                    let (started, reconnects) = (self.started, self.reconnects);
                    *self = Self {
                        address: Some(address),
                        started,
                        reconnects: reconnects + 1,
                        ..fresh
                    };
                    return Ok(());
                }
                Err(error) if error.is_lost() => error,
                Err(error) => return Err(error),
            };
            let now_ms = self.started.elapsed().as_millis() as u64;
            let wait_ms = match supervisor.on_failure(now_ms) {
                Decision::RetryAt(at_ms) => at_ms.saturating_sub(now_ms),
                Decision::StayOff => u64::from(orion_supervise::ACCESS_RETRY_MS),
            };
            if !keep_trying(&supervisor.status()) {
                return Err(error);
            }
            std::thread::sleep(Duration::from_millis(wait_ms));
        }
    }

    fn incoming(&self) -> Incoming<'_> {
        // Checked when it was taken.
        Incoming {
            view: frame::decode(&self.inbox[..self.inbox_len])
                .unwrap_or_else(|_| unreachable!("checked on arrival")),
        }
    }

    /// Takes frames until one is the program's. `false` if nothing whole is there.
    fn take(&mut self) -> Result<bool, ClientError> {
        loop {
            let len = match self.transport.take(&mut self.inbox, &mut self.fds)? {
                Step::Frame(len) => len,
                Step::Nothing => return Ok(false),
                Step::Closed => return Err(ClientError::Lost),
            };
            let view = frame::decode(&self.inbox[..len])
                .map_err(|_| ClientError::Protocol("a damaged frame"))?;
            self.last_heard = Instant::now();
            match view.kind() {
                kind::PING => {
                    let ping =
                        Ping::decode(view.payload()).map_err(|_| ClientError::Protocol("ping"))?;
                    self.send(&Pong {
                        now_ms: ping.now_ms,
                    })?;
                }
                kind::PONG => {}
                // A welcome to a hello said again over a link: the session the comm core has
                // for this one now. (The one before ended when that hello came.)
                kind::WELCOME => self.welcomed = hello::welcome_of(&view, &self.transport)?,
                // The comm core has no session for this one: it ended there (a link let go,
                // the comm core restarted) while this side went on. Lost: say hello again.
                kind::REJECT => return Err(ClientError::Lost),
                _ => {
                    self.inbox_len = len;
                    return Ok(true);
                }
            }
        }
    }
}

#[cfg(target_os = "linux")]
impl std::os::fd::AsFd for Session {
    fn as_fd(&self) -> BorrowedFd<'_> {
        self.transport.fd()
    }
}

fn open(address: &Address, options: &Options) -> Result<Transport, ClientError> {
    match address {
        #[cfg(target_os = "linux")]
        Address::Local(path) => Transport::local(path),
        Address::Network { host, key } => Transport::network(
            host,
            key.as_ref(),
            options.identity.as_ref(),
            options.timeout,
        ),
        // Said before the port is opened: nothing on the line hears a session that cannot be.
        Address::Serial { key: Some(_), .. } if options.identity.is_none() => Err(
            ClientError::Denied("a board's key can only be checked on a signed session"),
        ),
        #[cfg(target_os = "linux")]
        Address::Serial { path, baud, key } => {
            signed(Transport::serial(path, *baud)?, key.as_ref(), options)
        }
        #[cfg(not(target_os = "linux"))]
        Address::Local(_) | Address::Serial { .. } => Err(ClientError::Io(std::io::Error::new(
            std::io::ErrorKind::Unsupported,
            "a board's own socket and a serial port by path are Linux's; use the network, or \
             a link the program opened (Session::over)",
        ))),
    }
}

/// A link's session, signed if this client has a key: the board then knows the client by it,
/// not as a device on the link, and must prove it holds `expect` if that is given.
fn signed(
    mut transport: Transport,
    expect: Option<&[u8; 32]>,
    options: &Options,
) -> Result<Transport, ClientError> {
    match &options.identity {
        Some(identity) => {
            transport.sign_link(identity, expect, options.timeout)?;
            Ok(transport)
        }
        None if expect.is_some() => Err(ClientError::Denied(
            "a board's key can only be checked on a signed session",
        )),
        None => Ok(transport),
    }
}

#[cfg(all(feature = "tokio", target_os = "linux"))]
mod wait;
