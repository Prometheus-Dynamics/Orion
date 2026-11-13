//! The SSH way in.
//!
//! The comm core answers SSH itself, so a board needs no other SSH server to be reached,
//! repaired or looked at. A person's ordinary `ssh` connects to it.
//!
//! - **Logins go through the one check.** A client proves it holds a key; the trust store
//!   says whose key that is; the check decides whether that operator may log in, under the
//!   same policy and the same switch as every other request. An operator trusted with
//!   everything may. With security off the check admits anyone, with a key or without, as
//!   it does everywhere else. There are no passwords and no `authorized_keys`: one trust
//!   store says who may do what on a board.
//! - **A shell as the user who logged in.** The name a client gives is an account of the
//!   board's. Each session runs in a helper process that becomes that user, with all its
//!   groups, checks that it has, and then becomes the user's shell ([`helper`]). A comm
//!   core that runs as root can let in any account; one that does not can only let in the
//!   account it runs as, and says so when asked for another.
//! - **Small before the login.** Before a client is let in it reaches the key exchange and
//!   the proof of its key and nothing else: one family of key exchange, two ciphers, public
//!   keys only. Connections that have not logged in are few and do not have long.
//! - **Always a key.** The host key is made on the first start. One that is broken is moved
//!   aside and replaced, one that cannot be kept is used for the run: nothing about the disk
//!   closes the way in ([`hostkey`]).

#![cfg(target_os = "linux")]

mod door;
pub mod helper;
pub mod hostkey;
mod keeper;
mod session;
pub mod users;

use std::borrow::Cow;
use std::collections::HashMap;
use std::ffi::OsString;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::Duration;

use orion_check::{Admits, Policy, Request, Security, What};
use orion_config::{ConfigError, Source};
use orion_keys::SharedKeys;
use russh::keys::{PrivateKey, PublicKey};
use russh::server::{Auth, ChannelOpenHandle, Handler, Msg, Session};
use russh::{Channel, ChannelId, MethodKind, MethodSet, Preferred};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::TcpListener;
use tokio::task::JoinHandle;
use tracing::debug;

pub use door::{Door, Local};
pub use keeper::{Admitted, Io, Keeper, Started, Terminal, Wanted};
use session::{Input, Running};

/// Who may log in: an operator, and only one that has been granted it.
pub const LOGIN: Policy = Policy::new(Admits::OPERATOR);

/// A request to log in.
#[derive(Clone, Copy, Debug)]
pub struct Login;

impl Request for Login {
    fn what(&self) -> What<'_> {
        What::Login
    }
}

/// The program that sessions run in, and the arguments that go before the helper's own.
/// The comm core's binary names itself; a test names `orion-ssh-helper`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Helper {
    /// The program.
    pub program: PathBuf,
    /// Arguments before [`helper::MODE`].
    pub args: Vec<OsString>,
}

/// A subsystem: a program a client asks for by name, run as the user in place of a shell.
/// `sftp` is one.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Subsystem {
    /// The name a client asks for.
    pub name: String,
    /// The program and its arguments.
    pub program: Vec<OsString>,
}

/// What the SSH server is told.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SshConfig {
    /// `ssh.listen`: where SSH clients connect.
    pub listen: SocketAddr,
    /// `ssh.interface`: the one network interface SSH is open on, whatever its address
    /// (`SO_BINDTODEVICE`). One that is not there yet is waited for.
    pub interface: Option<String>,
    /// `ssh.host_key`: the host key, in OpenSSH's format. Made if it is not there.
    pub host_key: PathBuf,
    /// The program sessions run in.
    pub helper: Helper,
    /// The subsystems there are.
    pub subsystems: Vec<Subsystem>,
    /// `ssh.forward`: whether someone who is logged in may ask for connections from the
    /// board (`ssh -L`, `ssh -W`). Only in a build with feature `forward`.
    pub forward: bool,
    /// Where the board's `passwd` and `group` are.
    pub etc: PathBuf,
    /// For the recovery way in, where the board itself may be broken: if there is no
    /// account file, or none with `root` in it, root is still an account when the comm
    /// core is root; and an account whose shell is not there gets `/bin/sh`.
    pub recovery: bool,
    /// `ssh.max_connections`: the most connections at once.
    pub max_connections: usize,
    /// `ssh.max_unauthenticated`: the most connections at once that have not logged in.
    pub max_unauthenticated: usize,
    /// `ssh.login_ms`: how long a connection has to log in.
    pub login: Duration,
}

impl SshConfig {
    /// The server's settings, or `None` if neither `ssh.listen` nor `ssh.interface` is set:
    /// no SSH.
    ///
    /// # Errors
    ///
    /// A setting that does not parse.
    pub fn from_source(
        source: &mut Source,
        state_dir: &std::path::Path,
        helper: Helper,
    ) -> Result<Option<Self>, ConfigError> {
        let listen: Option<SocketAddr> = source.optional("ssh.listen")?;
        let interface: Option<String> = source.optional("ssh.interface")?;
        let on = listen.is_some() || interface.is_some();
        let config = Self {
            listen: listen.unwrap_or_else(|| SocketAddr::from(([0, 0, 0, 0], 22))),
            interface,
            host_key: source
                .optional("ssh.host_key")?
                .unwrap_or_else(|| state_dir.join("ssh_host_ed25519_key")),
            helper,
            subsystems: Vec::new(),
            forward: source.get("ssh.forward", true)?,
            etc: "/etc".into(),
            recovery: false,
            max_connections: source.get("ssh.max_connections", 64_usize)?.max(1),
            max_unauthenticated: source.get("ssh.max_unauthenticated", 10_usize)?.max(1),
            login: Duration::from_millis(source.get("ssh.login_ms", 60_000_u64)?.max(1000)),
        };
        Ok(on.then_some(config))
    }
}

/// Listens on `listen`, and only on the network interface named `interface` if there is one.
///
/// # Errors
///
/// If it cannot be listened on; [`absent`] says whether that is for want of the interface.
pub fn listen(listen: SocketAddr, interface: Option<&str>) -> std::io::Result<TcpListener> {
    let socket = if listen.is_ipv4() {
        tokio::net::TcpSocket::new_v4()?
    } else {
        tokio::net::TcpSocket::new_v6()?
    };
    socket.set_reuseaddr(true)?;
    if let Some(name) = interface {
        socket.bind_device(Some(name.as_bytes()))?;
    }
    socket.bind(listen)?;
    socket.listen(1024)
}

/// Whether listening failed because the interface it was to be on is not there (yet).
#[must_use]
pub fn absent(error: &std::io::Error) -> bool {
    error.raw_os_error() == Some(rustix::io::Errno::NODEV.raw_os_error())
}

/// Listens as [`listen`] does, waiting for an interface that is not there yet: a USB
/// gadget's network comes up after the comm core does. Tries every second, and says so
/// once.
pub async fn listen_when_there(at: SocketAddr, interface: &str) -> TcpListener {
    let mut said = false;
    loop {
        match listen(at, Some(interface)) {
            Ok(listener) => {
                tracing::info!("ssh: {interface} is there; open on it at {at}");
                return listener;
            }
            Err(error) => {
                if !said {
                    tracing::info!("ssh: waiting for {interface} to listen on: {error}");
                    said = true;
                }
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
        }
    }
}

/// The places there are for connections: so many in all, so many that have not logged in.
#[derive(Debug)]
pub struct Places {
    connections: AtomicUsize,
    unauthenticated: AtomicUsize,
    most: usize,
    most_unauthenticated: usize,
}

/// One place, held until dropped.
#[derive(Debug)]
pub struct Place(Arc<Places>, bool);

impl Drop for Place {
    fn drop(&mut self) {
        let count = if self.1 {
            &self.0.unauthenticated
        } else {
            &self.0.connections
        };
        count.fetch_sub(1, Ordering::AcqRel);
    }
}

impl Places {
    /// The places `config` allows.
    #[must_use]
    pub fn new(config: &SshConfig) -> Arc<Self> {
        Arc::new(Self {
            connections: AtomicUsize::new(0),
            unauthenticated: AtomicUsize::new(0),
            most: config.max_connections,
            most_unauthenticated: config.max_unauthenticated,
        })
    }

    fn one(count: &AtomicUsize, most: usize) -> bool {
        let mut now = count.load(Ordering::Acquire);
        loop {
            if now >= most {
                return false;
            }
            match count.compare_exchange_weak(now, now + 1, Ordering::AcqRel, Ordering::Acquire) {
                Ok(_) => return true,
                Err(actual) => now = actual,
            }
        }
    }

    /// A place for a new connection, and one among those that have not logged in: the
    /// second is given up when it logs in. `None` if there is no room.
    #[must_use]
    pub fn take(self: &Arc<Self>) -> Option<(Place, Place)> {
        if !Self::one(&self.connections, self.most) {
            return None;
        }
        let connection = Place(self.clone(), false);
        Self::one(&self.unauthenticated, self.most_unauthenticated)
            .then(|| (connection, Place(self.clone(), true)))
    }

    /// How many connections there are now.
    #[must_use]
    pub fn connections(&self) -> usize {
        self.connections.load(Ordering::Acquire)
    }
}

/// One channel of a connection.
#[derive(Default)]
struct Chan {
    terminal: Option<Terminal>,
    running: Option<Running>,
}

/// One client's connection: the side that reads what the client sends.
struct Client<D: Door> {
    door: D,
    channels: HashMap<ChannelId, Chan>,
    /// Set when it has logged in.
    logged_in: Arc<AtomicBool>,
}

/// The bytes of `key`, if it is of the kind the trust store holds.
fn ed25519(key: &PublicKey) -> Option<[u8; 32]> {
    key.key_data().ed25519().map(|key| key.0)
}

impl<D: Door> Client<D> {
    async fn login(&mut self, name: &str, key: Option<[u8; 32]>) -> Auth {
        if self.door.login(name, key).await {
            self.logged_in.store(true, Ordering::Release);
            Auth::Accept
        } else {
            Auth::reject()
        }
    }

    /// Starts what the client wanted on `channel`.
    async fn start(
        &mut self,
        channel: ChannelId,
        wanted: Wanted,
        client: russh::server::Handle,
    ) -> bool {
        if !self.logged_in.load(Ordering::Acquire) {
            return false;
        }
        let chan = self.channels.entry(channel).or_default();
        if chan.running.is_some() {
            return false;
        }
        let carried = match self.door.start(wanted, chan.terminal.clone()).await {
            Ok(started) => session::carry(started, client, channel),
            Err(error) => Err(error),
        };
        match carried {
            Ok(running) => {
                chan.running = Some(running);
                true
            }
            Err(error) => {
                debug!("ssh: a session could not start: {error}");
                false
            }
        }
    }
}

impl<D: Door> Handler for Client<D> {
    type Error = russh::Error;

    async fn auth_none(&mut self, user: &str) -> Result<Auth, Self::Error> {
        // Nobody known. Let in only where the check lets anyone in: security off.
        Ok(self.login(user, None).await)
    }

    async fn auth_publickey_offered(
        &mut self,
        _user: &str,
        offer: russh::server::PublicKeyOffer<'_>,
    ) -> Result<Auth, Self::Error> {
        // A key that would not be let in is not asked to prove itself.
        Ok(if self.door.offered(ed25519(offer.public_key)).await {
            Auth::Accept
        } else {
            Auth::reject()
        })
    }

    async fn auth_publickey(
        &mut self,
        user: &str,
        key: russh::server::VerifiedPublicKey<'_>,
    ) -> Result<Auth, Self::Error> {
        Ok(self.login(user, ed25519(key.public_key)).await)
    }

    async fn channel_open_session(
        &mut self,
        channel: Channel<Msg>,
        reply: ChannelOpenHandle,
        _session: &mut Session,
    ) -> Result<(), Self::Error> {
        self.channels.insert(channel.id(), Chan::default());
        reply.accept().await;
        Ok(())
    }

    #[cfg(feature = "forward")]
    async fn channel_open_direct_tcpip(
        &mut self,
        channel: Channel<Msg>,
        host_to_connect: &str,
        port_to_connect: u32,
        _originator_address: &str,
        _originator_port: u32,
        reply: ChannelOpenHandle,
        _session: &mut Session,
    ) -> Result<(), Self::Error> {
        if !self.logged_in.load(Ordering::Acquire) {
            reply
                .reject(russh::ChannelOpenFailure::AdministrativelyProhibited)
                .await;
            return Ok(());
        }
        let (door, host) = (self.door.clone(), host_to_connect.to_owned());
        // Connecting can take a while: the rest of this client's connection goes on.
        tokio::spawn(async move {
            let stream = door
                .connect(&host, port_to_connect)
                .await
                .and_then(|stream| {
                    stream
                        .set_nonblocking(true)
                        .map_err(|error| error.to_string())?;
                    tokio::net::TcpStream::from_std(stream).map_err(|error| error.to_string())
                });
            match stream {
                Ok(stream) => {
                    reply.accept().await;
                    let _ = orion_ssh_forward::carry(channel, stream).await;
                }
                Err(why) if why == "forwarding is off" => {
                    reply
                        .reject(russh::ChannelOpenFailure::AdministrativelyProhibited)
                        .await;
                }
                Err(_) => reply.reject(russh::ChannelOpenFailure::ConnectFailed).await,
            }
        });
        Ok(())
    }

    async fn pty_request(
        &mut self,
        channel: ChannelId,
        term: &str,
        col_width: u32,
        row_height: u32,
        _pix_width: u32,
        _pix_height: u32,
        _modes: &[(russh::Pty, u32)],
        session: &mut Session,
    ) -> Result<(), Self::Error> {
        let chan = self.channels.entry(channel).or_default();
        let terminal = Terminal {
            term: term.to_owned(),
            cols: col_width,
            rows: row_height,
        };
        if chan.running.is_some() || !terminal.is_named() {
            return session.channel_failure(channel);
        }
        chan.terminal = Some(terminal);
        session.channel_success(channel)
    }

    async fn shell_request(
        &mut self,
        channel: ChannelId,
        session: &mut Session,
    ) -> Result<(), Self::Error> {
        if self.start(channel, Wanted::Shell, session.handle()).await {
            session.channel_success(channel)
        } else {
            session.channel_failure(channel)
        }
    }

    async fn exec_request(
        &mut self,
        channel: ChannelId,
        data: &[u8],
        session: &mut Session,
    ) -> Result<(), Self::Error> {
        if self
            .start(channel, Wanted::Command(data.to_vec()), session.handle())
            .await
        {
            session.channel_success(channel)
        } else {
            session.channel_failure(channel)
        }
    }

    async fn subsystem_request(
        &mut self,
        channel: ChannelId,
        name: &str,
        session: &mut Session,
    ) -> Result<(), Self::Error> {
        if self
            .start(
                channel,
                Wanted::Subsystem(name.to_owned()),
                session.handle(),
            )
            .await
        {
            session.channel_success(channel)
        } else {
            session.channel_failure(channel)
        }
    }

    async fn data(
        &mut self,
        channel: ChannelId,
        data: &[u8],
        _session: &mut Session,
    ) -> Result<(), Self::Error> {
        if let Some(running) = self
            .channels
            .get(&channel)
            .and_then(|chan| chan.running.as_ref())
        {
            running.feed(Input::Data(data.to_vec())).await;
        }
        Ok(())
    }

    async fn channel_eof(
        &mut self,
        channel: ChannelId,
        _session: &mut Session,
    ) -> Result<(), Self::Error> {
        if let Some(running) = self
            .channels
            .get(&channel)
            .and_then(|chan| chan.running.as_ref())
        {
            running.feed(Input::End).await;
        }
        Ok(())
    }

    async fn window_change_request(
        &mut self,
        channel: ChannelId,
        col_width: u32,
        row_height: u32,
        _pix_width: u32,
        _pix_height: u32,
        _session: &mut Session,
    ) -> Result<(), Self::Error> {
        if let Some(running) = self
            .channels
            .get(&channel)
            .and_then(|chan| chan.running.as_ref())
        {
            running.resize(col_width, row_height);
        }
        Ok(())
    }

    async fn channel_close(
        &mut self,
        channel: ChannelId,
        _session: &mut Session,
    ) -> Result<(), Self::Error> {
        // Dropping what runs on it ends it.
        self.channels.remove(&channel);
        Ok(())
    }
}

/// How the server speaks SSH, with `host_key` as the board's key: one family of key
/// exchange, two ciphers, public keys only.
#[must_use]
pub fn protocol(host_key: PrivateKey) -> Arc<russh::server::Config> {
    let mut methods = MethodSet::empty();
    methods.push(MethodKind::PublicKey);
    Arc::new(russh::server::Config {
        methods,
        keys: vec![host_key],
        // Refusals take a moment, so that guessing is slow; the first, which every
        // client makes to ask what is taken, does not.
        auth_rejection_time: Duration::from_millis(500),
        auth_rejection_time_initial: Some(Duration::ZERO),
        max_auth_attempts: 6,
        keepalive_interval: Some(Duration::from_secs(30)),
        keepalive_max: 3,
        // A client that is there answers the keepalives. One that has stopped
        // answering, logged in or not, is dropped.
        inactivity_timeout: Some(Duration::from_secs(120)),
        preferred: Preferred {
            kex: Cow::Borrowed(&[
                russh::kex::MLKEM768X25519_SHA256,
                russh::kex::CURVE25519,
                russh::kex::CURVE25519_PRE_RFC_8731,
                russh::kex::EXTENSION_SUPPORT_AS_SERVER,
            ]),
            cipher: Cow::Borrowed(&[russh::cipher::CHACHA20_POLY1305, russh::cipher::AES_256_GCM]),
            ..Preferred::DEFAULT
        },
        ..russh::server::Config::default()
    })
}

/// Serves one client on `stream` until it is done: reads everything it sends, and asks
/// `door` for whatever needs the board's rights. A client that has not logged in within
/// `login` is dropped. `logged_in` is called when it has.
pub async fn serve<D: Door>(
    stream: impl AsyncRead + AsyncWrite + Unpin + Send + 'static,
    door: D,
    protocol: Arc<russh::server::Config>,
    login: Duration,
    logged_in: impl FnOnce() + Send + 'static,
) {
    let flag = Arc::new(AtomicBool::new(false));
    let client = Client {
        door,
        channels: HashMap::new(),
        logged_in: flag.clone(),
    };
    let deadline = tokio::time::Instant::now() + login;
    // Until the client has said which SSH it speaks there is no session to end: the
    // connection is dropped.
    let greeted = tokio::time::timeout_at(
        deadline,
        russh::server::run_stream(protocol, stream, client),
    )
    .await;
    let running = match greeted {
        Ok(Ok(running)) => running,
        Ok(Err(error)) => {
            debug!("ssh: {error}");
            return;
        }
        Err(_) => {
            debug!("ssh: a client said nothing in time");
            return;
        }
    };
    let handle = running.handle();
    tokio::pin!(running);
    // Told the moment it logs in, or dropped when its time is up.
    let mut logged_in = Some(logged_in);
    let mut tick = tokio::time::interval(Duration::from_millis(50));
    let ended = loop {
        tokio::select! {
            ended = &mut running => break ended,
            _ = tick.tick(), if logged_in.is_some() => {
                if flag.load(Ordering::Acquire) {
                    if let Some(tell) = logged_in.take() {
                        tell();
                    }
                } else if tokio::time::Instant::now() >= deadline {
                    debug!("ssh: a client did not log in in time");
                    let _ = handle
                        .disconnect(russh::Disconnect::ByApplication, "no login in time".into(), String::new())
                        .await;
                    break running.await;
                }
            }
        }
    };
    if let Err(error) = ended {
        debug!("ssh: {error}");
    }
}

/// The SSH server. Dropping it stops taking connections.
pub struct Ssh {
    local_addr: SocketAddr,
    places: Arc<Places>,
    task: JoinHandle<()>,
}

impl std::fmt::Debug for Ssh {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Ssh({})", self.local_addr)
    }
}

impl Drop for Ssh {
    fn drop(&mut self) {
        self.task.abort();
    }
}

impl Ssh {
    /// Starts taking SSH connections. Logins are decided by the check, with `security` as
    /// its switch and `keys` as its trust store.
    ///
    /// # Errors
    ///
    /// If the address cannot be listened on, or the kernel gives no random bytes for a
    /// host key.
    pub async fn start(
        config: SshConfig,
        security: Security,
        keys: SharedKeys,
    ) -> std::io::Result<Self> {
        let host_key = hostkey::load_or_make(&config.host_key)
            .map_err(|error| std::io::Error::new(error.kind(), format!("no host key: {error}")))?;
        let protocol = protocol(host_key);
        let (at, interface) = (config.listen, config.interface.clone());
        let listener = match listen(at, interface.as_deref()) {
            Ok(listener) => Some(listener),
            Err(error) if interface.is_some() && absent(&error) => None,
            Err(error) => return Err(error),
        };
        let local_addr = listener.as_ref().map_or(Ok(at), TcpListener::local_addr)?;
        match (&listener, &interface) {
            (Some(_), Some(interface)) => {
                tracing::info!("ssh: on {interface} only, open on {local_addr}")
            }
            (Some(_), None) => tracing::info!("ssh: open on {local_addr}"),
            (None, _) => {}
        }
        let places = Places::new(&config);
        let login = config.login;
        let keeper = Arc::new(Keeper::new(config, security, keys, local_addr));
        let room = places.clone();
        let task = tokio::spawn(async move {
            let listener = match (listener, interface) {
                (Some(listener), _) => listener,
                (None, Some(interface)) => listen_when_there(at, &interface).await,
                (None, None) => unreachable!("only a named interface is waited for"),
            };
            loop {
                let (stream, peer) = match listener.accept().await {
                    Ok(accepted) => accepted,
                    Err(error) => {
                        debug!(%error, "ssh: a connection could not be accepted");
                        tokio::time::sleep(Duration::from_millis(100)).await;
                        continue;
                    }
                };
                let Some((connection, waiting)) = room.take() else {
                    debug!("ssh: {peer} turned away: too many connections");
                    continue;
                };
                let (door, protocol) = (Local::new(keeper.clone(), Some(peer)), protocol.clone());
                tokio::spawn(async move {
                    let _ = stream.set_nodelay(true);
                    serve(stream, door, protocol, login, move || drop(waiting)).await;
                    drop(connection);
                });
            }
        });
        Ok(Self {
            local_addr,
            places,
            task,
        })
    }

    /// Where SSH clients connect.
    #[must_use]
    pub const fn local_addr(&self) -> SocketAddr {
        self.local_addr
    }

    /// How many connections there are now.
    #[must_use]
    pub fn connections(&self) -> usize {
        self.places.connections()
    }
}
