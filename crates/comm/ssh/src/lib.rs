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

pub mod helper;
pub mod hostkey;
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

use orion_check::{Admits, Check, Checked, Policy, Principal, Request, Security, What};
use orion_config::{ConfigError, Source};
use orion_keys::SharedKeys;
use russh::keys::PublicKey;
use russh::server::{Auth, ChannelOpenHandle, Handler, Msg, Session};
use russh::{Channel, ChannelId, MethodKind, MethodSet, Preferred};
use tokio::net::TcpListener;
use tokio::task::JoinHandle;
use tracing::{debug, info, warn};

use helper::Launch;
use session::{Input, Running, Terminal};
use users::User;

/// Who may log in: an operator, and only one that has been granted it.
pub const LOGIN: Policy = Policy::new(Admits::OPERATOR);

/// Where programs are looked for in a session.
const PATH: &str = "/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin";

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

/// What a client asked a channel to run.
enum Wanted<'a> {
    Shell,
    Command(&'a [u8]),
    Program(Vec<OsString>),
}

/// What the SSH server is told.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SshConfig {
    /// `ssh.listen`: where SSH clients connect.
    pub listen: SocketAddr,
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
    /// `ssh.max_connections`: the most connections at once.
    pub max_connections: usize,
    /// `ssh.max_unauthenticated`: the most connections at once that have not logged in.
    pub max_unauthenticated: usize,
    /// `ssh.login_ms`: how long a connection has to log in.
    pub login: Duration,
}

impl SshConfig {
    /// The server's settings, or `None` if `ssh.listen` is not set: no SSH.
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
        let config = Self {
            listen: listen.unwrap_or_else(|| SocketAddr::from(([0, 0, 0, 0], 22))),
            host_key: source
                .optional("ssh.host_key")?
                .unwrap_or_else(|| state_dir.join("ssh_host_ed25519_key")),
            helper,
            subsystems: Vec::new(),
            forward: source.get("ssh.forward", true)?,
            etc: "/etc".into(),
            max_connections: source.get("ssh.max_connections", 64_usize)?.max(1),
            max_unauthenticated: source.get("ssh.max_unauthenticated", 10_usize)?.max(1),
            login: Duration::from_millis(source.get("ssh.login_ms", 60_000_u64)?.max(1000)),
        };
        Ok(listen.map(|_| config))
    }
}

struct Shared {
    config: SshConfig,
    security: Security,
    keys: SharedKeys,
    local_addr: SocketAddr,
    connections: AtomicUsize,
    unauthenticated: AtomicUsize,
}

/// One connection counted against a limit, until dropped.
struct Counted(Arc<Shared>, fn(&Shared) -> &AtomicUsize);

impl Counted {
    /// Counts one more, unless that is more than `most`.
    fn take(shared: &Arc<Shared>, which: fn(&Shared) -> &AtomicUsize, most: usize) -> Option<Self> {
        let count = which(shared);
        let mut now = count.load(Ordering::Acquire);
        let taken = loop {
            if now >= most {
                break false;
            }
            match count.compare_exchange_weak(now, now + 1, Ordering::AcqRel, Ordering::Acquire) {
                Ok(_) => break true,
                Err(actual) => now = actual,
            }
        };
        taken.then(|| Self(shared.clone(), which))
    }
}

impl Drop for Counted {
    fn drop(&mut self) {
        (self.1)(&self.0).fetch_sub(1, Ordering::AcqRel);
    }
}

/// One channel of a connection.
#[derive(Default)]
struct Chan {
    terminal: Option<Terminal>,
    running: Option<Running>,
}

/// One client's connection.
struct Client {
    shared: Arc<Shared>,
    peer: Option<SocketAddr>,
    /// Who logged in, and as which account: only the check makes the first.
    admitted: Option<(Checked<Login>, User)>,
    channels: HashMap<ChannelId, Chan>,
    /// Counted as not logged in, until it is.
    waiting: Option<Counted>,
    /// Set when it has logged in, for the task that ends connections that never do.
    logged_in: Arc<AtomicBool>,
    _connection: Counted,
}

impl Client {
    fn from(&self) -> String {
        self.peer
            .map_or_else(|| "?".to_owned(), |peer| peer.to_string())
    }

    /// Decides a login as `name` by `who`. The check first, then the account.
    fn login(&mut self, name: &str, who: &Principal, how: &str) -> Auth {
        let check = Check::with_grants(self.shared.security, &self.shared.keys);
        let checked = match check.decide(who, Login, &LOGIN) {
            Ok(checked) => checked,
            Err(refused) => {
                info!(
                    "ssh: {} was not let in as {name} with {how}: {refused}",
                    self.from()
                );
                return Auth::reject();
            }
        };
        let Some(user) = users::lookup(&self.shared.config.etc, name) else {
            info!(
                "ssh: {} asked for {name}, which is not an account here",
                self.from()
            );
            return Auth::reject();
        };
        let me = rustix::process::getuid();
        if !me.is_root() && me.as_raw() != user.uid {
            warn!(
                "ssh: {} asked for {name}, but the comm core runs as user {} and can only \
                 let that user in",
                self.from(),
                me.as_raw()
            );
            return Auth::reject();
        }
        info!("ssh: {} logged in as {name} with {how}", self.from());
        self.admitted = Some((checked, user));
        self.waiting = None;
        self.logged_in.store(true, Ordering::Release);
        Auth::Accept
    }

    /// Whose key `key` is.
    fn who(&self, key: &PublicKey) -> Principal {
        match key.key_data().ed25519() {
            Some(key) => self.shared.keys.lock().principal(&key.0),
            // The trust store holds keys of one kind.
            None => Principal::Anonymous,
        }
    }

    /// Starts what the client wanted on `channel`, as the account that logged in.
    fn start(&mut self, channel: ChannelId, wanted: Wanted<'_>, session: &Session) -> bool {
        let Some((_checked, user)) = &self.admitted else {
            return false;
        };
        let chan = self.channels.entry(channel).or_default();
        if chan.running.is_some() {
            return false;
        }
        let mut env = vec![
            ("HOME".to_owned(), user.home.to_string_lossy().into_owned()),
            ("USER".to_owned(), user.name.clone()),
            ("LOGNAME".to_owned(), user.name.clone()),
            (
                "SHELL".to_owned(),
                user.shell.to_string_lossy().into_owned(),
            ),
            ("PATH".to_owned(), PATH.to_owned()),
        ];
        if let Some(terminal) = &chan.terminal {
            env.push(("TERM".to_owned(), terminal.term.clone()));
        }
        if let Some(peer) = self.peer {
            let here = self.shared.local_addr;
            env.push((
                "SSH_CONNECTION".to_owned(),
                format!(
                    "{} {} {} {}",
                    peer.ip(),
                    peer.port(),
                    here.ip(),
                    here.port()
                ),
            ));
        }
        let launch = Launch {
            uid: user.uid,
            gid: user.gid,
            groups: user.groups.clone(),
            home: user.home.clone(),
            tty: chan.terminal.is_some(),
            env,
            shell: user.shell.clone(),
            command: match &wanted {
                Wanted::Command(command) => {
                    use std::os::unix::ffi::OsStringExt;
                    Some(OsString::from_vec(command.to_vec()))
                }
                _ => None,
            },
            program: match wanted {
                Wanted::Program(program) => program,
                _ => Vec::new(),
            },
        };
        let started = session::start(
            &self.shared.config.helper,
            &launch,
            chan.terminal.as_ref(),
            session.handle(),
            channel,
        );
        match started {
            Ok(running) => {
                chan.running = Some(running);
                true
            }
            Err(error) => {
                warn!("ssh: a session for {} could not start: {error}", user.name);
                false
            }
        }
    }
}

impl Handler for Client {
    type Error = russh::Error;

    async fn auth_none(&mut self, user: &str) -> Result<Auth, Self::Error> {
        // Nobody known. Let in only where the check lets anyone in: security off.
        Ok(self.login(user, &Principal::Anonymous, "no key"))
    }

    async fn auth_publickey_offered(
        &mut self,
        _user: &str,
        offer: russh::server::PublicKeyOffer<'_>,
    ) -> Result<Auth, Self::Error> {
        // A key that would not be let in is not asked to prove itself.
        let who = self.who(offer.public_key);
        let check = Check::with_grants(self.shared.security, &self.shared.keys);
        Ok(match check.decide(&who, Login, &LOGIN) {
            Ok(_) => Auth::Accept,
            Err(_) => Auth::reject(),
        })
    }

    async fn auth_publickey(
        &mut self,
        user: &str,
        key: russh::server::VerifiedPublicKey<'_>,
    ) -> Result<Auth, Self::Error> {
        let who = self.who(key.public_key);
        Ok(self.login(user, &who, "its key"))
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
        let Some((_checked, user)) = &self.admitted else {
            reply
                .reject(russh::ChannelOpenFailure::AdministrativelyProhibited)
                .await;
            return Ok(());
        };
        if !self.shared.config.forward {
            info!(
                "ssh: {} asked for a connection to {host_to_connect}:{port_to_connect}: forwarding is off",
                user.name
            );
            reply
                .reject(russh::ChannelOpenFailure::AdministrativelyProhibited)
                .await;
            return Ok(());
        }
        let (who, host) = (user.name.clone(), host_to_connect.to_owned());
        // Connecting can take a while: the rest of this client's connection goes on.
        tokio::spawn(async move {
            match orion_ssh_forward::connect(&host, port_to_connect).await {
                Ok(stream) => {
                    info!("ssh: {who} is connected to {host}:{port_to_connect} from the board");
                    reply.accept().await;
                    let _ = orion_ssh_forward::carry(channel, stream).await;
                }
                Err(refused) => {
                    info!("ssh: {who} was not connected to {host}:{port_to_connect}: {refused}");
                    reply.reject(russh::ChannelOpenFailure::ConnectFailed).await;
                }
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
        // The name of a terminal is letters, digits and a few signs.
        let named = !term.is_empty()
            && term.len() <= 64
            && term
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || b"-_.+".contains(&byte));
        if chan.running.is_some() || !named {
            return session.channel_failure(channel);
        }
        chan.terminal = Some(Terminal {
            term: term.to_owned(),
            cols: col_width,
            rows: row_height,
        });
        session.channel_success(channel)
    }

    async fn shell_request(
        &mut self,
        channel: ChannelId,
        session: &mut Session,
    ) -> Result<(), Self::Error> {
        if self.start(channel, Wanted::Shell, session) {
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
        if self.start(channel, Wanted::Command(data), session) {
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
        let program = self
            .shared
            .config
            .subsystems
            .iter()
            .find(|subsystem| subsystem.name == name)
            .map(|subsystem| subsystem.program.clone());
        // A subsystem speaks bytes: never on a terminal.
        if let Some(chan) = self.channels.get_mut(&channel) {
            chan.terminal = None;
        }
        let started = match program {
            Some(program) => self.start(channel, Wanted::Program(program), session),
            None => false,
        };
        if started {
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

/// The SSH server. Dropping it stops taking connections.
pub struct Ssh {
    local_addr: SocketAddr,
    shared: Arc<Shared>,
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
        let host_key = hostkey::load_or_make(&config.host_key)?;
        let listener = TcpListener::bind(config.listen).await?;
        let local_addr = listener.local_addr()?;
        let mut methods = MethodSet::empty();
        methods.push(MethodKind::PublicKey);
        let server = Arc::new(russh::server::Config {
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
                cipher: Cow::Borrowed(&[
                    russh::cipher::CHACHA20_POLY1305,
                    russh::cipher::AES_256_GCM,
                ]),
                ..Preferred::DEFAULT
            },
            ..russh::server::Config::default()
        });
        let shared = Arc::new(Shared {
            config,
            security,
            keys,
            local_addr,
            connections: AtomicUsize::new(0),
            unauthenticated: AtomicUsize::new(0),
        });
        let accepting = shared.clone();
        let task = tokio::spawn(async move {
            loop {
                let (stream, peer) = match listener.accept().await {
                    Ok(accepted) => accepted,
                    Err(error) => {
                        debug!(%error, "ssh: a connection could not be accepted");
                        tokio::time::sleep(Duration::from_millis(100)).await;
                        continue;
                    }
                };
                let limits = &accepting.config;
                let (Some(connection), Some(waiting)) = (
                    Counted::take(
                        &accepting,
                        |shared| &shared.connections,
                        limits.max_connections,
                    ),
                    Counted::take(
                        &accepting,
                        |shared| &shared.unauthenticated,
                        limits.max_unauthenticated,
                    ),
                ) else {
                    debug!("ssh: {peer} turned away: too many connections");
                    continue;
                };
                let logged_in = Arc::new(AtomicBool::new(false));
                let client = Client {
                    logged_in: logged_in.clone(),
                    shared: accepting.clone(),
                    peer: Some(peer),
                    admitted: None,
                    channels: HashMap::new(),
                    waiting: Some(waiting),
                    _connection: connection,
                };
                let (server, login) = (server.clone(), limits.login);
                tokio::spawn(async move {
                    let _ = stream.set_nodelay(true);
                    let deadline = tokio::time::Instant::now() + login;
                    // Until the client has said which SSH it speaks there is no session
                    // to end: the connection is dropped.
                    let greeted = tokio::time::timeout_at(
                        deadline,
                        russh::server::run_stream(server, stream, client),
                    )
                    .await;
                    let running = match greeted {
                        Ok(Ok(running)) => running,
                        Ok(Err(error)) => {
                            debug!("ssh: {peer}: {error}");
                            return;
                        }
                        Err(_) => {
                            debug!("ssh: {peer} said nothing in time");
                            return;
                        }
                    };
                    let handle = running.handle();
                    tokio::pin!(running);
                    // A connection that has not logged in by now never will.
                    let ended = tokio::select! {
                        ended = &mut running => ended,
                        () = tokio::time::sleep_until(deadline) => {
                            if !logged_in.load(Ordering::Acquire) {
                                debug!("ssh: {peer} did not log in in time");
                                let _ = handle
                                    .disconnect(
                                        russh::Disconnect::ByApplication,
                                        "no login in time".into(),
                                        String::new(),
                                    )
                                    .await;
                            }
                            running.await
                        }
                    };
                    if let Err(error) = ended {
                        debug!("ssh: {peer}: {error}");
                    }
                });
            }
        });
        Ok(Self {
            local_addr,
            shared,
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
        self.shared.connections.load(Ordering::Acquire)
    }
}
