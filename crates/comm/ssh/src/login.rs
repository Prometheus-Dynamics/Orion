//! The login helper: the one process on the board that may become someone else.
//!
//! The comm core decides who is let in, and then runs as a user of its own, with no right to
//! become anyone. Starting a session as the account that logged in, or a child for an SSH
//! connection as the account that owns nothing, is asked of this helper, on a socket only the
//! comm core's user may use (`login.helper`).
//!
//! - **It decides again.** A login is started only if the helper, on its own, reaches the same
//!   yes: the same check, with the switch read from the same record by the same reader, against
//!   the same trust store (read, never written), and the same rules for the account. A comm
//!   core that was taken over can ask for anything; it gets only what the board would have let
//!   in anyway.
//! - **It starts nothing else.** A session as the account a login was let in as, or an SSH
//!   connection's child as `ssh.sandbox_user`, its own setting: never an account the asker
//!   names.
//! - **It holds what it starts.** A session's status socket goes back to the comm core;
//!   closing it ends the session, which the helper does, so the comm core needs no right to
//!   signal another user's processes.
//! - **It is not there when nobody is.** Started by the system when the comm core first
//!   connects, it exits when no session is running and nothing has come for `login.idle`.

use std::net::SocketAddr;
use std::os::fd::{AsFd, OwnedFd};
use std::os::unix::net::{UnixListener, UnixStream};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use orion_check::{Check, Principal, Security};
use orion_keys::{Keys, KeysConfig, SharedKeys};
use orion_types::{DeviceName, KeyId, NodeName};
use serde::{Deserialize, Serialize};
use tracing::{info, warn};

use crate::keeper::{Io, Started, Terminal, Wanted};
use crate::{Keeper, SHELL, ShellAs, SshConfig, users};

/// The word that says a command line is for the helper.
pub const MODE: &str = "login-helper";

/// The longest message either way.
const MAX_MESSAGE: usize = 64 * 1024;

/// Who a shell is asked for, as it crosses.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Who {
    /// Nothing is known.
    Anonymous,
    /// A program on the board.
    Local {
        /// User id.
        uid: u32,
        /// Group id.
        gid: u32,
    },
    /// An operator, by key.
    Operator([u8; 32]),
    /// Another node.
    Peer(String),
    /// A device on a link.
    Device(String),
}

impl From<&Principal> for Who {
    fn from(principal: &Principal) -> Self {
        match principal {
            Principal::Anonymous => Self::Anonymous,
            Principal::Local { uid, gid } => Self::Local {
                uid: *uid,
                gid: *gid,
            },
            Principal::Operator(key) => Self::Operator(key.0),
            Principal::Peer(name) => Self::Peer(name.to_string()),
            Principal::Device(name) => Self::Device(name.to_string()),
        }
    }
}

impl Who {
    /// The principal again; `None` if a name does not parse.
    fn principal(&self) -> Option<Principal> {
        Some(match self {
            Self::Anonymous => Principal::Anonymous,
            Self::Local { uid, gid } => Principal::Local {
                uid: *uid,
                gid: *gid,
            },
            Self::Operator(key) => Principal::Operator(KeyId(*key)),
            Self::Peer(name) => Principal::Peer(NodeName::new(name).ok()?),
            Self::Device(name) => Principal::Device(DeviceName::new(name).ok()?),
        })
    }
}

/// What a session is to run, as it crosses.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Want {
    /// A login shell.
    Shell,
    /// A command.
    Command(Vec<u8>),
    /// A subsystem, by name.
    Subsystem(String),
}

impl From<Wanted> for Want {
    fn from(wanted: Wanted) -> Self {
        match wanted {
            Wanted::Shell => Self::Shell,
            Wanted::Command(command) => Self::Command(command),
            Wanted::Subsystem(name) => Self::Subsystem(name),
        }
    }
}

impl From<Want> for Wanted {
    fn from(want: Want) -> Self {
        match want {
            Want::Shell => Self::Shell,
            Want::Command(command) => Self::Command(command),
            Want::Subsystem(name) => Self::Subsystem(name),
        }
    }
}

/// A terminal as it crosses: its name and its size.
type Term = Option<(String, u32, u32)>;

pub(crate) fn term(terminal: Option<&Terminal>) -> Term {
    terminal.map(|terminal| (terminal.term.clone(), terminal.cols, terminal.rows))
}

fn terminal(term: Term) -> Option<Terminal> {
    term.map(|(term, cols, rows)| Terminal { term, cols, rows })
}

/// What the comm core asks.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Ask {
    /// A session for an SSH login as `account` by whoever holds `key`.
    Ssh {
        /// The account.
        account: String,
        /// The key the client proved it holds.
        key: Option<[u8; 32]>,
        /// Where the client is.
        peer: Option<SocketAddr>,
        /// Where it connected to.
        local: SocketAddr,
        /// What to run.
        want: Want,
        /// The terminal, if one.
        term: Term,
        /// Whether the comm core is in recovery: then the recovery keys decide.
        recovery: bool,
    },
    /// A shell over an Orion session as `account`, for `who`.
    Shell {
        /// The account.
        account: String,
        /// The session's principal.
        who: Who,
        /// What to run.
        want: Want,
        /// The terminal, if one.
        term: Term,
        /// Whether the comm core is in recovery.
        recovery: bool,
    },
    /// A child for an SSH connection: with it come the client's connection and the socket
    /// to the comm core.
    Sandbox,
}

/// What the helper answers.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Answer {
    /// A session on a terminal: with it come the terminal and the status socket.
    Terminal,
    /// A session on pipes: with it come its input, its output, its errors and the status
    /// socket.
    Pipes,
    /// The connection's child, by process id: with it comes its status socket.
    Sandbox(u32),
    /// Not done, and why.
    Refused(String),
}

fn broken(why: impl std::fmt::Display) -> std::io::Error {
    std::io::Error::new(std::io::ErrorKind::InvalidData, why.to_string())
}

fn send<T: Serialize>(socket: &UnixStream, message: &T, fds: &[OwnedFd]) -> std::io::Result<()> {
    let bytes = postcard::to_allocvec(message).map_err(broken)?;
    let fds: Vec<_> = fds.iter().map(AsFd::as_fd).collect();
    shm_fdpass::send_on(socket, &[&bytes], &fds).map_err(|error| broken(error.to_string()))
}

fn recv<T: for<'de> Deserialize<'de>>(
    socket: &UnixStream,
) -> std::io::Result<Option<(T, Vec<OwnedFd>)>> {
    let mut buffer = vec![0_u8; MAX_MESSAGE];
    let received = match shm_fdpass::recv(socket.as_fd(), &mut buffer, 4, false) {
        Ok(Some(received)) => received,
        Ok(None) => return Ok(None),
        Err(shm_fdpass::RecvError::Io(error)) => return Err(error),
        Err(error) => return Err(broken(error.to_string())),
    };
    let message = postcard::from_bytes(&buffer[..received.len]).map_err(broken)?;
    let mut fds = received.fds;
    let fds = (0..fds.len()).filter_map(|index| fds.take(index)).collect();
    Ok(Some((message, fds)))
}

/// The comm core's end: asks the helper at `path`, one connection a question.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LoginClient {
    path: PathBuf,
}

impl LoginClient {
    /// The helper listening at `path`.
    #[must_use]
    pub fn new(path: impl Into<PathBuf>) -> Self {
        Self { path: path.into() }
    }

    /// Where it listens.
    #[must_use]
    pub fn path(&self) -> &Path {
        &self.path
    }

    fn ask(&self, ask: &Ask, fds: &[OwnedFd]) -> std::io::Result<(Answer, Vec<OwnedFd>)> {
        let socket = UnixStream::connect(&self.path).map_err(|error| {
            std::io::Error::new(
                error.kind(),
                format!(
                    "the login helper at {} is not there: {error}",
                    self.path.display()
                ),
            )
        })?;
        send(&socket, ask, fds)?;
        recv(&socket)?.ok_or_else(|| {
            std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "the login helper answered nothing",
            )
        })
    }

    /// Asks for a session, and gives it as the keeper would have started it.
    ///
    /// # Errors
    ///
    /// If the helper is not there, or refused: its words.
    pub fn session(&self, ask: &Ask) -> std::io::Result<Started> {
        let (answer, fds) = self.ask(ask, &[])?;
        let mut fds = fds.into_iter();
        let mut next = || {
            fds.next()
                .ok_or_else(|| std::io::Error::other("a descriptor is missing"))
        };
        let io = match answer {
            Answer::Terminal => Io::Terminal(next()?),
            Answer::Pipes => Io::Pipes {
                input: next()?,
                output: next()?,
                errors: next()?,
            },
            Answer::Refused(why) => {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::PermissionDenied,
                    format!("the login helper refused: {why}"),
                ));
            }
            Answer::Sandbox(_) => return Err(std::io::Error::other("not a session")),
        };
        Ok(Started {
            io,
            status: next()?,
        })
    }

    /// Asks for an SSH connection's child, on `client` and talking to `socket`. Gives its
    /// process id and its status socket: closing that ends the child.
    ///
    /// # Errors
    ///
    /// If the helper is not there, or refused: its words.
    pub fn sandbox(&self, client: OwnedFd, socket: OwnedFd) -> std::io::Result<(u32, OwnedFd)> {
        match self.ask(&Ask::Sandbox, &[client, socket])? {
            (Answer::Sandbox(pid), fds) => fds
                .into_iter()
                .next()
                .map(|status| (pid, status))
                .ok_or_else(|| std::io::Error::other("the status socket is missing")),
            (Answer::Refused(why), _) => Err(std::io::Error::new(
                std::io::ErrorKind::PermissionDenied,
                format!("the login helper refused: {why}"),
            )),
            _ => Err(std::io::Error::other("not a child")),
        }
    }
}

/// What the helper is told, from the same settings file as the comm core.
#[derive(Clone, Debug)]
pub struct HelperConfig {
    /// The SSH way in, if it is on.
    pub ssh: Option<SshConfig>,
    /// The shell over a session, if it is on.
    pub shell: Option<SshConfig>,
    /// The account SSH connections' children run as.
    pub sandbox_user: String,
    /// The trust store, read and never written.
    pub keys: KeysConfig,
    /// The security switch's record.
    pub security_view: PathBuf,
    /// The only user that may ask: the comm core's.
    pub comm_uid: u32,
    /// How long it waits with nothing running and nothing asked before it exits.
    pub idle: Duration,
    /// How a comm core in recovery is answered.
    pub recovery: RecoveryLogin,
}

/// Logins while the comm core is in recovery: whoever the recovery keys name, as the recovery
/// way in lets them in. A comm core that says it is in recovery when it is not gains nothing:
/// the holders of the recovery keys are trusted with everything anyway, and with the switch
/// off the board is open either way.
#[derive(Clone, Debug)]
pub struct RecoveryLogin {
    /// The SSH settings recovery sessions start with (`recovery` set).
    pub ssh: SshConfig,
    /// The recovery keys.
    pub keys_file: PathBuf,
    /// How the keys file becomes a trust store: the recovery way in's own reading of it.
    pub trust: fn(&Path) -> Option<Keys>,
}

/// The helper: answers on `listener` until it has been idle for `config.idle`.
///
/// # Errors
///
/// If the listener fails.
pub fn serve(config: &HelperConfig, listener: &UnixListener) -> std::io::Result<()> {
    use rustix::event::{PollFd, PollFlags, poll};
    let running = Arc::new(AtomicUsize::new(0));
    let mut last = Instant::now();
    listener.set_nonblocking(true)?;
    loop {
        let quiet = last.elapsed();
        if running.load(Ordering::SeqCst) == 0 && quiet >= config.idle {
            info!("login helper: nothing running and nothing asked: exiting");
            return Ok(());
        }
        let wait = config
            .idle
            .saturating_sub(quiet)
            .max(Duration::from_millis(100));
        let timeout = rustix::event::Timespec::try_from(wait).ok();
        let mut fds = [PollFd::new(listener, PollFlags::IN)];
        match poll(&mut fds, timeout.as_ref()) {
            Ok(0) => continue,
            Ok(_) => {}
            Err(rustix::io::Errno::INTR) => continue,
            Err(error) => return Err(error.into()),
        }
        let socket = match listener.accept() {
            Ok((socket, _)) => socket,
            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => continue,
            Err(error) => {
                warn!("login helper: a connection could not be taken: {error}");
                continue;
            }
        };
        let _ = socket.set_nonblocking(false);
        let _ = socket.set_read_timeout(Some(Duration::from_secs(5)));
        let answer = answer(config, &socket, &running);
        let (answer, fds) = match answer {
            Ok(done) => done,
            Err(why) => {
                info!("login helper: refused: {why}");
                (Answer::Refused(why), Vec::new())
            }
        };
        let _ = send(&socket, &answer, &fds);
        last = Instant::now();
    }
}

/// The asker's user id, from the kernel.
fn asker_uid(socket: &UnixStream) -> Option<u32> {
    rustix::net::sockopt::socket_peercred(socket)
        .ok()
        .map(|cred| cred.uid.as_raw())
}

/// One question, decided and done.
fn answer(
    config: &HelperConfig,
    socket: &UnixStream,
    running: &Arc<AtomicUsize>,
) -> Result<(Answer, Vec<OwnedFd>), String> {
    let uid = asker_uid(socket).ok_or("the asker's user is not known")?;
    if uid != config.comm_uid {
        return Err(format!(
            "user {uid} asked, and only the comm core's user ({}) may",
            config.comm_uid
        ));
    }
    let (ask, fds) = match recv::<Ask>(socket) {
        Ok(Some(asked)) => asked,
        Ok(None) => return Err("the asker said nothing".to_owned()),
        Err(error) => return Err(format!("what was asked could not be read: {error}")),
    };
    // Read for every question, by the reader the comm core uses: a switch turned since the
    // last one counts at once.
    let view = orion_config::SecurityView::read(&config.security_view);
    let security = view.security;
    let (keeper, admitted, want, term, peer) = match ask {
        Ask::Ssh {
            account,
            key,
            peer,
            local,
            want,
            term,
            recovery,
        } => {
            let (ssh, keys) = if recovery {
                (config.recovery.ssh.clone(), recovery_trust(config)?)
            } else {
                let ssh = config.ssh.clone().ok_or("the SSH way in is off")?;
                (ssh, trust(config, security)?)
            };
            let keeper = Keeper::new(ssh, security, keys, local);
            let admitted = keeper
                .login(&account, key.as_ref(), peer)
                .ok_or_else(|| format!("an SSH login as {account} is not let in"))?;
            (keeper, admitted, want, term, peer)
        }
        Ask::Shell {
            account,
            who,
            want,
            term,
            recovery,
        } => {
            let (shell, keys) = if recovery {
                (config.recovery.ssh.clone(), recovery_trust(config)?)
            } else {
                let shell = config.shell.clone().ok_or("the shell is off")?;
                (shell, trust(config, security)?)
            };
            let principal = who.principal().ok_or("the principal does not parse")?;
            let checked = Check::with_grants(security, &keys)
                .decide(&principal, ShellAs(&account), &SHELL)
                .map_err(|refused| format!("a shell as {account} for {principal}: {refused}"))?;
            let listen = shell.listen;
            let keeper = Keeper::new(shell, security, keys, listen);
            let from = format!("{principal} (through the comm core)");
            let admitted = keeper
                .admit(&checked, &account, &principal, &from)
                .ok_or_else(|| format!("a shell as {account} is not let in for {principal}"))?;
            (keeper, admitted, want, term, None)
        }
        Ask::Sandbox => return sandbox(config, fds, running),
    };
    let started = keeper
        .start_here(&admitted, want.into(), terminal(term).as_ref(), peer)
        .map_err(|error| format!("the session could not be started: {error}"))?;
    Ok(session(started, running))
}

/// The trust store, read as it is now.
fn trust(config: &HelperConfig, security: Security) -> Result<SharedKeys, String> {
    Keys::read_only(config.keys.clone(), security)
        .map(SharedKeys::new)
        .map_err(|error| format!("the trust store could not be read: {error}"))
}

/// The recovery keys, read as they are now.
fn recovery_trust(config: &HelperConfig) -> Result<SharedKeys, String> {
    (config.recovery.trust)(&config.recovery.keys_file)
        .map(SharedKeys::new)
        .ok_or_else(|| {
            format!(
                "the recovery keys in {} could not be read",
                config.recovery.keys_file.display()
            )
        })
}

/// A started session as it goes back, counted while it runs.
fn session(started: Started, running: &Arc<AtomicUsize>) -> (Answer, Vec<OwnedFd>) {
    let Started { io, status } = started;
    let status = counted(status, running);
    match io {
        Io::Terminal(terminal) => (Answer::Terminal, vec![terminal, status]),
        Io::Pipes {
            input,
            output,
            errors,
        } => (Answer::Pipes, vec![input, output, errors, status]),
    }
}

/// Counts a session as running until its status socket says it ended: the end the comm core
/// gets is passed through a socket pair, and a thread carries the code across and counts
/// down when it has.
fn counted(status: OwnedFd, running: &Arc<AtomicUsize>) -> OwnedFd {
    use std::io::{Read, Write};
    let Ok((ours, theirs)) = UnixStream::pair() else {
        return status;
    };
    running.fetch_add(1, Ordering::SeqCst);
    let counter = running.clone();
    let carried = std::thread::Builder::new()
        .name("login-session".to_owned())
        .spawn(move || {
            let mut inner = UnixStream::from(status);
            let mut outer = ours;
            // Whichever ends first: the session (its code comes), or the comm core (it
            // closed its end, and dropping ours ends the session).
            let mut code = [0_u8; 4];
            let ended = {
                let mut watching = [
                    rustix::event::PollFd::new(&inner, rustix::event::PollFlags::IN),
                    rustix::event::PollFd::new(&outer, rustix::event::PollFlags::IN),
                ];
                while let Err(rustix::io::Errno::INTR) = rustix::event::poll(&mut watching, None) {}
                !watching[0].revents().is_empty()
            };
            if ended && inner.read_exact(&mut code).is_ok() {
                let _ = outer.write_all(&code);
            }
            drop(inner);
            counter.fetch_sub(1, Ordering::SeqCst);
        });
    if carried.is_err() {
        running.fetch_sub(1, Ordering::SeqCst);
    }
    theirs.into()
}

/// An SSH connection's child, as `ssh.sandbox_user` and nobody else.
fn sandbox(
    config: &HelperConfig,
    fds: Vec<OwnedFd>,
    running: &Arc<AtomicUsize>,
) -> Result<(Answer, Vec<OwnedFd>), String> {
    let ssh = config.ssh.as_ref().ok_or("the SSH way in is off")?;
    let mut fds = fds.into_iter();
    let (Some(client), Some(socket), None) = (fds.next(), fds.next(), fds.next()) else {
        return Err(
            "a child needs the client's connection and the socket, and only those".to_owned(),
        );
    };
    let user = users::lookup(&ssh.etc, &config.sandbox_user)
        .filter(|user| user.uid != 0)
        .ok_or_else(|| {
            format!(
                "ssh.sandbox_user {} is not an account without rights",
                config.sandbox_user
            )
        })?;
    let mut command = std::process::Command::new(&ssh.helper.program);
    command
        .args(&ssh.helper.args)
        .arg(crate::SANDBOX_MODE)
        .stdin(std::process::Stdio::from(client))
        .stdout(std::process::Stdio::from(socket));
    // The child becomes the account itself, first thing, before it reads a byte: as root it
    // can first empty its bounding set, which after the switch it no longer could. The
    // account is named here, by the helper, never by the comm core. Without the right to,
    // it runs as this process's user, as a comm core that is not root runs it itself.
    if rustix::process::getuid().is_root() {
        command
            .arg(crate::SANDBOX_BECOME)
            .arg(format!("{}:{}", user.uid, user.gid));
    }
    let child = command
        .spawn()
        .map_err(|error| format!("the child could not be started: {error}"))?;
    let pid = child.id();
    let (ours, theirs) = UnixStream::pair().map_err(|error| error.to_string())?;
    std::thread::Builder::new()
        .name("login-sandbox".to_owned())
        .spawn(move || crate::keeper::watch(child, ours.into()))
        .map_err(|error| error.to_string())?;
    Ok((Answer::Sandbox(pid), vec![counted(theirs.into(), running)]))
}
