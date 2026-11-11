//! The part of the SSH way in that needs the board's rights: deciding a login, starting a
//! session as the account, making a connection from the board. Everything else, which is
//! everything that reads what a client sends, asks here ([`crate::Door`]).
//!
//! Every function here blocks, and none of them reads a byte a client wrote that was not
//! first taken apart by someone else: a name, a key, a command, a terminal's name and size.

use std::ffi::OsString;
use std::net::SocketAddr;
use std::os::fd::OwnedFd;
use std::process::Stdio;

use orion_check::{Check, Checked, Principal, Security};
use orion_keys::SharedKeys;
use rustix::fs::{Mode, OFlags};
use rustix::pty::{OpenptFlags, grantpt, openpt, ptsname, unlockpt};
use rustix::termios::{Winsize, tcsetwinsize};
use tracing::{info, warn};

use crate::helper::{Launch, MODE};
use crate::users::{self, User};
use crate::{LOGIN, Login, SshConfig};

/// Where programs are looked for in a session.
const PATH: &str = "/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin";

/// The terminal a client asked for.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Terminal {
    /// Its kind, as `TERM` names it.
    pub term: String,
    /// Its width in characters.
    pub cols: u32,
    /// Its height in characters.
    pub rows: u32,
}

impl Terminal {
    /// Whether the name is one a terminal could have: letters, digits and a few signs.
    #[must_use]
    pub fn is_named(&self) -> bool {
        !self.term.is_empty()
            && self.term.len() <= 64
            && self
                .term
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || b"-_.+".contains(&byte))
    }
}

/// What a client asked a channel to run.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Wanted {
    /// A login shell.
    Shell,
    /// A command, for the account's shell to run.
    Command(Vec<u8>),
    /// A subsystem, by its name.
    Subsystem(String),
}

/// A login that the check let through, and the account it is for. Only [`Keeper::login`]
/// makes one.
#[derive(Debug)]
pub struct Admitted {
    _checked: Checked<Login>,
    user: User,
}

impl Admitted {
    /// The account.
    #[must_use]
    pub const fn user(&self) -> &User {
        &self.user
    }
}

/// A session's input and output, as the descriptors of whoever carries them.
#[derive(Debug)]
pub enum Io {
    /// A terminal: this end of it.
    Terminal(OwnedFd),
    /// Three pipes.
    Pipes {
        /// What is written here is the session's input.
        input: OwnedFd,
        /// The session's output is read here.
        output: OwnedFd,
        /// And its errors here.
        errors: OwnedFd,
    },
}

/// A session that was started.
#[derive(Debug)]
pub struct Started {
    /// Its input and output.
    pub io: Io,
    /// A socket that says how it ended: four bytes, the exit code, when it has. Closing it
    /// ends the session.
    pub status: OwnedFd,
}

pub(crate) fn winsize(cols: u32, rows: u32) -> Winsize {
    Winsize {
        ws_row: rows.min(u32::from(u16::MAX)) as u16,
        ws_col: cols.min(u32::from(u16::MAX)) as u16,
        ws_xpixel: 0,
        ws_ypixel: 0,
    }
}

/// A terminal: the server's end, and the end the session gets.
fn open_terminal(terminal: &Terminal) -> std::io::Result<(OwnedFd, OwnedFd)> {
    let ours = openpt(OpenptFlags::RDWR | OpenptFlags::NOCTTY | OpenptFlags::CLOEXEC)?;
    grantpt(&ours)?;
    unlockpt(&ours)?;
    let name = ptsname(&ours, Vec::new())?;
    tcsetwinsize(&ours, winsize(terminal.cols, terminal.rows))?;
    // Not this process's terminal: the helper takes it as its own.
    let theirs = rustix::fs::open(
        name.as_c_str(),
        OFlags::RDWR | OFlags::NOCTTY | OFlags::CLOEXEC,
        Mode::empty(),
    )?;
    Ok((ours, theirs))
}

/// Waits for the session's process and says how it ended on `status`; if `status` is
/// closed first, ends the process.
fn watch(mut child: std::process::Child, status: OwnedFd) {
    use rustix::event::{PollFd, PollFlags, poll};
    use std::io::Write;
    use std::os::unix::process::ExitStatusExt;
    let ended = (|| {
        let pid = rustix::process::Pid::from_raw(i32::try_from(child.id()).ok()?)?;
        let process =
            rustix::process::pidfd_open(pid, rustix::process::PidfdFlags::empty()).ok()?;
        loop {
            let mut waiting = [
                PollFd::new(&process, PollFlags::IN),
                PollFd::new(&status, PollFlags::IN),
            ];
            match poll(&mut waiting, None) {
                Ok(_) => {}
                Err(rustix::io::Errno::INTR) => continue,
                Err(_) => return None,
            }
            if !waiting[0].revents().is_empty() {
                return Some(true);
            }
            if !waiting[1].revents().is_empty() {
                // Whoever carried the session is gone, or closed it.
                return Some(false);
            }
        }
    })();
    if ended != Some(true) {
        let _ = child.kill();
    }
    let code = match child.wait() {
        // Ended by a signal: what a shell would report.
        Ok(exit) => exit.code().map_or_else(
            || 128 + exit.signal().unwrap_or(0) as u32,
            |code| code as u32,
        ),
        Err(_) => 255,
    };
    let mut status = std::os::unix::net::UnixStream::from(status);
    let _ = status.write_all(&code.to_le_bytes());
}

/// The keeper of the board's rights for the SSH way in.
#[derive(Debug)]
pub struct Keeper {
    config: SshConfig,
    security: Security,
    keys: SharedKeys,
    local_addr: SocketAddr,
}

impl Keeper {
    /// A keeper deciding logins by the check, with `security` as its switch and `keys` as
    /// its trust store, for the server listening at `local_addr`.
    #[must_use]
    pub const fn new(
        config: SshConfig,
        security: Security,
        keys: SharedKeys,
        local_addr: SocketAddr,
    ) -> Self {
        Self {
            config,
            security,
            keys,
            local_addr,
        }
    }

    /// What it was told.
    #[must_use]
    pub const fn config(&self) -> &SshConfig {
        &self.config
    }

    fn who(&self, key: Option<&[u8; 32]>) -> Principal {
        match key {
            Some(key) => self.keys.lock().principal(key),
            // No key, or one of a kind the trust store does not hold.
            None => Principal::Anonymous,
        }
    }

    /// Whether a login with `key` could be let in at all: asked before a client is put to
    /// proving it holds the key.
    #[must_use]
    pub fn offered(&self, key: Option<&[u8; 32]>) -> bool {
        Check::with_grants(self.security, &self.keys)
            .decide(&self.who(key), Login, &LOGIN)
            .is_ok()
    }

    /// Decides a login as the account `name` by whoever holds `key`, from `peer`. The
    /// check first, then the account.
    #[must_use]
    pub fn login(
        &self,
        name: &str,
        key: Option<&[u8; 32]>,
        peer: Option<SocketAddr>,
    ) -> Option<Admitted> {
        let from = peer.map_or_else(|| "?".to_owned(), |peer| peer.to_string());
        let how = if key.is_some() { "its key" } else { "no key" };
        let check = Check::with_grants(self.security, &self.keys);
        let checked = match check.decide(&self.who(key), Login, &LOGIN) {
            Ok(checked) => checked,
            // Every client first asks with no key, to learn what is taken: not news.
            Err(refused) if key.is_none() => {
                tracing::debug!("ssh: {from} was not let in as {name} with {how}: {refused}");
                return None;
            }
            Err(refused) => {
                info!("ssh: {from} was not let in as {name} with {how}: {refused}");
                return None;
            }
        };
        let Some(user) = users::lookup(&self.config.etc, name).or_else(|| self.root_anyway(name))
        else {
            info!("ssh: {from} asked for {name}, which is not an account here");
            return None;
        };
        let me = rustix::process::getuid();
        if !me.is_root() && me.as_raw() != user.uid {
            warn!(
                "ssh: {from} asked for {name}, but the comm core runs as user {} and can only \
                 let that user in",
                me.as_raw()
            );
            return None;
        }
        info!("ssh: {from} logged in as {name} with {how}");
        Some(Admitted {
            _checked: checked,
            user,
        })
    }

    /// For the recovery way in: root, when the board's account files do not say who root
    /// is and this process is root.
    fn root_anyway(&self, name: &str) -> Option<User> {
        (self.config.recovery && name == "root" && rustix::process::getuid().is_root()).then(|| {
            User {
                name: "root".to_owned(),
                uid: 0,
                gid: 0,
                groups: vec![0],
                home: "/".into(),
                shell: "/bin/sh".into(),
            }
        })
    }

    /// Starts what `wanted` says as the account that was let in, on a terminal if one was
    /// asked for.
    ///
    /// # Errors
    ///
    /// If there is no such subsystem, or the session's process cannot be started.
    pub fn start(
        &self,
        admitted: &Admitted,
        wanted: Wanted,
        terminal: Option<&Terminal>,
        peer: Option<SocketAddr>,
    ) -> std::io::Result<Started> {
        let user = &admitted.user;
        // A subsystem speaks bytes: never on a terminal.
        let terminal = terminal
            .filter(|terminal| !matches!(wanted, Wanted::Subsystem(_)) && terminal.is_named());
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
        if let Some(terminal) = terminal {
            env.push(("TERM".to_owned(), terminal.term.clone()));
        }
        if let Some(peer) = peer {
            let here = self.local_addr;
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
        let (command, program) = match wanted {
            Wanted::Shell => (None, Vec::new()),
            Wanted::Command(command) => {
                use std::os::unix::ffi::OsStringExt;
                (Some(OsString::from_vec(command)), Vec::new())
            }
            Wanted::Subsystem(name) => {
                let subsystem = self
                    .config
                    .subsystems
                    .iter()
                    .find(|subsystem| subsystem.name == name)
                    .ok_or_else(|| std::io::Error::other(format!("no subsystem {name}")))?;
                (None, subsystem.program.clone())
            }
        };
        let launch = Launch {
            uid: user.uid,
            gid: user.gid,
            groups: user.groups.clone(),
            home: user.home.clone(),
            tty: terminal.is_some(),
            env,
            // For the recovery way in, a shell that is not there is not a reason to keep
            // someone out of a board they came to repair.
            shell: if self.config.recovery && !user.shell.exists() {
                "/bin/sh".into()
            } else {
                user.shell.clone()
            },
            command,
            program,
        };
        let mut process = std::process::Command::new(&self.config.helper.program);
        process
            .args(&self.config.helper.args)
            .arg(MODE)
            .args(launch.to_args());
        let io = if let Some(terminal) = terminal {
            let (ours, theirs) = open_terminal(terminal)?;
            process
                .stdin(Stdio::from(theirs.try_clone()?))
                .stdout(Stdio::from(theirs.try_clone()?))
                .stderr(Stdio::from(theirs));
            Io::Terminal(ours)
        } else {
            use rustix::pipe::{PipeFlags, pipe_with};
            let (stdin, input) = pipe_with(PipeFlags::CLOEXEC)?;
            let (output, stdout) = pipe_with(PipeFlags::CLOEXEC)?;
            let (errors, stderr) = pipe_with(PipeFlags::CLOEXEC)?;
            process
                .stdin(Stdio::from(stdin))
                .stdout(Stdio::from(stdout))
                .stderr(Stdio::from(stderr));
            Io::Pipes {
                input,
                output,
                errors,
            }
        };
        let child = process.spawn()?;
        // The session's ends are the session's alone now: when the last program holding
        // them goes, reading this side ends.
        drop(process);
        let (ours, theirs) = rustix::net::socketpair(
            rustix::net::AddressFamily::UNIX,
            rustix::net::SocketType::STREAM,
            rustix::net::SocketFlags::CLOEXEC,
            None,
        )?;
        std::thread::Builder::new()
            .name("ssh-session".to_owned())
            .spawn(move || watch(child, ours))?;
        Ok(Started { io, status: theirs })
    }

    /// Connects from the board to `host` on `port`, for the account that was let in.
    ///
    /// # Errors
    ///
    /// Why not, in words for the log.
    pub fn connect(
        &self,
        admitted: &Admitted,
        host: &str,
        port: u32,
    ) -> Result<std::net::TcpStream, String> {
        let who = &admitted.user.name;
        if !self.config.forward {
            info!("ssh: {who} asked for a connection to {host}:{port}: forwarding is off");
            return Err("forwarding is off".to_owned());
        }
        #[cfg(feature = "forward")]
        {
            match orion_ssh_forward::connect_blocking(host, port) {
                Ok(stream) => {
                    info!("ssh: {who} is connected to {host}:{port} from the board");
                    Ok(stream)
                }
                Err(refused) => {
                    info!("ssh: {who} was not connected to {host}:{port}: {refused}");
                    Err(refused.to_string())
                }
            }
        }
        #[cfg(not(feature = "forward"))]
        Err("this build does not forward".to_owned())
    }
}
