//! The SSH way in, with the part that reads what a client sends in a process of its own.
//!
//! An SSH server reads a great deal from someone it does not know yet: a key exchange,
//! names, keys, signatures. In `orion-ssh` alone that reading happens in the comm core.
//! Here each connection gets a child process, and the child does all of it:
//!
//! - **The child has no rights.** Before it reads a byte from the client it becomes an
//!   account that owns nothing (`ssh.sandbox_user`, `nobody`), with no groups, checks that
//!   it cannot go back, and gives up gaining rights by any means. It has no trust store,
//!   and cannot start anything as anyone.
//! - **It asks the keeper.** Whether a key may log in, a session as the account, a
//!   connection from the board: each is a question on a socket to the comm core, in a few
//!   plain values ([`protocol`]). The keeper decides with the same check as ever, and it
//!   answers a session or a connection only after it has itself let that connection log
//!   in. What comes back is yes or no, or descriptors.
//! - **A fault stays in the child.** A child that crashes, hangs or is taken over ends
//!   one connection. The comm core, and every other connection, go on. The keeper holds
//!   the child to the time a login has, whatever the child says.
//! - **What the child does hold** is the board's host key, which it needs to prove to
//!   clients which board this is. Someone who took a child over could pretend to be the
//!   board to a client later. They could not get into the board.

#![cfg_attr(not(test), deny(clippy::unwrap_used))]
#![cfg(target_os = "linux")]

pub mod child;
pub mod monitor;
pub mod protocol;

use std::net::SocketAddr;
use std::os::fd::OwnedFd;
use std::os::unix::net::UnixStream;
use std::process::Stdio;
use std::sync::{Arc, Mutex, PoisonError};

use orion_check::Security;
use orion_keys::SharedKeys;
use orion_ssh::{Keeper, Places, SshConfig, users};
use russh::keys::ssh_key::LineEnding;
use tokio::task::JoinHandle;
use tracing::{debug, warn};

use protocol::Hello;

/// What the sandbox is told, beside what the SSH server is.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SandboxConfig {
    /// `ssh.sandbox_user`: the account children become. It should own nothing.
    pub user: String,
    /// `login.helper`: the login helper that starts the children, and the sessions, when
    /// the comm core is not root. `None`: this process starts them.
    pub login_helper: Option<std::path::PathBuf>,
}

impl Default for SandboxConfig {
    fn default() -> Self {
        Self {
            user: "nobody".to_owned(),
            login_helper: None,
        }
    }
}

struct Shared {
    keeper: Keeper,
    places: Arc<Places>,
    host_key: String,
    become_ids: Option<(u32, u32)>,
    /// The login helper, which starts the children when there is one.
    #[cfg(feature = "login-helper")]
    helper: Option<orion_ssh::login::LoginClient>,
    /// The children there are, by process id.
    children: Mutex<Vec<u32>>,
}

/// The SSH server with a child for each connection. Dropping it stops taking connections.
pub struct SandboxedSsh {
    local_addr: SocketAddr,
    shared: Arc<Shared>,
    task: JoinHandle<()>,
}

impl std::fmt::Debug for SandboxedSsh {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "SandboxedSsh({})", self.local_addr)
    }
}

impl Drop for SandboxedSsh {
    fn drop(&mut self) {
        self.task.abort();
    }
}

/// One connection: starts its child and answers it until it is gone.
fn connection(shared: &Arc<Shared>, client: OwnedFd, peer: SocketAddr) -> std::io::Result<()> {
    let Some((place, waiting)) = shared.places.take() else {
        debug!("ssh: {peer} turned away: too many connections");
        return Ok(());
    };
    let (ours, theirs) = UnixStream::pair()?;
    let config = shared.keeper.config();
    let child = Child::start(shared, config, client, theirs)?;
    let pid = child.id();
    let children = |with: &dyn Fn(&mut Vec<u32>)| {
        with(
            &mut shared
                .children
                .lock()
                .unwrap_or_else(PoisonError::into_inner),
        );
    };
    children(&|children| children.push(pid));
    let hello = Hello {
        host_key: shared.host_key.clone(),
        login_ms: config.login.as_millis() as u64,
        become_ids: shared.become_ids,
    };
    if protocol::send(&ours, &hello, &[]).is_ok() {
        monitor::serve(&shared.keeper, &ours, Some(peer), config.login, move || {
            drop(waiting)
        });
    }
    // Gone, silent too long, or not making sense: whichever, it ends here, and with it
    // every session it carried, whose status sockets close with it.
    child.end();
    children(&|children| children.retain(|child| *child != pid));
    drop(place);
    Ok(())
}

/// A connection's child: started here, or by the login helper and held by its status socket.
enum Child {
    Here(std::process::Child),
    #[cfg(feature = "login-helper")]
    Helper(u32, OwnedFd),
}

impl Child {
    /// The client's connection is the child's input, and the socket to here its output.
    fn start(
        shared: &Shared,
        config: &orion_ssh::SshConfig,
        client: OwnedFd,
        socket: UnixStream,
    ) -> std::io::Result<Self> {
        #[cfg(feature = "login-helper")]
        if let Some(helper) = &shared.helper {
            let (pid, status) = helper.sandbox(client, socket.into())?;
            return Ok(Self::Helper(pid, status));
        }
        #[cfg(not(feature = "login-helper"))]
        let _ = shared;
        std::process::Command::new(&config.helper.program)
            .args(&config.helper.args)
            .arg(child::MODE)
            .stdin(Stdio::from(client))
            .stdout(Stdio::from(OwnedFd::from(socket)))
            .spawn()
            .map(Self::Here)
    }

    fn id(&self) -> u32 {
        match self {
            Self::Here(child) => child.id(),
            #[cfg(feature = "login-helper")]
            Self::Helper(pid, _) => *pid,
        }
    }

    /// Ends it: killed here, or its status socket closed, which the helper ends it for.
    fn end(self) {
        match self {
            Self::Here(mut child) => {
                let _ = child.kill();
                let _ = child.wait();
            }
            #[cfg(feature = "login-helper")]
            Self::Helper(_, status) => drop(status),
        }
    }
}

impl SandboxedSsh {
    /// Starts taking SSH connections, each in a child of its own. Logins are decided
    /// here, by the check, with `security` as its switch and `keys` as its trust store.
    ///
    /// # Errors
    ///
    /// If the address cannot be listened on, or the kernel gives no random bytes for a
    /// host key.
    pub async fn start(
        config: SshConfig,
        sandbox: SandboxConfig,
        security: Security,
        keys: SharedKeys,
    ) -> std::io::Result<Self> {
        let host_key = orion_ssh::hostkey::load_or_make(&config.host_key)
            .map_err(|error| std::io::Error::new(error.kind(), format!("no host key: {error}")))?
            .to_openssh(LineEnding::LF)
            .map_err(std::io::Error::other)?
            .to_string();
        let mut listeners = orion_ssh::Listeners::start(config.listen, &config.interfaces)?;
        let local_addr = listeners.local_addr();
        tracing::info!("ssh: each connection is read by a child; open on {local_addr}");
        // Who a child becomes, if this process may make it anyone. With a login helper the
        // helper makes it its own `ssh.sandbox_user`, before it starts.
        let with_helper = cfg!(feature = "login-helper") && sandbox.login_helper.is_some();
        let become_ids = if with_helper {
            None
        } else if rustix::process::getuid().is_root() {
            let user = users::lookup(&config.etc, &sandbox.user)
                .filter(|user| user.uid != 0)
                .ok_or_else(|| {
                    std::io::Error::other(format!(
                        "ssh.sandbox_user {} is not an account without rights",
                        sandbox.user
                    ))
                })?;
            Some((user.uid, user.gid))
        } else {
            warn!(
                "ssh: the comm core is not root, so the children that read what clients send \
                 run as the same user: apart from it, but with its files"
            );
            None
        };
        let shared = Arc::new(Shared {
            places: Places::new(&config),
            #[cfg(feature = "login-helper")]
            keeper: Keeper::new(config, security, keys, local_addr)
                .with_helper(sandbox.login_helper.clone()),
            #[cfg(not(feature = "login-helper"))]
            keeper: Keeper::new(config, security, keys, local_addr),
            #[cfg(feature = "login-helper")]
            helper: sandbox
                .login_helper
                .clone()
                .map(orion_ssh::login::LoginClient::new),
            host_key,
            become_ids,
            children: Mutex::new(Vec::new()),
        });
        let accepting = shared.clone();
        let task = tokio::spawn(async move {
            loop {
                let Some((stream, peer)) = listeners.accept().await else {
                    return;
                };
                let _ = stream.set_nodelay(true);
                let Ok(client) = stream.into_std().map(OwnedFd::from) else {
                    continue;
                };
                let shared = accepting.clone();
                // A thread for the connection: answering blocks, and one that is stuck
                // holds up nobody else.
                let started = std::thread::Builder::new()
                    .name("ssh-keeper".to_owned())
                    .spawn(move || {
                        if let Err(error) = connection(&shared, client, peer) {
                            warn!("ssh: {peer}: a child could not be started: {error}");
                        }
                    });
                if let Err(error) = started {
                    warn!("ssh: {peer}: no thread for the connection: {error}");
                }
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
        self.shared.places.connections()
    }

    /// The process ids of the children there are now.
    #[must_use]
    pub fn children(&self) -> Vec<u32> {
        self.shared
            .children
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }
}
