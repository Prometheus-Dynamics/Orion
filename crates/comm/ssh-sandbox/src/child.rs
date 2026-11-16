//! The child: one process for one connection. It reads everything the client sends, with
//! no rights of its own, and asks the keeper for what needs any.

use std::os::fd::OwnedFd;
use std::os::unix::net::UnixStream;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use orion_ssh::{Door, Io, Started, Terminal, Wanted};
use rustix::process::{getgid, getuid};
use rustix::thread::{Gid, Uid};

use crate::protocol::{Answer, Ask, Hello, recv, send};

/// The word that says a command line is for the child.
pub const MODE: &str = orion_ssh::SANDBOX_MODE;

/// The door from the child: a socket to the keeper. One question at a time.
#[derive(Clone, Debug)]
pub struct Remote(Arc<Mutex<UnixStream>>);

impl Remote {
    fn ask(&self, ask: &Ask) -> std::io::Result<(Answer, Vec<OwnedFd>)> {
        let socket = self.0.lock().unwrap_or_else(PoisonError::into_inner);
        send(&socket, ask, &[])?;
        recv(&socket)?.ok_or_else(|| std::io::ErrorKind::UnexpectedEof.into())
    }

    /// Asks from a thread that may block.
    async fn asked(&self, ask: Ask) -> std::io::Result<(Answer, Vec<OwnedFd>)> {
        let door = self.clone();
        tokio::task::spawn_blocking(move || door.ask(&ask))
            .await
            .map_err(std::io::Error::other)?
    }
}

impl Door for Remote {
    async fn offered(&self, key: Option<[u8; 32]>) -> bool {
        matches!(self.asked(Ask::Offered(key)).await, Ok((Answer::Yes, _)))
    }

    async fn login(&self, name: &str, key: Option<[u8; 32]>) -> bool {
        matches!(
            self.asked(Ask::Login(name.to_owned(), key)).await,
            Ok((Answer::Yes, _))
        )
    }

    async fn start(&self, wanted: Wanted, terminal: Option<Terminal>) -> std::io::Result<Started> {
        let terminal = terminal.map(|terminal| (terminal.term, terminal.cols, terminal.rows));
        let (answer, fds) = self.asked(Ask::Start(wanted.into(), terminal)).await?;
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
            Answer::Failed(why) => return Err(std::io::Error::other(why)),
            _ => return Err(std::io::Error::other("the session was not started")),
        };
        Ok(Started {
            io,
            status: next()?,
        })
    }

    async fn connect(&self, host: &str, port: u32) -> Result<std::net::TcpStream, String> {
        match self.asked(Ask::Connect(host.to_owned(), port)).await {
            Ok((Answer::Connected, fds)) => fds
                .into_iter()
                .next()
                .map(std::net::TcpStream::from)
                .ok_or_else(|| "the connection is missing".to_owned()),
            Ok((Answer::Failed(why), _)) => Err(why),
            Ok(_) => Err("not connected".to_owned()),
            Err(error) => Err(error.to_string()),
        }
    }
}

/// Empties this process's bounding set and checks that it is empty. Each capability up to
/// the last the kernel knows: past that the kernel says the number is not one.
fn empty_bounding_set() -> Result<(), String> {
    use rustix::thread::{CapabilitySet, capability_is_in_bounding_set};
    let each = (0..64).map(|bit| CapabilitySet::from_bits_retain(1 << bit));
    for capability in each.clone() {
        match rustix::thread::remove_capability_from_bounding_set(capability) {
            Ok(()) => {}
            Err(rustix::io::Errno::INVAL) => break,
            Err(error) => return Err(format!("bounding set: {error}")),
        }
    }
    for capability in each {
        match capability_is_in_bounding_set(capability) {
            Ok(false) => {}
            Ok(true) => return Err(format!("{capability:?} is still in the bounding set")),
            Err(_) => break,
        }
    }
    Ok(())
}

/// Who the login helper named for this child to become, if it named anyone: `--become
/// uid:gid` after the mode word.
fn named_by_helper() -> Result<Option<(u32, u32)>, String> {
    let mut args = std::env::args().skip_while(|arg| arg != MODE).skip(1);
    match args.next() {
        None => Ok(None),
        Some(flag) if flag == orion_ssh::SANDBOX_BECOME => {
            let ids = args.next().unwrap_or_default();
            let (uid, gid) = ids
                .split_once(':')
                .and_then(|(uid, gid)| Some((uid.parse().ok()?, gid.parse().ok()?)))
                .ok_or_else(|| format!("{flag} needs uid:gid, not {ids:?}"))?;
            Ok(Some((uid, gid)))
        }
        Some(other) => Err(format!("unknown argument {other}")),
    }
}

/// Gives up every right this process has that it does not need, and checks that they are
/// gone. Called before a byte from the client is read.
fn give_up_rights(become_ids: Option<(u32, u32)>) -> Result<(), String> {
    if getuid().is_root() {
        let (uid, gid) = become_ids.ok_or("the keeper named nobody to become")?;
        let (uid, gid) = (Uid::from_raw(uid), Gid::from_raw(gid));
        if uid.is_root() {
            return Err("the keeper named root".to_owned());
        }
        // Nothing this process or what it starts could ever hold: only root may empty it.
        empty_bounding_set()?;
        // One thread so far, so what is set for the thread is set for the process.
        rustix::thread::set_thread_groups(&[]).map_err(|error| format!("groups: {error}"))?;
        rustix::thread::set_thread_res_gid(gid, gid, gid)
            .map_err(|error| format!("group: {error}"))?;
        rustix::thread::set_thread_res_uid(uid, uid, uid)
            .map_err(|error| format!("user: {error}"))?;
        if getuid() != uid || getgid() != gid || rustix::process::geteuid() != uid {
            return Err("the process is not the user it was to become".to_owned());
        }
        if rustix::thread::set_thread_res_uid(Uid::ROOT, Uid::ROOT, Uid::ROOT).is_ok() {
            return Err("the process could become root again".to_owned());
        }
    }
    // Nothing this process starts may gain rights it does not have, by any means.
    rustix::thread::set_no_new_privs(true)
        .map_err(|error| format!("no new privileges: {error}"))?;
    let _ = rustix::process::chdir("/");
    Ok(())
}

/// The child's whole program, for a binary to call when its first argument is [`MODE`].
/// Its input is the client's connection and its output is the socket to the keeper.
/// Returns the exit code.
#[must_use]
pub fn main() -> i32 {
    let fail = |why: String| {
        eprintln!("orion: an SSH connection could not be served: {why}");
        1
    };
    let (client, keeper) = match (
        rustix::stdio::stdin().try_clone_to_owned(),
        rustix::stdio::stdout().try_clone_to_owned(),
    ) {
        (Ok(client), Ok(keeper)) => (client, UnixStream::from(keeper)),
        _ => return fail("it was not given its two sockets".to_owned()),
    };
    // Named by the login helper: who to become, before anything is read, even the keeper.
    let named = match named_by_helper() {
        Ok(named) => named,
        Err(why) => return fail(why),
    };
    if named.is_some()
        && let Err(why) = give_up_rights(named)
    {
        return fail(why);
    }
    // From the keeper, which is trusted: the client has not been read yet.
    let hello: Hello = match recv(&keeper) {
        Ok(Some((hello, _))) => hello,
        _ => return fail("the keeper said nothing".to_owned()),
    };
    if named.is_none()
        && let Err(why) = give_up_rights(hello.become_ids)
    {
        return fail(why);
    }
    let Ok(host_key) = russh::keys::PrivateKey::from_openssh(&hello.host_key) else {
        return fail("the host key is not a key".to_owned());
    };
    let runtime = match tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
    {
        Ok(runtime) => runtime,
        Err(error) => return fail(error.to_string()),
    };
    runtime.block_on(async {
        let client = std::net::TcpStream::from(client);
        let made = client
            .set_nonblocking(true)
            .and_then(|()| tokio::net::TcpStream::from_std(client));
        let Ok(client) = made else {
            return;
        };
        let door = Remote(Arc::new(Mutex::new(keeper)));
        orion_ssh::serve(
            client,
            door,
            orion_ssh::protocol(host_key),
            Duration::from_millis(hello.login_ms),
            || {},
        )
        .await;
    });
    0
}
