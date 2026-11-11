//! What the child may ask the keeper, and what it is answered: a few plain values each
//! way, with descriptors beside the answers that hand something over.

use std::os::fd::{AsFd, OwnedFd};
use std::os::unix::net::UnixStream;

use orion_ssh::{Terminal, Wanted};
use serde::{Deserialize, Serialize};

/// The longest message either way: a command is the longest thing in one.
pub const MAX_MESSAGE: usize = 64 * 1024;

/// What the keeper tells a child before the child reads anything a client sent.
#[derive(Debug, Serialize, Deserialize)]
pub struct Hello {
    /// The board's host key, in OpenSSH's format.
    pub host_key: String,
    /// How long the client has to log in, in milliseconds.
    pub login_ms: u64,
    /// The user and group to become, if the child has the right to become anyone.
    pub become_ids: Option<(u32, u32)>,
}

/// What a child asks.
#[derive(Debug, Serialize, Deserialize)]
pub enum Ask {
    /// Whether a login with this key could be let in at all.
    Offered(Option<[u8; 32]>),
    /// To be let in as this account, holding this key.
    Login(String, Option<[u8; 32]>),
    /// To start a session. A terminal is its name and its size.
    Start(Want, Option<(String, u32, u32)>),
    /// A connection from the board.
    Connect(String, u32),
}

/// What a session is to run.
#[derive(Debug, Serialize, Deserialize)]
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

/// A terminal as it crosses.
pub fn terminal(terminal: Option<(String, u32, u32)>) -> Option<Terminal> {
    terminal.map(|(term, cols, rows)| Terminal { term, cols, rows })
}

/// What a child is answered.
#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Answer {
    /// Yes.
    Yes,
    /// No.
    No,
    /// A session on a terminal: with it come the terminal and the status socket.
    Terminal,
    /// A session on pipes: with it come its input, its output, its errors and the status
    /// socket.
    Pipes,
    /// A connection from the board: with it comes the connection.
    Connected,
    /// It was not done, and why.
    Failed(String),
}

fn broken(why: impl std::fmt::Display) -> std::io::Error {
    std::io::Error::new(std::io::ErrorKind::InvalidData, why.to_string())
}

/// Sends `message` with `fds` beside it.
///
/// # Errors
///
/// If the other end is gone.
pub fn send<T: Serialize>(
    socket: &UnixStream,
    message: &T,
    fds: &[OwnedFd],
) -> std::io::Result<()> {
    let bytes = postcard::to_allocvec(message).map_err(broken)?;
    let fds: Vec<_> = fds.iter().map(AsFd::as_fd).collect();
    orion_fdpass::send_on(socket, &[&bytes], &fds).map_err(|error| broken(format!("{error:?}")))
}

/// Receives one message and what came beside it. `None` when the other end is gone.
///
/// # Errors
///
/// If what came is not a message, or is longer than one may be.
pub fn recv<T: for<'de> Deserialize<'de>>(
    socket: &UnixStream,
) -> std::io::Result<Option<(T, Vec<OwnedFd>)>> {
    let mut buffer = vec![0_u8; MAX_MESSAGE];
    let received = match orion_fdpass::recv(socket.as_fd(), &mut buffer, 4, false) {
        Ok(Some(received)) => received,
        Ok(None) => return Ok(None),
        Err(orion_fdpass::RecvError::Io(error)) => return Err(error),
        Err(error) => return Err(broken(format!("{error:?}"))),
    };
    let message = postcard::from_bytes(&buffer[..received.len]).map_err(broken)?;
    let mut fds = received.fds;
    let fds = (0..fds.len()).filter_map(|index| fds.take(index)).collect();
    Ok(Some((message, fds)))
}
