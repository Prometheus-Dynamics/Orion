//! Local forwarding (`ssh -L`, `ssh -W`).
//!
//! A client that is logged in asks for a connection from the board to somewhere the board
//! can reach: a service on the board itself, or on a network only the board is on. The
//! board makes the connection and carries its bytes on a channel of the SSH connection,
//! both ways, until either end closes.
//!
//! - **Only for someone who is logged in.** The SSH server asks for nothing here before
//!   that. Whoever may log in may reach what the board can reach, as they could from a
//!   shell.
//! - **The board connects, not the client.** The other end sees a connection from the
//!   board. Nothing listens on the board for it: this is the client's `-L` and `-W`, not
//!   `-R`.
//! - **Bounded.** A connection that is not made in [`CONNECT`] is given up, and a channel
//!   is closed when either end is.

use std::net::SocketAddr;
use std::time::Duration;

use russh::Channel;
use russh::server::Msg;
use tokio::net::TcpStream;
use tokio::task::JoinHandle;

/// How long the board tries to connect.
pub const CONNECT: Duration = Duration::from_secs(10);

/// Why a connection was not made.
#[derive(Debug)]
pub enum Refused {
    /// The port is not one.
    Port(u32),
    /// The name is of nowhere.
    Unknown(String),
    /// Nothing answered, or it said no.
    Connect(std::io::Error),
}

impl std::fmt::Display for Refused {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Port(port) => write!(f, "{port} is not a port"),
            Self::Unknown(host) => write!(f, "{host} is not known"),
            Self::Connect(error) => write!(f, "{error}"),
        }
    }
}

impl std::error::Error for Refused {}

/// Connects from the board to `host`, a name or an address, on `port`. Each address the
/// name has is tried in turn.
///
/// # Errors
///
/// [`Refused`], within [`CONNECT`].
pub async fn connect(host: &str, port: u32) -> Result<TcpStream, Refused> {
    let port = u16::try_from(port)
        .ok()
        .filter(|port| *port != 0)
        .ok_or(Refused::Port(port))?;
    let attempt = async {
        let addresses: Vec<SocketAddr> = tokio::net::lookup_host((host, port))
            .await
            .map_err(|_| Refused::Unknown(host.to_owned()))?
            .collect();
        let mut last = None;
        for address in addresses {
            match TcpStream::connect(address).await {
                Ok(stream) => return Ok(stream),
                Err(error) => last = Some(error),
            }
        }
        Err(last.map_or_else(|| Refused::Unknown(host.to_owned()), Refused::Connect))
    };
    match tokio::time::timeout(CONNECT, attempt).await {
        Ok(made) => made,
        Err(_) => Err(Refused::Connect(std::io::ErrorKind::TimedOut.into())),
    }
}

/// Carries `stream` on `channel`, both ways, until either end closes. The task ends with
/// how many bytes went to the stream and how many came from it.
pub fn carry(
    channel: Channel<Msg>,
    mut stream: TcpStream,
) -> JoinHandle<std::io::Result<(u64, u64)>> {
    tokio::spawn(async move {
        let _ = stream.set_nodelay(true);
        let mut channel = channel.into_stream();
        tokio::io::copy_bidirectional(&mut channel, &mut stream).await
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn a_connection_is_made_or_refused_with_the_reason() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("listen");
        let port = u32::from(listener.local_addr().expect("addr").port());
        // By address and by name.
        assert!(connect("127.0.0.1", port).await.is_ok());
        assert!(connect("localhost", port).await.is_ok());
        drop(listener);
        // Nothing listening, a port that is not one, a name of nowhere.
        assert!(matches!(
            connect("127.0.0.1", port).await,
            Err(Refused::Connect(_))
        ));
        assert!(matches!(
            connect("127.0.0.1", 0).await,
            Err(Refused::Port(0))
        ));
        assert!(matches!(
            connect("127.0.0.1", 70_000).await,
            Err(Refused::Port(70_000))
        ));
        assert!(matches!(
            connect("no-such-host.invalid", 80).await,
            Err(Refused::Unknown(_))
        ));
    }
}
