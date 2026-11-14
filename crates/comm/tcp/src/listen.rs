//! Opening the listening socket: on an address, on one network interface if asked, and again
//! and again until it opens.

use std::io;
use std::net::SocketAddr;
use std::time::Duration;

use tokio::net::{TcpListener, TcpSocket};

use crate::{TcpCarrier, TcpOptions};

/// How long a way in that could not open waits before it tries again: the first wait, and the
/// most it waits.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Retry {
    /// The first wait.
    pub first: Duration,
    /// The longest wait: each wait is twice the last, up to this.
    pub most: Duration,
}

impl Default for Retry {
    /// Five seconds, then ten.
    fn default() -> Self {
        Self {
            first: Duration::from_secs(5),
            most: Duration::from_secs(10),
        }
    }
}

/// Where a carrier listens: an address, and the one network interface it takes connections
/// from, if it is to take them from one only.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Listen {
    /// The address.
    pub addr: SocketAddr,
    /// The interface, by name (`eth0`, `usb0`). Connections that arrive on any other are not
    /// taken.
    pub interface: Option<String>,
}

impl std::fmt::Display for Listen {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self.interface {
            Some(interface) => write!(f, "{} on {interface}", self.addr),
            None => write!(f, "{}", self.addr),
        }
    }
}

fn listener(listen: &Listen) -> io::Result<TcpListener> {
    let socket = if listen.addr.is_ipv4() {
        TcpSocket::new_v4()?
    } else {
        TcpSocket::new_v6()?
    };
    socket.set_reuseaddr(true)?;
    if let Some(interface) = &listen.interface {
        socket.bind_device(Some(interface.as_bytes()))?;
    }
    socket.bind(listen.addr)?;
    socket.listen(1024)
}

impl TcpCarrier {
    /// Listens as `listen` says. Must be called inside the runtime the carrier will be served
    /// on.
    ///
    /// # Errors
    ///
    /// Anything making the socket, choosing its interface, or `bind` fails with: an interface
    /// that is not there is `ENODEV`.
    pub fn listen(listen: &Listen, options: TcpOptions) -> io::Result<Self> {
        Ok(Self::on(listener(listen)?, options))
    }

    /// Listens as `listen` says, trying again after `retry` for as long as it takes: an
    /// interface not up yet, an address not given to this board yet, a port someone else
    /// still has. What went wrong is logged once, as a warning, and that it opened once it
    /// does.
    pub async fn listen_retrying(listen: &Listen, options: TcpOptions, retry: Retry) -> Self {
        let mut wait = retry.first;
        let mut warned = false;
        loop {
            match listener(listen) {
                Ok(socket) => {
                    if warned {
                        tracing::info!(listen = %listen, "the network way in is open");
                    }
                    return Self::on(socket, options);
                }
                Err(error) if !warned => {
                    tracing::warn!(
                        listen = %listen,
                        %error,
                        "the network way in cannot open: trying again every {}-{} s, \
                         and the rest of the comm core runs meanwhile",
                        retry.first.as_secs(),
                        retry.most.as_secs()
                    );
                    warned = true;
                }
                Err(_) => {}
            }
            tokio::time::sleep(wait).await;
            wait = (wait * 2).min(retry.most);
        }
    }
}
