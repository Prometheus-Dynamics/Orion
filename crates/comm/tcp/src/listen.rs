//! Opening the listening socket: on an address, on one network interface if asked, and again
//! and again until it opens.

use std::io;
use std::net::SocketAddr;

use tokio::net::{TcpListener, TcpSocket};

pub use orion_server::Retry;
use orion_server::open_retrying;

use crate::{TcpCarrier, TcpOptions};

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
        let socket = open_retrying("the network way in", listen, retry, async || {
            listener(listen)
        })
        .await;
        Self::on(socket, options)
    }
}
