//! Opening the listening socket: on an address, on one network interface if asked, and again
//! and again until it opens. A socket kept to an interface follows it: one that goes, or is
//! made again under the same name, is listened on again once it is there.

use std::io;
use std::net::SocketAddr;
use std::time::Duration;

use tokio::net::{TcpListener, TcpSocket};
use tokio::time::{Instant, sleep_until};

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

/// A socket kept to an interface, and the interface it was kept to.
///
/// The kernel keeps a socket to an interface by its index, not its name. An interface that
/// goes and is made again under the same name (a USB gadget's network, rebuilt) has a new
/// index, and the socket kept to the old one takes nothing from it, and says nothing. So the
/// index it was kept to is looked at every `retry.first`, and a different one, or none, means
/// listening again.
#[derive(Debug)]
pub(crate) struct Follow {
    listen: Listen,
    name: String,
    index: u32,
    retry: Retry,
    /// When it looks next.
    next: Instant,
}

impl Follow {
    /// Waits until the interface the socket is kept to is no longer the one of that name.
    pub(crate) async fn gone(&mut self, listener: &TcpListener) {
        loop {
            sleep_until(self.next).await;
            self.next = Instant::now() + self.retry.first.max(Duration::from_millis(10));
            if !still_kept(listener, &self.name, self.index) {
                return;
            }
        }
    }

    /// Listens again, for as long as it takes.
    pub(crate) async fn again(&mut self) -> TcpListener {
        tracing::warn!(
            "the network way in on {}: the interface went, or was made again; listening again once it is there",
            self.name
        );
        let (listener, index) = retrying(&self.listen, self.retry).await;
        self.index = index;
        listener
    }
}

/// Opens as `listen` says, trying again after `retry` until it opens.
async fn retrying(listen: &Listen, retry: Retry) -> (TcpListener, u32) {
    open_retrying("the network way in", listen, retry, async || {
        kept(listen.addr, listen.interface.as_deref())
    })
    .await
}

/// A listening socket at `addr`, kept to the network interface named `interface` if there is
/// one, and the index of that interface (0 for none): what [`still_kept`] is asked with.
/// Every way in that listens on an interface listens through this.
///
/// # Errors
///
/// Anything making the socket, choosing its interface, or `bind` fails with: an interface
/// that is not there is `ENODEV`.
pub fn kept(addr: SocketAddr, interface: Option<&str>) -> io::Result<(TcpListener, u32)> {
    let socket = if addr.is_ipv4() {
        TcpSocket::new_v4()?
    } else {
        TcpSocket::new_v6()?
    };
    socket.set_reuseaddr(true)?;
    let mut index = 0;
    if let Some(interface) = interface {
        // Before the socket is kept to it: an interface made again after this has another
        // index, and is seen as such.
        index = rustix::net::netdevice::name_to_index(&socket, interface)?;
        socket.bind_device(Some(interface.as_bytes()))?;
    }
    socket.bind(addr)?;
    Ok((socket.listen(1024)?, index))
}

/// Whether the interface `listener` is kept to is still the one named `name`: the one it had
/// `index` when [`kept`] opened it. One that went, or was made again, is not.
#[must_use]
pub fn still_kept(listener: &TcpListener, name: &str, index: u32) -> bool {
    rustix::net::netdevice::name_to_index(listener, name).is_ok_and(|now| now == index)
}

impl TcpCarrier {
    /// Listens as `listen` says. Must be called inside the runtime the carrier will be served
    /// on. A socket kept to an interface looks every `retry.first` whether the interface is
    /// still the one it was kept to, and listens again as [`TcpCarrier::listen_retrying`] does
    /// when it is not.
    ///
    /// # Errors
    ///
    /// Anything making the socket, choosing its interface, or `bind` fails with: an interface
    /// that is not there is `ENODEV`.
    pub fn listen(listen: &Listen, options: TcpOptions, retry: Retry) -> io::Result<Self> {
        let (socket, index) = kept(listen.addr, listen.interface.as_deref())?;
        Ok(Self::on(socket, options).following(listen, index, retry))
    }

    /// Listens as `listen` says, trying again after `retry` for as long as it takes: an
    /// interface not up yet, an address not given to this board yet, a port someone else
    /// still has. What went wrong is logged once, as a warning, and that it opened once it
    /// does. It follows its interface as [`TcpCarrier::listen`] does.
    pub async fn listen_retrying(listen: &Listen, options: TcpOptions, retry: Retry) -> Self {
        let (socket, index) = retrying(listen, retry).await;
        Self::on(socket, options).following(listen, index, retry)
    }

    fn following(mut self, listen: &Listen, index: u32, retry: Retry) -> Self {
        self.follow = listen.interface.as_ref().map(|name| Follow {
            listen: listen.clone(),
            name: name.clone(),
            index,
            retry,
            next: Instant::now() + retry.first.max(Duration::from_millis(10)),
        });
        self
    }
}
