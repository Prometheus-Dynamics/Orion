//! The sockets the clock is served on (`time.serve`): one datagram socket and one listener at
//! the same port, on every interface the address reaches, or a pair kept to each interface
//! `time.interface` names. A pair kept to an interface is opened when the interface is there,
//! and again when it is made again; one that is not there holds up nothing, and is said once,
//! by its name.

use std::io;
use std::net::SocketAddr;
use std::time::Duration;

use rustix::net::{AddressFamily, SocketType, ipproto, sockopt};
use tokio::net::{TcpListener, TcpSocket, UdpSocket};
use tokio::task::JoinHandle;
use tracing::{info, warn};

use crate::sntp;
use crate::sync::ServedState;

/// The two sockets at `addr`, and where they are: the port they got, asked for any. Kept to
/// `interface` if one is named, and then that interface's index too (0 for none).
///
/// # Errors
///
/// What making, keeping or binding either failed with, saying which; an interface that is not
/// there is `ENODEV`.
pub(crate) fn open(
    addr: SocketAddr,
    interface: Option<&str>,
) -> io::Result<(UdpSocket, TcpListener, SocketAddr, u32)> {
    let context = |what: &str, on: SocketAddr, error: io::Error| {
        io::Error::new(error.kind(), format!("time.serve {on} ({what}): {error}"))
    };
    // Both are on one port. Asked for any port, the datagram socket gets one, and the
    // listener needs the same number, which something else may hold: then another is tried.
    let tries = if addr.port() == 0 { 16 } else { 1 };
    let mut last = None;
    for _ in 0..tries {
        let (udp, index) =
            datagram(addr, interface).map_err(|error| context("udp", addr, error))?;
        let on = udp.local_addr().unwrap_or(addr);
        match listener(on, interface) {
            Ok(tcp) => return Ok((udp, tcp, on, index)),
            Err(error) => last = Some(context("tcp", on, error)),
        }
    }
    Err(last.unwrap_or_else(|| io::Error::other("time.serve: no port")))
}

fn datagram(addr: SocketAddr, interface: Option<&str>) -> io::Result<(UdpSocket, u32)> {
    let family = if addr.is_ipv4() {
        AddressFamily::INET
    } else {
        AddressFamily::INET6
    };
    let fd = rustix::net::socket(family, SocketType::DGRAM, Some(ipproto::UDP))?;
    let mut index = 0;
    if let Some(interface) = interface {
        // Before it is kept to it: an interface made again has another index.
        index = rustix::net::netdevice::name_to_index(&fd, interface)?;
        // Each interface's socket is at the same address: told apart by the interface.
        sockopt::set_socket_reuseaddr(&fd, true)?;
        sockopt::set_socket_reuseport(&fd, true)?;
    }
    rustix::net::bind(&fd, &addr)?;
    let socket = std::net::UdpSocket::from(fd);
    socket.set_nonblocking(true)?;
    let socket = UdpSocket::from_std(socket)?;
    if let Some(interface) = interface {
        socket.bind_device(Some(interface.as_bytes()))?;
    }
    Ok((socket, index))
}

fn listener(addr: SocketAddr, interface: Option<&str>) -> io::Result<TcpListener> {
    let socket = if addr.is_ipv4() {
        TcpSocket::new_v4()?
    } else {
        TcpSocket::new_v6()?
    };
    if let Some(interface) = interface {
        socket.set_reuseaddr(true)?;
        socket.bind_device(Some(interface.as_bytes()))?;
    }
    socket.bind(addr)?;
    socket.listen(64)
}

/// Serves the clock on the two sockets, as two tasks.
pub(crate) fn serve(
    udp: UdpSocket,
    tcp: TcpListener,
    served: &ServedState,
    authority: bool,
) -> [JoinHandle<()>; 2] {
    let (for_udp, for_tcp) = (served.clone(), served.clone());
    [
        tokio::task::spawn_local(sntp::serve(
            udp,
            move || for_udp.served(authority),
            sntp::NtpTime::now,
        )),
        tokio::task::spawn_local(sntp::serve_tcp(
            tcp,
            move || for_tcp.served(authority),
            sntp::NtpTime::now,
        )),
    ]
}

/// Whether the interface named `name` is still the one with `index`.
fn still(name: &str, index: u32) -> bool {
    rustix::net::socket(AddressFamily::INET, SocketType::DGRAM, None)
        .and_then(|probe| rustix::net::netdevice::name_to_index(&probe, name))
        .is_ok_and(|now| now == index)
}

/// Serves the clock at `addr` on the interface `name` for as long as this runs: opened when
/// the interface is there, looked at every `retry`, and opened again when it is made again.
pub(crate) async fn keep(
    addr: SocketAddr,
    name: String,
    served: ServedState,
    authority: bool,
    retry: Duration,
) {
    let mut said = false;
    loop {
        match open(addr, Some(&name)) {
            Ok((udp, tcp, on, index)) => {
                info!("serving the clock over SNTP on {name}, at {on}");
                let serving = Serving(serve(udp, tcp, &served, authority));
                while still(&name, index) {
                    tokio::time::sleep(retry).await;
                }
                drop(serving);
                said = false;
            }
            Err(error) => {
                if !std::mem::replace(&mut said, true) {
                    warn!("the clock is not served on {name} yet: {error}. Trying again");
                }
                tokio::time::sleep(retry).await;
            }
        }
    }
}

/// The two tasks of an interface, stopped with it.
struct Serving([JoinHandle<()>; 2]);

impl Drop for Serving {
    fn drop(&mut self) {
        for task in &self.0 {
            task.abort();
        }
    }
}
