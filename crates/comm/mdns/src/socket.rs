//! A socket on each interface, and which interfaces there are.
//!
//! Each socket takes port 5353 with others (the board may run another responder), joins the
//! group on its interface by index, sends to the group through it, and is kept to it by name,
//! so it hears only what came in on it. An interface that goes, or is made again under its
//! name with another index, is seen at the next look and its socket opened again.

use std::io;
use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};

use rustix::net::{AddressFamily, SocketType, ipproto, sockopt};
use tokio::net::UdpSocket;

use crate::respond::{Interface, PORT};

/// The mDNS group, IPv4.
pub const GROUP: Ipv4Addr = Ipv4Addr::new(224, 0, 0, 251);

/// The interfaces that are up, not loopback, with an IPv4 address: of those `only` names, if
/// it names any.
///
/// # Errors
///
/// If they could not be read.
pub fn interfaces(only: &[String]) -> io::Result<Vec<Interface>> {
    let mut seen: Vec<Interface> = Vec::new();
    for each in if_addrs::get_if_addrs()? {
        let Some(index) = each.index else { continue };
        if each.is_loopback()
            || each.oper_status == if_addrs::IfOperStatus::Down
            || (!only.is_empty() && !only.contains(&each.name))
        {
            continue;
        }
        let at = match seen
            .iter()
            .position(|s| s.index == index && s.name == each.name)
        {
            Some(at) => at,
            None => {
                seen.push(Interface {
                    name: each.name.clone(),
                    index,
                    v4: Vec::new(),
                    v6: Vec::new(),
                });
                seen.len() - 1
            }
        };
        match each.ip() {
            std::net::IpAddr::V4(ip) => seen[at].v4.push(ip),
            std::net::IpAddr::V6(ip) => seen[at].v6.push(ip),
        }
    }
    seen.retain(|interface| !interface.v4.is_empty());
    Ok(seen)
}

/// A socket on interface `name`, whose index is `index`.
///
/// # Errors
///
/// What making it failed with.
pub fn open(name: &str, index: u32) -> io::Result<UdpSocket> {
    let fd = rustix::net::socket(AddressFamily::INET, SocketType::DGRAM, Some(ipproto::UDP))?;
    sockopt::set_socket_reuseaddr(&fd, true)?;
    sockopt::set_socket_reuseport(&fd, true)?;
    rustix::net::bind(&fd, &SocketAddrV4::new(Ipv4Addr::UNSPECIFIED, PORT))?;
    sockopt::set_ip_add_membership_with_ifindex(&fd, &GROUP, &Ipv4Addr::UNSPECIFIED, index)?;
    sockopt::set_ip_multicast_if_with_ifindex(&fd, &GROUP, &Ipv4Addr::UNSPECIFIED, index)?;
    // RFC 6762 §11: 255, so a receiver can tell it came from the link.
    sockopt::set_ip_multicast_ttl(&fd, 255)?;
    sockopt::set_ip_multicast_loop(&fd, true)?;
    let socket = std::net::UdpSocket::from(fd);
    socket.set_nonblocking(true)?;
    let socket = UdpSocket::from_std(socket)?;
    socket.bind_device(Some(name.as_bytes()))?;
    Ok(socket)
}

/// Where a message to the group goes.
#[must_use]
pub fn group() -> SocketAddr {
    SocketAddr::V4(SocketAddrV4::new(GROUP, PORT))
}
