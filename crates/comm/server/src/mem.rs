//! A carrier in memory: connections between tasks of one thread. For tests of the server and
//! of every service built on it, and for a program that embeds a client.
//!
//! A connection holds a bounded number of bytes each way, as a socket does: a side that does
//! not read makes the other side's sends wait.

use std::cell::RefCell;
use std::collections::VecDeque;
use std::io;
use std::os::fd::OwnedFd;
use std::rc::Rc;

use orion_check::Principal;
use orion_msg_session::Roles;
use shm_fdpass::FdSet;
use tokio::sync::Notify;

use crate::{Carrier, Connection, FrameRead, FrameWrite};

/// Frames going one way.
#[derive(Debug)]
struct Pipe {
    state: RefCell<PipeState>,
    readable: Notify,
    writable: Notify,
}

#[derive(Debug)]
struct PipeState {
    bytes: VecDeque<u8>,
    frames: VecDeque<(u32, u8)>,
    fds: VecDeque<OwnedFd>,
    capacity: usize,
    closed: bool,
}

impl Pipe {
    fn new(capacity: usize) -> Rc<Self> {
        Rc::new(Self {
            state: RefCell::new(PipeState {
                bytes: VecDeque::with_capacity(capacity),
                frames: VecDeque::new(),
                fds: VecDeque::new(),
                capacity,
                closed: false,
            }),
            readable: Notify::new(),
            writable: Notify::new(),
        })
    }

    fn close(&self) {
        self.state.borrow_mut().closed = true;
        self.readable.notify_one();
        self.writable.notify_one();
    }
}

/// The reading half of a [`MemConnection`].
#[derive(Debug)]
pub struct MemRead(Rc<Pipe>);

/// The writing half of a [`MemConnection`].
#[derive(Debug)]
pub struct MemWrite(Rc<Pipe>);

impl Drop for MemRead {
    fn drop(&mut self) {
        self.0.close();
    }
}

impl Drop for MemWrite {
    fn drop(&mut self) {
        self.0.close();
    }
}

impl FrameRead for MemRead {
    async fn recv(&mut self, buf: &mut [u8], fds: &mut FdSet) -> io::Result<Option<usize>> {
        loop {
            {
                let mut state = self.0.state.borrow_mut();
                if let Some((len, count)) = state.frames.pop_front() {
                    let len = len as usize;
                    let Some(out) = buf.get_mut(..len) else {
                        return Err(io::Error::new(
                            io::ErrorKind::InvalidData,
                            "frame too large",
                        ));
                    };
                    for (byte, out) in state.bytes.drain(..len).zip(out) {
                        *out = byte;
                    }
                    for _ in 0..count {
                        if let Some(fd) = state.fds.pop_front() {
                            fds.push(fd);
                        }
                    }
                    self.0.writable.notify_one();
                    return Ok(Some(len));
                }
                if state.closed {
                    return Ok(None);
                }
            }
            self.0.readable.notified().await;
        }
    }
}

impl FrameWrite for MemWrite {
    async fn send(&mut self, frame: &[u8], fds: &[OwnedFd]) -> io::Result<()> {
        loop {
            {
                let mut state = self.0.state.borrow_mut();
                if state.closed {
                    return Err(io::ErrorKind::BrokenPipe.into());
                }
                if frame.len() > state.capacity {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "frame too large",
                    ));
                }
                if state.bytes.len() + frame.len() <= state.capacity {
                    state.bytes.extend(frame);
                    for fd in fds {
                        state.fds.push_back(fd.try_clone()?);
                    }
                    state
                        .frames
                        .push_back((frame.len() as u32, fds.len() as u8));
                    self.0.readable.notify_one();
                    return Ok(());
                }
            }
            self.0.writable.notified().await;
        }
    }
}

/// One end of a connection in memory.
#[derive(Debug)]
pub struct MemConnection {
    read: MemRead,
    write: MemWrite,
    principal: Principal,
    offers: Roles,
    descriptors: bool,
}

impl MemConnection {
    /// Sends one frame from this end, as a client would.
    ///
    /// # Errors
    ///
    /// If the other end is gone.
    pub async fn send(&mut self, frame: &[u8], fds: &[OwnedFd]) -> io::Result<()> {
        self.write.send(frame, fds).await
    }

    /// Receives one frame at this end, as a client would.
    ///
    /// # Errors
    ///
    /// If the frame doesn't fit `buf`.
    pub async fn recv(&mut self, buf: &mut [u8], fds: &mut FdSet) -> io::Result<Option<usize>> {
        self.read.recv(buf, fds).await
    }
}

impl Connection for MemConnection {
    type Read = MemRead;
    type Write = MemWrite;

    fn principal(&self) -> Principal {
        self.principal.clone()
    }

    fn offers(&self) -> Roles {
        self.offers
    }

    fn passes_descriptors(&self) -> bool {
        self.descriptors
    }

    fn split(self) -> (MemRead, MemWrite) {
        (self.read, self.write)
    }
}

/// Two ends of one connection, each holding up to `capacity` bytes for the other. The first
/// end is the one to hand to the server: `principal` is who the server is told the second end
/// is, and `offers` the roles it may ask for.
#[must_use]
pub fn pair(
    principal: Principal,
    offers: Roles,
    capacity: usize,
) -> (MemConnection, MemConnection) {
    let (to_server, to_client) = (Pipe::new(capacity), Pipe::new(capacity));
    let server = MemConnection {
        read: MemRead(to_server.clone()),
        write: MemWrite(to_client.clone()),
        principal,
        offers,
        descriptors: true,
    };
    let client = MemConnection {
        read: MemRead(to_client),
        write: MemWrite(to_server),
        principal: Principal::Anonymous,
        offers: Roles(0),
        descriptors: true,
    };
    (server, client)
}

#[derive(Debug, Default)]
struct Waiting {
    connections: RefCell<VecDeque<MemConnection>>,
    arrived: Notify,
}

/// A carrier whose connections come from a [`MemConnector`].
#[derive(Debug)]
pub struct MemCarrier(Rc<Waiting>);

/// Makes connections to a [`MemCarrier`].
#[derive(Clone, Debug)]
pub struct MemConnector {
    waiting: Rc<Waiting>,
    offers: Roles,
    capacity: usize,
}

/// A carrier in memory and the way to connect to it.
#[must_use]
pub fn carrier(offers: Roles, capacity: usize) -> (MemCarrier, MemConnector) {
    let waiting = Rc::new(Waiting::default());
    let connector = MemConnector {
        waiting: waiting.clone(),
        offers,
        capacity,
    };
    (MemCarrier(waiting), connector)
}

impl MemConnector {
    /// Connects as `principal` over a connection that cannot carry descriptors, as a network
    /// or a link cannot, and returns the client's end.
    #[must_use]
    pub fn connect_without_descriptors(&self, principal: Principal) -> MemConnection {
        let (mut server, client) = pair(principal, self.offers, self.capacity);
        server.descriptors = false;
        self.waiting.connections.borrow_mut().push_back(server);
        self.waiting.arrived.notify_one();
        client
    }

    /// Connects as `principal` and returns the client's end.
    #[must_use]
    pub fn connect(&self, principal: Principal) -> MemConnection {
        let (server, client) = pair(principal, self.offers, self.capacity);
        self.waiting.connections.borrow_mut().push_back(server);
        self.waiting.arrived.notify_one();
        client
    }
}

impl Carrier for MemCarrier {
    type Connection = MemConnection;

    async fn accept(&mut self) -> io::Result<MemConnection> {
        loop {
            if let Some(connection) = self.0.connections.borrow_mut().pop_front() {
                return Ok(connection);
            }
            self.0.arrived.notified().await;
        }
    }
}
