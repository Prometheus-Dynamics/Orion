//! The carrier over a CAN bus: one socket, many devices, a session each.

use std::cell::{Cell, RefCell};
use std::collections::VecDeque;
use std::future::Future;
use std::io;
use std::os::fd::OwnedFd;
use std::rc::Rc;
use std::time::{Duration, Instant};

use orion_server::{Carrier, Connection, FdSet, FrameRead, FrameWrite, Principal, Roles};
use orion_supervise::{Decision, Path, Report, Status, Supervisor};
use orion_types::DeviceName;
use orion_wire::frame;
use orion_wire::{CanLinkIds, Reassembler, SegmentMtu, Segmenter};
use tokio::sync::Notify;
use tracing::{info, warn};

/// The largest frame of the wire a CAN link carries.
pub const MAX_FRAME: usize = 4096;
/// How many bytes of whole frames wait for a device's session to read them. More are dropped:
/// the wire's own retries cover a loss, and one slow session must not hold the bus.
const QUEUE: usize = 4 * MAX_FRAME;

/// One CAN or CAN FD data frame.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CanFrame {
    /// The identifier, without flags.
    pub id: u32,
    /// Whether it is a 29-bit identifier.
    pub extended: bool,
    /// How many bytes of `data` are the frame's.
    pub len: u8,
    /// The data.
    pub data: [u8; 64],
}

impl CanFrame {
    /// The frame's data.
    #[must_use]
    pub fn bytes(&self) -> &[u8] {
        &self.data[..usize::from(self.len).min(64)]
    }
}

/// A CAN socket.
pub trait CanIo {
    /// The next data frame on the bus. Remote and error frames are not returned. Dropping the
    /// future must lose no frame.
    fn recv(&self) -> impl Future<Output = io::Result<CanFrame>>;
    /// Sends `frame`, waiting while the transmit queue is full.
    fn send(&self, frame: &CanFrame) -> impl Future<Output = io::Result<()>>;
}

/// Something that can open a CAN bus, again and again.
pub trait OpenCan {
    /// What it opens.
    type Io: CanIo + 'static;
    /// Opens the bus, to receive the identifiers in `ids`.
    fn open(&mut self, ids: &[CanLinkIds]) -> impl Future<Output = io::Result<Self::Io>>;
    /// What is being opened, for the log.
    fn describe(&self) -> String;
}

/// A bus and who is on it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CanBus {
    /// The bus's name. A device on it is `<name>.<address>`.
    pub name: String,
    /// The identifiers of address 0. A device's are these plus its address.
    pub base: CanLinkIds,
    /// The addresses that may be on this bus.
    pub addresses: Vec<u32>,
    /// CAN FD frames, up to 64 bytes, instead of classic CAN's 8.
    pub fd: bool,
}

/// What a bus has seen.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct BusStats {
    /// Times the bus was opened.
    pub opens: u64,
    /// CAN frames received for one of the bus's devices.
    pub segments_rx: u64,
    /// CAN frames sent.
    pub segments_tx: u64,
    /// Whole valid frames of the wire received.
    pub frames_rx: u64,
    /// Messages dropped in reassembly: a lost or repeated segment, a bad checksum, too long.
    pub broken: u64,
    /// Whole frames dropped because the device's session was not reading.
    pub overrun: u64,
    /// CAN frames with an identifier that is nobody's on this bus.
    pub foreign: u64,
    /// Connections handed to the server.
    pub connections: u64,
}

/// One device on the bus.
struct Slot {
    name: DeviceName,
    ids: CanLinkIds,
    reassembler: Box<Reassembler<MAX_FRAME>>,
    /// Whether a session has this device.
    connected: bool,
    /// The bus went away under the session.
    lost: bool,
    /// Whole frames waiting to be read, each as a length and its bytes.
    queue: VecDeque<u8>,
    arrived: Rc<Notify>,
}

struct Shared<I> {
    io: RefCell<Option<Rc<I>>>,
    slots: RefCell<Vec<Slot>>,
    /// The socket failed and must be opened again.
    failed: Cell<bool>,
    /// Wakes the carrier when a writer finds the socket failed.
    broke: Notify,
    stats: Cell<BusStats>,
    mtu: SegmentMtu,
}

impl<I> Shared<I> {
    fn count(&self, change: impl FnOnce(&mut BusStats)) {
        let mut stats = self.stats.get();
        change(&mut stats);
        self.stats.set(stats);
    }
}

/// A CAN bus as a carrier of the one wire.
pub struct CanCarrier<O: OpenCan> {
    open: O,
    name: String,
    ids: Vec<CanLinkIds>,
    shared: Rc<Shared<O::Io>>,
    supervisor: Supervisor,
    started: Instant,
    report: Option<Box<dyn Report>>,
    /// Where a completed frame is put back together.
    scratch: Vec<u8>,
}

impl<O: OpenCan> std::fmt::Debug for CanCarrier<O> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "CanCarrier({})", self.open.describe())
    }
}

impl<O: OpenCan> CanCarrier<O> {
    /// The bus `bus`, over what `open` opens.
    ///
    /// # Errors
    ///
    /// If an address gives identifiers outside the range or two that collide, or a device name
    /// that is not one.
    pub fn new(bus: &CanBus, open: O) -> Result<Self, String> {
        let mut slots = Vec::with_capacity(bus.addresses.len());
        let mut taken: Vec<u32> = Vec::new();
        for &address in &bus.addresses {
            let ids = CanLinkIds::for_address(bus.base, address).ok_or_else(|| {
                format!("address {address} has no valid identifiers on {}", bus.name)
            })?;
            if taken.contains(&ids.device_to_host) || taken.contains(&ids.host_to_device) {
                return Err(format!(
                    "address {address} shares an identifier with another on {}",
                    bus.name
                ));
            }
            taken.extend([ids.device_to_host, ids.host_to_device]);
            slots.push(Slot {
                name: format!("{}.{address}", bus.name)
                    .parse()
                    .map_err(|error| format!("{}.{address}: {error}", bus.name))?,
                ids,
                reassembler: Box::new(Reassembler::new()),
                connected: false,
                lost: false,
                queue: VecDeque::with_capacity(QUEUE),
                arrived: Rc::new(Notify::new()),
            });
        }
        Ok(Self {
            open,
            name: bus.name.clone(),
            ids: slots.iter().map(|slot| slot.ids).collect(),
            shared: Rc::new(Shared {
                io: RefCell::new(None),
                slots: RefCell::new(slots),
                failed: Cell::new(false),
                broke: Notify::new(),
                stats: Cell::new(BusStats::default()),
                mtu: if bus.fd {
                    SegmentMtu::FD
                } else {
                    SegmentMtu::CLASSIC
                },
            }),
            supervisor: Supervisor::new(Path::Access),
            started: Instant::now(),
            report: None,
            scratch: vec![0; MAX_FRAME],
        })
    }

    /// Where the bus's state goes whenever it changes: running, or retrying and when.
    #[must_use]
    pub fn reporting(mut self, report: impl Report + 'static) -> Self {
        self.report = Some(Box::new(report));
        self
    }

    /// A way to read the bus's counters from elsewhere.
    pub fn stats(&self) -> impl Fn() -> BusStats + 'static {
        let shared = self.shared.clone();
        move || shared.stats.get()
    }

    fn tell(&mut self, status: Status) {
        if let Some(report) = &mut self.report {
            report.report(&status);
        }
    }

    /// The open bus, opening it as often as it takes.
    async fn io(&mut self) -> Rc<O::Io> {
        loop {
            if let Some(io) = self.shared.io.borrow().as_ref() {
                return io.clone();
            }
            match self.open.open(&self.ids).await {
                Ok(io) => {
                    info!(bus = %self.name, on = %self.open.describe(), "bus open");
                    self.shared.count(|stats| stats.opens += 1);
                    self.supervisor.on_success();
                    self.tell(self.supervisor.status());
                    *self.shared.io.borrow_mut() = Some(Rc::new(io));
                }
                Err(error) => self.failed("open", &error).await,
            }
        }
    }

    /// Notes a failure and sleeps until the next try is due.
    async fn failed(&mut self, what: &str, error: &io::Error) {
        let now_ms = self.started.elapsed().as_millis() as u64;
        if let Some(suppressed) = self.supervisor.log_failure(now_ms) {
            warn!(bus = %self.name, on = %self.open.describe(), %error, suppressed, "bus {what} failed");
        }
        let decision = self.supervisor.on_failure(now_ms);
        self.tell(self.supervisor.status());
        let wait_ms = match decision {
            Decision::RetryAt(at_ms) => at_ms.saturating_sub(now_ms),
            Decision::StayOff => u64::from(orion_supervise::ACCESS_RETRY_MS),
        };
        tokio::time::sleep(Duration::from_millis(wait_ms)).await;
    }

    /// The bus is gone: every session on it ends, and it is opened again.
    async fn lost(&mut self, error: &io::Error) {
        *self.shared.io.borrow_mut() = None;
        for slot in self.shared.slots.borrow_mut().iter_mut() {
            slot.reassembler.reset();
            if slot.connected {
                slot.lost = true;
                slot.arrived.notify_one();
            }
        }
        self.failed("socket", error).await;
    }

    /// Takes one CAN frame. Returns the device whose first whole frame this completed, if it
    /// has no session yet.
    fn take(&mut self, received: &CanFrame) -> Option<usize> {
        let mut slots = self.shared.slots.borrow_mut();
        let Some((index, slot)) = slots.iter_mut().enumerate().find(|(_, slot)| {
            slot.ids.device_to_host == received.id && slot.ids.extended == received.extended
        }) else {
            self.shared.count(|stats| stats.foreign += 1);
            return None;
        };
        self.shared.count(|stats| stats.segments_rx += 1);
        let len = match slot.reassembler.push(received.bytes()) {
            Ok(Some(view)) => {
                frame::encode(view.header(), view.payload(), &mut self.scratch).ok()?
            }
            Ok(None) => return None,
            Err(_) => {
                self.shared.count(|stats| stats.broken += 1);
                return None;
            }
        };
        self.shared.count(|stats| stats.frames_rx += 1);
        if slot.queue.len() + 2 + len > QUEUE {
            self.shared.count(|stats| stats.overrun += 1);
            return None;
        }
        slot.queue.extend((len as u16).to_le_bytes());
        slot.queue.extend(&self.scratch[..len]);
        if slot.connected {
            slot.arrived.notify_one();
            return None;
        }
        slot.connected = true;
        slot.lost = false;
        Some(index)
    }
}

impl<O: OpenCan> Carrier for CanCarrier<O> {
    type Connection = CanConnection<O::Io>;

    async fn accept(&mut self) -> io::Result<Self::Connection> {
        loop {
            if self.shared.failed.replace(false) {
                self.lost(&io::Error::other("send failed")).await;
            }
            let io = self.io().await;
            let received = tokio::select! {
                received = io.recv() => received,
                () = self.shared.broke.notified() => continue,
            };
            match received {
                Ok(received) => {
                    if let Some(index) = self.take(&received) {
                        self.shared.count(|stats| stats.connections += 1);
                        let slots = self.shared.slots.borrow();
                        return Ok(CanConnection {
                            shared: self.shared.clone(),
                            io,
                            index,
                            name: slots[index].name.clone(),
                            ids: slots[index].ids,
                            arrived: slots[index].arrived.clone(),
                        });
                    }
                }
                Err(error) => self.lost(&error).await,
            }
        }
    }
}

/// One device's connection over the bus.
pub struct CanConnection<I: CanIo> {
    shared: Rc<Shared<I>>,
    io: Rc<I>,
    index: usize,
    name: DeviceName,
    ids: CanLinkIds,
    arrived: Rc<Notify>,
}

impl<I: CanIo> std::fmt::Debug for CanConnection<I> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "CanConnection({})", self.name)
    }
}

impl<I: CanIo + 'static> Connection for CanConnection<I> {
    type Read = CanRead<I>;
    type Write = CanWrite<I>;

    fn principal(&self) -> Principal {
        Principal::Device(self.name.clone())
    }

    fn offers(&self) -> Roles {
        Roles::DEVICE
            .and(Roles::PROVIDER)
            .and(Roles::EXECUTOR)
            .and(Roles::CLIENT)
    }

    fn passes_descriptors(&self) -> bool {
        false
    }

    fn split(self) -> (Self::Read, Self::Write) {
        (
            CanRead {
                shared: self.shared.clone(),
                index: self.index,
                arrived: self.arrived,
            },
            CanWrite {
                shared: self.shared,
                io: self.io,
                ids: self.ids,
            },
        )
    }
}

/// The reading half.
pub struct CanRead<I> {
    shared: Rc<Shared<I>>,
    index: usize,
    arrived: Rc<Notify>,
}

impl<I> std::fmt::Debug for CanRead<I> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("CanRead")
    }
}

impl<I> FrameRead for CanRead<I> {
    async fn recv(&mut self, buf: &mut [u8], _fds: &mut FdSet) -> io::Result<Option<usize>> {
        loop {
            {
                let mut slots = self.shared.slots.borrow_mut();
                let slot = &mut slots[self.index];
                if slot.queue.len() >= 2 {
                    let len = usize::from(u16::from_le_bytes([slot.queue[0], slot.queue[1]]));
                    slot.queue.drain(..2);
                    let Some(to) = buf.get_mut(..len) else {
                        slot.queue.drain(..len);
                        return Err(io::Error::new(
                            io::ErrorKind::InvalidData,
                            "a frame longer than the buffer",
                        ));
                    };
                    for (to, from) in to.iter_mut().zip(slot.queue.drain(..len)) {
                        *to = from;
                    }
                    return Ok(Some(len));
                }
                if slot.lost {
                    return Ok(None);
                }
            }
            self.arrived.notified().await;
        }
    }
}

impl<I> Drop for CanRead<I> {
    fn drop(&mut self) {
        // The device is free for its next session, which starts clean.
        let mut slots = self.shared.slots.borrow_mut();
        let slot = &mut slots[self.index];
        slot.connected = false;
        slot.lost = false;
        slot.queue.clear();
        slot.reassembler.reset();
    }
}

/// The writing half.
pub struct CanWrite<I> {
    shared: Rc<Shared<I>>,
    io: Rc<I>,
    ids: CanLinkIds,
}

impl<I> std::fmt::Debug for CanWrite<I> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("CanWrite")
    }
}

impl<I: CanIo> FrameWrite for CanWrite<I> {
    async fn send(&mut self, frame: &[u8], fds: &[OwnedFd]) -> io::Result<()> {
        if !fds.is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "a link carries no descriptors",
            ));
        }
        let mut segments = Segmenter::for_frame(frame, self.shared.mtu);
        let mut out = CanFrame {
            id: self.ids.host_to_device,
            extended: self.ids.extended,
            len: 0,
            data: [0; 64],
        };
        loop {
            let len = match segments.next_into(&mut out.data) {
                Ok(Some(len)) => len,
                Ok(None) => return Ok(()),
                Err(_) => {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "a frame that cannot be cut",
                    ));
                }
            };
            out.len = len as u8;
            if let Err(error) = self.io.send(&out).await {
                self.shared.failed.set(true);
                self.shared.broke.notify_one();
                return Err(error);
            }
            self.shared.count(|stats| stats.segments_tx += 1);
        }
    }
}
