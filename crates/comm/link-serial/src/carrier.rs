//! The carrier over a byte stream: open, wait for a frame, hand over a connection, and open
//! again when it fails.

use std::cell::{Cell, RefCell};
use std::future::Future;
use std::io;
use std::os::fd::OwnedFd;
use std::rc::Rc;
use std::time::{Duration, Instant};

use orion_keys::SharedKeys;
use orion_net_handshake::Seal;
use orion_server::{Carrier, Connection, FdSet, FrameRead, FrameWrite, Principal, Roles};
use orion_supervise::{Decision, Path, Report, Status, Supervisor};
use orion_types::DeviceName;
use orion_wire::frame;
use orion_wire::{StreamDecoder, StreamEncoder};
use tokio::sync::Notify;
use tracing::{info, warn};

use crate::signed::{self, Frames, Signed};

/// The largest frame a link carries.
pub const MAX_FRAME: usize = 8192;
/// How many bytes are read from, or written to, the stream at a time.
const CHUNK: usize = 1024;

/// Bytes in order, both ways. Hang-up is a read of 0.
pub trait ByteStream {
    /// Waits until bytes are there and reads some. `Ok(0)` means the other end is gone.
    fn read(&self, buf: &mut [u8]) -> impl Future<Output = io::Result<usize>>;
    /// Waits until the stream takes bytes and writes some of `bytes`.
    fn write(&self, bytes: &[u8]) -> impl Future<Output = io::Result<usize>>;
}

/// Something that can open a byte stream, again and again.
pub trait Open {
    /// What it opens.
    type Stream: ByteStream + 'static;
    /// Opens the stream.
    fn open(&mut self) -> impl Future<Output = io::Result<Self::Stream>>;
    /// What is being opened, for the log.
    fn describe(&self) -> String;
}

/// What a link has seen.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct LinkStats {
    /// Times the stream was opened.
    pub opens: u64,
    /// Times a read returned: bytes, hang-up or an error. A silent link does not move this.
    pub reads: u64,
    /// Bytes read.
    pub bytes_rx: u64,
    /// Bytes written.
    pub bytes_tx: u64,
    /// Whole valid frames received.
    pub frames_rx: u64,
    /// Packets dropped: a bad checksum, bad framing, or longer than [`MAX_FRAME`].
    pub dropped: u64,
    /// Connections handed to the server.
    pub connections: u64,
}

/// What reading needs, kept in one allocation that outlives every connection.
struct Reading {
    decoder: StreamDecoder<MAX_FRAME>,
    chunk: [u8; CHUNK],
    at: usize,
    len: usize,
    /// The frame that made the carrier hand over a connection.
    first: Vec<u8>,
}

struct Shared<S> {
    stream: RefCell<Option<Rc<S>>>,
    /// Home of the reading state while no connection has it.
    reading: RefCell<Option<Box<Reading>>>,
    /// The reading state came home.
    home: Notify,
    /// The stream failed and must be opened again.
    failed: Cell<bool>,
    stats: Cell<LinkStats>,
}

impl<S> Shared<S> {
    fn count(&self, change: impl FnOnce(&mut LinkStats)) {
        let mut stats = self.stats.get();
        change(&mut stats);
        self.stats.set(stats);
    }
}

/// Reads until one whole valid frame is in `out`, and returns its length. `None` on hang-up.
async fn next_frame<S: ByteStream>(
    shared: &Shared<S>,
    stream: &S,
    reading: &mut Reading,
    out: &mut [u8],
) -> io::Result<Option<usize>> {
    loop {
        if reading.at == reading.len {
            let read = stream.read(&mut reading.chunk).await;
            shared.count(|stats| stats.reads += 1);
            let len = read?;
            if len == 0 {
                return Ok(None);
            }
            shared.count(|stats| stats.bytes_rx += len as u64);
            (reading.at, reading.len) = (0, len);
        }
        let (used, pushed) = reading
            .decoder
            .push_slice(&reading.chunk[reading.at..reading.len]);
        reading.at += used;
        match pushed {
            Ok(Some(view)) => {
                shared.count(|stats| stats.frames_rx += 1);
                // The decoder gives the frame in parts; put it back together for the server.
                return frame::encode(view.header(), view.payload(), out)
                    .map(Some)
                    .map_err(|_| {
                        io::Error::new(io::ErrorKind::InvalidData, "a frame longer than the buffer")
                    });
            }
            Ok(None) => {}
            // Noise, a damaged frame, one too long: dropped, and the stream goes on at the
            // next delimiter.
            Err(_) => shared.count(|stats| stats.dropped += 1),
        }
    }
}

/// Writes `frame` to the stream, encoded, through `out`.
async fn write_frame<S: ByteStream>(
    shared: &Shared<S>,
    stream: &S,
    out: &mut [u8; CHUNK],
    frame: &[u8],
) -> io::Result<()> {
    // The leading delimiter ends any noise since the last frame, so it cannot spoil this one.
    let mut encoder = StreamEncoder::for_frame(frame).with_leading_delimiter();
    loop {
        let len = encoder.fill(out);
        if len == 0 {
            return Ok(());
        }
        let mut sent = 0;
        while sent < len {
            match stream.write(&out[sent..len]).await {
                Ok(0) => {
                    shared.failed.set(true);
                    return Err(io::ErrorKind::WriteZero.into());
                }
                Ok(wrote) => {
                    sent += wrote;
                    shared.count(|stats| stats.bytes_tx += wrote as u64);
                }
                Err(error) => {
                    shared.failed.set(true);
                    return Err(error);
                }
            }
        }
    }
}

/// The handshake's frames, on the stream itself, before any connection has it.
struct Handshake<'a, S> {
    shared: &'a Shared<S>,
    stream: &'a S,
    reading: &'a mut Reading,
    frame: Vec<u8>,
    out: [u8; CHUNK],
}

impl<S: ByteStream> Frames for Handshake<'_, S> {
    async fn send(&mut self, frame: &[u8]) -> io::Result<()> {
        write_frame(self.shared, self.stream, &mut self.out, frame).await
    }

    async fn next(&mut self) -> io::Result<Option<&[u8]>> {
        let found = next_frame(self.shared, self.stream, self.reading, &mut self.frame).await?;
        Ok(found.map(|len| &self.frame[..len]))
    }
}

/// A link over a byte stream, as a carrier of the one wire.
pub struct StreamCarrier<O: Open> {
    open: O,
    name: DeviceName,
    shared: Rc<Shared<O::Stream>>,
    supervisor: Supervisor,
    started: Instant,
    report: Option<Box<dyn Report>>,
    keys: Option<SharedKeys>,
}

impl<O: Open> std::fmt::Debug for StreamCarrier<O> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "StreamCarrier({})", self.open.describe())
    }
}

impl<O: Open> StreamCarrier<O> {
    /// A link called `name`, which is who the device on it is taken to be, over what `open`
    /// opens.
    pub fn new(name: DeviceName, open: O) -> Self {
        Self {
            open,
            name,
            shared: Rc::new(Shared {
                stream: RefCell::new(None),
                reading: RefCell::new(Some(Box::new(Reading {
                    decoder: StreamDecoder::new(),
                    chunk: [0; CHUNK],
                    at: 0,
                    len: 0,
                    first: vec![0; MAX_FRAME],
                }))),
                home: Notify::new(),
                failed: Cell::new(false),
                stats: Cell::new(LinkStats::default()),
            }),
            supervisor: Supervisor::new(Path::Access),
            started: Instant::now(),
            report: None,
            keys: None,
        }
    }

    /// Takes signed sessions too: a tool that proves a key with the board's `keys` is that key's
    /// operator (or pinned peer) on this link, not its device, and its frames are sealed. Without
    /// this a tool that asks is told the link takes no signed sessions.
    #[must_use]
    pub fn signed(mut self, keys: SharedKeys) -> Self {
        self.keys = Some(keys);
        self
    }

    /// Where the link's state goes whenever it changes: running, or retrying and when.
    #[must_use]
    pub fn reporting(mut self, report: impl Report + 'static) -> Self {
        self.report = Some(Box::new(report));
        self
    }

    /// A way to read the link's counters from elsewhere.
    pub fn stats(&self) -> impl Fn() -> LinkStats + 'static {
        let shared = self.shared.clone();
        move || shared.stats.get()
    }

    fn tell(&mut self, status: Status) {
        if let Some(report) = &mut self.report {
            report.report(&status);
        }
    }

    /// The open stream, opening it as often as it takes.
    async fn stream(&mut self) -> Rc<O::Stream> {
        loop {
            if let Some(stream) = self.shared.stream.borrow().as_ref() {
                return stream.clone();
            }
            match self.open.open().await {
                Ok(stream) => {
                    info!(link = %self.name, port = %self.open.describe(), "link open");
                    self.shared.count(|stats| stats.opens += 1);
                    self.supervisor.on_success();
                    self.tell(self.supervisor.status());
                    *self.shared.stream.borrow_mut() = Some(Rc::new(stream));
                }
                Err(error) => self.failed("open", &error).await,
            }
        }
    }

    /// Notes a failure and sleeps until the next try is due.
    async fn failed(&mut self, what: &str, error: &io::Error) {
        let now_ms = self.started.elapsed().as_millis() as u64;
        if let Some(suppressed) = self.supervisor.log_failure(now_ms) {
            warn!(link = %self.name, port = %self.open.describe(), %error, suppressed, "link {what} failed");
        }
        let decision = self.supervisor.on_failure(now_ms);
        self.tell(self.supervisor.status());
        let wait_ms = match decision {
            Decision::RetryAt(at_ms) => at_ms.saturating_sub(now_ms),
            // A way in is never switched off; were it ever, look again at the usual pace.
            Decision::StayOff => u64::from(orion_supervise::ACCESS_RETRY_MS),
        };
        tokio::time::sleep(Duration::from_millis(wait_ms)).await;
    }
}

impl<O: Open> Carrier for StreamCarrier<O> {
    type Connection = StreamConnection<O::Stream>;

    async fn accept(&mut self) -> io::Result<Self::Connection> {
        loop {
            // One device, one connection: wait for the last one to end.
            let mut reading = loop {
                let home = self.shared.reading.borrow_mut().take();
                match home {
                    Some(reading) => break reading,
                    None => self.shared.home.notified().await,
                }
            };
            if self.shared.failed.replace(false) {
                *self.shared.stream.borrow_mut() = None;
                (reading.at, reading.len) = (0, 0);
                reading.decoder.reset();
                self.failed("stream", &io::Error::other("lost")).await;
            }
            let stream = self.stream().await;
            // Nothing is handed to the server until the other end has said something whole.
            let mut first = std::mem::take(&mut reading.first);
            let found = next_frame(&self.shared, &stream, &mut reading, &mut first).await;
            reading.first = first;
            match found {
                Ok(Some(len)) if signed::asks(&reading.first[..len]) => {
                    let asked = reading.first[..len].to_vec();
                    let mut handshake = Handshake {
                        shared: &self.shared,
                        stream: &stream,
                        reading: &mut reading,
                        frame: vec![0; MAX_FRAME],
                        out: [0; CHUNK],
                    };
                    let welcomed = tokio::time::timeout(
                        signed::HANDSHAKE,
                        signed::welcome(&mut handshake, &asked, self.keys.as_ref()),
                    )
                    .await;
                    match welcomed {
                        Ok(Ok(signed)) => {
                            info!(link = %self.name, principal = ?signed.principal, "a signed session on the link");
                            self.shared.count(|stats| stats.connections += 1);
                            return Ok(StreamConnection {
                                name: self.name.clone(),
                                shared: self.shared.clone(),
                                stream,
                                reading,
                                first: 0,
                                signed: Some(signed),
                            });
                        }
                        Ok(Err(error)) => {
                            info!(link = %self.name, %error, "a signed session was not let in");
                        }
                        Err(_) => info!(link = %self.name, "a signed session took too long"),
                    }
                }
                Ok(Some(len)) => {
                    self.shared.count(|stats| stats.connections += 1);
                    return Ok(StreamConnection {
                        name: self.name.clone(),
                        shared: self.shared.clone(),
                        stream,
                        reading,
                        first: len,
                        signed: None,
                    });
                }
                Ok(None) => self.shared.failed.set(true),
                Err(error) => {
                    warn!(link = %self.name, %error, "link read failed");
                    self.shared.failed.set(true);
                }
            }
            *self.shared.reading.borrow_mut() = Some(reading);
        }
    }
}

/// One device's connection over the link.
pub struct StreamConnection<S: ByteStream> {
    name: DeviceName,
    shared: Rc<Shared<S>>,
    stream: Rc<S>,
    reading: Box<Reading>,
    first: usize,
    /// The key the session proved, and its seals; `None` for a device.
    signed: Option<Signed>,
}

impl<S: ByteStream> std::fmt::Debug for StreamConnection<S> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "StreamConnection({})", self.name)
    }
}

impl<S: ByteStream + 'static> Connection for StreamConnection<S> {
    type Read = StreamRead<S>;
    type Write = StreamWrite<S>;

    fn principal(&self) -> Principal {
        match &self.signed {
            Some(signed) => signed.principal.clone(),
            None => Principal::Device(self.name.clone()),
        }
    }

    fn offers(&self) -> Roles {
        match &self.signed {
            // What the network offers a key: a tool, or another node.
            Some(_) => Roles::CLIENT.and(Roles::PEER),
            None => Roles::DEVICE
                .and(Roles::PROVIDER)
                .and(Roles::EXECUTOR)
                .and(Roles::CLIENT),
        }
    }

    fn passes_descriptors(&self) -> bool {
        false
    }

    fn split(self) -> (Self::Read, Self::Write) {
        let (reading, writing) = match self.signed {
            Some(signed) => (Some(signed.reading), Some(signed.writing)),
            None => (None, None),
        };
        // Room for one sealed frame each way, only on a signed session.
        let room = |seal: &Option<Seal>| {
            if seal.is_some() {
                vec![0; MAX_FRAME]
            } else {
                Vec::new()
            }
        };
        (
            StreamRead {
                shared: self.shared.clone(),
                stream: self.stream.clone(),
                reading: Some(self.reading),
                first: self.first,
                sealed: room(&reading),
                seal: reading,
            },
            StreamWrite {
                shared: self.shared,
                stream: self.stream,
                out: [0; CHUNK],
                sealed: room(&writing),
                seal: writing,
            },
        )
    }
}

/// The reading half.
pub struct StreamRead<S: ByteStream> {
    shared: Rc<Shared<S>>,
    stream: Rc<S>,
    reading: Option<Box<Reading>>,
    first: usize,
    seal: Option<Seal>,
    sealed: Vec<u8>,
}

impl<S: ByteStream> std::fmt::Debug for StreamRead<S> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("StreamRead")
    }
}

impl<S: ByteStream> FrameRead for StreamRead<S> {
    async fn recv(&mut self, buf: &mut [u8], _fds: &mut FdSet) -> io::Result<Option<usize>> {
        let Some(reading) = self.reading.as_mut() else {
            return Ok(None);
        };
        if self.first > 0 {
            let len = std::mem::take(&mut self.first);
            let Some(to) = buf.get_mut(..len) else {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "a frame longer than the buffer",
                ));
            };
            to.copy_from_slice(&reading.first[..len]);
            return Ok(Some(len));
        }
        let Some(seal) = &mut self.seal else {
            let found = next_frame(&self.shared, &self.stream, reading, buf).await;
            if !matches!(found, Ok(Some(_))) {
                self.shared.failed.set(true);
            }
            return found;
        };
        match next_frame(&self.shared, &self.stream, reading, &mut self.sealed).await {
            // A frame that fails its check ends the session, not the link: the next frame on
            // the link starts over.
            Ok(Some(len)) => signed::open(seal, &self.sealed[..len], buf).map(Some),
            other => {
                self.shared.failed.set(true);
                other
            }
        }
    }
}

impl<S: ByteStream> Drop for StreamRead<S> {
    fn drop(&mut self) {
        // The link is free for its next connection.
        *self.shared.reading.borrow_mut() = self.reading.take();
        self.shared.home.notify_one();
    }
}

/// The writing half.
pub struct StreamWrite<S: ByteStream> {
    shared: Rc<Shared<S>>,
    stream: Rc<S>,
    out: [u8; CHUNK],
    seal: Option<Seal>,
    sealed: Vec<u8>,
}

impl<S: ByteStream> std::fmt::Debug for StreamWrite<S> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("StreamWrite")
    }
}

impl<S: ByteStream> FrameWrite for StreamWrite<S> {
    async fn send(&mut self, frame: &[u8], fds: &[OwnedFd]) -> io::Result<()> {
        if !fds.is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "a link carries no descriptors",
            ));
        }
        match &mut self.seal {
            Some(seal) => {
                let sealed = signed::seal(seal, frame, &mut self.sealed)?;
                write_frame(&self.shared, &self.stream, &mut self.out, sealed).await
            }
            None => write_frame(&self.shared, &self.stream, &mut self.out, frame).await,
        }
    }
}
