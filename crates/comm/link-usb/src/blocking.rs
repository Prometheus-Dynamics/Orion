//! A byte stream over two descriptors that block, such as a gadget function's bulk endpoints,
//! which can't be waited on. A thread sits in each call, so nothing is polled, and the bytes
//! cross to the comm core's task through two buffers that are made once.
//!
//! **The host going away is not the end.** An endpoint whose host is unplugged, reset, or
//! unconfigured, or whose function is disabled, fails the call that was waiting with
//! `ESHUTDOWN`, and every call after with `EAGAIN` until it is back. Neither ends the link: the
//! descriptors are kept, a read is tried again 100 ms later, doubling to 1 s while the endpoint
//! stays down, and bytes being written then are lost, not the link (the next frame's leading
//! delimiter puts the far end back in step). What runs over the link notices by its own
//! deadlines. Any other error, or the end of a file, ends the link. [`verdict`] is that rule.
//!
//! **A reader let go hands on what it reads.** A call already waiting in `read` cannot be
//! taken back, only outlived. When the same endpoint is offered again (whoever holds the
//! gadget keeps its files open through the host going away), the old thread may still be in
//! that call when the host comes back, and it is the one the host's next bytes come to. So a
//! stream that is taken over is told what took it over ([`Blocking::hand_to`]): what it still
//! holds, and anything its thread reads after it was let go, goes to the stream that took
//! over, ahead of that stream's own bytes, in the order it came. Nothing the host sends is
//! lost to a stream nobody reads.

use std::collections::VecDeque;
use std::io::{self, Read, Write};
use std::sync::{Arc, Condvar, Mutex, MutexGuard, PoisonError};
use std::time::Duration;

use tokio::sync::Notify;
use tracing::{info, warn};

/// How many bytes wait for the comm core to read them. The reading thread stops reading while
/// it is full, which is the device's flow control.
const BUFFER: usize = 64 * 1024;
const CHUNK: usize = 4096;
/// The first wait before reading a down endpoint again.
const DOWN_FIRST: Duration = Duration::from_millis(100);
/// The longest wait between tries while it stays down.
const DOWN_MOST: Duration = Duration::from_secs(1);

/// What an endpoint's error means for the link.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Verdict {
    /// The host is away or the function disabled: keep the descriptors and try again.
    Down,
    /// The link is over.
    End,
}

/// The one rule: `ESHUTDOWN` and `EAGAIN` are the host away, everything else the end.
pub(crate) fn verdict(error: &io::Error) -> Verdict {
    match error.raw_os_error() {
        Some(code)
            if code == rustix::io::Errno::AGAIN.raw_os_error()
                || code == rustix::io::Errno::SHUTDOWN.raw_os_error() =>
        {
            Verdict::Down
        }
        _ => Verdict::End,
    }
}

#[derive(Default)]
struct Incoming {
    bytes: VecDeque<u8>,
    /// How many bytes at the front came from a stream this one took over: what those handed on
    /// later goes after them, and ahead of what this stream's own thread read.
    handed: usize,
    /// The descriptor reached its end or failed.
    closed: bool,
    /// Nobody wants the bytes any more.
    stop: bool,
}

#[derive(Default)]
struct Outgoing {
    bytes: Vec<u8>,
    /// The writing thread has these bytes and has not finished with them.
    busy: bool,
    failed: bool,
    stop: bool,
}

#[derive(Default)]
struct Shared {
    incoming: Mutex<Incoming>,
    /// Room was made in `incoming`, or it is time to stop.
    room: Condvar,
    /// Bytes arrived, or the descriptor closed.
    arrived: Notify,
    outgoing: Mutex<Outgoing>,
    /// There are bytes to write, or it is time to stop.
    work: Condvar,
    /// The bytes were written, or the descriptor failed.
    written: Notify,
    /// The stream that took this one over, which gets what this one reads from now on.
    next: Mutex<Option<Arc<Shared>>>,
}

/// Gives `bytes`, read by a stream that was let go, to the stream that took it over, or to the
/// one that took that over in turn. Nobody to give them to: the link is over, and they go too.
fn hand_on(from: &Shared, bytes: impl ExactSizeIterator<Item = u8>) {
    let Some(mut to) = lock(&from.next).clone() else {
        return;
    };
    loop {
        let mut incoming = lock(&to.incoming);
        if incoming.stop {
            let next = lock(&to.next).clone();
            drop(incoming);
            match next {
                Some(next) => to = next,
                None => return,
            }
            continue;
        }
        let at = incoming.handed.min(incoming.bytes.len());
        let len = bytes.len();
        // After what was handed on before, ahead of this stream's own.
        let own: Vec<u8> = incoming.bytes.drain(at..).collect();
        incoming.bytes.extend(bytes);
        incoming.bytes.extend(own);
        incoming.handed = at + len;
        drop(incoming);
        to.arrived.notify_one();
        return;
    }
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

pub(crate) struct Blocking(Arc<Shared>);

/// The reading thread: reads until the end, keeping on through the host going away.
fn reading(mut read: impl Read, shared: &Shared) {
    let mut chunk = [0_u8; CHUNK];
    let mut wait = DOWN_FIRST;
    let mut down = false;
    loop {
        if lock(&shared.incoming).stop {
            return;
        }
        let len = match read.read(&mut chunk) {
            Ok(len) => len,
            Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
            Err(error) if verdict(&error) == Verdict::Down => {
                if !down {
                    info!(%error, "usb link: the host is away; the link waits for it");
                    down = true;
                }
                // Asleep until the next try, or until the link is let go.
                let incoming = lock(&shared.incoming);
                if incoming.stop {
                    return;
                }
                let (incoming, _) = shared
                    .room
                    .wait_timeout(incoming, wait)
                    .unwrap_or_else(PoisonError::into_inner);
                if incoming.stop {
                    return;
                }
                wait = (wait * 2).min(DOWN_MOST);
                continue;
            }
            Err(error) => {
                warn!(%error, "usb link: the endpoint failed; the link ends");
                0
            }
        };
        if down && len > 0 {
            info!("usb link: the host is back");
            (down, wait) = (false, DOWN_FIRST);
        }
        let mut incoming = lock(&shared.incoming);
        while !incoming.stop && incoming.bytes.len() + len > BUFFER {
            incoming = shared
                .room
                .wait(incoming)
                .unwrap_or_else(PoisonError::into_inner);
        }
        if incoming.stop {
            // Let go while it was reading: what it read is the host's, for whoever took over.
            // Handed on under this stream's lock, so it goes after what it still held.
            hand_on(shared, chunk[..len].iter().copied());
            return;
        }
        incoming.bytes.extend(&chunk[..len]);
        incoming.closed = len == 0;
        drop(incoming);
        shared.arrived.notify_one();
        if len == 0 {
            return;
        }
    }
}

/// All of `bytes` to `write`. The host going away loses them, not the link.
fn write_out(write: &mut impl Write, bytes: &[u8]) -> io::Result<()> {
    match write.write_all(bytes).and_then(|()| write.flush()) {
        Err(error) if verdict(&error) == Verdict::Down => {
            info!(%error, lost = bytes.len(), "usb link: the host is away; bytes written are lost");
            Ok(())
        }
        other => other,
    }
}

/// The writing thread: writes what it is given until it is stopped or the endpoint ends.
fn writing(mut write: impl Write, shared: &Shared) {
    let mut mine = Vec::with_capacity(CHUNK);
    loop {
        {
            let mut outgoing = lock(&shared.outgoing);
            while !outgoing.stop && !outgoing.busy {
                outgoing = shared
                    .work
                    .wait(outgoing)
                    .unwrap_or_else(PoisonError::into_inner);
            }
            if outgoing.stop {
                return;
            }
            // The two buffers change hands, so neither is ever made again.
            std::mem::swap(&mut mine, &mut outgoing.bytes);
        }
        let result = write_out(&mut write, &mine);
        if let Err(error) = &result {
            warn!(%error, "usb link: the endpoint failed; the link ends");
        }
        mine.clear();
        let mut outgoing = lock(&shared.outgoing);
        outgoing.busy = false;
        outgoing.failed = result.is_err();
        let failed = outgoing.failed;
        drop(outgoing);
        shared.written.notify_one();
        if failed {
            return;
        }
    }
}

impl Blocking {
    /// Starts a thread reading `read` and one writing `write`. Each closes its descriptor when
    /// it ends.
    pub(crate) fn start(
        read: impl Read + Send + 'static,
        write: impl Write + Send + 'static,
    ) -> io::Result<Self> {
        let shared = Arc::new(Shared::default());
        lock(&shared.incoming).bytes.reserve_exact(BUFFER);
        lock(&shared.outgoing).bytes.reserve_exact(CHUNK);

        let reader = shared.clone();
        std::thread::Builder::new()
            .name("orion-usb-read".into())
            .spawn(move || reading(read, &reader))?;
        let writer = shared.clone();
        std::thread::Builder::new()
            .name("orion-usb-write".into())
            .spawn(move || writing(write, &writer))?;
        Ok(Self(shared))
    }
    pub(crate) async fn read(&self, buf: &mut [u8]) -> io::Result<usize> {
        loop {
            {
                let mut incoming = lock(&self.0.incoming);
                if !incoming.bytes.is_empty() {
                    let len = buf.len().min(incoming.bytes.len());
                    for (to, from) in buf.iter_mut().zip(incoming.bytes.drain(..len)) {
                        *to = from;
                    }
                    incoming.handed = incoming.handed.saturating_sub(len);
                    drop(incoming);
                    self.0.room.notify_one();
                    return Ok(len);
                }
                if incoming.closed {
                    return Ok(0);
                }
            }
            self.0.arrived.notified().await;
        }
    }

    pub(crate) async fn write(&self, bytes: &[u8]) -> io::Result<usize> {
        let len = bytes.len().min(CHUNK);
        loop {
            {
                let mut outgoing = lock(&self.0.outgoing);
                if outgoing.failed {
                    return Err(io::ErrorKind::BrokenPipe.into());
                }
                if !outgoing.busy {
                    outgoing.bytes.clear();
                    outgoing.bytes.extend_from_slice(&bytes[..len]);
                    outgoing.busy = true;
                    break;
                }
            }
            self.0.written.notified().await;
        }
        self.0.work.notify_one();
        // The bytes are the device's only when the thread has written them.
        loop {
            self.0.written.notified().await;
            let outgoing = lock(&self.0.outgoing);
            if outgoing.failed {
                return Err(io::ErrorKind::BrokenPipe.into());
            }
            if !outgoing.busy {
                return Ok(len);
            }
        }
    }
}

/// A stream's reading side, kept by its link to say what took the stream over.
pub(crate) struct Reader(Arc<Shared>);

impl Reader {
    /// `next` takes this stream over: what this one still holds when it is let go, and what
    /// its thread reads after, are `next`'s.
    pub(crate) fn hand_to(&self, next: &Self) {
        *lock(&self.0.next) = Some(next.0.clone());
    }
}

impl Blocking {
    pub(crate) fn reader(&self) -> Reader {
        Reader(self.0.clone())
    }
}

impl Drop for Blocking {
    fn drop(&mut self) {
        // The writing thread ends now. The reading one ends when its call returns: a blocked
        // read cannot be taken back, only outlived, and its descriptor closes with it. What
        // was read and not taken goes to whoever took over, under the lock the thread hands
        // on under, so it stays ahead of anything the thread reads after.
        {
            let mut incoming = lock(&self.0.incoming);
            incoming.stop = true;
            let held = std::mem::take(&mut incoming.bytes);
            hand_on(&self.0, held.into_iter());
        }
        lock(&self.0.outgoing).stop = true;
        self.0.room.notify_one();
        self.0.work.notify_one();
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicBool, Ordering};

    use super::*;

    fn os(errno: rustix::io::Errno) -> io::Error {
        io::Error::from_raw_os_error(errno.raw_os_error())
    }

    /// An endpoint that answers from a script, and says when it was let go.
    struct Script {
        steps: VecDeque<io::Result<Vec<u8>>>,
        written: Arc<Mutex<Vec<u8>>>,
        closed: Arc<AtomicBool>,
    }

    impl Script {
        fn new(steps: Vec<io::Result<Vec<u8>>>) -> (Self, Arc<Mutex<Vec<u8>>>, Arc<AtomicBool>) {
            let (written, closed) = (Arc::default(), Arc::new(AtomicBool::new(false)));
            let script = Self {
                steps: steps.into(),
                written: Arc::clone(&written),
                closed: Arc::clone(&closed),
            };
            (script, written, closed)
        }

        fn step(&mut self) -> io::Result<Vec<u8>> {
            // A script that has run out waits, as an endpoint with nothing to say does.
            self.steps.pop_front().unwrap_or_else(|| {
                std::thread::sleep(Duration::from_secs(3600));
                Ok(Vec::new())
            })
        }
    }

    impl Read for Script {
        fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
            let bytes = self.step()?;
            buf[..bytes.len()].copy_from_slice(&bytes);
            Ok(bytes.len())
        }
    }

    impl Write for Script {
        fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
            self.step()?;
            lock(&self.written).extend_from_slice(buf);
            Ok(buf.len())
        }

        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    impl Drop for Script {
        fn drop(&mut self) {
            self.closed.store(true, Ordering::SeqCst);
        }
    }

    fn run(test: impl Future<Output = ()>) {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime")
            .block_on(test);
    }

    async fn until(what: &str, done: impl Fn() -> bool) {
        for _ in 0..1000 {
            if done() {
                return;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        panic!("never happened: {what}");
    }

    #[test]
    fn the_host_away_is_kept_through_and_anything_else_is_the_end() {
        use rustix::io::Errno;
        assert_eq!(verdict(&os(Errno::SHUTDOWN)), Verdict::Down);
        assert_eq!(verdict(&os(Errno::AGAIN)), Verdict::Down);
        for end in [
            Errno::BADMSG,
            Errno::NODEV,
            Errno::IO,
            Errno::PIPE,
            Errno::BADF,
        ] {
            assert_eq!(verdict(&os(end)), Verdict::End, "{end:?}");
        }
        assert_eq!(verdict(&io::Error::other("no code")), Verdict::End);
    }

    #[test]
    fn reading_goes_on_through_the_host_going_away_and_ends_on_anything_else() {
        use rustix::io::Errno;
        run(async {
            let (read, _, read_closed) = Script::new(vec![
                Ok(b"before ".to_vec()),
                Err(os(Errno::SHUTDOWN)),
                Err(os(Errno::AGAIN)),
                Err(os(Errno::AGAIN)),
                Ok(b"after".to_vec()),
                Err(os(Errno::BADMSG)),
            ]);
            let (write, _, write_closed) = Script::new(Vec::new());
            let link = Blocking::start(read, write).expect("start");
            let mut got = Vec::new();
            let mut buf = [0_u8; 64];
            loop {
                let len = link.read(&mut buf).await.expect("read");
                if len == 0 {
                    break;
                }
                got.extend_from_slice(&buf[..len]);
            }
            // Every byte from before and after the host was away, then the end.
            assert_eq!(got, b"before after");
            until("the endpoint read is closed", || {
                read_closed.load(Ordering::SeqCst)
            })
            .await;
            // The other endpoint is closed when the link is let go.
            assert!(!write_closed.load(Ordering::SeqCst));
            drop(link);
            until("the endpoint written is closed", || {
                write_closed.load(Ordering::SeqCst)
            })
            .await;
        });
    }

    /// An endpoint whose reads wait for the test to give them bytes, as a host's do. Its
    /// sender dropped is the end.
    struct Gated(std::sync::mpsc::Receiver<Vec<u8>>);

    impl Read for Gated {
        fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
            Ok(self.0.recv().map_or(0, |bytes| {
                buf[..bytes.len()].copy_from_slice(&bytes);
                bytes.len()
            }))
        }
    }

    #[test]
    fn a_reader_let_go_inside_a_read_hands_what_it_reads_to_the_stream_that_took_over() {
        run(async {
            let (old_host, old_endpoint) = std::sync::mpsc::channel();
            let (new_host, new_endpoint) = std::sync::mpsc::channel();
            let old = Blocking::start(Gated(old_endpoint), Script::new(Vec::new()).0).expect("old");
            let next =
                Blocking::start(Gated(new_endpoint), Script::new(Vec::new()).0).expect("next");
            // The old stream has read something nobody took.
            old_host.send(b"he".to_vec()).expect("send");
            until("the old stream holds it", || {
                lock(&old.0.incoming).bytes.len() == 2
            })
            .await;

            // The same endpoint is offered again. The new stream takes over while the old
            // thread is still waiting in its read, which cannot be taken back.
            old.reader().hand_to(&next.reader());
            drop(old);
            until("what it held is handed on", || {
                lock(&next.0.incoming).bytes.len() == 2
            })
            .await;
            // The new thread reads what came after...
            new_host.send(b" after".to_vec()).expect("send");
            until("the new stream reads", || {
                lock(&next.0.incoming).bytes.len() == 8
            })
            .await;
            // ...and the host's bytes that came before it land on the old thread, which hands
            // them on, after what it held and ahead of the new thread's.
            old_host.send(b"llo".to_vec()).expect("send");
            until("the old thread hands them on", || {
                lock(&next.0.incoming).bytes.len() == 11
            })
            .await;
            let mut buf = [0_u8; 64];
            let len = next.read(&mut buf).await.expect("read");
            assert_eq!(&buf[..len], b"hello after");
        });
    }

    #[test]
    fn bytes_written_while_the_host_is_away_are_lost_not_the_link() {
        use rustix::io::Errno;
        run(async {
            let (read, _, _) = Script::new(Vec::new());
            let (write, written, closed) = Script::new(vec![
                Ok(Vec::new()),
                Err(os(Errno::SHUTDOWN)),
                Err(os(Errno::AGAIN)),
                Ok(Vec::new()),
                Err(os(Errno::BADMSG)),
            ]);
            let link = Blocking::start(read, write).expect("start");
            assert_eq!(link.write(b"one ").await.expect("written"), 4);
            assert_eq!(link.write(b"lost ").await.expect("lost, not failed"), 5);
            assert_eq!(link.write(b"lost too ").await.expect("lost, not failed"), 9);
            assert_eq!(link.write(b"two").await.expect("written"), 3);
            assert_eq!(&*lock(&written), b"one two");
            // Any other error is the end of writing, and of the endpoint.
            assert!(link.write(b"never").await.is_err());
            until("the endpoint written is closed", || {
                closed.load(Ordering::SeqCst)
            })
            .await;
            assert!(link.write(b"after").await.is_err());
        });
    }
}
