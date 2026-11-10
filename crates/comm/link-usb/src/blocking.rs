//! A byte stream over two descriptors that block, such as a gadget function's bulk endpoints,
//! which can't be waited on. A thread sits in each call, so nothing is polled, and the bytes
//! cross to the comm core's task through two buffers that are made once.

use std::collections::VecDeque;
use std::fs::File;
use std::io::{self, Read, Write};
use std::sync::{Arc, Condvar, Mutex, MutexGuard, PoisonError};

use tokio::sync::Notify;

/// How many bytes wait for the comm core to read them. The reading thread stops reading while
/// it is full, which is the device's flow control.
const BUFFER: usize = 64 * 1024;
const CHUNK: usize = 4096;

#[derive(Default)]
struct Incoming {
    bytes: VecDeque<u8>,
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
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

pub(crate) struct Blocking(Arc<Shared>);

impl Blocking {
    /// Starts a thread reading `read` and one writing `write`.
    pub(crate) fn start(mut read: File, mut write: File) -> io::Result<Self> {
        let shared = Arc::new(Shared::default());
        lock(&shared.incoming).bytes.reserve_exact(BUFFER);
        lock(&shared.outgoing).bytes.reserve_exact(CHUNK);

        let reading = shared.clone();
        std::thread::Builder::new()
            .name("orion-usb-read".into())
            .spawn(move || {
                let mut chunk = [0_u8; CHUNK];
                loop {
                    let len = match read.read(&mut chunk) {
                        Ok(len) if len > 0 => len,
                        Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
                        _ => 0,
                    };
                    let mut incoming = lock(&reading.incoming);
                    while !incoming.stop && incoming.bytes.len() + len > BUFFER {
                        incoming = reading
                            .room
                            .wait(incoming)
                            .unwrap_or_else(PoisonError::into_inner);
                    }
                    if incoming.stop {
                        return;
                    }
                    incoming.bytes.extend(&chunk[..len]);
                    incoming.closed = len == 0;
                    drop(incoming);
                    reading.arrived.notify_one();
                    if len == 0 {
                        return;
                    }
                }
            })?;

        let writing = shared.clone();
        std::thread::Builder::new()
            .name("orion-usb-write".into())
            .spawn(move || {
                let mut mine = Vec::with_capacity(CHUNK);
                loop {
                    {
                        let mut outgoing = lock(&writing.outgoing);
                        while !outgoing.stop && !outgoing.busy {
                            outgoing = writing
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
                    let result = write.write_all(&mine).and_then(|()| write.flush());
                    mine.clear();
                    let mut outgoing = lock(&writing.outgoing);
                    outgoing.busy = false;
                    outgoing.failed = result.is_err();
                    let failed = outgoing.failed;
                    drop(outgoing);
                    writing.written.notify_one();
                    if failed {
                        return;
                    }
                }
            })?;
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

impl Drop for Blocking {
    fn drop(&mut self) {
        // The writing thread ends now. The reading one ends when its call returns: a blocked
        // read cannot be taken back, only outlived, and its descriptor closes with it.
        lock(&self.0.incoming).stop = true;
        lock(&self.0.outgoing).stop = true;
        self.0.room.notify_one();
        self.0.work.notify_one();
    }
}
