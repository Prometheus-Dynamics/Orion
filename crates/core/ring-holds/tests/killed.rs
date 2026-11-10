//! A reader in another process is killed while it holds a frame.
//!
//! Its marks stay in its page: nothing ran to clear them. The publisher never waits on them. It
//! drops frames while the dead reader's limit still protects the buffer, and has every buffer
//! back as soon as the reader is removed (what the comm core does when the connection closes)
//! or its hold time runs out, whichever is first.

#![allow(unsafe_code)]

use std::os::fd::{AsFd, AsRawFd, BorrowedFd, FromRawFd, OwnedFd};
use std::os::unix::net::UnixStream;
use std::os::unix::process::ExitStatusExt;
use std::process::Command;
use std::time::Duration;

use orion_fdpass::{recv, send};
use orion_ring::{FieldId, FrameTime, Reader, Ring, Spec};
use orion_ring_buffers::{
    BufferDesc, BufferReader, BufferWriter, Dropped, NextFrame, Pool, PoolView,
};
use orion_ring_holds::{HoldTable, Limits, Marks};

const SOCKET_ENV: &str = "ORION_TEST_READER_SOCKET";
const BUFFERS: u32 = 2;
const FRAME: usize = 4096;
const MS: u64 = 1_000_000;

/// The reader process. Does nothing unless the publisher started it.
#[test]
fn reader_process() {
    let Ok(socket) = std::env::var(SOCKET_ENV) else {
        return;
    };
    // SAFETY: the publisher passed this descriptor to this process for it to own.
    let socket = unsafe { OwnedFd::from_raw_fd(socket.parse().expect("descriptor number")) };
    let mut received = recv(socket.as_fd(), &mut [0_u8; 8], 3, false)
        .expect("pool offer")
        .expect("the publisher is there");
    let ring = received.fds.take(0).expect("ring");
    let page = received.fds.take(1).expect("pool page");
    let memory = received.fds.take(2).expect("pool memory");
    let buffers: Vec<BufferDesc> = (0..u64::from(BUFFERS))
        .map(|index| BufferDesc {
            descriptor: 0,
            offset: index * FRAME as u64,
            size: FRAME as u64,
        })
        .collect();
    let view = PoolView::attach(page, vec![memory], &buffers).expect("pool view");
    let marks = Marks::create(BUFFERS, 0, 0).expect("marks");
    let marks_page = marks.page_fd().expect("marks page");
    let mut reader =
        BufferReader::new(Reader::attach(ring).expect("ring"), FieldId(0), view, marks);
    send(socket.as_fd(), &[b"marks"], &[marks_page.as_fd()], false).expect("send marks");

    // Hold the first frame and never let go. The publisher kills this process here.
    let held = loop {
        if let NextFrame::Frame(frame) = reader.next() {
            break frame.held;
        }
        std::thread::sleep(Duration::from_millis(1));
    };
    assert!(held.bytes(0).expect("bytes").iter().all(|byte| *byte == 1));
    send(socket.as_fd(), &[b"holding"], &[], false).expect("say holding");
    loop {
        std::thread::sleep(Duration::from_secs(1));
    }
}

struct Publisher {
    writer: BufferWriter,
    table: HoldTable,
}

impl Publisher {
    fn send(&mut self, fill: u8, now_ms: u64) -> Result<(), Dropped> {
        self.table.enforce(now_ms * MS, self.writer.pool().page());
        let buffer = self.writer.take(&self.table)?;
        self.writer.buffer_mut(buffer).expect("free").fill(fill);
        self.writer
            .publish(&[buffer], FrameTime::from_nanos(now_ms * MS), &[fill])
            .expect("publish");
        Ok(())
    }
}

/// Starts the reader process holding frame 1, kills it, and returns the publisher as it then
/// stands: one buffer still marked by a process that no longer exists.
fn publisher_with_a_killed_reader(limits: Limits) -> Publisher {
    let ring = Ring::create(&Spec {
        fields: 1,
        history: 4,
        max_payload: 32,
        expiry_ms: 0,
    })
    .expect("ring");
    let pool = Pool::in_memory(1, BUFFERS, FRAME).expect("pool");
    let (ours, theirs) = UnixStream::pair().expect("socket pair");
    ours.set_read_timeout(Some(Duration::from_secs(30)))
        .expect("timeout");
    // SAFETY: `theirs` is open. The child inherits it.
    unsafe { assert_eq!(libc::fcntl(theirs.as_raw_fd(), libc::F_SETFD, 0), 0) };
    let mut child = Command::new(std::env::current_exe().expect("test binary"))
        .args(["--exact", "reader_process", "--nocapture"])
        .env(SOCKET_ENV, theirs.as_raw_fd().to_string())
        .spawn()
        .expect("reader process");
    drop(theirs);

    let ring_fd = ring.read_fd().expect("ring read end");
    let page_fd = pool.page_fd().expect("pool page");
    let memory = pool.descriptors().next().expect("pool memory");
    let fds = [ring_fd.as_fd(), page_fd.as_fd(), memory];
    send(ours.as_fd(), &[b"pool"], &fds, false).expect("pool offer");

    let mut buffer = [0_u8; 16];
    let mut received = recv(ours.as_fd(), &mut buffer, 1, false)
        .expect("marks")
        .expect("the reader is there");
    assert_eq!(&buffer[..received.len], b"marks");
    let mut table = HoldTable::new(BUFFERS);
    table
        .add(0, received.fds.take(0).expect("marks page"), limits)
        .expect("add reader");
    let mut publisher = Publisher {
        writer: BufferWriter::new(ring.writer().expect("writer"), FieldId(0), pool),
        table,
    };
    publisher.send(1, 0).expect("frame");
    expect(ours.as_fd(), b"holding");

    child.kill().expect("kill the reader");
    let status = child.wait().expect("reader exit");
    assert_eq!(status.signal(), Some(libc::SIGKILL));
    publisher
}

fn expect(socket: BorrowedFd<'_>, message: &[u8]) {
    let mut buffer = [0_u8; 16];
    let received = recv(socket, &mut buffer, 0, false)
        .expect("message")
        .expect("the reader is there");
    assert_eq!(&buffer[..received.len], message);
}

#[test]
fn a_killed_readers_buffers_come_back_when_it_is_removed() {
    let mut publisher = publisher_with_a_killed_reader(Limits::default());
    // The dead reader's mark is still there, so its buffer is left alone, and with the newest
    // frame kept there is nowhere to put a frame. The publisher drops; it does not wait.
    publisher.send(2, 1).expect("the other buffer");
    assert_eq!(publisher.send(3, 2), Err(Dropped));
    assert_eq!(publisher.send(4, 3), Err(Dropped));
    // The connection closing is how the comm core knows. Removing the reader frees its buffer.
    publisher.table.remove(0, publisher.writer.pool().page());
    for round in 5..20 {
        publisher
            .send(round, u64::from(round))
            .expect("frames flow");
    }
}

#[test]
fn a_killed_readers_buffers_come_back_when_its_hold_time_runs_out() {
    let limits = Limits {
        max_held: 0,
        max_hold: Some(Duration::from_millis(500)),
    };
    let mut publisher = publisher_with_a_killed_reader(limits);
    publisher.send(2, 1).expect("the other buffer");
    assert_eq!(publisher.send(3, 100), Err(Dropped));
    // Nobody removed it. Past its hold time it is cut off all the same.
    for round in 4..20 {
        publisher
            .send(round, 1000 + u64::from(round))
            .expect("frames flow");
    }
    assert!(publisher.table.is_cut_off(0));
}
