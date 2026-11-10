//! A pool passes between two processes and a frame is read back with no copy.
//!
//! The test starts this same test binary again as the subscriber. The publisher sends it, over
//! a socket, what the comm core will send a subscriber: the ring's read end, the pool page, the
//! pool's descriptor and where each buffer is. Then it publishes a frame, and changes the frame
//! where it lies; the subscriber sees both through its own mapping.

#![allow(unsafe_code)]

use std::os::fd::{AsFd, AsRawFd, BorrowedFd, FromRawFd, OwnedFd};
use std::os::unix::net::UnixStream;
use std::process::Command;
use std::time::{Duration, Instant};

use orion_fdpass::{recv, send};
use orion_ring::{FieldId, FrameTime, Reader, Ring, Spec};
use orion_ring_buffers::{
    BufferDesc, BufferReader, BufferWriter, NextFrame, NoHolders, NoMarks, Pool, PoolView,
};

const SOCKET_ENV: &str = "ORION_TEST_SUBSCRIBER_SOCKET";
const BUFFERS: u32 = 3;
const FRAME: usize = 64 * 1024;

fn wait_for(what: &str, mut done: impl FnMut() -> bool) {
    let started = Instant::now();
    while !done() {
        assert!(
            started.elapsed() < Duration::from_secs(30),
            "timed out waiting for {what}"
        );
        std::thread::sleep(Duration::from_millis(1));
    }
}

/// The subscriber process. Does nothing unless the publisher started it.
#[test]
fn subscriber_process() {
    let Ok(socket) = std::env::var(SOCKET_ENV) else {
        return;
    };
    // SAFETY: the publisher passed this descriptor to this process for it to own.
    let socket = unsafe { OwnedFd::from_raw_fd(socket.parse().expect("descriptor number")) };

    // One frame: the buffer list, with the ring, the pool page and the pool's descriptor.
    let mut buffer = [0_u8; 256];
    let mut received = recv(socket.as_fd(), &mut buffer, 3, false)
        .expect("pool offer")
        .expect("the publisher is there");
    let ring = received.fds.take(0).expect("ring");
    let page = received.fds.take(1).expect("pool page");
    let memory = received.fds.take(2).expect("pool memory");
    let buffers: Vec<BufferDesc> = buffer[..received.len]
        .as_chunks::<16>()
        .0
        .iter()
        .map(|bytes| BufferDesc {
            descriptor: 0,
            offset: u64::from_le_bytes(bytes[..8].try_into().expect("offset")),
            size: u64::from_le_bytes(bytes[8..].try_into().expect("size")),
        })
        .collect();
    assert_eq!(buffers.len(), BUFFERS as usize);

    let view = PoolView::attach(page, vec![memory], &buffers).expect("pool view");
    let mut reader = BufferReader::new(
        Reader::attach(ring).expect("ring"),
        FieldId(0),
        view,
        NoMarks,
    );
    send(socket.as_fd(), &[b"attached"], &[], false).expect("say attached");

    let mut found = None;
    wait_for("a frame", || {
        if let NextFrame::Frame(frame) = reader.next() {
            assert_eq!(
                (frame.time, frame.meta),
                (FrameTime::from_nanos(1234), &b"exposure=7"[..])
            );
            found = Some(frame.held);
        }
        found.is_some()
    });
    let held = found.expect("frame");
    let bytes = held.bytes(0).expect("bytes");
    assert_eq!(bytes.len(), FRAME);
    assert!(
        bytes.iter().all(|byte| *byte == 0x5A),
        "the frame the publisher wrote"
    );

    // The publisher now writes over the frame where it lies. Seeing that through the same
    // slice is what "no copy" means: this process never received those bytes any other way.
    send(socket.as_fd(), &[b"read"], &[], false).expect("say read");
    wait_for("the publisher's second write", || bytes[FRAME - 1] == 0xC3);
    assert!(bytes.iter().all(|byte| *byte == 0xC3));
    send(socket.as_fd(), &[b"same memory"], &[], false).expect("say done");
}

fn expect(socket: BorrowedFd<'_>, message: &[u8]) {
    let mut buffer = [0_u8; 64];
    let received = recv(socket, &mut buffer, 0, false)
        .expect("message")
        .expect("the subscriber is there");
    assert_eq!(&buffer[..received.len], message);
}

#[test]
fn a_pool_passes_between_processes_and_a_frame_is_read_back_with_no_copy() {
    let ring = Ring::create(&Spec {
        fields: 1,
        history: 4,
        max_payload: 64,
        expiry_ms: 0,
    })
    .expect("ring");
    let pool = Pool::in_memory(1, BUFFERS, FRAME).expect("pool");

    let (ours, theirs) = UnixStream::pair().expect("socket pair");
    ours.set_read_timeout(Some(Duration::from_secs(30)))
        .expect("timeout");
    // The child inherits its end: take close-on-exec off it.
    // SAFETY: `theirs` is open.
    unsafe { assert_eq!(libc::fcntl(theirs.as_raw_fd(), libc::F_SETFD, 0), 0) };
    let mut child = Command::new(std::env::current_exe().expect("test binary"))
        .args(["--exact", "subscriber_process", "--nocapture"])
        .env(SOCKET_ENV, theirs.as_raw_fd().to_string())
        .spawn()
        .expect("subscriber process");
    drop(theirs);

    let mut offer = Vec::new();
    for buffer in pool.buffers() {
        offer.extend_from_slice(&buffer.offset.to_le_bytes());
        offer.extend_from_slice(&buffer.size.to_le_bytes());
    }
    let ring_fd = ring.read_fd().expect("ring read end");
    let page_fd = pool.page_fd().expect("pool page");
    let memory = pool.descriptors().next().expect("pool memory");
    send(
        ours.as_fd(),
        &[&offer],
        &[ring_fd.as_fd(), page_fd.as_fd(), memory],
        false,
    )
    .expect("pool offer");
    drop((ring_fd, page_fd));

    let mut writer = BufferWriter::new(ring.writer().expect("writer"), FieldId(0), pool);
    expect(ours.as_fd(), b"attached");
    let buffer = writer.take(&NoHolders).expect("a free buffer");
    writer.buffer_mut(buffer).expect("free").fill(0x5A);
    writer
        .publish(&[buffer], FrameTime::from_nanos(1234), b"exposure=7")
        .expect("publish");

    expect(ours.as_fd(), b"read");
    // Nothing holds the buffer in this test, so the publisher may take it back and write it.
    assert!(writer.reclaim(buffer, &NoHolders));
    writer.buffer_mut(buffer).expect("free").fill(0xC3);
    expect(ours.as_fd(), b"same memory");
    assert!(child.wait().expect("subscriber exit").success());
}
