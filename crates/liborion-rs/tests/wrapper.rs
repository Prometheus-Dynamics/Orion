//! The wrapper against the library, in whichever mode it was built: the version handshake, a
//! failed connect with the library's error text, and a latest-value channel (a payload larger
//! than the first buffer, a descriptor, a wait that times out).

use std::io::{Read, Seek, Write};
use std::os::fd::OwnedFd;
use std::time::Duration;

use liborion_rs::{FrameStatus, Latest, Link, Status, check_version, sys, version};
use orion_transport_ipc::UnixFdLatestServer;

#[test]
fn the_library_serves_this_wrapper() {
    check_version().expect("compatible");
    let library = version();
    assert_eq!(library.major, sys::ORION_ABI_MAJOR);
    assert_eq!(library.layout, sys::ORION_LAYOUT_HASH);
}

#[test]
fn a_failed_connect_carries_the_library_error() {
    let error = Link::connect(
        std::path::Path::new("/nonexistent/orion-link.sock"),
        "wrapper",
        Duration::from_millis(100),
    )
    .expect_err("no socket");
    assert_eq!(error.status, Status::NOT_FOUND);
    assert!(!error.message.is_empty(), "{error}");
    let error = Link::connect(std::path::Path::new("/tmp/x"), "nul\0name", Duration::ZERO)
        .expect_err("a NUL");
    assert_eq!(error.status, Status::INVALID_ARGUMENT);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_latest_value_channel_hands_over_payload_and_descriptors() {
    let path = std::env::temp_dir().join(format!("liborion-rs-latest-{}.sock", std::process::id()));
    let _ = std::fs::remove_file(&path);
    let server = UnixFdLatestServer::bind(&path).await.expect("binds");
    let publisher = server.publisher();
    let task = tokio::spawn(server.serve());

    let mut file = tempfile();
    file.write_all(b"plane-0").expect("writes");
    let payload: Vec<u8> = (0..10_000u32).map(|i| i as u8).collect();
    let sequence = publisher
        .publish(payload.clone(), vec![OwnedFd::from(file)])
        .expect("publishes");

    let socket = path.clone();
    tokio::task::spawn_blocking(move || {
        let mut latest = Latest::connect(&socket).expect("connects");
        let frame = latest.get().expect("a reply");
        assert_eq!(frame.status, FrameStatus::Frame);
        assert_eq!(frame.sequence, sequence);
        assert_eq!(frame.payload, payload);
        assert_eq!(frame.fds.len(), 1);
        let mut plane = std::fs::File::from(frame.fds.into_iter().next().expect("one fd"));
        plane.rewind().expect("seeks");
        let mut text = String::new();
        plane.read_to_string(&mut text).expect("reads");
        assert_eq!(text, "plane-0");

        let later = latest
            .next(sequence, Duration::from_millis(50))
            .expect("a reply");
        assert_eq!(later.status, FrameStatus::Timeout);
        assert_eq!(later.sequence, sequence);
        assert!(later.payload.is_empty() && later.fds.is_empty());
    })
    .await
    .expect("the client runs");
    task.abort();
    let _ = std::fs::remove_file(&path);
}

fn tempfile() -> std::fs::File {
    let path = std::env::temp_dir().join(format!("liborion-rs-plane-{}", std::process::id()));
    let file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(true)
        .open(&path)
        .expect("opens");
    let _ = std::fs::remove_file(&path);
    file
}
