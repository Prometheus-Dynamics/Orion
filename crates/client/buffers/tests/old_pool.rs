//! A comm core that hands over a buffer pool whose page this build does not read: the
//! subscriber says what the shared-memory crate found, not that the wire was broken. A pool
//! page of another layout is said as one, with both layouts, as a ring of another layout is.

#![cfg(target_os = "linux")]

use std::os::fd::OwnedFd;
use std::path::PathBuf;
use std::sync::atomic::Ordering;
use std::sync::mpsc;
use std::time::Duration;

use orion_buffers::{Buffers, HoldLimits};
use orion_connect::{Address, Client, ClientError, Options, Session};
use orion_msg_topics::{BufferInfo, BufferOffer, Opened};
use orion_server::{
    Admits, Checked, Comm, Cx, Incoming, Limits, Policy, Security, Service, SessionId, Sessions,
    What,
};
use orion_topics::{TopicError, Topics};
use orion_types::Error;
use orion_unix::UnixCarrier;
use orion_wire::{FrameView, List, kind};
use shm_ring::{MemMut, Ring, Spec, WriteMap, sealed_memfd};

fn ring() -> Ring {
    Ring::create(&Spec {
        fields: 1,
        history: 8,
        max_payload: 64,
        expiry_ms: 0,
    })
    .expect("ring")
}

/// A pool page of layout `layout`, one buffer, as a publisher of a device library that writes
/// that layout would hand over.
fn pool_page(layout: u64) -> OwnedFd {
    let fd = sealed_memfd(4096).expect("memfd");
    let page = WriteMap::new(fd.try_clone().expect("fd")).expect("map");
    page.store(2, 1 | 1 << 32, Ordering::Relaxed);
    page.store(1, layout, Ordering::Relaxed);
    page.store(0, u64::from_le_bytes(*b"SHMPOOL1"), Ordering::Release);
    fd
}

/// Answers a subscription with a ring of this build, and then a pool whose page is not a
/// pool page of this build's: a pool page of `layout`, or with none, memory that is not a
/// pool page at all.
struct OldPool {
    layout: Option<u64>,
}

impl Service for OldPool {
    fn kinds(&self) -> &'static [u8] {
        &[kind::TOPIC_SUBSCRIBE]
    }

    fn asks<'a>(&self, _frame: &FrameView<'a>) -> Result<(What<'a>, Policy), Error> {
        Ok((
            What::Read("topics"),
            Policy::new(Admits::ANYONE).without_grant(),
        ))
    }

    fn handle(&mut self, _request: Checked<Incoming<'_>>, cx: &mut Cx<'_>) -> Result<(), Error> {
        let io = |_| Error::Internal;
        let frames = ring();
        let opened = Opened {
            request_seq: cx.request_seq(),
            handle: 1,
            shared_memory: true,
            ring_bytes: 0,
        };
        cx.reply_with_fds(&opened, [frames.read_fd().map_err(io)?])?;
        let buffers = [BufferInfo {
            index: 0,
            size: 64,
            descriptor: 0,
            offset: 0,
        }];
        let offer = BufferOffer {
            handle: 1,
            generation: 1,
            buffers: List::new(&buffers),
            reader_slots: 0,
        };
        let memory = ring();
        let page = match self.layout {
            Some(layout) => pool_page(layout),
            // A ring's memory: sealed, readable, and not a pool page.
            None => ring().read_fd().map_err(io)?,
        };
        cx.reply_with_fds(&offer, [page, memory.read_fd().map_err(io)?])
    }

    fn quiet(&mut self, _session: SessionId, _quiet_ms: u64, _sessions: &mut Sessions) {}
}

fn old_core(layout: Option<u64>) -> (PathBuf, tokio::sync::oneshot::Sender<()>) {
    let dir = std::env::temp_dir().join(format!(
        "orion-old-pool-{}-{}",
        layout.unwrap_or(0),
        std::process::id()
    ));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).expect("dir");
    let socket = dir.join("orion.sock");
    let (ready, started) = mpsc::channel();
    let (stop, stopped) = tokio::sync::oneshot::channel::<()>();
    let path = socket.clone();
    std::thread::spawn(move || {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime");
        tokio::task::LocalSet::new().block_on(&runtime, async move {
            let comm = Comm::new(
                "board".parse().expect("name"),
                Security::Off,
                None,
                Limits::default(),
            );
            comm.add_service(OldPool { layout });
            let unix = UnixCarrier::bind(&path, Some(0o600), &[]).expect("socket");
            ready.send(()).expect("ready");
            tokio::task::spawn_local(async move { comm.serve(unix).await });
            let _ = stopped.await;
        });
    });
    started.recv().expect("the comm core starts");
    (socket, stop)
}

/// What subscribing to a pool from a comm core handing over `layout` ends in.
fn subscribed(layout: Option<u64>) -> TopicError {
    let (socket, _stop) = old_core(layout);
    let session =
        Session::connect(&Address::Local(socket), "viewer", Options::default()).expect("connect");
    let client = Client::new(session);
    let buffers = Buffers::new(&client, &Topics::new(&client));
    match buffers.subscribe("camera.front", &[], HoldLimits::default()) {
        Err(error) => error,
        Ok(mut subscriber) => loop {
            if let Err(error) = subscriber.wait(Duration::from_millis(50)) {
                break error;
            }
        },
    }
}

#[test]
fn a_pool_this_build_does_not_read_is_said_with_the_shared_memory_crates_words() {
    let error = subscribed(None);
    assert!(
        matches!(&error, TopicError::Client(ClientError::Memory { what: "a buffer pool", why })
            if why.contains("not a pool page")),
        "{error:?}"
    );
}

#[test]
fn a_pool_page_of_another_layout_is_said_as_one_with_both_layouts() {
    let error = subscribed(Some(1));
    assert!(
        matches!(
            &error,
            TopicError::Client(ClientError::Layout {
                what: "a buffer pool",
                found: 1,
                reads: 2,
                ..
            })
        ),
        "{error:?}"
    );
}
