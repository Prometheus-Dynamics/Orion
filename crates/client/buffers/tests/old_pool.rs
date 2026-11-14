//! A comm core that hands over a buffer pool whose page this build does not read: the
//! subscriber says what the shared-memory crate found, not that the wire was broken.

#![cfg(target_os = "linux")]

use std::path::PathBuf;
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
use shm_ring::{Ring, Spec};

fn ring() -> Ring {
    Ring::create(&Spec {
        fields: 1,
        history: 8,
        max_payload: 64,
        expiry_ms: 0,
    })
    .expect("ring")
}

/// Answers a subscription with a ring of this build, and then a pool whose page is not a
/// pool page of this build's: what a publisher of another device library would hand over.
struct OldPool;

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
        // The page is a ring's memory: sealed, readable, and not a pool page.
        let (page, memory) = (ring(), ring());
        cx.reply_with_fds(
            &offer,
            [page.read_fd().map_err(io)?, memory.read_fd().map_err(io)?],
        )
    }

    fn quiet(&mut self, _session: SessionId, _quiet_ms: u64, _sessions: &mut Sessions) {}
}

fn old_core() -> (PathBuf, tokio::sync::oneshot::Sender<()>) {
    let dir = std::env::temp_dir().join(format!("orion-old-pool-{}", std::process::id()));
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
            comm.add_service(OldPool);
            let unix = UnixCarrier::bind(&path, Some(0o600), &[]).expect("socket");
            ready.send(()).expect("ready");
            tokio::task::spawn_local(async move { comm.serve(unix).await });
            let _ = stopped.await;
        });
    });
    started.recv().expect("the comm core starts");
    (socket, stop)
}

#[test]
fn a_pool_this_build_does_not_read_is_said_with_the_shared_memory_crates_words() {
    let (socket, _stop) = old_core();
    let session =
        Session::connect(&Address::Local(socket), "viewer", Options::default()).expect("connect");
    let client = Client::new(session);
    let buffers = Buffers::new(&client, &Topics::new(&client));
    let error = match buffers.subscribe("camera.front", &[], HoldLimits::default()) {
        Err(error) => error,
        Ok(mut subscriber) => loop {
            if let Err(error) = subscriber.wait(Duration::from_millis(50)) {
                break error;
            }
        },
    };
    assert!(
        matches!(&error, TopicError::Client(ClientError::Memory { what: "a buffer pool", why })
            if why.contains("not a pool page")),
        "{error:?}"
    );
}
