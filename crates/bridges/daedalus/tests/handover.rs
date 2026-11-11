//! A frame handed to Daedalus is the publisher's memory, not a copy of it: the node is run
//! here without a host, and what it hands over is in the pool's shared memory.

use std::path::{Path, PathBuf};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use daedalus_abi::{OWNER_HANDED, Slot, SlotState, Slots, StepCx, StepEnd};
use orion::buffers::Pool;
use orion::topics::FrameTime;
use orion::{Address, Orion};
use orion_comm_app::{CommCore, CommSettings};
use orion_daedalus::NODES;
use tokio::sync::oneshot;

const WAIT: Duration = Duration::from_secs(60);
const SIZE: usize = 4096;

fn until(what: &str, mut done: impl FnMut() -> bool) {
    let deadline = Instant::now() + WAIT;
    while !done() {
        assert!(Instant::now() < deadline, "timed out waiting for {what}");
        std::thread::sleep(Duration::from_millis(5));
    }
}

fn temp(name: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!("orion-daedalus-{name}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).expect("dir");
    dir
}

/// A comm core in a thread of its own, until dropped.
struct Comm {
    stop: Option<oneshot::Sender<()>>,
    thread: Option<JoinHandle<()>>,
}

impl Comm {
    fn start(dir: &Path) -> Self {
        let text = format!(
            "node.name = board\nsocket = {0}/orion.sock\nstate.dir = {0}/state\n",
            dir.display()
        );
        let mut source = orion_config::Source::parse(&text, Vec::new()).expect("source");
        let settings = CommSettings::from_source(&mut source).expect("settings");
        let (stop, stopped) = oneshot::channel::<()>();
        let thread = std::thread::spawn(move || {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("runtime");
            tokio::task::LocalSet::new().block_on(&runtime, async move {
                let core = CommCore::start(settings)
                    .await
                    .expect("the comm core starts");
                let _ = stopped.await;
                drop(core);
            });
        });
        Self {
            stop: Some(stop),
            thread: Some(thread),
        }
    }
}

impl Drop for Comm {
    fn drop(&mut self) {
        if let (Some(stop), Some(thread)) = (self.stop.take(), self.thread.take()) {
            let _ = stop.send(());
            let _ = thread.join();
        }
    }
}

#[test]
fn a_frame_is_handed_over_where_it_is_in_shared_memory() {
    let dir = temp("handover");
    let comm = Comm::start(&dir);
    let socket = dir.join("orion.sock");
    until("the comm core's socket", || socket.exists());
    // SAFETY: this test binary runs this one test, and nothing else reads the environment
    // while it is set.
    unsafe { std::env::set_var("ORION_ADDRESS", &socket) };
    let address: Address = socket.to_str().expect("path").parse().expect("address");
    let orion = Orion::connect(&address, "camera").expect("connect");
    let mut camera = orion
        .declare_buffers("camera", 4, 16, Pool::in_memory(1, 2, SIZE).expect("pool"))
        .expect("declare");

    let frame = NODES
        .iter()
        .find(|node| node.id == "orion.frame.read")
        .expect("the frame node");
    let topic = b"camera";
    let mut cells = [
        Slot {
            state: SlotState::Full as u32,
            owner: 1,
            data: topic.as_ptr().cast_mut(),
            len: topic.len(),
            cap: topic.len(),
            ..Slot::EMPTY
        },
        Slot::EMPTY,
    ];
    let mut cx = StepCx::new();
    let mut sent = 0_u64;
    until("a frame to be handed over", || {
        let _ = orion.pump(Duration::ZERO);
        camera.poll().expect("poll");
        if let Ok(buffer) = camera.take() {
            sent += 1;
            camera.buffer_mut(buffer).expect("memory").fill(7);
            camera
                .publish(&[buffer], FrameTime::from_nanos(sent), &[])
                .expect("publish");
        }
        cells[1] = Slot::EMPTY;
        let end = (frame.run)(&mut Slots::of(&mut cells), &mut cx);
        assert!(matches!(end, StepEnd::Done), "the step faulted");
        cells[1].state == SlotState::Full as u32
    });

    let out = cells[1];
    let handed = cx.handover[0];
    assert_eq!(out.owner, OWNER_HANDED);
    assert_eq!(out.data.cast_const(), handed.ptr);
    assert_eq!(handed.len, SIZE);
    // SAFETY: the handed bytes stay valid until `release` is called, below.
    let seen = || unsafe { std::slice::from_raw_parts(handed.ptr, handed.len) }.to_vec();
    assert!(seen().iter().all(|byte| *byte == 7));

    // The handed bytes are the pool's shared memory, mapped from its memfd: not a copy on the
    // heap.
    let at = handed.ptr as usize;
    let maps = std::fs::read_to_string("/proc/self/maps").expect("maps");
    let region = maps
        .lines()
        .find(|line| {
            let (start, end) = line
                .split_whitespace()
                .next()
                .and_then(|range| range.split_once('-'))
                .expect("range");
            let start = usize::from_str_radix(start, 16).expect("start");
            let end = usize::from_str_radix(end, 16).expect("end");
            (start..end).contains(&at)
        })
        .expect("the bytes are mapped");
    assert!(
        region.contains("memfd:orion-ring"),
        "the frame was copied: {region}"
    );

    // Given back once, by the release the plugin named.
    let release = handed.release.expect("a release");
    // SAFETY: `data` is the plugin's, given back once, as the step's contract says.
    unsafe { release(handed.data) };
    drop(orion);
    drop(comm);
    let _ = std::fs::remove_dir_all(&dir);
}
