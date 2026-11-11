//! A microcontroller, end to end. The firmware is built from the device crates, as a real
//! one is, and runs on one end of a pseudo-terminal; the board serves the other end as a
//! serial link, with node logic. An ordinary client, which knows nothing of devices, finds
//! it as a resource, reads its topic and runs its action. When the device goes, its
//! resource is lost, not gone; when it comes back, it is ready again.

use std::fs::File;
use std::io::{ErrorKind, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use orion::actions::{ActionCall, Outcome, Target, Update};
use orion::provider::{Found, ResourceState};
use orion::topics::Next;
use orion::{Address, Orion};
use orion_device_actions::{ActionSpec, Actions, Admits, Heard as Run, Policy};
use orion_device_core::{DeviceConfig, StreamDevice};
use orion_device_topics::{Field, FieldType, TopicSpec, Topics};
use orion_node_app::{Node, NodeSettings};
use orion_stubs::Board;
use rustix::pty::{OpenptFlags, grantpt, openpt, ptsname, unlockpt};
use tokio::sync::oneshot;

static TOPICS: [TopicSpec; 1] = [TopicSpec::latest(
    "imu-board.yaw",
    &[Field::new("yaw", FieldType::Float, "deg")],
)];

static ACTIONS: [ActionSpec; 1] = [ActionSpec {
    name: "zero",
    target: orion_device_actions::Target::Resource("imu-board"),
    policy: Policy::new(Admits::LOCAL),
    description: "Sets yaw to zero",
}];

/// The firmware, in a thread of its own on the device's end of a pseudo-terminal, until
/// dropped. It publishes its yaw every few milliseconds while anyone listens, and its
/// action sets the yaw to zero.
struct Firmware {
    stop: Arc<AtomicBool>,
    zeroed: Arc<AtomicU32>,
    thread: Option<JoinHandle<()>>,
}

impl Firmware {
    fn start(master: File) -> Self {
        let stop = Arc::new(AtomicBool::new(false));
        let zeroed = Arc::new(AtomicU32::new(0));
        let (stopping, zeroing) = (stop.clone(), zeroed.clone());
        let thread = std::thread::spawn(move || run(master, &stopping, &zeroing));
        Self {
            stop,
            zeroed,
            thread: Some(thread),
        }
    }
}

impl Drop for Firmware {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}

/// A firmware's main loop, as one on a microcontroller is written: feed what arrived, poll
/// with the millisecond counter, send what is handed out.
fn run(mut port: File, stop: &AtomicBool, zeroed: &AtomicU32) {
    let mut device = StreamDevice::<512, 512>::new(DeviceConfig::new("imu-board"));
    let mut topics = Topics::new(&TOPICS);
    let mut actions = Actions::new(&ACTIONS);
    let started = Instant::now();
    let mut yaw = 10.0_f64;
    let mut runs: Vec<u64> = Vec::new();
    let mut next_sample = 0_u32;
    let (mut rx, mut tx) = ([0_u8; 256], [0_u8; 256]);
    while !stop.load(Ordering::Relaxed) {
        let now = u32::try_from(started.elapsed().as_millis()).unwrap_or(u32::MAX);
        match port.read(&mut rx) {
            Ok(n) => device.receive(&rx[..n], |_, frame| {
                topics.handle(frame);
                if let Run::Run { call, .. } = actions.handle(frame) {
                    runs.push(call);
                }
            }),
            Err(error) if error.kind() == ErrorKind::WouldBlock => {}
            Err(error) => panic!("the device's port: {error}"),
        }
        device.link.poll(now);
        while device.link.next_event().is_some() {}
        topics.poll(&mut device.link, now);
        actions.poll(&mut device.link, now);
        if let Some(&call) = runs.first() {
            yaw = 0.0;
            let finished =
                actions.finish(&mut device.link, call, Outcome::Succeeded, b"zeroed", "");
            if finished.is_ok() {
                runs.remove(0);
                zeroed.fetch_add(1, Ordering::Relaxed);
            }
        }
        if topics.subscribers(0) > 0 && now >= next_sample {
            let time = u64::from(now) * 1_000_000;
            if topics
                .publish_f64(&mut device.link, 0, 0, time, yaw)
                .is_ok()
            {
                next_sample = now + 5;
                yaw += 1.0;
            }
        }
        loop {
            let n = device.transmit(&mut tx);
            if n == 0 {
                break;
            }
            let mut out = &tx[..n];
            while !out.is_empty() {
                match port.write(out) {
                    Ok(wrote) => out = &out[wrote..],
                    Err(error) if error.kind() == ErrorKind::WouldBlock => {
                        std::thread::sleep(Duration::from_millis(1));
                    }
                    Err(error) => panic!("the device's port: {error}"),
                }
            }
        }
        std::thread::sleep(Duration::from_millis(1));
    }
}

/// A pseudo-terminal: the device's end, and the path of the end the board opens.
fn pty() -> (File, PathBuf) {
    let master = openpt(OpenptFlags::RDWR | OpenptFlags::NOCTTY).expect("openpt");
    grantpt(&master).expect("grantpt");
    unlockpt(&master).expect("unlockpt");
    let path = ptsname(&master, Vec::new()).expect("ptsname");
    rustix::io::ioctl_fionbio(&master, true).expect("non-blocking");
    (
        File::from(master),
        PathBuf::from(path.into_string().expect("path")),
    )
}

fn dir(name: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!("orion-device-{name}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).expect("directory");
    dir
}

fn settings(dir: &Path) -> NodeSettings {
    let mut source = orion_config::Source::parse("", []).expect("no settings");
    let mut settings = NodeSettings::from_source(&mut source).expect("defaults");
    settings.config.socket = dir.join("orion.sock");
    settings.config.state_dir = dir.join("state");
    settings.files_dir = dir.join("files");
    settings.audit = Some(orion_audit::AuditConfig::new(&dir.join("state/audit.log")));
    settings.config.node_name = "board".parse().expect("name");
    settings
}

/// Node logic in a thread of its own, until dropped.
struct Logic {
    kill: Option<oneshot::Sender<()>>,
    thread: Option<JoinHandle<()>>,
}

impl Logic {
    fn start(settings: NodeSettings) -> Self {
        let (kill, killed) = oneshot::channel::<()>();
        let thread = std::thread::spawn(move || {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("runtime");
            runtime.block_on(async move {
                let mut node = Node::load(settings).expect("node logic starts");
                tokio::select! {
                    () = node.run(std::future::pending()) => {}
                    _ = killed => {}
                }
            });
        });
        Self {
            kill: Some(kill),
            thread: Some(thread),
        }
    }
}

impl Drop for Logic {
    fn drop(&mut self) {
        if let (Some(kill), Some(thread)) = (self.kill.take(), self.thread.take()) {
            let _ = kill.send(());
            let _ = thread.join();
        }
    }
}

fn until<T>(what: &str, mut done: impl FnMut() -> Option<T>) -> T {
    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        if let Some(value) = done() {
            return value;
        }
        assert!(Instant::now() < deadline, "{what}");
        std::thread::sleep(Duration::from_millis(20));
    }
}

/// The device's resource once it is in `state` with its topic and action listed.
fn resource(dashboard: &Orion, state: ResourceState) -> Found {
    until(&format!("the device listed as {state:?}"), || {
        dashboard
            .resources("imu-board")
            .ok()?
            .into_iter()
            .find(|found| {
                found.state == state && !found.topics.is_empty() && !found.actions.is_empty()
            })
    })
}

#[test]
fn a_device_on_a_link_is_a_resource_an_ordinary_client_reads_and_drives() {
    let dir = dir("e2e");
    let (port, link) = pty();
    let board = Board::start_for_node_with_links(&dir, &[link]);
    let _logic = Logic::start(settings(&dir));
    let firmware = Firmware::start(port.try_clone().expect("dup"));
    let address = Address::Local(board.socket().to_owned());
    let dashboard = Orion::connect(&address, "dashboard").expect("connect");

    // Found, by a program that has never heard of the device's kind.
    let found = resource(&dashboard, ResourceState::Ready);
    assert_eq!(
        (found.name.as_str(), found.kind.as_str()),
        ("imu-board", "device")
    );
    assert_eq!(found.node, "board");
    assert_eq!(found.topics, ["imu-board.yaw"]);
    assert_eq!(found.actions, ["imu-board.zero"]);

    // Read: its topic, as any topic. The device publishes only once someone listens.
    let mut yaw = dashboard.subscribe("imu-board.yaw").expect("subscribe");
    let mut read = Vec::new();
    until("samples from the device", || {
        let _ = yaw.wait(Duration::from_millis(200));
        // A latest-value topic keeps two: a reader that is behind skips to the newest.
        loop {
            match yaw.next() {
                Next::Sample(sample) => {
                    read.push(f64::from_le_bytes(
                        sample.payload.try_into().expect("a float"),
                    ));
                }
                Next::Skipped(_) => {}
                Next::Empty => break,
            }
        }
        (read.len() >= 3).then_some(())
    });
    assert!(read.windows(2).all(|pair| pair[1] > pair[0]), "{read:?}");

    // Driven: its action, as any action.
    let mut run = dashboard
        .run(&ActionCall {
            name: "zero",
            target: Target::Resource("imu-board".parse().expect("name")),
            args: &[],
            deadline_ms: 5_000,
            detached: false,
        })
        .expect("run");
    let finished = until("the action's result", || {
        match run.wait(Duration::from_millis(200)) {
            Ok(Some(Update::Finished(finished))) => Some(finished),
            _ => None,
        }
    });
    assert_eq!(finished.outcome, Outcome::Succeeded);
    assert_eq!(finished.output, b"zeroed");
    assert_eq!(firmware.zeroed.load(Ordering::Relaxed), 1);

    // Gone: lost, not gone, so whoever used it sees why.
    drop(firmware);
    let lost = resource(&dashboard, ResourceState::Lost);
    assert_eq!(lost.topics, ["imu-board.yaw"]);

    // Back: ready again with what it declares now.
    let _firmware = Firmware::start(port);
    resource(&dashboard, ResourceState::Ready);
}
