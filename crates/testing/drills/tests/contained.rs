//! The failure rule: one misbehaving client, peer, provider or link is contained. Each drill
//! runs the board's ordinary traffic (a publisher and a subscriber that checks every value
//! arrives in order), lets one neighbour misbehave, and checks that the traffic, the command
//! line and the comm core never noticed.

mod support;

use std::io::{ErrorKind, Read, Write};
use std::net::TcpStream;
use std::os::unix::net::UnixStream;
use std::time::{Duration, Instant};

use orion::topics::FieldId;
use orion::{Address, Orion};
use support::{Board, Proc, clean};

const LONG: Duration = Duration::from_secs(10);

/// Bytes that are no frame: a counter run through a cheap mixer, so no run of them repeats.
fn garbage(len: usize) -> Vec<u8> {
    let mut state = 0x9e37_79b9_u32;
    (0..len)
        .map(|_| {
            state ^= state << 13;
            state ^= state >> 17;
            state ^= state << 5;
            state as u8
        })
        .collect()
}

/// The board's ordinary traffic is still flowing, whole, and the command line still works.
fn unharmed(board: &Board, subscriber: &Proc, comm: &mut Proc) {
    let seen = subscriber.last("seen");
    subscriber.wait_for("seen", seen + 200, LONG);
    let (ok, show) = board.ctl(&["node", "show"]);
    assert!(ok, "{show}");
    assert!(comm.running(), "the comm core is up");
    clean(subscriber);
    assert_eq!(subscriber.count("reconnected"), 0);
}

#[test]
fn a_bad_client_is_contained() {
    let board = Board::new("client", "");
    let mut comm = board.comm();
    let _node = board.node();
    let _publisher = board.role("publisher", &[]);
    let subscriber = board.role("subscriber", &[]);
    subscriber.wait_for("seen", 100, LONG);

    // One sends bytes that are no frame.
    let mut noise = UnixStream::connect(&board.socket).expect("connect");
    let _ = noise.write_all(&garbage(256 * 1024));
    // One connects and never says who it is.
    let _silent = UnixStream::connect(&board.socket).expect("connect");
    // One subscribes and never reads again.
    let stalled = board.role("subscriber", &["stall"]);
    stalled.wait_line("stalled", LONG);
    // And many connect at once and go.
    for _ in 0..200 {
        drop(UnixStream::connect(&board.socket));
    }
    unharmed(&board, &subscriber, &mut comm);

    // The one that sent noise was let go.
    noise
        .set_read_timeout(Some(Duration::from_secs(2)))
        .expect("timeout");
    let mut rest = Vec::new();
    match noise.read_to_end(&mut rest) {
        Ok(_) => {}
        Err(error) => assert_ne!(error.kind(), ErrorKind::WouldBlock, "still connected"),
    }
}

#[test]
fn a_bad_peer_is_contained() {
    let board = Board::new("peer", "");
    let mut comm = board.comm();
    let _node = board.node();
    let _publisher = board.role("publisher", &[]);
    let subscriber = board.role("subscriber", &[]);
    subscriber.wait_for("seen", 100, LONG);
    // A second board reads the first's topic, as a good peer.
    let other = Board::named(
        "peer-other",
        "other",
        &format!("peer.nodes = drill={}\n", board.peer),
    );
    let _other_comm = other.comm();
    let remote = other.role("subscriber", &["drill/drill.count"]);
    remote.wait_for("seen", 100, LONG);

    // A peer that sends noise, one that connects and says nothing, and one that sends a
    // hello's worth of noise and then stops reading.
    let mut noise = TcpStream::connect(board.peer).expect("connect");
    let _ = noise.write_all(&garbage(256 * 1024));
    let _silent = TcpStream::connect(board.peer).expect("connect");
    let mut deaf = TcpStream::connect(board.peer).expect("connect");
    let _ = deaf.write_all(&garbage(64));
    for _ in 0..100 {
        drop(TcpStream::connect(board.peer));
    }
    unharmed(&board, &subscriber, &mut comm);
    let seen = remote.last("seen");
    remote.wait_for("seen", seen + 200, LONG);
    clean(&remote);
}

#[test]
fn a_bad_provider_is_contained() {
    let board = Board::new("provider", "");
    let mut comm = board.comm();
    let _node = board.node();
    let _publisher = board.role("publisher", &[]);
    let subscriber = board.role("subscriber", &[]);
    let mut provider = board.role("provider", &[]);
    provider.wait_line("registered", LONG);
    subscriber.wait_for("seen", 100, LONG);

    // An action that never answers ends at its deadline, and does not hold up another.
    let asked = Instant::now();
    let (ok, stuck) = board.ctl(&["action", "run", "drill.stuck", "--for", "300"]);
    assert!(!ok, "{stuck}");
    assert!(
        asked.elapsed() < Duration::from_secs(3),
        "{:?}",
        asked.elapsed()
    );
    let (ok, echoed) = board.ctl(&["action", "run", "drill.echo", "--args", "fine"]);
    assert!(ok && echoed.contains("fine"), "{echoed}");

    // A provider that dies in the middle of a run: the run ends, it does not hang.
    let asked = Instant::now();
    let (ok, crashed) = board.ctl(&["action", "run", "drill.crash", "--for", "5000"]);
    assert!(!ok, "{crashed}");
    assert!(!provider.running(), "the provider exited");
    eprintln!(
        "a run whose provider died ended after {:?}: {crashed}",
        asked.elapsed()
    );
    unharmed(&board, &subscriber, &mut comm);
}

#[test]
fn a_bad_link_is_contained() {
    let (noisy, noisy_path) = link::pty();
    let (good, good_path) = link::pty();
    let board = Board::new(
        "link",
        &format!(
            "links.serial = noisy={}@115200,good={}@115200\n",
            noisy_path.display(),
            good_path.display()
        ),
    );
    let mut comm = board.comm();
    let _node = board.node();
    let _publisher = board.role("publisher", &[]);
    let subscriber = board.role("subscriber", &[]);
    let device = link::Device::start(good);
    subscriber.wait_for("seen", 100, LONG);

    let orion = Orion::connect(&Address::Local(board.socket.clone()), "drill").expect("connect");
    let mut reading = orion.subscribe(link::TOPIC).expect("subscribe");
    let value = |reading: &mut orion::topics::Subscriber| {
        let _ = reading.wait(Duration::from_millis(100));
        while !matches!(reading.next(), orion::topics::Next::Empty) {}
        reading
            .latest(FieldId(0))
            .map(|sample| f64::from_le_bytes(sample.payload.try_into().unwrap_or_default()))
    };
    let deadline = Instant::now() + LONG;
    while value(&mut reading).is_none() {
        assert!(Instant::now() < deadline, "the device's topic never came");
    }

    // The comm core has the other link open, and it floods noise for a while.
    let opened = |path: &std::path::Path| {
        std::fs::read_dir(format!("/proc/{}/fd", comm.pid()))
            .map(|fds| {
                fds.filter_map(Result::ok)
                    .any(|fd| std::fs::read_link(fd.path()).is_ok_and(|to| to == path))
            })
            .unwrap_or(false)
    };
    assert!(
        opened(&noisy_path) && opened(&good_path),
        "both links are open"
    );
    let flood = link::flood(noisy, Duration::from_secs(1));
    let before = value(&mut reading).expect("a value");
    std::thread::sleep(Duration::from_millis(300));
    let during = value(&mut reading).expect("a value");
    assert!(
        during > before,
        "the device went on: {before} then {during}"
    );
    flood.join().expect("flood");
    unharmed(&board, &subscriber, &mut comm);
    drop(device);
}

/// The device side of serial links, on pseudo-terminals.
mod link {
    use std::fs::File;
    use std::io::{ErrorKind, Read, Write};
    use std::path::PathBuf;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::thread::JoinHandle;
    use std::time::{Duration, Instant};

    use orion_device_core::{DeviceConfig, StreamDevice};
    use orion_device_topics::{Field, FieldType, TopicSpec, Topics};
    use rustix::pty::{OpenptFlags, grantpt, openpt, ptsname, unlockpt};

    pub const TOPIC: &str = "drill-device.count";

    static TOPICS: [TopicSpec; 1] = [TopicSpec::latest(
        TOPIC,
        &[Field::new("count", FieldType::Float, "")],
    )];

    /// A pseudo-terminal: the device's end, and the path of the end the board opens.
    pub fn pty() -> (File, PathBuf) {
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

    /// Writes noise into `port` for `long`, reading whatever comes back.
    pub fn flood(mut port: File, long: Duration) -> JoinHandle<()> {
        std::thread::spawn(move || {
            let noise = super::garbage(4096);
            let started = Instant::now();
            let mut sink = [0_u8; 4096];
            while started.elapsed() < long {
                let _ = port.write(&noise);
                let _ = port.read(&mut sink);
                std::thread::sleep(Duration::from_millis(1));
            }
        })
    }

    /// A device counting on its topic every 5 ms while anyone listens, until dropped.
    pub struct Device {
        stop: Arc<AtomicBool>,
        thread: Option<JoinHandle<()>>,
    }

    impl Device {
        pub fn start(port: File) -> Self {
            let stop = Arc::new(AtomicBool::new(false));
            let stopping = stop.clone();
            let thread = std::thread::spawn(move || run(port, &stopping));
            Self {
                stop,
                thread: Some(thread),
            }
        }
    }

    impl Drop for Device {
        fn drop(&mut self) {
            self.stop.store(true, Ordering::Relaxed);
            if let Some(thread) = self.thread.take() {
                let _ = thread.join();
            }
        }
    }

    fn run(mut port: File, stop: &AtomicBool) {
        let mut device = StreamDevice::<512, 512>::new(DeviceConfig::new("drill-device"));
        let mut topics = Topics::new(&TOPICS);
        let started = Instant::now();
        let (mut count, mut next) = (0.0_f64, 0_u32);
        let (mut rx, mut tx) = ([0_u8; 256], [0_u8; 256]);
        while !stop.load(Ordering::Relaxed) {
            let now = u32::try_from(started.elapsed().as_millis()).unwrap_or(u32::MAX);
            match port.read(&mut rx) {
                Ok(n) => device.receive(&rx[..n], |_, frame| {
                    topics.handle(frame);
                }),
                Err(error) if error.kind() == ErrorKind::WouldBlock => {}
                Err(_) => return,
            }
            device.link.poll(now);
            while device.link.next_event().is_some() {}
            topics.poll(&mut device.link, now);
            if topics.subscribers(0) > 0
                && now >= next
                && topics
                    .publish_f64(&mut device.link, 0, 0, u64::from(now) * 1_000_000, count)
                    .is_ok()
            {
                next = now + 5;
                count += 1.0;
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
                        Err(_) => return,
                    }
                }
            }
            std::thread::sleep(Duration::from_millis(1));
        }
    }
}
