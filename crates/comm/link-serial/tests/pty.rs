//! A serial link over a pseudo-terminal: the wire crosses it, it comes back after the device
//! goes away, and a quiet link costs nothing.

use std::cell::RefCell;
use std::fs::File;
use std::path::PathBuf;
use std::rc::Rc;
use std::time::Duration;

use orion_link_serial::{LinkStats, Serial, StreamCarrier};
use orion_measure::sched::thread_cpu_ns;
use orion_measure::{CountingAlloc, thread_counts};
use orion_msg_session::{Hello, Ping, Pong, Welcome};
use orion_server::{Comm, Limits, Roles, Security};
use orion_supervise::{Report, State, Status};
use orion_wire::{Body, Decode, StreamDecoder, encode_body, encode_frame, kind};
use rustix::pty::{OpenptFlags, grantpt, openpt, ptsname, unlockpt};
use tokio::net::unix::pipe;

#[global_allocator]
static GLOBAL: CountingAlloc = CountingAlloc::system();

fn run(test: impl Future<Output = ()>) {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("runtime");
    tokio::task::LocalSet::new().block_on(&runtime, test);
}

/// The device's end of a pseudo-terminal, speaking the wire with fixed buffers.
struct Device {
    rx: pipe::Receiver,
    tx: pipe::Sender,
    decoder: Box<StreamDecoder<1024>>,
    chunk: [u8; 256],
    at: usize,
    len: usize,
    payload: [u8; 512],
    payload_len: usize,
    seq: u16,
}

impl Device {
    /// A new pseudo-terminal: the device's end, and the path of the end a board would open.
    fn pty() -> (Self, PathBuf) {
        let master = openpt(OpenptFlags::RDWR | OpenptFlags::NOCTTY).expect("openpt");
        grantpt(&master).expect("grantpt");
        unlockpt(&master).expect("unlockpt");
        let path = ptsname(&master, Vec::new()).expect("ptsname");
        rustix::io::ioctl_fionbio(&master, true).expect("non-blocking");
        let master = File::from(master);
        let device = Self {
            rx: pipe::Receiver::from_file_unchecked(master.try_clone().expect("dup")).expect("rx"),
            tx: pipe::Sender::from_file_unchecked(master).expect("tx"),
            decoder: Box::new(StreamDecoder::new()),
            chunk: [0; 256],
            at: 0,
            len: 0,
            payload: [0; 512],
            payload_len: 0,
            seq: 0,
        };
        (device, PathBuf::from(path.into_string().expect("path")))
    }

    async fn write(&mut self, mut bytes: &[u8]) {
        while !bytes.is_empty() {
            self.tx.writable().await.expect("writable");
            match self.tx.try_write(bytes) {
                Ok(wrote) => bytes = &bytes[wrote..],
                Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {}
                Err(error) => panic!("device write: {error}"),
            }
        }
    }

    async fn send<B: Body>(&mut self, body: &B) {
        self.seq += 1;
        let (mut frame, mut wire) = ([0_u8; 256], [0_u8; 300]);
        let len = encode_body(body, self.seq, &mut frame).expect("encode");
        let len = encode_frame(&frame[..len], &mut wire).expect("cobs");
        self.write(&wire[..len]).await;
    }

    /// The kind of the next frame from the board; its payload is kept for [`Device::body`].
    async fn next(&mut self) -> u8 {
        loop {
            if self.at == self.len {
                self.rx.readable().await.expect("readable");
                match self.rx.try_read(&mut self.chunk) {
                    Ok(len) => (self.at, self.len) = (0, len),
                    Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => continue,
                    Err(error) => panic!("device read: {error}"),
                }
            }
            let (used, pushed) = self.decoder.push_slice(&self.chunk[self.at..self.len]);
            self.at += used;
            if let Ok(Some(frame)) = pushed {
                self.payload_len = frame.payload().len();
                self.payload[..self.payload_len].copy_from_slice(frame.payload());
                return frame.kind();
            }
        }
    }

    fn body<'a, B: Decode<'a>>(&'a self) -> B {
        B::decode(&self.payload[..self.payload_len]).expect("body")
    }

    async fn hello(&mut self) {
        self.send(&Hello {
            name: "sensor",
            roles: Roles::DEVICE,
            max_frame: 512,
            revision: 1,
        })
        .await;
        assert_eq!(self.next().await, kind::WELCOME);
        let _: Welcome = self.body();
    }

    async fn ping(&mut self, now_ms: u64) {
        self.send(&Ping { now_ms }).await;
        loop {
            // The board pings too; only the answer to ours is waited for.
            if self.next().await == kind::PONG {
                assert_eq!(self.body::<Pong>().now_ms, now_ms);
                return;
            }
        }
    }
}

#[derive(Clone, Default)]
struct States(Rc<RefCell<Vec<Status>>>);

impl Report for States {
    fn report(&mut self, status: &Status) {
        self.0.borrow_mut().push(*status);
    }
}

struct Board {
    comm: Comm,
    stats: Box<dyn Fn() -> LinkStats>,
    states: States,
}

fn board(path: PathBuf, heartbeat_ms: u32) -> Board {
    let comm = Comm::new(
        "board".parse().expect("name"),
        Security::Off,
        None,
        Limits {
            heartbeat_ms,
            hello_ms: 300,
            ..Limits::default()
        },
    );
    let states = States::default();
    let carrier = StreamCarrier::new(
        "uart0".parse().expect("name"),
        Serial {
            path,
            baud: 115_200,
        },
    )
    .reporting(states.clone());
    let stats = Box::new(carrier.stats());
    let serving = comm.clone();
    tokio::task::spawn_local(async move { serving.serve(carrier).await });
    Board {
        comm,
        stats,
        states,
    }
}

async fn until(what: &str, mut done: impl FnMut() -> bool) {
    for _ in 0..2000 {
        if done() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(2)).await;
    }
    panic!("never happened: {what}");
}

#[test]
fn the_wire_crosses_a_serial_port_and_noise_does_not_stop_it() {
    run(async {
        let (mut device, path) = Device::pty();
        let board = board(path, 2000);
        until("the port opens", || (board.stats)().opens == 1).await;

        // Line noise, then a frame with a damaged byte: neither is a connection.
        device.write(&[0x55, 0xAA, 0x13, 0x00, 0x37]).await;
        let (mut frame, mut wire) = ([0_u8; 64], [0_u8; 80]);
        let len = encode_body(&Ping { now_ms: 1 }, 1, &mut frame).expect("encode");
        let len = encode_frame(&frame[..len], &mut wire).expect("cobs");
        wire[3] ^= 0x40;
        device.write(&wire[..len]).await;
        until("the damage is counted", || (board.stats)().dropped == 2).await;
        assert_eq!(
            ((board.stats)().connections, board.comm.stats().opened),
            (0, 0)
        );

        // The device says hello like any client, and is answered.
        device.hello().await;
        for round in 0..50 {
            device.ping(round).await;
        }
        let stats = (board.stats)();
        assert_eq!(
            (stats.connections, stats.opens, stats.frames_rx),
            (1, 1, 51)
        );
        assert!(stats.bytes_tx > 0 && stats.bytes_rx > 0);
        assert_eq!(board.comm.stats().opened, 1);
        assert_eq!(
            board.states.0.borrow().last().map(|status| status.state),
            Some(State::Running)
        );
    });
}

#[test]
fn a_link_is_opened_again_for_as_long_as_it_takes() {
    run(async {
        let dir = std::env::temp_dir().join(format!("orion-link-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).expect("dir");
        let port = dir.join("uart");
        // The device is not plugged in when the board starts.
        let board = board(port.clone(), 2000);
        until("a few tries have failed", || {
            board.states.0.borrow().len() >= 3
        })
        .await;
        {
            let states = board.states.0.borrow();
            assert!(states.iter().all(|status| status.state == State::Retrying));
            // A way in is never backed off: every try is the same short wait after the last.
            let waits: Vec<u64> = states
                .windows(2)
                .map(|pair| {
                    pair[1].retry_at_ms.expect("retry") - pair[0].retry_at_ms.expect("retry")
                })
                .collect();
            assert!(
                waits.iter().all(|wait| (240..400).contains(wait)),
                "{waits:?}"
            );
            assert_eq!(states.last().expect("state").failures, states.len() as u32);
        }
        assert_eq!((board.stats)().opens, 0);

        // It is plugged in.
        let (mut device, slave) = Device::pty();
        std::os::unix::fs::symlink(&slave, &port).expect("symlink");
        until("the port opens", || (board.stats)().opens == 1).await;
        device.hello().await;
        device.ping(1).await;
        assert_eq!(board.comm.with(|sessions, _| sessions.len()), 1);

        // It is pulled out: the session ends, and the board goes back to trying.
        drop(device);
        until("the session ends", || {
            board.comm.with(|sessions, _| sessions.len()) == 0
        })
        .await;
        until("the board is trying again", || {
            board
                .states
                .0
                .borrow()
                .last()
                .is_some_and(|status| status.state == State::Retrying)
        })
        .await;

        // Another is plugged in, as a new port under the same name.
        let (mut device, slave) = Device::pty();
        std::fs::remove_file(&port).expect("unlink");
        std::os::unix::fs::symlink(&slave, &port).expect("symlink");
        until("the port opens again", || (board.stats)().opens == 2).await;
        device.hello().await;
        device.ping(2).await;
        assert_eq!((board.stats)().connections, 2);
        assert_eq!(
            board.states.0.borrow().last().map(|status| status.state),
            Some(State::Running)
        );

        // A device that stops talking loses its session, not the port: the next hello is
        // taken on the same open port.
        until("the quiet session is closed", || {
            board.comm.with(|sessions, _| sessions.len()) == 0
        })
        .await;
        assert_eq!((board.stats)().opens, 2);
        device.hello().await;
        device.ping(3).await;
        assert_eq!(((board.stats)().opens, (board.stats)().connections), (2, 3));
        let _ = std::fs::remove_dir_all(dir);
    });
}

#[test]
fn a_quiet_link_allocates_nothing_and_polls_nothing() {
    run(async {
        let (mut device, path) = Device::pty();
        let board = board(path, 2000);
        until("the port opens", || (board.stats)().opens == 1).await;
        tokio::time::sleep(Duration::from_millis(20)).await;

        // Open, with nothing on the other end saying anything.
        let (allocs, reads, cpu) = (thread_counts(), (board.stats)().reads, thread_cpu_ns());
        tokio::time::sleep(Duration::from_millis(400)).await;
        let allocated = thread_counts().since(allocs);
        let cpu_ms = (thread_cpu_ns() - cpu) / 1_000_000;
        assert_eq!(allocated.allocs, 0, "{allocated:?} on a silent link");
        assert_eq!((board.stats)().reads, reads, "a silent link was read");
        assert_eq!(board.comm.stats().opened, 0, "silence is not a session");
        assert!(
            cpu_ms < 20,
            "{cpu_ms} ms of processor time in 400 ms of silence"
        );

        // Connected, with nothing to say but heartbeats.
        device.hello().await;
        for round in 0..200 {
            device.ping(round).await;
        }
        let (allocs, reads) = (thread_counts(), (board.stats)().reads);
        for round in 0..2000 {
            device.ping(round).await;
        }
        let allocated = thread_counts().since(allocs);
        assert_eq!(allocated.allocs, 0, "{allocated:?} in 2000 heartbeats");
        // A read per frame at most: the port is read when it has bytes, not to see if it has.
        let read = (board.stats)().reads - reads;
        assert!(read <= 2000, "{read} reads for 2000 frames");

        // And connected with nothing to say at all, between heartbeats.
        let (allocs, reads, cpu) = (thread_counts(), (board.stats)().reads, thread_cpu_ns());
        tokio::time::sleep(Duration::from_millis(400)).await;
        let allocated = thread_counts().since(allocs);
        let cpu_ms = (thread_cpu_ns() - cpu) / 1_000_000;
        assert_eq!(allocated.allocs, 0, "{allocated:?} on an idle session");
        assert_eq!((board.stats)().reads, reads);
        assert!(cpu_ms < 20, "{cpu_ms} ms of processor time in 400 ms idle");
    });
}
