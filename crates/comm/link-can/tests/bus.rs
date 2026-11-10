//! A CAN bus in memory: many devices, a session each, and a bus that comes back.

use std::cell::{Cell, RefCell};
use std::collections::VecDeque;
use std::io;
use std::rc::Rc;
use std::time::Duration;

use orion_link_can::{BusStats, CanBus, CanCarrier, CanFrame, CanIo, OpenCan};
use orion_msg_session::{Hello, Ping, Pong};
use orion_server::{Comm, Limits, Principal, Roles, Security};
use orion_supervise::{Report, State, Status};
use orion_wire::{Body, CanLinkIds, Decode, Reassembler, SegmentMtu, Segmenter, encode_body, kind};
use tokio::sync::Notify;

fn run(test: impl Future<Output = ()>) {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .start_paused(true)
        .build()
        .expect("runtime");
    tokio::task::LocalSet::new().block_on(&runtime, test);
}

/// The wires of a bus, as the board's socket and the devices see them.
#[derive(Default)]
struct Wires {
    to_board: RefCell<VecDeque<CanFrame>>,
    to_devices: RefCell<VecDeque<CanFrame>>,
    board_wake: Notify,
    device_wake: Notify,
    /// The interface is down: the socket fails and cannot be opened.
    down: Cell<bool>,
    opened_for: RefCell<Vec<CanLinkIds>>,
}

struct BoardSocket(Rc<Wires>);

impl CanIo for BoardSocket {
    async fn recv(&self) -> io::Result<CanFrame> {
        loop {
            if self.0.down.get() {
                return Err(io::Error::other("network is down"));
            }
            if let Some(frame) = self.0.to_board.borrow_mut().pop_front() {
                return Ok(frame);
            }
            self.0.board_wake.notified().await;
        }
    }

    async fn send(&self, frame: &CanFrame) -> io::Result<()> {
        if self.0.down.get() {
            return Err(io::Error::other("network is down"));
        }
        self.0.to_devices.borrow_mut().push_back(*frame);
        self.0.device_wake.notify_waiters();
        Ok(())
    }
}

struct Interface(Rc<Wires>);

impl OpenCan for Interface {
    type Io = BoardSocket;

    async fn open(&mut self, ids: &[CanLinkIds]) -> io::Result<BoardSocket> {
        if self.0.down.get() {
            return Err(io::Error::other("no such device"));
        }
        *self.0.opened_for.borrow_mut() = ids.to_vec();
        Ok(BoardSocket(self.0.clone()))
    }

    fn describe(&self) -> String {
        "test bus".into()
    }
}

/// One device on the bus.
struct Device {
    wires: Rc<Wires>,
    ids: CanLinkIds,
    mtu: SegmentMtu,
    reassembler: Box<Reassembler<1024>>,
    payload: Vec<u8>,
    seq: u16,
    /// Drops the segment with this number of the next message sent.
    lose: Option<usize>,
}

impl Device {
    fn new(wires: &Rc<Wires>, base: CanLinkIds, address: u32, fd: bool) -> Self {
        Self {
            wires: wires.clone(),
            ids: CanLinkIds::for_address(base, address).expect("ids"),
            mtu: if fd {
                SegmentMtu::FD
            } else {
                SegmentMtu::CLASSIC
            },
            reassembler: Box::new(Reassembler::new()),
            payload: Vec::new(),
            seq: 0,
            lose: None,
        }
    }

    fn send<B: Body>(&mut self, body: &B) {
        self.seq += 1;
        let mut frame = [0_u8; 256];
        let len = encode_body(body, self.seq, &mut frame).expect("encode");
        let mut segments = Segmenter::for_frame(&frame[..len], self.mtu);
        let lose = self.lose.take();
        let mut number = 0;
        let mut out = CanFrame {
            id: self.ids.device_to_host,
            extended: self.ids.extended,
            len: 0,
            data: [0; 64],
        };
        while let Some(len) = segments.next_into(&mut out.data).expect("segment") {
            out.len = len as u8;
            if lose != Some(number) {
                self.wires.to_board.borrow_mut().push_back(out);
            }
            number += 1;
        }
        self.wires.board_wake.notify_one();
    }

    /// The kind of the next frame the board sends this device.
    async fn next(&mut self) -> u8 {
        loop {
            let mine = {
                let mut bus = self.wires.to_devices.borrow_mut();
                let at = bus
                    .iter()
                    .position(|frame| frame.id == self.ids.host_to_device);
                at.and_then(|at| bus.remove(at))
            };
            let Some(segment) = mine else {
                self.wires.device_wake.notified().await;
                continue;
            };
            assert_eq!(segment.extended, self.ids.extended);
            if let Ok(Some(frame)) = self.reassembler.push(segment.bytes()) {
                self.payload = frame.payload().to_vec();
                return frame.kind();
            }
        }
    }

    async fn hello(&mut self, name: &str) {
        self.send(&Hello {
            name,
            roles: Roles::DEVICE,
            max_frame: 1024,
            revision: 1,
        });
        assert_eq!(self.next().await, kind::WELCOME);
    }

    async fn ping(&mut self, now_ms: u64) {
        self.send(&Ping { now_ms });
        loop {
            if self.next().await == kind::PONG {
                assert_eq!(Pong::decode(&self.payload).expect("pong").now_ms, now_ms);
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
    wires: Rc<Wires>,
    stats: Box<dyn Fn() -> BusStats>,
    states: States,
}

const BASE: CanLinkIds = CanLinkIds::new(0x100, 0x180, false);

fn bus(fd: bool) -> CanBus {
    CanBus {
        name: "can0".into(),
        base: BASE,
        addresses: vec![1, 2, 5],
        fd,
    }
}

fn board(fd: bool) -> Board {
    let comm = Comm::new(
        "board".parse().expect("name"),
        Security::Off,
        None,
        Limits::default(),
    );
    let wires = Rc::new(Wires::default());
    let states = States::default();
    let carrier = CanCarrier::new(&bus(fd), Interface(wires.clone()))
        .expect("bus")
        .reporting(states.clone());
    let stats = Box::new(carrier.stats());
    let serving = comm.clone();
    tokio::task::spawn_local(async move { serving.serve(carrier).await });
    Board {
        comm,
        wires,
        stats,
        states,
    }
}

fn devices(board: &Board) -> Vec<String> {
    let mut names: Vec<String> = board.comm.with(|sessions, _| {
        sessions
            .iter()
            .filter(|session| session.open)
            .map(|session| match &session.principal {
                Principal::Device(name) => format!("{name}={}", session.name),
                other => format!("{other:?}"),
            })
            .collect()
    });
    names.sort();
    names
}

#[test]
fn every_device_on_a_bus_is_its_own_session() {
    for fd in [false, true] {
        run(async move {
            let board = board(fd);
            let mut arm = Device::new(&board.wires, BASE, 1, fd);
            let mut wrist = Device::new(&board.wires, BASE, 5, fd);
            arm.hello("arm").await;
            wrist.hello("wrist").await;
            assert_eq!(devices(&board), ["can0.1=arm", "can0.5=wrist"]);
            // The socket was asked for these devices' identifiers and no others.
            assert_eq!(
                board
                    .wires
                    .opened_for
                    .borrow()
                    .iter()
                    .map(|ids| ids.device_to_host)
                    .collect::<Vec<_>>(),
                [0x101, 0x102, 0x105]
            );
            for round in 0..100 {
                arm.ping(round).await;
                wrist.ping(1000 + round).await;
            }
            let stats = (board.stats)();
            assert_eq!(
                (
                    stats.connections,
                    stats.frames_rx,
                    stats.broken,
                    stats.opens
                ),
                (2, 202, 0, 1)
            );
            // Every frame took at least one CAN frame each way, and none was sent in vain.
            assert!(
                stats.segments_rx >= 202 && stats.segments_tx >= 202,
                "{stats:?}"
            );

            // A segment lost on the bus costs that message and nothing after it.
            arm.lose = Some(1);
            arm.send(&Hello {
                name: "a message long enough to need several segments on any bus, classic or FD alike",
                roles: Roles::DEVICE,
                max_frame: 1024,
                revision: 1,
            });
            arm.ping(7).await;
            assert_eq!((board.stats)().broken, 1);

            // An identifier that is nobody's, and an address not on this bus, are not sessions.
            let mut stranger = Device::new(&board.wires, BASE, 9, fd);
            stranger.send(&Ping { now_ms: 1 });
            wrist.ping(8).await;
            assert!((board.stats)().foreign >= 1);
            assert_eq!(devices(&board).len(), 2);

            // A device that stops talking loses its session; the other is not touched.
            for round in 0..40 {
                tokio::time::sleep(Duration::from_millis(500)).await;
                wrist.ping(round).await;
            }
            assert_eq!(devices(&board), ["can0.5=wrist"]);
            // And it is taken back when it speaks again.
            arm.hello("arm").await;
            assert_eq!(devices(&board), ["can0.1=arm", "can0.5=wrist"]);
            assert_eq!((board.stats)().connections, 3);
        });
    }
}

#[test]
fn a_bus_that_goes_away_is_opened_again_for_as_long_as_it_takes() {
    run(async {
        let board = board(false);
        // The interface is not there when the board starts.
        board.wires.down.set(true);
        tokio::time::sleep(Duration::from_secs(5)).await;
        {
            let states = board.states.0.borrow();
            // A way in is never backed off: a try every 250 ms, however long it has failed.
            assert!(
                (19..=21).contains(&states.len()),
                "{} tries in 5 s",
                states.len()
            );
            assert!(states.iter().all(|status| status.state == State::Retrying));
        }
        board.wires.down.set(false);
        tokio::time::sleep(Duration::from_millis(300)).await;
        assert_eq!((board.stats)().opens, 1);

        let mut arm = Device::new(&board.wires, BASE, 1, false);
        let mut wrist = Device::new(&board.wires, BASE, 2, false);
        arm.hello("arm").await;
        wrist.hello("wrist").await;
        assert_eq!(devices(&board).len(), 2);

        // The interface goes down under two sessions: both end, and the board tries again.
        board.wires.down.set(true);
        board.wires.board_wake.notify_one();
        tokio::time::sleep(Duration::from_secs(1)).await;
        assert_eq!(devices(&board).len(), 0);
        assert_eq!(
            board.states.0.borrow().last().map(|status| status.state),
            Some(State::Retrying)
        );

        // It comes back, and so do the devices.
        board.wires.down.set(false);
        board.wires.to_devices.borrow_mut().clear();
        tokio::time::sleep(Duration::from_millis(300)).await;
        assert_eq!((board.stats)().opens, 2);
        arm.hello("arm").await;
        arm.ping(1).await;
        assert_eq!(devices(&board), ["can0.1=arm"]);
        assert_eq!(
            board.states.0.borrow().last().map(|status| status.state),
            Some(State::Running)
        );
    });
}

#[test]
fn a_bus_whose_identifiers_cannot_work_is_refused() {
    let wires = Rc::new(Wires::default());
    let refused =
        |bus: CanBus| CanCarrier::new(&bus, Interface(wires.clone())).expect_err("refused");
    // Beyond the 11 bits of a standard identifier.
    let error = refused(CanBus {
        addresses: vec![0x700],
        ..bus(false)
    });
    assert!(error.contains("address 1792"), "{error}");
    // One device's sending identifier is another's receiving one.
    let error = refused(CanBus {
        addresses: vec![0, 0x80],
        ..bus(false)
    });
    assert!(error.contains("shares an identifier"), "{error}");
    // The same address twice.
    refused(CanBus {
        addresses: vec![3, 3],
        ..bus(false)
    });
    // Extended identifiers give the room standard ones do not.
    let extended = CanBus {
        base: CanLinkIds::new(0x1000_0000, 0x1100_0000, true),
        addresses: vec![0x700, 0x7_0000],
        ..bus(true)
    };
    assert!(CanCarrier::new(&extended, Interface(wires.clone())).is_ok());
}
