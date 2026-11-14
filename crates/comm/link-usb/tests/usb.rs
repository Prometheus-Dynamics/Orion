//! USB links over handles handed to the comm core: a stream, and a pair of blocking endpoints.

use std::fs::File;
use std::os::fd::OwnedFd;
use std::rc::Rc;
use std::time::Duration;

use orion_link_serial::{LinkStats, StreamCarrier};
use orion_link_usb::UsbLinks;
use orion_measure::{CountingAlloc, thread_counts};
use orion_msg_session::{
    Hello, LinkOffer, LinkTaken, LinkTransport, Ping, Pong, Refusal, Reject, RejectReason,
};
use orion_server::mem::{self, MemConnection, MemConnector};
use orion_server::{Comm, Error, FdSet, Grants, Limits, Principal, Roles, Security, What};
use orion_types::KeyId;
use orion_wire::{Body, Decode, StreamDecoder, encode_body, encode_frame, frame, kind};
use tokio::net::unix::pipe;

#[global_allocator]
static GLOBAL: CountingAlloc = CountingAlloc::system();

const ROOT: Principal = Principal::Local { uid: 0, gid: 0 };

struct NoGrants;

impl Grants for NoGrants {
    fn operator_may(&self, _key: &KeyId, _what: What<'_>) -> bool {
        true
    }
}

fn run(test: impl Future<Output = ()>) {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("runtime");
    tokio::task::LocalSet::new().block_on(&runtime, test);
}

struct Board {
    comm: Comm,
    connector: MemConnector,
    stats: Box<dyn Fn() -> LinkStats>,
}

fn board() -> Board {
    let comm = Comm::new(
        "board".parse().expect("name"),
        Security::On,
        Some(Rc::new(NoGrants)),
        Limits::default(),
    );
    let links = UsbLinks::new();
    comm.add_service(links.service());
    let carrier = StreamCarrier::new("usb0".parse().expect("name"), links.link("usb0"));
    let stats = Box::new(carrier.stats());
    let serving = comm.clone();
    tokio::task::spawn_local(async move { serving.serve(carrier).await });
    let (socket, connector) = mem::carrier(Roles::CLIENT.and(Roles::PROVIDER), 64 * 1024);
    let serving = comm.clone();
    tokio::task::spawn_local(async move { serving.serve(socket).await });
    Board {
        comm,
        connector,
        stats,
    }
}

/// The program that owns the hardware, as a client on the board's socket.
struct Owner {
    connection: MemConnection,
    seq: u16,
}

impl Owner {
    async fn open(connection: MemConnection) -> Self {
        let mut owner = Self { connection, seq: 0 };
        let hello = Hello {
            name: "hardware",
            roles: Roles::CLIENT,
            max_frame: 4096,
            revision: 1,
            level: 0,
        };
        assert_eq!(owner.ask(&hello, Vec::new()).await, Ok(kind::WELCOME));
        owner
    }

    /// Sends `body` with `fds` and returns the kind of the answer, or why it was refused.
    async fn ask<B: Body>(&mut self, body: &B, fds: Vec<OwnedFd>) -> Result<u8, Option<Error>> {
        self.seq += 1;
        let mut out = [0_u8; 256];
        let len = encode_body(body, self.seq, &mut out).expect("encode");
        self.connection.send(&out[..len], &fds).await.expect("send");
        let mut buf = [0_u8; 4096];
        loop {
            let len = self
                .connection
                .recv(&mut buf, &mut FdSet::default())
                .await
                .expect("recv")
                .expect("open");
            let view = frame::decode(&buf[..len]).expect("frame");
            match view.kind() {
                kind::PING | kind::PONG => {}
                kind::REFUSAL => {
                    return Err(Refusal::decode(view.payload()).expect("refusal").error());
                }
                kind::LINK_TAKEN => {
                    assert_eq!(
                        LinkTaken::decode(view.payload())
                            .expect("taken")
                            .request_seq,
                        self.seq
                    );
                    return Ok(kind::LINK_TAKEN);
                }
                other => return Ok(other),
            }
        }
    }

    async fn offer(
        &mut self,
        link: &str,
        transport: LinkTransport,
        fds: Vec<OwnedFd>,
    ) -> Result<u8, Option<Error>> {
        let offer = LinkOffer {
            link,
            transport,
            interface: 0,
            endpoint_in: 0,
            endpoint_out: 0,
        };
        self.ask(&offer, fds).await
    }
}

/// The device's end: something to read and something to write.
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

fn waitable(fd: OwnedFd) -> File {
    rustix::io::ioctl_fionbio(&fd, true).expect("non-blocking");
    File::from(fd)
}

impl Device {
    fn over(read: OwnedFd, write: OwnedFd) -> Self {
        Self {
            rx: pipe::Receiver::from_file_unchecked(waitable(read)).expect("rx"),
            tx: pipe::Sender::from_file_unchecked(waitable(write)).expect("tx"),
            decoder: Box::new(StreamDecoder::new()),
            chunk: [0; 256],
            at: 0,
            len: 0,
            payload: [0; 512],
            payload_len: 0,
            seq: 0,
        }
    }

    /// A socket pair: the device's end, and the end to hand to the board.
    fn stream() -> (Self, OwnedFd) {
        let (ours, theirs) = std::os::unix::net::UnixStream::pair().expect("pair");
        let ours = OwnedFd::from(ours);
        (
            Self::over(ours.try_clone().expect("dup"), ours),
            OwnedFd::from(theirs),
        )
    }

    /// Two pipes: the device's ends, and the read and write ends to hand to the board.
    fn endpoints() -> (Self, Vec<OwnedFd>) {
        let (board_reads, device_writes) = rustix::pipe::pipe().expect("pipe");
        let (device_reads, board_writes) = rustix::pipe::pipe().expect("pipe");
        (
            Self::over(device_reads, device_writes),
            vec![board_reads, board_writes],
        )
    }

    async fn send<B: Body>(&mut self, body: &B) {
        self.seq += 1;
        let (mut frame, mut wire) = ([0_u8; 256], [0_u8; 300]);
        let len = encode_body(body, self.seq, &mut frame).expect("encode");
        let len = encode_frame(&frame[..len], &mut wire).expect("cobs");
        let mut bytes = &wire[..len];
        while !bytes.is_empty() {
            self.tx.writable().await.expect("writable");
            match self.tx.try_write(bytes) {
                Ok(wrote) => bytes = &bytes[wrote..],
                Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {}
                Err(error) => panic!("device write: {error}"),
            }
        }
    }

    /// The kind of the next frame from the board, or `None` when the board has let go.
    async fn next(&mut self) -> Option<u8> {
        loop {
            if self.at == self.len {
                self.rx.readable().await.expect("readable");
                match self.rx.try_read(&mut self.chunk) {
                    Ok(0) => return None,
                    Ok(len) => (self.at, self.len) = (0, len),
                    Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => continue,
                    Err(_) => return None,
                }
            }
            let (used, pushed) = self.decoder.push_slice(&self.chunk[self.at..self.len]);
            self.at += used;
            if let Ok(Some(frame)) = pushed {
                self.payload_len = frame.payload().len();
                self.payload[..self.payload_len].copy_from_slice(frame.payload());
                return Some(frame.kind());
            }
        }
    }

    async fn hello(&mut self) {
        self.send(&Hello {
            name: "sensor",
            roles: Roles::DEVICE,
            max_frame: 512,
            revision: 1,
            level: 0,
        })
        .await;
        assert_eq!(self.next().await, Some(kind::WELCOME));
    }

    async fn ping(&mut self, now_ms: u64) {
        self.send(&Ping { now_ms }).await;
        loop {
            if self.next().await.expect("the board answers") == kind::PONG {
                assert_eq!(
                    Pong::decode(&self.payload[..self.payload_len])
                        .expect("pong")
                        .now_ms,
                    now_ms
                );
                return;
            }
        }
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

fn device_sessions(board: &Board) -> usize {
    board.comm.with(|sessions, _| {
        sessions
            .iter()
            .filter(|session| session.open && matches!(session.principal, Principal::Device(_)))
            .count()
    })
}

#[test]
fn a_device_of_another_wire_revision_on_a_handed_over_link_is_told_both_numbers() {
    run(async {
        let board = board();
        let mut owner = Owner::open(board.connector.connect(ROOT)).await;
        let (mut device, handle) = Device::stream();
        assert_eq!(
            owner
                .offer("usb0", LinkTransport::Stream, vec![handle])
                .await,
            Ok(kind::LINK_TAKEN)
        );
        until("the link takes the handle", || (board.stats)().opens == 1).await;
        let other = orion_wire::INTERFACE_REVISION + 1;
        device
            .send(&Hello {
                name: "sensor",
                roles: Roles::DEVICE,
                max_frame: 512,
                revision: other,
                level: 0,
            })
            .await;
        assert_eq!(device.next().await, Some(kind::REJECT));
        let reject = Reject::decode(&device.payload[..device.payload_len]).expect("reject");
        assert_eq!(
            (reject.reason, reject.node_revision, reject.peer_revision),
            (
                RejectReason::RevisionMismatch,
                orion_wire::INTERFACE_REVISION,
                other
            )
        );
    });
}

#[test]
fn a_link_runs_over_whatever_the_hardware_owner_hands_over() {
    run(async {
        let board = board();
        let mut owner = Owner::open(board.connector.connect(ROOT)).await;
        // Nothing has been handed over: the link has opened nothing and is not trying to.
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!((board.stats)().opens, 0);

        // A stream: one descriptor, both ways.
        let (mut device, handle) = Device::stream();
        assert_eq!(
            owner
                .offer("usb0", LinkTransport::Stream, vec![handle])
                .await,
            Ok(kind::LINK_TAKEN)
        );
        until("the link takes the handle", || (board.stats)().opens == 1).await;
        device.hello().await;
        for round in 0..200 {
            device.ping(round).await;
        }
        assert_eq!(device_sessions(&board), 1);
        let before = thread_counts();
        for round in 0..2000 {
            device.ping(round).await;
        }
        let allocated = thread_counts().since(before);
        assert_eq!(
            allocated.allocs, 0,
            "{allocated:?} in 2000 heartbeats over a stream"
        );

        // The device is unplugged and back: its owner hands over the new one. The link lets
        // go of the old and goes on over the new.
        let (mut again, handle) = Device::endpoints();
        assert_eq!(
            owner.offer("usb0", LinkTransport::Endpoints, handle).await,
            Ok(kind::LINK_TAKEN)
        );
        assert_eq!(device.next().await, None, "the old handle was closed");
        until("the link takes the new handle", || {
            (board.stats)().opens == 2
        })
        .await;
        again.hello().await;
        for round in 0..200 {
            again.ping(round).await;
        }
        assert_eq!(device_sessions(&board), 1);
        let before = thread_counts();
        for round in 0..2000 {
            again.ping(round).await;
        }
        let allocated = thread_counts().since(before);
        assert_eq!(
            allocated.allocs, 0,
            "{allocated:?} in 2000 heartbeats over endpoints"
        );

        // The device goes away with nothing to replace it: the session ends, and the link
        // waits to be handed another, opening nothing meanwhile.
        drop(again);
        until("the session ends", || device_sessions(&board) == 0).await;
        tokio::time::sleep(Duration::from_millis(600)).await;
        assert_eq!((board.stats)().opens, 2);
        let (mut third, handle) = Device::stream();
        assert_eq!(
            owner
                .offer("usb0", LinkTransport::Stream, vec![handle])
                .await,
            Ok(kind::LINK_TAKEN)
        );
        third.hello().await;
        third.ping(1).await;
        assert_eq!(((board.stats)().opens, (board.stats)().connections), (3, 3));
    });
}

#[test]
fn only_a_program_on_the_board_hands_over_a_device_and_only_a_whole_one() {
    run(async {
        let board = board();
        // An operator, however trusted, is not on the board.
        let operator = Principal::Operator(KeyId([7; 32]));
        let mut remote = Owner::open(board.connector.connect(operator)).await;
        let (_device, handle) = Device::stream();
        assert_eq!(
            remote
                .offer("usb0", LinkTransport::Stream, vec![handle])
                .await,
            Err(Some(Error::Denied))
        );

        let mut owner = Owner::open(board.connector.connect(ROOT)).await;
        // A link this board does not have.
        let (_device, handle) = Device::stream();
        assert_eq!(
            owner
                .offer("usb9", LinkTransport::Stream, vec![handle])
                .await,
            Err(Some(Error::NotFound))
        );
        // The wrong number of descriptors for what they are said to be.
        let (_device, handles) = Device::endpoints();
        assert_eq!(
            owner.offer("usb0", LinkTransport::Stream, handles).await,
            Err(Some(Error::Malformed))
        );
        let (_device, handle) = Device::stream();
        assert_eq!(
            owner
                .offer("usb0", LinkTransport::Endpoints, vec![handle])
                .await,
            Err(Some(Error::Malformed))
        );
        assert_eq!(
            owner.offer("usb0", LinkTransport::Stream, Vec::new()).await,
            Err(Some(Error::Malformed))
        );
        // A kind of handle this build does not know.
        let (_device, handle) = Device::stream();
        assert_eq!(
            owner
                .offer("usb0", LinkTransport::Unknown(9), vec![handle])
                .await,
            Err(Some(Error::Unsupported))
        );
        assert_eq!(
            (board.stats)().opens,
            0,
            "nothing that was refused reached the link"
        );
    });
}

#[test]
fn with_no_list_of_links_root_may_hand_over_a_link_by_any_name() {
    run(async {
        let comm = Comm::new(
            "board".parse().expect("name"),
            Security::On,
            Some(Rc::new(NoGrants)),
            Limits::default(),
        );
        let links = UsbLinks::new();
        comm.add_service(links.service());
        let made = Rc::new(std::cell::RefCell::new(Vec::<String>::new()));
        {
            let (comm, made) = (comm.clone(), made.clone());
            links.any_name(move |name, link| {
                made.borrow_mut().push(name.to_owned());
                let carrier = StreamCarrier::new(name.parse().expect("name"), link);
                let comm = comm.clone();
                tokio::task::spawn_local(async move { comm.serve(carrier).await });
            });
        }
        let (socket, connector) = mem::carrier(Roles::CLIENT.and(Roles::PROVIDER), 64 * 1024);
        let serving = comm.clone();
        tokio::task::spawn_local(async move { serving.serve(socket).await });
        let board = Board {
            comm,
            connector,
            stats: Box::new(LinkStats::default),
        };

        // Someone on the board who is not root may not name a link.
        let user = Principal::Local {
            uid: 1000,
            gid: 1000,
        };
        let mut someone = Owner::open(board.connector.connect(user)).await;
        let (_device, handle) = Device::stream();
        assert_eq!(
            someone
                .offer("gadget", LinkTransport::Stream, vec![handle])
                .await,
            Err(Some(Error::NotFound))
        );
        assert!(made.borrow().is_empty());

        // Root may: the link is made, served, and runs.
        let mut owner = Owner::open(board.connector.connect(ROOT)).await;
        let (mut device, handle) = Device::stream();
        assert_eq!(
            owner
                .offer("gadget", LinkTransport::Stream, vec![handle])
                .await,
            Ok(kind::LINK_TAKEN)
        );
        device.hello().await;
        device.ping(1).await;
        assert_eq!(device_sessions(&board), 1);

        // The same name again is the same link, taking the new handle: not a second link.
        let (mut again, handle) = Device::stream();
        assert_eq!(
            owner
                .offer("gadget", LinkTransport::Stream, vec![handle])
                .await,
            Ok(kind::LINK_TAKEN)
        );
        again.hello().await;
        again.ping(2).await;
        assert_eq!(*made.borrow(), ["gadget"]);
    });
}

#[test]
fn an_end_descriptor_takes_the_endpoints_back_and_all_three_are_closed() {
    run(async {
        let board = board();
        let mut owner = Owner::open(board.connector.connect(ROOT)).await;
        for (round, written) in [(1_u64, true), (2, false)] {
            let (mut device, mut handle) = Device::endpoints();
            let (end, offerer_end) = rustix::pipe::pipe().expect("pipe");
            handle.push(end);
            assert_eq!(
                owner.offer("usb0", LinkTransport::Endpoints, handle).await,
                Ok(kind::LINK_TAKEN)
            );
            device.hello().await;
            device.ping(round).await;
            assert_eq!(device_sessions(&board), 1);

            // The holder wants its endpoints back: it writes to its end, or closes it.
            if written {
                rustix::io::write(&offerer_end, b"x").expect("write");
            } else {
                drop(offerer_end);
            }
            // The board lets go: the endpoint it wrote to is closed, the session is over.
            let closed = tokio::time::timeout(Duration::from_secs(5), device.next()).await;
            assert_eq!(closed, Ok(None), "the board's write end was never closed");
            until("the session ends", || device_sessions(&board) == 0).await;
            // The thread reading its endpoint lets go once its call returns.
            let mut closed = false;
            for _ in 0..500 {
                match device.tx.try_write(b"\0") {
                    Err(error) if error.kind() == std::io::ErrorKind::BrokenPipe => {
                        closed = true;
                        break;
                    }
                    _ => tokio::time::sleep(Duration::from_millis(4)).await,
                }
            }
            assert!(closed, "the board's read end was never closed");
        }
        // Each offer was taken and let go; the link waits for the next.
        assert_eq!((board.stats)().opens, 2);
        let (mut device, handle) = Device::stream();
        assert_eq!(
            owner
                .offer("usb0", LinkTransport::Stream, vec![handle])
                .await,
            Ok(kind::LINK_TAKEN)
        );
        device.hello().await;
        device.ping(3).await;
    });
}
