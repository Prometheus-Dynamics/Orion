//! A device brought to the board over the network, inside the sealed channel.
#![cfg(feature = "sealed")]

use std::io::ErrorKind;
use std::net::SocketAddr;
use std::time::Duration;

use orion_keys::{Identity, Keys, KeysConfig, SharedKeys};
use orion_link_serial::sealed::{LINK_ATTACH, SealedListener, SealedStream};
use orion_link_serial::{ByteStream, LinkStats, StreamCarrier};
use orion_measure::{CountingAlloc, thread_counts};
use orion_msg_session::{Hello, Ping, Pong};
use orion_server::{Comm, Limits, Roles, Security};
use orion_wire::{Body, Decode, StreamDecoder, encode_body, encode_frame, kind};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

#[global_allocator]
static GLOBAL: CountingAlloc = CountingAlloc::system();

fn run(test: impl Future<Output = ()>) {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("runtime");
    tokio::task::LocalSet::new().block_on(&runtime, test);
}

struct Board {
    keys: SharedKeys,
    addr: SocketAddr,
    comm: Comm,
    stats: Box<dyn Fn() -> LinkStats>,
}

fn board() -> Board {
    let keys =
        SharedKeys::new(Keys::open(KeysConfig::in_memory("lab"), Security::On).expect("keys"));
    let listener =
        SealedListener::bind("127.0.0.1:0".parse().expect("addr"), "board", keys.clone())
            .expect("bind");
    let addr = listener.local_addr().expect("addr");
    let comm = Comm::new(
        "board".parse().expect("name"),
        Security::On,
        None,
        Limits::default(),
    );
    let carrier = StreamCarrier::new("bench".parse().expect("name"), listener);
    let stats = Box::new(carrier.stats());
    let serving = comm.clone();
    tokio::task::spawn_local(async move { serving.serve(carrier).await });
    Board {
        keys,
        addr,
        comm,
        stats,
    }
}

/// An operator the board has enrolled, with or without the grant to attach a link.
fn operator(board: &Board, name: &str, granted: bool) -> Identity {
    let key = Identity::generate();
    let mut keys = board.keys.lock();
    keys.approve_operator(key.public(), name).expect("enroll");
    let actions = if granted {
        vec![LINK_ATTACH.to_owned()]
    } else {
        Vec::new()
    };
    assert!(
        keys.grant(&key.key_id(), true, actions, false)
            .expect("grant")
    );
    key
}

/// The device, as the operator's computer relays it.
struct Device {
    link: SealedStream,
    decoder: Box<StreamDecoder<1024>>,
    chunk: [u8; 256],
    at: usize,
    len: usize,
    payload: [u8; 512],
    payload_len: usize,
    seq: u16,
}

impl Device {
    fn over(link: SealedStream) -> Self {
        Self {
            link,
            decoder: Box::new(StreamDecoder::new()),
            chunk: [0; 256],
            at: 0,
            len: 0,
            payload: [0; 512],
            payload_len: 0,
            seq: 0,
        }
    }

    async fn send<B: Body>(&mut self, body: &B) {
        self.seq += 1;
        let (mut frame, mut wire) = ([0_u8; 256], [0_u8; 300]);
        let len = encode_body(body, self.seq, &mut frame).expect("encode");
        let len = encode_frame(&frame[..len], &mut wire).expect("cobs");
        assert_eq!(self.link.write(&wire[..len]).await.expect("write"), len);
    }

    async fn next(&mut self) -> u8 {
        loop {
            if self.at == self.len {
                self.len = self.link.read(&mut self.chunk).await.expect("read");
                assert!(self.len > 0, "the board hung up");
                self.at = 0;
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

    async fn hello(&mut self) {
        self.send(&Hello {
            name: "sensor",
            roles: Roles::DEVICE,
            max_frame: 512,
            revision: 1,
        })
        .await;
        assert_eq!(self.next().await, kind::WELCOME);
    }

    async fn ping(&mut self, now_ms: u64) {
        self.send(&Ping { now_ms }).await;
        loop {
            if self.next().await == kind::PONG {
                let pong = Pong::decode(&self.payload[..self.payload_len]).expect("pong");
                assert_eq!(pong.now_ms, now_ms);
                return;
            }
        }
    }
}

#[test]
fn an_operator_with_the_grant_brings_a_device_to_the_board() {
    run(async {
        let board = board();
        let board_key = board.keys.lock().identity().public();
        let alice = operator(&board, "alice", true);

        let link = SealedStream::connect(board.addr, "alice", &alice, Some(&board_key))
            .await
            .expect("attached");
        let mut device = Device::over(link);
        device.hello().await;
        for round in 0..300 {
            device.ping(round).await;
        }
        assert_eq!(board.comm.stats().opened, 1);
        assert_eq!((board.stats)().connections, 1);

        // Steady traffic through the seal allocates nothing, on either side.
        let before = thread_counts();
        for round in 0..2000 {
            device.ping(round).await;
        }
        let allocated = thread_counts().since(before);
        assert_eq!(
            allocated.allocs, 0,
            "{allocated:?} in 2000 sealed heartbeats"
        );

        // The device goes: the board takes the next one that is brought.
        drop(device);
        let link = SealedStream::connect(board.addr, "alice", &alice, None)
            .await
            .expect("attached again");
        let mut device = Device::over(link);
        device.hello().await;
        device.ping(1).await;
        assert_eq!((board.stats)().opens, 2);
    });
}

#[test]
fn nobody_else_gets_a_link() {
    run(async {
        let board = board();
        let board_key = board.keys.lock().identity().public();

        // Enrolled, but not granted the action.
        let bob = operator(&board, "bob", false);
        let refused = SealedStream::connect(board.addr, "bob", &bob, Some(&board_key))
            .await
            .expect_err("refused");
        assert_eq!(refused.kind(), ErrorKind::PermissionDenied);
        assert!(refused.to_string().contains("may not attach"), "{refused}");

        // A key the board has never seen, under an enrolled operator's name.
        let stranger = Identity::generate();
        let refused = SealedStream::connect(board.addr, "alice", &stranger, None)
            .await
            .expect_err("refused");
        assert_eq!(refused.kind(), ErrorKind::PermissionDenied);

        // A board that is not the one the operator meant.
        let alice = operator(&board, "alice", true);
        let wrong = Identity::generate().public();
        let refused = SealedStream::connect(board.addr, "alice", &alice, Some(&wrong))
            .await
            .expect_err("another board");
        assert_eq!(refused.kind(), ErrorKind::InvalidData);

        // Bytes that are not a handshake, and a caller that says nothing: neither holds the
        // door, and neither becomes a session.
        let mut junk = tokio::net::TcpStream::connect(board.addr)
            .await
            .expect("connect");
        junk.write_all(&[0xFF; 64]).await.expect("write");
        let mut end = [0_u8; 8];
        assert!(
            matches!(junk.read(&mut end).await, Ok(0) | Err(_)),
            "the board hung up"
        );
        let _silent = tokio::net::TcpStream::connect(board.addr)
            .await
            .expect("connect");
        assert_eq!((board.comm.stats().opened, (board.stats)().opens), (0, 0));

        // The silent caller holds nobody up: the granted operator gets in at once.
        tokio::time::timeout(Duration::from_secs(2), async {
            let link = SealedStream::connect(board.addr, "alice", &alice, Some(&board_key))
                .await
                .expect("attached");
            let mut device = Device::over(link);
            device.hello().await;
            device.ping(7).await;
        })
        .await
        .expect("the door is not held by the silent caller");
        assert_eq!(board.comm.stats().opened, 1);
    });
}
