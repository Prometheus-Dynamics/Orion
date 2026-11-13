//! The time sync as the comm core runs it: its state as a topic, a peer's clock, and an
//! operator's clock over the session it already has.

#[path = "../../broker/tests/common/mod.rs"]
mod common;

use std::cell::RefCell;
use std::rc::Rc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use common::{Event, Wire};
use orion_broker::{Broker, BrokerConfig};
use orion_msg_peers::{
    CLOCK_MONOTONIC, CLOCK_REALTIME, TimeOffer, TimeReply, TimeRequest, TimeTurn,
};
use orion_msg_session::{Hello, Refusal};
use orion_server::mem::{self, MemConnection, MemConnector};
use orion_server::{Comm, Error, FdSet, Grants, Limits, Principal, Roles, Security, What};
use orion_time::{CLOCK_SET, Clock, TIME_TOPIC, TimeConfig, TimeMode, TimeSync};
use orion_types::{KeyId, SampleTime};
use orion_wire::{Body, Decode, encode_body, frame, kind};
use shm_ring::{FieldId, Reader};

const S: i64 = 1_000_000_000;
const USER: Principal = Principal::Local {
    uid: 1000,
    gid: 1000,
};
const TRUSTED: KeyId = KeyId([7; 32]);

#[derive(Default)]
struct FakeClock {
    steps: RefCell<Vec<i64>>,
    slews: RefCell<Vec<i64>>,
}

impl Clock for FakeClock {
    fn step(&self, offset_ns: i64) -> std::io::Result<()> {
        self.steps.borrow_mut().push(offset_ns);
        Ok(())
    }

    fn slew(&self, offset_ns: i64, _max_error_ns: i64) -> std::io::Result<()> {
        self.slews.borrow_mut().push(offset_ns);
        Ok(())
    }
}

/// One operator has been granted the clock.
struct OneGrant;

impl Grants for OneGrant {
    fn operator_may(&self, key: &KeyId, what: What<'_>) -> bool {
        *key == TRUSTED && what == What::Run(CLOCK_SET)
    }
}

fn run(paused: bool, test: impl Future<Output = ()>) {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .start_paused(paused)
        .build()
        .expect("runtime");
    tokio::task::LocalSet::new().block_on(&runtime, test);
}

fn wall_ns() -> i128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock")
        .as_nanos() as i128
}

struct Board {
    sync: TimeSync,
    clock: Rc<FakeClock>,
    connector: MemConnector,
}

fn board(config: TimeConfig) -> Board {
    let node: orion_types::NodeName = "board".parse().expect("name");
    let comm = Comm::new(
        node.clone(),
        Security::On,
        Some(Rc::new(OneGrant)),
        Limits::default(),
    );
    let broker = Broker::new(node, BrokerConfig::default());
    let clock = Rc::new(FakeClock::default());
    let sync = TimeSync::start_with(
        &comm,
        Some(&broker.topics()),
        config,
        None,
        clock.clone(),
        false,
    )
    .expect("start");
    comm.add_service(broker);
    let (carrier, connector) = mem::carrier(Roles::CLIENT.and(Roles::PROVIDER), 64 * 1024);
    tokio::task::spawn_local(async move { comm.serve(carrier).await });
    Board {
        sync,
        clock,
        connector,
    }
}

fn config(mode: TimeMode, servers: &[&str]) -> TimeConfig {
    TimeConfig {
        mode,
        servers: servers.iter().map(|server| (*server).to_owned()).collect(),
        synced_file: None,
        ..TimeConfig::default()
    }
}

/// A session that speaks the time messages.
struct Client {
    connection: MemConnection,
    buf: Vec<u8>,
    len: usize,
    seq: u16,
}

impl Client {
    async fn open(connection: MemConnection) -> Self {
        let mut client = Self {
            connection,
            buf: vec![0; 4096],
            len: 0,
            seq: 0,
        };
        client
            .send(&Hello {
                name: "tool",
                roles: Roles::CLIENT,
                max_frame: 4096,
                revision: 1,
            })
            .await;
        assert_eq!(client.next().await, kind::WELCOME);
        client
    }

    async fn send<B: Body>(&mut self, body: &B) -> u16 {
        self.seq += 1;
        let mut out = [0_u8; 256];
        let len = encode_body(body, self.seq, &mut out).expect("encode");
        self.connection.send(&out[..len], &[]).await.expect("send");
        self.seq
    }

    /// The kind of the next frame that is not a heartbeat.
    async fn next(&mut self) -> u8 {
        loop {
            let mut fds = FdSet::default();
            self.len = self
                .connection
                .recv(&mut self.buf, &mut fds)
                .await
                .expect("recv")
                .expect("open");
            let kind = frame::decode(&self.buf[..self.len]).expect("frame").kind();
            if kind != kind::PING && kind != kind::PONG {
                return kind;
            }
        }
    }

    fn body<'a, B: Decode<'a>>(&'a self) -> B {
        B::decode(
            frame::decode(&self.buf[..self.len])
                .expect("frame")
                .payload(),
        )
        .expect("body")
    }

    async fn refused(&mut self) -> Option<Error> {
        assert_eq!(self.next().await, kind::REFUSAL);
        self.body::<Refusal>().error()
    }

    /// Answers the board's next question with a clock `ahead_ns` off.
    async fn answer(&mut self, ahead_ns: i64) {
        assert_eq!(self.next().await, kind::TIME_REQUEST);
        let asked: TimeRequest = self.body();
        let now = (wall_ns() + i128::from(ahead_ns)) as u64;
        self.send(&TimeReply {
            sent_ns: asked.sent_ns,
            received_ns: now,
            replied_ns: now,
        })
        .await;
    }
}

fn text(reader: &mut Reader, field: u16) -> Option<String> {
    let sample = reader.latest(FieldId(field))?;
    Some(String::from_utf8(sample.payload.to_vec()).expect("text"))
}

fn number<const N: usize>(reader: &mut Reader, field: u16) -> Option<[u8; N]> {
    reader.latest(FieldId(field))?.payload.try_into().ok()
}

#[test]
fn a_board_takes_a_peers_clock_and_says_so_in_a_topic() {
    run(false, async {
        // One board is the network's reference and serves its clock.
        let reference = board(TimeConfig {
            serve: Some("127.0.0.1:0".parse().expect("addr")),
            serve_authority: true,
            synced_file: None,
            ..TimeConfig::default()
        });
        let serving = reference.sync.serving().expect("serving");
        // The other takes it, first over UDP and, were that to fail, over TCP.
        let peer = format!("peer://{serving}");
        let over_tcp = format!("tcp://{serving}");
        let follower = board(config(
            TimeMode::Discipline,
            &["127.0.0.1:9", &peer, &over_tcp],
        ));

        let mut client =
            common::Client::open(Wire::Mem(follower.connector.connect(USER)), "dashboard").await;
        let seq = client.subscribe(TIME_TOPIC, None, &[]).await;
        let (_, mut state, _) = client.opened(seq).await;
        while !follower.sync.status().synced {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        // Both clocks are this machine's, so there was nothing to step.
        assert!(follower.clock.steps.borrow().is_empty());
        assert_eq!(follower.clock.slews.borrow().len(), 1);
        let status = follower.sync.status();
        assert_eq!((status.mode, status.last_action), ("discipline", "slewed"));
        assert_eq!(
            (status.source.as_deref(), status.stratum),
            (Some(peer.as_str()), Some(10))
        );

        // The same, as anyone on the board reads it.
        assert_eq!(text(&mut state, 0).as_deref(), Some("discipline"));
        assert_eq!(number::<1>(&mut state, 1), Some([1]));
        assert_eq!(text(&mut state, 2), Some(peer.clone()));
        let offset_ms = f64::from_le_bytes(number(&mut state, 3).expect("offset"));
        let delay_ms = f64::from_le_bytes(number(&mut state, 4).expect("delay"));
        assert!(
            offset_ms.abs() < 100.0 && (0.0..100.0).contains(&delay_ms),
            "{offset_ms} {delay_ms}"
        );
        assert_eq!(number(&mut state, 5).map(i64::from_le_bytes), Some(10));
        assert_eq!(text(&mut state, 6).as_deref(), Some("slewed"));
        assert_eq!(text(&mut state, 7).as_deref(), Some(""));

        // The TCP way in answers the same question.
        let tcp_only = board(config(TimeMode::Report, &[&over_tcp]));
        while tcp_only.sync.status().offset_ns.is_none() {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        let status = tcp_only.sync.status();
        assert_eq!((status.last_action, status.synced), ("would slew", false));
        assert!(
            tcp_only.clock.slews.borrow().is_empty(),
            "report mode never touches the clock"
        );

        // A board that serves nothing and syncs nothing has no topic and no sockets.
        let quiet = board(TimeConfig::default());
        assert_eq!(quiet.sync.serving(), None);
        let mut client =
            common::Client::open(Wire::Mem(quiet.connector.connect(USER)), "dashboard").await;
        let seq = client.open_topic(TIME_TOPIC, false).await;
        assert!(
            matches!(client.next().await, Event::Refused { request_seq, .. } if request_seq == seq)
        );
    });
}

#[test]
fn an_operator_with_the_grant_gives_the_board_its_clock() {
    run(true, async {
        let board = board(config(TimeMode::Discipline, &["operator"]));
        // Nobody offers yet.
        tokio::time::sleep(Duration::from_millis(10)).await;
        assert!(
            board
                .sync
                .status()
                .error
                .expect("error")
                .contains("no operator is offering")
        );

        // An operator without the grant, and a program on the board, may not set the clock.
        let stranger = Principal::Operator(KeyId([9; 32]));
        let mut other = Client::open(board.connector.connect_without_descriptors(stranger)).await;
        other.send(&TimeOffer { stratum: 1 }).await;
        assert_eq!(other.refused().await, Some(Error::Denied));
        let mut local = Client::open(board.connector.connect(USER)).await;
        local.send(&TimeOffer { stratum: 1 }).await;
        assert_eq!(local.refused().await, Some(Error::Denied));
        // But anyone may ask what time the board has.
        local
            .send(&TimeRequest {
                sent_ns: 42,
                clock: CLOCK_REALTIME,
            })
            .await;
        assert_eq!(local.next().await, kind::TIME_REPLY);
        let reply: TimeReply = local.body();
        assert_eq!(reply.sent_ns, 42);
        assert!((i128::from(reply.received_ns) - wall_ns()).abs() < i128::from(S));
        assert!(reply.replied_ns >= reply.received_ns);
        // On the clock sample times are on, when asked for it; another clock is refused.
        local
            .send(&TimeRequest {
                sent_ns: 43,
                clock: CLOCK_MONOTONIC,
            })
            .await;
        assert_eq!(local.next().await, kind::TIME_REPLY);
        let reply: TimeReply = local.body();
        let monotonic = i128::from(SampleTime::now().as_nanos());
        assert!((i128::from(reply.received_ns) - monotonic).abs() < i128::from(S));
        local
            .send(&TimeRequest {
                sent_ns: 44,
                clock: 7,
            })
            .await;
        assert_eq!(local.refused().await, Some(Error::Unsupported));

        // The operator with the grant offers a clock seven seconds ahead.
        let mut operator = Client::open(
            board
                .connector
                .connect_without_descriptors(Principal::Operator(TRUSTED)),
        )
        .await;
        // An answer nobody asked for is not one.
        operator
            .send(&TimeReply {
                sent_ns: 1,
                received_ns: 2,
                replied_ns: 3,
            })
            .await;
        assert_eq!(operator.refused().await, Some(Error::OutOfOrder));
        let seq = operator.send(&TimeOffer { stratum: 3 }).await;
        assert_eq!(operator.next().await, kind::TIME_TURN);
        let turn: TimeTurn = operator.body();
        assert_eq!(turn.request_seq, seq);
        assert!(turn.poll_ms >= 2_000);

        // The board asks at once, not at its next poll, and again to see the answer agree.
        operator.answer(7 * S).await;
        operator.answer(7 * S).await;
        tokio::time::sleep(Duration::from_millis(10)).await;
        let steps = board.clock.steps.borrow().clone();
        assert_eq!(steps.len(), 1, "{:?}", board.sync.status());
        assert!((steps[0] - 7 * S).abs() < S, "stepped {} ns", steps[0]);
        let status = board.sync.status();
        assert_eq!(
            (status.source.as_deref(), status.stratum),
            (Some("operator"), Some(3))
        );
        assert!(status.synced);

        // The operator leaves: the board says so and keeps running.
        drop(operator);
        tokio::time::sleep(Duration::from_secs(200)).await;
        assert!(
            board
                .sync
                .status()
                .error
                .expect("error")
                .contains("operator")
        );
        assert_eq!(board.clock.steps.borrow().len(), 1);
    });
}

#[test]
fn a_board_that_does_not_list_operators_takes_no_offer() {
    run(true, async {
        let board = board(config(TimeMode::Report, &["127.0.0.1:9"]));
        let mut operator = Client::open(
            board
                .connector
                .connect_without_descriptors(Principal::Operator(TRUSTED)),
        )
        .await;
        operator.send(&TimeOffer { stratum: 3 }).await;
        assert_eq!(operator.refused().await, Some(Error::Unsupported));
    });
}
