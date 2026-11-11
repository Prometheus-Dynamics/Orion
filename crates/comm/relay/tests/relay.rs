//! The relay: requests reach node logic and answers come back, node logic being away is a
//! refusal and nothing worse, and a watch outlives the node logic it was first passed to.

use orion_msg_records::{Changed, Get, RecordClass, Watch};
use orion_msg_session::{Hello, NodeAnswer, NodeAttach, NodeGone, NodeRequest, Refusal, Welcome};
use orion_relay::{RECORDS, Relay};
use orion_server::mem::{self, MemConnection, MemConnector};
use orion_server::{Comm, Error, FdSet, Limits, Principal, Roles, Security};
use orion_types::HlcTimestamp;
use orion_wire::body::encode_fields;
use orion_wire::frame;
use orion_wire::{Body, Bytes, Decode, List, encode_body, kind};

const USER: Principal = Principal::Local {
    uid: 1000,
    gid: 1000,
};
const ROOT: Principal = Principal::Local { uid: 0, gid: 0 };

fn run(test: impl Future<Output = ()>) {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .start_paused(true)
        .build()
        .expect("runtime");
    tokio::task::LocalSet::new().block_on(&runtime, test);
}

fn board() -> MemConnector {
    let node: orion_types::NodeName = "board".parse().expect("name");
    let comm = Comm::new(node, Security::Off, None, Limits::default());
    comm.add_service(Relay::new(RECORDS));
    let (carrier, connector) = mem::carrier(Roles::CLIENT.and(Roles::NODE), 64 * 1024);
    tokio::task::spawn_local(async move { comm.serve(carrier).await });
    connector
}

struct Client {
    connection: MemConnection,
    buf: Vec<u8>,
    len: usize,
    seq: u16,
}

impl Client {
    async fn open(connection: MemConnection, roles: Roles) -> Self {
        let mut client = Self {
            connection,
            buf: vec![0; 32 * 1024],
            len: 0,
            seq: 0,
        };
        let hello = Hello {
            name: "test",
            roles,
            max_frame: 32 * 1024,
            revision: 1,
        };
        client.send(&hello).await;
        assert_eq!(client.next().await, kind::WELCOME);
        let _: Welcome = Welcome::decode(client.payload()).expect("welcome");
        client
    }

    async fn send<B: Body>(&mut self, body: &B) -> u16 {
        self.seq += 1;
        let mut out = [0_u8; 1024];
        let len = encode_body(body, self.seq, &mut out).expect("encode");
        self.connection.send(&out[..len], &[]).await.expect("send");
        self.seq
    }

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
            if !matches!(kind, kind::PING | kind::PONG) {
                return kind;
            }
        }
    }

    fn payload(&self) -> &[u8] {
        frame::decode(&self.buf[..self.len])
            .expect("frame")
            .payload()
    }

    async fn refused(&mut self, seq: u16) -> Error {
        assert_eq!(self.next().await, kind::REFUSAL);
        let refusal = Refusal::decode(self.payload()).expect("refusal");
        assert_eq!(refusal.request_seq, seq);
        Error::from_code(refusal.code).expect("a known code")
    }

    /// The next request passed to this node logic: who asked, its `seq`, kind and payload.
    async fn passed(&mut self) -> (u32, u16, u8, Vec<u8>) {
        assert_eq!(self.next().await, kind::NODE_REQUEST);
        let request = NodeRequest::decode(self.payload()).expect("request");
        (
            request.session,
            request.seq,
            request.kind,
            request.payload.0.to_vec(),
        )
    }
}

async fn node(board: &MemConnector) -> Client {
    let mut node = Client::open(board.connect(ROOT), Roles::NODE).await;
    node.send(&NodeAttach {
        kinds: Bytes(&[kind::PUT, kind::GET, kind::WATCH]),
    })
    .await;
    node
}

const GET: Get<'static> = Get {
    class: RecordClass::Resource,
    prefix: "cam/",
};

const WATCH: Watch<'static> = Watch {
    class: RecordClass::Resource,
    prefix: "cam/",
    after: HlcTimestamp::ZERO,
};

#[test]
fn without_node_logic_a_request_is_refused_as_unavailable() {
    run(async {
        let board = board();
        let mut client = Client::open(board.connect(USER), Roles::CLIENT).await;
        let seq = client.send(&GET).await;
        assert_eq!(client.refused(seq).await, Error::Unavailable);
        // And the session is as good as before.
        let seq = client.send(&GET).await;
        assert_eq!(client.refused(seq).await, Error::Unavailable);
    });
}

#[test]
fn only_the_node_role_attaches() {
    run(async {
        let board = board();
        let mut client = Client::open(board.connect(USER), Roles::CLIENT).await;
        let seq = client
            .send(&NodeAttach {
                kinds: Bytes(&[kind::GET]),
            })
            .await;
        assert_eq!(client.refused(seq).await, Error::Denied);
        let seq = client.send(&GET).await;
        assert_eq!(client.refused(seq).await, Error::Unavailable);
    });
}

#[test]
fn a_request_is_passed_and_its_answer_comes_back() {
    run(async {
        let board = board();
        let mut node = node(&board).await;
        let mut client = Client::open(board.connect(USER), Roles::CLIENT).await;
        let seq = client.send(&GET).await;

        let (session, request_seq, kind, payload) = node.passed().await;
        assert_eq!((request_seq, kind), (seq, kind::GET));
        let get = Get::decode(&payload).expect("the request as it came");
        assert_eq!(get.prefix, "cam/");
        {
            let request = NodeRequest::decode(node.payload()).expect("request");
            assert_eq!((request.uid, request.caller), (1000, ""));
        }

        let mut answer = [0_u8; 64];
        let len = encode_fields(
            &Changed {
                request_seq,
                records: List::new(&[]),
                more: false,
            },
            &mut answer,
        )
        .expect("encode");
        node.send(&NodeAnswer {
            session,
            kind: kind::CHANGED,
            may_drop: false,
            payload: Bytes(&answer[..len]),
        })
        .await;
        assert_eq!(client.next().await, kind::CHANGED);
        let changed = Changed::decode(client.payload()).expect("changed");
        assert_eq!(changed.request_seq, seq);

        // When the session ends, node logic is told, once.
        drop(client);
        assert_eq!(node.next().await, kind::NODE_GONE);
        let gone = NodeGone::decode(node.payload()).expect("gone");
        assert_eq!(gone.session, session);
    });
}

#[test]
fn a_kind_node_logic_does_not_answer_is_refused() {
    run(async {
        let board = board();
        let mut node = Client::open(board.connect(ROOT), Roles::NODE).await;
        node.send(&NodeAttach {
            kinds: Bytes(&[kind::GET]),
        })
        .await;
        let mut client = Client::open(board.connect(USER), Roles::CLIENT).await;
        let seq = client.send(&GET).await;
        let (_, passed, ..) = node.passed().await;
        assert_eq!(passed, seq);
        let seq = client
            .send(&orion_msg_records::Put {
                records: List::new(&[]),
            })
            .await;
        assert_eq!(client.refused(seq).await, Error::Unavailable);
    });
}

#[test]
fn a_watch_is_passed_again_to_the_node_logic_that_comes_back() {
    run(async {
        let board = board();
        let mut first = node(&board).await;
        let mut client = Client::open(board.connect(USER), Roles::CLIENT).await;
        let watch = client.send(&WATCH).await;
        let (session, seq, kind, _) = first.passed().await;
        assert_eq!((seq, kind), (watch, kind::WATCH));

        // Node logic dies. A read is refused, the client's session stays.
        drop(first);
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        let get = client.send(&GET).await;
        assert_eq!(client.refused(get).await, Error::Unavailable);

        // It comes back and is passed the watch, as the client made it.
        let mut second = node(&board).await;
        let (again, seq, kind, payload) = second.passed().await;
        assert_eq!((again, seq, kind), (session, watch, kind::WATCH));
        assert_eq!(Watch::decode(&payload).expect("watch").prefix, "cam/");

        // And reads are answered for again.
        let get = client.send(&GET).await;
        let (_, seq, kind, _) = second.passed().await;
        assert_eq!((seq, kind), (get, kind::GET));
    });
}

#[test]
fn a_watch_made_while_node_logic_is_away_waits_for_it() {
    run(async {
        let board = board();
        let mut client = Client::open(board.connect(USER), Roles::CLIENT).await;
        let watch = client.send(&WATCH).await;
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        let mut node = node(&board).await;
        let (_, seq, kind, _) = node.passed().await;
        assert_eq!((seq, kind), (watch, kind::WATCH));
    });
}

#[test]
fn a_second_node_logic_takes_over_from_the_first() {
    run(async {
        let board = board();
        let mut first = node(&board).await;
        let mut second = node(&board).await;
        let mut client = Client::open(board.connect(USER), Roles::CLIENT).await;
        let seq = client.send(&GET).await;
        let (_, passed, ..) = second.passed().await;
        assert_eq!(passed, seq);
        // The first was closed.
        let mut fds = FdSet::default();
        let mut buf = [0_u8; 256];
        loop {
            match first.connection.recv(&mut buf, &mut fds).await {
                Ok(None) | Err(_) => break,
                Ok(Some(len)) => {
                    let kind = frame::decode(&buf[..len]).expect("frame").kind();
                    assert!(matches!(kind, kind::PING | kind::PONG), "kind {kind:#x}");
                }
            }
        }
    });
}
