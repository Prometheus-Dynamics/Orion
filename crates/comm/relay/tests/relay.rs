//! The relay: requests reach node logic and answers come back, node logic being away is a
//! refusal and nothing worse, and a watch outlives the node logic it was first passed to.

use orion_msg_actions::{Register, Run, Target};
use orion_msg_records::{Changed, Get, RecordClass, Watch};
use orion_msg_session::{Hello, NodeAnswer, NodeAttach, NodeGone, NodeRequest, Refusal, Welcome};
use orion_relay::{MAX_STANDING, Relay, routes};
use orion_server::mem::{self, MemConnection, MemConnector};
use orion_server::{Admits, Policy};
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
    board_with(Security::Off)
}

fn board_with(security: Security) -> MemConnector {
    let node: orion_types::NodeName = "board".parse().expect("name");
    let comm = Comm::new(node, security, None, Limits::default());
    comm.add_service(Relay::new(&routes()));
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
            level: 0,
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
        kinds: Bytes(&[
            kind::PUT,
            kind::GET,
            kind::WATCH,
            kind::ACTION_REGISTER,
            kind::ACTION_RUN,
        ]),
    })
    .await;
    node
}

fn fan() -> Target {
    Target::Resource("fan.left".parse().expect("name"))
}

fn register(name: &str, admits: Admits) -> Register<'static> {
    Register {
        name: name.parse().expect("name"),
        target: fan(),
        policy: Policy::new(admits),
        description: "",
    }
}

fn run_of(name: &str, target: Target) -> Run<'static> {
    Run {
        call: 1,
        name: name.parse().expect("name"),
        target,
        args: Bytes(b"x"),
        deadline_ms: 0,
        detached: false,
        on_behalf: 0,
    }
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

#[test]
fn a_run_is_decided_at_the_door_by_the_policy_its_action_was_registered_with() {
    run_test_secure(Security::On, true);
}

#[test]
fn with_security_off_the_same_run_passes() {
    run_test_secure(Security::Off, false);
}

fn run_test_secure(security: Security, refused: bool) {
    run(async move {
        let board = board_with(security);
        let mut node = node(&board).await;
        let mut provider = Client::open(board.connect(USER), Roles::CLIENT).await;
        let mut caller = Client::open(board.connect(USER), Roles::CLIENT).await;

        // Nothing is registered here: the action may be another node's. The run is let
        // through to node logic, which carries it there or says there is no such action.
        let seq = caller.send(&run_of("open", fan())).await;
        let (_, passed, passed_kind, _) = node.passed().await;
        assert_eq!((passed, passed_kind), (seq, kind::ACTION_RUN));

        // One action for programs on the board, one for operators alone.
        provider.send(&register("open", Admits::LOCAL)).await;
        provider.send(&register("wipe", Admits::OPERATOR)).await;
        let (_, _, kind, _) = node.passed().await;
        assert_eq!(kind, kind::ACTION_REGISTER);
        let (_, _, kind, _) = node.passed().await;
        assert_eq!(kind, kind::ACTION_REGISTER);

        let open = caller.send(&run_of("open", fan())).await;
        let (_, seq, kind, payload) = node.passed().await;
        assert_eq!((seq, kind), (open, kind::ACTION_RUN));
        assert_eq!(Run::decode(&payload).expect("run").name.as_str(), "open");

        let wipe = caller.send(&run_of("wipe", fan())).await;
        if refused {
            // Node logic never sees it.
            assert_eq!(caller.refused(wipe).await, Error::Denied);
        } else {
            let (_, seq, ..) = node.passed().await;
            assert_eq!(seq, wipe);
        }

        // The same name for another target is another action, and is not registered here.
        let other = Target::Resource("fan.right".parse().expect("name"));
        let seq = caller.send(&run_of("open", other)).await;
        let (_, passed, passed_kind, _) = node.passed().await;
        assert_eq!((passed, passed_kind), (seq, kind::ACTION_RUN));

        // The program goes, and its actions with it: node logic is told, and a run of what
        // it had is again a run of nothing registered here.
        drop(provider);
        assert_eq!(node.next().await, kind::NODE_GONE);
        let seq = caller.send(&run_of("open", fan())).await;
        let (_, passed, passed_kind, _) = node.passed().await;
        assert_eq!((passed, passed_kind), (seq, kind::ACTION_RUN));
    });
}

#[test]
fn a_registration_node_logic_refuses_does_not_stand() {
    run(async {
        let board = board_with(Security::On);
        let mut first = node(&board).await;
        let mut provider = Client::open(board.connect(USER), Roles::CLIENT).await;
        let mut caller = Client::open(board.connect(USER), Roles::CLIENT).await;
        let registered = provider.send(&register("open", Admits::LOCAL)).await;
        let (session, seq, ..) = first.passed().await;
        assert_eq!(seq, registered);

        // Node logic says no: another program has the name.
        let mut refusal = [0_u8; 64];
        let len = encode_fields(
            &Refusal {
                request_seq: seq,
                code: Error::AlreadyExists.code(),
                detail: "",
            },
            &mut refusal,
        )
        .expect("encode");
        first
            .send(&NodeAnswer {
                session,
                kind: kind::REFUSAL,
                may_drop: false,
                payload: Bytes(&refusal[..len]),
            })
            .await;
        assert_eq!(provider.refused(registered).await, Error::AlreadyExists);

        // So a run of it is not decided by its policy: it is as any run of an action that is
        // not registered here. And a node logic that comes back is not passed it.
        let seq = caller.send(&run_of("open", fan())).await;
        let (_, passed, passed_kind, _) = first.passed().await;
        assert_eq!((passed, passed_kind), (seq, kind::ACTION_RUN));
        drop(first);
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        let mut second = node(&board).await;
        let watch = caller.send(&WATCH).await;
        let (_, seq, kind, _) = second.passed().await;
        assert_eq!(
            (seq, kind),
            (watch, kind::WATCH),
            "the registration was not passed again"
        );
    });
}

#[test]
fn registered_actions_are_passed_again_to_the_node_logic_that_comes_back() {
    run(async {
        let board = board();
        let first = node(&board).await;
        let mut provider = Client::open(board.connect(USER), Roles::CLIENT).await;
        let registered = provider.send(&register("open", Admits::LOCAL)).await;
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        drop(first);
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        let mut second = node(&board).await;
        let (_, seq, kind, payload) = second.passed().await;
        assert_eq!((seq, kind), (registered, kind::ACTION_REGISTER));
        assert_eq!(
            Register::decode(&payload).expect("register").name.as_str(),
            "open"
        );
    });
}

#[test]
fn a_session_stands_for_so_much_and_no_more() {
    run(async {
        let board = board();
        let _node = node(&board).await;
        let mut provider = Client::open(board.connect(USER), Roles::CLIENT).await;
        for index in 0..MAX_STANDING {
            provider
                .send(&register(&format!("a{index}"), Admits::LOCAL))
                .await;
        }
        let over = provider.send(&register("one-more", Admits::LOCAL)).await;
        assert_eq!(provider.refused(over).await, Error::Busy);
    });
}

#[test]
fn whoever_was_waiting_on_node_logic_is_told_when_it_dies() {
    run(async {
        let board = board();
        let mut node = node(&board).await;
        let mut provider = Client::open(board.connect(USER), Roles::CLIENT).await;
        let mut caller = Client::open(board.connect(USER), Roles::CLIENT).await;
        provider.send(&register("open", Admits::LOCAL)).await;
        let _ = node.passed().await;

        // Answered: nothing is owed for it.
        let answered = caller.send(&GET).await;
        let (session, seq, ..) = node.passed().await;
        let mut answer = [0_u8; 64];
        let len = encode_fields(
            &Changed {
                request_seq: seq,
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
        assert_eq!(caller.next().await, kind::CHANGED);
        let _ = answered;

        // A read and a run that node logic took and never answered.
        let get = caller.send(&GET).await;
        let _ = node.passed().await;
        let started = caller.send(&run_of("open", fan())).await;
        let _ = node.passed().await;
        drop(node);
        assert_eq!(caller.refused(get).await, Error::Unavailable);
        assert_eq!(caller.refused(started).await, Error::Unavailable);
        // And nothing more: the answered read is not refused after the fact.
        let next = caller.send(&GET).await;
        assert_eq!(caller.refused(next).await, Error::Unavailable);
    });
}
