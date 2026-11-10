//! Enrollment through the server: enroll, refuse, revoke, and the open mode.

use std::rc::Rc;

use orion_keys::enroll::{NONCE_LEN, Side, Transcript, operator_proof, proof, verify_proof};
use orion_keys::{
    Identity, Keys, KeysConfig, KeysService, OperatorState, SharedKeys, random, verify,
};
use orion_msg_peers::{
    EnrollChallenge, EnrollConfirm, EnrollHello, EnrollRole, OperatorEnroll, OperatorList,
    OperatorWelcome, Operators, PeerRevoke,
};
use orion_msg_session::{Hello, Refusal, Welcome};
use orion_server::mem::{self, MemConnection, MemConnector};
use orion_server::{Comm, Error, FdSet, Limits, Principal, Roles, Security};
use orion_types::{KeyId, NodeName};
use orion_wire::frame;
use orion_wire::{Body, Bytes, Decode, encode_body, kind};

const ROOT: Principal = Principal::Local { uid: 0, gid: 0 };
const USER: Principal = Principal::Local {
    uid: 1000,
    gid: 1000,
};
const SECRET: &[u8] = b"correct horse";

fn run(test: impl Future<Output = ()>) {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .start_paused(true)
        .build()
        .expect("runtime");
    tokio::task::LocalSet::new().block_on(&runtime, test);
}

fn board(security: Security, enrollment_key: Option<&[u8]>) -> (SharedKeys, MemConnector) {
    let config = KeysConfig {
        enrollment_key: enrollment_key.map(<[u8]>::to_vec),
        operators_actions: vec!["locate".to_owned()],
        ..KeysConfig::in_memory("lab")
    };
    let keys = SharedKeys::new(Keys::open(config, security).expect("keys"));
    let node: NodeName = "board".parse().expect("name");
    let comm = Comm::new(
        node.clone(),
        security,
        Some(Rc::new(keys.clone())),
        Limits::default(),
    );
    comm.add_service(KeysService::new(node, keys.clone()));
    let (carrier, connector) = mem::carrier(Roles::CLIENT.and(Roles::PEER), 64 * 1024);
    tokio::task::spawn_local(async move { comm.serve(carrier).await });
    (keys, connector)
}

struct Client {
    connection: MemConnection,
    buf: Vec<u8>,
    len: usize,
    seq: u16,
}

impl Client {
    async fn open(connector: &MemConnector, principal: Principal) -> Self {
        let mut client = Self {
            connection: connector.connect(principal),
            buf: vec![0; 16 * 1024],
            len: 0,
            seq: 0,
        };
        let hello = Hello {
            name: "test",
            roles: Roles::CLIENT,
            max_frame: 16 * 1024,
            revision: 1,
        };
        client.send(&hello).await;
        let _: Welcome = client.recv().await.expect("welcome");
        client
    }

    async fn send<B: Body>(&mut self, body: &B) -> u16 {
        self.seq += 1;
        let mut out = [0_u8; 1024];
        let len = encode_body(body, self.seq, &mut out).expect("encode");
        self.connection.send(&out[..len], &[]).await.expect("send");
        self.seq
    }

    /// The answer as a `B`, or the error it was refused with.
    async fn recv<'a, B: Body + Decode<'a>>(&'a mut self) -> Result<B, Option<Error>> {
        let mut fds = FdSet::default();
        self.len = self
            .connection
            .recv(&mut self.buf, &mut fds)
            .await
            .expect("recv")
            .expect("open");
        let frame = frame::decode(&self.buf[..self.len]).expect("frame");
        if frame.kind() == kind::REFUSAL {
            return Err(Refusal::decode(frame.payload()).expect("refusal").error());
        }
        Ok(orion_wire::decode_body(&frame).unwrap_or_else(|_| panic!("kind {:#x}", frame.kind())))
    }

    /// Asks to be enrolled and returns the state it was left in.
    async fn enroll(
        &mut self,
        key: &[u8; 32],
        proof: &[u8],
    ) -> Result<(OperatorState, bool), Option<Error>> {
        let enroll = OperatorEnroll {
            public_key: Bytes(key),
            name: "alice",
            proof: Bytes(proof),
        };
        self.send(&enroll).await;
        let welcome: OperatorWelcome<'_> = self.recv().await?;
        assert_eq!((welcome.node.as_str(), welcome.cluster), ("board", "lab"));
        let entry = welcome
            .operator
            .iter()
            .next()
            .expect("one entry")
            .expect("entry");
        Ok((entry.state, entry.all_actions))
    }

    /// Lists the operators, which needs read access: how many there are, or the refusal.
    async fn list(&mut self) -> Result<usize, Option<Error>> {
        self.send(&OperatorList {}).await;
        let operators: Operators<'_> = self.recv().await?;
        Ok(operators.operators.len())
    }
}

#[test]
fn enroll_refuse_and_revoke() {
    run(async {
        let (keys, connector) = board(Security::On, Some(SECRET));
        let alice = Identity::generate().public();
        let mut operator = Client::open(&connector, Principal::Operator(KeyId(alice))).await;
        let mut admin = Client::open(&connector, ROOT).await;

        // An operator that is not enrolled may do nothing, and may ask.
        assert_eq!(operator.list().await, Err(Some(Error::Denied)));
        assert_eq!(
            operator.enroll(&alice, b"").await,
            Ok((OperatorState::Pending, false))
        );
        assert_eq!(operator.list().await, Err(Some(Error::Denied)));

        // It may not speak for another key, and neither may an ordinary program on the board.
        let other = Identity::generate().public();
        assert_eq!(operator.enroll(&other, b"").await, Err(Some(Error::Denied)));
        let mut user = Client::open(&connector, USER).await;
        assert_eq!(user.enroll(&other, b"").await, Err(Some(Error::Denied)));

        // An administrator approves it: now it may read.
        assert_eq!(
            admin.enroll(&alice, b"").await,
            Ok((OperatorState::Enrolled, false))
        );
        assert_eq!(operator.list().await, Ok(1));
        assert_eq!(admin.list().await, Ok(1));

        // An operator that knows the enrollment key enrolls itself.
        let bob = Identity::generate().public();
        let mut second = Client::open(&connector, Principal::Operator(KeyId(bob))).await;
        let good = proof(SECRET, &operator_proof("board", &bob));
        assert_eq!(
            second.enroll(&bob, &good).await,
            Ok((OperatorState::Enrolled, false))
        );
        assert_eq!(second.list().await, Ok(2));

        // Only an administrator revokes.
        second.send(&PeerRevoke { key: KeyId(alice) }).await;
        assert_eq!(
            second.recv::<Refusal>().await.err(),
            Some(Some(Error::Denied))
        );
        admin.send(&PeerRevoke { key: KeyId(bob) }).await;
        // Revoked: nothing is granted any more, on the connection it already has.
        assert_eq!(second.list().await, Err(Some(Error::Denied)));
        // And it is not enrolled again by asking, even with the proof that worked before.
        assert_eq!(
            second.enroll(&bob, &good).await,
            Ok((OperatorState::Revoked, false))
        );
        assert_eq!(second.list().await, Err(Some(Error::Denied)));
        assert!(keys.lock().is_revoked(&bob));
        // The other operator is untouched.
        assert_eq!(operator.list().await, Ok(2));
    });
}

#[test]
fn with_security_off_a_tool_with_no_key_may_do_everything_and_nothing_is_recorded() {
    run(async {
        let (keys, connector) = board(Security::Off, None);
        let mut tool = Client::open(&connector, Principal::Anonymous).await;
        assert_eq!(
            tool.enroll(&[0; 32], b"").await,
            Ok((OperatorState::Enrolled, true))
        );
        assert_eq!(tool.list().await, Ok(0));
        assert_eq!(keys.lock().operators().count(), 0);
    });
}

/// The initiator's side of the node handshake, as another node runs it.
struct Initiator {
    identity: Identity,
    name: NodeName,
    nonce: [u8; NONCE_LEN],
}

impl Initiator {
    fn new(name: &str) -> Self {
        Self {
            identity: Identity::generate(),
            name: name.parse().expect("name"),
            nonce: random(),
        }
    }

    async fn hello(&self, client: &mut Client, role: EnrollRole) {
        let public = self.identity.public();
        let hello = EnrollHello {
            cluster: "lab",
            initiator: self.name.clone(),
            public_key: Bytes(&public),
            nonce: Bytes(&self.nonce),
            responder: "board".parse().expect("name"),
            role,
        };
        client.send(&hello).await;
    }

    /// Checks the board's challenge against `secret` and returns the confirm to send, with
    /// the board's key and nonce.
    fn answer(
        &self,
        challenge: &EnrollChallenge<'_>,
        role: EnrollRole,
        secret: &[u8],
    ) -> (Vec<u8>, [u8; 64], Vec<u8>) {
        let public = self.identity.public();
        let transcript = Transcript {
            role: role.code(),
            cluster: "lab",
            initiator: self.name.as_str(),
            initiator_key: &public,
            initiator_nonce: &self.nonce,
            responder: "board",
            responder_key: &challenge.public_key,
            responder_nonce: &challenge.nonce,
        };
        // The board proved it knows the key and holds the identity it presents.
        let theirs = transcript.message(Side::Responder);
        assert!(verify_proof(SECRET, &theirs, &challenge.proof));
        let board_key: [u8; 32] = challenge.public_key.0.try_into().expect("key");
        assert!(verify(&board_key, &theirs, &challenge.signature));
        let ours = transcript.message(Side::Initiator);
        (
            proof(secret, &ours).to_vec(),
            self.identity.sign(&ours),
            challenge.nonce.to_vec(),
        )
    }
}

async fn confirm(
    client: &mut Client,
    initiator: &Initiator,
    parts: &(Vec<u8>, [u8; 64], Vec<u8>),
) -> Result<(), Option<Error>> {
    let confirm = EnrollConfirm {
        initiator: initiator.name.clone(),
        responder: "board".parse().expect("name"),
        nonce: Bytes(&parts.2),
        proof: Bytes(&parts.0),
        signature: Bytes(&parts.1),
    };
    client.send(&confirm).await;
    client.recv::<EnrollConfirm<'_>>().await.map(|_| ())
}

#[test]
fn a_node_that_knows_the_enrollment_key_is_pinned_and_one_that_does_not_is_not() {
    run(async {
        let (keys, connector) = board(Security::On, Some(SECRET));
        let left: NodeName = "left".parse().expect("name");

        // The handshake, start to finish.
        let node = Initiator::new("left");
        let mut client = Client::open(&connector, Principal::Anonymous).await;
        node.hello(&mut client, EnrollRole::Node).await;
        let challenge: EnrollChallenge<'_> = client.recv().await.expect("challenge");
        let parts = node.answer(&challenge, EnrollRole::Node, SECRET);
        assert_eq!(confirm(&mut client, &node, &parts).await, Ok(()));
        assert_eq!(keys.lock().peer_key(&left), Some(&node.identity.public()));
        assert_eq!(
            keys.lock().principal(&node.identity.public()),
            Principal::Peer(left.clone())
        );

        // The same confirm again is refused: its handshake is used up.
        assert_eq!(
            confirm(&mut client, &node, &parts).await,
            Err(Some(Error::OutOfOrder))
        );
        // So is a confirm on another connection than the hello.
        let mut elsewhere = Client::open(&connector, Principal::Anonymous).await;
        assert_eq!(
            confirm(&mut elsewhere, &node, &parts).await,
            Err(Some(Error::OutOfOrder))
        );

        // A node that does not know the key is not pinned, and gets one try per hello.
        let stranger = Initiator::new("stranger");
        stranger.hello(&mut client, EnrollRole::Node).await;
        let challenge: EnrollChallenge<'_> = client.recv().await.expect("challenge");
        let wrong = stranger.answer(&challenge, EnrollRole::Node, b"a guess");
        let right = stranger.answer(&challenge, EnrollRole::Node, SECRET);
        assert_eq!(
            confirm(&mut client, &stranger, &wrong).await,
            Err(Some(Error::Denied))
        );
        assert_eq!(
            confirm(&mut client, &stranger, &right).await,
            Err(Some(Error::OutOfOrder))
        );
        assert_eq!(
            keys.lock().peer_key(&"stranger".parse().expect("name")),
            None
        );

        // Another node claiming a name that is already pinned to a key is refused.
        let impostor = Initiator::new("left");
        impostor.hello(&mut client, EnrollRole::Node).await;
        let challenge: EnrollChallenge<'_> = client.recv().await.expect("challenge");
        let parts = impostor.answer(&challenge, EnrollRole::Node, SECRET);
        assert_eq!(
            confirm(&mut client, &impostor, &parts).await,
            Err(Some(Error::Denied))
        );
        assert_eq!(keys.lock().peer_key(&left), Some(&node.identity.public()));

        // A proof made for one role does not enroll in another.
        let tool = Initiator::new("tool");
        tool.hello(&mut client, EnrollRole::Operator).await;
        let challenge: EnrollChallenge<'_> = client.recv().await.expect("challenge");
        let as_node = tool.answer_unchecked(&challenge, EnrollRole::Node);
        assert_eq!(
            confirm(&mut client, &tool, &as_node).await,
            Err(Some(Error::Denied))
        );
        // The same handshake in the operator role enrolls an operator.
        tool.hello(&mut client, EnrollRole::Operator).await;
        let challenge: EnrollChallenge<'_> = client.recv().await.expect("challenge");
        let parts = tool.answer(&challenge, EnrollRole::Operator, SECRET);
        assert_eq!(confirm(&mut client, &tool, &parts).await, Ok(()));
        let enrolled = keys
            .lock()
            .operator(&KeyId(tool.identity.public()))
            .map(|operator| operator.state);
        assert_eq!(enrolled, Some(OperatorState::Enrolled));

        // A hello for another board or another cluster is not this board's to answer.
        let public = node.identity.public();
        let elsewhere_hello = EnrollHello {
            cluster: "lab",
            initiator: left.clone(),
            public_key: Bytes(&public),
            nonce: Bytes(&node.nonce),
            responder: "another".parse().expect("name"),
            role: EnrollRole::Node,
        };
        client.send(&elsewhere_hello).await;
        assert_eq!(
            client.recv::<EnrollChallenge<'_>>().await.err(),
            Some(Some(Error::NotFound))
        );
    });
}

impl Initiator {
    /// A confirm made for `role` without checking the challenge against that role.
    fn answer_unchecked(
        &self,
        challenge: &EnrollChallenge<'_>,
        role: EnrollRole,
    ) -> (Vec<u8>, [u8; 64], Vec<u8>) {
        let public = self.identity.public();
        let ours = Transcript {
            role: role.code(),
            cluster: "lab",
            initiator: self.name.as_str(),
            initiator_key: &public,
            initiator_nonce: &self.nonce,
            responder: "board",
            responder_key: &challenge.public_key,
            responder_nonce: &challenge.nonce,
        }
        .message(Side::Initiator);
        (
            proof(SECRET, &ours).to_vec(),
            self.identity.sign(&ours),
            challenge.nonce.to_vec(),
        )
    }
}

#[test]
fn without_an_enrollment_key_nobody_enrolls_themselves() {
    run(async {
        let (keys, connector) = board(Security::On, None);
        let node = Initiator::new("left");
        let mut client = Client::open(&connector, Principal::Anonymous).await;
        node.hello(&mut client, EnrollRole::Node).await;
        assert_eq!(
            client.recv::<EnrollChallenge<'_>>().await.err(),
            Some(Some(Error::Unsupported))
        );
        // An operator that asks waits for an administrator, whatever it sends as proof.
        let alice = Identity::generate().public();
        let mut operator = Client::open(&connector, Principal::Operator(KeyId(alice))).await;
        let guess = proof(b"", &operator_proof("board", &alice));
        assert_eq!(
            operator.enroll(&alice, &guess).await,
            Ok((OperatorState::Pending, false))
        );
        assert_eq!(keys.lock().peers().count(), 0);
    });
}
