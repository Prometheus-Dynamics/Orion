//! The hello document: signed, fresh, carrying other programs' facts, and there with nothing
//! else running.

use std::rc::Rc;

use orion_broker::{Broker, BrokerConfig};
use orion_hello::{Hello as HelloService, verify};
use orion_keys::{Identity as Key, Keys, KeysConfig, SharedKeys, random};
use orion_msg_session::{Hello, Identify, Identity, Refusal, Welcome};
use orion_msg_topics::{Declare, Field, FieldType, Opened};
use orion_server::mem::{self, MemConnection, MemConnector};
use orion_server::{Comm, Error, FdSet, Limits, Principal, Roles, Security};
use orion_types::KeyId;
use orion_wire::frame;
use orion_wire::{Body, Bytes, Decode, List, encode_body, kind};
use shm_ring::{FieldId, Flags, SampleTime, Writer};

const USER: Principal = Principal::Local {
    uid: 1000,
    gid: 1000,
};

fn run(test: impl Future<Output = ()>) {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .start_paused(true)
        .build()
        .expect("runtime");
    tokio::task::LocalSet::new().block_on(&runtime, test);
}

/// A comm core with nothing but the broker and hello: no node logic, no other client.
fn board(security: Security) -> (SharedKeys, MemConnector) {
    let keys = SharedKeys::new(Keys::open(KeysConfig::in_memory("lab"), security).expect("keys"));
    let node: orion_types::NodeName = "board".parse().expect("name");
    let comm = Comm::new(
        node.clone(),
        security,
        Some(Rc::new(keys.clone())),
        Limits::default(),
    );
    let broker = Broker::new(node.clone(), BrokerConfig::default());
    comm.add_service(HelloService::new(node, keys.clone(), Some(broker.topics())));
    comm.add_service(broker);
    let (carrier, connector) = mem::carrier(Roles::CLIENT.and(Roles::PROVIDER), 64 * 1024);
    tokio::task::spawn_local(async move { comm.serve(carrier).await });
    (keys, connector)
}

struct Client {
    connection: MemConnection,
    buf: Vec<u8>,
    len: usize,
    fds: FdSet,
}

impl Client {
    async fn open(connection: MemConnection) -> Self {
        let mut client = Self {
            connection,
            buf: vec![0; 32 * 1024],
            len: 0,
            fds: FdSet::default(),
        };
        let hello = Hello {
            name: "test",
            roles: Roles::CLIENT.and(Roles::PROVIDER),
            max_frame: 32 * 1024,
            revision: 1,
        };
        client.send(&hello).await;
        assert_eq!(client.next().await, kind::WELCOME);
        let _: Welcome = Welcome::decode(client.payload()).expect("welcome");
        client
    }

    async fn send<B: Body>(&mut self, body: &B) {
        let mut out = [0_u8; 1024];
        let len = encode_body(body, 1, &mut out).expect("encode");
        self.connection.send(&out[..len], &[]).await.expect("send");
    }

    async fn next(&mut self) -> u8 {
        self.fds = FdSet::default();
        self.len = self
            .connection
            .recv(&mut self.buf, &mut self.fds)
            .await
            .expect("recv")
            .expect("open");
        frame::decode(&self.buf[..self.len]).expect("frame").kind()
    }

    fn payload(&self) -> &[u8] {
        frame::decode(&self.buf[..self.len])
            .expect("frame")
            .payload()
    }

    /// Asks who the board is and returns the answer's payload.
    async fn identify(&mut self, nonce: &[u8]) -> &[u8] {
        self.send(&Identify {
            nonce: Bytes(nonce),
        })
        .await;
        assert_eq!(self.next().await, kind::IDENTITY);
        self.payload()
    }

    /// Publishes one board fact as a topic and returns the writer that keeps it.
    async fn publish_fact(&mut self, name: &str, ty: FieldType, value: &[u8]) -> Writer {
        let fields = [Field {
            name: "value",
            ty,
            unit: "",
            type_name: "",
        }];
        let declare = Declare {
            topic: name.parse().expect("topic"),
            fields: List::new(&fields),
            history: 2,
            expiry_ms: 0,
            max_payload: 512,
        };
        self.send(&declare).await;
        assert_eq!(self.next().await, kind::TOPIC_OPENED);
        let _: Opened = Opened::decode(self.payload()).expect("opened");
        let mut writer = Writer::attach(self.fds.take(0).expect("write end")).expect("writer");
        writer
            .publish(FieldId(0), SampleTime::ZERO, Flags::NONE, value)
            .expect("publish");
        writer
    }
}

fn facts<'a>(identity: &Identity<'a>) -> Vec<(&'a str, u8, &'a [u8])> {
    identity
        .fields
        .iter()
        .map(|field| {
            let field = field.expect("field");
            (field.name, field.ty, field.value.0)
        })
        .collect()
}

#[test]
fn a_signed_hello_verifies_and_answers_only_the_question_it_was_asked() {
    run(async {
        let (keys, connector) = board(Security::On);
        let board_key = keys.lock().identity().public();
        // Nothing is known about this caller, and no node logic is running: it is answered.
        let mut stranger = Client::open(connector.connect(Principal::Anonymous)).await;
        let nonce: [u8; 32] = random();
        let payload = stranger.identify(&nonce).await.to_vec();

        let identity = verify(&payload, &nonce).expect("a genuine answer");
        assert_eq!(
            (identity.node.as_str(), identity.cluster, identity.revision),
            ("board", "lab", 1)
        );
        assert_eq!(identity.public_key.0, board_key);
        assert!(identity.time_ms > 1_700_000_000_000);
        assert!(
            identity.fields.is_empty(),
            "Orion invents no facts about the board"
        );

        // The same answer does not pass for another question.
        assert!(verify(&payload, &random::<32>()).is_none());
        // Any byte changed, anywhere, and it does not verify.
        for at in [
            0,
            3,
            12,
            payload.len() / 2,
            payload.len() - 70,
            payload.len() - 1,
        ] {
            let mut changed = payload.clone();
            changed[at] ^= 1;
            assert!(verify(&changed, &nonce).is_none(), "byte {at}");
        }
        // Signed by another key in the board's name: it names the board's key, so no.
        let forger = Key::generate();
        let mut forged = payload.clone();
        let unsigned = forged.len() - 65;
        let mut message = b"orion-hello-v1".to_vec();
        message.extend_from_slice(&forged[..unsigned]);
        let signature = forger.sign(&message);
        forged[unsigned + 1..].copy_from_slice(&signature);
        assert!(verify(&forged, &nonce).is_none());
        // Cut short, or not a document at all.
        assert!(verify(&payload[..payload.len() - 1], &nonce).is_none());
        assert!(verify(&[], &nonce).is_none());

        // An operator the board has never heard of may ask too: this needs no grant.
        let unknown = Principal::Operator(KeyId([4; 32]));
        let mut tool = Client::open(connector.connect_without_descriptors(unknown)).await;
        let payload = tool.identify(&nonce).await.to_vec();
        assert!(verify(&payload, &nonce).is_some());
        // Over a connection that is not on the board the answer is signed even with no nonce.
        let payload = tool.identify(&[]).await.to_vec();
        assert!(verify(&payload, &[]).is_some());
        // A nonce longer than allowed is refused, not signed.
        tool.send(&Identify {
            nonce: Bytes(&[0; 33]),
        })
        .await;
        assert_eq!(tool.next().await, kind::REFUSAL);
        assert_eq!(
            Refusal::decode(tool.payload()).expect("refusal").error(),
            Some(Error::Malformed)
        );
    });
}

#[test]
fn facts_other_programs_publish_are_in_the_document_and_go_when_they_go() {
    run(async {
        let (_keys, connector) = board(Security::Off);
        let mut asker = Client::open(connector.connect(USER)).await;
        let nonce = [1_u8; 8];

        // The device's own package says what the board is. Orion passes it on.
        let mut package = Client::open(connector.connect(USER)).await;
        let _model = package
            .publish_fact("hello.model", FieldType::Text, b"x1")
            .await;
        let mut serial = package
            .publish_fact("hello.serial", FieldType::Text, b"0000a1")
            .await;
        // A topic that is not a fact, a fact with no value, and one too long to carry.
        let _other = package
            .publish_fact("cpu.temp", FieldType::Float, &21.5_f64.to_le_bytes())
            .await;
        let mut driver = Client::open(connector.connect(USER)).await;
        let _empty = driver
            .publish_fact("hello.empty", FieldType::Bytes, b"")
            .await;
        let _long = driver
            .publish_fact("hello.long", FieldType::Bytes, &[7; 300])
            .await;
        let _slots = driver
            .publish_fact("hello.slots", FieldType::Int, &2_i64.to_le_bytes())
            .await;

        let payload = asker.identify(&nonce).await.to_vec();
        let identity = verify(&payload, &nonce).expect("genuine");
        let text = FieldType::Text.code();
        assert_eq!(
            facts(&identity),
            [
                ("model", text, &b"x1"[..]),
                ("serial", text, b"0000a1"),
                ("slots", FieldType::Int.code(), &2_i64.to_le_bytes()),
            ],
            "in name order, with their types"
        );

        // A fact that changes is the new value the next time someone asks.
        serial
            .publish(FieldId(0), SampleTime::ZERO, Flags::NONE, b"0000b2")
            .expect("publish");
        let payload = asker.identify(&nonce).await.to_vec();
        assert_eq!(
            facts(&verify(&payload, &nonce).expect("genuine"))[1].2,
            b"0000b2"
        );

        // The driver goes away: its fact goes with it. The rest of the document is unchanged.
        drop(driver);
        tokio::time::sleep(std::time::Duration::from_millis(1)).await;
        let payload = asker.identify(&nonce).await.to_vec();
        let identity = verify(&payload, &nonce).expect("genuine");
        let names: Vec<&str> = facts(&identity).iter().map(|fact| fact.0).collect();
        assert_eq!(names, ["model", "serial"]);

        // Someone on the board who gives no nonce gets the document unsigned.
        let payload = asker.identify(&[]).await.to_vec();
        let identity = Identity::decode(&payload).expect("document");
        assert!(identity.signature.is_empty() && identity.fields.len() == 2);
        assert!(verify(&payload, &[]).is_none(), "nothing to verify");
    });
}
