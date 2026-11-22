//! A hello too large for a CAN bus, asked by a device on it: refused, saying its size and the
//! bus's limit, where it would have been sent to be lost in the reassembler.

use std::rc::Rc;

use orion_broker::{Broker, BrokerConfig};
use orion_hello::Hello as HelloService;
use orion_keys::{Keys, KeysConfig, SharedKeys};
use orion_link_can::{CanCarrier, MAX_FRAME};
use orion_msg_session::{Hello, Identify, Refusal, Welcome};
use orion_msg_topics::{Declare, Field, FieldType, Opened};
use orion_server::mem::{self, MemConnection};
use orion_server::{Comm, Error, FdSet, Limits, Principal, Roles, Security};
use orion_wire::{Body, Bytes, Decode, List, encode_body, frame, kind};
use shm_ring::{FieldId, Flags, SampleTime, Writer};

use super::bus::{BASE, Device, Interface, Wires, bus, run};

const ROOT: Principal = Principal::Local { uid: 0, gid: 0 };

/// A program on the board, on its socket.
struct Program {
    connection: MemConnection,
    buf: Vec<u8>,
    len: usize,
    fds: FdSet,
}

impl Program {
    async fn open(connection: MemConnection) -> Self {
        let mut program = Self {
            connection,
            buf: vec![0; 32 * 1024],
            len: 0,
            fds: FdSet::default(),
        };
        program
            .send(&Hello {
                name: "facts",
                roles: Roles::CLIENT.and(Roles::PROVIDER),
                max_frame: 32 * 1024,
                revision: 1,
                level: 0,
            })
            .await;
        assert_eq!(program.next().await, kind::WELCOME);
        program
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

    /// Publishes the text fact `name` as `value`; the writer keeps it.
    async fn publish_fact(&mut self, name: &str, value: &[u8]) -> Writer {
        let fields = [Field {
            name: "value",
            ty: FieldType::Text,
            unit: "",
            type_name: "",
        }];
        self.send(&Declare {
            topic: name.parse().expect("topic"),
            fields: List::new(&fields),
            history: 2,
            expiry_ms: 0,
            max_payload: 2048,
            on_subscribe: Default::default(),
        })
        .await;
        assert_eq!(self.next().await, kind::TOPIC_OPENED);
        let payload = frame::decode(&self.buf[..self.len]).expect("frame");
        let _: Opened = Opened::decode(payload.payload()).expect("opened");
        let mut writer = Writer::attach(self.fds.take(0).expect("write end")).expect("writer");
        writer
            .publish(FieldId(0), SampleTime::ZERO, Flags::NONE, value)
            .expect("publish");
        writer
    }
}

#[test]
fn a_hello_too_large_for_the_bus_is_refused_saying_its_size_and_the_limit() {
    run(async {
        let keys =
            SharedKeys::new(Keys::open(KeysConfig::in_memory("lab"), Security::Off).expect("keys"));
        let node: orion_types::NodeName = "board".parse().expect("name");
        let comm = Comm::new(node.clone(), Security::Off, None, Limits::default());
        let broker = Broker::new(node.clone(), BrokerConfig::default());
        comm.add_service(HelloService::new(node, keys, Some(broker.topics())));
        comm.add_service(broker);
        let wires = Rc::new(Wires::default());
        let carrier = CanCarrier::new(&bus(true), Interface(wires.clone())).expect("bus");
        let serving = comm.clone();
        tokio::task::spawn_local(async move { serving.serve(carrier).await });
        let (socket, connector) = mem::carrier(Roles::CLIENT.and(Roles::PROVIDER), 64 * 1024);
        let serving = comm.clone();
        tokio::task::spawn_local(async move { serving.serve(socket).await });

        // A device that says it takes 64 KiB is told the bus's 4096.
        let mut device = Device::new(&wires, BASE, 1, true);
        device.send(&Hello {
            name: "sensor",
            roles: Roles::DEVICE,
            max_frame: 64 * 1024,
            revision: 1,
            level: 0,
        });
        assert_eq!(device.next().await, kind::WELCOME);
        let welcome = Welcome::decode(&device.payload).expect("welcome");
        assert_eq!(welcome.max_frame as usize, MAX_FRAME);

        // A small hello comes.
        let nonce = [4_u8; 16];
        device.send(&Identify {
            nonce: Bytes(&nonce),
        });
        assert_eq!(device.next().await, kind::IDENTITY);

        // Five facts of 1000 bytes make the hello over 5000: refused, saying so.
        let mut program = Program::open(connector.connect(ROOT)).await;
        let mut kept = Vec::new();
        for at in 0..5 {
            kept.push(
                program
                    .publish_fact(&format!("hello.fact{at}"), &[b'f'; 1000])
                    .await,
            );
        }
        device.send(&Identify {
            nonce: Bytes(&nonce),
        });
        assert_eq!(device.next().await, kind::REFUSAL);
        let refusal = Refusal::decode(&device.payload).expect("refusal");
        assert_eq!(refusal.error(), Some(Error::BufferTooSmall));
        let size: usize = refusal
            .detail
            .strip_prefix("the hello is ")
            .and_then(|rest| rest.split(' ').next())
            .and_then(|size| size.parse().ok())
            .unwrap_or_else(|| panic!("no size in {:?}", refusal.detail));
        assert!(size > 5000, "{}", refusal.detail);
        assert!(
            refusal
                .detail
                .ends_with("bytes, over the 4096 bytes a frame to this session may be"),
            "{}",
            refusal.detail
        );
        drop(kept);
    });
}
