//! The Rust ports, against a host: on a UART and on a CAN controller, a firmware's layers
//! are declared and registered, publish once someone listens, and answer a run.

use std::collections::VecDeque;
use std::convert::Infallible;

use embedded_can::{Frame, Id, StandardId};
use orion_device_actions::{ActionSpec, Actions, Heard as Run, Outcome};
use orion_device_core::{Event, SegmentMtu};
use orion_device_topics::{Field, FieldType, TopicSpec, Topics};
use orion_mcu_template::{App, CanPort, FrameView, Link, TX, UartPort};
use orion_msg_actions::{ActionResult, Claim, Target};
use orion_msg_topics::Sample;
use orion_wire::{Bytes, kind};

mod common;
use common::{CanReader, Peer, StreamReader, segments, stream};

static TOPICS: [TopicSpec; 1] = [TopicSpec::latest(
    "imu-board.yaw",
    &[Field::new("yaw", FieldType::Float, "deg")],
)];

static ACTIONS: [ActionSpec; 1] = [ActionSpec::on_resource(
    "zero",
    "imu-board",
    "Sets yaw to zero",
)];

/// A firmware with a topic and an action: what a port's `App` holds.
struct Imu {
    topics: Topics<1>,
    actions: Actions<1>,
    yaw: f64,
}

impl Imu {
    const fn new() -> Self {
        Self {
            topics: Topics::new(&TOPICS),
            actions: Actions::new(&ACTIONS),
            yaw: 10.0,
        }
    }
}

impl App<TX> for Imu {
    fn handle(&mut self, link: &mut Link<TX>, frame: &FrameView<'_>) {
        self.topics.handle(frame);
        if let Run::Run { call, .. } = self.actions.handle(frame) {
            self.yaw = 0.0;
            let _ = self
                .actions
                .finish(link, call, Outcome::Succeeded, b"zeroed", "");
        }
    }

    fn poll(&mut self, link: &mut Link<TX>, now_ms: u32) {
        self.topics.poll(link, now_ms);
        self.actions.poll(link, now_ms);
        if self.topics.subscribers(0) > 0 {
            let _ = self
                .topics
                .publish_f64(link, 0, 0, u64::from(now_ms) * 1_000_000, self.yaw);
        }
    }
}

fn claim(peer: &mut Peer) -> Vec<u8> {
    peer.raw(&Claim {
        call: 5,
        name: "zero".parse().expect("name"),
        target: Target::Resource("imu-board".parse().expect("name")),
        args: Bytes(&[]),
        deadline_ms: 0,
        caller: "",
        session: 0,
    })
}

/// What the host saw: the device's name, its samples, and the run's result.
fn check(peer: &mut Peer) {
    assert_eq!(peer.name, "imu-board");
    assert_eq!(peer.topics.len(), 1);
    assert_eq!(peer.actions, 1);
    let samples = peer.take(kind::TOPIC_SAMPLE);
    assert!(samples.len() > 3, "{} samples", samples.len());
    let values: Vec<f64> = samples
        .iter()
        .map(|sent| {
            let sample: Sample = sent.body();
            assert_eq!(sample.handle, peer.topics[0].1);
            f64::from_le_bytes(sample.payload.0.try_into().expect("a float"))
        })
        .collect();
    assert_eq!(values.first(), Some(&10.0));
    assert_eq!(values.last(), Some(&0.0), "zeroed by the run");
    let results = peer.take(kind::ACTION_RESULT);
    assert_eq!(results.len(), 1);
    let result: ActionResult = results[0].body();
    assert_eq!((result.call, result.output.0), (5, &b"zeroed"[..]));
}

/// A UART in memory: `rx` is what the device reads, `tx` what it wrote.
#[derive(Default)]
struct Uart {
    rx: VecDeque<u8>,
    tx: Vec<u8>,
}

impl embedded_io::ErrorType for Uart {
    type Error = Infallible;
}

impl embedded_io::Read for Uart {
    fn read(&mut self, buf: &mut [u8]) -> Result<usize, Infallible> {
        let n = buf.len().min(self.rx.len());
        for slot in &mut buf[..n] {
            *slot = self.rx.pop_front().expect("a byte");
        }
        Ok(n)
    }
}

impl embedded_io::ReadReady for Uart {
    fn read_ready(&mut self) -> Result<bool, Infallible> {
        Ok(!self.rx.is_empty())
    }
}

impl embedded_io::Write for Uart {
    fn write(&mut self, buf: &[u8]) -> Result<usize, Infallible> {
        self.tx.extend_from_slice(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> Result<(), Infallible> {
        Ok(())
    }
}

#[test]
fn a_firmware_on_a_uart_declares_publishes_and_runs() {
    let mut port = UartPort::new(Uart::default(), "imu-board");
    let mut imu = Imu::new();
    let (mut peer, mut reader) = (Peer::default(), StreamReader::default());
    let mut connected = false;
    for now in 1..400_u32 {
        port.service(now, &mut imu).expect("service");
        while let Some(event) = port.next_event() {
            connected |= matches!(event, Event::Connected { .. });
        }
        let written = std::mem::take(&mut port.uart_mut().tx);
        for sent in reader.read(&written) {
            for frame in peer.answer(sent) {
                port.uart_mut().rx.extend(stream(&frame));
            }
        }
        if now == 200 {
            let frame = claim(&mut peer);
            port.uart_mut().rx.extend(stream(&frame));
        }
    }
    assert!(connected);
    check(&mut peer);
}

/// A classic CAN frame.
#[derive(Clone, Debug)]
struct CanFrame {
    id: Id,
    data: Vec<u8>,
}

impl Frame for CanFrame {
    fn new(id: impl Into<Id>, data: &[u8]) -> Option<Self> {
        (data.len() <= 8).then(|| Self {
            id: id.into(),
            data: data.to_vec(),
        })
    }

    fn new_remote(_: impl Into<Id>, _: usize) -> Option<Self> {
        None
    }

    fn is_extended(&self) -> bool {
        matches!(self.id, Id::Extended(_))
    }

    fn is_remote_frame(&self) -> bool {
        false
    }

    fn id(&self) -> Id {
        self.id
    }

    fn dlc(&self) -> usize {
        self.data.len()
    }

    fn data(&self) -> &[u8] {
        &self.data
    }
}

/// A controller on a bus in memory, with three transmit mailboxes.
#[derive(Default)]
struct Bus {
    rx: VecDeque<CanFrame>,
    tx: Vec<CanFrame>,
}

impl embedded_can::nb::Can for Bus {
    type Frame = CanFrame;
    type Error = embedded_can::ErrorKind;

    fn transmit(&mut self, frame: &CanFrame) -> nb::Result<Option<CanFrame>, Self::Error> {
        if self.tx.len() >= 3 {
            return Err(nb::Error::WouldBlock);
        }
        self.tx.push(frame.clone());
        Ok(None)
    }

    fn receive(&mut self) -> nb::Result<CanFrame, Self::Error> {
        self.rx.pop_front().ok_or(nb::Error::WouldBlock)
    }
}

#[test]
fn a_firmware_on_a_can_bus_declares_publishes_and_runs() {
    let id = |raw| Id::Standard(StandardId::new(raw).expect("an identifier"));
    let (to_host, from_host) = (id(0x105), id(0x185));
    let mut port = CanPort::new(
        Bus::default(),
        to_host,
        from_host,
        SegmentMtu::CLASSIC,
        "imu-board",
    );
    let mut imu = Imu::new();
    let (mut peer, mut reader) = (Peer::default(), CanReader::default());
    let send = |port: &mut CanPort<Bus>, frame: &[u8]| {
        for segment in segments(frame) {
            // Frames for another device are on the bus too, and are not this one's.
            port.can_mut().rx.push_back(CanFrame {
                id: id(0x186),
                data: vec![0; 8],
            });
            port.can_mut().rx.push_back(CanFrame {
                id: from_host,
                data: segment,
            });
        }
    };
    for now in 1..600_u32 {
        port.service(now, &mut imu).expect("service");
        for frame in std::mem::take(&mut port.can_mut().tx) {
            assert_eq!(frame.id, to_host);
            if let Some(sent) = reader.read(&frame.data) {
                for answer in peer.answer(sent) {
                    send(&mut port, &answer);
                }
            }
        }
        if now == 300 {
            let frame = claim(&mut peer);
            send(&mut port, &frame);
        }
    }
    check(&mut peer);
}
