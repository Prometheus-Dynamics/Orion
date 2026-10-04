//! Host-side check that the template ports talk to a real `HostSession` / `HostBus`.

use std::collections::VecDeque;
use std::convert::Infallible;

use embedded_can::{ExtendedId, Frame, Id, StandardId};
use orion_link::device::DeviceEvent;
use orion_link::host::{HostBus, HostConfig, HostEvent, HostSession};
use orion_link::message::{LeaseRecord, NodeId};
use orion_link::{CanLinkIds, Packet};
use orion_mcu_template::{CanPort, UartPort, provider_record, resource_record};

/// A loopback UART: `rx` is what the device reads, `tx` what it wrote.
#[derive(Default)]
struct MockUart {
    rx: VecDeque<u8>,
    tx: Vec<u8>,
}

impl embedded_io::ErrorType for MockUart {
    type Error = Infallible;
}

impl embedded_io::Read for MockUart {
    fn read(&mut self, buf: &mut [u8]) -> Result<usize, Infallible> {
        let n = buf.len().min(self.rx.len());
        for slot in &mut buf[..n] {
            *slot = self.rx.pop_front().unwrap();
        }
        Ok(n)
    }
}

impl embedded_io::ReadReady for MockUart {
    fn read_ready(&mut self) -> Result<bool, Infallible> {
        Ok(!self.rx.is_empty())
    }
}

impl embedded_io::Write for MockUart {
    fn write(&mut self, buf: &[u8]) -> Result<usize, Infallible> {
        self.tx.extend_from_slice(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> Result<(), Infallible> {
        Ok(())
    }
}

fn lease() -> Vec<LeaseRecord> {
    vec![
        LeaseRecord::builder("imu-board.imu-0")
            .holder_node(NodeId::new("node-a"))
            .build(),
    ]
}

#[test]
fn uart_port_connects_publishes_and_receives_leases() {
    let mut port = UartPort::new(MockUart::default(), "imu-board");
    let provider = provider_record("imu-board", "imu.sample_source").unwrap();
    let resource =
        resource_record("imu-board", "imu-board.imu-0", "imu.sample_source", true).unwrap();
    port.publish(&provider, &[resource]).unwrap();

    let mut host = HostSession::stream(HostConfig::new(NodeId::new("node-a")));
    host.set_leases(lease());
    let mut got_state = false;
    let mut got_leases = false;
    for now in 0..200u64 {
        port.service(now).unwrap();
        let written = std::mem::take(&mut port.uart_mut().tx);
        host.receive(&written);
        host.poll(now);
        while let Some(bytes) = host.transmit() {
            port.uart_mut().rx.extend(bytes);
        }
        while let Some(event) = host.next_event() {
            got_state |= matches!(event, HostEvent::ProviderState { .. });
        }
        while let Some(event) = port.next_event() {
            got_leases |= event == DeviceEvent::Leases(lease());
        }
    }
    assert!(port.session().is_connected());
    assert!(got_state && got_leases);
}

/// A CAN frame type implementing `embedded_can::Frame` (classic CAN, up to 8 bytes).
#[derive(Debug, Clone)]
struct TestFrame {
    id: Id,
    data: Vec<u8>,
}

impl Frame for TestFrame {
    fn new(id: impl Into<Id>, data: &[u8]) -> Option<Self> {
        (data.len() <= 8).then(|| Self {
            id: id.into(),
            data: data.to_vec(),
        })
    }

    fn new_remote(_id: impl Into<Id>, _dlc: usize) -> Option<Self> {
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

/// A CAN controller with a one-frame transmit mailbox, so `WouldBlock` is exercised.
#[derive(Default)]
struct MockCan {
    rx: VecDeque<TestFrame>,
    mailbox: Option<TestFrame>,
}

impl embedded_can::nb::Can for MockCan {
    type Frame = TestFrame;
    type Error = Infallible;

    fn transmit(&mut self, frame: &TestFrame) -> nb::Result<Option<TestFrame>, Infallible> {
        if self.mailbox.is_some() {
            return Err(nb::Error::WouldBlock);
        }
        self.mailbox = Some(frame.clone());
        Ok(None)
    }

    fn receive(&mut self) -> nb::Result<TestFrame, Infallible> {
        self.rx.pop_front().ok_or(nb::Error::WouldBlock)
    }
}

#[test]
fn can_port_connects_through_a_host_bus() {
    let base = CanLinkIds::new(0x100, 0x180, false);
    let ids = CanLinkIds::for_address(base, 5).unwrap();
    let mut port = CanPort::new(MockCan::default(), ids, Packet::CLASSIC, "can-board");
    let provider = provider_record("can-board", "imu.sample_source").unwrap();
    port.publish(&provider, &[]).unwrap();
    let mut bus = HostBus::new(
        HostConfig::new(NodeId::new("node-a")),
        base,
        Packet::CLASSIC,
        1..=63,
    );

    for now in 0..2_000u64 {
        port.service(now).unwrap();
        // The bus "transmits" the mailbox frame and delivers host frames.
        if let Some(frame) = port.can_mut().mailbox.take() {
            let raw = match frame.id {
                Id::Standard(id) => u32::from(id.as_raw()),
                Id::Extended(id) => id.as_raw(),
            };
            bus.receive(raw, frame.is_extended(), &frame.data);
        }
        bus.poll(now);
        while let Some(out) = bus.next_frame() {
            let id = if out.extended {
                Id::Extended(ExtendedId::new(out.id).unwrap())
            } else {
                Id::Standard(StandardId::new(out.id as u16).unwrap())
            };
            port.can_mut()
                .rx
                .push_back(TestFrame::new(id, out.segment.as_bytes()).unwrap());
        }
        while bus.next_event().is_some() {}
        while port.next_event().is_some() {}
        if port.session().is_connected() && !port.session().state_pending() {
            break;
        }
    }
    assert!(port.session().is_connected());
    assert!(!port.session().state_pending());
    assert_eq!(bus.address_of("can-board"), Some(5));
}
