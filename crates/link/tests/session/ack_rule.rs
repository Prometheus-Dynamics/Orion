//! Acknowledgements of the device's reliable streams (provider snapshot, action catalog,
//! `DeviceInfo`): each stream is acknowledged only by an `Ack` for its own latest transmission
//! (docs/link-protocol.md, "Reliability"). An ack for an earlier transmission never clears it.

use orion_core::{NodeId, ProviderId, ResourceId};
use orion_link::device::{DeviceConfig, DeviceEvent, StreamDevice};
use orion_link::message::{Message, ProviderRecord, ResourceRecord, Welcome, kind};
use orion_link::wire::{ActivateMode, FirmwareInfo, FirmwareSink, XferBeginView, XferStatus};
use orion_link::{Stream, StreamDecoder, StreamEncoder};

/// A two-slot firmware sink, so the device reports `DeviceInfo`. It never transfers anything.
struct TwoSlots;

impl FirmwareSink for TwoSlots {
    fn info(&self) -> FirmwareInfo<'_> {
        FirmwareInfo {
            fw_version: "1.0.0",
            active_slot: 0,
            slots: 2,
            trial: false,
            max_chunk: 64,
            window: 1,
            bootloader: "",
        }
    }
    fn begin(&mut self, _: &XferBeginView<'_>) -> Result<u32, XferStatus> {
        Err(XferStatus::Unsupported)
    }
    fn write(&mut self, _: u32, _: &[u8]) -> Result<(), XferStatus> {
        Err(XferStatus::Unsupported)
    }
    fn finish(&mut self, _: u32, _: u32, _: u32) -> Result<(), XferStatus> {
        Err(XferStatus::Unsupported)
    }
    fn activate(&mut self, _: u32, _: ActivateMode) -> Result<(), XferStatus> {
        Err(XferStatus::Unsupported)
    }
    fn confirm(&mut self, _: u8) -> Result<(), XferStatus> {
        Err(XferStatus::Unsupported)
    }
}

type Device = StreamDevice<512, 512, &'static str, 512, TwoSlots>;

fn provider() -> ProviderRecord {
    ProviderRecord::builder(ProviderId::new("provider.dev"), NodeId::new("node-a")).build()
}

fn resource() -> ResourceRecord {
    ResourceRecord::builder(
        ResourceId::new("dev.r1"),
        "imu.sample_source",
        ProviderId::new("provider.dev"),
    )
    .build()
}

/// Feeds one host frame with sequence number `seq` to the device.
fn host_frame(device: &mut Device, message: &Message, seq: u16) {
    let mut frame = vec![0u8; 1024];
    let len = message.encode(seq, &mut frame).unwrap();
    let wire: Vec<u8> = StreamEncoder::for_frame(&frame[..len]).collect();
    device.receive(&wire);
}

/// The `(kind, seq)` of every frame the device sends now.
fn sent(device: &mut Device) -> Vec<(u8, u16)> {
    let mut bytes = Vec::new();
    let mut chunk = [0u8; 64];
    loop {
        let n = device.transmit(&mut chunk);
        if n == 0 {
            break;
        }
        bytes.extend_from_slice(&chunk[..n]);
    }
    let mut decoder = StreamDecoder::<1024>::new();
    let mut out = Vec::new();
    let mut rest = &bytes[..];
    while !rest.is_empty() {
        let (used, result) = decoder.push_slice(rest);
        if let Ok(Some(frame)) = result {
            out.push((frame.header().kind, frame.header().seq));
        }
        rest = &rest[used..];
    }
    out
}

/// The sequence number of the first frame of `kind` in `frames`.
fn seq_of(frames: &[(u8, u16)], kind: u8) -> u16 {
    frames
        .iter()
        .find(|(k, _)| *k == kind)
        .map(|(_, seq)| *seq)
        .unwrap_or_else(|| panic!("no frame of kind {kind:#04x} in {frames:?}"))
}

fn has(frames: &[(u8, u16)], kind: u8) -> bool {
    frames.iter().any(|(k, _)| *k == kind)
}

fn events(device: &mut Device) -> Vec<DeviceEvent> {
    core::iter::from_fn(|| device.next_event()).collect()
}

#[test]
fn stale_acks_never_clear_a_reliable_stream_and_a_dropped_snapshot_is_retransmitted() {
    let mut device = Device::with_firmware(DeviceConfig::provider("dev"), Stream, TwoSlots);
    device.declare_actions(&["locate", "flash"]).unwrap();
    device
        .publish_provider_state(&provider(), &[resource()])
        .unwrap();
    device.poll(0);
    assert!(has(&sent(&mut device), kind::HELLO));

    // Heartbeat long enough that the host is never silent for the whole test.
    host_frame(
        &mut device,
        &Message::Welcome(Welcome {
            node_id: NodeId::new("node-a"),
            session_id: 1,
            heartbeat_ms: 10_000,
            max_frame: 4_096,
            revision: 0,
        }),
        100,
    );
    device.poll(1);
    let first = sent(&mut device);
    let (catalog1, info1, snapshot1) = (
        seq_of(&first, kind::ACTION_CATALOG),
        seq_of(&first, kind::DEVICE_INFO),
        seq_of(&first, kind::PROVIDER_STATE),
    );
    assert!(catalog1 != info1 && info1 != snapshot1 && catalog1 != snapshot1);

    // The catalog and DeviceInfo are acknowledged; the snapshot's acknowledgement is lost.
    let mut host_seq = 100u16;
    let mut ack = |device: &mut Device, acked: u16| {
        host_seq = host_seq.wrapping_add(1);
        host_frame(device, &Message::Ack { seq: acked }, host_seq);
    };
    ack(&mut device, catalog1);
    ack(&mut device, info1);
    assert!(device.state_pending());

    // The snapshot is sent again after its retransmission interval, and only the snapshot.
    device.poll(1_000);
    let second = sent(&mut device);
    assert!(!has(&second, kind::ACTION_CATALOG) && !has(&second, kind::DEVICE_INFO));
    let snapshot2 = seq_of(&second, kind::PROVIDER_STATE);
    assert_ne!(snapshot2, snapshot1);

    // Stale acks: the snapshot's earlier transmission, and the catalog's and DeviceInfo's
    // transmissions. None of them is the snapshot's latest transmission.
    ack(&mut device, snapshot1);
    ack(&mut device, catalog1);
    ack(&mut device, info1);
    assert!(
        device.state_pending(),
        "an ack for an earlier transmission does not acknowledge the snapshot"
    );
    assert!(!events(&mut device).contains(&DeviceEvent::StateAcked));

    // Still unacknowledged: sent again.
    device.poll(2_000);
    let third = sent(&mut device);
    let snapshot3 = seq_of(&third, kind::PROVIDER_STATE);
    assert_ne!(snapshot3, snapshot2);
    assert!(!has(&third, kind::ACTION_CATALOG) && !has(&third, kind::DEVICE_INFO));

    // Its own latest acknowledgement settles it, and nothing is sent afterwards.
    ack(&mut device, snapshot3);
    assert!(!device.state_pending());
    assert!(events(&mut device).contains(&DeviceEvent::StateAcked));
    device.poll(4_000);
    assert!(!has(&sent(&mut device), kind::PROVIDER_STATE));
}
