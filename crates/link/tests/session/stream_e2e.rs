//! Device ↔ host over a simulated byte stream.

use orion_link::device::{DeviceEvent, LinkState};
use orion_link::host::{HostEvent, HostSession};
use orion_link::message::{Message, RejectReason, kind};
use orion_link::{FrameHeader, StreamEncoder};

use crate::sim::{Faults, HEARTBEAT_MS, StreamPair, host_config, leases, provider, resources};

fn connected_pair(seed: u64) -> StreamPair {
    let mut pair = StreamPair::new("imu-board", host_config(), seed);
    assert!(pair.run_until(2_000, StreamPair::connected), "handshake");
    pair
}

#[test]
fn handshake_publish_and_ack() {
    let mut pair = StreamPair::new("imu-board", host_config(), 1);
    pair.device
        .publish_provider_state(&provider("imu-board"), &resources("imu-board", 1))
        .unwrap();
    assert!(pair.run_until(1_000, |p| !p.device.state_pending()));

    assert_eq!(pair.device.node_id().map(|n| n.as_str()), Some("node-a"));
    assert!(matches!(
        pair.device_events[0].1,
        DeviceEvent::Connected { ref node_id, .. } if node_id.as_str() == "node-a"
    ));
    assert!(matches!(
        &pair.host_events[0].1,
        HostEvent::DeviceConnected { device_name, .. } if device_name == "imu-board"
    ));
    assert_eq!(pair.host_states(), vec![&resources("imu-board", 1)]);
    assert_eq!(pair.count_device(|e| *e == DeviceEvent::StateAcked), 1);
    // The (empty) lease set arrives right after Welcome.
    assert_eq!(pair.device_leases(), vec![&Vec::new()]);
    assert_eq!(pair.host.device_name(), Some("imu-board"));
    // The connection is established within a few milliseconds of the first poll.
    assert!(pair.device_events[0].0 <= 10);
}

#[test]
fn lost_state_is_retransmitted_until_acked() {
    let mut pair = connected_pair(2);
    pair.up.drop_kinds = vec![kind::PROVIDER_STATE, kind::PROVIDER_STATE];
    pair.device
        .publish_provider_state(&provider("imu-board"), &resources("imu-board", 1))
        .unwrap();
    assert!(pair.run_until(3_000, |p| !p.device.state_pending()));
    assert_eq!(pair.host_states(), vec![&resources("imu-board", 1)]);
    assert!(pair.device.stats().state_retransmits >= 2);
}

#[test]
fn lost_ack_causes_retransmission_but_one_host_event() {
    let mut pair = connected_pair(3);
    pair.down.drop_kinds = vec![kind::ACK, kind::ACK];
    pair.device
        .publish_provider_state(&provider("imu-board"), &resources("imu-board", 1))
        .unwrap();
    assert!(pair.run_until(3_000, |p| !p.device.state_pending()));
    assert!(pair.device.stats().state_retransmits >= 2);
    assert_eq!(
        pair.host_states().len(),
        1,
        "duplicate snapshots are not re-reported"
    );
}

#[test]
fn retransmit_backoff_is_capped_at_the_heartbeat() {
    let mut pair = connected_pair(4);
    pair.up.faults.cut = true;
    pair.down.faults.cut = true;
    pair.device
        .publish_provider_state(&provider("imu-board"), &resources("imu-board", 1))
        .unwrap();
    // Before host loss is detected (3 heartbeats): 200, 400, 800, then capped at 1000 ms.
    pair.run(2_900);
    let sent = pair
        .up
        .sent_kinds
        .iter()
        .filter(|&&k| k == kind::PROVIDER_STATE);
    // t=0, +200, +400, +800 (=1400), +1000 (=2400) -> 5 transmissions.
    assert_eq!(sent.count(), 5);
}

#[test]
fn newest_snapshot_wins() {
    let mut pair = connected_pair(5);
    pair.up.faults.cut = true;
    pair.device
        .publish_provider_state(&provider("imu-board"), &resources("imu-board", 1))
        .unwrap();
    pair.run(50);
    pair.device
        .publish_provider_state(&provider("imu-board"), &resources("imu-board", 2))
        .unwrap();
    pair.run(50);
    pair.up.faults.cut = false;
    assert!(pair.run_until(2_000, |p| !p.device.state_pending()));
    assert_eq!(pair.host_states(), vec![&resources("imu-board", 2)]);
    assert_eq!(
        pair.host.provider_state().map(|s| &s.resources),
        Some(&resources("imu-board", 2))
    );
}

#[test]
fn publish_mid_transmission_aborts_the_old_frame() {
    let mut pair = connected_pair(6);
    pair.device
        .publish_provider_state(&provider("imu-board"), &resources("imu-board", 1))
        .unwrap();
    pair.device.poll(pair.now);
    let mut partial = [0u8; 8];
    assert_eq!(pair.device.transmit(&mut partial), 8);
    pair.host.receive(&partial);
    pair.device
        .publish_provider_state(&provider("imu-board"), &resources("imu-board", 2))
        .unwrap();
    assert_eq!(pair.device.stats().aborted_frames, 1);
    assert!(pair.run_until(1_000, |p| !p.device.state_pending()));
    assert_eq!(pair.host_states(), vec![&resources("imu-board", 2)]);
}

#[test]
fn leases_are_delivered_and_repaired_within_one_heartbeat() {
    let mut pair = connected_pair(7);
    pair.host.set_leases(leases("imu-board", 1));
    assert!(pair.run_until(100, |p| p.device_leases().len() == 2));
    assert_eq!(pair.device_leases()[1], &leases("imu-board", 1));

    pair.down.drop_kinds = vec![kind::LEASES];
    let changed_at = pair.now;
    assert!(pair.host.set_leases(leases("imu-board", 2)));
    assert!(!pair.host.set_leases(leases("imu-board", 2)), "unchanged");
    assert!(
        pair.run_until(u64::from(HEARTBEAT_MS) + 50, |p| p.device_leases().len()
            == 3)
    );
    assert_eq!(pair.device_leases()[2], &leases("imu-board", 2));
    let repaired_at = pair.device_events.last().unwrap().0;
    assert!(repaired_at - changed_at <= u64::from(HEARTBEAT_MS) + 10);

    // The piggybacked copies after every Pong are not re-reported.
    pair.run(5_000);
    assert_eq!(pair.device_leases().len(), 3);
}

#[test]
fn host_loss_is_detected_and_the_device_reconnects() {
    let mut pair = connected_pair(8);
    pair.host.set_leases(leases("imu-board", 1));
    pair.device
        .publish_provider_state(&provider("imu-board"), &resources("imu-board", 1))
        .unwrap();
    assert!(pair.run_until(500, |p| !p.device.state_pending()));

    pair.up.faults.cut = true;
    pair.down.faults.cut = true;
    let cut_at = pair.now;
    assert!(pair.run_until(
        4_000,
        |p| p.count_device(|e| *e == DeviceEvent::Disconnected) == 1
    ));
    let lost_at = pair.now;
    assert!(lost_at - cut_at >= 2 * u64::from(HEARTBEAT_MS));
    assert!(lost_at - cut_at <= 3 * u64::from(HEARTBEAT_MS) + 50);
    assert_eq!(pair.device.link_state(), LinkState::Connecting);
    assert_eq!(
        pair.count_host(|e| matches!(e, HostEvent::DeviceLost { .. })),
        1
    );

    pair.up.faults.cut = false;
    pair.down.faults.cut = false;
    assert!(pair.run_until(6_000, StreamPair::connected));
    // The snapshot is re-sent on the new session without republishing, and leases follow.
    assert!(pair.run_until(1_000, |p| p.host_states().len() == 2));
    assert!(pair.run_until(1_000, |p| p.device_leases().len() >= 2));
    assert_eq!(
        pair.device_leases().last().unwrap(),
        &&leases("imu-board", 1)
    );
    assert_eq!(
        pair.count_device(|e| matches!(e, DeviceEvent::Connected { .. })),
        2
    );
}

#[test]
fn host_restart_makes_the_device_reconnect_quickly() {
    let mut pair = connected_pair(9);
    pair.device
        .publish_provider_state(&provider("imu-board"), &resources("imu-board", 1))
        .unwrap();
    assert!(pair.run_until(500, |p| !p.device.state_pending()));
    let old_session = pair.device.session_id();

    let mut config = host_config();
    config.session_seed = 1_000;
    pair.host = HostSession::stream(config);
    let restarted_at = pair.now;
    // The next ping hits a host without a session, which answers NoSession.
    assert!(pair.run_until(3_000, |p| p.host.provider_state().is_some()));
    assert!(pair.now - restarted_at <= u64::from(HEARTBEAT_MS) + 100);
    assert_ne!(pair.device.session_id(), old_session);
    assert_eq!(
        pair.count_device(|e| matches!(e, DeviceEvent::Rejected(_))),
        0
    );
}

#[test]
fn device_loss_is_detected_by_the_host() {
    let mut pair = connected_pair(10);
    pair.device_frozen = true;
    let frozen_at = pair.now;
    assert!(pair.run_until(5_000, |p| {
        p.count_host(
            |e| matches!(e, HostEvent::DeviceLost { device_name } if device_name == "imu-board"),
        ) == 1
    }));
    assert!(pair.now - frozen_at <= 3 * u64::from(HEARTBEAT_MS) + 50);
    assert!(!pair.host.is_connected());
}

#[test]
fn allowlist_rejects_unknown_devices_with_backoff() {
    let config = host_config().allow_device("other-board");
    let mut pair = StreamPair::new("imu-board", config, 11);
    pair.run(20_000);
    assert!(!pair.host.is_connected());
    let rejects: Vec<u64> = pair
        .device_events
        .iter()
        .filter(|(_, e)| *e == DeviceEvent::Rejected(RejectReason::UnknownDevice))
        .map(|(t, _)| *t)
        .collect();
    // Rejected at once, then after 5 s and 10 s of backoff (5 s, 15 s).
    assert_eq!(rejects.len(), 3, "{rejects:?}");
    assert!(rejects[1] - rejects[0] >= 5_000);
    assert!(rejects[2] - rejects[1] >= 10_000);
    assert!(pair.count_host(|e| matches!(
        e,
        HostEvent::DeviceRejected { device_name: Some(name), reason: RejectReason::UnknownDevice } if name == "imu-board"
    )) >= 3);
    let hellos = pair
        .up
        .sent_kinds
        .iter()
        .filter(|&&k| k == kind::HELLO)
        .count();
    assert_eq!(hellos, 3);
}

#[test]
fn host_rejects_another_protocol_version() {
    let mut host = HostSession::stream(host_config());
    let hello = Message::Hello(orion_link::message::Hello {
        device_name: "future-board".into(),
        roles: orion_link::message::Roles::PROVIDER,
        max_frame: 512,
    });
    let mut frame = vec![0u8; 128];
    let len = hello.encode(1, &mut frame).unwrap();
    frame.truncate(len);
    // Re-encode with a future version byte.
    let header = FrameHeader {
        version: 2,
        kind: kind::HELLO,
        seq: 1,
    };
    let payload = frame[4..len - 4].to_vec();
    let wire: Vec<u8> = StreamEncoder::for_message(header, &payload).collect();
    host.receive(&wire);
    assert!(!host.is_connected());
    let reply = host.transmit().unwrap();
    let mut decoder = orion_link::StreamDecoder::<64>::new();
    let (_, result) = decoder.push_slice(&reply[1..]);
    let frame = result.unwrap().unwrap();
    assert_eq!(frame.kind(), kind::REJECT);
    assert_eq!(
        Message::decode(&frame).unwrap(),
        Message::Reject(RejectReason::VersionMismatch)
    );
    assert!(matches!(
        host.next_event(),
        Some(HostEvent::DeviceRejected {
            reason: RejectReason::VersionMismatch,
            ..
        })
    ));
}

#[test]
fn unknown_kinds_are_ignored_by_both_sides() {
    let mut pair = connected_pair(12);
    let before = pair.host_events.len();
    // A host frame of an unknown kind and one of a reserved kind, with fresh sequence numbers.
    for (kind, seq) in [(0x7E, 40_000u16), (kind::WORKLOADS, 40_001)] {
        let wire: Vec<u8> = StreamEncoder::for_message(FrameHeader::new(kind, seq), b"\x01\x02")
            .with_leading_delimiter()
            .collect();
        pair.device.receive(&wire);
    }
    for (kind, seq) in [(0x7F, 50_000u16), (kind::STATUS, 50_001)] {
        let wire: Vec<u8> = StreamEncoder::for_message(FrameHeader::new(kind, seq), b"\xFF")
            .with_leading_delimiter()
            .collect();
        pair.host.receive(&wire);
    }
    pair.run(3_000);
    assert!(pair.connected());
    assert_eq!(pair.device.stats().decode_errors, 0);
    assert_eq!(pair.host.stats().decode_errors, 0);
    assert_eq!(pair.host_events.len(), before);
    assert!(
        pair.device_events
            .iter()
            .all(|(_, e)| !matches!(e, DeviceEvent::Disconnected | DeviceEvent::Rejected(_)))
    );
}

#[test]
fn converges_over_a_noisy_chunked_stream() {
    let mut corrupt_packets = 0;
    for seed in 0..8u64 {
        let mut pair = StreamPair::new("imu-board", host_config(), 100 + seed);
        pair.up.faults = Faults::lossy(5);
        pair.down.faults = Faults::lossy(5);
        for generation in 1..=6u64 {
            pair.device
                .publish_provider_state(&provider("imu-board"), &resources("imu-board", generation))
                .unwrap();
            pair.host.set_leases(leases("imu-board", generation));
            pair.run(1_500);
        }
        pair.up.faults = Faults::default();
        pair.down.faults = Faults::default();
        assert!(
            pair.run_until(10_000, |p| {
                p.connected()
                    && !p.device.state_pending()
                    && p.host.provider_state().map(|s| &s.resources)
                        == Some(&resources("imu-board", 6))
                    && p.device_leases().last() == Some(&&leases("imu-board", 6))
            }),
            "seed {seed} did not converge"
        );
        let decoder = pair.device.decoder().stats();
        let host_decoder = pair.host.decoder().stats();
        assert!(decoder.frames > 0 && host_decoder.frames > 0);
        corrupt_packets += decoder.dropped() + host_decoder.dropped();
    }
    // Corruption was actually exercised.
    assert!(corrupt_packets > 0);
}
