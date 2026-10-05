//! Device ↔ host over simulated classic CAN and CAN FD buses.

use orion_link::device::DeviceEvent;
use orion_link::host::HostEvent;
use orion_link::message::kind;
use orion_link::{Packet, SegmentMtu};

use crate::sim::{CanBus, Faults, HEARTBEAT_MS, host_config, leases, provider, resources};

fn host_state(bus: &CanBus, address: u32) -> Option<Vec<orion_link::message::ResourceRecord>> {
    bus.host
        .session(address)
        .and_then(|s| s.provider_state())
        .map(|s| s.resources.clone())
}

fn last_leases(bus: &CanBus, address: u32) -> Option<Vec<orion_link::message::LeaseRecord>> {
    let node = bus.nodes.iter().find(|n| n.address == address)?;
    node.lease_sets.last().cloned()
}

fn publish(bus: &mut CanBus, address: u32, generation: u64) {
    let name = format!("dev-{address}");
    bus.node(address)
        .device
        .publish_provider_state(&provider(&name), &resources(&name, generation))
        .unwrap();
}

fn exercise(transport: Packet, seed: u64) {
    let mut bus = CanBus::new(host_config(), transport, &[5], seed);
    publish(&mut bus, 5, 1);
    assert!(bus.run_until(2_000, |b| host_state(b, 5) == Some(resources("dev-5", 1))));
    assert!(bus.nodes[0].device.is_connected());
    assert!(bus.run_until(500, |b| !b.nodes[0].device.state_pending()));

    // Lost state segments are repaired by retransmission.
    bus.node(5).up.drop_kinds = vec![kind::PROVIDER_STATE];
    publish(&mut bus, 5, 2);
    assert!(bus.run_until(2_000, |b| host_state(b, 5) == Some(resources("dev-5", 2))));
    assert!(bus.nodes[0].device.stats().state_retransmits >= 1);

    // Lost lease segments are repaired within one heartbeat.
    bus.node(5).down.drop_kinds = vec![kind::LEASES];
    bus.host.set_leases(5, leases("dev-5", 1));
    assert!(bus.run_until(u64::from(HEARTBEAT_MS) + 50, |b| {
        last_leases(b, 5) == Some(leases("dev-5", 1))
    }));

    // Random segment loss, corruption, and CAN-level duplicates.
    bus.node(5).up.faults = Faults::lossy(3);
    bus.node(5).down.faults = Faults::lossy(3);
    for generation in 3..=8 {
        publish(&mut bus, 5, generation);
        bus.host.set_leases(5, leases("dev-5", generation));
        bus.run(1_200);
    }
    bus.node(5).up.faults = Faults::default();
    bus.node(5).down.faults = Faults::default();
    assert!(
        bus.run_until(10_000, |b| {
            host_state(b, 5) == Some(resources("dev-5", 8))
                && last_leases(b, 5) == Some(leases("dev-5", 8))
                && !b.nodes[0].device.state_pending()
        }),
        "did not converge"
    );
}

#[test]
fn classic_can_handshake_state_leases_and_loss() {
    for seed in 0..4 {
        exercise(Packet::CLASSIC, seed);
    }
}

#[test]
fn can_fd_handshake_state_leases_and_loss() {
    for seed in 0..4 {
        exercise(Packet::FD, 10 + seed);
    }
}

#[test]
fn can_fd_with_a_reduced_mtu() {
    exercise(Packet::new(SegmentMtu::new(32).unwrap()), 20);
}

#[test]
fn duplicated_segments_are_harmless() {
    let mut bus = CanBus::new(host_config(), Packet::CLASSIC, &[9], 30);
    bus.node(9).up.faults.dup_pct = 50;
    bus.node(9).down.faults.dup_pct = 50;
    publish(&mut bus, 9, 1);
    bus.host.set_leases(9, leases("dev-9", 1));
    assert!(bus.run_until(3_000, |b| {
        host_state(b, 9) == Some(resources("dev-9", 1))
            && last_leases(b, 9) == Some(leases("dev-9", 1))
    }));
    bus.run(3_000);
    let node = &bus.nodes[0];
    assert_eq!(
        node.events
            .iter()
            .filter(|(_, e)| matches!(e, DeviceEvent::Connected { .. }))
            .count(),
        1
    );
    let states = bus
        .host_events
        .iter()
        .filter(|(_, _, e)| matches!(e, HostEvent::ProviderState { .. }))
        .count();
    assert_eq!(states, 1);
}

#[test]
fn several_devices_share_one_bus() {
    let addresses = [1, 2, 3, 17];
    let mut bus = CanBus::new(host_config(), Packet::CLASSIC, &addresses, 40);
    for &address in &addresses {
        publish(&mut bus, address, u64::from(address));
    }
    assert!(bus.run_until(3_000, |b| {
        addresses
            .iter()
            .all(|&a| host_state(b, a) == Some(resources(&format!("dev-{a}"), u64::from(a))))
    }));
    for &address in &addresses {
        assert_eq!(
            bus.host.address_of(&format!("dev-{address}")),
            Some(address)
        );
        bus.host.set_leases(
            address,
            leases(&format!("dev-{address}"), u64::from(address)),
        );
    }
    assert!(bus.run_until(1_000, |b| {
        addresses
            .iter()
            .all(|&a| last_leases(b, a) == Some(leases(&format!("dev-{a}"), u64::from(a))))
    }));
    // Every device only ever saw its own leases.
    for node in &bus.nodes {
        for set in &node.lease_sets {
            assert!(
                set.is_empty()
                    || *set == leases(&format!("dev-{}", node.address), u64::from(node.address))
            );
        }
    }

    // One device dies; only it is reported lost.
    bus.node(2).frozen = true;
    assert!(bus.run_until(4_000, |b| {
        b.host_events
            .iter()
            .any(|(_, a, e)| *a == 2 && matches!(e, HostEvent::DeviceLost { .. }))
    }));
    bus.run(3_000);
    let lost: Vec<u32> = bus
        .host_events
        .iter()
        .filter(|(_, _, e)| matches!(e, HostEvent::DeviceLost { .. }))
        .map(|(_, a, _)| *a)
        .collect();
    assert_eq!(lost, vec![2]);
    for &address in &[1, 3, 17] {
        assert!(bus.host.session(address).unwrap().is_connected());
    }
}

#[test]
fn bus_ignores_foreign_identifiers() {
    let mut bus = CanBus::new(host_config(), Packet::CLASSIC, &[1], 50);
    assert!(
        !bus.host.receive(0x181, false, &[0xC0, 1, 2]),
        "host->device id"
    );
    assert!(
        !bus.host.receive(0x101, true, &[0xC0, 1, 2]),
        "wrong id type"
    );
    assert!(
        !bus.host.receive(0x100, false, &[0xC0, 1, 2]),
        "address 0 outside range"
    );
    assert!(
        bus.host.receive(0x101, false, &[0xC0, 1, 2]),
        "device 1 (garbage is dropped)"
    );
    bus.run(100);
    assert!(bus.nodes[0].device.is_connected());
}
