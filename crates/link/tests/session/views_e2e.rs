//! The minimal device path end to end: a device publishing borrowed `wire` views (no records, no
//! allocator on the device side) against the real `HostSession` / `HostBus`, over a noisy stream
//! and over classic CAN and CAN FD. The host must see exactly the records the views stand for.

use orion_link::host::HostEvent;
use orion_link::message::{StatusEntry, TypedConfigValue};
use orion_link::wire::{LeaseState, ProviderView, ResourceView, StatusView, Value};
use orion_link::{Packet, SegmentMtu};

use crate::sim::{CanBus, Faults, StreamPair, host_config, leases, resources};

/// `sim::provider(name)` as a view.
fn provider_view(provider_id: &str) -> ProviderView<'_> {
    ProviderView::new(provider_id, "node-a").with_resource_types(&["imu.sample_source"])
}

/// `sim::resources(name, generation)` as views.
fn resource_views<'a>(resource_id: &'a str, provider_id: &'a str) -> [ResourceView<'a>; 1] {
    [ResourceView::new(resource_id, "imu.sample_source", provider_id).with_labels(&["imu"])]
}

#[test]
fn stream_device_publishing_views_is_seen_as_records() {
    for seed in 0..4u64 {
        let mut pair = StreamPair::new("imu-board", host_config(), 300 + seed);
        pair.up.faults = Faults::lossy(5);
        pair.down.faults = Faults::lossy(5);
        for generation in 1..=3u64 {
            let resource_id = format!("imu-board.imu-{generation}");
            pair.device
                .publish_provider_state(
                    &provider_view("provider.imu-board"),
                    &resource_views(&resource_id, "provider.imu-board"),
                )
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
                        == Some(&resources("imu-board", 3))
                    && p.device
                        .leases()
                        .map(|l| l.resource_id)
                        .eq(["imu-board.imu-3"])
            }),
            "seed {seed} did not converge"
        );
        let lease = pair.device.leases().next().unwrap();
        assert_eq!(lease.lease_state, LeaseState::Unleased); // `sim::leases` sets only a holder
        assert_eq!(lease.holder_node_id, Some("node-a"));
        assert_eq!(pair.device.lease_records(), leases("imu-board", 3));
    }
}

#[test]
fn status_views_arrive_as_status_entries() {
    let mut pair = StreamPair::new("imu-board", host_config(), 310);
    assert!(pair.run_until(2_000, StreamPair::connected));
    let views = [
        StatusView::new("temperature_mc", Value::Int(-1_250)).with_ttl_ms(2_000),
        StatusView::new("mode", Value::String("streaming")),
        StatusView::new("raw", Value::Bytes(&[0, 1, 255])),
    ];
    pair.device.publish_status(&views).unwrap();
    pair.run(100);
    let batches: Vec<&Vec<StatusEntry>> = pair
        .host_events
        .iter()
        .filter_map(|(_, e)| match e {
            HostEvent::Status { entries, .. } => Some(entries),
            _ => None,
        })
        .collect();
    assert_eq!(
        batches,
        vec![&vec![
            StatusEntry::new("temperature_mc", TypedConfigValue::Int(-1_250)).with_ttl_ms(2_000),
            StatusEntry::new("mode", TypedConfigValue::String("streaming".into())),
            StatusEntry::new("raw", TypedConfigValue::Bytes(vec![0, 1, 255])),
        ]]
    );
}

fn can_exercise(transport: Packet, seed: u64) {
    let mut bus = CanBus::new(host_config(), transport, &[5, 9], seed);
    for address in [5u32, 9] {
        let provider_id = format!("provider.dev-{address}");
        let resource_id = format!("dev-{address}.imu-1");
        bus.node(address)
            .device
            .publish_provider_state(
                &provider_view(&provider_id),
                &resource_views(&resource_id, &provider_id),
            )
            .unwrap();
        bus.host
            .set_leases(address, leases(&format!("dev-{address}"), 1));
    }
    for node in &mut bus.nodes {
        node.up.faults = Faults::lossy(3);
        node.down.faults = Faults::lossy(3);
    }
    bus.run(3_000);
    for node in &mut bus.nodes {
        node.up.faults = Faults::default();
        node.down.faults = Faults::default();
    }
    assert!(bus.run_until(10_000, |b| {
        [5u32, 9].iter().all(|&a| {
            let state = b.host.session(a).and_then(|s| s.provider_state());
            let node = b.nodes.iter().find(|n| n.address == a).unwrap();
            state.map(|s| &s.resources) == Some(&resources(&format!("dev-{a}"), 1))
                && !node.device.state_pending()
                && node.device.leases().len() == 1
        })
    }));
}

#[test]
fn can_devices_publishing_views_are_seen_as_records() {
    can_exercise(Packet::CLASSIC, 320);
    can_exercise(Packet::FD, 321);
    can_exercise(Packet::new(SegmentMtu::new(12).unwrap()), 322);
}
