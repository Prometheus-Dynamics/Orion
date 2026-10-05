//! Host-side check that the C entry points (`ffi-uart`, the default) drive a real `HostSession`.

#![cfg(all(feature = "ffi-uart", not(feature = "ffi-can")))]

use orion_link::host::{HostConfig, HostEvent, HostSession};
use orion_link::message::{LeaseRecord, NodeId};
use orion_mcu_template::ffi::{
    ORION_EVENT_CONNECTED, ORION_EVENT_LEASES, ORION_EVENT_STATE_ACKED, orion_init,
    orion_lease_count, orion_poll, orion_publish, orion_rx, orion_tx,
};

#[test]
fn c_api_connects_publishes_and_counts_leases() {
    let name = b"c-board";
    // SAFETY: the pointers are valid for the given lengths.
    unsafe {
        assert!(!orion_init(b"".as_ptr(), 0), "empty names are refused");
        assert!(orion_init(name.as_ptr(), name.len()));
        let (id, kind) = (b"c-board.adc-0", b"adc.sample_source");
        assert_eq!(
            orion_publish(id.as_ptr(), id.len(), kind.as_ptr(), kind.len(), true),
            0
        );
        assert_eq!(
            orion_publish([0xFF].as_ptr(), 1, kind.as_ptr(), kind.len(), true),
            -2
        );
    }
    let mut host = HostSession::stream(HostConfig::new(NodeId::new("node-a")));
    host.set_leases(vec![
        LeaseRecord::builder("c-board.adc-0")
            .holder_node(NodeId::new("node-a"))
            .build(),
    ]);
    let mut bits = 0;
    let mut published = None;
    let mut out = [0u8; 16];
    for now in 0..500u64 {
        bits |= orion_poll(now);
        loop {
            // SAFETY: `out` is valid for its length.
            let n = unsafe { orion_tx(out.as_mut_ptr(), out.len()) };
            if n == 0 {
                break;
            }
            host.receive(&out[..n]);
        }
        host.poll(now);
        while let Some(bytes) = host.transmit() {
            // SAFETY: `bytes` is valid for its length.
            unsafe { orion_rx(bytes.as_ptr(), bytes.len()) };
        }
        while let Some(event) = host.next_event() {
            if let HostEvent::ProviderState {
                provider,
                resources,
                ..
            } = event
            {
                published = Some((provider, resources));
            }
        }
    }
    let wanted = ORION_EVENT_CONNECTED | ORION_EVENT_LEASES | ORION_EVENT_STATE_ACKED;
    assert_eq!(bits & wanted, wanted);
    assert_eq!(orion_lease_count(), 1);
    let (provider, resources) = published.expect("a snapshot");
    assert_eq!(provider.provider_id.as_str(), "provider.c-board");
    assert_eq!(resources.len(), 1);
    assert_eq!(resources[0].resource_id.as_str(), "c-board.adc-0");
    assert_eq!(resources[0].provider_id.as_str(), "provider.c-board");
    assert_eq!(host.device_name(), Some("c-board"));
}
