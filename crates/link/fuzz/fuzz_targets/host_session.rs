//! Any byte stream into a host session (COBS framing, every role): no panic, and output stays
//! bounded by the session's queue whatever arrives.
#![no_main]

use libfuzzer_sys::fuzz_target;
use orion_link::host::{HostConfig, HostSession};
use orion_link::message::NodeId;

fuzz_target!(|data: &[u8]| {
    let mut config = HostConfig::new(NodeId::new("node-a"));
    config.serve_clients = true;
    config.serve_streams = true;
    let mut host = HostSession::stream(config);
    let mut now = 0u64;
    let mut out = Vec::with_capacity(4096);
    // The first byte picks the chunk size, so frame boundaries land anywhere.
    let Some((&step, bytes)) = data.split_first() else {
        return;
    };
    for chunk in bytes.chunks(usize::from(step).max(1)) {
        host.receive(chunk);
        now += 7;
        host.poll(now);
        while host.next_event().is_some() {}
        let mut frames = 0;
        while host.transmit_into(&mut out) {
            frames += 1;
            assert!(frames <= 1024, "unbounded output");
        }
    }
});
