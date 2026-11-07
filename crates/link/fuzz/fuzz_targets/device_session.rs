//! Any byte stream into a device session: no panic.
#![no_main]

use libfuzzer_sys::fuzz_target;
use orion_link::Stream;
use orion_link::device::{DeviceConfig, StreamDevice};

fuzz_target!(|data: &[u8]| {
    let mut device = StreamDevice::<512, 512>::new(DeviceConfig::provider("imu-board"), Stream);
    let mut chunk = [0u8; 512];
    let Some((&step, bytes)) = data.split_first() else {
        return;
    };
    let mut now = 0u64;
    for piece in bytes.chunks(usize::from(step).max(1)) {
        device.receive(piece);
        now += 7;
        device.poll(now);
        while device.next_event().is_some() {}
        while device.transmit(&mut chunk) > 0 {}
    }
});
