//! An STM32G4 device on Orion's wire: two topics over FDCAN (default) or a UART (feature
//! `link-uart`).
//!
//! - **`loop`**, field `iterations`: main-loop turns per 100 ms, a direct reading of how much
//!   CPU the link leaves. It is published only while someone listens.
//! - **`stack`**, fields `high_water` and `size`: the deepest the stack has grown (measured by
//!   painting, see `stack.rs`) and what it may use, once a second.
//!
//! No HAL or peripheral access crate: the registers used are listed in `reg.rs`, the FDCAN and
//! USART drivers are polled, and the clock is the 16 MHz HSI the chip starts on. This keeps the
//! size numbers in `scripts/device-size.sh` about Orion's device crates, not a HAL.

#![no_std]
#![no_main]

#[cfg(all(feature = "link-can", feature = "link-uart"))]
compile_error!("choose one link: feature `link-can` or `link-uart`");

#[cfg(feature = "hw-crc")]
mod crc;
#[cfg(feature = "small-mem")]
mod mem;
mod reg;
mod stack;

#[cfg(feature = "link-can")]
mod fdcan;
#[cfg(feature = "link-uart")]
mod usart;

use core::sync::atomic::{AtomicU32, Ordering};

use cortex_m_rt::{entry, exception};
use orion_device_core::{DeviceConfig, Link};
use orion_device_topics::{Field, FieldType, TopicSpec, Topics};

/// Receive and transmit frame buffers: the largest frame each way.
const RX: usize = 128;
const TX: usize = 128;
const NAME: &str = "g4-demo";

const LOOP: usize = 0;
const STACK: usize = 1;
static TOPICS: [TopicSpec; 2] = [
    TopicSpec::latest("loop", &[Field::new("iterations", FieldType::Int, "")]),
    TopicSpec::latest(
        "stack",
        &[
            Field::new("high_water", FieldType::Int, "B"),
            Field::new("size", FieldType::Int, "B"),
        ],
    ),
];
const SAMPLE_MS: u64 = 100;
const STATUS_MS: u64 = 1000;

/// The device interrupt vectors. The firmware is polled and enables no interrupt, so one empty
/// entry (the minimum `cortex-m-rt` accepts) replaces the 240 it fills by default: 956 bytes of
/// flash. A port that enables an interrupt lists the vectors up to it here.
#[unsafe(link_section = ".vector_table.interrupts")]
#[unsafe(no_mangle)]
static __INTERRUPTS: [Option<unsafe extern "C" fn()>; 1] = [None];

static MILLIS: AtomicU32 = AtomicU32::new(0);

#[exception]
fn SysTick() {
    MILLIS.store(
        MILLIS.load(Ordering::Relaxed).wrapping_add(1),
        Ordering::Relaxed,
    );
}

/// Extends the 32-bit SysTick count to 64-bit milliseconds, for sample times.
struct Clock {
    last: u32,
    high: u64,
}

impl Clock {
    fn start() -> Self {
        reg::write(reg::systick::RVR, 16_000 - 1); // 1 ms at 16 MHz
        reg::write(reg::systick::CVR, 0);
        reg::write(reg::systick::CSR, 0b111); // processor clock, interrupt, enable
        Self { last: 0, high: 0 }
    }

    /// The session's wrapping counter and the extended one.
    fn now_ms(&mut self) -> (u32, u64) {
        let now = MILLIS.load(Ordering::Relaxed);
        if now < self.last {
            self.high += 1 << 32;
        }
        self.last = now;
        (now, self.high | u64::from(now))
    }
}

/// What the firmware does with the session: declares the topics and reads what the host
/// says about them.
struct App {
    topics: Topics<2>,
}

impl App {
    fn handle(&mut self, frame: &orion_device_core::FrameView<'_>) {
        // Who listens is kept by `Topics`; nothing else here needs an answer.
        let _ = self.topics.handle(frame);
    }
}

#[cfg(feature = "link-can")]
mod link {
    use crate::fdcan::{Fdcan, Ids, Received};
    use orion_device_core::{CanDevice, DeviceConfig, Link, SegmentMtu};

    /// Address 1 on a bus whose base identifiers are 0x100 (device to host) and 0x180 (host to
    /// device): the comm core's `can0=can0:0x100:0x180:1:fd`.
    const IDS: Ids = Ids {
        device_to_host: 0x101,
        host_to_device: 0x181,
        extended: false,
    };

    pub struct Port {
        can: Fdcan,
        device: CanDevice<{ crate::RX }, { crate::TX }>,
        /// One frame's data, for receiving and transmitting in turn.
        buf: Received,
    }

    impl Port {
        pub fn new(config: DeviceConfig) -> Self {
            Self {
                can: Fdcan::new(IDS),
                device: CanDevice::new(config, SegmentMtu::FD),
                buf: Received {
                    data: [0; 64],
                    len: 0,
                },
            }
        }

        pub fn link(&mut self) -> &mut Link<{ crate::TX }> {
            &mut self.device.link
        }

        pub fn service(&mut self, app: &mut crate::App, now_ms: u32) {
            self.can.recover();
            while self.can.receive(&mut self.buf) {
                let data = self.buf.data.get(..self.buf.len).unwrap_or_default();
                self.device
                    .receive_segment(data, |_, frame| app.handle(frame));
            }
            self.device.link.poll(now_ms);
            app.topics.poll(&mut self.device.link, now_ms);
            // A segment is taken only once the controller has room for it, so it is written
            // straight into the shared buffer and never put back.
            while self.can.can_transmit() {
                let Some(len) = self.device.next_segment(&mut self.buf.data) else {
                    break;
                };
                self.can
                    .transmit(self.buf.data.get(..len).unwrap_or_default());
            }
        }
    }
}

#[cfg(feature = "link-uart")]
mod link {
    use crate::usart::Usart;
    use orion_device_core::{DeviceConfig, Link, StreamDevice};

    pub struct Port {
        uart: Usart,
        device: StreamDevice<{ crate::RX }, { crate::TX }>,
        tx: [u8; 16],
        tx_len: usize,
        tx_at: usize,
    }

    impl Port {
        pub fn new(config: DeviceConfig) -> Self {
            Self {
                uart: Usart::new(),
                device: StreamDevice::new(config),
                tx: [0; 16],
                tx_len: 0,
                tx_at: 0,
            }
        }

        pub fn link(&mut self) -> &mut Link<{ crate::TX }> {
            &mut self.device.link
        }

        pub fn service(&mut self, app: &mut crate::App, now_ms: u32) {
            while let Some(byte) = self.uart.read() {
                self.device.receive(&[byte], |_, frame| app.handle(frame));
            }
            self.device.link.poll(now_ms);
            app.topics.poll(&mut self.device.link, now_ms);
            // Never waits on the UART: bytes the register cannot take yet go on the next turn.
            while self.uart.can_write() {
                if self.tx_at == self.tx_len {
                    self.tx_len = self.device.transmit(&mut self.tx);
                    self.tx_at = 0;
                    if self.tx_len == 0 {
                        break;
                    }
                }
                if let Some(byte) = self.tx.get(self.tx_at) {
                    self.uart.write(*byte);
                }
                self.tx_at += 1;
            }
        }
    }
}

/// Publishes the stack's numbers. One that the link has no room for waits for the next second.
fn publish_stack(app: &mut App, link: &mut Link<TX>, time_ns: u64) {
    let _ = app
        .topics
        .publish_i64(link, STACK, 0, time_ns, i64::from(stack::high_water()));
    let _ = app
        .topics
        .publish_i64(link, STACK, 1, time_ns, i64::from(stack::size()));
}

#[entry]
fn main() -> ! {
    stack::paint();
    #[cfg(feature = "hw-crc")]
    crc::init();
    let mut clock = Clock::start();
    let mut port = link::Port::new(DeviceConfig::new(NAME));
    let mut app = App {
        topics: Topics::new(&TOPICS),
    };

    let mut iterations: u32 = 0;
    let mut next_sample = 0;
    let mut next_status = 0;
    loop {
        let (now, millis) = clock.now_ms();
        port.service(&mut app, now);
        // Drained so the queue never fills; a new session is `Topics::poll`'s to see.
        while port.link().next_event().is_some() {}
        iterations = iterations.wrapping_add(1);

        let time_ns = millis * 1_000_000;
        if millis >= next_sample {
            next_sample = millis + SAMPLE_MS;
            if app.topics.subscribers(LOOP) > 0 {
                let value = i64::from(iterations);
                let _ = app.topics.publish_i64(port.link(), LOOP, 0, time_ns, value);
            }
            iterations = 0;
        }
        if millis >= next_status {
            next_status = millis + STATUS_MS;
            publish_stack(&mut app, port.link(), time_ns);
        }
    }
}

#[panic_handler]
fn panic(_: &core::panic::PanicInfo<'_>) -> ! {
    // Resets (SCB AIRCR SYSRESETREQ) so the board comes back on the link.
    reg::write(0xE000_ED0C, (0x05FA << 16) | (1 << 2));
    loop {}
}
