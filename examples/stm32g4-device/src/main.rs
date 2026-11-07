//! An STM32G4 device on the Orion link: one record and one stream, over FDCAN (default) or UART
//! (feature `link-uart`).
//!
//! - **Record:** the provider `provider.g4-demo` with one resource, `g4-demo.mcu`.
//! - **Stream:** `g4-demo.loop`, one `u32` series: main-loop iterations per 10 ms, a direct
//!   reading of how much CPU the link leaves. It is sampled only while someone subscribes.
//! - **Status:** `stack.high_water_bytes`, the deepest the stack has grown (measured by painting,
//!   see `stack.rs`), and `stack.size_bytes`, what it may use.
//!
//! No HAL or peripheral access crate: the registers used are listed in `reg.rs`, the FDCAN and
//! USART drivers are polled, and the clock is the 16 MHz HSI the chip starts on. This keeps the
//! size numbers in `scripts/mcu-size.sh` about the Orion device tier, not a HAL.

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
use orion_link::device::{DeviceConfig, DeviceSession};
use orion_link::wire::{
    HistoryView, ProviderView, ResourceView, SampleWriter, Sampling, SeriesKind, SeriesView,
    StatusView, Value,
};

/// Receive and transmit frame buffers: the largest frame each way (the lease set and the
/// provider snapshot must fit).
const RX: usize = 128;
const TX: usize = 128;

const PROVIDER: ProviderView<'static> =
    ProviderView::new("provider.g4-demo", "unassigned").with_resource_types(&["mcu"]);
const RESOURCES: [ResourceView<'static>; 1] =
    [ResourceView::new("g4-demo.mcu", "mcu", "provider.g4-demo")];

const STREAM: &str = "g4-demo.loop";
const SAMPLE_MS: u64 = 10;
/// Samples per batch: a batch of 8 `u32` samples is about 50 bytes, one CAN FD frame.
const BATCH: u32 = 8;
const SERIES: [SeriesView<'static>; 1] = [SeriesView {
    name: "iterations",
    unit: None,
    kind: SeriesKind::U32,
    period_us: SAMPLE_MS as u32 * 1000,
    history: HistoryView {
        full_ms: 10_000,
        reduced_ms: 0,
        reduced_period_ms: 0,
    },
    sampling: Sampling::WhileSubscribed,
}];
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

/// Extends the 32-bit SysTick count to the session's 64-bit milliseconds.
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

    fn now_ms(&mut self) -> u64 {
        let now = MILLIS.load(Ordering::Relaxed);
        if now < self.last {
            self.high += 1 << 32;
        }
        self.last = now;
        self.high | u64::from(now)
    }
}

#[cfg(feature = "link-can")]
mod link {
    use crate::fdcan::{Fdcan, Received};
    use orion_link::{CanLinkIds, Packet};

    pub type Transport = Packet;
    pub const TRANSPORT: Packet = Packet::FD;
    /// The link identifiers for device address 1 (base 0x100 device-to-host, 0x180 back).
    const IDS: CanLinkIds = match CanLinkIds::for_address(CanLinkIds::new(0x100, 0x180, false), 1) {
        Some(ids) => ids,
        None => panic!("link ids"),
    };

    pub struct Link {
        can: Fdcan,
        /// One frame's data, for receiving and transmitting in turn.
        buf: Received,
    }

    impl Link {
        pub fn new() -> Self {
            Self {
                can: Fdcan::new(IDS),
                buf: Received {
                    data: [0; 64],
                    len: 0,
                },
            }
        }

        pub fn service(&mut self, session: &mut super::Session, now_ms: u64) {
            self.can.recover();
            while self.can.receive(&mut self.buf) {
                session.receive_segment(&self.buf.data[..self.buf.len]);
            }
            session.poll(now_ms);
            // A segment is taken only once the controller has room for it, so it is written
            // straight into the shared buffer (no `Segment` copies) and never put back.
            while self.can.can_transmit() {
                let len = session.transmit_segment(&mut self.buf.data);
                if len == 0 {
                    break;
                }
                self.can.transmit(&self.buf.data[..len]);
            }
        }
    }
}

#[cfg(feature = "link-uart")]
mod link {
    use crate::usart::Usart;
    use orion_link::Stream;

    pub type Transport = Stream;
    pub const TRANSPORT: Stream = Stream;

    pub struct Link {
        uart: Usart,
        tx: [u8; 16],
        tx_len: usize,
        tx_at: usize,
    }

    impl Link {
        pub fn new() -> Self {
            Self {
                uart: Usart::new(),
                tx: [0; 16],
                tx_len: 0,
                tx_at: 0,
            }
        }

        pub fn service(&mut self, session: &mut super::Session, now_ms: u64) {
            while let Some(byte) = self.uart.read() {
                session.receive(&[byte]);
            }
            session.poll(now_ms);
            // Never waits on the UART: bytes the register cannot take yet go on the next turn.
            while self.uart.can_write() {
                if self.tx_at == self.tx_len {
                    self.tx_len = session.transmit(&mut self.tx);
                    self.tx_at = 0;
                    if self.tx_len == 0 {
                        break;
                    }
                }
                self.uart.write(self.tx[self.tx_at]);
                self.tx_at += 1;
            }
        }
    }
}

type Session = DeviceSession<link::Transport, RX, TX>;

#[entry]
fn main() -> ! {
    stack::paint();
    #[cfg(feature = "hw-crc")]
    crc::init();
    let mut clock = Clock::start();
    let mut link = link::Link::new();
    let mut session = Session::new(DeviceConfig::provider("g4-demo"), link::TRANSPORT);
    // Both fit their buffers by construction; a failure here is a build-time mistake.
    let _ = session.publish_provider_state(&PROVIDER, &RESOURCES);
    let _ = session.declare_stream(STREAM, &SERIES);

    let mut samples = [0u8; 64];
    let mut writer = SampleWriter::new(&mut samples);
    let mut in_batch = 0;
    let mut iterations: u32 = 0;
    let mut next_sample = 0;
    let mut next_status = 0;
    loop {
        let now = clock.now_ms();
        link.service(&mut session, now);
        // Drained so the queue never fills; this device acts on none of the events.
        while session.next_event().is_some() {}
        iterations = iterations.wrapping_add(1);

        if now >= next_sample {
            next_sample = now + SAMPLE_MS;
            let wanted = session.stream_accepted()
                && session
                    .stream_demand()
                    .is_some_and(|demand| demand.wants(0));
            if wanted && writer.push_u32(now * 1000, iterations) {
                in_batch += 1;
            }
            if in_batch >= BATCH {
                // The newest batch wins: one that cannot be queued yet is dropped.
                if !session.stream_pending() {
                    let _ = session.publish_stream(&[writer.chunk(0)]);
                }
                writer.clear();
                in_batch = 0;
            }
            iterations = 0;
        }
        if now >= next_status {
            next_status = now + STATUS_MS;
            let _ = session.publish_status(&[
                StatusView::new(
                    "stack.high_water_bytes",
                    Value::UInt(u64::from(stack::high_water())),
                )
                .with_ttl_ms(3 * STATUS_MS as u32),
                StatusView::new("stack.size_bytes", Value::UInt(u64::from(stack::size())))
                    .with_ttl_ms(3 * STATUS_MS as u32),
            ]);
        }
    }
}

#[panic_handler]
fn panic(_: &core::panic::PanicInfo<'_>) -> ! {
    // A real board resets here (SCB AIRCR SYSRESETREQ) so it comes back on the link.
    reg::write(0xE000_ED0C, (0x05FA << 16) | (1 << 2));
    loop {}
}
