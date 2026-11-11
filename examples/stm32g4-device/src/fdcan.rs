//! FDCAN1 as an Orion link: CAN FD with bit-rate switching, one acceptance filter for the link's
//! host-to-device identifier, receive FIFO 0 and the transmit FIFO. Polled, no interrupts.
//!
//! The STM32G4's message RAM layout is fixed (RM0440, "FDCAN message RAM"), so the driver only
//! sets the filter counts and computes element addresses.

use crate::reg::{self, gpioa, rcc};

const BASE: u32 = 0x4000_6400;
const CCCR: u32 = BASE + 0x18;
const NBTP: u32 = BASE + 0x1C;
const DBTP: u32 = BASE + 0x0C;
const TDCR: u32 = BASE + 0x48;
const RXGFC: u32 = BASE + 0x80;
const RXF0S: u32 = BASE + 0x90;
const RXF0A: u32 = BASE + 0x94;
const TXFQS: u32 = BASE + 0xC4;
const TXBAR: u32 = BASE + 0xCC;

const CCCR_INIT: u32 = 1 << 0;
const CCCR_CCE: u32 = 1 << 1;
const CCCR_FDOE: u32 = 1 << 8;
const CCCR_BRSE: u32 = 1 << 9;
const DBTP_TDC: u32 = 1 << 23;

/// FDCAN1's message RAM (SRAMCAN), in words.
const RAM: u32 = 0x4000_A400;
const STD_FILTERS: u32 = RAM;
const EXT_FILTERS: u32 = RAM + 0x070;
const RX_FIFO0: u32 = RAM + 0x0B0;
const TX_BUFFERS: u32 = RAM + 0x278;
/// One receive or transmit element: two header words and 64 data bytes.
const ELEMENT: u32 = 18 * 4;

/// The largest extended (29-bit) identifier.
const MAX_EXTENDED_ID: u32 = 0x1FFF_FFFF;

/// A device's pair of identifiers on the bus: the bus's base identifiers plus its address
/// (`crates/comm/link-can`).
#[derive(Clone, Copy)]
pub struct Ids {
    /// What the device sends on.
    pub device_to_host: u32,
    /// What the device is sent on.
    pub host_to_device: u32,
    /// 29-bit identifiers instead of 11-bit ones.
    pub extended: bool,
}

/// The link's FDCAN1 driver.
pub struct Fdcan {
    ids: Ids,
}

/// A received frame's data, copied out of message RAM.
pub struct Received {
    pub data: [u8; 64],
    pub len: usize,
}

impl Fdcan {
    /// Clocks FDCAN1 from PCLK1 (16 MHz HSI after reset), routes PA11/PA12, and sets 500 kbit/s
    /// nominal and 2 Mbit/s data rate with one filter for `ids.host_to_device`.
    pub fn new(ids: Ids) -> Self {
        reg::set(rcc::AHB2ENR, 1 << 0); // GPIOA
        reg::set(rcc::APB1ENR1, 1 << 25); // FDCAN
        reg::field(rcc::CCIPR, 24, 0b11, 0b10); // FDCAN kernel clock = PCLK1
        gpioa::alternate(11, 9);
        gpioa::alternate(12, 9);

        reg::set(CCCR, CCCR_INIT);
        while reg::read(CCCR) & CCCR_INIT == 0 {}
        reg::set(CCCR, CCCR_CCE);
        reg::set(CCCR, CCCR_FDOE | CCCR_BRSE);
        // 16 MHz / 500 kbit/s = 32 time quanta: 1 + 26 + 5, sample point 84%.
        reg::write(NBTP, (4 << 25) | (25 << 8) | 4);
        // 16 MHz / 2 Mbit/s = 8 time quanta: 1 + 5 + 2, with transmitter delay compensation.
        reg::write(DBTP, DBTP_TDC | (4 << 8) | (1 << 4) | 1);
        reg::write(TDCR, 6 << 8);

        // One filter, matching exactly; reject everything else and all remote frames.
        if ids.extended {
            reg::write(EXT_FILTERS, (0b001 << 29) | ids.host_to_device);
            reg::write(EXT_FILTERS + 4, (0b10 << 30) | MAX_EXTENDED_ID);
            reg::write(RXGFC, (1 << 24) | (0b10 << 4) | (0b10 << 2) | 0b11);
        } else {
            reg::write(
                STD_FILTERS,
                (0b10 << 30) | (0b001 << 27) | (ids.host_to_device << 16) | 0x7FF,
            );
            reg::write(RXGFC, (1 << 16) | (0b10 << 4) | (0b10 << 2) | 0b11);
        }
        reg::write(CCCR, reg::read(CCCR) & !(CCCR_INIT | CCCR_CCE));
        Self { ids }
    }

    /// Leaves bus-off by itself: the controller sets INIT on bus-off, and clearing it starts the
    /// standard recovery (128 × 11 recessive bits). The link is never switched off.
    pub fn recover(&mut self) {
        let cccr = reg::read(CCCR);
        if cccr & CCCR_INIT != 0 {
            reg::write(CCCR, cccr & !(CCCR_INIT | CCCR_CCE));
        }
    }

    /// The oldest frame in receive FIFO 0, if any. The filter only admits the link's identifier.
    pub fn receive(&mut self, out: &mut Received) -> bool {
        let status = reg::read(RXF0S);
        if status & 0xF == 0 {
            return false;
        }
        let index = (status >> 8) & 0x3;
        let element = RX_FIFO0 + index * ELEMENT;
        let len = dlc_to_len((reg::read(element + 4) >> 16) & 0xF);
        for word in 0..len.div_ceil(4) {
            let bytes = reg::read(element + 8 + word as u32 * 4).to_le_bytes();
            for (to, from) in out.data[word * 4..].iter_mut().zip(bytes) {
                *to = from;
            }
        }
        out.len = len;
        reg::write(RXF0A, index);
        true
    }

    /// Whether the transmit FIFO has a free element.
    pub fn can_transmit(&self) -> bool {
        reg::read(TXFQS) & (1 << 21) == 0
    }

    /// Queues `data` (a valid CAN FD length) on the link's device-to-host identifier, with bit
    /// rate switching. Call only when [`Fdcan::can_transmit`].
    pub fn transmit(&mut self, data: &[u8]) {
        let index = (reg::read(TXFQS) >> 16) & 0x3;
        let element = TX_BUFFERS + index * ELEMENT;
        let id = self.ids.device_to_host;
        let (xtd, id) = if self.ids.extended {
            (1 << 30, id)
        } else {
            (0, id << 18)
        };
        reg::write(element, xtd | id);
        reg::write(
            element + 4,
            (1 << 21) | (1 << 20) | (len_to_dlc(data.len()) << 16),
        );
        for (word, chunk) in data.chunks(4).enumerate() {
            let mut bytes = [0u8; 4];
            for (to, from) in bytes.iter_mut().zip(chunk) {
                *to = *from;
            }
            reg::write(element + 8 + word as u32 * 4, u32::from_le_bytes(bytes));
        }
        reg::write(TXBAR, 1 << index);
    }
}

fn dlc_to_len(dlc: u32) -> usize {
    match dlc {
        0..=8 => dlc as usize,
        9 => 12,
        10 => 16,
        11 => 20,
        12 => 24,
        13 => 32,
        14 => 48,
        _ => 64,
    }
}

fn len_to_dlc(len: usize) -> u32 {
    match len {
        0..=8 => len as u32,
        9..=12 => 9,
        13..=16 => 10,
        17..=20 => 11,
        21..=24 => 12,
        25..=32 => 13,
        33..=48 => 14,
        _ => 15,
    }
}
