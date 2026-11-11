//! USART2 as an Orion link: 115200 baud 8N1 on PA2 (TX) and PA3 (RX), polled.

use crate::reg::{self, gpioa, rcc};

const BASE: u32 = 0x4000_4400;
const CR1: u32 = BASE;
const BRR: u32 = BASE + 0x0C;
const ISR: u32 = BASE + 0x1C;
const ICR: u32 = BASE + 0x20;
const RDR: u32 = BASE + 0x24;
const TDR: u32 = BASE + 0x28;

const ISR_ORE: u32 = 1 << 3;
const ISR_RXNE: u32 = 1 << 5;
const ISR_TXE: u32 = 1 << 7;

/// The link's USART2 driver.
pub struct Usart;

impl Usart {
    /// Clocks USART2 from PCLK1 (16 MHz HSI after reset) and routes PA2/PA3.
    pub fn new() -> Self {
        reg::set(rcc::AHB2ENR, 1 << 0); // GPIOA
        reg::set(rcc::APB1ENR1, 1 << 17); // USART2
        gpioa::alternate(2, 7);
        gpioa::alternate(3, 7);
        reg::write(BRR, 139); // 16 MHz / 115200
        reg::write(CR1, (1 << 3) | (1 << 2) | 1); // TE, RE, UE
        Self
    }

    /// The next received byte, if any. An overrun is cleared; the lost bytes fail the frame's CRC
    /// and the session retransmits.
    pub fn read(&mut self) -> Option<u8> {
        let isr = reg::read(ISR);
        if isr & ISR_ORE != 0 {
            reg::write(ICR, ISR_ORE);
        }
        (isr & ISR_RXNE != 0).then(|| reg::read(RDR) as u8)
    }

    /// Whether the transmit register takes a byte.
    pub fn can_write(&self) -> bool {
        reg::read(ISR) & ISR_TXE != 0
    }

    /// Writes one byte. Call only when [`Usart::can_write`].
    pub fn write(&mut self, byte: u8) {
        reg::write(TDR, u32::from(byte));
    }
}
