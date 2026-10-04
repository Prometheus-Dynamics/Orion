#![allow(dead_code)]

use orion_link::FrameHeader;

/// A delivered or sent message: `(kind, seq, payload)`.
pub type Message = (u8, u16, Vec<u8>);

/// Tiny deterministic PRNG (xorshift64*), so property tests need no extra dependencies.
pub struct Rng(u64);

impl Rng {
    pub fn new(seed: u64) -> Self {
        Self(seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1)
    }

    pub fn next_u64(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x >> 12;
        x ^= x << 25;
        x ^= x >> 27;
        self.0 = x;
        x.wrapping_mul(0x2545_F491_4F6C_DD1D)
    }

    pub fn below(&mut self, bound: usize) -> usize {
        if bound == 0 {
            0
        } else {
            (self.next_u64() % bound as u64) as usize
        }
    }

    pub fn byte(&mut self) -> u8 {
        self.next_u64() as u8
    }

    pub fn chance(&mut self, percent: usize) -> bool {
        self.below(100) < percent
    }

    /// Random payload biased towards zeros and 254-byte runs so COBS edge cases come up often.
    pub fn payload(&mut self, max_len: usize) -> Vec<u8> {
        let len = match self.below(4) {
            0 => self.below(8.min(max_len + 1)),
            _ => self.below(max_len + 1),
        };
        let style = self.below(4);
        (0..len)
            .map(|_| match style {
                0 => self.byte(),
                1 => {
                    if self.chance(30) {
                        0
                    } else {
                        self.byte()
                    }
                }
                2 => {
                    if self.chance(1) {
                        0
                    } else {
                        self.byte() | 1
                    }
                }
                _ => {
                    if self.chance(50) {
                        0
                    } else {
                        0xFF
                    }
                }
            })
            .collect()
    }

    pub fn header(&mut self) -> FrameHeader {
        FrameHeader::new(self.byte(), self.next_u64() as u16)
    }
}

/// Builds the frame bytes for a header and payload.
pub fn frame_bytes(header: FrameHeader, payload: &[u8]) -> Vec<u8> {
    let mut out = vec![0u8; orion_link::frame::frame_len(payload.len())];
    let n = orion_link::frame::encode(header, payload, &mut out).expect("frame fits");
    out.truncate(n);
    out
}

/// COBS stream bytes for a header and payload.
pub fn stream_bytes(header: FrameHeader, payload: &[u8]) -> Vec<u8> {
    orion_link::StreamEncoder::for_message(header, payload).collect()
}
