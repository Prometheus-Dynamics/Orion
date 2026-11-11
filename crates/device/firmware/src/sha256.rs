//! SHA-256, small: one round function in a loop, for a microcontroller's flash. A general
//! implementation unrolls the rounds for speed and costs several times the bytes; an image is
//! hashed once when it arrives, so size wins. Tests hold it to the general one.

const K: [u32; 64] = [
    0x428a_2f98,
    0x7137_4491,
    0xb5c0_fbcf,
    0xe9b5_dba5,
    0x3956_c25b,
    0x59f1_11f1,
    0x923f_82a4,
    0xab1c_5ed5,
    0xd807_aa98,
    0x1283_5b01,
    0x2431_85be,
    0x550c_7dc3,
    0x72be_5d74,
    0x80de_b1fe,
    0x9bdc_06a7,
    0xc19b_f174,
    0xe49b_69c1,
    0xefbe_4786,
    0x0fc1_9dc6,
    0x240c_a1cc,
    0x2de9_2c6f,
    0x4a74_84aa,
    0x5cb0_a9dc,
    0x76f9_88da,
    0x983e_5152,
    0xa831_c66d,
    0xb003_27c8,
    0xbf59_7fc7,
    0xc6e0_0bf3,
    0xd5a7_9147,
    0x06ca_6351,
    0x1429_2967,
    0x27b7_0a85,
    0x2e1b_2138,
    0x4d2c_6dfc,
    0x5338_0d13,
    0x650a_7354,
    0x766a_0abb,
    0x81c2_c92e,
    0x9272_2c85,
    0xa2bf_e8a1,
    0xa81a_664b,
    0xc24b_8b70,
    0xc76c_51a3,
    0xd192_e819,
    0xd699_0624,
    0xf40e_3585,
    0x106a_a070,
    0x19a4_c116,
    0x1e37_6c08,
    0x2748_774c,
    0x34b0_bcb5,
    0x391c_0cb3,
    0x4ed8_aa4a,
    0x5b9c_ca4f,
    0x682e_6ff3,
    0x748f_82ee,
    0x78a5_636f,
    0x84c8_7814,
    0x8cc7_0208,
    0x90be_fffa,
    0xa450_6ceb,
    0xbef9_a3f7,
    0xc671_78f2,
];

const H0: [u32; 8] = [
    0x6a09_e667,
    0xbb67_ae85,
    0x3c6e_f372,
    0xa54f_f53a,
    0x510e_527f,
    0x9b05_688c,
    0x1f83_d9ab,
    0x5be0_cd19,
];

/// A SHA-256 in progress.
#[derive(Clone, Debug)]
pub struct Sha256 {
    state: [u32; 8],
    block: [u8; 64],
    /// Bytes in `block`.
    filled: usize,
    /// Bytes fed in all.
    total: u64,
}

impl Default for Sha256 {
    fn default() -> Self {
        Self::new()
    }
}

impl Sha256 {
    /// Nothing fed yet.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            state: H0,
            block: [0; 64],
            filled: 0,
            total: 0,
        }
    }

    /// Feeds `bytes`.
    pub fn update(&mut self, bytes: &[u8]) {
        for &byte in bytes {
            self.push(byte);
        }
        self.total = self.total.wrapping_add(bytes.len() as u64);
    }

    fn push(&mut self, byte: u8) {
        if let Some(slot) = self.block.get_mut(self.filled) {
            *slot = byte;
        }
        self.filled += 1;
        if self.filled == 64 {
            compress(&mut self.state, &self.block);
            self.filled = 0;
        }
    }

    /// The digest.
    #[must_use]
    pub fn finish(mut self) -> [u8; 32] {
        let bits = self.total.wrapping_mul(8);
        self.push(0x80);
        while self.filled != 56 {
            self.push(0);
        }
        for byte in bits.to_be_bytes() {
            self.push(byte);
        }
        let mut out = [0_u8; 32];
        for (i, to) in out.iter_mut().enumerate() {
            let word = self.state.get(i / 4).copied().unwrap_or(0);
            *to = word.to_be_bytes().get(i % 4).copied().unwrap_or(0);
        }
        out
    }
}

fn compress(state: &mut [u32; 8], block: &[u8; 64]) {
    let mut w = [0_u32; 64];
    for (i, byte) in block.iter().enumerate() {
        if let Some(word) = w.get_mut(i / 4) {
            *word = (*word << 8) | u32::from(*byte);
        }
    }
    for i in 16..64 {
        let (w2, w7, w15, w16) = (w[i - 2], w[i - 7], w[i - 15], w[i - 16]);
        let s0 = w15.rotate_right(7) ^ w15.rotate_right(18) ^ (w15 >> 3);
        let s1 = w2.rotate_right(17) ^ w2.rotate_right(19) ^ (w2 >> 10);
        w[i] = w16.wrapping_add(s0).wrapping_add(w7).wrapping_add(s1);
    }
    let mut v = *state;
    for (k, w) in K.iter().zip(w) {
        let [a, b, c, d, e, f, g, h] = v;
        let s1 = e.rotate_right(6) ^ e.rotate_right(11) ^ e.rotate_right(25);
        let ch = (e & f) ^ (!e & g);
        let t1 = h
            .wrapping_add(s1)
            .wrapping_add(ch)
            .wrapping_add(*k)
            .wrapping_add(w);
        let s0 = a.rotate_right(2) ^ a.rotate_right(13) ^ a.rotate_right(22);
        let maj = (a & b) ^ (a & c) ^ (b & c);
        let t2 = s0.wrapping_add(maj);
        v = [t1.wrapping_add(t2), a, b, c, d.wrapping_add(t1), e, f, g];
    }
    for (s, v) in state.iter_mut().zip(v) {
        *s = s.wrapping_add(v);
    }
}

#[cfg(test)]
mod tests {
    use super::Sha256;
    use sha2::Digest;

    #[test]
    fn it_is_sha_256_at_every_length_around_a_block() {
        let bytes: Vec<u8> = (0..300_u32).map(|n| (n * 7 + 3) as u8).collect();
        for len in (0..200).chain([255, 256, 299]) {
            let mut ours = Sha256::new();
            // Fed in uneven pieces, as flash is read.
            for piece in bytes[..len].chunks(13) {
                ours.update(piece);
            }
            let want: [u8; 32] = sha2::Sha256::digest(&bytes[..len]).into();
            assert_eq!(ours.finish(), want, "at {len} bytes");
        }
    }
}
