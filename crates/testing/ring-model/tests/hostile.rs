//! The hostile-ring check on the stable toolchain. The `ring` fuzz target goes deeper with the
//! same function.

use orion_ring_model::hostile;

struct Random(u64);

impl Random {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }
}

#[test]
fn random_rings_never_take_a_reader_out_of_its_shape() {
    let mut random = Random(0x2545_F491_4F6C_DD1D);
    let mut data = Vec::new();
    for round in 0..20_000_u64 {
        data.clear();
        data.push(round as u8);
        for _ in 0..(random.next() % 200) {
            data.extend_from_slice(&random.next().to_le_bytes());
        }
        hostile(&data);
    }
}

#[test]
fn a_valid_header_over_random_contents_is_read_safely() {
    // Random bytes almost never pass the header check, so give the reader a real header and
    // random everything else: head, lane heads, index, stamps, lengths.
    let mut random = Random(0x9E37_79B9_7F4A_7C15);
    let shapes: [(u64, u64, u64); 4] = [(1, 2, 8), (3, 4, 24), (2, 8, 0), (7, 2, 40)];
    for round in 0..20_000_u64 {
        let (fields, history, max_payload) = shapes[(round % 4) as usize];
        let lanes = fields * history;
        let total = 16 + fields + lanes.next_power_of_two() + lanes * (4 + max_payload.div_ceil(8));
        let mut words = vec![0_u64; 192];
        for word in &mut words {
            // Mostly small numbers, which is where the interesting stamps and lengths are.
            *word = match random.next() % 4 {
                0 => random.next(),
                1 => random.next() % 64,
                2 => (random.next() % 32) << 1,
                _ => 0,
            };
        }
        words[0] = u64::from_le_bytes(*b"ORIONRG1");
        words[1] = 1;
        words[2] = fields | history << 32;
        words[3] = max_payload;
        words[4] = total;
        let mut data = vec![0_u8];
        for word in &words {
            data.extend_from_slice(&word.to_le_bytes());
        }
        hostile(&data);
    }
}
