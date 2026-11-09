//! The recorded bytes. To record a deliberate addition: `UPDATE_VECTORS=1 cargo test -p orion-wire-vectors`.
//! Never re-record a kind that already has a line. That is the change these tests exist to catch.

use std::collections::BTreeMap;
use std::fmt::Write as _;

use orion_wire::kind;
use orion_wire_vectors::{all, decoders};

const RECORDED: &str = include_str!("../vectors.txt");

fn recorded() -> BTreeMap<u8, Vec<u8>> {
    RECORDED
        .lines()
        .filter(|line| !line.is_empty() && !line.starts_with('#'))
        .map(|line| {
            let mut parts = line.split_whitespace();
            let number = u8::from_str_radix(parts.next().unwrap(), 16).unwrap();
            let _name = parts.next().unwrap();
            let hex = parts.next().unwrap_or("");
            let bytes = (0..hex.len())
                .step_by(2)
                .map(|i| u8::from_str_radix(&hex[i..i + 2], 16).unwrap())
                .collect();
            (number, bytes)
        })
        .collect()
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().fold(String::new(), |mut out, byte| {
        write!(out, "{byte:02x}").unwrap();
        out
    })
}

#[test]
fn every_body_matches_its_recorded_bytes() {
    let samples = all();
    if std::env::var_os("UPDATE_VECTORS").is_some() {
        let mut text = String::from("# kind name body-hex. Pins every body byte for byte.\n");
        for sample in &samples {
            let row = kind::info(sample.kind).unwrap();
            writeln!(
                text,
                "{:02x} {} {}",
                sample.kind,
                row.name,
                hex(&sample.bytes)
            )
            .unwrap();
        }
        let path = concat!(env!("CARGO_MANIFEST_DIR"), "/vectors.txt");
        std::fs::write(path, text).unwrap();
        return;
    }
    let recorded = recorded();
    for sample in &samples {
        let row = kind::info(sample.kind).unwrap();
        let want = recorded
            .get(&sample.kind)
            .unwrap_or_else(|| panic!("{} has no recorded bytes", row.name));
        assert_eq!(
            hex(&sample.bytes),
            hex(want),
            "{} changed on the wire",
            row.name
        );
    }
}

#[test]
fn every_kind_has_a_vector_and_a_decoder() {
    let samples = all();
    let recorded = recorded();
    for row in kind::TABLE {
        assert_eq!(
            samples.iter().filter(|s| s.kind == row.kind).count(),
            1,
            "{} needs exactly one sample",
            row.name
        );
        assert!(
            recorded.contains_key(&row.kind),
            "{} has no recorded line",
            row.name
        );
        assert!(
            decoders::decode_kind(row.kind, &[]),
            "{} has no decoder",
            row.name
        );
    }
    assert_eq!(samples.len(), kind::TABLE.len());
}

#[test]
fn a_truncated_body_never_panics() {
    for sample in all() {
        for cut in 0..sample.bytes.len() {
            decoders::decode_kind(sample.kind, &sample.bytes[..cut]);
        }
    }
}

struct Random(u64);

impl Random {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }

    fn below(&mut self, limit: usize) -> usize {
        (self.next() % limit.max(1) as u64) as usize
    }
}

/// A fast, always-on cousin of the fuzz targets.
#[test]
fn garbage_never_panics_any_decoder() {
    let mut random = Random(0x9E37_79B9_7F4A_7C15);
    let samples = all();
    for sample in &samples {
        for _ in 0..1_000 {
            let mut bytes = sample.bytes.clone();
            for _ in 0..=random.below(4) {
                if bytes.is_empty() {
                    break;
                }
                let at = random.below(bytes.len());
                bytes[at] = random.next() as u8;
            }
            if random.below(4) == 0 {
                bytes.truncate(random.below(bytes.len() + 1));
            }
            decoders::decode_kind(sample.kind, &bytes);
        }
    }
    for _ in 0..20_000 {
        let len = random.below(128);
        let bytes: Vec<u8> = (0..len).map(|_| random.next() as u8).collect();
        for row in kind::TABLE {
            decoders::decode_kind(row.kind, &bytes);
        }
        decoders::plain_frame(&bytes);
        decoders::stream(&bytes);
        decoders::packet(&bytes);
        decoders::socket(&bytes);
    }
}
