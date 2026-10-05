//! The compact UTF-8 validator of the device path (`wire::str_from_utf8`) accepts exactly what
//! `core::str::from_utf8` accepts. Its result is used with `from_utf8_unchecked`, so this is the
//! soundness argument: exhaustive over every 1- and 2-byte input and every 3-byte input with a
//! multi-byte lead, near-exhaustive over 4-byte forms, plus random mixed strings.

#![cfg(feature = "device")]

#[path = "common/mod.rs"]
mod common;

use orion_link::wire::str_from_utf8;

fn agrees(bytes: &[u8]) {
    let ours = str_from_utf8(bytes).ok();
    let core = core::str::from_utf8(bytes).ok();
    assert_eq!(ours, core, "{bytes:02x?}");
}

#[test]
fn every_short_input_agrees_with_core() {
    agrees(&[]);
    for a in 0..=255u8 {
        agrees(&[a]);
        for b in 0..=255u8 {
            agrees(&[a, b]);
        }
    }
    // Three bytes: ASCII and continuation leads reduce to the cases above.
    for a in 0xC0..=0xFFu8 {
        for b in 0..=255u8 {
            for c in 0..=255u8 {
                agrees(&[a, b, c]);
            }
        }
    }
}

#[test]
fn four_byte_forms_agree_with_core() {
    // Continuation bytes around the 10xxxxxx boundaries.
    let around: Vec<u8> = (0x70..=0xC8).collect();
    for a in 0xF0..=0xF8u8 {
        for &b in &around {
            for &c in &around {
                for &d in &around {
                    agrees(&[a, b, c, d]);
                }
            }
        }
    }
}

#[test]
fn random_strings_agree_with_core() {
    let mut rng = common::Rng::new(0x5EED);
    let samples = [
        "a",
        "é",
        "€",
        "😀",
        "\u{10FFFF}",
        "\u{FFFF}",
        "\u{D7FF}",
        "\u{E000}",
    ];
    for _ in 0..20_000 {
        let mut bytes = Vec::new();
        for _ in 0..rng.below(8) {
            match rng.below(3) {
                0 => bytes.extend_from_slice(samples[rng.below(samples.len())].as_bytes()),
                1 => bytes.push(rng.byte()),
                _ => bytes.push(0x80 | (rng.byte() & 0x3F)),
            }
        }
        agrees(&bytes);
        for cut in 0..bytes.len() {
            agrees(&bytes[..cut]);
        }
    }
}
