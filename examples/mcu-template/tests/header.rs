//! The C header says what the library does: every value in it is the library's, every
//! function the library exports is declared and nothing else is, and it compiles under each
//! of its configurations.

#![cfg(any(feature = "ffi-uart", feature = "ffi-can"))]

use std::collections::BTreeMap;
use std::path::Path;
use std::process::Command;

use orion_mcu_template::ffi;

const HEADER: &str = include_str!("../include/orion_mcu.h");
const SOURCE: &str = concat!(
    include_str!("../src/ffi/mod.rs"),
    include_str!("../src/ffi/slots.rs")
);

/// Evaluates what the header defines: `8`, `1u`, `(1u << 3)`, `(-1)`, and the version.
fn defines() -> BTreeMap<String, i64> {
    let mut found = BTreeMap::new();
    for line in HEADER.lines() {
        let Some(rest) = line.strip_prefix("#define ") else {
            continue;
        };
        let rest = rest.split("/*").next().unwrap_or_default().trim();
        let Some((name, value)) = rest.split_once(' ') else {
            continue;
        };
        let value = value.trim().trim_start_matches('(').trim_end_matches(')');
        let number = |text: &str| text.trim().trim_end_matches('u').parse::<i64>().ok();
        let value = if let Some((left, right)) = value.split_once("<<") {
            number(left).zip(number(right)).map(|(l, r)| l << r)
        } else {
            number(value)
        };
        if let Some(value) = value {
            found.insert(name.to_owned(), value);
        }
    }
    found
}

#[test]
fn every_value_in_the_header_is_the_librarys() {
    let defines = defines();
    let library: &[(&str, i64)] = &[
        ("ORION_MCU_API_MAJOR", ffi::ORION_MCU_API_MAJOR.into()),
        ("ORION_MCU_API_MINOR", ffi::ORION_MCU_API_MINOR.into()),
        ("ORION_MAX_TOPICS", ffi::ORION_MAX_TOPICS as i64),
        ("ORION_MAX_ACTIONS", ffi::ORION_MAX_ACTIONS as i64),
        ("ORION_EVENT_CONNECTED", ffi::ORION_EVENT_CONNECTED.into()),
        (
            "ORION_EVENT_DISCONNECTED",
            ffi::ORION_EVENT_DISCONNECTED.into(),
        ),
        ("ORION_EVENT_REJECTED", ffi::ORION_EVENT_REJECTED.into()),
        ("ORION_EVENT_RUN", ffi::ORION_EVENT_RUN.into()),
        ("ORION_EVENT_CANCEL", ffi::ORION_EVENT_CANCEL.into()),
        ("ORION_EVENT_DEMAND", ffi::ORION_EVENT_DEMAND.into()),
        ("ORION_EVENT_FIRMWARE", ffi::ORION_EVENT_FIRMWARE.into()),
        ("ORION_FIELD_BOOL", ffi::ORION_FIELD_BOOL.into()),
        ("ORION_FIELD_INT", ffi::ORION_FIELD_INT.into()),
        ("ORION_FIELD_FLOAT", ffi::ORION_FIELD_FLOAT.into()),
        ("ORION_ADMITS_LOCAL", ffi::ORION_ADMITS_LOCAL.into()),
        ("ORION_ADMITS_OPERATOR", ffi::ORION_ADMITS_OPERATOR.into()),
        ("ORION_ADMITS_PEER", ffi::ORION_ADMITS_PEER.into()),
        (
            "ORION_OUTCOME_SUCCEEDED",
            ffi::ORION_OUTCOME_SUCCEEDED.into(),
        ),
        ("ORION_OUTCOME_FAILED", ffi::ORION_OUTCOME_FAILED.into()),
        ("ORION_OUTCOME_REJECTED", ffi::ORION_OUTCOME_REJECTED.into()),
        ("ORION_ERR_STATE", ffi::ORION_ERR_STATE.into()),
        ("ORION_ERR_ARG", ffi::ORION_ERR_ARG.into()),
        ("ORION_ERR_FULL", ffi::ORION_ERR_FULL.into()),
        ("ORION_ERR_CLOSED", ffi::ORION_ERR_CLOSED.into()),
        ("ORION_ERR_BUSY", ffi::ORION_ERR_BUSY.into()),
        ("ORION_ERR_LINK", ffi::ORION_ERR_LINK.into()),
    ];
    for (name, value) in library {
        assert_eq!(defines.get(*name), Some(value), "{name}");
    }
    let named: Vec<&str> = library.iter().map(|(name, _)| *name).collect();
    for name in defines.keys() {
        assert!(named.contains(&name.as_str()), "{name} is not checked");
    }
}

/// The names in `text` that follow `before` and end at `(`.
fn names(text: &str, before: &str) -> Vec<String> {
    let mut found: Vec<String> = text
        .match_indices(before)
        .filter_map(|(at, _)| {
            let rest = &text[at + before.len()..];
            let end = rest.find('(')?;
            let name = rest[..end].trim();
            name.starts_with("orion_").then(|| name.to_owned())
        })
        .collect();
    found.sort();
    found.dedup();
    found
}

#[test]
fn the_header_declares_what_the_library_exports() {
    // The library's own, and the hooks it calls, which the port defines.
    let mut exported: Vec<String> = names(SOURCE, "extern \"C\" fn ")
        .into_iter()
        .chain(names(SOURCE, "        fn "))
        .collect();
    exported.sort();
    exported.dedup();
    let mut declared = Vec::new();
    for line in HEADER
        .lines()
        .filter(|line| !line.starts_with('#') && !line.starts_with(' '))
    {
        if let Some(open) = line.find('(') {
            let name = line[..open].rsplit([' ', '*']).next().unwrap_or_default();
            if name.starts_with("orion_") {
                declared.push(name.to_owned());
            }
        }
    }
    declared.sort();
    assert_eq!(declared, exported);
}

#[test]
fn the_header_compiles_in_every_configuration() {
    let Some(cc) = ["cc", "gcc", "clang"].into_iter().find(|cc| {
        Command::new(cc)
            .arg("--version")
            .output()
            .is_ok_and(|out| out.status.success())
    }) else {
        eprintln!("no C compiler: the header is not compiled");
        return;
    };
    let root = Path::new(env!("CARGO_MANIFEST_DIR"));
    for defines in [
        &["-DORION_MCU_UART"][..],
        &["-DORION_MCU_CAN"],
        &["-DORION_MCU_UART", "-DORION_MCU_SLOTS"],
        &["-DORION_MCU_CAN", "-DORION_MCU_SLOTS"],
    ] {
        let out = Command::new(cc)
            .args([
                "-std=c11",
                "-Wall",
                "-Wextra",
                "-Wno-type-limits",
                "-Werror",
            ])
            .args(["-fsyntax-only", "-I"])
            .arg(root.join("include"))
            .args(defines)
            .arg(root.join("tests/c/header.c"))
            .output()
            .expect("cc");
        assert!(
            out.status.success(),
            "{defines:?}: {}",
            String::from_utf8_lossy(&out.stderr)
        );
    }
}
