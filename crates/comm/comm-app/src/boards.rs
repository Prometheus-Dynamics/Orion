//! The other boards heard on the local network, as `orion/boards` says them: what each
//! announces of itself, and how far this board's keys know it.
//!
//! One line a board: its name, its addresses, its port, its key's fingerprint, its cluster,
//! what it is doing, its security switch, its clock, how this board's keys know it, and how
//! long ago it was last heard. A value it does not say is `-`.
//!
//! **How the keys know it**, when this board's security is on: `enrolled`, its key is the one
//! pinned for its name; `key-mismatch`, another key is; `revoked`, its key was revoked here;
//! `discovered`, the keys do not know it. With security off the keys are not asked: `open`.
//! A board that speaks another wire is `incompatible`, and one whose announcement cannot be
//! read is `unreadable`.
//!
//! **Nothing here is proven, and nothing connects to anything.** Anyone on the network can
//! announce any name and any key: `enrolled` says the announcement names a pinned key, and
//! the key is checked when something connects.

use std::fmt::Write as _;
use std::time::Instant;

use orion_keys::SharedKeys;
use orion_mdns::{Boards, Found, Known};

/// The first line.
pub(crate) const HEADER: &str =
    "# name addresses port key cluster state security clock trust heard_ms_ago\n";

/// The value of `key` in TXT strings: the first there is.
fn value<'a>(txt: &'a [Vec<u8>], key: &str) -> Option<&'a str> {
    txt.iter().find_map(|string| {
        let (name, value) = std::str::from_utf8(string).ok()?.split_once('=')?;
        name.eq_ignore_ascii_case(key).then_some(value)
    })
}

/// The key TXT strings say, if they say one.
fn key(txt: &[Vec<u8>]) -> Option<[u8; 32]> {
    let text = value(txt, "pk")?;
    if text.len() != 64 || !text.is_ascii() {
        return None;
    }
    let mut key = [0_u8; 32];
    for (byte, pair) in key.iter_mut().zip(text.as_bytes().chunks(2)) {
        *byte = u8::from_str_radix(std::str::from_utf8(pair).ok()?, 16).ok()?;
    }
    Some(key)
}

/// How this board's keys know the board that says `txt`.
fn trust(keys: &SharedKeys, secured: bool, txt: &[Vec<u8>]) -> &'static str {
    let (Some(node), Some(key)) = (value(txt, "id").and_then(|id| id.parse().ok()), key(txt))
    else {
        return "unreadable";
    };
    if value(txt, "wv") != Some(orion_wire::PROTOCOL_VERSION.to_string().as_str()) {
        return "incompatible";
    }
    if !secured {
        return "open";
    }
    let keys = keys.lock();
    if keys.is_revoked(&key) {
        return "revoked";
    }
    match keys.peer_key(&node) {
        Some(pinned) if *pinned == key => "enrolled",
        Some(_) => "key-mismatch",
        None => "discovered",
    }
}

/// Which announcements are of a board the keys pin: those always have a place in the list.
pub(crate) fn known(keys: SharedKeys, secured: bool) -> Known {
    Box::new(move |txt| trust(&keys, secured, txt) == "enrolled")
}

/// A value as one word of a line.
fn word(value: Option<&str>) -> String {
    match value {
        Some(value) if !value.is_empty() => value
            .chars()
            .map(|c| {
                if c.is_whitespace() || c.is_control() {
                    '_'
                } else {
                    c
                }
            })
            .collect(),
        _ => "-".to_owned(),
    }
}

fn line(text: &mut String, found: &Found, keys: &SharedKeys, secured: bool, now: Instant) {
    let txt = found.txt.as_slice();
    let addresses: Vec<String> = found.addresses.iter().map(ToString::to_string).collect();
    let fingerprint = key(txt).map(|key| orion_keys::fingerprint(&key));
    let _ = writeln!(
        text,
        "{} {} {} {} {} {} {} {} {} {}",
        word(value(txt, "id").or(Some(&found.instance))),
        word(Some(&addresses.join(","))),
        found.port,
        word(fingerprint.as_deref()),
        word(value(txt, "cl")),
        word(value(txt, "state")),
        word(value(txt, "security")),
        word(value(txt, "clock")),
        trust(keys, secured, txt),
        now.saturating_duration_since(found.heard).as_millis(),
    );
}

/// What `orion/boards` says at `now`.
pub(crate) fn text(boards: &Boards, keys: &SharedKeys, secured: bool, now: Instant) -> String {
    said(boards.found(), boards.refused(), keys, secured, now)
}

/// The same, of what was `found`, with `refused` announcements not kept.
fn said(found: Vec<Found>, refused: u64, keys: &SharedKeys, secured: bool, now: Instant) -> String {
    let mut text = String::from(HEADER);
    for found in &found {
        line(&mut text, found, keys, secured, now);
    }
    if refused > 0 {
        let _ = writeln!(
            text,
            "not listed: {refused} announcements of boards past the {} kept",
            orion_mdns::MOST
        );
    }
    text
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use orion_keys::{Keys, KeysConfig};

    use super::*;

    const KEY: [u8; 32] = [0xab; 32];

    fn keys() -> SharedKeys {
        let config = KeysConfig::in_memory("lab");
        SharedKeys::new(Keys::open(config, orion_config::Security::On).expect("keys"))
    }

    fn hex(key: &[u8; 32]) -> String {
        key.iter().map(|byte| format!("{byte:02x}")).collect()
    }

    /// What a board named `id` with `key` announces, speaking wire `wire`.
    fn txt(id: &str, key: &[u8; 32], wire: u8) -> Vec<Vec<u8>> {
        [
            "v=2".to_owned(),
            format!("id={id}"),
            format!("pk={}", hex(key)),
            format!("wv={wire}"),
            "cl=lab".to_owned(),
            "state=running".to_owned(),
            "security=on".to_owned(),
        ]
        .into_iter()
        .map(String::into_bytes)
        .collect()
    }

    const WIRE: u8 = orion_wire::PROTOCOL_VERSION;

    #[test]
    fn how_the_keys_know_a_board_is_said_in_one_word() {
        let keys = keys();
        let cam = txt("cam", &KEY, WIRE);
        let pinned = known(keys.clone(), true);
        assert_eq!(trust(&keys, true, &cam), "discovered");
        assert!(!pinned(&cam));
        // With security off the keys are not asked, whatever they hold.
        assert_eq!(trust(&keys, false, &cam), "open");

        let name = "cam".parse().expect("a name");
        assert!(keys.lock().trust_peer(&name, KEY).expect("kept"));
        assert_eq!(trust(&keys, true, &cam), "enrolled");
        assert!(pinned(&cam), "a pinned board has a place");
        assert!(
            !known(keys.clone(), false)(&cam),
            "none is known with security off"
        );
        assert_eq!(trust(&keys, false, &cam), "open");
        // The name with another key.
        let other = txt("cam", &[0xcd; 32], WIRE);
        assert_eq!(trust(&keys, true, &other), "key-mismatch");
        assert!(!pinned(&other));
        // Another wire, before anything the keys say.
        assert_eq!(
            trust(&keys, true, &txt("cam", &KEY, WIRE + 1)),
            "incompatible"
        );
        assert_eq!(
            trust(&keys, false, &txt("cam", &KEY, WIRE + 1)),
            "incompatible"
        );

        keys.lock().revoke(KEY).expect("kept");
        assert_eq!(trust(&keys, true, &cam), "revoked");
        assert!(!pinned(&cam));

        // No name, no key, a key that is not one, a name that is not one.
        for broken in [
            vec![b"v=2".to_vec()],
            vec![b"id=cam".to_vec()],
            vec![b"id=cam".to_vec(), b"pk=abcd".to_vec()],
            vec![b"id=".to_vec(), format!("pk={}", hex(&KEY)).into_bytes()],
            vec![vec![0xff, 0xfe]],
        ] {
            assert_eq!(trust(&keys, true, &broken), "unreadable", "{broken:?}");
            assert_eq!(trust(&keys, false, &broken), "unreadable", "{broken:?}");
        }
    }

    #[test]
    fn the_file_is_one_line_a_board_each_value_one_word() {
        let keys = keys();
        let now = Instant::now();
        let found = |instance: &str, txt: Vec<Vec<u8>>, addresses: &[&str], ago: u64| Found {
            instance: instance.to_owned(),
            port: 9200,
            addresses: addresses
                .iter()
                .map(|a| a.parse().expect("an address"))
                .collect(),
            txt,
            heard: now.checked_sub(Duration::from_millis(ago)).expect("then"),
        };
        let mut odd = txt("has space", &KEY, WIRE);
        odd.retain(|string| !string.starts_with(b"cl=") && !string.starts_with(b"id="));
        odd.push(b"cl=two words\tand a tab".to_vec());
        odd.push(b"clock=".to_vec());
        let mut timed = txt("arm", &KEY, WIRE);
        timed.push(b"clock=synced".to_vec());
        let text = said(
            vec![
                found("arm", timed, &["10.0.0.3", "fe80::1"], 1200),
                found("zed instance", odd, &[], 7),
            ],
            0,
            &keys,
            true,
            now,
        );
        let fingerprint = orion_keys::fingerprint(&KEY);
        assert_eq!(
            text,
            format!(
                "{HEADER}\
                 arm 10.0.0.3,fe80::1 9200 {fingerprint} lab running on synced discovered 1200\n\
                 zed_instance - 9200 {fingerprint} two_words_and_a_tab running on - unreadable 7\n"
            )
        );
        // Announcements of boards past those kept: said after the boards.
        let text = said(Vec::new(), 3, &keys, true, now);
        assert_eq!(
            text,
            format!("{HEADER}not listed: 3 announcements of boards past the 64 kept\n")
        );
    }
}
