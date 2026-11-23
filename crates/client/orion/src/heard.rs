//! The other boards a board hears on its local network: what each announces of itself, and
//! how far the board's keys know it. Read from the board's own file `orion/boards`.
//!
//! **Nothing here is proven.** Anyone on that network can announce any name and any key. A
//! board is checked when something connects to it.

use std::net::IpAddr;

/// How the keys of the board that was asked know a board it hears.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Trust {
    /// It announces the key pinned for its name.
    Enrolled,
    /// The keys do not know it.
    Discovered,
    /// The key it announces was revoked.
    Revoked,
    /// It announces another key than the one pinned for its name: a board with a new key,
    /// or something pretending to be it.
    KeyMismatch,
    /// It speaks another wire.
    Incompatible,
    /// The board that was asked has security off: its keys are not asked.
    Open,
    /// Its announcement could not be read.
    Unreadable,
    /// A word this build does not know, as it was said.
    Other(String),
}

/// A board heard.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct HeardBoard {
    /// Its name.
    pub name: String,
    /// Its addresses on the network it was heard on.
    pub addresses: Vec<IpAddr>,
    /// Where it takes connections.
    pub port: u16,
    /// The fingerprint of the key it announces.
    pub key: Option<String>,
    /// Its cluster.
    pub cluster: Option<String>,
    /// What it is doing: `running` or `flasher`.
    pub state: Option<String>,
    /// Its security switch: `on`, `off` or `damaged`.
    pub security: Option<String>,
    /// Its clock, if it measures it: `synced` or `unset`.
    pub clock: Option<String>,
    /// How the keys of the board that was asked know it.
    pub trust: Trust,
    /// How long before the answer it was last heard, in milliseconds. A board is dropped
    /// when it says goodbye, or 150 seconds after it was last heard.
    pub heard_ms_ago: u64,
}

/// The boards a board hears.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct HeardBoards {
    /// The boards, by name.
    pub boards: Vec<HeardBoard>,
    /// How many announcements were of boards past the most the board keeps: not zero, there
    /// are more boards than are listed.
    pub not_listed: u64,
}

fn said(word: &str) -> Option<String> {
    (word != "-").then(|| word.to_owned())
}

fn board(line: &str) -> Option<HeardBoard> {
    let mut words = line.split(' ');
    let mut next = || words.next();
    Some(HeardBoard {
        name: next()?.to_owned(),
        addresses: next()?
            .split(',')
            .filter_map(|address| address.parse().ok())
            .collect(),
        port: next()?.parse().ok()?,
        key: said(next()?),
        cluster: said(next()?),
        state: said(next()?),
        security: said(next()?),
        clock: said(next()?),
        trust: match next()? {
            "enrolled" => Trust::Enrolled,
            "discovered" => Trust::Discovered,
            "revoked" => Trust::Revoked,
            "key-mismatch" => Trust::KeyMismatch,
            "incompatible" => Trust::Incompatible,
            "open" => Trust::Open,
            "unreadable" => Trust::Unreadable,
            other => Trust::Other(other.to_owned()),
        },
        heard_ms_ago: next()?.parse().ok()?,
    })
}

/// What `orion/boards` says. A line this build cannot read is left out.
#[must_use]
pub fn read(text: &str) -> HeardBoards {
    let mut heard = HeardBoards::default();
    for line in text.lines() {
        if let Some(rest) = line.strip_prefix("not listed: ") {
            heard.not_listed = rest
                .split(' ')
                .next()
                .and_then(|count| count.parse().ok())
                .unwrap_or(0);
        } else if !line.starts_with('#')
            && let Some(board) = board(line)
        {
            heard.boards.push(board);
        }
    }
    heard
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_boards_file_is_read_line_by_line_and_what_cannot_be_read_is_left_out() {
        let heard = read(
            "# name addresses port key cluster state security clock trust heard_ms_ago\n\
             arm 10.0.0.3,fe80::1 9200 sha256:ab12 lab running on synced enrolled 1200\n\
             cam - 9201 - - - - - unreadable 7\n\
             new 10.0.0.4 9200 sha256:cd34 lab flasher off - later-word 9 and more\n\
             half 10.0.0.5 9200\n\
             not listed: 3 announcements of boards past the 64 kept\n",
        );
        assert_eq!(heard.not_listed, 3);
        assert_eq!(heard.boards.len(), 3, "{heard:?}");
        assert_eq!(
            heard.boards[0],
            HeardBoard {
                name: "arm".into(),
                addresses: vec![
                    "10.0.0.3".parse().expect("v4"),
                    "fe80::1".parse().expect("v6")
                ],
                port: 9200,
                key: Some("sha256:ab12".into()),
                cluster: Some("lab".into()),
                state: Some("running".into()),
                security: Some("on".into()),
                clock: Some("synced".into()),
                trust: Trust::Enrolled,
                heard_ms_ago: 1200,
            }
        );
        let cam = &heard.boards[1];
        assert_eq!(
            (cam.addresses.len(), &cam.key, &cam.clock),
            (0, &None, &None)
        );
        assert_eq!(cam.trust, Trust::Unreadable);
        assert_eq!(heard.boards[2].trust, Trust::Other("later-word".into()));
        assert_eq!(read("").boards, Vec::new());
    }
}
