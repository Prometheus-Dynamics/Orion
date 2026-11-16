//! The index of an offered prefix: what a program offers, and how large each file can be, so
//! that a client can say how much it will fetch, and warn of what may hold a user's data,
//! before it fetches any of it. Orion owns the format, so every program writes one alike, and
//! still knows nothing of what the files hold.
//!
//! A prefix `p/` offers its index as `p/index`. It is text, one item a line:
//!
//! ```text
//! orion-index 1
//! total 1048576
//! p/kernel 262144 no-user-data
//! p/journal 786432 user-data
//! ```
//!
//! - the first line is the format and its version;
//! - `total` bounds the whole prefix, in bytes;
//! - then each file: its whole name, the most bytes it can be, and whether it may hold a user's
//!   data (`user-data`) or not (`no-user-data`).
//!
//! Nothing else is a line of an index. A fetch held to an entry's bound
//! ([`crate::Fetching::within`]) refuses a file said to be larger.

use std::fmt;

/// The first line of an index.
pub const FIRST_LINE: &str = "orion-index 1";

/// One file of an index.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Entry {
    /// Its whole name, the prefix included.
    pub name: String,
    /// The most bytes it can be.
    pub max_bytes: u64,
    /// Whether it may hold a user's data.
    pub user_data: bool,
}

/// What a prefix offers.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Index {
    /// The most bytes of the whole prefix.
    pub total: u64,
    /// Each file.
    pub entries: Vec<Entry>,
}

/// Why text is not an index.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NotAnIndex {
    /// The line, counted from one.
    pub line: usize,
    /// What is wrong with it.
    pub why: &'static str,
}

impl fmt::Display for NotAnIndex {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "line {}: {}", self.line, self.why)
    }
}

impl std::error::Error for NotAnIndex {}

impl Index {
    /// The index as text.
    #[must_use]
    pub fn write(&self) -> String {
        let mut text = format!("{FIRST_LINE}\ntotal {}\n", self.total);
        for entry in &self.entries {
            let data = if entry.user_data {
                "user-data"
            } else {
                "no-user-data"
            };
            text.push_str(&format!("{} {} {data}\n", entry.name, entry.max_bytes));
        }
        text
    }

    /// Reads an index.
    ///
    /// # Errors
    ///
    /// Which line is not as an index has it, and why: another format or version, no `total`, a
    /// name that is not one, a number that is not, a word other than `user-data` or
    /// `no-user-data`, or a name twice.
    pub fn parse(text: &str) -> Result<Self, NotAnIndex> {
        let mut lines = text.lines().enumerate().map(|(at, line)| (at + 1, line));
        let bad = |line, why| NotAnIndex { line, why };
        match lines.next() {
            Some((_, FIRST_LINE)) => {}
            _ => return Err(bad(1, "not an index of this format")),
        }
        let total = match lines.next() {
            Some((line, text)) => text
                .strip_prefix("total ")
                .and_then(|total| total.parse().ok())
                .ok_or(bad(line, "not `total <bytes>`"))?,
            None => return Err(bad(2, "no `total <bytes>`")),
        };
        let mut entries: Vec<Entry> = Vec::new();
        for (line, text) in lines {
            let mut words = text.split(' ');
            let (Some(name), Some(max), Some(data), None) =
                (words.next(), words.next(), words.next(), words.next())
            else {
                return Err(bad(line, "not `<name> <max bytes> user-data|no-user-data`"));
            };
            if orion_types::FileName::new(name).is_err() {
                return Err(bad(line, "not a name"));
            }
            let max_bytes = max
                .parse()
                .map_err(|_| bad(line, "not a number of bytes"))?;
            let user_data = match data {
                "user-data" => true,
                "no-user-data" => false,
                _ => return Err(bad(line, "neither `user-data` nor `no-user-data`")),
            };
            if entries.iter().any(|entry| entry.name == name) {
                return Err(bad(line, "a name twice"));
            }
            entries.push(Entry {
                name: name.to_owned(),
                max_bytes,
                user_data,
            });
        }
        Ok(Self { total, entries })
    }

    /// The most bytes `name` can be, if the index has it.
    #[must_use]
    pub fn bound(&self, name: &str) -> Option<u64> {
        self.entries
            .iter()
            .find(|entry| entry.name == name)
            .map(|entry| entry.max_bytes)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample() -> Index {
        Index {
            total: 1 << 20,
            entries: vec![
                Entry {
                    name: "p/kernel".into(),
                    max_bytes: 262_144,
                    user_data: false,
                },
                Entry {
                    name: "p/journal".into(),
                    max_bytes: 786_432,
                    user_data: true,
                },
            ],
        }
    }

    #[test]
    fn an_index_written_reads_back_as_it_was() {
        let text = sample().write();
        assert_eq!(
            text,
            "orion-index 1\ntotal 1048576\np/kernel 262144 no-user-data\np/journal 786432 user-data\n"
        );
        assert_eq!(Index::parse(&text), Ok(sample()));
        assert_eq!(sample().bound("p/journal"), Some(786_432));
        assert_eq!(sample().bound("p/other"), None);
    }

    #[test]
    fn what_is_not_an_index_says_which_line_and_why() {
        let cases = [
            ("orion-index 2\ntotal 1\n", 1, "not an index of this format"),
            ("orion-index 1\n", 2, "no `total <bytes>`"),
            ("orion-index 1\ntotal lots\n", 2, "not `total <bytes>`"),
            (
                "orion-index 1\ntotal 1\np/a 1\n",
                3,
                "not `<name> <max bytes> user-data|no-user-data`",
            ),
            (
                "orion-index 1\ntotal 1\n../a 1 user-data\n",
                3,
                "not a name",
            ),
            (
                "orion-index 1\ntotal 1\np/a ten user-data\n",
                3,
                "not a number of bytes",
            ),
            (
                "orion-index 1\ntotal 1\np/a 1 maybe\n",
                3,
                "neither `user-data` nor `no-user-data`",
            ),
            (
                "orion-index 1\ntotal 1\np/a 1 user-data\np/a 2 user-data\n",
                4,
                "a name twice",
            ),
        ];
        for (text, line, why) in cases {
            assert_eq!(
                Index::parse(text),
                Err(NotAnIndex { line, why }),
                "{text:?}"
            );
        }
    }
}
