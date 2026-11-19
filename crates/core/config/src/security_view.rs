//! The board's one security switch: a record its owner keeps, which every reader reads the
//! same way.
//!
//! The record is two lines, `on` or `off` and then `generation <n>`:
//!
//! ```text
//! on
//! generation 4
//! ```
//!
//! - No record: **off**, the default of a board nobody has secured.
//! - A record that cannot be read, or is not those two lines: **on**. A board is never left
//!   open because its record is damaged; the words say which file and what is wrong.

use std::fmt;
use std::io;
use std::path::{Path, PathBuf};

use orion_check::Security;

/// Where the record is. Its owner, the device library's record crate, will define this path;
/// when that crate is a dependency here, this becomes its constant, so every reader on a board
/// reads the one file.
pub const SECURITY_VIEW: &str = "/etc/board/security";

/// The switch, as one read of the record found it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SecurityView {
    /// On or off.
    pub security: Security,
    /// The record's generation, when it was read whole.
    pub generation: Option<u64>,
    /// The file that was read.
    pub path: PathBuf,
    /// Why the switch is on although the record does not say so: it could not be read, or
    /// is not what a record is. `None` when the record was read, or is not there.
    pub fault: Option<String>,
}

impl SecurityView {
    /// Reads the record at `path`, failing secure.
    #[must_use]
    pub fn read(path: &Path) -> Self {
        let view = |security, generation, fault| Self {
            security,
            generation,
            path: path.to_owned(),
            fault,
        };
        match std::fs::read_to_string(path) {
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                view(Security::Off, None, None)
            }
            Err(error) => view(
                Security::On,
                None,
                Some(format!("it cannot be read: {error}")),
            ),
            Ok(text) => match parse(&text) {
                Ok((security, generation)) => view(security, Some(generation), None),
                Err(fault) => view(Security::On, None, Some(fault)),
            },
        }
    }
}

/// `on` or `off`, then `generation <n>`, and nothing else.
fn parse(text: &str) -> Result<(Security, u64), String> {
    let mut lines = text.lines().map(str::trim).filter(|line| !line.is_empty());
    let security = match lines.next() {
        Some("on") => Security::On,
        Some("off") => Security::Off,
        Some(other) => return Err(format!("its first line is {other:?}, not on or off")),
        None => return Err("it is empty".to_owned()),
    };
    let generation = match lines.next() {
        Some(line) => line
            .strip_prefix("generation ")
            .and_then(|number| number.trim().parse::<u64>().ok())
            .ok_or_else(|| format!("its second line is {line:?}, not generation <n>"))?,
        None => return Err("it has no generation".to_owned()),
    };
    if let Some(more) = lines.next() {
        return Err(format!("it goes on after the generation: {more:?}"));
    }
    Ok((security, generation))
}

impl fmt::Display for SecurityView {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let switch = match self.security {
            Security::On => "on",
            Security::Off => "off",
        };
        let path = self.path.display();
        match (&self.fault, self.generation) {
            (Some(fault), _) => write!(f, "security is ON: {path} is damaged ({fault})"),
            (None, Some(generation)) => {
                write!(f, "security is {switch}: {path}, generation {generation}")
            }
            (None, None) => write!(f, "security is off: there is no {path}"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use orion_scratch::Scratch;

    fn read(text: Option<&str>) -> SecurityView {
        let dir = Scratch::new(&format!("security-view-{}", text.map_or(0, str::len)));
        let path = dir.join("security");
        if let Some(text) = text {
            std::fs::write(&path, text).expect("write");
        }
        SecurityView::read(&path)
    }

    #[test]
    fn the_record_says_on_or_off_none_is_off_and_a_damaged_one_is_on() {
        let on = read(Some("on\ngeneration 4\n"));
        assert_eq!(
            (on.security, on.generation, on.fault),
            (Security::On, Some(4), None)
        );
        let off = read(Some("off\ngeneration 7"));
        assert_eq!((off.security, off.generation), (Security::Off, Some(7)));
        assert!(off.to_string().ends_with("security, generation 7"), "{off}");

        let none = read(None);
        assert_eq!(
            (none.security, none.fault.as_deref()),
            (Security::Off, None)
        );
        assert!(
            none.to_string()
                .starts_with("security is off: there is no ")
        );

        for (text, says) in [
            ("", "it is empty"),
            ("maybe\ngeneration 1", "\"maybe\""),
            ("off", "no generation"),
            ("off\ngen 2", "not generation"),
            ("off\ngeneration -1", "not generation"),
            ("off\ngeneration 2\noff", "goes on"),
        ] {
            let damaged = read(Some(text));
            assert_eq!(damaged.security, Security::On, "{text:?}");
            let fault = damaged.fault.clone().expect("fault");
            assert!(fault.contains(says), "{text:?}: {fault}");
            assert!(
                damaged.to_string().starts_with("security is ON: "),
                "{damaged}"
            );
        }
    }

    #[test]
    fn a_record_that_cannot_be_read_is_on() {
        // A directory where the record should be: it is there, and it cannot be read.
        let dir = Scratch::new("security-dir");
        let view = SecurityView::read(&dir);
        assert_eq!(view.security, Security::On);
        assert!(view.fault.expect("fault").starts_with("it cannot be read"));
    }
}
