//! Node logic's actions on the roots, registered as any program's are, so the relay checks
//! each run against its policy and the operator's grants.
//!
//! Their arguments are text, words split by spaces, and so is what `files.list` gives:
//!
//! | Action | Arguments | Does |
//! |---|---|---|
//! | `files.list` | none | the roots: `root rw app` a line |
//! | `files.list` | `roots/<root>/<dir> [<after>]` | up to [`PAGE`] entries of `<dir>` by name, in at most [`PAGE_BYTES`], after `<after>` if given: `<kind> <size> <mtime> <name>` a line, `kind` one of `file`, `dir`, `link`, `other`, `mtime` in seconds since 1970, `name` as it is carried (escaped: [`orion_types::Escaped`]) and, when that is not how it shows, the rest of the line what it shows as; a line begins `long` for an entry whose name, with its directory's, is too long to be asked for; then `more` if there are more |
//! | `files.remove` | `roots/<root>/<path>` | removes a file, a link or an empty directory |
//! | `files.mkdir` | `roots/<root>/<path>` | makes one directory, in one that is there |
//! | `files.rename` | `roots/<root>/<from> roots/<root>/<to> [replace]` | renames within a root; what is at `<to>` is replaced only if `replace` is said |
//!
//! A run that cannot be done ends `Rejected` (the arguments, a read-only root) or `Failed`
//! (what the file system said), with the words in its detail.

use std::cmp::Ordering;
use std::collections::BinaryHeap;
use std::fmt::Write as _;

use orion_check::{Admits, Policy};
use orion_connect::ClientError;
use orion_msg_actions::{ActionResult, Claim, Outcome, Register, Target};
use orion_types::{Escaped, FileName, NodeName};
use orion_wire::Bytes;
use rustix::fs::{AtFlags, Mode, OFlags, RenameFlags, mkdirat, renameat_with, unlinkat};
use rustix::io::Errno;

use crate::path::{self, At, OWN, PREFIX};
use crate::walk::{self, Kind};
use crate::{Link, Roots};

/// Lists a root's directory.
pub const LIST: &str = "files.list";
/// Removes a file or an empty directory.
pub const REMOVE: &str = "files.remove";
/// Makes a directory.
pub const MKDIR: &str = "files.mkdir";
/// Renames within a root.
pub const RENAME: &str = "files.rename";

/// The most entries `files.list` gives at once.
pub const PAGE: usize = 128;

/// The most bytes of lines `files.list` gives at once: a page of long names is shorter, so it
/// fits a client's frame and is all node logic holds for it.
pub const PAGE_BYTES: usize = 16 * 1024;

/// Whether `name` is one of these actions.
#[must_use]
pub fn ours(name: &str) -> bool {
    [LIST, REMOVE, MKDIR, RENAME].contains(&name)
}

/// The registrations of the actions on node `me`: programs on the board and operators with
/// a grant may run them.
#[must_use]
pub fn registers(me: &NodeName) -> Vec<Register<'static>> {
    [
        (LIST, "lists the roots, or a directory in one"),
        (REMOVE, "removes a file or an empty directory in a root"),
        (MKDIR, "makes a directory in a root"),
        (RENAME, "renames a file or a directory within a root"),
    ]
    .into_iter()
    .filter_map(|(name, description)| {
        Some(Register {
            name: name.parse().ok()?,
            target: Target::Node(me.clone()),
            policy: Policy::new(Admits::LOCAL.and(Admits::OPERATOR)),
            description,
            keep_ms: 0,
        })
    })
    .collect()
}

/// How a run ended: what it gives, or how it did not.
type Ended = Result<String, (Outcome, String)>;

fn rejected<T>(why: impl Into<String>) -> Result<T, (Outcome, String)> {
    Err((Outcome::Rejected, why.into()))
}

fn failed(error: Errno) -> (Outcome, String) {
    (Outcome::Failed, path::words(error))
}

/// One entry of a listing, ordered by its name.
struct Listed {
    name: String,
    line: String,
}

impl PartialEq for Listed {
    fn eq(&self, other: &Self) -> bool {
        self.name == other.name
    }
}
impl Eq for Listed {}
impl PartialOrd for Listed {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for Listed {
    fn cmp(&self, other: &Self) -> Ordering {
        self.name.cmp(&other.name)
    }
}

impl Roots {
    /// Runs one of the actions on the roots that the action list handed over.
    ///
    /// # Errors
    ///
    /// If the connection broke.
    pub fn claim(&mut self, claim: &Claim<'_>, link: &mut impl Link) -> Result<(), ClientError> {
        let ended = match std::str::from_utf8(claim.args.0) {
            Err(_) => rejected("the arguments are not text"),
            Ok(args) => {
                let words: Vec<&str> = args.split_whitespace().collect();
                match claim.name.as_str() {
                    LIST => self.list(&words),
                    REMOVE => self.change(&words, |dir, name| {
                        let name = path::raw(name);
                        match unlinkat(dir, &name, AtFlags::empty()) {
                            Err(Errno::ISDIR) => unlinkat(dir, &name, AtFlags::REMOVEDIR),
                            done => done,
                        }
                    }),
                    // As a file is made: what the process's umask leaves, so a root shared by
                    // a group is shared in what is made there too.
                    MKDIR => self.change(&words, |dir, name| {
                        mkdirat(dir, path::raw(name), Mode::from_raw_mode(0o777))
                    }),
                    RENAME => self.rename(&words),
                    _ => rejected("not an action on the roots"),
                }
            }
        };
        let (outcome, output, detail) = match &ended {
            Ok(output) => (Outcome::Succeeded, output.as_str(), ""),
            Err((outcome, why)) => (*outcome, "", why.as_str()),
        };
        link.send(&ActionResult {
            call: claim.call,
            outcome,
            output: Bytes(output.as_bytes()),
            detail,
            code: 0,
            instance: 0,
            run: 0,
            action: "",
        })
    }

    fn find<'a>(&'a self, name: &'a str) -> Result<At<'a>, (Outcome, String)> {
        path::find(&self.roots, name).map_err(|why| (Outcome::Rejected, why))
    }

    fn list(&self, words: &[&str]) -> Ended {
        let (name, after) = match words {
            [] => return Ok(self.list_roots()),
            [name] if name.trim_end_matches('/') == "roots" => return Ok(self.list_roots()),
            [name] => (*name, None),
            [name, after] => (*name, Some(*after)),
            _ => return rejected("files.list takes a directory, and a name to list after"),
        };
        let at = self.find(name)?;
        let root = path::open_root(at.root).map_err(failed)?;
        let dir =
            path::beneath(&root, at.path, OFlags::RDONLY | OFlags::DIRECTORY).map_err(failed)?;
        // The first entries by name after `after`, and no more than a page of them held.
        let mut page: BinaryHeap<Listed> = BinaryHeap::new();
        let (mut more, mut bytes) = (false, 0_usize);
        let dir_name = name.trim_end_matches('/');
        walk::read(&dir, |_, raw, kind, stat| {
            let name = Escaped(raw).to_string();
            if (at.path.is_empty() && name == OWN) || after.is_some_and(|after| *name <= *after) {
                return;
            }
            if page.len() == PAGE {
                if page.peek().is_some_and(|last| name < last.name) {
                    if let Some(last) = page.pop() {
                        bytes -= last.line.len();
                    }
                } else {
                    more = true;
                    return;
                }
                more = true;
            }
            let size = if kind == Kind::File { stat.st_size } else { 0 };
            // Too long a name, with its directory's, for any request to carry: listed, and
            // said to be out of reach.
            let reached = FileName::new(&format!("{dir_name}/{name}")).is_ok();
            let mut line = String::new();
            if !reached {
                line.push_str("long ");
            }
            let _ = write!(line, "{} {size} {} {name}", kind.word(), stat.st_mtime);
            if name.as_bytes() != raw {
                // What it shows as, when that is not how it is carried.
                line.push(' ');
                line.extend(String::from_utf8_lossy(raw).chars().map(|c| {
                    if c.is_control() {
                        char::REPLACEMENT_CHARACTER
                    } else {
                        c
                    }
                }));
            }
            line.push('\n');
            bytes += line.len();
            page.push(Listed { name, line });
            // Over its bytes, the page gives up its last names by order, until it fits.
            while bytes > PAGE_BYTES && page.len() > 1 {
                if let Some(last) = page.pop() {
                    bytes -= last.line.len();
                }
                more = true;
            }
        })
        .map_err(failed)?;
        let mut text = String::new();
        for listed in page.into_sorted_vec() {
            text.push_str(&listed.line);
        }
        if more {
            text.push_str("more\n");
        }
        Ok(text)
    }

    fn list_roots(&self) -> String {
        let mut text = String::new();
        for root in &self.roots {
            let mode = if root.writable { "rw" } else { "ro" };
            let _ = writeln!(text, "root {mode} {}", root.name);
        }
        text
    }

    /// What `name` names, in a root that takes changes: the directory that holds it, and its
    /// last part.
    fn writable<'a>(
        &'a self,
        name: &'a str,
    ) -> Result<(At<'a>, &'a str, &'a str), (Outcome, String)> {
        let at = self.find(name)?;
        if !at.root.writable {
            return rejected("the root is read-only");
        }
        let Some((parent, last)) = at.split() else {
            return rejected("a root is not changed: only what is in it");
        };
        Ok((at, parent, last))
    }

    fn change(
        &self,
        words: &[&str],
        how: impl FnOnce(&std::os::fd::OwnedFd, &str) -> Result<(), Errno>,
    ) -> Ended {
        let [name] = words else {
            return rejected(format!("one name under {PREFIX}, and nothing else"));
        };
        let (at, parent, last) = self.writable(name)?;
        let root = path::open_root(at.root).map_err(failed)?;
        let dir = path::directory(&root, parent).map_err(failed)?;
        how(&dir, last).map_err(failed)?;
        Ok(String::new())
    }

    fn rename(&self, words: &[&str]) -> Ended {
        let (from, to, replace) = match words {
            [from, to] => (*from, *to, false),
            [from, to, "replace"] => (*from, *to, true),
            _ => return rejected("files.rename takes a name, a new name, and replace if it may"),
        };
        let (at, from_parent, from_last) = self.writable(from)?;
        let (into, to_parent, to_last) = self.writable(to)?;
        if at.root.name != into.root.name {
            return rejected("a file is renamed only within its root");
        }
        let root = path::open_root(at.root).map_err(failed)?;
        let (old, new) = (
            path::directory(&root, from_parent).map_err(failed)?,
            path::directory(&root, to_parent).map_err(failed)?,
        );
        let flags = if replace {
            RenameFlags::empty()
        } else {
            RenameFlags::NOREPLACE
        };
        renameat_with(&old, path::raw(from_last), &new, path::raw(to_last), flags)
            .map_err(failed)?;
        Ok(String::new())
    }
}
