//! What this node served at its last good run, so that a lookup in the gap after a restart is
//! told [`Error::Restoring`] and not [`Error::NotFound`].
//!
//! After the comm core restarts, or the whole node does, every program that registered an
//! action or provides a resource connects again and says so again. Until it has, a caller that
//! asks for that action finds nothing, though it was there a moment ago and will be again. The
//! names served are kept in a small file in the state directory, and during a short window after
//! node logic connects, a lookup that misses a name on that list is told `Restoring`: ask again.
//! Past the window, or for a name not on the list, it is `NotFound` as always.
//!
//! On a first boot (a fresh flash, wiped state) there is no list, and nothing is `Restoring`:
//! the node served nothing before, so there is nothing for it to be coming back to. A caller is
//! told `NotFound` at once rather than made to wait out a window for something no program ever
//! offered here. A list that cannot be read is taken as no list, never as a reason not to start.
//!
//! The list is bounded ([`MAX_SERVED`] names) and written off the hot path: looked at once a
//! second at most, and written from a blocking task only when it changed. A name stays on it for
//! [`LINGER_MS`] after its program left, so a node that is being stopped (its programs going
//! first) still lists what it served when it comes back.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Write as _;
use std::io;
use std::path::{Path, PathBuf};

use orion_msg_actions::{Run, Target};
use orion_msg_records::{Get, RecordClass};
use orion_reply::Asked;
use orion_types::Error;
use orion_wire::kind;

/// How many names the list holds at most. Past it, the first in order are kept.
pub(crate) const MAX_SERVED: usize = 1024;

/// How long a name stays on the list after nothing serves it.
pub(crate) const LINGER_MS: u64 = 60_000;

/// The window after node logic connects in which a name on the list is `Restoring`: as long as
/// a provider has to write its resources again before they are marked lost.
pub(crate) const RESTORE_MS: u64 = crate::records::RECLAIM_MS;

const FILE: &str = "served";
const HEADER: &str = "orion served 1";

/// Something a program served here.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum Name {
    /// An action, by its name and what it is on: `node:<node>` or `resource:<resource>`.
    Action { name: String, target: String },
    /// A resource of this node, which a program provided.
    Resource(String),
}

impl Name {
    pub(crate) fn action(name: &str, target: &Target) -> Self {
        Self::Action {
            name: name.to_owned(),
            target: match target {
                Target::Node(node) => format!("node:{node}"),
                Target::Resource(resource) => format!("resource:{resource}"),
            },
        }
    }
}

/// The list, and the window it is used in.
#[derive(Debug)]
pub(crate) struct Served {
    path: PathBuf,
    /// What the file says.
    written: BTreeSet<Name>,
    /// What was served before the restart now being come back from.
    before: BTreeSet<Name>,
    /// Until when, on node logic's clock, a name in `before` is `Restoring`.
    until: Option<u64>,
    /// Names on the list that nothing serves now, since when.
    unserved: BTreeMap<Name, u64>,
    /// When the list was last looked at.
    looked_at: Option<u64>,
}

impl Served {
    /// No list, kept nowhere: until [`Served::load`] gives one.
    pub(crate) const fn none() -> Self {
        Self {
            path: PathBuf::new(),
            written: BTreeSet::new(),
            before: BTreeSet::new(),
            until: None,
            unserved: BTreeMap::new(),
            looked_at: None,
        }
    }

    /// The list kept in `state_dir`, and what to say of it.
    pub(crate) fn load(state_dir: &Path) -> (Self, String) {
        let path = state_dir.join(FILE);
        let (written, said) = match std::fs::read_to_string(&path) {
            Ok(text) => match parse(&text) {
                Some(names) => {
                    let said = format!("{} names served before", names.len());
                    (names, said)
                }
                None => (
                    BTreeSet::new(),
                    format!(
                        "{} could not be read: taken as a first boot, nothing is restoring",
                        path.display()
                    ),
                ),
            },
            Err(error) if error.kind() == io::ErrorKind::NotFound => (
                BTreeSet::new(),
                "nothing was served before: a first boot, nothing is restoring".to_owned(),
            ),
            Err(error) => (
                BTreeSet::new(),
                format!(
                    "{} could not be read ({error}): taken as a first boot",
                    path.display()
                ),
            ),
        };
        (
            Self {
                path,
                written,
                ..Self::none()
            },
            said,
        )
    }

    /// Node logic connected to a comm core, which knows nothing of what was served: what is on
    /// the list and what was served a moment ago (`now`) is restoring until `now_ms` and
    /// [`RESTORE_MS`].
    pub(crate) fn restart(&mut self, now: BTreeSet<Name>, now_ms: u64) {
        self.before = self.written.iter().cloned().chain(now).collect();
        self.until = (!self.before.is_empty()).then_some(now_ms + RESTORE_MS);
    }

    /// Whether a lookup of `name` that found nothing is to be told `Restoring`.
    pub(crate) fn restoring(&self, name: &Name, now_ms: u64) -> bool {
        self.until.is_some_and(|until| now_ms < until) && self.before.contains(name)
    }

    /// Whether the window is open at `now_ms`.
    pub(crate) fn in_window(&self, now_ms: u64) -> bool {
        self.until.is_some_and(|until| now_ms < until)
    }

    /// Whether the list is to be looked at again: once a second, and never in the window,
    /// when what is served is still coming back.
    pub(crate) fn due(&self, now_ms: u64) -> bool {
        !self.in_window(now_ms) && self.looked_at.is_none_or(|at| now_ms >= at + 1_000)
    }

    /// Takes in what is served `now`, and returns the list's new text if it changed.
    pub(crate) fn update(&mut self, now: BTreeSet<Name>, now_ms: u64) -> Option<String> {
        self.looked_at = Some(now_ms);
        let now: BTreeSet<Name> = now.into_iter().take(MAX_SERVED).collect();
        self.unserved.retain(|name, _| !now.contains(name));
        for name in &self.written {
            if !now.contains(name) {
                self.unserved.entry(name.clone()).or_insert(now_ms);
            }
        }
        let lingering = self
            .unserved
            .iter()
            .filter(|(_, since)| now_ms < **since + LINGER_MS)
            .map(|(name, _)| name.clone());
        let next: BTreeSet<Name> = now
            .iter()
            .cloned()
            .chain(lingering)
            .take(MAX_SERVED)
            .collect();
        self.unserved.retain(|name, _| next.contains(name));
        if next == self.written {
            return None;
        }
        self.written = next;
        Some(text(&self.written))
    }

    /// Where the list is kept.
    pub(crate) fn path(&self) -> &Path {
        &self.path
    }
}

impl crate::logic::Logic {
    /// What programs serve here now.
    pub(crate) fn serving(&self) -> BTreeSet<Name> {
        let actions = self
            .actions
            .actions()
            .map(|(name, target)| Name::action(name.as_str(), target));
        let resources = self
            .records
            .resources()
            .providing()
            .map(|resource| Name::Resource(resource.to_owned()));
        actions.chain(resources).collect()
    }

    /// `Restoring` for a read that would find nothing, in the window: a records read of
    /// exactly one resource that was provided here before the restart and has no record now
    /// (the store's last save was older than it). A resource that has a record says
    /// `ResourceState::Restoring` in it, which is the answer; a read of many is answered as
    /// it is.
    pub(crate) fn restoring_read(&self, asked: &Asked<'_>) -> Option<Error> {
        if asked.kind != kind::GET || !self.served.in_window(self.now_ms()) {
            return None;
        }
        let get: Get<'_> = asked.body().ok()?;
        let name = Name::Resource(get.prefix.to_owned());
        (get.class == RecordClass::Resource
            && self
                .records
                .store()
                .get(RecordClass::Resource, get.prefix)
                .is_none()
            && self.served.restoring(&name, self.now_ms()))
        .then_some(Error::Restoring)
    }

    /// `Restoring` in place of `NotFound` for a run of an action served before the restart.
    pub(crate) fn restoring_run(
        &self,
        asked: &Asked<'_>,
        answered: Result<(), Error>,
    ) -> Result<(), Error> {
        match answered {
            Err(Error::NotFound) if asked.kind == kind::ACTION_RUN => {
                let restoring = asked.body::<Run<'_>>().is_ok_and(|run| {
                    self.served
                        .restoring(&Name::action(run.name.as_str(), &run.target), self.now_ms())
                });
                Err(if restoring {
                    Error::Restoring
                } else {
                    Error::NotFound
                })
            }
            answered => answered,
        }
    }

    /// The list's new text, if it is time to look and it changed.
    pub(crate) fn served_due(&mut self) -> Option<(PathBuf, String)> {
        let now = self.now_ms();
        if !self.served.due(now) || self.served.path().as_os_str().is_empty() {
            return None;
        }
        let serving = self.serving();
        let text = self.served.update(serving, now)?;
        Some((self.served.path().to_owned(), text))
    }
}

/// Writes the list's text to `path`, whole or not at all.
pub(crate) fn write(path: &Path, text: &str) -> io::Result<()> {
    let partial = path.with_extension("partial");
    std::fs::write(&partial, text)?;
    std::fs::rename(&partial, path)
}

fn text(names: &BTreeSet<Name>) -> String {
    let mut text = String::from(HEADER);
    text.push('\n');
    for name in names {
        let _ = match name {
            Name::Action { name, target } => writeln!(text, "action {name} {target}"),
            Name::Resource(resource) => writeln!(text, "resource {resource}"),
        };
    }
    text
}

/// The names in a list's text, or `None` if any of it is not a list.
fn parse(text: &str) -> Option<BTreeSet<Name>> {
    let mut lines = text.lines();
    if lines.next()? != HEADER {
        return None;
    }
    let mut names = BTreeSet::new();
    for line in lines {
        let words: Vec<&str> = line.split(' ').collect();
        let name = match words.as_slice() {
            ["action", name, target]
                if target.starts_with("node:") || target.starts_with("resource:") =>
            {
                Name::Action {
                    name: (*name).to_owned(),
                    target: (*target).to_owned(),
                }
            }
            ["resource", resource] if !resource.is_empty() => {
                Name::Resource((*resource).to_owned())
            }
            _ => return None,
        };
        if names.len() == MAX_SERVED {
            return None;
        }
        names.insert(name);
    }
    Some(names)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn dir(name: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!("orion-served-{name}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).expect("directory");
        dir
    }

    fn run(name: &str) -> Name {
        Name::Action {
            name: name.to_owned(),
            target: "resource:arm".to_owned(),
        }
    }

    fn set(names: &[Name]) -> BTreeSet<Name> {
        names.iter().cloned().collect()
    }

    #[test]
    fn what_was_served_is_restoring_for_the_window_after_a_restart_and_no_longer() {
        let dir = dir("window");
        let (mut served, _) = Served::load(&dir);
        let text = served
            .update(set(&[run("arm.home"), Name::Resource("arm".into())]), 0)
            .expect("changed");
        write(served.path(), &text).expect("written");

        let (mut again, said) = Served::load(&dir);
        assert_eq!(said, "2 names served before");
        again.restart(BTreeSet::new(), 10_000);
        assert!(again.restoring(&run("arm.home"), 10_000));
        assert!(again.restoring(&Name::Resource("arm".into()), 10_000 + RESTORE_MS - 1));
        assert!(!again.restoring(&run("arm.home"), 10_000 + RESTORE_MS));
        // Never served: not restoring, even in the window.
        assert!(!again.restoring(&run("arm.park"), 10_000));
        // In the window the list is not looked at, so it cannot lose what is coming back.
        assert!(!again.due(10_000 + 1));
        assert!(again.due(10_000 + RESTORE_MS));
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn what_was_served_a_moment_ago_is_restoring_though_the_file_lags() {
        let (mut served, _) = Served::load(&dir("moment"));
        served.restart(set(&[run("arm.home")]), 0);
        assert!(served.restoring(&run("arm.home"), 1));
    }

    #[test]
    fn a_first_boot_and_a_list_that_cannot_be_read_restore_nothing() {
        let dir = dir("first");
        let (mut served, said) = Served::load(&dir);
        assert!(said.contains("first boot"), "{said}");
        served.restart(BTreeSet::new(), 0);
        assert!(!served.in_window(0));

        for broken in [
            "not a list\naction a node:b\n",
            "orion served 1\naction a\n",
            "orion served 1\naction a elsewhere:b\n",
        ] {
            std::fs::write(dir.join(FILE), broken).expect("written");
            let (mut served, said) = Served::load(&dir);
            assert!(said.contains("could not be read"), "{said}");
            served.restart(BTreeSet::new(), 0);
            assert!(!served.restoring(&run("a"), 0));
        }
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn a_name_stays_on_the_list_for_a_while_after_its_program_left() {
        let (mut served, _) = Served::load(&dir("linger"));
        assert!(served.update(set(&[run("a"), run("b")]), 0).is_some());
        // b's program leaves, as a node being stopped sees: b is still listed.
        assert_eq!(served.update(set(&[run("a")]), 1_000), None);
        assert_eq!(served.update(set(&[run("a")]), 1_000 + LINGER_MS - 1), None);
        let text = served
            .update(set(&[run("a")]), 1_000 + LINGER_MS)
            .expect("b is gone now");
        assert_eq!(text, "orion served 1\naction a resource:arm\n");
        // Served again: back on the list, and unserved again starts over.
        assert!(served.update(set(&[run("a"), run("b")]), 100_000).is_some());
        assert_eq!(served.update(set(&[run("a")]), 100_001), None);
    }

    #[test]
    fn the_list_is_bounded_and_read_back_as_written() {
        let many: BTreeSet<Name> = (0..MAX_SERVED + 10)
            .map(|i| run(&format!("a{i:05}")))
            .collect();
        let (mut served, _) = Served::load(&dir("bounded"));
        let text = served.update(many, 0).expect("changed");
        let read = parse(&text).expect("a list");
        assert_eq!(read.len(), MAX_SERVED);
        assert_eq!(read, served.written);
        // Names that linger count toward the bound like the ones served now.
        let others: BTreeSet<Name> = (0..MAX_SERVED).map(|i| run(&format!("b{i:05}"))).collect();
        let text = served.update(others, 1).expect("changed");
        assert_eq!(parse(&text).expect("a list").len(), MAX_SERVED);
        // One more than the bound is not a list this build wrote.
        let mut over = text.clone();
        over.push_str("resource extra\n");
        assert_eq!(parse(&over), None);
    }
}
