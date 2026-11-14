//! Who may take a lease another holds.
//!
//! A lease with no end is never freed by time. It is freed by its holder, or by its resource
//! going. But a program on the board that holds one may stop answering: stopped in a debugger,
//! or wedged. Its socket is open, so the comm core keeps its session and says it is quiet.
//! Then the lease can be **contested**:
//!
//! - A claim for it succeeds once its holder's session has been quiet for `leases.quiet_ms`,
//!   unless a run of one of the resource's actions that the holder's session asked for is still
//!   under way. Work in flight is not quiet.
//! - Nothing is lost without a claimant, however long the holder is quiet.
//! - The old holder is refused [`Error::LeaseLost`] for whatever it asks under the lease while
//!   the new holder keeps it, with who took it: writing or freeing the lease, and running one of
//!   the resource's actions.
//! - A claim refused as busy says who holds the lease and since when, and why it could not be
//!   taken.
//!
//! Only a program on the board's own socket is ever kept while quiet; a session from anywhere
//! else ends when it goes quiet, as before.

use std::collections::BTreeMap;
use std::fmt::Write as _;

use orion_msg_records::{LeaseRecord, RecordClass};
use orion_store::Store;
use orion_types::Error;
use orion_wire::Decode;

/// How long a holder must be quiet before its lease can be taken, unless the node says
/// otherwise.
pub const QUIET_MS: u64 = 10_000;

/// Why a write was refused, with words for the caller when there are more than the code says.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Refused {
    /// The code.
    pub error: Error,
    /// What the caller is told besides.
    pub words: Option<String>,
}

impl From<Error> for Refused {
    fn from(error: Error) -> Self {
        Self { error, words: None }
    }
}

/// A lease a write takes from a quiet holder, to be made so once the write is made.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Taken {
    /// The resource.
    pub resource: String,
    /// The session that held it.
    pub from: u32,
    /// Who holds it now.
    pub by: String,
}

/// A lease a session lost while it was quiet.
#[derive(Clone, Debug)]
struct Lost {
    resource: String,
    taker: String,
}

/// What node logic knows of leases beyond the store: who wrote each, and who is quiet.
#[derive(Debug)]
pub struct Contest {
    /// `leases.quiet_ms`.
    quiet_ms: u64,
    /// Resource to the session that wrote its lease.
    writers: BTreeMap<String, u32>,
    /// Session to when it went quiet, on node logic's clock.
    quiet: BTreeMap<u32, u64>,
    /// Session to the leases it lost.
    lost: BTreeMap<u32, Vec<Lost>>,
}

/// The words for a lease lost to `taker`.
fn lost_words(taker: &str) -> String {
    format!("lease lost while quiet: taken by {taker}")
}

impl Contest {
    /// Leases taken from a holder quiet for `quiet_ms`.
    #[must_use]
    pub const fn new(quiet_ms: u64) -> Self {
        Self {
            quiet_ms,
            writers: BTreeMap::new(),
            quiet: BTreeMap::new(),
            lost: BTreeMap::new(),
        }
    }

    /// How long a holder must be quiet before its lease can be taken.
    pub const fn set_quiet_ms(&mut self, quiet_ms: u64) {
        self.quiet_ms = quiet_ms;
    }

    /// `session` went quiet `quiet_ms` before `now_ms`, or with `quiet_ms` zero is heard again.
    pub fn quiet(&mut self, session: u32, quiet_ms: u64, now_ms: u64) {
        if quiet_ms == 0 {
            self.quiet.remove(&session);
        } else {
            self.quiet.insert(session, now_ms.saturating_sub(quiet_ms));
        }
    }

    /// `session` wrote the lease of `resource`, or freed it.
    pub(crate) fn wrote(&mut self, resource: &str, session: u32, freed: bool) {
        if freed {
            self.writers.remove(resource);
        } else {
            self.writers.insert(resource.to_owned(), session);
        }
    }

    /// `taken` was made: the lease is its new holder's, and the old one is told when it asks.
    pub fn taken(&mut self, taken: Taken, session: u32) {
        self.writers.insert(taken.resource.clone(), session);
        let lost = self.lost.entry(taken.from).or_default();
        lost.retain(|lost| lost.resource != taken.resource);
        lost.push(Lost {
            resource: taken.resource,
            taker: taken.by,
        });
    }

    /// `session` is gone.
    pub(crate) fn gone(&mut self, session: u32) {
        self.quiet.remove(&session);
        self.lost.remove(&session);
        self.writers.retain(|_, writer| *writer != session);
    }

    /// Node logic's sessions are gone.
    pub(crate) fn forget_sessions(&mut self) {
        self.writers.clear();
        self.quiet.clear();
        self.lost.clear();
    }

    /// The words for `session`, if the lease of `resource` was taken from it and its taker
    /// still holds it.
    pub(crate) fn lost_here(
        &self,
        store: &Store,
        session: u32,
        resource: &str,
        wall_ms: u64,
    ) -> Option<String> {
        let lost = self.lost.get(&session)?;
        let lost = lost.iter().find(|lost| lost.resource == resource)?;
        (crate::holder(store, resource, wall_ms).as_deref() == Some(lost.taker.as_str()))
            .then(|| lost_words(&lost.taker))
    }

    /// The resources whose leases `session` lost and whose takers still hold them.
    pub(crate) fn lost_by(
        &self,
        store: &Store,
        session: u32,
        wall_ms: u64,
    ) -> Vec<(String, String)> {
        self.lost.get(&session).map_or_else(Vec::new, |lost| {
            lost.iter()
                .filter(|lost| {
                    crate::holder(store, &lost.resource, wall_ms).as_deref()
                        == Some(lost.taker.as_str())
                })
                .map(|lost| (lost.resource.clone(), lost_words(&lost.taker)))
                .collect()
        })
    }

    /// Whether a claim by `session` for `resource` as `by`, refused because another holds
    /// it, takes it from a quiet holder. If not, why, in words.
    ///
    /// `in_flight` says whether a session has a run of one of the given actions under way.
    pub(crate) fn contest(
        &self,
        store: &Store,
        session: u32,
        resource: &str,
        by: &str,
        now_ms: u64,
        in_flight: &dyn Fn(u32, &[String]) -> bool,
    ) -> Result<Taken, Refused> {
        let version = store.get(RecordClass::Lease, resource).ok_or(Error::Busy)?;
        let lease = LeaseRecord::decode(&version.value).map_err(|_| Error::Busy)?;
        let mut words = format!(
            "{resource} is held by {} since {} ms",
            lease.holder, version.stamp.physical_ms
        );
        let busy = |why: &str, mut words: String| {
            let _ = write!(words, ": {why}");
            Refused {
                error: Error::Busy,
                words: Some(words),
            }
        };
        if lease.expires_ms != 0 {
            return Err(busy("its lease ends on its own", words));
        }
        let Some(&holder) = self
            .writers
            .get(resource)
            .filter(|holder| **holder != session)
        else {
            return Err(busy("its holder is not a session here", words));
        };
        let Some(&since) = self.quiet.get(&holder) else {
            return Err(busy("its holder is answering", words));
        };
        let quiet = now_ms.saturating_sub(since);
        if quiet < self.quiet_ms {
            let _ = write!(words, ", quiet {quiet} ms");
            return Err(busy(
                &format!("a lease is taken only after {} ms quiet", self.quiet_ms),
                words,
            ));
        }
        let actions = crate::find(store, resource, version.stamp.physical_ms)
            .map(|found| found.actions)
            .unwrap_or_default();
        if in_flight(holder, &actions) {
            return Err(busy("a run of its action is under way", words));
        }
        Ok(Taken {
            resource: resource.to_owned(),
            from: holder,
            by: by.to_owned(),
        })
    }
}
