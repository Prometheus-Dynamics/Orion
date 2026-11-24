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

/// Who writes: the session, and the principal behind it.
#[derive(Clone, Copy, Debug)]
pub struct Caller<'a> {
    /// The session.
    pub session: u32,
    /// As the audit log names it: `local:<uid>`, `operator:<key>` and so on.
    pub principal: &'a str,
    /// Whether it is root on this board.
    pub root: bool,
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
    /// Session to when it was said quiet, and for how long it had been then.
    quiet: BTreeMap<u32, Told>,
    /// Session to the leases it lost.
    lost: BTreeMap<u32, Vec<Lost>>,
}

/// That a session was quiet: when node logic was told, on its clock, and how long the session
/// had been quiet then, at least (the comm core rounds down).
#[derive(Clone, Copy, Debug)]
struct Told {
    at_ms: u64,
    quiet_ms: u64,
}

impl Told {
    /// How long the session has been quiet at `now_ms`, at least. Node logic's clock counts
    /// whole milliseconds, so `now_ms` one past `at_ms` may be only a hair after the telling:
    /// the milliseconds since count from the one after it, never more than passed.
    const fn quiet_at(self, now_ms: u64) -> u64 {
        self.quiet_ms + now_ms.saturating_sub(self.at_ms).saturating_sub(1)
    }
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
    /// `quiet_ms` is a floor, as the comm core says it, and is kept with `now_ms` rather than
    /// as one instant: a lease is never taken before its holder was quiet long enough.
    pub fn quiet(&mut self, session: u32, quiet_ms: u64, now_ms: u64) {
        if quiet_ms == 0 {
            self.quiet.remove(&session);
        } else {
            let told = Told {
                at_ms: now_ms,
                quiet_ms,
            };
            self.quiet.insert(session, told);
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

    /// Whether `caller` may free the lease of `resource`, which `holder` holds: only the
    /// session that wrote it, a caller whose principal is the holder, or root.
    pub(crate) fn may_free(&self, caller: &Caller<'_>, resource: &str, holder: &str) -> bool {
        caller.root
            || caller.principal == holder
            || self.writers.get(resource) == Some(&caller.session)
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
        // Held, by a lease that cannot be said: still in words, so that a caller that asks
        // again on a bare `busy` (want of room) does not wait this out.
        let unread = || Refused {
            error: Error::Busy,
            words: Some(format!("{resource} is held: its lease cannot be read")),
        };
        let version = store.get(RecordClass::Lease, resource).ok_or_else(unread)?;
        let lease = LeaseRecord::decode(&version.value).map_err(|_| unread())?;
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
        let Some(&told) = self.quiet.get(&holder) else {
            return Err(busy("its holder is answering", words));
        };
        let quiet = told.quiet_at(now_ms);
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

#[cfg(test)]
mod tests {
    use super::*;
    use orion_store::StoreConfig;

    #[test]
    fn a_lease_that_cannot_be_read_is_refused_in_words_as_held() {
        let mut store = Store::new("board".parse().expect("name"), StoreConfig::default());
        let contest = Contest::new(10_000);
        let refused = |store: &Store| {
            contest
                .contest(store, 1, "cam.front", "workload:a", 0, &|_, _| false)
                .expect_err("held")
        };
        let said = Refused {
            error: Error::Busy,
            words: Some("cam.front is held: its lease cannot be read".to_owned()),
        };
        // No lease record at all, and one that is not a lease.
        assert_eq!(refused(&store), said);
        store.put(RecordClass::Lease, "cam.front", &[0xFF; 3], 1_000);
        assert_eq!(refused(&store), said);
    }

    /// Node logic's clock counts whole milliseconds, so a report and a claim one apart may be
    /// as little as a hair apart. The lease is never taken before the holder has been quiet
    /// `quiet_ms`, however the two sit about a millisecond's edge.
    #[test]
    fn a_lease_is_never_taken_before_its_holder_was_quiet_long_enough() {
        let mut store = Store::new("board".parse().expect("name"), StoreConfig::default());
        let mut value = vec![0_u8; 128];
        let len = orion_wire::body::encode_fields(
            &LeaseRecord {
                resource: "cam.front".parse().expect("name"),
                holder: "workload:a",
                expires_ms: 0,
                label: "",
            },
            &mut value,
        )
        .expect("encode");
        store.put(RecordClass::Lease, "cam.front", &value[..len], 1_000);
        let mut contest = Contest::new(1_500);
        contest.wrote("cam.front", 1, false);
        let claim = |contest: &Contest, now_ms| {
            contest.contest(&store, 2, "cam.front", "workload:b", now_ms, &|_, _| false)
        };
        // Told at 1000 ms (1000.999 by the hair) that the holder had been quiet 1000 ms: it
        // went quiet no earlier than 0.999 ms. At 1500 ms (1500.000) that is 1499.001 ms.
        contest.quiet(1, 1_000, 1_000);
        let refused = claim(&contest, 1_500).expect_err("too soon");
        assert_eq!(refused.error, Error::Busy, "{refused:?}");
        // A millisecond on, it has been 1500 ms however the edges sat.
        let taken = claim(&contest, 1_501).expect("quiet long enough");
        assert_eq!((taken.from, taken.by.as_str()), (1, "workload:b"));
        // Told it had been quiet as long as it must, it is taken at once: the claim comes
        // after the telling.
        contest.quiet(1, 1_500, 2_000);
        assert!(claim(&contest, 2_000).is_ok());
    }
}
