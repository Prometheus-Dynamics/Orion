//! Resources.
//!
//! A resource is a thing that exists: a camera, a fan, a motor controller. Orion does not know
//! what kinds there are. A provider, any program on the board, writes a record for each thing
//! it offers: its name, its kind in the provider's words, the node it is on, whether it can
//! be used, and what it offers: labels, topics, and actions. Records are the store's (E1) and
//! reach every node (E8). This crate is the rules that make a record a resource:
//!
//! - **A resource has one provider.** The session that wrote it is the one that may write it
//!   again or take it away. Another program is refused.
//! - **A resource is where its provider is.** A program writes resources for its own node.
//! - **When its provider goes, a resource is `Lost`**, not gone: whoever was using it sees
//!   why. When the provider is back and writes it again, it is whatever the provider says.
//! - **A binding is a lease.** Whoever is to use a resource alone, a workload above all,
//!   holds it: a lease record under the resource's name, with the holder and when the lease
//!   ends. A resource has one holder at a time. A lease that has ended holds nothing.
//! - **A lease with no end can be contested** when its holder is quiet: see [`Contest`].

mod contest;

use std::collections::BTreeMap;

use orion_msg_records::{
    LeaseRecord, RecordClass, RecordEntry, ResourceDescription, ResourceRecord, ResourceState,
};
use orion_store::{Record, Store};
use orion_types::{Error, HlcTimestamp, NodeName, ResourceName};
use orion_wire::body::encode_fields;
use orion_wire::{Bytes, Decode};

pub use contest::{Caller, Contest, QUIET_MS, Refused, Taken};

/// A resource, as it is listed.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Listed {
    /// Its name.
    pub name: String,
    /// Its kind, in its provider's words.
    pub kind: String,
    /// The node it is on.
    pub node: NodeName,
    /// Whether it can be used.
    pub state: ResourceState,
    /// Its tags: whatever its provider says about it.
    pub labels: Vec<(String, String)>,
    /// Its topics, by full name.
    pub topics: Vec<String>,
    /// Its actions, by full name.
    pub actions: Vec<String>,
    /// Who holds it, if anyone does now.
    pub holder: Option<String>,
}

/// The resources of one node: who provides which.
#[derive(Debug)]
pub struct Resources {
    me: NodeName,
    /// Resource name to the session that provides it.
    providers: BTreeMap<String, u32>,
    /// Who wrote which lease, who is quiet, and what was taken from whom.
    contest: Contest,
    /// Whether a lease's holder is its writer's principal (security on), not what it says.
    bind_holders: bool,
}

/// Who holds `resource` at `wall_ms`, by its lease.
#[must_use]
pub fn holder(store: &Store, resource: &str, wall_ms: u64) -> Option<String> {
    let version = store.get(RecordClass::Lease, resource)?;
    let lease = LeaseRecord::decode(&version.value).ok()?;
    (lease.expires_ms == 0 || lease.expires_ms > wall_ms).then(|| lease.holder.to_owned())
}

fn listed(store: &Store, value: &[u8], wall_ms: u64) -> Option<Listed> {
    let resource = ResourceRecord::decode(value).ok()?;
    let mut listed = Listed {
        name: resource.name.to_string(),
        kind: resource.kind.to_owned(),
        node: resource.node.clone(),
        state: resource.state,
        labels: Vec::new(),
        topics: Vec::new(),
        actions: Vec::new(),
        holder: holder(store, resource.name.as_str(), wall_ms),
    };
    // A provider that wrote nothing this build reads about what it offers is still listed.
    if let Ok(description) = ResourceDescription::decode(resource.attributes.0) {
        listed.labels = description
            .labels
            .iter()
            .flatten()
            .map(|label| (label.key.to_owned(), label.value.to_owned()))
            .collect();
        listed.topics = description
            .topics
            .iter()
            .flatten()
            .map(|offered| offered.name.to_owned())
            .collect();
        listed.actions = description
            .actions
            .iter()
            .flatten()
            .map(|offered| offered.name.to_owned())
            .collect();
    }
    Some(listed)
}

/// Every resource whose name starts with `prefix`, on whatever node, by name.
#[must_use]
pub fn list(store: &Store, prefix: &str, wall_ms: u64) -> Vec<Listed> {
    store
        .list(RecordClass::Resource, prefix)
        .filter_map(|record| listed(store, &record.version.value, wall_ms))
        .collect()
}

/// One resource.
#[must_use]
pub fn find(store: &Store, name: &str, wall_ms: u64) -> Option<Listed> {
    let version = store.get(RecordClass::Resource, name)?;
    listed(store, &version.value, wall_ms)
}

/// Whether `holder` may take or keep `resource` at `wall_ms`.
fn may_hold(store: &Store, resource: &str, by: &str, wall_ms: u64) -> Result<(), Error> {
    let found = find(store, resource, wall_ms).ok_or(Error::NotFound)?;
    match &found.holder {
        Some(other) if other != by => Err(Error::Busy),
        // Keeping what one holds is always allowed: a lost resource is still its holder's.
        Some(_) => Ok(()),
        None if found.state == ResourceState::Ready => Ok(()),
        None => Err(Error::Unavailable),
    }
}

/// Binds `resource` to `holder` until `expires_ms` on the wall clock, or until it is
/// released if that is zero. Binding again by the same holder renews the lease.
///
/// # Errors
///
/// [`Error::NotFound`] if there is no such resource, [`Error::Unavailable`] if it is not
/// ready, [`Error::Busy`] if another holds it.
pub fn bind(
    store: &mut Store,
    resource: &ResourceName,
    holder: &str,
    expires_ms: u64,
    wall_ms: u64,
) -> Result<HlcTimestamp, Error> {
    may_hold(store, resource.as_str(), holder, wall_ms)?;
    let lease = LeaseRecord {
        resource: resource.clone(),
        holder,
        expires_ms,
        label: "",
    };
    let mut value = vec![0_u8; 256 + holder.len()];
    let len = encode_fields(&lease, &mut value)?;
    Ok(store.put(
        RecordClass::Lease,
        resource.as_str(),
        &value[..len],
        wall_ms,
    ))
}

/// The wall clock was stepped by `by_ms`: every lease this node wrote that ends at a time
/// ends as much later (or earlier), so it lasts as long as it was given. A lease another node
/// wrote is that node's to move, and one with no end has nothing to move. How many moved.
pub fn shift_leases(store: &mut Store, by_ms: i64, wall_ms: u64) -> usize {
    let own: Vec<Record> = store
        .list(RecordClass::Lease, "")
        .filter(|record| store.wrote(&record.version.stamp))
        .collect();
    let mut moved = 0;
    for record in own {
        let Ok(lease) = LeaseRecord::decode(&record.version.value) else {
            continue;
        };
        if lease.expires_ms == 0 {
            continue;
        }
        let shifted = LeaseRecord {
            expires_ms: lease.expires_ms.saturating_add_signed(by_ms).max(1),
            ..lease
        };
        let mut value = vec![0_u8; record.version.value.len() + 16];
        if let Ok(len) = encode_fields(&shifted, &mut value) {
            store.put(RecordClass::Lease, &record.key, &value[..len], wall_ms);
            moved += 1;
        }
    }
    moved
}

/// Releases `resource` if `holder` holds it. Whether it did.
pub fn release(store: &mut Store, resource: &str, by: &str, wall_ms: u64) -> bool {
    if holder(store, resource, wall_ms).as_deref() != Some(by) {
        return false;
    }
    store
        .delete(RecordClass::Lease, resource, wall_ms)
        .is_some()
}

/// Releases everything `holder` holds. How many.
pub fn release_all(store: &mut Store, by: &str, wall_ms: u64) -> usize {
    let held: Vec<String> = store
        .list(RecordClass::Lease, "")
        .filter(|record| holder(store, &record.key, wall_ms).as_deref() == Some(by))
        .map(|record| record.key)
        .collect();
    for resource in &held {
        store.delete(RecordClass::Lease, resource, wall_ms);
    }
    held.len()
}

impl Resources {
    /// The resources of the node `me`.
    #[must_use]
    pub const fn new(me: NodeName) -> Self {
        Self::with_quiet_ms(me, QUIET_MS)
    }

    /// The resources of the node `me`, whose leases can be taken from a holder quiet for
    /// `quiet_ms` (`leases.quiet_ms`).
    #[must_use]
    pub const fn with_quiet_ms(me: NodeName, quiet_ms: u64) -> Self {
        Self {
            me,
            providers: BTreeMap::new(),
            contest: Contest::new(quiet_ms),
            bind_holders: false,
        }
    }

    /// With security on, a lease's holder is the principal of the caller that writes it: what
    /// the caller put there is kept as the lease's `label`. See [`Resources::as_written`].
    pub const fn bind_holders(&mut self, bind: bool) {
        self.bind_holders = bind;
    }

    /// The value `entry` is to be checked and stored as: with holders bound, a lease whose
    /// holder is not `caller`'s principal is written with the principal as its holder, and the
    /// holder it said as its label. `None`: as it is.
    ///
    /// # Errors
    ///
    /// [`Error::Malformed`] for a lease that does not decode.
    pub fn as_written(
        &self,
        caller: &Caller<'_>,
        entry: &RecordEntry<'_>,
    ) -> Result<Option<Vec<u8>>, Error> {
        if !self.bind_holders || entry.class != RecordClass::Lease || entry.deleted {
            return Ok(None);
        }
        let lease = LeaseRecord::decode(entry.value.0)?;
        if lease.holder == caller.principal {
            return Ok(None);
        }
        let bound = LeaseRecord {
            holder: caller.principal,
            label: lease.holder,
            ..lease
        };
        let mut value = vec![0_u8; 256 + caller.principal.len() + lease.holder.len()];
        let len = encode_fields(&bound, &mut value)?;
        value.truncate(len);
        Ok(Some(value))
    }

    /// How long a holder must be quiet before its lease can be taken (`leases.quiet_ms`).
    pub const fn set_quiet_ms(&mut self, quiet_ms: u64) {
        self.contest.set_quiet_ms(quiet_ms);
    }

    /// `session` went quiet `quiet_ms` before `now_ms`, or with `quiet_ms` zero is heard again.
    pub fn quiet(&mut self, session: u32, quiet_ms: u64, now_ms: u64) {
        self.contest.quiet(session, quiet_ms, now_ms);
    }

    /// The words `session` is refused with for running `action`, if it lost the lease of the
    /// resource the action is offered by and the lease's taker still holds it.
    #[must_use]
    pub fn lost_action(
        &self,
        store: &Store,
        session: u32,
        action: &str,
        wall_ms: u64,
    ) -> Option<String> {
        self.contest
            .lost_by(store, session, wall_ms)
            .into_iter()
            .find(|(resource, _)| {
                find(store, resource, wall_ms)
                    .is_some_and(|found| found.actions.iter().any(|offered| offered == action))
            })
            .map(|(_, words)| words)
    }

    /// `taken` was written by `session`: the lease is its now.
    pub fn taken(&mut self, taken: Taken, session: u32) {
        self.contest.taken(taken, session);
    }

    /// How many resources have a provider here now.
    #[must_use]
    pub fn provided(&self) -> usize {
        self.providers.len()
    }

    /// The names of the resources that have a provider here now.
    pub fn providing(&self) -> impl Iterator<Item = &str> {
        self.providers.keys().map(String::as_str)
    }

    /// Whether `resource` has a provider here now.
    #[must_use]
    pub fn provides(&self, resource: &str) -> bool {
        self.providers.contains_key(resource)
    }

    /// Forgets who provides what: the sessions are gone with the comm core they came
    /// through, or node logic was away. A provider that is still there writes its resources
    /// again; [`Resources::lose_unprovided`] marks the rest lost.
    pub fn forget_sessions(&mut self) {
        self.providers.clear();
        self.contest.forget_sessions();
    }

    /// Marks restoring every resource of this node that is not lost: kept across a restart,
    /// and nobody has written it again yet. Call it after [`Resources::forget_sessions`].
    /// How many that was.
    pub fn restoring(&mut self, store: &mut Store, wall_ms: u64) -> usize {
        self.mine(store)
            .iter()
            .filter(|name| set_state(store, name, ResourceState::Restoring, wall_ms))
            .count()
    }

    /// The names of this node's resources that nobody provides now.
    fn mine(&self, store: &Store) -> Vec<String> {
        store
            .list(RecordClass::Resource, "")
            .filter(|record| !self.providers.contains_key(&record.key))
            .filter(|record| {
                ResourceRecord::decode(&record.version.value)
                    .is_ok_and(|resource| resource.node == self.me)
            })
            .map(|record| record.key)
            .collect()
    }

    /// Marks lost every resource of this node that nobody provides now. How many that was.
    pub fn lose_unprovided(&mut self, store: &mut Store, wall_ms: u64) -> usize {
        self.mine(store)
            .iter()
            .filter(|name| lose(store, name, wall_ms))
            .count()
    }

    /// Whether `session` may write `entry`. Records of other classes are not this crate's
    /// and are let through. Call it for every entry of a write before any of them is made.
    ///
    /// # Errors
    ///
    /// [`Error::Malformed`] for a record that is not what its class says or is under another
    /// name than its own, [`Error::Denied`] for a resource on another node or one that
    /// another program provides, what [`bind`] refuses a lease for, and
    /// [`Error::LeaseLost`] for a lease this session lost while quiet, and [`Error::Denied`]
    /// for freeing a lease held by another: only the session that wrote it, a caller whose
    /// principal is its holder, or root frees it. A refusal as busy says who holds the lease
    /// and since when.
    ///
    /// `Ok(Some)`: the write takes a lease from a quiet holder. Call [`Resources::taken`] once
    /// it is made. `now_ms` is node logic's clock, and `in_flight` says whether a session has a
    /// run of one of the given actions under way.
    pub fn check(
        &self,
        store: &Store,
        caller: &Caller<'_>,
        entry: &RecordEntry<'_>,
        (wall_ms, now_ms): (u64, u64),
        in_flight: &dyn Fn(u32, &[String]) -> bool,
    ) -> Result<Option<Taken>, Refused> {
        let session = caller.session;
        self.check_entry(store, session, entry, wall_ms)?;
        if entry.class != RecordClass::Lease {
            return Ok(None);
        }
        if entry.deleted {
            // A lease that has ended, or is not there, holds nothing: anyone may clear it.
            return match holder(store, entry.key, wall_ms) {
                Some(held) if !self.contest.may_free(caller, entry.key, &held) => Err(Refused {
                    error: Error::Denied,
                    words: Some(format!(
                        "{} is held by {held}: only its holder or root frees it",
                        entry.key
                    )),
                }),
                _ => Ok(None),
            };
        }
        let lease = LeaseRecord::decode(entry.value.0)?;
        match may_hold(store, entry.key, lease.holder, wall_ms) {
            Err(Error::Busy) => self
                .contest
                .contest(store, session, entry.key, lease.holder, now_ms, in_flight)
                .map(Some),
            other => other.map(|()| None).map_err(Refused::from),
        }
    }

    fn check_entry(
        &self,
        store: &Store,
        session: u32,
        entry: &RecordEntry<'_>,
        wall_ms: u64,
    ) -> Result<(), Refused> {
        match entry.class {
            RecordClass::Resource => {
                if self
                    .providers
                    .get(entry.key)
                    .is_some_and(|provider| *provider != session)
                {
                    return Err(Error::Denied.into());
                }
                if entry.deleted {
                    return Ok(());
                }
                let resource = ResourceRecord::decode(entry.value.0)?;
                if resource.name.as_str() != entry.key {
                    return Err(Error::Malformed.into());
                }
                if resource.node != self.me {
                    return Err(Error::Denied.into());
                }
                Ok(())
            }
            RecordClass::Lease => {
                if let Some(words) = self.contest.lost_here(store, session, entry.key, wall_ms) {
                    return Err(Refused {
                        error: Error::LeaseLost,
                        words: Some(words),
                    });
                }
                if entry.deleted {
                    return Ok(());
                }
                let lease = LeaseRecord::decode(entry.value.0)?;
                if lease.resource.as_str() != entry.key || lease.holder.is_empty() {
                    return Err(Error::Malformed.into());
                }
                Ok(())
            }
            _ => Ok(()),
        }
    }

    /// `session` wrote `entry`, which passed [`Resources::check`]. A resource that was taken
    /// away takes its lease with it.
    pub fn wrote(
        &mut self,
        store: &mut Store,
        session: u32,
        entry: &RecordEntry<'_>,
        wall_ms: u64,
    ) {
        if entry.class == RecordClass::Lease {
            self.contest.wrote(entry.key, session, entry.deleted);
            return;
        }
        if entry.class != RecordClass::Resource {
            return;
        }
        if entry.deleted {
            self.providers.remove(entry.key);
            self.contest.wrote(entry.key, session, true);
            store.delete(RecordClass::Lease, entry.key, wall_ms);
        } else {
            self.providers.insert(entry.key.to_owned(), session);
        }
    }

    /// A session is gone. What it provided is lost. How many resources that was.
    pub fn gone(&mut self, store: &mut Store, session: u32, wall_ms: u64) -> usize {
        self.contest.gone(session);
        let names: Vec<String> = self
            .providers
            .iter()
            .filter(|(_, provider)| **provider == session)
            .map(|(name, _)| name.clone())
            .collect();
        for name in &names {
            self.providers.remove(name);
        }
        names
            .iter()
            .filter(|name| lose(store, name, wall_ms))
            .count()
    }
}

/// Marks a resource lost. Whether there was one to mark.
fn lose(store: &mut Store, name: &str, wall_ms: u64) -> bool {
    set_state(store, name, ResourceState::Lost, wall_ms)
}

/// Writes `state` into the resource record `name`, unless it is lost or already says so.
/// Whether it did.
fn set_state(store: &mut Store, name: &str, state: ResourceState, wall_ms: u64) -> bool {
    let Some(version) = store.get(RecordClass::Resource, name) else {
        return false;
    };
    let value = version.value.clone();
    let Ok(resource) = ResourceRecord::decode(&value) else {
        return false;
    };
    if resource.state == ResourceState::Lost || resource.state == state {
        return false;
    }
    let lost = ResourceRecord {
        state,
        attributes: Bytes(resource.attributes.0),
        ..resource
    };
    let mut out = vec![0_u8; value.len() + 16];
    let Ok(len) = encode_fields(&lost, &mut out) else {
        return false;
    };
    store.put(RecordClass::Resource, name, &out[..len], wall_ms);
    true
}
