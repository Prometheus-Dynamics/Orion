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

use std::collections::BTreeMap;

use orion_msg_records::{
    LeaseRecord, RecordClass, RecordEntry, ResourceDescription, ResourceRecord, ResourceState,
};
use orion_store::Store;
use orion_types::{Error, HlcTimestamp, NodeName, ResourceName};
use orion_wire::body::encode_fields;
use orion_wire::{Bytes, Decode};

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
        Self {
            me,
            providers: BTreeMap::new(),
        }
    }

    /// How many resources have a provider here now.
    #[must_use]
    pub fn provided(&self) -> usize {
        self.providers.len()
    }

    /// Forgets who provides what: the sessions are gone with the comm core they came
    /// through. Their resources are marked lost.
    pub fn forget_sessions(&mut self, store: &mut Store, wall_ms: u64) -> usize {
        let names: Vec<String> = std::mem::take(&mut self.providers).into_keys().collect();
        names
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
    /// another program provides, and what [`bind`] refuses a lease for.
    pub fn check(
        &self,
        store: &Store,
        session: u32,
        entry: &RecordEntry<'_>,
        wall_ms: u64,
    ) -> Result<(), Error> {
        match entry.class {
            RecordClass::Resource => {
                if self
                    .providers
                    .get(entry.key)
                    .is_some_and(|provider| *provider != session)
                {
                    return Err(Error::Denied);
                }
                if entry.deleted {
                    return Ok(());
                }
                let resource = ResourceRecord::decode(entry.value.0)?;
                if resource.name.as_str() != entry.key {
                    return Err(Error::Malformed);
                }
                if resource.node != self.me {
                    return Err(Error::Denied);
                }
                Ok(())
            }
            RecordClass::Lease => {
                if entry.deleted {
                    return Ok(());
                }
                let lease = LeaseRecord::decode(entry.value.0)?;
                if lease.resource.as_str() != entry.key || lease.holder.is_empty() {
                    return Err(Error::Malformed);
                }
                may_hold(store, entry.key, lease.holder, wall_ms)
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
        if entry.class != RecordClass::Resource {
            return;
        }
        if entry.deleted {
            self.providers.remove(entry.key);
            store.delete(RecordClass::Lease, entry.key, wall_ms);
        } else {
            self.providers.insert(entry.key.to_owned(), session);
        }
    }

    /// A session is gone. What it provided is lost. How many resources that was.
    pub fn gone(&mut self, store: &mut Store, session: u32, wall_ms: u64) -> usize {
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
    let Some(version) = store.get(RecordClass::Resource, name) else {
        return false;
    };
    let value = version.value.clone();
    let Ok(resource) = ResourceRecord::decode(&value) else {
        return false;
    };
    if resource.state == ResourceState::Lost {
        return false;
    }
    let lost = ResourceRecord {
        state: ResourceState::Lost,
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
