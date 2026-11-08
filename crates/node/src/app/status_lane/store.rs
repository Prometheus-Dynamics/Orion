//! The in-memory latest-value store behind the volatile status lane.
//!
//! Pure data structure: no locks, no clock, no I/O. Callers pass the current Unix time in
//! milliseconds. Memory is bounded by the node-wide and per-publisher entry caps plus the fixed
//! per-entry key and value size limits.

use orion::control_plane::{StatusEntry, StatusKey, StatusQuery, StatusSubject, TypedConfigValue};
use std::borrow::Borrow;
use std::cmp::Ordering;
use std::collections::BTreeMap;

/// Longest status key, in bytes.
pub(crate) const MAX_STATUS_KEY_BYTES: usize = 128;
/// Hard ceiling for string or byte values, in bytes. `ORION_NODE_STATUS_MAX_VALUE_BYTES` cannot
/// raise the cap above it (64 KiB, one SPI transaction, like `Bytes` action values).
pub(crate) const STATUS_VALUE_CEILING_BYTES: usize = 64 * 1024;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct StatusLimits {
    pub(crate) max_entries: usize,
    pub(crate) max_entries_per_publisher: usize,
    pub(crate) max_ttl_ms: u64,
    /// Longest string or byte value accepted.
    pub(crate) max_value_bytes: usize,
    /// Bytes of string and byte values held node-wide (the memory bound behind the entry caps).
    pub(crate) max_total_value_bytes: usize,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum StatusPublishError {
    Invalid(String),
    NodeFull { needed: usize, max: usize },
    PublisherFull { needed: usize, max: usize },
    ValueBytesFull { needed: usize, max: usize },
}

impl std::fmt::Display for StatusPublishError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Invalid(reason) => write!(f, "invalid status entry: {reason}"),
            Self::NodeFull { needed, max } => write!(
                f,
                "status lane is full: batch needs {needed} entries, node cap is {max}"
            ),
            Self::PublisherFull { needed, max } => write!(
                f,
                "publisher status cap reached: batch needs {needed} entries, cap is {max}"
            ),
            Self::ValueBytesFull { needed, max } => write!(
                f,
                "status value budget reached: batch needs {needed} value bytes, budget is {max}"
            ),
        }
    }
}

/// A `(subject, key)` identity seen through a reference, so an incoming entry can look up the
/// stored one without building (and allocating) a [`StatusKey`].
trait KeyView {
    fn subject(&self) -> &StatusSubject;
    fn key(&self) -> &str;
}

impl KeyView for StatusKey {
    fn subject(&self) -> &StatusSubject {
        &self.subject
    }
    fn key(&self) -> &str {
        &self.key
    }
}

impl KeyView for StatusEntry {
    fn subject(&self) -> &StatusSubject {
        &self.subject
    }
    fn key(&self) -> &str {
        &self.key
    }
}

// The order is `StatusKey`'s derived one (subject, then key), which `Borrow` requires.
impl PartialEq for dyn KeyView + '_ {
    fn eq(&self, other: &Self) -> bool {
        self.subject() == other.subject() && self.key() == other.key()
    }
}

impl Eq for dyn KeyView + '_ {}

impl PartialOrd for dyn KeyView + '_ {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for dyn KeyView + '_ {
    fn cmp(&self, other: &Self) -> Ordering {
        self.subject()
            .cmp(other.subject())
            .then_with(|| self.key().cmp(other.key()))
    }
}

impl<'a> Borrow<dyn KeyView + 'a> for StatusKey {
    fn borrow(&self) -> &(dyn KeyView + 'a) {
        self
    }
}

#[derive(Debug)]
struct Stored {
    entry: StatusEntry,
    publisher: String,
    /// The entry's place in `expiries`.
    expiry: (u64, u64),
}

/// Result of an accepted publish.
#[derive(Debug, Default, PartialEq, Eq)]
pub(crate) struct PublishOutcome {
    /// Entries whose value is new or changed (watchers are told about these).
    pub(crate) changed: Vec<StatusEntry>,
    /// Entries accepted in total (including TTL refreshes of unchanged values).
    pub(crate) accepted: usize,
}

#[derive(Debug, Default)]
pub(crate) struct StatusStore {
    entries: BTreeMap<StatusKey, Stored>,
    per_publisher: BTreeMap<String, usize>,
    /// `(expires_at_ms, id)` to key: removing an entry's expiry needs no copy of its key.
    expiries: BTreeMap<(u64, u64), StatusKey>,
    next_expiry_id: u64,
    /// Sum of the value sizes of every held entry (kept exact by `remove` and `publish`).
    value_bytes: usize,
}

impl StatusStore {
    pub(crate) fn len(&self) -> usize {
        self.entries.len()
    }

    pub(crate) fn publishers(&self) -> usize {
        self.per_publisher.len()
    }

    /// Earliest expiry time, if any entry is held.
    pub(crate) fn next_expiry_ms(&self) -> Option<u64> {
        self.expiries.first_key_value().map(|((at, _), _)| *at)
    }

    /// Applies a batch atomically: either every entry is stored or none is.
    #[cfg(test)]
    pub(crate) fn publish(
        &mut self,
        publisher: &str,
        entries: Vec<StatusEntry>,
        now_ms: u64,
        limits: StatusLimits,
    ) -> Result<PublishOutcome, StatusPublishError> {
        let mut entries = entries;
        self.publish_with(publisher, &mut entries, now_ms, limits, true)
    }

    /// [`Self::publish`], collecting the changed entries only when `watched` (someone is told).
    ///
    /// A batch that only refreshes keys its publisher already holds, the steady state of a
    /// device reporting the same keys, is applied in place: no key is copied and nothing is
    /// allocated unless a watcher needs the changed entries. Then only the values are taken
    /// from `entries`, which keeps the subjects and keys for the caller to fill again; any other
    /// batch is moved out, leaving `entries` empty.
    pub(crate) fn publish_with(
        &mut self,
        publisher: &str,
        entries: &mut Vec<StatusEntry>,
        now_ms: u64,
        limits: StatusLimits,
        watched: bool,
    ) -> Result<PublishOutcome, StatusPublishError> {
        for entry in entries.iter() {
            validate(entry, limits.max_value_bytes)?;
        }
        if self.refreshes_only(publisher, entries) {
            return self.refresh(entries, now_ms, limits, watched);
        }
        let entries = std::mem::take(entries);
        // Later entries for the same key win within one batch. One vector, sorted by key (a stable
        // sort keeps the batch order within a key, so the last of each run wins), instead of a map
        // node per entry: the common batch is one entry.
        let mut batch: Vec<(StatusKey, StatusEntry)> = Vec::with_capacity(entries.len());
        for entry in entries {
            batch.push((entry.status_key(), entry));
        }
        if batch.len() > 1 {
            batch.sort_by(|a, b| a.0.cmp(&b.0));
            let mut kept: Vec<(StatusKey, StatusEntry)> = Vec::with_capacity(batch.len());
            for item in batch {
                match kept.last_mut() {
                    Some(last) if last.0 == item.0 => *last = item,
                    _ => kept.push(item),
                }
            }
            batch = kept;
        }
        let keys = || batch.iter().map(|(key, _)| key);
        let new_global = keys()
            .filter(|key| !self.entries.contains_key(*key))
            .count();
        let needed = self.entries.len().saturating_add(new_global);
        if needed > limits.max_entries {
            return Err(StatusPublishError::NodeFull {
                needed,
                max: limits.max_entries,
            });
        }
        let new_for_publisher = keys()
            .filter(|key| {
                self.entries
                    .get(*key)
                    .is_none_or(|stored| stored.publisher != publisher)
            })
            .count();
        let held = self.per_publisher.get(publisher).copied().unwrap_or(0);
        let needed = held.saturating_add(new_for_publisher);
        if needed > limits.max_entries_per_publisher {
            return Err(StatusPublishError::PublisherFull {
                needed,
                max: limits.max_entries_per_publisher,
            });
        }

        let batch_value_bytes: usize = batch.iter().map(|(_, e)| value_len(&e.value)).sum();
        let replaced_value_bytes: usize = keys()
            .filter_map(|key| self.entries.get(key))
            .map(|stored| value_len(&stored.entry.value))
            .sum();
        let needed_value_bytes = self
            .value_bytes
            .saturating_sub(replaced_value_bytes)
            .saturating_add(batch_value_bytes);
        if needed_value_bytes > limits.max_total_value_bytes {
            return Err(StatusPublishError::ValueBytesFull {
                needed: needed_value_bytes,
                max: limits.max_total_value_bytes,
            });
        }

        let mut outcome = PublishOutcome::default();
        for (key, mut entry) in batch {
            entry.ttl_ms = if entry.ttl_ms == 0 {
                limits.max_ttl_ms
            } else {
                entry.ttl_ms.min(limits.max_ttl_ms)
            };
            entry.published_at_ms = now_ms;
            let previous = self.take(&key);
            let changed = previous
                .as_ref()
                .is_none_or(|previous| previous.entry.value != entry.value);
            let owner = match previous {
                // The same publisher replacing its entry: its count and name stay, no new string.
                Some(previous) if previous.publisher == publisher => previous.publisher,
                Some(previous) => {
                    self.release(&previous.publisher);
                    self.count(publisher)
                }
                None => self.count(publisher),
            };
            self.next_expiry_id = self.next_expiry_id.wrapping_add(1);
            let expiry = (entry.expires_at_ms(), self.next_expiry_id);
            self.expiries.insert(expiry, key.clone());
            self.value_bytes = self.value_bytes.saturating_add(value_len(&entry.value));
            if changed && watched {
                outcome.changed.push(entry.clone());
            }
            outcome.accepted += 1;
            self.entries.insert(
                key,
                Stored {
                    entry,
                    publisher: owner,
                    expiry,
                },
            );
        }
        Ok(outcome)
    }

    /// Whether every entry of the batch replaces one `publisher` already holds, with no key twice
    /// (small batches only: the check is quadratic).
    fn refreshes_only(&self, publisher: &str, entries: &[StatusEntry]) -> bool {
        const MAX_IN_PLACE: usize = 16;
        entries.len() <= MAX_IN_PLACE
            && entries.iter().enumerate().all(|(i, entry)| {
                let view: &dyn KeyView = entry;
                self.entries
                    .get(view)
                    .is_some_and(|stored| stored.publisher == publisher)
                    && entries[..i].iter().all(|earlier| {
                        let earlier: &dyn KeyView = earlier;
                        earlier != view
                    })
            })
    }

    /// Applies a batch of [`Self::refreshes_only`] entries in place.
    fn refresh(
        &mut self,
        entries: &mut [StatusEntry],
        now_ms: u64,
        limits: StatusLimits,
        watched: bool,
    ) -> Result<PublishOutcome, StatusPublishError> {
        let mut replaced_value_bytes = 0usize;
        let mut batch_value_bytes = 0usize;
        for entry in entries.iter() {
            let view: &dyn KeyView = entry;
            if let Some(stored) = self.entries.get(view) {
                replaced_value_bytes += value_len(&stored.entry.value);
            }
            batch_value_bytes += value_len(&entry.value);
        }
        let needed_value_bytes = self
            .value_bytes
            .saturating_sub(replaced_value_bytes)
            .saturating_add(batch_value_bytes);
        if needed_value_bytes > limits.max_total_value_bytes {
            return Err(StatusPublishError::ValueBytesFull {
                needed: needed_value_bytes,
                max: limits.max_total_value_bytes,
            });
        }
        self.value_bytes = needed_value_bytes;
        let mut outcome = PublishOutcome {
            changed: Vec::new(),
            accepted: entries.len(),
        };
        for entry in entries.iter_mut() {
            let view: &dyn KeyView = &*entry;
            let Some(stored) = self.entries.get_mut(view) else {
                continue; // checked by `refreshes_only`
            };
            let ttl_ms = if entry.ttl_ms == 0 {
                limits.max_ttl_ms
            } else {
                entry.ttl_ms.min(limits.max_ttl_ms)
            };
            let changed = stored.entry.value != entry.value;
            stored.entry.value = std::mem::replace(&mut entry.value, TypedConfigValue::Bool(false));
            stored.entry.ttl_ms = ttl_ms;
            stored.entry.published_at_ms = now_ms;
            self.next_expiry_id = self.next_expiry_id.wrapping_add(1);
            let expiry = (stored.entry.expires_at_ms(), self.next_expiry_id);
            let previous = std::mem::replace(&mut stored.expiry, expiry);
            if changed && watched {
                outcome.changed.push(stored.entry.clone());
            }
            // The key moves from the old expiry slot to the new one: no copy.
            if let Some(key) = self.expiries.remove(&previous) {
                self.expiries.insert(expiry, key);
            }
        }
        Ok(outcome)
    }

    /// Drops every entry whose TTL ran out at `now_ms` and returns their keys.
    pub(crate) fn expire(&mut self, now_ms: u64) -> Vec<StatusKey> {
        let mut expired = Vec::new();
        while let Some(entry) = self.expiries.first_entry() {
            if entry.key().0 > now_ms {
                break;
            }
            let key = entry.remove();
            self.remove(&key);
            expired.push(key);
        }
        expired
    }

    /// Live entries matching `query`, ordered by subject then key.
    pub(crate) fn query(&self, query: &StatusQuery, now_ms: u64) -> Vec<StatusEntry> {
        self.entries
            .iter()
            .filter(|(key, stored)| {
                stored.entry.expires_at_ms() > now_ms && query.matches(&key.subject, &key.key)
            })
            .map(|(_, stored)| stored.entry.clone())
            .collect()
    }

    fn remove(&mut self, key: &StatusKey) -> Option<Stored> {
        let stored = self.take(key)?;
        self.release(&stored.publisher);
        Some(stored)
    }

    /// Removes the entry and its expiry and value bytes, but not its publisher's count.
    fn take(&mut self, key: &StatusKey) -> Option<Stored> {
        let stored = self.entries.remove(key)?;
        self.expiries.remove(&stored.expiry);
        self.value_bytes = self
            .value_bytes
            .saturating_sub(value_len(&stored.entry.value));
        Some(stored)
    }

    /// One entry more for `publisher`; returns its name for the stored entry.
    fn count(&mut self, publisher: &str) -> String {
        match self.per_publisher.get_mut(publisher) {
            Some(count) => *count += 1,
            None => {
                self.per_publisher.insert(publisher.to_owned(), 1);
            }
        }
        publisher.to_owned()
    }

    /// One entry less for `publisher`.
    fn release(&mut self, publisher: &str) {
        if let Some(count) = self.per_publisher.get_mut(publisher) {
            *count = count.saturating_sub(1);
            if *count == 0 {
                self.per_publisher.remove(publisher);
            }
        }
    }
}

/// Size of a value's payload: string and byte values count their bytes, scalars count nothing.
fn value_len(value: &TypedConfigValue) -> usize {
    match value {
        TypedConfigValue::String(value) => value.len(),
        TypedConfigValue::Bytes(value) => value.len(),
        TypedConfigValue::Bool(_)
        | TypedConfigValue::Int(_)
        | TypedConfigValue::UInt(_)
        | TypedConfigValue::F64(_) => 0,
    }
}

fn validate(entry: &StatusEntry, max_value_bytes: usize) -> Result<(), StatusPublishError> {
    if entry.key.trim().is_empty() {
        return Err(StatusPublishError::Invalid("empty key".into()));
    }
    if entry.key.len() > MAX_STATUS_KEY_BYTES {
        return Err(StatusPublishError::Invalid(format!(
            "key `{}...` is longer than {MAX_STATUS_KEY_BYTES} bytes",
            entry.key.chars().take(16).collect::<String>()
        )));
    }
    let len = value_len(&entry.value);
    if len > max_value_bytes {
        return Err(StatusPublishError::Invalid(format!(
            "value of `{}` is {len} bytes, limit is {max_value_bytes}",
            entry.key
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use orion::{ProviderId, control_plane::StatusSubject};

    const LIMITS: StatusLimits = StatusLimits {
        max_entries: 4,
        max_entries_per_publisher: 3,
        max_ttl_ms: 1_000,
        max_value_bytes: 1024,
        max_total_value_bytes: 4096,
    };

    fn entry(provider: &str, key: &str, value: u64) -> StatusEntry {
        StatusEntry::new(
            StatusSubject::Provider(ProviderId::new(provider)),
            key,
            TypedConfigValue::UInt(value),
        )
    }

    #[test]
    fn latest_value_wins_and_unchanged_values_only_refresh() {
        let mut store = StatusStore::default();
        let first = store
            .publish("a", vec![entry("p", "fps", 30)], 10, LIMITS)
            .expect("publish");
        assert_eq!(first.changed.len(), 1);
        assert_eq!(first.changed[0].ttl_ms, 1_000);
        assert_eq!(first.changed[0].published_at_ms, 10);

        let same = store
            .publish("a", vec![entry("p", "fps", 30)], 20, LIMITS)
            .expect("publish");
        assert!(same.changed.is_empty());
        assert_eq!(same.accepted, 1);

        let newer = store
            .publish(
                "a",
                vec![entry("p", "fps", 29).with_ttl_ms(5_000)],
                30,
                LIMITS,
            )
            .expect("publish");
        assert_eq!(newer.changed.len(), 1);
        let all = store.query(&StatusQuery::all(), 30);
        assert_eq!(all.len(), 1);
        assert_eq!(all[0].value, TypedConfigValue::UInt(29));
        assert_eq!(all[0].ttl_ms, 1_000, "TTL is capped at the node maximum");
    }

    #[test]
    fn a_refresh_in_place_moves_the_expiry_and_keeps_the_batch() {
        let mut store = StatusStore::default();
        let mut batch = vec![entry("p", "fps", 30), entry("p", "temp", 40)];
        store
            .publish_with("a", &mut batch.clone(), 0, LIMITS, true)
            .expect("first publish");
        let refreshed = store
            .publish_with("a", &mut batch, 900, LIMITS, false)
            .expect("refresh");
        assert_eq!(refreshed.accepted, 2);
        assert!(refreshed.changed.is_empty(), "nobody watches");
        assert_eq!(
            batch.len(),
            2,
            "an in-place refresh leaves the subjects and keys"
        );
        assert_eq!(batch[0].key, "fps");
        // The first publish expired at 1,000; the refresh moved both to 1,900.
        assert!(store.expire(1_000).is_empty());
        assert_eq!(store.len(), 2);
        assert_eq!(store.next_expiry_ms(), Some(1_900));
        assert_eq!(store.expire(1_900).len(), 2);
        assert_eq!(store.len(), 0);
    }

    #[test]
    fn a_batch_with_a_new_key_or_a_repeated_key_takes_the_full_path() {
        let mut store = StatusStore::default();
        store
            .publish("a", vec![entry("p", "fps", 30)], 0, LIMITS)
            .expect("publish");
        let mut batch = vec![entry("p", "fps", 31), entry("p", "new", 1)];
        let outcome = store
            .publish_with("a", &mut batch, 1, LIMITS, true)
            .expect("publish");
        assert_eq!(outcome.changed.len(), 2);
        assert!(batch.is_empty(), "the full path moves the batch out");
        let mut repeated = vec![entry("p", "fps", 32), entry("p", "fps", 33)];
        store
            .publish_with("a", &mut repeated, 2, LIMITS, true)
            .expect("publish");
        let all = store.query(&StatusQuery::all(), 2);
        let fps = all.iter().find(|entry| entry.key == "fps").expect("fps");
        assert_eq!(
            fps.value,
            TypedConfigValue::UInt(33),
            "the last of a key wins"
        );
        // Another publisher's key is not refreshed in place either.
        let mut other = vec![entry("p", "fps", 34)];
        store
            .publish_with("b", &mut other, 3, LIMITS, true)
            .expect("publish");
        assert_eq!(store.publishers(), 2);
    }

    #[test]
    fn entries_expire_after_their_ttl() {
        let mut store = StatusStore::default();
        store
            .publish(
                "a",
                vec![
                    entry("p", "short", 1).with_ttl_ms(100),
                    entry("p", "long", 2),
                ],
                0,
                LIMITS,
            )
            .expect("publish");
        assert_eq!(store.next_expiry_ms(), Some(100));
        assert!(store.expire(99).is_empty());
        assert_eq!(store.query(&StatusQuery::all(), 100).len(), 1);
        let expired = store.expire(100);
        assert_eq!(expired.len(), 1);
        assert_eq!(expired[0].key, "short");
        assert_eq!(store.len(), 1);
        assert_eq!(store.next_expiry_ms(), Some(1_000));
        assert_eq!(store.expire(5_000).len(), 1);
        assert_eq!(store.len(), 0);
        assert_eq!(store.publishers(), 0);
    }

    #[test]
    fn caps_reject_whole_batches() {
        let mut store = StatusStore::default();
        let batch = |publisher: &str, keys: &[&str]| {
            keys.iter()
                .map(|key| entry(publisher, key, 1))
                .collect::<Vec<_>>()
        };
        store
            .publish("a", batch("a", &["1", "2", "3"]), 0, LIMITS)
            .expect("three entries fit the publisher cap");
        assert!(matches!(
            store.publish("a", batch("a", &["4"]), 0, LIMITS),
            Err(StatusPublishError::PublisherFull { needed: 4, max: 3 })
        ));
        // Updating existing keys never needs new capacity.
        store
            .publish("a", batch("a", &["1", "2", "3"]), 0, LIMITS)
            .expect("updates fit");
        store
            .publish("b", batch("b", &["1"]), 0, LIMITS)
            .expect("fourth entry fits the node cap");
        assert!(matches!(
            store.publish("b", batch("b", &["2"]), 0, LIMITS),
            Err(StatusPublishError::NodeFull { needed: 5, max: 4 })
        ));
        assert_eq!(store.len(), 4);
    }

    #[test]
    fn invalid_entries_reject_the_batch() {
        let mut store = StatusStore::default();
        let long_key = "k".repeat(MAX_STATUS_KEY_BYTES + 1);
        assert!(matches!(
            store.publish(
                "a",
                vec![entry("p", "ok", 1), entry("p", &long_key, 1)],
                0,
                LIMITS
            ),
            Err(StatusPublishError::Invalid(_))
        ));
        let big = StatusEntry::new(
            StatusSubject::Provider(ProviderId::new("p")),
            "blob",
            TypedConfigValue::Bytes(vec![0; LIMITS.max_value_bytes + 1]),
        );
        assert!(store.publish("a", vec![big], 0, LIMITS).is_err());
        assert_eq!(store.len(), 0);
    }

    #[test]
    fn value_budget_is_exact_and_refreshes_free_it() {
        let limits = StatusLimits {
            max_entries: 16,
            max_entries_per_publisher: 16,
            max_ttl_ms: 1_000,
            max_value_bytes: 1024,
            max_total_value_bytes: 100,
        };
        let text = |key: &str, len: usize| {
            StatusEntry::new(
                StatusSubject::Provider(ProviderId::new("p")),
                key,
                TypedConfigValue::String("x".repeat(len)),
            )
        };
        let mut store = StatusStore::default();
        store
            .publish("a", vec![text("a", 60)], 0, limits)
            .expect("60 of 100 bytes");
        // 60 + 50 is over budget: the whole batch is refused and nothing changes.
        assert!(matches!(
            store.publish("a", vec![text("b", 50)], 0, limits),
            Err(StatusPublishError::ValueBytesFull {
                needed: 110,
                max: 100
            })
        ));
        assert_eq!(store.len(), 1);
        // Replacing the held value frees its bytes first: 30 (replacement) fits.
        store
            .publish("a", vec![text("a", 30)], 0, limits)
            .expect("replacement needs only its own bytes");
        store
            .publish("a", vec![text("b", 70)], 0, limits)
            .expect("30 + 70 fits exactly");
        // Expiry releases the bytes too.
        assert_eq!(store.expire(2_000).len(), 2);
        store
            .publish("a", vec![text("c", 100)], 2_000, limits)
            .expect("expired entries no longer count");
    }
}
